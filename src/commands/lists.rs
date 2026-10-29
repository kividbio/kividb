use crate::commands::blocking;
use crate::commands::ctx::Ctx;
use crate::commands::helpers::{lazy_expire_key, lazy_expire_on_read, parse_utf8};
use crate::commands::list_move;
use crate::keyspace_events::{KS_LIST, notify};
use crate::resp::write::*;
use crate::store;
use crate::store::Key;
use crate::store::ValueRef;
use crate::types::KiviDBValue;
use crate::{read_lock, write_lock};
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::Arc;

/// Pop up to `count` elements from one end of the list at `key`. Nothing if
/// there is no such key.
fn pop_from(ctx: &Ctx, key: &str, left: bool, count: usize) -> Result<Vec<Arc<[u8]>>, Vec<u8>> {
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);
    match map.get(key) {
        Some(ValueRef::List(list)) => {
            let mut list = list.write();
            let mut popped = Vec::with_capacity(count.min(list.len()));
            while popped.len() < count {
                let element = if left {
                    list.pop_front()
                } else {
                    list.pop_back()
                };
                match element {
                    Some(element) => popped.push(element),
                    None => break,
                }
            }
            Ok(popped)
        }
        Some(_) => Err(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => Ok(Vec::new()),
    }
}

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;

    match name {
        "LPUSH" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'lpush' command",
                ));
            }
            // The key as it stands in the command: it was copied to a
            // string of its own for every push, and the values gathered in
            // a vector before going into the list.
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let mut map = write_lock!(shard);

            let list = match map.get(key) {
                None => {
                    let l = Arc::new(RwLock::new(VecDeque::new()));
                    map.insert(Key::from(key), KiviDBValue::List(Arc::clone(&l)));
                    l
                }
                Some(ValueRef::List(l)) => Arc::clone(l),
                Some(_) => {
                    return Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }
            };

            let mut inner = list.write();
            // Every element goes into the list; a client blocked on the key
            // is woken afterwards and pops for itself (see
            // `blocking::wait_for_keys`).
            for value in &cmd[2..] {
                inner.push_front(Arc::from(*value));
            }
            let len = inner.len();
            drop(inner);
            drop(map);
            ctx.wait_table.notify_signal(key);
            notify(ctx.pubsub, *ctx.selected_db, KS_LIST, "lpush", key);
            Some(resp_integer(len as i64))
        }

        "RPUSH" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'rpush' command",
                ));
            }
            // The key as it stands in the command: it was copied to a
            // string of its own for every push, and the values gathered in
            // a vector before going into the list.
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let mut map = write_lock!(shard);

            let list = match map.get(key) {
                None => {
                    let l = Arc::new(RwLock::new(VecDeque::new()));
                    map.insert(Key::from(key), KiviDBValue::List(Arc::clone(&l)));
                    l
                }
                Some(ValueRef::List(l)) => Arc::clone(l),
                Some(_) => {
                    return Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }
            };

            let mut inner = list.write();
            inner.reserve(cmd.len() - 2);
            for value in &cmd[2..] {
                inner.push_back(Arc::from(*value));
            }
            let len = inner.len();
            drop(inner);
            drop(map);
            ctx.wait_table.notify_signal(key);
            notify(ctx.pubsub, *ctx.selected_db, KS_LIST, "rpush", key);
            Some(resp_integer(len as i64))
        }

        "LPOP" => {
            if cmd.len() < 2 || cmd.len() > 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'lpop' command",
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

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard);

            match map.get(key) {
                // No key: a null array where a count asked for an array.
                None if count.is_some() => Some(resp_null_array()),
                None => Some(resp_null_bulk()),
                Some(ValueRef::List(l)) => {
                    let mut inner = l.write();
                    match count {
                        // No count: original behaviour — return single bulk string or nil
                        None => match inner.pop_front() {
                            Some(val) => {
                                drop(inner);
                                notify(ctx.pubsub, *ctx.selected_db, KS_LIST, "lpop", key);
                                Some(resp_bulk(&val))
                            }
                            None => Some(resp_null_bulk()),
                        },
                        // With count: return array (empty array if list was empty)
                        Some(n) => {
                            let mut items: Vec<Vec<u8>> = Vec::with_capacity(n.min(inner.len()));
                            for _ in 0..n {
                                match inner.pop_front() {
                                    Some(val) => items.push(resp_bulk(&val)),
                                    None => break,
                                }
                            }
                            crate::stats::record_hit();
                            if !items.is_empty() {
                                drop(inner);
                                notify(ctx.pubsub, *ctx.selected_db, KS_LIST, "lpop", key);
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

        "RPOP" => {
            if cmd.len() < 2 || cmd.len() > 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'rpop' command",
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

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard);

            match map.get(key) {
                // No key: a null array where a count asked for an array.
                None if count.is_some() => Some(resp_null_array()),
                None => Some(resp_null_bulk()),
                Some(ValueRef::List(l)) => {
                    let mut inner = l.write();
                    match count {
                        None => match inner.pop_back() {
                            Some(val) => {
                                drop(inner);
                                notify(ctx.pubsub, *ctx.selected_db, KS_LIST, "rpop", key);
                                Some(resp_bulk(&val))
                            }
                            None => Some(resp_null_bulk()),
                        },
                        Some(n) => {
                            let mut items: Vec<Vec<u8>> = Vec::with_capacity(n.min(inner.len()));
                            for _ in 0..n {
                                match inner.pop_back() {
                                    Some(val) => items.push(resp_bulk(&val)),
                                    None => break,
                                }
                            }
                            crate::stats::record_hit();
                            if !items.is_empty() {
                                drop(inner);
                                notify(ctx.pubsub, *ctx.selected_db, KS_LIST, "rpop", key);
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

        "LLEN" => {
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'llen' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };

            let gone = lazy_expire_on_read(ctx.store, ctx.expiry_store, *ctx.selected_db, key);

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard);

            match map.get(key).filter(|_| !gone) {
                None => {
                    crate::stats::record_miss();
                    Some(resp_integer(0))
                }
                Some(ValueRef::List(l)) => {
                    crate::stats::record_hit();
                    Some(resp_integer(l.read().len() as i64))
                }
                Some(_) => {
                    crate::stats::record_miss();
                    Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ))
                }
            }
        }

        "LRANGE" => {
            if cmd.len() != 4 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'lrange' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be a valid utf-8")),
            };
            let start: i64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };
            let stop: i64 = match parse_utf8(cmd[3]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard);

            match map.get(key) {
                None => {
                    crate::stats::record_miss();
                    Some(resp_array(vec![]))
                }
                Some(ValueRef::List(l)) => {
                    let inner = l.read();
                    let len = inner.len() as i64;

                    // convert negative indices
                    let start = if start < 0 {
                        (len + start).max(0) as usize
                    } else {
                        start.min(len) as usize
                    };

                    let stop = if stop < 0 {
                        (len + stop).max(-1)
                    } else {
                        stop.min(len - 1)
                    };

                    if stop < 0 || start as i64 > stop {
                        return Some(resp_array(vec![]));
                    }

                    let stop = stop as usize;
                    let count = stop - start + 1;

                    // Write directly into one pre-allocated buffer instead of
                    // collecting N separate Vec<u8> allocations and then
                    // concatenating them in resp_array. For LRANGE 100 this
                    // cuts allocations from 101 down to 1.
                    use crate::resp::write::write_bulk;

                    // Array header: *count\r\n
                    let mut buf = Vec::with_capacity(16 + count * 24);
                    buf.push(b'*');
                    buf.extend_from_slice(count.to_string().as_bytes());
                    buf.extend_from_slice(b"\r\n");

                    for v in inner.iter().skip(start).take(count) {
                        write_bulk(&mut buf, v);
                    }

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

        "LTRIM" => {
            if cmd.len() != 4 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'ltrim' command",
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

            let stop: i64 = match parse_utf8(cmd[3]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard);

            match map.get(key) {
                None => Some(resp_simple("OK")),
                Some(ValueRef::List(l)) => {
                    let mut inner = l.write();
                    let len = inner.len() as i64;

                    let start = if start < 0 {
                        (len + start).max(0) as usize
                    } else {
                        start.min(len) as usize
                    };

                    let stop = if stop < 0 {
                        (len + stop).max(-1)
                    } else {
                        stop.min(len - 1)
                    };

                    // entire range is out of bounds - clear the list
                    if stop < 0 || start as i64 > stop {
                        inner.clear();
                        return Some(resp_simple("OK"));
                    }

                    let stop = stop as usize;

                    // drain from the back first, then the front
                    // order matters here
                    inner.drain(stop + 1..);
                    inner.drain(..start);
                    drop(inner);
                    notify(ctx.pubsub, *ctx.selected_db, KS_LIST, "ltrim", key);
                    Some(resp_simple("OK"))
                }
                Some(_) => Some(resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )),
            }
        }

        "LPOS" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'lpos' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let element = &cmd[2];

            let mut rank: i64 = 1;
            let mut count: Option<usize> = None;
            let mut maxlen: usize = 0;

            let mut i = 3;
            while i < cmd.len() {
                let tok = std::str::from_utf8(cmd[i])
                    .unwrap_or("")
                    .to_ascii_uppercase();
                match tok.as_str() {
                    "RANK" => {
                        i += 1;
                        rank = match cmd
                            .get(i)
                            .and_then(|b| std::str::from_utf8(b).ok())
                            .and_then(|s| s.parse::<i64>().ok())
                        {
                            // The magnitude is a count of matches.
                            Some(i64::MIN) => {
                                return Some(resp_error("ERR value is out of range"));
                            }
                            Some(n) if n != 0 => n,
                            _ => {
                                return Some(resp_error(
                                    "ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list",
                                ));
                            }
                        };
                    }
                    "COUNT" => {
                        i += 1;
                        count = match cmd
                            .get(i)
                            .and_then(|b| std::str::from_utf8(b).ok())
                            .and_then(|s| s.parse::<usize>().ok())
                        {
                            Some(n) => Some(n),
                            None => {
                                return Some(resp_error(
                                    "ERR value is not an integer or out of range",
                                ));
                            }
                        };
                    }
                    "MAXLEN" => {
                        i += 1;
                        maxlen = match cmd
                            .get(i)
                            .and_then(|b| std::str::from_utf8(b).ok())
                            .and_then(|s| s.parse::<usize>().ok())
                        {
                            Some(n) => n,
                            None => {
                                return Some(resp_error(
                                    "ERR value is not an integer or out of range",
                                ));
                            }
                        };
                    }
                    _ => {}
                }
                i += 1;
            }

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard);

            match map.get(key) {
                None => {
                    crate::stats::record_miss();
                    Some(match count {
                        Some(_) => resp_array(vec![]),
                        None => resp_null_bulk(),
                    })
                }
                Some(ValueRef::List(l)) => {
                    let inner = l.read();
                    let list_len = inner.len();

                    // scan_len: how many elements to examine. 0 = whole list.
                    let scan_len = if maxlen == 0 {
                        list_len
                    } else {
                        maxlen.min(list_len)
                    };

                    let want_all = count == Some(0);
                    let max_matches = if want_all {
                        usize::MAX
                    } else {
                        count.unwrap_or(1)
                    };

                    // skips_left: how many matches to skip before collecting.
                    // rank is 1-based so rank=1 means skip 0, rank=2 means skip 1.
                    // For negative rank the magnitude is also 1-based.
                    let skips_left_init = (rank.unsigned_abs() as usize).saturating_sub(1);
                    let mut skips_left = skips_left_init;

                    let mut matches: Vec<Vec<u8>> = Vec::new();

                    if rank >= 0 {
                        // Forward scan: indices 0..scan_len
                        for idx in 0..scan_len {
                            if inner[idx].as_ref() == *element {
                                if skips_left > 0 {
                                    skips_left -= 1;
                                } else {
                                    matches.push(resp_integer(idx as i64));
                                    if matches.len() >= max_matches {
                                        break;
                                    }
                                }
                            }
                        }
                    } else {
                        // Reverse scan: walk from tail, examine scan_len elements.
                        // start is the first index to examine counting from the end.
                        // Using saturating arithmetic — list_len and scan_len are both
                        // already bounded so this won't underflow, but .saturating_sub
                        // makes it explicit and panic-free.
                        let start = list_len.saturating_sub(scan_len);
                        for idx in (start..list_len).rev() {
                            if inner[idx].as_ref() == *element {
                                if skips_left > 0 {
                                    skips_left -= 1;
                                } else {
                                    // Index returned is always the real forward index
                                    matches.push(resp_integer(idx as i64));
                                    if matches.len() >= max_matches {
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    crate::stats::record_hit();
                    Some(match count {
                        None => matches.into_iter().next().unwrap_or_else(resp_null_bulk),
                        Some(_) => resp_array(matches),
                    })
                }
                Some(_) => {
                    crate::stats::record_miss();
                    Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ))
                }
            }
        }

        "LINDEX" => {
            if cmd.len() != 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'lindex' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };

            let idx: i64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard);

            match map.get(key) {
                None => {
                    crate::stats::record_miss();
                    Some(resp_null_bulk())
                }
                Some(ValueRef::List(l)) => {
                    let inner = l.read();
                    let len = inner.len() as i64;
                    let real_idx = if idx < 0 { len + idx } else { idx };

                    if real_idx < 0 || real_idx >= len {
                        return Some(resp_null_bulk());
                    }

                    match inner.get(real_idx as usize) {
                        Some(v) => {
                            crate::stats::record_hit();
                            Some(resp_bulk(v))
                        }
                        None => {
                            crate::stats::record_miss();
                            Some(resp_null_bulk())
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

        "LPUSHX" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'lpushx' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = write_lock!(shard);

            match map.get(key) {
                None => {
                    crate::stats::record_miss();
                    Some(resp_integer(0))
                }
                Some(ValueRef::List(l)) => {
                    let list = Arc::clone(l);
                    drop(map); // release shard lock before locking inner list
                    let mut inner = list.write();
                    for val in cmd.iter().skip(2) {
                        inner.push_front(Arc::from(*val));
                    }
                    crate::stats::record_hit();
                    let len = inner.len();
                    drop(inner);
                    ctx.wait_table.notify_signal(key);
                    Some(resp_integer(len as i64))
                }
                Some(_) => {
                    crate::stats::record_miss();
                    Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ))
                }
            }
        }

        "RPUSHX" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'rpushx' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = write_lock!(shard);

            match map.get(key) {
                None => Some(resp_integer(0)),
                Some(ValueRef::List(l)) => {
                    let list = Arc::clone(l);
                    drop(map);
                    let mut inner = list.write();
                    for val in cmd.iter().skip(2) {
                        inner.push_back(Arc::from(*val));
                    }
                    let len = inner.len();
                    drop(inner);
                    ctx.wait_table.notify_signal(key);
                    Some(resp_integer(len as i64))
                }
                Some(_) => Some(resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )),
            }
        }

        "LSET" => {
            if cmd.len() != 4 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'lset' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let idx: i64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };
            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard); // read is enough — we don't change the map, only the inner list

            match map.get(key) {
                None => Some(resp_error("ERR no such key")),
                Some(ValueRef::List(l)) => {
                    let mut inner = l.write();
                    let len = inner.len() as i64;
                    let real_idx = if idx < 0 { len + idx } else { idx };
                    if real_idx < 0 || real_idx >= len {
                        return Some(resp_error("ERR index out of range"));
                    }
                    inner[real_idx as usize] = Arc::from(cmd[3]);
                    drop(inner);
                    notify(ctx.pubsub, *ctx.selected_db, KS_LIST, "lset", key);
                    Some(resp_simple("OK"))
                }
                Some(_) => Some(resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )),
            }
        }

        "LINSERT" => {
            if cmd.len() != 5 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'linsert' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let direction = String::from_utf8_lossy(cmd[2]).to_ascii_uppercase();
            if direction != "BEFORE" && direction != "AFTER" {
                return Some(resp_error("ERR syntax error"));
            }
            let pivot = &cmd[3];
            let element = &cmd[4];

            lazy_expire_key(ctx.store, ctx.expiry_store, *ctx.selected_db, key);

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard);

            match map.get(key) {
                None => Some(resp_integer(0)),
                Some(ValueRef::List(l)) => {
                    let mut inner = l.write();
                    let pos = inner.iter().position(|v| v.as_ref() == *pivot);
                    match pos {
                        None => Some(resp_integer(-1)),
                        Some(i) => {
                            let insert_at = if direction == "BEFORE" { i } else { i + 1 };
                            inner.insert(insert_at, Arc::from(*element));
                            let len = inner.len();
                            drop(inner);
                            ctx.wait_table.notify_signal(key);
                            notify(ctx.pubsub, *ctx.selected_db, KS_LIST, "linsert", key);
                            Some(resp_integer(len as i64))
                        }
                    }
                }
                Some(_) => Some(resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )),
            }
        }

        "LREM" => {
            if cmd.len() != 4 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'lrem' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let count: i64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };
            let element = &cmd[3];

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard);

            match map.get(key) {
                None => Some(resp_integer(0)),
                Some(ValueRef::List(l)) => {
                    let mut inner = l.write();
                    let mut removed = 0i64;
                    let limit = count.unsigned_abs() as usize;

                    if count == 0 {
                        // remove all occurrences
                        inner.retain(|v| {
                            if v.as_ref() == *element {
                                removed += 1;
                                false
                            } else {
                                true
                            }
                        });
                    } else if count > 0 {
                        // remove from head
                        let mut i = 0;
                        while i < inner.len() && (limit == 0 || removed < limit as i64) {
                            if inner[i].as_ref() == *element {
                                inner.remove(i);
                                removed += 1;
                            } else {
                                i += 1;
                            }
                        }
                    } else {
                        // count < 0: remove from tail
                        let mut i = inner.len();
                        while i > 0 && (limit == 0 || removed < limit as i64) {
                            i -= 1;
                            if inner[i].as_ref() == *element {
                                inner.remove(i);
                                removed += 1;
                            }
                        }
                    }
                    if removed > 0 {
                        drop(inner);
                        notify(ctx.pubsub, *ctx.selected_db, KS_LIST, "lrem", key);
                    }
                    Some(resp_integer(removed))
                }
                Some(_) => Some(resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )),
            }
        }

        "LMOVE" => {
            if cmd.len() != 5 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'lmove' command",
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
            let (Some(from_left), Some(to_left)) =
                (list_move::parse_end(cmd[3]), list_move::parse_end(cmd[4]))
            else {
                return Some(resp_error("ERR syntax error"));
            };
            Some(
                match list_move::move_one(ctx, &src_key, &dst_key, from_left, to_left) {
                    Ok(Some(element)) => resp_bulk(&element),
                    Ok(None) => resp_null_bulk(),
                    Err(e) => e,
                },
            )
        }

        "RPOPLPUSH" => {
            // Legacy alias: RPOPLPUSH src dst = LMOVE src dst RIGHT LEFT
            if cmd.len() != 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'rpoplpush' command",
                ));
            }
            // Reuse LMOVE logic by building a synthetic command
            let src_key = match parse_utf8(cmd[1]) {
                Some(k) => k.to_string(),
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let dst_key = match parse_utf8(cmd[2]) {
                Some(k) => k.to_string(),
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            Some(
                match list_move::move_one(ctx, &src_key, &dst_key, false, true) {
                    Ok(Some(element)) => resp_bulk(&element),
                    Ok(None) => resp_null_bulk(),
                    Err(e) => e,
                },
            )
        }

        "BLPOP" | "BRPOP" => {
            // BLPOP key [key ...] timeout
            // BRPOP key [key ...] timeout
            if cmd.len() < 3 {
                return Some(resp_error(&format!(
                    "ERR wrong number of arguments for '{}' command",
                    name.to_ascii_lowercase()
                )));
            }
            // The last argument is the timeout in seconds (a float).
            let timeout_secs = match blocking::parse_timeout(cmd[cmd.len() - 1]) {
                Ok(secs) => secs,
                Err(e) => return Some(e),
            };
            let keys: Vec<String> = cmd[1..cmd.len() - 1]
                .iter()
                .filter_map(|b| parse_utf8(b).map(|s| s.to_string()))
                .collect();
            let left = name == "BLPOP";

            // The first key, in the order given, that has an element.
            Some(blocking::wait_for_keys(
                ctx,
                &keys,
                blocking::timeout_from_secs(timeout_secs),
                |ctx, keys| {
                    for key in keys {
                        match pop_from(ctx, key, left, 1) {
                            Ok(mut popped) => {
                                if let Some(element) = popped.pop() {
                                    crate::stats::record_hit();
                                    return Some(resp_array(vec![
                                        resp_bulk_str(key),
                                        resp_bulk(element.as_ref()),
                                    ]));
                                }
                            }
                            // A key of another type is an error, not a
                            // reason to wait.
                            Err(e) => return Some(e),
                        }
                    }
                    None
                },
                resp_null_array,
            ))
        }

        "LMPOP" => {
            // LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
            let (numkeys, right, count) =
                match crate::commands::helpers::parse_mpop(cmd, 1, ["LEFT", "RIGHT"]) {
                    Ok(parsed) => parsed,
                    Err(e) => return Some(e),
                };
            // Up to `count` elements from the first key that has any; a key
            // that holds something other than a list is an error.
            for raw in &cmd[2..2 + numkeys] {
                let Some(key) = parse_utf8(raw) else {
                    return Some(resp_error("ERR must be valid UTF-8"));
                };
                match pop_from(ctx, key, !right, count) {
                    Ok(popped) if popped.is_empty() => {}
                    Ok(popped) => {
                        crate::stats::record_hit();
                        return Some(resp_array(vec![
                            resp_bulk_str(key),
                            resp_array(popped.iter().map(|el| resp_bulk(el.as_ref())).collect()),
                        ]));
                    }
                    Err(e) => return Some(e),
                }
            }
            crate::stats::record_miss();
            Some(resp_null_array())
        }

        "BRPOPLPUSH" | "BLMOVE" => {
            // BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
            // BRPOPLPUSH source destination timeout
            //   — deprecated since Redis 6.2: BLMOVE … RIGHT LEFT.
            let legacy = name == "BRPOPLPUSH";
            if cmd.len() != if legacy { 4 } else { 6 } {
                return Some(resp_error(&format!(
                    "ERR wrong number of arguments for '{}' command",
                    name.to_ascii_lowercase()
                )));
            }
            let (Some(src), Some(dst)) = (parse_utf8(cmd[1]), parse_utf8(cmd[2])) else {
                return Some(resp_error("ERR must be valid UTF-8"));
            };
            let (src, dst) = (src.to_string(), dst.to_string());
            let (from_left, to_left) = if legacy {
                (false, true)
            } else {
                match (list_move::parse_end(cmd[3]), list_move::parse_end(cmd[4])) {
                    (Some(from_left), Some(to_left)) => (from_left, to_left),
                    _ => return Some(resp_error("ERR syntax error")),
                }
            };
            let timeout_secs = match blocking::parse_timeout(cmd[cmd.len() - 1]) {
                Ok(secs) => secs,
                Err(e) => return Some(e),
            };

            let keys = [src.clone()];
            Some(blocking::wait_for_keys(
                ctx,
                &keys,
                blocking::timeout_from_secs(timeout_secs),
                |ctx, _| match list_move::move_one(ctx, &src, &dst, from_left, to_left) {
                    Ok(Some(element)) => {
                        crate::stats::record_hit();
                        Some(resp_bulk(&element))
                    }
                    Ok(None) => None,
                    // A destination of the wrong type is an error whenever
                    // it is found; a source of the wrong type, found on
                    // being woken, is a reason to go on waiting.
                    Err(e) if list_move::is_not_a_list(ctx, &dst) => Some(blocking::definite(e)),
                    Err(e) => Some(e),
                },
                resp_null_bulk,
            ))
        }

        "BLMPOP" => {
            // BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
            if cmd.len() < 5 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'blmpop' command",
                ));
            }
            let (numkeys, right, count) =
                match crate::commands::helpers::parse_mpop(cmd, 2, ["LEFT", "RIGHT"]) {
                    Ok(parsed) => parsed,
                    Err(e) => return Some(e),
                };
            let left = !right;
            let timeout_secs = match blocking::parse_timeout(cmd[1]) {
                Ok(secs) => secs,
                Err(e) => return Some(e),
            };
            let keys: Vec<String> = cmd[3..3 + numkeys]
                .iter()
                .filter_map(|b| parse_utf8(b).map(|s| s.to_string()))
                .collect();

            // Up to `count` elements from the first key that has any.
            Some(blocking::wait_for_keys(
                ctx,
                &keys,
                blocking::timeout_from_secs(timeout_secs),
                |ctx, keys| {
                    for key in keys {
                        match pop_from(ctx, key, left, count) {
                            Ok(popped) if popped.is_empty() => {}
                            Ok(popped) => {
                                crate::stats::record_hit();
                                return Some(resp_array(vec![
                                    resp_bulk_str(key),
                                    resp_array(
                                        popped.iter().map(|el| resp_bulk(el.as_ref())).collect(),
                                    ),
                                ]));
                            }
                            Err(e) => return Some(e),
                        }
                    }
                    None
                },
                resp_null_array,
            ))
        }

        // LMOVEM / BLMOVEM; `None` tells the router this module doesn't handle it.
        _ => list_move::handle(ctx, name),
    }
}
