use crate::commands::ctx::Ctx;
use crate::commands::helpers::*;
use crate::resp::write::*;
use crate::store;
use crate::store::ValueRef;
use crate::types::SharedBytes;
use crate::{read_lock, write_lock};

// ── Bitmap helpers ────────────────────────────────────────────────────────────
//
// Redis bitmaps are just strings. Bit `offset` lives in byte `offset / 8`,
// at bit position `7 - (offset % 8)` within that byte (big-endian bit order).
//
// SETBIT changes its byte where the string is kept when nothing else holds
// it, and copies the string otherwise; BITFIELD copies it for any write.

/// Returns (byte_index, bit_mask) for a given bit offset.
/// Redis uses big-endian bit ordering within each byte.
#[inline]
fn bit_byte_and_mask(offset: u64) -> (usize, u8) {
    let byte_idx = (offset / 8) as usize;
    let bit_pos = 7 - (offset % 8); // MSB first
    (byte_idx, 1u8 << bit_pos)
}

/// Count set bits in a byte slice using Rust's built-in popcount.
/// This compiles to a single `popcnt` CPU instruction on x86-64.
#[inline]
fn popcount(data: &[u8]) -> u64 {
    data.iter().map(|b| b.count_ones() as u64).sum()
}

#[inline]
fn parse_i64_arg(arg: &[u8]) -> Result<i64, ()> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(())
}

// ── GETBIT ────────────────────────────────────────────────────────────────────
fn cmd_getbit(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() != 3 {
        return resp_error("ERR wrong number of arguments for 'getbit' command");
    }
    let key: &[u8] = cmd[1];
    let offset: u64 = match std::str::from_utf8(cmd[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_error("ERR bit offset is not an integer or out of range"),
    };

    if is_expired(ctx.store, *ctx.selected_db, key) {
        expire_on_read(ctx.store, *ctx.selected_db, key);
        crate::stats::record_miss();
        return resp_integer(0);
    }

    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, key);
    let map = read_lock!(shard);

    let found = map.get(key);
    crate::stats::record_lookup(found.is_some());
    match found {
        Some(ValueRef::String(bytes)) => {
            let (byte_idx, mask) = bit_byte_and_mask(offset);
            if byte_idx >= bytes.len() {
                resp_integer(0)
            } else {
                resp_integer(if bytes[byte_idx] & mask != 0 { 1 } else { 0 })
            }
        }
        None => resp_integer(0),
        _ => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

// ── SETBIT ────────────────────────────────────────────────────────────────────
// The changed byte is written where the string is when nothing else holds it
// (`ShardMap::edit_str_in_place`); otherwise the string is copied once.
fn cmd_setbit(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() != 4 {
        return resp_error("ERR wrong number of arguments for 'setbit' command");
    }
    let key: &[u8] = cmd[1];
    let offset: u64 = match std::str::from_utf8(cmd[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) if n < 2u64.pow(32) => n, // Redis limit: 2^32 - 1
        _ => return resp_error("ERR bit offset is not an integer or out of range"),
    };
    let bit_val: u8 = match std::str::from_utf8(cmd[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v @ 0) | Some(v @ 1) => v,
        _ => return resp_error("ERR bit is not an integer or out of range"),
    };

    let (byte_idx, mask) = bit_byte_and_mask(offset);

    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, key);
    let mut map = write_lock!(shard);

    // Read the bit where the stored bytes are, without copying them.
    let existing: &[u8] = match map.get(key) {
        Some(ValueRef::String(b)) => b.as_bytes(),
        None => &[],
        _ => {
            return resp_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    };

    // Remember the old bit value before we mutate
    let old_bit = if byte_idx < existing.len() && existing[byte_idx] & mask != 0 {
        1i64
    } else {
        0i64
    };

    // No-op: bit already matches — skip touching the DB / replication dirty counters (Redis).
    if byte_idx < existing.len() {
        let cur_set = existing[byte_idx] & mask != 0;
        let want_set = bit_val == 1;
        if cur_set == want_set {
            ctx.skip_write_side_effects = true;
            return resp_integer(old_bit);
        }
    }

    // A byte past the end is zero, as are those the string grows by.
    let byte = existing.get(byte_idx).copied().unwrap_or(0);
    let byte = if bit_val == 1 {
        byte | mask
    } else {
        byte & !mask
    };
    let Some(written) = map.edit_str_in_place(key, byte_idx, &[byte]) else {
        // Not reached: the key was a string or absent under this lock.
        return resp_error("WRONGTYPE Operation against a key holding the wrong kind of value");
    };
    drop(map);
    // A copied string's old entry is freed with the shard unlocked.
    drop(written);
    resp_integer(old_bit)
}

// ── BITCOUNT ──────────────────────────────────────────────────────────────────
// Syntax: BITCOUNT key [start end [BYTE | BIT]]
fn cmd_bitcount(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return resp_error("ERR wrong number of arguments for 'bitcount' command");
    }
    if !(cmd.len() == 2 || cmd.len() == 4 || cmd.len() == 5) {
        return resp_error("ERR syntax error");
    }

    let key: &[u8] = cmd[1];

    // Hot path: whole string — no range parsing
    if cmd.len() == 2 {
        if is_expired(ctx.store, *ctx.selected_db, key) {
            expire_on_read(ctx.store, *ctx.selected_db, key);
            crate::stats::record_miss();
            return resp_integer(0);
        }
        let db = &ctx.store[*ctx.selected_db];
        let shard = store::get_shard(db, key);
        let map = read_lock!(shard);
        let found = map.get(key);
        crate::stats::record_lookup(found.is_some());
        match found {
            Some(ValueRef::String(b)) => resp_integer(popcount(&b) as i64),
            None => resp_integer(0),
            _ => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    } else {
        let start: i64 = match std::str::from_utf8(cmd[2])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(n) => n,
            None => return resp_error("ERR value is not an integer or out of range"),
        };
        let end: i64 = match std::str::from_utf8(cmd[3])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(n) => n,
            None => return resp_error("ERR value is not an integer or out of range"),
        };

        let bit_mode = if cmd.len() == 5 {
            match std::str::from_utf8(cmd[4])
                .unwrap_or("")
                .to_ascii_uppercase()
                .as_str()
            {
                "BIT" => true,
                "BYTE" => false,
                _ => return resp_error("ERR syntax error"),
            }
        } else {
            false
        };

        let gone = is_expired(ctx.store, *ctx.selected_db, key)
            && expire_on_read(ctx.store, *ctx.selected_db, key);

        let db = &ctx.store[*ctx.selected_db];
        let shard = store::get_shard(db, key);
        let map = read_lock!(shard);

        let found = map.get(key).filter(|_| !gone);
        crate::stats::record_lookup(found.is_some());
        let bytes: SharedBytes = match found {
            Some(ValueRef::String(b)) => b.to_shared(),
            None => SharedBytes::from(&[][..]),
            _ => {
                return resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                );
            }
        };
        drop(map);

        bitcount_range(&bytes, start, end, bit_mode)
    }
}

#[inline]
fn bitcount_range(bytes: &[u8], start: i64, end: i64, bit_mode: bool) -> Vec<u8> {
    // Redis: two negative bounds with start > end → empty range (before index normalization)
    if start < 0 && end < 0 && start > end {
        return resp_integer(0);
    }

    let strlen = bytes.len() as i64;
    let mut totlen = strlen;
    if bit_mode {
        totlen = strlen.saturating_mul(8);
    }

    let mut s = start;
    let mut e = end;
    if s < 0 {
        s += totlen;
    }
    if e < 0 {
        e += totlen;
    }
    if s < 0 {
        s = 0;
    }
    if e < 0 {
        e = 0;
    }
    if e >= totlen {
        e = totlen - 1;
    }

    // Missing key: Redis returns 0 after normalizing indexes (before start>end)
    if bytes.is_empty() {
        return resp_integer(0);
    }

    if s > e {
        return resp_integer(0);
    }

    if bit_mode {
        let mut count = 0u64;
        for bit in (s as usize)..=(e as usize) {
            let (bi, mask) = bit_byte_and_mask(bit as u64);
            if bi < bytes.len() && bytes[bi] & mask != 0 {
                count += 1;
            }
        }
        resp_integer(count as i64)
    } else {
        let si = s as usize;
        let ei = e as usize;
        resp_integer(popcount(&bytes[si..=ei]) as i64)
    }
}

// ── BITPOS ────────────────────────────────────────────────────────────────────
// Finds first set (1) or clear (0) bit.
fn cmd_bitpos(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'bitpos' command");
    }
    if !(cmd.len() == 3 || cmd.len() == 4 || cmd.len() == 5 || cmd.len() == 6) {
        return resp_error("ERR syntax error");
    }

    let key: &[u8] = cmd[1];

    let bit_ll: i64 = match parse_i64_arg(cmd[2]) {
        Ok(v) => v,
        Err(()) => return resp_error("ERR value is not an integer or out of range"),
    };
    if bit_ll != 0 && bit_ll != 1 {
        return resp_error("ERR The bit argument must be 1 or 0.");
    }
    let search_bit = bit_ll as u8;

    let (start_byte, end_byte, end_given, bit_mode) = match cmd.len() {
        3 => (0i64, -1i64, false, false),
        4 => {
            let s = match parse_i64_arg(cmd[3]) {
                Ok(v) => v,
                Err(()) => return resp_error("ERR value is not an integer or out of range"),
            };
            (s, -1, false, false)
        }
        5 => {
            let s = match parse_i64_arg(cmd[3]) {
                Ok(v) => v,
                Err(()) => return resp_error("ERR value is not an integer or out of range"),
            };
            let e = match parse_i64_arg(cmd[4]) {
                Ok(v) => v,
                Err(()) => return resp_error("ERR value is not an integer or out of range"),
            };
            (s, e, true, false)
        }
        6 => {
            let s = match parse_i64_arg(cmd[3]) {
                Ok(v) => v,
                Err(()) => return resp_error("ERR value is not an integer or out of range"),
            };
            let mode = std::str::from_utf8(cmd[5])
                .unwrap_or("")
                .to_ascii_uppercase();
            let bm = if mode == "BIT" {
                true
            } else if mode == "BYTE" {
                false
            } else {
                return resp_error("ERR syntax error");
            };
            let e = match parse_i64_arg(cmd[4]) {
                Ok(v) => v,
                Err(()) => return resp_error("ERR value is not an integer or out of range"),
            };
            (s, e, true, bm)
        }
        _ => unreachable!(),
    };

    if is_expired(ctx.store, *ctx.selected_db, key) {
        expire_on_read(ctx.store, *ctx.selected_db, key);
        crate::stats::record_miss();
        return resp_integer(if search_bit == 0 { 0 } else { -1 });
    }

    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, key);
    let map = read_lock!(shard);

    let found = map.get(key);
    crate::stats::record_lookup(found.is_some());
    let bytes = match found {
        Some(ValueRef::String(b)) => b.to_shared(),
        None => {
            return resp_integer(if search_bit == 0 { 0 } else { -1 });
        }
        _ => {
            return resp_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    };
    drop(map);

    let len = bytes.len() as i64;

    if bit_mode {
        // BIT mode: indices are bit positions
        let total_bits = len * 8;
        let s = if start_byte < 0 {
            (total_bits + start_byte).max(0)
        } else {
            start_byte
        } as usize;
        let e = if end_byte < 0 {
            (total_bits + end_byte).max(0)
        } else {
            end_byte
        } as usize;
        for bit in s..=e.min((total_bits - 1).max(0) as usize) {
            let (bi, mask) = bit_byte_and_mask(bit as u64);
            if bi < bytes.len() {
                let found = (bytes[bi] & mask != 0) == (search_bit == 1);
                if found {
                    return resp_integer(bit as i64);
                }
            } else if search_bit == 0 {
                return resp_integer(bit as i64);
            }
        }
        return resp_integer(-1);
    }

    // BYTE mode
    let s = if start_byte < 0 {
        (len + start_byte).max(0)
    } else {
        start_byte.min(len)
    } as usize;
    let e = if end_byte < 0 {
        (len + end_byte).max(0)
    } else if end_given {
        end_byte.min(len - 1)
    } else {
        len - 1
    } as usize;

    for byte_i in s..=e {
        let b = if byte_i < bytes.len() {
            bytes[byte_i]
        } else {
            0u8
        };
        for bit_i in 0..8u64 {
            let mask = 1u8 << (7 - bit_i);
            let is_set = b & mask != 0;
            if is_set == (search_bit == 1) {
                return resp_integer((byte_i as i64) * 8 + bit_i as i64);
            }
        }
    }

    // Special case: searching for 0 with no explicit end — look past the string
    if search_bit == 0 && !end_given {
        return resp_integer(len * 8);
    }

    resp_integer(-1)
}

// ── BITOP ─────────────────────────────────────────────────────────────────────
// BITOP AND|OR|XOR|NOT|ONE|DIFF|DIFF1|ANDOR destkey key [key ...]
// Rust lesson: zip/fold patterns on byte slices
fn cmd_bitop(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'bitop' command");
    }
    let op = std::str::from_utf8(cmd[1])
        .unwrap_or("")
        .to_ascii_uppercase();
    let dest_key: Vec<u8> = cmd[2].to_vec();

    if op == "NOT" && cmd.len() != 4 {
        return resp_error("ERR BITOP NOT must be called with a single source key");
    }

    if matches!(op.as_str(), "DIFF" | "DIFF1" | "ANDOR") && cmd.len() < 5 {
        return resp_error(&format!(
            "ERR BITOP {} must be called with at least two source keys.",
            op
        ));
    }

    let db = &ctx.store[*ctx.selected_db];

    // Collect all source byte arrays (under their respective read locks)
    let mut sources: Vec<Vec<u8>> = Vec::new();
    for src_raw in &cmd[3..] {
        let key: &[u8] = src_raw;
        if is_expired(ctx.store, *ctx.selected_db, key) {
            delete_key_if_expired(ctx.store, *ctx.selected_db, key);
            crate::stats::record_miss();
            sources.push(vec![]);
            continue;
        }
        let shard = store::get_shard(db, key);
        let map = read_lock!(shard);
        let found = map.get(key);
        crate::stats::record_lookup(found.is_some());
        match found {
            Some(ValueRef::String(b)) => sources.push(b.to_vec()),
            None => sources.push(vec![]),
            _ => {
                return resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                );
            }
        }
    }

    // Find max length — output is padded to that length
    let max_len = sources.iter().map(|s| s.len()).max().unwrap_or(0);

    let result: Vec<u8> = match op.as_str() {
        "NOT" => {
            // NOT only works on single source
            sources[0].iter().map(|b| !b).collect()
        }
        "AND" => (0..max_len)
            .map(|i| {
                sources
                    .iter()
                    .map(|s| s.get(i).copied().unwrap_or(0))
                    .fold(0xFF, |acc, b| acc & b)
            })
            .collect(),
        "OR" => (0..max_len)
            .map(|i| {
                sources
                    .iter()
                    .map(|s| s.get(i).copied().unwrap_or(0))
                    .fold(0x00, |acc, b| acc | b)
            })
            .collect(),
        "XOR" => (0..max_len)
            .map(|i| {
                sources
                    .iter()
                    .map(|s| s.get(i).copied().unwrap_or(0))
                    .fold(0x00, |acc, b| acc ^ b)
            })
            .collect(),
        "ONE" => (0..max_len)
            .map(|i| {
                let mut output = sources.first().and_then(|s| s.get(i)).copied().unwrap_or(0);
                let mut common_bits = 0u8;
                for src in sources.iter().skip(1) {
                    let byte = src.get(i).copied().unwrap_or(0);
                    common_bits |= output & byte;
                    output ^= byte;
                    output &= !common_bits;
                }
                output
            })
            .collect(),
        "DIFF" | "DIFF1" | "ANDOR" => {
            let combine: fn(u8, u8) -> u8 = match op.as_str() {
                "DIFF" => |o, d| o & !d,
                "DIFF1" => |o, d| !o & d,
                "ANDOR" => |o, d| o & d,
                _ => unreachable!(),
            };
            (0..max_len)
                .map(|i| {
                    let o = sources.first().and_then(|s| s.get(i)).copied().unwrap_or(0);
                    let mut disj = 0u8;
                    for src in sources.iter().skip(1) {
                        disj |= src.get(i).copied().unwrap_or(0);
                    }
                    combine(o, disj)
                })
                .collect()
        }
        _ => {
            return resp_error(&format!(
                "ERR Don't know what to do for \"{}\"",
                op.to_lowercase()
            ));
        }
    };

    let result_len = result.len() as i64;
    // Store result at dest key
    let dest_shard = store::get_shard(db, &dest_key);
    let mut map = write_lock!(dest_shard);
    map.insert_str(dest_key, result.as_slice());
    drop(map);

    resp_integer(result_len)
}

// ── BITFIELD (basic subset) ───────────────────────────────────────────────────
// Full BITFIELD is complex. We implement GET, SET, INCRBY with u/i encodings.
// This teaches you: parsing subcommands in a loop, signed/unsigned bit fields.
fn cmd_bitfield(ctx: &mut Ctx, readonly: bool) -> Vec<u8> {
    // BITFIELD returns one result slot per subcommand; a failed GET or an
    // OVERFLOW FAIL yields a nil *inside* that array, which the dispatcher's
    // top-level null normalisation cannot reach.
    let resp = crate::commands::handler::effective_resp_for_reply(ctx);
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return resp_error("ERR wrong number of arguments for 'bitfield' command");
    }
    let key: &[u8] = cmd[1];

    // We'll accumulate results for each subcommand
    let mut results: Vec<Vec<u8>> = Vec::new();
    let mut i = 2usize;

    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, key);

    // A second hold on the stored bytes, read with the shard let go.
    let map = read_lock!(shard);
    let found = map.get(key);
    // Only GETs (or `BITFIELD_RO`): a read, counted as Redis counts one.
    let writes = cmd[2..]
        .iter()
        .any(|a| a.eq_ignore_ascii_case(b"SET") || a.eq_ignore_ascii_case(b"INCRBY"));
    if readonly || !writes {
        crate::stats::record_lookup(found.is_some());
    }
    let held: Option<SharedBytes> = match found {
        Some(ValueRef::String(b)) => Some(b.to_shared()),
        None => None,
        _ => {
            return resp_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    };
    drop(map);
    let stored: &[u8] = held.as_deref().unwrap_or(&[]);
    // The writable copy, made by the first successful write.
    let mut written: Option<Vec<u8>> = None;
    // Byte range the writes touched: only it can differ from `stored`.
    let (mut touched_from, mut touched_to) = (usize::MAX, 0usize);

    let mut overflow_mode = "WRAP"; // WRAP | SAT | FAIL

    while i < cmd.len() {
        let sub = std::str::from_utf8(cmd[i])
            .unwrap_or("")
            .to_ascii_uppercase();
        i += 1;

        match sub.as_str() {
            "OVERFLOW" => {
                if i >= cmd.len() {
                    return resp_error("ERR syntax error");
                }
                overflow_mode = match std::str::from_utf8(cmd[i])
                    .unwrap_or("")
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "WRAP" => "WRAP",
                    "SAT" => "SAT",
                    "FAIL" => "FAIL",
                    _ => return resp_error("ERR Invalid OVERFLOW type specified"),
                };
                i += 1;
            }
            "GET" => {
                if i + 1 >= cmd.len() {
                    return resp_error("ERR syntax error");
                }
                let encoding = std::str::from_utf8(cmd[i]).unwrap_or("").to_string();
                i += 1;
                let (_, enc_bits) = match parse_encoding(&encoding) {
                    Some(x) => x,
                    None => {
                        return resp_error(
                            "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.",
                        );
                    }
                };
                let offset = match parse_bitfield_offset(cmd[i], enc_bits) {
                    Ok(o) => o,
                    Err(()) => {
                        return resp_error("ERR bit offset is not an integer or out of range");
                    }
                };
                i += 1;
                let val = bitfield_get(written.as_deref().unwrap_or(stored), &encoding, offset);
                results.push(match val {
                    Some(v) => resp_integer(v),
                    None => resp_null_for(resp),
                });
            }
            op @ "SET" | op @ "INCRBY" => {
                if readonly {
                    return resp_error("ERR BITFIELD_RO only supports the GET subcommand");
                }
                if i + 2 >= cmd.len() {
                    return resp_error("ERR syntax error");
                }
                let encoding = std::str::from_utf8(cmd[i]).unwrap_or("").to_string();
                i += 1;
                let (_, enc_bits) = match parse_encoding(&encoding) {
                    Some(x) => x,
                    None => {
                        return resp_error(
                            "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.",
                        );
                    }
                };
                let offset = match parse_bitfield_offset(cmd[i], enc_bits) {
                    Ok(o) => o,
                    Err(()) => {
                        return resp_error("ERR bit offset is not an integer or out of range");
                    }
                };
                i += 1;
                let value: i128 = std::str::from_utf8(cmd[i])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                i += 1;

                let old_val = bitfield_get(written.as_deref().unwrap_or(stored), &encoding, offset);
                let new_val: i128 = if op == "SET" {
                    value
                } else {
                    old_val.unwrap_or(0) as i128 + value // no pre-wrap; clamp decides
                };
                let clamped = bitfield_clamp(&encoding, new_val, overflow_mode);
                match clamped {
                    Some(v) => {
                        let bytes = written.get_or_insert_with(|| stored.to_vec());
                        bitfield_set(bytes, &encoding, offset, v as i64);
                        touched_from = touched_from.min(offset / 8);
                        touched_to = touched_to.max((offset + enc_bits as usize).div_ceil(8));
                        if op == "SET" {
                            results.push(resp_integer(old_val.unwrap_or(0)));
                        } else {
                            results.push(resp_integer(v as i64));
                        }
                    }
                    None => {
                        // Overflow FAIL yields a nil in this slot of the
                        // results array — nested, so it needs the protocol's
                        // own null encoding.
                        results.push(resp_null_for(resp));
                    }
                }
            }
            _ => {
                return resp_error("ERR syntax error");
            }
        }
    }

    // Stored only if the bytes changed on the whole.
    let changed = written.filter(|bytes| {
        bytes.len() != stored.len()
            || bytes.get(touched_from..touched_to) != stored.get(touched_from..touched_to)
    });
    if let Some(bytes) = changed {
        let mut map = write_lock!(shard);
        map.insert_str(key, bytes.as_slice());
    } else {
        ctx.skip_write_side_effects = true;
    }

    resp_array(results)
}

// ── Bitfield encoding helpers ─────────────────────────────────────────────────
// encoding: "u8", "i16", "u32", etc. — u = unsigned, i = signed

/// `#N` means bit offset `N * encoding_bits` (Redis-compatible successive field layout).
/// A bit offset, as SETBIT's: below 2^32, so that a string is never made
/// bigger than 512 MB (Redis's `proto-max-bulk-len`).
fn parse_bitfield_offset(arg: &[u8], encoding_bits: u8) -> Result<usize, ()> {
    let s = std::str::from_utf8(arg).map_err(|_| ())?;
    let offset = if let Some(rest) = s.strip_prefix('#') {
        if rest.is_empty() {
            return Err(());
        }
        let idx: u64 = rest.parse().map_err(|_| ())?;
        (idx as u128).checked_mul(encoding_bits as u128).ok_or(())?
    } else {
        s.parse::<u64>().map_err(|_| ())? as u128
    };
    if offset >= 1 << 32 {
        return Err(());
    }
    usize::try_from(offset).map_err(|_| ())
}

fn parse_encoding(enc: &str) -> Option<(bool, u8)> {
    let (signed, rest) = if let Some(rest) = enc.strip_prefix('i') {
        (true, rest)
    } else {
        let rest = enc.strip_prefix('u')?;
        (false, rest)
    };
    let bits: u8 = rest.parse().ok()?;
    if bits == 0 || bits > 64 || !signed && bits > 63 {
        return None;
    }
    Some((signed, bits))
}

fn bitfield_get(bytes: &[u8], encoding: &str, bit_offset: usize) -> Option<i64> {
    let (signed, bits) = parse_encoding(encoding)?;
    let mut val: u64 = 0;
    for i in 0..bits as usize {
        let global_bit = bit_offset + i;
        let byte_i = global_bit / 8;
        let bit_i = 7 - (global_bit % 8);
        let b = bytes.get(byte_i).copied().unwrap_or(0);
        if b & (1 << bit_i) != 0 {
            val |= 1u64 << (bits as usize - 1 - i);
        }
    }
    if signed && bits < 64 && val & (1u64 << (bits - 1)) != 0 {
        // Sign extend
        let extended = val | !((1u64 << bits) - 1);
        Some(extended as i64)
    } else {
        Some(val as i64)
    }
}

fn bitfield_set(bytes: &mut Vec<u8>, encoding: &str, bit_offset: usize, value: i64) {
    let (_, bits) = match parse_encoding(encoding) {
        Some(e) => e,
        None => return,
    };
    let val = value as u64;
    let needed_bytes = (bit_offset + bits as usize).div_ceil(8);
    if bytes.len() < needed_bytes {
        bytes.resize(needed_bytes, 0);
    }
    for i in 0..bits as usize {
        let global_bit = bit_offset + i;
        let byte_i = global_bit / 8;
        let bit_i = 7 - (global_bit % 8);
        let src_bit = (bits as usize - 1 - i) as u64;
        if val & (1u64 << src_bit) != 0 {
            bytes[byte_i] |= 1 << bit_i;
        } else {
            bytes[byte_i] &= !(1 << bit_i);
        }
    }
}

fn bitfield_clamp(encoding: &str, value: i128, overflow: &str) -> Option<i128> {
    let (signed, bits) = parse_encoding(encoding)?;
    let (min, max): (i128, i128) = if signed {
        (-(1i128 << (bits - 1)), (1i128 << (bits - 1)) - 1)
    } else {
        (0, (1i128 << bits) - 1)
    };
    match overflow {
        "FAIL" => {
            if value < min || value > max {
                None
            } else {
                Some(value)
            }
        }
        "SAT" => Some(value.clamp(min, max)),
        _ => {
            // WRAP
            let range = max - min + 1;
            let mut v = value % range;
            if v < min {
                v += range;
            }
            Some(v)
        }
    }
}

// ── Public handle function ─────────────────────────────────────────────────────
pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    match name {
        "GETBIT" => Some(cmd_getbit(ctx)),
        "SETBIT" => Some(cmd_setbit(ctx)),
        "BITCOUNT" => Some(cmd_bitcount(ctx)),
        "BITPOS" => Some(cmd_bitpos(ctx)),
        "BITOP" => Some(cmd_bitop(ctx)),
        "BITFIELD" => Some(cmd_bitfield(ctx, false)),
        "BITFIELD_RO" => Some(cmd_bitfield(ctx, true)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::ctx::NoopAofSender;
    use std::sync::Arc;
    use std::time::Instant;

    /// A string kept in its entry, in a slab, and in allocations of its own.
    const SIZES: [usize; 4] = [1000, 16 << 10, 64 << 10, 1 << 20];

    /// What a command that copies no value may allocate: its reply.
    const SMALL: isize = 512;

    /// `line`, split at spaces, run by the bitmap handler on `store`'s
    /// database 0: the reply, whether the write is left unpropagated, and
    /// the most the handler allocated at once, if this thread counts it.
    fn run(store: &store::Store, line: &str) -> (Vec<u8>, bool, Option<isize>) {
        with_ctx(store, line, &crate::watch::NoopWatchRegistry, |mut ctx| {
            let name = line.split(' ').next().unwrap();
            let (reply, peak) = crate::memory_use::peak_during(|| handle(&mut ctx, name));
            (reply.unwrap(), ctx.skip_write_side_effects, peak)
        })
    }

    /// `line` run as a client's command is, by the dispatcher, which tells
    /// `watches` and the keyspace events of what it writes.
    fn dispatched(
        store: &store::Store,
        line: &str,
        watches: &dyn crate::watch::WatchNotifier,
    ) -> Vec<u8> {
        with_ctx(
            store,
            line,
            watches,
            crate::commands::handler::execute_command,
        )
    }

    /// `f` given a context for `line`, split at spaces, on `store`'s
    /// database 0.
    fn with_ctx<R>(
        store: &store::Store,
        line: &str,
        watches: &dyn crate::watch::WatchNotifier,
        f: impl FnOnce(Ctx) -> R,
    ) -> R {
        let lastsave = Arc::new(parking_lot::RwLock::new(0i64));
        let (hub_tx, _hub_rx) = crate::replication::HubTx::channel();
        let repl_state = Arc::new(crate::replication::ReplState::new_master());
        let config = Arc::new(crate::config::Config::default());
        let started_at = Instant::now();
        let index_store = crate::vector::new_index_store();
        let dict_store = crate::vector::new_dict_store();
        let wait_table = Arc::new(crate::wait_table::WaitTable::new());
        let ts_index = crate::store::new_ts_index();
        let pubsub = crate::pubsub::PubSubHub::new();
        let acl_store = crate::acl::new_acl_store("");
        let acl_log = crate::acl::new_acl_log();
        let slowlog = crate::slowlog::new_slowlog(u64::MAX, 0);
        let latency = crate::latency::new_latency_tracker();
        let (compaction_tx, _compaction_rx) = crate::vector::new_compaction_channel();
        let cluster = Arc::new(crate::cluster::ClusterState::new_disabled());
        let client_registry = crate::client_registry::new_client_registry();
        let monitor_registry = crate::monitor::new_monitor_registry();
        let tracking_table = crate::tracking::new_tracking_table();
        let mut selected_db = 0usize;
        f(Ctx {
            cmd: line.split(' ').map(str::as_bytes).collect(),
            store,
            lastsave: &lastsave,
            selected_db: &mut selected_db,
            aof_tx: &NoopAofSender,
            aof_enabled: false,
            config: &config,
            started_at: &started_at,
            index_store: &index_store,
            dict_store: &dict_store,
            wait_table: &wait_table,
            ts_index: &ts_index,
            pubsub: &pubsub,
            hub_tx: &hub_tx,
            repl_state: &repl_state,
            watch_registry: watches,
            client_id: 0,
            client_registry: &client_registry,
            resp: 2,
            acl_store: &acl_store,
            acl_log: &acl_log,
            slowlog: &slowlog,
            latency: &latency,
            compaction_tx: &compaction_tx,
            cluster: &cluster,
            bypass_cluster_check: false,
            apply_replication: false,
            skip_write_side_effects: false,
            peer_addr: "127.0.0.1:1",
            peer_ipv6: false,
            monitor_registry: &monitor_registry,
            authenticated_user: "default",
            tracking_table: &tracking_table,
            blocking: None,
        })
    }

    /// [`run`] in a test that counts allocations (see [`counting`]).
    fn counted(store: &store::Store, line: &str) -> (Vec<u8>, bool, isize) {
        let (reply, skipped, peak) = run(store, line);
        (
            reply,
            skipped,
            peak.expect("a thread that counts its own allocations"),
        )
    }

    /// Runs `test` on this thread if it counts its own allocations, else in
    /// a process of its own, where it does (see `memory_use`).
    fn counting(name: &str, test: impl FnOnce()) {
        const CHILD: &str = "KIVIDB_BITMAP_TEST_CHILD";
        if crate::memory_use::allocated_by_this_thread().is_some() {
            return test();
        }
        assert!(std::env::var_os(CHILD).is_none(), "no counter in a child");
        in_a_process_of_its_own(name, CHILD);
    }

    /// Runs `test` in a process of its own: it sets what the whole process
    /// shares.
    fn alone(name: &str, test: impl FnOnce()) {
        const CHILD: &str = "KIVIDB_BITMAP_TEST_ALONE";
        if std::env::var_os(CHILD).is_some() {
            return test();
        }
        in_a_process_of_its_own(name, CHILD);
    }

    /// The test `name` run again in a new process, with `child` set there.
    fn in_a_process_of_its_own(name: &str, child: &str) {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([name, "--exact", "--test-threads", "1"])
            .env(child, "1")
            .status()
            .unwrap();
        assert!(status.success(), "{name} failed in a process of its own");
    }

    fn put(store: &store::Store, key: &[u8], value: &[u8]) {
        drop(
            store::get_shard(&store[0], key)
                .write()
                .insert_str(key, value),
        );
    }

    /// `key`'s bytes, and where they are.
    fn stored(store: &store::Store, key: &[u8]) -> Option<(Vec<u8>, usize)> {
        let map = store::get_shard(&store[0], key).read();
        match map.get(key) {
            Some(ValueRef::String(b)) => Some((b.to_vec(), b.as_ptr() as usize)),
            _ => None,
        }
    }

    /// Whether `key`'s bytes are in its entry: a second hold is then a copy.
    fn in_entry(store: &store::Store, key: &[u8]) -> bool {
        let map = store::get_shard(&store[0], key).read();
        matches!(map.get(key), Some(ValueRef::String(b)) if !b.is(&b.to_shared()))
    }

    /// `BITFIELD`'s reply of `values`, a nil for each `None`.
    fn ints(values: &[Option<i64>]) -> Vec<u8> {
        let each = values
            .iter()
            .map(|v| v.map_or_else(|| resp_null_for(2), resp_integer));
        resp_array(each.collect())
    }

    #[test]
    fn the_larger_sizes_are_in_a_slab_and_on_their_own() {
        assert!(crate::value_slab::class_of(SIZES[1] + 8).is_some());
        assert!(crate::value_slab::class_of(SIZES[2] + 8).is_none());
        // Only the first is in its entry.
        for len in SIZES {
            let store = store::new_store(1);
            put(&store, b"k", &vec![0; len]);
            assert_eq!(in_entry(&store, b"k"), len == SIZES[0], "{len}");
        }
    }

    /// A `SETBIT` that finds the bit as asked copies nothing and writes
    /// nothing; one that changes it writes the byte where it is.
    #[test]
    fn a_setbit_that_changes_nothing_copies_nothing() {
        counting(
            "commands::bitmap::tests::a_setbit_that_changes_nothing_copies_nothing",
            || {
                for len in SIZES {
                    let store = store::new_store(1);
                    let mut value = vec![0xAAu8; len];
                    put(&store, b"k", &value);
                    let (_, at) = stored(&store, b"k").unwrap();
                    let last = (len - 1) * 8;
                    for (line, old) in [
                        (format!("SETBIT k {last} 1"), 1),
                        (format!("SETBIT k {} 0", last + 1), 0),
                    ] {
                        let (reply, skipped, peak) = counted(&store, &line);
                        assert_eq!(reply, resp_integer(old), "{len} {line}");
                        assert!(skipped, "{len} {line}");
                        assert!(peak < SMALL, "{len} {line}: {peak} bytes");
                        assert_eq!(stored(&store, b"k"), Some((value.clone(), at)), "{len}");
                    }
                    let (reply, skipped, peak) = counted(&store, &format!("SETBIT k {last} 0"));
                    assert_eq!((reply, skipped), (resp_integer(1), false), "{len}");
                    assert!(peak < SMALL, "{len}: {peak} bytes");
                    value[len - 1] = 0x2A;
                    assert_eq!(stored(&store, b"k"), Some((value, at)), "{len}");
                }
            },
        );
    }

    /// What holds a value besides its key while a `SETBIT` runs.
    #[derive(Debug, Clone, Copy, PartialEq)]
    enum Holder {
        Nothing,
        /// The entry, as a snapshot being written does.
        Entry,
        /// The bytes, as a `DUMP` being encoded does: a copy if in the entry.
        Bytes,
    }

    /// A `SETBIT` that changes a bit of a value nothing else holds writes
    /// it where it is, at every size up to 64 MiB. Held, the value is
    /// copied once, and the holder keeps reading the bytes it held.
    #[test]
    fn a_setbit_that_changes_a_bit_copies_only_a_held_value() {
        counting(
            "commands::bitmap::tests::a_setbit_that_changes_a_bit_copies_only_a_held_value",
            || {
                for len in SIZES.into_iter().chain([64 << 20]) {
                    let store = store::new_store(1);
                    let mut value = vec![0xAAu8; len];
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
                        let (reply, skipped, peak) =
                            counted(&store, &format!("SETBIT k {offset} 0"));
                        assert_eq!(
                            (reply, skipped),
                            (resp_integer(1), false),
                            "{len} {holder:?}"
                        );
                        value[0] &= !(0x80 >> offset);
                        let (now, moved_to) = stored(&store, b"k").unwrap();
                        assert_eq!(now, value, "{len} {holder:?}");
                        let copied = holder == Holder::Entry || holder == Holder::Bytes && !inline;
                        assert_eq!(moved_to != at, copied, "{len} {holder:?}");
                        if copied {
                            // One copy, not the two or three of a copy made
                            // to change and then stored.
                            let most = (len + len / 2) as isize + SMALL;
                            assert!(
                                peak >= len as isize && peak < most,
                                "{len} {holder:?}: {peak}"
                            );
                        } else {
                            assert!(peak < SMALL, "{len} {holder:?}: {peak} bytes");
                        }
                        if let Some(Some(entry)) = &entry {
                            assert!(
                                matches!(entry.value(), ValueRef::String(b) if b[..] == old[..])
                            );
                        }
                        if let Some(bytes) = &bytes {
                            assert_eq!(bytes[..], old[..], "{len} {holder:?}");
                        }
                    }
                }
            },
        );
    }

    /// Growth stays where the value is while its block keeps its class:
    /// the entry's, or a slab block's. A system allocation, or growth past
    /// the class, copies the value once into a new block.
    #[test]
    fn a_setbit_that_grows_a_value_copies_it_only_past_its_block() {
        counting(
            "commands::bitmap::tests::a_setbit_that_grows_a_value_copies_it_only_past_its_block",
            || {
                let slab = |len: usize| crate::value_slab::class_of(len + 8);
                // A slab value with room in its class, then at its class's top.
                let top = crate::value_slab::block_size(slab(16 << 10).unwrap()) - 8;
                assert!(top > (16 << 10) + 1, "no room in the class");
                for (len, grow_to, stays) in [
                    (100, 101, true),
                    (16 << 10, top, true),
                    (top, top + 1, false),
                    (64 << 10, (64 << 10) + 1, false),
                    (1 << 20, (1 << 20) + 1, false),
                ] {
                    let store = store::new_store(1);
                    let mut value = vec![0xAAu8; len];
                    put(&store, b"k", &value);
                    let (_, at) = stored(&store, b"k").unwrap();
                    let line = format!("SETBIT k {} 1", grow_to * 8 - 1);
                    let (reply, skipped, peak) = counted(&store, &line);
                    assert_eq!((reply, skipped), (resp_integer(0), false), "{len}");
                    value.resize(grow_to, 0);
                    value[grow_to - 1] = 1;
                    let (now, moved_to) = stored(&store, b"k").unwrap();
                    assert_eq!(now, value, "{len}");
                    assert_eq!(moved_to == at, stays, "{len}");
                    if stays {
                        assert!(peak < SMALL, "{len}: {peak} bytes");
                    } else {
                        let most = (len + len / 2) as isize + SMALL;
                        assert!(peak >= len as isize && peak < most, "{len}: {peak}");
                    }
                }
            },
        );
    }

    /// A key's time to live stays across a `SETBIT`, in place or copied;
    /// a key past it is written as a new one, without one. Another type is
    /// left as it was.
    #[test]
    fn setbit_keeps_a_time_to_live_and_leaves_other_types() {
        let store = store::new_store(1);
        let later = crate::commands::helpers::now_ms() + 3_600_000;
        for (name, len) in [("inline", 100), ("slab", 16 << 10), ("own", 64 << 10)] {
            let key = name.as_bytes();
            let shard = store::get_shard(&store[0], key);
            drop(
                shard
                    .write()
                    .insert_str_expiring(key, vec![0u8; len], Some(later)),
            );
            let line = format!("SETBIT {name} 0 1");
            assert_eq!(run(&store, &line).0, resp_integer(0));
            let held = shard.read().get_ref(key);
            let line = format!("SETBIT {name} 1 1");
            assert_eq!(run(&store, &line).0, resp_integer(0));
            drop(held);
            assert_eq!(shard.read().expires(key), Some(later), "{len}");
            assert_eq!(stored(&store, key).unwrap().0[0], 0xC0, "{len}");
        }
        let shard = store::get_shard(&store[0], b"past");
        drop(
            shard
                .write()
                .insert_str_expiring(b"past", b"\xFF\xFF", Some(1)),
        );
        assert_eq!(run(&store, "SETBIT past 15 1").0, resp_integer(0));
        assert_eq!(stored(&store, b"past").unwrap().0, b"\x00\x01");
        assert_eq!(shard.read().expires(b"past"), None);
        let set = crate::member_set::MemberSet::new();
        let set = crate::types::KiviDBValue::Set(Arc::new(parking_lot::RwLock::new(set)));
        drop(
            store::get_shard(&store[0], b"set")
                .write()
                .insert(b"set", set),
        );
        let reply = run(&store, "SETBIT set 0 1").0;
        assert!(reply.starts_with(b"-WRONGTYPE"));
        let map = store::get_shard(&store[0], b"set").read();
        assert!(matches!(map.get(b"set"), Some(ValueRef::Set(_))));
    }

    /// A `SETBIT` written in place breaks a `WATCH` on its key and announces
    /// `setbit`, as one that copies a held value does. One that changes
    /// nothing breaks no watch, and is announced as well (see
    /// `keyspace_events_new.rs`). Alone: the event flags are the process's.
    #[test]
    fn a_setbit_written_in_place_breaks_a_watch_and_announces_itself() {
        alone(
            "commands::bitmap::tests::a_setbit_written_in_place_breaks_a_watch_and_announces_itself",
            || {
                use crate::keyspace_events as events;
                use crate::watch::WatchNotifier;
                use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
                events::KS_FLAGS.store(events::parse_ks_flags("KE$"), Relaxed);
                let store = store::new_store(1);
                let watches = crate::watch::WatchRegistryInner::new();
                for (name, len) in [("inline", 100), ("slab", 16 << 10), ("own", 64 << 10)] {
                    let key = name.as_bytes();
                    put(&store, key, &vec![0; len]);
                    // The bit, whether the value is held, whether it changes.
                    for (bit, held, changes) in
                        [(0, false, true), (0, false, false), (9, true, true)]
                    {
                        let shard = store::get_shard(&store[0], key);
                        let held = held.then(|| shard.read().get_ref(key));
                        let at = stored(&store, key).unwrap().1;
                        let broken = Arc::new(AtomicBool::new(false));
                        watches.watch(0, key, Arc::clone(&broken), false);
                        events::hold();
                        let line = format!("SETBIT {name} {bit} 1");
                        let reply = dispatched(&store, &line, &watches);
                        let announced = events::release();
                        let case = format!("{line}, held: {}", held.is_some());
                        assert_eq!(reply, resp_integer(i64::from(!changes)), "{case}");
                        let in_place = stored(&store, key).unwrap().1 == at;
                        assert_eq!(in_place, held.is_none(), "{case}");
                        assert_eq!(broken.load(Relaxed), changes, "{case}: the watch");
                        let mut space = b"__keyspace@0__:".to_vec();
                        space.extend_from_slice(key);
                        let event = b"__keyevent@0__:setbit".to_vec();
                        let want: Vec<(Vec<u8>, Arc<[u8]>)> =
                            vec![(space, Arc::from(&b"setbit"[..])), (event, Arc::from(key))];
                        assert_eq!(announced, want, "{case}: the events");
                        drop(held);
                    }
                    let mut want = vec![0; len];
                    (want[0], want[1]) = (0x80, 0x40);
                    assert_eq!(stored(&store, key).unwrap().0, want, "{name}");
                }
            },
        );
    }

    /// A capture of the dataset holds each string's entry: what `BGSAVE`, a
    /// full sync, an AOF rewrite and `BACKUP` write out. `SETBIT`s after it
    /// change the live value only, and the image has the bytes from before.
    #[test]
    fn a_capture_keeps_the_bytes_from_before_a_setbit() {
        let store = store::new_store(1);
        let keys = ["inline", "slab", "own"];
        for (key, len) in keys.into_iter().zip([100, 16 << 10, 1 << 20]) {
            put(&store, key.as_bytes(), &vec![0xAA; len]);
        }
        let frozen = crate::snapshot::freeze_dataset(&store, None).unwrap();
        for key in keys {
            // The first copies the value; the second finds it unheld.
            for line in [format!("SETBIT {key} 0 0"), format!("SETBIT {key} 2 0")] {
                assert_eq!(run(&store, &line).0, resp_integer(1), "{line}");
            }
            assert_eq!(stored(&store, key.as_bytes()).unwrap().0[0], 0x0A, "{key}");
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dump.kdb");
        let path = path.to_str().unwrap();
        frozen.write_file(path).unwrap();
        let loaded = crate::snapshot::load_snapshot(path).unwrap();
        for key in keys {
            match loaded.dbs[0].get(&store::Key::from(key)) {
                Some(crate::types::KiviDBValue::String(b)) => {
                    assert!(b.iter().all(|&byte| byte == 0xAA), "{key}")
                }
                _ => panic!("{key} not in the image"),
            }
        }
    }

    #[test]
    fn setbit_past_the_end_or_on_a_new_key_writes() {
        let store = store::new_store(1);
        put(&store, b"k", b"\x80");
        // A clear bit past the end still lengthens the string.
        for (line, key, want) in [
            ("SETBIT k 15 0", b"k", &b"\x80\x00"[..]),
            ("SETBIT n 3 0", b"n", b"\x00"),
            ("SETBIT o 9 1", b"o", b"\x00\x40"),
        ] {
            let (reply, skipped, _) = run(&store, line);
            assert_eq!((reply, skipped), (resp_integer(0), false), "{line}");
            assert_eq!(stored(&store, key).unwrap().0, want, "{line}");
        }
        // Refused arguments touch nothing.
        for line in [
            "SETBIT k x 1",
            "SETBIT k 4294967296 1",
            "SETBIT k 0 2",
            "SETBIT k 0",
        ] {
            assert!(run(&store, line).0.starts_with(b"-ERR"), "{line}");
        }
        assert_eq!(stored(&store, b"k").unwrap().0, b"\x80\x00");
    }

    /// `BITFIELD` that only reads, `BITFIELD_RO`, and `OVERFLOW FAIL`
    /// writes that all fail copy no bytes of their own block; bytes in the
    /// entry are copied once, to be read with the shard let go.
    #[test]
    fn a_bitfield_that_writes_nothing_copies_only_bytes_in_the_entry() {
        counting(
            "commands::bitmap::tests::a_bitfield_that_writes_nothing_copies_only_bytes_in_the_entry",
            || {
                for len in SIZES {
                    let store = store::new_store(1);
                    let value = vec![0xAAu8; len];
                    put(&store, b"k", &value);
                    let (_, at) = stored(&store, b"k").unwrap();
                    let mut most = SMALL;
                    if in_entry(&store, b"k") {
                        most += len as isize;
                    }
                    let last = (len - 1) * 8;
                    for (line, want) in [
                        (
                            format!("BITFIELD k GET u8 {last} GET i8 #0"),
                            &[Some(170), Some(-86)][..],
                        ),
                        (
                            format!("BITFIELD_RO k GET u8 0 GET u4 {last}"),
                            &[Some(170), Some(10)],
                        ),
                        (
                            format!("BITFIELD k OVERFLOW FAIL INCRBY u8 {last} 100"),
                            &[None],
                        ),
                        (
                            "BITFIELD k OVERFLOW FAIL SET i8 0 128 GET u8 0 INCRBY i8 8 -100"
                                .into(),
                            &[None, Some(170), None],
                        ),
                    ] {
                        let (reply, skipped, peak) = counted(&store, &line);
                        assert_eq!(reply, ints(want), "{len} {line}");
                        assert!(skipped, "{len} {line}");
                        assert!(peak < most, "{len} {line}: {peak} bytes");
                        assert_eq!(stored(&store, b"k"), Some((value.clone(), at)), "{len}");
                    }
                }
            },
        );
    }

    /// A write is made in one copy of the value, which later subcommands
    /// read; the copy is stored once they are done.
    #[test]
    fn a_bitfield_write_copies_the_value_once() {
        counting(
            "commands::bitmap::tests::a_bitfield_write_copies_the_value_once",
            || {
                for len in SIZES {
                    let store = store::new_store(1);
                    let mut value = vec![0xAAu8; len];
                    put(&store, b"k", &value);
                    let last = (len - 1) * 8;
                    let (reply, skipped, peak) = counted(
                        &store,
                        &format!(
                            "BITFIELD k GET u8 0 OVERFLOW FAIL INCRBY u8 0 100 SET u8 0 1 GET u8 0 \
                         INCRBY u8 0 255 INCRBY u8 {last} 2 GET u8 0"
                        ),
                    );
                    let want = [
                        Some(170),
                        None,
                        Some(170),
                        Some(1),
                        None,
                        Some(172),
                        Some(1),
                    ];
                    assert_eq!((reply, skipped), (ints(&want), false), "{len}");
                    // The copy and the value stored from it; no third.
                    assert!(peak >= len as isize, "{len}: {peak} bytes");
                    if len >= 64 << 10 {
                        assert!(peak < (len * 5 / 2) as isize, "{len}: {peak} bytes");
                    }
                    value[0] = 1;
                    value[len - 1] = 172;
                    assert_eq!(stored(&store, b"k").unwrap().0, value, "{len}");
                }
            },
        );
    }

    #[test]
    fn bitfield_writes_and_failures_in_either_order() {
        let store = store::new_store(1);
        put(&store, b"k", b"\x01\x02\x03");
        let bitfield = |subcommands: &str| {
            let (reply, skipped, _) = run(&store, &format!("BITFIELD k {subcommands}"));
            (reply, skipped)
        };
        // A write, then a failure: the write is kept; and the other way round.
        assert_eq!(
            bitfield("SET u8 0 7 OVERFLOW FAIL INCRBY u8 0 255"),
            (ints(&[Some(1), None]), false)
        );
        assert_eq!(
            bitfield("OVERFLOW FAIL INCRBY u8 8 255 SET u8 8 9"),
            (ints(&[None, Some(2)]), false)
        );
        assert_eq!(stored(&store, b"k").unwrap().0, b"\x07\x09\x03");
        // Bytes put back at one end and changed at the other are a change.
        assert_eq!(
            bitfield("SET u8 0 0 SET u8 0 7 INCRBY u8 16 1"),
            (ints(&[Some(7), Some(0), Some(4)]), false)
        );
        assert_eq!(stored(&store, b"k").unwrap().0, b"\x07\x09\x04");
        // A zero written past the end lengthens the string.
        assert_eq!(bitfield("SET u8 24 0"), (ints(&[Some(0)]), false));
        assert_eq!(stored(&store, b"k").unwrap().0, b"\x07\x09\x04\x00");
        // An error after a write keeps nothing.
        for subcommands in [
            "SET u8 0 99 GET",
            "SET u8 0 99 GET u99 0",
            "SET u8 0 99 GET u8 -1",
            "SET u8 0 99 OVERFLOW NOPE",
            "SET u8 0 99 BOGUS",
        ] {
            assert!(
                bitfield(subcommands).0.starts_with(b"-ERR"),
                "{subcommands}"
            );
        }
        assert_eq!(
            run(&store, "BITFIELD_RO k GET u8 0 SET u8 0 99").0,
            resp_error("ERR BITFIELD_RO only supports the GET subcommand")
        );
        assert_eq!(stored(&store, b"k").unwrap().0, b"\x07\x09\x04\x00");
        // A field across two bytes, changed in the second only, is a change.
        assert_eq!(bitfield("SET u4 14 7"), (ints(&[Some(4)]), false));
        assert_eq!(stored(&store, b"k").unwrap().0, b"\x07\x09\xC4\x00");
    }

    #[test]
    fn bitfield_on_a_missing_key() {
        let store = store::new_store(1);
        for line in [
            "BITFIELD k GET u8 0",
            "BITFIELD_RO k GET u8 0",
            "BITFIELD k OVERFLOW FAIL INCRBY u8 0 256",
        ] {
            let (reply, skipped, _) = run(&store, line);
            let want = if line.contains("FAIL") { None } else { Some(0) };
            assert_eq!((reply, skipped), (ints(&[want]), true), "{line}");
        }
        assert_eq!(stored(&store, b"k"), None);
        let (reply, skipped, _) = run(&store, "BITFIELD k SET u4 4 3");
        assert_eq!((reply, skipped), (ints(&[Some(0)]), false));
        assert_eq!(stored(&store, b"k").unwrap().0, b"\x03");
    }
}
