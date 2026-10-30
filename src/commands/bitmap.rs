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
// IMPORTANT Rust lesson: when you do `string[byte_idx] |= mask`, you need a
// mutable reference. Since KiviDBValue::String wraps an `Arc<[u8]>`, which is
// immutable, we need to copy-on-write: get the existing bytes, clone into a
// Vec<u8>, mutate, then store back as a new Arc<[u8]>.

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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let offset: u64 = match std::str::from_utf8(cmd[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_error("ERR bit offset is not an integer or out of range"),
    };

    if is_expired(ctx.store, *ctx.selected_db, key) {
        expire_on_read(ctx.store, *ctx.selected_db, key);
        return resp_integer(0);
    }

    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, key);
    let map = read_lock!(shard);

    match map.get(key) {
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
// Rust lesson: Arc<[u8]> is immutable. To mutate, we must:
//   1. Read the existing bytes (or empty)
//   2. Clone into a Vec<u8>
//   3. Extend if needed (Redis auto-extends with zeros)
//   4. Mutate the specific byte
//   5. Store back as a new Arc<[u8]>
fn cmd_setbit(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() != 4 {
        return resp_error("ERR wrong number of arguments for 'setbit' command");
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
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

    let key_owned = key.to_string();
    let (byte_idx, mask) = bit_byte_and_mask(offset);

    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, key);
    let mut map = write_lock!(shard);

    // Get existing bytes or start empty
    let existing: Vec<u8> = match map.get(key) {
        Some(ValueRef::String(b)) => b.to_vec(), // Arc<[u8]> → Vec<u8> (clone)
        None => vec![],
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

    // Extend with zeros if needed, then set/clear the bit
    let mut new_bytes = existing;
    if byte_idx >= new_bytes.len() {
        new_bytes.resize(byte_idx + 1, 0u8);
    }
    if bit_val == 1 {
        new_bytes[byte_idx] |= mask;
    } else {
        new_bytes[byte_idx] &= !mask;
    }

    map.insert_str(key_owned, new_bytes.as_slice());

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

    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

    // Hot path: whole string — no range parsing
    if cmd.len() == 2 {
        if is_expired(ctx.store, *ctx.selected_db, key) {
            expire_on_read(ctx.store, *ctx.selected_db, key);
            return resp_integer(0);
        }
        let db = &ctx.store[*ctx.selected_db];
        let shard = store::get_shard(db, key);
        let map = read_lock!(shard);
        match map.get(key) {
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

        let bytes: SharedBytes = match map.get(key).filter(|_| !gone) {
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

    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

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
        return resp_integer(if search_bit == 0 { 0 } else { -1 });
    }

    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, key);
    let map = read_lock!(shard);

    let bytes = match map.get(key) {
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
    let dest_key = match parse_utf8(cmd[2]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR dest key must be valid UTF-8"),
    };

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
        let key = match parse_utf8(src_raw) {
            Some(k) => k,
            None => {
                sources.push(vec![]);
                continue;
            }
        };
        if is_expired(ctx.store, *ctx.selected_db, key) {
            delete_key_if_expired(ctx.store, *ctx.selected_db, key);
            sources.push(vec![]);
            continue;
        }
        let shard = store::get_shard(db, key);
        let map = read_lock!(shard);
        match map.get(key) {
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };

    // We'll accumulate results for each subcommand
    let mut results: Vec<Vec<u8>> = Vec::new();
    let mut i = 2usize;

    // Read current bytes (we may need to mutate)
    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, &key);

    let mut current_bytes: Vec<u8> = {
        let map = read_lock!(shard);
        match map.get(key.as_str()) {
            Some(ValueRef::String(b)) => b.to_vec(),
            None => vec![],
            _ => {
                return resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                );
            }
        }
    };
    let snapshot_before = current_bytes.clone();

    let mut overflow_mode = "WRAP"; // WRAP | SAT | FAIL
    let mut modified = false;

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
                let val = bitfield_get(&current_bytes, &encoding, offset);
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

                let old_val = bitfield_get(&current_bytes, &encoding, offset);
                let new_val: i128 = if op == "SET" {
                    value
                } else {
                    old_val.unwrap_or(0) as i128 + value // no pre-wrap; clamp decides
                };
                let clamped = bitfield_clamp(&encoding, new_val, overflow_mode);
                match clamped {
                    Some(v) => {
                        bitfield_set(&mut current_bytes, &encoding, offset, v as i64);
                        modified = true;
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

    if modified && current_bytes == snapshot_before {
        modified = false;
    }
    if modified {
        let mut map = write_lock!(shard);
        map.insert_str(key, current_bytes.as_slice());
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
