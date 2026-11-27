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
    crate::commands::login::strict_integer(arg).ok_or(())
}

#[inline]
fn parse_bit_offset_arg(arg: &[u8]) -> Result<u64, ()> {
    let offset = u64::try_from(parse_i64_arg(arg)?).map_err(|_| ())?;
    if offset >= 1 << 32 {
        return Err(());
    }
    Ok(offset)
}

// ── GETBIT ────────────────────────────────────────────────────────────────────
fn cmd_getbit(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() != 3 {
        return resp_error("ERR wrong number of arguments for 'getbit' command");
    }
    let key: &[u8] = cmd[1];
    let offset: u64 = match parse_bit_offset_arg(cmd[2]) {
        Ok(n) => n,
        Err(()) => return resp_error("ERR bit offset is not an integer or out of range"),
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
    let offset: u64 = match parse_bit_offset_arg(cmd[2]) {
        Ok(n) => n,
        Err(()) => return resp_error("ERR bit offset is not an integer or out of range"),
    };
    let bit_val: u8 = match parse_i64_arg(cmd[3]) {
        Ok(0) => 0,
        Ok(1) => 1,
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
        let start: i64 = match parse_i64_arg(cmd[2]) {
            Ok(n) => n,
            Err(()) => return resp_error("ERR value is not an integer or out of range"),
        };
        let end: i64 = match parse_i64_arg(cmd[3]) {
            Ok(n) => n,
            Err(()) => return resp_error("ERR value is not an integer or out of range"),
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
    let total = if bit_mode { len * 8 } else { len };
    let mut start = start_byte;
    let mut end = if end_given { end_byte } else { total - 1 };

    if start < 0 {
        start += total;
    }
    if end < 0 {
        end += total;
    }
    if start < 0 {
        start = 0;
    }
    if end < 0 {
        end = 0;
    }
    if end >= total {
        end = total - 1;
    }
    if start > end {
        return resp_integer(-1);
    }

    if bit_mode {
        // BIT mode: indices are bit positions
        for bit in start..=end {
            let (bi, mask) = bit_byte_and_mask(bit as u64);
            let found = (bytes[bi] & mask != 0) == (search_bit == 1);
            if found {
                return resp_integer(bit);
            }
        }
        return resp_integer(-1);
    }

    // BYTE mode
    for byte_i in start..=end {
        let b = bytes[byte_i as usize];
        for bit_i in 0..8u64 {
            let mask = 1u8 << (7 - bit_i);
            let is_set = b & mask != 0;
            if is_set == (search_bit == 1) {
                return resp_integer(byte_i * 8 + bit_i as i64);
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
    const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
    // BITFIELD returns one result slot per subcommand; a failed GET or an
    // OVERFLOW FAIL yields a nil *inside* that array, which the dispatcher's
    // top-level null normalisation cannot reach.
    let resp = crate::commands::handler::effective_resp_for_reply(ctx);
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return resp_error("ERR wrong number of arguments for 'bitfield' command");
    }
    let key: &[u8] = cmd[1];
    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, key);
    // Parsed before the key is locked; another type is still refused before
    // a malformed operation is.
    let ops = parse_bitfield(cmd, readonly);
    // Only GETs (or `BITFIELD_RO`): a read, counted as Redis counts one.
    let writes = cmd[2..]
        .iter()
        .any(|a| a.eq_ignore_ascii_case(b"SET") || a.eq_ignore_ascii_case(b"INCRBY"));
    if readonly || !writes {
        let map = read_lock!(shard);
        let found = map.get(key);
        crate::stats::record_lookup(found.is_some());
        let bytes = match found {
            Some(ValueRef::String(bytes)) => bytes.as_bytes(),
            None => &[],
            Some(_) => return resp_error(WRONGTYPE),
        };
        let ops = match ops {
            Ok(ops) => ops,
            Err(reply) => return reply,
        };
        let replies = ops
            .iter()
            .map(|op| resp_integer(op.get(&field_bytes(bytes, op.first(), op.end()))))
            .collect();
        drop(map);
        ctx.skip_write_side_effects = true;
        return resp_array(replies);
    }

    // The operations run in order on the live bytes, with the shard locked
    // from the type check to the last write: nothing sees them half done.
    let mut map = write_lock!(shard);
    let start = match map.get(key) {
        Some(ValueRef::String(bytes)) => bytes.len(),
        None => 0,
        Some(_) => return resp_error(WRONGTYPE),
    };
    let ops = match ops {
        Ok(ops) => ops,
        Err(reply) => return reply,
    };
    // An entry a write replaced, freed once the shard is unlocked (see
    // `keep`).
    let mut replaced: Option<store::Taken> = None;
    // Grown once, before the operations, to the end of the furthest write
    // applied whatever the field holds. A write OVERFLOW FAIL may refuse
    // grows the string only once it is applied.
    let mut len = start;
    let always = ops
        .iter()
        .filter(|op| op.always_applies())
        .map(FieldOp::end)
        .max()
        .unwrap_or(0);
    if always > len {
        grow_zeroed(&mut map, key, always - len, &mut replaced);
        len = always;
    }
    // Redis 8's rule: the command writes if an applied SET or INCRBY grows
    // the string or changes its field, even when a later one puts the bits
    // back. One that finds the bits as asked within the string does not,
    // nor does one OVERFLOW FAIL refuses. A write adds one to the dirty
    // count, as any write command does.
    let mut wrote = len > start;
    let mut replies = Vec::with_capacity(ops.len());
    for op in &ops {
        let (first, end) = (op.first(), op.end());
        let mut field = [0; 9];
        if first < len
            && let Some(ValueRef::String(bytes)) = map.peek(key)
        {
            field = field_bytes(bytes.as_bytes(), first, end);
        }
        let field = &mut field[..end - first];
        let old = op.get(field);
        let Some(write) = &op.write else {
            replies.push(resp_integer(old));
            continue;
        };
        let target = if write.incrby {
            i128::from(old).saturating_add(write.value)
        } else {
            write.value
        };
        let Some(new) = bitfield_clamp(op.encoding, target, write.overflow.name()) else {
            // Overflow FAIL yields a nil in this slot of the results array —
            // nested, so it needs the protocol's own null encoding.
            replies.push(resp_null_for(resp));
            continue;
        };
        if end > len {
            grow_zeroed(&mut map, key, end - len, &mut replaced);
            len = end;
            wrote = true;
        }
        let mask = u64::MAX >> (64 - op.bits);
        if (new as i64 as u64 ^ old as u64) & mask != 0 {
            bitfield_set(field, op.encoding, op.offset % 8, new as i64);
            let Some(written) = map.edit_str_in_place(key, first, field) else {
                unreachable!("a string under this lock, and no field ends past MAX_LEN");
            };
            keep(&mut replaced, written);
            wrote = true;
        }
        replies.push(resp_integer(if write.incrby { new as i64 } else { old }));
    }
    drop(map);
    drop(replaced);
    if !wrote {
        ctx.skip_write_side_effects = true;
    }
    resp_array(replies)
}

/// `n` zero bytes added to `key`'s string, which is not another type, as
/// an `APPEND` adds bytes: in place when nothing else holds it and its
/// block has room, else into a block with room for more, copied once.
fn grow_zeroed(
    map: &mut store::ShardMap,
    key: &[u8],
    n: usize,
    replaced: &mut Option<store::Taken>,
) {
    let Ok((_, written)) = map.append_zeros(key, n, crate::shared_bytes::MAX_LEN) else {
        unreachable!("a string under this lock, and no field ends past MAX_LEN");
    };
    keep(replaced, written);
}

/// Keeps the entry `written` replaced, to be freed once the shard is
/// unlocked, or frees one of two here: an inline one, which is small. The
/// first replaced is the key's own, which a save or a sync may hold; after
/// it the key's entry is one this command made, held by nothing else, and
/// only an inline one of those is replaced (grown past its block).
fn keep(replaced: &mut Option<store::Taken>, written: store::Written) {
    let Some(old) = written.replaced() else {
        return;
    };
    let freed = match replaced.take() {
        Some(kept) if kept.is_inline() => {
            *replaced = Some(old);
            kept
        }
        Some(kept) => {
            *replaced = Some(kept);
            old
        }
        None => {
            *replaced = Some(old);
            return;
        }
    };
    drop(freed);
}

/// Bytes `first..end` of `bytes`, zeros past its end: a field of up to 64
/// bits, from any bit, is in nine.
fn field_bytes(bytes: &[u8], first: usize, end: usize) -> [u8; 9] {
    let mut field = [0; 9];
    if let Some(have) = bytes.get(first..end.min(bytes.len())) {
        field[..have.len()].copy_from_slice(have);
    }
    field
}

/// One BITFIELD operation: a GET when `write` is `None`.
struct FieldOp<'a> {
    encoding: &'a str,
    /// Of the field's first bit.
    offset: usize,
    bits: u32,
    write: Option<FieldWrite>,
}

impl FieldOp<'_> {
    /// Bytes `first()..end()` hold the field.
    fn first(&self) -> usize {
        self.offset / 8
    }

    fn end(&self) -> usize {
        self.offset.saturating_add(self.bits as usize).div_ceil(8)
    }

    /// The field's value, in its bytes from `first()`.
    fn get(&self, field: &[u8]) -> i64 {
        bitfield_get(field, self.encoding, self.offset % 8).unwrap_or(0)
    }

    /// A write applied whatever the field holds: all but an OVERFLOW FAIL
    /// SET of a value out of range, and an OVERFLOW FAIL INCRBY of other
    /// than 0.
    fn always_applies(&self) -> bool {
        self.write
            .as_ref()
            .is_some_and(|write| match write.overflow {
                Overflow::Fail if write.incrby => write.value == 0,
                Overflow::Fail => bitfield_clamp(self.encoding, write.value, "FAIL").is_some(),
                Overflow::Wrap | Overflow::Sat => true,
            })
    }
}

struct FieldWrite {
    value: i128,
    incrby: bool,
    overflow: Overflow,
}

#[derive(Clone, Copy)]
enum Overflow {
    Wrap,
    Sat,
    Fail,
}

impl Overflow {
    fn name(self) -> &'static str {
        match self {
            Overflow::Wrap => "WRAP",
            Overflow::Sat => "SAT",
            Overflow::Fail => "FAIL",
        }
    }
}

/// `cmd`'s operations, checked in order: the first error is the reply.
fn parse_bitfield<'a>(cmd: &[&'a [u8]], readonly: bool) -> Result<Vec<FieldOp<'a>>, Vec<u8>> {
    const BAD_TYPE: &str = "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.";
    // Each takes three arguments or more.
    let mut ops = Vec::with_capacity(cmd.len().saturating_sub(2) / 3);
    let mut overflow = Overflow::Wrap;
    let mut i = 2usize;
    while i < cmd.len() {
        let sub = std::str::from_utf8(cmd[i])
            .unwrap_or("")
            .to_ascii_uppercase();
        i += 1;
        let incrby = match sub.as_str() {
            "OVERFLOW" => {
                if i >= cmd.len() {
                    return Err(resp_error("ERR syntax error"));
                }
                overflow = match std::str::from_utf8(cmd[i])
                    .unwrap_or("")
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "WRAP" => Overflow::Wrap,
                    "SAT" => Overflow::Sat,
                    "FAIL" => Overflow::Fail,
                    _ => return Err(resp_error("ERR Invalid OVERFLOW type specified")),
                };
                i += 1;
                continue;
            }
            "GET" => {
                if i + 1 >= cmd.len() {
                    return Err(resp_error("ERR syntax error"));
                }
                None
            }
            "SET" | "INCRBY" => {
                if readonly {
                    return Err(resp_error(
                        "ERR BITFIELD_RO only supports the GET subcommand",
                    ));
                }
                if i + 2 >= cmd.len() {
                    return Err(resp_error("ERR syntax error"));
                }
                Some(sub == "INCRBY")
            }
            _ => return Err(resp_error("ERR syntax error")),
        };
        let encoding = std::str::from_utf8(cmd[i]).unwrap_or("");
        i += 1;
        let Some((_, bits)) = parse_encoding(encoding) else {
            return Err(resp_error(BAD_TYPE));
        };
        let Ok(offset) = parse_bitfield_offset(cmd[i], bits) else {
            return Err(resp_error(
                "ERR bit offset is not an integer or out of range",
            ));
        };
        i += 1;
        let write = if let Some(incrby) = incrby {
            let Ok(value) = parse_i64_arg(cmd[i]) else {
                return Err(resp_error("ERR value is not an integer or out of range"));
            };
            i += 1;
            Some(FieldWrite {
                value: i128::from(value),
                incrby,
                overflow,
            })
        } else {
            None
        };
        ops.push(FieldOp {
            encoding,
            offset,
            bits: u32::from(bits),
            write,
        });
    }
    Ok(ops)
}

// ── Bitfield encoding helpers ─────────────────────────────────────────────────
// encoding: "u8", "i16", "u32", etc. — u = unsigned, i = signed

/// `#N` means bit offset `N * encoding_bits` (Redis-compatible successive field layout).
/// A bit offset, as SETBIT's: below 2^32, so that a string is never made
/// bigger than 512 MB (Redis's `proto-max-bulk-len`).
fn parse_bitfield_offset(arg: &[u8], encoding_bits: u8) -> Result<usize, ()> {
    let (raw, scale) = if let Some(rest) = arg.strip_prefix(b"#") {
        (rest, encoding_bits as u128)
    } else {
        (arg, 1)
    };
    let idx = parse_i64_arg(raw)?;
    if idx < 0 {
        return Err(());
    }
    let offset = (idx as u128).checked_mul(scale).ok_or(())?;
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
    let bits = crate::commands::login::strict_integer(rest.as_bytes())?;
    if !(1..=64).contains(&bits) || (!signed && bits > 63) {
        return None;
    }
    Some((signed, bits as u8))
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

fn bitfield_set(bytes: &mut [u8], encoding: &str, bit_offset: usize, value: i64) {
    let (_, bits) = match parse_encoding(encoding) {
        Some(e) => e,
        None => return,
    };
    let val = value as u64;
    debug_assert!(bytes.len() >= (bit_offset + bits as usize).div_ceil(8));
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
            } else if v > max {
                v -= range;
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
pub(crate) mod tests {
    use super::*;
    use crate::commands::ctx::NoopAofSender;
    use std::sync::Arc;
    use std::time::Instant;

    /// A string kept in its entry, in a slab, and in allocations of its own.
    pub(crate) const SIZES: [usize; 4] = [1000, 16 << 10, 64 << 10, 1 << 20];

    /// What a command that copies no value may allocate: its reply.
    pub(crate) const SMALL: isize = 512;

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
    pub(crate) fn dispatched(
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
    pub(crate) fn with_ctx<R>(
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
        thread_local! {
            // A hub's thread outlives it by up to a second: one hub a test
            // thread, not one a command, or many commands make thousands.
            static PUBSUB: crate::pubsub::PubSubHub = crate::pubsub::PubSubHub::new();
        }
        let pubsub = PUBSUB.with(Clone::clone);
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
    pub(crate) fn counting(name: &str, test: impl FnOnce()) {
        const CHILD: &str = "KIVIDB_BITMAP_TEST_CHILD";
        if crate::memory_use::allocated_by_this_thread().is_some() {
            return test();
        }
        assert!(std::env::var_os(CHILD).is_none(), "no counter in a child");
        in_a_process_of_its_own(name, CHILD);
    }

    /// Runs `test` in a process of its own: it sets what the whole process
    /// shares.
    pub(crate) fn alone(name: &str, test: impl FnOnce()) {
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

    pub(crate) fn put(store: &store::Store, key: &[u8], value: &[u8]) {
        drop(
            store::get_shard(&store[0], key)
                .write()
                .insert_str(key, value),
        );
    }

    /// `key`'s bytes, and where they are.
    pub(crate) fn stored(store: &store::Store, key: &[u8]) -> Option<(Vec<u8>, usize)> {
        let map = store::get_shard(&store[0], key).read();
        match map.get(key) {
            Some(ValueRef::String(b)) => Some((b.to_vec(), b.as_ptr() as usize)),
            _ => None,
        }
    }

    /// Whether `key`'s bytes are in its entry: a second hold is then a copy.
    pub(crate) fn in_entry(store: &store::Store, key: &[u8]) -> bool {
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
    pub(crate) enum Holder {
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
    /// writes that all fail copy no bytes and leave the value where it is.
    #[test]
    fn a_bitfield_that_writes_nothing_copies_nothing() {
        counting(
            "commands::bitmap::tests::a_bitfield_that_writes_nothing_copies_nothing",
            || {
                for len in SIZES {
                    let store = store::new_store(1);
                    let value = vec![0xAAu8; len];
                    put(&store, b"k", &value);
                    let (_, at) = stored(&store, b"k").unwrap();
                    let most = SMALL;
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

    /// Writes at both ends of the value, which later operations read, are
    /// made where it is: nothing copied.
    #[test]
    fn bitfield_writes_at_both_ends_go_in_place() {
        counting(
            "commands::bitmap::tests::bitfield_writes_at_both_ends_go_in_place",
            || {
                for len in SIZES {
                    let store = store::new_store(1);
                    let mut value = vec![0xAAu8; len];
                    put(&store, b"k", &value);
                    let (_, at) = stored(&store, b"k").unwrap();
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
                    // The operations and their replies only.
                    assert!(peak < 7 * 160, "{len}: {peak} bytes");
                    value[0] = 1;
                    value[len - 1] = 172;
                    assert_eq!(stored(&store, b"k"), Some((value, at)), "{len}");
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

    /// A write edits the value where it is, at every size, keeping its time
    /// to live: no copy.
    #[test]
    fn a_bitfield_write_edits_the_value_in_place() {
        counting(
            "commands::bitmap::tests::a_bitfield_write_edits_the_value_in_place",
            || {
                let later = crate::commands::helpers::now_ms() + 3_600_000;
                for len in SIZES {
                    let store = store::new_store(1);
                    let shard = store::get_shard(&store[0], b"k");
                    let mut value = vec![0xAAu8; len];
                    drop(shard.write().insert_str_expiring(b"k", &value, Some(later)));
                    let (_, at) = stored(&store, b"k").unwrap();
                    let mid = len / 2 * 8;
                    let (reply, skipped, peak) = counted(
                        &store,
                        &format!("BITFIELD k INCRBY u8 {mid} 1 SET i16 {} -2", mid + 12),
                    );
                    assert_eq!((reply, skipped), (ints(&[Some(171), Some(-21846)]), false));
                    assert!(peak < SMALL, "{len}: {peak} bytes");
                    value[len / 2..len / 2 + 4].copy_from_slice(&[0xAB, 0xAF, 0xFF, 0xEA]);
                    assert_eq!(stored(&store, b"k"), Some((value, at)), "{len}");
                    assert_eq!(shard.read().expires(b"k"), Some(later), "{len}");
                }
            },
        );
    }

    /// A write past the end grows the value as an `APPEND` does: in its
    /// block if it has room, else in one with room for more, the value
    /// copied at most once.
    #[test]
    fn a_bitfield_that_grows_the_value_copies_it_at_most_once() {
        counting(
            "commands::bitmap::tests::a_bitfield_that_grows_the_value_copies_it_at_most_once",
            || {
                for len in SIZES {
                    let store = store::new_store(1);
                    put(&store, b"k", &vec![0xAAu8; len]);
                    let (reply, skipped, peak) =
                        counted(&store, &format!("BITFIELD k SET u8 {} 7", (len + 4) * 8));
                    assert_eq!((reply, skipped), (ints(&[Some(0)]), false));
                    assert!(peak < (len * 3 / 2) as isize + SMALL, "{len}: {peak} bytes");
                    let now = stored(&store, b"k").unwrap().0;
                    assert_eq!(now.len(), len + 5);
                    assert_eq!(&now[len - 1..], b"\xAA\0\0\0\0\x07", "{len}");
                }
            },
        );
    }

    /// An entry held elsewhere, as a snapshot holds one, keeps its bytes;
    /// the key gets the written ones and keeps its time to live.
    #[test]
    fn a_bitfield_beside_a_held_entry_leaves_it_as_it_was() {
        let later = crate::commands::helpers::now_ms() + 3_600_000;
        for len in SIZES {
            let store = store::new_store(1);
            let shard = store::get_shard(&store[0], b"k");
            let value = vec![0x11u8; len];
            drop(shard.write().insert_str_expiring(b"k", &value, Some(later)));
            let held = shard.read().get_ref(b"k").unwrap();
            assert_eq!(
                bitfield(&store, "k", "SET u8 0 34 INCRBY u8 8 1"),
                (ints(&[Some(17), Some(18)]), false),
                "{len}"
            );
            assert!(matches!(held.value(), ValueRef::String(b) if b.as_bytes() == value));
            let mut want = value.clone();
            want[..2].copy_from_slice(&[34, 18]);
            assert_eq!(stored(&store, b"k").unwrap().0, want, "{len}");
            assert_eq!(shard.read().expires(b"k"), Some(later), "{len}");
        }
    }

    /// Writes that fail, or change no bits, copy nothing however many they
    /// are.
    #[test]
    fn bitfield_writes_that_change_nothing_copy_nothing() {
        counting(
            "commands::bitmap::tests::bitfield_writes_that_change_nothing_copy_nothing",
            || {
                let len = 8 << 20;
                let store = store::new_store(1);
                put(&store, b"k", &vec![0xAAu8; len]);
                for n in [1_000usize, 10_000] {
                    let at = |k: usize| k * (len / n) * 8;
                    let fail: String = (0..n)
                        .map(|k| format!(" INCRBY u8 {} 100", at(k)))
                        .collect();
                    let same: String = (0..n).map(|k| format!(" SET u8 {} 170", at(k))).collect();
                    for (what, ops) in [("fail", format!("OVERFLOW FAIL{fail}")), ("same", same)] {
                        let line = format!("BITFIELD k {}", ops.trim_start());
                        let (_, skipped, peak) = counted(&store, &line);
                        assert!(skipped, "{what} {n}");
                        // The operations and their replies only.
                        assert!(peak < n as isize * 160, "{what} {n}: {peak} bytes");
                    }
                }
                assert_eq!(stored(&store, b"k").unwrap().0, vec![0xAAu8; len]);
            },
        );
    }

    /// SETs that OVERFLOW FAIL refuses whatever the field holds grow
    /// nothing and allocate nothing, however many and however far. One that
    /// can succeed, under FAIL in range or under WRAP or SAT, grows the
    /// string to its field.
    #[test]
    fn bitfield_sets_that_must_fail_grow_nothing() {
        counting(
            "commands::bitmap::tests::bitfield_sets_that_must_fail_grow_nothing",
            || {
                let store = store::new_store(1);
                let refused = [
                    ("u8", "256"),
                    ("i8", "-129"),
                    ("i16", "32768"),
                    ("u63", "-1"),
                ];
                for (n, reach) in [
                    (2usize, (1usize << 32) - 8),
                    (1_000, 4 << 23),
                    (10_000, (1 << 32) - 8),
                ] {
                    put(&store, b"k", &[0u8; 100]);
                    let fail: String = (1..n)
                        .map(|k| {
                            let (encoding, value) = refused[k % refused.len()];
                            let at = reach / 8 * k / (n - 1) * 8;
                            format!(" SET {encoding} {at} {value}")
                        })
                        .collect();
                    let line = format!("BITFIELD k SET u8 8 1 OVERFLOW FAIL{fail}");
                    let (reply, skipped, peak) = counted(&store, &line);
                    let mut want = vec![None; n];
                    want[0] = Some(0);
                    assert_eq!((reply, skipped), (ints(&want), false), "{n}");
                    // The operations and their replies only.
                    assert!(peak < (n * 160) as isize + SMALL, "{n}: {peak} bytes");
                    let mut now = vec![0u8; 100];
                    now[1] = 1;
                    assert_eq!(stored(&store, b"k").unwrap().0, now, "{n}");
                }
                // A far SET that can succeed: under FAIL in range, or out of
                // range under WRAP or SAT.
                let far = 4 << 23;
                for tail in [
                    format!("OVERFLOW FAIL SET u8 {far} 255"),
                    format!("OVERFLOW WRAP SET u8 {far} 256"),
                    format!("OVERFLOW SAT SET u8 {far} 256"),
                ] {
                    put(&store, b"k", &[0u8; 100]);
                    let line = format!("BITFIELD k SET u8 8 1 {tail}");
                    let (reply, skipped, peak) = counted(&store, &line);
                    assert_eq!(
                        (reply, skipped),
                        (ints(&[Some(0), Some(0)]), false),
                        "{tail}"
                    );
                    assert!(peak >= far as isize / 8, "{tail}: {peak} bytes");
                    let now = stored(&store, b"k").unwrap().0;
                    assert_eq!((now.len(), now[1]), (far / 8 + 1, 1), "{tail}");
                }
            },
        );
    }

    // ── BITFIELD semantics ───────────────────────────────────────────────────

    /// `BITFIELD key subcommands`: its reply, and whether it was skipped as
    /// a command that wrote nothing.
    fn bitfield(store: &store::Store, key: &str, subcommands: &str) -> (Vec<u8>, bool) {
        let (reply, skipped, _) = run(store, &format!("BITFIELD {key} {subcommands}"));
        (reply, skipped)
    }

    #[test]
    fn bitfield_operations_on_one_field_see_each_other() {
        let store = store::new_store(1);
        // WRAP by default: 15 - 20 is 251.
        assert_eq!(
            bitfield(
                &store,
                "k",
                "SET u8 0 10 INCRBY u8 0 5 INCRBY u8 0 -20 GET u8 0"
            ),
            (ints(&[Some(0), Some(15), Some(251), Some(251)]), false)
        );
        assert_eq!(stored(&store, b"k").unwrap().0, b"\xFB");
        assert_eq!(
            bitfield(
                &store,
                "k",
                "OVERFLOW SAT INCRBY u8 0 10 SET u8 0 3 INCRBY u8 0 -300 GET u8 0"
            ),
            (ints(&[Some(255), Some(255), Some(0), Some(0)]), false)
        );
        assert_eq!(stored(&store, b"k").unwrap().0, b"\x00");
        // The mode holds until the next OVERFLOW.
        assert_eq!(
            bitfield(
                &store,
                "k",
                "SET i8 0 -100 INCRBY i8 0 -28 OVERFLOW SAT INCRBY i8 0 -1 \
                 OVERFLOW FAIL INCRBY i8 0 -1 INCRBY i8 0 255 GET i8 0"
            ),
            (
                ints(&[Some(0), Some(-128), Some(-128), None, Some(127), Some(127)]),
                false
            )
        );
        assert_eq!(stored(&store, b"k").unwrap().0, b"\x7F");
    }

    #[test]
    fn bitfield_get_reads_the_writes_before_it() {
        let store = store::new_store(1);
        put(&store, b"k", b"\x05\x00");
        assert_eq!(
            bitfield(
                &store,
                "k",
                "GET u8 0 SET u8 0 200 GET u8 0 GET u4 4 INCRBY u8 0 1 GET u16 0"
            ),
            (
                ints(&[
                    Some(5),
                    Some(5),
                    Some(200),
                    Some(8),
                    Some(201),
                    Some(0xC900)
                ]),
                false
            )
        );
        assert_eq!(stored(&store, b"k").unwrap().0, b"\xC9\x00");
    }

    /// Commands whose operations change bytes and then put them back, on
    /// `\x01\x02`, and their replies.
    const PUT_BACK: [(&str, &[Option<i64>]); 4] = [
        ("SET u8 0 7 SET u8 0 1", &[Some(1), Some(7)]),
        ("INCRBY u8 8 5 INCRBY u8 8 -5", &[Some(7), Some(2)]),
        (
            "SET u16 0 0 SET u8 0 1 SET u8 8 2",
            &[Some(258), Some(0), Some(0)],
        ),
        ("INCRBY u8 0 255 INCRBY u8 0 1", &[Some(0), Some(1)]),
    ];

    /// An operation that changes bits is a write, whatever the operations
    /// after it do, as in Redis. The bytes stay as they were.
    #[test]
    fn a_bitfield_that_puts_its_bytes_back_writes() {
        let store = store::new_store(1);
        put(&store, b"k", b"\x01\x02");
        for (subcommands, replies) in PUT_BACK {
            assert_eq!(
                bitfield(&store, "k", subcommands),
                (ints(replies), false),
                "{subcommands}"
            );
            assert_eq!(stored(&store, b"k").unwrap().0, b"\x01\x02");
        }
    }

    #[test]
    fn a_bitfield_that_writes_the_same_bits_is_skipped() {
        let store = store::new_store(1);
        put(&store, b"k", b"\x01\x02");
        for (subcommands, replies) in [
            ("SET u8 0 1", &[Some(1)][..]),
            ("INCRBY u8 8 0", &[Some(2)]),
            ("SET u16 0 258", &[Some(258)]),
            // 257 wraps to 1.
            ("SET u8 0 257", &[Some(1)]),
            ("SET u4 4 1 SET u4 12 2", &[Some(1), Some(2)]),
            ("OVERFLOW FAIL INCRBY u8 0 0", &[Some(1)]),
            (
                "GET u8 0 SET i8 8 2 INCRBY i16 0 0",
                &[Some(1), Some(2), Some(258)],
            ),
        ] {
            assert_eq!(
                bitfield(&store, "k", subcommands),
                (ints(replies), true),
                "{subcommands}"
            );
            assert_eq!(stored(&store, b"k").unwrap().0, b"\x01\x02");
        }
    }

    #[test]
    fn a_zero_written_past_the_end_grows_the_string() {
        let store = store::new_store(1);
        put(&store, b"k", b"abc");
        for (subcommands, now) in [
            ("SET u8 24 0", &b"abc\0"[..]),
            ("INCRBY u16 40 0", b"abc\0\0\0\0"),
            // Bits 60 to 63: the eighth byte.
            ("SET u4 60 0", b"abc\0\0\0\0\0"),
        ] {
            assert_eq!(
                bitfield(&store, "k", subcommands),
                (ints(&[Some(0)]), false),
                "{subcommands}"
            );
            assert_eq!(stored(&store, b"k").unwrap().0, now, "{subcommands}");
        }
        // A read past the end grows nothing.
        assert_eq!(bitfield(&store, "k", "GET u8 80"), (ints(&[Some(0)]), true));
        assert_eq!(stored(&store, b"k").unwrap().0.len(), 8);
        // On a missing key: made, of zeros, to the field's last byte.
        assert_eq!(
            bitfield(&store, "z", "INCRBY i64 3 0"),
            (ints(&[Some(0)]), false)
        );
        assert_eq!(stored(&store, b"z").unwrap().0, [0u8; 9]);
        assert_eq!(
            bitfield(&store, "y", "SET u1 0 0"),
            (ints(&[Some(0)]), false)
        );
        assert_eq!(stored(&store, b"y").unwrap().0, b"\0");
    }

    /// A failed operation writes nothing and grows nothing, before or after
    /// a write that succeeds. Redis grows the string to a failed field.
    #[test]
    fn an_overflow_fail_after_a_write_grows_nothing() {
        let store = store::new_store(1);
        put(&store, b"k", b"\x10");
        assert_eq!(
            bitfield(
                &store,
                "k",
                "SET u8 0 20 OVERFLOW FAIL INCRBY u8 0 300 SET u8 80 999"
            ),
            (ints(&[Some(16), None, None]), false)
        );
        assert_eq!(stored(&store, b"k").unwrap().0, b"\x14");
        assert_eq!(
            bitfield(&store, "m", "SET u8 0 1 OVERFLOW FAIL SET u8 100 300"),
            (ints(&[Some(0), None]), false)
        );
        assert_eq!(stored(&store, b"m").unwrap().0, b"\x01");
        // 300 wraps to 44 once the mode is WRAP again.
        assert_eq!(
            bitfield(
                &store,
                "k",
                "OVERFLOW FAIL SET u8 0 300 OVERFLOW WRAP SET u8 0 300"
            ),
            (ints(&[None, Some(20)]), false)
        );
        assert_eq!(stored(&store, b"k").unwrap().0, b"\x2C");
        assert_eq!(
            bitfield(&store, "k", "OVERFLOW FAIL INCRBY u8 0 300 GET u8 0"),
            (ints(&[None, Some(44)]), true)
        );
        assert_eq!(stored(&store, b"k").unwrap().0, b"\x2C");
        // An INCRBY past the end that FAIL may refuse, by what it finds there:
        // refused, it grows nothing; applied, the string grows to its field.
        assert_eq!(
            bitfield(&store, "k", "OVERFLOW FAIL INCRBY i8 800 -129 GET u8 800"),
            (ints(&[None, Some(0)]), true)
        );
        assert_eq!(stored(&store, b"k").unwrap().0, b"\x2C");
        assert_eq!(
            bitfield(
                &store,
                "k",
                "OVERFLOW FAIL INCRBY u8 40 5 INCRBY i8 800 -129"
            ),
            (ints(&[Some(5), None]), false)
        );
        assert_eq!(stored(&store, b"k").unwrap().0, b"\x2C\0\0\0\0\x05");
    }

    /// An operation that cannot be parsed fails the whole command: nothing
    /// before it is written, and a missing key is not made.
    #[test]
    fn a_malformed_operation_after_writes_writes_nothing() {
        let store = store::new_store(1);
        put(&store, b"k", b"abc");
        for tail in [
            "GET",
            "GET u99 0",
            "GET u8 -1",
            "GET u8 #",
            "SET u8 0",
            "INCRBY u8 0",
            "SET u64 0 1",
            "SET u8 4294967296 1",
            "OVERFLOW NOPE",
            "BOGUS",
        ] {
            for key in ["k", "n"] {
                let (reply, _) =
                    bitfield(&store, key, &format!("SET u8 0 99 INCRBY u8 64 1 {tail}"));
                assert!(reply.starts_with(b"-ERR"), "{key} {tail}");
            }
            assert_eq!(stored(&store, b"k").unwrap().0, b"abc", "{tail}");
            assert_eq!(stored(&store, b"n"), None, "{tail}");
        }
    }

    #[test]
    fn bitfield_fields_across_bytes() {
        let store = store::new_store(1);
        put(&store, b"k", b"\0\0\0");
        assert_eq!(
            bitfield(&store, "k", "SET u12 4 2748"),
            (ints(&[Some(0)]), false)
        );
        assert_eq!(stored(&store, b"k").unwrap().0, b"\x0A\xBC\x00");
        // 0xABC as i12; 0xB as u4 and i4; bits 13 to 17 (`10000`) as i5.
        assert_eq!(
            bitfield(&store, "k", "GET i12 4 GET u4 8 GET i4 8 SET i5 13 -1"),
            (ints(&[Some(-1348), Some(11), Some(-5), Some(-16)]), false)
        );
        assert_eq!(stored(&store, b"k").unwrap().0, b"\x0A\xBF\xC0");
        assert_eq!(
            bitfield(&store, "k", "GET i1 0 GET u1 4 SET i1 7 -1"),
            (ints(&[Some(0), Some(1), Some(0)]), false)
        );
        assert_eq!(stored(&store, b"k").unwrap().0, b"\x0B\xBF\xC0");
    }

    #[test]
    fn bitfield_unaligned_64_bit_fields_and_hash_offsets() {
        let store = store::new_store(1);
        // Bits 3 to 66.
        assert_eq!(
            bitfield(
                &store,
                "w",
                "SET i64 3 -2 GET i64 3 GET u63 4 INCRBY i64 3 1"
            ),
            (
                ints(&[Some(0), Some(-2), Some(i64::MAX - 1), Some(-1)]),
                false
            )
        );
        let mut want = vec![0x1F];
        want.extend([0xFF; 7]);
        want.push(0xE0);
        assert_eq!(stored(&store, b"w").unwrap().0, want);
        // `#N`: the Nth field of the type's width.
        assert_eq!(
            bitfield(
                &store,
                "h",
                "SET u8 #2 9 SET i16 #2 -300 GET u8 #5 GET i16 #2 INCRBY i64 #1 1"
            ),
            (
                ints(&[Some(0), Some(0), Some(212), Some(-300), Some(1)]),
                false
            )
        );
        let mut want = b"\0\0\x09\0\xFE\xD4\0\0".to_vec();
        want.extend([0; 7]);
        want.push(1);
        assert_eq!(stored(&store, b"h").unwrap().0, want);
        // u63 at its largest, at the second u63 field: bits 63 to 125.
        assert_eq!(
            bitfield(
                &store,
                "u",
                "SET u63 #1 9223372036854775807 GET u63 63 GET u8 #15"
            ),
            (ints(&[Some(0), Some(i64::MAX), Some(0xFC)]), false)
        );
        assert_eq!(stored(&store, b"u").unwrap().0.len(), 16);
    }

    #[test]
    fn bitfield_keeps_a_time_to_live_and_leaves_other_types() {
        let store = store::new_store(1);
        let later = crate::commands::helpers::now_ms() + 3_600_000;
        for (name, len) in [("inline", 100), ("own", 64 << 10)] {
            let key = name.as_bytes();
            let shard = store::get_shard(&store[0], key);
            drop(
                shard
                    .write()
                    .insert_str_expiring(key, vec![0u8; len], Some(later)),
            );
            for (subcommands, replies, skipped) in [
                ("SET u8 8 65".to_string(), [Some(0)], false),
                (format!("SET u8 {} 1", len * 8), [Some(0)], false),
                ("SET u8 8 65".to_string(), [Some(65)], true),
            ] {
                assert_eq!(
                    bitfield(&store, name, &subcommands),
                    (ints(&replies), skipped),
                    "{name} {subcommands}"
                );
                assert_eq!(shard.read().expires(key), Some(later), "{name}");
            }
            let now = stored(&store, key).unwrap().0;
            assert_eq!((now.len(), now[1], now[len]), (len + 1, 65, 1), "{name}");
        }
        // A key past its time is a missing one.
        let shard = store::get_shard(&store[0], b"past");
        drop(
            shard
                .write()
                .insert_str_expiring(b"past", b"\xFF\xFF", Some(1)),
        );
        assert_eq!(
            bitfield(&store, "past", "GET u8 0"),
            (ints(&[Some(0)]), true)
        );
        assert_eq!(stored(&store, b"past"), None);
        assert_eq!(
            bitfield(&store, "past", "SET u8 8 65"),
            (ints(&[Some(0)]), false)
        );
        assert_eq!(stored(&store, b"past").unwrap().0, b"\x00A");
        assert_eq!(shard.read().expires(b"past"), None);
        // An empty string is a key: read as zeros, grown by a write.
        put(&store, b"e", b"");
        assert_eq!(bitfield(&store, "e", "GET u8 0"), (ints(&[Some(0)]), true));
        assert_eq!(stored(&store, b"e").unwrap().0, b"");
        assert_eq!(
            bitfield(&store, "e", "SET u8 0 0"),
            (ints(&[Some(0)]), false)
        );
        assert_eq!(stored(&store, b"e").unwrap().0, b"\0");
        // Another type: WRONGTYPE, before the arguments are looked at.
        let set = crate::member_set::MemberSet::new();
        let set = crate::types::KiviDBValue::Set(Arc::new(parking_lot::RwLock::new(set)));
        drop(
            store::get_shard(&store[0], b"set")
                .write()
                .insert(b"set", set),
        );
        for line in [
            "BITFIELD set SET u8 0 1",
            "BITFIELD set GET u99 0",
            "BITFIELD set SET u8 0 1 GET",
            "BITFIELD_RO set GET u8 0",
        ] {
            assert!(run(&store, line).0.starts_with(b"-WRONGTYPE"), "{line}");
        }
        let map = store::get_shard(&store[0], b"set").read();
        assert!(matches!(map.get(b"set"), Some(ValueRef::Set(_))));
    }

    /// A signed INCRBY under WRAP answers what the field holds after it: the
    /// sum taken modulo the field's range, however many ranges past either
    /// end it is. Replies from Redis 8.10.3.
    #[test]
    fn a_signed_incrby_wraps_into_the_field() {
        let store = store::new_store(1);
        let cases: [(&str, i64, i64, i64); 17] = [
            ("i8", 0, 1000, -24),
            ("i8", 0, -1000, 24),
            ("i8", -24, 1000, -48),
            ("i8", 100, 300, -112),
            ("i8", 127, 1, -128),
            ("i8", -128, -1, 127),
            ("i8", 5, 256, 5),
            ("i8", 5, -256, 5),
            ("i16", 32767, 1, -32768),
            ("i16", -32768, -1, 32767),
            ("i16", 0, 100000, -31072),
            ("i16", 0, -100000, 31072),
            ("i64", i64::MAX, 1, i64::MIN),
            ("i64", i64::MIN, -1, i64::MAX),
            ("i64", i64::MAX, i64::MAX, -2),
            ("i64", i64::MIN, i64::MIN, 0),
            ("i64", -1, i64::MIN, i64::MAX),
        ];
        for (at, (encoding, start, by, want)) in cases.into_iter().enumerate() {
            // From bit 3: the field across a byte more than its width.
            let ops = format!("SET {encoding} 3 {start} INCRBY {encoding} 3 {by} GET {encoding} 3");
            let (reply, _) = bitfield(&store, &format!("w{at}"), &ops);
            assert_eq!(reply, ints(&[Some(0), Some(want), Some(want)]), "{ops}");
        }
    }

    /// `i64` and `u63` at their limits under each OVERFLOW mode, run in
    /// turn on one key: the replies Redis 8.10.3 gives, and a write only
    /// where a field changes or the string grows.
    #[test]
    fn i64_and_u63_fields_at_their_limits() {
        let store = store::new_store(1);
        let (max, min) = (Some(i64::MAX), Some(i64::MIN));
        let cases: [(&str, &[Option<i64>], bool); 12] = [
            (
                "SET i64 0 9223372036854775807 GET i64 0",
                &[Some(0), max],
                true,
            ),
            ("OVERFLOW SAT INCRBY i64 0 1", &[max], false),
            ("OVERFLOW FAIL INCRBY i64 0 1", &[None], false),
            ("INCRBY i64 0 1", &[min], true),
            ("OVERFLOW SAT INCRBY i64 0 -1", &[min], false),
            (
                "OVERFLOW FAIL INCRBY i64 0 -9223372036854775808",
                &[None],
                false,
            ),
            ("INCRBY i64 0 -1", &[max], true),
            ("INCRBY i64 0 9223372036854775807", &[Some(-2)], true),
            (
                "SET i64 0 -9223372036854775808 INCRBY i64 0 -9223372036854775808",
                &[Some(-2), Some(0)],
                true,
            ),
            // The largest u63 plus one wraps to 0: bits changed and put back.
            (
                "SET u63 0 9223372036854775807 INCRBY u63 0 1",
                &[Some(0), Some(0)],
                true,
            ),
            (
                "OVERFLOW SAT SET u63 0 9223372036854775807 INCRBY u63 0 1",
                &[Some(0), max],
                true,
            ),
            (
                "OVERFLOW FAIL INCRBY u63 0 1 OVERFLOW SAT INCRBY u63 0 -9223372036854775808",
                &[None, Some(0)],
                true,
            ),
        ];
        for (ops, replies, writes) in cases {
            assert_eq!(
                bitfield(&store, "k", ops),
                (ints(replies), !writes),
                "{ops}"
            );
        }
        assert_eq!(stored(&store, b"k").unwrap().0, [0; 8]);
        // An i64 from bit 5: the ninth byte holds its last five bits.
        assert_eq!(
            bitfield(
                &store,
                "n",
                "SET i64 5 -1 GET i64 5 GET u8 0 GET u8 64 GET u8 8"
            ),
            (
                ints(&[Some(0), Some(-1), Some(7), Some(248), Some(255)]),
                false
            )
        );
        assert_eq!(stored(&store, b"n").unwrap().0.len(), 9);
    }

    /// `key`'s length and its last `n` bytes: a long string is not copied
    /// to be looked at.
    fn length_and_last(store: &store::Store, key: &[u8], n: usize) -> (usize, Vec<u8>) {
        let map = store::get_shard(&store[0], key).read();
        match map.get(key) {
            Some(ValueRef::String(b)) => (b.len(), b[b.len() - n..].to_vec()),
            _ => panic!("no string"),
        }
    }

    /// Fields at the last bit offsets there are, below 2^32, grow the
    /// string to their last byte: past 512 MiB, by eight bytes, for an i64
    /// from the last bit, which the room an append leaves does not cap.
    /// Replies and lengths from Redis 8.10.3. Alone, and one string grown
    /// once: about 512 MiB.
    #[test]
    fn fields_at_the_last_bit_offsets_grow_the_string_to_them() {
        alone(
            "commands::bitmap::tests::fields_at_the_last_bit_offsets_grow_the_string_to_them",
            || {
                let store = store::new_store(1);
                let top = 1usize << 29;
                // The u8 sets the last bit below 2^32; the i64 from it, read
                // as its sign, puts 1 there instead: bit 6 of the last byte.
                assert_eq!(
                    bitfield(
                        &store,
                        "a",
                        "SET u8 4294967288 1 SET i64 4294967295 1 GET u8 4294967288 \
                         GET i64 4294967295"
                    ),
                    (ints(&[Some(0), Some(i64::MIN), Some(0), Some(1)]), false)
                );
                assert_eq!(
                    length_and_last(&store, b"a", 9),
                    (top + 8, vec![0, 0, 0, 0, 0, 0, 0, 0, 0x02])
                );
                // The last i64 field (`#N`) ends at 512 MiB.
                assert_eq!(
                    bitfield(
                        &store,
                        "a",
                        "SET i64 #67108863 -1 GET u8 4294967288 GET i64 4294967295"
                    ),
                    (ints(&[Some(0), Some(255), Some(i64::MIN + 1)]), false)
                );
                assert_eq!(
                    length_and_last(&store, b"a", 9),
                    (top + 8, vec![255, 0, 0, 0, 0, 0, 0, 0, 0x02])
                );
            },
        );
    }

    /// At the last bit offsets, operations that write nothing allocate
    /// nothing: reads past the end are zeros, and writes OVERFLOW FAIL
    /// refuses grow nothing. An offset from 2^32 is refused.
    #[test]
    fn fields_at_the_last_bit_offsets_that_write_nothing_allocate_nothing() {
        counting(
            "commands::bitmap::tests::fields_at_the_last_bit_offsets_that_write_nothing_allocate_nothing",
            || {
                let store = store::new_store(1);
                put(&store, b"k", b"abc");
                for (line, want) in [
                    (
                        "BITFIELD k GET i64 4294967295 GET u8 #536870911",
                        &[Some(0), Some(0)][..],
                    ),
                    ("BITFIELD_RO k GET u1 4294967295", &[Some(0)]),
                    (
                        "BITFIELD k OVERFLOW FAIL INCRBY u8 4294967288 256 \
                         GET i64 #67108863 SET u1 4294967295 2",
                        &[None, Some(0), None],
                    ),
                    ("BITFIELD m GET i64 4294967295", &[Some(0)]),
                ] {
                    let (reply, skipped, peak) = counted(&store, line);
                    assert_eq!((reply, skipped), (ints(want), true), "{line}");
                    assert!(peak < SMALL, "{line}: {peak} bytes");
                }
                for line in [
                    "BITFIELD k SET u8 4294967296 1",
                    "BITFIELD k GET i64 #67108864",
                    "BITFIELD k INCRBY u1 4294967296 1",
                ] {
                    let (reply, _, peak) = counted(&store, line);
                    assert_eq!(
                        reply,
                        resp_error("ERR bit offset is not an integer or out of range"),
                        "{line}"
                    );
                    assert!(peak < SMALL, "{line}: {peak} bytes");
                }
                assert_eq!(stored(&store, b"k").unwrap().0, b"abc");
                assert_eq!(stored(&store, b"m"), None);
            },
        );
    }

    /// Two writes 60 MiB apart on a 64 MiB string: where it is, nothing
    /// allocated, when nothing else holds it. Held, as a snapshot or a
    /// `DUMP` holds it, the value is copied once, and the holder keeps the
    /// bytes it had.
    #[test]
    fn two_writes_far_apart_on_a_64_mib_string_copy_it_at_most_once() {
        counting(
            "commands::bitmap::tests::two_writes_far_apart_on_a_64_mib_string_copy_it_at_most_once",
            || {
                let len = 64 << 20;
                let (near, far) = (2 << 20, 62 << 20);
                for holder in [Holder::Nothing, Holder::Entry, Holder::Bytes] {
                    let store = store::new_store(1);
                    let mut value = vec![0xAAu8; len];
                    put(&store, b"k", &value);
                    let (_, at) = stored(&store, b"k").unwrap();
                    let shard = store::get_shard(&store[0], b"k");
                    let entry = (holder == Holder::Entry).then(|| shard.read().get_ref(b"k"));
                    let bytes = (holder == Holder::Bytes).then(|| match shard.read().get(b"k") {
                        Some(ValueRef::String(b)) => b.to_shared(),
                        _ => panic!("no string"),
                    });
                    let line = format!("BITFIELD k SET u8 {} 1 INCRBY u16 {} 1", near * 8, far * 8);
                    let (reply, skipped, peak) = counted(&store, &line);
                    assert_eq!(
                        (reply, skipped),
                        (ints(&[Some(170), Some(0xAAAB)]), false),
                        "{holder:?}"
                    );
                    value[near] = 1;
                    value[far + 1] = 0xAB;
                    let (now, moved_to) = stored(&store, b"k").unwrap();
                    assert!(now == value, "{holder:?}");
                    let held = holder != Holder::Nothing;
                    assert_eq!(moved_to != at, held, "{holder:?}");
                    if held {
                        let most = (len + len / 2) as isize;
                        assert!(peak >= len as isize && peak < most, "{holder:?}: {peak}");
                    } else {
                        assert!(peak < SMALL, "{holder:?}: {peak} bytes");
                    }
                    if let Some(Some(entry)) = &entry {
                        let kept = matches!(entry.value(), ValueRef::String(b) if b.iter().all(|&b| b == 0xAA));
                        assert!(kept, "the entry held");
                    }
                    if let Some(bytes) = &bytes {
                        assert!(bytes.iter().all(|&b| b == 0xAA), "the bytes held");
                    }
                }
            },
        );
    }

    /// `key`'s string's room past its length.
    fn spare(store: &store::Store, key: &[u8]) -> usize {
        match store::get_shard(&store[0], key).read().get(key) {
            Some(ValueRef::String(b)) => b.to_shared().spare(),
            _ => panic!("no string"),
        }
    }

    /// Writes that grow a string use the room an `APPEND` leaves. INCRBYs
    /// OVERFLOW FAIL could refuse grow it as they are applied: the first
    /// moves or copies it once, into a block with that room, and the rest
    /// go into the room. Writes applied whatever the field holds grow it
    /// once, before the first, to the furthest. Held, it is copied once.
    #[test]
    fn growing_writes_use_the_room_an_append_leaves() {
        counting(
            "commands::bitmap::tests::growing_writes_use_the_room_an_append_leaves",
            || {
                use crate::shared_bytes::room_for_append;
                let (len, n) = (1 << 20, 1000);
                for (mode, held) in [
                    ("FAIL", false),
                    ("FAIL", true),
                    ("WRAP", false),
                    ("WRAP", true),
                ] {
                    let store = store::new_store(1);
                    let mut value = vec![0xAAu8; len];
                    put(&store, b"k", &value);
                    let shard = store::get_shard(&store[0], b"k");
                    let held = held.then(|| shard.read().get_ref(b"k"));
                    let ops: String = (0..n)
                        .map(|i| format!(" INCRBY u8 #{} 1", len + i))
                        .collect();
                    let line = format!("BITFIELD k OVERFLOW {mode}{ops}");
                    let (reply, skipped, peak) = counted(&store, &line);
                    let case = format!("{mode}, held: {}", held.is_some());
                    assert_eq!((reply, skipped), (ints(&vec![Some(1); n]), false), "{case}");
                    value.resize(len + n, 1);
                    assert!(stored(&store, b"k").unwrap().0 == value, "{case}");
                    let room = if mode == "FAIL" {
                        room_for_append(len + 1)
                    } else {
                        room_for_append(len + n)
                    };
                    assert_eq!(spare(&store, b"k"), room - (len + n), "{case}");
                    // The new block, or what the old one grew by; and the
                    // operations and their replies.
                    let most = if held.is_some() { room } else { room - len } + n * 160;
                    assert!(peak < most as isize, "{case}: {peak} bytes");
                    if let Some(Some(held)) = &held {
                        let kept = matches!(held.value(), ValueRef::String(b) if b.len() == len);
                        assert!(kept, "{case}: the entry held");
                    }
                }
            },
        );
    }

    /// A held value copied by a field's write, then grown by INCRBYs that
    /// OVERFLOW FAIL could refuse: grown where the copy is, or past an
    /// inline entry's block into new ones, which nothing else held. The
    /// holder keeps the bytes it had. At 64 MiB, one copy and the room an
    /// append leaves.
    #[test]
    fn a_held_value_copied_then_grown_by_a_bitfield() {
        counting(
            "commands::bitmap::tests::a_held_value_copied_then_grown_by_a_bitfield",
            || {
                for len in [1000, 64 << 20] {
                    let store = store::new_store(1);
                    let mut value = vec![0xAAu8; len];
                    put(&store, b"k", &value);
                    let shard = store::get_shard(&store[0], b"k");
                    let held = shard.read().get_ref(b"k").unwrap();
                    let ends = [len, len + 2000, len + 5000];
                    let line = format!(
                        "BITFIELD k SET u8 0 1 OVERFLOW FAIL INCRBY u8 #{} 1 INCRBY u8 #{} 1 \
                         INCRBY u8 #{} 1",
                        ends[0], ends[1], ends[2]
                    );
                    let (reply, skipped, peak) = counted(&store, &line);
                    let want = [Some(170), Some(1), Some(1), Some(1)];
                    assert_eq!((reply, skipped), (ints(&want), false), "{len}");
                    value[0] = 1;
                    value.resize(ends[2] + 1, 0);
                    for end in ends {
                        value[end] = 1;
                    }
                    assert!(stored(&store, b"k").unwrap().0 == value, "{len}");
                    let kept = matches!(held.value(), ValueRef::String(b) if b.len() == len && b[0] == 0xAA);
                    assert!(kept, "{len}: the entry held");
                    if len == 64 << 20 {
                        let room = crate::shared_bytes::room_for_append(len + 1);
                        assert_eq!(spare(&store, b"k"), room - (ends[2] + 1));
                        assert!(
                            peak >= len as isize && peak < (room + len / 8) as isize,
                            "{peak}"
                        );
                    }
                }
            },
        );
    }

    /// Through the dispatcher, a BITFIELD that writes breaks a `WATCH` on
    /// its key and announces `setbit`, once: a change, a change put back,
    /// a zero written past the end. One that finds the bits as asked, or
    /// whose writes OVERFLOW FAIL refuses, does neither. Alone: the event
    /// flags are the process's.
    #[test]
    fn a_bitfield_announces_itself_only_when_it_writes() {
        alone(
            "commands::bitmap::tests::a_bitfield_announces_itself_only_when_it_writes",
            || {
                use crate::keyspace_events as events;
                use crate::watch::WatchNotifier;
                use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
                events::KS_FLAGS.store(events::parse_ks_flags("KE$"), Relaxed);
                let store = store::new_store(1);
                let watches = crate::watch::WatchRegistryInner::new();
                put(&store, b"k", b"\x01\x02");
                for (ops, writes) in [
                    ("SET u8 0 1", false),
                    ("INCRBY u8 8 0 GET u8 0", false),
                    ("OVERFLOW FAIL INCRBY u8 0 255 SET u8 80 256", false),
                    ("SET u8 0 7", true),
                    ("SET u8 0 1 SET u8 0 7", true),
                    ("SET u8 16 0", true),
                ] {
                    let broken = Arc::new(AtomicBool::new(false));
                    watches.watch(0, b"k", Arc::clone(&broken), false);
                    events::hold();
                    let reply = dispatched(&store, &format!("BITFIELD k {ops}"), &watches);
                    let announced = events::release();
                    assert!(reply.starts_with(b"*"), "{ops}: {reply:?}");
                    assert_eq!(broken.load(Relaxed), writes, "{ops}: the watch");
                    let want: Vec<(Vec<u8>, Arc<[u8]>)> = if writes {
                        vec![
                            (b"__keyspace@0__:k".to_vec(), Arc::from(&b"setbit"[..])),
                            (b"__keyevent@0__:setbit".to_vec(), Arc::from(&b"k"[..])),
                        ]
                    } else {
                        vec![]
                    };
                    assert_eq!(announced, want, "{ops}: the events");
                }
                assert_eq!(stored(&store, b"k").unwrap().0, b"\x07\x02\x00");
                // A SETBIT that finds the bit as asked is announced.
                events::hold();
                dispatched(&store, "SETBIT k 0 0", &watches);
                assert_eq!(events::release().len(), 2, "SETBIT");
            },
        );
    }

    /// BITFIELDs run whole, each under the key's lock: increments of two
    /// fields in one command are never seen apart by a reader, and every
    /// increment is counted beside other writers, readers and a holder that
    /// makes each write copy the value now and then.
    #[test]
    fn bitfields_run_whole_beside_readers_writers_and_holders() {
        use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
        let store = store::new_store(1);
        put(&store, b"k", &[0u8; 64 << 10]);
        let (writers, each) = (4, 500);
        let done = AtomicBool::new(false);
        std::thread::scope(|scope| {
            let writing: Vec<_> = (0..writers)
                .map(|_| {
                    scope.spawn(|| {
                        for _ in 0..each {
                            let (reply, _) =
                                bitfield(&store, "k", "INCRBY u32 0 1 INCRBY u32 #2047 1");
                            assert!(reply.starts_with(b"*2"), "{reply:?}");
                        }
                    })
                })
                .collect();
            scope.spawn(|| {
                while !done.load(Relaxed) {
                    let (reply, skipped, _) = run(&store, "BITFIELD_RO k GET u32 0 GET u32 #2047");
                    let got = parse_ints(&reply).unwrap();
                    assert!(skipped && got[0] == got[1], "{got:?}");
                }
            });
            scope.spawn(|| {
                while !done.load(Relaxed) {
                    let held = store::get_shard(&store[0], b"k").read().get_ref(b"k");
                    std::thread::yield_now();
                    drop(held);
                }
            });
            scope.spawn(|| {
                while !done.load(Relaxed) {
                    bitfield(&store, "other", "INCRBY u8 0 1");
                }
            });
            let ended: Vec<_> = writing.into_iter().map(|writer| writer.join()).collect();
            done.store(true, Relaxed);
            for writer in ended {
                writer.unwrap();
            }
        });
        let want = Some((writers * each) as i64);
        assert_eq!(
            bitfield(&store, "k", "GET u32 0 GET u32 #2047").0,
            ints(&[want, want])
        );
        assert_eq!(stored(&store, b"k").unwrap().0.len(), 64 << 10);
    }

    // ── BITFIELD against a copying model ────────────────────────────────────

    /// Randomised commands, the same every run.
    const MODEL_CASES: usize = 20_000;

    /// A reply of integers and nils, as `ints` writes one.
    fn parse_ints(reply: &[u8]) -> Option<Vec<Option<i64>>> {
        let text = std::str::from_utf8(reply).ok()?;
        let mut lines = text.split("\r\n");
        let n: usize = lines.next()?.strip_prefix('*')?.parse().ok()?;
        (0..n)
            .map(|_| match lines.next()? {
                "$-1" => Some(None),
                line => Some(Some(line.strip_prefix(':')?.parse().ok()?)),
            })
            .collect()
    }

    /// What the model run found.
    #[derive(Debug, Default)]
    struct Tally {
        cases: usize,
        errors: usize,
        /// Commands that changed the bytes, or made the key.
        changed: usize,
        /// Commands an operation of which changed bits, or grew the string,
        /// that left the bytes as they were.
        put_back: usize,
        /// Commands whose writes all failed under OVERFLOW FAIL.
        only_failed: usize,
        grown: usize,
    }

    /// A command is a write if any one operation changed bits or grew the
    /// string. The model's run of the commands, beside KiviDB's: replies,
    /// bytes, length, and whether it wrote.
    #[test]
    fn bitfield_matches_a_copying_model() {
        use bitfield_model as model;
        let store = store::new_store(1);
        let mut rng = model::Rng(0x9E37_79B9_7F4A_7C15);
        let mut tally = Tally::default();
        for case in 0..MODEL_CASES {
            let (initial, ops) = model::case(&mut rng);
            let key = format!("m{case}");
            if let Some(bytes) = &initial {
                put(&store, key.as_bytes(), bytes);
            }
            let line = format!("BITFIELD {key} {}", model::text(&ops));
            let (reply, skipped, _) = run(&store, &line);
            let want = model::run(initial.as_deref(), &ops);
            let now = stored(&store, key.as_bytes()).map(|(bytes, _)| bytes);
            assert_eq!(now, want.bytes, "{line}: from {initial:?}");
            tally.cases += 1;
            if want.error {
                assert!(reply.starts_with(b"-ERR"), "{line}");
                tally.errors += 1;
                continue;
            }
            let got = parse_ints(&reply).unwrap_or_else(|| panic!("{line}: {reply:?}"));
            assert_eq!(got, want.replies, "{line}");
            assert!(want.wrote || !want.changed, "{line}");
            assert_eq!(!skipped, want.wrote, "{line}: from {initial:?}");
            tally.changed += usize::from(want.changed);
            tally.put_back += usize::from(want.wrote && !want.changed);
            tally.only_failed += usize::from(want.only_failed);
            tally.grown += usize::from(want.grew);
        }
        eprintln!("{tally:?}");
        // The run reaches what it is for.
        assert!(tally.errors > 100, "{tally:?}");
        assert!(tally.put_back > 100, "{tally:?}");
        assert!(tally.only_failed > 100, "{tally:?}");
        assert!(tally.grown > 1000, "{tally:?}");
    }

    /// `BITFIELD` as intended, on a plain byte vector grown with zeros: an
    /// oracle written apart from the handler, sharing none of its helpers.
    /// Integers are Redis's: a wrapped signed field is answered in range.
    mod bitfield_model {
        #[derive(Clone, Copy, Debug)]
        pub enum Mode {
            Wrap,
            Sat,
            Fail,
        }

        #[derive(Clone, Copy, Debug)]
        pub struct Field {
            pub signed: bool,
            pub bits: u32,
            /// In bits, or in fields of `bits` with `hash`.
            pub offset: u64,
            pub hash: bool,
        }

        #[derive(Clone, Debug)]
        pub enum Op {
            Overflow(Mode),
            Get(Field),
            Set(Field, i64),
            Incrby(Field, i64),
            /// Text the command cannot parse.
            Malformed(&'static str),
        }

        pub struct Outcome {
            pub error: bool,
            pub replies: Vec<Option<i64>>,
            /// The key's bytes after; None for no key.
            pub bytes: Option<Vec<u8>>,
            /// An operation changed bits, or grew the string.
            pub wrote: bool,
            /// The bytes, or whether there is a key, differ from before.
            pub changed: bool,
            pub grew: bool,
            /// It had writes, and every one failed.
            pub only_failed: bool,
        }

        impl Field {
            fn bit(self) -> u64 {
                if self.hash {
                    self.offset * u64::from(self.bits)
                } else {
                    self.offset
                }
            }

            fn range(self) -> (i128, i128) {
                if self.signed {
                    (-(1i128 << (self.bits - 1)), (1i128 << (self.bits - 1)) - 1)
                } else {
                    (0, (1i128 << self.bits) - 1)
                }
            }

            /// The value of `raw`, the field's bits.
            fn value(self, raw: u64) -> i64 {
                let top = raw >> (self.bits - 1) & 1 == 1;
                if self.signed && self.bits < 64 && top {
                    (i128::from(raw) - (1i128 << self.bits)) as i64
                } else {
                    raw as i64
                }
            }

            /// The nine bytes from the field's first, big-endian; zeros past
            /// the end of `bytes`.
            fn window(self, bytes: &[u8]) -> u128 {
                let first = (self.bit() / 8) as usize;
                (first..first + 9).fold(0u128, |acc, at| {
                    acc << 8 | u128::from(bytes.get(at).copied().unwrap_or(0))
                })
            }

            /// Where the field's lowest bit is in [`Field::window`].
            fn shift(self) -> u32 {
                72 - (self.bit() % 8) as u32 - self.bits
            }

            fn mask(self) -> u128 {
                (1u128 << self.bits) - 1
            }

            fn read(self, bytes: &[u8]) -> u64 {
                (self.window(bytes) >> self.shift() & self.mask()) as u64
            }

            /// Bytes from the first to the field's last.
            fn end(self) -> usize {
                (self.bit() + u64::from(self.bits)).div_ceil(8) as usize
            }

            fn write(self, bytes: &mut Vec<u8>, raw: u64) {
                if bytes.len() < self.end() {
                    bytes.resize(self.end(), 0);
                }
                let shift = self.shift();
                let window = self.window(bytes) & !(self.mask() << shift)
                    | (u128::from(raw) & self.mask()) << shift;
                let first = (self.bit() / 8) as usize;
                for (k, byte) in bytes[first..self.end()].iter_mut().enumerate() {
                    *byte = (window >> (64 - 8 * k)) as u8;
                }
            }

            fn text(self) -> String {
                let kind = if self.signed { 'i' } else { 'u' };
                let hash = if self.hash { "#" } else { "" };
                format!("{kind}{} {hash}{}", self.bits, self.offset)
            }
        }

        pub fn run(initial: Option<&[u8]>, ops: &[Op]) -> Outcome {
            if ops.iter().any(|op| matches!(op, Op::Malformed(_))) {
                return Outcome {
                    error: true,
                    replies: Vec::new(),
                    bytes: initial.map(<[u8]>::to_vec),
                    wrote: false,
                    changed: false,
                    grew: false,
                    only_failed: false,
                };
            }
            let mut bytes = initial.map(<[u8]>::to_vec).unwrap_or_default();
            let mut mode = Mode::Wrap;
            let mut replies = Vec::new();
            let (mut wrote, mut grew, mut tried, mut succeeded) = (false, false, false, false);
            for op in ops {
                let (field, target) = match *op {
                    Op::Overflow(to) => {
                        mode = to;
                        continue;
                    }
                    Op::Get(field) => {
                        replies.push(Some(field.value(field.read(&bytes))));
                        continue;
                    }
                    Op::Set(field, to) => (field, i128::from(to)),
                    Op::Incrby(field, by) => {
                        let old = field.value(field.read(&bytes));
                        (field, i128::from(old) + i128::from(by))
                    }
                    Op::Malformed(_) => unreachable!(),
                };
                tried = true;
                let old = field.read(&bytes);
                let (min, max) = field.range();
                let new = match mode {
                    _ if (min..=max).contains(&target) => target,
                    Mode::Wrap => {
                        let width = 1i128 << field.bits;
                        let low = target.rem_euclid(width);
                        if field.signed && low > max {
                            low - width
                        } else {
                            low
                        }
                    }
                    Mode::Sat => target.clamp(min, max),
                    Mode::Fail => {
                        replies.push(None);
                        continue;
                    }
                };
                succeeded = true;
                let raw = (new as u128 & field.mask()) as u64;
                let grows = field.end() > bytes.len();
                wrote |= raw != old || grows;
                grew |= grows;
                field.write(&mut bytes, raw);
                let incrby = matches!(op, Op::Incrby(..));
                replies.push(Some(if incrby { new as i64 } else { field.value(old) }));
            }
            let bytes = (initial.is_some() || succeeded).then_some(bytes);
            Outcome {
                error: false,
                changed: bytes.as_deref() != initial,
                replies,
                bytes,
                wrote,
                grew,
                only_failed: tried && !succeeded,
            }
        }

        pub fn text(ops: &[Op]) -> String {
            let words: Vec<String> = ops
                .iter()
                .map(|op| match op {
                    Op::Overflow(mode) => {
                        format!("OVERFLOW {}", format!("{mode:?}").to_uppercase())
                    }
                    Op::Get(field) => format!("GET {}", field.text()),
                    Op::Set(field, to) => format!("SET {} {to}", field.text()),
                    Op::Incrby(field, by) => format!("INCRBY {} {by}", field.text()),
                    Op::Malformed(text) => text.to_string(),
                })
                .collect();
            words.join(" ")
        }

        /// xorshift64: the same commands on every run.
        pub struct Rng(pub u64);

        impl Rng {
            fn next(&mut self) -> u64 {
                self.0 ^= self.0 << 13;
                self.0 ^= self.0 >> 7;
                self.0 ^= self.0 << 17;
                self.0
            }

            fn below(&mut self, n: u64) -> u64 {
                self.next() % n
            }

            fn pick<T: Copy>(&mut self, of: &[T]) -> T {
                of[self.below(of.len() as u64) as usize]
            }

            fn bytes(&mut self, len: usize) -> Vec<u8> {
                (0..len).map(|_| self.next() as u8).collect()
            }
        }

        const MALFORMED: [&str; 10] = [
            "GET",
            "GET u99 0",
            "GET u8 -1",
            "GET u8 #",
            "SET u8 0",
            "INCRBY u8 0",
            "SET u64 0 1",
            "SET u8 4294967296 1",
            "OVERFLOW NOPE",
            "BOGUS",
        ];

        /// A key's bytes before (None: no key), and a command's operations.
        pub fn case(rng: &mut Rng) -> (Option<Vec<u8>>, Vec<Op>) {
            let initial = match rng.below(100) {
                0..15 => None,
                15..18 => Some(Vec::new()),
                // Past an entry's 4 KiB: a block of its own.
                18..23 => {
                    let len = 4000 + rng.below(20_000) as usize;
                    Some(rng.bytes(len))
                }
                _ => {
                    let len = 1 + rng.below(48) as usize;
                    Some(rng.bytes(len))
                }
            };
            let start = initial.as_deref().unwrap_or(&[]);
            let mut ops = Vec::new();
            let mut fields: Vec<Field> = Vec::new();
            for _ in 0..=rng.below(10) {
                if rng.below(100) < 10 {
                    ops.push(Op::Overflow(rng.pick(&[Mode::Wrap, Mode::Sat, Mode::Fail])));
                    continue;
                }
                let field = field(rng, start.len() as u64, &fields);
                fields.push(field);
                ops.push(match rng.below(100) {
                    0..28 => Op::Get(field),
                    28..64 => Op::Set(field, set_value(rng, field, start)),
                    _ => Op::Incrby(field, increment(rng, field, &ops)),
                });
            }
            match rng.below(100) {
                // A field written before, put back as it was.
                0..10 => {
                    let written: Vec<Field> = ops
                        .iter()
                        .filter_map(|op| match op {
                            Op::Set(field, _) | Op::Incrby(field, _) => Some(*field),
                            _ => None,
                        })
                        .collect();
                    if !written.is_empty() {
                        let field = rng.pick(&written);
                        ops.push(Op::Overflow(Mode::Wrap));
                        ops.push(Op::Set(field, field.value(field.read(start))));
                    }
                }
                // Only writes that overflow, under FAIL.
                10..15 => {
                    ops.retain(|op| matches!(op, Op::Get(_)));
                    ops.push(Op::Overflow(Mode::Fail));
                    for field in fields.iter().take(3) {
                        let (_, max) = field.range();
                        let past = (max + 1).min(i64::MAX.into()) as i64;
                        ops.push(Op::Set(*field, past));
                    }
                }
                15..19 => ops.push(Op::Malformed(rng.pick(&MALFORMED))),
                _ => {}
            }
            (initial, ops)
        }

        /// One seen before a quarter of the time; else anywhere in the
        /// string and a little past it, now and then far past it.
        fn field(rng: &mut Rng, len: u64, seen: &[Field]) -> Field {
            if !seen.is_empty() && rng.below(4) == 0 {
                return rng.pick(seen);
            }
            let signed = rng.below(2) == 0;
            let bits = 1 + rng.below(if signed { 64 } else { 63 }) as u32;
            let span = len * 8 + 48;
            let (offset, hash) = match rng.below(100) {
                0..15 => (rng.below(span / u64::from(bits) + 2), true),
                15..20 => (rng.below(4096), false),
                _ => (rng.below(span), false),
            };
            Field {
                signed,
                bits,
                offset,
                hash,
            }
        }

        fn set_value(rng: &mut Rng, field: Field, start: &[u8]) -> i64 {
            let (min, max) = field.range();
            let edge = |v: i128| v.clamp(i64::MIN.into(), i64::MAX.into()) as i64;
            match rng.below(100) {
                0..35 => rng.below(9) as i64 - 4,
                35..60 => edge(rng.pick(&[min - 1, min, max, max + 1])),
                60..75 => rng.next() as i64 >> rng.below(64),
                // The field as it was before the command: puts it back.
                _ => field.value(field.read(start)),
            }
        }

        fn increment(rng: &mut Rng, field: Field, before: &[Op]) -> i64 {
            let half = 1i128 << (field.bits - 1);
            let edge = |v: i128| v.clamp(i64::MIN.into(), i64::MAX.into()) as i64;
            match rng.below(100) {
                0..45 => rng.below(7) as i64 - 3,
                45..65 => edge(rng.pick(&[half, -half, half - 1])),
                65..80 => rng.next() as i64 >> rng.below(64),
                // Undoes the last increment.
                _ => before
                    .iter()
                    .rev()
                    .find_map(|op| match op {
                        Op::Incrby(_, by) => Some(by.wrapping_neg()),
                        _ => None,
                    })
                    .unwrap_or(0),
            }
        }
    }
}
