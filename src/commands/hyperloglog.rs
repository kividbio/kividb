use crate::commands::ctx::Ctx;
use crate::commands::helpers::{delete_key_if_expired, expire_on_read, is_expired, parse_utf8};
use crate::resp::write::*;
use crate::store;
use crate::store::Key;
use crate::types::KiviDBValue;
use crate::types::SharedBytes;
use crate::{read_lock, write_lock};

// ── HyperLogLog constants ─────────────────────────────────────────────────────
//
// Redis HLL uses:
//   P = 14          → 2^14 = 16384 registers
//   6 bits/register → max storable value = 63
//   Dense layout    → 16384 * 6 bits = 12288 bytes = 12KB
//
// The header is 16 bytes (Redis-compatible magic + metadata),
// then 12288 bytes of packed 6-bit registers.
//
// We use the same layout so PFMERGE across instances works correctly.

const HLL_P: u32 = 14;
const HLL_REGISTERS: usize = 1 << HLL_P; // 16384
const HLL_BITS: usize = 6;
const HLL_MAX_VAL: u8 = (1 << HLL_BITS) - 1; // 63
const HLL_DENSE_SIZE: usize = (HLL_REGISTERS * HLL_BITS).div_ceil(8); // 12288 bytes
const HLL_HEADER_SIZE: usize = 16;
const HLL_TOTAL_SIZE: usize = HLL_HEADER_SIZE + HLL_DENSE_SIZE; // 12304 bytes

// Redis HLL header magic bytes (we match these for wire-compatibility)
const HLL_MAGIC: &[u8; 4] = b"HYLL";

// ── Hash function ─────────────────────────────────────────────────────────────
//
// Rust lesson: we need a good 64-bit hash. We'll use a simple but effective
// MurmurHash3-inspired finalizer (same family Redis uses).
// In your Cargo.toml you already have `ahash`, but we want raw u64 output here.
// We'll write a tiny 64-bit hash inline — this is instructive.

fn hll_hash(data: &[u8]) -> u64 {
    // FNV-1a 64-bit hash — simple, no-dependency, good distribution for HLL
    const FNV_OFFSET: u64 = 14695981039346656037;
    const FNV_PRIME: u64 = 1099511628211;
    let mut hash = FNV_OFFSET;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    // Finalizer: mix the bits aggressively (xorshift)
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
    hash ^= hash >> 33;
    hash
}

// ── Register packing/unpacking ────────────────────────────────────────────────
//
// Rust lesson: 6-bit registers packed into bytes means each register
// can straddle a byte boundary. Register `i` starts at bit offset `i * 6`.
// We need to read/write 6 bits at potentially unaligned byte boundaries.
//
// registers is the 12288-byte dense payload (NOT including the header).

fn get_register(registers: &[u8], i: usize) -> u8 {
    // Bit offset of register i
    let bit_offset = i * HLL_BITS;
    let byte_idx = bit_offset / 8;
    let bit_shift = bit_offset % 8;

    // The 6 bits might span two bytes
    let word = if byte_idx + 1 < registers.len() {
        (registers[byte_idx] as u16) | ((registers[byte_idx + 1] as u16) << 8)
    } else {
        registers[byte_idx] as u16
    };

    ((word >> bit_shift) & 0x3F) as u8
}

fn set_register(registers: &mut [u8], i: usize, val: u8) {
    let bit_offset = i * HLL_BITS;
    let byte_idx = bit_offset / 8;
    let bit_shift = bit_offset % 8;

    // Clear the 6 bits, then OR in the new value
    let mask = !(0x3Fu16 << bit_shift);
    let word = if byte_idx + 1 < registers.len() {
        (registers[byte_idx] as u16) | ((registers[byte_idx + 1] as u16) << 8)
    } else {
        registers[byte_idx] as u16
    };

    let new_word = (word & mask) | ((val as u16 & 0x3F) << bit_shift);
    registers[byte_idx] = (new_word & 0xFF) as u8;
    if byte_idx + 1 < registers.len() {
        registers[byte_idx + 1] = (new_word >> 8) as u8;
    }
}

// ── HLL blob construction ─────────────────────────────────────────────────────
//
// We store HLL as KiviDBValue::String with a 16-byte Redis-compatible header:
//   bytes 0-3:  magic "HYLL"
//   byte  4:    encoding (1 = dense)
//   bytes 5-7:  unused (zero)
//   bytes 8-15: cached cardinality, as Redis keeps it: little-endian u64,
//               the top bit of byte 15 set when it is stale (#28)
// Then 12288 bytes of packed 6-bit registers.
//
// The registers are not Redis's: the hash and the way a register and a run
// are taken from it differ, there is no sparse encoding, and byte 4 is 1
// where Redis's dense encoding is 0.

/// Mark the cached cardinality stale, as Redis's `HLL_INVALIDATE_CACHE`.
fn invalidate_cache(blob: &mut [u8]) {
    blob[15] |= 0x80;
}

/// The cached cardinality, if it is not stale.
///
/// Zero is taken as not known: a HyperLogLog saved before 1.1.1 has zeros
/// there, whatever it holds, since nothing filled the cache in then. Only an
/// empty one is counted again for it, which costs nothing that matters.
fn cached_count(blob: &[u8]) -> Option<u64> {
    let count = u64::from_le_bytes(blob[8..16].try_into().unwrap());
    (count != 0 && blob[15] & 0x80 == 0).then_some(count)
}

fn make_empty_hll() -> Vec<u8> {
    let mut blob = vec![0u8; HLL_TOTAL_SIZE];
    blob[0..4].copy_from_slice(HLL_MAGIC);
    blob[4] = 1; // dense encoding
    blob
}

fn is_hll(blob: &[u8]) -> bool {
    blob.len() == HLL_TOTAL_SIZE && has_hll_header(blob)
}

/// The magic, and the one encoding there is.
fn has_hll_header(blob: &[u8]) -> bool {
    blob.len() > 4 && blob[0..4] == *HLL_MAGIC && blob[4] == 1
}

/// The error for a string that is not a HyperLogLog: one that never was,
/// or — the header is right and the registers are not all there, or there is
/// something after them — one that has been damaged.
fn not_hll(blob: &[u8]) -> Vec<u8> {
    if has_hll_header(blob) {
        resp_error("INVALIDOBJ Corrupted HLL object detected")
    } else {
        resp_error("WRONGTYPE Key is not a valid HyperLogLog string value")
    }
}

// ── Add an element to HLL registers ──────────────────────────────────────────
//
// Returns true if any register was updated (cardinality estimate changed).
//
// Algorithm:
//   1. Hash the element to a 64-bit value
//   2. Use the top P bits (14 bits) as the register index
//   3. Count the position of the first 1-bit in the remaining 50 bits
//   4. Update register[index] = max(register[index], first_one_position)

fn hll_add(registers: &mut [u8], element: &[u8]) -> bool {
    let hash = hll_hash(element);

    // Top 14 bits → register index (0..16383)
    let reg_idx = (hash >> (64 - HLL_P)) as usize;

    // Remaining 50 bits: count leading zeros + 1 = position of first 1
    // We mask away the top 14 bits, then count leading zeros in the 50-bit value.
    let remaining = hash << HLL_P;
    // leading_zeros counts from the top of u64, so we get zeros in the 50 bits
    // then add 1 (we want position-of-first-1, 1-indexed)
    let first_one = (remaining.leading_zeros() + 1).min(HLL_MAX_VAL as u32) as u8;

    let old = get_register(registers, reg_idx);
    if first_one > old {
        set_register(registers, reg_idx, first_one);
        true
    } else {
        false
    }
}

// ── Cardinality estimation (HarmonicMean formula) ─────────────────────────────
//
// Rust lesson: this is pure math — a great place to learn Rust's f64 iterator methods.
//
// The HyperLogLog estimator:
//   E = α_m * m^2 * (Σ 2^(-register[i]))^(-1)
//
// Where:
//   m = number of registers (16384)
//   α_m = correction constant ≈ 0.7213 / (1 + 1.079/m)

fn hll_count(registers: &[u8]) -> u64 {
    let m = HLL_REGISTERS as f64;

    // α_m correction factor
    let alpha = 0.7213 / (1.0 + 1.079 / m);

    // Harmonic mean: sum of 2^(-register[i]) for all i
    // Rust: (0..N).map(...).sum() is idiomatic and gets auto-vectorized
    let sum: f64 = (0..HLL_REGISTERS)
        .map(|i| {
            let val = get_register(registers, i) as i32;
            // 2^(-val) = 1.0 / 2^val
            // Using f64::powi for integer exponents is faster than powf
            2.0f64.powi(-val)
        })
        .sum();

    let raw_estimate = alpha * m * m / sum;

    // Small range correction (linear counting)
    if raw_estimate <= 2.5 * m {
        let zeros = (0..HLL_REGISTERS)
            .filter(|&i| get_register(registers, i) == 0)
            .count() as f64;
        if zeros > 0.0 {
            // Linear counting: m * ln(m / zeros)
            return (m * (m / zeros).ln()).round() as u64;
        }
    }

    // Large range correction (not needed for 64-bit hash — essentially never triggers)
    // For 64-bit hashes the large range correction threshold is 2^64/30 ≈ 6.1e17
    // which is astronomically large, so we skip it.

    raw_estimate.round() as u64
}

// ── Get or create HLL blob ────────────────────────────────────────────────────

fn get_or_create_hll(ctx: &mut Ctx, key: &str) -> Result<Vec<u8>, Vec<u8>> {
    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, key);
    let map = read_lock!(shard);
    match map.get(key) {
        Some(KiviDBValue::String(b)) => {
            if !is_hll(b) {
                return Err(not_hll(b));
            }
            Ok(b.to_vec())
        }
        None => Ok(make_empty_hll()),
        _ => Err(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
    }
}

fn save_hll(ctx: &mut Ctx, key: &str, blob: Vec<u8>) {
    let key_owned = key.to_string();
    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, key);
    let mut map = write_lock!(shard);
    map.insert(
        Key::from(key_owned),
        KiviDBValue::String(SharedBytes::from(blob.as_slice())),
    );
}

// Like get_or_create_hll but returns an error if the key doesn't exist,
// rather than creating a new empty blob. Used by PFDEBUG which requires
// the key to already exist.
fn get_hll_blob(ctx: &Ctx, key: &str) -> Result<Vec<u8>, Vec<u8>> {
    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, key);
    let map = read_lock!(shard);
    match map.get(key) {
        Some(KiviDBValue::String(b)) => {
            if !is_hll(b) {
                return Err(not_hll(b));
            }
            Ok(b.to_vec())
        }
        None => Err(resp_error("ERR no such key")),
        _ => Err(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
    }
}

// ── PFADD ─────────────────────────────────────────────────────────────────────
// PFADD key [element [element ...]]
// Returns 1 if the HLL was modified (new elements added), 0 otherwise.
fn cmd_pfadd(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = ctx.cmd.clone();
    if cmd.len() < 2 {
        return resp_error("ERR wrong number of arguments for 'pfadd' command");
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };

    // If no elements given, just ensure the key exists (return 0 if it does, 1 if created)
    if cmd.len() == 2 {
        let db = &ctx.store[*ctx.selected_db];
        let shard = store::get_shard(db, &key);
        let map = read_lock!(shard);
        if map.contains_key(key.as_str()) {
            return resp_integer(0);
        }
        drop(map);
        save_hll(ctx, &key, make_empty_hll());
        return resp_integer(1);
    }

    let mut blob = match get_or_create_hll(ctx, &key) {
        Ok(b) => b,
        Err(e) => return e,
    };

    // registers start at HLL_HEADER_SIZE
    let mut modified = false;
    for element in &cmd[2..] {
        if hll_add(&mut blob[HLL_HEADER_SIZE..], element) {
            modified = true;
        }
    }

    if modified {
        invalidate_cache(&mut blob);
        save_hll(ctx, &key, blob);
        resp_integer(1)
    } else {
        resp_integer(0)
    }
}

// ── PFCOUNT ───────────────────────────────────────────────────────────────────
// PFCOUNT key [key ...]
// Single key: return cardinality estimate.
// Multiple keys: merge all HLLs into a temporary one, return its cardinality.
fn cmd_pfcount(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return resp_error("ERR wrong number of arguments for 'pfcount' command");
    }

    if cmd.len() == 2 {
        // Fast path: single key
        let key = match parse_utf8(cmd[1]) {
            Some(k) => k,
            None => return resp_error("ERR key must be valid UTF-8"),
        };

        if is_expired(ctx.expiry_store, *ctx.selected_db, key) {
            expire_on_read(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
            return resp_integer(0);
        }

        let db = &ctx.store[*ctx.selected_db];
        let shard = store::get_shard(db, key);
        let map = read_lock!(shard);

        let counted = match map.get(key) {
            None => return resp_integer(0),
            Some(KiviDBValue::String(b)) if is_hll(b) => match cached_count(b) {
                Some(count) => return resp_integer(count as i64),
                None => (b.clone(), hll_count(&b[HLL_HEADER_SIZE..])),
            },
            Some(KiviDBValue::String(b)) => return not_hll(b),
            _ => {
                return resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                );
            }
        };
        drop(map);
        // Keep the count in the value, as Redis does, for the next PFCOUNT
        // and for whoever reads the bytes: unless the value has changed
        // since it was counted. The bytes of a string are not changed in
        // place, so a counted copy replaces it. Not a write: replicas keep
        // their own cache, and the dataset digest leaves it out.
        let (counted_bytes, count) = counted;
        let mut map = write_lock!(shard);
        if let Some(KiviDBValue::String(now)) = map.get_mut(key)
            && SharedBytes::ptr_eq(now, &counted_bytes)
        {
            let mut blob = counted_bytes.to_vec();
            blob[8..16].copy_from_slice(&count.to_le_bytes());
            *now = SharedBytes::from(blob.as_slice());
        }
        return resp_integer(count as i64);
    }

    // Multi-key: merge all into a temp HLL
    // Rust lesson: we build a temporary Vec<u8> without persisting it — no store write needed
    let mut merged = vec![0u8; HLL_DENSE_SIZE];

    for key_bytes in &cmd[1..] {
        let key = match parse_utf8(key_bytes) {
            Some(k) => k,
            None => continue,
        };

        if is_expired(ctx.expiry_store, *ctx.selected_db, key) {
            expire_on_read(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
            continue;
        }

        let db = &ctx.store[*ctx.selected_db];
        let shard = store::get_shard(db, key);
        let map = read_lock!(shard);

        if let Some(KiviDBValue::String(b)) = map.get(key)
            && is_hll(b)
        {
            // Merge: take element-wise max of each register
            let src = &b[HLL_HEADER_SIZE..];
            for i in 0..HLL_REGISTERS {
                let src_val = get_register(src, i);
                let dst_val = get_register(&merged, i);
                if src_val > dst_val {
                    set_register(&mut merged, i, src_val);
                }
            }
        }
    }

    resp_integer(hll_count(&merged) as i64)
}

// ── PFMERGE ───────────────────────────────────────────────────────────────────
// PFMERGE destkey [sourcekey [sourcekey ...]]
// Merges all source HLLs into dest by taking element-wise max of registers.
fn cmd_pfmerge(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = ctx.cmd.clone();
    if cmd.len() < 2 {
        return resp_error("ERR wrong number of arguments for 'pfmerge' command");
    }
    let dest_key = match parse_utf8(cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR dest key must be valid UTF-8"),
    };

    // Start with an empty HLL (or the existing dest if it exists)
    let mut merged_blob = match get_or_create_hll(ctx, &dest_key) {
        Ok(b) => b,
        Err(e) => return e,
    };

    // Merge each source into merged_blob
    for key_bytes in &cmd[2..] {
        let key = match parse_utf8(key_bytes) {
            Some(k) => k,
            None => continue,
        };

        let db = &ctx.store[*ctx.selected_db];
        let shard = store::get_shard(db, key);
        let map = read_lock!(shard);

        if let Some(KiviDBValue::String(b)) = map.get(key) {
            if !is_hll(b) {
                return not_hll(b);
            }
            let src = &b[HLL_HEADER_SIZE..];
            let dst = &mut merged_blob[HLL_HEADER_SIZE..];
            for i in 0..HLL_REGISTERS {
                let src_val = get_register(src, i);
                let dst_val = get_register(dst, i);
                if src_val > dst_val {
                    set_register(dst, i, src_val);
                }
            }
        } else if map.get(key).is_some() {
            return resp_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        // Missing keys contribute empty HLL (no-op)
    }

    invalidate_cache(&mut merged_blob);
    save_hll(ctx, &dest_key, merged_blob);

    resp_simple("OK")
}

// ── PFSELFTEST ────────────────────────────────────────────────────────────────
// Runs a self-test of our HLL implementation.
// Redis's version tests encoding/decoding and estimation accuracy.
// We test the same: round-trip a known set of elements and verify the
// cardinality estimate is within the expected 2% error bound.
fn cmd_pfselftest(_ctx: &mut Ctx) -> Vec<u8> {
    // Test 1: empty HLL should estimate 0
    let empty = make_empty_hll();
    let count = hll_count(&empty[HLL_HEADER_SIZE..]);
    if count != 0 {
        return resp_error("ERR HLL self-test failed: empty HLL returned non-zero count");
    }

    // Test 2: add a known number of unique elements and check estimate is sane
    let mut blob = make_empty_hll();
    let n: u64 = 10_000;
    for i in 0..n {
        let element = i.to_le_bytes();
        hll_add(&mut blob[HLL_HEADER_SIZE..], &element);
    }
    let estimate = hll_count(&blob[HLL_HEADER_SIZE..]);
    // Allow 2% error — HLL guarantees ~0.81% std error at 16384 registers
    let lower = (n as f64 * 0.98) as u64;
    let upper = (n as f64 * 1.02) as u64;
    if estimate < lower || estimate > upper {
        return resp_error(&format!(
            "ERR HLL self-test failed: expected ~{n}, got {estimate}"
        ));
    }

    // Test 3: register encode/decode round-trip
    let mut registers = vec![0u8; HLL_DENSE_SIZE];
    for i in 0..HLL_REGISTERS {
        let val = (i % 64) as u8; // 0..63, fits in 6 bits
        set_register(&mut registers, i, val);
    }
    for i in 0..HLL_REGISTERS {
        let expected = (i % 64) as u8;
        let got = get_register(&registers, i);
        if got != expected {
            return resp_error(&format!(
                "ERR HLL self-test failed: register {i} expected {expected} got {got}"
            ));
        }
    }

    resp_simple("OK")
}

// ── PFDEBUG ───────────────────────────────────────────────────────────────────
// PFDEBUG subcommand key
// Subcommands (matching Redis's own):
//   GETREG key     → array of all 16384 register values as integers
//   DECODE key     → bulk string describing the internal layout
//   ENCODING key   → "dense" (we only use dense)
//   TODENSE key    → no-op for us (already dense), returns OK
//   RESET key      → zero all registers, invalidate cached cardinality
fn cmd_pfdebug(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = ctx.cmd.clone();
    if cmd.len() != 3 {
        return resp_error("ERR wrong number of arguments for 'pfdebug' command");
    }

    let subcmd = std::str::from_utf8(cmd[1])
        .unwrap_or("")
        .to_ascii_uppercase();
    let key = match parse_utf8(cmd[2]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };

    if is_expired(ctx.expiry_store, *ctx.selected_db, &key) {
        delete_key_if_expired(ctx.store, ctx.expiry_store, *ctx.selected_db, &key);
        return resp_error("ERR no such key");
    }

    match subcmd.as_str() {
        // GETREG: dump all 16384 register values as an integer array
        // Useful for inspecting which registers have been set and to what depth
        "GETREG" => {
            let blob = match get_hll_blob(ctx, &key) {
                Ok(b) => b,
                Err(e) => return e,
            };
            let registers = &blob[HLL_HEADER_SIZE..];
            let items: Vec<Vec<u8>> = (0..HLL_REGISTERS)
                .map(|i| resp_integer(get_register(registers, i) as i64))
                .collect();
            resp_array(items)
        }

        // DECODE: human-readable description of the HLL internal state
        "DECODE" => {
            let blob = match get_hll_blob(ctx, &key) {
                Ok(b) => b,
                Err(e) => return e,
            };
            let registers = &blob[HLL_HEADER_SIZE..];
            let estimate = hll_count(registers);
            let nonzero = (0..HLL_REGISTERS)
                .filter(|&i| get_register(registers, i) > 0)
                .count();
            let max_reg = (0..HLL_REGISTERS)
                .map(|i| get_register(registers, i))
                .max()
                .unwrap_or(0);
            let desc = format!(
                "encoding:dense registers:{HLL_REGISTERS} nonzero:{nonzero} \
                 max_register:{max_reg} estimate:{estimate}"
            );
            resp_bulk_str(&desc)
        }

        // ENCODING: always "dense" for us
        "ENCODING" => resp_bulk_str("dense"),

        // TODENSE: we only have dense, so this is always a no-op
        "TODENSE" => resp_simple("OK"),

        // RESET: zero all registers and invalidate the cached cardinality
        "RESET" => {
            let mut blob = match get_hll_blob(ctx, &key) {
                Ok(b) => b,
                Err(e) => return e,
            };
            // Zero all register bytes
            blob[HLL_HEADER_SIZE..].fill(0);
            invalidate_cache(&mut blob);
            save_hll(ctx, &key, blob);
            resp_simple("OK")
        }

        _ => resp_error(&format!("ERR unknown PFDEBUG subcommand '{subcmd}'")),
    }
}

// ── Public handle ─────────────────────────────────────────────────────────────
pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    match name {
        "PFADD" => Some(cmd_pfadd(ctx)),
        "PFCOUNT" => Some(cmd_pfcount(ctx)),
        "PFMERGE" => Some(cmd_pfmerge(ctx)),
        "PFDEBUG" => Some(cmd_pfdebug(ctx)),
        "PFSELFTEST" => Some(cmd_pfselftest(ctx)),
        _ => None,
    }
}
