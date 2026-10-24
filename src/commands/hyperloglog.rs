use crate::commands::ctx::Ctx;
use crate::commands::helpers::{delete_key_if_expired, expire_on_read, is_expired, parse_utf8};
use crate::resp::write::*;
use crate::store;
use crate::store::Key;
use crate::types::KiviDBValue;
use crate::types::SharedBytes;
use crate::{read_lock, write_lock};
use std::sync::atomic::{AtomicBool, Ordering};

static HLL_WRITE_REDIS: AtomicBool = AtomicBool::new(false);

pub fn get_hll_write_format() -> String {
    if HLL_WRITE_REDIS.load(Ordering::Relaxed) {
        "redis".to_string()
    } else {
        "legacy".to_string()
    }
}

pub fn set_hll_write_format(v: &str) {
    HLL_WRITE_REDIS.store(v == "redis", Ordering::Relaxed);
}

// ── HyperLogLog constants ─────────────────────────────────────────────────────
//
// Redis HLL uses:
//   P = 14          → 2^14 = 16384 registers
//   6 bits/register → max storable value = 63
//   Dense layout    → 16384 * 6 bits = 12288 bytes = 12KB
//
// The header is 16 bytes: "HYLL", the encoding byte, three unused bytes, and
// the cached cardinality. Redis dense HLLs have encoding 0 and are 12304 bytes
// long. Sparse HLLs have encoding 1 and a variable length.
//
// KiviDB before 1.1.2 wrote a dense HLL of the same size with encoding 1 and a
// different hash. 1.1.2 still writes the old (legacy) format by default
// for rolling-upgrade compatibility. Those values are still read, and an old
// value that is updated is written back in its original format, so an old
// value does not lose elements when it is next changed.

const HLL_P: u32 = 14;
const HLL_Q: u32 = 64 - HLL_P; // 50 bits left for the leading-zero rank
const HLL_REGISTERS: usize = 1 << HLL_P; // 16384
const HLL_BITS: usize = 6;
const HLL_MAX_VAL: u8 = (1 << HLL_BITS) - 1; // 63
const HLL_DENSE_SIZE: usize = (HLL_REGISTERS * HLL_BITS).div_ceil(8); // 12288
const HLL_HEADER_SIZE: usize = 16;
const HLL_TOTAL_SIZE: usize = HLL_HEADER_SIZE + HLL_DENSE_SIZE; // 12304
const HLL_DENSE_ENCODING: u8 = 0;
const HLL_SPARSE_ENCODING: u8 = 1;
const HLL_LEGACY_ENCODING: u8 = 1;

// Redis HLL header magic bytes
const HLL_MAGIC: &[u8; 4] = b"HYLL";

// Redis's HLL_ALPHA_INF: 0.5 / ln(2)
const HLL_ALPHA_INF: f64 = 0.721_347_520_444_481_7;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HllFormat {
    /// Redis dense: encoding 0, exactly 12304 bytes.
    Dense,
    /// Redis sparse: encoding 1, anything but the legacy dense size.
    Sparse,
    /// KiviDB before 1.1.2/1.1.2: encoding 1, exactly 12304 bytes, FNV-1a hash.
    LegacyDense,
}

/// What sort of HLL `blob` is, if it is one.
///
/// Encoding 1 and the legacy dense size overlap: a Redis sparse HLL can be
/// exactly 12304 bytes, and KiviDB's pre-1.1.2 dense format is also encoding 1
/// at that size. The old format left no marker to tell them apart, so use the
/// only validation available: a Redis sparse payload must decode completely
/// and cover exactly 16384 registers with canonical sparse encoding. If it does,
/// it is sparse; otherwise it is kept as legacy dense.
///
/// When in doubt, prefer the legacy dense interpretation for 12304-byte
/// encoding-1 blobs to be safe for rolling upgrades from old KiviDB builds.
fn hll_format(blob: &[u8]) -> Option<HllFormat> {
    if blob.len() < HLL_HEADER_SIZE || blob[0..4] != *HLL_MAGIC {
        return None;
    }
    match blob[4] {
        HLL_DENSE_ENCODING if blob.len() == HLL_TOTAL_SIZE => Some(HllFormat::Dense),
        HLL_SPARSE_ENCODING if blob.len() == HLL_TOTAL_SIZE => {
            match sparse_to_dense_canonical(&blob[HLL_HEADER_SIZE..]) {
                Ok(_) => Some(HllFormat::Sparse),
                Err(()) => Some(HllFormat::LegacyDense),
            }
        }
        HLL_SPARSE_ENCODING => match sparse_to_dense_canonical(&blob[HLL_HEADER_SIZE..]) {
            Ok(_) => Some(HllFormat::Sparse),
            Err(()) => None,
        },
        _ => None,
    }
}

fn is_hll(blob: &[u8]) -> bool {
    hll_format(blob).is_some()
}

// ── Hash function ─────────────────────────────────────────────────────────────
//
// Redis uses MurmurHash64A with a fixed seed. The old KiviDB format used
// FNV-1a with an xorshift finalizer, which has to stay for old values.

const MURMUR_M: u64 = 0xc6a4a7935bd1e995;
const MURMUR_R: u32 = 47;
const REDIS_HLL_SEED: u64 = 0xadc83b19;

fn murmurhash64a(data: &[u8], seed: u64) -> u64 {
    let mut hash = seed ^ (data.len() as u64).wrapping_mul(MURMUR_M);
    let (chunks, tail) = data.as_chunks::<8>();
    for chunk in chunks {
        let mut k = u64::from_le_bytes(*chunk);
        k = k.wrapping_mul(MURMUR_M);
        k ^= k >> MURMUR_R;
        k = k.wrapping_mul(MURMUR_M);
        hash ^= k;
        hash = hash.wrapping_mul(MURMUR_M);
    }
    for (i, &byte) in tail.iter().enumerate() {
        hash ^= (byte as u64) << (8 * i);
    }
    if !tail.is_empty() {
        hash = hash.wrapping_mul(MURMUR_M);
    }
    hash ^= hash >> MURMUR_R;
    hash = hash.wrapping_mul(MURMUR_M);
    hash ^= hash >> MURMUR_R;
    hash
}

fn redis_hll_hash(data: &[u8]) -> u64 {
    murmurhash64a(data, REDIS_HLL_SEED)
}

fn legacy_hll_hash(data: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 14695981039346656037;
    const FNV_PRIME: u64 = 1099511628211;
    let mut hash = FNV_OFFSET;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
    hash ^= hash >> 33;
    hash
}

// ── Register packing/unpacking ────────────────────────────────────────────────
//
// Register `i` starts at bit offset `i * 6`, so it can straddle two bytes.
// `registers` is the 12288-byte dense payload, without the header.

fn get_register(registers: &[u8], i: usize) -> u8 {
    let bit_offset = i * HLL_BITS;
    let byte_idx = bit_offset / 8;
    let bit_shift = bit_offset % 8;
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

fn hll_write_redis() -> bool {
    HLL_WRITE_REDIS.load(Ordering::Relaxed)
}

fn make_empty_hll() -> Vec<u8> {
    let mut blob = vec![0u8; HLL_TOTAL_SIZE];
    blob[0..4].copy_from_slice(HLL_MAGIC);
    if hll_write_redis() {
        blob[4] = HLL_DENSE_ENCODING;
    } else {
        blob[4] = HLL_LEGACY_ENCODING;
    }
    blob
}

fn dense_blob(encoding: u8, cached: &[u8], registers: &[u8]) -> Vec<u8> {
    let mut blob = vec![0u8; HLL_TOTAL_SIZE];
    blob[0..4].copy_from_slice(HLL_MAGIC);
    blob[4] = encoding;
    if !cached.is_empty() {
        blob[8..16].copy_from_slice(cached);
    }
    blob[HLL_HEADER_SIZE..].copy_from_slice(registers);
    blob
}

/// The error for a string that is not a HyperLogLog: wrong magic, unknown
/// encoding, or a dense length that is not Redis's.
fn not_hll(_blob: &[u8]) -> Vec<u8> {
    resp_error("WRONGTYPE Key is not a valid HyperLogLog string value.")
}

/// The error Redis returns when a sparse representation does not decode.
fn corrupt_hll() -> Vec<u8> {
    resp_error("INVALIDOBJ Corrupted HLL object detected")
}

fn wrongtype() -> Vec<u8> {
    resp_error("WRONGTYPE Operation against a key holding the wrong kind of value")
}

/// Registers from legacy KiviDB HLLs and Redis-compatible HLLs index different
/// hash spaces. Without the original elements there is no way to convert one
/// into the other, so a union across them is refused rather than approximated.
fn incompatible_hash_formats() -> Vec<u8> {
    resp_error("ERR Cannot merge HyperLogLogs with different hash formats.")
}

// ── Sparse representation ─────────────────────────────────────────────────────
//
// Three opcodes:
//   00xxxxxx             ZERO:  run length x + 1 (1..64)
//   01xxxxxx yyyyyyyy    XZERO: run length (x << 8 | y) + 1 (1..16384)
//   1vvvvvxx             VAL:   value v + 1 (1..32), run length x + 1 (1..4)

#[allow(dead_code)]
fn sparse_to_dense(body: &[u8]) -> Result<Vec<u8>, ()> {
    sparse_to_dense_canonical(body)
}

fn is_zero_run_opcode(opcode: u8) -> bool {
    (opcode & 0xC0) == 0x00 || (opcode & 0xC0) == 0x40
}

fn sparse_to_dense_canonical(body: &[u8]) -> Result<Vec<u8>, ()> {
    let mut registers = vec![0u8; HLL_DENSE_SIZE];
    let mut index = 0usize;
    let mut at = 0usize;
    let mut last_was_zero_run = false;

    while at < body.len() {
        let opcode = body[at];
        let is_zero = is_zero_run_opcode(opcode);
        if is_zero && last_was_zero_run {
            return Err(());
        }
        match opcode & 0xC0 {
            0x00 => {
                let run = ((opcode & 0x3F) as usize) + 1;
                index = index.checked_add(run).ok_or(())?;
                if index > HLL_REGISTERS {
                    return Err(());
                }
                at += 1;
                last_was_zero_run = true;
            }
            0x40 => {
                if at + 1 >= body.len() {
                    return Err(());
                }
                let run = (((opcode & 0x3F) as usize) << 8 | body[at + 1] as usize) + 1;
                index = index.checked_add(run).ok_or(())?;
                if index > HLL_REGISTERS {
                    return Err(());
                }
                at += 2;
                last_was_zero_run = true;
            }
            _ => {
                let run = ((opcode & 0x03) as usize) + 1;
                let value = ((opcode >> 2) & 0x1F) + 1;
                if value == 0 || value > 32 {
                    return Err(());
                }
                if index + run > HLL_REGISTERS {
                    return Err(());
                }
                for register in index..index + run {
                    set_register(&mut registers, register, value);
                }
                index += run;
                at += 1;
                last_was_zero_run = false;
            }
        }
    }

    if index != HLL_REGISTERS {
        return Err(());
    }
    Ok(registers)
}

/// `PFDEBUG DECODE`'s textual form of a sparse body, as Redis builds it.
fn sparse_description(body: &[u8]) -> Result<String, ()> {
    let mut description = String::new();
    let mut at = 0usize;

    while at < body.len() {
        let opcode = body[at];
        match opcode & 0xC0 {
            0x00 => {
                let run = ((opcode & 0x3F) as usize) + 1;
                description.push_str(&format!("z:{run} "));
                at += 1;
            }
            0x40 => {
                if at + 1 >= body.len() {
                    return Err(());
                }
                let run = (((opcode & 0x3F) as usize) << 8 | body[at + 1] as usize) + 1;
                description.push_str(&format!("Z:{run} "));
                at += 2;
            }
            _ => {
                let run = ((opcode & 0x03) as usize) + 1;
                let value = ((opcode >> 2) & 0x1F) + 1;
                description.push_str(&format!("v:{value},{run} "));
                at += 1;
            }
        }
    }

    if description.ends_with(' ') {
        description.pop();
    }
    Ok(description)
}

fn registers_from_blob(blob: &[u8], format: HllFormat) -> Result<Vec<u8>, Vec<u8>> {
    match format {
        HllFormat::Dense | HllFormat::LegacyDense => Ok(blob[HLL_HEADER_SIZE..].to_vec()),
        HllFormat::Sparse => {
            sparse_to_dense_canonical(&blob[HLL_HEADER_SIZE..]).map_err(|_| corrupt_hll())
        }
    }
}

fn merge_registers(destination: &mut [u8], source: &[u8]) {
    for i in 0..HLL_REGISTERS {
        let source_value = get_register(source, i);
        if source_value > get_register(destination, i) {
            set_register(destination, i, source_value);
        }
    }
}

// ── Add an element to HLL registers ──────────────────────────────────────────

fn redis_position(hash: u64) -> (usize, u8) {
    let index = (hash & (HLL_REGISTERS as u64 - 1)) as usize;
    let rank = ((hash >> HLL_P) | (1u64 << HLL_Q)).trailing_zeros() + 1;
    (index, rank as u8)
}

fn legacy_position(hash: u64) -> (usize, u8) {
    let index = (hash >> (64 - HLL_P)) as usize;
    let remaining = hash << HLL_P;
    let rank = (remaining.leading_zeros() + 1).min(HLL_MAX_VAL as u32) as u8;
    (index, rank)
}

fn hll_add(registers: &mut [u8], element: &[u8], legacy: bool) -> bool {
    let (index, rank) = if legacy {
        legacy_position(legacy_hll_hash(element))
    } else {
        redis_position(redis_hll_hash(element))
    };
    if rank > get_register(registers, index) {
        set_register(registers, index, rank);
        true
    } else {
        false
    }
}

// ── Cardinality estimation ────────────────────────────────────────────────────
//
// Ertl's estimator, as Redis 8 uses it:
// "New cardinality estimation algorithms for HyperLogLog sketches",
// Otmar Ertl, arXiv:1702.01284.

fn hll_tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let mut y = 1.0;
    let mut z = 1.0 - x;
    loop {
        x = x.sqrt();
        let z_prime = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if z_prime == z {
            break;
        }
    }
    z / 3.0
}

fn hll_sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let mut y = 1.0;
    let mut z = x;
    loop {
        x *= x;
        let z_prime = z;
        z += x * y;
        y += y;
        if z_prime == z {
            break;
        }
    }
    z
}

fn hll_count(registers: &[u8]) -> u64 {
    let mut histogram = [0u32; 64];
    for i in 0..HLL_REGISTERS {
        histogram[get_register(registers, i) as usize] += 1;
    }

    let m = HLL_REGISTERS as f64;
    let mut z = m * hll_tau((m - histogram[HLL_Q as usize + 1] as f64) / m);
    for j in (1..=HLL_Q as usize).rev() {
        z += histogram[j] as f64;
        z *= 0.5;
    }
    z += m * hll_sigma(histogram[0] as f64 / m);

    (HLL_ALPHA_INF * m * m / z).round() as u64
}

// ── Get blobs ─────────────────────────────────────────────────────────────────

fn delete_expired_key(ctx: &Ctx, key: &str) {
    if is_expired(ctx.expiry_store, *ctx.selected_db, key) {
        delete_key_if_expired(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
    }
}

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
        _ => Err(wrongtype()),
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
        None => Err(resp_error("ERR The specified key does not exist")),
        _ => Err(wrongtype()),
    }
}

fn get_optional_hll(ctx: &Ctx, key: &str) -> Result<Option<Vec<u8>>, Vec<u8>> {
    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, key);
    let map = read_lock!(shard);
    match map.get(key) {
        Some(KiviDBValue::String(b)) => {
            if !is_hll(b) {
                return Err(not_hll(b));
            }
            Ok(Some(b.to_vec()))
        }
        None => Ok(None),
        _ => Err(wrongtype()),
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

    delete_expired_key(ctx, &key);

    // If no elements are given, create the key if it is missing and otherwise
    // just check that it is an HLL.
    if cmd.len() == 2 {
        let db = &ctx.store[*ctx.selected_db];
        let shard = store::get_shard(db, &key);
        let exists = read_lock!(shard).contains_key(key.as_str());
        if exists {
            return match get_hll_blob(ctx, &key) {
                Ok(_) => resp_integer(0),
                Err(e) => e,
            };
        }
        save_hll(ctx, &key, make_empty_hll());
        return resp_integer(1);
    }

    let blob = match get_or_create_hll(ctx, &key) {
        Ok(b) => b,
        Err(e) => return e,
    };
    let format = hll_format(&blob).expect("get_or_create_hll validates the header");
    let legacy = format == HllFormat::LegacyDense;
    let mut registers = match registers_from_blob(&blob, format) {
        Ok(registers) => registers,
        Err(e) => return e,
    };

    let mut modified = false;
    for element in &cmd[2..] {
        if hll_add(&mut registers, element, legacy) {
            modified = true;
        }
    }

    if modified {
        let encoding = if legacy {
            HLL_LEGACY_ENCODING
        } else {
            // Existing Redis/dense format keys keep their format
            HLL_DENSE_ENCODING
        };
        let mut new_blob = dense_blob(encoding, &blob[8..16], &registers);
        invalidate_cache(&mut new_blob);
        save_hll(ctx, &key, new_blob);
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
            Some(KiviDBValue::String(b)) => {
                let Some(format) = hll_format(b) else {
                    return not_hll(b);
                };
                match cached_count(b) {
                    Some(count) => return resp_integer(count as i64),
                    None => {
                        let registers = match registers_from_blob(b.as_ref(), format) {
                            Ok(registers) => registers,
                            Err(e) => return e,
                        };
                        (b.clone(), hll_count(&registers))
                    }
                }
            }
            _ => return wrongtype(),
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

    let mut merged = vec![0u8; HLL_DENSE_SIZE];
    let mut legacy_space: Option<bool> = None;

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

        match map.get(key) {
            Some(KiviDBValue::String(b)) => {
                let Some(format) = hll_format(b) else {
                    return not_hll(b);
                };
                let legacy = format == HllFormat::LegacyDense;
                if matches!(legacy_space, Some(seen) if seen != legacy) {
                    return incompatible_hash_formats();
                }
                legacy_space = Some(legacy);
                let registers = match registers_from_blob(b.as_ref(), format) {
                    Ok(registers) => registers,
                    Err(e) => return e,
                };
                merge_registers(&mut merged, &registers);
            }
            Some(_) => return wrongtype(),
            None => {}
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

    delete_expired_key(ctx, &dest_key);

    let destination = match get_optional_hll(ctx, &dest_key) {
        Ok(blob) => blob,
        Err(e) => return e,
    };

    let mut merged = vec![0u8; HLL_DENSE_SIZE];
    let mut legacy_space: Option<bool> = None;

    if let Some(blob) = &destination {
        let format = hll_format(blob).expect("get_optional_hll validates the header");
        legacy_space = Some(format == HllFormat::LegacyDense);
        let registers = match registers_from_blob(blob, format) {
            Ok(registers) => registers,
            Err(e) => return e,
        };
        merge_registers(&mut merged, &registers);
    }

    for key_bytes in &cmd[2..] {
        let key = match parse_utf8(key_bytes) {
            Some(k) => k,
            None => continue,
        };

        delete_expired_key(ctx, key);

        let source = match get_optional_hll(ctx, key) {
            Ok(Some(blob)) => blob,
            Ok(None) => continue,
            Err(e) => return e,
        };
        let format = hll_format(&source).expect("get_optional_hll validates the header");
        let legacy = format == HllFormat::LegacyDense;
        if matches!(legacy_space, Some(seen) if seen != legacy) {
            return incompatible_hash_formats();
        }
        legacy_space = Some(legacy);
        let registers = match registers_from_blob(&source, format) {
            Ok(registers) => registers,
            Err(e) => return e,
        };
        merge_registers(&mut merged, &registers);
    }

    // A destination that exists keeps its format. A new one becomes legacy
    // when every HLL merged into it is legacy: its registers then came from
    // the old hash, and must keep being read with it. The checks above ensure
    // every source shares that hash space, so no incompatible registers reach
    // the destination.
    let legacy = if let Some(blob) = &destination {
        hll_format(blob).expect("get_optional_hll validates the header") == HllFormat::LegacyDense
    } else {
        legacy_space == Some(true) || !hll_write_redis()
    };
    let encoding = if legacy {
        HLL_LEGACY_ENCODING
    } else {
        HLL_DENSE_ENCODING
    };
    let cached = destination.as_ref().map(|b| &b[8..16]).unwrap_or(&[]);
    let mut merged_blob = dense_blob(encoding, cached, &merged);
    invalidate_cache(&mut merged_blob);
    save_hll(ctx, &dest_key, merged_blob);

    resp_simple("OK")
}

// ── PFSELFTEST ────────────────────────────────────────────────────────────────
// Runs a self-test of our HLL implementation.
fn cmd_pfselftest(_ctx: &mut Ctx) -> Vec<u8> {
    let empty = make_empty_hll();
    let count = hll_count(&empty[HLL_HEADER_SIZE..]);
    if count != 0 {
        return resp_error("ERR HLL self-test failed: empty HLL returned non-zero count");
    }

    let mut blob = make_empty_hll();
    let n: u64 = 10_000;
    for i in 0..n {
        let element = i.to_le_bytes();
        hll_add(&mut blob[HLL_HEADER_SIZE..], &element, false);
    }
    let estimate = hll_count(&blob[HLL_HEADER_SIZE..]);
    let lower = (n as f64 * 0.98) as u64;
    let upper = (n as f64 * 1.02) as u64;
    if estimate < lower || estimate > upper {
        return resp_error(&format!(
            "ERR HLL self-test failed: expected ~{n}, got {estimate}"
        ));
    }

    let mut registers = vec![0u8; HLL_DENSE_SIZE];
    for i in 0..HLL_REGISTERS {
        let val = (i % 64) as u8;
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
//   DECODE key     → sparse layout description
//   ENCODING key   → "dense" or "sparse"
//   TODENSE key    → convert sparse to dense, returning 1 if it did
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
        return resp_error("ERR The specified key does not exist");
    }

    match subcmd.as_str() {
        "GETREG" => {
            let blob = match get_hll_blob(ctx, &key) {
                Ok(b) => b,
                Err(e) => return e,
            };
            let format = hll_format(&blob).expect("get_hll_blob validates the header");
            let registers = match registers_from_blob(&blob, format) {
                Ok(registers) => registers,
                Err(e) => return e,
            };
            if format == HllFormat::Sparse {
                // Redis's GETREG converts the value and keeps the dense form.
                let dense = dense_blob(HLL_DENSE_ENCODING, &blob[8..16], &registers);
                save_hll(ctx, &key, dense);
            }
            let items: Vec<Vec<u8>> = (0..HLL_REGISTERS)
                .map(|i| resp_integer(get_register(&registers, i) as i64))
                .collect();
            resp_array(items)
        }

        "DECODE" => {
            let blob = match get_hll_blob(ctx, &key) {
                Ok(b) => b,
                Err(e) => return e,
            };
            let format = hll_format(&blob).expect("get_hll_blob validates the header");
            if format != HllFormat::Sparse {
                return resp_error("ERR HLL encoding is not sparse");
            }
            match sparse_description(&blob[HLL_HEADER_SIZE..]) {
                Ok(description) => resp_bulk_str(&description),
                Err(()) => corrupt_hll(),
            }
        }

        "ENCODING" => {
            let blob = match get_hll_blob(ctx, &key) {
                Ok(b) => b,
                Err(e) => return e,
            };
            match hll_format(&blob).expect("get_hll_blob validates the header") {
                HllFormat::Sparse => resp_simple("sparse"),
                HllFormat::Dense | HllFormat::LegacyDense => resp_simple("dense"),
            }
        }

        "TODENSE" => {
            let blob = match get_hll_blob(ctx, &key) {
                Ok(b) => b,
                Err(e) => return e,
            };
            let format = hll_format(&blob).expect("get_hll_blob validates the header");
            if format != HllFormat::Sparse {
                return resp_integer(0);
            }
            let registers = match registers_from_blob(&blob, format) {
                Ok(registers) => registers,
                Err(e) => return e,
            };
            let dense = dense_blob(HLL_DENSE_ENCODING, &blob[8..16], &registers);
            save_hll(ctx, &key, dense);
            resp_integer(1)
        }

        "RESET" => {
            let blob = match get_hll_blob(ctx, &key) {
                Ok(b) => b,
                Err(e) => return e,
            };
            let format = hll_format(&blob).expect("get_hll_blob validates the header");
            let encoding = if format == HllFormat::LegacyDense {
                HLL_LEGACY_ENCODING
            } else {
                HLL_DENSE_ENCODING
            };
            let registers = vec![0u8; HLL_DENSE_SIZE];
            let mut reset = dense_blob(encoding, &blob[8..16], &registers);
            invalidate_cache(&mut reset);
            save_hll(ctx, &key, reset);
            resp_simple("OK")
        }

        _ => resp_error(&format!("ERR Unknown PFDEBUG subcommand '{subcmd}'")),
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

#[cfg(test)]
mod tests {
    use super::*;

    // Redis 8.0.5 register assignments, verified against PFDEBUG GETREG.
    const REDIS_HASH_VECTORS: &[(&[u8], u64, usize, u8)] = &[
        (b"", 0xd8dfea6585bc9732, 5938, 2),
        (b"\x61", 0x53d2470a9b43b1a7, 12711, 2),
        (b"\x62", 0xf10cdf96c004fda4, 15780, 1),
        (b"\x63", 0x7585a45533f260f4, 8436, 1),
        (b"\x68\x65\x6c\x6c\x6f", 0x0f656f01eecfe400, 9216, 1),
        (b"\x77\x6f\x72\x6c\x64", 0xcf8f62764b210ab6, 2742, 3),
        (b"\x66\x6f\x6f", 0xe64609b8b0141cb4, 7348, 5),
        (b"\x62\x61\x72", 0x331e54cf23496717, 10007, 1),
        (
            b"\x31\x32\x33\x34\x35\x36\x37",
            0x85563db163632857,
            10327,
            3,
        ),
        (
            b"\x31\x32\x33\x34\x35\x36\x37\x38",
            0x95ebb86389132953,
            10579,
            3,
        ),
        (
            b"\x31\x32\x33\x34\x35\x36\x37\x38\x39",
            0x217532cb09f2a44d,
            9293,
            2,
        ),
        (
            b"\x30\x31\x32\x33\x34\x35\x36\x37\x38\x39\x61\x62\x63\x64\x65\x66",
            0x9f8565428eaa573d,
            5949,
            1,
        ),
        (
            b"\x30\x31\x32\x33\x34\x35\x36\x37\x38\x39\x61\x62\x63\x64\x65\x66\x67",
            0xf6c82f549aceb6e9,
            14057,
            2,
        ),
        (
            b"\x00\x01\x02\x03\x04\x05\x06\x07",
            0x710ab1039e88eed5,
            11989,
            1,
        ),
        (
            b"\xff\xff\xff\xff\xff\xff\xff\xff\xff",
            0x0aa85648c4e6684c,
            10316,
            1,
        ),
        (
            b"\x65\x6c\x65\x6d\x65\x6e\x74\x3a\x30",
            0xcf17d3277ca7e082,
            8322,
            1,
        ),
        (
            b"\x65\x6c\x65\x6d\x65\x6e\x74\x3a\x31",
            0x897671146919c48f,
            1167,
            1,
        ),
        (
            b"\x65\x6c\x65\x6d\x65\x6e\x74\x3a\x39\x39\x39",
            0xb770fb01dc795951,
            6481,
            1,
        ),
    ];

    #[test]
    fn murmur_hash_and_register_position_match_redis() {
        for &(element, hash, index, rank) in REDIS_HASH_VECTORS {
            assert_eq!(redis_hll_hash(element), hash, "{element:?}");
            assert_eq!(redis_position(hash), (index, rank), "{element:?}");
        }
    }

    #[test]
    fn redis_sparse_fixture_decodes_to_its_count() {
        let blob = include_bytes!("../../tests/fixtures/redis-8.0-hll-small-sparse.bin");
        assert_eq!(hll_format(blob), Some(HllFormat::Sparse));
        let registers = sparse_to_dense(&blob[HLL_HEADER_SIZE..]).unwrap();
        assert_eq!(hll_count(&registers), 3);
    }

    #[test]
    fn ambiguous_size_is_resolved_by_sparse_validation() {
        let sparse = include_bytes!("../../tests/fixtures/redis-8.0-hll-sparse-12304.bin");
        assert_eq!(sparse.len(), HLL_TOTAL_SIZE);
        assert_eq!(hll_format(sparse), Some(HllFormat::Sparse));
        let registers = sparse_to_dense(&sparse[HLL_HEADER_SIZE..]).unwrap();
        assert_eq!(hll_count(&registers), 31516);

        let legacy = include_bytes!("../../tests/fixtures/kividb-legacy-hll-abc.bin");
        assert_eq!(legacy.len(), HLL_TOTAL_SIZE);
        assert_eq!(hll_format(legacy), Some(HllFormat::LegacyDense));

        let mut malformed = vec![0xFFu8; HLL_TOTAL_SIZE];
        malformed[..4].copy_from_slice(HLL_MAGIC);
        malformed[4] = HLL_SPARSE_ENCODING;
        malformed[15] = 0x80;
        assert!(sparse_to_dense(&malformed[HLL_HEADER_SIZE..]).is_err());
        assert_eq!(hll_format(&malformed), Some(HllFormat::LegacyDense));
    }

    #[test]
    fn redis_sparse_description_matches_redis() {
        let blob = include_bytes!("../../tests/fixtures/redis-8.0-hll-small-sparse.bin");
        assert_eq!(
            sparse_description(&blob[HLL_HEADER_SIZE..]).unwrap(),
            "Z:8436 v:1,1 Z:4274 v:2,1 Z:3068 v:1,1 Z:603"
        );
    }
}
