//! Whose RDB a file, a full-sync payload or a `DUMP` payload is.
//!
//! Redis and Valkey share the RDB format up to RDB 11 (Redis 7.2, Valkey 7.2
//! and 8). Past it they part: Redis went on to RDB 12 and later, and Valkey
//! 9.0 jumped to RDB 80, with a `VALKEY` header in place of `REDIS` and
//! numbers of its own for what it added. The same type byte can mean two
//! things: 22 is Valkey's hash with field TTLs and a Redis 7.4 pre-release
//! encoding. So the reader is told whose file it reads, by the header (a
//! file, a full sync) or by the version in the trailer (a `DUMP` payload,
//! which has no header).
//!
//! Whatever this does not know is refused by name, never skipped: a load
//! that drops part of the data and reports success is the one thing a
//! migration must not do.

use super::{
    hint, rdb_invalid, rdb_read_millisecond_time, rdb_read_plain_length, rdb_read_string,
    rdb_read_value,
};
use crate::types::KiviDBValue;
use std::io::{self, Read};
use std::sync::Arc;

/// The RDB version Valkey 9.0 and 9.1 write (`RDB_VERSION` in Valkey's
/// `rdb.h`). Valkey 9.2 writes 81, with a type this does not read yet.
pub(super) const VALKEY_RDB_VERSION: u16 = 80;

/// The newest Redis RDB version read: the one Redis 8.10 writes.
pub(super) const REDIS_RDB_VERSION_MAX: u16 = 15;

/// Whose numbering an RDB's type bytes and opcodes follow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Dialect {
    /// Redis, and everything that writes Redis's format (Valkey up to 8).
    Redis,
    /// Valkey 9 (RDB 80).
    Valkey,
}

/// The dialect a file's 9-byte header names: `REDIS` and four digits, or
/// `VALKEY` and three. A Valkey version other than the one this reads is
/// refused, as Valkey refuses one newer than its own.
pub(super) fn from_header(header: &[u8; 9]) -> io::Result<Dialect> {
    if header.starts_with(b"REDIS") {
        return Ok(Dialect::Redis);
    }
    let Some(digits) = header.strip_prefix(b"VALKEY") else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not a Redis or Valkey RDB file",
        ));
    };
    let version: u16 = std::str::from_utf8(digits)
        .ok()
        .and_then(|digits| digits.parse().ok())
        .ok_or_else(|| rdb_invalid("a VALKEY header without an RDB version"))?;
    match version {
        VALKEY_RDB_VERSION => Ok(Dialect::Valkey),
        newer if newer > VALKEY_RDB_VERSION => Err(rdb_invalid(format!(
            "Valkey RDB version {newer} is newer than the one this server reads \
             ({VALKEY_RDB_VERSION}, Valkey 9.0 and 9.1)"
        ))),
        // Valkey itself never writes a VALKEY header below 80.
        older => Err(rdb_invalid(format!(
            "a VALKEY header with RDB version {older}, which Valkey does not write"
        ))),
    }
}

/// Whether `prefix`, the first bytes of a file, is the header of an RDB
/// this reads: Redis's or Valkey's.
pub fn is_rdb_header(prefix: &[u8]) -> bool {
    prefix.starts_with(b"REDIS") || prefix.starts_with(b"VALKEY")
}

/// The dialect of a `DUMP` payload, by the RDB version in its trailer:
/// Redis's up to 8.10's, or Valkey 9's. `None` for any other: Redis
/// releases newer than this knows, and Valkey's newer RDB versions, are
/// refused, as each refuses a payload from a newer version than its own.
pub(super) fn of_dump_version(version: u16) -> Option<Dialect> {
    match version {
        0..=REDIS_RDB_VERSION_MAX => Some(Dialect::Redis),
        VALKEY_RDB_VERSION => Some(Dialect::Valkey),
        _ => None,
    }
}

/// A key's value in the given dialect, its type byte already read. `None`
/// for a key that holds nothing any more (a hash all of whose fields are
/// past their TTL), which Valkey skips too.
pub(super) fn read_value(
    r: &mut impl Read,
    type_id: u8,
    dialect: Dialect,
) -> io::Result<Option<KiviDBValue>> {
    match dialect {
        Dialect::Redis => rdb_read_value(r, type_id),
        Dialect::Valkey => match type_id {
            // RDB_TYPE_HASH_2: a hash whose fields may have TTLs.
            22 => read_valkey_hash_2(r),
            // The types Valkey shares with Redis: up to RDB 11's.
            0..=21 => rdb_read_value(r, type_id),
            other => Err(rdb_invalid(format!(
                "RDB type {other} is not one Valkey 9.0 or 9.1 writes"
            ))),
        },
    }
}

/// Valkey's `RDB_OPCODE_SLOT_IMPORT` (243), read so that the refusal can say
/// what it was: a slot migration into this node, in progress when the file
/// was saved. The keys of those slots it holds are part of what was being
/// moved, not the whole of it, so the file is refused rather than loaded
/// with them, or without them.
pub(super) fn refuse_slot_import(r: &mut impl Read) -> io::Error {
    let what = describe_slot_import(r).unwrap_or_default();
    rdb_invalid(format!(
        "the file holds a Valkey slot migration into this node that was in progress \
         when it was saved{what}: the keys of those slots are not all there. Finish or \
         cancel the migration on the Valkey node, then save the file again"
    ))
}

/// The job's name and its slot ranges, as the refusal names them.
fn describe_slot_import(r: &mut impl Read) -> io::Result<String> {
    let job = String::from_utf8_lossy(&rdb_read_string(r)?).into_owned();
    let count = rdb_read_plain_length(r)?;
    let mut ranges = Vec::new();
    for _ in 0..count.min(16) {
        let start = rdb_read_plain_length(r)?;
        let end = rdb_read_plain_length(r)?;
        ranges.push(format!("{start}-{end}"));
    }
    if count > 16 {
        ranges.push("...".into());
    }
    Ok(format!(" (job {job}, slots {})", ranges.join(" ")))
}

/// When a hash field whose expiry is `at_ms` (Unix milliseconds) expires,
/// on the clock field TTLs are kept by; `None` if that time has passed.
///
/// Worked out from the wall clock to the nanosecond, as `HPEXPIREAT` works
/// it out, so that `HPEXPIRETIME` reads back the very millisecond the file
/// holds. Taken from the clock in whole milliseconds, it read back one
/// later about every other time.
pub(super) fn field_deadline(at_ms: u64) -> io::Result<Option<std::time::Instant>> {
    let out_of_range = || rdb_invalid("hash field expiry out of range");
    let at = std::time::UNIX_EPOCH
        .checked_add(std::time::Duration::from_millis(at_ms))
        .ok_or_else(out_of_range)?;
    let Ok(left) = at.duration_since(std::time::SystemTime::now()) else {
        return Ok(None);
    };
    if left.is_zero() {
        return Ok(None);
    }
    std::time::Instant::now()
        .checked_add(left)
        .map(Some)
        .ok_or_else(out_of_range)
}

/// Valkey 9's `RDB_TYPE_HASH_2`: a count, then each field's name, its value
/// and its expiry, an 8-byte little-endian Unix time in milliseconds, or -1
/// for none. Fields already past their time are dropped, as Valkey drops
/// them on load (and sends the deletes of them to its replicas).
fn read_valkey_hash_2(r: &mut impl Read) -> io::Result<Option<KiviDBValue>> {
    let count = rdb_read_plain_length(r)?;
    let mut fields: Vec<(Vec<u8>, Vec<u8>, Option<u64>)> =
        Vec::with_capacity(hint(usize::try_from(count).unwrap_or(usize::MAX)));
    for _ in 0..count {
        let field = rdb_read_string(r)?;
        let value = rdb_read_string(r)?;
        let expires_at = match rdb_read_millisecond_time(r)? {
            -1 => None,
            at => Some(u64::try_from(at).map_err(|_| rdb_invalid("bad hash field expiry"))?),
        };
        fields.push((field, value, expires_at));
    }

    let mut inner = crate::types::HashInner::new();
    // Every field's name, kept or not: one named twice is a corrupt file.
    let mut names = std::collections::HashSet::with_capacity(fields.len());
    for (field, value, expires_at) in fields {
        let field = String::from_utf8_lossy(&field).into_owned();
        if !names.insert(field.clone()) {
            return Err(rdb_invalid("duplicate hash fields"));
        }
        if let Some(at) = expires_at {
            match field_deadline(at)? {
                Some(deadline) => {
                    inner.expiries.insert(field.clone(), deadline);
                }
                None => continue,
            }
        }
        inner.fields.insert(field, Arc::from(value.as_slice()));
    }
    // Nothing left (or nothing there): no key, as Valkey loads none.
    if inner.fields.is_empty() {
        return Ok(None);
    }
    if inner.expiries.is_empty() {
        // No field has a TTL left: a plain hash, as Valkey writes one
        // that never had any.
        return Ok(Some(KiviDBValue::Hash(Arc::new(parking_lot::RwLock::new(
            inner.fields,
        )))));
    }
    Ok(Some(KiviDBValue::HashEx(Arc::new(
        parking_lot::RwLock::new(inner),
    ))))
}
