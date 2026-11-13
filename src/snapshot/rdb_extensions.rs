//! What Dragonfly and KeyDB add to Redis's RDB format.
//!
//! Both write `REDIS0009` files and payloads, and put more in them than Redis
//! 6.2 did:
//!
//! - Dragonfly writes types of its own past the ones Redis had up to RDB 11
//!   (22, a hash whose fields have TTLs, in Valkey 9's encoding; 30, JSON;
//!   31, its older hash with field TTLs; 32, a set whose members have TTLs;
//!   33 to 37, its Bloom and Cuckoo filters, count-min sketches and top-k),
//!   and an opcode of its own (220) for a key's flags.
//! - KeyDB writes the TTLs that `EXPIREMEMBER` gives a hash field, a set
//!   member or a sorted-set member as two aux fields after the key
//!   (`keydb-subexpire-key`, `keydb-subexpire-when`), and ends its `DUMP`
//!   payloads with one more (`mvcc-tstamp`).
//!
//! A hash field's TTL loads as KiviDB's field TTL. A set or sorted-set
//! member's has nothing to load into, and is refused, with the key: a load
//! that kept the member and dropped its TTL would keep data its source was
//! going to delete, and say nothing.

use super::rdb_dialect::field_deadline;
use super::{hint, rdb_invalid, rdb_read_plain_length, rdb_read_string};
use crate::types::KiviDBValue;
use std::io::{self, Read};
use std::sync::Arc;

/// What a file or payload holds that KiviDB has nowhere to keep (a set
/// member's TTL, a Bloom filter, ...): refused for what it is, the same
/// every time it is read. Other errors (a file cut short, one that does not
/// read) carry no such mark: see [`is_unrepresentable`].
#[derive(Debug)]
struct Unrepresentable(String);

impl std::fmt::Display for Unrepresentable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Unrepresentable {}

/// The refusal of what the source holds and KiviDB can't, worded as
/// `rdb_invalid` words its errors.
pub(super) fn unrepresentable(what: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        Unrepresentable(format!("RDB: {what}")),
    )
}

/// Whether a load failed for what the data holds, rather than because it
/// was cut short or does not read. A replica of a master whose dataset is
/// refused so is refused the same on every full sync, and waits longer
/// before it asks again.
pub fn is_unrepresentable(e: &io::Error) -> bool {
    e.get_ref()
        .is_some_and(|inner| inner.is::<Unrepresentable>())
}

/// KeyDB's `KEYDB.CRON` job (RDB type 64), refused by name.
pub(super) fn refuse_keydb_cron(key: &str) -> io::Error {
    unrepresentable(format!(
        "the key {key} is a KeyDB cron job (KEYDB.CRON, RDB type 64), which KiviDB does not have"
    ))
}

/// Dragonfly keeps a member's TTL in seconds from this time (`kMemberExpiryBase`
/// in Dragonfly's `common.h`, 2023-02-01), and writes it so in its older
/// encodings (types 31 and 32).
const DRAGONFLY_MEMBER_EXPIRY_BASE_S: u64 = 1_675_209_600;

/// Whether a type byte, in a file or payload of this RDB version, is one of
/// Dragonfly's. Redis wrote none of these numbers up to RDB 11, and Dragonfly
/// writes RDB 9.
pub(super) fn is_dragonfly_type(version: u16, type_id: u8) -> bool {
    version <= 11 && matches!(type_id, 22 | 30..=37)
}

/// Whether an opcode, in a file of this RDB version, is one of Dragonfly's
/// (Dragonfly keeps 200 to 240; from 240 on, the loader refuses what it
/// does not know as Redis's opcodes). None of them is a key's type.
pub(super) fn is_dragonfly_opcode(version: u16, opcode: u8) -> bool {
    version <= 11 && (200..240).contains(&opcode)
}

/// A value of one of Dragonfly's types, its type byte read. `key` names it
/// in a refusal, when there is one. `None`: nothing is left of it (every
/// field or member had run out).
pub(super) fn read_dragonfly_value(
    r: &mut impl Read,
    key: Option<&str>,
    type_id: u8,
) -> io::Result<Option<KiviDBValue>> {
    let named = || key.map(|k| format!(" {k}")).unwrap_or_default();
    match type_id {
        // RDB_TYPE_VALKEY_AND_DF_HASH_WITH_EXPIRY_MS: field, value, then the
        // field's expiry as 8 bytes, little-endian Unix milliseconds, or -1.
        // It is Valkey 9's RDB_TYPE_HASH_2, read as Valkey's is.
        22 => super::rdb_dialect::read_value(r, 22, super::rdb_dialect::Dialect::Valkey),
        // RDB_TYPE_JSON: the document, as JSON text.
        30 => {
            let text = rdb_read_string(r)?;
            let json = serde_json::from_slice(&text)
                .map_err(|e| rdb_invalid(format!("Dragonfly JSON{}: {e}", named())))?;
            if crate::commands::json::value_depth(&json) >= crate::commands::json::MAX_DEPTH {
                return Err(rdb_invalid(format!(
                    "Dragonfly JSON{} nested too deeply",
                    named()
                )));
            }
            Ok(Some(KiviDBValue::Json(Arc::new(parking_lot::RwLock::new(
                json,
            )))))
        }
        // RDB_TYPE_HASH_WITH_EXPIRY_DEPRECATED: field, value, then the
        // field's expiry as a number, in Dragonfly's member seconds, or -1.
        31 => {
            let count = rdb_read_plain_length(r)?;
            let mut fields = Vec::with_capacity(hint(usize::try_from(count).unwrap_or(usize::MAX)));
            for _ in 0..count {
                let field = rdb_read_string(r)?;
                let value = rdb_read_string(r)?;
                let expiry = member_seconds_to_unix_ms(&rdb_read_string(r)?)?;
                fields.push((field, value, expiry));
            }
            hash_with_ttls(fields)
        }
        // RDB_TYPE_SET_WITH_EXPIRY (SADDEX): member, then its expiry as a
        // number, in Dragonfly's member seconds, or -1.
        32 => {
            let count = rdb_read_plain_length(r)?;
            let now_ms = super::unix_ms_now();
            let mut set = crate::member_set::MemberSet::new();
            for _ in 0..count {
                let member = rdb_read_string(r)?;
                match member_seconds_to_unix_ms(&rdb_read_string(r)?)? {
                    // Run out: Dragonfly would not load it either.
                    Some(at) if at <= now_ms => continue,
                    Some(_) => {
                        return Err(unrepresentable(format!(
                            "the set{} has members with their own TTLs (Dragonfly's SADDEX): \
                             KiviDB keeps no TTLs for set members, and loading them without \
                             theirs would keep members Dragonfly is going to delete",
                            named()
                        )));
                    }
                    None => {}
                }
                set.insert(Arc::from(member.as_slice()));
            }
            if set.is_empty() {
                return Ok(None);
            }
            Ok(Some(KiviDBValue::Set(Arc::new(parking_lot::RwLock::new(
                set,
            )))))
        }
        33 | 34 => Err(dragonfly_only(named(), "a Bloom filter", type_id)),
        35 => Err(dragonfly_only(named(), "a count-min sketch", type_id)),
        36 => Err(dragonfly_only(named(), "a top-k", type_id)),
        37 => Err(dragonfly_only(named(), "a Cuckoo filter", type_id)),
        other => Err(rdb_invalid(format!("unsupported RDB type {other}"))),
    }
}

fn dragonfly_only(key: String, what: &str, type_id: u8) -> io::Error {
    unrepresentable(format!(
        "the key{key} is {what} (Dragonfly's RDB type {type_id}), which KiviDB does not have"
    ))
}

/// Dragonfly's `RDB_OPCODE_DF_MASK` (220): a key's flags, 4 bytes, then its
/// memcached flags, 4 more, if it has any. A sticky key (`STICK`, never
/// evicted) loads as any other: KiviDB has no such flag, and nothing of the
/// key is lost. Memcached flags are part of what a memcached client reads
/// back, and KiviDB has nowhere to keep them, so they are refused. Returns
/// whether the key was sticky.
pub(super) fn read_dragonfly_mask(r: &mut impl Read) -> io::Result<bool> {
    const STICKY: u32 = 1;
    const MC_FLAGS: u32 = 1 << 1;
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    let mask = u32::from_le_bytes(buf);
    if mask & MC_FLAGS != 0 {
        r.read_exact(&mut buf)?;
        if u32::from_le_bytes(buf) != 0 {
            return Err(unrepresentable(
                "a key has memcached flags (Dragonfly's memcached protocol), which KiviDB \
                 has nowhere to keep",
            ));
        }
    }
    Ok(mask & STICKY != 0)
}

/// The refusal of a Dragonfly opcode this does not read, by what it is.
pub(super) fn refuse_dragonfly_opcode(opcode: u8) -> io::Error {
    unrepresentable(match opcode {
        201 | 202 => format!(
            "the file has Dragonfly's compressed blocks (opcode {opcode}, {}), which KiviDB \
             does not read: save it again from Dragonfly started with --compression_mode=0",
            if opcode == 201 { "zstd" } else { "LZ4" }
        ),
        200 | 210 | 211 => format!(
            "Dragonfly's replication opcode {opcode} in a file: only its own replicas read it"
        ),
        222 | 223 => {
            format!("Dragonfly's search index data (opcode {opcode}) is not supported")
        }
        224 => "Dragonfly's tagged chunks (opcode 224, a value split across the \
                snapshot's streams) are not read"
            .to_string(),
        other => format!("Dragonfly's opcode {other} is not supported"),
    })
}

/// A Dragonfly member TTL as its older encodings write it: an RDB string
/// holding a number of seconds from `DRAGONFLY_MEMBER_EXPIRY_BASE_S`, or -1
/// for none. In Unix milliseconds.
fn member_seconds_to_unix_ms(text: &[u8]) -> io::Result<Option<u64>> {
    let n: i64 = std::str::from_utf8(text)
        .ok()
        .and_then(|t| t.parse().ok())
        .ok_or_else(|| rdb_invalid("bad Dragonfly member expiry"))?;
    match n {
        -1 => Ok(None),
        // Checked: the number is the file's or the payload's word.
        n if n >= 0 => (n as u64)
            .checked_add(DRAGONFLY_MEMBER_EXPIRY_BASE_S)
            .and_then(|s| s.checked_mul(1000))
            .map(Some)
            .ok_or_else(|| rdb_invalid("Dragonfly member expiry out of range")),
        _ => Err(rdb_invalid("bad Dragonfly member expiry")),
    }
}

/// A hash from its fields, each with its expiry in Unix milliseconds or
/// none. Fields already past their time are kept with it (see
/// `rdb_dialect::field_deadline`); `None` for no fields. A field named twice
/// is a corrupt file.
fn hash_with_ttls(fields: Vec<(Vec<u8>, Vec<u8>, Option<u64>)>) -> io::Result<Option<KiviDBValue>> {
    let mut inner = crate::types::HashInner::new();
    let mut names = std::collections::HashSet::with_capacity(fields.len());
    for (field, value, expires_at) in fields {
        let field = String::from_utf8_lossy(&field).into_owned();
        if !names.insert(field.clone()) {
            return Err(rdb_invalid("duplicate hash fields"));
        }
        if let Some(at) = expires_at {
            inner.expiries.insert(field.clone(), field_deadline(at));
        }
        inner.fields.insert(field, Arc::from(value.as_slice()));
    }
    if inner.fields.is_empty() {
        return Ok(None);
    }
    if inner.expiries.is_empty() {
        return Ok(Some(KiviDBValue::Hash(Arc::new(parking_lot::RwLock::new(
            inner.fields,
        )))));
    }
    Ok(Some(KiviDBValue::HashEx(Arc::new(
        parking_lot::RwLock::new(inner),
    ))))
}

/// What became of a key KeyDB gave a member TTL to.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum SubExpiry {
    /// The key is there, the field with its TTL (one that has passed
    /// too, for `field_expiry` to remove), or the field was not there.
    Applied,
    /// The hash has no fields: the key is gone.
    Emptied,
}

/// A TTL KeyDB gave a member of `key` with `EXPIREMEMBER`, read from the
/// `keydb-subexpire-key` and `keydb-subexpire-when` aux fields after the key:
/// the member, and when it expires, in Unix milliseconds. A hash field's
/// becomes the field's TTL; a set or sorted-set member's is refused.
pub(super) fn apply_keydb_subexpiry(
    value: &mut KiviDBValue,
    key: &str,
    member: &[u8],
    when: &[u8],
) -> io::Result<SubExpiry> {
    let at: u64 = std::str::from_utf8(when)
        .ok()
        .and_then(|t| t.trim().parse::<i64>().ok())
        .map(|at| at.max(0) as u64)
        .ok_or_else(|| rdb_invalid(format!("bad KeyDB member expiry for {key}")))?;
    let field = String::from_utf8_lossy(member).into_owned();
    let inner = match value {
        KiviDBValue::Hash(h) => {
            let fields = std::mem::take(&mut *h.write());
            crate::types::HashInner::from_fields(fields)
        }
        KiviDBValue::HashEx(h) => std::mem::take(&mut *h.write()),
        KiviDBValue::Set(_) | KiviDBValue::SortedSet(_) => {
            let kind = if matches!(value, KiviDBValue::Set(_)) {
                "set"
            } else {
                "sorted set"
            };
            return Err(unrepresentable(format!(
                "the {kind} {key} has a member with its own TTL (KeyDB's EXPIREMEMBER): \
                 KiviDB keeps no TTLs for {kind} members, and loading the member without its \
                 TTL would keep it after KeyDB deletes it"
            )));
        }
        other => {
            return Err(unrepresentable(format!(
                "KeyDB member TTL on {key}, a {}",
                other.type_name()
            )));
        }
    };
    let mut inner = inner;
    if inner.fields.contains_key(field.as_str()) {
        inner.expiries.insert(field, field_deadline(at));
    }
    let emptied = inner.fields.is_empty();
    *value = KiviDBValue::HashEx(Arc::new(parking_lot::RwLock::new(inner)));
    Ok(if emptied {
        SubExpiry::Emptied
    } else {
        SubExpiry::Applied
    })
}

/// What follows a value in a KeyDB `DUMP` payload: an aux field,
/// `mvcc-tstamp`, KeyDB's own version of the value for its active
/// replication, which means nothing here. The bytes after it, or `None` if
/// what follows is some other aux field.
pub(super) fn strip_keydb_mvcc(rest: &[u8]) -> Option<&[u8]> {
    let Some((&0xFA, mut after)) = rest.split_first() else {
        return Some(rest);
    };
    let name = rdb_read_string(&mut after).ok()?;
    let stamp = rdb_read_string(&mut after).ok()?;
    let digits = !stamp.is_empty() && stamp.iter().all(u8::is_ascii_digit);
    (name == b"mvcc-tstamp" && digits).then_some(after)
}
