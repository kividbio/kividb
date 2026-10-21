//! `SUNIONCARD` and `SDIFFCARD` (Redis 8.10): the count-only counterparts of
//! `SUNION` and `SDIFF`.
//!
//! Neither materialises its result for the client, and with `LIMIT` both stop
//! as soon as the answer is known to have reached it.

use crate::commands::ctx::Ctx;
use crate::commands::helpers::{lazy_expire_on_read, parse_utf8};
use crate::read_lock;
use crate::resp::write::*;
use crate::store;
use crate::types::KiviDBValue;
use parking_lot::RwLock;
use std::sync::Arc;

type Set = Arc<RwLock<crate::member_set::MemberSet>>;

struct CardArgs<'a> {
    keys: &'a [&'a [u8]],
    /// `0` = unlimited.
    limit: usize,
}

/// Parse `numkeys key [key ...] [APPROX] [LIMIT limit]`. `APPROX` is only
/// recognised when `allow_approx` is set (`SUNIONCARD`).
fn parse_args<'a>(cmd: &'a [&'a [u8]], allow_approx: bool) -> Result<CardArgs<'a>, Vec<u8>> {
    // Not a number is the same error as not a positive one, as in Redis.
    let numkeys: i64 = parse_utf8(cmd[1])
        .and_then(|s| s.parse().ok())
        .filter(|n| *n > 0)
        .ok_or_else(|| resp_error("ERR numkeys should be greater than 0"))?;
    let keys_end = 2 + numkeys as usize;
    if cmd.len() < keys_end {
        return Err(resp_error(
            "ERR Number of keys can't be greater than number of args",
        ));
    }

    let mut limit = 0usize;
    let mut i = keys_end;
    while i < cmd.len() {
        if cmd[i].eq_ignore_ascii_case(b"LIMIT") && i + 1 < cmd.len() {
            let n: i64 = parse_utf8(cmd[i + 1])
                .and_then(|s| s.parse().ok())
                .filter(|n| *n >= 0)
                .ok_or_else(|| resp_error("ERR LIMIT can't be negative"))?;
            limit = n as usize;
            i += 2;
        } else if allow_approx && cmd[i].eq_ignore_ascii_case(b"APPROX") {
            // Accepted. The count is always exact here, which is within any
            // error bound an estimate would have had.
            i += 1;
        } else {
            return Err(resp_error("ERR syntax error"));
        }
    }
    Ok(CardArgs {
        keys: &cmd[2..keys_end],
        limit,
    })
}

/// The sets at `keys`, in order; a missing key is `None` (an empty set).
/// Unlike the older set-algebra commands, a key of another type is an error.
fn sets_at(ctx: &Ctx, keys: &[&[u8]]) -> Result<Vec<Option<Set>>, Vec<u8>> {
    let db = *ctx.selected_db;
    let mut out = Vec::with_capacity(keys.len());
    for raw in keys {
        let Some(key) = parse_utf8(raw) else {
            // Keys are stored as UTF-8, so no such key exists.
            out.push(None);
            continue;
        };
        let gone = lazy_expire_on_read(ctx.store, ctx.expiry_store, db, key);
        let shard = store::get_shard(&ctx.store[db], key);
        match read_lock!(shard).get(key).filter(|_| !gone) {
            None => out.push(None),
            Some(KiviDBValue::Set(s)) => out.push(Some(Arc::clone(s))),
            Some(_) => {
                return Err(resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }
        }
    }
    Ok(out)
}

fn union_card(sets: &[Option<Set>], limit: usize) -> usize {
    let mut sets: Vec<&Set> = sets.iter().flatten().collect();
    // One set (or the same one named repeatedly) needs no accumulator at all.
    sets.dedup_by(|a, b| Arc::ptr_eq(a, b));
    if let [only] = sets.as_slice() {
        let len = only.read().len();
        return if limit > 0 { len.min(limit) } else { len };
    }

    let mut seen: crate::member_set::MemberSet = crate::member_set::MemberSet::new();
    for set in sets {
        for member in set.read().iter() {
            if seen.insert(Arc::clone(member)) && limit > 0 && seen.len() >= limit {
                return limit;
            }
        }
    }
    seen.len()
}

fn diff_card(sets: &[Option<Set>], limit: usize) -> usize {
    let Some(Some(first)) = sets.first() else {
        return 0;
    };
    let mut others: Vec<&Set> = Vec::new();
    for other in sets[1..].iter().flatten() {
        if Arc::ptr_eq(other, first) {
            // A set minus itself.
            return 0;
        }
        // Each lock is taken once; taking a read lock twice on one thread can
        // deadlock behind a queued writer.
        if !others.iter().any(|seen| Arc::ptr_eq(seen, other)) {
            others.push(other);
        }
    }

    let first = first.read();
    let others: Vec<_> = others.iter().map(|s| s.read()).collect();
    let mut count = 0usize;
    for member in first.iter() {
        if others.iter().all(|other| !other.contains(member)) {
            count += 1;
            if limit > 0 && count >= limit {
                return limit;
            }
        }
    }
    count
}

pub(super) fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    let union = match name {
        "SUNIONCARD" => true,
        "SDIFFCARD" => false,
        _ => return None,
    };
    if ctx.cmd.len() < 3 {
        return Some(resp_error(&format!(
            "ERR wrong number of arguments for '{}' command",
            name.to_ascii_lowercase()
        )));
    }
    let args = match parse_args(&ctx.cmd, union) {
        Ok(a) => a,
        Err(e) => return Some(e),
    };
    let sets = match sets_at(ctx, args.keys) {
        Ok(s) => s,
        Err(e) => return Some(e),
    };
    let count = if union {
        union_card(&sets, args.limit)
    } else {
        diff_card(&sets, args.limit)
    };
    crate::stats::record_hit();
    Some(resp_integer(count as i64))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(members: &[&str]) -> Option<Set> {
        Some(Arc::new(RwLock::new(
            members.iter().map(|m| Arc::from(m.as_bytes())).collect(),
        )))
    }

    #[test]
    fn union_counts_distinct_members_and_stops_at_the_limit() {
        let sets = [set(&["a", "b", "c"]), None, set(&["c", "d", "e"])];
        assert_eq!(union_card(&sets, 0), 5);
        assert_eq!(union_card(&sets, 3), 3);
        assert_eq!(union_card(&sets, 9), 5);
        assert_eq!(union_card(&[None, None], 0), 0);

        let one = set(&["a", "b"]);
        assert_eq!(union_card(&[one.clone(), one.clone()], 0), 2);
        assert_eq!(union_card(&[one], 1), 1);
    }

    #[test]
    fn diff_counts_what_only_the_first_set_has() {
        let sets = [set(&["a", "b", "c"]), set(&["c", "d", "e"]), None];
        assert_eq!(diff_card(&sets, 0), 2);
        assert_eq!(diff_card(&sets, 1), 1);
        assert_eq!(diff_card(&[None, set(&["a"])], 0), 0);
        assert_eq!(diff_card(&[set(&["a", "b"])], 0), 2);

        // The same set on both sides, and the same subtrahend twice.
        let s = set(&["a", "b"]);
        assert_eq!(diff_card(&[s.clone(), s.clone()], 0), 0);
        let other = set(&["a"]);
        assert_eq!(diff_card(&[s, other.clone(), other], 0), 1);
    }

    #[test]
    fn argument_parsing() {
        let parse =
            |args: &[&[u8]], approx| parse_args(args, approx).map(|a| (a.keys.len(), a.limit));
        assert_eq!(parse(&[b"X", b"2", b"a", b"b"], false), Ok((2, 0)));
        assert_eq!(
            parse(&[b"X", b"1", b"a", b"limit", b"3"], false),
            Ok((1, 3))
        );
        assert_eq!(
            parse(&[b"X", b"1", b"a", b"APPROX", b"LIMIT", b"3"], true),
            Ok((1, 3))
        );
        assert!(parse(&[b"X", b"1", b"a", b"APPROX"], false).is_err());
        assert!(parse(&[b"X", b"0", b"a"], false).is_err());
        assert!(parse(&[b"X", b"3", b"a"], false).is_err());
        assert!(parse(&[b"X", b"1", b"a", b"LIMIT", b"-1"], false).is_err());
        assert!(parse(&[b"X", b"1", b"a", b"LIMIT"], false).is_err());
    }
}
