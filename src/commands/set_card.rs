//! `SUNIONCARD` and `SDIFFCARD` (Redis 8.10): the count-only counterparts of
//! `SUNION` and `SDIFF`.
//!
//! Neither materialises its result for the client, and with `LIMIT` both stop
//! as soon as the answer is known to have reached it.
//!
//! The walks over the stored sets (`inter_each`, `diff_each`) are shared with
//! `SINTER`, `SINTERSTORE`, `SINTERCARD`, `SDIFF` and `SDIFFSTORE` in
//! `sets.rs`. They read the sets where they are, under their read locks, and
//! copy nothing: an intersection costs the smallest set's size, whatever the
//! size of the others (#759).

use crate::commands::ctx::Ctx;
use crate::commands::helpers::{lazy_expire_on_read, parse_utf8};
use crate::member_set::MemberSet;
use crate::read_lock;
use crate::resp::write::*;
use crate::store;
use crate::store::ValueRef;
use parking_lot::{RwLock, RwLockReadGuard};
use std::sync::Arc;

pub(super) type Set = Arc<RwLock<MemberSet>>;

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

/// The sets at `keys`, in order; a missing (or expired) key is `None` (an
/// empty set). A key of another type is a WRONGTYPE error, wherever it is in
/// the list and whatever the other keys hold, as in Redis.
///
/// Only the handles are taken, each under its shard's lock in turn; nothing
/// is copied. A write of several keys takes the command gate alone (#729), so
/// none can land between two of these lookups.
pub(super) fn sets_at(ctx: &Ctx, keys: &[&[u8]]) -> Result<Vec<Option<Set>>, Vec<u8>> {
    let db = *ctx.selected_db;
    let mut out = Vec::with_capacity(keys.len());
    for raw in keys {
        let key: &[u8] = raw;
        let gone = lazy_expire_on_read(ctx.store, db, key);
        let shard = store::get_shard(&ctx.store[db], key);
        match read_lock!(shard).get(key).filter(|_| !gone) {
            None => out.push(None),
            Some(ValueRef::Set(s)) => out.push(Some(Arc::clone(s))),
            Some(_) => {
                return Err(resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }
        }
    }
    Ok(out)
}

/// Each set once: a key named twice is one set, and its lock must be taken
/// once (a second read lock on one thread can wait behind a queued writer).
pub(super) fn distinct<'a>(sets: impl Iterator<Item = &'a Set>) -> Vec<&'a Set> {
    let mut out: Vec<&Set> = Vec::new();
    for set in sets {
        if !out.iter().any(|seen| Arc::ptr_eq(seen, set)) {
            out.push(set);
        }
    }
    out
}

/// Read guards on `sets` (distinct sets), in the order given.
///
/// The locks are taken in address order, not argument order. A reader waits
/// behind a queued writer, so two commands holding one set each and each
/// waiting for the other's (`SINTER a b` and `SINTER b a`, with an `SADD` queued
/// on each) would wait forever. With one global order, the second command
/// waits for the first set before it holds any.
pub(super) fn read_all<'a>(sets: &[&'a Set]) -> Vec<RwLockReadGuard<'a, MemberSet>> {
    let mut order: Vec<usize> = (0..sets.len()).collect();
    order.sort_unstable_by_key(|&i| Arc::as_ptr(sets[i]) as usize);
    let mut guards: Vec<Option<RwLockReadGuard<'a, MemberSet>>> =
        sets.iter().map(|_| None).collect();
    for i in order {
        guards[i] = Some(sets[i].read());
    }
    guards.into_iter().flatten().collect()
}

/// The intersection of `sets`: calls `each` for every member of it, up to
/// `limit` of them (`0`: all), and returns how many that was.
///
/// It walks the smallest set and looks each member up in the others, smallest
/// first, so it costs O(smallest x number of sets) and never touches the rest
/// of a big set (#759). A missing key is an empty set, and so is the result.
pub(super) fn inter_each(
    sets: &[Option<Set>],
    limit: usize,
    mut each: impl FnMut(&Arc<[u8]>),
) -> usize {
    if sets.iter().any(Option::is_none) {
        return 0;
    }
    let handles = distinct(sets.iter().flatten());
    let mut guards = read_all(&handles);
    guards.sort_by_key(|set| set.len());
    let Some((smallest, others)) = guards.split_first() else {
        return 0;
    };
    let mut count = 0usize;
    for member in smallest.iter() {
        if others.iter().all(|other| other.contains(member)) {
            each(member);
            count += 1;
            if limit > 0 && count >= limit {
                break;
            }
        }
    }
    count
}

/// The members of the first set that no other set has: calls `each` for each
/// of them, up to `limit` (`0`: all), and returns how many that was. It walks
/// the first set only and looks its members up in the others.
pub(super) fn diff_each(
    sets: &[Option<Set>],
    limit: usize,
    mut each: impl FnMut(&Arc<[u8]>),
) -> usize {
    let Some(Some(first)) = sets.first() else {
        return 0;
    };
    let mut handles: Vec<&Set> = vec![first];
    for other in sets[1..].iter().flatten() {
        if Arc::ptr_eq(other, first) {
            // A set minus itself.
            return 0;
        }
        if !handles.iter().any(|seen| Arc::ptr_eq(seen, other)) {
            handles.push(other);
        }
    }
    let guards = read_all(&handles);
    let Some((first, others)) = guards.split_first() else {
        return 0;
    };
    let mut count = 0usize;
    for member in first.iter() {
        if others.iter().all(|other| !other.contains(member)) {
            each(member);
            count += 1;
            if limit > 0 && count >= limit {
                break;
            }
        }
    }
    count
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
    diff_each(sets, limit, |_| {})
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

    fn inter(sets: &[Option<Set>], limit: usize) -> Vec<String> {
        let mut out = Vec::new();
        let n = inter_each(sets, limit, |m| {
            out.push(String::from_utf8(m.to_vec()).unwrap())
        });
        assert_eq!(n, out.len());
        out.sort();
        out
    }

    #[test]
    fn intersection_whatever_the_order_and_sizes() {
        let big: Vec<String> = (0..1000).map(|i| i.to_string()).collect();
        let big: Vec<&str> = big.iter().map(String::as_str).collect();
        let small = set(&["5", "999", "x", "17"]);
        let big = set(&big);
        let want = ["17", "5", "999"];
        assert_eq!(inter(&[big.clone(), small.clone()], 0), want);
        assert_eq!(inter(&[small.clone(), big.clone()], 0), want);
        // The same key twice is one set (and one lock).
        assert_eq!(inter(&[small.clone(), big.clone(), small.clone()], 0), want);
        assert_eq!(inter(&[big.clone(), big.clone()], 0).len(), 1000);
        // A missing key empties it, wherever it is.
        assert_eq!(inter(&[big.clone(), small.clone(), None], 0).len(), 0);
        assert_eq!(inter(&[None, big.clone()], 0).len(), 0);
        // LIMIT stops early, and never counts past the real size.
        assert_eq!(inter(&[big.clone(), small.clone()], 2).len(), 2);
        assert_eq!(inter(&[big.clone(), small.clone()], 3), want);
        assert_eq!(inter(&[big.clone(), small.clone()], 9), want);
        assert_eq!(inter(&[set(&[]), big], 0).len(), 0);
    }

    #[test]
    fn difference_walks_the_first_set() {
        let mut out = Vec::new();
        let first = set(&["a", "b", "c", "d"]);
        let n = diff_each(
            &[first.clone(), set(&["b"]), None, set(&["d", "e"])],
            0,
            |m| out.push(m.to_vec()),
        );
        out.sort();
        assert_eq!(n, 2);
        assert_eq!(out, [b"a".to_vec(), b"c".to_vec()]);
        assert_eq!(diff_each(&[first.clone(), first.clone()], 0, |_| {}), 0);
        assert_eq!(diff_each(&[None, first], 0, |_| {}), 0);
    }

    #[test]
    fn locks_are_taken_in_one_order() {
        // Two readers naming the same sets in opposite orders, with writers
        // queued on both, must not wait for each other.
        let a = set(&["1", "2"]).unwrap();
        let b = set(&["2", "3"]).unwrap();
        let handles: Vec<_> = (0..8)
            .map(|t| {
                let (a, b) = (Arc::clone(&a), Arc::clone(&b));
                std::thread::spawn(move || {
                    for i in 0..2000 {
                        let (x, y) = if t % 2 == 0 { (&a, &b) } else { (&b, &a) };
                        if t % 4 < 2 {
                            let sets = [Some(Arc::clone(x)), Some(Arc::clone(y))];
                            assert!(inter_each(&sets, 0, |_| {}) >= 1);
                        } else {
                            x.write().insert(Arc::from(format!("w{t}-{i}").as_bytes()));
                            y.write().remove(format!("w{t}-{i}").as_bytes());
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
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
