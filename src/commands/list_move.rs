//! Moving elements between lists: the shared core of `LMOVE`, `RPOPLPUSH`,
//! `BLMOVE`, `BRPOPLPUSH`, and the multi-element `LMOVEM` / `BLMOVEM`
//! (Redis 8.10).
//!
//! The single-element commands used to pop from the source first and only then
//! look at the destination, so `LMOVE list not-a-list LEFT LEFT` returned
//! WRONGTYPE *after* removing the element — it was simply gone. Everything
//! here checks the destination before touching the source, and puts the
//! elements back if the destination changes type in between.

use crate::commands::blocking;
use crate::commands::ctx::Ctx;
use crate::commands::helpers::{lazy_expire_key, parse_utf8};
use crate::keyspace_events::{KS_GENERIC, KS_LIST, notify};
use crate::resp::write::*;
use crate::store;
use crate::store::Key;
use crate::store::ValueRef;
use crate::types::KiviDBValue;
use crate::{read_lock, write_lock};
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::Arc;

type Element = Arc<[u8]>;
type List = Arc<RwLock<VecDeque<Element>>>;

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

/// What to move, from which end to which, and how the batch is laid down.
pub(super) struct MoveSpec {
    pub from_left: bool,
    pub to_left: bool,
    pub count: usize,
    /// `EXACTLY`: all `count` elements or none.
    pub exactly: bool,
    /// `OBO` (one by one) rather than `BULK`. See [`destination_order`].
    pub one_by_one: bool,
}

impl MoveSpec {
    /// One element — `LMOVE` and friends.
    pub fn single(from_left: bool, to_left: bool) -> Self {
        Self {
            from_left,
            to_left,
            count: 1,
            exactly: false,
            one_by_one: true,
        }
    }
}

/// `LEFT` → `Some(true)`, `RIGHT` → `Some(false)`.
pub(super) fn parse_end(arg: &[u8]) -> Option<bool> {
    if arg.eq_ignore_ascii_case(b"LEFT") {
        Some(true)
    } else if arg.eq_ignore_ascii_case(b"RIGHT") {
        Some(false)
    } else {
        None
    }
}

/// Parse `<LEFT|RIGHT> <LEFT|RIGHT>` plus the optional
/// `<COUNT n | EXACTLY n> <OBO | BULK>` block. The block is all-or-nothing:
/// a selector without an ordering (or the reverse) is a syntax error.
fn parse_spec(wherefrom: &[u8], whereto: &[u8], how_many: &[&[u8]]) -> Result<MoveSpec, Vec<u8>> {
    let syntax = || resp_error("ERR syntax error");
    let from_left = parse_end(wherefrom).ok_or_else(syntax)?;
    let to_left = parse_end(whereto).ok_or_else(syntax)?;
    let mut spec = MoveSpec::single(from_left, to_left);
    if how_many.is_empty() {
        return Ok(spec);
    }

    // As Redis 8.10's `lmovemParseOptions`: when present, exactly three
    // words, in this order, <COUNT|EXACTLY> <count> <OBO|BULK>.
    let [selector, count, ordering] = how_many else {
        return Err(syntax());
    };
    spec.exactly = if selector.eq_ignore_ascii_case(b"COUNT") {
        false
    } else if selector.eq_ignore_ascii_case(b"EXACTLY") {
        true
    } else {
        return Err(syntax());
    };
    spec.count = parse_utf8(count)
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|n| *n >= 1)
        .ok_or_else(|| resp_error("ERR count should be greater than 0"))? as usize;
    spec.one_by_one = if ordering.eq_ignore_ascii_case(b"OBO") {
        true
    } else if ordering.eq_ignore_ascii_case(b"BULK") {
        false
    } else {
        return Err(syntax());
    };
    Ok(spec)
}

/// The moved elements in the order they end up at the destination, left to
/// right — which is also the order the reply lists them in.
///
/// `popped` is in pop order. `OBO` pushes them one at a time, so onto the left
/// end each lands in front of the one before it. `BULK` moves the block as it
/// stood in the source, whichever end it was taken from.
fn destination_order(mut popped: Vec<Element>, spec: &MoveSpec) -> Vec<Element> {
    let reverse = if spec.one_by_one {
        spec.to_left
    } else {
        // Popping from the right yields the block back to front.
        !spec.from_left
    };
    if reverse {
        popped.reverse();
    }
    popped
}

/// Lay `ordered` down at the chosen end of `list` so it reads, left to right,
/// exactly as given.
fn place(list: &mut VecDeque<Element>, ordered: &[Element], to_left: bool) {
    if to_left {
        for el in ordered.iter().rev() {
            list.push_front(Arc::clone(el));
        }
    } else {
        list.extend(ordered.iter().cloned());
    }
}

/// How many elements a source of `len` yields, or `None` for "nothing to do".
fn take_count(len: usize, spec: &MoveSpec) -> Option<usize> {
    if len == 0 || (spec.exactly && len < spec.count) {
        None
    } else {
        Some(spec.count.min(len))
    }
}

fn pop_n(list: &mut VecDeque<Element>, n: usize, from_left: bool) -> Vec<Element> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let el = if from_left {
            list.pop_front()
        } else {
            list.pop_back()
        };
        match el {
            Some(el) => out.push(el),
            None => break,
        }
    }
    out
}

/// Look `key` up as a list. `Ok(None)` when it does not exist.
fn list_at(ctx: &Ctx, key: &str) -> Result<Option<List>, Vec<u8>> {
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    match read_lock!(shard).get(key) {
        None => Ok(None),
        Some(ValueRef::List(l)) => Ok(Some(Arc::clone(l))),
        Some(_) => Err(resp_error(WRONGTYPE)),
    }
}

/// What [`move_elements`] moved: the elements in destination order, and
/// whether that emptied the source, which is then deleted. The caller tells
/// of the delete last, after the move itself, as Redis does.
pub(super) struct Moved {
    pub elements: Vec<Element>,
    pub source_deleted: bool,
}

/// Move elements from `src` to `dst`. `Ok(None)` means the source could not
/// satisfy the request (missing, empty, or short of an `EXACTLY` count) and
/// nothing changed.
pub(super) fn move_elements(
    ctx: &Ctx,
    src: &str,
    dst: &str,
    spec: &MoveSpec,
) -> Result<Option<Moved>, Vec<u8>> {
    let db = *ctx.selected_db;
    lazy_expire_key(ctx.store, db, src);
    // The source first, as in Redis: with nothing to move, what the
    // destination holds does not come into it.
    let Some(src_list) = list_at(ctx, src)? else {
        return Ok(None);
    };
    if dst != src {
        lazy_expire_key(ctx.store, db, dst);
        // The destination's type is checked before anything leaves the source.
        list_at(ctx, dst)?;
    }

    if src == dst {
        // One list, one lock: the elements never leave it.
        let mut inner = src_list.write();
        let Some(n) = take_count(inner.len(), spec) else {
            return Ok(None);
        };
        let popped = pop_n(&mut inner, n, spec.from_left);
        let ordered = destination_order(popped, spec);
        place(&mut inner, &ordered, spec.to_left);
        drop(inner);
        ctx.wait_table.notify_signal(dst);
        return Ok(Some(Moved {
            elements: ordered,
            source_deleted: false,
        }));
    }

    let popped = {
        let mut inner = src_list.write();
        let Some(n) = take_count(inner.len(), spec) else {
            return Ok(None);
        };
        pop_n(&mut inner, n, spec.from_left)
    };
    let pop_order = popped.clone();
    let ordered = destination_order(popped, spec);

    {
        // Held across the push, like LPUSH, so the list cannot be removed
        // from the keyspace between finding it and adding to it.
        let shard = store::get_shard(&ctx.store[db], dst);
        let mut map = write_lock!(shard);
        let dst_list = match map.get(dst) {
            None => {
                let l: List = Arc::new(RwLock::new(VecDeque::new()));
                map.insert(Key::from(dst), KiviDBValue::List(Arc::clone(&l)));
                l
            }
            Some(ValueRef::List(l)) => Arc::clone(l),
            Some(_) => {
                // The destination changed type after we checked it. Put the
                // elements back where they came from rather than lose them.
                drop(map);
                let mut inner = src_list.write();
                for el in pop_order.into_iter().rev() {
                    if spec.from_left {
                        inner.push_front(el);
                    } else {
                        inner.push_back(el);
                    }
                }
                return Err(resp_error(WRONGTYPE));
            }
        };
        place(&mut dst_list.write(), &ordered, spec.to_left);
    }
    ctx.wait_table.notify_signal(dst);

    // A list that has been emptied is deleted, as in Redis.
    let mut source_deleted = false;
    if src_list.read().is_empty() {
        let shard = store::get_shard(&ctx.store[db], src);
        let mut map = write_lock!(shard);
        let still_empty = matches!(
            map.get(src),
            Some(ValueRef::List(l)) if Arc::ptr_eq(l, &src_list) && l.read().is_empty()
        );
        if still_empty {
            let removed = map.remove(src);
            drop(map);
            drop(removed);
            source_deleted = true;
        }
    }
    Ok(Some(Moved {
        elements: ordered,
        source_deleted,
    }))
}

/// The `del` event of a source a move emptied, after the move's own events.
fn tell_source_deleted(ctx: &Ctx, moved: &Moved, src: &str) {
    if moved.source_deleted {
        notify(ctx.pubsub, *ctx.selected_db, KS_GENERIC, "del", src);
    }
}

/// Whether `key` holds something, and it is not a list.
pub(super) fn is_not_a_list(ctx: &Ctx, key: &str) -> bool {
    list_at(ctx, key).is_err()
}

fn elements_reply(elements: &[Element]) -> Vec<u8> {
    resp_array(elements.iter().map(|e| resp_bulk(e)).collect())
}

fn key_arg(arg: &[u8]) -> Result<String, Vec<u8>> {
    parse_utf8(arg)
        .map(str::to_string)
        .ok_or_else(|| resp_error("ERR must be valid UTF-8"))
}

/// `LMOVE` / `RPOPLPUSH`, and the non-blocking half of `BLMOVE` /
/// `BRPOPLPUSH`: move one element and reply with it. `Ok(None)` when the
/// source had nothing to give.
pub(super) fn move_one(
    ctx: &Ctx,
    src: &str,
    dst: &str,
    from_left: bool,
    to_left: bool,
) -> Result<Option<Element>, Vec<u8>> {
    let Some(mut moved) = move_elements(ctx, src, dst, &MoveSpec::single(from_left, to_left))?
    else {
        return Ok(None);
    };
    let element = moved.elements.pop();
    if element.is_some() {
        let db = *ctx.selected_db;
        // As Redis (`lmoveGenericCommand`): the push to the destination, the
        // pop from the source, and then, if that emptied it, its delete. The
        // delete came first, and the pop before the push (#173).
        notify(
            ctx.pubsub,
            db,
            KS_LIST,
            if to_left { "lpush" } else { "rpush" },
            dst,
        );
        notify(
            ctx.pubsub,
            db,
            KS_LIST,
            if from_left { "lpop" } else { "rpop" },
            src,
        );
        tell_source_deleted(ctx, &moved, src);
    }
    Ok(element)
}

pub(super) fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    match name {
        "LMOVEM" => {
            // LMOVEM source destination <LEFT|RIGHT> <LEFT|RIGHT>
            //        [<COUNT count | EXACTLY exactly> <OBO | BULK>]
            if ctx.cmd.len() < 5 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'lmovem' command",
                ));
            }
            let parsed = key_arg(ctx.cmd[1]).and_then(|src| {
                let dst = key_arg(ctx.cmd[2])?;
                let spec = parse_spec(ctx.cmd[3], ctx.cmd[4], &ctx.cmd[5..])?;
                Ok((src, dst, spec))
            });
            let (src, dst, spec) = match parsed {
                Ok(p) => p,
                Err(e) => return Some(e),
            };
            Some(match move_elements(ctx, &src, &dst, &spec) {
                Err(e) => e,
                Ok(Some(moved)) => {
                    let db = *ctx.selected_db;
                    notify(ctx.pubsub, db, KS_LIST, "lmovem", &src);
                    notify(ctx.pubsub, db, KS_LIST, "lmovem", &dst);
                    tell_source_deleted(ctx, &moved, &src);
                    elements_reply(&moved.elements)
                }
                Ok(None) if spec.exactly => resp_null_array(),
                Ok(None) => resp_array(Vec::new()),
            })
        }

        "BLMOVEM" => {
            // BLMOVEM source destination <LEFT|RIGHT> <LEFT|RIGHT> timeout
            //         [<COUNT count | EXACTLY exactly> <OBO | BULK>]
            if ctx.cmd.len() < 6 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'blmovem' command",
                ));
            }
            let parsed = key_arg(ctx.cmd[1]).and_then(|src| {
                let dst = key_arg(ctx.cmd[2])?;
                let spec = parse_spec(ctx.cmd[3], ctx.cmd[4], &ctx.cmd[6..])?;
                let timeout = blocking::parse_timeout(ctx.cmd[5])?;
                Ok((src, dst, spec, timeout))
            });
            let (src, dst, spec, timeout) = match parsed {
                Ok(p) => p,
                Err(e) => return Some(e),
            };

            // It may wake to a source that is still too short for EXACTLY,
            // take nothing, and wait again against the same deadline.
            let keys = [src.clone()];
            Some(blocking::wait_for_keys(
                ctx,
                &keys,
                blocking::timeout_from_secs(timeout),
                |ctx, _| match move_elements(ctx, &src, &dst, &spec) {
                    Err(e) if is_not_a_list(ctx, &dst) => Some(blocking::definite(e)),
                    Err(e) => Some(e),
                    Ok(Some(moved)) => {
                        let db = *ctx.selected_db;
                        notify(ctx.pubsub, db, KS_LIST, "lmovem", &src);
                        notify(ctx.pubsub, db, KS_LIST, "lmovem", &dst);
                        tell_source_deleted(ctx, &moved, &src);
                        crate::stats::record_hit();
                        Some(elements_reply(&moved.elements))
                    }
                    Ok(None) => None,
                },
                resp_null_array,
            ))
        }

        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn els(items: &[&str]) -> Vec<Element> {
        items.iter().map(|s| Arc::from(s.as_bytes())).collect()
    }

    fn strs(items: &[Element]) -> Vec<String> {
        items
            .iter()
            .map(|e| String::from_utf8_lossy(e).into_owned())
            .collect()
    }

    fn spec(from_left: bool, to_left: bool, one_by_one: bool) -> MoveSpec {
        MoveSpec {
            from_left,
            to_left,
            count: 2,
            exactly: false,
            one_by_one,
        }
    }

    /// Source `1 2 3 4`, two elements, every combination of ends and ordering.
    #[test]
    fn destination_order_matches_the_documented_orderings() {
        // From the left the pop order is 1, 2; from the right it is 4, 3.
        let left = || els(&["1", "2"]);
        let right = || els(&["4", "3"]);

        // OBO: each element is pushed in turn.
        assert_eq!(
            strs(&destination_order(left(), &spec(true, true, true))),
            ["2", "1"]
        );
        assert_eq!(
            strs(&destination_order(left(), &spec(true, false, true))),
            ["1", "2"]
        );
        assert_eq!(
            strs(&destination_order(right(), &spec(false, true, true))),
            ["3", "4"]
        );
        assert_eq!(
            strs(&destination_order(right(), &spec(false, false, true))),
            ["4", "3"]
        );

        // BULK: the block keeps the order it had in the source.
        for to_left in [true, false] {
            assert_eq!(
                strs(&destination_order(left(), &spec(true, to_left, false))),
                ["1", "2"]
            );
            assert_eq!(
                strs(&destination_order(right(), &spec(false, to_left, false))),
                ["3", "4"]
            );
        }
    }

    #[test]
    fn exactly_is_all_or_nothing_and_count_is_up_to() {
        let mut s = spec(true, true, true);
        s.count = 3;
        assert_eq!(take_count(2, &s), Some(2));
        assert_eq!(take_count(5, &s), Some(3));
        assert_eq!(take_count(0, &s), None);
        s.exactly = true;
        assert_eq!(take_count(2, &s), None);
        assert_eq!(take_count(3, &s), Some(3));
    }

    #[test]
    fn the_how_many_block_needs_both_halves() {
        let ok = |args: &[&[u8]]| parse_spec(b"LEFT", b"right", args).is_ok();
        assert!(ok(&[]));
        assert!(ok(&[b"COUNT", b"2", b"OBO"]));
        assert!(ok(&[b"exactly", b"2", b"bulk"]));
        assert!(!ok(&[b"COUNT", b"2"]));
        assert!(!ok(&[b"OBO"]));
        assert!(!ok(&[b"COUNT", b"0", b"OBO"]));
        assert!(!ok(&[b"COUNT", b"x", b"OBO"]));
        assert!(!ok(&[b"COUNT", b"2", b"SIDEWAYS"]));
        // In Redis's order only.
        assert!(!ok(&[b"OBO", b"COUNT", b"2"]));
        assert!(!ok(&[b"COUNT", b"2", b"OBO", b"extra"]));
        assert!(parse_spec(b"UP", b"LEFT", &[]).is_err());

        let s = parse_spec(b"right", b"LEFT", &[b"EXACTLY", b"3", b"BULK"]).unwrap();
        assert!(!s.from_left && s.to_left && s.exactly && !s.one_by_one);
        assert_eq!(s.count, 3);
    }
}
