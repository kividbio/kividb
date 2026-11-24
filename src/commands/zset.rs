// src/commands/zset.rs

use crate::commands::blocking;
use crate::commands::ctx::Ctx;
use crate::commands::helpers::format_score;
use crate::keyspace_events::{KS_SORTED_SET, notify};
use crate::member_set::Member;
use crate::resp::write::*;
use crate::store;
use crate::store::ValueRef;
use crate::types::{KiviDBValue, SortedSetInner, ZMember};
use crate::zset_table::Probe;
use crate::{read_lock, write_lock};
use ordered_float::OrderedFloat;
use parking_lot::RwLock;
use std::ops::Bound;
use std::sync::Arc;

// ── RESP3 shaping ─────────────────────────────────────────────────────────
//
// Two things change for sorted sets once RESP3 is negotiated:
//
//  1. Scores become the RESP3 double type (`,`) instead of bulk strings.
//     RESP2 has no double, so scores have always gone out as strings; a RESP3
//     client (redis-py included) expects a real double and yields a float.
//  2. `WITHSCORES`-style replies stop being flat. RESP2 sends
//     `[m1, s1, m2, s2, ...]`; RESP3 sends `[[m1, s1], [m2, s2], ...]`, so the
//     outer length is the member count rather than twice it.
//
// The RESP2 branches below deliberately keep going through `format_score`, so
// RESP2 output stays byte-for-byte what it was before this change.

/// Effective protocol for the reply being built (honours `redis.setresp`).
#[inline]
fn reply_resp(ctx: &Ctx) -> u8 {
    crate::commands::handler::effective_resp_for_reply(ctx)
}

/// A score: RESP3 double, RESP2 bulk string.
#[inline]
fn score_reply(resp: u8, score: f64) -> Vec<u8> {
    if resp >= 3 {
        resp_double_resp3(score)
    } else {
        resp_bulk_str(&format_score(score))
    }
}

/// Append one `member` (optionally with its `score`) to a range-style reply.
///
/// Mirrors Redis: flat under RESP2, one nested `[member, score]` array per
/// entry under RESP3. Callers that never emit scores can ignore `score`.
#[inline]
fn push_member_score(out: &mut Vec<Vec<u8>>, resp: u8, member: &[u8], score: Option<f64>) {
    match score {
        None => out.push(resp_bulk(member)),
        Some(s) if resp >= 3 => {
            out.push(resp_array(vec![resp_bulk(member), score_reply(resp, s)]));
        }
        Some(s) => {
            out.push(resp_bulk(member));
            out.push(score_reply(resp, s));
        }
    }
}

/// The reply to a range command: the members, each with its score if those
/// were asked for. Flat under RESP2, a `[member, score]` pair for each entry
/// under RESP3, as Redis has it.
///
/// It is written into one buffer. It used to be made of a vector for every
/// member and another for every score, joined at the end: for a hundred
/// members, some three hundred allocations to an answer. The members are
/// the set's own, read where they are held: none is copied first.
fn range_reply(pairs: &[(Member<'_>, f64)], resp: u8, with_scores: bool) -> Vec<u8> {
    let bytes: usize = pairs.iter().map(|(member, _)| member.len()).sum();
    pairs_reply(
        pairs.iter().map(|(member, score)| (&member[..], *score)),
        pairs.len(),
        bytes,
        resp,
        with_scores,
        with_scores && resp >= 3,
    )
}

/// [`range_reply`] for members wherever they are held. `paired` is whether
/// each member and its score are an array of their own.
fn pairs_reply<'a>(
    pairs: impl Iterator<Item = (&'a [u8], f64)>,
    count: usize,
    bytes: usize,
    resp: u8,
    with_scores: bool,
    paired: bool,
) -> Vec<u8> {
    let mut out =
        Vec::with_capacity(16 + bytes + count * 16 + if with_scores { count * 28 } else { 0 });
    pairs_header(&mut out, count, with_scores, paired);
    for (member, score) in pairs {
        push_pair(&mut out, member, score, resp, with_scores, paired);
    }
    out
}

/// The array header of [`pairs_reply`] for `count` members.
#[inline]
fn pairs_header(out: &mut Vec<u8>, count: usize, with_scores: bool, paired: bool) {
    let entries = if with_scores && !paired {
        count * 2
    } else {
        count
    };
    crate::resp::write::push_header(out, b'*', entries);
}

/// One member of [`pairs_reply`], and its score if asked for.
#[inline]
fn push_pair(
    out: &mut Vec<u8>,
    member: &[u8],
    score: f64,
    resp: u8,
    with_scores: bool,
    paired: bool,
) {
    if paired {
        out.extend_from_slice(b"*2\r\n");
    }
    crate::resp::write::push_header(out, b'$', member.len());
    out.extend_from_slice(member);
    out.extend_from_slice(b"\r\n");
    if with_scores {
        if resp >= 3 {
            out.extend_from_slice(&resp_double_resp3(score));
        } else if let Some(whole) = crate::commands::helpers::whole_score(score) {
            // Its digits, straight into the reply: no string is made.
            let mut digits = [0u8; 12];
            let mut at = digits.len();
            let mut left = whole.unsigned_abs();
            loop {
                at -= 1;
                digits[at] = b'0' + (left % 10) as u8;
                left /= 10;
                if left == 0 {
                    break;
                }
            }
            if whole < 0 {
                at -= 1;
                digits[at] = b'-';
            }
            crate::resp::write::push_header(out, b'$', digits.len() - at);
            out.extend_from_slice(&digits[at..]);
            out.extend_from_slice(b"\r\n");
        } else {
            let text = format_score(score);
            crate::resp::write::push_header(out, b'$', text.len());
            out.extend_from_slice(text.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
    }
}

/// Write the sorted set at `key` with `write`; the set is made if there is
/// none, with what `write` puts in it. `Err` is the WRONGTYPE reply.
///
/// All of it with the key's shard held: for reading when the set is there,
/// which keeps a command that empties it (`ZREM`, `ZPOPMIN`) from taking it
/// out of the keyspace meanwhile; for writing when it is made, and then
/// made whole, never empty. Made empty, then let go of and written, it was
/// taken for one a `ZREM` had emptied, removed, and the members went into a
/// set no longer in the keyspace (#271). Nor is a set left empty: `ZADD XX`
/// of a key that is not there makes nothing.
fn write_zset<R>(
    ctx: &Ctx,
    key: &[u8],
    mut write: impl FnMut(&mut SortedSetInner) -> R,
) -> Result<R, Vec<u8>> {
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    match read_lock!(shard).get(key) {
        Some(ValueRef::SortedSet(zs)) => return Ok(write_and_hand(zs, &mut write)),
        Some(_) => {
            return Err(resp_error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            ));
        }
        None => {}
    }
    let mut map = write_lock!(shard);
    // Looked for again: it may have been made since.
    match map.get(key) {
        Some(ValueRef::SortedSet(zs)) => Ok(write_and_hand(zs, &mut write)),
        Some(_) => Err(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => {
            let mut zs = SortedSetInner::new();
            let out = write(&mut zs);
            if !zs.is_empty() {
                // Made whole by one command: a block with no room to spare.
                zs.shrink_to_fit();
                map.insert(key, KiviDBValue::SortedSet(crate::sorted_set::shared(zs)));
            }
            Ok(out)
        }
    }
}

/// `write` on the set, which is then handed to the background if its table
/// was left with a resize to finish (`sorted_set::after_write`).
#[inline]
fn write_and_hand<R>(
    zs: &Arc<RwLock<SortedSetInner>>,
    write: &mut impl FnMut(&mut SortedSetInner) -> R,
) -> R {
    let mut inner = zs.write();
    let out = write(&mut inner);
    crate::sorted_set::after_write(zs, &mut inner);
    out
}

/// The first bulk string of an array reply: the key, in the reply of a
/// command that takes several and answers with the one it used.
fn first_bulk(reply: &[u8]) -> Option<&[u8]> {
    // `*n\r\n$len\r\n` and then `len` bytes, which may hold anything,
    // line breaks too: the key is read by its length, not by lines.
    let rest = reply.strip_prefix(b"*")?;
    let rest = &rest[memchr::memmem::find(rest, b"\r\n")? + 2..];
    let rest = rest.strip_prefix(b"$")?;
    let end = memchr::memmem::find(rest, b"\r\n")?;
    let len: usize = std::str::from_utf8(&rest[..end]).ok()?.parse().ok()?;
    rest.get(end + 2..end + 2 + len)
}

/// The keyspace event of a sorted-set command that has just answered
/// `reply`, if what it did is one. (`ZADD` announces its own: whether it
/// changed anything is not in its reply.)
fn announce(ctx: &Ctx, name: &str, reply: &[u8]) {
    if crate::keyspace_events::KS_FLAGS.load(std::sync::atomic::Ordering::Relaxed) == 0 {
        return;
    }
    let removed_some = || reply.first() == Some(&b':') && !reply.starts_with(b":0\r\n");
    let popped_some = || {
        reply.first() == Some(&b'*') && !reply.starts_with(b"*0\r\n") && !reply.starts_with(b"*-1")
    };
    let key_argument = || ctx.cmd.get(1).copied();
    let lower;
    let (event, key): (&str, Option<&[u8]>) = match name {
        "ZINCRBY" if reply.first() != Some(&b'-') => ("zincr", key_argument()),
        "ZREM" if removed_some() => ("zrem", key_argument()),
        "ZREMRANGEBYSCORE" | "ZREMRANGEBYRANK" | "ZREMRANGEBYLEX" | "ZUNIONSTORE"
        | "ZINTERSTORE" | "ZDIFFSTORE" | "ZRANGESTORE"
            if removed_some() =>
        {
            lower = name.to_ascii_lowercase();
            (lower.as_str(), key_argument())
        }
        "ZPOPMIN" if popped_some() => ("zpopmin", key_argument()),
        "ZPOPMAX" if popped_some() => ("zpopmax", key_argument()),
        "BZPOPMIN" if popped_some() => ("zpopmin", first_bulk(reply)),
        "BZPOPMAX" if popped_some() => ("zpopmax", first_bulk(reply)),
        "ZMPOP" | "BZMPOP" if popped_some() => {
            let max = ctx.cmd.iter().any(|word| word.eq_ignore_ascii_case(b"MAX"));
            (if max { "zpopmax" } else { "zpopmin" }, first_bulk(reply))
        }
        _ => return,
    };
    if let Some(key) = key {
        notify(ctx.pubsub, *ctx.selected_db, KS_SORTED_SET, event, key);
    }
}

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    let reply = run(ctx, name)?;
    announce(ctx, name, &reply);
    Some(reply)
}

fn run(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    match name {
        "ZADD" => Some(zadd(ctx)),
        "ZCARD" => Some(zcard(ctx)),
        "ZSCORE" => Some(zscore(ctx)),
        "ZMSCORE" => Some(zmscore(ctx)),
        "ZRANK" => Some(zrank(ctx)),
        "ZREVRANK" => Some(zrevrank(ctx)),
        "ZINCRBY" => Some(zincrby(ctx)),
        "ZREM" => Some(zrem(ctx)),
        "ZCOUNT" => Some(zcount(ctx)),
        "ZLEXCOUNT" => Some(zlexcount(ctx)),
        "ZRANGE" => Some(zrange(ctx)),
        "ZRANGEBYSCORE" => Some(zrangebyscore(ctx)),
        "ZREVRANGEBYSCORE" => Some(zrevrangebyscore(ctx)),
        "ZREVRANGE" => Some(zrevrange(ctx)),
        "ZRANGEBYLEX" => Some(zrangebylex(ctx)),
        "ZREVRANGEBYLEX" => Some(zrevrangebylex(ctx)),
        "ZRANGESTORE" => Some(zrangestore(ctx)),
        "ZPOPMIN" => Some(zpopmin(ctx)),
        "ZPOPMAX" => Some(zpopmax(ctx)),
        "ZSCAN" => Some(zscan(ctx)),
        "ZRANDMEMBER" => Some(zrandmember(ctx)),
        "ZDIFF" => Some(zdiff(ctx)),
        "ZDIFFSTORE" => Some(zdiffstore(ctx)),
        "ZINTER" => Some(zinter(ctx)),
        "ZINTERSTORE" => Some(zinterstore(ctx)),
        "ZINTERCARD" => Some(zintercard(ctx)),
        "ZUNION" => Some(zunion(ctx)),
        "ZUNIONSTORE" => Some(zunionstore(ctx)),
        "ZREMRANGEBYSCORE" => Some(zremrangebyscore(ctx)),
        "ZREMRANGEBYRANK" => Some(zremrangebyrank(ctx)),
        "ZREMRANGEBYLEX" => Some(zremrangebylex(ctx)),
        "BZPOPMIN" => Some(bzpopmin(ctx)),
        "BZPOPMAX" => Some(bzpopmax(ctx)),
        "ZMPOP" => Some(zmpop(ctx)),
        "BZMPOP" => Some(bzmpop(ctx)),
        _ => None,
    }
}

/// Parameters for a range query — built by each public command and passed to zrange_pairs.
/// What may follow the range of `ZRANGE` and `ZRANGESTORE`.
struct RangeOptions {
    by_score: bool,
    by_lex: bool,
    rev: bool,
    with_scores: bool,
    limit_offset: usize,
    limit_count: usize,
}

/// `LIMIT offset count` of the ZRANGEBYSCORE / ZRANGEBYLEX family, as ZRANGE
/// takes it: integers, a negative offset past every element (an empty reply,
/// as in Redis 8.10), a negative count all of them.
fn parse_limit(offset: &[u8], count: &[u8]) -> Result<(usize, usize), Vec<u8>> {
    let number = |raw: &[u8]| {
        crate::member_set::canonical_i64(raw)
            .ok_or_else(|| resp_error("ERR value is not an integer or out of range"))
    };
    let (offset, count) = (number(offset)?, number(count)?);
    Ok((
        usize::try_from(offset).unwrap_or(usize::MAX),
        usize::try_from(count).unwrap_or(usize::MAX),
    ))
}

/// `[BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]` from
/// `cmd[from]` on. `storing`: the command stores its result, and has no
/// scores to return.
fn range_options(cmd: &[&[u8]], from: usize, storing: bool) -> Result<RangeOptions, Vec<u8>> {
    let mut options = RangeOptions {
        by_score: false,
        by_lex: false,
        rev: false,
        with_scores: false,
        limit_offset: 0,
        limit_count: usize::MAX,
    };
    let mut limited = false;
    let mut i = from;
    while i < cmd.len() {
        let word = String::from_utf8_lossy(cmd[i]).to_ascii_uppercase();
        match word.as_str() {
            "BYSCORE" => options.by_score = true,
            "BYLEX" => options.by_lex = true,
            "REV" => options.rev = true,
            "WITHSCORES" if !storing => options.with_scores = true,
            "LIMIT" if i + 2 < cmd.len() => {
                let number = |raw: &[u8]| {
                    crate::member_set::canonical_i64(raw)
                        .ok_or_else(|| resp_error("ERR value is not an integer or out of range"))
                };
                let (offset, count) = (number(cmd[i + 1])?, number(cmd[i + 2])?);
                // A negative offset is past every element; a negative count
                // is all of them.
                options.limit_offset = if offset < 0 {
                    usize::MAX
                } else {
                    offset as usize
                };
                options.limit_count = if count < 0 {
                    usize::MAX
                } else {
                    count as usize
                };
                limited = true;
                i += 2;
            }
            _ => return Err(resp_error("ERR syntax error")),
        }
        i += 1;
    }
    if options.by_score && options.by_lex {
        return Err(resp_error("ERR syntax error"));
    }
    if limited && !options.by_score && !options.by_lex {
        return Err(resp_error(
            "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
        ));
    }
    if options.with_scores && options.by_lex {
        return Err(resp_error(
            "ERR syntax error, WITHSCORES not supported in combination with BYLEX",
        ));
    }
    Ok(options)
}

struct RangeParams<'a> {
    start: &'a [u8],
    stop: &'a [u8],
    by_score: bool,
    by_lex: bool,
    rev: bool,
    limit_offset: usize,
    limit_count: usize, // usize::MAX means "no limit"
}

/// The members of a rank walk, pushed one by one: `collect` left the walk
/// out of line (#920). Borrowed, not copied.
fn push_members<'a>(members: impl Iterator<Item = (Member<'a>, f64)>) -> Vec<(Member<'a>, f64)> {
    let mut found = Vec::with_capacity(members.size_hint().0);
    for pair in members {
        found.push(pair);
    }
    found
}

/// Core range logic: the members asked for, as the set holds them, and
/// their scores — no RESP encoding, and no copy of any member.
/// All public range commands call this and format the output themselves:
///   - ZRANGE formats with optional WITHSCORES
///   - ZRANGESTORE builds the destination from them without a parse round-trip
///   - Legacy wrappers (ZRANGEBYSCORE etc.) just build RangeParams and call this
fn zrange_pairs<'a>(inner: &'a SortedSetInner, p: &RangeParams) -> Vec<(Member<'a>, f64)> {
    if p.by_score {
        // REV swaps the bounds — caller passes (max, min) in REV mode
        let (min_raw, max_raw) = if p.rev {
            (p.stop, p.start)
        } else {
            (p.start, p.stop)
        };

        let (Ok(min), Ok(max)) = (parse_score_bound(min_raw), parse_score_bound(max_raw)) else {
            return vec![];
        };
        let Some(range) = score_range(min, max) else {
            return vec![];
        };
        // The range's ranks, then the `LIMIT` window of them: the offset is
        // added to a rank, not stepped over (#875). Pushed one by one, as a
        // rank walk's: `collect` left the walk out of line (#920).
        let (lo, hi) = inner.rank_range_by_score(range);
        let (start, end) = limit_window(lo, hi, p.limit_offset, p.limit_count, p.rev);
        if p.rev {
            push_members(inner.ranks(start, end).rev())
        } else {
            push_members(inner.ranks(start, end))
        }
    } else if p.by_lex {
        // REV swaps the bounds
        let (min_raw, max_raw) = if p.rev {
            (p.stop, p.start)
        } else {
            (p.start, p.stop)
        };
        let (Ok(min), Ok(max)) = (parse_lex_bound(min_raw), parse_lex_bound(max_raw)) else {
            return vec![];
        };
        // Every member at one score: the order is the members' own, and the
        // range is ranks found by a walk down for each end (#875).
        if let Some(bounds) = one_score_lex_bounds(inner, &min, &max) {
            let (lo, hi) = match bounds {
                Some((from, to)) => inner.rank_range(from, to),
                None => return vec![],
            };
            let (start, end) = limit_window(lo, hi, p.limit_offset, p.limit_count, p.rev);
            return if p.rev {
                push_members(inner.ranks(start, end).rev())
            } else {
                push_members(inner.ranks(start, end))
            };
        }
        let lex_in_range = move |member: &[u8]| lex_in_range(member, &min, &max);

        if p.rev {
            inner
                .iter()
                .rev()
                .filter(|(member, _)| lex_in_range(member.as_ref()))
                .skip(p.limit_offset)
                .take(p.limit_count)
                .collect()
        } else {
            inner
                .iter()
                .filter(|(member, _)| lex_in_range(member.as_ref()))
                .skip(p.limit_offset)
                .take(p.limit_count)
                .collect()
        }
    } else {
        // Index-based — unchanged, already correct
        let len = inner.len() as i64;
        if len == 0 {
            return vec![];
        }

        let parse_idx = |b: &[u8]| -> i64 { crate::member_set::canonical_i64(b).unwrap_or(0) };
        let mut start = parse_idx(p.start);
        let mut stop = parse_idx(p.stop);

        if start < 0 {
            start = (len + start).max(0);
        }
        if stop < 0 {
            stop += len;
        }
        if stop >= len {
            stop = len - 1;
        }
        if start > stop {
            return vec![];
        }

        // The tree has no way straight to the hundredth member: it is
        // walked to it, from whichever end is nearer. `start` counts from
        // the end the answer begins at (the top, for REV); past half way,
        // the walk comes from the other end and the answer is turned round.
        let from_far_end = (start as usize) > inner.len() / 2;
        let (start, stop) = (start as usize, stop as usize);
        // The same members as ranks from the bottom, for REV.
        let (first, last) = (len as usize - 1 - stop, len as usize - 1 - start);
        if p.rev == from_far_end {
            if p.rev {
                // Counted from the top and far from it: near the bottom.
                let mut found = push_members(inner.range_by_rank(first, last));
                found.reverse();
                found
            } else {
                push_members(inner.range_by_rank(start, stop))
            }
        } else if p.rev {
            push_members(inner.range_by_rank(first, last).rev())
        } else {
            // Forward, but nearer the back: walk from the back and turn it.
            let mut found = push_members(inner.range_by_rank(start, stop).rev());
            found.reverse();
            found
        }
    }
}

fn zadd(ctx: &mut Ctx) -> Vec<u8> {
    // ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'zadd' command");
    }

    let key: Vec<u8> = cmd[1].to_vec();

    // ── Parse optional flags before the score/member pairs ──────────────────
    // We scan from index 2 until we find something that parses as f64,
    // which signals the start of score/member pairs.
    let mut i = 2usize;
    let mut flag_nx = false; // Only add new members, never update existing
    let mut flag_xx = false; // Only update existing members, never add new
    let mut flag_gt = false; // Only update if new score > current score
    let mut flag_lt = false; // Only update if new score < current score
    let mut flag_ch = false; // Return count of *changed* elements (added + updated)
    let mut flag_incr = false; // Treat score as increment, like ZINCRBY

    while i < cmd.len() {
        let token = match std::str::from_utf8(cmd[i]) {
            Ok(s) => s.to_ascii_uppercase(),
            Err(_) => return resp_error("ERR flag must be valid UTF-8"),
        };
        match token.as_str() {
            "NX" => {
                flag_nx = true;
                i += 1;
            }
            "XX" => {
                flag_xx = true;
                i += 1;
            }
            "GT" => {
                flag_gt = true;
                i += 1;
            }
            "LT" => {
                flag_lt = true;
                i += 1;
            }
            "CH" => {
                flag_ch = true;
                i += 1;
            }
            "INCR" => {
                flag_incr = true;
                i += 1;
            }
            _ => break, // not a flag → must be start of score/member pairs
        }
    }

    // Validate flag combinations Redis rejects:
    if flag_nx && flag_xx {
        return resp_error("ERR XX and NX options at the same time are not compatible");
    }
    if (flag_gt && flag_lt) || ((flag_gt || flag_lt) && flag_nx) {
        return resp_error("ERR GT, LT, and/or NX options at the same time are not compatible");
    }
    // Scores and members come in pairs, and there is at least one.
    if i >= cmd.len() || !(cmd.len() - i).is_multiple_of(2) {
        return resp_error("ERR syntax error");
    }
    if flag_incr && (cmd.len() - i) != 2 {
        // INCR mode only accepts a single score/member pair
        return resp_error("ERR INCR option supports a single increment-element pair");
    }

    // Every score is read before anything is written: a command that is
    // refused has changed nothing. The members are the command's own bytes.
    let mut pairs: Vec<(f64, &[u8])> = Vec::with_capacity((cmd.len() - i) / 2);
    while i + 1 < cmd.len() {
        let Some(score) = parse_score(cmd[i]) else {
            return resp_error("ERR value is not a valid float");
        };
        pairs.push((score, cmd[i + 1]));
        i += 2;
    }

    if pairs.is_empty() {
        return resp_error("ERR syntax error");
    }

    let written = write_zset(ctx, &key, |inner| {
        if flag_incr
            && let Some((incr, member)) = pairs.first()
            && (inner.score(member).unwrap_or(0.0) + incr).is_nan()
        {
            return Err(resp_error("ERR resulting score is not a number (NaN)"));
        }
        // A block that this command's pairs cannot fit becomes the large
        // form before any is written, as Redis judges by the command.
        inner.expect(pairs.len());
        let mut added = 0i64;
        let mut changed = 0i64;
        let mut last_score: Option<f64> = None; // used for INCR mode
        for &(score, member) in &pairs {
            // One lookup a member: its score, if it has one, decides what
            // is written.
            inner.upsert(member, |existing| {
                let final_score = if flag_incr {
                    // INCR: add to the existing score; a new member's is the
                    // increment itself, `-0` too, as Redis replies (#422).
                    existing.map_or(score, |old| old + score)
                } else {
                    score
                };
                match existing {
                    None => {
                        // Member doesn't exist
                        if flag_xx {
                            // XX: skip, don't add
                            return (None, ());
                        }
                        // Every member goes into the set; a client blocked on
                        // the key is woken afterwards and pops for itself (see
                        // `blocking::wait_for_keys`).
                        added += 1;
                        changed += 1;
                        last_score = Some(final_score);
                        (Some(final_score), ())
                    }
                    Some(old_score) => {
                        // Member exists. NX: skip, don't update; GT/LT gating.
                        if flag_nx
                            || (flag_gt && final_score <= old_score)
                            || (flag_lt && final_score >= old_score)
                        {
                            return (None, ());
                        }
                        // (Any difference: two scores a hair apart are two
                        // scores.)
                        if final_score != old_score {
                            changed += 1;
                        }
                        last_score = Some(final_score);
                        // The same score writes nothing.
                        (Some(final_score), ())
                    }
                }
            });
        }
        // A table reserved for every pair, members given twice included,
        // is made as small as what came.
        inner.settle(pairs.len());
        Ok((added, changed, last_score))
    });
    let (added, changed, last_score) = match written {
        Ok(Ok(written)) => written,
        Err(e) | Ok(Err(e)) => return e,
    };
    if changed > 0 {
        let event = if flag_incr { "zincr" } else { "zadd" };
        notify(ctx.pubsub, *ctx.selected_db, KS_SORTED_SET, event, &key);
    }

    // INCR mode returns the new score (a double under RESP3, bulk string under
    // RESP2), or nil if NX/XX skipped the update. The nil is the whole reply, so
    // `resp3_normalize_null` in the dispatcher rewrites it for RESP3.
    if flag_incr {
        return match last_score {
            Some(s) => score_reply(reply_resp(ctx), s),
            None => resp_null_bulk(), // ← this is your "nil"
        };
    }

    // CH flag: return added + updated; default: return only added
    resp_integer(if flag_ch { changed } else { added })
}

fn zcard(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() != 2 {
        return resp_error("ERR wrong number of arguments for 'zcard' command");
    }
    let key: &[u8] = cmd[1];

    if crate::commands::helpers::lazy_expire_on_read(ctx.store, *ctx.selected_db, key) {
        return resp_integer(0);
    }

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_integer(0),
        Some(ValueRef::SortedSet(zs)) => resp_integer(zs.read().len() as i64),
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

fn zscore(ctx: &mut Ctx) -> Vec<u8> {
    // ZSCORE key member → bulk string score or nil
    let cmd = &ctx.cmd;
    if cmd.len() != 3 {
        return resp_error("ERR wrong number of arguments for 'zscore' command");
    }
    let key: &[u8] = cmd[1];

    let resp = reply_resp(ctx);
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => {
            crate::stats::record_miss();
            resp_null_bulk()
        }
        Some(ValueRef::SortedSet(zs)) => match zs.read().score(cmd[2]) {
            Some(score) => {
                crate::stats::record_hit();
                score_reply(resp, score)
            }
            None => {
                crate::stats::record_miss();
                resp_null_bulk()
            }
        },
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

fn zmscore(ctx: &mut Ctx) -> Vec<u8> {
    // ZMSCORE key member [member ...] → array of bulk strings or nils
    // Rust lesson: when you have multiple members to look up, collect all results
    // into a Vec<Vec<u8>> and wrap with resp_array — one allocation, one response.
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'zmscore' command");
    }
    let key: &[u8] = cmd[1];

    // Nils here sit *inside* the array, so `resp3_normalize_null` (which only
    // rewrites a reply that is nothing but a null) never sees them — they have
    // to be encoded for the right protocol at this site.
    let resp = reply_resp(ctx);
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => {
            // Key missing → all nils
            let nils = vec![resp_null_for(resp); cmd.len() - 2];
            resp_array(nils)
        }
        Some(ValueRef::SortedSet(zs)) => {
            let inner = zs.read();
            let results = cmd[2..]
                .iter()
                .map(|m| match inner.score(m) {
                    Some(score) => score_reply(resp, score),
                    None => resp_null_for(resp),
                })
                .collect();
            resp_array(results)
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

fn zrank(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() < 3 || cmd.len() > 4 {
        return resp_error("ERR wrong number of arguments for 'zrank' command");
    }
    let with_score = match cmd.get(3) {
        None => false,
        Some(flag) if flag.eq_ignore_ascii_case(b"WITHSCORE") => true,
        Some(_) => return resp_error("ERR syntax error"),
    };
    // Nothing to report: a null array where a rank and score were asked
    // for, a null string where a rank was.
    let none = || {
        if with_score {
            resp_null_array()
        } else {
            resp_null_bulk()
        }
    };

    let key: &[u8] = cmd[1];

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => {
            crate::stats::record_miss();
            none()
        }
        Some(ValueRef::SortedSet(zs)) => {
            let inner = zs.read();
            let member = cmd[2];

            match inner.rank(member) {
                None => {
                    crate::stats::record_miss();
                    none()
                }
                Some((rank, score)) => {
                    crate::stats::record_hit();
                    let rank = rank as i64;

                    if with_score {
                        resp_array(vec![
                            resp_integer(rank),
                            score_reply(reply_resp(ctx), score),
                        ])
                    } else {
                        resp_integer(rank)
                    }
                }
            }
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

fn zrevrank(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() < 3 || cmd.len() > 4 {
        return resp_error("ERR wrong number of arguments for 'zrevrank' command");
    }
    let with_score = cmd.len() == 4 && {
        let flag = std::str::from_utf8(cmd[3])
            .unwrap_or("")
            .to_ascii_uppercase();
        if flag != "WITHSCORE" {
            return resp_error("ERR syntax error");
        }
        true
    };

    let key: &[u8] = cmd[1];

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    let none = || {
        if with_score {
            resp_null_array()
        } else {
            resp_null_bulk()
        }
    };
    match map.get(key) {
        None => none(),
        Some(ValueRef::SortedSet(zs)) => {
            let inner = zs.read();
            let member = cmd[2];

            match inner.rank(member) {
                None => none(),
                Some((asc_rank, score)) => {
                    let revrank = inner.len() as i64 - 1 - asc_rank as i64;

                    if with_score {
                        resp_array(vec![
                            resp_integer(revrank),
                            score_reply(reply_resp(ctx), score),
                        ])
                    } else {
                        resp_integer(revrank)
                    }
                }
            }
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

/// A score as Redis 8.10 reads one (`string2d`): decimal, `inf` /
/// `infinity` with a sign or without, or a C hexadecimal float such as
/// `0x1p+4`, all of the argument. Not `nan`, not an empty string, nothing
/// with space around it.
///
/// A number past the range of a double is where Redis is particular (#422).
/// Its decimal reader (`fast_float`) takes a number of at most 19
/// significant digits, `digits × 10^q`, as infinite or as zero, if that is
/// what it rounds to, as long as `q` is from -342 to 308, and refuses it
/// past them: `2e308` is infinite, `1e309` refused, `1e-342` zero, `1e-343`
/// refused. A number of more digits, and a hexadecimal one (`strtod`), is
/// refused if it rounds to infinity or to zero.
pub(crate) fn parse_score(raw: &[u8]) -> Option<f64> {
    let text = std::str::from_utf8(raw).ok()?;
    if text.is_empty() || text.starts_with(char::is_whitespace) {
        return None;
    }
    let unsigned = text.trim_start_matches(['+', '-']);
    if unsigned.len() + 1 < text.len() {
        return None;
    }
    if unsigned.eq_ignore_ascii_case("inf") || unsigned.eq_ignore_ascii_case("infinity") {
        return Some(if text.starts_with('-') {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        });
    }
    let (value, decimal) = match parse_hex_float(text) {
        Some(value) => (value, false),
        None => {
            // Rust reads `inf` and `nan` too, but those were dealt with or
            // are refused: only digits, a point, an exponent.
            if !unsigned
                .bytes()
                .all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'e' | b'E' | b'+' | b'-'))
            {
                return None;
            }
            (text.parse::<f64>().ok()?, true)
        }
    };
    let rounded_away = value.is_infinite() || (value == 0.0 && !is_zero_written(unsigned));
    if !rounded_away {
        return Some(value);
    }
    match decimal.then(|| decimal_shape(unsigned)).flatten() {
        Some((digits, q)) if digits <= 19 && (-342..=308).contains(&q) => Some(value),
        _ => None,
    }
}

/// Whether `text`, a number without its sign, is zero as written: no digit
/// of its mantissa is other than 0.
fn is_zero_written(text: &str) -> bool {
    let mantissa = match text.find(['e', 'E', 'p', 'P']) {
        Some(at) => &text[..at],
        None => text,
    };
    let mantissa = mantissa
        .strip_prefix("0x")
        .or_else(|| mantissa.strip_prefix("0X"))
        .unwrap_or(mantissa);
    mantissa.bytes().all(|b| matches!(b, b'0' | b'.'))
}

/// A decimal number without its sign as `fast_float` sees it: how many
/// significant digits it has (leading zeros are not, trailing ones are),
/// and `q`, the power of ten that its digits are multiplied by. `None` if
/// it is no number.
fn decimal_shape(text: &str) -> Option<(usize, i64)> {
    let (mantissa, exponent) = match text.find(['e', 'E']) {
        Some(at) => (&text[..at], &text.as_bytes()[at + 1..]),
        None => (text, &b""[..]),
    };
    let exponent = if exponent.is_empty() {
        0
    } else {
        let (negative, digits) = match exponent[0] {
            b'-' => (true, &exponent[1..]),
            b'+' => (false, &exponent[1..]),
            _ => (false, exponent),
        };
        if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
            return None;
        }
        let value = digits.iter().fold(0i64, |acc, &d| {
            (acc * 10 + i64::from(d - b'0')).min(1 << 40)
        });
        if negative { -value } else { value }
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let all = whole.bytes().chain(fraction.bytes());
    let significant = all.skip_while(|&b| b == b'0').count();
    Some((significant, exponent - fraction.len() as i64))
}

/// One end of a lexicographic range: `-`, `+`, `[member` or `(member`.
#[derive(Clone, Debug, PartialEq)]
enum LexBound {
    /// `-`: before every member.
    Lowest,
    /// `+`: after every member.
    Highest,
    Inclusive(Vec<u8>),
    Exclusive(Vec<u8>),
}

fn parse_lex_bound(raw: &[u8]) -> Result<LexBound, Vec<u8>> {
    match raw {
        b"-" => Ok(LexBound::Lowest),
        b"+" => Ok(LexBound::Highest),
        [b'[', member @ ..] => Ok(LexBound::Inclusive(member.to_vec())),
        [b'(', member @ ..] => Ok(LexBound::Exclusive(member.to_vec())),
        _ => Err(resp_error("ERR min or max not valid string range item")),
    }
}

/// Whether `member` is in `min ..= max`. `+` as the lower end, or `-` as
/// the upper, is a range with nothing in it.
fn lex_in_range(member: &[u8], min: &LexBound, max: &LexBound) -> bool {
    let above = match min {
        LexBound::Lowest => true,
        LexBound::Highest => false,
        LexBound::Inclusive(bound) => member >= bound.as_slice(),
        LexBound::Exclusive(bound) => member > bound.as_slice(),
    };
    let below = match max {
        LexBound::Highest => true,
        LexBound::Lowest => false,
        LexBound::Inclusive(bound) => member <= bound.as_slice(),
        LexBound::Exclusive(bound) => member < bound.as_slice(),
    };
    above && below
}

/// Ranks `start..end` that `LIMIT offset count` leaves of ranks `lo..hi`
/// (`lo <= hi`): counted from `lo`, or for `rev` from `hi` down. A count of
/// `usize::MAX` is no limit; an offset of `usize::MAX` (a negative one)
/// leaves nothing.
fn limit_window(lo: usize, hi: usize, offset: usize, count: usize, rev: bool) -> (usize, usize) {
    if rev {
        let end = hi.saturating_sub(offset).max(lo);
        (end.saturating_sub(count).max(lo), end)
    } else {
        let start = lo.saturating_add(offset).min(hi);
        (start, start.saturating_add(count).min(hi))
    }
}

/// The `(score, member)` bounds of `min ..= max` in a set whose members are
/// all at one score: `Some(None)` if the range holds nothing (`+` as the
/// lower end, `-` as the upper). `None` if the set is empty or its scores
/// differ: then each member is compared with the bounds, in the order of
/// the set.
fn one_score_lex_bounds<'b>(
    inner: &SortedSetInner,
    min: &'b LexBound,
    max: &'b LexBound,
) -> Option<Option<(Bound<Probe<'b>>, Bound<Probe<'b>>)>> {
    let (low, high) = inner.score_ends()?;
    if low != high {
        return None;
    }
    let at = |member: &'b [u8]| Probe { score: low, member };
    let from = match min {
        LexBound::Lowest => Bound::Unbounded,
        LexBound::Highest => return Some(None),
        LexBound::Inclusive(bound) => Bound::Included(at(bound)),
        LexBound::Exclusive(bound) => Bound::Excluded(at(bound)),
    };
    let to = match max {
        LexBound::Highest => Bound::Unbounded,
        LexBound::Lowest => return Some(None),
        LexBound::Inclusive(bound) => Bound::Included(at(bound)),
        LexBound::Exclusive(bound) => Bound::Excluded(at(bound)),
    };
    Some(Some((from, to)))
}

/// The error for a range whose ends cannot be read, if they cannot. Asked
/// before the key is looked at: a bad range is an error whether or not the
/// key exists.
fn range_error(p: &RangeParams) -> Option<Vec<u8>> {
    if p.by_score {
        for end in [p.start, p.stop] {
            if let Err(e) = parse_score_bound(end) {
                return Some(e);
            }
        }
    } else if p.by_lex {
        for end in [p.start, p.stop] {
            if let Err(e) = parse_lex_bound(end) {
                return Some(e);
            }
        }
    } else {
        for end in [p.start, p.stop] {
            if crate::member_set::canonical_i64(end).is_none() {
                return Some(resp_error("ERR value is not an integer or out of range"));
            }
        }
    }
    None
}

/// `[+-]0x<hex>[.<hex>][p[+-]<dec>]`
fn parse_hex_float(text: &str) -> Option<f64> {
    let (negative, rest) = match text.as_bytes().first()? {
        b'-' => (true, &text[1..]),
        b'+' => (false, &text[1..]),
        _ => (false, text),
    };
    let rest = rest
        .strip_prefix("0x")
        .or_else(|| rest.strip_prefix("0X"))?;
    let (mantissa, exponent) = match rest.find(['p', 'P']) {
        Some(at) => (&rest[..at], parse_binary_exponent(&rest[at + 1..])?),
        None => (rest, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    // The value is `bits × 2^exp`, and a little more if `sticky`: the
    // first 60 bits or so of the digits, exactly, and whether any digit
    // after them is not 0. Rounded once, at the end, as `strtod` rounds.
    let mut bits = 0u64;
    let mut exp = exponent;
    let mut sticky = false;
    for (digit, in_fraction) in whole
        .chars()
        .map(|c| (c, false))
        .chain(fraction.chars().map(|c| (c, true)))
    {
        let digit = u64::from(digit.to_digit(16)?);
        if bits >> 56 == 0 {
            bits = bits << 4 | digit;
            if in_fraction {
                exp -= 4;
            }
        } else {
            sticky |= digit != 0;
            if !in_fraction {
                exp += 4;
            }
        }
    }
    let value = round_to_double(bits, exp, sticky);
    Some(if negative { -value } else { value })
}

/// The decimal exponent after `p`: a sign and at least one digit. Past a
/// million it is a million, which is past the range either way.
fn parse_binary_exponent(text: &str) -> Option<i64> {
    let (negative, digits) = match text.as_bytes().first()? {
        b'-' => (true, &text[1..]),
        b'+' => (false, &text[1..]),
        _ => (false, text),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let value = digits.bytes().fold(0i64, |acc, d| {
        (acc * 10 + i64::from(d - b'0')).min(1_000_000)
    });
    Some(if negative { -value } else { value })
}

/// `bits × 2^exp`, and a little more if `sticky`, as the nearest double,
/// ties to even: infinite past the largest, zero under half the smallest
/// subnormal. `bits` has at least two bits more than a double keeps, if
/// `sticky`.
fn round_to_double(bits: u64, exp: i64, sticky: bool) -> f64 {
    if bits == 0 {
        return 0.0;
    }
    let width = 64 - i64::from(bits.leading_zeros());
    // bits × 2^exp < 2^top.
    let top = exp + width;
    if top > 1024 {
        return f64::INFINITY;
    }
    if top < -1074 {
        return 0.0;
    }
    // The exponent of the lowest bit kept: 53 bits, or fewer below the
    // normal range.
    let lowest = (top - 53).max(-1074);
    let mut kept = if lowest <= exp {
        bits << (exp - lowest)
    } else {
        let shift = (lowest - exp) as u32;
        let kept = if shift >= 64 { 0 } else { bits >> shift };
        let half = bits >> (shift - 1) & 1 == 1;
        let below = sticky || bits & ((1u64 << (shift - 1)) - 1) != 0;
        if half && (below || kept & 1 == 1) {
            kept + 1
        } else {
            kept
        }
    };
    let mut lowest = lowest;
    if kept == 1 << 53 {
        kept >>= 1;
        lowest += 1;
    }
    if kept >= 1 << 52 {
        // Normal: the leading bit is implied.
        let biased = lowest + 52 + 1023;
        if biased >= 2047 {
            return f64::INFINITY;
        }
        f64::from_bits((biased as u64) << 52 | (kept & ((1 << 52) - 1)))
    } else {
        // Subnormal (or zero): `lowest` is -1074.
        f64::from_bits(kept)
    }
}

/// Parse a Redis score bound like "5.0", "(5.0", "-inf", "+inf"
/// Returns (value, exclusive) — the bool is true if the paren form was used.
///
/// Read as Redis's `zslParseRange` reads it, which is not how a score is
/// read (`parse_score`, Redis's `string2d`): with `fast_float_strtod`, which
/// is `strtod` wherever its fast path does not read the whole bound. Only
/// what is left after the number, or NaN, is refused. So an empty bound is
/// 0, as `(` alone is an open 0: nothing is read, and nothing is left. A
/// number too large for a double is infinite, where a score is refused for
/// it. Space before the number is skipped, as `strtod` skips it, and
/// hexadecimal is read; space after it is left over (#413).
fn parse_score_bound(raw: &[u8]) -> Result<(f64, bool), Vec<u8>> {
    let (exclusive, number) = match raw.split_first() {
        Some((b'(', rest)) => (true, rest),
        _ => (false, raw),
    };
    // `strtod` reads a C string, which ends at a NUL.
    let number = number.split(|&b| b == 0).next().unwrap_or_default();
    let value = if number.is_empty() {
        Some(0.0)
    } else {
        std::str::from_utf8(number)
            .ok()
            .and_then(|text| {
                let text = text.trim_start_matches([' ', '\t', '\n', '\x0b', '\x0c', '\r']);
                parse_hex_float(text).or_else(|| text.parse::<f64>().ok())
            })
            .filter(|value| !value.is_nan())
    };
    value
        .map(|value| (value, exclusive))
        .ok_or_else(|| resp_error("ERR min or max is not a float"))
}

type ScoreKey = (OrderedFloat<f64>, ZMember);

/// The part of `by_score` with scores from `min` to `max`, each a bound
/// from [`parse_score_bound`]; `None` when no score can be in it.
///
/// No member sorts before the empty one, so `(score, "")` is the first key
/// of a score. An open end moves to the next float, which keeps both ends
/// on such a key and leaves nothing for the caller to filter out.
fn score_range(
    (min, min_excl): (f64, bool),
    (max, max_excl): (f64, bool),
) -> Option<(Bound<ScoreKey>, Bound<ScoreKey>)> {
    let first = if min_excl {
        if min == f64::INFINITY {
            return None;
        }
        min.next_up()
    } else {
        min
    };
    // The least score past the range, if there is one.
    let past = if max_excl {
        Some(max)
    } else if max == f64::INFINITY {
        None
    } else {
        Some(max.next_up())
    };
    if past.is_some_and(|past| first > past) {
        return None;
    }
    let least: ZMember = Arc::from(&[][..]);
    let lower = if first == f64::NEG_INFINITY {
        Bound::Unbounded
    } else {
        Bound::Included((OrderedFloat(first), Arc::clone(&least)))
    };
    let upper = match past {
        Some(past) => Bound::Excluded((OrderedFloat(past), least)),
        None => Bound::Unbounded,
    };
    Some((lower, upper))
}

fn zcount(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() != 4 {
        return resp_error("ERR wrong number of arguments for 'zcount' command");
    }
    let key: &[u8] = cmd[1];
    let min = match parse_score_bound(cmd[2]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let max = match parse_score_bound(cmd[3]) {
        Ok(v) => v,
        Err(e) => return e,
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_integer(0),
        Some(ValueRef::SortedSet(zs)) => {
            let inner = zs.read();
            let count = match score_range(min, max) {
                None => 0,
                Some((Bound::Unbounded, Bound::Unbounded)) => inner.len(),
                Some(range) => inner.count_by_score(range),
            };
            resp_integer(count as i64)
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

fn zincrby(ctx: &mut Ctx) -> Vec<u8> {
    // ZINCRBY key increment member → new score as bulk string
    let cmd = &ctx.cmd;
    if cmd.len() != 4 {
        return resp_error("ERR wrong number of arguments for 'zincrby' command");
    }
    let key: Vec<u8> = cmd[1].to_vec();
    let Some(incr) = parse_score(cmd[2]) else {
        return resp_error("ERR value is not a valid float");
    };
    let member: &[u8] = cmd[3];

    let resp = reply_resp(ctx);
    let written = write_zset(ctx, &key, |inner| {
        // A new member's score is the increment itself: `-0` too, as Redis
        // replies (#422). It is stored as 0 (`SortedSetInner::upsert`).
        inner.upsert(member, |old| {
            let new_score = old.map_or(incr, |old| old + incr);
            if new_score.is_nan() {
                return (
                    None,
                    Err(resp_error("ERR resulting score is not a number (NaN)")),
                );
            }
            (Some(new_score), Ok(new_score))
        })
    });
    match written {
        Ok(Ok(new_score)) => score_reply(resp, new_score),
        Err(e) | Ok(Err(e)) => e,
    }
}

fn zrem(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'zrem' command");
    }
    let key: &[u8] = cmd[1];

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_integer(0),
        Some(ValueRef::SortedSet(zs)) => {
            let mut inner = zs.write();
            let mut removed = 0i64;
            for m in &cmd[2..] {
                if inner.remove(m) {
                    removed += 1;
                }
            }
            crate::sorted_set::after_write(zs, &mut inner);
            resp_integer(removed)
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

fn zrange(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'zrange' command");
    }
    let key: &[u8] = cmd[1];

    let RangeOptions {
        by_score,
        by_lex,
        rev,
        with_scores,
        limit_offset,
        limit_count,
    } = match range_options(cmd, 4, false) {
        Ok(options) => options,
        Err(e) => return e,
    };

    let params = RangeParams {
        start: cmd[2],
        stop: cmd[3],
        by_score,
        by_lex,
        rev,
        limit_offset,
        limit_count,
    };

    if let Some(error) = range_error(&params) {
        return error;
    }
    let resp = reply_resp(ctx);
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_array(vec![]),
        Some(ValueRef::SortedSet(zs)) => {
            let inner = zs.read();
            let pairs = zrange_pairs(&inner, &params);
            range_reply(&pairs, resp, with_scores)
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

fn zpopmin(ctx: &mut Ctx) -> Vec<u8> {
    // ZPOPMIN key [count] → array of [member, score, ...]
    zset_pop(ctx, false)
}

fn zpopmax(ctx: &mut Ctx) -> Vec<u8> {
    // ZPOPMAX key [count] → array of [member, score, ...]
    zset_pop(ctx, true)
}

fn zset_pop(ctx: &mut Ctx, max: bool) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() < 2 || cmd.len() > 3 {
        return resp_error("ERR wrong number of arguments");
    }
    let key: &[u8] = cmd[1];
    let count = match cmd.get(2) {
        None => 1,
        Some(raw) => match crate::member_set::canonical_i64(raw) {
            Some(n) if n >= 0 => n as usize,
            Some(_) => return resp_error("ERR value is out of range, must be positive"),
            None => return resp_error("ERR value is not an integer or out of range"),
        },
    };

    // Redis nests each `[member, score]` pair only when RESP3 is negotiated
    // *and* an explicit count was supplied
    // (`use_nested_array = (c->resp > 2 && count != -1)` in zpopMinMaxCommand).
    // The countless form stays a flat 2-element reply under both protocols, so
    // a RESP3 client still gets `[member, score]` rather than `[[member, score]]`.
    let resp = reply_resp(ctx);
    let nest = resp >= 3 && cmd.len() == 3;

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_array(vec![]),
        Some(ValueRef::SortedSet(zs)) => {
            let mut inner = zs.write();
            let popping = count.min(inner.len());
            // Into one buffer: a vector for the member, one for the score
            // and one for the pair were made and joined, for every pop. Each
            // member is written from where the set holds it, then taken out.
            let mut out = Vec::with_capacity(16 + popping * 48);
            pairs_header(&mut out, popping, true, nest);
            for _ in 0..popping {
                inner.pop_with(max, |member, score| {
                    push_pair(&mut out, member, score, resp, true, nest)
                });
            }
            crate::sorted_set::after_write(zs, &mut inner);
            out
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

fn zscan(ctx: &mut Ctx) -> Vec<u8> {
    // ZSCAN key cursor [MATCH pattern] [COUNT count] [NOSCORES]
    // Returns [next_cursor, [member, score, member, score, ...]]
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'zscan' command");
    }
    let key: &[u8] = cmd[1];
    let cursor = match crate::commands::helpers::scan_cursor(cmd, 2) {
        Ok(cursor) => cursor,
        Err(e) => return e,
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_array(vec![resp_bulk_str("0"), resp_array(vec![])]),
        Some(ValueRef::SortedSet(zs)) => {
            // The options are read once the key is known to be a sorted set,
            // as Redis does: a missing key is an empty scan whatever they say.
            let options = match crate::commands::helpers::scan_options(cmd, 3, &["NOSCORES"]) {
                Ok(options) => options,
                Err(e) => return e,
            };
            let pattern = options
                .pattern
                .as_deref()
                .map(crate::commands::glob_redis::Glob::new);
            // The set's read lock, once: the form is told from this guard. A
            // second read on this thread (the form looked up again, as
            // `generics::scans_whole` once did) waits behind a writer queued
            // since the first, which waits for the first: a deadlock.
            let inner = zs.read();
            // A table (#875) gives the cursor itself: a page reads its own
            // groups, and `COUNT` is the hint it is in Redis (#770). A
            // cursor of the earlier form goes on from that position.
            if let Some(table) = inner.table() {
                return crate::commands::table_scan::zscan_reply(
                    table,
                    cursor,
                    options.count,
                    pattern.as_ref(),
                    options.no_scores,
                );
            }
            // A block comes back whole, in order, as Redis returns a
            // listpack.
            let next_cursor = 0;
            let mut items: Vec<Vec<u8>> = Vec::with_capacity(inner.len() * 2);
            for (member, score) in inner.iter() {
                if pattern.as_ref().is_some_and(|glob| !glob.matches(&member)) {
                    continue;
                }
                // ZSCAN keeps the flat, all-bulk-string shape under RESP3
                // too. Redis's `scanCallback` renders zset scores with
                // `d2string` and emits them via `addReplyBulk` regardless of
                // protocol, so this is intentionally *not* converted to
                // doubles or nested pairs.
                items.push(resp_bulk(member.as_ref()));
                if !options.no_scores {
                    items.push(resp_bulk_str(&format_score(score)));
                }
            }

            resp_array(vec![
                resp_bulk_str(&next_cursor.to_string()),
                resp_array(items),
            ])
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

fn zunion(ctx: &mut Ctx) -> Vec<u8> {
    // ZUNION numkeys key [key ...] [WEIGHTS w [w ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'zunion' command");
    }
    let num_keys = match numkeys(cmd, 1, "zunion") {
        Ok(n) => n,
        Err(e) => return e,
    };

    let with_scores = cmd.iter().any(|b| {
        std::str::from_utf8(b)
            .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
            .unwrap_or(false)
    });

    let (weights, agg, count_only) = match parse_weights_aggregate(cmd, 2 + num_keys, num_keys) {
        Ok(v) => v,
        Err(e) => return e,
    };

    // Load all sets and apply weights
    // Rust lesson: AHashMap is used here (not BTreeMap) because we need fast
    // membership lookup while merging. We sort at the end for output only.
    let mut result: ahash::AHashMap<Vec<u8>, f64> = ahash::AHashMap::new();

    for (idx, key_bytes) in cmd[2..2 + num_keys].iter().enumerate() {
        let key: &[u8] = key_bytes;
        let snapshot = match read_zset_snapshot(ctx, key) {
            Ok(Some(s)) => s,
            Ok(None) => continue, // missing key contributes nothing to union
            Err(e) => return e,
        };
        let weight = weights[idx];
        for (member, score) in snapshot {
            let weighted = contribution(score, weight, count_only);
            result
                .entry(member)
                .and_modify(|existing| *existing = agg(*existing, weighted))
                .or_insert(weighted);
        }
    }

    // Sort by score then member for deterministic output
    let mut sorted: Vec<(Vec<u8>, f64)> = result.into_iter().collect();
    sorted.sort_by(|(ma, sa), (mb, sb)| OrderedFloat(*sa).cmp(&OrderedFloat(*sb)).then(ma.cmp(mb)));

    let resp = reply_resp(ctx);
    let mut items: Vec<Vec<u8>> = Vec::with_capacity(sorted.len() * 2);
    for (member, score) in sorted.iter() {
        push_member_score(&mut items, resp, member, with_scores.then_some(*score));
    }

    resp_array(items)
}

fn zunionstore(ctx: &mut Ctx) -> Vec<u8> {
    // ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...]
    // Same as ZUNION but writes result to destination key, returns count.
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'zunionstore' command");
    }
    let dst: Vec<u8> = cmd[1].to_vec();
    let num_keys = match numkeys(cmd, 2, "zunionstore") {
        Ok(n) => n,
        Err(e) => return e,
    };
    if cmd.iter().any(|a| a.eq_ignore_ascii_case(b"WITHSCORES")) {
        return resp_error("ERR syntax error");
    }

    let (weights, agg, count_only) = match parse_weights_aggregate(cmd, 3 + num_keys, num_keys) {
        Ok(v) => v,
        Err(e) => return e,
    };

    let mut result: ahash::AHashMap<Vec<u8>, f64> = ahash::AHashMap::new();
    for (idx, key_bytes) in cmd[3..3 + num_keys].iter().enumerate() {
        let key: &[u8] = key_bytes;
        let snapshot = match read_zset_snapshot(ctx, key) {
            Ok(Some(s)) => s,
            Ok(None) => continue,
            Err(e) => return e,
        };
        let weight = weights[idx];
        for (member, score) in snapshot {
            let weighted = contribution(score, weight, count_only);
            result
                .entry(member)
                .and_modify(|existing| *existing = agg(*existing, weighted))
                .or_insert(weighted);
        }
    }

    let count = result.len() as i64;
    store_zset(ctx, &dst, result);
    resp_integer(count)
}

fn zinter(ctx: &mut Ctx) -> Vec<u8> {
    // ZINTER numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...] [WITHSCORES]
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'zinter' command");
    }
    let num_keys = match numkeys(cmd, 1, "zinter") {
        Ok(n) => n,
        Err(e) => return e,
    };
    let with_scores = cmd.iter().any(|b| {
        std::str::from_utf8(b)
            .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
            .unwrap_or(false)
    });
    let (weights, agg, count_only) = match parse_weights_aggregate(cmd, 2 + num_keys, num_keys) {
        Ok(v) => v,
        Err(e) => return e,
    };

    // Load all snapshots first
    let mut snapshots: Vec<std::collections::BTreeMap<Vec<u8>, f64>> = Vec::new();
    for key_bytes in &cmd[2..2 + num_keys] {
        let key: &[u8] = key_bytes;
        match read_zset_snapshot(ctx, key) {
            Ok(Some(s)) => snapshots.push(s),
            Ok(None) => return resp_array(vec![]), // any missing key → empty intersection
            Err(e) => return e,
        }
    }

    if snapshots.is_empty() {
        return resp_array(vec![]);
    }

    let result = intersect_weighted(&snapshots, &weights, agg, count_only);

    let mut sorted: Vec<(Vec<u8>, f64)> = result.into_iter().collect();
    sorted.sort_by(|(ma, sa), (mb, sb)| OrderedFloat(*sa).cmp(&OrderedFloat(*sb)).then(ma.cmp(mb)));

    let resp = reply_resp(ctx);
    let mut items: Vec<Vec<u8>> = Vec::with_capacity(sorted.len() * 2);
    for (member, score) in sorted.iter() {
        push_member_score(&mut items, resp, member, with_scores.then_some(*score));
    }

    resp_array(items)
}

fn zinterstore(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'zinterstore' command");
    }
    let dst: Vec<u8> = cmd[1].to_vec();
    let num_keys = match numkeys(cmd, 2, "zinterstore") {
        Ok(n) => n,
        Err(e) => return e,
    };
    if cmd.iter().any(|a| a.eq_ignore_ascii_case(b"WITHSCORES")) {
        return resp_error("ERR syntax error");
    }
    let (weights, agg, count_only) = match parse_weights_aggregate(cmd, 3 + num_keys, num_keys) {
        Ok(v) => v,
        Err(e) => return e,
    };

    let mut snapshots: Vec<std::collections::BTreeMap<Vec<u8>, f64>> = Vec::new();
    for key_bytes in &cmd[3..3 + num_keys] {
        let key: &[u8] = key_bytes;
        match read_zset_snapshot(ctx, key) {
            Ok(Some(s)) => snapshots.push(s),
            Ok(None) => {
                // Empty intersection — store empty zset, return 0
                store_zset(ctx, &dst, ahash::AHashMap::new());
                return resp_integer(0);
            }
            Err(e) => return e,
        }
    }

    if snapshots.is_empty() {
        store_zset(ctx, &dst, ahash::AHashMap::new());
        return resp_integer(0);
    }

    let result = intersect_weighted(&snapshots, &weights, agg, count_only);

    let count = result.len() as i64;
    store_zset(ctx, &dst, result);
    resp_integer(count)
}

fn zintercard(ctx: &mut Ctx) -> Vec<u8> {
    // ZINTERCARD numkeys key [key ...] [LIMIT limit]
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'zintercard' command");
    }
    let num_keys = match numkeys(cmd, 1, "zintercard") {
        Ok(n) => n,
        Err(e) => return e,
    };

    // After the keys: `LIMIT n`, or nothing. 0 is no limit.
    let mut limit = usize::MAX;
    match &cmd[2 + num_keys..] {
        [] => {}
        [word, n] if word.eq_ignore_ascii_case(b"LIMIT") => {
            match crate::member_set::canonical_i64(n) {
                Some(n) if n > 0 => limit = n as usize,
                Some(0) => {}
                _ => return resp_error("ERR LIMIT can't be negative"),
            }
        }
        _ => return resp_error("ERR syntax error"),
    }

    let mut snapshots: Vec<std::collections::BTreeMap<Vec<u8>, f64>> = Vec::new();
    for key_bytes in &cmd[2..2 + num_keys] {
        let key: &[u8] = key_bytes;
        match read_zset_snapshot(ctx, key) {
            Ok(Some(s)) => snapshots.push(s),
            Ok(None) => return resp_integer(0), // any missing = empty intersection
            Err(e) => return e,
        }
    }

    if snapshots.is_empty() {
        return resp_integer(0);
    }
    snapshots.sort_by_key(|s| s.len());

    let count = snapshots[0]
        .keys()
        .filter(|m| snapshots[1..].iter().all(|s| s.contains_key(*m)))
        .take(limit)
        .count();

    resp_integer(count as i64)
}

fn zdiff(ctx: &mut Ctx) -> Vec<u8> {
    // ZDIFF numkeys key [key ...] [WITHSCORES]
    // Members in first key that don't appear in any subsequent key.
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'zdiff' command");
    }
    let num_keys = match numkeys(cmd, 1, "zdiff") {
        Ok(n) => n,
        Err(e) => return e,
    };
    if cmd[1 + 1 + num_keys..]
        .iter()
        .any(|a| !a.eq_ignore_ascii_case(b"WITHSCORES"))
    {
        return resp_error("ERR syntax error");
    }
    let with_scores = cmd.iter().any(|b| {
        std::str::from_utf8(b)
            .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
            .unwrap_or(false)
    });

    let mut snapshots: Vec<Option<std::collections::BTreeMap<Vec<u8>, f64>>> = Vec::new();
    for key_bytes in &cmd[2..2 + num_keys] {
        let key: &[u8] = key_bytes;
        match read_zset_snapshot(ctx, key) {
            Ok(s) => snapshots.push(s),
            Err(e) => return e,
        }
    }

    // First set is the base; anything missing = empty result
    let base = match snapshots.into_iter().next().flatten() {
        Some(s) => s,
        None => return resp_array(vec![]),
    };

    // Load the rest as membership sets (we only need to check presence)
    let mut others: Vec<std::collections::BTreeMap<Vec<u8>, f64>> = Vec::new();
    for key_bytes in &cmd[3..2 + num_keys] {
        let key: &[u8] = key_bytes;
        if let Ok(Some(s)) = read_zset_snapshot(ctx, key) {
            others.push(s);
        }
    }

    let mut result: Vec<(Vec<u8>, f64)> = base
        .into_iter()
        .filter(|(member, _)| others.iter().all(|o| !o.contains_key(member)))
        .collect();

    result.sort_by(|(ma, sa), (mb, sb)| OrderedFloat(*sa).cmp(&OrderedFloat(*sb)).then(ma.cmp(mb)));

    let resp = reply_resp(ctx);
    let mut items: Vec<Vec<u8>> = Vec::with_capacity(result.len() * 2);
    for (member, score) in result.iter() {
        push_member_score(&mut items, resp, member, with_scores.then_some(*score));
    }

    resp_array(items)
}

fn zdiffstore(ctx: &mut Ctx) -> Vec<u8> {
    // ZDIFFSTORE destination numkeys key [key ...]
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'zdiffstore' command");
    }
    let dst: Vec<u8> = cmd[1].to_vec();
    let num_keys = match numkeys(cmd, 2, "zdiffstore") {
        Ok(n) => n,
        Err(e) => return e,
    };
    if cmd.len() > 2 + 1 + num_keys {
        return resp_error("ERR syntax error");
    }

    let base = match read_zset_snapshot(ctx, cmd[3]) {
        Ok(Some(s)) => s,
        Ok(None) => {
            store_zset(ctx, &dst, ahash::AHashMap::new());
            return resp_integer(0);
        }
        Err(e) => return e,
    };

    let mut others: Vec<std::collections::BTreeMap<Vec<u8>, f64>> = Vec::new();
    for key_bytes in &cmd[4..3 + num_keys] {
        let key: &[u8] = key_bytes;
        if let Ok(Some(s)) = read_zset_snapshot(ctx, key) {
            others.push(s);
        }
    }

    let result: ahash::AHashMap<Vec<u8>, f64> = base
        .into_iter()
        .filter(|(member, _)| others.iter().all(|o| !o.contains_key(member)))
        .collect();

    let count = result.len() as i64;
    store_zset(ctx, &dst, result);
    resp_integer(count)
}

fn zremrangebyscore(ctx: &mut Ctx) -> Vec<u8> {
    // ZREMRANGEBYSCORE key min max
    let cmd = &ctx.cmd;
    if cmd.len() != 4 {
        return resp_error("ERR wrong number of arguments for 'zremrangebyscore' command");
    }
    let key: &[u8] = cmd[1];
    let min = match parse_score_bound(cmd[2]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let max = match parse_score_bound(cmd[3]) {
        Ok(v) => v,
        Err(e) => return e,
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_integer(0),
        Some(ValueRef::SortedSet(zs)) => {
            let Some(range) = score_range(min, max) else {
                return resp_integer(0);
            };
            let mut inner = zs.write();
            let removed = inner.remove_range_by_score(range);
            crate::sorted_set::after_write(zs, &mut inner);
            resp_integer(removed as i64)
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

fn zremrangebyrank(ctx: &mut Ctx) -> Vec<u8> {
    // ZREMRANGEBYRANK key start stop  (0-based inclusive indices, negatives ok)
    let cmd = &ctx.cmd;
    if cmd.len() != 4 {
        return resp_error("ERR wrong number of arguments for 'zremrangebyrank' command");
    }
    let key: &[u8] = cmd[1];
    // Integers as Redis reads them, before the key is looked at: anything
    // else is an error, where it used to be taken as 0.
    let (Some(mut start), Some(mut stop)) = (
        crate::member_set::canonical_i64(cmd[2]),
        crate::member_set::canonical_i64(cmd[3]),
    ) else {
        return resp_error("ERR value is not an integer or out of range");
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_integer(0),
        Some(ValueRef::SortedSet(zs)) => {
            let mut inner = zs.write();
            let len = inner.len() as i64;
            if len == 0 {
                return resp_integer(0);
            }
            if start < 0 {
                start = (len + start).max(0);
            }
            if stop < 0 {
                stop += len;
            }
            if stop >= len {
                stop = len - 1;
            }
            if start > stop {
                return resp_integer(0);
            }

            let count = inner.remove_ranks(start as usize, stop as usize);
            crate::sorted_set::after_write(zs, &mut inner);
            resp_integer(count as i64)
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

fn zremrangebylex(ctx: &mut Ctx) -> Vec<u8> {
    // ZREMRANGEBYLEX key min max  (lex range, '[' inclusive, '(' exclusive, '-'/'+' bounds)
    let cmd = &ctx.cmd;
    if cmd.len() != 4 {
        return resp_error("ERR wrong number of arguments for 'zremrangebylex' command");
    }
    let key: &[u8] = cmd[1];
    let (min, max) = match (parse_lex_bound(cmd[2]), parse_lex_bound(cmd[3])) {
        (Ok(min), Ok(max)) => (min, max),
        (Err(e), _) | (_, Err(e)) => return e,
    };
    let lex_in_range = |member: &[u8]| lex_in_range(member, &min, &max);

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_integer(0),
        Some(ValueRef::SortedSet(zs)) => {
            let mut inner = zs.write();
            let count = inner.remove_where(|member, _| lex_in_range(member));
            crate::sorted_set::after_write(zs, &mut inner);
            resp_integer(count as i64)
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

fn zrandmember(ctx: &mut Ctx) -> Vec<u8> {
    // ZRANDMEMBER key [count [WITHSCORES]]
    // Positive count → distinct members (up to set size)
    // Negative count → |count| members with repetition allowed

    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return resp_error("ERR wrong number of arguments for 'zrandmember' command");
    }
    let key: &[u8] = cmd[1];

    // `[count [WITHSCORES]]`
    let count_arg: Option<i64> = match cmd.get(2) {
        None => None,
        Some(raw) => match crate::member_set::canonical_i64(raw) {
            // -i64::MAX ..= i64::MAX: the magnitude has to be a count.
            Some(n) if n != i64::MIN => Some(n),
            Some(_) => return resp_error("ERR value is out of range"),
            None => return resp_error("ERR value is not an integer or out of range"),
        },
    };
    let with_scores = match &cmd[cmd.len().min(3)..] {
        [] => false,
        [word] if word.eq_ignore_ascii_case(b"WITHSCORES") => true,
        _ => return resp_error("ERR syntax error"),
    };
    // Each member is two elements of the reply then, and the reply's length
    // has to fit.
    if with_scores && count_arg.is_some_and(|n| n.unsigned_abs() > (i64::MAX / 2) as u64) {
        return resp_error("ERR value is out of range");
    }

    let resp = reply_resp(ctx);
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => {
            if cmd.len() > 2 {
                resp_array(vec![])
            } else {
                resp_null_bulk()
            }
        }
        Some(ValueRef::SortedSet(zs)) => {
            let inner = zs.read();
            let len = inner.len();

            if len == 0 {
                return if cmd.len() > 2 {
                    resp_array(vec![])
                } else {
                    resp_null_bulk()
                };
            }

            // Every member is picked by `random` / `random_distinct`: a
            // block's by its index, the large form's by a random bucket of
            // its table. The work is the count asked for, not the size of
            // the set.
            let mut rng = rand::thread_rng();

            // No count argument → return single random member as bulk string
            let Some(count_arg) = count_arg else {
                return match inner.random(&mut rng) {
                    Some((member, _)) => resp_bulk(&member),
                    None => resp_null_bulk(),
                };
            };

            if count_arg >= 0 && (count_arg as u64) >= len as u64 {
                // As many as there are, or more: all of them, as Redis
                // answers, from the highest score down.
                let all: Vec<(Member<'_>, f64)> = inner.iter().rev().collect();
                range_reply(&all, resp, with_scores)
            } else if count_arg >= 0 {
                // Distinct members, in a random order.
                let picks = inner.random_distinct(count_arg as usize, &mut rng);
                range_reply(&picks, resp, with_scores)
            } else {
                // Any members, each picked on its own: written as they are
                // picked (a sparse table: the rest by one walk).
                let count = count_arg.unsigned_abs() as usize;
                let paired = with_scores && resp >= 3;
                let mut out =
                    Vec::with_capacity(16 + count.min(1 << 16) * if with_scores { 48 } else { 24 });
                pairs_header(&mut out, count, with_scores, paired);
                inner.random_each(count, &mut rng, |member, score| {
                    push_pair(&mut out, &member, score, resp, with_scores, paired);
                });
                out
            }
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

fn zlexcount(ctx: &mut Ctx) -> Vec<u8> {
    // ZLEXCOUNT key min max
    let cmd = &ctx.cmd;
    if cmd.len() != 4 {
        return resp_error("ERR wrong number of arguments for 'zlexcount' command");
    }
    let key: &[u8] = cmd[1];
    let (min, max) = match (parse_lex_bound(cmd[2]), parse_lex_bound(cmd[3])) {
        (Ok(min), Ok(max)) => (min, max),
        (Err(e), _) | (_, Err(e)) => return e,
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_integer(0),
        Some(ValueRef::SortedSet(zs)) => {
            let count = lex_count(&zs.read(), &min, &max);
            resp_integer(count as i64)
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

/// How many members are in `min ..= max` (`ZLEXCOUNT`): of a set at one
/// score, two ranks apart; else each member compared.
fn lex_count(inner: &SortedSetInner, min: &LexBound, max: &LexBound) -> usize {
    match one_score_lex_bounds(inner, min, max) {
        Some(Some((from, to))) => {
            let (lo, hi) = inner.rank_range(from, to);
            hi - lo
        }
        Some(None) => 0,
        None => inner
            .iter()
            .filter(|(member, _)| lex_in_range(member.as_ref(), min, max))
            .count(),
    }
}

fn zrangestore(ctx: &mut Ctx) -> Vec<u8> {
    // ZRANGESTORE dst src min max [BYSCORE | BYLEX] [REV] [LIMIT offset count]
    let cmd = &ctx.cmd;
    if cmd.len() < 5 {
        return resp_error("ERR wrong number of arguments for 'zrangestore' command");
    }
    let dst: Vec<u8> = cmd[1].to_vec();
    let src: &[u8] = cmd[2];

    let RangeOptions {
        by_score,
        by_lex,
        rev,
        limit_offset,
        limit_count,
        ..
    } = match range_options(cmd, 5, true) {
        Ok(options) => options,
        Err(e) => return e,
    };

    let params = RangeParams {
        start: cmd[3],
        stop: cmd[4],
        by_score,
        by_lex,
        rev,
        limit_offset,
        limit_count,
    };

    if let Some(error) = range_error(&params) {
        return error;
    }
    // Read src, apply range, build dst from it — no RESP round-trip, and
    // no copy of a member but the destination's own.
    let stored = {
        let db_ref = &ctx.store[*ctx.selected_db];
        let shard = store::get_shard(db_ref, src);
        let map = read_lock!(shard);

        match map.get(src) {
            None => SortedSetInner::new(),
            Some(ValueRef::SortedSet(zs)) => {
                let inner = zs.read();
                SortedSetInner::from_pairs(zrange_pairs(&inner, &params))
            }
            Some(_) => {
                return resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                );
            }
        }
    }; // ← read lock on src is dropped here before we take write lock on dst

    // An empty result leaves no destination — whatever was there goes, as
    // with every STORE command.
    let count = stored.len() as i64;
    store_built(ctx, &dst, stored);
    resp_integer(count)
}

/// Read a SortedSetInner snapshot out of the store for a given key.
/// Returns None if key doesn't exist, Err if wrong type.
fn read_zset_snapshot(
    ctx: &Ctx,
    key: &[u8],
) -> Result<Option<std::collections::BTreeMap<Vec<u8>, f64>>, Vec<u8>> {
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => Ok(None),
        Some(ValueRef::SortedSet(zs)) => {
            // Clone member→score into a plain BTreeMap for manipulation.
            // We use Vec<u8> keys here (not Arc) since we're working with owned
            // data outside the lock — the lock is dropped after this block.
            let inner = zs.read();
            let snapshot: std::collections::BTreeMap<Vec<u8>, f64> = inner
                .iter_unordered()
                .map(|(k, v)| (k.as_ref().to_vec(), v))
                .collect();
            Ok(Some(snapshot))
        }
        Some(ValueRef::Set(set)) => Ok(Some(
            set.read()
                .iter()
                .map(|member| (member.as_ref().to_vec(), 1.0))
                .collect(),
        )),
        Some(_) => Err(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
    }
}

type WeightsAggregate = Result<(Vec<f64>, fn(f64, f64) -> f64, bool), Vec<u8>>;

/// What one input set contributes for a member it holds.
///
/// Normally the member's score times the set's weight. Under
/// `AGGREGATE COUNT` (Redis 8.8) scores are ignored: each set containing the
/// member contributes its weight — 1 by default — so the result counts how
/// many of the inputs the member appears in.
#[inline]
fn contribution(score: f64, weight: f64, count_only: bool) -> f64 {
    if count_only {
        weight
    } else {
        let value = score * weight;
        if value.is_nan() { 0.0 } else { value }
    }
}

/// Intersect `snapshots` (one per input key, in key order), combining each
/// surviving member's contributions with `agg`.
///
/// The smallest set is walked first, but each set keeps the weight given for
/// *its* key. The sets used to be sorted by size while the weights stayed in
/// key order, so `ZINTER 2 big small WEIGHTS 2 3` applied 2 to `small` and 3
/// to `big` whenever the second set was the smaller one.
fn intersect_weighted(
    snapshots: &[std::collections::BTreeMap<Vec<u8>, f64>],
    weights: &[f64],
    agg: fn(f64, f64) -> f64,
    count_only: bool,
) -> ahash::AHashMap<Vec<u8>, f64> {
    let mut sets: Vec<(&std::collections::BTreeMap<Vec<u8>, f64>, f64)> =
        snapshots.iter().zip(weights.iter().copied()).collect();
    sets.sort_by_key(|(set, _)| set.len());
    let Some(((first, first_weight), rest)) = sets.split_first() else {
        return ahash::AHashMap::new();
    };

    let mut result: ahash::AHashMap<Vec<u8>, f64> = first
        .iter()
        .map(|(member, &score)| {
            (
                member.clone(),
                contribution(score, *first_weight, count_only),
            )
        })
        .collect();
    for (set, weight) in rest {
        result.retain(|member, accumulated| match set.get(member) {
            None => false,
            Some(&score) => {
                *accumulated = agg(*accumulated, contribution(score, *weight, count_only));
                true
            }
        });
        if result.is_empty() {
            break;
        }
    }
    result
}

/// Parse WEIGHTS and AGGREGATE options shared by ZINTER/ZUNION/ZDIFF.
/// Returns (weights, aggregate_fn, count_only): aggregate_fn combines two
/// contributions, and count_only says whether a contribution is the set's
/// weight rather than the weighted score (see [`contribution`]).
/// `numkeys` at `cmd[at]`, with the keys after it: at least one key, and
/// no more than there are arguments left.
fn numkeys(cmd: &[&[u8]], at: usize, command: &str) -> Result<usize, Vec<u8>> {
    let n = cmd
        .get(at)
        .and_then(|raw| crate::member_set::canonical_i64(raw))
        .ok_or_else(|| resp_error("ERR value is not an integer or out of range"))?;
    if n < 1 {
        return Err(resp_error(&format!(
            "ERR at least 1 input key is needed for '{command}' command"
        )));
    }
    if n as usize > cmd.len() - at - 1 {
        return Err(resp_error("ERR syntax error"));
    }
    Ok(n as usize)
}

fn parse_weights_aggregate(cmd: &[&[u8]], start: usize, num_keys: usize) -> WeightsAggregate {
    let mut weights: Vec<f64> = vec![1.0; num_keys];
    // aggregate: "SUM" | "MIN" | "MAX" | "COUNT" — default SUM
    fn sum(a: f64, b: f64) -> f64 {
        let total = a + b;
        // inf + -inf
        if total.is_nan() { 0.0 } else { total }
    }
    let mut agg: fn(f64, f64) -> f64 = sum;
    let mut count_only = false;

    let mut i = start;
    while i < cmd.len() {
        match std::str::from_utf8(cmd[i])
            .unwrap_or("")
            .to_ascii_uppercase()
            .as_str()
        {
            "WEIGHTS" => {
                i += 1;
                for w in weights.iter_mut() {
                    let v: f64 = parse_score(cmd.get(i).copied().unwrap_or(b""))
                        .ok_or_else(|| resp_error("ERR weight value is not a float"))?;
                    *w = v;
                    i += 1;
                }
            }
            "AGGREGATE" => {
                i += 1;
                match std::str::from_utf8(cmd.get(i).copied().unwrap_or(b""))
                    .unwrap_or("")
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "SUM" => {
                        agg = sum;
                        count_only = false;
                    }
                    "MIN" => {
                        agg = |a: f64, b: f64| a.min(b);
                        count_only = false;
                    }
                    "MAX" => {
                        agg = |a: f64, b: f64| a.max(b);
                        count_only = false;
                    }
                    "COUNT" => {
                        agg = sum;
                        count_only = true;
                    }
                    _ => return Err(resp_error("ERR syntax error")),
                }
                i += 1;
            }
            "WITHSCORES" => i += 1,
            _ => return Err(resp_error("ERR syntax error")),
        }
    }
    Ok((weights, agg, count_only))
}

/// Write a HashMap<member_bytes, score> into the store as a SortedSet.
/// Overwrites whatever was at dst_key — matches Redis STORE semantics: the
/// destination's previous value and TTL go, and an empty result leaves no key
/// behind rather than an empty sorted set.
///
/// Built whole, in the form its size and members call for: a block, laid
/// down once, if they fit one.
fn store_zset(ctx: &mut Ctx, dst_key: &[u8], members: ahash::AHashMap<Vec<u8>, f64>) {
    store_built(ctx, dst_key, SortedSetInner::from_pairs(members));
}

/// [`store_zset`] of a set already built.
fn store_built(ctx: &mut Ctx, dst_key: &[u8], inner: SortedSetInner) {
    if inner.is_empty() {
        crate::commands::helpers::delete_key(ctx.store, *ctx.selected_db, dst_key);
        return;
    }
    let zs = crate::sorted_set::shared(inner);
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, dst_key);
    let replaced = write_lock!(shard).insert_expiring(dst_key, KiviDBValue::SortedSet(zs), None);
    drop(replaced);
}

fn zrangebyscore(ctx: &mut Ctx) -> Vec<u8> {
    // ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'zrangebyscore' command");
    }
    let key: &[u8] = cmd[1];

    let mut with_scores = false;
    let mut limit_offset = 0usize;
    let mut limit_count = usize::MAX;
    let mut i = 4usize;
    while i < cmd.len() {
        match std::str::from_utf8(cmd[i])
            .unwrap_or("")
            .to_ascii_uppercase()
            .as_str()
        {
            "WITHSCORES" => {
                with_scores = true;
                i += 1;
            }
            "LIMIT" if i + 2 < cmd.len() => {
                (limit_offset, limit_count) = match parse_limit(cmd[i + 1], cmd[i + 2]) {
                    Ok(limit) => limit,
                    Err(e) => return e,
                };
                i += 3;
            }
            _ => return resp_error("ERR syntax error"),
        }
    }

    let params = RangeParams {
        start: cmd[2], // min
        stop: cmd[3],  // max
        by_score: true,
        by_lex: false,
        rev: false,
        limit_offset,
        limit_count,
    };

    execute_range(ctx, key, &params, with_scores)
}

fn zrevrangebyscore(ctx: &mut Ctx) -> Vec<u8> {
    // ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
    // Note: argument order is reversed — max before min
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'zrevrangebyscore' command");
    }
    let key: &[u8] = cmd[1];

    let mut with_scores = false;
    let mut limit_offset = 0usize;
    let mut limit_count = usize::MAX;
    let mut i = 4usize;
    while i < cmd.len() {
        match std::str::from_utf8(cmd[i])
            .unwrap_or("")
            .to_ascii_uppercase()
            .as_str()
        {
            "WITHSCORES" => {
                with_scores = true;
                i += 1;
            }
            "LIMIT" if i + 2 < cmd.len() => {
                (limit_offset, limit_count) = match parse_limit(cmd[i + 1], cmd[i + 2]) {
                    Ok(limit) => limit,
                    Err(e) => return e,
                };
                i += 3;
            }
            _ => return resp_error("ERR syntax error"),
        }
    }

    let params = RangeParams {
        start: cmd[2], // this is max in ZREVRANGEBYSCORE syntax
        stop: cmd[3],  // this is min
        by_score: true,
        by_lex: false,
        rev: true, // zrange_pairs swaps min/max when rev=true
        limit_offset,
        limit_count,
    };

    execute_range(ctx, key, &params, with_scores)
}

fn zrevrange(ctx: &mut Ctx) -> Vec<u8> {
    // ZREVRANGE key start stop [WITHSCORES]
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'zrevrange' command");
    }
    let key: &[u8] = cmd[1];
    let with_scores = match &cmd[4..] {
        [] => false,
        [word] if word.eq_ignore_ascii_case(b"WITHSCORES") => true,
        _ => return resp_error("ERR syntax error"),
    };

    let params = RangeParams {
        start: cmd[2],
        stop: cmd[3],
        by_score: false,
        by_lex: false,
        rev: true,
        limit_offset: 0,
        limit_count: usize::MAX,
    };

    execute_range(ctx, key, &params, with_scores)
}

fn zrangebylex(ctx: &mut Ctx) -> Vec<u8> {
    // ZRANGEBYLEX key min max [LIMIT offset count]
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'zrangebylex' command");
    }
    let key: &[u8] = cmd[1];

    let mut limit_offset = 0usize;
    let mut limit_count = usize::MAX;
    let mut i = 4usize;
    while i < cmd.len() {
        match std::str::from_utf8(cmd[i])
            .unwrap_or("")
            .to_ascii_uppercase()
            .as_str()
        {
            "LIMIT" if i + 2 < cmd.len() => {
                (limit_offset, limit_count) = match parse_limit(cmd[i + 1], cmd[i + 2]) {
                    Ok(limit) => limit,
                    Err(e) => return e,
                };
                i += 3;
            }
            _ => return resp_error("ERR syntax error"),
        }
    }

    let params = RangeParams {
        start: cmd[2],
        stop: cmd[3],
        by_score: false,
        by_lex: true,
        rev: false,
        limit_offset,
        limit_count,
    };

    // BYLEX never uses WITHSCORES — members all share the same score
    execute_range(ctx, key, &params, false)
}

fn zrevrangebylex(ctx: &mut Ctx) -> Vec<u8> {
    // ZREVRANGEBYLEX key max min [LIMIT offset count]
    // Note: max before min, same reversal as ZREVRANGEBYSCORE
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'zrevrangebylex' command");
    }
    let key: &[u8] = cmd[1];

    let mut limit_offset = 0usize;
    let mut limit_count = usize::MAX;
    let mut i = 4usize;
    while i < cmd.len() {
        match std::str::from_utf8(cmd[i])
            .unwrap_or("")
            .to_ascii_uppercase()
            .as_str()
        {
            "LIMIT" if i + 2 < cmd.len() => {
                (limit_offset, limit_count) = match parse_limit(cmd[i + 1], cmd[i + 2]) {
                    Ok(limit) => limit,
                    Err(e) => return e,
                };
                i += 3;
            }
            _ => return resp_error("ERR syntax error"),
        }
    }

    let params = RangeParams {
        start: cmd[2], // max in ZREVRANGEBYLEX syntax
        stop: cmd[3],  // min
        by_score: false,
        by_lex: true,
        rev: true, // zrange_pairs swaps bounds when rev=true
        limit_offset,
        limit_count,
    };

    execute_range(ctx, key, &params, false)
}

/// Look up key, call zrange_pairs, format as RESP array.
/// All public range commands funnel through here after building RangeParams.
fn execute_range(ctx: &Ctx, key: &[u8], params: &RangeParams, with_scores: bool) -> Vec<u8> {
    if let Some(error) = range_error(params) {
        return error;
    }
    let resp = reply_resp(ctx);
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_array(vec![]),
        Some(ValueRef::SortedSet(zs)) => {
            let inner = zs.read();
            let pairs = zrange_pairs(&inner, params);
            range_reply(&pairs, resp, with_scores)
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

fn bzpopmin(ctx: &mut Ctx) -> Vec<u8> {
    zset_blocking_pop(ctx, false)
}

fn bzpopmax(ctx: &mut Ctx) -> Vec<u8> {
    zset_blocking_pop(ctx, true)
}

/// A popped member and its score, each already encoded as a reply.
type PoppedMember = (Vec<u8>, Vec<u8>);

/// Pop up to `count` members from the low or the high end of the sorted set
/// at `key`, as replies `[member, score]`. Nothing if there is no such key.
fn pop_members(
    ctx: &Ctx,
    resp: u8,
    key: &[u8],
    want_max: bool,
    count: usize,
) -> Result<Vec<PoppedMember>, Vec<u8>> {
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);
    match map.get(key) {
        Some(ValueRef::SortedSet(zs)) => {
            let mut inner = zs.write();
            let mut popped = Vec::new();
            while popped.len() < count {
                let entry = inner.pop_with(want_max, |member, score| {
                    (resp_bulk(member), score_reply(resp, score))
                });
                let Some(entry) = entry else {
                    break;
                };
                popped.push(entry);
            }
            crate::sorted_set::after_write(zs, &mut inner);
            Ok(popped)
        }
        Some(_) => Err(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => Ok(Vec::new()),
    }
}

fn zset_blocking_pop(ctx: &mut Ctx, want_max: bool) -> Vec<u8> {
    let resp = reply_resp(ctx);
    // BZPOPMIN key [key ...] timeout
    // BZPOPMAX key [key ...] timeout
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments");
    }

    let timeout_secs = match blocking::parse_timeout(cmd[cmd.len() - 1]) {
        Ok(secs) => secs,
        Err(e) => return e,
    };

    let keys: Vec<Vec<u8>> = cmd[1..cmd.len() - 1].iter().map(|b| b.to_vec()).collect();

    // `[key, member, score]` from the first key, in the order given, that
    // has a member.
    blocking::wait_for_keys(
        ctx,
        &keys,
        blocking::timeout_from_secs(timeout_secs),
        |ctx, keys| {
            for key in keys {
                match pop_members(ctx, resp, key, want_max, 1) {
                    Ok(mut popped) => {
                        if let Some((member, score)) = popped.pop() {
                            return Some(resp_array(vec![resp_bulk(key), member, score]));
                        }
                    }
                    Err(e) => return Some(e),
                }
            }
            None
        },
        resp_null_array,
    )
}

fn zmpop(ctx: &mut Ctx) -> Vec<u8> {
    let resp = reply_resp(ctx);
    // ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'zmpop' command");
    }
    let (num_keys, want_max, count) =
        match crate::commands::helpers::parse_mpop(cmd, 1, ["MIN", "MAX"]) {
            Ok(parsed) => parsed,
            Err(e) => return e,
        };

    // Try each key in order, pop from the first non-empty one
    for key_bytes in &cmd[2..2 + num_keys] {
        let key: &[u8] = key_bytes;

        let db_ref = &ctx.store[*ctx.selected_db];
        let shard = store::get_shard(db_ref, key);
        let map = read_lock!(shard);

        match map.get(key) {
            None => continue,
            Some(ValueRef::SortedSet(zs)) => {
                let mut inner = zs.write();
                if inner.is_empty() {
                    continue;
                }

                let mut popped_pairs: Vec<Vec<u8>> = Vec::new();
                for _ in 0..count {
                    // Each pair is its own nested array: [member, score]
                    let entry = inner.pop_with(want_max, |member, score| {
                        resp_array(vec![resp_bulk(member), score_reply(resp, score)])
                    });
                    match entry {
                        None => break,
                        Some(pair) => popped_pairs.push(pair),
                    }
                }
                crate::sorted_set::after_write(zs, &mut inner);

                if popped_pairs.is_empty() {
                    continue;
                }

                return resp_array(vec![resp_bulk(key), resp_array(popped_pairs)]);
            }
            Some(_) => {
                return resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                );
            }
        }
    }

    resp_null_array() // no non-empty key found
}

fn bzmpop(ctx: &mut Ctx) -> Vec<u8> {
    let resp = reply_resp(ctx);
    // BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
    let cmd = &ctx.cmd;
    if cmd.len() < 5 {
        return resp_error("ERR wrong number of arguments for 'bzmpop' command");
    }

    let (num_keys, want_max, count) =
        match crate::commands::helpers::parse_mpop(cmd, 2, ["MIN", "MAX"]) {
            Ok(parsed) => parsed,
            Err(e) => return e,
        };
    let timeout_secs = match blocking::parse_timeout(cmd[1]) {
        Ok(secs) => secs,
        Err(e) => return e,
    };

    let keys: Vec<Vec<u8>> = cmd[3..3 + num_keys].iter().map(|b| b.to_vec()).collect();

    // `[key, [[member, score], ...]]`: up to `count` members from the first
    // key that has any.
    blocking::wait_for_keys(
        ctx,
        &keys,
        blocking::timeout_from_secs(timeout_secs),
        |ctx, keys| {
            for key in keys {
                match pop_members(ctx, resp, key, want_max, count) {
                    Ok(popped) if popped.is_empty() => {}
                    Ok(popped) => {
                        let pairs = popped
                            .into_iter()
                            .map(|(member, score)| resp_array(vec![member, score]))
                            .collect();
                        return Some(resp_array(vec![resp_bulk(key), resp_array(pairs)]));
                    }
                    Err(e) => return Some(e),
                }
            }
            None
        },
        resp_null_array,
    )
}

#[cfg(test)]
mod aggregate_tests {
    use super::*;
    use std::collections::BTreeMap;

    fn set(members: &[(&str, f64)]) -> BTreeMap<Vec<u8>, f64> {
        members
            .iter()
            .map(|(m, s)| (m.as_bytes().to_vec(), *s))
            .collect()
    }

    fn sum(a: f64, b: f64) -> f64 {
        a + b
    }

    /// Each weight belongs to its own key, whichever set is smaller.
    #[test]
    fn intersection_weights_follow_their_keys() {
        let big = set(&[("a", 1.0), ("b", 1.0), ("c", 1.0)]);
        let small = set(&[("a", 10.0)]);
        // big * 2 + small * 3 = 2 + 30
        let r = intersect_weighted(&[big.clone(), small.clone()], &[2.0, 3.0], sum, false);
        assert_eq!(r.len(), 1);
        assert_eq!(r[&b"a"[..]], 32.0);
        // The same sets the other way round: small * 2 + big * 3 = 20 + 3
        let r = intersect_weighted(&[small, big], &[2.0, 3.0], sum, false);
        assert_eq!(r[&b"a"[..]], 23.0);
    }

    #[test]
    fn count_ignores_scores_and_sums_weights() {
        let s1 = set(&[("a", 100.0), ("b", 5.0)]);
        let s2 = set(&[("a", -7.0)]);
        let r = intersect_weighted(&[s1.clone(), s2.clone()], &[1.0, 1.0], sum, true);
        assert_eq!(r[&b"a"[..]], 2.0);
        let r = intersect_weighted(&[s1, s2], &[2.0, 5.0], sum, true);
        assert_eq!(r[&b"a"[..]], 7.0);
        assert_eq!(contribution(100.0, 3.0, true), 3.0);
        assert_eq!(contribution(100.0, 3.0, false), 300.0);
    }

    #[test]
    fn count_is_accepted_as_an_aggregate() {
        let cmd: Vec<&[u8]> = vec![b"ZUNION", b"1", b"k", b"AGGREGATE", b"COUNT"];
        let (weights, agg, count_only) = parse_weights_aggregate(&cmd, 3, 1).unwrap();
        assert!(count_only);
        assert_eq!(weights, vec![1.0]);
        assert_eq!(agg(1.0, 1.0), 2.0);
        let cmd: Vec<&[u8]> = vec![b"ZUNION", b"1", b"k", b"AGGREGATE", b"MEDIAN"];
        assert!(parse_weights_aggregate(&cmd, 3, 1).is_err());
    }
}

#[cfg(test)]
mod score_range_tests {
    use super::*;

    fn in_bounds(score: f64, (min, min_excl): (f64, bool), (max, max_excl): (f64, bool)) -> bool {
        let above = if min_excl { score > min } else { score >= min };
        let below = if max_excl { score < max } else { score <= max };
        above && below
    }

    fn by_scan(zs: &SortedSetInner, min: (f64, bool), max: (f64, bool)) -> Vec<ScoreKey> {
        zs.iter()
            .filter(|(_, score)| in_bounds(*score, min, max))
            .map(|(member, score)| (OrderedFloat(score), member.to_shared()))
            .collect()
    }

    fn by_range(zs: &SortedSetInner, min: (f64, bool), max: (f64, bool)) -> Vec<ScoreKey> {
        score_range(min, max).map_or_else(Vec::new, |range| {
            zs.range_by_score(range)
                .map(|(member, score)| (OrderedFloat(score), member.to_shared()))
                .collect()
        })
    }

    fn member(name: &str) -> ZMember {
        Arc::from(name.as_bytes())
    }

    const EDGES: [f64; 17] = [
        f64::NEG_INFINITY,
        f64::MIN,
        -1.5,
        -1.0,
        -f64::MIN_POSITIVE,
        -5e-324,
        -0.0,
        0.0,
        5e-324,
        f64::MIN_POSITIVE,
        1.0f64.next_down(),
        1.0,
        1.000_000_000_000_000_2,
        1.5,
        f64::MAX,
        f64::INFINITY,
        2.0,
    ];

    /// Several members to each score, the empty member among them, at
    /// every score next to a bound that is easy to get wrong.
    fn edge_set() -> SortedSetInner {
        let mut zs = SortedSetInner::new();
        for (i, &score) in EDGES.iter().enumerate() {
            for name in ["", "a", "b"] {
                zs.insert(member(&format!("{name}{i}")), score);
            }
        }
        zs.insert(member(""), 1.0);
        zs
    }

    #[test]
    fn every_pair_of_edge_bounds_matches_a_full_scan() {
        let zs = edge_set();
        for &min in &EDGES {
            for &max in &EDGES {
                for min_excl in [false, true] {
                    for max_excl in [false, true] {
                        let (lo, hi) = ((min, min_excl), (max, max_excl));
                        assert_eq!(by_range(&zs, lo, hi), by_scan(&zs, lo, hi), "{lo:?} {hi:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn signed_zeros_are_the_same_score() {
        let mut zs = SortedSetInner::new();
        zs.insert(member("neg"), -0.0);
        zs.insert(member("pos"), 0.0);
        for (min, max) in [(-0.0, 0.0), (0.0, -0.0), (0.0, 0.0), (-0.0, -0.0)] {
            assert_eq!(by_range(&zs, (min, false), (max, false)).len(), 2);
            assert!(by_range(&zs, (min, true), (max, false)).is_empty());
            assert!(by_range(&zs, (min, false), (max, true)).is_empty());
        }
        assert_eq!(by_range(&zs, (-5e-324, true), (5e-324, true)).len(), 2);
    }

    #[test]
    fn empty_and_inverted_ranges_have_no_bounds() {
        assert!(score_range((2.0, false), (1.0, false)).is_none());
        assert!(score_range((1.0, true), (1.0, true)).is_none());
        assert!(score_range((f64::INFINITY, true), (f64::INFINITY, false)).is_none());
        assert!(score_range((f64::INFINITY, false), (f64::NEG_INFINITY, false)).is_none());
        let zs = edge_set();
        // One-sided empties still give bounds a BTreeMap accepts.
        assert!(by_range(&zs, (1.0, true), (1.0, false)).is_empty());
        assert!(by_range(&zs, (1.0, false), (1.0, true)).is_empty());
        assert!(by_range(&zs, (f64::NEG_INFINITY, false), (f64::NEG_INFINITY, true)).is_empty());
        assert!(by_range(&zs, (f64::INFINITY, false), (f64::INFINITY, true)).is_empty());
    }

    /// Fixed-seed xorshift, so a failure can be run again.
    struct Rng(u64);

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
        fn score(&mut self) -> f64 {
            match self.below(10) {
                0 => EDGES[self.below(EDGES.len() as u64) as usize],
                1 => (self.below(2001) as f64 - 1000.0) / 7.0,
                // Mostly a few whole scores, so that most of them repeat.
                _ => self.below(21) as f64 - 10.0,
            }
        }
    }

    #[test]
    fn random_ranges_match_a_full_scan() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        for _ in 0..200 {
            let mut zs = SortedSetInner::new();
            for i in 0..rng.below(300) {
                let score = rng.score();
                zs.insert(member(&format!("m{i:04}")), score);
            }
            for _ in 0..50 {
                let lo = (rng.score(), rng.below(2) == 0);
                let hi = (rng.score(), rng.below(2) == 0);
                assert_eq!(by_range(&zs, lo, hi), by_scan(&zs, lo, hi), "{lo:?} {hi:?}");
            }
        }
    }
}

#[cfg(test)]
mod limit_and_lex_tests {
    //! `LIMIT` windows of score ranges and lex ranges against a reference:
    //! a `BTreeMap` of `(score, member)`, filtered member by member and the
    //! window stepped through, as the commands' replies are defined.
    use super::*;
    use std::collections::BTreeMap;

    type Model = BTreeMap<(OrderedFloat<f64>, Vec<u8>), ()>;

    /// Fixed-seed xorshift, so a failure can be run again.
    struct Rng(u64);

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
    }

    const SCORES: [f64; 7] = [f64::NEG_INFINITY, -2.5, 0.0, 1.0, 1.5, 7.0, f64::INFINITY];

    fn score_text(score: f64, excl: bool) -> Vec<u8> {
        let text = if score == f64::INFINITY {
            "+inf".to_string()
        } else if score == f64::NEG_INFINITY {
            "-inf".to_string()
        } else {
            format!("{score}")
        };
        format!("{}{text}", if excl { "(" } else { "" }).into_bytes()
    }

    fn name(i: u64) -> Vec<u8> {
        format!("m{i:03}").into_bytes()
    }

    /// A lex bound near the set's members: `-`, `+`, a member, a prefix of
    /// one, the empty member, one past every member.
    fn lex_bound(rng: &mut Rng, n: u64) -> Vec<u8> {
        let body = match rng.below(6) {
            0 => return b"-".to_vec(),
            1 => return b"+".to_vec(),
            2 => Vec::new(),
            3 => name(rng.below(n + 1))[..3].to_vec(),
            4 => b"n".to_vec(),
            _ => name(rng.below(n + 2)),
        };
        let mut out = vec![if rng.below(2) == 0 { b'[' } else { b'(' }];
        out.extend(body);
        out
    }

    /// An offset or a count: 0, within the set, past its end, or negative
    /// (`usize::MAX`, as `parse_limit` reads it).
    fn limit_number(rng: &mut Rng, n: u64) -> usize {
        match rng.below(6) {
            0 => 0,
            1 => usize::MAX,
            2 => (n + 1 + rng.below(5)) as usize,
            _ => rng.below(n + 1) as usize,
        }
    }

    fn window(
        mut found: Vec<(Vec<u8>, f64)>,
        rev: bool,
        offset: usize,
        count: usize,
    ) -> Vec<(Vec<u8>, f64)> {
        if rev {
            found.reverse();
        }
        found.into_iter().skip(offset).take(count).collect()
    }

    fn by_model(model: &Model, keep: impl Fn(f64, &[u8]) -> bool) -> Vec<(Vec<u8>, f64)> {
        model
            .keys()
            .filter(|key| keep(key.0.0, &key.1))
            .map(|key| (key.1.clone(), key.0.0))
            .collect()
    }

    fn pairs(inner: &SortedSetInner, p: &RangeParams) -> Vec<(Vec<u8>, f64)> {
        zrange_pairs(inner, p)
            .into_iter()
            .map(|(member, score)| (member.as_ref().to_vec(), score))
            .collect()
    }

    fn score_in(score: f64, (min, min_excl): (f64, bool), (max, max_excl): (f64, bool)) -> bool {
        let above = if min_excl { score > min } else { score >= min };
        let below = if max_excl { score < max } else { score <= max };
        above && below
    }

    /// Sets of up to 300 members (the block and the large form), every
    /// member at one score or at a few: score ranges and lex ranges with
    /// every bound form, `LIMIT` at 0, inside, past the end and negative,
    /// both ways, and `ZLEXCOUNT`, each against the model.
    #[test]
    fn limits_and_lex_ranges_match_a_model() {
        let mut rng = Rng(0x2545_f491_4f6c_dd1d);
        for round in 0..240 {
            let n = match round % 4 {
                0 => rng.below(8),
                1 => rng.below(129),
                _ => 129 + rng.below(172),
            };
            let one_score = round % 3 != 0;
            let mut inner = SortedSetInner::new();
            let mut model = Model::new();
            let shared = SCORES[rng.below(SCORES.len() as u64) as usize];
            for i in 0..n {
                // Members in a random order, so the form and the index see
                // inserts all over.
                let i = (i * 7919 + round) % n.max(1);
                let score = if one_score {
                    shared
                } else {
                    SCORES[rng.below(SCORES.len() as u64) as usize]
                };
                let member = name(i);
                if inner.insert_bytes(&member, score) {
                    model.insert((OrderedFloat(score), member), ());
                } else {
                    model.retain(|(_, m), _| *m != member);
                    model.insert((OrderedFloat(score), member), ());
                }
            }
            assert_eq!(inner.len(), model.len());
            for _ in 0..40 {
                let rev = rng.below(2) == 0;
                let (offset, count) = if rng.below(4) == 0 {
                    (0, usize::MAX)
                } else {
                    (limit_number(&mut rng, n), limit_number(&mut rng, n))
                };
                // A score range.
                let min = (SCORES[rng.below(7) as usize], rng.below(3) == 0);
                let max = (SCORES[rng.below(7) as usize], rng.below(3) == 0);
                let (min_raw, max_raw) = (score_text(min.0, min.1), score_text(max.0, max.1));
                let p = RangeParams {
                    start: if rev { &max_raw } else { &min_raw },
                    stop: if rev { &min_raw } else { &max_raw },
                    by_score: true,
                    by_lex: false,
                    rev,
                    limit_offset: offset,
                    limit_count: count,
                };
                let want = window(
                    by_model(&model, |s, _| score_in(s, min, max)),
                    rev,
                    offset,
                    count,
                );
                assert_eq!(
                    pairs(&inner, &p),
                    want,
                    "score {min:?} {max:?} rev={rev} {offset} {count} n={n}"
                );
                // A lex range.
                let (lo_raw, hi_raw) = (lex_bound(&mut rng, n), lex_bound(&mut rng, n));
                let (lo, hi) = (
                    parse_lex_bound(&lo_raw).unwrap(),
                    parse_lex_bound(&hi_raw).unwrap(),
                );
                let p = RangeParams {
                    start: if rev { &hi_raw } else { &lo_raw },
                    stop: if rev { &lo_raw } else { &hi_raw },
                    by_score: false,
                    by_lex: true,
                    rev,
                    limit_offset: offset,
                    limit_count: count,
                };
                let all = by_model(&model, |_, m| lex_in_range(m, &lo, &hi));
                let what = format!(
                    "lex {} {} rev={rev} {offset} {count} n={n} one_score={one_score}",
                    String::from_utf8_lossy(&lo_raw),
                    String::from_utf8_lossy(&hi_raw)
                );
                assert_eq!(lex_count(&inner, &lo, &hi), all.len(), "{what}");
                assert_eq!(pairs(&inner, &p), window(all, rev, offset, count), "{what}");
            }
        }
    }

    /// The window arithmetic at its edges.
    #[test]
    fn limit_windows() {
        assert_eq!(limit_window(3, 10, 0, usize::MAX, false), (3, 10));
        assert_eq!(limit_window(3, 10, 2, 3, false), (5, 8));
        assert_eq!(limit_window(3, 10, 7, 3, false), (10, 10));
        assert_eq!(limit_window(3, 10, usize::MAX, 3, false), (10, 10));
        assert_eq!(limit_window(3, 10, 2, 3, true), (5, 8));
        assert_eq!(limit_window(3, 10, 6, 3, true), (3, 4));
        assert_eq!(limit_window(3, 10, usize::MAX, 3, true), (3, 3));
        assert_eq!(limit_window(3, 10, 0, usize::MAX, true), (3, 10));
        assert_eq!(limit_window(4, 4, 0, 5, true), (4, 4));
    }
}
