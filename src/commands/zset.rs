// src/commands/zset.rs

use crate::commands::blocking;
use crate::commands::ctx::Ctx;
use crate::commands::helpers::{format_score, parse_utf8};
use crate::keyspace_events::{KS_SORTED_SET, notify};
use crate::resp::write::*;
use crate::store;
use crate::store::Key;
use crate::types::{KiviDBValue, SortedSetInner, ZMember};
use crate::{read_lock, write_lock};
use ordered_float::OrderedFloat;
use parking_lot::RwLock;
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
/// members, some three hundred allocations to an answer.
fn range_reply(pairs: &[(Vec<u8>, f64)], resp: u8, with_scores: bool) -> Vec<u8> {
    let bytes: usize = pairs.iter().map(|(member, _)| member.len()).sum();
    pairs_reply(
        pairs
            .iter()
            .map(|(member, score)| (member.as_slice(), *score)),
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
    let entries = if with_scores && !paired {
        count * 2
    } else {
        count
    };
    crate::resp::write::push_header(&mut out, b'*', entries);
    for (member, score) in pairs {
        if paired {
            out.extend_from_slice(b"*2\r\n");
        }
        crate::resp::write::push_header(&mut out, b'$', member.len());
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
                crate::resp::write::push_header(&mut out, b'$', digits.len() - at);
                out.extend_from_slice(&digits[at..]);
                out.extend_from_slice(b"\r\n");
            } else {
                let text = format_score(score);
                crate::resp::write::push_header(&mut out, b'$', text.len());
                out.extend_from_slice(text.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
        }
    }
    out
}

fn get_or_create_zset(ctx: &mut Ctx, key: &str) -> Result<Arc<RwLock<SortedSetInner>>, Vec<u8>> {
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let mut map = write_lock!(shard);

    match map.get(key) {
        Some(KiviDBValue::SortedSet(zs)) => Ok(Arc::clone(zs)),
        Some(_) => Err(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => {
            let zs = Arc::new(RwLock::new(SortedSetInner::new()));
            map.insert(Key::from(key), KiviDBValue::SortedSet(Arc::clone(&zs)));
            Ok(zs)
        }
    }
}

/// The first bulk string of an array reply: the key, in the reply of a
/// command that takes several and answers with the one it used.
fn first_bulk(reply: &[u8]) -> Option<&str> {
    let mut lines = reply.split(|b| *b == b'\n');
    lines.next().filter(|line| line.first() == Some(&b'*'))?;
    lines.next().filter(|line| line.first() == Some(&b'$'))?;
    std::str::from_utf8(lines.next()?.strip_suffix(b"\r")?).ok()
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
    let key_argument = || ctx.cmd.get(1).and_then(|raw| std::str::from_utf8(raw).ok());
    let lower;
    let (event, key): (&str, Option<&str>) = match name {
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
        std::str::from_utf8(raw)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
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
                    std::str::from_utf8(raw)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
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

/// Core range logic. Returns owned (member_bytes, score) pairs — no RESP encoding.
/// All public range commands call this and format the output themselves.
/// Returning Vec<(Vec<u8>, f64)> instead of Vec<u8> means:
///   - ZRANGE formats with optional WITHSCORES
///   - ZRANGESTORE writes directly into a SortedSetInner without a parse round-trip
///   - Legacy wrappers (ZRANGEBYSCORE etc.) just build RangeParams and call this
fn zrange_pairs(inner: &SortedSetInner, p: &RangeParams) -> Vec<(Vec<u8>, f64)> {
    if p.by_score {
        let min_str = std::str::from_utf8(p.start).unwrap_or("-inf");
        let max_str = std::str::from_utf8(p.stop).unwrap_or("+inf");

        // REV swaps the bounds — caller passes (max, min) in REV mode
        let (min_str, max_str) = if p.rev {
            (max_str, min_str)
        } else {
            (min_str, max_str)
        };

        let (min_val, min_excl) = match parse_score_bound(min_str) {
            Ok(v) => v,
            Err(_) => return vec![],
        };
        let (max_val, max_excl) = match parse_score_bound(max_str) {
            Ok(v) => v,
            Err(_) => return vec![],
        };

        let in_range = |score: f64| -> bool {
            let above = if min_excl {
                score > min_val
            } else {
                score >= min_val
            };
            let below = if max_excl {
                score < max_val
            } else {
                score <= max_val
            };
            above && below
        };

        // The tree is in score order: begin at the first score that can be
        // in the range and stop at the first that cannot, where this used
        // to look at every member of the set, in the range or not.
        use std::ops::Bound;
        // The least member there can be: with a score, the least key of
        // that score.
        let least: ZMember = Arc::from(&[][..]);
        let lower = if min_val == f64::NEG_INFINITY || min_val.is_nan() {
            Bound::Unbounded
        } else {
            Bound::Included((OrderedFloat(min_val), Arc::clone(&least)))
        };
        // Everything up to the next score above the largest wanted. There
        // is none above infinity, and nothing to leave out there.
        let upper = if max_val == f64::INFINITY || max_val.is_nan() {
            Bound::Unbounded
        } else {
            Bound::Excluded((OrderedFloat(max_val.next_up()), least))
        };
        if min_val > max_val {
            return vec![];
        }
        let within = inner.by_score.range((lower, upper));
        // The ends themselves may be left out (`(1.5`): `in_range` knows.
        if p.rev {
            within
                .rev()
                .filter(|((score, _), _)| in_range(score.0))
                .map(|((score, member), _)| (member.as_ref().to_vec(), score.0))
                .skip(p.limit_offset)
                .take(p.limit_count)
                .collect()
        } else {
            within
                .filter(|((score, _), _)| in_range(score.0))
                .map(|((score, member), _)| (member.as_ref().to_vec(), score.0))
                .skip(p.limit_offset)
                .take(p.limit_count)
                .collect()
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
        let lex_in_range = move |member: &[u8]| lex_in_range(member, &min, &max);

        if p.rev {
            inner
                .by_score
                .iter()
                .rev()
                .filter(|((_, member), _)| lex_in_range(member.as_ref()))
                .map(|((score, member), _)| (member.as_ref().to_vec(), score.0))
                .skip(p.limit_offset)
                .take(p.limit_count)
                .collect()
        } else {
            inner
                .by_score
                .iter()
                .filter(|((_, member), _)| lex_in_range(member.as_ref()))
                .map(|((score, member), _)| (member.as_ref().to_vec(), score.0))
                .skip(p.limit_offset)
                .take(p.limit_count)
                .collect()
        }
    } else {
        // Index-based — unchanged, already correct
        let len = inner.by_score.len() as i64;
        if len == 0 {
            return vec![];
        }

        let parse_idx = |b: &[u8]| -> i64 {
            std::str::from_utf8(b)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(0)
        };
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

        // Only the members asked for are copied. Every member of the set
        // used to be, for each call, and all but these thrown away.
        let wanted = (stop - start + 1) as usize;
        let copy = |((score, member), _): (&(OrderedFloat<f64>, ZMember), &())| {
            (member.as_ref().to_vec(), score.0)
        };
        // The tree has no way straight to the hundredth member: it is
        // walked to it, from whichever end is nearer. `start` counts from
        // the end the answer begins at (the top, for REV); past half way,
        // the walk comes from the other end and the answer is turned round.
        let from_far_end = (start as usize) > inner.by_score.len() / 2;
        if p.rev == from_far_end {
            if p.rev {
                // Counted from the top and far from it: near the bottom.
                let first = len as usize - 1 - stop as usize;
                let mut found: Vec<(Vec<u8>, f64)> = inner
                    .by_score
                    .iter()
                    .skip(first)
                    .take(wanted)
                    .map(copy)
                    .collect();
                found.reverse();
                found
            } else {
                inner
                    .by_score
                    .iter()
                    .skip(start as usize)
                    .take(wanted)
                    .map(copy)
                    .collect()
            }
        } else if p.rev {
            inner
                .by_score
                .iter()
                .rev()
                .skip(start as usize)
                .take(wanted)
                .map(copy)
                .collect()
        } else {
            // Forward, but nearer the back: walk from the back and turn it.
            let first = len as usize - 1 - stop as usize;
            let mut found: Vec<(Vec<u8>, f64)> = inner
                .by_score
                .iter()
                .rev()
                .skip(first)
                .take(wanted)
                .map(copy)
                .collect();
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

    let key = match parse_utf8(cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };

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

    // Parse score/member pairs
    // Rust lesson: collect into Vec<(f64, ZMember)> so we own the data before
    // taking the write lock — avoids borrow conflicts.
    let mut pairs: Vec<(f64, ZMember)> = Vec::new();
    while i + 1 < cmd.len() {
        let Some(score) = parse_score(cmd[i]) else {
            return resp_error("ERR value is not a valid float");
        };
        let member: ZMember = Arc::from(cmd[i + 1]);
        pairs.push((score, member));
        i += 2;
    }

    if pairs.is_empty() {
        return resp_error("ERR syntax error");
    }

    let zs = match get_or_create_zset(ctx, &key) {
        Ok(z) => z,
        Err(e) => return e,
    };

    let mut inner = zs.write();
    if flag_incr
        && let Some((incr, member)) = pairs.first()
        && (inner.members.get(member.as_ref()).copied().unwrap_or(0.0) + incr).is_nan()
    {
        return resp_error("ERR resulting score is not a number (NaN)");
    }
    let mut added = 0i64;
    let mut changed = 0i64;
    let mut last_score: Option<f64> = None; // used for INCR mode

    for (score, member) in pairs {
        let existing = inner.members.get(member.as_ref()).copied();

        let final_score = if flag_incr {
            // INCR: add to existing score (or treat as 0 if not present)
            existing.unwrap_or(0.0) + score
        } else {
            score
        };

        match existing {
            None => {
                // Member doesn't exist
                if flag_xx {
                    // XX: skip, don't add
                    continue;
                }
                // Every member goes into the set; a client blocked on the
                // key is woken afterwards and pops for itself (see
                // `blocking::wait_for_keys`).
                inner.insert(member, final_score);
                added += 1;
                changed += 1;
                last_score = Some(final_score);
            }
            Some(old_score) => {
                // Member exists
                if flag_nx {
                    // NX: skip, don't update
                    continue;
                }
                // GT/LT gating
                if flag_gt && final_score <= old_score {
                    continue;
                }
                if flag_lt && final_score >= old_score {
                    continue;
                }

                // (Any difference: two scores a hair apart are two
                // scores.)
                if final_score != old_score {
                    inner.insert(member, final_score);
                    changed += 1;
                }
                last_score = Some(final_score);
            }
        }
    }

    drop(inner);
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

    if crate::commands::helpers::lazy_expire_on_read(
        ctx.store,
        ctx.expiry_store,
        *ctx.selected_db,
        key,
    ) {
        return resp_integer(0);
    }

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_integer(0),
        Some(KiviDBValue::SortedSet(zs)) => resp_integer(zs.read().len() as i64),
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

fn zscore(ctx: &mut Ctx) -> Vec<u8> {
    // ZSCORE key member → bulk string score or nil
    let cmd = &ctx.cmd;
    if cmd.len() != 3 {
        return resp_error("ERR wrong number of arguments for 'zscore' command");
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

    let resp = reply_resp(ctx);
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => {
            crate::stats::record_miss();
            resp_null_bulk()
        }
        Some(KiviDBValue::SortedSet(zs)) => match zs.read().members.get(cmd[2]) {
            Some(&score) => {
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

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
        Some(KiviDBValue::SortedSet(zs)) => {
            let inner = zs.read();
            let results = cmd[2..]
                .iter()
                .map(|m| match inner.members.get(*m) {
                    Some(&score) => score_reply(resp, score),
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

    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => {
            crate::stats::record_miss();
            none()
        }
        Some(KiviDBValue::SortedSet(zs)) => {
            let inner = zs.read();
            let member = cmd[2];

            // Single lookup — get both the Arc key and the score at once
            match inner.members.get_key_value(member) {
                None => {
                    crate::stats::record_miss();
                    none()
                }
                Some((stored_arc, &score)) => {
                    crate::stats::record_hit();
                    // Count all entries that sort strictly before this one.
                    // The BTreeSet orders by (score, member) so we range
                    // up to (exclusive) the exact key of this member.
                    let rank = inner
                        .by_score
                        .range(..(OrderedFloat(score), stored_arc.clone()))
                        .count() as i64;

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

    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

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
        Some(KiviDBValue::SortedSet(zs)) => {
            let inner = zs.read();
            let member = cmd[2];

            match inner.members.get_key_value(member) {
                None => none(),
                Some((stored_arc, &score)) => {
                    let asc_rank = inner
                        .by_score
                        .range(..(OrderedFloat(score), stored_arc.clone()))
                        .count() as i64;
                    let revrank = inner.members.len() as i64 - 1 - asc_rank;

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

/// A score as Redis reads one (`strtod`, then a check that all of the
/// argument was used and the result is a number): decimal, `inf` /
/// `infinity` with a sign or without, or a C hexadecimal float such as
/// `0x1p+4`. Not `nan`, not an empty string, nothing with space around it.
pub(crate) fn parse_score(raw: &[u8]) -> Option<f64> {
    let text = std::str::from_utf8(raw).ok()?;
    if text.is_empty() || text.starts_with(char::is_whitespace) {
        return None;
    }
    let value = match parse_hex_float(text) {
        Some(value) => value,
        None => text.parse::<f64>().ok()?,
    };
    if value.is_nan() {
        return None;
    }
    // Infinity only when infinity was written: a number too large for a
    // double is out of range (`strtod` sets ERANGE), not infinite.
    if value.is_infinite() {
        let unsigned = text.trim_start_matches(['+', '-']);
        if !unsigned.eq_ignore_ascii_case("inf") && !unsigned.eq_ignore_ascii_case("infinity") {
            return None;
        }
    }
    Some(value)
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

/// The error for a range whose ends cannot be read, if they cannot. Asked
/// before the key is looked at: a bad range is an error whether or not the
/// key exists.
fn range_error(p: &RangeParams) -> Option<Vec<u8>> {
    if p.by_score {
        for end in [p.start, p.stop] {
            if let Err(e) = parse_score_bound(std::str::from_utf8(end).unwrap_or("")) {
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
            if std::str::from_utf8(end)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .is_none()
            {
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
        Some(at) => (&rest[..at], rest[at + 1..].parse::<i32>().ok()?),
        None => (rest, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    let mut value = 0f64;
    for digit in whole.chars() {
        value = value * 16.0 + digit.to_digit(16)? as f64;
    }
    let mut scale = 1.0 / 16.0;
    for digit in fraction.chars() {
        value += digit.to_digit(16)? as f64 * scale;
        scale /= 16.0;
    }
    let value = value * 2f64.powi(exponent);
    Some(if negative { -value } else { value })
}

/// Parse a Redis score bound like "5.0", "(5.0", "-inf", "+inf"
/// Returns (value, exclusive) — the bool is true if the paren form was used.
fn parse_score_bound(s: &str) -> Result<(f64, bool), Vec<u8>> {
    if s.eq_ignore_ascii_case("-inf") {
        return Ok((f64::NEG_INFINITY, false));
    }
    if s.eq_ignore_ascii_case("+inf") || s.eq_ignore_ascii_case("inf") {
        return Ok((f64::INFINITY, false));
    }
    let (exclusive, num_str) = if let Some(rest) = s.strip_prefix('(') {
        (true, rest)
    } else {
        (false, s)
    };
    parse_score(num_str.as_bytes())
        .map(|v| (v, exclusive))
        .ok_or_else(|| resp_error("ERR min or max is not a float"))
}

fn zcount(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() != 4 {
        return resp_error("ERR wrong number of arguments for 'zcount' command");
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let min_str = match std::str::from_utf8(cmd[2]) {
        Ok(s) => s,
        Err(_) => return resp_error("ERR min must be UTF-8"),
    };
    let max_str = match std::str::from_utf8(cmd[3]) {
        Ok(s) => s,
        Err(_) => return resp_error("ERR max must be UTF-8"),
    };

    let (min_val, min_excl) = match parse_score_bound(min_str) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let (max_val, max_excl) = match parse_score_bound(max_str) {
        Ok(v) => v,
        Err(e) => return e,
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_integer(0),
        Some(KiviDBValue::SortedSet(zs)) => {
            let inner = zs.read();
            // score_in_range is a closure — captures min/max by value.
            // Rust lesson: closures that only read captured vars implement Fn,
            // which means we can call them multiple times freely.
            let in_range = |score: f64| -> bool {
                let above_min = if min_excl {
                    score > min_val
                } else {
                    score >= min_val
                };
                let below_max = if max_excl {
                    score < max_val
                } else {
                    score <= max_val
                };
                above_min && below_max
            };
            let count = inner.members.values().filter(|&&s| in_range(s)).count() as i64;
            resp_integer(count)
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let Some(incr) = parse_score(cmd[2]) else {
        return resp_error("ERR value is not a valid float");
    };
    let member: ZMember = Arc::from(cmd[3]);

    let zs = match get_or_create_zset(ctx, &key) {
        Ok(z) => z,
        Err(e) => return e,
    };

    let resp = reply_resp(ctx);
    let mut inner = zs.write();
    let old = inner.members.get(member.as_ref()).copied().unwrap_or(0.0);
    let new_score = old + incr;
    if new_score.is_nan() {
        return resp_error("ERR resulting score is not a number (NaN)");
    }
    inner.insert(member, new_score);
    score_reply(resp, new_score)
}

fn zrem(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'zrem' command");
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_integer(0),
        Some(KiviDBValue::SortedSet(zs)) => {
            let mut inner = zs.write();
            let mut removed = 0i64;
            for m in &cmd[2..] {
                if inner.remove(m) {
                    removed += 1;
                }
            }
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

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
        Some(KiviDBValue::SortedSet(zs)) => {
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let count = match cmd.get(2) {
        None => 1,
        Some(raw) => match std::str::from_utf8(raw)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
        {
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
        Some(KiviDBValue::SortedSet(zs)) => {
            let mut inner = zs.write();
            let mut popped: Vec<(ZMember, f64)> =
                Vec::with_capacity(count.min(inner.by_score.len()));

            for _ in 0..count {
                // BTreeMap::pop_first / pop_last are stable since Rust 1.66
                let next = if max {
                    inner.by_score.pop_last()
                } else {
                    inner.by_score.pop_first()
                };
                match next {
                    None => break,
                    Some(((score, member), _)) => {
                        inner.members.remove(member.as_ref());
                        popped.push((member, score.0));
                    }
                }
            }
            // Into one buffer: a vector for the member, one for the score
            // and one for the pair were made and joined, for every pop.
            let bytes: usize = popped.iter().map(|(member, _)| member.len()).sum();
            pairs_reply(
                popped
                    .iter()
                    .map(|(member, score)| (member.as_ref(), *score)),
                popped.len(),
                bytes,
                resp,
                true,
                nest,
            )
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let (cursor, options) = match crate::commands::helpers::scan_arguments(cmd, 2, &["NOSCORES"]) {
        Ok(parsed) => parsed,
        Err(e) => return e,
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_array(vec![resp_bulk_str("0"), resp_array(vec![])]),
        Some(value @ KiviDBValue::SortedSet(zs)) => {
            // A sorted set Redis would encode compactly comes back whole.
            let (cursor, count) = if crate::commands::generics::scans_whole(value) {
                (0, usize::MAX)
            } else {
                (cursor, options.count)
            };
            let inner = zs.read();
            // In the order of the members' positions: see
            // `helpers::scan_position`.
            let (page, next_cursor) = crate::commands::helpers::scan_page(
                inner.members.iter().map(|(member, score)| {
                    (
                        crate::commands::helpers::scan_position(member),
                        (member, *score),
                    )
                }),
                cursor,
                count,
                inner.members.len(),
                1024,
            );
            let mut items: Vec<Vec<u8>> = Vec::with_capacity(page.len() * 2);
            for (member, score) in page {
                if options.pattern.as_deref().is_some_and(|pat| {
                    !crate::commands::helpers::glob_match(pat, &String::from_utf8_lossy(member))
                }) {
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
        let key = match std::str::from_utf8(key_bytes) {
            Ok(k) => k,
            Err(_) => return resp_error("ERR key must be valid UTF-8"),
        };
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
    let dst = match std::str::from_utf8(cmd[1]) {
        Ok(k) => k.to_string(),
        Err(_) => return resp_error("ERR destination must be valid UTF-8"),
    };
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
        let key = match std::str::from_utf8(key_bytes) {
            Ok(k) => k,
            Err(_) => return resp_error("ERR key must be valid UTF-8"),
        };
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
        let key = match std::str::from_utf8(key_bytes) {
            Ok(k) => k,
            Err(_) => return resp_error("ERR key must be valid UTF-8"),
        };
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
    let dst = match std::str::from_utf8(cmd[1]) {
        Ok(k) => k.to_string(),
        Err(_) => return resp_error("ERR destination must be valid UTF-8"),
    };
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
        let key = match std::str::from_utf8(key_bytes) {
            Ok(k) => k,
            Err(_) => return resp_error("ERR key must be valid UTF-8"),
        };
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
            match std::str::from_utf8(n)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(n) if n > 0 => limit = n as usize,
                Some(0) => {}
                _ => return resp_error("ERR LIMIT can't be negative"),
            }
        }
        _ => return resp_error("ERR syntax error"),
    }

    let mut snapshots: Vec<std::collections::BTreeMap<Vec<u8>, f64>> = Vec::new();
    for key_bytes in &cmd[2..2 + num_keys] {
        let key = match std::str::from_utf8(key_bytes) {
            Ok(k) => k,
            Err(_) => return resp_error("ERR key must be valid UTF-8"),
        };
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
        let key = match std::str::from_utf8(key_bytes) {
            Ok(k) => k,
            Err(_) => return resp_error("ERR key must be valid UTF-8"),
        };
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
        let key = match std::str::from_utf8(key_bytes) {
            Ok(k) => k,
            Err(_) => return resp_error("ERR key must be valid UTF-8"),
        };
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
    let dst = match std::str::from_utf8(cmd[1]) {
        Ok(k) => k.to_string(),
        Err(_) => return resp_error("ERR destination must be valid UTF-8"),
    };
    let num_keys = match numkeys(cmd, 2, "zdiffstore") {
        Ok(n) => n,
        Err(e) => return e,
    };
    if cmd.len() > 2 + 1 + num_keys {
        return resp_error("ERR syntax error");
    }

    let base = match read_zset_snapshot(
        ctx,
        match std::str::from_utf8(cmd[3]) {
            Ok(k) => k,
            Err(_) => return resp_error("ERR key must be valid UTF-8"),
        },
    ) {
        Ok(Some(s)) => s,
        Ok(None) => {
            store_zset(ctx, &dst, ahash::AHashMap::new());
            return resp_integer(0);
        }
        Err(e) => return e,
    };

    let mut others: Vec<std::collections::BTreeMap<Vec<u8>, f64>> = Vec::new();
    for key_bytes in &cmd[4..3 + num_keys] {
        let key = match std::str::from_utf8(key_bytes) {
            Ok(k) => k,
            Err(_) => return resp_error("ERR key must be valid UTF-8"),
        };
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let min_str = std::str::from_utf8(cmd[2]).unwrap_or("");
    let max_str = std::str::from_utf8(cmd[3]).unwrap_or("");
    let (min_val, min_excl) = match parse_score_bound(min_str) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let (max_val, max_excl) = match parse_score_bound(max_str) {
        Ok(v) => v,
        Err(e) => return e,
    };

    let in_range = move |score: f64| -> bool {
        let above = if min_excl {
            score > min_val
        } else {
            score >= min_val
        };
        let below = if max_excl {
            score < max_val
        } else {
            score <= max_val
        };
        above && below
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_integer(0),
        Some(KiviDBValue::SortedSet(zs)) => {
            let mut inner = zs.write();
            // Collect members to remove first (can't mutate while iterating)
            let to_remove: Vec<(f64, ZMember)> = inner
                .by_score
                .iter()
                .filter(|((score, _), _)| in_range(score.0))
                .map(|((score, member), _)| (score.0, member.clone()))
                .collect();
            let count = to_remove.len() as i64;
            for (score, member) in to_remove {
                inner
                    .by_score
                    .remove(&(OrderedFloat(score), member.clone()));
                inner.members.remove(member.as_ref());
            }
            resp_integer(count)
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_integer(0),
        Some(KiviDBValue::SortedSet(zs)) => {
            let mut inner = zs.write();
            let len = inner.by_score.len() as i64;
            if len == 0 {
                return resp_integer(0);
            }

            let parse_idx = |b: &[u8]| -> i64 {
                std::str::from_utf8(b)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0)
            };
            let mut start = parse_idx(cmd[2]);
            let mut stop = parse_idx(cmd[3]);
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

            let to_remove: Vec<(OrderedFloat<f64>, ZMember)> = inner
                .by_score
                .keys()
                .cloned()
                .enumerate()
                .filter(|(i, _)| *i as i64 >= start && *i as i64 <= stop)
                .map(|(_, k)| k)
                .collect();

            let count = to_remove.len() as i64;
            for key in to_remove {
                inner.members.remove(key.1.as_ref());
                inner.by_score.remove(&key);
            }
            resp_integer(count)
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
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
        Some(KiviDBValue::SortedSet(zs)) => {
            let mut inner = zs.write();
            let to_remove: Vec<(OrderedFloat<f64>, ZMember)> = inner
                .by_score
                .keys()
                .filter(|(_, member)| lex_in_range(member.as_ref()))
                .cloned()
                .collect();
            let count = to_remove.len() as i64;
            for key in to_remove {
                inner.members.remove(key.1.as_ref());
                inner.by_score.remove(&key);
            }
            resp_integer(count)
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

    // `[count [WITHSCORES]]`
    let count_arg: Option<i64> = match cmd.get(2) {
        None => None,
        Some(raw) => match std::str::from_utf8(raw)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
        {
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
        Some(KiviDBValue::SortedSet(zs)) => {
            let inner = zs.read();
            // Collect into a Vec for index-based random access
            let entries: Vec<(&ZMember, f64)> =
                inner.members.iter().map(|(k, &v)| (k, v)).collect();

            if entries.is_empty() {
                return if cmd.len() > 2 {
                    resp_array(vec![])
                } else {
                    resp_null_bulk()
                };
            }

            // No count argument → return single random member as bulk string
            if cmd.len() == 2 {
                let idx = pseudo_rand(entries.len());
                return resp_bulk(entries[idx].0.as_ref());
            }

            let count_arg = count_arg.unwrap_or(1);

            let mut result: Vec<Vec<u8>> = Vec::new();
            if count_arg >= 0 {
                // Distinct: Fisher-Yates partial shuffle
                let n = (count_arg as usize).min(entries.len());
                let mut indices: Vec<usize> = (0..entries.len()).collect();
                for i in 0..n {
                    let j = i + pseudo_rand(entries.len() - i);
                    indices.swap(i, j);
                }
                for &i in &indices[..n] {
                    let (member, score) = entries[i];
                    push_member_score(
                        &mut result,
                        resp,
                        member.as_ref(),
                        with_scores.then_some(score),
                    );
                }
            } else {
                // Repetition allowed
                let n = count_arg.unsigned_abs() as usize;
                for _ in 0..n {
                    let i = pseudo_rand(entries.len());
                    let (member, score) = entries[i];
                    push_member_score(
                        &mut result,
                        resp,
                        member.as_ref(),
                        with_scores.then_some(score),
                    );
                }
            }

            resp_array(result)
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

/// A random index below `n`, for ZRANDMEMBER. It used to be derived from
/// the clock, which gave the same index to every call made within one tick —
/// `ZRANDMEMBER key -5` could return one member five times.
fn pseudo_rand(n: usize) -> usize {
    rand::random::<usize>() % n
}

fn zlexcount(ctx: &mut Ctx) -> Vec<u8> {
    // ZLEXCOUNT key min max
    let cmd = &ctx.cmd;
    if cmd.len() != 4 {
        return resp_error("ERR wrong number of arguments for 'zlexcount' command");
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let (min, max) = match (parse_lex_bound(cmd[2]), parse_lex_bound(cmd[3])) {
        (Ok(min), Ok(max)) => (min, max),
        (Err(e), _) | (_, Err(e)) => return e,
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_integer(0),
        Some(KiviDBValue::SortedSet(zs)) => {
            let inner = zs.read();
            let count = inner
                .by_score
                .keys()
                .filter(|(_, member)| lex_in_range(member.as_ref(), &min, &max))
                .count() as i64;
            resp_integer(count)
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

fn zrangestore(ctx: &mut Ctx) -> Vec<u8> {
    // ZRANGESTORE dst src min max [BYSCORE | BYLEX] [REV] [LIMIT offset count]
    let cmd = &ctx.cmd;
    if cmd.len() < 5 {
        return resp_error("ERR wrong number of arguments for 'zrangestore' command");
    }
    let dst = match std::str::from_utf8(cmd[1]) {
        Ok(k) => k.to_string(),
        Err(_) => return resp_error("ERR dst must be valid UTF-8"),
    };
    let src = match parse_utf8(cmd[2]) {
        Some(k) => k,
        None => return resp_error("ERR src must be valid UTF-8"),
    };

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
    // Read src, apply range, write to dst — no RESP round-trip
    let pairs = {
        let db_ref = &ctx.store[*ctx.selected_db];
        let shard = store::get_shard(db_ref, src);
        let map = read_lock!(shard);

        match map.get(src) {
            None => vec![],
            Some(KiviDBValue::SortedSet(zs)) => zrange_pairs(&zs.read(), &params),
            Some(_) => {
                return resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                );
            }
        }
    }; // ← read lock on src is dropped here before we take write lock on dst

    // An empty result leaves no destination — whatever was there goes, as
    // with every STORE command.
    let count = pairs.len() as i64;
    let as_map: ahash::AHashMap<Vec<u8>, f64> = pairs.into_iter().collect();
    store_zset(ctx, &dst, as_map);
    resp_integer(count)
}

/// Read a SortedSetInner snapshot out of the store for a given key.
/// Returns None if key doesn't exist, Err if wrong type.
fn read_zset_snapshot(
    ctx: &Ctx,
    key: &str,
) -> Result<Option<std::collections::BTreeMap<Vec<u8>, f64>>, Vec<u8>> {
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => Ok(None),
        Some(KiviDBValue::SortedSet(zs)) => {
            // Clone member→score into a plain BTreeMap for manipulation.
            // We use Vec<u8> keys here (not Arc) since we're working with owned
            // data outside the lock — the lock is dropped after this block.
            let inner = zs.read();
            let snapshot: std::collections::BTreeMap<Vec<u8>, f64> = inner
                .members
                .iter()
                .map(|(k, &v)| (k.as_ref().to_vec(), v))
                .collect();
            Ok(Some(snapshot))
        }
        Some(KiviDBValue::Set(set)) => Ok(Some(
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
        .and_then(|raw| std::str::from_utf8(raw).ok())
        .and_then(|s| s.parse::<i64>().ok())
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
fn store_zset(ctx: &mut Ctx, dst_key: &str, members: ahash::AHashMap<Vec<u8>, f64>) {
    if members.is_empty() {
        crate::commands::helpers::delete_key(
            ctx.store,
            ctx.expiry_store,
            *ctx.selected_db,
            dst_key,
        );
        return;
    }
    crate::commands::helpers::clear_expiry(ctx.expiry_store, *ctx.selected_db, dst_key);
    let mut inner = SortedSetInner::new();
    for (member, score) in members {
        let arc: ZMember = Arc::from(member.as_slice());
        inner.insert(arc, score);
    }
    let zs = Arc::new(RwLock::new(inner));
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, dst_key);
    write_lock!(shard).insert(Key::from(dst_key), KiviDBValue::SortedSet(zs));
}

fn zrangebyscore(ctx: &mut Ctx) -> Vec<u8> {
    // ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'zrangebyscore' command");
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

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
fn execute_range(ctx: &Ctx, key: &str, params: &RangeParams, with_scores: bool) -> Vec<u8> {
    if let Some(error) = range_error(params) {
        return error;
    }
    let resp = reply_resp(ctx);
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_array(vec![]),
        Some(KiviDBValue::SortedSet(zs)) => {
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
    key: &str,
    want_max: bool,
    count: usize,
) -> Result<Vec<PoppedMember>, Vec<u8>> {
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);
    match map.get(key) {
        Some(KiviDBValue::SortedSet(zs)) => {
            let mut inner = zs.write();
            let mut popped = Vec::new();
            while popped.len() < count {
                let entry = if want_max {
                    inner.by_score.pop_last()
                } else {
                    inner.by_score.pop_first()
                };
                let Some(((score, member), _)) = entry else {
                    break;
                };
                inner.members.remove(member.as_ref());
                popped.push((resp_bulk(member.as_ref()), score_reply(resp, score.0)));
            }
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

    let keys: Vec<String> = cmd[1..cmd.len() - 1]
        .iter()
        .filter_map(|b| parse_utf8(b).map(|s| s.to_string()))
        .collect();

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
                            return Some(resp_array(vec![resp_bulk_str(key), member, score]));
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
        let key = match std::str::from_utf8(key_bytes) {
            Ok(k) => k,
            Err(_) => return resp_error("ERR key must be valid UTF-8"),
        };

        let db_ref = &ctx.store[*ctx.selected_db];
        let shard = store::get_shard(db_ref, key);
        let map = read_lock!(shard);

        match map.get(key) {
            None => continue,
            Some(KiviDBValue::SortedSet(zs)) => {
                let mut inner = zs.write();
                if inner.is_empty() {
                    continue;
                }

                let mut popped_pairs: Vec<Vec<u8>> = Vec::new();
                for _ in 0..count {
                    let entry = if want_max {
                        inner.by_score.pop_last()
                    } else {
                        inner.by_score.pop_first()
                    };
                    match entry {
                        None => break,
                        Some(((score, member), _)) => {
                            inner.members.remove(member.as_ref());
                            // Each pair is its own nested array: [member, score]
                            popped_pairs.push(resp_array(vec![
                                resp_bulk(member.as_ref()),
                                score_reply(resp, score.0),
                            ]));
                        }
                    }
                }

                if popped_pairs.is_empty() {
                    continue;
                }

                return resp_array(vec![resp_bulk_str(key), resp_array(popped_pairs)]);
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

    let keys: Vec<String> = cmd[3..3 + num_keys]
        .iter()
        .filter_map(|b| std::str::from_utf8(b).ok().map(|s| s.to_string()))
        .collect();

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
                        return Some(resp_array(vec![resp_bulk_str(key), resp_array(pairs)]));
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
