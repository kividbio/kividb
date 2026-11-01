//! Replication payloads, matching Redis: a write is propagated as the *effect*
//! it had, not as the command the client typed, whenever those differ.
//!
//! Two properties are needed, and they are not the same thing:
//!
//! * **Determinism** — a replica re-running the command must reach the master's
//!   state. `SPOP` picks random members and `XADD … *` mints an ID from the
//!   local clock, so replaying either produces a *different* result on the
//!   other side.
//! * **Idempotence** — replaying must be safe even if the effect is already
//!   present. This is what AOF recovery needs, because a snapshot taken while
//!   writes are in flight may already contain a write that is also in the log.
//!   `INCRBYFLOAT` is perfectly deterministic yet double-applies, which is why
//!   it is rewritten to an absolute `SET`.
//!
//! This was originally reached only from Lua (`redis.call`). The direct client
//! path propagated `ctx.cmd` verbatim, so `SPOP` and `XADD *` diverged a live
//! replica — measured, not theorised: a replica kept a different 12 of 20 set
//! members, and 4 of 5 stream IDs differed by a millisecond.

use crate::commands::ctx::Ctx;
use crate::commands::helpers::parse_utf8;
use crate::read_lock;
use std::time::UNIX_EPOCH;

thread_local! {
    /// What the command now running on this thread did, written down by the
    /// handler itself in a form that can be replayed. See [`effects_begin`].
    static EFFECTS: std::cell::RefCell<Option<Vec<Vec<Vec<u8>>>>> =
        const { std::cell::RefCell::new(None) };
    /// Whether one of those effects changed the dataset, rather than only
    /// metadata. See [`effect_of_metadata`].
    static CHANGED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Start recording the running command's effects.
///
/// Most rewrites below can work out what to propagate from the command and
/// its reply. A few cannot: which entries `XREADGROUP … CLAIM` took depends
/// on the clock and on every delivery before it, and whether an
/// `XADD … IDMP` added anything is not in its reply at all. Those handlers
/// call this on entry and [`effect`] for each change they make, and what they
/// recorded is what gets propagated — nothing, if they recorded nothing.
///
/// Thread-local rather than threaded through `Ctx`: the handler and the
/// propagation step run back to back on one thread, and every command that
/// reads the record begins a fresh one first, so a record left behind by a
/// write nobody was listening to is never mistaken for a later command's.
pub(crate) fn effects_begin() {
    EFFECTS.with_borrow_mut(|e| *e = Some(Vec::new()));
    CHANGED.set(false);
}

/// Record one replayable command as an effect of the running command.
pub(crate) fn effect(parts: Vec<Vec<u8>>) {
    CHANGED.set(true);
    effect_of_metadata(parts);
}

/// [`effect`], for a change Redis does not count as one: replicas and the
/// AOF get it, but `rdb_changes_since_last_save` does not grow. The delivery
/// count and time an `XREADGROUP` of a consumer's own pending entries bumps
/// are such a change (#27): Redis does not even propagate them.
pub(crate) fn effect_of_metadata(parts: Vec<Vec<u8>>) {
    EFFECTS.with_borrow_mut(|e| {
        if let Some(recorded) = e {
            recorded.push(parts);
        }
    });
}

/// Whether the running command's effects changed the dataset (see
/// [`effect_of_metadata`]). Not cleared when the effects are taken.
pub(crate) fn effects_changed_data() -> bool {
    CHANGED.get()
}

fn effects_take() -> Vec<Vec<Vec<u8>>> {
    EFFECTS.with_borrow_mut(|e| e.take()).unwrap_or_default()
}

/// Build one or more commands to send on the replication / AOF stream for a
/// successful write. Empty vec ⇒ nothing to replicate (e.g. a `SPOP` miss).
///
/// `resp` is the reply the command just produced; the rewrites read the effect
/// out of it (which members were popped, which ID was assigned).
pub fn commands_for_propagation(
    ctx: &Ctx<'_>,
    name: &str,
    cmd: &[&[u8]],
    resp: &[u8],
) -> Vec<Vec<Vec<u8>>> {
    // Upper case already, from every caller that runs commands; made so only
    // if it is not.
    let upper;
    let name: &str = if name.bytes().any(|b| b.is_ascii_lowercase()) {
        upper = name.to_ascii_uppercase();
        &upper
    } else {
        name
    };
    match name {
        "SPOP" => spop_feed(cmd, resp),
        "EXPIRE" | "PEXPIRE" => expire_feed(ctx, cmd, resp),
        // A TTL given as "this long from now" means something else to whoever
        // runs the command later — a replica a moment behind, or an AOF
        // replayed after a restart, where every TTL would start over. These
        // are sent with the moment of expiry instead.
        "SET" => set_feed(cmd),
        "SETEX" | "PSETEX" => setex_feed(name, cmd),
        "GETEX" => getex_feed(cmd, resp),
        "MSETEX" => vec![absolute_ttl_options(
            cmd,
            msetex_options_start(cmd),
            &[],
            None,
        )],
        "HEXPIRE" | "HPEXPIRE" => hexpire_feed(name, cmd),
        "HSETEX" | "HGETEX" => hash_ttl_feed(cmd),
        // A SET of the result, or DEL/UNLINK; see `cmd_increx`.
        "INCREX" => effects_take(),
        "RESTORE" => restore_feed(cmd),
        "INCRBYFLOAT" => incrbyfloat_feed(cmd, resp),
        "XADD" => xadd_feed(cmd, resp),
        // A `~` trim is sent as what it removed; see `exact_trim_args`.
        "XTRIM" if cmd.get(3).is_some_and(|mark| *mark == b"~") => effects_take(),
        // Propagated as the pending-list changes they made; see `effects_begin`.
        "XREADGROUP" | "XCLAIM" | "XAUTOCLAIM" => effects_take(),
        // Sent by the command itself as it went: the documents `DD` deleted
        // as DELs, then the drop without `DD` (`vector::ft_dropindex`); by a
        // replica to its own AOF too.
        "FT.DROPINDEX" => Vec::new(),
        "MIGRATE" => migrate_feed(cmd, resp),
        "LMOVEM" | "BLMOVEM" => lmovem_feed(name, cmd, resp),
        "TS.ADD" => ts_add_feed(cmd, resp),
        "TS.MADD" => ts_madd_feed(cmd, resp),
        "TS.INCRBY" | "TS.DECRBY" => ts_incrby_feed(cmd, resp),
        // The fieldset lives in the client's session, which no replica (and no
        // AOF replay) has: send the hash it produced instead.
        "HIMPORT" => crate::commands::hash_import::propagation(ctx.client_id, cmd),
        "BLMOVE" => blocking_move_feed(cmd, resp, "LMOVE", 5),
        "BRPOPLPUSH" => blocking_move_feed(cmd, resp, "RPOPLPUSH", 3),
        _ => {
            if cmd.is_empty() {
                return Vec::new();
            }
            vec![cmd.iter().map(|b| b.to_vec()).collect()]
        }
    }
}

/// Replace an auto-generated `XADD` ID (`*`, or `ms-*`) with the concrete ID the
/// master assigned, leaving every other argument alone.
///
/// Without this each side generates its own ID from its own clock, so entries
/// that are logically the same get different identities — and a stream ID *is*
/// the entry's identity, so consumer-group state, `XRANGE` bounds and `XACK`
/// all key differently on the replica.
///
/// An explicit ID is already deterministic and passes through untouched.
fn xadd_feed(cmd: &[&[u8]], resp: &[u8]) -> Vec<Vec<Vec<u8>>> {
    let verbatim = || vec![cmd.iter().map(|b| b.to_vec()).collect::<Vec<Vec<u8>>>()];

    let opts = crate::commands::streams::parse_xadd_options(cmd);
    // An idempotent XADD may have added nothing — a recognised retry replies
    // with the original entry's ID, which looks exactly like a fresh one. The
    // handler recorded what, if anything, it added.
    // Nor is how much a `~` trimmed.
    if opts.idmp.is_some() || opts.approx {
        return effects_take();
    }
    let Some(id_arg) = cmd.get(opts.id_index) else {
        return verbatim();
    };
    // `*` (both ms and seq auto) or `ms-*` (seq auto) — anything else is fixed.
    if !id_arg.contains(&b'*') {
        return verbatim();
    }

    // XADD replies with the assigned ID as a bulk string. Anything else (an
    // error, or NOMKSTREAM's null on a missing key) means no entry was added.
    let Some((_, Some(assigned))) = parse_bulk(resp) else {
        return Vec::new();
    };

    let mut parts: Vec<Vec<u8>> = cmd.iter().map(|b| b.to_vec()).collect();
    parts[opts.id_index] = assigned;
    vec![parts]
}

/// The keys a `MIGRATE` invocation names, in either form:
///
/// ```text
/// MIGRATE host port key   db timeout [COPY] [REPLACE] [AUTH ..] [AUTH2 ..]
/// MIGRATE host port ""    db timeout [...] KEYS key [key ...]
/// ```
///
/// `KEYS` is always the last option, so everything after it is a key.
pub(crate) fn migrate_keys<'a>(cmd: &[&'a [u8]]) -> Vec<&'a [u8]> {
    if cmd.len() < 6 {
        return Vec::new();
    }
    if !cmd[3].is_empty() {
        return vec![cmd[3]];
    }
    match cmd[6..]
        .iter()
        .position(|a| a.eq_ignore_ascii_case(b"KEYS"))
    {
        Some(i) => cmd[6 + i + 1..].to_vec(),
        None => Vec::new(),
    }
}

/// True when `MIGRATE` was asked to leave the source keys in place.
pub(crate) fn migrate_is_copy(cmd: &[&[u8]]) -> bool {
    let options_end = cmd
        .iter()
        .skip(6)
        .position(|a| a.eq_ignore_ascii_case(b"KEYS"))
        .map_or(cmd.len(), |i| 6 + i);
    cmd.get(6..options_end)
        .is_some_and(|opts| opts.iter().any(|a| a.eq_ignore_ascii_case(b"COPY")))
}

/// `MIGRATE` is propagated as the `DEL` of the keys it moved away, as Redis
/// does. Sending it verbatim would have every replica — and AOF replay — open
/// its own connection to the destination and migrate the keys again.
///
/// Nothing is propagated for `COPY` (the source keeps its keys) or for any
/// reply other than `+OK` (`NOKEY`, or an error that left the keys in place).
fn migrate_feed(cmd: &[&[u8]], resp: &[u8]) -> Vec<Vec<Vec<u8>>> {
    if !resp.starts_with(b"+OK") || migrate_is_copy(cmd) {
        return Vec::new();
    }
    let keys = migrate_keys(cmd);
    if keys.is_empty() {
        return Vec::new();
    }
    let mut parts = vec![b"DEL".to_vec()];
    parts.extend(keys.into_iter().map(|k| k.to_vec()));
    vec![parts]
}

/// `LMOVEM` propagates as itself when it moved something and as nothing when
/// it did not. `BLMOVEM` propagates as `LMOVEM`: the replica must not wait,
/// and by the time the master replies the elements are there to be moved.
fn lmovem_feed(name: &str, cmd: &[&[u8]], resp: &[u8]) -> Vec<Vec<Vec<u8>>> {
    // A non-empty array reply means elements moved; `*0` and `*-1` do not.
    let moved =
        resp.first() == Some(&b'*') && !resp.starts_with(b"*0\r\n") && !resp.starts_with(b"*-1");
    if !moved {
        return Vec::new();
    }
    let mut parts: Vec<Vec<u8>> = cmd.iter().map(|b| b.to_vec()).collect();
    if name == "BLMOVEM" && parts.len() >= 6 {
        parts.remove(5); // timeout
        parts[0] = b"LMOVEM".to_vec();
    }
    vec![parts]
}

/// `BLMOVE` / `BRPOPLPUSH` propagate as their non-blocking forms, and only
/// when an element actually moved. `timeout_index` is the argument to drop.
fn blocking_move_feed(
    cmd: &[&[u8]],
    resp: &[u8],
    plain_name: &str,
    timeout_index: usize,
) -> Vec<Vec<Vec<u8>>> {
    if !matches!(parse_bulk(resp), Some((_, Some(_)))) {
        return Vec::new();
    }
    let mut parts: Vec<Vec<u8>> = cmd.iter().map(|b| b.to_vec()).collect();
    if parts.len() > timeout_index {
        parts.remove(timeout_index);
    }
    parts[0] = plain_name.as_bytes().to_vec();
    vec![parts]
}

/// `TS.ADD key * value`: the `*` is the master's clock at the moment it ran.
/// A replica (or the AOF, replayed later) would stamp the sample with its own,
/// so the timestamp the master assigned is sent in its place.
fn ts_add_feed(cmd: &[&[u8]], resp: &[u8]) -> Vec<Vec<Vec<u8>>> {
    let mut parts: Vec<Vec<u8>> = cmd.iter().map(|b| b.to_vec()).collect();
    if cmd.len() > 2
        && cmd[2] == b"*"
        && let Some(ts) = parse_resp_integer(resp)
    {
        parts[2] = ts.to_string().into_bytes();
    }
    vec![parts]
}

/// `TS.MADD` with `*` timestamps: each replaced by the one it was given, read
/// from the matching element of the reply.
fn ts_madd_feed(cmd: &[&[u8]], resp: &[u8]) -> Vec<Vec<Vec<u8>>> {
    let mut parts: Vec<Vec<u8>> = cmd.iter().map(|b| b.to_vec()).collect();
    if !cmd.iter().skip(2).step_by(3).any(|ts| *ts == b"*") {
        return vec![parts];
    }
    // The reply is an array with one element per sample: `:timestamp`, or an
    // error for a sample that was refused.
    let mut lines = resp.split(|b| *b == b'\n');
    lines.next(); // the array header
    for (sample, line) in lines.enumerate() {
        let ts_index = 2 + sample * 3;
        if ts_index >= parts.len() {
            break;
        }
        if cmd[ts_index] == b"*"
            && let Some(assigned) = line.strip_prefix(b":")
        {
            parts[ts_index] = assigned.trim_ascii().to_vec();
        }
    }
    vec![parts]
}

/// `TS.INCRBY` / `TS.DECRBY` default to "now" too; pin the timestamp used.
fn ts_incrby_feed(cmd: &[&[u8]], resp: &[u8]) -> Vec<Vec<Vec<u8>>> {
    let verbatim = || vec![cmd.iter().map(|b| b.to_vec()).collect::<Vec<Vec<u8>>>()];
    let Some(ts) = parse_resp_integer(resp) else {
        return verbatim();
    };
    let mut parts: Vec<Vec<u8>> = Vec::with_capacity(cmd.len() + 2);
    let mut i = 0;
    while i < cmd.len() {
        if i >= 3 && cmd[i].eq_ignore_ascii_case(b"TIMESTAMP") {
            // Dropped here, re-added below with the concrete value.
            i += 2;
            continue;
        }
        parts.push(cmd[i].to_vec());
        i += 1;
    }
    parts.push(b"TIMESTAMP".to_vec());
    parts.push(ts.to_string().into_bytes());
    vec![parts]
}

fn spop_feed(cmd: &[&[u8]], resp: &[u8]) -> Vec<Vec<Vec<u8>>> {
    if cmd.len() < 2 {
        return Vec::new();
    }
    let key = cmd[1].to_vec();
    match resp.first().copied() {
        Some(b'$') => {
            if let Some((_n, member_opt)) = parse_bulk(resp)
                && let Some(member) = member_opt
            {
                return vec![vec![b"SREM".to_vec(), key, member]];
            }
            Vec::new()
        }
        Some(b'*') => {
            if let Some((_n, members)) = parse_array(resp) {
                if members.is_empty() {
                    return Vec::new();
                }
                let mut parts = vec![b"SREM".to_vec(), key];
                parts.extend(members);
                return vec![parts];
            }
            Vec::new()
        }
        _ => Vec::new(),
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn integer_arg(arg: &[u8]) -> Option<i64> {
    parse_utf8(arg).and_then(|s| s.parse().ok())
}

fn owned(cmd: &[&[u8]]) -> Vec<Vec<u8>> {
    cmd.iter().map(|arg| arg.to_vec()).collect()
}

/// When `key` expires, in Unix milliseconds, if it has a TTL.
fn expiry_ms(ctx: &Ctx<'_>, key: &str) -> Option<u64> {
    let shard = crate::store::get_shard(&ctx.store[*ctx.selected_db], key);
    read_lock!(shard).expires(key)
}

/// `cmd` with a relative `EX seconds` / `PX milliseconds` option, looked for
/// from `start`, replaced by `PXAT <unix-ms>`.
///
/// `with_argument` names the options that take an argument, so that argument
/// is not read as an option; the scan stops at `stop`, if given.
fn absolute_ttl_options(
    cmd: &[&[u8]],
    start: usize,
    with_argument: &[&str],
    stop: Option<&str>,
) -> Vec<Vec<u8>> {
    let mut out = owned(cmd);
    let mut i = start;
    while i < cmd.len() {
        let token = String::from_utf8_lossy(cmd[i]).to_ascii_uppercase();
        if stop == Some(token.as_str()) {
            break;
        }
        if token == "EX" || token == "PX" {
            if let Some(ttl) = cmd.get(i + 1).and_then(|arg| integer_arg(arg)) {
                let ms = if token == "EX" {
                    ttl.saturating_mul(1000)
                } else {
                    ttl
                };
                out[i] = b"PXAT".to_vec();
                out[i + 1] = now_ms().saturating_add(ms).to_string().into_bytes();
            }
            break;
        }
        i += if with_argument.contains(&token.as_str()) || token == "EXAT" || token == "PXAT" {
            2
        } else {
            1
        };
    }
    out
}

/// `SET key value [...] EX|PX ttl` → the same with `PXAT`.
fn set_feed(cmd: &[&[u8]]) -> Vec<Vec<Vec<u8>>> {
    vec![absolute_ttl_options(
        cmd,
        3,
        &["IFEQ", "IFNE", "IFDEQ", "IFDNE"],
        None,
    )]
}

/// `SETEX key seconds value` / `PSETEX key ms value` → `SET key value PXAT at`.
fn setex_feed(name: &str, cmd: &[&[u8]]) -> Vec<Vec<Vec<u8>>> {
    let ttl = match cmd.get(2).and_then(|arg| integer_arg(arg)) {
        Some(ttl) if cmd.len() == 4 => ttl,
        _ => return vec![owned(cmd)],
    };
    let ms = if name == "SETEX" {
        ttl.saturating_mul(1000)
    } else {
        ttl
    };
    vec![vec![
        b"SET".to_vec(),
        cmd[1].to_vec(),
        cmd[3].to_vec(),
        b"PXAT".to_vec(),
        now_ms().saturating_add(ms).to_string().into_bytes(),
    ]]
}

/// `GETEX` reads; what it changes is the key's TTL, and that is what is sent.
fn getex_feed(cmd: &[&[u8]], resp: &[u8]) -> Vec<Vec<Vec<u8>>> {
    // Nothing there, or no option: nothing changed.
    if cmd.len() < 3 || resp.starts_with(b"$-1") || resp.starts_with(b"_") {
        return Vec::new();
    }
    let option = String::from_utf8_lossy(cmd[2]).to_ascii_uppercase();
    if option == "PERSIST" {
        return vec![vec![b"PERSIST".to_vec(), cmd[1].to_vec()]];
    }
    let Some(n) = cmd.get(3).and_then(|arg| integer_arg(arg)) else {
        return vec![owned(cmd)];
    };
    let at = match option.as_str() {
        "EX" => now_ms().saturating_add(n.saturating_mul(1000)),
        "PX" => now_ms().saturating_add(n),
        "EXAT" => n.saturating_mul(1000),
        "PXAT" => n,
        _ => return vec![owned(cmd)],
    };
    vec![vec![
        b"PEXPIREAT".to_vec(),
        cmd[1].to_vec(),
        at.to_string().into_bytes(),
    ]]
}

/// Where the options of `MSETEX numkeys key value [key value ...] [options]`
/// begin.
fn msetex_options_start(cmd: &[&[u8]]) -> usize {
    let numkeys = cmd
        .get(1)
        .and_then(|arg| integer_arg(arg))
        .filter(|n| *n > 0)
        .unwrap_or(0) as usize;
    2usize.saturating_add(numkeys.saturating_mul(2))
}

/// `HEXPIRE key seconds …` / `HPEXPIRE key ms …` → `HPEXPIREAT key at …`.
fn hexpire_feed(name: &str, cmd: &[&[u8]]) -> Vec<Vec<Vec<u8>>> {
    let Some(ttl) = cmd.get(2).and_then(|arg| integer_arg(arg)) else {
        return vec![owned(cmd)];
    };
    let ms = if name == "HEXPIRE" {
        ttl.saturating_mul(1000)
    } else {
        ttl
    };
    let mut out = owned(cmd);
    out[0] = b"HPEXPIREAT".to_vec();
    out[2] = now_ms().saturating_add(ms).to_string().into_bytes();
    vec![out]
}

/// `HSETEX` / `HGETEX key [options] FIELDS …` with a relative TTL → `PXAT`.
fn hash_ttl_feed(cmd: &[&[u8]]) -> Vec<Vec<Vec<u8>>> {
    // The older `HSETEX key seconds …` form has no absolute spelling (its
    // KEEPTTL means "keep a TTL, or else use this one"); it goes as it came.
    if cmd.get(2).is_some_and(|arg| integer_arg(arg).is_some()) {
        return vec![owned(cmd)];
    }
    vec![absolute_ttl_options(cmd, 2, &[], Some("FIELDS"))]
}

/// `INCREX` can refuse to move (a bound), can round (BYFLOAT), and can set a
/// TTL only when there is none (ENX): what is sent is where it left the key.
/// `RESTORE key ttl payload …` with a relative TTL → the same with `ABSTTL`.
fn restore_feed(cmd: &[&[u8]]) -> Vec<Vec<Vec<u8>>> {
    let mut out = owned(cmd);
    let absolute = cmd
        .iter()
        .skip(4)
        .any(|arg| arg.eq_ignore_ascii_case(b"ABSTTL"));
    if !absolute
        && let Some(ttl) = cmd.get(2).and_then(|arg| integer_arg(arg))
        && ttl > 0
    {
        out[2] = now_ms().saturating_add(ttl).to_string().into_bytes();
        out.push(b"ABSTTL".to_vec());
    }
    vec![out]
}

/// `EXPIRE key seconds` / `PEXPIRE key ms` → `PEXPIREAT key at`, or `DEL key`
/// when the TTL was not in the future and the key went at once.
fn expire_feed(ctx: &Ctx<'_>, cmd: &[&[u8]], resp: &[u8]) -> Vec<Vec<Vec<u8>>> {
    if cmd.len() < 3 {
        return Vec::new();
    }
    let Some(key) = parse_utf8(cmd[1]) else {
        return Vec::new();
    };
    let Some(ttl) = integer_arg(cmd[2]) else {
        return Vec::new();
    };
    // 0: no such key, or a NX/XX/GT/LT condition that did not hold.
    if parse_resp_integer(resp) != Some(1) {
        return Vec::new();
    }

    // Redis: a TTL of zero or less deletes the key.
    if ttl <= 0 {
        return vec![vec![b"DEL".to_vec(), cmd[1].to_vec()]];
    }

    let Some(ms) = expiry_ms(ctx, key) else {
        return Vec::new();
    };
    vec![vec![
        b"PEXPIREAT".to_vec(),
        cmd[1].to_vec(),
        ms.to_string().into_bytes(),
    ]]
}

fn incrbyfloat_feed(cmd: &[&[u8]], resp: &[u8]) -> Vec<Vec<Vec<u8>>> {
    if cmd.len() != 3 {
        return Vec::new();
    }
    let Some((_n, val_opt)) = parse_bulk(resp) else {
        return Vec::new();
    };
    let Some(val) = val_opt else {
        return Vec::new();
    };
    vec![vec![
        b"SET".to_vec(),
        cmd[1].to_vec(),
        val,
        b"KEEPTTL".to_vec(),
    ]]
}

fn parse_bulk(resp: &[u8]) -> Option<(usize, Option<Vec<u8>>)> {
    if resp.first()? != &b'$' {
        return None;
    }
    let line_end = resp.windows(2).position(|w| w == b"\r\n")?;
    let len_s = std::str::from_utf8(resp.get(1..line_end)?).ok()?;
    let n: isize = len_s.parse().ok()?;
    let header_end = line_end + 2;
    if n < 0 {
        return Some((header_end, None));
    }
    let n = usize::try_from(n).ok()?;
    let body_end = header_end.checked_add(n)?.checked_add(2)?;
    if body_end > resp.len() {
        return None;
    }
    Some((body_end, Some(resp[header_end..header_end + n].to_vec())))
}

fn parse_array(resp: &[u8]) -> Option<(usize, Vec<Vec<u8>>)> {
    if resp.first()? != &b'*' {
        return None;
    }
    let line_end = resp.windows(2).position(|w| w == b"\r\n")?;
    let count_s = std::str::from_utf8(resp.get(1..line_end)?).ok()?;
    let count: usize = count_s.parse().ok()?;
    let mut pos = line_end + 2;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let tail = resp.get(pos..)?;
        let (consumed, bulk) = parse_bulk(tail)?;
        pos += consumed;
        out.push(bulk?);
    }
    Some((pos, out))
}

fn parse_resp_integer(resp: &[u8]) -> Option<i64> {
    if resp.first()? != &b':' {
        return None;
    }
    let line_end = resp.windows(2).position(|w| w == b"\r\n")?;
    let s = std::str::from_utf8(resp.get(1..line_end)?).ok()?;
    s.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::streams::parse_xadd_options;

    fn cmd(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    fn refs(owned: &[Vec<u8>]) -> Vec<&[u8]> {
        owned.iter().map(|v| v.as_slice()).collect()
    }

    fn bulk(s: &str) -> Vec<u8> {
        format!("${}\r\n{}\r\n", s.len(), s).into_bytes()
    }

    /// The rewrite must replace the argument the executor consumed as the ID.
    /// XADD's ID is positional but its offset depends on the options given, so
    /// a hardcoded index would corrupt a `MAXLEN`/`NOMKSTREAM` command by
    /// overwriting an option value instead — silently, on the wire.
    #[test]
    fn xadd_id_index_matches_executor() {
        // (command, index the ID actually sits at)
        let cases: &[(&[&str], usize)] = &[
            (&["XADD", "k", "*", "f", "v"], 2),
            (&["XADD", "k", "NOMKSTREAM", "*", "f", "v"], 3),
            (&["XADD", "k", "MAXLEN", "5", "*", "f", "v"], 4),
            (&["XADD", "k", "MAXLEN", "~", "5", "*", "f", "v"], 5),
            (&["XADD", "k", "MAXLEN", "=", "5", "*", "f", "v"], 5),
            (
                &[
                    "XADD", "k", "MAXLEN", "~", "5", "LIMIT", "100", "*", "f", "v",
                ],
                7,
            ),
            (&["XADD", "k", "MINID", "~", "5-5", "*", "f", "v"], 5),
            (
                &[
                    "XADD",
                    "k",
                    "NOMKSTREAM",
                    "MAXLEN",
                    "=",
                    "10",
                    "123-4",
                    "f",
                    "v",
                ],
                6,
            ),
        ];
        for (parts, want) in cases {
            let owned = cmd(parts);
            let got = parse_xadd_options(&refs(&owned)).id_index;
            assert_eq!(got, *want, "wrong ID index for {parts:?}");
            // And that index really is the ID-looking token.
            let tok = std::str::from_utf8(&owned[got]).unwrap();
            assert!(
                tok == "*" || tok.contains('-'),
                "index {got} of {parts:?} is {tok:?}, not an ID"
            );
        }
    }

    #[test]
    fn xadd_auto_id_is_rewritten_to_the_assigned_id() {
        let owned = cmd(&["XADD", "s", "*", "f", "v"]);
        let out = xadd_feed(&refs(&owned), &bulk("1786732884990-0"));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0][2], b"1786732884990-0".to_vec());
        // Everything else is untouched.
        assert_eq!(out[0][0], b"XADD".to_vec());
        assert_eq!(out[0][1], b"s".to_vec());
        assert_eq!(out[0][3], b"f".to_vec());
        assert_eq!(out[0][4], b"v".to_vec());
    }

    #[test]
    fn xadd_partial_auto_id_is_rewritten() {
        // `ms-*` auto-generates only the sequence, and is just as divergent.
        let owned = cmd(&["XADD", "s", "5-*", "f", "v"]);
        let out = xadd_feed(&refs(&owned), &bulk("5-3"));
        assert_eq!(out[0][2], b"5-3".to_vec());
    }

    #[test]
    fn xadd_rewrite_lands_after_options() {
        let owned = cmd(&["XADD", "s", "MAXLEN", "=", "5", "*", "f", "v"]);
        let out = xadd_feed(&refs(&owned), &bulk("7-0"));
        assert_eq!(out[0][5], b"7-0".to_vec());
        // The option value must NOT have been clobbered.
        assert_eq!(out[0][4], b"5".to_vec());
    }

    #[test]
    fn xadd_with_an_approximate_trim_is_sent_as_what_the_handler_recorded() {
        // How much `~` trimmed is the handler's to say: nothing is made up
        // from the command.
        let owned = cmd(&["XADD", "s", "MAXLEN", "~", "5", "*", "f", "v"]);
        effects_begin();
        effect(cmd(&["XADD", "s", "MAXLEN", "=", "3", "7-0", "f", "v"]));
        let out = xadd_feed(&refs(&owned), &bulk("7-0"));
        assert_eq!(
            out,
            vec![cmd(&["XADD", "s", "MAXLEN", "=", "3", "7-0", "f", "v"])]
        );
        assert!(xadd_feed(&refs(&owned), &bulk("7-0")).is_empty());
    }

    #[test]
    fn xadd_explicit_id_passes_through_unchanged() {
        let owned = cmd(&["XADD", "s", "5-5", "f", "v"]);
        let out = xadd_feed(&refs(&owned), &bulk("5-5"));
        assert_eq!(out[0], owned);
    }

    #[test]
    fn xadd_that_added_nothing_propagates_nothing() {
        // NOMKSTREAM against a missing key replies null: no entry, nothing to
        // replicate. Propagating verbatim here would create the stream on the
        // replica that the master declined to create.
        let owned = cmd(&["XADD", "s", "NOMKSTREAM", "*", "f", "v"]);
        assert!(xadd_feed(&refs(&owned), b"$-1\r\n").is_empty());
    }

    #[test]
    fn migrate_is_propagated_as_del_of_the_moved_keys() {
        let owned = cmd(&["MIGRATE", "10.0.0.2", "6379", "k1", "0", "5000"]);
        let out = commands_for_propagation_test("MIGRATE", &refs(&owned), b"+OK\r\n");
        assert_eq!(out, vec![cmd(&["DEL", "k1"])]);

        let owned = cmd(&[
            "MIGRATE", "10.0.0.2", "6379", "", "0", "5000", "REPLACE", "KEYS", "a", "b", "c",
        ]);
        let out = commands_for_propagation_test("MIGRATE", &refs(&owned), b"+OK\r\n");
        assert_eq!(out, vec![cmd(&["DEL", "a", "b", "c"])]);
    }

    #[test]
    fn migrate_that_moved_nothing_propagates_nothing() {
        // COPY leaves the source keys in place.
        let owned = cmd(&["MIGRATE", "h", "1", "k1", "0", "5000", "COPY"]);
        assert!(commands_for_propagation_test("MIGRATE", &refs(&owned), b"+OK\r\n").is_empty());
        // NOKEY: nothing existed to move.
        let owned = cmd(&["MIGRATE", "h", "1", "k1", "0", "5000"]);
        assert!(commands_for_propagation_test("MIGRATE", &refs(&owned), b"+NOKEY\r\n").is_empty());
        // A failed transfer leaves the keys where they were.
        assert!(
            commands_for_propagation_test("MIGRATE", &refs(&owned), b"-IOERR timeout\r\n")
                .is_empty()
        );
    }

    /// A key literally named `COPY` or `KEYS` must not be read as an option.
    #[test]
    fn migrate_options_are_not_confused_with_key_names() {
        let owned = cmd(&["MIGRATE", "h", "1", "", "0", "5000", "KEYS", "COPY", "x"]);
        assert!(!migrate_is_copy(&refs(&owned)));
        assert_eq!(
            migrate_keys(&refs(&owned)),
            vec![b"COPY".as_slice(), b"x".as_slice()]
        );
        let owned = cmd(&["MIGRATE", "h", "1", "COPY", "0", "5000"]);
        assert!(!migrate_is_copy(&refs(&owned)));
    }

    /// `migrate_feed` without needing a `Ctx`.
    fn commands_for_propagation_test(name: &str, cmd: &[&[u8]], resp: &[u8]) -> Vec<Vec<Vec<u8>>> {
        match name {
            "MIGRATE" => migrate_feed(cmd, resp),
            _ => unreachable!(),
        }
    }

    #[test]
    fn time_series_clock_timestamps_are_pinned() {
        let c = cmd(&["TS.ADD", "k", "*", "1.5", "LABELS", "a", "b"]);
        assert_eq!(
            ts_add_feed(&refs(&c), b":1700000000123\r\n"),
            vec![cmd(&[
                "TS.ADD",
                "k",
                "1700000000123",
                "1.5",
                "LABELS",
                "a",
                "b"
            ])]
        );
        let explicit = cmd(&["TS.ADD", "k", "5", "1.5"]);
        assert_eq!(
            ts_add_feed(&refs(&explicit), b":5\r\n"),
            vec![explicit.clone()]
        );

        let c = cmd(&["TS.MADD", "a", "*", "1", "b", "7", "2", "c", "*", "3"]);
        assert_eq!(
            ts_madd_feed(&refs(&c), b"*3\r\n:100\r\n:7\r\n:101\r\n"),
            vec![cmd(&[
                "TS.MADD", "a", "100", "1", "b", "7", "2", "c", "101", "3"
            ])]
        );

        let c = cmd(&["TS.INCRBY", "k", "2"]);
        assert_eq!(
            ts_incrby_feed(&refs(&c), b":42\r\n"),
            vec![cmd(&["TS.INCRBY", "k", "2", "TIMESTAMP", "42"])]
        );
        let c = cmd(&["TS.DECRBY", "k", "2", "TIMESTAMP", "*", "RETENTION", "9"]);
        assert_eq!(
            ts_incrby_feed(&refs(&c), b":42\r\n"),
            vec![cmd(&[
                "TS.DECRBY",
                "k",
                "2",
                "RETENTION",
                "9",
                "TIMESTAMP",
                "42"
            ])]
        );
    }

    #[test]
    fn blocking_moves_propagate_as_their_non_blocking_forms() {
        let c = cmd(&[
            "BLMOVEM", "s", "d", "LEFT", "RIGHT", "5", "COUNT", "2", "OBO",
        ]);
        let moved = b"*1\r\n$1\r\na\r\n";
        assert_eq!(
            lmovem_feed("BLMOVEM", &refs(&c), moved),
            vec![cmd(&[
                "LMOVEM", "s", "d", "LEFT", "RIGHT", "COUNT", "2", "OBO"
            ])]
        );
        assert!(lmovem_feed("BLMOVEM", &refs(&c), b"*-1\r\n").is_empty());
        assert!(lmovem_feed("BLMOVEM", &refs(&c), b"*0\r\n").is_empty());

        let c = cmd(&["LMOVEM", "s", "d", "LEFT", "RIGHT"]);
        assert_eq!(lmovem_feed("LMOVEM", &refs(&c), moved), vec![c.clone()]);

        let c = cmd(&["BLMOVE", "s", "d", "LEFT", "RIGHT", "0"]);
        assert_eq!(
            blocking_move_feed(&refs(&c), &bulk("a"), "LMOVE", 5),
            vec![cmd(&["LMOVE", "s", "d", "LEFT", "RIGHT"])]
        );
        assert!(blocking_move_feed(&refs(&c), b"$-1\r\n", "LMOVE", 5).is_empty());

        let c = cmd(&["BRPOPLPUSH", "s", "d", "0"]);
        assert_eq!(
            blocking_move_feed(&refs(&c), &bulk("a"), "RPOPLPUSH", 3),
            vec![cmd(&["RPOPLPUSH", "s", "d"])]
        );
    }

    #[test]
    fn spop_is_rewritten_to_srem_of_the_actual_members() {
        let owned = cmd(&["SPOP", "s", "2"]);
        let resp = b"*2\r\n$3\r\nm01\r\n$3\r\nm07\r\n";
        let out = spop_feed(&refs(&owned), resp);
        assert_eq!(
            out[0],
            vec![
                b"SREM".to_vec(),
                b"s".to_vec(),
                b"m01".to_vec(),
                b"m07".to_vec()
            ]
        );
    }

    #[test]
    fn spop_miss_propagates_nothing() {
        let owned = cmd(&["SPOP", "s"]);
        assert!(spop_feed(&refs(&owned), b"$-1\r\n").is_empty());
    }

    #[test]
    fn incrbyfloat_is_rewritten_to_an_absolute_set() {
        // Deterministic, but NOT idempotent: replaying the delta over a
        // snapshot that already contains it would double-apply.
        let owned = cmd(&["INCRBYFLOAT", "f", "1.1"]);
        let out = incrbyfloat_feed(&refs(&owned), &bulk("6.5"));
        assert_eq!(
            out[0],
            vec![
                b"SET".to_vec(),
                b"f".to_vec(),
                b"6.5".to_vec(),
                b"KEEPTTL".to_vec()
            ]
        );
    }
}
