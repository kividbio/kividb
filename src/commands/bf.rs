//! `BF.*`: Bloom filters, as Redis 8 has them built in (formerly
//! RedisBloom).
//!
//! The replies and errors are Redis 8.10's, byte for byte, in RESP2 and
//! RESP3, quirks included: `BF.EXISTS` and `BF.MEXISTS` answer 0 for a key
//! of another type rather than `WRONGTYPE`; `BF.MADD` and `BF.INSERT` stop
//! at an item a full filter cannot take, and the reply ends with that
//! item's error; `BF.RESERVE` ignores words it does not know; the error
//! messages differ between `BF.RESERVE` (`ERR bad capacity`) and
//! `BF.INSERT` (`Bad capacity`). The filter itself is `sketch::bloom`.
//!
//! Two things Redis leaves to chance are made definite here:
//!
//! * A filter a command makes for want of one (`BF.ADD`, `BF.MADD`,
//!   `BF.INSERT` on a missing key) is made with the `bf-*` settings. It is
//!   sent to replicas and the AOF as a `BF.INSERT` naming them, so that a
//!   replica, or a replay, with other settings makes the same filter. A
//!   `BF.RESERVE` without `EXPANSION` is sent with the one it took from
//!   `bf-expansion-factor` (one that does not scale, which may not name
//!   one, as the `BF.LOADCHUNK` of its header).
//! * A write that found the key, or a filter in its way, invalidates a
//!   `WATCH` on it even when it fails, as Redis's do (see [`take_touched`]).
//!
//! Beyond Redis: a filter is held to size limits (`bloom-max-filter-bytes`
//! and those of `sketch::bloom`), and a command from a client that would
//! allocate more than `maxmemory` leaves room for evicts first, under the
//! policy, and is refused with `OOM` only if that is not enough. A command
//! from a master or the AOF is held to neither `maxmemory` nor
//! `bloom-max-filter-bytes`, only to the fixed limits, so that a replica or
//! a replay never drops what the master took.
//!
//! A command of many items (`BF.MADD`, `BF.INSERT`, `BF.MEXISTS`) is held
//! to a per-command limit (`sketch::bloom::PROBE_BUDGET`): it stops at the
//! item that could take it past the limit, and its reply ends with an
//! error for that item, as it ends at an item a full filter cannot take.
//! A write that stopped early is sent to replicas and the AOF with the
//! items it took and no others.

use std::cell::{Cell, RefCell};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::commands::ctx::Ctx;
use crate::resp::write::{
    resp_array, resp_bool_resp3, resp_bulk, resp_bulk_str, resp_error, resp_integer, resp_map_for,
    resp_null_for, resp_simple,
};
use crate::sketch::Sketch;
use crate::sketch::bloom::{
    BloomChain, BloomError, Limits, MAX_CAPACITY, MAX_ERROR, MAX_EXPANSION, MAX_MAX_FILTER_BYTES,
    NO_MEMORY, Params, Probes, ShapeError,
};
use crate::store::{self, Key, ValueRef};
use crate::types::KiviDBValue;
use crate::{read_lock, write_lock};

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const NOT_FOUND: &str = "ERR not found";
const FULL: &str = "ERR non scaling filter is full";
/// A filter that grows and cannot: Redis's words.
const CANNOT_GROW: &str = "ERR problem inserting into filter";
const BAD_DATA: &str = "ERR received bad data";
const TOO_LARGE: &str = "ERR filter is too large";
const OOM: &str = "OOM command not allowed when used memory > 'maxmemory'.";
/// A command of many items at its per-command limit.
const OVER_BUDGET: &str =
    "ERR command reached the per-command limit for Bloom filters; send fewer items per command";

// ── Settings ─────────────────────────────────────────────────────────────

/// `bf-error-rate`, as its bits: 0.01.
static ERROR_RATE: AtomicU64 = AtomicU64::new(0x3f84_7ae1_47ae_147b);
/// `bf-initial-size`.
static INITIAL_SIZE: AtomicU64 = AtomicU64::new(100);
/// `bf-expansion-factor`.
static EXPANSION: AtomicU32 = AtomicU32::new(2);

fn default_error() -> f64 {
    f64::from_bits(ERROR_RATE.load(Ordering::Relaxed))
}

/// `CONFIG SET` of a `bf-*` setting, with Redis's checks and messages.
/// An error rate above 0.25 is kept as 0.25, as Redis keeps it.
/// `bloom-max-filter-bytes` is KiviDB's: a size, with units as `maxmemory`
/// takes them.
pub(crate) fn config_set(param: &str, value: &str) -> Vec<u8> {
    let failed = |why: String| {
        resp_error(&format!(
            "ERR CONFIG SET failed (possibly related to argument '{param}') - {why}"
        ))
    };
    let invalid = || failed(format!("Invalid value for `{param}`"));
    match param {
        "bf-error-rate" => match parse_double(value.as_bytes()) {
            None => invalid(),
            Some(e) if !(e > 0.0 && e < 1.0) => failed(format!(
                "Value for `{param}` must be in the range (0.000000, 1.000000)"
            )),
            Some(e) => {
                ERROR_RATE.store(e.min(MAX_ERROR).to_bits(), Ordering::Relaxed);
                resp_simple("OK")
            }
        },
        "bf-initial-size" | "bf-expansion-factor" => {
            let (min, max) = if param == "bf-initial-size" {
                (1, MAX_CAPACITY)
            } else {
                (0, MAX_EXPANSION)
            };
            match parse_ll(value.as_bytes()) {
                None => invalid(),
                Some(v) if v < min || v > max => failed(format!(
                    "Value for `{param}` must be in the range [{min}, {max}]"
                )),
                Some(v) if param == "bf-initial-size" => {
                    INITIAL_SIZE.store(v as u64, Ordering::Relaxed);
                    resp_simple("OK")
                }
                Some(v) => {
                    EXPANSION.store(v as u32, Ordering::Relaxed);
                    resp_simple("OK")
                }
            }
        }
        "bloom-max-filter-bytes" => match crate::config::parse_memory(param, value) {
            Err(_) => invalid(),
            Ok(v) if !(1..=MAX_MAX_FILTER_BYTES).contains(&v) => failed(format!(
                "Value for `{param}` must be in the range [1, {MAX_MAX_FILTER_BYTES}]"
            )),
            Ok(v) => {
                crate::sketch::bloom::set_max_filter_bytes(v);
                resp_simple("OK")
            }
        },
        _ => resp_error("ERR Unknown option"),
    }
}

/// The `bf-*` settings as `CONFIG GET` lists them, in Redis's order.
pub(crate) fn config_rows() -> Vec<(&'static str, String)> {
    vec![
        (
            "bf-initial-size",
            INITIAL_SIZE.load(Ordering::Relaxed).to_string(),
        ),
        (
            "bf-expansion-factor",
            EXPANSION.load(Ordering::Relaxed).to_string(),
        ),
        ("bf-error-rate", format!("{}", default_error())),
        (
            "bloom-max-filter-bytes",
            crate::sketch::bloom::max_filter_bytes().to_string(),
        ),
    ]
}

/// Each command's arity, as Redis's `COMMAND` gives it. The handlers check
/// their own arguments; this is for `COMMAND INFO` alone, and kept out of
/// the table every command is looked up in.
pub(crate) fn arity(name: &str) -> Option<i16> {
    Some(match name {
        "BF.ADD" | "BF.EXISTS" | "BF.SCANDUMP" => 3,
        "BF.MADD" | "BF.MEXISTS" => -3,
        "BF.RESERVE" | "BF.INSERT" => -4,
        "BF.INFO" => -2,
        "BF.CARD" | "BF.DEBUG" => 2,
        "BF.LOADCHUNK" => 4,
        _ => return None,
    })
}

/// What a filter made for want of one is made with.
fn default_params() -> Params {
    Params {
        capacity: INITIAL_SIZE.load(Ordering::Relaxed),
        error: default_error(),
        expansion: EXPANSION.load(Ordering::Relaxed),
        nonscaling: false,
    }
}

// ── Propagation and WATCH ────────────────────────────────────────────────

thread_local! {
    /// What a command is to be sent as instead of itself: the `BF.INSERT`
    /// naming how it made its filter, or the items it took when it stopped
    /// early (none, nothing).
    static MADE: RefCell<Option<Vec<Vec<Vec<u8>>>>> = const { RefCell::new(None) };
    /// Whether the running write got as far as the key.
    static TOUCHED: Cell<bool> = const { Cell::new(false) };
}

fn begin_write() {
    MADE.with_borrow_mut(|made| *made = None);
    TOUCHED.set(false);
}

/// What `BF.ADD`, `BF.MADD`, `BF.INSERT` or `BF.RESERVE` (`cmd`) is sent to
/// replicas and the AOF as: itself; or, if it made its filter with a
/// setting of this node's, a command that names what it took; or, if it
/// stopped before its last item, one with the items it took.
pub(crate) fn propagation(cmd: &[&[u8]]) -> Vec<Vec<Vec<u8>>> {
    match MADE.with_borrow_mut(Option::take) {
        Some(cmds) => cmds,
        None => vec![cmd.iter().map(|b| b.to_vec()).collect()],
    }
}

fn send_as(cmds: Vec<Vec<Vec<u8>>>) {
    MADE.with_borrow_mut(|m| *m = Some(cmds));
}

/// What a `BF.MADD` or `BF.INSERT` (`cmd`, its items the last `items` of
/// it) that took the first `done` of them is to be sent as, if not as
/// itself: `made` is the parameters it made the filter with, if it made
/// it, and `header` the filter's header if it took no item.
fn sent_items(
    cmd: &[&[u8]],
    key: &[u8],
    items: &[&[u8]],
    done: usize,
    made: Option<&Params>,
    header: Option<Vec<u8>>,
) -> Option<Vec<Vec<Vec<u8>>>> {
    match (made, header) {
        (Some(params), _) if done > 0 => Some(vec![insert_form(key, params, &items[..done])]),
        // Made, and nothing put in it: the empty filter, as its header.
        (Some(_), Some(header)) => Some(vec![vec![
            b"BF.LOADCHUNK".to_vec(),
            key.to_vec(),
            b"1".to_vec(),
            header,
        ]]),
        (Some(_), None) => None,
        (None, _) if done == items.len() => None,
        (None, _) if done == 0 => Some(Vec::new()),
        (None, _) => {
            let head = &cmd[..cmd.len() - items.len()];
            Some(vec![
                head.iter()
                    .chain(&items[..done])
                    .map(|b| b.to_vec())
                    .collect(),
            ])
        }
    }
}

/// Whether the `BF.*` write that just failed had got as far as its key,
/// so that a `WATCH` on the key is invalidated, as Redis invalidates it
/// for a write that opened its key whatever came of it.
pub(crate) fn take_touched() -> bool {
    TOUCHED.replace(false)
}

fn insert_form(key: &[u8], p: &Params, items: &[&[u8]]) -> Vec<Vec<u8>> {
    let mut parts: Vec<Vec<u8>> = vec![
        b"BF.INSERT".to_vec(),
        key.to_vec(),
        b"CAPACITY".to_vec(),
        p.capacity.to_string().into_bytes(),
        b"ERROR".to_vec(),
        // The shortest digits that read back as the same rate.
        format!("{}", p.error).into_bytes(),
        b"EXPANSION".to_vec(),
        p.expansion.to_string().into_bytes(),
    ];
    if p.nonscaling {
        parts.push(b"NONSCALING".to_vec());
    }
    parts.push(b"ITEMS".to_vec());
    parts.extend(items.iter().map(|i| i.to_vec()));
    parts
}

// ── Parsing ──────────────────────────────────────────────────────────────

/// An integer as Redis reads one (`string2ll`): an optional `-`, digits, no
/// leading zero, no `+`, nothing else.
fn parse_ll(raw: &[u8]) -> Option<i64> {
    let (negative, digits) = match raw {
        [b'-', rest @ ..] => (true, rest),
        _ => (false, raw),
    };
    if digits.is_empty()
        || !digits.iter().all(u8::is_ascii_digit)
        || (digits[0] == b'0' && (digits.len() > 1 || negative))
    {
        return None;
    }
    let text = std::str::from_utf8(raw).ok()?;
    text.parse::<i64>().ok()
}

/// A double as Redis reads one (`strtod`, all of it): decimal or
/// hexadecimal, `inf`, a sign; not NaN, not blanks around it.
fn parse_double(raw: &[u8]) -> Option<f64> {
    let text = std::str::from_utf8(raw).ok()?;
    if text.is_empty()
        || text.starts_with(char::is_whitespace)
        || text.ends_with(char::is_whitespace)
    {
        return None;
    }
    let value = match text.parse::<f64>() {
        Ok(v) => v,
        Err(_) => parse_hex_double(text)?,
    };
    (!value.is_nan()).then_some(value)
}

/// `0x1.8p3` and the like.
fn parse_hex_double(text: &str) -> Option<f64> {
    let (sign, rest) = match text.as_bytes().first()? {
        b'-' => (-1.0, &text[1..]),
        b'+' => (1.0, &text[1..]),
        _ => (1.0, text),
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
    for c in whole.chars() {
        value = value * 16.0 + f64::from(c.to_digit(16)?);
    }
    let mut scale = 1.0 / 16.0;
    for c in fraction.chars() {
        value += f64::from(c.to_digit(16)?) * scale;
        scale /= 16.0;
    }
    Some(sign * value * 2f64.powi(exponent))
}

fn wrong_args(name: &str) -> Vec<u8> {
    resp_error(&format!(
        "ERR wrong number of arguments for '{}' command",
        name.to_ascii_lowercase()
    ))
}

fn add_error(e: BloomError) -> Vec<u8> {
    resp_error(match e {
        BloomError::Full => FULL,
        BloomError::CannotGrow => CANNOT_GROW,
        BloomError::TooLarge => TOO_LARGE,
        BloomError::NoRoom => OOM,
        BloomError::NoMemory => NO_MEMORY,
        BloomError::OverBudget => OVER_BUDGET,
    })
}

fn shape_error(e: ShapeError) -> Vec<u8> {
    resp_error(match e {
        ShapeError::Bad => BAD_DATA,
        ShapeError::TooLarge => TOO_LARGE,
        ShapeError::NoRoom => OOM,
        ShapeError::NoMemory => NO_MEMORY,
    })
}

/// The limits of a write, and room for it under `maxmemory`: for a command
/// from a client, when there is a limit and what it would allocate
/// (`bytes`, asked only then) does not fit, keys are evicted first as the
/// policy allows, as Redis evicts before a command; `OOM` if that is not
/// enough. Before any lock is taken, as eviction takes them. A command
/// from a master or the AOF is held to neither `maxmemory` nor
/// `bloom-max-filter-bytes` ([`Limits::structural`]), nor is a replica, as
/// no write on one is.
fn limits(ctx: &mut Ctx, bytes: impl FnOnce(&Ctx) -> u64) -> Result<Limits, Vec<u8>> {
    let from_client = !ctx.apply_replication && !ctx.repl_state.is_slave();
    let limit = crate::RUNTIME_MAXMEMORY.load(Ordering::Relaxed) as u64;
    if from_client && limit != 0 {
        let bytes = bytes(ctx);
        if !crate::sketch::room_for(bytes) {
            let made = limit
                .checked_sub(bytes)
                .is_some_and(|room| crate::eviction::make_room(ctx, room as usize, true));
            if !made {
                return Err(resp_error(OOM));
            }
        }
    }
    Ok(if from_client {
        Limits::client()
    } else {
        Limits::structural()
    })
}

/// The per-command limit of a write of many items: none for one from a
/// master or the AOF, which must take what the master took.
fn probes(ctx: &Ctx) -> Probes {
    if ctx.apply_replication {
        Probes::unlimited()
    } else {
        Probes::budgeted()
    }
}

/// What adding `items` to the filter at `key` would allocate at most: its
/// growth, or, if there is none, the filter `make` would make.
fn bytes_to_add(ctx: &Ctx, key: &[u8], make: Option<&Params>, items: usize) -> u64 {
    let items = items as u64;
    match with_filter(ctx, key, |chain| chain.bytes_to_add(items)) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => make.map_or(0, |p| BloomChain::bytes_to_make(p, items)),
        Err(()) => 0,
    }
}

/// The reply protocol of this command (a script's `redis.setresp` too).
fn resp(ctx: &Ctx) -> u8 {
    crate::commands::handler::effective_resp_for_reply(ctx)
}

/// A count as a reply: one past `i64::MAX` (a loaded filter can say
/// anything) is `i64::MAX`.
fn count(n: u64) -> Vec<u8> {
    resp_integer(i64::try_from(n).unwrap_or(i64::MAX))
}

/// Whether `word` is `keyword` as `BF.INSERT` takes its words: compared,
/// regardless of case, for as long as the shorter of the two (`CAP`, `C`
/// and `CAPACITYX` are all `CAPACITY`), as Redis 8.10 compares them.
fn keyword(word: &[u8], wanted: &[u8]) -> bool {
    let n = word.len().min(wanted.len());
    word[..n].eq_ignore_ascii_case(&wanted[..n])
}

fn boolean(resp: u8, b: bool) -> Vec<u8> {
    if resp >= 3 {
        resp_bool_resp3(b)
    } else {
        resp_integer(i64::from(b))
    }
}

// ── Dispatch ─────────────────────────────────────────────────────────────

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    if !name.starts_with("BF.") {
        return None;
    }
    let reply = match name {
        "BF.RESERVE" => reserve(ctx),
        "BF.ADD" => add(ctx),
        "BF.MADD" => madd(ctx),
        "BF.INSERT" => insert(ctx),
        "BF.EXISTS" => exists(ctx),
        "BF.MEXISTS" => mexists(ctx),
        "BF.INFO" => info(ctx),
        "BF.CARD" => card(ctx),
        "BF.SCANDUMP" => scandump(ctx),
        "BF.LOADCHUNK" => loadchunk(ctx),
        "BF.DEBUG" => debug(ctx),
        _ => return None,
    };
    Some(reply)
}

/// The filter at `key` for reading: `Ok(None)` if there is no key,
/// `Err(())` if it holds another type.
fn with_filter<T>(
    ctx: &Ctx,
    key: &[u8],
    f: impl FnOnce(&BloomChain) -> T,
) -> Result<Option<T>, ()> {
    if crate::commands::helpers::lazy_expire_on_read(ctx.store, *ctx.selected_db, key) {
        return Ok(None);
    }
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    let map = read_lock!(shard);
    match map.get(key) {
        None => Ok(None),
        Some(ValueRef::Sketch(sketch)) => match &*sketch.read() {
            Sketch::Bloom(chain) => Ok(Some(f(chain))),
            #[allow(unreachable_patterns)]
            _ => Err(()),
        },
        Some(_) => Err(()),
    }
}

/// The filter at `key` for writing. Missing, `make` decides: `Some`
/// makes one with those parameters, `None` leaves the key missing and
/// gives `missing`. The flag is whether it was made.
fn with_filter_mut<T>(
    ctx: &Ctx,
    key: &[u8],
    make: Option<Params>,
    limits: Limits,
    missing: impl FnOnce() -> Vec<u8>,
    f: impl FnOnce(&mut BloomChain, bool) -> T,
) -> Result<T, Vec<u8>> {
    crate::commands::helpers::lazy_expire_key(ctx.store, *ctx.selected_db, key);
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    let mut map = write_lock!(shard);
    let (sketch, made) = match map.get(key) {
        Some(ValueRef::Sketch(sketch)) => (Arc::clone(sketch), false),
        Some(_) => {
            TOUCHED.set(true);
            return Err(resp_error(WRONGTYPE));
        }
        None => {
            let Some(params) = make else {
                return Err(missing());
            };
            let chain = BloomChain::new(params, limits).map_err(add_error)?;
            let sketch = Arc::new(parking_lot::RwLock::new(Sketch::Bloom(chain)));
            map.insert(Key::from(key), KiviDBValue::Sketch(Arc::clone(&sketch)));
            (sketch, true)
        }
    };
    TOUCHED.set(true);
    let mut guard = sketch.write();
    match &mut *guard {
        Sketch::Bloom(chain) => Ok(f(chain, made)),
        #[allow(unreachable_patterns)]
        _ => Err(resp_error(WRONGTYPE)),
    }
}

// ── Commands ─────────────────────────────────────────────────────────────

/// `BF.RESERVE key error_rate capacity [EXPANSION expansion] [NONSCALING]`
fn reserve(ctx: &mut Ctx) -> Vec<u8> {
    begin_write();
    let cmd = ctx.cmd.clone();
    if cmd.len() < 4 {
        return wrong_args("BF.RESERVE");
    }
    // At most `EXPANSION n NONSCALING` after the capacity, as Redis
    // counts: anything else there is ignored, but not more of it.
    if cmd.len() > 7 {
        return wrong_args("BF.RESERVE");
    }
    let opts = &cmd[4..];
    let expansion_at = opts
        .iter()
        .position(|a| a.eq_ignore_ascii_case(b"EXPANSION"));
    let key = cmd[1];
    let Some(error) = parse_double(cmd[2]) else {
        return resp_error("ERR bad error rate");
    };
    if !(error > 0.0 && error < 1.0) {
        return resp_error("ERR error rate must be in the range (0.000000, 1.000000)");
    }
    let Some(capacity) = parse_ll(cmd[3]) else {
        return resp_error("ERR bad capacity");
    };
    if !(1..=MAX_CAPACITY).contains(&capacity) {
        return resp_error("ERR capacity must be in the range [1, 1073741824]");
    }
    let nonscaling = opts.iter().any(|a| a.eq_ignore_ascii_case(b"NONSCALING"));
    let mut expansion = EXPANSION.load(Ordering::Relaxed);
    if let Some(at) = expansion_at {
        let Some(raw) = opts.get(at + 1) else {
            return resp_error("ERR no expansion");
        };
        let Some(value) = parse_ll(raw) else {
            return resp_error("ERR bad expansion");
        };
        // Before the range: `NONSCALING EXPANSION -1` is this error.
        if nonscaling && value != 0 {
            return resp_error("Nonscaling filters cannot expand");
        }
        if !(0..=MAX_EXPANSION).contains(&value) {
            return resp_error("ERR expansion must be in the range [0, 32768]");
        }
        expansion = value as u32;
    }
    let params = Params {
        capacity: capacity as u64,
        error,
        expansion,
        nonscaling,
    };
    let limits = match limits(ctx, |_| BloomChain::bytes_to_make(&params, 0)) {
        Ok(limits) => limits,
        Err(e) => return e,
    };
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    crate::commands::helpers::lazy_expire_key(ctx.store, *ctx.selected_db, key);
    let mut map = write_lock!(shard);
    TOUCHED.set(true);
    match map.get(key) {
        Some(ValueRef::Sketch(_)) => return resp_error("ERR item exists"),
        Some(_) => return resp_error(WRONGTYPE),
        None => {}
    }
    match BloomChain::new(params, limits) {
        Ok(chain) => {
            // The expansion came from `bf-expansion-factor`: sent with it, so
            // that a replica or a replay with another makes the same filter.
            if expansion_at.is_none() {
                let made = if nonscaling {
                    // `NONSCALING` may not name an expansion, and the filter
                    // keeps one all the same: its header says it.
                    vec![
                        b"BF.LOADCHUNK".to_vec(),
                        key.to_vec(),
                        b"1".to_vec(),
                        chain.header(),
                    ]
                } else {
                    vec![
                        b"BF.RESERVE".to_vec(),
                        key.to_vec(),
                        cmd[2].to_vec(),
                        cmd[3].to_vec(),
                        b"EXPANSION".to_vec(),
                        expansion.to_string().into_bytes(),
                    ]
                };
                send_as(vec![made]);
            }
            map.insert(
                Key::from(key),
                KiviDBValue::Sketch(Arc::new(parking_lot::RwLock::new(Sketch::Bloom(chain)))),
            );
            resp_simple("OK")
        }
        Err(e) => add_error(e),
    }
}

/// `BF.ADD key item`
fn add(ctx: &mut Ctx) -> Vec<u8> {
    begin_write();
    if ctx.cmd.len() != 3 {
        return wrong_args("BF.ADD");
    }
    let key = ctx.cmd[1];
    let item = ctx.cmd[2];
    let params = default_params();
    let resp = resp(ctx);
    let limits = match limits(ctx, |ctx| bytes_to_add(ctx, key, Some(&params), 1)) {
        Ok(limits) => limits,
        Err(e) => return e,
    };
    let result = with_filter_mut(ctx, key, Some(params), limits, Vec::new, |chain, made| {
        (chain.add(item, limits), made)
    });
    match result {
        Err(e) => e,
        Ok((Err(e), _)) => add_error(e),
        Ok((Ok(added), made)) => {
            if made {
                send_as(vec![insert_form(key, &params, &[item])]);
            }
            boolean(resp, added)
        }
    }
}

/// What [`add_each`] did: the replies, how many items it took, and the
/// filter's header if it took none.
struct Added {
    replies: Vec<Vec<u8>>,
    done: usize,
    header: Option<Vec<u8>>,
}

/// Add each of `items`, stopping at one the filter cannot take, or at the
/// per-command limit (`probes`): the replies, the last of them that one's
/// error. Nothing of the item it stopped at is written.
fn add_each(
    chain: &mut BloomChain,
    items: &[&[u8]],
    resp: u8,
    limits: Limits,
    mut probes: Probes,
) -> Added {
    let mut replies = Vec::with_capacity(items.len());
    let mut done = 0;
    for item in items {
        match chain.add_within(item, limits, &mut probes) {
            Ok(added) => {
                replies.push(boolean(resp, added));
                done += 1;
            }
            Err(e) => {
                replies.push(add_error(e));
                break;
            }
        }
    }
    let header = (done == 0).then(|| chain.header());
    Added {
        replies,
        done,
        header,
    }
}

/// `BF.MADD key item [item ...]`
fn madd(ctx: &mut Ctx) -> Vec<u8> {
    begin_write();
    if ctx.cmd.len() < 3 {
        return wrong_args("BF.MADD");
    }
    let key = ctx.cmd[1];
    let count = ctx.cmd.len() - 2;
    let params = default_params();
    let resp = resp(ctx);
    let limits = match limits(ctx, |ctx| bytes_to_add(ctx, key, Some(&params), count)) {
        Ok(limits) => limits,
        Err(e) => return e,
    };
    let probes = probes(ctx);
    let cmd = &ctx.cmd;
    let items = &cmd[2..];
    match with_filter_mut(ctx, key, Some(params), limits, Vec::new, |chain, made| {
        (add_each(chain, items, resp, limits, probes), made)
    }) {
        Err(e) => e,
        Ok((added, made)) => {
            let made = made.then_some(&params);
            if let Some(cmds) = sent_items(cmd, key, items, added.done, made, added.header) {
                send_as(cmds);
            }
            resp_array(added.replies)
        }
    }
}

/// `BF.INSERT key [CAPACITY capacity] [ERROR error] [EXPANSION expansion]
/// [NOCREATE] [NONSCALING] ITEMS item [item ...]`
fn insert(ctx: &mut Ctx) -> Vec<u8> {
    begin_write();
    let cmd = ctx.cmd.clone();
    if cmd.len() < 4 {
        return wrong_args("BF.INSERT");
    }
    let mut params = default_params();
    let mut nocreate = false;
    let mut items: Option<&[&[u8]]> = None;
    let mut at = 2;
    while at < cmd.len() {
        let word = cmd[at];
        let value = cmd.get(at + 1);
        if keyword(word, b"ITEMS") {
            items = Some(&cmd[at + 1..]);
            break;
        } else if keyword(word, b"NONSCALING") {
            // Before NOCREATE: `N` and `NO` are NONSCALING.
            params.nonscaling = true;
            at += 1;
        } else if keyword(word, b"NOCREATE") {
            nocreate = true;
            at += 1;
        } else if keyword(word, b"CAPACITY") {
            let Some(value) = value else {
                return wrong_args("BF.INSERT");
            };
            match parse_ll(value) {
                Some(c) if (1..=MAX_CAPACITY).contains(&c) => params.capacity = c as u64,
                _ => return resp_error("Bad capacity"),
            }
            at += 2;
        } else if keyword(word, b"EXPANSION") {
            // Before ERROR: `E` is EXPANSION.
            let Some(value) = value else {
                return wrong_args("BF.INSERT");
            };
            match parse_ll(value) {
                Some(x) if (0..=MAX_EXPANSION).contains(&x) => params.expansion = x as u32,
                _ => return resp_error("Bad expansion"),
            }
            at += 2;
        } else if keyword(word, b"ERROR") {
            let Some(value) = value else {
                return wrong_args("BF.INSERT");
            };
            match parse_double(value) {
                Some(e) if e > 0.0 && e < 1.0 => params.error = e,
                _ => return resp_error("Bad error rate"),
            }
            at += 2;
        } else {
            return resp_error("Unknown argument received");
        }
    }
    let items = match items {
        Some(items) if !items.is_empty() => items,
        _ => return wrong_args("BF.INSERT"),
    };
    let key = cmd[1];
    let make = (!nocreate).then_some(params);
    let resp = resp(ctx);
    let limits = match limits(ctx, |ctx| {
        bytes_to_add(ctx, key, make.as_ref(), items.len())
    }) {
        Ok(limits) => limits,
        Err(e) => return e,
    };
    let probes = probes(ctx);
    match with_filter_mut(
        ctx,
        key,
        make,
        limits,
        || resp_error(NOT_FOUND),
        |chain, made| (add_each(chain, items, resp, limits, probes), made),
    ) {
        Err(e) => e,
        Ok((added, made)) => {
            let made = made.then_some(&params);
            if let Some(cmds) = sent_items(&cmd, key, items, added.done, made, added.header) {
                send_as(cmds);
            }
            resp_array(added.replies)
        }
    }
}

/// `BF.EXISTS key item`: 0 for a missing key, and for a key of another
/// type, as in Redis.
fn exists(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 3 {
        return wrong_args("BF.EXISTS");
    }
    let key = ctx.cmd[1];
    let item = ctx.cmd[2];
    let found = matches!(
        with_filter(ctx, key, |chain| chain.contains(item)),
        Ok(Some(true))
    );
    boolean(resp(ctx), found)
}

/// `BF.MEXISTS key item [item ...]`
fn mexists(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() < 3 {
        return wrong_args("BF.MEXISTS");
    }
    let key = ctx.cmd[1];
    let items = &ctx.cmd[2..];
    let resp = resp(ctx);
    let replies = match with_filter(ctx, key, |chain| {
        // A read: held to the per-command limit wherever it comes from.
        let mut probes = Probes::budgeted();
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            match chain.contains_within(item, &mut probes) {
                Ok(found) => out.push(boolean(resp, found)),
                Err(e) => {
                    out.push(add_error(e));
                    break;
                }
            }
        }
        out
    }) {
        Ok(Some(replies)) => replies,
        _ => vec![boolean(resp, false); items.len()],
    };
    resp_array(replies)
}

/// `BF.INFO key [CAPACITY | SIZE | FILTERS | ITEMS | EXPANSION]`
fn info(ctx: &mut Ctx) -> Vec<u8> {
    if !(2..=3).contains(&ctx.cmd.len()) {
        return wrong_args("BF.INFO");
    }
    let key = ctx.cmd[1];
    let resp = resp(ctx);
    let fields = match with_filter(ctx, key, |chain| {
        let expansion = if chain.nonscaling() {
            resp_null_for(resp)
        } else {
            resp_integer(i64::from(chain.expansion))
        };
        [
            ("Capacity", "CAPACITY", count(chain.capacity())),
            ("Size", "SIZE", count(chain.info_size())),
            (
                "Number of filters",
                "FILTERS",
                count(chain.layers.len() as u64),
            ),
            ("Number of items inserted", "ITEMS", count(chain.items)),
            ("Expansion rate", "EXPANSION", expansion),
        ]
    }) {
        Ok(Some(fields)) => fields,
        Ok(None) => return resp_error(NOT_FOUND),
        Err(()) => return resp_error(WRONGTYPE),
    };
    match ctx.cmd.get(2) {
        None => resp_map_for(
            resp,
            fields
                .into_iter()
                .flat_map(|(name, _, value)| [resp_simple(name), value])
                .collect(),
        ),
        Some(wanted) => {
            let Some((name, _, value)) = fields
                .into_iter()
                .find(|(_, word, _)| wanted.eq_ignore_ascii_case(word.as_bytes()))
            else {
                return resp_error("Invalid information value");
            };
            if resp >= 3 {
                resp_map_for(resp, vec![resp_simple(name), value])
            } else {
                resp_array(vec![value])
            }
        }
    }
}

/// `BF.CARD key`: 0 for a missing key.
fn card(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 2 {
        return wrong_args("BF.CARD");
    }
    let key = ctx.cmd[1];
    match with_filter(ctx, key, |chain| chain.items) {
        Ok(Some(n)) => count(n),
        Ok(None) => resp_integer(0),
        Err(()) => resp_error(WRONGTYPE),
    }
}

/// `BF.SCANDUMP key iterator`
fn scandump(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 3 {
        return wrong_args("BF.SCANDUMP");
    }
    let Some(iter) = parse_ll(ctx.cmd[2]) else {
        return resp_error("Second argument must be numeric");
    };
    let key = ctx.cmd[1];
    match with_filter(ctx, key, |chain| chain.scandump(iter)) {
        Ok(Some((next, bytes))) => resp_array(vec![resp_integer(next), resp_bulk(&bytes)]),
        Ok(None) => resp_error(NOT_FOUND),
        Err(()) => resp_error(WRONGTYPE),
    }
}

/// `BF.LOADCHUNK key iterator data`: iterator 1 is the header, which makes
/// the filter; the rest fill it.
fn loadchunk(ctx: &mut Ctx) -> Vec<u8> {
    begin_write();
    if ctx.cmd.len() != 4 {
        return wrong_args("BF.LOADCHUNK");
    }
    let Some(iter) = parse_ll(ctx.cmd[2]) else {
        return resp_error("ERR Second argument must be numeric");
    };
    let key = ctx.cmd[1];
    let data = ctx.cmd[3];
    // A header makes a filter: what its layers take, under `maxmemory`; a
    // data chunk, the copy of the layer it lands in if another copy of the
    // filter shares it.
    let limits = match limits(ctx, |ctx| {
        if iter == 1 {
            BloomChain::header_bytes(data)
        } else {
            with_filter(ctx, key, |chain| chain.chunk_copy_bytes(iter, data.len()))
                .ok()
                .flatten()
                .unwrap_or(0)
        }
    }) {
        Ok(limits) => limits,
        Err(e) => return e,
    };
    crate::commands::helpers::lazy_expire_key(ctx.store, *ctx.selected_db, key);
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    let mut map = write_lock!(shard);
    TOUCHED.set(true);
    let existing = match map.get(key) {
        None => None,
        Some(ValueRef::Sketch(sketch)) => Some(Arc::clone(sketch)),
        Some(_) => return resp_error(WRONGTYPE),
    };
    if iter == 1 {
        if existing.is_some() {
            return resp_error(BAD_DATA);
        }
        return match BloomChain::from_header(data, limits) {
            Err(e) => shape_error(e),
            Ok(chain) => {
                map.insert(
                    Key::from(key),
                    KiviDBValue::Sketch(Arc::new(parking_lot::RwLock::new(Sketch::Bloom(chain)))),
                );
                resp_simple("OK")
            }
        };
    }
    let Some(sketch) = existing else {
        return resp_error(NOT_FOUND);
    };
    drop(map);
    let mut guard = sketch.write();
    #[allow(irrefutable_let_patterns)]
    let Sketch::Bloom(chain) = &mut *guard else {
        return resp_error(WRONGTYPE);
    };
    match chain.load_chunk(iter, data) {
        Ok(()) => resp_simple("OK"),
        Err(e) => resp_error(e),
    }
}

/// `BF.DEBUG key`
fn debug(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 2 {
        return wrong_args("BF.DEBUG");
    }
    let key = ctx.cmd[1];
    match with_filter(ctx, key, |chain| chain.debug_lines()) {
        Ok(Some(lines)) => resp_array(lines.iter().map(|l| resp_bulk_str(l)).collect()),
        Ok(None) => resp_error(NOT_FOUND),
        Err(()) => resp_error(WRONGTYPE),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_as_redis_reads_them() {
        assert_eq!(parse_ll(b"100"), Some(100));
        assert_eq!(parse_ll(b"-1"), Some(-1));
        assert_eq!(parse_ll(b"0"), Some(0));
        for bad in [
            &b"+100"[..],
            b"1e2",
            b"0x10",
            b"100 ",
            b" 100",
            b"01",
            b"-0",
            b"",
            b"-",
            b"1.5",
        ] {
            assert_eq!(parse_ll(bad), None, "{bad:?}");
        }
        assert_eq!(parse_ll(b"9223372036854775808"), None);
        assert_eq!(parse_ll(b"-9223372036854775808"), Some(i64::MIN));
    }

    #[test]
    fn doubles_as_redis_reads_them() {
        assert_eq!(parse_double(b"0.01"), Some(0.01));
        assert_eq!(parse_double(b"+0.01"), Some(0.01));
        assert_eq!(parse_double(b".5"), Some(0.5));
        assert_eq!(parse_double(b"1e-2"), Some(0.01));
        assert_eq!(parse_double(b"0x0.1p0"), Some(0.0625));
        assert_eq!(parse_double(b"inf"), Some(f64::INFINITY));
        for bad in [&b"nan"[..], b" 0.01", b"0.01 ", b"abc", b""] {
            assert_eq!(parse_double(bad), None, "{bad:?}");
        }
    }

    /// A write that stopped before its last item is sent with the items it
    /// took and no others; one that took them all, as itself.
    #[test]
    fn writes_that_stop_early_are_sent_with_what_they_took() {
        let p = Params {
            capacity: 100,
            error: 0.01,
            expansion: 2,
            nonscaling: false,
        };
        let cmd: Vec<&[u8]> = vec![b"BF.MADD", b"k", b"a", b"b", b"c"];
        let items = &cmd[2..];
        let words = |cmds: Vec<Vec<Vec<u8>>>| -> Vec<Vec<String>> {
            cmds.into_iter()
                .map(|c| {
                    c.into_iter()
                        .map(|w| String::from_utf8_lossy(&w).into_owned())
                        .collect()
                })
                .collect()
        };
        assert_eq!(sent_items(&cmd, b"k", items, 3, None, None), None);
        assert_eq!(
            words(sent_items(&cmd, b"k", items, 2, None, None).unwrap()),
            [["BF.MADD", "k", "a", "b"]]
        );
        assert_eq!(
            sent_items(&cmd, b"k", items, 0, None, Some(vec![])),
            Some(vec![])
        );
        let insert: Vec<&[u8]> = vec![b"BF.INSERT", b"k", b"NOCREATE", b"ITEMS", b"a", b"b"];
        assert_eq!(
            words(sent_items(&insert, b"k", &insert[4..], 1, None, None).unwrap()),
            [["BF.INSERT", "k", "NOCREATE", "ITEMS", "a"]]
        );
        // Made: the BF.INSERT that names how, with the items taken.
        let made = words(sent_items(&cmd, b"k", items, 1, Some(&p), None).unwrap());
        assert_eq!(made[0][0], "BF.INSERT");
        assert_eq!(made[0].last().map(String::as_str), Some("a"));
        let made = words(sent_items(&cmd, b"k", items, 3, Some(&p), None).unwrap());
        assert_eq!(made[0].len(), 12);
        // Made, and nothing taken: the empty filter, as its header.
        let sent = sent_items(&cmd, b"k", items, 0, Some(&p), Some(b"hdr".to_vec())).unwrap();
        assert_eq!(
            sent,
            vec![vec![
                b"BF.LOADCHUNK".to_vec(),
                b"k".to_vec(),
                b"1".to_vec(),
                b"hdr".to_vec()
            ]]
        );
    }

    #[test]
    fn made_filters_propagate_as_inserts() {
        let p = Params {
            capacity: 100,
            error: 0.01,
            expansion: 2,
            nonscaling: false,
        };
        let form = insert_form(b"k", &p, &[b"a", b"b"]);
        let words: Vec<&[u8]> = form.iter().map(Vec::as_slice).collect();
        assert_eq!(
            words,
            [
                &b"BF.INSERT"[..],
                b"k",
                b"CAPACITY",
                b"100",
                b"ERROR",
                b"0.01",
                b"EXPANSION",
                b"2",
                b"ITEMS",
                b"a",
                b"b"
            ]
        );
    }
}
