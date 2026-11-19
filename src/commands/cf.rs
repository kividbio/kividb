//! `CF.*`: Cuckoo filters, as Redis 8 has them built in (formerly
//! RedisBloom). The filter itself is `sketch::cuckoo`.
//!
//! The replies and errors are Redis 8.10's, in RESP2 and RESP3, with the
//! order in which Redis checks a command's arguments and its quirks:
//! `CF.EXISTS`, `CF.MEXISTS` and `CF.COUNT` answer 0 for a key of another
//! type; `CF.DEL` answers `Not found` for one; `CF.RESERVE` ignores words it
//! does not know; `CF.INSERT` takes its keywords by their first letters.
//! Filters are Redis's byte for byte, so `CF.SCANDUMP` gives Redis's chunks
//! (for sub-filters up to 16 MiB, which Redis gives whole) and `CF.LOADCHUNK`
//! takes them.
//!
//! The `cf-*` settings (`cf-bucket-size`, `cf-initial-size`,
//! `cf-expansion-factor`, `cf-max-iterations`, `cf-max-expansions`) work
//! through `CONFIG SET/GET`, with Redis's checks. A filter a command makes
//! for want of one (`CF.ADD`, `CF.ADDNX`, `CF.INSERT`, `CF.INSERTNX` on a
//! missing key) is sent to replicas and the AOF as a `CF.RESERVE` naming how
//! it was made, then the command: a replica, or a replay, with other
//! settings makes the same filter. `cf-max-expansions` holds a client's
//! commands only; a command from a master or the AOF grows a filter as the
//! master grew it, up to the fixed limits.
//!
//! A write invalidates a `WATCH` on its key once it has read its arguments,
//! even when it fails, as Redis's do. No keyspace events, as in Redis.
//!
//! Beyond Redis: a filter is held to `sketch::cuckoo`'s size limits, and a
//! command of many items to a per-command amount of work ([`BUDGET`]),
//! reckoned before it starts, and a compaction to `sketch::cuckoo`'s
//! `COMPACT_BUDGET`. Under `maxmemory`, a client's command that would
//! allocate more than is left evicts first, under the policy, before it
//! takes its key (`sketch::room`), and is refused with `OOM` only if that
//! is not enough. What it is given room for: making the filter; an add's
//! new sub-filter only when no bucket of the item has a free slot; for a
//! command of many items, the growth that holds them at the load its bucket
//! size reaches (`Cuckoo::bytes_to_add_items`), an item that would grow it
//! further when there is no room left being answered with `OOM` on its own;
//! and a copy of what a snapshot or a copy (`COPY`) shares and the command
//! writes to, `CF.DEL` and `CF.COMPACT` included (which otherwise free
//! memory, and are let run over the limit).
//!
//! A command either changes nothing or succeeds: every check is made before
//! a filter is made, and an item of a command of many that cannot be added
//! is answered on its own. When one is refused for memory (which a replica
//! or a replay need not be), the command is sent to replicas and the AOF as
//! the items it added.

use std::cell::{Cell, RefCell};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::commands::bf::parse_ll;
use crate::commands::ctx::Ctx;
use crate::resp::write::{
    resp_array, resp_bool_resp3, resp_bulk, resp_bulk_str, resp_error, resp_integer, resp_map_for,
    resp_null_for, resp_simple,
};
use crate::sketch::Sketch;
use crate::sketch::cuckoo::{
    Cuckoo, CuckooError, HeaderError, MAX_BYTES, MAX_CAPACITY, MAX_FILTERS, Params, lookup,
};
use crate::store::{self, Key, ValueRef};
use crate::types::KiviDBValue;
use crate::{read_lock, write_lock};

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const NOT_FOUND: &str = "ERR not found";
const EXISTS: &str = "ERR item exists";
const FULL: &str = "Filter is full";
const MAX_EXPANSIONS: &str = "Maximum expansions reached";
const TOO_LARGE: &str = "ERR filter is too large";
const NO_MEMORY: &str = "ERR could not allocate memory for the filter";
const OOM: &str = "OOM command not allowed when used memory > 'maxmemory'.";
const OVER_BUDGET: &str =
    "ERR command reached the per-command limit for Cuckoo filters; send fewer items per command";

/// What one command of many items may cost at most, reckoned before it
/// starts: for each item, every sub-filter's two buckets, and for an add the
/// kicks it may take.
pub const BUDGET: u64 = 8_000_000;

// ── Settings ─────────────────────────────────────────────────────────────

static BUCKET_SIZE: AtomicU64 = AtomicU64::new(2);
static INITIAL_SIZE: AtomicU64 = AtomicU64::new(1024);
static EXPANSION: AtomicU64 = AtomicU64::new(1);
static MAX_ITERATIONS: AtomicU64 = AtomicU64::new(20);
static MAX_EXPANSIONS_SETTING: AtomicU64 = AtomicU64::new(32);

/// Each setting: its name, where it is kept, and its range.
static SETTINGS: [(&str, &AtomicU64, i64, i64); 5] = [
    ("cf-bucket-size", &BUCKET_SIZE, 1, 255),
    ("cf-initial-size", &INITIAL_SIZE, 4, 1_073_741_824),
    ("cf-expansion-factor", &EXPANSION, 0, 32768),
    ("cf-max-iterations", &MAX_ITERATIONS, 1, 65535),
    ("cf-max-expansions", &MAX_EXPANSIONS_SETTING, 1, 65536),
];

/// `CONFIG SET` of a `cf-*` setting, with Redis's checks and messages.
pub(crate) fn config_set(param: &str, value: &str) -> Vec<u8> {
    let Some(&(_, cell, min, max)) = SETTINGS.iter().find(|(name, ..)| *name == param) else {
        return resp_error("ERR Unknown option");
    };
    // The bucket size is bounded by half of `cf-initial-size` too, as Redis
    // bounds it.
    // And the initial size by twice the bucket size.
    let max = if param == "cf-bucket-size" {
        max.min((setting(&INITIAL_SIZE) / 2) as i64)
    } else {
        max
    };
    let min = if param == "cf-initial-size" {
        2 * setting(&BUCKET_SIZE) as i64
    } else {
        min
    };
    let (min, max) = (&min, &max);
    let failed = |why: String| {
        resp_error(&format!(
            "ERR CONFIG SET failed (possibly related to argument '{param}') - {why}"
        ))
    };
    match parse_ll(value.as_bytes()) {
        None => failed(format!("Invalid value for `{param}`")),
        Some(v) if v < *min || v > *max => failed(format!(
            "Value for `{param}` must be in the range [{min}, {max}]"
        )),
        Some(v) => {
            cell.store(v as u64, Ordering::Relaxed);
            resp_simple("OK")
        }
    }
}

/// The `cf-*` settings as `CONFIG GET` lists them.
pub(crate) fn config_rows() -> Vec<(&'static str, String)> {
    SETTINGS
        .iter()
        .map(|(name, cell, ..)| (*name, cell.load(Ordering::Relaxed).to_string()))
        .collect()
}

fn setting(cell: &AtomicU64) -> u64 {
    cell.load(Ordering::Relaxed)
}

/// What a filter made for want of one is made with.
fn default_params(capacity: Option<u64>) -> Params {
    Params {
        capacity: capacity.unwrap_or_else(|| setting(&INITIAL_SIZE)),
        bucket_size: setting(&BUCKET_SIZE) as u16,
        max_iterations: setting(&MAX_ITERATIONS) as u16,
        expansion: setting(&EXPANSION) as u16,
    }
}

/// Each command's arity, as Redis's `COMMAND` gives it, for `COMMAND INFO`
/// alone.
pub(crate) fn arity(name: &str) -> Option<i16> {
    Some(match name {
        "CF.ADD" | "CF.ADDNX" | "CF.EXISTS" | "CF.COUNT" | "CF.DEL" | "CF.SCANDUMP" => 3,
        "CF.MEXISTS" | "CF.RESERVE" => -3,
        "CF.INSERT" | "CF.INSERTNX" => -4,
        "CF.LOADCHUNK" => 4,
        "CF.INFO" | "CF.DEBUG" => 2,
        "CF.COMPACT" => -1,
        _ => return None,
    })
}

// ── Propagation and WATCH ────────────────────────────────────────────────

thread_local! {
    /// The `CF.RESERVE` naming how the running command made its filter, if
    /// it did, to be sent before it.
    static MADE: RefCell<Option<Vec<Vec<u8>>>> = const { RefCell::new(None) };
    /// What the running command is sent as in its own place, if not as
    /// itself (`None` in it: nothing): a `CF.INSERT` of the items it
    /// added, when it was refused others for memory.
    static SENT: RefCell<Option<Option<Vec<Vec<u8>>>>> = const { RefCell::new(None) };
    /// Whether the running write got as far as its key.
    static TOUCHED: Cell<bool> = const { Cell::new(false) };
}

fn begin_write() {
    MADE.with_borrow_mut(|m| *m = None);
    SENT.with_borrow_mut(|s| *s = None);
    TOUCHED.set(false);
}

/// What `CF.RESERVE`, `CF.ADD`, `CF.ADDNX`, `CF.INSERT` or `CF.INSERTNX`
/// (`cmd`) is sent to replicas and the AOF as: a `CF.RESERVE` naming every
/// parameter of the filter it made, then (but for `CF.RESERVE`) itself.
pub(crate) fn propagation(cmd: &[&[u8]]) -> Vec<Vec<Vec<u8>>> {
    let this: Option<Vec<Vec<u8>>> = SENT
        .with_borrow_mut(Option::take)
        .unwrap_or_else(|| Some(cmd.iter().map(|b| b.to_vec()).collect()));
    match MADE.with_borrow_mut(Option::take) {
        // `CF.RESERVE` itself: as the one that names everything.
        Some(reserve)
            if cmd
                .first()
                .is_some_and(|n| n.eq_ignore_ascii_case(b"CF.RESERVE")) =>
        {
            vec![reserve]
        }
        Some(reserve) => std::iter::once(reserve).chain(this).collect(),
        None => this.into_iter().collect(),
    }
}

/// A `CF.INSERT` of `items` to the filter at `key`, which is there.
fn insert_form(key: &[u8], items: &[&[u8]]) -> Vec<Vec<u8>> {
    let mut form = Vec::with_capacity(items.len() + 4);
    form.push(b"CF.INSERT".to_vec());
    form.push(key.to_vec());
    form.push(b"NOCREATE".to_vec());
    form.push(b"ITEMS".to_vec());
    form.extend(items.iter().map(|i| i.to_vec()));
    form
}

/// Whether the `CF.*` write that just failed had got as far as its key.
pub(crate) fn take_touched() -> bool {
    TOUCHED.replace(false)
}

fn reserve_form(key: &[u8], p: &Params) -> Vec<Vec<u8>> {
    vec![
        b"CF.RESERVE".to_vec(),
        key.to_vec(),
        p.capacity.to_string().into_bytes(),
        b"BUCKETSIZE".to_vec(),
        p.bucket_size.to_string().into_bytes(),
        b"MAXITERATIONS".to_vec(),
        p.max_iterations.to_string().into_bytes(),
        b"EXPANSION".to_vec(),
        p.expansion.to_string().into_bytes(),
    ]
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn wrong_args(name: &str) -> Vec<u8> {
    resp_error(&format!(
        "ERR wrong number of arguments for '{}' command",
        name.to_ascii_lowercase()
    ))
}

fn resp(ctx: &Ctx) -> u8 {
    crate::commands::handler::effective_resp_for_reply(ctx)
}

fn boolean(resp: u8, b: bool) -> Vec<u8> {
    if resp >= 3 {
        resp_bool_resp3(b)
    } else {
        resp_integer(i64::from(b))
    }
}

/// The largest bucket size a command may make a filter with: what Redis
/// takes from a client (255, or half of `cf-initial-size` if less); the
/// fixed 255 for a command from a master or the AOF, which must make what
/// the master made whatever this node's settings.
fn max_bucket_size(ctx: &Ctx) -> u16 {
    if ctx.apply_replication {
        255
    } else {
        (setting(&INITIAL_SIZE) / 2).min(255) as u16
    }
}

/// Room under `maxmemory` for `bytes` more, for a command from a client:
/// `OOM` if they do not fit. The room was made before the command took its
/// key (`sketch::room`, which also stops a command here that is only asked
/// what it wants): nothing is evicted here.
fn room(ctx: &Ctx, bytes: u64) -> Result<(), Vec<u8>> {
    crate::sketch::room::check(ctx, |_| bytes).map_err(|_| resp_error(OOM))
}

/// How many sub-filters a write may grow a filter to as it goes: the fixed
/// limit. (`cf-max-expansions` is checked before a client's write starts,
/// as Redis checks it: a write may grow a filter past it.)
fn max_filters(_ctx: &Ctx) -> u64 {
    MAX_FILTERS
}

fn add_error(e: CuckooError) -> Vec<u8> {
    resp_error(match e {
        CuckooError::Full => FULL,
        CuckooError::MaxExpansions => MAX_EXPANSIONS,
        CuckooError::TooLarge => TOO_LARGE,
        CuckooError::NoMemory => NO_MEMORY,
        CuckooError::NoRoom => OOM,
    })
}

/// What adding one item may cost a filter at most.
fn add_cost(cf: &Cuckoo) -> u64 {
    add_cost_of(cf.filters.len() as u64, cf.bucket_size, cf.max_iterations)
}

/// What adding one item may cost a filter of `filters` sub-filters with
/// buckets of `bucket_size` and `max_iterations` kicks at most.
fn add_cost_of(filters: u64, bucket_size: u16, max_iterations: u16) -> u64 {
    let bs = u64::from(bucket_size);
    (filters + 1) * 2 * bs + 2 * u64::from(max_iterations) * (bs + 1)
}

/// What looking one item up costs.
fn look_cost(cf: &Cuckoo) -> u64 {
    cf.filters.len() as u64 * 2 * u64::from(cf.bucket_size)
}

/// Run `f` on the filter at `key` for reading: `Ok(None)` if there is no
/// key, `Err(())` if it holds another type.
fn with_filter<T>(ctx: &Ctx, key: &[u8], f: impl FnOnce(&Cuckoo) -> T) -> Result<Option<T>, ()> {
    if crate::commands::helpers::lazy_expire_on_read(ctx.store, *ctx.selected_db, key) {
        return Ok(None);
    }
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    let map = read_lock!(shard);
    match map.get(key) {
        None => Ok(None),
        Some(ValueRef::Sketch(sketch)) => match &*sketch.read() {
            Sketch::Cuckoo(cf) => Ok(Some(f(cf))),
            #[allow(unreachable_patterns)]
            _ => Err(()),
        },
        Some(_) => Err(()),
    }
}

/// The filter at `key` for writing: `Ok(None)` if missing. A sketch of
/// another type is told by the caller, under its own lock of it.
fn filter_for_write(
    ctx: &Ctx,
    key: &[u8],
) -> Result<Option<Arc<parking_lot::RwLock<Sketch>>>, Vec<u8>> {
    crate::commands::helpers::lazy_expire_key(ctx.store, *ctx.selected_db, key);
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    let map = read_lock!(shard);
    match map.get(key) {
        None => Ok(None),
        Some(ValueRef::Sketch(sketch)) => Ok(Some(Arc::clone(sketch))),
        Some(_) => Err(resp_error(WRONGTYPE)),
    }
}

/// Whether `sketch` is a Cuckoo filter.
fn is_cuckoo(sketch: &parking_lot::RwLock<Sketch>) -> bool {
    matches!(&*sketch.read(), Sketch::Cuckoo(_))
}

/// Put `cf`, made by the running command, at `key`, which was missing when
/// it looked: `Err` if something came there meanwhile (it is then
/// dropped, and the command looks again).
fn publish(ctx: &Ctx, key: &[u8], cf: Cuckoo) -> Result<(), ()> {
    debug_assert!(!crate::sketch::room::asking());
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    let mut map = write_lock!(shard);
    if map.get(key).is_some() {
        return Err(());
    }
    map.insert(
        Key::from(key),
        KiviDBValue::Sketch(Arc::new(parking_lot::RwLock::new(Sketch::Cuckoo(cf)))),
    );
    Ok(())
}

/// What a filter made for want of one (`capacity`, or the setting) is made
/// with, or why a command may not make it: checked before anything is
/// made, so that a command that makes a filter never fails after it.
fn params_to_make(ctx: &Ctx, capacity: Option<u64>) -> Result<Params, Vec<u8>> {
    // Redis refuses a client's add to a filter of `cf-max-expansions`
    // sub-filters or more before it tries, however much room there is. (It
    // makes a missing filter first, and keeps it; here it is not made, so
    // that nothing a command did goes unsent to replicas.)
    if !ctx.apply_replication && setting(&MAX_EXPANSIONS_SETTING) <= 1 {
        return Err(resp_error(MAX_EXPANSIONS));
    }
    let p = default_params(capacity);
    if Cuckoo::bytes_for(&p) > MAX_BYTES {
        return Err(resp_error(TOO_LARGE));
    }
    Ok(p)
}

// ── Dispatch ─────────────────────────────────────────────────────────────

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    if !name.starts_with("CF.") {
        return None;
    }
    Some(match name {
        "CF.RESERVE" => reserve(ctx),
        "CF.ADD" => add(ctx, false),
        "CF.ADDNX" => add(ctx, true),
        "CF.INSERT" => insert(ctx, false),
        "CF.INSERTNX" => insert(ctx, true),
        "CF.EXISTS" => exists(ctx),
        "CF.MEXISTS" => mexists(ctx),
        "CF.COUNT" => count(ctx),
        "CF.DEL" => del(ctx),
        "CF.INFO" => info(ctx),
        "CF.SCANDUMP" => scandump(ctx),
        "CF.LOADCHUNK" => loadchunk(ctx),
        "CF.COMPACT" => compact(ctx),
        "CF.DEBUG" => debug(ctx),
        _ => return None,
    })
}

// ── Commands ─────────────────────────────────────────────────────────────

/// The value after the first `word` among the options, compared regardless
/// of case.
fn option<'a>(opts: &[&'a [u8]], word: &[u8]) -> Option<Option<&'a [u8]>> {
    let at = opts.iter().position(|a| a.eq_ignore_ascii_case(word))?;
    Some(opts.get(at + 1).copied())
}

/// `CF.RESERVE key capacity [BUCKETSIZE bucketsize] [MAXITERATIONS
/// maxiterations] [EXPANSION expansion]`
fn reserve(ctx: &mut Ctx) -> Vec<u8> {
    begin_write();
    let argc = ctx.cmd.len();
    if argc < 3 || !(argc - 3).is_multiple_of(2) {
        return wrong_args("CF.RESERVE");
    }
    let key = ctx.cmd[1];
    let Some(capacity) = parse_ll(ctx.cmd[2]) else {
        return resp_error("Bad capacity");
    };
    let opts = ctx.cmd[3..].to_vec();
    // Unless given, as the settings have them (on a replica too: what the
    // master took from its own is sent with the command).
    let mut p = Params {
        capacity: 0,
        ..default_params(None)
    };
    for (word, slot, min, max) in [
        ("MAXITERATIONS", 0, 1, 65535),
        ("BUCKETSIZE", 1, 1, i64::from(max_bucket_size(ctx))),
        ("EXPANSION", 2, 0, 32768),
    ] {
        match option(&opts, word.as_bytes()) {
            None => {}
            Some(None) => return wrong_args("CF.RESERVE"),
            Some(Some(raw)) => match parse_ll(raw) {
                None => return resp_error(&format!("Couldn't parse {word}")),
                Some(v) if v < min || v > max => {
                    return resp_error(&format!(
                        "{word}: value must be in the range [{min}, {max}]"
                    ));
                }
                Some(v) => match slot {
                    0 => p.max_iterations = v as u16,
                    1 => p.bucket_size = v as u16,
                    _ => p.expansion = v as u16,
                },
            },
        }
    }
    if capacity < 2 * i64::from(p.bucket_size) || capacity > MAX_CAPACITY {
        return resp_error("Capacity must be in the range [2 * BUCKETSIZE, 1073741824]");
    }
    p.capacity = capacity as u64;
    TOUCHED.set(true);
    match filter_for_write(ctx, key) {
        Err(e) => return e,
        Ok(Some(s)) if !is_cuckoo(&s) => return resp_error(WRONGTYPE),
        Ok(Some(_)) => return resp_error(EXISTS),
        Ok(None) => {}
    }
    if Cuckoo::bytes_for(&p) > MAX_BYTES {
        return resp_error(TOO_LARGE);
    }
    if let Err(e) = room(ctx, Cuckoo::bytes_for(&p)) {
        return e;
    }
    let cf = match Cuckoo::new(p) {
        Ok(cf) => cf,
        Err(e) => return add_error(e),
    };
    if publish(ctx, key, cf).is_err() {
        return resp_error(EXISTS);
    }
    // Sent naming everything it took, so that a replica or a replay with
    // other settings makes the same filter.
    MADE.with_borrow_mut(|m| *m = Some(reserve_form(key, &p)));
    resp_simple("OK")
}

/// Whether a client's add to `cf` is refused before it starts, as Redis
/// refuses one to a filter of `cf-max-expansions` sub-filters or more.
fn too_many_filters(ctx: &Ctx, cf: &Cuckoo) -> bool {
    !ctx.apply_replication && cf.filters.len() as u64 >= setting(&MAX_EXPANSIONS_SETTING)
}

/// For a command only asked what it wants (`sketch::room`): its answer,
/// from the filter `sketch` as `f` finds it (`f` refuses the command, or
/// says what it would allocate).
fn asked(
    ctx: &Ctx,
    sketch: &parking_lot::RwLock<Sketch>,
    wrong_type: &str,
    f: impl FnOnce(&Cuckoo) -> Result<u64, Vec<u8>>,
) -> Vec<u8> {
    let bytes = match &*sketch.read() {
        Sketch::Cuckoo(cf) => f(cf),
        #[allow(unreachable_patterns)]
        _ => return resp_error(wrong_type),
    };
    match bytes {
        Ok(bytes) => room(ctx, bytes).err().unwrap_or_default(),
        Err(refused) => refused,
    }
}

/// `CF.ADD key item` and `CF.ADDNX key item`
fn add(ctx: &mut Ctx, nx: bool) -> Vec<u8> {
    begin_write();
    let name = if nx { "CF.ADDNX" } else { "CF.ADD" };
    if ctx.cmd.len() != 3 {
        return wrong_args(name);
    }
    TOUCHED.set(true);
    let key = ctx.cmd[1];
    let l = lookup(ctx.cmd[2]);
    let resp = resp(ctx);
    let max = max_filters(ctx);
    loop {
        let sketch = match filter_for_write(ctx, key) {
            Ok(Some(s)) => s,
            Ok(None) => {
                // Missing: every check, and room, before the filter is
                // made; it is made with the item in it (a new filter has a
                // free slot for its first item), and then put there.
                let p = match params_to_make(ctx, None) {
                    Ok(p) => p,
                    Err(e) => return e,
                };
                if let Err(e) = room(ctx, Cuckoo::bytes_for(&p)) {
                    return e;
                }
                let mut cf = match Cuckoo::new(p) {
                    Ok(cf) => cf,
                    Err(e) => return add_error(e),
                };
                if let Err(e) = cf.insert(l, max) {
                    return add_error(e);
                }
                if publish(ctx, key, cf).is_err() {
                    continue;
                }
                MADE.with_borrow_mut(|m| *m = Some(reserve_form(key, &p)));
                return boolean(resp, true);
            }
            Err(e) => return e,
        };
        if crate::sketch::room::asking() {
            return asked(ctx, &sketch, WRONGTYPE, |cf| {
                if too_many_filters(ctx, cf) {
                    return Err(resp_error(MAX_EXPANSIONS));
                }
                Ok(cf.bytes_to_add_item(l))
            });
        }
        // The one hold of the filter: its type, the limits, room, the add.
        let mut guard = sketch.write();
        let Sketch::Cuckoo(cf) = &mut *guard else {
            return resp_error(WRONGTYPE);
        };
        if too_many_filters(ctx, cf) {
            return resp_error(MAX_EXPANSIONS);
        }
        if let Err(e) = room(ctx, cf.bytes_to_add_item(l)) {
            return e;
        }
        if nx && cf.contains(l) {
            return boolean(resp, false);
        }
        return match cf.insert(l, max) {
            Ok(()) => boolean(resp, true),
            Err(e) => add_error(e),
        };
    }
}

/// Add each of `items` to `cf` (for `CF.INSERT`, `CF.INSERTNX` if `nx`),
/// the filter at `key`: their reply. It grows the filter by `growth` bytes
/// as it was given room for; past that, only while there is room: an item
/// that would grow it further when there is none, or that the system has
/// no memory for, is answered with that error and changes nothing, and the
/// command is then sent to replicas and the AOF as the items it added.
fn add_all(
    ctx: &Ctx,
    cf: &mut Cuckoo,
    key: &[u8],
    items: &[&[u8]],
    nx: bool,
    resp: u8,
    mut growth: u64,
) -> Vec<u8> {
    let max = max_filters(ctx);
    let mut replies = Vec::with_capacity(items.len());
    let mut added: Vec<&[u8]> = Vec::with_capacity(items.len());
    let mut refused = false;
    for item in items {
        let l = lookup(item);
        if nx && cf.contains(l) {
            replies.push(resp_integer(0));
            continue;
        }
        let mut may_grow = |bytes: u64| {
            if growth >= bytes {
                growth -= bytes;
                return true;
            }
            growth = 0;
            crate::sketch::room::fits(ctx, |_| bytes)
        };
        match cf.insert_within(l, max, &mut may_grow) {
            Ok(()) => {
                added.push(item);
                replies.push(if nx {
                    resp_integer(1)
                } else {
                    boolean(resp, true)
                });
            }
            Err(e @ (CuckooError::NoRoom | CuckooError::NoMemory)) => {
                refused = true;
                replies.push(add_error(e));
            }
            Err(_) => replies.push(resp_integer(-1)),
        }
    }
    if refused {
        let sent = (!added.is_empty()).then(|| insert_form(key, &added));
        SENT.with_borrow_mut(|s| *s = Some(sent));
    }
    resp_array(replies)
}

/// Whether `word` is `keyword` as `CF.INSERT` takes its words: by their
/// first letter alone, regardless of case (`C1` is `CAPACITY`), as Redis
/// 8.10 takes them.
fn keyword(word: &[u8], wanted: &[u8]) -> bool {
    matches!((word.first(), wanted.first()), (Some(a), Some(b)) if a.eq_ignore_ascii_case(b))
}

/// `CF.INSERT key [CAPACITY capacity] [NOCREATE] ITEMS item [item ...]`,
/// and `CF.INSERTNX`: 1 for an item added, 0 for one `CF.INSERTNX` found
/// there, -1 for one the filter had no room for.
fn insert(ctx: &mut Ctx, nx: bool) -> Vec<u8> {
    begin_write();
    let name = if nx { "CF.INSERTNX" } else { "CF.INSERT" };
    let cmd = ctx.cmd.clone();
    if cmd.len() < 4 {
        return wrong_args(name);
    }
    let mut capacity = None;
    let mut nocreate = false;
    let mut items: Option<&[&[u8]]> = None;
    let mut at = 2;
    while at < cmd.len() {
        let word = cmd[at];
        if keyword(word, b"ITEMS") {
            items = Some(&cmd[at + 1..]);
            break;
        } else if keyword(word, b"NOCREATE") {
            nocreate = true;
            at += 1;
        } else if keyword(word, b"CAPACITY") {
            let Some(value) = cmd.get(at + 1) else {
                return wrong_args(name);
            };
            match parse_ll(value) {
                None => return resp_error("Bad capacity"),
                Some(c) => {
                    // Not this node's setting for a command from a master or the
                    // AOF: it took what the master took.
                    let least = if ctx.apply_replication {
                        2
                    } else {
                        2 * setting(&BUCKET_SIZE) as i64
                    };
                    if c < least || c > MAX_CAPACITY {
                        return resp_error(
                            "Capacity must be in the range [cf-bucket-size * 2, 1073741824]",
                        );
                    }
                    capacity = Some(c as u64);
                }
            }
            at += 2;
        } else {
            return resp_error("Unknown argument received");
        }
    }
    let items = match items {
        Some(items) if !items.is_empty() => items,
        _ => return wrong_args(name),
    };
    TOUCHED.set(true);
    let key = cmd[1];
    let resp = resp(ctx);
    let n = items.len() as u64;
    // The per-command limit, reckoned before it starts.
    let budgeted = !ctx.apply_replication && n > 1;
    let over_budget = |cost: u64| budgeted && n.saturating_mul(cost) > BUDGET;
    loop {
        let sketch = match filter_for_write(ctx, key) {
            Ok(Some(s)) => s,
            Ok(None) if nocreate => return resp_error(NOT_FOUND),
            Ok(None) => {
                // Missing: every check, and room for the filter and for
                // what the items grow it by, before the filter is made; the
                // items go into it, and it is then put there.
                let p = match params_to_make(ctx, capacity) {
                    Ok(p) => p,
                    Err(e) => return e,
                };
                if over_budget(add_cost_of(1, p.bucket_size, p.max_iterations)) {
                    return resp_error(OVER_BUDGET);
                }
                let wanted = Cuckoo::bytes_to_make_and_add(&p, n);
                if let Err(e) = room(ctx, wanted) {
                    return e;
                }
                let mut cf = match Cuckoo::new(p) {
                    Ok(cf) => cf,
                    Err(e) => return add_error(e),
                };
                let growth = wanted.saturating_sub(Cuckoo::bytes_for(&p));
                let reply = add_all(ctx, &mut cf, key, items, nx, resp, growth);
                if publish(ctx, key, cf).is_err() {
                    SENT.with_borrow_mut(|s| *s = None);
                    continue;
                }
                MADE.with_borrow_mut(|m| *m = Some(reserve_form(key, &p)));
                return reply;
            }
            Err(e) => return e,
        };
        let refused = |cf: &Cuckoo| {
            if too_many_filters(ctx, cf) {
                Some(resp_error(MAX_EXPANSIONS))
            } else if over_budget(add_cost(cf)) {
                Some(resp_error(OVER_BUDGET))
            } else {
                None
            }
        };
        if crate::sketch::room::asking() {
            return asked(ctx, &sketch, WRONGTYPE, |cf| match refused(cf) {
                Some(e) => Err(e),
                None => Ok(cf.bytes_to_add_items(n)),
            });
        }
        // The one hold of the filter: its type, the limits, room, the adds.
        let mut guard = sketch.write();
        let Sketch::Cuckoo(cf) = &mut *guard else {
            return resp_error(WRONGTYPE);
        };
        if let Some(e) = refused(cf) {
            return e;
        }
        let wanted = cf.bytes_to_add_items(n);
        if let Err(e) = room(ctx, wanted) {
            return e;
        }
        let growth = wanted.saturating_sub(cf.shared_bytes());
        return add_all(ctx, cf, key, items, nx, resp, growth);
    }
}

/// `CF.EXISTS key item`: 0 for a missing key or another type, as in Redis.
fn exists(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 3 {
        return wrong_args("CF.EXISTS");
    }
    let l = lookup(ctx.cmd[2]);
    let found = matches!(
        with_filter(ctx, ctx.cmd[1], |cf| cf.contains(l)),
        Ok(Some(true))
    );
    boolean(resp(ctx), found)
}

/// `CF.MEXISTS key item [item ...]`
fn mexists(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() < 3 {
        return wrong_args("CF.MEXISTS");
    }
    let resp = resp(ctx);
    let items = &ctx.cmd[2..];
    let budgeted = !ctx.apply_replication;
    match with_filter(ctx, ctx.cmd[1], |cf| {
        if budgeted && (items.len() as u64).saturating_mul(look_cost(cf)) > BUDGET {
            return resp_error(OVER_BUDGET);
        }
        resp_array(
            items
                .iter()
                .map(|i| boolean(resp, cf.contains(lookup(i))))
                .collect(),
        )
    }) {
        Ok(Some(reply)) => reply,
        _ => resp_array(vec![boolean(resp, false); items.len()]),
    }
}

/// `CF.COUNT key item`
fn count(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 3 {
        return wrong_args("CF.COUNT");
    }
    let l = lookup(ctx.cmd[2]);
    match with_filter(ctx, ctx.cmd[1], |cf| cf.count(l)) {
        Ok(Some(n)) => resp_integer(i64::try_from(n).unwrap_or(i64::MAX)),
        _ => resp_integer(0),
    }
}

/// `CF.DEL key item`
fn del(ctx: &mut Ctx) -> Vec<u8> {
    begin_write();
    if ctx.cmd.len() != 3 {
        return wrong_args("CF.DEL");
    }
    TOUCHED.set(true);
    let resp = resp(ctx);
    let l = lookup(ctx.cmd[2]);
    let sketch = match filter_for_write(ctx, ctx.cmd[1]) {
        Ok(Some(s)) => s,
        _ => return resp_error("Not found"),
    };
    // Room only for a copy of what a snapshot or a copy shares: a delete
    // frees, and is let run over the limit otherwise.
    if crate::sketch::room::asking() {
        return asked(ctx, &sketch, "Not found", |cf| Ok(cf.bytes_to_delete(l)));
    }
    let mut guard = sketch.write();
    let Sketch::Cuckoo(cf) = &mut *guard else {
        return resp_error("Not found");
    };
    if let Err(e) = room(ctx, cf.bytes_to_delete(l)) {
        return e;
    }
    match cf.delete(l) {
        Ok(found) => boolean(resp, found),
        Err(e) => add_error(e),
    }
}

/// `CF.INFO key`
fn info(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 2 {
        return wrong_args("CF.INFO");
    }
    let resp = resp(ctx);
    let n = |v: u64| resp_integer(i64::try_from(v).unwrap_or(i64::MAX));
    match with_filter(ctx, ctx.cmd[1], |cf| {
        resp_map_for(
            resp,
            vec![
                resp_simple("Size"),
                n(cf.info_size()),
                resp_simple("Number of buckets"),
                n(cf.filters[0].buckets),
                resp_simple("Number of filters"),
                n(cf.filters.len() as u64),
                resp_simple("Number of items inserted"),
                n(cf.items),
                resp_simple("Number of items deleted"),
                n(cf.deletes),
                resp_simple("Bucket size"),
                n(u64::from(cf.bucket_size)),
                resp_simple("Expansion rate"),
                n(u64::from(cf.expansion)),
                resp_simple("Max iterations"),
                n(u64::from(cf.max_iterations)),
            ],
        )
    }) {
        Ok(Some(r)) => r,
        Ok(None) => resp_error(NOT_FOUND),
        Err(()) => resp_error(WRONGTYPE),
    }
}

/// `CF.DEBUG key`
fn debug(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 2 {
        return wrong_args("CF.DEBUG");
    }
    match with_filter(ctx, ctx.cmd[1], |cf| {
        format!(
            "bktsize:{} buckets:{} items:{} deletes:{} filters:{} max_iterations:{} expansion:{}",
            cf.bucket_size,
            cf.filters[0].buckets,
            cf.items,
            cf.deletes,
            cf.filters.len(),
            cf.max_iterations,
            cf.expansion
        )
    }) {
        Ok(Some(s)) => resp_bulk_str(&s),
        Ok(None) => resp_error(NOT_FOUND),
        Err(()) => resp_error(WRONGTYPE),
    }
}

/// `CF.SCANDUMP key iterator`: `[0, nil]` at the end, for a missing key,
/// and for a filter with no items, as in Redis.
fn scandump(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 3 {
        return wrong_args("CF.SCANDUMP");
    }
    let Some(iter) = parse_ll(ctx.cmd[2]).filter(|&i| i >= 0) else {
        return resp_error("Invalid position");
    };
    match with_filter(ctx, ctx.cmd[1], |cf| cf.scandump(iter)) {
        Ok(Some(Some((next, chunk)))) => resp_array(vec![resp_integer(next), resp_bulk(&chunk)]),
        Ok(Some(None)) => resp_array(vec![resp_integer(0), resp_null_for(resp(ctx))]),
        Ok(None) => resp_error(NOT_FOUND),
        Err(()) => resp_error(WRONGTYPE),
    }
}

/// `CF.LOADCHUNK key iterator data`: iterator 1 is the header, which makes
/// the filter; the rest fill it.
fn loadchunk(ctx: &mut Ctx) -> Vec<u8> {
    begin_write();
    if ctx.cmd.len() != 4 {
        return wrong_args("CF.LOADCHUNK");
    }
    let Some(iter) = parse_ll(ctx.cmd[2]).filter(|&i| i != 0) else {
        return resp_error("Invalid position");
    };
    TOUCHED.set(true);
    let key = ctx.cmd[1];
    let data = ctx.cmd[3];
    let existing = match filter_for_write(ctx, key) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let header_error = |e: HeaderError| match e {
        HeaderError::Invalid => resp_error("Invalid header"),
        HeaderError::Bad => resp_error("Couldn't create filter!"),
        HeaderError::TooLarge => resp_error(TOO_LARGE),
        HeaderError::NoMemory => resp_error(NO_MEMORY),
    };
    if iter == 1 {
        match &existing {
            Some(s) if !is_cuckoo(s) => return resp_error(WRONGTYPE),
            Some(_) => return resp_error(EXISTS),
            None => {}
        }
        // The header's shape checked, then room for its sub-filters, and
        // only then are they allocated.
        let (shape, bytes) = match Cuckoo::header_shape(data, max_bucket_size(ctx)) {
            Ok(checked) => checked,
            Err(e) => return header_error(e),
        };
        if let Err(e) = room(ctx, bytes) {
            return e;
        }
        let cf = match Cuckoo::from_shape(&shape) {
            Ok(cf) => cf,
            Err(e) => return header_error(e),
        };
        if publish(ctx, key, cf).is_err() {
            return resp_error(EXISTS);
        }
        return resp_simple("OK");
    }
    let Some(sketch) = existing else {
        return resp_error(NOT_FOUND);
    };
    // A chunk is copied into a sub-filter, which is copied first if a
    // snapshot or a copy shares it: room for that, and nothing written
    // while only asked.
    if crate::sketch::room::asking() {
        return asked(ctx, &sketch, WRONGTYPE, |cf| {
            Ok(cf.bytes_to_load_chunk(iter, data.len()))
        });
    }
    let mut guard = sketch.write();
    let Sketch::Cuckoo(cf) = &mut *guard else {
        return resp_error(WRONGTYPE);
    };
    if let Err(e) = room(ctx, cf.bytes_to_load_chunk(iter, data.len())) {
        return e;
    }
    match cf.load_chunk(iter, data) {
        Ok(()) => resp_simple("OK"),
        Err(e) => resp_error(e),
    }
}

/// `CF.COMPACT key`: Redis's words for a missing key and for another type
/// alike.
fn compact(ctx: &mut Ctx) -> Vec<u8> {
    begin_write();
    if ctx.cmd.len() != 2 {
        return wrong_args("CF.COMPACT");
    }
    TOUCHED.set(true);
    const MISSING: &str = "Cuckoo filter was not found";
    let sketch = match filter_for_write(ctx, ctx.cmd[1]) {
        Ok(Some(s)) => s,
        _ => return resp_error(MISSING),
    };
    // Room only for a copy of what a snapshot or a copy shares, as for
    // `CF.DEL`.
    if crate::sketch::room::asking() {
        return asked(ctx, &sketch, MISSING, |cf| Ok(cf.bytes_to_compact()));
    }
    let mut guard = sketch.write();
    let Sketch::Cuckoo(cf) = &mut *guard else {
        return resp_error(MISSING);
    };
    if let Err(e) = room(ctx, cf.bytes_to_compact()) {
        return e;
    }
    match cf.compact(true) {
        Ok(()) => resp_simple("OK"),
        Err(e) => add_error(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arities_as_redis_reports_them() {
        assert_eq!(arity("CF.RESERVE"), Some(-3));
        assert_eq!(arity("CF.INSERT"), Some(-4));
        assert_eq!(arity("CF.LOADCHUNK"), Some(4));
        assert_eq!(arity("CF.COMPACT"), Some(-1));
        assert_eq!(arity("CF.NOPE"), None);
    }

    #[test]
    fn made_filters_propagate_as_reserves() {
        let p = Params {
            capacity: 1024,
            bucket_size: 2,
            max_iterations: 20,
            expansion: 1,
        };
        let words: Vec<String> = reserve_form(b"k", &p)
            .into_iter()
            .map(|w| String::from_utf8(w).unwrap())
            .collect();
        assert_eq!(
            words,
            [
                "CF.RESERVE",
                "k",
                "1024",
                "BUCKETSIZE",
                "2",
                "MAXITERATIONS",
                "20",
                "EXPANSION",
                "1"
            ]
        );
    }
}
