// src/commands/search_tools.rs
//
// What surrounds a query: cursors over the rows of an aggregation
// (`FT.AGGREGATE … WITHCURSOR`, `FT.CURSOR`), the plan of a query
// (`FT.EXPLAIN`, `FT.EXPLAINCLI`), a query run with its timings
// (`FT.PROFILE`), and the settings of the query engine (`FT.CONFIG`).

use ahash::AHashMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use crate::commands::aggregate_pipeline::value_reply;
use crate::commands::ctx::Ctx;
use crate::resp::write::*;
use crate::vector::filter::{self, Filter, Knn, Params, TextMatch};

fn text(arg: &[u8]) -> &str {
    std::str::from_utf8(arg).unwrap_or("")
}

fn is(arg: &[u8], word: &str) -> bool {
    arg.eq_ignore_ascii_case(word.as_bytes())
}

// ── Rows ──────────────────────────────────────────────────────────────────────

/// One row of an aggregation, as it is shown: names and values, in order.
pub(crate) type ShownRow = Vec<(String, String)>;

/// The reply of an aggregation: `total`, then the rows. `timed_out` is
/// whether the scan stopped because `TIMEOUT` ran out (`ON_TIMEOUT return`).
pub(crate) fn rows_reply(resp: u8, total: usize, rows: &[ShownRow], timed_out: bool) -> Vec<u8> {
    let fields = |row: &ShownRow| -> Vec<Vec<u8>> {
        row.iter()
            .flat_map(|(name, value)| [resp_bulk_str(name), value_reply(value, resp)])
            .collect()
    };
    let warning = if timed_out {
        vec![resp_bulk_str("Timeout limit was reached")]
    } else {
        vec![]
    };
    if resp == 3 {
        return resp_map_for(
            3,
            vec![
                resp_bulk_str("attributes"),
                resp_array(vec![]),
                resp_bulk_str("format"),
                resp_bulk_str("STRING"),
                resp_bulk_str("results"),
                resp_array(
                    rows.iter()
                        .map(|row| {
                            resp_map_for(
                                3,
                                vec![
                                    resp_bulk_str("extra_attributes"),
                                    resp_map_for(3, fields(row)),
                                    resp_bulk_str("values"),
                                    resp_array(vec![]),
                                ],
                            )
                        })
                        .collect(),
                ),
                resp_bulk_str("total_results"),
                resp_integer(total as i64),
                resp_bulk_str("warning"),
                resp_array(warning),
            ],
        );
    }
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(rows.len() + 1);
    out.push(resp_integer(total as i64));
    out.extend(rows.iter().map(|row| resp_array(fields(row))));
    resp_array(out)
}

// ── Cursors ───────────────────────────────────────────────────────────────────
//
// `FT.AGGREGATE … WITHCURSOR [COUNT n] [MAXIDLE ms]` answers with the first
// `n` rows and a cursor; `FT.CURSOR READ index cursor [COUNT n]` with the
// next, until the cursor it answers with is 0. The aggregation is run once,
// when the cursor is opened, and its rows are kept: what a cursor reads is
// the result as it was then.
//
// A cursor nobody has read for its `MAXIDLE` is gone, as is one whose index
// has been dropped.

struct Cursor {
    index: String,
    rows: VecDeque<ShownRow>,
    count: usize,
    max_idle: Duration,
    used: Instant,
    /// Whether the aggregation ran out of time: every page of a partial
    /// result says so, not only the first.
    timed_out: bool,
}

static CURSORS: LazyLock<Mutex<AHashMap<u64, Cursor>>> =
    LazyLock::new(|| Mutex::new(AHashMap::new()));

/// More than this many cursors open at once is a client that is not
/// reading, or closing, the ones it opens.
const MAX_CURSORS: usize = 1024;
const DEFAULT_COUNT: usize = 1000;

/// `[rows, cursor]`.
fn cursor_reply(resp: u8, rows: &[ShownRow], cursor: u64, timed_out: bool) -> Vec<u8> {
    resp_array(vec![
        rows_reply(resp, rows.len(), rows, timed_out),
        resp_integer(cursor as i64),
    ])
}

fn forget_idle(cursors: &mut AHashMap<u64, Cursor>) {
    let now = Instant::now();
    cursors.retain(|_, cursor| now.duration_since(cursor.used) <= cursor.max_idle);
}

/// The `WITHCURSOR` of an `FT.AGGREGATE`, if it has one: how many rows a
/// read returns and how long the cursor may sit unread.
pub(crate) fn cursor_options(cmd: &[&[u8]]) -> Result<Option<(usize, Duration)>, String> {
    let Some(at) = cmd.iter().position(|a| is(a, "WITHCURSOR")) else {
        return Ok(None);
    };
    let mut count = DEFAULT_COUNT;
    let mut max_idle = Duration::from_millis(config_number("CURSOR_MAX_IDLE", 300_000));
    let mut i = at + 1;
    while let Some(word) = cmd.get(i) {
        let number = cmd
            .get(i + 1)
            .and_then(|n| text(n).parse::<u64>().ok())
            .filter(|n| *n > 0);
        if is(word, "COUNT") {
            count = number.ok_or("Bad arguments for COUNT: Expected a positive integer")? as usize;
        } else if is(word, "MAXIDLE") {
            // No longer than the server allows.
            let limit = max_idle;
            max_idle = Duration::from_millis(
                number.ok_or("Bad arguments for MAXIDLE: Expected a positive integer")?,
            )
            .min(limit);
        } else {
            break;
        }
        i += 2;
    }
    Ok(Some((count, max_idle)))
}

/// Answer with the first rows, and keep the rest under a cursor.
pub(crate) fn open_cursor(
    resp: u8,
    index: &str,
    rows: Vec<ShownRow>,
    count: usize,
    max_idle: Duration,
    timed_out: bool,
) -> Vec<u8> {
    let mut rows: VecDeque<ShownRow> = rows.into();
    let first: Vec<ShownRow> = rows.drain(..count.min(rows.len())).collect();
    if rows.is_empty() {
        return cursor_reply(resp, &first, 0, timed_out);
    }
    let mut cursors = CURSORS.lock();
    forget_idle(&mut cursors);
    if cursors.len() >= MAX_CURSORS {
        return resp_error("Too many cursors allocated for index");
    }
    let id = loop {
        // Positive as a signed integer, and never 0: that is "no cursor".
        let id = rand::random::<u64>() >> 1;
        if id != 0 && !cursors.contains_key(&id) {
            break id;
        }
    };
    cursors.insert(
        id,
        Cursor {
            index: index.to_string(),
            rows,
            count,
            max_idle,
            used: Instant::now(),
            timed_out,
        },
    );
    cursor_reply(resp, &first, id, timed_out)
}

/// `FT.CURSOR READ index cursor [COUNT n]` / `FT.CURSOR DEL index cursor`.
pub(crate) fn ft_cursor(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    let sub = cmd.get(1).map(|s| text(s).to_ascii_uppercase());
    let Some(sub) = sub else {
        return Some(resp_error(
            "ERR wrong number of arguments for 'FT.CURSOR' command",
        ));
    };
    if !matches!(sub.as_str(), "READ" | "DEL" | "GC") {
        return Some(resp_error("Unknown subcommand"));
    }
    if sub == "GC" {
        // Collection of idle cursors: here that happens on every use.
        let mut cursors = CURSORS.lock();
        let before = cursors.len();
        forget_idle(&mut cursors);
        return Some(resp_integer((before - cursors.len()) as i64));
    }
    let (Some(index), Some(id)) = (
        cmd.get(2).map(|a| text(a)),
        cmd.get(3).and_then(|a| text(a).parse::<u64>().ok()),
    ) else {
        return Some(resp_error(
            "ERR wrong number of arguments for 'FT.CURSOR' command",
        ));
    };
    let index_exists = ctx.index_store.read().contains_key(index);

    let mut cursors = CURSORS.lock();
    forget_idle(&mut cursors);
    let known = cursors.get(&id).is_some_and(|cursor| cursor.index == index);
    if known && !index_exists {
        cursors.remove(&id);
    }
    if !known || !index_exists {
        return Some(resp_error(if sub == "DEL" {
            "Cursor does not exist"
        } else {
            "Cursor not found"
        }));
    }
    if sub == "DEL" {
        cursors.remove(&id);
        return Some(resp_simple("OK"));
    }

    let mut count = None;
    if let Some(word) = cmd.get(4) {
        if !is(word, "COUNT") {
            return Some(resp_error(&format!("Unknown argument `{}`", text(word))));
        }
        match cmd
            .get(5)
            .and_then(|n| text(n).parse::<usize>().ok())
            .filter(|n| *n > 0)
        {
            Some(n) => count = Some(n),
            None => {
                return Some(resp_error(
                    "Bad arguments for COUNT: Expected a positive integer",
                ));
            }
        }
    }
    let cursor = cursors.get_mut(&id)?;
    let count = count.unwrap_or(cursor.count);
    let rows: Vec<ShownRow> = cursor.rows.drain(..count.min(cursor.rows.len())).collect();
    cursor.used = Instant::now();
    let left = !cursor.rows.is_empty();
    let timed_out = cursor.timed_out;
    if !left {
        cursors.remove(&id);
    }
    Some(cursor_reply(
        ctx.resp,
        &rows,
        if left { id } else { 0 },
        timed_out,
    ))
}

// ── FT.CONFIG ─────────────────────────────────────────────────────────────────
//
// The settings of the query engine, by the names RediSearch gives them. They
// can be read and set, so that a tool that manages them works; the ones this
// engine acts on are marked.

static CONFIG: LazyLock<Mutex<Vec<(&'static str, String)>>> = LazyLock::new(|| {
    Mutex::new(
        [
            // Acted on: the longest a cursor may sit unread.
            ("CURSOR_MAX_IDLE", "300000"),
            // Acted on: a query longer than this many milliseconds is cut
            // short (`ON_TIMEOUT` says whether that is an error or a
            // partial reply). `0` is no limit, and the default: a query here
            // can look at every document (`FT.SEARCH idx * LIMIT 0 0` counts
            // them one by one), and a limit nobody asked for would make a
            // large index's counts silently short over RESP2. The server
            // settings `search-timeout` and `search-on-timeout` are these two.
            ("TIMEOUT", "0"),
            ("ON_TIMEOUT", "return"),
            // Acted on: the most results one FT.SEARCH may be asked for.
            ("MAXSEARCHRESULTS", "1000000"),
            ("MAXAGGREGATERESULTS", "unlimited"),
            ("DEFAULT_DIALECT", "1"),
            ("MINPREFIX", "2"),
            ("MINSTEMLEN", "4"),
            ("MAXPREFIXEXPANSIONS", "200"),
            ("MAXEXPANSIONS", "200"),
            ("MAXDOCTABLESIZE", "1000000"),
            ("MIN_PHONETIC_TERM_LEN", "3"),
            ("DEFAULT_SCORER", "BM25STD"),
            ("WORKERS", "0"),
            ("NOGC", "false"),
            ("FORK_GC_RUN_INTERVAL", "30"),
            ("FORK_GC_CLEAN_THRESHOLD", "100"),
            ("PARTIAL_INDEXED_DOCS", "false"),
            ("UNION_ITERATOR_HEAP", "20"),
            ("VSS_MAX_RESIZE", "0"),
            ("INDEX_CURSOR_LIMIT", "128"),
            ("MULTI_TEXT_SLOP", "100"),
            ("BM25STD_TANH_FACTOR", "4"),
        ]
        .into_iter()
        .map(|(name, value)| (name, value.to_string()))
        .collect(),
    )
});

/// A numeric setting, or `default` if it does not hold a number.
pub(crate) fn config_number(name: &str, default: u64) -> u64 {
    CONFIG
        .lock()
        .iter()
        .find(|(n, _)| *n == name)
        .and_then(|(_, value)| value.parse().ok())
        .unwrap_or(default)
}

/// `ON_TIMEOUT fail` and `return_strict` error when a query runs out of
/// time, instead of returning what was found.
pub(crate) fn on_timeout_fails() -> bool {
    CONFIG
        .lock()
        .iter()
        .find(|(n, _)| *n == "ON_TIMEOUT")
        .is_some_and(|(_, value)| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "fail" | "return_strict"
            )
        })
}

/// What a setting holds now. The server settings `search-timeout` and
/// `search-on-timeout` are read from here, so that `TIMEOUT` and
/// `ON_TIMEOUT` have one value whichever name sets them.
pub(crate) fn config_value(name: &str) -> String {
    CONFIG
        .lock()
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, value)| value.clone())
        .unwrap_or_default()
}

/// Set a setting as `FT.CONFIG SET` does. `false`: not a value it takes.
pub(crate) fn set_config(name: &str, value: &str) -> bool {
    let mut config = CONFIG.lock();
    match config.iter_mut().find(|(n, _)| *n == name) {
        Some(slot) if config_value_ok(name, value) => {
            slot.1 = value.to_string();
            true
        }
        _ => false,
    }
}

/// Settings that hold something other than a non-negative number.
fn config_value_ok(name: &str, value: &str) -> bool {
    match name {
        "ON_TIMEOUT" => matches!(
            value.to_ascii_lowercase().as_str(),
            "return" | "fail" | "return_strict"
        ),
        "NOGC" | "PARTIAL_INDEXED_DOCS" => {
            matches!(value.to_ascii_lowercase().as_str(), "true" | "false")
        }
        "DEFAULT_SCORER" => !value.is_empty(),
        "MAXAGGREGATERESULTS" => value == "unlimited" || value.parse::<u64>().is_ok(),
        "DEFAULT_DIALECT" => matches!(value, "1" | "2" | "3" | "4"),
        // No more than `search-timeout` takes, which is this setting too: a
        // value one name took and the other refused would not read back
        // from the config file `CONFIG REWRITE` writes.
        "TIMEOUT" => value.parse::<i64>().is_ok_and(|ms| ms >= 0),
        _ => value.parse::<u64>().is_ok(),
    }
}

pub(crate) fn ft_config(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    let sub = cmd
        .get(1)
        .map(|s| text(s).to_ascii_uppercase())
        .unwrap_or_default();
    match sub.as_str() {
        "GET" => {
            let Some(pattern) = cmd.get(2).map(|a| text(a).to_ascii_uppercase()) else {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'FT.CONFIG|GET' command",
                ));
            };
            let config = CONFIG.lock();
            let found: Vec<&(&'static str, String)> = config
                .iter()
                .filter(|(name, _)| crate::commands::helpers::glob_match(&pattern, name))
                .collect();
            if ctx.resp == 3 {
                return Some(resp_map_for(
                    3,
                    found
                        .iter()
                        .flat_map(|(name, value)| [resp_bulk_str(name), resp_bulk_str(value)])
                        .collect(),
                ));
            }
            Some(resp_array(
                found
                    .iter()
                    .map(|(name, value)| {
                        resp_array(vec![resp_bulk_str(name), resp_bulk_str(value)])
                    })
                    .collect(),
            ))
        }
        "SET" => {
            let (Some(name), Some(value)) = (
                cmd.get(2).map(|a| text(a).to_ascii_uppercase()),
                cmd.get(3).map(|a| text(a)),
            ) else {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'FT.CONFIG|SET' command",
                ));
            };
            let mut config = CONFIG.lock();
            let Some(slot) = config.iter_mut().find(|(n, _)| *n == name) else {
                return Some(resp_error("Invalid option"));
            };
            if !config_value_ok(&name, value) {
                return Some(resp_error(&format!(
                    "Bad arguments for {name}: Could not convert argument to expected type"
                )));
            }
            // As `search-on-timeout` reads it back.
            slot.1 = if name == "ON_TIMEOUT" {
                value.to_ascii_lowercase()
            } else {
                value.to_string()
            };
            Some(resp_simple("OK"))
        }
        "HELP" => {
            let config = CONFIG.lock();
            Some(resp_array(
                config
                    .iter()
                    .filter(|(name, _)| {
                        cmd.get(2).is_none_or(|pattern| {
                            crate::commands::helpers::glob_match(
                                &text(pattern).to_ascii_uppercase(),
                                name,
                            )
                        })
                    })
                    .map(|(name, value)| {
                        resp_array(vec![
                            resp_bulk_str(name),
                            resp_bulk_str("Description"),
                            resp_bulk_str(""),
                            resp_bulk_str("Value"),
                            resp_bulk_str(value),
                        ])
                    })
                    .collect(),
            ))
        }
        _ => Some(resp_error("Unknown subcommand")),
    }
}

// ── FT.EXPLAIN ────────────────────────────────────────────────────────────────

/// `PARAMS n name value …`, wherever in the command it is.
fn params_of(cmd: &[&[u8]]) -> Params {
    let mut params = Params::new();
    if let Some(at) = cmd.iter().position(|a| is(a, "PARAMS")) {
        let n = cmd
            .get(at + 1)
            .and_then(|a| text(a).parse::<usize>().ok())
            .unwrap_or(0);
        let values: Vec<&[u8]> = cmd.iter().skip(at + 2).take(n).copied().collect();
        for pair in values.chunks(2) {
            if let [name, value] = pair {
                params.insert(text(name).to_string(), value.to_vec());
            }
        }
    }
    params
}

fn bound(value: f64) -> String {
    if value == f64::INFINITY {
        "inf".to_string()
    } else if value == f64::NEG_INFINITY {
        "-inf".to_string()
    } else {
        format!("{value:.6}")
    }
}

fn term_text(term: &TextMatch) -> String {
    match term {
        TextMatch::Exact(word) => word.clone(),
        TextMatch::Prefix(start) => format!("PREFIX{{{start}*}}"),
        TextMatch::Suffix(end) => format!("SUFFIX{{*{end}}}"),
        TextMatch::Infix(part) => format!("INFIX{{*{part}*}}"),
        TextMatch::Fuzzy(word, _) => format!("FUZZY{{{word}}}"),
    }
}

/// `@field:` if the clause names one field, nothing if it is every field.
fn scope(fields: &[String], every: usize) -> String {
    match fields {
        [only] if every != 1 => format!("@{only}:"),
        _ => String::new(),
    }
}

/// Every operand of a run of the same operator: `a b c` is one INTERSECT of
/// three, not one inside another.
fn operands<'a>(filter: &'a Filter, and: bool, out: &mut Vec<&'a Filter>) {
    match filter {
        Filter::And(l, r) if and => {
            operands(l, and, out);
            operands(r, and, out);
        }
        Filter::Or(l, r) if !and => {
            operands(l, and, out);
            operands(r, and, out);
        }
        other => out.push(other),
    }
}

/// The plan of `filter`, a line at a time, as `FT.EXPLAIN` writes one.
fn explain_filter(filter: &Filter, text_fields: usize, depth: usize, out: &mut Vec<String>) {
    let pad = "  ".repeat(depth);
    let block = |open: &str, children: &[&Filter], out: &mut Vec<String>| {
        out.push(format!("{pad}{open} {{"));
        for child in children {
            explain_filter(child, text_fields, depth + 1, out);
        }
        out.push(format!("{pad}}}"));
    };
    match filter {
        Filter::And(..) | Filter::Or(..) => {
            let and = matches!(filter, Filter::And(..));
            let mut children = Vec::new();
            operands(filter, and, &mut children);
            block(if and { "INTERSECT" } else { "UNION" }, &children, out);
        }
        Filter::Not(inner) => block("NOT", &[inner], out),
        Filter::Optional(inner) => block("OPTIONAL", &[inner], out),
        Filter::Text { fields, term } => out.push(format!(
            "{pad}{}{}",
            scope(fields, text_fields),
            term_text(term)
        )),
        Filter::Phrase { fields, terms } => {
            out.push(format!("{pad}{}EXACT {{", scope(fields, text_fields)));
            for term in terms {
                out.push(format!("{pad}  {term}"));
            }
            out.push(format!("{pad}}}"));
        }
        Filter::Tag {
            field,
            values,
            prefixes,
            ..
        } => {
            out.push(format!("{pad}TAG:@{field} {{"));
            for value in values {
                out.push(format!("{pad}  {value}"));
            }
            for prefix in prefixes {
                out.push(format!("{pad}  PREFIX{{{prefix}*}}"));
            }
            out.push(format!("{pad}}}"));
        }
        Filter::Numeric {
            field,
            lo,
            lo_exclusive,
            hi,
            hi_exclusive,
        } => out.push(format!(
            "{pad}NUMERIC {{{} {} @{field} {} {}}}",
            bound(*lo),
            if *lo_exclusive { "<" } else { "<=" },
            if *hi_exclusive { "<" } else { "<=" },
            bound(*hi)
        )),
        Filter::NumericNot { field, value } => {
            out.push(format!("{pad}NOT {{"));
            out.push(format!(
                "{pad}  NUMERIC {{{} <= @{field} <= {}}}",
                bound(*value),
                bound(*value)
            ));
            out.push(format!("{pad}}}"));
        }
        Filter::VectorRange {
            field,
            radius,
            yield_as,
            ..
        } => out.push(format!(
            "{pad}VECTOR {{Vectors that are within {radius} distance radius from the query \
             vector in vector index associated with field @{field}{}}}",
            match yield_as {
                Some(name) => format!(", yields distance as `{name}`"),
                None => String::new(),
            }
        )),
        Filter::MatchAll => out.push(format!("{pad}<WILDCARD>")),
        Filter::MatchNone => out.push(format!("{pad}<EMPTY>")),
    }
}

fn explain_query(query: &filter::Query, text_fields: usize) -> Vec<String> {
    let mut out = Vec::new();
    match &query.knn {
        None => explain_filter(&query.filter, text_fields, 0, &mut out),
        Some(Knn {
            k, field, alias, ..
        }) => {
            let nearest = format!(
                "{{K={k} nearest vectors to the query vector in vector index associated with \
                 field @{field}, yields distance as `{alias}`}}"
            );
            if matches!(query.filter, Filter::MatchAll) {
                out.push(format!("VECTOR {nearest}"));
            } else {
                out.push("VECTOR {".to_string());
                explain_filter(&query.filter, text_fields, 1, &mut out);
                out.push(format!("}} => {nearest}"));
            }
        }
    }
    out
}

/// `FT.EXPLAIN index query [PARAMS …] [DIALECT n]` — the plan as one
/// string; `FT.EXPLAINCLI` — as an array of its lines.
pub(crate) fn ft_explain(ctx: &mut Ctx, lines: bool) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return Some(resp_error(&format!(
            "ERR wrong number of arguments for '{}' command",
            text(cmd[0])
        )));
    }
    let index = text(cmd[1]);
    let Some(def) = ctx.index_store.read().get(index).cloned() else {
        return Some(resp_error(&crate::commands::search::no_such_index(index)));
    };
    let query = match filter::parse_query(text(cmd[2]), &params_of(cmd), &def) {
        Ok(query) => query,
        Err(e) => return Some(resp_error(&e)),
    };
    let plan = explain_query(&query, text_field_count(&def));
    Some(if lines {
        resp_array(plan.iter().map(|line| resp_bulk_str(line)).collect())
    } else {
        resp_bulk_str(&(plan.join("\n") + "\n"))
    })
}

fn text_field_count(def: &crate::vector::IndexDef) -> usize {
    def.fields
        .iter()
        .filter(|f| matches!(f.field_type, crate::vector::FieldType::Text))
        .count()
}

// ── FT.PROFILE ────────────────────────────────────────────────────────────────

fn millis(elapsed: Duration) -> Vec<u8> {
    resp_bulk_str(&format!("{:.3}", elapsed.as_secs_f64() * 1000.0))
}

/// One node of the "Iterators profile": what the query looks things up by.
/// The times are those of the query as a whole — this engine does not time
/// its iterators one by one.
fn iterator_profile(filter: &Filter, reads: i64) -> Vec<u8> {
    let node = |kind: &str, term: Option<String>, children: Vec<&Filter>| -> Vec<u8> {
        let mut out = vec![resp_bulk_str("Type"), resp_bulk_str(kind)];
        if let Some(term) = term {
            out.push(resp_bulk_str("Term"));
            out.push(resp_bulk_str(&term));
        }
        out.push(resp_bulk_str("Time"));
        out.push(resp_bulk_str("0"));
        out.push(resp_bulk_str("Number of reading operations"));
        out.push(resp_integer(reads));
        if !children.is_empty() {
            out.push(resp_bulk_str("Child iterators"));
            out.push(resp_array(
                children
                    .into_iter()
                    .map(|child| iterator_profile(child, reads))
                    .collect(),
            ));
        }
        resp_array(out)
    };
    match filter {
        Filter::And(..) | Filter::Or(..) => {
            let and = matches!(filter, Filter::And(..));
            let mut children = Vec::new();
            operands(filter, and, &mut children);
            node(if and { "INTERSECT" } else { "UNION" }, None, children)
        }
        Filter::Not(inner) => node("NOT", None, vec![inner]),
        Filter::Optional(inner) => node("OPTIONAL", None, vec![inner]),
        Filter::Text { term, .. } => node("TEXT", Some(term_text(term)), vec![]),
        Filter::Phrase { terms, .. } => node("TEXT", Some(terms.join(" ")), vec![]),
        Filter::Tag {
            values, prefixes, ..
        } => node(
            "TAG",
            Some(
                values
                    .iter()
                    .cloned()
                    .chain(prefixes.iter().map(|p| format!("{p}*")))
                    .collect::<Vec<_>>()
                    .join(" | "),
            ),
            vec![],
        ),
        Filter::Numeric { lo, hi, .. } => node(
            "NUMERIC",
            Some(format!("{} - {}", bound(*lo), bound(*hi))),
            vec![],
        ),
        Filter::NumericNot { value, .. } => {
            node("NUMERIC", Some(format!("!= {}", bound(*value))), vec![])
        }
        Filter::VectorRange { .. } => node("VECTOR", None, vec![]),
        Filter::MatchAll => node("WILDCARD", None, vec![]),
        Filter::MatchNone => node("EMPTY", None, vec![]),
    }
}

/// The number a RESP2 result array starts with: the total.
fn total_of(reply: &[u8]) -> i64 {
    let Some(first) = reply.iter().position(|b| *b == b':') else {
        return 0;
    };
    let digits: Vec<u8> = reply[first + 1..]
        .iter()
        .take_while(|b| b.is_ascii_digit())
        .copied()
        .collect();
    text(&digits).parse().unwrap_or(0)
}

/// `FT.PROFILE index SEARCH | AGGREGATE | HYBRID [LIMITED] QUERY query …` —
/// the query's reply, and how the time it took was spent.
pub(crate) fn ft_profile(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 5 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'FT.PROFILE' command",
        ));
    }
    let kind = text(cmd[2]).to_ascii_uppercase();
    let command: &'static [u8] = match kind.as_str() {
        "SEARCH" => b"FT.SEARCH",
        "AGGREGATE" => b"FT.AGGREGATE",
        "HYBRID" => b"FT.HYBRID",
        _ => return Some(resp_error("No `SEARCH`, `HYBRID` or `AGGREGATE` provided")),
    };
    let mut at = 3;
    if is(cmd[at], "LIMITED") {
        at += 1;
    }
    if !cmd.get(at).is_some_and(|a| is(a, "QUERY")) || cmd.len() <= at + 1 {
        return Some(resp_error("The QUERY keyword is expected"));
    }
    let index = text(cmd[1]).to_string();

    // The query's plan, for the profile. Parsed here as the command itself
    // will parse it; a query that does not parse fails there, with its error.
    let parse_started = Instant::now();
    let plan = (kind != "HYBRID")
        .then(|| ctx.index_store.read().get(&index).cloned())
        .flatten()
        .and_then(|def| filter::parse_query(text(cmd[at + 1]), &params_of(cmd), &def).ok());
    let parsing = parse_started.elapsed();

    // The command, as it would have been sent.
    let saved = std::mem::take(&mut ctx.cmd);
    ctx.cmd = std::iter::once(command)
        .chain(std::iter::once(saved[1]))
        .chain(saved[at + 1..].iter().copied())
        .collect();
    let started = Instant::now();
    let reply = match kind.as_str() {
        "SEARCH" => crate::commands::search::ft_search(ctx),
        "AGGREGATE" => crate::commands::vector::ft_aggregate(ctx),
        _ => crate::commands::hybrid::ft_hybrid(ctx),
    };
    let total = started.elapsed();
    ctx.cmd = saved;
    let reply = reply?;
    if reply.first() == Some(&b'-') {
        return Some(reply);
    }

    let results = total_of(&reply);
    let processor = |kind: &str| -> Vec<u8> {
        resp_array(vec![
            resp_bulk_str("Type"),
            resp_bulk_str(kind),
            resp_bulk_str("Time"),
            resp_bulk_str("0"),
            resp_bulk_str("Results processed"),
            resp_integer(results),
        ])
    };
    let mut processors = vec![processor("Index")];
    if let Some(plan) = &plan {
        if plan.knn.is_some() {
            processors.push(processor("Metrics Applier"));
        }
        if plan.filter.has_text() {
            processors.push(processor("Scorer"));
        }
    }
    processors.push(processor("Sorter"));
    if kind != "SEARCH" {
        processors.push(processor("Pager/Limiter"));
    }
    processors.push(processor("Loader"));

    let iterators = match &plan {
        Some(plan) if plan.knn.is_some() => resp_array(vec![
            resp_bulk_str("Type"),
            resp_bulk_str("VECTOR"),
            resp_bulk_str("Time"),
            resp_bulk_str("0"),
            resp_bulk_str("Number of reading operations"),
            resp_integer(results),
            resp_bulk_str("Vector search mode"),
            resp_bulk_str(if matches!(plan.filter, Filter::MatchAll) {
                "STANDARD_KNN"
            } else {
                "HYBRID_ADHOC_BF"
            }),
        ]),
        Some(plan) => iterator_profile(&plan.filter, results),
        None => resp_array(vec![]),
    };

    let shard = resp_map_for(
        ctx.resp,
        vec![
            resp_bulk_str("Total profile time"),
            millis(total + parsing),
            resp_bulk_str("Parsing time"),
            millis(parsing),
            resp_bulk_str("Pipeline creation time"),
            resp_bulk_str("0"),
            resp_bulk_str("Total GIL time"),
            millis(total),
            resp_bulk_str("Warning"),
            resp_bulk_str("None"),
            resp_bulk_str("Iterators profile"),
            iterators,
            resp_bulk_str("Result processors profile"),
            resp_array(processors),
        ],
    );
    let profile = resp_map_for(
        ctx.resp,
        vec![
            resp_bulk_str("Shards"),
            resp_array(vec![shard]),
            resp_bulk_str("Coordinator"),
            resp_array(vec![]),
        ],
    );
    Some(if ctx.resp == 3 {
        resp_map_for(
            3,
            vec![
                resp_bulk_str("Results"),
                reply,
                resp_bulk_str("Profile"),
                profile,
            ],
        )
    } else {
        resp_array(vec![reply, profile])
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn word(w: &str) -> Filter {
        Filter::Text {
            fields: vec!["title".into(), "body".into()],
            term: TextMatch::Exact(w.into()),
        }
    }

    fn and(a: Filter, b: Filter) -> Filter {
        Filter::And(Box::new(a), Box::new(b))
    }

    fn or(a: Filter, b: Filter) -> Filter {
        Filter::Or(Box::new(a), Box::new(b))
    }

    /// The example of the `FT.EXPLAIN` page.
    #[test]
    fn a_plan_reads_as_redis_writes_one() {
        let date = |lo: f64, hi: f64| Filter::Numeric {
            field: "date".into(),
            lo,
            lo_exclusive: false,
            hi,
            hi_exclusive: false,
        };
        let query = filter::Query {
            filter: and(
                or(
                    and(word("foo"), word("bar")),
                    and(word("hello"), word("world")),
                ),
                or(date(100.0, 200.0), date(500.0, f64::INFINITY)),
            ),
            knn: None,
        };
        assert_eq!(
            explain_query(&query, 2).join("\n"),
            "INTERSECT {
  UNION {
    INTERSECT {
      foo
      bar
    }
    INTERSECT {
      hello
      world
    }
  }
  UNION {
    NUMERIC {100.000000 <= @date <= 200.000000}
    NUMERIC {500.000000 <= @date <= inf}
  }
}"
        );
    }

    #[test]
    fn three_words_are_one_intersection() {
        let mut out = Vec::new();
        explain_filter(&and(and(word("a"), word("b")), word("c")), 2, 0, &mut out);
        assert_eq!(out, ["INTERSECT {", "  a", "  b", "  c", "}"]);
    }

    #[test]
    fn a_cursor_hands_out_rows_until_there_are_none() {
        let rows: Vec<ShownRow> = (0..5)
            .map(|n| vec![("n".to_string(), n.to_string())])
            .collect();
        let first = open_cursor(
            2,
            "cursor-test-index",
            rows.clone(),
            2,
            Duration::from_secs(60),
            false,
        );
        // [[2, row, row], cursor]
        assert!(first.starts_with(b"*2\r\n*3\r\n:2\r\n"), "{first:?}");
        assert!(!first.ends_with(b":0\r\n"));

        // Everything in one go: no cursor is left open.
        let all = open_cursor(
            2,
            "cursor-test-index",
            rows,
            10,
            Duration::from_secs(60),
            false,
        );
        assert!(all.starts_with(b"*2\r\n*6\r\n:5\r\n"), "{all:?}");
        assert!(all.ends_with(b":0\r\n"));
    }

    #[test]
    fn the_total_is_read_off_a_reply() {
        assert_eq!(total_of(b"*3\r\n:12\r\n$1\r\na\r\n*0\r\n"), 12);
        assert_eq!(total_of(b"*0\r\n"), 0);
    }
}
