// Reusable aggregation pipeline runner.
//
// FT.AGGREGATE and FT.HYBRID both end with a sequence of pipeline clauses
// (LOAD / GROUPBY+REDUCE / APPLY / FILTER / SORTBY / LIMIT) operating on
// a Vec<Row>. The grammar is identical; only the row-source differs:
//   - FT.AGGREGATE: rows from a keyspace walk
//   - FT.HYBRID:    rows from fused-and-paged hybrid results, post-LOAD
//
// This module holds the parser-and-executor for the pipeline tail. Both
// commands build their initial Vec<Row> however they want, then call
// `run(rows, cmd, start_idx)`.
//
// The input row buffer is consumed (moved in) and the result is returned
// — same value-passing pattern the inline ft_aggregate code already uses.

pub type Row = ahash::AHashMap<String, String>;

// ── Numbers ─────────────────────────────────────────────────────────────────
//
// A row's values are strings, but a number the pipeline computed, or read
// from a NUMERIC field, is a number to the steps after it (they work on the
// double, not on the text of it), and is written as Redis writes a double:
// as an integer when it is one (and fits), else with 12 significant digits
// (`1.20376952202`). So a number is held in a row as this marker and its 16
// hex digits of bits, and written as text only where the reply is
// (`value_reply`), or where a step takes it as text (`text_of`).

pub(crate) const NUMBER: char = '\u{2}';

/// `x` as a row holds a number.
pub(crate) fn number_value(x: f64) -> String {
    format!("{NUMBER}{:016x}", x.to_bits())
}

/// The number a row value is, if it is held as one.
fn held_number(value: &str) -> Option<f64> {
    u64::from_str_radix(value.strip_prefix(NUMBER)?, 16)
        .ok()
        .map(f64::from_bits)
}

/// A row value as a number: one held as a number, or text that reads as one.
pub(crate) fn as_number(value: &str) -> Option<f64> {
    held_number(value).or_else(|| value.parse::<f64>().ok())
}

/// A row value as text: a number as Redis writes it, anything else as it is.
pub(crate) fn text_of(value: &str) -> std::borrow::Cow<'_, str> {
    match held_number(value) {
        Some(x) => std::borrow::Cow::Owned(redis_number_text(x)),
        None => std::borrow::Cow::Borrowed(value),
    }
}

/// A double as Redis writes it in an aggregate reply: an integer when it is
/// one that fits in 64 bits (`%lld`), else `%.12g`; `nan`, `-nan`, `inf` and
/// `-inf` for the rest, the sign of a NaN as the C library prints it.
pub(crate) fn redis_number_text(x: f64) -> String {
    if x.is_nan() {
        return if x.is_sign_negative() { "-nan" } else { "nan" }.to_string();
    }
    if x.is_infinite() {
        return if x > 0.0 { "inf" } else { "-inf" }.to_string();
    }
    if x == x.trunc() && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&x) {
        return format!("{}", x as i64);
    }
    // `%.12g`: 12 significant digits, in the style of exponent form when the
    // exponent is below -4 or 12 or more, else fixed; trailing zeros off.
    let scientific = format!("{x:.11e}");
    let (digits, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    let trim = |text: &str| -> String {
        if text.contains('.') {
            text.trim_end_matches('0').trim_end_matches('.').to_string()
        } else {
            text.to_string()
        }
    };
    if !(-4..12).contains(&exponent) {
        let sign = if exponent < 0 { '-' } else { '+' };
        format!("{}e{sign}{:02}", trim(digits), exponent.abs())
    } else {
        let decimals = (11 - exponent) as usize;
        trim(&format!("{x:.decimals$}"))
    }
}

// ── Values that are not strings ──────────────────────────────────────────────
//
// A row's values are strings, which is what nearly every stage of a
// pipeline makes and reads. Two reducers make something else: `TOLIST` and
// `RANDOM_SAMPLE` a list, `COLLECT` a list of documents. Those travel as a
// string too — this marker, then JSON — and are unpacked where the reply is
// written.

const NESTED: char = '\u{1}';

fn nested(value: &serde_json::Value) -> String {
    format!("{NESTED}{value}")
}

/// One value of a row, as the reply carries it: a string, or for a nested
/// value arrays within arrays (an entry of a `COLLECT` is name, value, name,
/// value … under RESP2 and a map under RESP3).
pub fn value_reply(value: &str, resp: u8) -> Vec<u8> {
    use crate::resp::write::{resp_array, resp_bulk_str, resp_map_for};
    fn write(value: &serde_json::Value, resp: u8) -> Vec<u8> {
        match value {
            serde_json::Value::String(s) => resp_bulk_str(s),
            serde_json::Value::Array(items) => {
                resp_array(items.iter().map(|item| write(item, resp)).collect())
            }
            serde_json::Value::Object(entries) => resp_map_for(
                resp,
                entries
                    .iter()
                    .flat_map(|(name, value)| [resp_bulk_str(name), write(value, resp)])
                    .collect(),
            ),
            other => resp_bulk_str(&other.to_string()),
        }
    }
    match value
        .strip_prefix(NESTED)
        .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
    {
        Some(value) => write(&value, resp),
        None => resp_bulk_str(&text_of(value)),
    }
}

/// Rows in the order `keys` gives: each a field and whether ascending.
/// Values that are both numbers compare as numbers.
fn sort_rows(rows: &mut [&Row], keys: &[(String, bool)]) {
    rows.sort_by(|a, b| compare_rows(a, b, keys));
}

fn compare_rows(a: &Row, b: &Row, keys: &[(String, bool)]) -> std::cmp::Ordering {
    for (field, ascending) in keys {
        let va = a.get(field).map(|s| s.as_str()).unwrap_or("");
        let vb = b.get(field).map(|s| s.as_str()).unwrap_or("");
        let ord = match (as_number(va), as_number(vb)) {
            (Some(na), Some(nb)) => na.total_cmp(&nb),
            _ => text_of(va).cmp(&text_of(vb)),
        };
        let ord = if *ascending { ord } else { ord.reverse() };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

/// `field [ASC | DESC] …` — sort keys, as `SORTBY` gives them.
fn parse_sort_keys(words: &[String]) -> Vec<(String, bool)> {
    let mut keys = Vec::new();
    let mut i = 0;
    while i < words.len() {
        let field = words[i].trim_start_matches('@').to_string();
        i += 1;
        let mut ascending = true;
        if let Some(direction) = words.get(i) {
            if direction.eq_ignore_ascii_case("ASC") {
                i += 1;
            } else if direction.eq_ignore_ascii_case("DESC") {
                ascending = false;
                i += 1;
            }
        }
        keys.push((field, ascending));
    }
    keys
}

/// The number at `cmd[i]`, or 0.
fn count_at(cmd: &[&[u8]], i: usize) -> usize {
    cmd.get(i)
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0)
}

/// Where the clause at `cmd[i]` ends: what [`run_within`] steps over for it,
/// without running it.
fn clause_end(cmd: &[&[u8]], i: usize) -> usize {
    let is = |at: usize, word: &str| {
        cmd.get(at)
            .is_some_and(|a| a.eq_ignore_ascii_case(word.as_bytes()))
    };
    let clause = String::from_utf8_lossy(cmd[i]).to_ascii_uppercase();
    match clause.as_str() {
        "LOAD" if cmd.get(i + 1).is_some_and(|a| *a == b"*") => i + 2,
        "LOAD" | "PARAMS" => i + 2 + count_at(cmd, i + 1),
        "GROUPBY" => {
            let mut at = i + 2 + count_at(cmd, i + 1);
            while is(at, "REDUCE") {
                at += 3 + count_at(cmd, at + 2);
                if is(at, "AS") {
                    at += 2;
                }
            }
            at
        }
        "APPLY" if is(i + 2, "AS") => i + 4,
        "APPLY" | "FILTER" | "DIALECT" | "SCORER" | "TIMEOUT" | "LANGUAGE" => i + 2,
        "SORTBY" => {
            let at = i + 2 + count_at(cmd, i + 1);
            if is(at, "MAX") { at + 2 } else { at }
        }
        "LIMIT" => i + 3,
        "WITHCURSOR" => {
            let mut at = i + 1;
            while is(at, "COUNT") || is(at, "MAXIDLE") {
                at += 2;
            }
            at
        }
        _ => i + 1,
    }
}

/// Where the option `name` (`TIMEOUT`, ...) is in the clauses from
/// `cmd[start]` on, walked clause by clause as [`run_within`] walks them: a word
/// inside a clause (an `APPLY`'s `AS timeout`, a parameter's name or value,
/// a `FILTER`'s expression) is not the option.
pub fn option_at(cmd: &[&[u8]], start: usize, name: &str) -> Option<usize> {
    let mut i = start;
    while i < cmd.len() {
        if cmd[i].eq_ignore_ascii_case(name.as_bytes()) {
            return Some(i);
        }
        i = clause_end(cmd, i);
    }
    None
}

/// Execute pipeline clauses starting at cmd[start_idx]. Returns the
/// transformed rows. On an unrecognised clause we advance past it (lenient
/// behaviour matching the existing ft_aggregate implementation), which
/// handles tokens like SCORER/DIALECT that ft_search consumes earlier.
///
/// It stops when `deadline` has passed; then the second value is
/// true. A step cut short keeps only what it finished (an `APPLY` the rows
/// it computed, a `FILTER` the rows it found good, a `GROUPBY` the groups
/// of the rows it read, or the groups it reduced), a sort with no time left
/// is left out, and so are the steps after it, but `LIMIT`, which is cheap
/// and bounds the reply. Under [`DEADLINE_EVERY`] rows no step reads the
/// clock.
///
/// [`DEADLINE_EVERY`]: crate::commands::search::DEADLINE_EVERY
pub fn run_within(
    rows: Vec<Row>,
    cmd: &[&[u8]],
    start_idx: usize,
    deadline: Option<std::time::Instant>,
) -> (Vec<Row>, bool) {
    let (rows, cut, _) = run_counted(rows, cmd, start_idx, deadline);
    (rows, cut)
}

/// [`run_within`], and the number of groups the last `GROUPBY` made, before
/// any `LIMIT` or `SORTBY ... MAX` cut them down: it is what Redis reports
/// as the total of a grouped aggregation, so that a client can page through
/// the groups. `None` when there is no `GROUPBY`, or a `FILTER` follows it
/// (Redis's total then depends on how many rows it read before the page was
/// full, and is not the same under RESP2 and RESP3).
pub fn run_counted(
    rows: Vec<Row>,
    cmd: &[&[u8]],
    start_idx: usize,
    deadline: Option<std::time::Instant>,
) -> (Vec<Row>, bool, Option<usize>) {
    run_from(rows, cmd, start_idx, deadline, false, None)
}

/// [`run_counted`] from `cmd[start_idx]` on, given what the steps before
/// it left: whether they were cut short (`cut`: only `LIMIT` runs then) and
/// the number of groups the last `GROUPBY` made. [`StreamGroups`] ran a
/// query's first `GROUPBY` with the scan; the steps after it start here.
pub fn run_from(
    mut rows: Vec<Row>,
    cmd: &[&[u8]],
    start_idx: usize,
    deadline: Option<std::time::Instant>,
    mut cut: bool,
    mut groups: Option<usize>,
) -> (Vec<Row>, bool, Option<usize>) {
    use crate::commands::search::{deadline_due, no_time_for};
    let mut i = start_idx;

    while i < cmd.len() {
        let clause = match std::str::from_utf8(cmd[i]) {
            Ok(s) => s.to_ascii_uppercase(),
            Err(_) => {
                i += 1;
                continue;
            }
        };
        if cut && clause != "LIMIT" {
            i = clause_end(cmd, i);
            continue;
        }

        match clause.as_str() {
            "LOAD" => {
                // LOAD count @field [@field ...] | LOAD *
                // Every field is in the row already; which of them the reply
                // shows is decided where the reply is written.
                i += 1;
                if cmd.get(i).is_some_and(|a| *a == b"*") {
                    i += 1;
                    continue;
                }
                let count = cmd
                    .get(i)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                i += 1 + count;
            }

            "GROUPBY" => {
                let (consumed, new_rows) = run_groupby(rows, cmd, i, deadline, &mut cut);
                rows = new_rows;
                groups = Some(rows.len());
                i = consumed;
            }

            "APPLY" => {
                i += 1;
                if let Some(expr_bytes) = cmd.get(i) {
                    if let Ok(expr_str) = std::str::from_utf8(expr_bytes)
                        && let Some(ast) = crate::commands::expr::parse(expr_str)
                    {
                        i += 1;
                        // Optional AS <alias>
                        let alias = if cmd
                            .get(i)
                            .and_then(|b| std::str::from_utf8(b).ok())
                            .map(|s| s.eq_ignore_ascii_case("AS"))
                            .unwrap_or(false)
                        {
                            i += 1;
                            let a = cmd
                                .get(i)
                                .and_then(|b| std::str::from_utf8(b).ok())
                                .unwrap_or("value")
                                .to_string();
                            i += 1;
                            a
                        } else {
                            // no alias keyword — synthesise from expression
                            expr_str.to_string()
                        };

                        let mut seen = 0u32;
                        let mut done = rows.len();
                        for (at, row) in rows.iter_mut().enumerate() {
                            if deadline_due(deadline, &mut seen) {
                                done = at;
                                cut = true;
                                break;
                            }
                            let value = crate::commands::expr::eval(&ast, row).row_string();
                            row.insert(alias.clone(), value);
                        }
                        rows.truncate(done);
                    } else {
                        i += 1;
                    }
                }
            }

            "FILTER" => {
                groups = None;
                i += 1;
                if let Some(expr_bytes) = cmd.get(i) {
                    if let Ok(expr_str) = std::str::from_utf8(expr_bytes)
                        && let Some(ast) = crate::commands::expr::parse(expr_str)
                    {
                        let mut seen = 0u32;
                        rows.retain(|row| {
                            if !cut && deadline_due(deadline, &mut seen) {
                                cut = true;
                            }
                            !cut && crate::commands::expr::eval(&ast, row).is_truthy()
                        });
                    }
                    i += 1;
                }
            }

            "SORTBY" => {
                i += 1;
                let nargs = cmd
                    .get(i)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                i += 1;

                let mut sort_keys: Vec<(String, bool)> = Vec::new();
                let end = i + nargs;
                while i < end && i < cmd.len() {
                    let field = match std::str::from_utf8(cmd[i]) {
                        Ok(s) => s.trim_start_matches('@').to_string(),
                        Err(_) => {
                            i += 1;
                            continue;
                        }
                    };
                    i += 1;

                    let ascending = if i < end && i < cmd.len() {
                        let dir = std::str::from_utf8(cmd[i])
                            .map(|s| s.to_ascii_uppercase())
                            .unwrap_or_default();
                        match dir.as_str() {
                            "ASC" => {
                                i += 1;
                                true
                            }
                            "DESC" => {
                                i += 1;
                                false
                            }
                            _ => true, // direction omitted, default ASC
                        }
                    } else {
                        true
                    };
                    sort_keys.push((field, ascending));
                }

                if no_time_for(deadline, rows.len()) {
                    cut = true;
                } else {
                    rows.sort_by(|a, b| compare_rows(a, b, &sort_keys));
                }

                // `MAX n`: only the first n are wanted.
                if cmd.get(i).is_some_and(|a| a.eq_ignore_ascii_case(b"MAX")) {
                    if let Some(max) = cmd
                        .get(i + 1)
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .and_then(|s| s.parse::<usize>().ok())
                    {
                        rows.truncate(max);
                    }
                    i += 2;
                }
            }

            "LIMIT" => {
                i += 1;
                let offset = cmd
                    .get(i)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                i += 1;
                let count = cmd
                    .get(i)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(usize::MAX);
                i += 1;
                rows = rows.into_iter().skip(offset).take(count).collect();
            }

            // `PARAMS n name value …` was read by the query parser.
            "PARAMS" => {
                let n = cmd
                    .get(i + 1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                i += 2 + n;
            }

            // Read where the command is: they say nothing to the pipeline.
            "DIALECT" | "SCORER" | "TIMEOUT" | "LANGUAGE" => {
                i += 2;
            }
            // `WITHCURSOR [COUNT n] [MAXIDLE ms]`
            "WITHCURSOR" => {
                i += 1;
                while cmd.get(i).is_some_and(|a| {
                    a.eq_ignore_ascii_case(b"COUNT") || a.eq_ignore_ascii_case(b"MAXIDLE")
                }) {
                    i += 2;
                }
            }

            _ => {
                i += 1;
            }
        }
    }

    (rows, cut, groups)
}

// ── GROUPBY + REDUCE ────────────────────────────────────────────────────────
//
// Extracted as its own function to keep `run` readable. Returns the
// post-clause cursor index and the new rows.

/// One `REDUCE` of a `GROUPBY`.
#[derive(Debug)]
struct Reducer {
    func: String,
    args: Vec<String>,
    alias: String,
}

impl Reducer {
    /// The field the reducer reads (its first argument), or "".
    fn field(&self) -> &str {
        self.args.first().map(String::as_str).unwrap_or("")
    }
}

/// The `GROUPBY` at `cmd[start]`: where it ends, the properties it groups
/// by, and its reducers.
fn parse_groupby(cmd: &[&[u8]], start: usize) -> (usize, Vec<String>, Vec<Reducer>) {
    let mut i = start + 1; // skip GROUPBY

    let nargs = cmd
        .get(i)
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);
    i += 1;

    let mut group_props: Vec<String> = Vec::new();
    for _ in 0..nargs {
        if let Some(prop) = cmd.get(i).and_then(|b| std::str::from_utf8(b).ok()) {
            group_props.push(prop.trim_start_matches('@').to_string());
        }
        i += 1;
    }

    let mut reducers: Vec<Reducer> = Vec::new();

    while i < cmd.len() {
        let next = match std::str::from_utf8(cmd[i]) {
            Ok(s) => s.to_ascii_uppercase(),
            Err(_) => break,
        };
        if next != "REDUCE" {
            break;
        }
        i += 1;

        let func = match cmd.get(i).and_then(|b| std::str::from_utf8(b).ok()) {
            Some(f) => f.to_ascii_uppercase(),
            None => break,
        };
        i += 1;

        let r_nargs = cmd
            .get(i)
            .and_then(|b| std::str::from_utf8(b).ok())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);
        i += 1;

        let mut args: Vec<String> = Vec::new();
        for _ in 0..r_nargs {
            if let Some(a) = cmd.get(i).and_then(|b| std::str::from_utf8(b).ok()) {
                args.push(a.trim_start_matches('@').to_string());
            }
            i += 1;
        }

        let alias = if cmd
            .get(i)
            .and_then(|b| std::str::from_utf8(b).ok())
            .map(|s| s.eq_ignore_ascii_case("AS"))
            .unwrap_or(false)
        {
            i += 1;
            let a = cmd
                .get(i)
                .and_then(|b| std::str::from_utf8(b).ok())
                .unwrap_or("value")
                .to_string();
            i += 1;
            a
        } else if args.is_empty() {
            func.to_lowercase()
        } else {
            format!("{}({})", func.to_lowercase(), args[0])
        };

        reducers.push(Reducer { func, args, alias });
    }
    (i, group_props, reducers)
}

/// The columns of the rows the `GROUPBY` at `cmd[start]` makes, in the order
/// Redis shows them: the properties it groups by, then its reducers' names.
pub(crate) fn group_columns(cmd: &[&[u8]], start: usize) -> Vec<String> {
    let (_, props, reducers) = parse_groupby(cmd, start);
    props
        .into_iter()
        .chain(reducers.into_iter().map(|reducer| reducer.alias))
        .collect()
}

fn run_groupby(
    rows: Vec<Row>,
    cmd: &[&[u8]],
    start: usize,
    deadline: Option<std::time::Instant>,
    cut: &mut bool,
) -> (usize, Vec<Row>) {
    use crate::commands::search::{DEADLINE_EVERY, deadline_due, past_deadline};
    let (i, group_props, reducers) = parse_groupby(cmd, start);

    // The rows of each group, groups in the order they first appear.
    let mut index: ahash::AHashMap<Vec<String>, usize> = ahash::AHashMap::new();
    let mut groups: Vec<(Vec<String>, Vec<&Row>)> = Vec::new();
    let mut seen = 0u32;
    for row in &rows {
        if deadline_due(deadline, &mut seen) {
            *cut = true;
            break;
        }
        let key: Vec<String> = group_props
            .iter()
            .map(|p| row.get(p).cloned().unwrap_or_default())
            .collect();
        let at = *index.entry(key.clone()).or_insert_with(|| {
            groups.push((key, Vec::new()));
            groups.len() - 1
        });
        groups[at].1.push(row);
    }

    let mut new_rows: Vec<Row> = Vec::with_capacity(groups.len());
    // A group's reducers cost as many rows as it has: the clock is read
    // every DEADLINE_EVERY rows reduced, however they are grouped. Grouping
    // cut short, the groups of the rows it read are reduced whole: that is
    // no more work than reading them was.
    let mut reduced = 0usize;
    for (group_key, members) in &groups {
        if !*cut && reduced >= DEADLINE_EVERY as usize {
            reduced = 0;
            if past_deadline(deadline) {
                *cut = true;
                break;
            }
        }
        reduced += members.len();
        let mut row: Row = ahash::AHashMap::new();
        for (prop, val) in group_props.iter().zip(group_key.iter()) {
            row.insert(prop.clone(), val.clone());
        }
        for reducer in &reducers {
            row.insert(
                reducer.alias.clone(),
                reduce(&reducer.func, &reducer.args, members),
            );
        }
        new_rows.push(row);
    }

    (i, new_rows)
}

// ── GROUPBY as the documents are read ───────────────────────────────────────
//
// Steps that begin with a GROUPBY whose reducers each keep a running value
// (COUNT, SUM, AVG, MIN, MAX, COUNT_DISTINCT) have no use for the rows of
// the documents: each document goes into its group as the scan reads it,
// and is never a row. A row is a map of every field of the document, each
// name and value a string of its own: on #761's 2,000,000 documents of four
// fields, some 14 million allocations and over a gigabyte held until the
// GROUPBY made its 1,000 groups of them.
//
// The groups, their order (that in which they first appear) and every
// reducer's value are those `run_groupby` makes of the same documents in
// the same order: the values a document is read for are those its row would
// hold, and sums are added up in the same order.

/// A reducer's value so far, in a group.
#[derive(Debug)]
enum Running {
    Count,
    /// The sum of the values that read as numbers, and how many there were:
    /// SUM, and AVG, which divides it by them, as `reduce` does.
    Sum(f64, usize),
    Avg(f64, usize),
    Min(Option<f64>),
    Max(Option<f64>),
    /// The distinct values, a missing one as "".
    Distinct(ahash::AHashSet<Box<str>>),
}

impl Running {
    /// What a reducer of `func` starts from, if it can run as documents
    /// are read.
    fn new(func: &str) -> Option<Self> {
        Some(match func {
            "COUNT" => Running::Count,
            // `Iterator::sum` of f64 starts from -0.0, as `reduce` adds up.
            "SUM" => Running::Sum(-0.0, 0),
            "AVG" => Running::Avg(-0.0, 0),
            "MIN" => Running::Min(None),
            "MAX" => Running::Max(None),
            "COUNT_DISTINCT" | "COUNT_DISTINCTISH" => Running::Distinct(Default::default()),
            _ => return None,
        })
    }

    #[inline]
    fn add(&mut self, value: Option<&str>) {
        let number = || value.and_then(as_number);
        match self {
            Running::Count => {}
            Running::Sum(sum, count) | Running::Avg(sum, count) => {
                if let Some(n) = number() {
                    *sum += n;
                    *count += 1;
                }
            }
            // As `Iterator::min_by` keeps the first of equal values and
            // `max_by` the last.
            Running::Min(least) => {
                if let Some(n) = number()
                    && least.is_none_or(|l| n.total_cmp(&l).is_lt())
                {
                    *least = Some(n);
                }
            }
            Running::Max(most) => {
                if let Some(n) = number()
                    && most.is_none_or(|m| n.total_cmp(&m).is_ge())
                {
                    *most = Some(n);
                }
            }
            Running::Distinct(seen) => {
                let value = value.unwrap_or("");
                if !seen.contains(value) {
                    seen.insert(value.into());
                }
            }
        }
    }

    /// The reducer's value for a group of `members` documents.
    fn value(self, members: usize) -> String {
        match self {
            Running::Count => members.to_string(),
            Running::Sum(sum, count) => number_value(sum_of(sum, count)),
            Running::Avg(sum, count) => number_value(average_of(sum, count)),
            Running::Min(n) => number_value(n.unwrap_or(f64::INFINITY)),
            Running::Max(n) => number_value(n.unwrap_or(f64::NEG_INFINITY)),
            Running::Distinct(seen) => seen.len().to_string(),
        }
    }
}

#[derive(Debug)]
struct StreamGroup {
    /// The group's value of each property it is grouped by.
    key: Box<[Box<str>]>,
    members: usize,
    running: Box<[Running]>,
}

/// A query's first `GROUPBY`, run as the documents are read: see above.
#[derive(Debug)]
pub struct StreamGroups {
    props: Vec<String>,
    reducers: Vec<Reducer>,
    /// Where the steps after the `GROUPBY` start.
    end: usize,
    hasher: ahash::RandomState,
    /// Group numbers, found by the hash of their key.
    index: hashbrown::HashTable<u32>,
    groups: Vec<StreamGroup>,
}

impl StreamGroups {
    /// The first `GROUPBY` of the steps from `cmd[start]` on, if it can run
    /// as documents are read: no step before it but those that leave the
    /// rows as they are (`LOAD`, which only says what the reply shows, and
    /// the query's options), and only reducers that keep a running value.
    pub fn plan(cmd: &[&[u8]], start: usize) -> Option<Self> {
        let mut i = start;
        while i < cmd.len() {
            let word = cmd[i].to_ascii_uppercase();
            match word.as_slice() {
                b"GROUPBY" => {
                    let (end, props, reducers) = parse_groupby(cmd, i);
                    // A GROUPBY of no property is one group of every row.
                    if props.is_empty() || reducers.iter().any(|r| Running::new(&r.func).is_none())
                    {
                        return None;
                    }
                    return Some(Self {
                        props,
                        reducers,
                        end,
                        hasher: ahash::RandomState::new(),
                        index: hashbrown::HashTable::new(),
                        groups: Vec::new(),
                    });
                }
                b"LOAD" | b"PARAMS" | b"DIALECT" | b"SCORER" | b"TIMEOUT" | b"LANGUAGE"
                | b"WITHCURSOR" => i = clause_end(cmd, i),
                b"APPLY" | b"FILTER" | b"SORTBY" | b"LIMIT" => return None,
                _ => i += 1,
            }
        }
        None
    }

    fn hash<'a>(hasher: &ahash::RandomState, parts: impl Iterator<Item = &'a str>) -> u64 {
        use std::hash::{BuildHasher, Hasher};
        let mut h = hasher.build_hasher();
        for part in parts {
            h.write_usize(part.len());
            h.write(part.as_bytes());
        }
        h.finish()
    }

    /// A document into its group: `value(name)` is what its row would hold
    /// under `name`.
    #[inline]
    pub fn add<'a>(&mut self, value: impl Fn(&str) -> Option<&'a str>) {
        let Self {
            props,
            reducers,
            hasher,
            index,
            groups,
            ..
        } = self;
        let of = |p: &String| value(p).unwrap_or("");
        let hash = Self::hash(hasher, props.iter().map(of));
        let found = index
            .find(hash, |&g| {
                groups[g as usize]
                    .key
                    .iter()
                    .zip(props.iter())
                    .all(|(k, p)| **k == *of(p))
            })
            .copied();
        let at = match found {
            Some(g) => g as usize,
            None => {
                groups.push(StreamGroup {
                    key: props.iter().map(|p| Box::from(of(p))).collect(),
                    members: 0,
                    running: reducers
                        .iter()
                        .map(|r| Running::new(&r.func).unwrap_or(Running::Count))
                        .collect(),
                });
                let g = (groups.len() - 1) as u32;
                index.insert_unique(hash, g, |&g| {
                    Self::hash(hasher, groups[g as usize].key.iter().map(|k| &**k))
                });
                g as usize
            }
        };
        let group = &mut groups[at];
        group.members += 1;
        for (reducer, running) in reducers.iter().zip(group.running.iter_mut()) {
            if matches!(running, Running::Count) {
                continue;
            }
            running.add(value(reducer.field()));
        }
    }

    /// Whether the groups are by a property of `numeric`, or a reducer
    /// counts the distinct values of one: that is by the number, not its
    /// text.
    pub fn goes_by_number(&self, numeric: &ahash::AHashSet<String>) -> bool {
        self.props.iter().any(|p| numeric.contains(p))
            || self
                .reducers
                .iter()
                .any(|r| r.func.starts_with("COUNT_DISTINCT") && numeric.contains(r.field()))
    }

    /// Where the steps after the `GROUPBY` start.
    pub fn end(&self) -> usize {
        self.end
    }

    /// The groups, as rows: as `run_groupby` makes them.
    pub fn finish(self) -> Vec<Row> {
        let Self {
            props,
            reducers,
            groups,
            ..
        } = self;
        groups
            .into_iter()
            .map(|group| {
                let mut row: Row = ahash::AHashMap::new();
                for (prop, value) in props.iter().zip(group.key.into_vec()) {
                    row.insert(prop.clone(), value.into_string());
                }
                for (reducer, running) in reducers.iter().zip(group.running.into_vec()) {
                    row.insert(reducer.alias.clone(), running.value(group.members));
                }
                row
            })
            .collect()
    }
}

/// SUM of `count` numbers that add up to `sum`: NaN when there were none.
fn sum_of(sum: f64, count: usize) -> f64 {
    if count == 0 { f64::NAN } else { sum }
}

/// AVG of `count` numbers that add up to `sum`: NaN when there were none.
fn average_of(sum: f64, count: usize) -> f64 {
    if count == 0 {
        f64::NAN
    } else {
        sum / count as f64
    }
}

/// One reducer over the rows of one group.
fn reduce(func: &str, args: &[String], members: &[&Row]) -> String {
    use serde_json::Value;

    let field = args.first().map(String::as_str).unwrap_or("");
    let values = || members.iter().filter_map(|row| row.get(field));
    let numbers = || values().filter_map(|v| as_number(v));
    // In the order they first appear.
    let distinct = || {
        let mut seen: ahash::AHashSet<&String> = ahash::AHashSet::new();
        values().filter(move |v| seen.insert(*v))
    };

    match func {
        "COUNT" => members.len().to_string(),
        // Of the values that are numbers: with none, SUM and AVG are NaN,
        // MIN is infinity and MAX minus infinity, as in Redis.
        "SUM" => {
            let all: Vec<f64> = numbers().collect();
            number_value(sum_of(all.iter().sum(), all.len()))
        }
        "AVG" => {
            let all: Vec<f64> = numbers().collect();
            number_value(average_of(all.iter().sum(), all.len()))
        }
        "MIN" => number_value(
            numbers()
                .min_by(|a, b| a.total_cmp(b))
                .unwrap_or(f64::INFINITY),
        ),
        "MAX" => number_value(
            numbers()
                .max_by(|a, b| a.total_cmp(b))
                .unwrap_or(f64::NEG_INFINITY),
        ),
        "COUNT_DISTINCT" | "COUNT_DISTINCTISH" => {
            let mut seen: ahash::AHashSet<&str> = ahash::AHashSet::new();
            members
                .iter()
                .filter(|row| seen.insert(row.get(field).map_or("", |v| v.as_str())))
                .count()
                .to_string()
        }
        "STDDEV" => {
            // Of a sample: n - 1 below the line.
            let all: Vec<f64> = numbers().collect();
            if all.len() < 2 {
                return number_value(0.0);
            }
            let mean = all.iter().sum::<f64>() / all.len() as f64;
            let variance =
                all.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (all.len() - 1) as f64;
            number_value(variance.sqrt())
        }
        // QUANTILE field q — the value at rank ceil(q * n) of the n numbers,
        // in order (the first for q 0).
        "QUANTILE" => {
            let mut all: Vec<f64> = numbers().collect();
            if all.is_empty() {
                return number_value(f64::NAN);
            }
            all.sort_by(|a, b| a.total_cmp(b));
            let q = args
                .get(1)
                .and_then(|q| q.parse::<f64>().ok())
                .unwrap_or(0.5)
                .clamp(0.0, 1.0);
            let rank = (q * all.len() as f64).ceil() as usize;
            number_value(all[rank.saturating_sub(1).min(all.len() - 1)])
        }
        // Every distinct value, as a list.
        "TOLIST" => nested(&Value::Array(
            distinct()
                .map(|v| Value::String(text_of(v).into_owned()))
                .collect(),
        )),
        // RANDOM_SAMPLE field n — n of the values, picked at random.
        "RANDOM_SAMPLE" => {
            use rand::seq::SliceRandom;
            let n = args
                .get(1)
                .and_then(|n| n.parse::<usize>().ok())
                .unwrap_or(1);
            let all: Vec<&String> = values().collect();
            nested(&Value::Array(
                all.choose_multiple(&mut rand::thread_rng(), n)
                    .map(|v| Value::String(text_of(v).into_owned()))
                    .collect(),
            ))
        }
        // FIRST_VALUE field [BY other [ASC | DESC]] — the group's first row's
        // value, or that of the row first by `other`.
        "FIRST_VALUE" => {
            let first = if args.get(1).is_some_and(|by| by.eq_ignore_ascii_case("BY")) {
                let keys = parse_sort_keys(&args[2..]);
                members
                    .iter()
                    .filter(|row| row.contains_key(field))
                    .min_by(|a, b| compare_rows(a, b, &keys))
            } else {
                members.iter().find(|row| row.contains_key(field))
            };
            first
                .and_then(|row| row.get(field))
                .cloned()
                .unwrap_or_default()
        }
        "COLLECT" => collect(args, members),
        _ => String::new(),
    }
}

/// `REDUCE COLLECT n FIELDS (* | m field …) [SORTBY k field [ASC | DESC] …]
/// [LIMIT offset count] [DISTINCT]` — the group's rows themselves, each as
/// the fields asked for: sorted, deduplicated and cut within the group.
/// (Redis 8.8. Whole documents per group in one query, where it used to take
/// a query per group.)
fn collect(args: &[String], members: &[&Row]) -> String {
    use serde_json::Value;

    // `None`: every field the pipeline has.
    let mut fields: Option<Vec<String>> = None;
    let mut order: Vec<(String, bool)> = Vec::new();
    let mut limit: Option<(usize, usize)> = None;
    let mut distinct = false;
    let number = |at: usize| args.get(at).and_then(|n| n.parse::<usize>().ok());
    let mut i = 0;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_str() {
            "FIELDS" if args.get(i + 1).is_some_and(|a| a == "*") => i += 2,
            "FIELDS" => {
                let n = number(i + 1).unwrap_or(0);
                let end = (i + 2).saturating_add(n).min(args.len());
                fields = Some(
                    args[(i + 2).min(end)..end]
                        .iter()
                        .map(|f| f.trim_start_matches('@').to_string())
                        .collect(),
                );
                i = end;
            }
            "SORTBY" => {
                let n = number(i + 1).unwrap_or(0);
                let end = (i + 2).saturating_add(n).min(args.len());
                order = parse_sort_keys(&args[(i + 2).min(end)..end]);
                i = end;
            }
            "LIMIT" => {
                limit = Some((
                    number(i + 1).unwrap_or(0),
                    number(i + 2).unwrap_or(usize::MAX),
                ));
                i += 3;
            }
            "DISTINCT" => {
                distinct = true;
                i += 1;
            }
            _ => i += 1,
        }
    }

    let mut rows: Vec<&Row> = members.to_vec();
    if !order.is_empty() {
        sort_rows(&mut rows, &order);
    }
    let entry = |row: &Row| -> Value {
        let mut entry = serde_json::Map::new();
        match &fields {
            Some(fields) => {
                for field in fields {
                    if let Some(value) = row.get(field) {
                        entry.insert(field.clone(), Value::String(text_of(value).into_owned()));
                    }
                }
            }
            None => {
                let mut all: Vec<(&String, &String)> = row
                    .iter()
                    .filter(|(name, _)| !name.starts_with("__"))
                    .collect();
                all.sort();
                for (name, value) in all {
                    entry.insert(name.clone(), Value::String(text_of(value).into_owned()));
                }
            }
        }
        Value::Object(entry)
    };
    let mut entries: Vec<Value> = rows.into_iter().map(entry).collect();
    if distinct {
        let mut seen: ahash::AHashSet<String> = ahash::AHashSet::new();
        entries.retain(|entry| seen.insert(entry.to_string()));
    }
    if let Some((offset, count)) = limit {
        entries = entries.into_iter().skip(offset).take(count).collect();
    }
    nested(&Value::Array(entries))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    /// A double as Redis 8.10.2 writes it in an FT.AGGREGATE reply (what
    /// `APPLY "<x>*1"` replied for each): an integer when it is one that fits
    /// in 64 bits, else 12 significant digits (`%.12g`).
    #[test]
    #[allow(clippy::approx_constant)]
    fn numbers_are_written_as_redis_writes_them() {
        for (x, text) in [
            (1e18, "1000000000000000000"),
            (9_223_372_036_854_775_807.0, "9.22337203685e+18"),
            (9.3e18, "9.3e+18"),
            (123_456_789_012_345_680.0, "123456789012345680"),
            (123_456_789.123_456_79, "123456789.123"),
            (0.000_099_999_999_999_999, "0.0001"),
            (99_999_999_999.95, "99999999999.9"),
            (999_999_999_999.5, "1e+12"),
            (999_999_999_999.4, "999999999999"),
            (1e11 * 1.5, "150000000000"),
            (0.000_01, "1e-05"),
            (12_345_678_901.234_5, "12345678901.2"),
            (1.5e300, "1.5e+300"),
            (5e-324, "4.94065645841e-324"),
            (2.0 / 3.0, "0.666666666667"),
            (0.1 * 3.0, "0.3"),
            (1.000_000_000_000_5, "1"),
            (1.0 / 7.0 * 1e15, "1.42857142857e+14"),
            (1.0 / 7.0 * 1e-5, "1.42857142857e-06"),
            (-7.25, "-7.25"),
            (-1e-7, "-1e-07"),
            (1.234567e-6, "1.234567e-06"),
            (1.203_769_522_023_45, "1.20376952202"),
            (3.141_592_653_589_79, "3.14159265359"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
            (f64::NAN, "nan"),
            // The sign of a NaN as the C library writes it (`0.0 / 0.0` and
            // the square root of a negative number have it set on x86).
            (f64::from_bits(0xfff8_0000_0000_0000), "-nan"),
        ] {
            assert_eq!(redis_number_text(x), text, "{x:e}");
        }
        // Held in a row, a number is its bits: the steps after it compute with
        // the double, and the reply writes the text.
        let held = number_value(2.0 / 3.0);
        assert_eq!(as_number(&held), Some(2.0 / 3.0));
        assert_eq!(text_of(&held), "0.666666666667");
        assert_eq!(text_of("1.50"), "1.50");
        assert_eq!(as_number("1.50"), Some(1.5));
    }

    /// REDUCE over values that are not numbers, as Redis 8.10.2 replies
    /// (a group of `abc` and `def`): NaN, NaN, infinity, minus infinity and
    /// 0 for SUM, AVG, MIN, MAX and STDDEV, and NaN for QUANTILE; and a
    /// quantile is the value at rank ceil(q n).
    #[test]
    fn reducers_work_on_the_values_that_are_numbers() {
        let row = |v: &str| -> Row { [("u".to_string(), v.to_string())].into_iter().collect() };
        let text = |members: Vec<Row>, func: &str, args: &[&str]| {
            let members: Vec<&Row> = members.iter().collect();
            let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
            text_of(&reduce(func, &args, &members)).into_owned()
        };
        let words = vec![row("abc"), row("def")];
        for (func, want) in [
            ("SUM", "nan"),
            ("AVG", "nan"),
            ("MIN", "inf"),
            ("MAX", "-inf"),
            ("STDDEV", "0"),
        ] {
            assert_eq!(text(words.clone(), func, &["u"]), want, "{func}");
        }
        assert_eq!(text(words, "QUANTILE", &["u", "0.5"]), "nan");
        // 2, x, 3.5: the average is of the two that are numbers.
        let mixed = vec![row("2"), row("x"), row("3.5")];
        assert_eq!(text(mixed.clone(), "SUM", &["u"]), "5.5");
        assert_eq!(text(mixed.clone(), "AVG", &["u"]), "2.75");
        assert_eq!(text(mixed, "STDDEV", &["u"]), "1.06066017178");
        // 1, 2, 10: rank ceil(q * 3).
        let three = vec![row("10"), row("1"), row("2")];
        for (q, want) in [
            ("0", "1"),
            ("0.1", "1"),
            ("0.5", "2"),
            ("0.75", "10"),
            ("1", "10"),
        ] {
            assert_eq!(text(three.clone(), "QUANTILE", &["u", q]), want, "q {q}");
        }
    }

    fn words(line: &str) -> Vec<&[u8]> {
        line.split(' ').map(str::as_bytes).collect()
    }

    /// An option is found where an option can be, not inside another
    /// clause that has a word of the same name.
    #[test]
    fn an_option_is_found_only_where_an_option_is() {
        let at = |line: &str| option_at(&words(line), 3, "TIMEOUT");
        assert_eq!(at("FT.AGGREGATE idx * TIMEOUT 5"), Some(3));
        assert_eq!(at("FT.AGGREGATE idx * LOAD 1 @t TIMEOUT 5"), Some(6));
        assert_eq!(at("FT.AGGREGATE idx * APPLY @t AS TIMEOUT"), None);
        assert_eq!(
            at("FT.AGGREGATE idx * APPLY @t AS timeout TIMEOUT 7"),
            Some(7)
        );
        assert_eq!(at("FT.AGGREGATE idx * PARAMS 2 TIMEOUT 5"), None);
        assert_eq!(at("FT.AGGREGATE idx * PARAMS 2 x TIMEOUT"), None);
        assert_eq!(at("FT.AGGREGATE idx * FILTER TIMEOUT"), None);
        assert_eq!(at("FT.AGGREGATE idx * LOAD 2 @a TIMEOUT"), None);
        assert_eq!(
            at(
                "FT.AGGREGATE idx * GROUPBY 1 @a REDUCE COUNT 0 AS TIMEOUT SORTBY 2 @TIMEOUT DESC MAX 3"
            ),
            None
        );
        assert_eq!(
            at("FT.AGGREGATE idx * GROUPBY 1 @a REDUCE SUM 1 @b AS s WITHCURSOR COUNT 2 TIMEOUT 9"),
            Some(15)
        );
        assert_eq!(
            at("FT.AGGREGATE idx * LIMIT 0 10 DIALECT 2 TIMEOUT 1"),
            Some(8)
        );
    }

    fn rows(n: usize) -> Vec<Row> {
        (0..n)
            .map(|i| {
                let mut row = Row::new();
                row.insert("n".to_string(), i.to_string());
                row.insert("g".to_string(), (i % 3).to_string());
                row
            })
            .collect()
    }

    /// A GROUPBY run as the documents are read makes the groups that of
    /// their rows does: the same groups, in the same order, with the same
    /// values, a missing or unreadable value included.
    #[test]
    fn groups_made_as_read_are_those_of_the_rows() {
        let docs: Vec<Row> = (0..5000)
            .map(|i: usize| {
                let mut row = Row::new();
                row.insert("__key".to_string(), format!("k:{i}"));
                if !i.is_multiple_of(7) {
                    row.insert("g".to_string(), (i % 13).to_string());
                }
                if !i.is_multiple_of(5) {
                    let v = match i % 4 {
                        0 => format!("{}.25", i % 17),
                        1 => "-0".to_string(),
                        2 => "x".to_string(),
                        _ => format!("-{}", i % 11),
                    };
                    row.insert("v".to_string(), v);
                }
                row.insert("h".to_string(), (i % 3).to_string());
                row
            })
            .collect();
        for line in [
            "FT.AGGREGATE idx * GROUPBY 1 @g REDUCE COUNT 0 AS n",
            "FT.AGGREGATE idx * LOAD 1 @v GROUPBY 2 @g @h REDUCE SUM 1 @v AS s REDUCE AVG 1 @v \
             REDUCE MIN 1 @v AS lo REDUCE MAX 1 @v AS hi REDUCE COUNT_DISTINCT 1 @v AS d \
             SORTBY 2 @s DESC LIMIT 0 7",
            "FT.AGGREGATE idx * GROUPBY 1 @v REDUCE COUNT_DISTINCTISH 1 @h REDUCE SUM 1 @nothing",
            "FT.AGGREGATE idx * GROUPBY 1 @__key REDUCE COUNT 0 AS n LIMIT 10 3",
        ] {
            let cmd = words(line);
            let (expect, cut, groups) = run_counted(docs.clone(), &cmd, 3, None);
            assert!(!cut);
            let mut stream = StreamGroups::plan(&cmd, 3).expect(line);
            for doc in &docs {
                stream.add(|name| doc.get(name).map(String::as_str));
            }
            let end = stream.end();
            let made = stream.finish();
            let n = made.len();
            let (got, cut, got_groups) = run_from(made, &cmd, end, None, false, Some(n));
            assert!(!cut);
            assert_eq!(got_groups, groups, "{line}");
            assert_eq!(got, expect, "{line}");
        }
        // Not as the documents are read: a step before the GROUPBY that
        // changes the rows, a reducer with no running value, no property.
        for line in [
            "FT.AGGREGATE idx * APPLY @v*2 AS w GROUPBY 1 @g REDUCE COUNT 0",
            "FT.AGGREGATE idx * FILTER @v>1 GROUPBY 1 @g REDUCE COUNT 0",
            "FT.AGGREGATE idx * GROUPBY 1 @g REDUCE TOLIST 1 @v",
            "FT.AGGREGATE idx * GROUPBY 0 REDUCE COUNT 0 AS n",
            "FT.AGGREGATE idx * SORTBY 1 @v",
        ] {
            assert!(StreamGroups::plan(&words(line), 3).is_none(), "{line}");
        }
    }

    /// Past the deadline a pipeline stops, keeping only rows it finished;
    /// under DEADLINE_EVERY rows, or with time left, it runs to the end.
    #[test]
    fn a_pipeline_keeps_the_deadline() {
        let gone = Some(Instant::now() - Duration::from_secs(1));
        let later = Some(Instant::now() + Duration::from_secs(60));
        let many = crate::commands::search::DEADLINE_EVERY as usize * 4;
        let cmd = words("FT.AGGREGATE idx * APPLY @n*2 AS d SORTBY 2 @n DESC LIMIT 0 5");

        let (out, cut) = run_within(rows(10), &cmd, 3, gone);
        assert!(!cut);
        assert_eq!(out.len(), 5);
        assert_eq!(text_of(&out[0]["d"]), "18");

        let (out, cut) = run_within(rows(many), &cmd, 3, later);
        assert!(!cut);
        assert_eq!(out[0]["n"], (many - 1).to_string());

        // APPLY stops at the first clock read: the rows it computed, then
        // LIMIT; SORTBY is left out.
        let (out, cut) = run_within(rows(many), &cmd, 3, gone);
        assert!(cut);
        assert_eq!(out.len(), 5);
        assert!(out.iter().all(|row| row.contains_key("d")));

        // FILTER keeps only rows it found good.
        let cmd = words("FT.AGGREGATE idx * FILTER @g==1");
        let (out, cut) = run_within(rows(many), &cmd, 3, gone);
        assert!(cut);
        assert!(out.len() < many / 3);
        assert!(out.iter().all(|row| row["g"] == "1"));

        // GROUPBY: the groups of the rows it read.
        let cmd = words("FT.AGGREGATE idx * GROUPBY 1 @g REDUCE COUNT 0 AS c");
        let (out, cut) = run_within(rows(many), &cmd, 3, gone);
        assert!(cut);
        let counted: usize = out
            .iter()
            .map(|row| row["c"].parse::<usize>().unwrap())
            .sum();
        assert!(counted < many);
        let (out, cut) = run_within(rows(many), &cmd, 3, later);
        assert!(!cut);
        let counted: usize = out
            .iter()
            .map(|row| row["c"].parse::<usize>().unwrap())
            .sum();
        assert_eq!(counted, many);
    }
}
