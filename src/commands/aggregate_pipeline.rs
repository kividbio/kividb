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
        None => resp_bulk_str(value),
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
        let ord = match (va.parse::<f64>(), vb.parse::<f64>()) {
            (Ok(na), Ok(nb)) => na.total_cmp(&nb),
            _ => va.cmp(vb),
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

/// Where the clause at `cmd[i]` ends: what [`run`] steps over for it,
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
/// `cmd[start]` on, walked clause by clause as [`run`] walks them: a word
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
pub fn run(rows: Vec<Row>, cmd: &[&[u8]], start_idx: usize) -> Vec<Row> {
    run_within(rows, cmd, start_idx, None).0
}

/// [`run`], stopping when `deadline` has passed; then the second value is
/// true. A step cut short keeps only what it finished (an `APPLY` the rows
/// it computed, a `FILTER` the rows it found good, a `GROUPBY` the groups
/// of the rows it read, or the groups it reduced), a sort with no time left
/// is left out, and so are the steps after it, but `LIMIT`, which is cheap
/// and bounds the reply. Under [`DEADLINE_EVERY`] rows no step reads the
/// clock.
///
/// [`DEADLINE_EVERY`]: crate::commands::search::DEADLINE_EVERY
pub fn run_within(
    mut rows: Vec<Row>,
    cmd: &[&[u8]],
    start_idx: usize,
    deadline: Option<std::time::Instant>,
) -> (Vec<Row>, bool) {
    use crate::commands::search::{deadline_due, no_time_for};
    let mut i = start_idx;
    let mut cut = false;

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
                            let value = crate::commands::expr::eval(&ast, row).to_resp_string();
                            row.insert(alias.clone(), value);
                        }
                        rows.truncate(done);
                    } else {
                        i += 1;
                    }
                }
            }

            "FILTER" => {
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

    (rows, cut)
}

// ── GROUPBY + REDUCE ────────────────────────────────────────────────────────
//
// Extracted as its own function to keep `run` readable. Returns the
// post-clause cursor index and the new rows.

fn run_groupby(
    rows: Vec<Row>,
    cmd: &[&[u8]],
    start: usize,
    deadline: Option<std::time::Instant>,
    cut: &mut bool,
) -> (usize, Vec<Row>) {
    use crate::commands::search::{DEADLINE_EVERY, deadline_due, past_deadline};
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

    #[derive(Debug)]
    struct Reducer {
        func: String,
        args: Vec<String>,
        alias: String,
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

/// One reducer over the rows of one group.
fn reduce(func: &str, args: &[String], members: &[&Row]) -> String {
    use crate::commands::vector::format_f64;
    use serde_json::Value;

    let field = args.first().map(String::as_str).unwrap_or("");
    let values = || members.iter().filter_map(|row| row.get(field));
    let numbers = || values().filter_map(|v| v.parse::<f64>().ok());
    // In the order they first appear.
    let distinct = || {
        let mut seen: ahash::AHashSet<&String> = ahash::AHashSet::new();
        values().filter(move |v| seen.insert(*v))
    };

    match func {
        "COUNT" => members.len().to_string(),
        "SUM" => format_f64(numbers().sum()),
        "AVG" => {
            if members.is_empty() {
                "0".to_string()
            } else {
                format_f64(numbers().sum::<f64>() / members.len() as f64)
            }
        }
        "MIN" => numbers()
            .min_by(|a, b| a.total_cmp(b))
            .map_or("0".to_string(), format_f64),
        "MAX" => numbers()
            .max_by(|a, b| a.total_cmp(b))
            .map_or("0".to_string(), format_f64),
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
                return "0".to_string();
            }
            let mean = all.iter().sum::<f64>() / all.len() as f64;
            let variance =
                all.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (all.len() - 1) as f64;
            format_f64(variance.sqrt())
        }
        // QUANTILE field q — the value a fraction q of the way up.
        "QUANTILE" => {
            let mut all: Vec<f64> = numbers().collect();
            if all.is_empty() {
                return "0".to_string();
            }
            all.sort_by(|a, b| a.total_cmp(b));
            let q = args
                .get(1)
                .and_then(|q| q.parse::<f64>().ok())
                .unwrap_or(0.5)
                .clamp(0.0, 1.0);
            format_f64(all[(q * (all.len() - 1) as f64).round() as usize])
        }
        // Every distinct value, as a list.
        "TOLIST" => nested(&Value::Array(
            distinct().map(|v| Value::String(v.clone())).collect(),
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
                    .map(|v| Value::String(v.to_string()))
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
                        entry.insert(field.clone(), Value::String(value.clone()));
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
                    entry.insert(name.clone(), Value::String(value.clone()));
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
        assert_eq!(out[0]["d"], "18");

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
