// FT.HYBRID — Redis 8.4+ hybrid text + vector search.
//
//   FT.HYBRID index
//     SEARCH query [SCORER name] [YIELD_SCORE_AS name]
//     VSIM @field $param
//       [KNN count K k [EF_RUNTIME ef] [SHARD_K_RATIO r]
//        | RANGE count RADIUS r [EPSILON e]]
//       [FILTER filter] [YIELD_SCORE_AS name]
//     [COMBINE RRF count [CONSTANT c] [WINDOW w] [YIELD_SCORE_AS name]
//      | COMBINE LINEAR count [ALPHA a BETA b] [WINDOW w] [YIELD_SCORE_AS name]]
//     [LIMIT offset num] [SORTBY nargs @field [ASC|DESC] ... | NOSORT]
//     [PARAMS nargs name value ...] [TIMEOUT ms] [FORMAT fmt]
//     [LOAD count @field [AS name] ... | LOAD *]
//     [GROUPBY ... [REDUCE ...]] [APPLY expr AS name] [FILTER expr]
//
// Two ranked lists, one from the text query (scored by `SCORER`, as FT.SEARCH
// scores it) and one from the vector query, fused into one:
//
//   RRF     score(d) = Σ 1 / (CONSTANT + rank_i(d)), over the lists d is in.
//   LINEAR  score(d) = ALPHA · text(d) + BETA · vector(d), where text is the
//           scorer's own score and vector the similarity Redis yields for
//           the distance (see `similarity`). A list d is not in adds 0.
//
// Each list is cut to WINDOW first. The reply is Redis's:
//
//   total_results N  results [row ...]  warnings [...]  execution_time ms
//
// a row being `__key` and `__score` (the fused score), or the fields LOADed,
// with the scores named by `YIELD_SCORE_AS`. RESP3 makes the reply and each
// row a map.

use std::time::Instant;

use ahash::AHashMap;

use crate::commands::aggregate_pipeline::Row;
use crate::commands::ctx::Ctx;
use crate::resp::write::{
    resp_array, resp_bulk, resp_bulk_str, resp_double_resp3, resp_error, resp_integer, resp_map_for,
};
use crate::vector::score::Scorer;
use crate::vector::{DistanceMetric, FieldType, KnnResult, LiveIndex, VectorAlgo};

/// The protocol a reply is written in: a script's own (`redis.setresp`)
/// inside one, the client's otherwise.
#[inline]
fn reply_resp(ctx: &Ctx) -> u8 {
    crate::commands::handler::effective_resp_for_reply(ctx)
}

// ── Parsed command ──────────────────────────────────────────────────────────

#[derive(Debug, PartialEq)]
enum VectorMethod {
    Knn { k: usize, ef_runtime: Option<usize> },
    Range { radius: f64, epsilon: Option<f64> },
}

#[derive(Debug, PartialEq)]
enum Fusion {
    Rrf {
        window: usize,
        constant: f64,
    },
    Linear {
        alpha: f64,
        beta: f64,
        window: usize,
    },
}

#[derive(Debug)]
struct HybridQuery {
    index_name: String,
    search_query: String,
    /// `SCORER`, as given: checked once the command has parsed, as Redis
    /// does.
    scorer: Option<String>,
    text_yield: Option<String>,
    vsim_field: String,
    vsim_param: String,
    method: VectorMethod,
    /// `FILTER` of the `VSIM` clause: which documents the vector query may
    /// return. A `FILTER` after the clause is a pipeline step.
    vsim_filter: Option<String>,
    vsim_yield: Option<String>,
    fusion: Fusion,
    hybrid_yield: Option<String>,
    limit_offset: usize,
    limit_count: usize,
    nosort: bool,
    timeout_ms: Option<u64>,
    params: AHashMap<String, Vec<u8>>,
    /// The pipeline steps, in order: `LOAD`, `APPLY`, `FILTER`, `GROUPBY`
    /// with its `REDUCE`s, and `SORTBY`.
    tail: Vec<Vec<u8>>,
}

fn word(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn arg<'a>(cmd: &[&'a [u8]], i: usize, name: &str) -> Result<&'a [u8], String> {
    cmd.get(i)
        .copied()
        .ok_or_else(|| format!("SEARCH_PARSE_ARGS {name}: Missing argument"))
}

fn number<T: std::str::FromStr>(cmd: &[&[u8]], i: usize, name: &str) -> Result<T, String> {
    std::str::from_utf8(arg(cmd, i, name)?)
        .ok()
        .and_then(|s| s.parse::<T>().ok())
        .ok_or_else(|| {
            format!("SEARCH_PARSE_ARGS {name}: Could not convert argument to expected type")
        })
}

/// `CONSTANT`, `ALPHA` or `BETA`: a number, refused when it is infinite or
/// too large for a double, as Redis 8.10.2 refuses it. Redis takes `nan`,
/// zero and negative numbers here, and so does this.
fn weight(cmd: &[&[u8]], i: usize, name: &str) -> Result<f64, String> {
    let value: f64 = number(cmd, i, name)?;
    if value.is_infinite() {
        return Err(format!(
            "SEARCH_PARSE_ARGS {name}: Could not convert argument to expected type"
        ));
    }
    Ok(value)
}

/// The `count` of a `KNN` / `RANGE` / `COMBINE` block: its arguments go in
/// name–value pairs.
fn pair_count(cmd: &[&[u8]], i: usize, what: &str) -> Result<usize, String> {
    let n: usize = std::str::from_utf8(arg(cmd, i, what)?)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| format!("SEARCH_SYNTAX Invalid argument count for {what}"))?;
    Ok(n)
}

fn is(b: &[u8], w: &str) -> bool {
    b.eq_ignore_ascii_case(w.as_bytes())
}

fn parse(cmd: &[&[u8]]) -> Result<HybridQuery, String> {
    if cmd.len() < 3 {
        return Err("ERR wrong number of arguments for 'FT.HYBRID' command".into());
    }
    let mut q = HybridQuery {
        index_name: word(cmd[1]),
        search_query: String::new(),
        scorer: None,
        text_yield: None,
        vsim_field: String::new(),
        vsim_param: String::new(),
        method: VectorMethod::Knn {
            k: 10,
            ef_runtime: None,
        },
        vsim_filter: None,
        vsim_yield: None,
        fusion: Fusion::Rrf {
            window: 20,
            constant: 60.0,
        },
        hybrid_yield: None,
        limit_offset: 0,
        limit_count: 10,
        nosort: false,
        timeout_ms: None,
        params: AHashMap::new(),
        tail: Vec::new(),
    };

    // ── SEARCH query [SCORER name] [YIELD_SCORE_AS name] ────────────────────
    if !is(cmd[2], "SEARCH") {
        return Err("SEARCH_SYNTAX Invalid subqueries count: expected an unsigned integer".into());
    }
    q.search_query = word(arg(cmd, 3, "SEARCH")?);
    let mut i = 4;
    loop {
        let Some(&tok) = cmd.get(i) else {
            return Err("SEARCH_SYNTAX VSIM argument is missing".into());
        };
        if is(tok, "SCORER") {
            q.scorer = Some(word(arg(cmd, i + 1, "SCORER")?));
            i += 2;
        } else if is(tok, "YIELD_SCORE_AS") {
            q.text_yield = Some(word(arg(cmd, i + 1, "YIELD_SCORE_AS")?));
            i += 2;
        } else if is(tok, "VSIM") {
            break;
        } else {
            return Err(format!(
                "SEARCH_ARG_UNRECOGNIZED Unknown argument `{}` in SEARCH",
                word(tok)
            ));
        }
    }

    // ── VSIM @field $param ──────────────────────────────────────────────────
    let field = word(arg(cmd, i + 1, "VSIM")?);
    q.vsim_field = field.strip_prefix('@').unwrap_or(&field).to_string();
    let param = word(arg(cmd, i + 2, "VSIM")?);
    q.vsim_param = param
        .strip_prefix('$')
        .ok_or_else(|| {
            "SEARCH_SYNTAX VSIM vector argument must be a parameter, as $name".to_string()
        })?
        .to_string();
    i += 3;

    // [KNN count ... | RANGE count ...]
    if let Some(&tok) = cmd.get(i)
        && (is(tok, "KNN") || is(tok, "RANGE"))
    {
        let knn = is(tok, "KNN");
        let what = if knn { "KNN" } else { "RANGE" };
        let n = pair_count(cmd, i + 1, what)?;
        if n == 0 || !n.is_multiple_of(2) {
            return Err(format!(
                "SEARCH_SYNTAX Invalid argument count: {n} (must be a positive even number for key/value pairs)"
            ));
        }
        let end = (i + 2).saturating_add(n);
        if end > cmd.len() {
            return Err(format!("SEARCH_SYNTAX Missing arguments for {what}"));
        }
        let mut k = 10usize;
        let mut ef_runtime = None;
        let mut radius = None;
        let mut epsilon = None;
        let mut at = i + 2;
        while at < end {
            let name = cmd[at].to_ascii_uppercase();
            match (knn, name.as_slice()) {
                (true, b"K") => {
                    k = std::str::from_utf8(cmd[at + 1])
                        .ok()
                        .and_then(|s| s.parse::<usize>().ok())
                        .filter(|k| *k > 0)
                        .ok_or_else(|| "SEARCH_SYNTAX Invalid K value".to_string())?;
                }
                (true, b"EF_RUNTIME") => {
                    ef_runtime = Some(number::<usize>(cmd, at + 1, "EF_RUNTIME")?);
                }
                (true, b"SHARD_K_RATIO") => {
                    // How many of `K` each shard returns: one node here.
                    number::<f64>(cmd, at + 1, "SHARD_K_RATIO")?;
                }
                (false, b"RADIUS") => radius = Some(number::<f64>(cmd, at + 1, "RADIUS")?),
                (false, b"EPSILON") => epsilon = Some(number::<f64>(cmd, at + 1, "EPSILON")?),
                _ => {
                    return Err(format!(
                        "SEARCH_ARG_UNRECOGNIZED Unknown argument `{}` in {what}",
                        word(cmd[at])
                    ));
                }
            }
            at += 2;
        }
        q.method = if knn {
            VectorMethod::Knn { k, ef_runtime }
        } else {
            VectorMethod::Range {
                radius: radius
                    .ok_or_else(|| "SEARCH_SYNTAX Missing required argument RADIUS".to_string())?,
                epsilon,
            }
        };
        i = end;
    }
    // [FILTER filter] [YIELD_SCORE_AS name]
    if cmd.get(i).is_some_and(|t| is(t, "FILTER")) {
        q.vsim_filter = Some(word(arg(cmd, i + 1, "FILTER")?));
        i += 2;
    }
    if cmd.get(i).is_some_and(|t| is(t, "YIELD_SCORE_AS")) {
        q.vsim_yield = Some(word(arg(cmd, i + 1, "YIELD_SCORE_AS")?));
        i += 2;
    }

    // ── The rest, in any order ──────────────────────────────────────────────
    while i < cmd.len() {
        let tok = cmd[i].to_ascii_uppercase();
        match tok.as_slice() {
            b"COMBINE" => i = parse_combine(cmd, i, &mut q)?,
            b"LIMIT" => {
                q.limit_offset = number(cmd, i + 1, "LIMIT")?;
                q.limit_count = number(cmd, i + 2, "LIMIT")?;
                i += 3;
            }
            b"NOSORT" => {
                q.nosort = true;
                i += 1;
            }
            b"PARAMS" => {
                let n: usize = number(cmd, i + 1, "PARAMS")?;
                if !n.is_multiple_of(2) {
                    return Err("SEARCH_PARSE_ARGS PARAMS: Bad arguments, expected an even number of name-value pairs".into());
                }
                if n > cmd.len() - (i + 2).min(cmd.len()) {
                    return Err("SEARCH_PARSE_ARGS PARAMS: Missing argument".into());
                }
                for pair in cmd[i + 2..i + 2 + n].chunks(2) {
                    q.params.insert(word(pair[0]), pair[1].to_vec());
                }
                i += 2 + n;
            }
            b"TIMEOUT" => {
                q.timeout_ms = Some(number(cmd, i + 1, "TIMEOUT")?);
                i += 2;
            }
            b"FORMAT" => {
                // `STRING` or `EXPAND`: values are strings either way here.
                arg(cmd, i + 1, "FORMAT")?;
                i += 2;
            }
            b"DIALECT" => {
                return Err("SEARCH_PARSE_ARGS DIALECT is not supported in FT.HYBRID or any of its subqueries. Please check the documentation on search-default-dialect configuration.".into());
            }
            b"LOAD" => {
                let end = if cmd.get(i + 1).is_some_and(|a| *a == b"*") {
                    i + 2
                } else {
                    let n: usize = number(cmd, i + 1, "LOAD")?;
                    let end = (i + 2).saturating_add(n);
                    if end > cmd.len() {
                        return Err("SEARCH_PARSE_ARGS LOAD: Missing argument".into());
                    }
                    let mut at = i + 2;
                    while at < end {
                        let name = cmd[at];
                        if !name.starts_with(b"@") && !name.starts_with(b"$") {
                            return Err(format!(
                                "SEARCH_PARSE_ARGS Missing prefix: name requires '@' prefix, JSON path require '$' prefix, got: {} in LOAD",
                                word(name)
                            ));
                        }
                        at += if cmd.get(at + 1).is_some_and(|a| is(a, "AS")) && at + 2 < end {
                            3
                        } else {
                            1
                        };
                    }
                    end
                };
                q.tail.extend(cmd[i..end].iter().map(|a| a.to_vec()));
                i = end;
            }
            b"SORTBY" | b"APPLY" | b"FILTER" | b"GROUPBY" => {
                let end = step_end(cmd, i)?;
                q.tail.extend(cmd[i..end].iter().map(|a| a.to_vec()));
                i = end;
            }
            _ => {
                return Err(format!(
                    "SEARCH_PARSE_ARGS {}: Unknown argument",
                    word(cmd[i])
                ));
            }
        }
    }

    if !q.params.contains_key(&q.vsim_param) {
        return Err(format!(
            "SEARCH_PARAM_NOT_FOUND Parameter not found `{}`",
            q.vsim_param
        ));
    }
    Ok(q)
}

/// Where the step after fusion at `cmd[i]` (`LOAD`, `SORTBY`, `APPLY`,
/// `FILTER`, `GROUPBY` with its `REDUCE`s) ends; an error if the command ends first.
/// Its counts are the client's: added without overflow, each step forward.
fn step_end(cmd: &[&[u8]], i: usize) -> Result<usize, String> {
    let count = |at: usize| -> usize {
        cmd.get(at)
            .and_then(|a| std::str::from_utf8(a).ok())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0)
    };
    let at_word = |at: usize, w: &str| cmd.get(at).is_some_and(|a| is(a, w));
    let end = if at_word(i, "LOAD") {
        if cmd.get(i + 1).is_some_and(|a| *a == b"*") {
            i + 2
        } else {
            (i + 2).saturating_add(count(i + 1))
        }
    } else if at_word(i, "SORTBY") {
        let end = (i + 2).saturating_add(count(i + 1));
        if at_word(end, "MAX") { end + 2 } else { end }
    } else if at_word(i, "APPLY") {
        if at_word(i + 2, "AS") { i + 4 } else { i + 2 }
    } else if at_word(i, "GROUPBY") {
        let mut at = (i + 2).saturating_add(count(i + 1));
        while at < cmd.len() && at_word(at, "REDUCE") {
            at = (at + 3).saturating_add(count(at + 2));
            if at_word(at, "AS") {
                at += 2;
            }
        }
        at
    } else {
        // FILTER expr
        i + 2
    };
    if end > cmd.len() {
        return Err(format!(
            "SEARCH_PARSE_ARGS {}: Missing argument",
            word(cmd[i])
        ));
    }
    Ok(end)
}

/// `COMBINE RRF|LINEAR count ... [YIELD_SCORE_AS name]` at `cmd[i]`; where
/// the clause ends.
fn parse_combine(cmd: &[&[u8]], i: usize, q: &mut HybridQuery) -> Result<usize, String> {
    let method = arg(cmd, i + 1, "COMBINE")?.to_ascii_uppercase();
    let rrf = match method.as_slice() {
        b"RRF" => true,
        b"LINEAR" => false,
        _ => return Err("SEARCH_PARSE_ARGS COMBINE: Invalid value for argument".into()),
    };
    let what = if rrf { "RRF" } else { "LINEAR" };
    let n: usize = number(cmd, i + 2, what)?;
    if !n.is_multiple_of(2) {
        return Err(format!(
            "SEARCH_PARSE_ARGS {what} expects pairs of key value arguments, argument count must be an even number"
        ));
    }
    let end = (i + 3).saturating_add(n);
    if end > cmd.len() {
        return Err(format!("SEARCH_PARSE_ARGS {what}: Missing argument"));
    }
    let mut window = 20usize;
    let mut constant = 60.0f64;
    let mut alpha = None;
    let mut beta = None;
    let mut at = i + 3;
    while at < end {
        let name = cmd[at].to_ascii_uppercase();
        match (rrf, name.as_slice()) {
            (_, b"WINDOW") => {
                window = number(cmd, at + 1, "WINDOW")?;
                if window == 0 {
                    return Err("SEARCH_PARSE_ARGS WINDOW: Value below minimum".into());
                }
            }
            (_, b"YIELD_SCORE_AS") => q.hybrid_yield = Some(word(cmd[at + 1])),
            (true, b"CONSTANT") => constant = weight(cmd, at + 1, "CONSTANT")?,
            (false, b"ALPHA") => alpha = Some(weight(cmd, at + 1, "ALPHA")?),
            (false, b"BETA") => beta = Some(weight(cmd, at + 1, "BETA")?),
            _ => {
                return Err(format!(
                    "SEARCH_PARSE_ARGS {}: Unknown argument",
                    word(cmd[at])
                ));
            }
        }
        at += 2;
    }
    q.fusion = if rrf {
        Fusion::Rrf { window, constant }
    } else {
        let (alpha, beta) = match (alpha, beta) {
            (Some(a), Some(b)) => (a, b),
            (None, None) => (0.3, 0.7),
            (Some(_), None) => return Err("SEARCH_SYNTAX Missing value for BETA".into()),
            (None, Some(_)) => return Err("SEARCH_SYNTAX Missing value for ALPHA".into()),
        };
        Fusion::Linear {
            alpha,
            beta,
            window,
        }
    };
    let mut end = end;
    if cmd.get(end).is_some_and(|t| is(t, "YIELD_SCORE_AS")) {
        q.hybrid_yield = Some(word(arg(cmd, end + 1, "YIELD_SCORE_AS")?));
        end += 2;
    }
    Ok(end)
}

/// `SCORER`'s name as Redis takes it: one of the scorers, in capitals.
fn scorer_named(name: &str) -> Result<Scorer, String> {
    Scorer::parse(name)
        .filter(|_| !name.bytes().any(|b| b.is_ascii_lowercase()))
        .ok_or_else(|| format!("SEARCH_QUERY_BAD No such scorer {name}"))
}

// ── Scores ──────────────────────────────────────────────────────────────────

/// The similarity Redis yields for a vector `distance`, and fuses with
/// LINEAR: higher is closer.
fn similarity(metric: &DistanceMetric, distance: f64) -> f64 {
    match metric {
        // The squared Euclidean distance, 0 to ∞.
        DistanceMetric::L2 => 1.0 / (1.0 + distance),
        // 1 - cos, 0 to 2.
        DistanceMetric::Cosine => 1.0 - distance / 2.0,
        // 1 - dot. Redis 8.10.2 yields (1 + distance) / 2 here.
        DistanceMetric::InnerProduct => (1.0 + distance) / 2.0,
    }
}

/// A score as Redis writes it in a row: `%.12g`.
fn score_text(value: f64) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    if !value.is_finite() {
        return if value.is_nan() {
            "nan".to_string()
        } else if value > 0.0 {
            "inf".to_string()
        } else {
            "-inf".to_string()
        };
    }
    let scientific = format!("{value:.11e}");
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    let trim = |s: &str| -> String {
        if s.contains('.') {
            s.trim_end_matches('0').trim_end_matches('.').to_string()
        } else {
            s.to_string()
        }
    };
    if !(-4..12).contains(&exponent) {
        let sign = if exponent < 0 { '-' } else { '+' };
        format!("{}e{sign}{:02}", trim(mantissa), exponent.abs())
    } else {
        trim(&format!("{:.*}", (11 - exponent) as usize, value))
    }
}

/// Best first; a tie by key, so a reply is the same each time.
fn rank(list: &mut [(String, f64)], higher_first: bool) {
    list.sort_unstable_by(|a, b| {
        let by_score = if higher_first {
            b.1.total_cmp(&a.1)
        } else {
            a.1.total_cmp(&b.1)
        };
        by_score.then_with(|| a.0.cmp(&b.0))
    });
}

/// RRF over the two windows: each document's Σ 1 / (constant + rank).
fn rrf_fuse(text: &[(String, f64)], vsim: &[(String, f64)], constant: f64) -> Vec<(String, f64)> {
    let mut scores: AHashMap<&str, f64> = AHashMap::new();
    for list in [text, vsim] {
        for (at, (key, _)) in list.iter().enumerate() {
            *scores.entry(key).or_insert(0.0) += 1.0 / (constant + (at + 1) as f64);
        }
    }
    scores
        .into_iter()
        .map(|(k, s)| (k.to_string(), s))
        .collect()
}

/// LINEAR over the two windows: α · text score + β · vector similarity.
fn linear_fuse(
    text: &[(String, f64)],
    vsim: &[(String, f64)],
    alpha: f64,
    beta: f64,
) -> Vec<(String, f64)> {
    let mut scores: AHashMap<&str, f64> = AHashMap::new();
    for (key, score) in text {
        *scores.entry(key).or_insert(0.0) += alpha * score;
    }
    for (key, similarity) in vsim {
        *scores.entry(key).or_insert(0.0) += beta * similarity;
    }
    scores
        .into_iter()
        .map(|(k, s)| (k.to_string(), s))
        .collect()
}

// ── The two queries ─────────────────────────────────────────────────────────

/// The text query, scored by `scorer`: (key, score), best first.
fn text_ranked(
    ctx: &Ctx,
    def: &crate::vector::IndexDef,
    query: &str,
    params: &AHashMap<String, Vec<u8>>,
    scorer: Scorer,
    deadline: Option<Instant>,
) -> Result<(Vec<(String, f64)>, bool), String> {
    let parsed = crate::vector::filter::parse_query(query, params, def)?;
    let found = crate::commands::search::filter_search_scored(
        ctx,
        def,
        &parsed.filter,
        None,
        deadline,
        scorer,
    );
    let mut out: Vec<(String, f64)> = found
        .matches
        .into_iter()
        .map(|found| (found.key, found.score))
        .collect();
    rank(&mut out, true);
    Ok((out, found.timed_out))
}

/// The vector query: (key, distance), nearest first.
#[allow(clippy::too_many_arguments)]
fn vsim_ranked(
    ctx: &Ctx,
    def: &crate::vector::IndexDef,
    field: &crate::vector::SchemaField,
    query_vec: &[f32],
    method: &VectorMethod,
    filter: Option<&str>,
    params: &AHashMap<String, Vec<u8>>,
    deadline: Option<Instant>,
) -> Result<(Vec<(String, f64)>, bool), String> {
    let FieldType::Vector { metric, algo, .. } = &field.field_type else {
        return Err(format!(
            "SEARCH_SYNTAX Expected a VECTOR field `{}`",
            field.attribute()
        ));
    };
    let live = def
        .indexes
        .get(&field.name)
        .ok_or_else(|| "The vector field has no index".to_string())?;
    let db = &ctx.store[*ctx.selected_db];
    let filter = match filter {
        Some(text) => crate::vector::filter::parse_query(text, params, def)?.filter,
        None => crate::vector::filter::Filter::MatchAll,
    };
    let unfiltered = matches!(filter, crate::vector::filter::Filter::MatchAll);
    let mut timed_out = false;

    // Every document the filter accepts, measured as it is found; the
    // nearest `k` of them kept.
    let scan = |k: usize, timed_out: &mut bool| -> Vec<KnnResult> {
        let mut scorer = crate::commands::vector::FilteredKnn::new(live, query_vec, k, metric);
        *timed_out = crate::commands::search::filter_each(
            ctx,
            def,
            &filter,
            None,
            deadline,
            |key, fields, json| {
                if json.is_some() || fields.get(field.name.as_str()).is_some() {
                    scorer.offer(key);
                }
            },
        );
        scorer.nearest()
    };

    let nearest: Vec<KnnResult> = match method {
        VectorMethod::Knn { k, ef_runtime } if unfiltered => {
            let ef = ef_runtime.unwrap_or_else(|| match algo {
                VectorAlgo::Hnsw { ef_runtime, .. } => *ef_runtime,
                VectorAlgo::Flat => k.saturating_mul(2),
            });
            crate::commands::vector::nearest_shown(db, &field.name, *k, |n| match live {
                LiveIndex::Flat(idx) => crate::vector::knn_flat(idx, query_vec, n, metric),
                LiveIndex::Hnsw(idx) => {
                    crate::vector::knn_hnsw(idx, query_vec, n, ef.max(n).max(50), metric)
                }
            })
        }
        VectorMethod::Knn { k, ef_runtime } => {
            let knn = crate::vector::filter::Knn {
                k: *k,
                field: field.name.clone(),
                vector: query_vec.to_vec(),
                alias: String::new(),
                ef_runtime: *ef_runtime,
                policy: None,
                batch_size: None,
            };
            match crate::commands::vector::knn_planned(
                ctx, def, &filter, None, &knn, field, deadline,
            ) {
                Some((nearest, cut)) => {
                    timed_out = cut;
                    nearest
                }
                None => scan(*k, &mut timed_out),
            }
        }
        VectorMethod::Range { radius, .. } => {
            let mut within = scan(usize::MAX, &mut timed_out);
            within.retain(|hit| f64::from(hit.score) <= *radius);
            within
        }
    };
    Ok((
        nearest
            .into_iter()
            .map(|hit| (hit.key, f64::from(hit.score)))
            .collect(),
        timed_out,
    ))
}

// ── Entry point ─────────────────────────────────────────────────────────────

pub fn ft_hybrid(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let started = Instant::now();
    match run(ctx, started) {
        Ok(reply) => Some(reply),
        Err(e) => Some(resp_error(&e)),
    }
}

/// `ON_TIMEOUT fail`: a query cut short is an error. Unlike FT.SEARCH here,
/// `return_strict` is not: Redis 8.10.2 answers FT.HYBRID under
/// `return-strict` with what was found and the warning.
fn timeout_fails() -> bool {
    crate::commands::search_tools::config_value("ON_TIMEOUT").eq_ignore_ascii_case("fail")
}

fn run(ctx: &mut Ctx, started: Instant) -> Result<Vec<u8>, String> {
    // This node's documents are only its slots': an answer from them alone
    // would be a part of the cluster's, given as the whole.
    if crate::cluster::search::coordinates(ctx) {
        return Err("ERR FT.HYBRID is not supported across a cluster's masters yet".into());
    }
    let q = parse(&ctx.cmd)?;
    let scorer = match &q.scorer {
        Some(name) => scorer_named(name)?,
        None => Scorer::default_scorer(),
    };

    let def = ctx
        .index_store
        .read()
        .get(&q.index_name)
        .cloned()
        .ok_or_else(|| crate::commands::search::no_such_index(&q.index_name))?;
    let field = def
        .field_by_attribute(&q.vsim_field)
        .or_else(|| def.fields.iter().find(|f| f.name == q.vsim_field))
        .filter(|f| matches!(f.field_type, FieldType::Vector { .. }))
        .ok_or_else(|| format!("SEARCH_SYNTAX Expected a VECTOR field `{}`", q.vsim_field))?;
    let FieldType::Vector {
        dim,
        data_type,
        metric,
        algo,
        ..
    } = &field.field_type
    else {
        unreachable!("checked above");
    };
    // EF_RUNTIME and EPSILON are HNSW's.
    let hnsw_only = match &q.method {
        VectorMethod::Knn { ef_runtime, .. } => ef_runtime.is_some(),
        VectorMethod::Range { epsilon, .. } => epsilon.is_some(),
    };
    if hnsw_only && matches!(algo, VectorAlgo::Flat) {
        return Err(
            "SEARCH_OPTION_INVALID Invalid option (Error parsing vector similarity parameters)"
                .into(),
        );
    }
    let raw = &q.params[&q.vsim_param];
    let query_vec = data_type.decode(raw, *dim).ok_or_else(|| {
        format!(
            "SEARCH_QUERY_BAD Error parsing vector similarity query: query vector blob size ({}) does not match index's expected size ({}).",
            raw.len(),
            dim * data_type.size()
        )
    })?;
    let deadline = crate::commands::search::search_deadline(q.timeout_ms);

    // ── The two lists, each cut to the window ───────────────────────────────
    let (mut text, text_cut) =
        text_ranked(ctx, &def, &q.search_query, &q.params, scorer, deadline)?;
    let (mut vsim, vsim_cut) = vsim_ranked(
        ctx,
        &def,
        field,
        &query_vec,
        &q.method,
        q.vsim_filter.as_deref(),
        &q.params,
        deadline,
    )?;
    // Nearest first; at the same distance, by key.
    rank(&mut vsim, false);
    let window = match q.fusion {
        Fusion::Rrf { window, .. } | Fusion::Linear { window, .. } => window,
    };
    text.truncate(window);
    vsim.truncate(window);
    for hit in &mut vsim {
        hit.1 = similarity(metric, hit.1);
    }

    let mut fused = match q.fusion {
        Fusion::Rrf { constant, .. } => rrf_fuse(&text, &vsim, constant),
        Fusion::Linear { alpha, beta, .. } => linear_fuse(&text, &vsim, alpha, beta),
    };
    if q.nosort {
        // As the lists gave them: the text query's, then the vector query's.
        let mut order: AHashMap<&str, usize> = AHashMap::new();
        for (at, (key, _)) in text.iter().chain(vsim.iter()).enumerate() {
            order.entry(key.as_str()).or_insert(at);
        }
        fused.sort_by_key(|(key, _)| order[key.as_str()]);
    } else {
        rank(&mut fused, true);
    }

    // ── Rows ────────────────────────────────────────────────────────────────
    let text_of: AHashMap<&str, f64> = text.iter().map(|(k, s)| (k.as_str(), *s)).collect();
    let vsim_of: AHashMap<&str, f64> = vsim.iter().map(|(k, s)| (k.as_str(), *s)).collect();
    let json = matches!(def.on, crate::vector::IndexOn::Json);
    let load_all = q.tail.windows(2).any(|w| is(&w[0], "LOAD") && w[1] == b"*");
    let db = &ctx.store[*ctx.selected_db];
    let mut rows: Vec<Row> = Vec::with_capacity(fused.len());
    for (key, score) in fused {
        let mut row: Row = AHashMap::new();
        // The document's fields, for the steps after fusion and for what
        // they show: only when there are any.
        if !q.tail.is_empty() {
            crate::commands::search::with_doc(db, &def, &key, |fields, doc| {
                for (name, value) in fields.iter() {
                    // Text only: a vector's bytes are not a value a row can
                    // show or a step can compute with.
                    let Ok(value) = std::str::from_utf8(value) else {
                        continue;
                    };
                    let shown = match def.fields.iter().find(|f| json && &f.name == name) {
                        Some(f) => f.attribute().to_string(),
                        None => name.clone(),
                    };
                    row.insert(shown, value.to_string());
                }
                if load_all && let Some(doc) = doc {
                    row.insert("$".to_string(), doc.to_string());
                }
            });
        }
        if let (Some(name), Some(s)) = (&q.text_yield, text_of.get(key.as_str())) {
            row.insert(name.clone(), score_text(*s));
        }
        if let (Some(name), Some(s)) = (&q.vsim_yield, vsim_of.get(key.as_str())) {
            row.insert(name.clone(), score_text(*s));
        }
        if let Some(name) = &q.hybrid_yield {
            row.insert(name.clone(), score_text(score));
        }
        row.insert("__score".to_string(), score_text(score));
        row.insert("__key".to_string(), key);
        rows.push(row);
    }

    // ── The steps after fusion, then the page ───────────────────────────────
    let mut tail_cut = false;
    if !q.tail.is_empty() {
        let tail: Vec<&[u8]> = q.tail.iter().map(Vec::as_slice).collect();
        let (after, cut) =
            crate::commands::aggregate_pipeline::run_within(rows, &tail, 0, deadline);
        rows = after;
        tail_cut = cut;
    }
    // A warning for each part that ran out of time, as Redis names them.
    let warnings: Vec<Vec<u8>> = [
        (text_cut, "Timeout limit was reached (SEARCH)"),
        (vsim_cut, "Timeout limit was reached (VSIM)"),
        (tail_cut, "Timeout limit was reached"),
    ]
    .into_iter()
    .filter(|(cut, _)| *cut)
    .map(|(_, warning)| resp_bulk_str(warning))
    .collect();
    if !warnings.is_empty() && timeout_fails() {
        return Err("SEARCH_TIMEOUT Timeout limit was reached".into());
    }
    let total = rows.len();
    let shown = shown_fields(&q);
    let grouped = q.tail.iter().any(|a| is(a, "GROUPBY"));
    let results: Vec<Vec<u8>> = rows
        .into_iter()
        .skip(q.limit_offset)
        .take(q.limit_count)
        .map(|row| row_reply(&row, &shown, grouped, load_all, reply_resp(ctx)))
        .collect();

    let resp = reply_resp(ctx);
    let millis = started.elapsed().as_secs_f64() * 1000.0;
    let execution_time = if resp == 3 {
        resp_double_resp3(millis)
    } else {
        resp_bulk_str(&format!("{millis:.6}"))
    };
    Ok(resp_map_for(
        resp,
        vec![
            resp_bulk_str("total_results"),
            resp_integer(total as i64),
            resp_bulk_str("results"),
            resp_array(results),
            resp_bulk_str("warnings"),
            resp_array(warnings),
            resp_bulk_str("execution_time"),
            execution_time,
        ],
    ))
}

/// What a row shows, in order, as (field, name shown): the text score's
/// name; the fields `LOAD`ed, or else `__key` and `__score`; the vector
/// score's and the fused score's names; and what `SORTBY` and `APPLY` name.
fn shown_fields(q: &HybridQuery) -> Vec<(String, String)> {
    let mut shown: Vec<(String, String)> = Vec::new();
    let mut push = |field: String, name: String| {
        if !shown.iter().any(|(_, n)| *n == name) {
            shown.push((field, name));
        }
    };
    if let Some(name) = &q.text_yield {
        push(name.clone(), name.clone());
    }
    let tail: Vec<&[u8]> = q.tail.iter().map(Vec::as_slice).collect();
    let loads = tail.iter().any(|a| is(a, "LOAD"));
    if !loads {
        push("__key".into(), "__key".into());
        if q.hybrid_yield.is_none() {
            push("__score".into(), "__score".into());
        }
    }
    let name_of = |a: &[u8]| -> String { word(a).trim_start_matches('@').to_string() };
    let mut i = 0;
    let mut after: Vec<(String, String)> = Vec::new();
    while i < tail.len() {
        let end = step_end(&tail, i).unwrap_or(tail.len());
        if is(tail[i], "LOAD") && tail.get(i + 1).is_some_and(|a| *a != b"*") {
            let mut at = i + 2;
            while at < end {
                let field = name_of(tail[at]);
                if at + 2 < end && is(tail[at + 1], "AS") {
                    push(field, name_of(tail[at + 2]));
                    at += 3;
                } else {
                    push(field.clone(), field);
                    at += 1;
                }
            }
        } else if is(tail[i], "SORTBY") {
            for a in &tail[(i + 2).min(end)..end] {
                if a.starts_with(b"@") {
                    let field = name_of(a);
                    after.push((field.clone(), field));
                }
            }
        } else if is(tail[i], "APPLY") {
            let name = if tail.get(i + 2).is_some_and(|a| is(a, "AS")) && i + 3 < tail.len() {
                word(tail[i + 3])
            } else {
                word(tail[i + 1])
            };
            after.push((name.clone(), name));
        }
        i = end.max(i + 1);
    }
    if let Some(name) = &q.vsim_yield {
        push(name.clone(), name.clone());
    }
    if let Some(name) = &q.hybrid_yield {
        push(name.clone(), name.clone());
    }
    for (field, name) in after {
        push(field, name);
    }
    shown
}

fn row_reply(
    row: &Row,
    shown: &[(String, String)],
    grouped: bool,
    load_all: bool,
    resp: u8,
) -> Vec<u8> {
    let mut pairs: Vec<(&str, &str)> = Vec::new();
    if grouped {
        // A group is not a document: every value it holds.
        let mut fields: Vec<(&String, &String)> =
            row.iter().filter(|(k, _)| !k.starts_with("__")).collect();
        fields.sort();
        pairs.extend(fields.into_iter().map(|(k, v)| (k.as_str(), v.as_str())));
    } else {
        for (field, name) in shown {
            if let Some(value) = row.get(field) {
                pairs.push((name.as_str(), value.as_str()));
            }
        }
        if load_all {
            // `LOAD *`: every field of the document, after what is named.
            let mut fields: Vec<(&String, &String)> = row
                .iter()
                .filter(|(k, _)| !k.starts_with("__") && !shown.iter().any(|(f, _)| f == *k))
                .collect();
            fields.sort();
            pairs.extend(fields.into_iter().map(|(k, v)| (k.as_str(), v.as_str())));
        }
    }
    let encoded: Vec<Vec<u8>> = pairs
        .into_iter()
        .flat_map(|(name, value)| {
            let value = if name == "__key" {
                resp_bulk(&crate::key_bytes::bytes_of(value))
            } else {
                crate::commands::aggregate_pipeline::value_reply(value, resp)
            };
            [resp_bulk_str(name), value]
        })
        .collect();
    resp_map_for(resp, encoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(words: &[&str]) -> Vec<Vec<u8>> {
        words.iter().map(|w| w.as_bytes().to_vec()).collect()
    }

    fn parsed(words: &[&str]) -> Result<HybridQuery, String> {
        let owned = args(words);
        let cmd: Vec<&[u8]> = owned.iter().map(Vec::as_slice).collect();
        parse(&cmd)
    }

    const TAIL: [&str; 4] = ["PARAMS", "2", "q", "xxxx"];

    fn with_tail(words: &[&'static str]) -> Vec<&'static str> {
        words.iter().copied().chain(TAIL).collect()
    }

    #[test]
    fn scorer_and_yield_in_the_search_clause() {
        let q = parsed(&with_tail(&[
            "FT.HYBRID",
            "idx",
            "SEARCH",
            "@t:rust",
            "SCORER",
            "BM25STD",
            "YIELD_SCORE_AS",
            "ts",
            "VSIM",
            "@v",
            "$q",
        ]))
        .unwrap();
        assert_eq!(q.scorer.as_deref(), Some("BM25STD"));
        assert_eq!(q.text_yield.as_deref(), Some("ts"));
        // Either order, the last of each.
        let q = parsed(&with_tail(&[
            "FT.HYBRID",
            "idx",
            "SEARCH",
            "@t:rust",
            "YIELD_SCORE_AS",
            "a",
            "SCORER",
            "TFIDF",
            "SCORER",
            "BM25",
            "VSIM",
            "@v",
            "$q",
        ]))
        .unwrap();
        assert_eq!(q.scorer.as_deref(), Some("BM25"));
        assert_eq!(q.text_yield.as_deref(), Some("a"));
    }

    #[test]
    fn scorer_names_are_redis_s() {
        assert_eq!(scorer_named("TFIDF").unwrap(), Scorer::Tfidf);
        assert_eq!(scorer_named("TFIDF.DOCNORM").unwrap(), Scorer::TfidfDocNorm);
        assert_eq!(
            scorer_named("NOPE").unwrap_err(),
            "SEARCH_QUERY_BAD No such scorer NOPE"
        );
        assert_eq!(
            scorer_named("tfidf").unwrap_err(),
            "SEARCH_QUERY_BAD No such scorer tfidf"
        );
    }

    #[test]
    fn vsim_filter_and_pipeline_filter() {
        let q = parsed(&with_tail(&[
            "FT.HYBRID",
            "idx",
            "SEARCH",
            "x",
            "VSIM",
            "@v",
            "$q",
            "KNN",
            "2",
            "K",
            "3",
            "FILTER",
            "@tag:{a}",
            "YIELD_SCORE_AS",
            "vs",
            "FILTER",
            "@n>2",
        ]))
        .unwrap();
        assert_eq!(
            q.method,
            VectorMethod::Knn {
                k: 3,
                ef_runtime: None
            }
        );
        assert_eq!(q.vsim_filter.as_deref(), Some("@tag:{a}"));
        assert_eq!(q.vsim_yield.as_deref(), Some("vs"));
        assert_eq!(q.tail, args(&["FILTER", "@n>2"]));
    }

    #[test]
    fn combine_clauses() {
        let q = parsed(&with_tail(&[
            "FT.HYBRID",
            "idx",
            "SEARCH",
            "x",
            "VSIM",
            "@v",
            "$q",
            "COMBINE",
            "LINEAR",
            "4",
            "ALPHA",
            "0.05",
            "BETA",
            "0.95",
            "YIELD_SCORE_AS",
            "hs",
        ]))
        .unwrap();
        assert_eq!(
            q.fusion,
            Fusion::Linear {
                alpha: 0.05,
                beta: 0.95,
                window: 20
            }
        );
        assert_eq!(q.hybrid_yield.as_deref(), Some("hs"));
        let q = parsed(&with_tail(&[
            "FT.HYBRID",
            "idx",
            "SEARCH",
            "x",
            "VSIM",
            "@v",
            "$q",
            "COMBINE",
            "LINEAR",
            "0",
        ]))
        .unwrap();
        assert_eq!(
            q.fusion,
            Fusion::Linear {
                alpha: 0.3,
                beta: 0.7,
                window: 20
            }
        );
        let q = parsed(&with_tail(&[
            "FT.HYBRID",
            "idx",
            "SEARCH",
            "x",
            "VSIM",
            "@v",
            "$q",
            "COMBINE",
            "RRF",
            "4",
            "WINDOW",
            "5",
            "CONSTANT",
            "10",
        ]))
        .unwrap();
        assert_eq!(
            q.fusion,
            Fusion::Rrf {
                window: 5,
                constant: 10.0
            }
        );
    }

    #[test]
    fn errors_are_redis_s() {
        let err = |words: &[&'static str]| parsed(&with_tail(words)).unwrap_err();
        assert_eq!(
            err(&[
                "FT.HYBRID",
                "idx",
                "SEARCH",
                "x",
                "VERBATIM",
                "VSIM",
                "@v",
                "$q"
            ]),
            "SEARCH_ARG_UNRECOGNIZED Unknown argument `VERBATIM` in SEARCH"
        );
        assert_eq!(
            err(&[
                "FT.HYBRID",
                "idx",
                "SEARCH",
                "x",
                "VSIM",
                "@v",
                "$q",
                "BOGUS"
            ]),
            "SEARCH_PARSE_ARGS BOGUS: Unknown argument"
        );
        assert_eq!(
            err(&[
                "FT.HYBRID",
                "idx",
                "SEARCH",
                "x",
                "VSIM",
                "@v",
                "$q",
                "KNN",
                "0"
            ]),
            "SEARCH_SYNTAX Invalid argument count: 0 (must be a positive even number for key/value pairs)"
        );
        assert_eq!(
            err(&[
                "FT.HYBRID",
                "idx",
                "SEARCH",
                "x",
                "VSIM",
                "@v",
                "$q",
                "KNN",
                "2",
                "K",
                "0"
            ]),
            "SEARCH_SYNTAX Invalid K value"
        );
        assert_eq!(
            err(&[
                "FT.HYBRID",
                "idx",
                "SEARCH",
                "x",
                "VSIM",
                "@v",
                "$q",
                "KNN",
                "4",
                "K",
                "3",
                "YIELD_SCORE_AS",
                "vs"
            ]),
            "SEARCH_ARG_UNRECOGNIZED Unknown argument `YIELD_SCORE_AS` in KNN"
        );
        assert_eq!(
            err(&[
                "FT.HYBRID",
                "idx",
                "SEARCH",
                "x",
                "VSIM",
                "@v",
                "$q",
                "COMBINE",
                "FOO",
                "0"
            ]),
            "SEARCH_PARSE_ARGS COMBINE: Invalid value for argument"
        );
        assert_eq!(
            err(&[
                "FT.HYBRID",
                "idx",
                "SEARCH",
                "x",
                "VSIM",
                "@v",
                "$q",
                "COMBINE",
                "LINEAR",
                "2",
                "ALPHA",
                "1"
            ]),
            "SEARCH_SYNTAX Missing value for BETA"
        );
        assert_eq!(
            err(&[
                "FT.HYBRID",
                "idx",
                "SEARCH",
                "x",
                "VSIM",
                "@v",
                "$q",
                "COMBINE",
                "RRF",
                "2",
                "WINDOW",
                "0"
            ]),
            "SEARCH_PARSE_ARGS WINDOW: Value below minimum"
        );
        assert_eq!(
            err(&[
                "FT.HYBRID",
                "idx",
                "SEARCH",
                "x",
                "VSIM",
                "@v",
                "$q",
                "SORTBY",
                "1",
                "@n",
                "DESC"
            ]),
            "SEARCH_PARSE_ARGS DESC: Unknown argument"
        );
        assert_eq!(
            err(&[
                "FT.HYBRID",
                "idx",
                "SEARCH",
                "x",
                "VSIM",
                "@v",
                "$q",
                "LOAD",
                "1",
                "title"
            ]),
            "SEARCH_PARSE_ARGS Missing prefix: name requires '@' prefix, JSON path require '$' prefix, got: title in LOAD"
        );
        assert_eq!(
            err(&[
                "FT.HYBRID",
                "idx",
                "SEARCH",
                "x",
                "VSIM",
                "@v",
                "$q",
                "TIMEOUT",
                "abc"
            ]),
            "SEARCH_PARSE_ARGS TIMEOUT: Could not convert argument to expected type"
        );
        assert!(
            err(&[
                "FT.HYBRID",
                "idx",
                "SEARCH",
                "x",
                "VSIM",
                "@v",
                "$q",
                "DIALECT",
                "2"
            ])
            .starts_with("SEARCH_PARSE_ARGS DIALECT is not supported in FT.HYBRID")
        );
        assert_eq!(
            parsed(&[
                "FT.HYBRID",
                "idx",
                "SEARCH",
                "x",
                "VSIM",
                "@v",
                "$nope",
                "PARAMS",
                "2",
                "q",
                "x"
            ])
            .unwrap_err(),
            "SEARCH_PARAM_NOT_FOUND Parameter not found `nope`"
        );
    }

    #[test]
    fn counts_past_the_end_are_refused() {
        let big = "18446744073709551615";
        for words in [
            vec!["LOAD", big, "@a"],
            vec!["PARAMS", "18446744073709551614", "q", "x"],
            vec!["SORTBY", big, "@a"],
            vec!["GROUPBY", big, "@a"],
            vec![
                "GROUPBY",
                "1",
                "@a",
                "REDUCE",
                "COUNT",
                "18446744073709551613",
                "AS",
                "c",
            ],
            vec!["COMBINE", "RRF", "18446744073709551614"],
        ] {
            let mut all = vec!["FT.HYBRID", "idx", "SEARCH", "x", "VSIM", "@v", "$q"];
            all.extend(words.iter().copied());
            all.extend(TAIL);
            assert!(parsed(&all).is_err(), "{words:?}");
        }
        let knn = [
            "FT.HYBRID",
            "idx",
            "SEARCH",
            "x",
            "VSIM",
            "@v",
            "$q",
            "KNN",
            "18446744073709551614",
        ];
        assert!(parsed(&with_tail(&knn)).is_err());
    }

    /// Redis 8.10.2 refuses an infinite `CONSTANT`, `ALPHA` or `BETA`, or one
    /// past a double's range, and takes `nan`, zero and negative numbers.
    #[test]
    fn combine_weights_are_finite() {
        let combine = |method: &'static str, n: &'static str, rest: &[&'static str]| {
            let mut words = vec![
                "FT.HYBRID",
                "idx",
                "SEARCH",
                "x",
                "VSIM",
                "@v",
                "$q",
                "COMBINE",
                method,
                n,
            ];
            words.extend_from_slice(rest);
            parsed(&with_tail(&words))
        };
        for bad in ["inf", "-inf", "+inf", "infinity", "1e400", "-1.8e308"] {
            assert_eq!(
                combine("RRF", "2", &["CONSTANT", bad]).unwrap_err(),
                "SEARCH_PARSE_ARGS CONSTANT: Could not convert argument to expected type",
                "{bad}"
            );
            assert_eq!(
                combine("LINEAR", "4", &["ALPHA", bad, "BETA", "1"]).unwrap_err(),
                "SEARCH_PARSE_ARGS ALPHA: Could not convert argument to expected type",
                "{bad}"
            );
            assert_eq!(
                combine("LINEAR", "4", &["ALPHA", "1", "BETA", bad]).unwrap_err(),
                "SEARCH_PARSE_ARGS BETA: Could not convert argument to expected type",
                "{bad}"
            );
        }
        for good in ["0", "-1", "nan", "0.5", "1e308"] {
            assert!(combine("RRF", "2", &["CONSTANT", good]).is_ok(), "{good}");
            assert!(
                combine("LINEAR", "4", &["ALPHA", good, "BETA", good]).is_ok(),
                "{good}"
            );
        }
    }

    #[test]
    fn scores_are_written_as_redis_writes_them() {
        assert_eq!(score_text(0.032266458496023), "0.032266458496");
        assert_eq!(score_text(2.0682214534039685), "2.0682214534");
        assert_eq!(score_text(1.0), "1");
        assert_eq!(score_text(0.0), "0");
        assert_eq!(score_text(2.0 / 3.0), "0.666666666667");
        assert_eq!(score_text(0.1), "0.1");
        assert_eq!(score_text(1.5e-7), "1.5e-07");
    }

    #[test]
    fn similarity_is_redis_s() {
        assert_eq!(similarity(&DistanceMetric::L2, 0.0), 1.0);
        assert!((similarity(&DistanceMetric::L2, 0.5) - 2.0 / 3.0).abs() < 1e-12);
        assert_eq!(similarity(&DistanceMetric::Cosine, 1.0), 0.5);
        assert_eq!(similarity(&DistanceMetric::InnerProduct, 0.0), 0.5);
    }
}
