// FT.HYBRID — Redis 8.4 hybrid text + vector search.
//
// Combines two ranked result lists (one from text/filter search, one from
// vector KNN) into a single ranked list using a score-fusion method.
// Two methods supported:
//
//   RRF (Reciprocal Rank Fusion)
//     score(d) = Σ 1 / (k + rank_i(d))    for each list i
//     where k = CONSTANT (default 60). Scale-free — doesn't care about
//     raw score distributions, only relative ranks within each list.
//
//   LINEAR
//     score(d) = α · text_norm(d) + β · vsim_norm(d)
//     where text_norm and vsim_norm are min-max normalized to [0,1].
//     Vector distances are inverted during normalization since lower
//     distance means more similar.
//
// Why a separate file: ft_search and ft_aggregate already make vector.rs
// over 2k lines. Hybrid is structurally distinct (two parallel rankings
// then fusion) — co-locating it with FT.SEARCH would entangle the parsers.

use crate::store::ValueRef;
use ahash::AHashMap;

use crate::commands::ctx::Ctx;
use crate::resp::write::{resp_array, resp_bulk_str, resp_error, resp_integer};
use crate::vector::{FieldType, KnnResult, LiveIndex, knn_flat, knn_hnsw};

#[derive(Debug)]
enum LoadSpec {
    All,
    Fields(Vec<String>),
}

// ── Parsed command structure ────────────────────────────────────────────────
//
// Hand-rolled parser instead of pulling in a parser-combinator crate.
// FT.HYBRID's grammar is small and section-based — sections are introduced
// by keywords (SEARCH, VSIM, COMBINE, etc.) so a token-walking state
// machine reads cleanly.

#[derive(Debug, Default)]
struct HybridQuery {
    index_name: String,
    search_query: String,
    vsim_field: String,
    vsim_param: String,
    knn_k: usize,
    knn_ef_runtime: Option<usize>,
    fusion: Fusion,
    limit_offset: usize,
    limit_count: usize,
    params: AHashMap<String, Vec<u8>>,

    /// Field name for the per-row text relevance score in the response.
    /// Defaults to "text_score". `YIELD_SCORE_AS` after SEARCH overrides.
    text_score_alias: Option<String>,

    /// Field name for the vector-similarity score. Defaults to "vsim_score".
    /// `YIELD_SCORE_AS` inside VSIM overrides.
    vsim_score_alias: Option<String>,

    /// Field name for the fused hybrid score. Defaults to "hybrid_score".
    /// `YIELD_SCORE_AS` inside COMBINE overrides.
    hybrid_score_alias: Option<String>,

    /// Optional explicit sort. None = sort by hybrid_score descending (the
    /// default). `NOSORT` produces Some(SortSpec::None). A SORTBY with a
    /// field name produces Some(SortSpec::By { ... }).
    sort: Option<SortSpec>,
    /// What hash fields to return per result.
    /// None = no LOAD specified, return only scores (4.1/4.2 behavior).
    /// Some(LoadSpec::All) = LOAD *, return every hash field.
    /// Some(LoadSpec::Fields(v)) = LOAD with explicit field names.
    load: Option<LoadSpec>,
    /// Index into `cmd` where the aggregation pipeline tail begins.
    /// Set by the parser when it encounters GROUPBY/APPLY/FILTER. None
    /// means there's no pipeline tail and we emit the score-and-load
    /// response directly.
    pipeline_start: Option<usize>,
}

impl HybridQuery {
    fn text_alias(&self) -> &str {
        self.text_score_alias.as_deref().unwrap_or("text_score")
    }
    fn vsim_alias(&self) -> &str {
        self.vsim_score_alias.as_deref().unwrap_or("vsim_score")
    }
    fn hybrid_alias(&self) -> &str {
        self.hybrid_score_alias.as_deref().unwrap_or("hybrid_score")
    }
}

#[derive(Debug)]
enum SortSpec {
    /// NOSORT: emit results in fusion order, don't re-sort after pagination.
    None,
    /// SORTBY: sort the result page by a named field with direction.
    By { field: String, descending: bool },
}

#[derive(Debug)]
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

impl Default for Fusion {
    fn default() -> Self {
        // Redis defaults: RRF with WINDOW=20 CONSTANT=60
        Fusion::Rrf {
            window: 20,
            constant: 60.0,
        }
    }
}

// ── Parser ──────────────────────────────────────────────────────────────────

fn parse(cmd: &[&[u8]]) -> Result<HybridQuery, String> {
    if cmd.len() < 6 {
        return Err("ERR wrong number of arguments for 'ft.hybrid' command".into());
    }

    let mut q = HybridQuery {
        knn_k: 10,
        limit_offset: 0,
        limit_count: 10,
        ..Default::default()
    };

    q.index_name = utf8(cmd[1])?.to_string();

    // Walk tokens. Every section is introduced by an uppercase keyword.
    // Within a section we consume option tokens until we see the next
    // section keyword or run off the end.
    let mut i = 2usize;
    while i < cmd.len() {
        let tok = utf8(cmd[i])?.to_ascii_uppercase();
        match tok.as_str() {
            "SEARCH" => {
                i += 1;
                if i >= cmd.len() {
                    return Err("ERR SEARCH requires a query".into());
                }
                q.search_query = utf8(cmd[i])?.to_string();
                i += 1;
                // SEARCH may be followed by SCORER (ignored, sub-step 4.4)
                // or YIELD_SCORE_AS. Only YIELD_SCORE_AS is implemented now.
                while i < cmd.len() {
                    let opt = utf8(cmd[i])?.to_ascii_uppercase();
                    if opt == "YIELD_SCORE_AS" {
                        i += 1;
                        if i >= cmd.len() {
                            return Err("ERR YIELD_SCORE_AS requires a name".into());
                        }
                        q.text_score_alias = Some(utf8(cmd[i])?.to_string());
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            "VSIM" => {
                i += 1;
                if i + 1 >= cmd.len() {
                    return Err("ERR VSIM requires field and param".into());
                }
                q.vsim_field = utf8(cmd[i])?
                    .strip_prefix('@')
                    .unwrap_or(utf8(cmd[i])?)
                    .to_string();
                i += 1;
                q.vsim_param = utf8(cmd[i])?
                    .strip_prefix('$')
                    .ok_or_else(|| "ERR VSIM param must start with $".to_string())?
                    .to_string();
                i += 1;
                // Optional KNN sub-block
                if i < cmd.len() && utf8(cmd[i])?.eq_ignore_ascii_case("KNN") {
                    i += 1;
                    // count — positional hint; we read+ignore the value
                    let _count: usize = utf8(cmd[i])?
                        .parse()
                        .map_err(|_| "ERR KNN count must be an integer".to_string())?;
                    i += 1;
                    while i < cmd.len() {
                        let opt = utf8(cmd[i])?.to_ascii_uppercase();
                        if matches!(
                            opt.as_str(),
                            "COMBINE"
                                | "LIMIT"
                                | "PARAMS"
                                | "SORTBY"
                                | "NOSORT"
                                | "LOAD"
                                | "YIELD_SCORE_AS"
                                | "GROUPBY"
                                | "APPLY"
                                | "FILTER"
                        ) {
                            break;
                        }
                        match opt.as_str() {
                            "K" => {
                                i += 1;
                                q.knn_k = utf8(cmd[i])?
                                    .parse()
                                    .map_err(|_| "ERR K must be an integer".to_string())?;
                                i += 1;
                            }
                            "EF_RUNTIME" => {
                                i += 1;
                                q.knn_ef_runtime =
                                    Some(utf8(cmd[i])?.parse().map_err(|_| {
                                        "ERR EF_RUNTIME must be integer".to_string()
                                    })?);
                                i += 1;
                            }
                            _ => break, // unknown option — let outer loop handle it
                        }
                    }
                }

                // VSIM-level options that come after the KNN sub-block:
                // YIELD_SCORE_AS for the vector-similarity score.
                while i < cmd.len() {
                    let opt = utf8(cmd[i])?.to_ascii_uppercase();
                    if opt == "YIELD_SCORE_AS" {
                        i += 1;
                        if i >= cmd.len() {
                            return Err("ERR YIELD_SCORE_AS requires a name".into());
                        }
                        q.vsim_score_alias = Some(utf8(cmd[i])?.to_string());
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            "COMBINE" => {
                i += 1;
                let method = utf8(cmd[i])?.to_ascii_uppercase();
                i += 1;
                // count is purely positional — read and discard
                let _count: usize = utf8(cmd[i])?
                    .parse()
                    .map_err(|_| "ERR COMBINE count must be an integer".to_string())?;
                i += 1;
                match method.as_str() {
                    "RRF" => {
                        let mut window = 20usize;
                        let mut constant = 60.0f64;
                        while i < cmd.len() {
                            let opt = utf8(cmd[i])?.to_ascii_uppercase();
                            if matches!(
                                opt.as_str(),
                                "COMBINE" | "LIMIT" | "PARAMS" | "SORTBY" | "NOSORT"
                            ) {
                                break;
                            }
                            match opt.as_str() {
                                "WINDOW" => {
                                    i += 1;
                                    window = utf8(cmd[i])?
                                        .parse()
                                        .map_err(|_| "ERR WINDOW must be integer".to_string())?;
                                    i += 1;
                                }
                                "CONSTANT" => {
                                    i += 1;
                                    constant = utf8(cmd[i])?
                                        .parse()
                                        .map_err(|_| "ERR CONSTANT must be a number".to_string())?;
                                    i += 1;
                                }
                                "YIELD_SCORE_AS" => {
                                    i += 1;
                                    if i >= cmd.len() {
                                        return Err("ERR YIELD_SCORE_AS requires a name".into());
                                    }
                                    q.hybrid_score_alias = Some(utf8(cmd[i])?.to_string());
                                    i += 1;
                                }
                                _ => break,
                            }
                        }
                        q.fusion = Fusion::Rrf { window, constant };
                    }
                    "LINEAR" => {
                        let mut alpha = 0.5f64;
                        let mut beta = 0.5f64;
                        let mut window = 20usize;
                        while i < cmd.len() {
                            let opt = utf8(cmd[i])?.to_ascii_uppercase();
                            if matches!(
                                opt.as_str(),
                                "COMBINE" | "LIMIT" | "PARAMS" | "SORTBY" | "NOSORT"
                            ) {
                                break;
                            }
                            match opt.as_str() {
                                "ALPHA" => {
                                    i += 1;
                                    alpha = utf8(cmd[i])?
                                        .parse()
                                        .map_err(|_| "ERR ALPHA must be a number".to_string())?;
                                    i += 1;
                                }
                                "BETA" => {
                                    i += 1;
                                    beta = utf8(cmd[i])?
                                        .parse()
                                        .map_err(|_| "ERR BETA must be a number".to_string())?;
                                    i += 1;
                                }
                                "WINDOW" => {
                                    i += 1;
                                    window = utf8(cmd[i])?
                                        .parse()
                                        .map_err(|_| "ERR WINDOW must be integer".to_string())?;
                                    i += 1;
                                }
                                "YIELD_SCORE_AS" => {
                                    i += 1;
                                    if i >= cmd.len() {
                                        return Err("ERR YIELD_SCORE_AS requires a name".into());
                                    }
                                    q.hybrid_score_alias = Some(utf8(cmd[i])?.to_string());
                                    i += 1;
                                }
                                _ => break,
                            }
                        }
                        q.fusion = Fusion::Linear {
                            alpha,
                            beta,
                            window,
                        };
                    }
                    other => return Err(format!("ERR unknown COMBINE method '{other}'")),
                }
            }
            "LIMIT" => {
                i += 1;
                q.limit_offset = utf8(cmd[i])?
                    .parse()
                    .map_err(|_| "ERR LIMIT offset must be integer".to_string())?;
                i += 1;
                q.limit_count = utf8(cmd[i])?
                    .parse()
                    .map_err(|_| "ERR LIMIT count must be integer".to_string())?;
                i += 1;
            }
            "PARAMS" => {
                i += 1;
                let n: usize = utf8(cmd[i])?
                    .parse()
                    .map_err(|_| "ERR PARAMS nargs must be integer".to_string())?;
                i += 1;
                if !n.is_multiple_of(2) {
                    return Err("ERR PARAMS nargs must be even".into());
                }
                for _ in 0..n / 2 {
                    if i + 1 >= cmd.len() {
                        return Err("ERR PARAMS truncated".into());
                    }
                    let name = utf8(cmd[i])?.to_string();
                    let val = cmd[i + 1].to_vec();
                    q.params.insert(name, val);
                    i += 2;
                }
            }
            "SORTBY" => {
                i += 1;
                // SORTBY <count> <field> [ASC|DESC]
                // count is the number-of-args hint; we read+ignore.
                let _count: usize = utf8(cmd[i])?
                    .parse()
                    .map_err(|_| "ERR SORTBY count must be integer".to_string())?;
                i += 1;
                let field = utf8(cmd[i])?
                    .strip_prefix('@')
                    .unwrap_or(utf8(cmd[i])?)
                    .to_string();
                i += 1;
                // Optional direction. Default ASC matches Redis SORTBY.
                let descending = if i < cmd.len() {
                    let dir = utf8(cmd[i])?.to_ascii_uppercase();
                    match dir.as_str() {
                        "ASC" => {
                            i += 1;
                            false
                        }
                        "DESC" => {
                            i += 1;
                            true
                        }
                        _ => false, // not a direction token; another section follows
                    }
                } else {
                    false
                };
                q.sort = Some(SortSpec::By { field, descending });
            }
            "NOSORT" => {
                i += 1;
                q.sort = Some(SortSpec::None);
            }
            "LOAD" => {
                i += 1;
                if i >= cmd.len() {
                    return Err("ERR LOAD requires count or *".into());
                }
                let first = utf8(cmd[i])?;
                if first == "*" {
                    q.load = Some(LoadSpec::All);
                    i += 1;
                } else {
                    let count: usize = first
                        .parse()
                        .map_err(|_| "ERR LOAD count must be * or integer".to_string())?;
                    i += 1;
                    // `count` is the client's word: no more than there are arguments.
                    let mut fields = Vec::with_capacity(count.min(cmd.len()));
                    for _ in 0..count {
                        if i >= cmd.len() {
                            return Err("ERR LOAD truncated".into());
                        }
                        let name = utf8(cmd[i])?
                            .strip_prefix('@')
                            .unwrap_or(utf8(cmd[i])?)
                            .to_string();
                        fields.push(name);
                        i += 1;
                    }
                    q.load = Some(LoadSpec::Fields(fields));
                }
            }
            "GROUPBY" | "APPLY" | "FILTER" => {
                // Before handing off to the pipeline, look ahead for a PARAMS
                // section anywhere in the remaining tokens. PARAMS is a
                // top-level FT.HYBRID concept; it can legitimately appear
                // after pipeline clauses in Redis's grammar.
                let mut j = i;
                while j < cmd.len() {
                    let t = utf8(cmd[j])?.to_ascii_uppercase();
                    if t == "PARAMS" {
                        let n: usize = utf8(cmd[j + 1])?
                            .parse()
                            .map_err(|_| "ERR PARAMS nargs must be integer".to_string())?;
                        if !n.is_multiple_of(2) {
                            return Err("ERR PARAMS nargs must be even".into());
                        }
                        let mut k = j + 2;
                        for _ in 0..n / 2 {
                            if k + 1 >= cmd.len() {
                                return Err("ERR PARAMS truncated".into());
                            }
                            q.params
                                .insert(utf8(cmd[k])?.to_string(), cmd[k + 1].to_vec());
                            k += 2;
                        }
                        break;
                    }
                    j += 1;
                }

                q.pipeline_start = Some(i);
                break;
            }
            // For now, anything we don't recognize is a syntax error.
            other => return Err(format!("ERR unknown FT.HYBRID token '{other}'")),
        }
    }

    if q.search_query.is_empty() {
        return Err("ERR FT.HYBRID requires SEARCH".into());
    }
    if q.vsim_field.is_empty() {
        return Err("ERR FT.HYBRID requires VSIM".into());
    }
    if !q.params.contains_key(&q.vsim_param) {
        return Err(format!(
            "ERR PARAMS does not contain VSIM param '{}'",
            q.vsim_param
        ));
    }

    Ok(q)
}

fn utf8(b: &[u8]) -> Result<&str, String> {
    std::str::from_utf8(b).map_err(|_| "ERR argument must be valid UTF-8".to_string())
}

// ── Score fusion ────────────────────────────────────────────────────────────

/// Apply RRF: each document's hybrid score = Σ 1 / (k + rank_in_list).
/// Documents not present in a list contribute 0 from that list.
/// Returns a sorted Vec of (key, hybrid_score) descending.
fn rrf_fuse(
    text_results: &[(String, f64)],
    vsim_results: &[(String, f64)],
    constant: f64,
    window: usize,
) -> Vec<(String, f64)> {
    let mut scores: AHashMap<String, f64> = AHashMap::new();

    for (rank, (key, _)) in text_results.iter().take(window).enumerate() {
        let r = (rank + 1) as f64; // 1-based rank
        *scores.entry(key.clone()).or_insert(0.0) += 1.0 / (constant + r);
    }
    for (rank, (key, _)) in vsim_results.iter().take(window).enumerate() {
        let r = (rank + 1) as f64;
        *scores.entry(key.clone()).or_insert(0.0) += 1.0 / (constant + r);
    }

    let mut out: Vec<(String, f64)> = scores.into_iter().collect();
    out.sort_unstable_by(|a, b| b.1.total_cmp(&a.1)); // descending
    out
}

/// Apply LINEAR fusion: hybrid = α · norm(text) + β · norm(vsim).
/// Both scores are min-max normalized to [0,1] independently. Vector
/// distances are inverted (1 - normalized) since lower distance = more
/// similar. Documents missing from a list contribute 0 from that list.
fn linear_fuse(
    text_results: &[(String, f64)],
    vsim_results: &[(String, f64)],
    alpha: f64,
    beta: f64,
    window: usize,
) -> Vec<(String, f64)> {
    fn min_max(vals: &[f64]) -> (f64, f64) {
        let mut lo = f64::INFINITY;
        let mut hi = f64::NEG_INFINITY;
        for &v in vals {
            if v < lo {
                lo = v;
            }
            if v > hi {
                hi = v;
            }
        }
        (lo, hi)
    }

    let text_window: Vec<&(String, f64)> = text_results.iter().take(window).collect();
    let vsim_window: Vec<&(String, f64)> = vsim_results.iter().take(window).collect();

    let text_vals: Vec<f64> = text_window.iter().map(|(_, s)| *s).collect();
    let vsim_vals: Vec<f64> = vsim_window.iter().map(|(_, s)| *s).collect();

    let (t_lo, t_hi) = min_max(&text_vals);
    let (v_lo, v_hi) = min_max(&vsim_vals);

    let t_range = t_hi - t_lo;
    let v_range = v_hi - v_lo;

    let mut scores: AHashMap<String, f64> = AHashMap::new();

    // Text score: higher is already better in our text-search returns
    // (we use a synthetic relevance, see text_search below). Normalize
    // upward: norm = (s - lo) / range.
    for (key, s) in &text_window {
        let n = if t_range > 0.0 {
            (s - t_lo) / t_range
        } else {
            1.0
        };
        *scores.entry(key.clone()).or_insert(0.0) += alpha * n;
    }

    // Vector score: this is a *distance* (lower = more similar). Invert
    // during normalization so higher = better.
    for (key, d) in &vsim_window {
        let n = if v_range > 0.0 {
            1.0 - (d - v_lo) / v_range
        } else {
            1.0
        };
        *scores.entry(key.clone()).or_insert(0.0) += beta * n;
    }

    let mut out: Vec<(String, f64)> = scores.into_iter().collect();
    out.sort_unstable_by(|a, b| b.1.total_cmp(&a.1));
    out
}

// ── The two component queries ───────────────────────────────────────────────

/// Run the text/filter portion. Returns (key, relevance_score) sorted by
/// score descending.
///
/// The query is the same language FT.SEARCH takes, and is run the same way:
/// see `commands::search::filter_search`.
pub(crate) fn text_search(
    ctx: &Ctx,
    def: &crate::vector::IndexDef,
    query: &str,
    params: &AHashMap<String, Vec<u8>>,
) -> Result<Vec<(String, f64)>, String> {
    let parsed = crate::vector::filter::parse_query(query, params, def)?;
    let mut out: Vec<(String, f64)> =
        crate::commands::search::filter_search(ctx, def, &parsed.filter, None, None)
            .matches
            .into_iter()
            .map(|found| (found.key, found.score))
            .collect();
    out.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    Ok(out)
}

/// Run the vector KNN portion. Returns (key, distance) sorted ascending.
fn vsim_search(
    def: &crate::vector::IndexDef,
    field: &str,
    query_vec: &[f32],
    k: usize,
    ef_runtime: Option<usize>,
) -> Result<Vec<(String, f64)>, String> {
    let vf = def
        .vector_field_by_name(field)
        .ok_or_else(|| format!("ERR no such vector field '{field}' in index"))?;

    let metric = match &vf.field_type {
        FieldType::Vector { metric, .. } => metric.clone(),
        _ => return Err("ERR field is not VECTOR".into()),
    };

    let live = def
        .indexes
        .get(field)
        .ok_or_else(|| "ERR vector field has no live index".to_string())?;

    let knn: Vec<KnnResult> = match live {
        LiveIndex::Flat(idx) => knn_flat(idx, query_vec, k, &metric),
        LiveIndex::Hnsw(idx) => {
            let ef = ef_runtime.unwrap_or_else(|| match &vf.field_type {
                FieldType::Vector {
                    algo: crate::vector::VectorAlgo::Hnsw { ef_runtime, .. },
                    ..
                } => *ef_runtime,
                _ => k * 2,
            });
            knn_hnsw(idx, query_vec, k, ef.max(k).max(50), &metric)
        }
    };

    Ok(knn.into_iter().map(|r| (r.key, r.score as f64)).collect())
}

/// Look up a single hash field on a key and parse it as f64.
/// Returns f64::NEG_INFINITY for missing keys, missing fields, or
/// non-numeric values — making them sort to the natural minimum.
fn read_numeric_field(
    db: &crate::store::Db,
    def: &crate::vector::IndexDef,
    key: &str,
    field: &str,
) -> f64 {
    let Some(value) = crate::store::get_shard(db, key)
        .read()
        .get(key)
        .map(ValueRef::to_value)
    else {
        return f64::NEG_INFINITY;
    };
    let Some(inner) = crate::vector::doc_fields(&def.on, &def.fields, value.view()) else {
        return f64::NEG_INFINITY;
    };
    // By the name the query knows the field by.
    let field = def
        .field_by_attribute(field)
        .map_or(field, |f| f.name.as_str());
    match inner.get(field) {
        Some(raw) => match std::str::from_utf8(raw) {
            Ok(s) => s.parse::<f64>().unwrap_or(f64::NEG_INFINITY),
            Err(_) => f64::NEG_INFINITY,
        },
        None => f64::NEG_INFINITY,
    }
}

// ── Entry point ─────────────────────────────────────────────────────────────

pub fn ft_hybrid(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let q = match parse(&ctx.cmd) {
        Ok(q) => q,
        Err(e) => return Some(resp_error(&e)),
    };

    // Snapshot the index def so we don't hold the index_store lock through
    // the keyspace walk + KNN.
    let (def_clone, query_vec) = {
        let store = ctx.index_store.read();
        let def = match store.get(&q.index_name) {
            Some(d) => d.clone(),
            None => return Some(resp_error("ERR no such index")),
        };

        // Decode vector param: PARAMS holds raw bytes in the field's type.
        let raw = q.params.get(&q.vsim_param).unwrap(); // checked in parse()
        let vf = match def.vector_field_by_name(&q.vsim_field) {
            Some(f) => f,
            None => {
                return Some(resp_error(&format!(
                    "ERR no such vector field '{}'",
                    q.vsim_field
                )));
            }
        };
        let (dim, data_type) = match &vf.field_type {
            FieldType::Vector { dim, data_type, .. } => (*dim, *data_type),
            _ => return Some(resp_error("ERR field is not VECTOR")),
        };
        let Some(v) = data_type.decode(raw, dim) else {
            return Some(resp_error(&format!(
                "ERR vector param wrong size: expected {} bytes, got {}",
                dim * data_type.size(),
                raw.len()
            )));
        };
        (def, v)
    };

    let text_ranked = match text_search(ctx, &def_clone, &q.search_query, &q.params) {
        Ok(r) => r,
        Err(e) => return Some(resp_error(&e)),
    };

    let vsim_ranked = match vsim_search(
        &def_clone,
        &q.vsim_field,
        &query_vec,
        q.knn_k,
        q.knn_ef_runtime,
    ) {
        Ok(r) => r,
        Err(e) => return Some(resp_error(&e)),
    };

    let fused = match q.fusion {
        Fusion::Rrf { window, constant } => rrf_fuse(&text_ranked, &vsim_ranked, constant, window),
        Fusion::Linear {
            alpha,
            beta,
            window,
        } => linear_fuse(&text_ranked, &vsim_ranked, alpha, beta, window),
    };

    // ── Apply user-specified sort or fall through to fusion order ─────────
    //
    // Default behaviour: results come out of rrf_fuse / linear_fuse already
    // sorted by hybrid score descending. SORTBY overrides with a named
    // field. NOSORT keeps fusion order (effectively a no-op since fusion
    // already produced sorted output, but documented here for clarity).
    let mut all_with_components: Vec<(String, f64, f64, f64)> = {
        // Build per-row score tuples (hybrid, text, vsim) so SORTBY can
        // pick any of them by name. Done before pagination because SORTBY
        // sorts the full set, then LIMIT pages.
        let text_lookup: AHashMap<&str, f64> =
            text_ranked.iter().map(|(k, s)| (k.as_str(), *s)).collect();
        let vsim_lookup: AHashMap<&str, f64> =
            vsim_ranked.iter().map(|(k, s)| (k.as_str(), *s)).collect();

        fused
            .into_iter()
            .map(|(key, hybrid)| {
                let t = text_lookup.get(key.as_str()).copied().unwrap_or(0.0);
                let v = vsim_lookup.get(key.as_str()).copied().unwrap_or(0.0);
                (key, hybrid, t, v)
            })
            .collect()
    };

    type ScoreGetterType = Option<fn(&(String, f64, f64, f64)) -> f64>;

    match &q.sort {
        None => {
            // Default: already sorted by hybrid descending — nothing to do.
        }
        Some(SortSpec::None) => {
            // NOSORT: leave fusion order untouched.
        }
        Some(SortSpec::By { field, descending }) => {
            // Built-in score fields first — these are already in the tuple.
            let score_getter: ScoreGetterType = if field == q.hybrid_alias() {
                Some(|t| t.1)
            } else if field == q.text_alias() {
                Some(|t| t.2)
            } else if field == q.vsim_alias() {
                Some(|t| t.3)
            } else {
                None
            };

            if let Some(get) = score_getter {
                if *descending {
                    all_with_components.sort_by(|a, b| get(b).total_cmp(&get(a)));
                } else {
                    all_with_components.sort_by(|a, b| get(a).total_cmp(&get(b)));
                }
            } else {
                // Hash field — need to read the value per row before sort.
                // We parse as f64; non-numeric or absent fields sort as
                // -inf (they sink to the bottom on DESC, top on ASC, which
                // matches Redis's default of "missing < any number").
                let db_ref = &ctx.store[*ctx.selected_db];
                let mut keyed: Vec<(f64, (String, f64, f64, f64))> =
                    Vec::with_capacity(all_with_components.len());
                for row in all_with_components.into_iter() {
                    let v = read_numeric_field(db_ref, &def_clone, &row.0, field);
                    keyed.push((v, row));
                }
                if *descending {
                    keyed.sort_by(|a, b| b.0.total_cmp(&a.0));
                } else {
                    keyed.sort_by(|a, b| a.0.total_cmp(&b.0));
                }
                all_with_components = keyed.into_iter().map(|(_, r)| r).collect();
            }
        }
    }

    let page: Vec<(String, f64, f64, f64)> = all_with_components
        .into_iter()
        .skip(q.limit_offset)
        .take(q.limit_count)
        .collect();

    // ── Load hash fields if requested ─────────────────────────────────────
    //
    // For each paged result, fetch its hash and pull the requested fields.
    // We do this once per result, taking the shard read lock briefly each
    // time. The alternative — pre-fetching all results before sort — would
    // duplicate work when LIMIT is small relative to the total.
    // ── Build per-row materialised data ───────────────────────────────────
    //
    // Convert the paged result into a Vec<Row> so we can either:
    //   (a) hand off to the aggregation pipeline if there's a tail
    //   (b) emit the score-and-load response directly
    //
    // Each row carries the score fields (always) and the LOAD'd hash
    // fields (if LOAD specified). __key holds the doc key for the pipeline
    // to use; we strip it before emitting.

    let db_ref = &ctx.store[*ctx.selected_db];

    let mut rows: Vec<crate::commands::aggregate_pipeline::Row> = Vec::with_capacity(page.len());

    for (key, hybrid_s, text_s, vsim_s) in &page {
        let mut row = ahash::AHashMap::new();
        row.insert("__key".to_string(), key.clone());
        row.insert(q.hybrid_alias().to_string(), format!("{hybrid_s:.6}"));
        row.insert(q.text_alias().to_string(), format!("{text_s:.6}"));
        row.insert(q.vsim_alias().to_string(), format!("{vsim_s:.6}"));

        if let Some(spec) = &q.load {
            let value = crate::store::get_shard(db_ref, key)
                .read()
                .get(key.as_str())
                .map(ValueRef::to_value);
            let inner = value.as_ref().and_then(|value| {
                crate::vector::doc_fields(&def_clone.on, &def_clone.fields, value.view())
            });
            if let Some(inner) = inner {
                // A field of a JSON document goes by the name the schema
                // gives its path; a hash field by its own.
                let json = matches!(def_clone.on, crate::vector::IndexOn::Json);
                match spec {
                    LoadSpec::All => {
                        for (fname, fval) in inner.iter() {
                            let shown =
                                match def_clone.fields.iter().find(|f| json && &f.name == fname) {
                                    Some(f) => f.attribute(),
                                    None => fname.as_str(),
                                };
                            // Score aliases take precedence over fields of
                            // the same name — don't overwrite.
                            if row.contains_key(shown) {
                                continue;
                            }
                            // Text only: a vector's bytes are not a value.
                            if let Ok(s) = std::str::from_utf8(fval) {
                                row.insert(shown.to_string(), s.to_string());
                            }
                        }
                    }
                    LoadSpec::Fields(names) => {
                        for name in names {
                            if row.contains_key(name) {
                                continue;
                            }
                            let stored = def_clone
                                .field_by_attribute(name)
                                .map_or(name.as_str(), |f| f.name.as_str());
                            if let Some(val) = inner.get(stored) {
                                let s = std::str::from_utf8(val).unwrap_or("").to_string();
                                row.insert(name.clone(), s);
                            }
                        }
                    }
                }
            }
        }

        rows.push(row);
    }

    // ── Run the aggregation pipeline if there's a tail ────────────────────
    //
    // GROUPBY/APPLY/FILTER (and any SORTBY/LIMIT they may carry) live in
    // aggregate_pipeline::run. Without a tail, rows are already paged and
    // ready to emit.
    if let Some(start) = q.pipeline_start {
        rows = crate::commands::aggregate_pipeline::run(rows, &ctx.cmd, start);
    }

    // ── Emit RESP response ─────────────────────────────────────────────────
    //
    // Format: [count, key1, [fields1], key2, [fields2], ...]
    // After GROUPBY there's no per-doc key (rows are aggregated groups),
    // so we emit them without a key element — same as FT.AGGREGATE does.

    let had_groupby = q.pipeline_start.is_some()
        && rows
            .first()
            .map(|r| !r.contains_key("__key"))
            .unwrap_or(false);

    let mut out: Vec<Vec<u8>> = Vec::new();
    out.push(resp_integer(rows.len() as i64));

    for row in rows {
        if had_groupby {
            // Aggregated group rows: emit as a single flat array of
            // alternating field/value, no leading key.
            let mut row_out: Vec<Vec<u8>> = Vec::new();
            for (k, v) in row {
                if k.starts_with("__") {
                    continue;
                }
                row_out.push(resp_bulk_str(&k));
                row_out.push(crate::commands::aggregate_pipeline::value_reply(
                    &v, ctx.resp,
                ));
            }
            out.push(resp_array(row_out));
        } else {
            // Per-doc rows: emit key followed by a fields array.
            let key = row.get("__key").cloned().unwrap_or_default();
            out.push(resp_bulk_str(&key));

            let mut row_out: Vec<Vec<u8>> = Vec::new();
            for (k, v) in row {
                if k == "__key" || k.starts_with("__") {
                    continue;
                }
                row_out.push(resp_bulk_str(&k));
                row_out.push(resp_bulk_str(&v));
            }
            out.push(resp_array(row_out));
        }
    }

    Some(resp_array(out))
}
