// src/commands/search.rs
//
// FT.CREATE's schema, FT.INFO, and FT.SEARCH: the arguments, running a parsed
// query (`vector::filter`) against an index, and the reply.

use crate::store::ValueRef;
use ahash::{AHashMap, AHashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::commands::ctx::Ctx;
use crate::resp::write::*;
use crate::store::Db;
use crate::types::KiviDBValue;
use crate::vector::filter::{self, Filter, Params, TextMatch};
use crate::vector::{
    DistanceMetric, FieldOptions, FieldType, IndexDef, IndexOn, IndexSchema, LiveIndex,
    SchemaField, VectorAlgo, VectorType,
};

use crate::vector::Fields;

fn text(arg: &[u8]) -> &str {
    std::str::from_utf8(arg).unwrap_or("")
}

fn is(arg: &[u8], word: &str) -> bool {
    arg.eq_ignore_ascii_case(word.as_bytes())
}

// ── Schema ────────────────────────────────────────────────────────────────────

/// `VECTOR <algo> <attr_count> [attr value …]`, after the word VECTOR.
fn parse_vector_spec(field: &str, cmd: &[&[u8]], i: &mut usize) -> Result<FieldType, String> {
    let algo_name = cmd
        .get(*i)
        .map(|a| text(a).to_ascii_uppercase())
        .ok_or_else(|| format!("VECTOR field `{field}` is missing its algorithm"))?;
    *i += 1;
    // The count is the number of attribute words that follow. Clients are not
    // careful with it — some count only the required ones — so it is checked
    // for being a number and the attributes are then read by name.
    let declared = cmd
        .get(*i)
        .and_then(|a| text(a).parse::<usize>().ok())
        .ok_or_else(|| format!("VECTOR field `{field}`: the attribute count must be a number"))?;
    *i += 1;
    if declared < 6 {
        return Err(format!(
            "VECTOR field `{field}` requires at least 6 attribute words (TYPE, DIM, \
             DISTANCE_METRIC)"
        ));
    }

    let mut dim: Option<usize> = None;
    let mut metric: Option<DistanceMetric> = None;
    let mut data_type = VectorType::Float32;
    let mut m = 16usize;
    let mut ef_construction = 200usize;
    let mut ef_runtime = 10usize;
    while *i + 1 < cmd.len() {
        let key = text(cmd[*i]).to_ascii_uppercase();
        let value = text(cmd[*i + 1]).to_ascii_uppercase();
        let number = || {
            value.parse::<usize>().map_err(|_| {
                format!("VECTOR field `{field}`: {key} must be a number, got `{value}`")
            })
        };
        match key.as_str() {
            // Redis 8.2's reply; 8.10 puts `SEARCH_PARSE_ARGS` in front of it.
            "TYPE" => {
                data_type = VectorType::parse(&value).ok_or_else(|| {
                    format!(
                        "Bad arguments for vector similarity {algo_name} index `TYPE`: Unknown \
                         argument"
                    )
                })?;
            }
            "DIM" => dim = Some(number()?),
            "DISTANCE_METRIC" => {
                metric = Some(DistanceMetric::metric_from_str(&value).ok_or_else(|| {
                    format!(
                        "VECTOR field `{field}`: invalid DISTANCE_METRIC `{value}` (use L2, \
                         COSINE or IP)"
                    )
                })?);
            }
            "M" => m = number()?,
            "EF_CONSTRUCTION" => ef_construction = number()?,
            "EF_RUNTIME" => ef_runtime = number()?,
            // Sizing and approximation hints that change no result.
            "INITIAL_CAP" | "BLOCK_SIZE" | "EPSILON" => {}
            _ => break,
        }
        *i += 2;
    }
    let dim = dim
        .filter(|d| *d > 0)
        .ok_or_else(|| format!("VECTOR field `{field}` is missing DIM"))?;
    let algo = match algo_name.as_str() {
        "FLAT" => VectorAlgo::Flat,
        "HNSW" => VectorAlgo::Hnsw {
            m,
            ef_construction,
            ef_runtime,
        },
        other => {
            return Err(format!(
                "VECTOR field `{field}`: unknown algorithm `{other}` (use FLAT or HNSW)"
            ));
        }
    };
    Ok(FieldType::Vector {
        algo,
        dim,
        metric: metric.unwrap_or(DistanceMetric::L2),
        data_type,
    })
}

/// `<name> [AS <attribute>] <type> [options…]`, repeated to the end of the
/// command. Shared by `FT.CREATE … SCHEMA` and `FT.ALTER … SCHEMA ADD`.
pub(crate) fn parse_schema_fields(
    cmd: &[&[u8]],
    i: &mut usize,
) -> Result<Vec<SchemaField>, String> {
    let mut fields: Vec<SchemaField> = Vec::new();
    while *i < cmd.len() {
        let name = std::str::from_utf8(cmd[*i])
            .map_err(|_| "Field names must be valid UTF-8".to_string())?
            .to_string();
        *i += 1;

        let mut options = FieldOptions::default();
        if cmd.get(*i).is_some_and(|a| is(a, "AS")) {
            let alias = cmd
                .get(*i + 1)
                .ok_or_else(|| format!("Field `{name}`: AS needs a name"))?;
            options.alias = Some(text(alias).to_string());
            *i += 2;
        }

        let Some(type_word) = cmd.get(*i).map(|a| text(a).to_ascii_uppercase()) else {
            return Err(match crate::vector::field_option_owner(&name) {
                // `… f TAG WEIGHT`: the option was taken for a field name.
                Some(owner) => misplaced_option(&name, owner, fields.last()),
                None => format!("Field `{name}` does not have a type"),
            });
        };
        *i += 1;
        let field_type = match type_word.as_str() {
            "TEXT" => FieldType::Text,
            "TAG" => FieldType::Tag,
            "NUMERIC" => FieldType::Numeric,
            "VECTOR" => parse_vector_spec(&name, cmd, i)?,
            "GEO" | "GEOSHAPE" => {
                return Err(format!(
                    "Field `{name}`: {type_word} fields are not supported"
                ));
            }
            other => {
                // The usual cause: an option that the previous field's type
                // does not take was read as a field name, and what followed
                // it as that field's type.
                return Err(match crate::vector::field_option_owner(&name) {
                    Some(owner) => misplaced_option(&name, owner, fields.last()),
                    None => format!("Invalid field type `{other}` for field `{name}`"),
                });
            }
        };
        options.consume(&name, &field_type, cmd, i)?;

        let field = SchemaField {
            name,
            field_type,
            options,
        };
        if fields.iter().any(|f| f.attribute() == field.attribute()) {
            return Err(format!("Duplicate field in schema - {}", field.attribute()));
        }
        fields.push(field);
    }
    Ok(fields)
}

fn misplaced_option(option: &str, owner: &str, previous: Option<&SchemaField>) -> String {
    let option = option.to_ascii_uppercase();
    match previous {
        Some(field) => format!(
            "Unsupported option `{option}` for {} field `{}`: it is an option of {owner} fields",
            field.type_name(),
            field.attribute()
        ),
        None => format!("`{option}` is a field option, not a field name"),
    }
}

/// A field as `FT.INFO` describes it, and as `FT.CREATE` would be given it.
fn describe_field(field: &SchemaField) -> Vec<Vec<u8>> {
    let mut out = vec![
        resp_bulk_str("identifier"),
        resp_bulk_str(&field.name),
        resp_bulk_str("attribute"),
        resp_bulk_str(field.attribute()),
        resp_bulk_str("type"),
        resp_bulk_str(field.type_name()),
    ];
    if let FieldType::Vector {
        algo,
        dim,
        metric,
        data_type,
    } = &field.field_type
    {
        let pairs = [
            (
                "algorithm",
                match algo {
                    VectorAlgo::Flat => "FLAT".to_string(),
                    VectorAlgo::Hnsw { .. } => "HNSW".to_string(),
                },
            ),
            ("data_type", data_type.name().to_string()),
            ("dim", dim.to_string()),
            (
                "distance_metric",
                match metric {
                    DistanceMetric::L2 => "L2",
                    DistanceMetric::Cosine => "COSINE",
                    DistanceMetric::InnerProduct => "IP",
                }
                .to_string(),
            ),
        ];
        for (name, value) in pairs {
            out.push(resp_bulk_str(name));
            // `dim` is a number, as Redis returns it.
            match value.parse::<i64>() {
                Ok(n) if name == "dim" => out.push(resp_integer(n)),
                _ => out.push(resp_bulk_str(&value)),
            }
        }
        if let VectorAlgo::Hnsw {
            m, ef_construction, ..
        } = algo
        {
            out.push(resp_bulk_str("M"));
            out.push(resp_integer(*m as i64));
            out.push(resp_bulk_str("ef_construction"));
            out.push(resp_integer(*ef_construction as i64));
        }
    }
    for word in field.options.words(&field.field_type) {
        out.push(resp_bulk_str(&word));
    }
    out
}

// ── FT.CREATE ─────────────────────────────────────────────────────────────────

pub(crate) fn ft_create(ctx: &mut Ctx) -> Option<Vec<u8>> {
    Some(match create(ctx) {
        Ok(()) => resp_simple("OK"),
        Err(e) => resp_error(&e),
    })
}

fn create(ctx: &mut Ctx) -> Result<(), String> {
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return Err("ERR wrong number of arguments for 'FT.CREATE' command".into());
    }
    let name = std::str::from_utf8(cmd[1])
        .map_err(|_| "Index names must be valid UTF-8".to_string())?
        .to_string();

    let mut on = IndexOn::Hash;
    let mut prefixes: Vec<String> = Vec::new();
    let mut stopwords: Option<Vec<String>> = None;
    let mut skip_initial_scan = false;
    let mut schema_found = false;
    let mut i = 2usize;
    // A count, then that many words.
    let counted = |what: &str, i: &mut usize| -> Result<Vec<String>, String> {
        let n = cmd
            .get(*i + 1)
            .and_then(|a| text(a).parse::<usize>().ok())
            .ok_or_else(|| format!("Bad arguments for {what}: expected a count"))?;
        let words: Vec<String> = cmd
            .iter()
            .skip(*i + 2)
            .take(n)
            .map(|a| text(a).to_string())
            .collect();
        if words.len() != n {
            return Err(format!(
                "Bad arguments for {what}: {n} values were announced"
            ));
        }
        *i += 2 + n;
        Ok(words)
    };
    while i < cmd.len() {
        let word = text(cmd[i]).to_ascii_uppercase();
        match word.as_str() {
            "SCHEMA" => {
                i += 1;
                schema_found = true;
                break;
            }
            "ON" => {
                on = match cmd
                    .get(i + 1)
                    .map(|a| text(a).to_ascii_uppercase())
                    .as_deref()
                {
                    Some("HASH") => IndexOn::Hash,
                    Some("JSON") => IndexOn::Json,
                    _ => return Err("Invalid rule type: use ON HASH or ON JSON".into()),
                };
                i += 2;
            }
            "PREFIX" => prefixes = counted("PREFIX", &mut i)?,
            "STOPWORDS" => {
                stopwords = Some(
                    counted("STOPWORDS", &mut i)?
                        .into_iter()
                        .map(|w| w.to_lowercase())
                        .collect(),
                );
            }
            "SKIPINITIALSCAN" => {
                skip_initial_scan = true;
                i += 1;
            }
            // Ranking and storage hints with no counterpart here. They
            // change no result a query can see.
            "LANGUAGE" | "LANGUAGE_FIELD" | "SCORE" | "SCORE_FIELD" | "PAYLOAD_FIELD"
            | "TEMPORARY" => {
                if cmd.get(i + 1).is_none() {
                    return Err(format!("Bad arguments for {word}: expected a value"));
                }
                i += 2;
            }
            "MAXTEXTFIELDS" | "NOOFFSETS" | "NOHL" | "NOFIELDS" | "NOFREQS" => i += 1,
            // This one would change results: an index of only the documents
            // an expression accepts. Accepting it and indexing everything
            // would be wrong.
            "FILTER" => return Err("FILTER is not supported by FT.CREATE".into()),
            _ => {
                return Err(format!(
                    "Unknown argument `{}` at position {i} for <main>",
                    text(cmd[i])
                ));
            }
        }
    }
    if !schema_found {
        return Err("No schema found".into());
    }
    let fields = parse_schema_fields(cmd, &mut i)?;
    if fields.is_empty() {
        return Err("Fields arguments are missing".into());
    }

    let mut def = IndexSchema {
        name: name.clone(),
        db_index: *ctx.selected_db,
        on,
        prefixes,
        fields,
        aliases: Vec::new(),
        stopwords,
        synonyms: Default::default(),
    }
    .into_def();

    if ctx.index_store.read().contains_key(&name) {
        return Err("Index already exists".into());
    }
    if !skip_initial_scan {
        crate::vector::backfill_from_db(&ctx.store[*ctx.selected_db], &def);
    }
    let mut store = ctx.index_store.write();
    if store.contains_key(&name) {
        return Err("Index already exists".into());
    }
    def.name = name.clone();
    store.insert(name, def);
    Ok(())
}

// ── FT.INFO ───────────────────────────────────────────────────────────────────

/// Documents the index covers: hashes, or JSON documents, under its prefixes.
fn count_docs(db: &Db, def: &IndexDef, cluster: Option<&crate::cluster::ClusterState>) -> usize {
    db.iter()
        .map(|shard| {
            shard
                .read()
                .iter()
                .filter(|(key, value)| {
                    crate::vector::is_document(&def.on, *value)
                        && covers(def, key)
                        && cluster.is_none_or(|c| crate::cluster::search::serves(c, key))
                })
                .count()
        })
        .sum()
}

fn covers(def: &IndexDef, key: &str) -> bool {
    def.prefixes.is_empty() || def.prefixes.iter().any(|p| key.starts_with(p.as_str()))
}

pub(crate) fn ft_info(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'FT.INFO' command",
        ));
    }
    let name = text(cmd[1]);
    let Some(def) = ctx.index_store.read().get(name).cloned() else {
        return Some(resp_error(&no_such_index(name)));
    };
    let def = &def;
    let resp = ctx.resp;
    let cluster = ctx.cluster.enabled.then_some(&**ctx.cluster);
    let mut num_docs = count_docs(&ctx.store[*ctx.selected_db], def, cluster);
    // In a cluster, the documents of every master.
    if crate::cluster::search::coordinates(ctx) {
        let owned: Vec<Vec<u8>> = ctx.cmd.iter().map(|a| a.to_vec()).collect();
        match crate::cluster::search::fan_out(ctx, &owned) {
            Ok(replies) => {
                for reply in replies {
                    if let crate::cluster::search::Reply::Array(items) = reply {
                        let at = items
                            .iter()
                            .position(|item| item.text().as_deref() == Some("num_docs"));
                        if let Some(crate::cluster::search::Reply::Int(n)) =
                            at.and_then(|at| items.get(at + 1))
                        {
                            num_docs += (*n).max(0) as usize;
                        }
                    }
                }
            }
            Err(e) => return Some(resp_error(&e)),
        }
    }

    let mut out: Vec<Vec<u8>> = Vec::new();
    let mut put = |key: &str, value: Vec<u8>| {
        out.push(resp_bulk_str(key));
        out.push(value);
    };
    put("index_name", resp_bulk_str(&def.name));
    put("index_options", resp_array(vec![]));
    put(
        "index_definition",
        resp_map_for(
            resp,
            vec![
                resp_bulk_str("key_type"),
                resp_bulk_str(match def.on {
                    IndexOn::Hash => "HASH",
                    IndexOn::Json => "JSON",
                }),
                resp_bulk_str("prefixes"),
                resp_array(def.prefixes.iter().map(|p| resp_bulk_str(p)).collect()),
                resp_bulk_str("default_score"),
                resp_bulk_str("1"),
            ],
        ),
    );
    put(
        "attributes",
        resp_array(
            def.fields
                .iter()
                .map(|field| resp_array(describe_field(field)))
                .collect(),
        ),
    );
    put("num_docs", resp_integer(num_docs as i64));
    put("max_doc_id", resp_integer(num_docs as i64));
    put("num_terms", resp_integer(def.vocab.read().len() as i64));
    put(
        "num_records",
        resp_integer(def.inverted.read().values().map(|s| s.len()).sum::<usize>() as i64),
    );
    put("indexing", resp_integer(0));
    put("percent_indexed", resp_bulk_str("1"));
    put("hash_indexing_failures", resp_integer(0));
    if let Some(words) = &def.stopwords {
        put(
            "stopwords_list",
            resp_array(words.iter().map(|w| resp_bulk_str(w)).collect()),
        );
    }

    // What this reply had before it was given Redis's shape. Kept, for
    // anything that reads it.
    put("index_on", {
        resp_bulk_str(match def.on {
            IndexOn::Hash => "hash",
            IndexOn::Json => "json",
        })
    });
    put(
        "prefixes",
        resp_array(def.prefixes.iter().map(|p| resp_bulk_str(p)).collect()),
    );
    put("num_fields", resp_bulk_str(&def.fields.len().to_string()));
    put(
        "fields",
        resp_array(
            def.fields
                .iter()
                .map(|f| {
                    let label = match &f.field_type {
                        FieldType::Vector {
                            algo,
                            dim,
                            metric,
                            data_type,
                        } => {
                            let algo = match algo {
                                VectorAlgo::Flat => "FLAT".to_string(),
                                VectorAlgo::Hnsw {
                                    m, ef_construction, ..
                                } => format!("HNSW(M={m},ef={ef_construction})"),
                            };
                            let metric = match metric {
                                DistanceMetric::L2 => "L2",
                                DistanceMetric::Cosine => "COSINE",
                                DistanceMetric::InnerProduct => "IP",
                            };
                            // As it was for FLOAT32, which was all there was.
                            match data_type {
                                VectorType::Float32 => format!("VECTOR {algo} DIM={dim} {metric}"),
                                ty => format!("VECTOR {algo} {} DIM={dim} {metric}", ty.name()),
                            }
                        }
                        _ => f.type_name().to_string(),
                    };
                    resp_bulk_str(&format!("{} ({})", f.name, label))
                })
                .collect(),
        ),
    );

    // ── HNSW stats: one entry per HNSW field, and flat keys for the first.
    use std::sync::atomic::Ordering::Relaxed;
    let graphs: Vec<(&str, &crate::vector::HnswIndex)> = def
        .fields
        .iter()
        .filter_map(|f| match def.indexes.get(&f.name) {
            Some(LiveIndex::Hnsw(graph)) => Some((f.name.as_str(), graph.as_ref())),
            _ => None,
        })
        .collect();
    if let Some((_, first)) = graphs.first() {
        put(
            "hnsw_live_count",
            resp_bulk_str(&first.live_count.load(Relaxed).to_string()),
        );
        put(
            "hnsw_tombstone_count",
            resp_bulk_str(&first.tombstone_count.load(Relaxed).to_string()),
        );
        put(
            "hnsw_tombstone_ratio",
            resp_bulk_str(&format!("{:.4}", first.tombstone_ratio())),
        );
        put(
            "hnsw_compaction_in_progress",
            resp_bulk_str(if first.compaction_in_progress.load(Relaxed) {
                "1"
            } else {
                "0"
            }),
        );
        put(
            "hnsw_indexes",
            resp_array(
                graphs
                    .iter()
                    .map(|(field, graph)| {
                        resp_array(vec![
                            resp_bulk_str("field"),
                            resp_bulk_str(field),
                            resp_bulk_str("live_count"),
                            resp_bulk_str(&graph.live_count.load(Relaxed).to_string()),
                            resp_bulk_str("tombstone_count"),
                            resp_bulk_str(&graph.tombstone_count.load(Relaxed).to_string()),
                            resp_bulk_str("tombstone_ratio"),
                            resp_bulk_str(&format!("{:.4}", graph.tombstone_ratio())),
                            resp_bulk_str("compaction_in_progress"),
                            resp_bulk_str(if graph.compaction_in_progress.load(Relaxed) {
                                "1"
                            } else {
                                "0"
                            }),
                        ])
                    })
                    .collect(),
            ),
        );
    }

    Some(resp_map_for(resp, out))
}

pub(crate) fn no_such_index(name: &str) -> String {
    format!("{name}: no such index")
}

// ── Running a filter ──────────────────────────────────────────────────────────

/// One document a query matched.
pub(crate) struct Match {
    pub key: String,
    /// Relevance: BM25 over the query's words; 1 for a query with none.
    pub score: f64,
    /// The distance a `VECTOR_RANGE` clause measured, if the query has one.
    pub distance: Option<f32>,
}

/// Documents a filter accepted, and whether the scan stopped because the
/// query's timeout ran out.
pub(crate) struct FilterHits {
    pub matches: Vec<Match>,
    pub timed_out: bool,
}

/// When the query must stop. `timeout_ms` is the command's `TIMEOUT`, or
/// the engine's `FT.CONFIG TIMEOUT` (`search-timeout`) when the command did
/// not name one. `0` is no limit, and the default.
pub(crate) fn search_deadline(timeout_ms: Option<u64>) -> Option<Instant> {
    let ms =
        timeout_ms.unwrap_or_else(|| crate::commands::search_tools::config_number("TIMEOUT", 0));
    if ms == 0 {
        None
    } else {
        // A limit further off than the clock can count is none: adding it
        // would panic where the clock counts less far.
        Instant::now().checked_add(Duration::from_millis(ms))
    }
}

pub(crate) fn past_deadline(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|d| Instant::now() >= d)
}

/// Documents between clock reads during a scan. A million-document query
/// then pays a thousand reads, not a million.
pub(crate) const DEADLINE_EVERY: u32 = 1024;

/// True when `deadline` has passed, reading the clock every [`DEADLINE_EVERY`]
/// documents. `seen` is how many have been looked at since the last read.
pub(crate) fn deadline_due(deadline: Option<Instant>, seen: &mut u32) -> bool {
    if deadline.is_none() {
        return false;
    }
    *seen += 1;
    if *seen < DEADLINE_EVERY {
        return false;
    }
    *seen = 0;
    past_deadline(deadline)
}

/// Whether a step over `n` items that cannot stop part-way (a sort) is to
/// be left out because the deadline has passed. Fewer than
/// [`DEADLINE_EVERY`] items cost less than reading the clock, and always
/// run: a query that small is never reported as cut short.
pub(crate) fn no_time_for(deadline: Option<Instant>, n: usize) -> bool {
    n >= DEADLINE_EVERY as usize && past_deadline(deadline)
}

/// `ON_TIMEOUT fail` and `return_strict` error instead of returning what
/// was found.
pub(crate) fn on_timeout_fails() -> bool {
    crate::commands::search_tools::on_timeout_fails()
}

/// The exact words a query looks for: what relevance is scored on, and what
/// `HIGHLIGHT` marks.
pub(crate) fn query_terms(filter: &Filter, out: &mut AHashSet<String>) {
    match filter {
        Filter::Text {
            term: TextMatch::Exact(term),
            ..
        } => {
            out.insert(term.clone());
        }
        Filter::Phrase { terms, .. } => out.extend(terms.iter().cloned()),
        Filter::And(l, r) | Filter::Or(l, r) => {
            query_terms(l, out);
            query_terms(r, out);
        }
        Filter::Optional(inner) => query_terms(inner, out),
        _ => {}
    }
}

/// The documents that could match, read off the inverted index — or `None`
/// if the filter has to be tried on every document.
///
/// What comes back is a superset: every key is evaluated before it counts.
/// (The index is only ever added to as documents change, so it may name a
/// document that no longer holds the word.)
fn candidates(filter: &Filter, def: &IndexDef) -> Option<AHashSet<String>> {
    let postings = |fields: &[String], pick: &dyn Fn(&str) -> bool, exact: Option<&str>| {
        let inverted = def.inverted.read();
        let mut keys: AHashSet<String> = AHashSet::new();
        for field in fields {
            match exact {
                Some(term) => {
                    if let Some(set) = inverted.get(&format!("{field}\x00{term}")) {
                        keys.extend(set.iter().cloned());
                    }
                }
                None => {
                    let start = format!("{field}\x00");
                    for (entry, set) in inverted.iter() {
                        if entry.strip_prefix(&start).is_some_and(pick) {
                            keys.extend(set.iter().cloned());
                        }
                    }
                }
            }
        }
        keys
    };
    match filter {
        Filter::MatchNone => Some(AHashSet::new()),
        Filter::Text { fields, term } => Some(match term {
            TextMatch::Exact(t) => postings(fields, &|_| false, Some(t)),
            TextMatch::Prefix(p) => postings(fields, &|tok| tok.starts_with(p.as_str()), None),
            TextMatch::Suffix(s) => postings(fields, &|tok| tok.ends_with(s.as_str()), None),
            TextMatch::Infix(s) => postings(fields, &|tok| tok.contains(s.as_str()), None),
            // Every word of the vocabulary would have to be measured against
            // it; trying the documents costs the same.
            TextMatch::Fuzzy(..) => return None,
        }),
        Filter::Phrase { fields, terms } => {
            // Stopwords hold a place in the phrase but are in no index.
            let mut sets = terms
                .iter()
                .filter(|t| !def.is_stopword(t))
                .map(|t| postings(fields, &|_| false, Some(t)));
            let first = sets.next()?;
            Some(sets.fold(first, |acc, set| {
                acc.into_iter().filter(|k| set.contains(k)).collect()
            }))
        }
        Filter::And(l, r) => match (candidates(l, def), candidates(r, def)) {
            (Some(a), Some(b)) => Some(a.into_iter().filter(|k| b.contains(k)).collect()),
            (Some(a), None) | (None, Some(a)) => Some(a),
            (None, None) => None,
        },
        Filter::Or(l, r) => match (candidates(l, def), candidates(r, def)) {
            (Some(mut a), Some(b)) => {
                a.extend(b);
                Some(a)
            }
            _ => None,
        },
        _ => None,
    }
}

/// BM25 relevance of one document for the query's words.
///
///   score(d, Q) = Σ_t  IDF(t) · tf_norm(t, d) · weight(field)
///
///   IDF(t)       = ln((N - df(t) + 0.5) / (df(t) + 0.5) + 1)
///   tf_norm(t,d) = tf · (k1 + 1) / (tf + k1 · (1 - b + b · dl/avgdl))
pub(crate) fn relevance(def: &IndexDef, fields: &Fields, terms: &AHashSet<String>) -> f64 {
    const K1: f64 = 1.2;
    const B: f64 = 0.75;
    if terms.is_empty() {
        return 1.0;
    }
    use std::sync::atomic::Ordering::Relaxed;
    let n = def.doc_count.load(Relaxed).max(1) as f64;
    let avgdl = (def.total_tokens.load(Relaxed) as f64 / n).max(1.0);
    let df_map = def.doc_freq.read();

    let mut score = 0.0f64;
    for field in &def.fields {
        if !matches!(field.field_type, FieldType::Text) || field.options.noindex {
            continue;
        }
        let Some(value) = fields
            .get(&field.name)
            .and_then(|b| std::str::from_utf8(b).ok())
        else {
            continue;
        };
        let mut tf: AHashMap<&str, usize> = AHashMap::new();
        let mut dl = 0usize;
        for token in crate::vector::tokenize_for_vocab(value) {
            if def.is_stopword(&token) {
                continue;
            }
            dl += 1;
            if let Some(term) = terms.get(&token) {
                *tf.entry(term.as_str()).or_insert(0) += 1;
            }
        }
        if dl == 0 {
            continue;
        }
        for (term, count) in tf {
            let df = df_map.get(term).copied().unwrap_or(1) as f64;
            let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
            let count = count as f64;
            let tf_norm = (count * (K1 + 1.0)) / (count + K1 * (1.0 - B + B * dl as f64 / avgdl));
            score += idf * tf_norm * field.options.weight;
        }
    }
    score.max(0.001)
}

/// Every document of the index that the filter accepts. In no particular
/// order. Stops when `deadline` has passed, and then `timed_out` is true.
pub(crate) fn filter_search(
    ctx: &Ctx,
    def: &IndexDef,
    filter: &Filter,
    only: Option<&AHashSet<String>>,
    deadline: Option<Instant>,
) -> FilterHits {
    let db = &ctx.store[*ctx.selected_db];
    let mut terms = AHashSet::new();
    query_terms(filter, &mut terms);
    let range = filter.vector_range();
    let mut out: Vec<Match> = Vec::new();
    let mut timed_out = false;
    let mut seen = 0u32;

    let mut consider = |key: &str, fields: &Fields| {
        if !filter::eval(filter, fields) {
            return;
        }
        out.push(Match {
            key: key.to_string(),
            score: relevance(def, fields, &terms),
            distance: range.and_then(|clause| filter::range_distance(clause, fields)),
        });
    };

    // The keys to look at, if they are known without reading everything: the
    // ones the caller named, or the ones the inverted index names.
    let known: Option<AHashSet<String>> = match (only, candidates(filter, def)) {
        (Some(only), Some(found)) => Some(found.into_iter().filter(|k| only.contains(k)).collect()),
        (Some(only), None) => Some(only.clone()),
        (None, found) => found,
    };
    match known {
        Some(keys) => {
            for key in &keys {
                if deadline_due(deadline, &mut seen) {
                    timed_out = true;
                    break;
                }
                if !covers(def, key) {
                    continue;
                }
                with_doc(db, def, key, |fields, _| consider(key, fields));
            }
        }
        None => {
            'shards: for shard in db.iter() {
                let map = shard.read();
                for (key, value) in map.iter() {
                    if deadline_due(deadline, &mut seen) {
                        timed_out = true;
                        break 'shards;
                    }
                    if !covers(def, key) {
                        continue;
                    }
                    if let Some(fields) = crate::vector::doc_fields(&def.on, &def.fields, value) {
                        consider(key, &fields);
                    }
                }
            }
        }
    }
    // A scan that reached the end is whole, however long it took: only one
    // that stopped early has timed out.
    FilterHits {
        matches: out,
        timed_out,
    }
}

/// Run `f` on the document at `key`, if there is one of the kind the index
/// covers: on its fields, and for a JSON document on the document too.
fn with_doc<R>(
    db: &Db,
    def: &IndexDef,
    key: &str,
    f: impl FnOnce(&Fields, Option<&serde_json::Value>) -> R,
) -> Option<R> {
    // Out of the shard, so that its lock is not held while `f` runs.
    let value = crate::store::get_shard(db, key)
        .read()
        .get(key)
        .map(ValueRef::to_value)?;
    match (&def.on, &value) {
        (IndexOn::Hash, KiviDBValue::Hash(hash)) => Some(f(&hash.read(), None)),
        (IndexOn::Hash, KiviDBValue::HashEx(hash)) => Some(f(&hash.read().fields, None)),
        (IndexOn::Json, KiviDBValue::Json(doc)) => {
            let doc = doc.read();
            Some(f(
                &crate::vector::project_json(&def.fields, &doc),
                Some(&doc),
            ))
        }
        _ => None,
    }
}

// ── FT.SEARCH arguments ───────────────────────────────────────────────────────

#[derive(Default, Debug)]
struct SearchArgs {
    params: Params,
    nocontent: bool,
    withscores: bool,
    withpayloads: bool,
    withsortkeys: bool,
    /// `RETURN`: (what to read, the name to return it under).
    returns: Option<Vec<(String, String)>>,
    /// `SORTBY`: the name, and whether ascending.
    sortby: Option<(String, bool)>,
    offset: usize,
    count: usize,
    /// `FILTER field min max`.
    numeric_filters: Vec<(String, String, String)>,
    inkeys: Option<AHashSet<String>>,
    /// `HIGHLIGHT`: the fields (empty: every TEXT field) and the two tags.
    highlight: Option<(Vec<String>, String, String)>,
    /// Command `TIMEOUT` in milliseconds. `None` uses `FT.CONFIG TIMEOUT`.
    timeout_ms: Option<u64>,
}

fn parse_search_args(cmd: &[&[u8]], start: usize) -> Result<SearchArgs, String> {
    let mut args = SearchArgs {
        count: 10,
        ..Default::default()
    };
    let mut i = start;
    let arg = |i: usize, what: &str| -> Result<&str, String> {
        cmd.get(i).map(|a| text(a)).ok_or_else(|| {
            format!("Bad arguments for {what}: expected an argument, but none provided")
        })
    };
    let number = |i: usize, what: &str| -> Result<usize, String> {
        arg(i, what)?
            .parse::<usize>()
            .map_err(|_| format!("Bad arguments for {what}: expected a non-negative integer"))
    };
    while i < cmd.len() {
        let word = text(cmd[i]).to_ascii_uppercase();
        match word.as_str() {
            "NOCONTENT" => {
                args.nocontent = true;
                i += 1;
            }
            "WITHSCORES" => {
                args.withscores = true;
                i += 1;
            }
            "WITHPAYLOADS" => {
                args.withpayloads = true;
                i += 1;
            }
            "WITHSORTKEYS" => {
                args.withsortkeys = true;
                i += 1;
            }
            // How words are expanded, and how phrases are held together:
            // none of it applies to an index without stemming or slop.
            "VERBATIM" | "NOSTOPWORDS" | "INORDER" => i += 1,
            "TIMEOUT" => {
                let raw = arg(i + 1, "TIMEOUT")?;
                args.timeout_ms = Some(raw.parse::<u64>().map_err(|_| {
                    "Bad arguments for TIMEOUT: expected a non-negative integer".to_string()
                })?);
                i += 2;
            }
            "SLOP" | "LANGUAGE" | "EXPANDER" | "PAYLOAD" => {
                arg(i + 1, &word)?;
                i += 2;
            }
            // One ranking function is implemented: BM25.
            "SCORER" => {
                arg(i + 1, "SCORER")?;
                i += 2;
            }
            "DIALECT" => {
                let dialect = number(i + 1, "DIALECT")?;
                if !(1..=4).contains(&dialect) {
                    return Err("DIALECT requires a non negative integer >=1 and <= 4".to_string());
                }
                i += 2;
            }
            "PARAMS" => {
                let n = number(i + 1, "PARAMS")?;
                if n % 2 != 0 {
                    return Err("Parameters must be specified in PARAM VALUE pairs".into());
                }
                if n > cmd.len().saturating_sub(i + 2) {
                    return Err("Bad arguments for PARAMS: not enough values".into());
                }
                for pair in cmd[i + 2..i + 2 + n].chunks(2) {
                    args.params
                        .insert(text(pair[0]).to_string(), pair[1].to_vec());
                }
                i += 2 + n;
            }
            "LIMIT" => {
                args.offset = number(i + 1, "LIMIT")?;
                args.count = number(i + 2, "LIMIT")?;
                let most =
                    crate::commands::search_tools::config_number("MAXSEARCHRESULTS", 1_000_000)
                        as usize;
                if args.count > most {
                    return Err(format!("LIMIT exceeds maximum of {most}"));
                }
                i += 3;
            }
            "SORTBY" => {
                let field = arg(i + 1, "SORTBY")?.to_string();
                i += 2;
                let mut ascending = true;
                if let Some(direction) = cmd.get(i) {
                    if is(direction, "ASC") {
                        i += 1;
                    } else if is(direction, "DESC") {
                        ascending = false;
                        i += 1;
                    }
                }
                args.sortby = Some((field.trim_start_matches('@').to_string(), ascending));
            }
            "RETURN" => {
                let n = number(i + 1, "RETURN")?;
                if n > cmd.len().saturating_sub(i + 2) {
                    return Err("Bad arguments for RETURN: not enough fields".into());
                }
                let words = &cmd[i + 2..i + 2 + n];
                let mut returns = Vec::new();
                let mut w = 0usize;
                while w < words.len() {
                    let source = text(words[w]).trim_start_matches('@').to_string();
                    w += 1;
                    let mut name = source.clone();
                    if words.get(w).is_some_and(|a| is(a, "AS")) {
                        name = words
                            .get(w + 1)
                            .map(|a| text(a).to_string())
                            .ok_or_else(|| {
                                "Bad arguments for RETURN: AS needs a name".to_string()
                            })?;
                        w += 2;
                    }
                    returns.push((source, name));
                }
                args.returns = Some(returns);
                i += 2 + n;
            }
            "FILTER" => {
                args.numeric_filters.push((
                    arg(i + 1, "FILTER")?.to_string(),
                    arg(i + 2, "FILTER")?.to_string(),
                    arg(i + 3, "FILTER")?.to_string(),
                ));
                i += 4;
            }
            "INKEYS" => {
                let n = number(i + 1, "INKEYS")?;
                if n > cmd.len().saturating_sub(i + 2) {
                    return Err("Bad arguments for INKEYS: not enough keys".into());
                }
                args.inkeys = Some(
                    cmd[i + 2..i + 2 + n]
                        .iter()
                        .map(|a| text(a).to_string())
                        .collect(),
                );
                i += 2 + n;
            }
            "HIGHLIGHT" => {
                i += 1;
                let mut fields = Vec::new();
                let (mut open, mut close) = ("<b>".to_string(), "</b>".to_string());
                loop {
                    if cmd.get(i).is_some_and(|a| is(a, "FIELDS")) {
                        let n = number(i + 1, "HIGHLIGHT FIELDS")?;
                        if n > cmd.len().saturating_sub(i + 2) {
                            return Err("Bad arguments for HIGHLIGHT: not enough fields".into());
                        }
                        fields = cmd[i + 2..i + 2 + n]
                            .iter()
                            .map(|a| text(a).to_string())
                            .collect();
                        i += 2 + n;
                    } else if cmd.get(i).is_some_and(|a| is(a, "TAGS")) {
                        open = arg(i + 1, "HIGHLIGHT TAGS")?.to_string();
                        close = arg(i + 2, "HIGHLIGHT TAGS")?.to_string();
                        i += 3;
                    } else {
                        break;
                    }
                }
                args.highlight = Some((fields, open, close));
            }
            "SUMMARIZE" => {
                // Fields are returned whole. The options are read so that
                // what follows them is.
                i += 1;
                loop {
                    if cmd.get(i).is_some_and(|a| is(a, "FIELDS")) {
                        i += 2 + number(i + 1, "SUMMARIZE FIELDS")?;
                    } else if cmd
                        .get(i)
                        .is_some_and(|a| is(a, "FRAGS") || is(a, "LEN") || is(a, "SEPARATOR"))
                    {
                        arg(i + 1, "SUMMARIZE")?;
                        i += 2;
                    } else {
                        break;
                    }
                }
            }
            // These would change which documents match, and are not
            // implemented. Ignoring them would return the wrong documents.
            "INFIELDS" | "GEOFILTER" | "EXPLAINSCORE" => {
                return Err(format!("{word} is not supported"));
            }
            _ => {
                return Err(format!(
                    "Unknown argument `{}` at position {i} for <main>",
                    text(cmd[i])
                ));
            }
        }
    }
    Ok(args)
}

// ── FT.SEARCH ─────────────────────────────────────────────────────────────────

/// One row of a result page, before it is written out.
struct Hit {
    key: String,
    score: f64,
    /// Values that come from the query, not the document: the distance,
    /// under the name the query gave it.
    extras: Vec<(String, String)>,
    sort_key: Option<SortKey>,
}

#[derive(Clone)]
enum SortKey {
    Number(f64),
    Text(String),
}

/// A float as Redis prints one: the shortest text that reads back the same.
pub(crate) fn number_text(value: f64) -> String {
    if value == value.trunc() && value.abs() < 1e15 {
        format!("{}", value as i64)
    } else {
        value.to_string()
    }
}

pub(crate) fn distance_text(distance: f32) -> String {
    if distance == 0.0 {
        "0".to_string()
    } else {
        distance.to_string()
    }
}

/// `text`, with each word that is one of `terms` wrapped in the two tags.
fn highlight(value: &str, terms: &AHashSet<String>, open: &str, close: &str) -> String {
    let mut out = String::with_capacity(value.len() + 16);
    let mut word = String::new();
    let flush = |word: &mut String, out: &mut String| {
        if word.is_empty() {
            return;
        }
        let token = crate::vector::tokenize_for_vocab(word).next();
        if token.is_some_and(|t| terms.contains(&t)) {
            out.push_str(open);
            out.push_str(word);
            out.push_str(close);
        } else {
            out.push_str(word);
        }
        word.clear();
    };
    let mut chars = value.chars();
    while let Some(c) = chars.next() {
        // An escaped character belongs to the word, as the tokenizer has it,
        // and the whole word is wrapped: `<b>users\.u_1</b>`. Redis 8.10
        // wraps `<b>users\</b>.u_1`, which cuts the word; KiviDB differs on
        // purpose here.
        if c == '\\' {
            word.push(c);
            if let Some(escaped) = chars.next() {
                word.push(escaped);
            }
        } else if crate::vector::is_token_separator(c) {
            flush(&mut word, &mut out);
            out.push(c);
        } else {
            word.push(c);
        }
    }
    flush(&mut word, &mut out);
    out
}

pub(crate) fn ft_search(ctx: &mut Ctx) -> Option<Vec<u8>> {
    Some(match search(ctx) {
        Ok(reply) => reply,
        Err(e) => resp_error(&e),
    })
}

fn search(ctx: &mut Ctx) -> Result<Vec<u8>, String> {
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return Err("ERR wrong number of arguments for 'FT.SEARCH' command".into());
    }
    let index_name = text(cmd[1]);
    let query_text =
        std::str::from_utf8(cmd[2]).map_err(|_| "The query must be valid UTF-8".to_string())?;
    let args = parse_search_args(cmd, 3)?;
    let deadline = search_deadline(args.timeout_ms);

    let def = ctx
        .index_store
        .read()
        .get(index_name)
        .cloned()
        .ok_or_else(|| no_such_index(index_name))?;
    let query = filter::parse_query(query_text, &args.params, &def)?;
    // Ordered by distance rather than relevance.
    let by_distance =
        query.knn.is_some() || (query.filter.vector_range().is_some() && !query.filter.has_text());
    // Asked by a coordinator, or answering this node's part for one.
    let shard = SHARD.with(|s| s.get()) || crate::cluster::search::is_forwarded(ctx);
    if !shard && crate::cluster::search::coordinates(ctx) {
        let owned: Vec<Vec<u8>> = ctx.cmd.iter().map(|a| a.to_vec()).collect();
        let nearest = query.knn.as_ref().map(|knn| knn.k);
        return coordinate(ctx, owned, &args, by_distance, nearest);
    }
    let mut filter = query.filter;
    for (name, lo, hi) in &args.numeric_filters {
        let field = def
            .field_by_attribute(name)
            .filter(|f| matches!(f.field_type, FieldType::Numeric))
            .ok_or_else(|| format!("FILTER: `{name}` is not a NUMERIC field of the index"))?;
        let bound = |raw: &str| -> Result<(f64, bool), String> {
            let (raw, exclusive) = match raw.strip_prefix('(') {
                Some(rest) => (rest, true),
                None => (raw, false),
            };
            let value = match raw.to_ascii_lowercase().as_str() {
                "-inf" => f64::NEG_INFINITY,
                "inf" | "+inf" => f64::INFINITY,
                other => other
                    .parse::<f64>()
                    .map_err(|_| format!("FILTER: `{raw}` is not a number"))?,
            };
            Ok((value, exclusive))
        };
        let (lo, lo_exclusive) = bound(lo)?;
        let (hi, hi_exclusive) = bound(hi)?;
        let clause = Filter::Numeric {
            field: field.name.clone(),
            lo,
            lo_exclusive,
            hi,
            hi_exclusive,
        };
        filter = match filter {
            Filter::MatchAll => clause,
            other => Filter::And(Box::new(other), Box::new(clause)),
        };
    }

    let db = &ctx.store[*ctx.selected_db];
    let mut terms = AHashSet::new();
    query_terms(&filter, &mut terms);

    // ── The matches, in the query's own order ────────────────────────────────
    let mut timed_out = false;
    let mut hits: Vec<Hit> = match &query.knn {
        Some(knn) => {
            let field = def
                .fields
                .iter()
                .find(|f| f.name == knn.field)
                .ok_or_else(|| "The vector field is gone from the index".to_string())?;
            let FieldType::Vector { metric, .. } = &field.field_type else {
                return Err("The vector field is gone from the index".into());
            };
            let live = def
                .indexes
                .get(&knn.field)
                .ok_or_else(|| "The vector field has no index".to_string())?;
            let nearest = if matches!(filter, Filter::MatchAll) && args.inkeys.is_none() {
                match live {
                    LiveIndex::Flat(index) => {
                        crate::vector::knn_flat(index, &knn.vector, knn.k, metric)
                    }
                    LiveIndex::Hnsw(graph) => {
                        let ef = knn.ef_runtime.unwrap_or(match &field.field_type {
                            FieldType::Vector {
                                algo: VectorAlgo::Hnsw { ef_runtime, .. },
                                ..
                            } => *ef_runtime,
                            _ => knn.k * 2,
                        });
                        crate::vector::knn_hnsw(graph, &knn.vector, knn.k, ef, metric)
                    }
                }
            } else {
                // The filter first, then the nearest among what it left:
                // exact, whatever the index type.
                let found = filter_search(ctx, &def, &filter, args.inkeys.as_ref(), deadline);
                let allowed: std::collections::HashSet<String> =
                    found.matches.into_iter().map(|m| m.key).collect();
                let (nearest, cut) = crate::commands::vector::knn_filtered(
                    live,
                    &knn.vector,
                    knn.k,
                    metric,
                    &allowed,
                    deadline,
                );
                timed_out = found.timed_out || cut;
                nearest
            };
            nearest
                .into_iter()
                .map(|found| Hit {
                    key: found.key,
                    score: f64::from(found.score),
                    extras: vec![(knn.alias.clone(), distance_text(found.score))],
                    sort_key: None,
                })
                .collect()
        }
        None => {
            let range_alias = match filter.vector_range() {
                Some(Filter::VectorRange { yield_as, .. }) => yield_as.clone(),
                _ => None,
            };
            let by_distance = filter.vector_range().is_some() && !filter.has_text();
            let found = filter_search(ctx, &def, &filter, args.inkeys.as_ref(), deadline);
            timed_out = found.timed_out;
            let mut matches = found.matches;
            // A sort cannot stop part-way. Past the deadline it is left
            // out, and what was found comes back in the order it was found.
            if no_time_for(deadline, matches.len()) {
                timed_out = true;
            } else if by_distance {
                // Nearest first.
                matches.sort_by(|a, b| {
                    a.distance
                        .unwrap_or(f32::MAX)
                        .total_cmp(&b.distance.unwrap_or(f32::MAX))
                        .then_with(|| a.key.cmp(&b.key))
                });
            } else {
                // Most relevant first.
                matches.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.key.cmp(&b.key)));
            }
            matches
                .into_iter()
                .map(|found| Hit {
                    // For a coordinator, what this node's order is by.
                    score: match found.distance {
                        Some(distance) if shard && by_distance => f64::from(distance),
                        _ => found.score,
                    },
                    extras: match (&range_alias, found.distance) {
                        (Some(alias), Some(distance)) => {
                            vec![(alias.clone(), distance_text(distance))]
                        }
                        _ => Vec::new(),
                    },
                    key: found.key,
                    sort_key: None,
                })
                .collect()
        }
    };
    // In a cluster, only the keys of the slots this node serves: a key part
    // of the way through a migration is not counted twice.
    if ctx.cluster.enabled {
        hits.retain(|hit| crate::cluster::search::serves(ctx.cluster, &hit.key));
    }

    // ── SORTBY ───────────────────────────────────────────────────────────────
    if let Some((name, ascending)) = &args.sortby {
        let schema_field = def.field_by_attribute(name);
        let numeric = schema_field.is_some_and(|f| matches!(f.field_type, FieldType::Numeric));
        let unf = schema_field.is_some_and(|f| f.options.unf);
        let source = schema_field.map_or(name.as_str(), |f| f.name.as_str());
        // A document read for each hit: as long as the scan, so the deadline
        // is kept here too. Cut short, the hits are not sorted.
        let mut seen = 0u32;
        let mut keyed = true;
        for hit in &mut hits {
            if deadline_due(deadline, &mut seen) {
                timed_out = true;
                keyed = false;
                break;
            }
            // A value the query produced (the distance) or one the document
            // holds.
            hit.sort_key = match hit.extras.iter().find(|(alias, _)| alias == name) {
                Some((_, value)) => value.parse::<f64>().ok().map(SortKey::Number),
                None => with_doc(db, &def, &hit.key, |fields, _| {
                    let raw = std::str::from_utf8(fields.get(source)?).ok()?.to_string();
                    if numeric {
                        raw.trim().parse::<f64>().ok().map(SortKey::Number)
                    } else if unf {
                        Some(SortKey::Text(raw))
                    } else {
                        Some(SortKey::Text(raw.to_lowercase()))
                    }
                })
                .flatten(),
            };
        }
        if keyed && no_time_for(deadline, hits.len()) {
            timed_out = true;
            keyed = false;
        }
        if keyed {
            hits.sort_by(|a, b| {
                use std::cmp::Ordering;
                let order = match (&a.sort_key, &b.sort_key) {
                    (Some(SortKey::Number(x)), Some(SortKey::Number(y))) => x.total_cmp(y),
                    (Some(SortKey::Text(x)), Some(SortKey::Text(y))) => x.cmp(y),
                    (Some(SortKey::Number(_)), Some(SortKey::Text(_))) => Ordering::Less,
                    (Some(SortKey::Text(_)), Some(SortKey::Number(_))) => Ordering::Greater,
                    // A document without the value comes last, either way round.
                    (Some(_), None) => return Ordering::Less,
                    (None, Some(_)) => return Ordering::Greater,
                    (None, None) => Ordering::Equal,
                };
                let order = if *ascending { order } else { order.reverse() };
                order.then_with(|| a.key.cmp(&b.key))
            });
        }
    }
    // A shard answers with what it found and whether it timed out; the
    // coordinator applies `ON_TIMEOUT`.
    if timed_out && !shard && on_timeout_fails() {
        return Err("Timeout limit was reached".into());
    }

    // ── The page ─────────────────────────────────────────────────────────────
    let total = hits.len();
    let page: Vec<Hit> = hits
        .into_iter()
        .skip(args.offset)
        .take(args.count)
        .collect();
    let nocontent = args.nocontent || args.returns.as_ref().is_some_and(|r| r.is_empty());

    // The fields of one hit, as (name, value).
    let fields_of = |hit: &Hit| -> Vec<(String, Vec<u8>)> {
        let shown = |source: &str, value: &[u8]| -> Vec<u8> {
            let Some((only, open, close)) = &args.highlight else {
                return value.to_vec();
            };
            let field = def.fields.iter().find(|f| f.name == source);
            let wanted = field.is_some_and(|f| matches!(f.field_type, FieldType::Text))
                && (only.is_empty()
                    || only
                        .iter()
                        .any(|name| field.is_some_and(|f| f.attribute() == name)));
            match std::str::from_utf8(value) {
                Ok(value) if wanted => highlight(value, &terms, open, close).into_bytes(),
                _ => value.to_vec(),
            }
        };
        let extras = || -> Vec<(String, Vec<u8>)> {
            hit.extras
                .iter()
                .map(|(alias, value)| (alias.clone(), value.clone().into_bytes()))
                .collect()
        };
        with_doc(db, &def, &hit.key, |fields, json| {
            match (&args.returns, json) {
                (Some(returns), _) => returns
                    .iter()
                    .filter_map(|(source, name)| {
                        if let Some((_, value)) =
                            hit.extras.iter().find(|(alias, _)| alias == source)
                        {
                            return Some((name.clone(), value.clone().into_bytes()));
                        }
                        // By the name the schema gives it, or as the document
                        // has it: a hash field's name, a JSON path.
                        let stored = def
                            .field_by_attribute(source)
                            .map_or(source.as_str(), |f| f.name.as_str());
                        match json {
                            // From the document, not from what the index reads
                            // of it: a vector comes back as the array it is.
                            Some(doc) => crate::vector::json_return_value(doc, stored)
                                .map(|value| (name.clone(), shown(stored, &value))),
                            None => fields
                                .get(stored)
                                .map(|value| (name.clone(), shown(stored, value))),
                        }
                    })
                    .collect(),
                // A JSON document is returned whole, under `$`.
                (None, Some(doc)) => {
                    let mut out = extras();
                    out.push(("$".to_string(), serde_json::to_vec(doc).unwrap_or_default()));
                    out
                }
                (None, None) => {
                    let mut out = extras();
                    let mut stored: Vec<(&String, &Arc<[u8]>)> = fields.iter().collect();
                    stored.sort_by(|a, b| a.0.cmp(b.0));
                    for (name, value) in stored {
                        if !hit.extras.iter().any(|(alias, _)| alias == name) {
                            out.push((name.clone(), shown(name, value)));
                        }
                    }
                    out
                }
            }
        })
        .unwrap_or_default()
    };
    let page: Vec<PageHit> = page
        .iter()
        .map(|hit| PageHit {
            key: hit.key.clone(),
            score: hit.score,
            sort_key: hit.sort_key.clone(),
            fields: if nocontent {
                Vec::new()
            } else {
                fields_of(hit)
            },
        })
        .collect();
    let page = encode_page(ctx.resp, &page, total, &args, nocontent, timed_out);
    // A coordinator that asks for it is told whether this part timed out;
    // one from before there was a timeout gets the page as it expects it.
    if shard && crate::cluster::search::wants_timeout_flag(&ctx.cmd) {
        Ok(crate::cluster::search::encode_search(page, timed_out))
    } else {
        Ok(page)
    }
}

/// A hit of the page, with what is shown of it.
struct PageHit {
    key: String,
    score: f64,
    sort_key: Option<SortKey>,
    fields: Vec<(String, Vec<u8>)>,
}

/// The reply to FT.SEARCH for `page`, of `total` matches.
fn encode_page(
    resp: u8,
    page: &[PageHit],
    total: usize,
    args: &SearchArgs,
    nocontent: bool,
    timed_out: bool,
) -> Vec<u8> {
    let fields_of = |hit: &PageHit| hit.fields.clone();
    let sort_key_reply = |hit: &PageHit| match &hit.sort_key {
        Some(SortKey::Number(n)) => resp_bulk_str(&format!("#{}", number_text(*n))),
        Some(SortKey::Text(t)) => resp_bulk_str(&format!("${t}")),
        None => resp_null_for(resp),
    };

    if resp == 3 {
        // RESP3: a map, with one map per result.
        let results: Vec<Vec<u8>> = page
            .iter()
            .map(|hit| {
                let mut entry = vec![resp_bulk_str("id"), resp_bulk_str(&hit.key)];
                if args.withscores {
                    entry.push(resp_bulk_str("score"));
                    entry.push(resp_double_for(3, hit.score));
                }
                if args.withsortkeys {
                    entry.push(resp_bulk_str("sortkey"));
                    entry.push(sort_key_reply(hit));
                }
                if !nocontent {
                    entry.push(resp_bulk_str("extra_attributes"));
                    entry.push(resp_map_for(
                        3,
                        fields_of(hit)
                            .into_iter()
                            .flat_map(|(name, value)| [resp_bulk_str(&name), resp_bulk(&value)])
                            .collect(),
                    ));
                }
                entry.push(resp_bulk_str("values"));
                entry.push(resp_array(vec![]));
                resp_map_for(3, entry)
            })
            .collect();
        return resp_map_for(
            3,
            vec![
                resp_bulk_str("attributes"),
                resp_array(vec![]),
                resp_bulk_str("format"),
                resp_bulk_str("STRING"),
                resp_bulk_str("results"),
                resp_array(results),
                resp_bulk_str("total_results"),
                resp_integer(total as i64),
                resp_bulk_str("warning"),
                resp_array(if timed_out {
                    vec![resp_bulk_str("Timeout limit was reached")]
                } else {
                    vec![]
                }),
            ],
        );
    }

    let mut out: Vec<Vec<u8>> = Vec::with_capacity(1 + page.len() * 2);
    out.push(resp_integer(total as i64));
    for hit in page {
        out.push(resp_bulk_str(&hit.key));
        if args.withscores {
            out.push(resp_bulk_str(&number_text(hit.score)));
        }
        if args.withpayloads {
            out.push(resp_null_bulk());
        }
        if args.withsortkeys {
            out.push(sort_key_reply(hit));
        }
        if !nocontent {
            out.push(resp_array(
                fields_of(hit)
                    .into_iter()
                    .flat_map(|(name, value)| [resp_bulk_str(&name), resp_bulk(&value)])
                    .collect(),
            ));
        }
    }
    resp_array(out)
}

thread_local! {
    /// Set while a coordinator runs its own part of a search.
    static SHARD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// FT.SEARCH across the cluster: see `cluster::search`.
fn coordinate(
    ctx: &mut Ctx,
    cmd: Vec<Vec<u8>>,
    args: &SearchArgs,
    by_distance: bool,
    nearest: Option<usize>,
) -> Result<Vec<u8>, String> {
    // What every master is asked: the first offset+count of its matches,
    // with what they are ordered by.
    let mut shard_cmd: Vec<Vec<u8>> = cmd[..3.min(cmd.len())].to_vec();
    let mut i = 3;
    while i < cmd.len() {
        let word = cmd[i].to_ascii_uppercase();
        match word.as_slice() {
            b"LIMIT" => i += 3,
            b"WITHSCORES" | b"WITHSORTKEYS" | b"WITHPAYLOADS" => i += 1,
            _ => {
                shard_cmd.push(cmd[i].clone());
                i += 1;
            }
        }
    }
    shard_cmd.push(b"WITHSCORES".to_vec());
    if args.sortby.is_some() {
        shard_cmd.push(b"WITHSORTKEYS".to_vec());
    }
    shard_cmd.push(b"LIMIT".to_vec());
    shard_cmd.push(b"0".to_vec());
    shard_cmd.push((args.offset + args.count).to_string().into_bytes());
    crate::cluster::search::ask_for_timeout_flag(&mut shard_cmd);

    // This node's part, then the others'.
    let local = {
        let parts: Vec<&[u8]> = shard_cmd.iter().map(Vec::as_slice).collect();
        // `parts` outlives the call below, and `ctx.cmd` is put back before
        // it is dropped.
        let parts: Vec<&'static [u8]> = unsafe { std::mem::transmute(parts) };
        let saved_cmd = std::mem::replace(&mut ctx.cmd, parts);
        let saved_resp = std::mem::replace(&mut ctx.resp, 2);
        /// Clears `SHARD` however the search ends: left set by a panic, it
        /// would make every later search on this thread answer as a shard.
        struct NotAShard;
        impl Drop for NotAShard {
            fn drop(&mut self) {
                SHARD.with(|s| s.set(false));
            }
        }
        SHARD.with(|s| s.set(true));
        let not_a_shard = NotAShard;
        let reply = search(ctx);
        drop(not_a_shard);
        ctx.resp = saved_resp;
        ctx.cmd = saved_cmd;
        crate::cluster::search::parse_reply(&reply?)?
    };
    let mut replies = vec![local];
    replies.extend(crate::cluster::search::fan_out(ctx, &shard_cmd)?);

    let nocontent = args.nocontent || args.returns.as_ref().is_some_and(|r| r.is_empty());
    use crate::cluster::search::Reply;
    let mut total = 0usize;
    let mut hits: Vec<PageHit> = Vec::new();
    let mut timed_out = false;
    for reply in replies {
        let (reply, shard_timed_out) = crate::cluster::search::decode_search(reply)?;
        timed_out |= shard_timed_out;
        let Reply::Array(items) = reply else {
            return Err("ERR a master answered the search with something else".into());
        };
        let mut items = items.into_iter();
        if let Some(Reply::Int(n)) = items.next() {
            total += n.max(0) as usize;
        }
        while let Some(key) = items.next() {
            let key = key.text().unwrap_or_default();
            let score = items
                .next()
                .and_then(|s| s.text())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let sort_key = if args.sortby.is_some() {
                match items.next() {
                    Some(Reply::Bulk(Some(raw))) => {
                        let raw = String::from_utf8_lossy(&raw).into_owned();
                        if let Some(n) = raw.strip_prefix('#') {
                            n.parse::<f64>().ok().map(SortKey::Number)
                        } else {
                            raw.strip_prefix('$').map(|t| SortKey::Text(t.to_string()))
                        }
                    }
                    _ => None,
                }
            } else {
                None
            };
            let fields = if nocontent {
                Vec::new()
            } else {
                match items.next() {
                    Some(Reply::Array(cells)) => cells
                        .chunks(2)
                        .filter_map(|pair| {
                            let name = pair.first()?.text()?;
                            match pair.get(1)? {
                                Reply::Bulk(Some(value)) => Some((name, value.clone())),
                                other => Some((name, other.text()?.into_bytes())),
                            }
                        })
                        .collect(),
                    _ => Vec::new(),
                }
            };
            hits.push(PageHit {
                key,
                score,
                sort_key,
                fields,
            });
        }
    }

    // One order over all of them, as one node orders its own.
    use std::cmp::Ordering;
    match &args.sortby {
        Some((_, ascending)) => hits.sort_by(|a, b| {
            let order = match (&a.sort_key, &b.sort_key) {
                (Some(SortKey::Number(x)), Some(SortKey::Number(y))) => x.total_cmp(y),
                (Some(SortKey::Text(x)), Some(SortKey::Text(y))) => x.cmp(y),
                (Some(SortKey::Number(_)), Some(SortKey::Text(_))) => Ordering::Less,
                (Some(SortKey::Text(_)), Some(SortKey::Number(_))) => Ordering::Greater,
                (Some(_), None) => return Ordering::Less,
                (None, Some(_)) => return Ordering::Greater,
                (None, None) => Ordering::Equal,
            };
            let order = if *ascending { order } else { order.reverse() };
            order.then_with(|| a.key.cmp(&b.key))
        }),
        None if by_distance => {
            hits.sort_by(|a, b| a.score.total_cmp(&b.score).then_with(|| a.key.cmp(&b.key)))
        }
        None => hits.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.key.cmp(&b.key))),
    }
    // KNN k: the k nearest of the cluster, not k from each master.
    if let Some(k) = nearest {
        hits.truncate(k);
        total = total.min(k);
    }
    let page: Vec<PageHit> = hits
        .into_iter()
        .skip(args.offset)
        .take(args.count)
        .collect();
    if timed_out && on_timeout_fails() {
        return Err("Timeout limit was reached".into());
    }
    Ok(encode_page(
        ctx.resp, &page, total, args, nocontent, timed_out,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(cmd: &[&str]) -> Result<Vec<SchemaField>, String> {
        let cmd: Vec<&[u8]> = cmd.iter().map(|w| w.as_bytes()).collect();
        let mut i = 0;
        parse_schema_fields(&cmd, &mut i)
    }

    /// The five schemas of the LiteLLM report: each was refused.
    #[test]
    fn field_options_are_read_and_kept() {
        let fields = words(&["f", "TEXT", "WEIGHT", "2.5", "g", "TAG", "SEPARATOR", ";"]).unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].options.weight, 2.5);
        assert_eq!(fields[1].options.separator, ';');

        let fields = words(&[
            "a",
            "TEXT",
            "SORTABLE",
            "b",
            "TAG",
            "CASESENSITIVE",
            "c",
            "NUMERIC",
            "SORTABLE",
            "UNF",
            "d",
            "TEXT",
            "NOSTEM",
            "NOINDEX",
            "PHONETIC",
            "dm:en",
            "WITHSUFFIXTRIE",
        ])
        .unwrap();
        assert_eq!(fields.len(), 4);
        assert!(fields[0].options.sortable && !fields[0].options.unf);
        assert!(fields[1].options.case_sensitive);
        assert!(fields[2].options.sortable && fields[2].options.unf);
        assert!(fields[3].options.nostem && fields[3].options.noindex);
        assert_eq!(fields[3].options.phonetic.as_deref(), Some("dm:en"));

        // As RedisVL writes LiteLLM's semantic-cache schema.
        let fields = words(&[
            "prompt",
            "TEXT",
            "WEIGHT",
            "1",
            "response",
            "TEXT",
            "WEIGHT",
            "1",
            "litellm_cache_key",
            "TAG",
            "SEPARATOR",
            ",",
            "prompt_vector",
            "VECTOR",
            "FLAT",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            "3",
            "DISTANCE_METRIC",
            "COSINE",
        ])
        .unwrap();
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[2].options.separator, ',');
        assert!(matches!(
            fields[3].field_type,
            FieldType::Vector { dim: 3, .. }
        ));
    }

    #[test]
    fn as_names_the_attribute() {
        let fields = words(&["$.title", "AS", "title", "TEXT", "n", "NUMERIC"]).unwrap();
        assert_eq!(fields[0].name, "$.title");
        assert_eq!(fields[0].attribute(), "title");
        assert_eq!(fields[1].attribute(), "n");
        assert!(
            words(&["a", "AS", "x", "TEXT", "b", "AS", "x", "TAG"])
                .unwrap_err()
                .contains("Duplicate field")
        );
    }

    /// The report's issue 8: say which option, on which field.
    #[test]
    fn a_misplaced_option_is_named() {
        let err = words(&["k", "TAG", "WEIGHT", "1"]).unwrap_err();
        assert!(
            err.contains("WEIGHT") && err.contains("TAG field `k`") && err.contains("TEXT"),
            "{err}"
        );
        let err = words(&["n", "NUMERIC", "SEPARATOR", ","]).unwrap_err();
        assert!(
            err.contains("SEPARATOR") && err.contains("NUMERIC field `n`"),
            "{err}"
        );
        let err = words(&["k", "TAG", "CASESENSITIVE", "t", "TEXT", "CASESENSITIVE"]).unwrap_err();
        assert!(
            err.contains("CASESENSITIVE") && err.contains("TEXT field `t`"),
            "{err}"
        );

        assert!(words(&["f"]).unwrap_err().contains("does not have a type"));
        assert!(
            words(&["f", "BLOB"])
                .unwrap_err()
                .contains("Invalid field type")
        );
        assert!(
            words(&["f", "TEXT", "WEIGHT"])
                .unwrap_err()
                .contains("WEIGHT")
        );
        assert!(
            words(&["f", "TEXT", "WEIGHT", "heavy"])
                .unwrap_err()
                .contains("heavy")
        );
        assert!(
            words(&["f", "TAG", "SEPARATOR", "ab"])
                .unwrap_err()
                .contains("single")
        );
        assert!(words(&["f", "GEO"]).unwrap_err().contains("not supported"));
    }

    #[test]
    fn options_are_described_as_they_were_given() {
        let fields = words(&[
            "t",
            "TEXT",
            "WEIGHT",
            "2",
            "NOSTEM",
            "SORTABLE",
            "k",
            "TAG",
            "SEPARATOR",
            "|",
            "CASESENSITIVE",
            "n",
            "NUMERIC",
        ])
        .unwrap();
        assert_eq!(
            fields[0].options.words(&fields[0].field_type),
            ["WEIGHT", "2", "NOSTEM", "SORTABLE"]
        );
        assert_eq!(
            fields[1].options.words(&fields[1].field_type),
            ["SEPARATOR", "|", "CASESENSITIVE"]
        );
        assert!(fields[2].options.words(&fields[2].field_type).is_empty());
        // And read back the same.
        for field in &fields {
            let mut cmd: Vec<String> = vec![field.name.clone(), field.type_name().to_string()];
            cmd.extend(field.options.words(&field.field_type));
            let cmd: Vec<&str> = cmd.iter().map(String::as_str).collect();
            let again = words(&cmd).unwrap();
            assert_eq!(again[0].options, field.options);
        }
    }

    #[test]
    fn highlighting_marks_whole_words() {
        let terms: AHashSet<String> = ["fox".to_string(), "quick".to_string()].into();
        assert_eq!(
            highlight("The Quick brown fox, foxes", &terms, "<b>", "</b>"),
            "The <b>Quick</b> brown <b>fox</b>, foxes"
        );
    }

    #[test]
    fn search_arguments() {
        let parse = |words: &[&str]| {
            let cmd: Vec<&[u8]> = words.iter().map(|w| w.as_bytes()).collect();
            parse_search_args(&cmd, 0)
        };
        let args = parse(&[
            "RETURN",
            "4",
            "a",
            "AS",
            "x",
            "b",
            "SORTBY",
            "n",
            "DESC",
            "LIMIT",
            "5",
            "20",
            "PARAMS",
            "2",
            "p",
            "v",
            "DIALECT",
            "2",
            "WITHSCORES",
            "NOCONTENT",
        ])
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            args.returns,
            Some(vec![("a".into(), "x".into()), ("b".into(), "b".into())])
        );
        assert_eq!(args.sortby, Some(("n".into(), false)));
        assert_eq!((args.offset, args.count), (5, 20));
        assert_eq!(args.params["p"], b"v");
        assert!(args.withscores && args.nocontent);
        assert_eq!(parse(&["TIMEOUT", "50"]).unwrap().timeout_ms, Some(50));
        assert_eq!(parse(&["TIMEOUT", "0"]).unwrap().timeout_ms, Some(0));
        assert!(parse(&["TIMEOUT", "soon"]).is_err());

        for bad in [
            &["BOGUS"][..],
            &["LIMIT", "1"],
            &["LIMIT", "a", "b"],
            &["PARAMS", "3", "a", "b", "c"],
            &["PARAMS", "4", "a", "b"],
            &["RETURN", "2", "a"],
            &["DIALECT", "9"],
            &["INFIELDS", "1", "f"],
        ] {
            assert!(parse(bad).is_err(), "{bad:?} was accepted");
        }
        assert!(
            parse(&["BOGUS"])
                .unwrap_err()
                .contains("Unknown argument `BOGUS`")
        );
    }

    #[test]
    fn a_deadline_already_passed_is_past() {
        assert!(past_deadline(Some(Instant::now() - Duration::from_secs(1))));
        assert!(!past_deadline(None));
        assert!(search_deadline(Some(0)).is_none());
        assert!(search_deadline(Some(50)).is_some());
        // As far off as a TIMEOUT can say: a deadline or none, not a panic.
        let _ = search_deadline(Some(u64::MAX));
    }

    #[test]
    fn the_clock_is_read_every_1024_documents() {
        let deadline = Some(Instant::now() - Duration::from_secs(1));
        let mut seen = 0u32;
        for _ in 0..DEADLINE_EVERY - 1 {
            assert!(!deadline_due(deadline, &mut seen));
        }
        assert!(deadline_due(deadline, &mut seen));
        assert!(!deadline_due(None, &mut 0u32));
        // A sort of fewer items than that always runs; of more, not past it.
        assert!(!no_time_for(deadline, DEADLINE_EVERY as usize - 1));
        assert!(no_time_for(deadline, DEADLINE_EVERY as usize));
        assert!(!no_time_for(None, usize::MAX));
        let later = Some(Instant::now() + Duration::from_secs(60));
        assert!(!no_time_for(later, usize::MAX));
    }
}
