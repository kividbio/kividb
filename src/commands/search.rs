// src/commands/search.rs
//
// FT.CREATE's schema, FT.INFO, and FT.SEARCH: the arguments, running a parsed
// query (`vector::filter`) against an index, and the reply.

use crate::store::ValueRef;
use ahash::AHashSet;
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
    let mut language = crate::vector::stem::LanguageOptions::default();
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
            // What the words of TEXT fields are stemmed in (see `stem`).
            "LANGUAGE" => {
                let name = cmd
                    .get(i + 1)
                    .map(|a| text(a))
                    .ok_or_else(|| "Bad arguments for LANGUAGE: expected a value".to_string())?;
                language.language = crate::vector::stem::Language::parse(name)
                    .ok_or_else(|| "Invalid language".to_string())?;
                i += 2;
            }
            "LANGUAGE_FIELD" => {
                let field = cmd.get(i + 1).map(|a| text(a)).ok_or_else(|| {
                    "Bad arguments for LANGUAGE_FIELD: expected a value".to_string()
                })?;
                language.field = Some(field.to_string());
                i += 2;
            }
            // Ranking and storage hints with no counterpart here. They
            // change no result a query can see.
            "SCORE" | "SCORE_FIELD" | "PAYLOAD_FIELD" | "TEMPORARY" => {
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
        language,
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

/// What `index_definition` of FT.INFO says of the index's language, as Redis
/// says it: nothing for English.
fn language_definition(language: &crate::vector::stem::LanguageOptions) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    if language.language != crate::vector::stem::Language::English {
        out.push(resp_bulk_str("default_language"));
        out.push(resp_bulk_str(language.language.name()));
    }
    if let Some(field) = &language.field {
        out.push(resp_bulk_str("language_field"));
        out.push(resp_bulk_str(field));
    }
    out
}

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
            ]
            .into_iter()
            .chain(language_definition(&def.language))
            .chain([resp_bulk_str("default_score"), resp_bulk_str("1")])
            .collect(),
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
    let (num_terms, num_records, text_bytes) = {
        let text = def.text.read();
        (text.num_terms(), text.num_records(), text.bytes())
    };
    put("num_terms", resp_integer(num_terms as i64));
    put("num_records", resp_integer(num_records as i64));
    // What the TEXT index holds, and what it costs.
    put("text_terms_bytes", resp_integer(text_bytes.terms as i64));
    put(
        "text_postings_bytes",
        resp_integer(text_bytes.postings as i64),
    );
    put("text_docs_bytes", resp_integer(text_bytes.docs as i64));
    // What the TAG and NUMERIC postings hold, and what they cost.
    if def.keeps_postings() {
        let p = def.postings.read();
        let bytes = p.bytes();
        put("doc_ids", resp_integer(p.len() as i64));
        put("tag_values", resp_integer(bytes.tag_values as i64));
        put("postings_ids_bytes", resp_integer(bytes.ids as i64));
        put("postings_tag_bytes", resp_integer(bytes.tags as i64));
        put("postings_numeric_bytes", resp_integer(bytes.numbers as i64));
    }
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

/// The error for an index that does not exist, as Redis 8 gives it for
/// every command that names one: `SEARCH_INDEX_NOT_FOUND`, then the name.
/// Clients look for "not found" in it to know they should create the index.
pub(crate) fn no_such_index(name: &str) -> String {
    format!("SEARCH_INDEX_NOT_FOUND Index not found: {name}")
}

/// Whether `error`, a node's error reply without its `-` (its line ending
/// may still be there), is [`no_such_index`]'s, or the wording KiviDB used
/// before it.
pub(crate) fn is_no_such_index(error: &str) -> bool {
    let error = error.trim_end();
    error.starts_with("SEARCH_INDEX_NOT_FOUND ") || error.ends_with("no such index")
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

/// Every document of `db` into `each`, shard by shard, until `deadline` has
/// passed: then true. The clock is read between runs of [`DEADLINE_EVERY`]
/// documents, counted across shards, and only while documents are left to
/// read: a scan that got to the end is whole, however long it took.
///
/// Not in the loop over the documents of a run: the scan waits on memory,
/// document after document, and with a check in that loop, however cheap,
/// the compiler kept less of the per-document work inline. Filtered KNN
/// queries took 12% longer that way.
pub(crate) fn scan_docs(
    db: &Db,
    deadline: Option<Instant>,
    mut each: impl FnMut(&str, ValueRef<'_>),
) -> bool {
    let run = DEADLINE_EVERY as usize;
    let mut seen = 0usize;
    for shard in db.iter() {
        let map = shard.read();
        let expired = expired_in(&map);
        let mut left = map.len();
        let mut docs = map.iter();
        while left > 0 {
            if seen >= run {
                seen = 0;
                if past_deadline(deadline) {
                    return true;
                }
            }
            let this_run = left.min(run);
            if expired.is_empty() {
                for (key, value) in docs.by_ref().take(this_run) {
                    each(key, value);
                }
            } else {
                for (key, value) in docs.by_ref().take(this_run) {
                    if !expired.contains(key) {
                        each(key, value);
                    }
                }
            }
            left -= this_run;
            seen += this_run;
        }
    }
    false
}

/// The keys of a shard whose time to live has ended, which nothing has
/// deleted yet: no documents, as a read finds them.
fn expired_in(map: &crate::store::ShardMap) -> AHashSet<String> {
    if map.ttl_len() == 0 {
        return AHashSet::new();
    }
    let now = crate::commands::helpers::now_ms();
    let (due, _) = map.due(now, usize::MAX);
    due.into_iter().map(|key| key.to_string()).collect()
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
            term: TextMatch::Exact(term) | TextMatch::Expanded(term),
            ..
        } => {
            out.insert(term.clone());
        }
        // A stem, written as the index has it.
        Filter::Text {
            term: TextMatch::Stem(stem),
            ..
        } => {
            out.insert(format!("{}{}", crate::vector::stem::STEM_PREFIX, stem.stem));
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

/// The documents that could match, read off the TEXT index (by their ids),
/// and whether they are exactly the documents that match — or `None` if the
/// filter has to be tried on every document.
///
/// A word, a stem, a prefix, suffix or infix, and `AND` and `OR` of them,
/// are exact: the TEXT index holds every document's words as they are now
/// (and no stopword, which a pattern does not match, as in Redis). A phrase
/// gives the documents to try: it is about where the words are.
fn candidates(filter: &Filter, def: &IndexDef) -> Option<(Bits, bool)> {
    let numbers = |fields: &[String]| -> Vec<u16> {
        fields.iter().filter_map(|f| def.text_field(f)).collect()
    };
    let word = |fields: &[String], term: &str| {
        let mut out = Bits::new();
        def.text.read().docs_with(term, &numbers(fields), &mut out);
        out
    };
    let matching = |fields: &[String], pick: &dyn Fn(&str) -> bool| {
        let mut out = Bits::new();
        def.text
            .read()
            .docs_matching(&numbers(fields), pick, &mut out);
        out
    };
    match filter {
        Filter::MatchNone => Some((Bits::new(), true)),
        Filter::Text { fields, term } => Some(match term {
            // A stopword a synonym brings in is in no index.
            TextMatch::Exact(t) | TextMatch::Expanded(t) => (word(fields, t), !def.is_stopword(t)),
            TextMatch::Stem(stem) => {
                let term = format!("{}{}", crate::vector::stem::STEM_PREFIX, stem.stem);
                (word(fields, &term), true)
            }
            TextMatch::Prefix(p) => (matching(fields, &|tok| tok.starts_with(p.as_str())), true),
            TextMatch::Suffix(s) => (matching(fields, &|tok| tok.ends_with(s.as_str())), true),
            TextMatch::Infix(s) => (matching(fields, &|tok| tok.contains(s.as_str())), true),
            // Every word of the vocabulary would have to be measured against
            // it; trying the documents costs the same.
            TextMatch::Fuzzy(..) => return None,
        }),
        Filter::Phrase { fields, terms } => {
            // Stopwords hold a place in the phrase but are in no index.
            let mut sets = terms
                .iter()
                .filter(|t| !def.is_stopword(t))
                .map(|t| word(fields, t));
            let mut all = sets.next()?;
            for set in sets {
                all.and_with(&set);
            }
            Some((all, false))
        }
        Filter::And(l, r) => match (candidates(l, def), candidates(r, def)) {
            (Some((mut a, exact_a)), Some((b, exact_b))) => {
                a.and_with(&b);
                Some((a, exact_a && exact_b))
            }
            (Some((a, _)), None) | (None, Some((a, _))) => Some((a, false)),
            (None, None) => None,
        },
        Filter::Or(l, r) => match (candidates(l, def), candidates(r, def)) {
            (Some((mut a, exact_a)), Some((b, exact_b))) => {
                a.or_with(&b);
                Some((a, exact_a && exact_b))
            }
            _ => None,
        },
        _ => None,
    }
}

// ── Answering from postings ──────────────────────────────────────────────────
//
// An index with a TAG or NUMERIC field keeps postings for them (see
// `vector::postings`). A filter made of TAG and NUMERIC clauses, `AND`, `OR`,
// `NOT`, `*` and `~`, is then a set of document ids read off them, exactly
// the documents the filter matches: nothing is read to find them. So is a
// TEXT word, stem or pattern, read off the TEXT index (`vector::text`); a
// phrase narrows the set to the documents its words are in, and the set is
// then one to try the filter on. A clause the postings know nothing of
// (a fuzzy word, a vector range) leaves the documents the rest allows, if
// the rest allows fewer than all: those are tried. Only a filter that
// allows anything the postings cannot narrow is tried on every document.

use crate::vector::postings::{Bits, Postings};

/// What postings say of the documents a filter matches.
enum Cand {
    /// Exactly these.
    Exact(Bits),
    /// Among these: each is to be tried.
    Superset(Bits),
    /// Any of them.
    Unknown,
}

fn resolve(filter: &Filter, def: &IndexDef, p: &Postings) -> Cand {
    let known = |found: Option<Bits>| found.map_or(Cand::Unknown, Cand::Exact);
    match filter {
        Filter::MatchAll | Filter::Optional(_) => Cand::Exact(p.live().clone()),
        Filter::MatchNone => Cand::Exact(Bits::new()),
        Filter::Tag {
            field,
            values,
            prefixes,
            separator,
            case_sensitive,
        } => known(p.tag(field, values, prefixes, *separator, *case_sensitive)),
        Filter::Numeric {
            field,
            lo,
            lo_exclusive,
            hi,
            hi_exclusive,
        } => known(p.range(field, *lo, *lo_exclusive, *hi, *hi_exclusive)),
        Filter::NumericNot { field, value } => known(p.not_equal(field, *value)),
        Filter::Text { .. } | Filter::Phrase { .. } => match candidates(filter, def) {
            Some((ids, true)) => Cand::Exact(ids),
            Some((ids, false)) => Cand::Superset(ids),
            None => Cand::Unknown,
        },
        Filter::VectorRange { .. } => Cand::Unknown,
        Filter::And(l, r) => match (resolve(l, def, p), resolve(r, def, p)) {
            (Cand::Exact(mut a), Cand::Exact(b)) => {
                a.and_with(&b);
                Cand::Exact(a)
            }
            (Cand::Exact(mut a) | Cand::Superset(mut a), Cand::Exact(b) | Cand::Superset(b)) => {
                a.and_with(&b);
                Cand::Superset(a)
            }
            (Cand::Exact(a) | Cand::Superset(a), Cand::Unknown)
            | (Cand::Unknown, Cand::Exact(a) | Cand::Superset(a)) => Cand::Superset(a),
            (Cand::Unknown, Cand::Unknown) => Cand::Unknown,
        },
        Filter::Or(l, r) => match (resolve(l, def, p), resolve(r, def, p)) {
            (Cand::Exact(mut a), Cand::Exact(b)) => {
                a.or_with(&b);
                Cand::Exact(a)
            }
            (Cand::Unknown, _) | (_, Cand::Unknown) => Cand::Unknown,
            (Cand::Exact(mut a) | Cand::Superset(mut a), Cand::Exact(b) | Cand::Superset(b)) => {
                a.or_with(&b);
                Cand::Superset(a)
            }
        },
        Filter::Not(inner) => match resolve(inner, def, p) {
            Cand::Exact(b) => {
                let mut all = p.live().clone();
                all.and_not(&b);
                Cand::Exact(all)
            }
            _ => Cand::Unknown,
        },
    }
}

/// The documents a query is to look at, read off the index's postings.
pub(crate) struct Plan {
    pub ids: Bits,
    /// They are exactly the matches; otherwise each is to be tried.
    pub exact: bool,
    /// Some documents of the index have fields with a time to live: what
    /// their postings hold may be behind them (whether they match was tried
    /// on them).
    pub volatile: bool,
}

/// The documents of `def` that `filter` may match, among `only` if given,
/// from postings: `None` if they cannot narrow it, and every document is to
/// be tried.
pub(crate) fn plan(
    ctx: &Ctx,
    def: &IndexDef,
    filter: &Filter,
    only: Option<&AHashSet<String>>,
) -> Option<Plan> {
    let db = *ctx.selected_db;
    // The postings are of the index's own database's documents.
    if !def.keeps_postings() || def.db_index != db {
        return None;
    }
    let (mut ids, exact, volatile) = {
        let p = def.postings.read();
        if !p.enabled() || p.unused {
            return None;
        }
        let (mut ids, exact) = match resolve(filter, def, &p) {
            Cand::Exact(ids) => (ids, true),
            Cand::Superset(ids) => (ids, false),
            Cand::Unknown => return None,
        };
        if let Some(only) = only {
            let mut named = Bits::new();
            for key in only {
                if let Some(id) = p.id(key) {
                    named.set(id);
                }
            }
            ids.and_with(&named);
        }
        // Trying nearly every document one by one costs more than reading
        // them all in order.
        if !exact && ids.count() * 2 > p.len() {
            return None;
        }
        // A hash with field TTLs can lose a field to a read before the
        // indexes hear of it: such documents are tried, whatever the
        // postings say of them.
        let volatile: Vec<(String, u32)> = crate::field_expiry::watched_keys(ctx.store, db)
            .into_iter()
            .filter(|key| only.is_none_or(|only| only.contains(key)))
            .filter_map(|key| p.id(&key).map(|id| (key, id)))
            .collect();
        (ids, exact, volatile)
    };
    let store = &ctx.store[db];
    let any_volatile = !volatile.is_empty();
    for (key, id) in volatile {
        if exact {
            let matches = with_doc(store, def, &key, |fields, _| filter::eval(filter, fields));
            if matches == Some(true) {
                ids.set(id);
            } else {
                ids.unset(id);
            }
        } else {
            ids.set(id);
        }
    }
    Some(Plan {
        ids,
        exact,
        volatile: any_volatile,
    })
}

/// The keys of the documents `ids` names, in id order.
pub(crate) fn keys_of(def: &IndexDef, ids: &Bits) -> Vec<Arc<str>> {
    let p = def.postings.peek();
    ids.iter().filter_map(|id| p.key(id).cloned()).collect()
}

/// Relevance of the document at `key`, whose fields are `fields`, for the
/// query `scoring` was made for: as Redis 8 scores it (see `vector::score`).
pub(crate) fn relevance(
    def: &IndexDef,
    key: &str,
    fields: &Fields,
    scoring: &crate::vector::score::Scoring,
) -> f64 {
    scoring.score(def, key, fields)
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
    let scorer = crate::vector::score::Scorer::default_scorer();
    filter_search_scored(ctx, def, filter, only, deadline, scorer)
}

/// [`filter_search`], scored by `scorer` (`SCORER`).
pub(crate) fn filter_search_scored(
    ctx: &Ctx,
    def: &IndexDef,
    filter: &Filter,
    only: Option<&AHashSet<String>>,
    deadline: Option<Instant>,
    scorer: crate::vector::score::Scorer,
) -> FilterHits {
    let scoring = crate::vector::score::Scoring::new(def, filter, scorer);
    let range = filter.vector_range();
    let mut out: Vec<Match> = Vec::new();
    let timed_out = filter_each(ctx, def, filter, only, deadline, |key, fields| {
        out.push(Match {
            key: key.to_string(),
            score: relevance(def, key, fields, &scoring),
            distance: range.and_then(|clause| filter::range_distance(clause, fields)),
        });
    });
    FilterHits {
        matches: out,
        timed_out,
    }
}

/// Every document of the index that the filter accepts, into `accept`, with
/// its key and fields: [`filter_search`] without the copies, for a caller
/// that keeps only some of them. True if it stopped because `deadline` had
/// passed. `accept` may run under a shard's read lock.
pub(crate) fn filter_each(
    ctx: &Ctx,
    def: &IndexDef,
    filter: &Filter,
    only: Option<&AHashSet<String>>,
    deadline: Option<Instant>,
    mut accept: impl FnMut(&str, &Fields),
) -> bool {
    let db = &ctx.store[*ctx.selected_db];
    let mut timed_out = false;

    // From postings, if they narrow the filter: the documents they name, each
    // read only to hand it over, or to try it where they are not exact.
    if let Some(plan) = plan(ctx, def, filter, only) {
        let mut seen = 0u32;
        for key in keys_of(def, &plan.ids) {
            if deadline_due(deadline, &mut seen) {
                return true;
            }
            with_doc(db, def, &key, |fields, _| {
                if plan.exact || filter::eval(filter, fields) {
                    accept(&key, fields);
                }
            });
        }
        return false;
    }

    let mut consider = |key: &str, fields: &Fields| {
        if filter::eval(filter, fields) {
            accept(key, fields);
        }
    };

    // The keys to look at, if they are known without reading everything: the
    // ones the caller named, or the ones the TEXT index names. Those are the
    // matches if the TEXT index says so, and it is of this database.
    let found = {
        // Every write queued so far applied (the TEXT index's part too: see
        // `postings::WithId`), and no id given back while the TEXT index's
        // are turned into keys. The postings, then the TEXT index.
        let p = def.postings.read();
        candidates(filter, def).map(|(ids, exact)| {
            let keys: Vec<Arc<str>> = ids.iter().filter_map(|id| p.key(id).cloned()).collect();
            (keys, exact && def.db_index == *ctx.selected_db)
        })
    };
    let (known, exact): (Option<Vec<Arc<str>>>, bool) = match (only, found) {
        (Some(only), Some((found, exact))) => (
            Some(found.into_iter().filter(|k| only.contains(&**k)).collect()),
            exact,
        ),
        (Some(only), None) => (
            Some(only.iter().map(|k| Arc::from(k.as_str())).collect()),
            false,
        ),
        (None, Some((found, exact))) => (Some(found), exact),
        (None, None) => (None, false),
    };
    match known {
        Some(keys) => {
            let mut seen = 0u32;
            for key in &keys {
                let key: &str = key;
                if deadline_due(deadline, &mut seen) {
                    timed_out = true;
                    break;
                }
                if !covers(def, key) {
                    continue;
                }
                with_doc(db, def, key, |fields, _| {
                    if exact || filter::eval(filter, fields) {
                        accept(key, fields);
                    }
                });
            }
        }
        None => {
            timed_out = scan_docs(db, deadline, |key, value| {
                if !covers(def, key) {
                    return;
                }
                if let Some(fields) = crate::vector::doc_fields(&def.on, &def.fields, value) {
                    consider(key, &fields);
                }
            });
        }
    }
    // A scan that reached the end is whole, however long it took: only one
    // that stopped early has timed out.
    timed_out
}

/// Run `f` on the document at `key`, if there is one of the kind the index
/// covers: on its fields, and for a JSON document on the document too.
pub(crate) fn with_doc<R>(
    db: &Db,
    def: &IndexDef,
    key: &str,
    f: impl FnOnce(&Fields, Option<&serde_json::Value>) -> R,
) -> Option<R> {
    // Out of the shard, so that its lock is not held while `f` runs. A key
    // whose time to live has ended is no document, though nothing has
    // deleted it yet.
    let value = {
        let map = crate::store::get_shard(db, key).read();
        let (value, expires) = map.get_expiring(key)?;
        if expires.is_some_and(crate::commands::helpers::is_past) {
            return None;
        }
        value.to_value()
    };
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

// ── The page, from postings ──────────────────────────────────────────────────
//
// A query whose matches the postings know exactly (`Plan::exact`), in an
// order they know too, has its page picked from them: of the 250,000
// documents a broad TAG filter matches, the ten on the page are read, not
// the 250,000. How many match is the size of the set. What the reply shows
// of a document (its fields, score, sort key) is read off the documents of
// the page, as the full path reads it, so the reply is the full path's.
//
// The orders are the full path's:
// * no SORTBY: by relevance, then by key. Without a query word to score,
//   every match scores the same, so: by key.
// * SORTBY a NUMERIC field: by its number (`f64::total_cmp`), then by key;
//   the documents without a number last, by key, either way round.
//
// A key whose time to live has ended is no document, though nothing has
// deleted it yet (`with_doc`): those are found in the keyspace's lists of
// keys with a time to live, while the lists are no longer than the matches
// are many. In a cluster, only the keys of the slots this node serves count.
//
// The full path answers instead whenever this cannot: text to score, a
// vector clause, SORTBY another kind of field, a NUMERIC field holding NaN
// (no place in the postings' order), field TTLs under a numeric order, more
// keys with a time to live than matches, or a document of the page found
// changed since the postings were read.

/// A page of FT.SEARCH picked from postings, and how many documents match.
struct Posted {
    page: Vec<Hit>,
    total: usize,
    timed_out: bool,
}

/// What a page picked from postings is in the order of.
#[derive(Clone, Copy)]
enum PageOrder<'a> {
    /// The key: what relevance comes down to when every match scores the same.
    Key,
    /// The number in the NUMERIC field read from `field`, then the key.
    Number { field: &'a str, ascending: bool },
}

/// The deadline of a pick from postings: the clock read every
/// [`DEADLINE_EVERY`] steps, and once past it, past it.
struct Clock {
    deadline: Option<Instant>,
    seen: u32,
    out: bool,
}

impl Clock {
    fn new(deadline: Option<Instant>) -> Self {
        Self {
            deadline,
            seen: 0,
            out: false,
        }
    }

    /// One more step: whether the deadline has passed.
    #[inline]
    fn ran_out(&mut self) -> bool {
        if !self.out && deadline_due(self.deadline, &mut self.seen) {
            self.out = true;
        }
        self.out
    }
}

/// Whether every document `filter` matches has the same relevance, so that
/// without SORTBY the order is the key's alone (the full path's tie-break).
/// Scored as `vector::score` scores (Redis 8's way): a word by its
/// frequency; a TAG value by how many documents hold it, against the
/// document's TEXT length, and `*` against that length too; a NUMERIC
/// clause and `NOT` the same for every document. In an index with no TEXT
/// field every length is 0. TFIDF and BM25 (not BM25STD) divide by what
/// differs from document to document.
fn every_match_scores_the_same(
    def: &IndexDef,
    filter: &Filter,
    scorer: crate::vector::score::Scorer,
) -> bool {
    use crate::vector::score::Scorer;
    fn same(filter: &Filter, lengths: bool) -> bool {
        match filter {
            Filter::MatchNone
            | Filter::Numeric { .. }
            | Filter::NumericNot { .. }
            | Filter::Not(_) => true,
            Filter::MatchAll => !lengths,
            Filter::Tag {
                values, prefixes, ..
            } => !lengths && values.len() == 1 && prefixes.is_empty(),
            Filter::And(l, r) => same(l, lengths) && same(r, lengths),
            _ => false,
        }
    }
    let lengths = def.text_fields().next().is_some();
    match scorer {
        Scorer::DocScore | Scorer::Hamming => true,
        Scorer::Tfidf | Scorer::TfidfDocNorm | Scorer::Bm25 if lengths => false,
        _ => same(filter, lengths),
    }
}

/// FT.SEARCH's page and count from postings, reading only the documents of
/// the page: see above. `None` where the full path is to answer.
fn page_from_postings(
    ctx: &Ctx,
    def: &IndexDef,
    filter: &Filter,
    args: &SearchArgs,
    deadline: Option<Instant>,
) -> Option<Posted> {
    // A distance is no part of the postings.
    if filter.vector_range().is_some() {
        return None;
    }
    let scorer = args
        .scorer
        .unwrap_or_else(crate::vector::score::Scorer::default_scorer);
    let (order, numeric) = match &args.sortby {
        // Only the count (LIMIT 0 0): no order to keep.
        None if args.count == 0 || every_match_scores_the_same(def, filter, scorer) => {
            (PageOrder::Key, None)
        }
        None => return None,
        Some((name, ascending)) => {
            let field = def
                .field_by_attribute(name)
                .filter(|f| matches!(f.field_type, FieldType::Numeric))?;
            let order = PageOrder::Number {
                field: &field.name,
                ascending: *ascending,
            };
            (order, Some(field.name.as_str()))
        }
    };
    if def.postings.peek().read_all {
        return None;
    }
    let plan = plan(ctx, def, filter, args.inkeys.as_ref())?;
    // Exactly the matches. Ordered by a number, with the numbers the
    // documents hold, which a field's time to live may have taken away.
    if !plan.exact || (plan.volatile && numeric.is_some()) {
        return None;
    }
    let mut ids = plan.ids;
    let db = &ctx.store[*ctx.selected_db];

    // Keys whose time to live has ended, which nothing has deleted yet.
    let ttls: usize = db.iter().map(|shard| shard.ttl_count()).sum();
    let mut expired: Vec<crate::store::Key> = Vec::new();
    if ttls > 0 {
        // Reading the documents costs less than going through the lists.
        if ttls > ids.count() {
            return None;
        }
        let now = crate::commands::helpers::now_ms();
        for shard in db.iter() {
            if !shard.holds_no_ttl() {
                expired.extend(shard.read().due(now, usize::MAX).0);
            }
        }
    }

    // How many of the matches the page needs, in order.
    let need = if args.count == 0 {
        0
    } else {
        args.offset.saturating_add(args.count)
    };
    let (picked, total, timed_out) = {
        // What was written since the plan is not among its ids.
        let p = def.postings.peek();
        // Documents that left the index since the plan was made.
        ids.and_with(p.live());
        for key in &expired {
            if let Some(id) = p.id(key) {
                ids.unset(id);
            }
        }
        if ctx.cluster.enabled {
            let slots = crate::cluster::search::served_slots(ctx.cluster);
            let mut served = Bits::new();
            for id in ids.iter() {
                let slot = p.slot(id) as usize;
                if slots[slot / 64] & (1 << (slot % 64)) != 0 {
                    served.set(id);
                }
            }
            ids = served;
        }
        let total = ids.count();
        let need = need.min(total);
        let mut clock = Clock::new(deadline);
        let first: Vec<(u32, Option<f64>)> = match order {
            PageOrder::Key => first_by_key(&p, &ids, need, total, &mut clock)
                .into_iter()
                .map(|id| (id, None))
                .collect(),
            PageOrder::Number { field, ascending } => {
                first_by_number(&p, field, &ids, need, ascending, &mut clock)?
            }
        };
        let picked: Vec<(Arc<str>, Option<f64>)> = first
            .into_iter()
            .skip(args.offset)
            .filter_map(|(id, number)| Some((Arc::clone(p.key(id)?), number)))
            .collect();
        (picked, total, clock.out)
    };

    // The documents of the page. One that is gone, no longer matches, or
    // holds another number than the postings had was written after they
    // were read: then the full path answers. Scored as the full path scores
    // (a TEXT query, its words read off the TEXT index, comes here too when
    // sorted by a number).
    let scoring = crate::vector::score::Scoring::new(def, filter, scorer);
    let mut page = Vec::with_capacity(picked.len());
    for (key, number) in picked {
        let hit = with_doc(db, def, &key, |fields, _| {
            if !filter::eval(filter, fields) {
                return None;
            }
            let sort_key = match numeric {
                Some(source) => {
                    let found = sort_key_of(fields, source, true, false);
                    let same = match (&found, number) {
                        (Some(SortKey::Number(x)), Some(n)) => x.to_bits() == n.to_bits(),
                        (None, None) => true,
                        _ => false,
                    };
                    if !same {
                        return None;
                    }
                    found
                }
                None => None,
            };
            Some(Hit {
                key: key.to_string(),
                score: relevance(def, &key, fields, &scoring),
                extras: Vec::new(),
                sort_key,
            })
        })
        .flatten()?;
        page.push(hit);
    }
    Some(Posted {
        page,
        total,
        timed_out,
    })
}

/// The key of document `id`, from postings `p`.
#[inline]
fn key_of(p: &Postings, id: u32) -> &str {
    p.key(id).map_or("", |key| &**key)
}

/// The first `need` of `ids` (`of` of them) in the order of their keys,
/// from the postings' order of them (`Postings::first_by_key`). Cut short
/// when `clock` runs out: then the first of those found.
fn first_by_key(p: &Postings, ids: &Bits, need: usize, of: usize, clock: &mut Clock) -> Vec<u32> {
    p.first_by_key(ids, need, of, &mut || clock.ran_out())
}

/// The first `need` of `ids` in the order SORTBY gives the NUMERIC field
/// read from `field`: by the number (`f64::total_cmp`), ascending or not,
/// then by key; those without a number last, by key. Each with its number.
/// `None` if a document holds NaN, which has no place in the postings'
/// order. Cut short when `clock` runs out.
fn first_by_number(
    p: &Postings,
    field: &str,
    ids: &Bits,
    need: usize,
    ascending: bool,
    clock: &mut Clock,
) -> Option<Vec<(u32, Option<f64>)>> {
    let numbers = p.numbers(field)?;
    if numbers.any_nan() {
        return None;
    }
    let mut valued = ids.clone();
    valued.and_with(numbers.has());
    let with_number = valued.count();
    let want = need.min(with_number);
    let mut picked: Vec<(f64, u32)> = Vec::with_capacity(want);
    if want > 0 {
        let value = |id: u32| numbers.value(id).unwrap_or(0.0);
        let cmp = |a: &(f64, u32), b: &(f64, u32)| {
            let order = a.0.total_cmp(&b.0);
            let order = if ascending { order } else { order.reverse() };
            order.then_with(|| key_of(p, a.1).cmp(key_of(p, b.1)))
        };
        // Going through the numbers in order meets the `want` first after
        // about want * ordered / with_number of them; taking the number of
        // each, after with_number. Whichever is fewer.
        let walked = want as u128 * numbers.ordered() as u128 / with_number as u128;
        // Matches bunched up late in the numbers' order make the walk far
        // longer than that: past four times as long, it gives up, and the
        // number of each is taken instead.
        let mut gave_up = false;
        if walked < with_number as u128 {
            let mut budget = (walked * 4).max(4096);
            // Those equal to the last one wanted are taken too: their keys
            // decide which of them are on the page.
            let mut last: Option<u64> = None;
            numbers.walk(ascending, |place, id| {
                if clock.ran_out() || last.is_some_and(|last| last != place) {
                    return false;
                }
                if budget == 0 {
                    gave_up = true;
                    return false;
                }
                budget -= 1;
                if valued.get(id) {
                    picked.push((value(id), id));
                    if picked.len() == want {
                        last = Some(place);
                    }
                }
                true
            });
        }
        if walked >= with_number as u128 || gave_up {
            picked.clear();
            for id in valued.iter() {
                if clock.ran_out() {
                    break;
                }
                picked.push((value(id), id));
            }
            if want < picked.len() {
                picked.select_nth_unstable_by(want, cmp);
                picked.truncate(want);
            }
        }
        picked.sort_unstable_by(cmp);
        picked.truncate(want);
    }
    let mut first: Vec<(u32, Option<f64>)> =
        picked.into_iter().map(|(n, id)| (id, Some(n))).collect();
    if need > with_number && !clock.out {
        let mut rest = ids.clone();
        rest.and_not(numbers.has());
        let of = rest.count();
        let more = first_by_key(p, &rest, need - with_number, of, clock);
        first.extend(more.into_iter().map(|id| (id, None)));
    }
    Some(first)
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
    /// `VERBATIM` and `LANGUAGE`: how the query's words are expanded.
    query: filter::QueryOptions,
    /// `SCORER`; `FT.CONFIG DEFAULT_SCORER` if not given.
    scorer: Option<crate::vector::score::Scorer>,
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
            // The query's words are looked for as they are, not also as
            // their stems.
            "VERBATIM" => {
                args.query.verbatim = true;
                i += 1;
            }
            // How phrases are held together: not applied here.
            "NOSTOPWORDS" | "INORDER" => i += 1,
            "TIMEOUT" => {
                let raw = arg(i + 1, "TIMEOUT")?;
                args.timeout_ms = Some(raw.parse::<u64>().map_err(|_| {
                    "Bad arguments for TIMEOUT: expected a non-negative integer".to_string()
                })?);
                i += 2;
            }
            "LANGUAGE" => {
                let name = arg(i + 1, "LANGUAGE")?;
                args.query.language = Some(
                    crate::vector::stem::Language::parse(name)
                        .ok_or_else(|| "No such language".to_string())?,
                );
                i += 2;
            }
            "SLOP" | "EXPANDER" | "PAYLOAD" => {
                arg(i + 1, &word)?;
                i += 2;
            }
            "SCORER" => {
                let name = arg(i + 1, "SCORER")?;
                args.scorer = Some(
                    crate::vector::score::Scorer::parse(name)
                        .ok_or_else(|| format!("No such scorer {name}"))?,
                );
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
/// What SORTBY orders a document by: its field `source`, a number if the
/// field is NUMERIC, folded to lower case unless the field is UNF.
fn sort_key_of(fields: &Fields, source: &str, numeric: bool, unf: bool) -> Option<SortKey> {
    let raw = std::str::from_utf8(fields.get(source)?).ok()?;
    if numeric {
        raw.trim().parse::<f64>().ok().map(SortKey::Number)
    } else if unf {
        Some(SortKey::Text(raw.to_string()))
    } else {
        Some(SortKey::Text(raw.to_lowercase()))
    }
}

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
fn highlight(
    value: &str,
    terms: &AHashSet<String>,
    open: &str,
    close: &str,
    language: crate::vector::stem::Language,
) -> String {
    use crate::vector::stem;
    let mut out = String::with_capacity(value.len() + 16);
    let mut word = String::new();
    // A word is marked if the query found it by its stem, too.
    let stems = terms.iter().any(|t| stem::is_stem(t));
    let found = |t: &str| {
        terms.contains(t)
            || (stems
                && stem::index_stem(language, t)
                    .is_some_and(|s| terms.contains(&format!("{}{s}", stem::STEM_PREFIX))))
    };
    let flush = |word: &mut String, out: &mut String| {
        if word.is_empty() {
            return;
        }
        let token = crate::vector::tokenize_for_vocab(word).next();
        if token.is_some_and(|t| found(&t)) {
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
    let query = filter::parse_query_with(query_text, &args.params, &def, args.query)?;
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

    // ── The page, from postings, where they know it ─────────────────────────
    let posted = match &query.knn {
        Some(_) => None,
        None => page_from_postings(ctx, &def, &filter, &args, deadline),
    };

    // ── Otherwise every match, in the query's own order ─────────────────────
    let mut timed_out = false;
    let mut hits: Vec<Hit> = match &query.knn {
        _ if posted.is_some() => Vec::new(),
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
            } else if let Some((nearest, cut)) = crate::commands::vector::knn_planned(
                ctx,
                &def,
                &filter,
                args.inkeys.as_ref(),
                knn,
                field,
                deadline,
            ) {
                // The documents the filter matches, from postings: see
                // `knn_planned`.
                timed_out = cut;
                nearest
            } else {
                // The filter first, then the nearest among what it left:
                // exact, whatever the index type. Each document the filter
                // accepts is measured as it is found.
                let mut scorer =
                    crate::commands::vector::FilteredKnn::new(live, &knn.vector, knn.k, metric);
                timed_out = filter_each(
                    ctx,
                    &def,
                    &filter,
                    args.inkeys.as_ref(),
                    deadline,
                    |key, _| scorer.offer(key),
                );
                scorer.nearest()
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
            let scorer = args
                .scorer
                .unwrap_or_else(crate::vector::score::Scorer::default_scorer);
            let found =
                filter_search_scored(ctx, &def, &filter, args.inkeys.as_ref(), deadline, scorer);
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
    if let Some((name, ascending)) = args.sortby.as_ref().filter(|_| posted.is_none()) {
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
                    sort_key_of(fields, source, numeric, unf)
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
    // ── The page ─────────────────────────────────────────────────────────────
    let (page, total) = match posted {
        Some(posted) => {
            timed_out = posted.timed_out;
            (posted.page, posted.total)
        }
        None => {
            let total = hits.len();
            let page: Vec<Hit> = hits
                .into_iter()
                .skip(args.offset)
                .take(args.count)
                .collect();
            (page, total)
        }
    };
    // A shard answers with what it found and whether it timed out; the
    // coordinator applies `ON_TIMEOUT`.
    if timed_out && !shard && on_timeout_fails() {
        return Err("Timeout limit was reached".into());
    }
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
                Ok(value) if wanted => {
                    highlight(value, &terms, open, close, def.language.language).into_bytes()
                }
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
            highlight(
                "The Quick brown fox, foxes",
                &terms,
                "<b>",
                "</b>",
                crate::vector::stem::Language::English
            ),
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

    /// Postings of `count` documents `k0` …, their NUMERIC field `n` from
    /// `values` in turn.
    fn numbered(values: &[&str], count: usize) -> Postings {
        let schema = vec![SchemaField {
            name: "n".into(),
            field_type: FieldType::Numeric,
            options: FieldOptions::default(),
        }];
        let mut p = Postings::new(&schema);
        for i in 0..count {
            let value: Arc<[u8]> = Arc::from(values[i % values.len()].as_bytes());
            p.write(&format!("k{i}"), &[("n".to_string(), value)], true);
        }
        // What the postings' thread does once there is enough of it.
        while p.tidy_step() {}
        p
    }

    /// `ids` in the full path's SORTBY order: every one sorted.
    fn sorted_whole(p: &Postings, ids: &Bits, ascending: bool) -> Vec<u32> {
        use std::cmp::Ordering;
        let numbers = p.numbers("n").unwrap();
        let mut all: Vec<u32> = ids.iter().collect();
        all.sort_by(|&a, &b| {
            let order = match (numbers.value(a), numbers.value(b)) {
                (Some(x), Some(y)) if ascending => x.total_cmp(&y),
                (Some(x), Some(y)) => y.total_cmp(&x),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            };
            order.then_with(|| key_of(p, a).cmp(key_of(p, b)))
        });
        all
    }

    #[test]
    fn a_page_from_postings_is_in_the_full_paths_order() {
        // Ties, minus zero beside zero, infinities, and a value that is no
        // number (last, either way round).
        let p = numbered(
            &["3", "-0", "0", "x", "-1.5", "3", "inf", "-inf", "0", "7"],
            300,
        );
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for round in 0..40 {
            // From all of them to a few: both ways through the numbers.
            let keep = [100, 90, 50, 10, 2][round % 5];
            let mut ids = Bits::new();
            for id in p.live().iter() {
                if next() % 100 < keep {
                    ids.set(id);
                }
            }
            let of = ids.count();
            for ascending in [true, false] {
                let whole = sorted_whole(&p, &ids, ascending);
                let needs = [0, 1, 2, 5, 17, of / 2, of.saturating_sub(1), of];
                for need in needs.into_iter().filter(|&need| need <= of) {
                    let mut clock = Clock::new(None);
                    let first: Vec<u32> =
                        first_by_number(&p, "n", &ids, need, ascending, &mut clock)
                            .unwrap()
                            .into_iter()
                            .map(|(id, _)| id)
                            .collect();
                    assert_eq!(
                        first.as_slice(),
                        &whole[..need],
                        "round {round}, {need} of {of}, ascending {ascending}"
                    );
                    assert!(!clock.out);
                }
            }
            let mut by_key: Vec<u32> = ids.iter().collect();
            by_key.sort_by(|&a, &b| key_of(&p, a).cmp(key_of(&p, b)));
            for need in [0, 1, 3, of / 5, of / 3, of]
                .into_iter()
                .filter(|&n| n <= of)
            {
                let first = first_by_key(&p, &ids, need, of, &mut Clock::new(None));
                assert_eq!(first.as_slice(), &by_key[..need], "round {round}");
            }
        }
        // A NaN anywhere in the field: no order from the postings.
        let p = numbered(&["1", "nan"], 10);
        assert!(first_by_number(&p, "n", p.live(), 3, true, &mut Clock::new(None)).is_none());
    }

    #[test]
    fn a_page_from_postings_stops_at_the_deadline() {
        let p = numbered(&["1", "2", "x"], 5000);
        let Some(past) = Instant::now().checked_sub(Duration::from_secs(1)) else {
            return;
        };
        // The clock is read after 1024 documents: the first of those.
        let mut clock = Clock::new(Some(past));
        let first = first_by_key(&p, p.live(), 3000, 5000, &mut clock);
        assert!(clock.out);
        assert!(first.len() < 3000, "{}", first.len());
        let mut clock = Clock::new(Some(past));
        let first = first_by_number(&p, "n", p.live(), 4000, true, &mut clock).unwrap();
        assert!(clock.out);
        assert!(first.len() < 4000, "{}", first.len());
        // Without a deadline, whole: those with a number, then the others.
        let mut clock = Clock::new(None);
        let first = first_by_number(&p, "n", p.live(), 4000, true, &mut clock).unwrap();
        assert_eq!(first.len(), 4000);
        assert!(first[3333].1.is_some() && first[3334].1.is_none());
        assert!(!clock.out);
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
