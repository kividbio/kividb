use std::collections::BTreeSet;
use std::sync::Arc;

use crate::commands::ctx::Ctx;
use crate::resp::write::*;
use crate::vector::{FieldType, LiveIndex, VectorAlgo};

/// Commands whose first argument names an index, and so may name an alias.
fn takes_index_name(name: &str) -> bool {
    matches!(
        name,
        "FT.INFO"
            | "FT.DROPINDEX"
            | "FT.SEARCH"
            | "FT.AGGREGATE"
            | "FT.ALTER"
            | "FT.EXPLAIN"
            | "FT.EXPLAINCLI"
            | "FT.PROFILE"
            | "FT.TAGVALS"
            | "FT.SYNUPDATE"
            | "FT.SYNDUMP"
            | "FT.SPELLCHECK"
            | "FT.HYBRID"
            | "FT.ALIASLIST"
    )
}

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    if !name.starts_with("FT.") {
        return None;
    }
    // An alias is resolved here, once, for every command that takes an index
    // name: the handler then runs against the real index and never sees the
    // alias. (`ctx.cmd` is put back afterwards, so what is propagated is what
    // the client sent.)
    let alias_target = if takes_index_name(name) {
        ctx.cmd
            .get(1)
            .and_then(|b| std::str::from_utf8(b).ok())
            .and_then(|given| crate::vector::alias_target(&ctx.index_store.read(), given))
    } else {
        None
    };
    if let Some(real) = alias_target {
        let original = ctx.cmd[1];
        // `real` outlives the dispatch below, and `ctx.cmd[1]` is restored
        // before it is dropped.
        let erased: &'static [u8] = unsafe { std::mem::transmute(real.as_bytes()) };
        ctx.cmd[1] = erased;
        let reply = dispatch(ctx, name);
        ctx.cmd[1] = original;
        return reply;
    }
    dispatch(ctx, name)
}

fn dispatch(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    match name {
        "FT.CREATE" => crate::commands::search::ft_create(ctx),
        "FT._LIST" => ft_list(ctx),
        "FT.INFO" => crate::commands::search::ft_info(ctx),
        "FT.DROPINDEX" => ft_dropindex(ctx),
        "FT.SEARCH" => crate::commands::search::ft_search(ctx),
        "FT.AGGREGATE" => ft_aggregate(ctx),

        // Alias commands — store a name → index-name mapping
        "FT.ALIASADD" => ft_alias_add(ctx),
        "FT.ALIASDEL" => ft_alias_del(ctx),
        "FT.ALIASUPDATE" => ft_alias_update(ctx),
        "FT.ALIASLIST" => ft_alias_list(ctx),

        "FT.ALTER" => ft_alter(ctx),

        // Dict commands — used for custom synonyms/stopwords
        "FT.DICTADD" => ft_dictadd(ctx),
        "FT.DICTDEL" => ft_dictdel(ctx),
        "FT.DICTDUMP" => ft_dictdump(ctx),

        "FT.EXPLAIN" => crate::commands::search_tools::ft_explain(ctx, false),
        "FT.EXPLAINCLI" => crate::commands::search_tools::ft_explain(ctx, true),
        "FT.PROFILE" => crate::commands::search_tools::ft_profile(ctx),
        "FT.CONFIG" => crate::commands::search_tools::ft_config(ctx),
        "FT.CURSOR" => crate::commands::search_tools::ft_cursor(ctx),
        "FT.TAGVALS" => ft_tagvals(ctx),
        "FT.SYNUPDATE" => ft_synupdate(ctx),
        "FT.SYNDUMP" => ft_syndump(ctx),
        "FT.SPELLCHECK" => ft_spellcheck(ctx),
        "FT.DEBUG" => ft_debug(ctx),
        "FT.HYBRID" => crate::commands::hybrid::ft_hybrid(ctx),
        _ => None,
    }
}

fn ft_list(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let store = ctx.index_store.read();
    // Indexes, not the aliases that point at them.
    let items: Vec<Vec<u8>> = store
        .iter()
        .filter(|(key, def)| !crate::vector::is_alias(key, def))
        .map(|(key, _)| resp_bulk_str(key))
        .collect();
    Some(resp_array(items))
}

fn ft_dropindex(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'ft.dropindex' command",
        ));
    }
    let name = match std::str::from_utf8(cmd[1]) {
        Ok(s) => s,
        Err(_) => return Some(resp_error("ERR index name must be valid UTF-8")),
    };

    let delete_documents = match &cmd[2..] {
        [] => false,
        [dd] if dd.eq_ignore_ascii_case(b"DD") => true,
        _ => return Some(resp_error("ERR syntax error")),
    };

    // The documents it covered, if they go too: found before the index goes,
    // by its definition. They were kept whatever was asked (#162).
    let documents: Vec<(usize, String)> = if delete_documents {
        let store = ctx.index_store.read();
        match store
            .get(name)
            .filter(|def| !crate::vector::is_alias(name, def))
        {
            Some(def) => {
                let db = def.db_index;
                let mut keys = Vec::new();
                for shard in ctx.store[db].iter() {
                    let map = shard.read();
                    for (key, value) in map.iter() {
                        let covered = def.prefixes.is_empty()
                            || def.prefixes.iter().any(|p| key.starts_with(p.as_str()));
                        if covered
                            && crate::vector::doc_fields(&def.on, &def.fields, value).is_some()
                        {
                            keys.push((db, key.to_string()));
                        }
                    }
                }
                keys
            }
            None => Vec::new(),
        }
    } else {
        Vec::new()
    };

    // The index goes, and its aliases with it.
    if !crate::vector::remove_index(&mut ctx.index_store.write(), name) {
        return Some(resp_error("ERR no such index"));
    }
    // Then its documents. Not each as a DEL to replicas and the AOF: they
    // run this command, and delete theirs.
    for (db, key) in &documents {
        if !crate::commands::helpers::delete_key(ctx.store, *db, key) {
            continue;
        }
        crate::vector::maybe_unindex(ctx.index_store, key);
        crate::stats::record_rdb_change();
        crate::keyspace_events::notify(
            ctx.pubsub,
            *db,
            crate::keyspace_events::KS_GENERIC,
            "del",
            key,
        );
        ctx.watch_registry.notify(*db, key);
        crate::commands::handler::invalidate_tracked_key(ctx, key, false);
    }
    Some(resp_simple("OK"))
}

// ── FT.ALTER ─────────────────────────────────────────────────────────────────
//
// FT.ALTER index [SKIPINITIALSCAN] SCHEMA ADD field type [options ...]
//
// Appends new fields to an existing index's schema.
// Does NOT rebuild the live vector index for existing keys — that would
// require a full table scan and is what SKIPINITIALSCAN controls in Redis.
// For non-vector fields (TEXT/TAG/NUMERIC) there is no live structure to
// update; they are evaluated at query time by scanning hash fields.
// For VECTOR fields added via ALTER we do backfill unless SKIPINITIALSCAN.
//
// Key Rust lesson: we take a write lock on the whole IndexStore, get a
// `&mut IndexDef` from it, and mutate in place. This is the idiomatic way
// to update a value inside Arc<RwLock<HashMap<..>>>.

fn ft_alter(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;

    // Minimum: FT.ALTER <index> SCHEMA ADD <field> <type>
    if cmd.len() < 6 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'ft.alter' command",
        ));
    }

    let index_name = match std::str::from_utf8(cmd[1]) {
        Ok(s) => s.to_string(),
        Err(_) => return Some(resp_error("ERR index name must be valid UTF-8")),
    };

    // Walk args to find SCHEMA ADD, skipping optional SKIPINITIALSCAN
    let mut i = 2usize;
    let mut skip_initial_scan = false;

    while i < cmd.len() {
        let tok = match std::str::from_utf8(cmd[i]) {
            Ok(s) => s.to_ascii_uppercase(),
            Err(_) => return Some(resp_error("ERR invalid argument")),
        };
        match tok.as_str() {
            "SKIPINITIALSCAN" => {
                skip_initial_scan = true;
                i += 1;
            }
            "SCHEMA" => {
                i += 1;
                break;
            }
            _ => {
                i += 1;
            }
        }
    }

    // Now expect ADD
    let add_tok = cmd
        .get(i)
        .and_then(|b| std::str::from_utf8(b).ok())
        .map(|s| s.to_ascii_uppercase());

    match add_tok.as_deref() {
        Some("ADD") => {
            i += 1;
        }
        _ => {
            return Some(resp_error(
                "ERR syntax: FT.ALTER index SCHEMA ADD field type ...",
            ));
        }
    }

    // The same parser as FT.CREATE, so the two always accept the same schema.
    let new_fields = match crate::commands::search::parse_schema_fields(cmd, &mut i) {
        Ok(fields) => fields,
        Err(e) => return Some(resp_error(&e)),
    };

    if new_fields.is_empty() {
        return Some(resp_error("ERR no fields specified"));
    }

    // ── Mutate the stored IndexDef ────────────────────────────────────────────
    //
    // We need a write lock on the store to get &mut IndexDef.
    // This is safe: write() gives exclusive access, so no other thread
    // can read or write the map until we're done.
    let mut store = ctx.index_store.write();
    let def = match store.get_mut(&index_name) {
        Some(d) => d,
        None => return Some(resp_error("ERR no such index")),
    };

    // Check for duplicate field names — Redis rejects them
    for nf in &new_fields {
        if def
            .fields
            .iter()
            .any(|f| f.name == nf.name || f.attribute() == nf.attribute())
        {
            return Some(resp_error(&format!(
                "Duplicate field in schema - {}",
                nf.attribute()
            )));
        }
    }

    // ── Provision live indexes for any newly added VECTOR fields ──────────────
    //
    // A VECTOR field needs an empty FLAT or HNSW structure so HSET can start
    // populating it. A TEXT field needs its words indexed. Both are then
    // filled in from the documents that already exist, below, unless
    // SKIPINITIALSCAN.
    let mut newly_added_vector_fields: Vec<String> = Vec::new();
    let mut newly_added_text_fields: Vec<String> = Vec::new();
    for field in &new_fields {
        match &field.field_type {
            FieldType::Vector { algo, .. } => {
                let live = match algo {
                    VectorAlgo::Flat => {
                        LiveIndex::Flat(Arc::new(crate::vector::VectorIndex::new()))
                    }
                    VectorAlgo::Hnsw {
                        m, ef_construction, ..
                    } => LiveIndex::Hnsw(Arc::new(crate::vector::HnswIndex::new(
                        *m,
                        *ef_construction,
                    ))),
                };
                def.indexes.insert(field.name.clone(), live);
                newly_added_vector_fields.push(field.name.clone());
            }
            FieldType::Text if !field.options.noindex => {
                newly_added_text_fields.push(field.name.clone());
            }
            _ => {}
        }
    }
    def.fields.extend(new_fields);
    let altered = def.clone();

    // We need to drop the write lock before backfilling — backfill takes
    // a *read* lock on the index store, and parking_lot's RwLock is not
    // re-entrant. Holding both would deadlock.
    drop(store);

    // ── Backfill ──────────────────────────────────────────────────────────────
    //
    // Walk every existing key matching the index's prefixes. For each
    // newly added VECTOR field that has a value in the hash, insert it
    // into that field's LiveIndex. Existing fields are already populated;
    // only the new ones need this pass.
    if !skip_initial_scan && !newly_added_vector_fields.is_empty() {
        backfill_new_vector_fields(ctx, &index_name, &newly_added_vector_fields);
    }
    if !skip_initial_scan && !newly_added_text_fields.is_empty() {
        crate::vector::backfill_text_fields(
            &ctx.store[*ctx.selected_db],
            &altered,
            &newly_added_text_fields,
        );
    }

    Some(resp_simple("OK"))
}

// ── FT.AGGREGATE ──────────────────────────────────────────────────────────────
//
// FT.AGGREGATE index query [LOAD ...] [GROUPBY ... REDUCE ...] [SORTBY ...] [LIMIT ...]
//
// The pipeline model:
//   1. Scan all hash keys matching the index prefixes (same as FT.SEARCH
//      but we collect ALL matches, not just KNN results).
//   2. Each clause transforms the Vec<Row> in place.
//   3. Emit the final rows as a RESP array.
//
// A Row is a HashMap<String, String> — field name → string value.
// This mirrors how Redis represents aggregate rows internally.
//
// Rust lesson: we use Vec<AHashMap<String, String>> as our row buffer and
// pass it through each pipeline stage by value (move semantics). Each stage
// consumes the input Vec and produces a new one. This is idiomatic Rust —
// no in-place mutation of shared state, no locks needed after the initial
// data collection phase.

type Row = ahash::AHashMap<String, String>;

pub(crate) fn ft_aggregate(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;

    if cmd.len() < 3 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'ft.aggregate' command",
        ));
    }

    let index_name = match std::str::from_utf8(cmd[1]) {
        Ok(s) => s.to_string(),
        Err(_) => return Some(resp_error("ERR index name must be valid UTF-8")),
    };

    let query_str = match std::str::from_utf8(cmd[2]) {
        Ok(s) => s.to_string(),
        Err(_) => return Some(resp_error("ERR query must be valid UTF-8")),
    };

    // Parse the query into a filter AST.
    // FT.AGGREGATE query syntax is the same as FT.SEARCH pre-filter syntax:
    //   "*"                        → match all (no filter)
    //   "@category:{electronics}"  → TAG filter
    //   "@price:[0 200]"           → NUMERIC range
    //   "(@tag:{x} @price:[0 100])"→ AND of two clauses
    //
    // We reuse parse_prefilter from the filter module — it already handles
    // the full pre-filter grammar. For aggregate queries there's no KNN
    // suffix, so parse_prefilter just sees the whole string as the filter part.
    //
    // The query is parsed against the index's schema, so the index comes first.

    // ── Resolve the index ─────────────────────────────────────────────────────
    let index_store = ctx.index_store.read();
    let def = match index_store.get(&index_name) {
        Some(d) => d.clone(), // clone so we can drop the store lock before scanning
        None => {
            return Some(resp_error(&crate::commands::search::no_such_index(
                &index_name,
            )));
        }
    };
    drop(index_store);

    // PARAMS, wherever in the command they are.
    let mut params = crate::vector::filter::Params::new();
    if let Some(at) = cmd.iter().position(|a| a.eq_ignore_ascii_case(b"PARAMS")) {
        let n = cmd
            .get(at + 1)
            .and_then(|a| std::str::from_utf8(a).ok())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);
        let values: Vec<&[u8]> = cmd.iter().skip(at + 2).take(n).copied().collect();
        for pair in values.chunks(2) {
            if let [name, value] = pair {
                params.insert(String::from_utf8_lossy(name).into_owned(), value.to_vec());
            }
        }
    }
    let (filter, knn) = match crate::vector::filter::parse_query(&query_str, &params, &def) {
        Ok(query) => (query.filter, query.knn),
        Err(e) => return Some(resp_error(&e)),
    };

    // `ADDSCORES`: the relevance of each document, as `@__score`.
    let addscores = cmd.iter().any(|a| a.eq_ignore_ascii_case(b"ADDSCORES"));
    let mut terms = ahash::AHashSet::new();
    crate::commands::search::query_terms(&filter, &mut terms);
    let cursor = match crate::commands::search_tools::cursor_options(cmd) {
        Ok(cursor) => cursor,
        Err(e) => return Some(resp_error(&e)),
    };
    // `TIMEOUT` where an option may be, not a word of another clause that
    // happens to read `TIMEOUT` (`APPLY … AS timeout`, a parameter).
    let mut timeout_ms = None;
    if let Some(at) = crate::commands::aggregate_pipeline::option_at(cmd, 3, "TIMEOUT") {
        match cmd
            .get(at + 1)
            .and_then(|a| std::str::from_utf8(a).ok())
            .and_then(|s| s.parse::<u64>().ok())
        {
            Some(ms) => timeout_ms = Some(ms),
            None => {
                return Some(resp_error(
                    "Bad arguments for TIMEOUT: expected a non-negative integer",
                ));
            }
        }
    }
    let deadline = crate::commands::search::search_deadline(timeout_ms);

    // ── Stage 0: Collect all matching rows ────────────────────────────────────
    //
    // Walk every shard, collect hashes that match our prefixes.
    // Each hash becomes a Row with all its fields as string values.
    let mut rows: Vec<Row> = Vec::new();
    let db_ref = &ctx.store[*ctx.selected_db];
    // In a cluster, only the keys of the slots this node serves: see
    // `cluster::search`.
    let cluster = ctx.cluster.enabled.then_some(ctx.cluster);
    // The deadline is kept between runs of documents: see `scan_docs`.
    let timed_out_scan = crate::commands::search::scan_docs(db_ref, deadline, |key, value| {
        // ── Prefix check ─────────────────────────────────────────────
        let matches =
            def.prefixes.is_empty() || def.prefixes.iter().any(|p| key.starts_with(p.as_str()));
        if !matches {
            return;
        }
        if cluster.is_some_and(|c| !crate::cluster::search::serves(c, key)) {
            return;
        }

        let Some(inner) = crate::vector::doc_fields(&def.on, &def.fields, value) else {
            return;
        };
        let json = matches!(def.on, crate::vector::IndexOn::Json);

        // ── Filter check ──────────────────────────────────────────────
        // Evaluate the filter against the raw hash fields BEFORE converting
        // to strings. This is the correct order: filter is cheap (byte
        // comparisons on Arc<[u8]>), string conversion is not free.
        // For MatchAll, eval() returns true immediately — no overhead.
        if !crate::vector::filter::eval(&filter, &*inner) {
            return;
        }

        // ── Build row ─────────────────────────────────────────────────
        // Only rows that passed the filter reach here.
        let mut row: Row = ahash::AHashMap::new();

        // Always include the key itself as a special field.
        // Prefixed with __ so it doesn't collide with user field names.
        row.insert("__key".to_string(), key.to_string());
        if addscores {
            row.insert(
                "__score".to_string(),
                crate::commands::search::number_text(crate::commands::search::relevance(
                    &def, &inner, &terms,
                )),
            );
        }

        for (field, val) in inner.iter() {
            // Text only: a vector's bytes are not a value a row can show
            // or a pipeline can compute with.
            let Ok(s) = std::str::from_utf8(val) else {
                continue;
            };
            // A field of a JSON document goes by the name the schema
            // gives its path.
            let shown = match def.fields.iter().find(|f| json && &f.name == field) {
                Some(f) => f.attribute().to_string(),
                None => field.clone(),
            };
            row.insert(shown, s.to_string());
        }

        rows.push(row);
    });
    let mut timed_out = timed_out_scan;

    // ── A KNN clause: the nearest `k` of what the filter left ────────────────
    //
    // Exactly, as FT.SEARCH does with a filter, and with the distance under
    // the clause's name, for the pipeline. It was dropped, and every
    // document the filter left went on (#162). After a scan cut short, of
    // the rows it found; and it keeps the deadline too.
    if let Some(knn) = &knn {
        let Some(field) = def.fields.iter().find(|f| f.name == knn.field) else {
            return Some(resp_error("The vector field is gone from the index"));
        };
        let FieldType::Vector { metric, .. } = &field.field_type else {
            return Some(resp_error("The vector field is gone from the index"));
        };
        let Some(live) = def.indexes.get(&knn.field) else {
            return Some(resp_error("The vector field has no index"));
        };
        let candidates = rows
            .iter()
            .filter_map(|row| row.get("__key").map(String::as_str));
        let (nearest, cut) = knn_filtered(live, &knn.vector, knn.k, metric, candidates, deadline);
        timed_out |= cut;
        let rank: ahash::AHashMap<String, (usize, f32)> = nearest
            .into_iter()
            .enumerate()
            .map(|(i, found)| (found.key, (i, found.score)))
            .collect();
        rows.retain(|row| row.get("__key").is_some_and(|key| rank.contains_key(key)));
        for row in &mut rows {
            let (_, distance) = rank[&row["__key"]];
            row.insert(
                knn.alias.clone(),
                crate::commands::search::distance_text(distance),
            );
        }
        rows.sort_by_key(|row| rank[&row["__key"]].0);
    }

    // ── The rest of the cluster ──────────────────────────────────────────────
    // Asked by a coordinator: the rows as they are, every column, and
    // whether this scan ran out of time, if the coordinator asks (one from
    // before there was a timeout does not). A coordinator: the rows of every
    // other master too, before any step, then `ON_TIMEOUT`.
    if crate::cluster::search::is_forwarded(ctx) {
        let flag = crate::cluster::search::wants_timeout_flag(cmd).then_some(timed_out);
        return Some(crate::cluster::search::encode_rows(&rows, flag));
    }
    if crate::cluster::search::coordinates(ctx) {
        let mut owned: Vec<Vec<u8>> = cmd.iter().map(|a| a.to_vec()).collect();
        crate::cluster::search::ask_for_timeout_flag(&mut owned);
        match crate::cluster::search::fan_out(ctx, &owned) {
            Ok(replies) => {
                for reply in replies {
                    let (more, shard_timed_out) = crate::cluster::search::decode_rows(reply);
                    rows.extend(more);
                    timed_out |= shard_timed_out;
                }
            }
            Err(e) => return Some(resp_error(&e)),
        }
        // Each master sent its own nearest: the nearest of them all.
        if let Some(knn) = &knn {
            let distance = |row: &Row| {
                row.get(&knn.alias)
                    .and_then(|d| d.parse::<f64>().ok())
                    .unwrap_or(f64::INFINITY)
            };
            rows.sort_by(|a, b| distance(a).total_cmp(&distance(b)));
            rows.truncate(knn.k);
        }
    }
    if timed_out && crate::commands::search::on_timeout_fails() {
        return Some(resp_error("Timeout limit was reached"));
    }

    // ── Parse and execute pipeline clauses ────────────────────────────────────
    // To the same deadline: GROUPBY, APPLY, FILTER and SORTBY over a large
    // scan cost as much as the scan.
    let (rows, cut) = crate::commands::aggregate_pipeline::run_within(rows, cmd, 3, deadline);
    timed_out |= cut;
    if cut && crate::commands::search::on_timeout_fails() {
        return Some(resp_error("Timeout limit was reached"));
    }

    // ── Emit final rows ─────────────────────────────────────────────────────
    //
    // A row shows what the command asked to see: the fields it LOADed, the
    // ones APPLY computed, the ones it sorted by — or, after a GROUPBY, the
    // group. It used to show every field of the document, whatever was
    // asked for, with a vector's bytes turned into an empty string.
    let word = |i: usize| -> String {
        cmd.get(i)
            .map(|a| String::from_utf8_lossy(a).to_ascii_uppercase())
            .unwrap_or_default()
    };
    let name = |i: usize| -> String {
        cmd.get(i)
            .map(|a| {
                String::from_utf8_lossy(a)
                    .trim_start_matches('@')
                    .to_string()
            })
            .unwrap_or_default()
    };
    let count = |i: usize| -> usize { word(i).parse::<usize>().unwrap_or(0) };
    let mut everything = false;
    let mut grouped = false;
    // (the row's field, the name to show it under)
    let mut shown: Vec<(String, String)> = Vec::new();
    let mut i = 3usize;
    while i < cmd.len() {
        match word(i).as_str() {
            "LOAD" if word(i + 1) == "*" => {
                everything = true;
                i += 2;
            }
            "LOAD" => {
                let end = (i + 2 + count(i + 1)).min(cmd.len());
                let mut at = i + 2;
                while at < end {
                    let field = name(at);
                    at += 1;
                    let mut label = field.clone();
                    if at + 1 < end && word(at) == "AS" {
                        label = name(at + 1);
                        at += 2;
                    }
                    shown.push((field, label));
                }
                i = end;
            }
            "GROUPBY" => {
                grouped = true;
                i += 1;
            }
            "APPLY" => {
                let label = if word(i + 2) == "AS" {
                    name(i + 3)
                } else {
                    cmd.get(i + 1)
                        .map(|a| String::from_utf8_lossy(a).into_owned())
                        .unwrap_or_default()
                };
                shown.push((label.clone(), label));
                i += 2;
            }
            "SORTBY" => {
                let end = (i + 2 + count(i + 1)).min(cmd.len());
                for arg in &cmd[(i + 2).min(end)..end] {
                    if let Some(field) = arg.strip_prefix(b"@") {
                        let field = String::from_utf8_lossy(field).into_owned();
                        shown.push((field.clone(), field));
                    }
                }
                i = end;
            }
            _ => i += 1,
        }
    }

    let rows_out: Vec<crate::commands::search_tools::ShownRow> = rows
        .into_iter()
        .map(|row| {
            let mut pairs: Vec<(String, String)> = Vec::new();
            if everything || grouped {
                let mut fields: Vec<(&String, &String)> =
                    row.iter().filter(|(k, _)| !k.starts_with("__")).collect();
                fields.sort();
                for (k, v) in fields {
                    pairs.push((k.clone(), v.clone()));
                }
            } else {
                let mut seen: Vec<&str> = Vec::new();
                for (field, label) in &shown {
                    if seen.contains(&label.as_str()) {
                        continue;
                    }
                    if let Some(value) = row.get(field) {
                        seen.push(label);
                        pairs.push((label.clone(), value.clone()));
                    }
                }
            }
            pairs
        })
        .collect();

    // `WITHCURSOR`: the first rows now, the rest as they are asked for.
    if let Some((count, max_idle)) = cursor {
        return Some(crate::commands::search_tools::open_cursor(
            ctx.resp,
            &index_name,
            rows_out,
            count,
            max_idle,
            timed_out,
        ));
    }
    Some(crate::commands::search_tools::rows_reply(
        ctx.resp,
        rows_out.len(),
        &rows_out,
        timed_out,
    ))
}
// ── format_f64 ────────────────────────────────────────────────────────────────
//
// Format a float for aggregate output.
// Integers display without decimal point; floats show up to 6 sig digits.
// This matches Redis's behaviour for numeric aggregate results.

pub(crate) fn format_f64(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{:.6}", v)
    }
}

// ── FT.TAGVALS ────────────────────────────────────────────────────────────────
//
// FT.TAGVALS index field_name
//
// Returns all distinct values stored in a TAG field across every indexed key.
// This is used by clients like RedisInsight for autocomplete dropdowns and
// by OM (Object Mapping) libraries to enumerate facet values.
//
// How it works:
//   1. Resolve the index by name.
//   2. Verify the named field exists and is of type TAG.
//   3. Walk every shard, collect hashes matching the index prefixes.
//   4. For each hash, read the TAG field value, split on the Redis TAG
//      separator ("|" or ","), trim, lowercase, and insert into a set.
//   5. Return the deduplicated set as a sorted RESP array.
//
// Rust lesson: we use an AHashSet<String> for O(1) deduplication while
// collecting, then collect into a Vec and sort before emitting. This is
// the standard collect-deduplicate-sort pattern.

fn ft_tagvals(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() != 3 {
        return Some(resp_error("ERR syntax: FT.TAGVALS index field"));
    }

    let index_name = match std::str::from_utf8(cmd[1]) {
        Ok(s) => s,
        Err(_) => return Some(resp_error("ERR index name must be valid UTF-8")),
    };
    let field_name = match std::str::from_utf8(cmd[2]) {
        Ok(s) => s.to_string(),
        Err(_) => return Some(resp_error("ERR field name must be valid UTF-8")),
    };

    // Resolve the index and the field: where its value is stored, and how it
    // is split into tags. Cloned out, so the store lock is not held while the
    // shards are scanned.
    let (prefixes, tag_field, doc_on, doc_schema) = {
        let store = ctx.index_store.read();
        let def = match store.get(index_name) {
            Some(d) => d,
            None => {
                return Some(resp_error(&crate::commands::search::no_such_index(
                    index_name,
                )));
            }
        };
        let tag_field = def
            .field_by_attribute(&field_name)
            .filter(|f| matches!(f.field_type, FieldType::Tag))
            .map(|f| {
                (
                    f.name.clone(),
                    f.options.separator,
                    f.options.case_sensitive,
                )
            });
        (
            def.prefixes.clone(),
            tag_field,
            def.on.clone(),
            def.fields.clone(),
        )
    };
    let Some((stored_as, separator, case_sensitive)) = tag_field else {
        return Some(resp_error(
            "ERR field is not a TAG field or does not exist in index",
        ));
    };

    // Scan all shards and collect distinct tag values.
    let mut distinct: ahash::AHashSet<String> = ahash::AHashSet::new();
    let db_ref = &ctx.store[*ctx.selected_db];

    for shard in db_ref.iter() {
        let map = shard.read();
        for (key, value) in map.iter() {
            let matches =
                prefixes.is_empty() || prefixes.iter().any(|p| key.starts_with(p.as_str()));
            if !matches {
                continue;
            }
            let Some(inner) = crate::vector::doc_fields(&doc_on, &doc_schema, value) else {
                continue;
            };
            let Some(s) = inner
                .get(&stored_as)
                .and_then(|v| std::str::from_utf8(v).ok())
            else {
                continue;
            };
            // The field's separator — it used to be split at `|` and `,`
            // whatever the schema said.
            for tag in crate::vector::filter::split_tags(s, separator) {
                distinct.insert(if case_sensitive {
                    tag.to_string()
                } else {
                    tag.to_lowercase()
                });
            }
        }
    }

    // In a cluster, the values of every master's documents.
    if crate::cluster::search::coordinates(ctx) {
        let owned: Vec<Vec<u8>> = ctx.cmd.iter().map(|a| a.to_vec()).collect();
        match crate::cluster::search::fan_out(ctx, &owned) {
            Ok(replies) => {
                for reply in replies {
                    if let crate::cluster::search::Reply::Array(items) = reply {
                        distinct.extend(items.iter().filter_map(|v| v.text()));
                    }
                }
            }
            Err(e) => return Some(resp_error(&e)),
        }
    }

    // Sort for deterministic output — clients often display these in UI
    let mut values: Vec<String> = distinct.into_iter().collect();
    values.sort_unstable();

    let items: Vec<Vec<u8>> = values.iter().map(|v| resp_bulk_str(v)).collect();
    Some(resp_array(items))
}

// ── FT.SYNUPDATE ─────────────────────────────────────────────────────────────
//
// FT.SYNUPDATE index synonym_group_id [SKIPINITIALSCAN] term [term ...]
//
// Creates or updates a synonym group. Multiple terms in a group are treated
// as equivalent during TEXT field search — searching for any one of them
// also matches the others.
//
// We store synonym groups inside IndexDef.synonyms (added above).
// The group_id is a string key; the value is a Vec<String> of terms.
//
// Rust lesson: `store.get_mut(key)` on a write-locked HashMap gives us
// &mut IndexDef so we can mutate synonyms in place. The entry API
// (`entry(...).or_insert_with(...)`) is idiomatic for "get or create".

fn ft_synupdate(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;

    // FT.SYNUPDATE index group_id [SKIPINITIALSCAN] term [term ...]
    if cmd.len() < 4 {
        return Some(resp_error(
            "ERR syntax: FT.SYNUPDATE index group_id term [term ...]",
        ));
    }

    let index_name = match std::str::from_utf8(cmd[1]) {
        Ok(s) => s.to_string(),
        Err(_) => return Some(resp_error("ERR index name must be valid UTF-8")),
    };
    let group_id = match std::str::from_utf8(cmd[2]) {
        Ok(s) => s.to_string(),
        Err(_) => return Some(resp_error("ERR group ID must be valid UTF-8")),
    };

    // Collect terms, skipping optional SKIPINITIALSCAN flag
    let terms: Vec<String> = cmd[3..]
        .iter()
        .filter_map(|b| std::str::from_utf8(b).ok())
        .filter(|s| !s.eq_ignore_ascii_case("SKIPINITIALSCAN"))
        .map(|s| s.to_lowercase())
        .collect();

    if terms.is_empty() {
        return Some(resp_error("ERR at least one term is required"));
    }

    let mut store = ctx.index_store.write();
    let def = match store.get_mut(&index_name) {
        Some(d) => d,
        None => return Some(resp_error("ERR no such index")),
    };

    // entry().or_insert_with() is the idiomatic Rust "get or create" pattern.
    // If the group already exists we extend it; if not we create it.
    let group = def.synonyms.entry(group_id).or_insert_with(Vec::new);
    for term in terms {
        if !group.contains(&term) {
            group.push(term);
        }
    }
    drop(store);

    Some(resp_simple("OK"))
}

// ── FT.SYNDUMP ────────────────────────────────────────────────────────────────
//
// FT.SYNDUMP index
//
// Returns all synonym groups as a flat array of alternating:
//   term, [group_id, group_id, ...]
//
// Redis's response format maps each *term* to the list of groups it belongs
// to (the inverse of how we store it). We invert on the fly.
//
// Example:
//   FT.SYNUPDATE idx grp1 hello world
//   FT.SYNUPDATE idx grp2 world earth
//   FT.SYNDUMP idx →
//     1) "hello"
//     2) 1) "grp1"
//     3) "world"
//     4) 1) "grp1"
//        2) "grp2"
//     5) "earth"
//     6) 1) "grp2"

fn ft_syndump(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() != 2 {
        return Some(resp_error("ERR syntax: FT.SYNDUMP index"));
    }

    let index_name = match std::str::from_utf8(cmd[1]) {
        Ok(s) => s,
        Err(_) => return Some(resp_error("ERR index name must be valid UTF-8")),
    };

    let store = ctx.index_store.read();
    let def = match store.get(index_name) {
        Some(d) => d,
        None => return Some(resp_error("ERR no such index")),
    };

    if def.synonyms.is_empty() {
        return Some(resp_array(vec![]));
    }

    // Invert: group_id → [term] becomes term → [group_id]
    // We use a BTreeMap here (not AHashMap) so output is sorted by term —
    // deterministic output matters for tests and client compatibility.
    let mut term_to_groups: std::collections::BTreeMap<&str, Vec<&str>> =
        std::collections::BTreeMap::new();

    for (group_id, terms) in &def.synonyms {
        for term in terms {
            term_to_groups
                .entry(term.as_str())
                .or_default()
                .push(group_id.as_str());
        }
    }

    // Build flat alternating array: term, [groups], term, [groups], ...
    let mut out: Vec<Vec<u8>> = Vec::new();
    for (term, mut groups) in term_to_groups {
        groups.sort_unstable(); // sort group IDs for determinism
        out.push(resp_bulk_str(term));
        let group_items: Vec<Vec<u8>> = groups.iter().map(|g| resp_bulk_str(g)).collect();
        out.push(resp_array(group_items));
    }

    Some(resp_array(out))
}

// ── FT.SPELLCHECK ─────────────────────────────────────────────────────────────
//
// FT.SPELLCHECK index query [DISTANCE distance] [TERMS INCLUDE|EXCLUDE dict ...]
//
// Returns spelling suggestions for unrecognized terms in the query.
// Redis compares query tokens against the indexed vocabulary (terms seen in
// TEXT fields) and returns candidates within edit distance N.
//
// Full implementation requires:
//   1. A vocabulary built from all indexed TEXT field values (expensive to
//      maintain incrementally — typically built at query time or periodically).
//   2. An edit-distance (Levenshtein) function.
//   3. Candidate generation (all words within distance N).
//
// We implement this properly with a real Levenshtein computation against the
// live vocabulary. The vocabulary is built on-demand by scanning indexed hashes
// for TEXT field values — this is O(n*m) where n=docs, m=text fields, but
// spell-check is a low-frequency operation so that's acceptable.
//
// Rust lesson: Levenshtein distance is a classic DP problem. We implement it
// with the standard two-row rolling array optimization: instead of an n×m
// matrix we keep only two rows at a time, cutting memory from O(n*m) to O(m).

fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let (na, nb) = (a.len(), b.len());

    if na == 0 {
        return nb;
    }
    if nb == 0 {
        return na;
    }

    // prev[j] = edit distance between a[0..0] and b[0..j]
    let mut prev: Vec<usize> = (0..=nb).collect();
    let mut curr: Vec<usize> = vec![0; nb + 1];

    for i in 1..=na {
        curr[0] = i; // deleting i chars from a to match empty b
        for j in 1..=nb {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            curr[j] = (prev[j] + 1) // deletion
                .min(curr[j - 1] + 1) // insertion
                .min(prev[j - 1] + cost); // substitution
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[nb] // after the swap, prev holds the last computed row
}

fn ft_spellcheck(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return Some(resp_error(
            "ERR syntax: FT.SPELLCHECK index query [DISTANCE n]",
        ));
    }

    let index_name = match std::str::from_utf8(cmd[1]) {
        Ok(s) => s.to_string(),
        Err(_) => return Some(resp_error("ERR index name must be valid UTF-8")),
    };
    let query = match std::str::from_utf8(cmd[2]) {
        Ok(s) => s.to_lowercase(),
        Err(_) => return Some(resp_error("ERR query must be valid UTF-8")),
    };

    // Parse optional DISTANCE
    let mut max_distance: usize = 1; // Redis default
    let mut i = 3;
    while i < cmd.len() {
        let tok = match std::str::from_utf8(cmd[i]) {
            Ok(s) => s.to_ascii_uppercase(),
            Err(_) => {
                i += 1;
                continue;
            }
        };
        if tok == "DISTANCE" {
            if let Some(d) = cmd
                .get(i + 1)
                .and_then(|b| std::str::from_utf8(b).ok())
                .and_then(|s| s.parse().ok())
            {
                max_distance = d;
            }
            i += 2;
        } else {
            i += 1;
        }
    }

    // ── Read the per-index vocabulary ─────────────────────────────────────
    //
    // The vocab is maintained incrementally by maybe_index and backfill_index.
    // We clone the Arc to drop the IndexStore lock before doing the lookup
    // loop — keeps the fast path uncontended.
    let vocab_arc = {
        let store = ctx.index_store.read();
        let def = match store.get(&index_name) {
            Some(d) => d,
            None => return Some(resp_error("ERR no such index")),
        };
        Arc::clone(&def.vocab)
    };

    // Take a read lock on the vocab itself. We need to convert it into a
    // BTreeSet<String> we can mutate (for INCLUDE/EXCLUDE dicts), so clone
    // the contents. For a typical 100k-word vocab this is a few MB — cheap
    // compared to the avoided shard walk.
    //
    // The vocab is then a local owned BTreeSet; we drop the read lock before
    // any further work so concurrent HSETs can keep updating the index.
    let mut vocab: BTreeSet<String> = {
        let v = vocab_arc.read();
        v.clone()
    };

    // AFTER the vocab-building loop over shards,
    // this block is used to merge in any INCLUDE dicts and remove EXCLUDE dicts.
    // First, re-parse TERMS INCLUDE/EXCLUDE from the command:

    // Parse TERMS INCLUDE|EXCLUDE dict clauses
    // These appear after DISTANCE in the command args.
    // We re-scan cmd here since we already consumed i above.
    let mut include_dicts: Vec<String> = Vec::new();
    let mut exclude_dicts: Vec<String> = Vec::new();
    let mut j = 3usize;
    while j < cmd.len() {
        let tok = match std::str::from_utf8(cmd[j]) {
            Ok(s) => s.to_ascii_uppercase(),
            Err(_) => {
                j += 1;
                continue;
            }
        };
        if tok == "TERMS" {
            j += 1;
            let mode = match std::str::from_utf8(cmd.get(j).unwrap_or(&b"".as_ref())) {
                Ok(s) => s.to_ascii_uppercase(),
                Err(_) => {
                    j += 1;
                    continue;
                }
            };
            j += 1;
            // Collect dict names until next keyword
            while j < cmd.len() {
                let next = match std::str::from_utf8(cmd[j]) {
                    Ok(s) => s.to_ascii_uppercase(),
                    Err(_) => break,
                };
                if next == "TERMS" || next == "DISTANCE" || next == "DIALECT" {
                    break;
                }
                let dict_name = std::str::from_utf8(cmd[j]).unwrap_or("").to_string();
                match mode.as_str() {
                    "INCLUDE" => include_dicts.push(dict_name),
                    "EXCLUDE" => exclude_dicts.push(dict_name),
                    _ => {}
                }
                j += 1;
            }
        } else {
            j += 1;
        }
    }

    // Merge INCLUDE dicts into vocabulary
    {
        let dict_store = ctx.dict_store.read();
        for dict_name in &include_dicts {
            if let Some(dict) = dict_store.get(dict_name) {
                vocab.extend(dict.iter().cloned());
            }
        }
        // Remove EXCLUDE dict words from vocabulary
        for dict_name in &exclude_dicts {
            if let Some(dict) = dict_store.get(dict_name) {
                for word in dict.iter() {
                    vocab.remove(word);
                }
            }
        }
    }

    // For each token in the query, check if it's in the vocabulary.
    // If not, find all vocab words within max_distance edits.
    //
    // Redis response format per term:
    //   ["TERM", term, [[score, suggestion], [score, suggestion], ...]]
    // Score = 1.0 - (edit_distance / max(len_a, len_b)) — higher is better.

    let mut out: Vec<Vec<u8>> = Vec::new();

    // The query's words as the index has them, so that `world_x` and
    // `users\.u_1` are the words the vocabulary holds.
    for token_clean in crate::vector::tokenize_for_vocab(&query) {
        if token_clean.is_empty() || vocab.contains(&token_clean) {
            continue; // correctly spelled — skip
        }

        // Find candidates within edit distance
        let mut candidates: Vec<(f64, &str)> = vocab
            .iter()
            .filter_map(|word| {
                let dist = levenshtein(&token_clean, word);
                if dist <= max_distance && dist > 0 {
                    // Normalize score: 1.0 = identical, lower = more edits needed
                    let max_len = token_clean.len().max(word.len()) as f64;
                    let score = 1.0 - (dist as f64 / max_len);
                    Some((score, word.as_str()))
                } else {
                    None
                }
            })
            .collect();

        // Sort by score descending (best suggestions first)
        candidates.sort_unstable_by(|a, b| b.0.total_cmp(&a.0));

        // Build the suggestion sub-array: [[score, word], ...]
        let suggestions: Vec<Vec<u8>> = candidates
            .iter()
            .map(|(score, word)| {
                resp_array(vec![
                    resp_bulk_str(&format!("{:.4}", score)),
                    resp_bulk_str(word),
                ])
            })
            .collect();

        // Each term result: ["TERM", token, [suggestions]]
        out.push(resp_array(vec![
            resp_bulk_str("TERM"),
            resp_bulk_str(&token_clean),
            resp_array(suggestions),
        ]));
    }

    Some(resp_array(out))
}

// ── Helpers for filtered search ──────────────────────────────────────────────

/// The nearest `k` of the documents offered to it, measured as they come:
/// what a filtered KNN keeps while the filter runs.
///
/// A filter used to hand its keys over as a set of cloned `String`s, which
/// the KNN then looked up again one by one. The scorer is offered each key
/// as the filter accepts it, measures it at once against the query (scaled
/// to unit length once, for a cosine), and keeps only the best `k`: a key is
/// copied only when it is among them so far.
///
/// Each offer takes the vector index's locks for itself and lets them go:
/// it runs under a shard's read lock, and a write to that shard can hold the
/// shard while it waits on them.
pub(crate) struct FilteredKnn<'a> {
    live: &'a LiveIndex,
    query: crate::vector::PreparedQuery<'a>,
    best: Best,
}

impl<'a> FilteredKnn<'a> {
    pub(crate) fn new(
        live: &'a LiveIndex,
        query: &'a [f32],
        k: usize,
        metric: &'a crate::vector::DistanceMetric,
    ) -> Self {
        Self {
            live,
            query: crate::vector::PreparedQuery::new(metric, query),
            best: Best::new(k),
        }
    }

    /// Measure the document at `key`, if the index holds a vector for it.
    #[inline]
    pub(crate) fn offer(&mut self, key: &str) {
        let dim = self.query.vector().len();
        let distance = match self.live {
            LiveIndex::Flat(idx) => {
                let vectors = idx.vectors.read();
                match vectors.get(key) {
                    Some(v) if v.len() == dim => self.query.to_raw(v),
                    _ => return,
                }
            }
            LiveIndex::Hnsw(idx) => {
                let Some(id) = idx.key_to_id.read().get(key).copied() else {
                    return;
                };
                let nodes = idx.nodes.read();
                match nodes.get(id) {
                    Some(n) if !n.vector.is_empty() && n.vector.len() == dim => {
                        self.query.to_unit(&n.vector)
                    }
                    _ => return,
                }
            }
        };
        self.best.offer(distance, key);
    }

    /// The nearest, nearest first.
    pub(crate) fn nearest(self) -> Vec<crate::vector::KnnResult> {
        self.best.into_sorted()
    }
}

/// The `k` smallest distances offered, with their keys: a max-heap whose
/// top is the one to beat.
struct Best {
    k: usize,
    heap: std::collections::BinaryHeap<Far>,
}

/// A distance ordered for the heap: `total_cmp`, so a NaN is the farthest.
struct Far(f32, String);

impl PartialEq for Far {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0).is_eq()
    }
}
impl Eq for Far {}
impl PartialOrd for Far {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Far {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl Best {
    fn new(k: usize) -> Self {
        Self {
            k,
            heap: std::collections::BinaryHeap::with_capacity(k.min(4096) + 1),
        }
    }

    #[inline]
    fn offer(&mut self, distance: f32, key: &str) {
        if self.heap.len() < self.k {
            self.heap.push(Far(distance, key.to_string()));
        } else if let Some(mut top) = self.heap.peek_mut()
            && distance.total_cmp(&top.0).is_lt()
        {
            // In place: the key's buffer is reused.
            top.0 = distance;
            top.1.clear();
            top.1.push_str(key);
        }
    }

    fn into_sorted(self) -> Vec<crate::vector::KnnResult> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|Far(score, key)| crate::vector::KnnResult { key, score })
            .collect()
    }
}

/// Brute-force KNN over only the keys in `candidates`. Stops when
/// `deadline` has passed, with the nearest of the candidates measured so
/// far, and then the second value is true.
pub(crate) fn knn_filtered<'k>(
    live_index: &crate::vector::LiveIndex,
    query: &[f32],
    k: usize,
    metric: &crate::vector::DistanceMetric,
    candidates: impl IntoIterator<Item = &'k str>,
    deadline: Option<std::time::Instant>,
) -> (Vec<crate::vector::KnnResult>, bool) {
    let mut scorer = FilteredKnn::new(live_index, query, k, metric);
    // A distance per candidate: the clock is read every DEADLINE_EVERY.
    let mut seen = 0u32;
    let mut cut = false;
    for key in candidates {
        if crate::commands::search::deadline_due(deadline, &mut seen) {
            cut = true;
            break;
        }
        scorer.offer(key);
    }
    (scorer.nearest(), cut)
}

/// Backfill a subset of vector fields after FT.ALTER added them.
///
/// Different from `backfill_index` (which runs on FT.CREATE and walks every
/// vector field in the schema): this one only walks the newly added fields,
/// since the existing ones already have their data. Sharing the loop body
/// would mean either a flag-driven version of backfill_index or duplicating
/// the keyspace walk — duplicating is clearer at this scale.
fn backfill_new_vector_fields(ctx: &Ctx, index_name: &str, new_field_names: &[String]) {
    use crate::vector::FieldType;

    let db_ref = &ctx.store[*ctx.selected_db];

    // Resolve metadata for each new field once, before the keyspace walk.
    // We hold the index store read lock only for this resolution; the walk
    // below takes shard read locks per shard and only re-enters the index
    // store via maybe_index... actually no, we call hnsw_insert / Flat
    // insert directly, so no further index_store locks needed.
    let store = ctx.index_store.read();
    let def = match store.get(index_name) {
        Some(d) => d,
        None => return, // index dropped between ALTER and backfill — fine
    };

    // Collect (field_name, dim, metric, live_index_arc) tuples.
    // We clone the Arc<LiveIndex variants> so we can drop the index_store
    // read lock before doing the long keyspace walk. Without the clone,
    // any concurrent FT.CREATE / DROPINDEX would block on us.
    struct FieldWork {
        name: String,
        dim: usize,
        data_type: crate::vector::VectorType,
        metric: crate::vector::DistanceMetric,
        live: LiveIndex,
    }
    let mut work: Vec<FieldWork> = Vec::with_capacity(new_field_names.len());
    for name in new_field_names {
        let vf = match def.vector_field_by_name(name) {
            Some(f) => f,
            None => continue,
        };
        let (dim, data_type, metric) = match &vf.field_type {
            FieldType::Vector {
                dim,
                data_type,
                metric,
                ..
            } => (*dim, *data_type, metric.clone()),
            _ => continue,
        };
        let live = match def.indexes.get(name) {
            Some(LiveIndex::Flat(a)) => LiveIndex::Flat(Arc::clone(a)),
            Some(LiveIndex::Hnsw(a)) => LiveIndex::Hnsw(Arc::clone(a)),
            None => continue,
        };
        work.push(FieldWork {
            name: name.clone(),
            dim,
            data_type,
            metric,
            live,
        });
    }
    let prefixes = def.prefixes.clone();
    let (doc_on, doc_schema) = (def.on.clone(), def.fields.clone());
    drop(store);

    if work.is_empty() {
        return;
    }

    // ── Walk every shard, every key, populate matching vector fields ──────────
    for shard in db_ref.iter() {
        #[cfg(target_os = "linux")]
        let map = shard.read();
        #[cfg(not(target_os = "linux"))]
        let map = shard.read();

        for (key, value) in map.iter() {
            let matches =
                prefixes.is_empty() || prefixes.iter().any(|p| key.starts_with(p.as_str()));
            if !matches {
                continue;
            }
            // Build a unified field view regardless of hash variant.
            // HashEx: sweep expired fields first, then clone the survivors.
            // Hash: clone directly. The clone is a shallow Arc clone per value.
            let fields: crate::types::HashFields = match value {
                crate::store::ValueRef::Json(doc)
                    if matches!(doc_on, crate::vector::IndexOn::Json) =>
                {
                    crate::vector::project_json(&doc_schema, &doc.read())
                }
                _ if matches!(doc_on, crate::vector::IndexOn::Json) => continue,
                crate::store::ValueRef::Hash(h) => h.read().clone(),
                crate::store::ValueRef::HashEx(h) => {
                    let mut w = h.write();
                    w.sweep_expired();
                    drop(w);
                    h.read().fields.clone()
                }
                _ => continue,
            };
            let inner = &fields;

            for w in &work {
                let raw_bytes = match inner.get(&w.name) {
                    Some(b) => b,
                    None => continue,
                };
                let Some(vec) = w.data_type.decode(raw_bytes, w.dim) else {
                    continue;
                };

                match &w.live {
                    LiveIndex::Flat(idx) => idx.insert(key.to_string(), vec),
                    LiveIndex::Hnsw(idx) => {
                        crate::vector::hnsw_insert(idx, key.to_string(), vec, &w.metric);
                    }
                }
            }
        }
    }
}

// ── Alias commands ────────────────────────────────────────────────────────────
//
// An alias lets clients refer to an index by another name. It is stored as a
// name that resolves to the index (see `crate::vector::set_alias`), not as a
// copy of it.

/// `FT.ALIASADD` / `FT.ALIASUPDATE`: the alias and the index it should name.
fn alias_args(ctx: &Ctx, command: &str) -> Result<(String, String), Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() != 3 {
        return Err(resp_error(&format!(
            "ERR wrong number of arguments for '{command}' command"
        )));
    }
    let alias =
        std::str::from_utf8(cmd[1]).map_err(|_| resp_error("ERR alias must be valid UTF-8"))?;
    let index = std::str::from_utf8(cmd[2])
        .map_err(|_| resp_error("ERR index name must be valid UTF-8"))?;
    Ok((alias.to_string(), index.to_string()))
}

fn ft_alias_add(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let (alias, index) = match alias_args(ctx, "FT.ALIASADD") {
        Ok(args) => args,
        Err(e) => return Some(e),
    };
    let mut store = ctx.index_store.write();
    if store.contains_key(&alias) {
        return Some(resp_error("ERR Alias already exists"));
    }
    // The target may itself be given by alias.
    let Some(real) = crate::vector::resolve_index_name(&store, &index) else {
        return Some(resp_error("ERR no such index"));
    };
    let def = store[&real].clone();
    crate::vector::set_alias(&mut store, &alias, &def);
    Some(resp_simple("OK"))
}

fn ft_alias_del(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() != 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'FT.ALIASDEL' command",
        ));
    }
    let alias = match std::str::from_utf8(cmd[1]) {
        Ok(s) => s,
        Err(_) => return Some(resp_error("ERR alias must be valid UTF-8")),
    };
    let mut store = ctx.index_store.write();
    // Only an alias: this must not be a way to drop an index by its own name.
    if !store
        .get(alias)
        .is_some_and(|def| crate::vector::is_alias(alias, def))
    {
        return Some(resp_error("ERR Alias does not exist"));
    }
    store.remove(alias);
    Some(resp_simple("OK"))
}

fn ft_alias_update(ctx: &mut Ctx) -> Option<Vec<u8>> {
    // Same as ALIASADD, but an existing alias is moved to the new index.
    let (alias, index) = match alias_args(ctx, "FT.ALIASUPDATE") {
        Ok(args) => args,
        Err(e) => return Some(e),
    };
    let mut store = ctx.index_store.write();
    if store
        .get(&alias)
        .is_some_and(|def| !crate::vector::is_alias(&alias, def))
    {
        // It would replace the index of that name.
        return Some(resp_error("ERR Alias already exists as an index name"));
    }
    let Some(real) = crate::vector::resolve_index_name(&store, &index) else {
        return Some(resp_error("ERR no such index"));
    };
    let def = store[&real].clone();
    crate::vector::set_alias(&mut store, &alias, &def);
    Some(resp_simple("OK"))
}

/// `FT.ALIASLIST index` (Redis 8.10): the aliases of an index — an array, or
/// a set under RESP3.
fn ft_alias_list(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() != 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'FT.ALIASLIST' command",
        ));
    }
    let index = match std::str::from_utf8(cmd[1]) {
        Ok(s) => s,
        Err(_) => return Some(resp_error("ERR index name must be valid UTF-8")),
    };
    let store = ctx.index_store.read();
    // `handle` has already resolved an alias to its index, so anything that
    // is not an index here does not exist.
    if store
        .get(index)
        .is_none_or(|def| crate::vector::is_alias(index, def))
    {
        return Some(resp_error("ERR no such index"));
    }
    let aliases: Vec<Vec<u8>> = crate::vector::index_aliases(&store, index)
        .iter()
        .map(|alias| resp_bulk_str(alias))
        .collect();
    let resp = crate::commands::handler::effective_resp_for_reply(ctx);
    Some(resp_set_for(resp, aliases))
}

// ── Dict commands ─────────────────────────────────────────────────────────────
//
// Redis uses these for custom stopword/synonym dictionaries used in TEXT
// field tokenisation.

fn ft_dictadd(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    // FT.DICTADD dict term [term ...]
    if cmd.len() < 3 {
        return Some(resp_error("ERR syntax: FT.DICTADD dict term [term ...]"));
    }
    let dict_name = match std::str::from_utf8(cmd[1]) {
        Ok(s) => s.to_string(),
        Err(_) => return Some(resp_error("ERR dict name must be valid UTF-8")),
    };

    let terms: Vec<String> = cmd[2..]
        .iter()
        .filter_map(|b| std::str::from_utf8(b).ok())
        .map(|s| s.to_lowercase())
        .collect();

    let mut store = ctx.dict_store.write();
    let dict = store.entry(dict_name).or_default();

    let mut added = 0i64;
    for term in terms {
        if dict.insert(term) {
            added += 1;
        }
    }

    drop(store);

    Some(resp_integer(added))
}

fn ft_dictdel(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return Some(resp_error("ERR syntax: FT.DICTDEL dict term [term ...]"));
    }
    let dict_name = match std::str::from_utf8(cmd[1]) {
        Ok(s) => s,
        Err(_) => return Some(resp_error("ERR dict name must be valid UTF-8")),
    };

    let mut store = ctx.dict_store.write();
    let dict = match store.get_mut(dict_name) {
        Some(d) => d,
        None => return Some(resp_integer(0)), // dict doesn't exist — 0 removed
    };

    let mut removed = 0i64;
    for term_bytes in &cmd[2..] {
        if let Ok(term) = std::str::from_utf8(term_bytes)
            && dict.remove(&term.to_lowercase())
        {
            removed += 1;
        }
    }

    drop(store);

    Some(resp_integer(removed))
}

fn ft_dictdump(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() != 2 {
        return Some(resp_error("ERR syntax: FT.DICTDUMP dict"));
    }
    let dict_name = match std::str::from_utf8(cmd[1]) {
        Ok(s) => s,
        Err(_) => return Some(resp_error("ERR dict name must be valid UTF-8")),
    };

    let store = ctx.dict_store.read();
    let dict = match store.get(dict_name) {
        Some(d) => d,
        None => return Some(resp_array(vec![])),
    };

    // Sort for deterministic output — same reason as FT.TAGVALS
    let mut words: Vec<&str> = dict.iter().map(|s| s.as_str()).collect();
    words.sort_unstable();

    let items: Vec<Vec<u8>> = words.iter().map(|w| resp_bulk_str(w)).collect();
    Some(resp_array(items))
}

// ── FT.DEBUG ──────────────────────────────────────────────────────────────────
//
// FT.DEBUG <subcommand> [args...]
//
// A multiplexer for debug/ops commands that need to inspect or manipulate
// internal index state. Current subcommands:
//
//   FT.DEBUG COMPACT <index>
//     Force-trigger HNSW compaction for the named index. Returns OK if a
//     compaction request was queued, ERR if the index doesn't exist or is
//     not HNSW. Same backend as the auto-threshold; the only difference is
//     that this bypasses the COMPACTION_MIN_TOMBSTONES and threshold checks.
//
// The subcommand pattern (FT.DEBUG <verb> ...) mirrors how Redis's own
// DEBUG command works, and how FT.CONFIG / FT.CURSOR work in this codebase.

fn ft_debug(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return Some(resp_error("ERR syntax: FT.DEBUG <subcommand> [args...]"));
    }
    let sub = std::str::from_utf8(cmd[1])
        .map(|s| s.to_ascii_uppercase())
        .unwrap_or_default();

    match sub.as_str() {
        "COMPACT" => ft_debug_compact(ctx),
        _ => Some(resp_error(&format!(
            "ERR unknown FT.DEBUG subcommand '{sub}'"
        ))),
    }
}

fn ft_debug_compact(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() != 4 {
        return Some(resp_error("ERR syntax: FT.DEBUG COMPACT <index> <field>"));
    }
    let index_name = match std::str::from_utf8(cmd[2]) {
        Ok(s) => s.to_string(),
        Err(_) => return Some(resp_error("ERR index name must be valid UTF-8")),
    };
    let field_name = match std::str::from_utf8(cmd[3]) {
        Ok(s) => s.to_string(),
        Err(_) => return Some(resp_error("ERR field name must be valid UTF-8")),
    };

    {
        let store = ctx.index_store.read();
        let def = match store.get(&index_name) {
            Some(d) => d,
            None => return Some(resp_error("ERR no such index")),
        };
        match def.indexes.get(&field_name) {
            Some(crate::vector::LiveIndex::Hnsw(_)) => {}
            Some(_) => {
                return Some(resp_error(
                    "ERR FT.DEBUG COMPACT only supported on HNSW fields",
                ));
            }
            None => {
                return Some(resp_error("ERR no such vector field on index"));
            }
        }
    }

    let req = crate::vector::CompactionRequest {
        index_name,
        field_name,
    };
    let _ = ctx.compaction_tx.try_send(req);

    Some(resp_simple("OK"))
}

#[cfg(test)]
mod filtered_knn_tests {
    use super::*;

    fn vector(dim: usize, seed: u64) -> Vec<f32> {
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        (0..dim)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                ((state >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
            })
            .collect()
    }

    /// The nearest `k` keys of `keys`, by the plain distance: what the
    /// filtered KNN returned before.
    fn exact(
        data: &[(String, Vec<f32>)],
        keys: &[&str],
        q: &[f32],
        k: usize,
        metric: &crate::vector::DistanceMetric,
    ) -> Vec<String> {
        let mut scored: Vec<(f32, &str)> = data
            .iter()
            .filter(|(key, _)| keys.contains(&key.as_str()))
            .map(|(key, v)| (crate::vector::distance(metric, q, v), key.as_str()))
            .collect();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0));
        scored
            .into_iter()
            .take(k)
            .map(|(_, key)| key.to_string())
            .collect()
    }

    #[test]
    fn the_scorer_keeps_the_exact_nearest_of_the_keys_offered() {
        use crate::vector::{DistanceMetric, HnswIndex, VectorIndex};
        let dim = 37;
        let data: Vec<(String, Vec<f32>)> = (0..600)
            .map(|i| (format!("doc:{i}"), vector(dim, i as u64 + 1)))
            .collect();
        // Every third key, as a filter would leave them, and one with no vector.
        let mut keys: Vec<&str> = data.iter().step_by(3).map(|(k, _)| k.as_str()).collect();
        keys.push("doc:none");
        for metric in [
            DistanceMetric::Cosine,
            DistanceMetric::L2,
            DistanceMetric::InnerProduct,
        ] {
            let flat = Arc::new(VectorIndex::new());
            let graph = Arc::new(HnswIndex::new(16, 64));
            for (key, v) in &data {
                flat.insert(key.clone(), v.clone());
                crate::vector::hnsw_insert(&graph, key.clone(), v.clone(), &metric);
            }
            let q = vector(dim, 9_999);
            for k in [0usize, 1, 10, 500] {
                let want = exact(&data, &keys, &q, k, &metric);
                for live in [
                    LiveIndex::Flat(flat.clone()),
                    LiveIndex::Hnsw(graph.clone()),
                ] {
                    let (got, cut) =
                        knn_filtered(&live, &q, k, &metric, keys.iter().copied(), None);
                    assert!(!cut);
                    let got_keys: Vec<String> = got.iter().map(|r| r.key.clone()).collect();
                    assert_eq!(got_keys.len(), want.len(), "{metric:?} k={k}");
                    // Equal up to the order of near ties.
                    let mut a = got_keys.clone();
                    let mut b = want.clone();
                    a.sort();
                    b.sort();
                    assert_eq!(a, b, "{metric:?} k={k}");
                    assert!(got.windows(2).all(|w| w[0].score <= w[1].score));
                }
            }
        }
    }
}
