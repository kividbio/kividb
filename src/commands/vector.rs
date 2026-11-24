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

/// What a command is told of a name it gave that is another database's.
#[derive(Clone, Copy)]
enum Elsewhere {
    /// An index (or an alias of one): `Index not found`.
    Index,
    /// The alias of `FT.ALIASDEL`: `Alias does not exist`.
    Alias,
    /// The alias of `FT.ALIASUPDATE`, which would be moved from another
    /// database's index: `Alias already exists`, as `FT.ALIASADD` says.
    AliasTaken,
}

/// The arguments of `name` that name an index or an alias of one, in the
/// order they are checked: for most, the first; `FT.ALIASADD` and
/// `FT.ALIASUPDATE` name the index second (and `FT.ALIASUPDATE` the alias
/// first), `FT.CURSOR` and `FT.DEBUG COMPACT|POSTINGS|PAGE` after their
/// subcommand.
fn index_name_positions(name: &str, cmd: &[&[u8]]) -> &'static [(usize, Elsewhere)] {
    match name {
        "FT.ALIASUPDATE" => &[(1, Elsewhere::AliasTaken), (2, Elsewhere::Index)],
        "FT.ALIASADD" | "FT.CURSOR" => &[(2, Elsewhere::Index)],
        "FT.ALIASDEL" => &[(1, Elsewhere::Alias)],
        "FT.DEBUG"
            if cmd.get(1).is_some_and(|sub| {
                [&b"COMPACT"[..], b"POSTINGS", b"PAGE"]
                    .iter()
                    .any(|known| sub.eq_ignore_ascii_case(known))
            }) =>
        {
            &[(2, Elsewhere::Index)]
        }
        _ if takes_index_name(name) => &[(1, Elsewhere::Index)],
        _ => &[],
    }
}

/// Whether the index or alias `given` is one of another database than the
/// selected one. An index is its database's: from any other, it is not
/// there (#447). (Redis has indexes in database 0 only: `FT.CREATE` in any
/// other is refused, and every database sees database 0's.)
fn in_another_database(ctx: &Ctx, given: &[u8]) -> bool {
    let Ok(given) = std::str::from_utf8(given) else {
        return false;
    };
    ctx.index_store
        .read()
        .get(given)
        .is_some_and(|def| def.db_index != *ctx.selected_db)
}

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    if !name.starts_with("FT.") {
        return None;
    }
    // An index, or an alias, of another database is not there.
    for &(at, elsewhere) in index_name_positions(name, &ctx.cmd) {
        let Some(given) = ctx.cmd.get(at).copied() else {
            continue;
        };
        if in_another_database(ctx, given) {
            let given = String::from_utf8_lossy(given);
            return Some(resp_error(&match elsewhere {
                Elsewhere::Index => crate::commands::search::no_such_index(&given),
                Elsewhere::Alias => "ERR Alias does not exist".to_string(),
                Elsewhere::AliasTaken => "ERR Alias already exists".to_string(),
            }));
        }
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
    // The selected database's indexes, not the aliases that point at them.
    // Those of every database used to be listed (#447).
    let db = *ctx.selected_db;
    let valkey = crate::commands::search::valkey_shape(ctx);
    let items: Vec<Vec<u8>> = store
        .iter()
        .filter(|(key, def)| !crate::vector::is_alias(key, def) && def.db_index == db)
        .map(|(key, _)| crate::commands::search::reply_name(key, valkey))
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
        Ok(s) => s.to_string(),
        Err(_) => return Some(resp_error("ERR index name must be valid UTF-8")),
    };

    // `DD` deletes the documents too. A server sends what it deleted as
    // DELs, and the drop without `DD` (below), so a `DD` a replica or an AOF
    // replay meets was written by a release before this one, and is run as
    // that release ran it, where that can be known: 1.1 and before kept the
    // documents; a 1.2.0 release candidate deleted them, and had each
    // replica delete what it found. A replica knows which release its
    // master is (`ReplState::master_dd_deletes`). An AOF does not say
    // which release wrote a record, and one file can hold the records of
    // several, so a replay keeps the documents: wrongly kept, a 1.2.0
    // candidate's deleted documents come back and can be deleted again;
    // wrongly deleted, a 1.1 server's documents would be lost.
    let relaxed = std::sync::atomic::Ordering::Relaxed;
    let delete_documents = match &cmd[2..] {
        [] => false,
        [dd] if dd.eq_ignore_ascii_case(b"DD") => {
            !ctx.apply_replication
                || (!crate::vector::DEFER_INDEXING.load(relaxed)
                    && ctx.repl_state.master_dd_deletes.load(relaxed))
        }
        _ => return Some(resp_error("SEARCH_ARG_UNRECOGNIZED Unknown argument")),
    };

    // The index's database, and what its documents are: taken from its
    // definition, which is not held while the keys are looked through.
    let found = ctx
        .index_store
        .read()
        .get(&name)
        .filter(|def| !crate::vector::is_alias(&name, def))
        .map(|def| (def.db_index, def.on.clone(), def.prefixes.clone()));
    let Some((db, on, prefixes)) = found else {
        return Some(resp_error(&crate::commands::search::no_such_index(&name)));
    };
    // The documents, if they go too: found before anything is deleted, one
    // shard at a time.
    let mut documents = Vec::new();
    if delete_documents {
        for shard in ctx.store[db].iter() {
            let map = shard.read();
            for (key, value) in map.iter() {
                let covered =
                    prefixes.is_empty() || prefixes.iter().any(|p| key.starts_with(p.as_bytes()));
                if covered && crate::vector::is_document(&on, value) {
                    documents.push(crate::store::Key::new(key));
                }
            }
        }
    }

    // Replicas and the AOF get it as Redis 8.10 sends it: the documents
    // deleted as DELs, and then the drop, without `DD`. A replica then
    // deletes what its master deleted and nothing else, where it used to
    // run `DD` itself and delete whatever its own copy of the index covered.
    //
    // The documents go a few at a time, each few a whole write of its own:
    // the write gate and the order of their keys are held for them only, so
    // a full resync or a checkpoint can cut between them, and writes to
    // other keys go on, however many documents there are. The index goes
    // last, as in Redis, so the state of this node after each record it
    // sends is the state a replica reaches by applying it, and a `DD` cut
    // short (the server killed part way, or a replica promoted with half
    // of it) leaves the index there to run it again: the documents left
    // are not orphaned. Dropping the index first would spare this node and
    // its replicas taking each document out of it (about two seconds a
    // million documents, measured), but not that.
    //
    // Each few is sent as one DEL of them all, where Redis sends a DEL of
    // each: a record apiece cost more than the deletes themselves (half
    // again as long, with the AOF on, for a million documents). In a
    // cluster, one DEL names the keys of one slot only, as a command from a
    // client would: a slot migration following the stream sends a command
    // on by its first key. So the documents are taken a slot at a time.
    //
    // A replica sends the same to its own AOF, whatever its master sent, so
    // that no `DD` is ever written again.
    let propagate = crate::replication::write_is_observed(ctx);
    let by_slot = propagate && ctx.cluster.enabled;
    let slot_of = |key: &[u8]| {
        if by_slot {
            crate::cluster::key_hash_slot(key)
        } else {
            0
        }
    };
    if by_slot {
        documents.sort_by_cached_key(|key| slot_of(key));
    }
    crate::aof::failpoint("dropindex-collected");
    let nested = crate::write_gate::held_by_this_thread();
    let mut deleted = 0usize;
    let mut del: Vec<Vec<u8>> = Vec::new();
    for chunk in documents.chunks(DROPINDEX_DOCUMENTS_PER_WRITE) {
        let _gate = dropindex_write_gate(ctx, nested);
        // The order of the chunk's keys, whether or not anything reads the
        // write stream: a write to one of them is then wholly before its
        // deletion here, or wholly after (#275).
        let _order = crate::write_order::lock_keys(db, chunk.iter().map(|k| k.as_bytes()));
        let mut slot = chunk.first().map(|key| slot_of(key));
        for key in chunk {
            if by_slot && slot != Some(slot_of(key)) {
                slot = Some(slot_of(key));
                send_dels(ctx, db, &mut del);
            }
            // One that has expired is gone already, as any read finds it.
            if crate::commands::helpers::is_expired(ctx.store, db, key) {
                crate::commands::helpers::delete_key_if_expired(ctx.store, db, key);
                continue;
            }
            // Only if it is still a document of the index (#275): one
            // written meanwhile as another type (a string, a hash under an
            // `ON JSON` index) is no longer one, and run after that write,
            // `DD` would have kept it. (Whether the index covers it goes by
            // its name and the index's prefixes, neither of which changes.)
            if !delete_document(ctx.store, db, key, &on) {
                continue;
            }
            deleted += 1;
            crate::stats::record_rdb_change();
            crate::keyspace_events::notify(
                ctx.pubsub,
                db,
                crate::keyspace_events::KS_GENERIC,
                "del",
                key,
            );
            ctx.watch_registry.notify(db, key);
            crate::commands::handler::invalidate_tracked_key(ctx, key, false);
            if propagate {
                if del.is_empty() {
                    del.push(b"DEL".to_vec());
                }
                del.push(key.as_bytes().to_vec());
            }
        }
        // Sent before the order of these keys is let go of.
        send_dels(ctx, db, &mut del);
    }

    // The index goes, and its aliases with it.
    let _gate = dropindex_write_gate(ctx, nested);
    if !crate::vector::remove_index(&mut ctx.index_store.write(), &name) {
        // Dropped meanwhile by another client, which sent the drop.
        if deleted == 0 {
            return Some(resp_error(&crate::commands::search::no_such_index(&name)));
        }
        return Some(resp_simple("OK"));
    }
    if propagate {
        propagate_in_db(ctx, db, &[b"FT.DROPINDEX".to_vec(), name.into_bytes()]);
    }
    Some(resp_simple("OK"))
}

/// How many documents `FT.DROPINDEX … DD` deletes under one hold of the
/// write gate and of their keys' order.
const DROPINDEX_DOCUMENTS_PER_WRITE: usize = 256;

/// The write gate, for one part of an `FT.DROPINDEX`, which the handler
/// leaves it to take (see `handler::run_command`): stepped out of while a
/// handoff is under way, as the handler does for any other write. Inside a
/// transaction or a script the gate is held already, and this takes nothing.
/// Delete `key` if it holds a document of an index `ON on`, deciding and
/// deleting under its shard's write lock. Whether it did.
fn delete_document(
    store: &crate::store::Store,
    db: usize,
    key: &[u8],
    on: &crate::vector::IndexOn,
) -> bool {
    let shard = crate::store::get_shard(&store[db], key);
    let removal = shard
        .write()
        .remove_if(key, |value| crate::vector::is_document(on, value));
    // Freed with the shard unlocked.
    matches!(removal, crate::store::Removal::Taken(_))
}

fn dropindex_write_gate(ctx: &Ctx, nested: bool) -> crate::write_gate::WriteGuard {
    let mut gate = crate::write_gate::enter();
    while !nested && !ctx.apply_replication && crate::pause::handoff() {
        drop(gate);
        crate::pause::wait_out_handoff();
        gate = crate::write_gate::enter();
    }
    gate
}

/// The DEL `del` holds, if it names a key, to replicas and the AOF; `del`
/// is left empty.
fn send_dels(ctx: &mut Ctx, db: usize, del: &mut Vec<Vec<u8>>) {
    if del.len() > 1 {
        propagate_in_db(ctx, db, del);
    }
    del.clear();
}

/// Hand `parts` to replicas and the AOF as a write made in database `db`,
/// which need not be the one selected: an index's documents are in its own.
fn propagate_in_db(ctx: &mut Ctx, db: usize, parts: &[Vec<u8>]) {
    let selected = *ctx.selected_db;
    *ctx.selected_db = db;
    crate::replication::replicate_one_write(ctx, parts);
    *ctx.selected_db = selected;
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
        None => {
            return Some(resp_error(&crate::commands::search::no_such_index(
                &index_name,
            )));
        }
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
    let added_filter_fields: Vec<crate::vector::SchemaField> = new_fields
        .iter()
        .filter(|f| matches!(f.field_type, FieldType::Tag | FieldType::Numeric))
        .cloned()
        .collect();
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
    let kept_keys = !def.gives_ids();
    def.fields.extend(new_fields);
    // An index of vectors alone that is given its first TEXT, TAG or
    // NUMERIC field counts its documents by their ids from here on: each
    // is given one below, whatever was added and with SKIPINITIALSCAN too.
    // Its keys are let go of, not emptied: a walk that began before this
    // (of an `FT.CREATE` or another `FT.ALTER`) goes on adding to the ones
    // it holds, which go with it, not to ones nothing reads or frees.
    let now_gives_ids = kept_keys && def.gives_ids();
    if now_gives_ids {
        def.docs = Arc::default();
    }
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
            &ctx.store[altered.db_index],
            &altered,
            &newly_added_text_fields,
        );
    }
    if !added_filter_fields.is_empty() || now_gives_ids {
        crate::vector::backfill_postings(
            &ctx.store[altered.db_index],
            &altered,
            &added_filter_fields,
            !skip_initial_scan,
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
    // `VERBATIM`, `LANGUAGE` and `SCORER`, where an option may be.
    let option = |name: &str| crate::commands::aggregate_pipeline::option_at(cmd, 3, name);
    let value_of = |at: usize| cmd.get(at + 1).and_then(|a| std::str::from_utf8(a).ok());
    let mut options = crate::vector::filter::QueryOptions {
        verbatim: option("VERBATIM").is_some(),
        language: None,
        dialect: crate::commands::search_tools::dialect_or_default(
            option("DIALECT")
                .and_then(value_of)
                .and_then(|n| n.parse::<u8>().ok())
                .filter(|n| (1..=4).contains(n))
                .unwrap_or(0),
        ),
    };
    if let Some(at) = option("LANGUAGE") {
        match value_of(at).and_then(crate::vector::stem::Language::parse) {
            Some(language) => options.language = Some(language),
            None => return Some(resp_error("No such language")),
        }
    }
    let scorer = match option("SCORER") {
        Some(at) => match value_of(at).and_then(crate::vector::score::Scorer::parse) {
            Some(scorer) => scorer,
            None => {
                return Some(resp_error(&format!(
                    "No such scorer {}",
                    value_of(at).unwrap_or_default()
                )));
            }
        },
        None => crate::vector::score::Scorer::default_scorer(),
    };
    let (filter, knn) =
        match crate::vector::filter::parse_query_with(&query_str, &params, &def, options) {
            Ok(query) => (query.filter, query.knn),
            Err(e) => return Some(resp_error(&e)),
        };

    // `ADDSCORES`: the relevance of each document, as `@__score`.
    let addscores = cmd.iter().any(|a| a.eq_ignore_ascii_case(b"ADDSCORES"));
    let scoring = crate::vector::score::Scoring::new(&def, &filter, scorer);
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
    // One moment for the whole query to judge field deadlines by.
    let clock = crate::types::QueryClock::new();
    let now = &clock;

    // ── Stage 0: Collect all matching rows ────────────────────────────────────
    //
    // Walk every shard, collect hashes that match our prefixes.
    // Each hash becomes a Row with all its fields as string values.
    let mut rows: Vec<Row> = Vec::new();
    let db_ref = &ctx.store[*ctx.selected_db];
    // In a cluster, only the keys of the slots this node serves: see
    // `cluster::search`. Read before the walk: the documents are looked at
    // with their shard locked, and the slot table is not read then (see
    // `ClusterState`).
    let served = ctx
        .cluster
        .enabled
        .then(|| crate::cluster::search::served_slots(ctx.cluster));
    let not_served = |key: &str| {
        served.as_deref().is_some_and(|s| {
            !crate::cluster::search::in_served(s, &*crate::key_bytes::bytes_of(key))
        })
    };
    let json = matches!(def.on, crate::vector::IndexOn::Json);
    // The rows of hashes the query does not see the KNN field of: one past
    // its deadline, left out of what the first read of the hash shows. The
    // KNN below skips these, as it skipped what a second read of each hash
    // found hidden.
    let mut unseen: ahash::AHashSet<String> = ahash::AHashSet::new();
    // No field has a deadline anywhere: none is past it, as `field_hidden`
    // knew.
    let watching = crate::field_expiry::any_watched();
    // Steps that begin with a GROUPBY that can take a document at a time
    // (`aggregate_pipeline::StreamGroups`): each document goes into its
    // group as it is read, and is never a row. Not with a KNN clause or
    // scores, which are of the rows; not in a cluster, whose masters send
    // their rows to be grouped together; not for JSON, whose rows name
    // fields by their schema's attribute.
    //
    // The NUMERIC fields of the index, by the name a row gives them: their
    // values are numbers in a row (see `aggregate_pipeline::number_value`).
    // A group by one, or a count of its distinct values, is by the number
    // (1.50 is 1.5): the stream is told which fields they are.
    let numeric: ahash::AHashSet<String> = def
        .fields
        .iter()
        .filter(|f| matches!(f.field_type, FieldType::Numeric))
        .map(|f| {
            if json {
                f.attribute().to_string()
            } else {
                f.name.clone()
            }
        })
        .collect();
    let mut stream = if knn.is_none()
        && !addscores
        && !json
        && !crate::cluster::search::is_forwarded(ctx)
        && !crate::cluster::search::coordinates(ctx)
    {
        crate::commands::aggregate_pipeline::StreamGroups::plan(cmd, 3)
            .map(|stream| stream.with_numeric(&numeric))
    } else {
        None
    };
    let mut add_row = |key: &str,
                       inner: &crate::vector::Fields,
                       json_doc: Option<&crate::json_tree::Value>| {
        if let Some(stream) = stream.as_mut() {
            // What the row would hold: each field that is text, and the
            // key as `__key` unless the hash has a field of that name.
            stream.add(|name| {
                match inner
                    .get_bytes(name)
                    .and_then(|v| std::str::from_utf8(v).ok())
                {
                    Some(v) => Some(v),
                    None if name == "__key" => Some(key),
                    None => None,
                }
            });
            return;
        }
        // ── Build row ─────────────────────────────────────────────────
        // Only rows that passed the filter reach here.
        let mut row: Row = ahash::AHashMap::new();

        if addscores {
            let score = crate::commands::search::relevance(&def, key, inner, &scoring, json_doc);
            if score == f64::NEG_INFINITY {
                return;
            }
            row.insert(
                "__score".to_string(),
                crate::commands::aggregate_pipeline::number_value(score),
            );
        }

        // Always include the key itself as a special field.
        // Prefixed with __ so it doesn't collide with user field names.
        row.insert(
            "__key".to_string(),
            crate::commands::aggregate_pipeline::escaped(key).into_owned(),
        );
        if let Some(knn) = &knn
            && watching
            && !json
            && inner.get(knn.field.as_str()).is_none()
        {
            unseen.insert(key.to_string());
        }
        for (field, val) in inner.iter() {
            // Text only: a vector's bytes are not a value a row can show
            // or a pipeline can compute with.
            let Ok(s) = std::str::from_utf8(&val) else {
                continue;
            };
            // A row's names are text (`key_bytes::text_of`).
            let field = crate::key_bytes::text_of(field);
            let field: &str = &field;
            // A field of a JSON document goes by the name the schema
            // gives its path.
            let shown = match def.fields.iter().find(|f| json && f.name == field) {
                Some(f) => f.attribute().to_string(),
                None => field.to_string(),
            };
            // A NUMERIC field's value is a number in the row, and written
            // as Redis writes one.
            if !numeric.is_empty()
                && numeric.contains(&shown)
                && let Ok(x) = s.trim().parse::<f64>()
            {
                row.insert(shown, crate::commands::aggregate_pipeline::number_value(x));
                continue;
            }
            row.insert(
                shown,
                crate::commands::aggregate_pipeline::escaped(s).into_owned(),
            );
        }

        rows.push(row);
    };
    // From postings, if they narrow the filter (see `search::plan`); else
    // every document, the deadline kept between runs of them (`scan_docs`).
    let timed_out_scan = match crate::commands::search::plan(ctx, &def, &filter, None, now) {
        Some(plan) => {
            let mut seen = 0u32;
            let mut cut = false;
            // Each document tried against the filter where the ids are not
            // exactly the matches (any more: see `search::plan_keys`).
            let (keys, exact) = crate::commands::search::plan_keys(&def, &plan);
            for key in keys {
                if crate::commands::search::deadline_due(deadline, &mut seen) {
                    cut = true;
                    break;
                }
                if not_served(&key) {
                    continue;
                }
                crate::commands::search::with_doc(db_ref, &def, &key, now, |inner, json| {
                    if exact || crate::vector::filter::eval(&filter, inner) {
                        add_row(&key, inner, json);
                    }
                });
            }
            cut
        }
        None => crate::commands::search::scan_docs(db_ref, deadline, |key, value| {
            if !def.covers(&crate::key_bytes::bytes_of(key)) {
                return;
            }
            if not_served(key) {
                return;
            }
            let Some(inner) = crate::vector::query_doc_fields(&def.on, &def.fields, value, now)
            else {
                return;
            };
            // The filter on the raw fields, before anything is turned into
            // strings.
            if crate::vector::filter::eval(&filter, &*inner) {
                let json = inner.json();
                add_row(key, &inner, json);
            }
        }),
    };
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
            .filter_map(|row| row.get("__key").map(String::as_str))
            .filter(|key| !unseen.contains(*key));
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
                crate::commands::aggregate_pipeline::number_value(f64::from(distance)),
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
        return Some(crate::cluster::search::encode_rows(
            &rows,
            flag,
            crate::cluster::search::wants_held_numbers(cmd),
        ));
    }
    if crate::cluster::search::coordinates(ctx) {
        let mut owned: Vec<Vec<u8>> = cmd.iter().map(|a| a.to_vec()).collect();
        // Every master reads the query in the dialect this node does.
        if option("DIALECT").is_none() {
            owned.push(b"DIALECT".to_vec());
            owned.push(options.dialect.to_string().into_bytes());
        }
        crate::cluster::search::ask_for_held_numbers(&mut owned);
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
                    .and_then(|d| crate::commands::aggregate_pipeline::as_number(d))
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
    let (rows, cut, groups) = match stream {
        // The GROUPBY ran with the scan: its groups go on to the steps
        // after it, which stop at the deadline as they do after grouped rows.
        // A scan cut short is reported through `timed_out` below.
        Some(stream) => {
            let end = stream.end();
            let grouped = stream.finish();
            let n = grouped.len();
            crate::commands::aggregate_pipeline::run_from(
                grouped,
                cmd,
                end,
                deadline,
                false,
                Some(n),
            )
        }
        None => crate::commands::aggregate_pipeline::run_counted(rows, cmd, 3, deadline),
    };
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
    // After a GROUPBY, the order of a group's columns: the properties it
    // groups by, its reducers' names, then what APPLY made after it.
    let mut group_order: Vec<String> = Vec::new();
    // (the row's field, the name to show it under). `ADDSCORES` shows the
    // score first in every row, whatever else is asked for, as Redis does:
    // it showed only when a `SORTBY` or `LOAD` named it, and a plain
    // `ADDSCORES` gave empty rows (#234).
    let mut shown: Vec<(String, String)> = Vec::new();
    if addscores {
        shown.push(("__score".to_string(), "__score".to_string()));
    }
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
                group_order = crate::commands::aggregate_pipeline::group_columns(cmd, i);
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
                if grouped {
                    group_order.push(label.clone());
                }
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
                // A group is not a document, and has no score.
                if let Some(score) = row.get("__score").filter(|_| addscores && !grouped) {
                    pairs.push(("__score".to_string(), score.clone()));
                }
                let mut fields: Vec<(&String, &String)> =
                    row.iter().filter(|(k, _)| !k.starts_with("__")).collect();
                fields.sort_by_key(|(k, _)| {
                    let at = group_order.iter().position(|c| c == *k);
                    (at.unwrap_or(usize::MAX), (*k).clone())
                });
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
            crate::commands::search::search_shape(ctx),
            &index_name,
            rows_out,
            count,
            max_idle,
            timed_out,
            groups,
        ));
    }
    // A grouped aggregation reports how many groups there were, not how many
    // are on this page; any other reports the page.
    let total = groups.unwrap_or(rows_out.len());
    let (resp, valkey) = crate::commands::search::search_shape(ctx);
    Some(crate::commands::search_tools::rows_reply(
        resp, valkey, total, &rows_out, timed_out,
    ))
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
    let (prefixes, tag_field, doc_on, doc_schema, db) = {
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
            def.db_index,
        )
    };
    let Some((stored_as, separator, case_sensitive)) = tag_field else {
        return Some(resp_error(
            "ERR field is not a TAG field or does not exist in index",
        ));
    };

    // Scan all shards of the index's database and collect distinct tag values.
    let mut distinct: ahash::AHashSet<String> = ahash::AHashSet::new();
    let db_ref = &ctx.store[db];

    for shard in db_ref.iter() {
        let map = shard.read();
        for (key, value) in map.iter() {
            let matches =
                prefixes.is_empty() || prefixes.iter().any(|p| key.starts_with(p.as_bytes()));
            if !matches {
                continue;
            }
            let Some(inner) = crate::vector::doc_fields(&doc_on, &doc_schema, value) else {
                continue;
            };
            let Some(s) = inner
                .get_bytes(&stored_as)
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
        None => {
            return Some(resp_error(&crate::commands::search::no_such_index(
                &index_name,
            )));
        }
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
        None => {
            return Some(resp_error(&crate::commands::search::no_such_index(
                index_name,
            )));
        }
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

    // The index first, as RediSearch looks it up before the rest.
    let Some(def) = ctx.index_store.read().get(&index_name).cloned() else {
        return Some(resp_error(&crate::commands::search::no_such_index(
            &index_name,
        )));
    };

    // The TERMS INCLUDE|EXCLUDE clauses.
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

    // The dictionaries, each of which must exist, the INCLUDE ones first,
    // as RediSearch checks them (before anything else is read). A query word
    // in an EXCLUDE dictionary is spelled right and gets no entry. The words
    // of an INCLUDE dictionary are offered as suggestions too, but a query
    // word that is only in one is still misspelled.
    let mut included: BTreeSet<String> = BTreeSet::new();
    let mut excluded: BTreeSet<String> = BTreeSet::new();
    {
        let dict_store = ctx.dict_store.read();
        for (names, into) in [
            (&include_dicts, &mut included),
            (&exclude_dicts, &mut excluded),
        ] {
            for dict_name in names {
                match dict_store.get(dict_name) {
                    Some(dict) => into.extend(dict.iter().cloned()),
                    None => {
                        return Some(resp_error(&format!("Dict does not exist: {dict_name}")));
                    }
                }
            }
        }
    }

    // The words some document holds, each with how many do, read in one
    // pass and cloned, so that the lock is let go of before the edit
    // distances are measured. A word no document holds any more is neither
    // spelled right nor offered, as in RediSearch once its GC has run.
    let vocab: std::collections::BTreeMap<String, usize> = {
        let text = def.text.read();
        text.live_words()
            .map(|(id, word)| (word.to_string(), text.df(id)))
            .collect()
    };

    // What RediSearch divides a word's document count by: every document of
    // the index (`docs.size - 1`), as `FT.INFO`'s `num_docs` counts them.
    let cluster = ctx.cluster.enabled.then_some(&**ctx.cluster);
    let total_docs = crate::commands::search::count_docs(&def, cluster).max(1) as f64;

    // RediSearch's reply per misspelled word, RESP2:
    //   ["TERM", word, [[score, suggestion], ...]]
    // and RESP3: {"results": {word: [{suggestion: score}, ...], ...}}.
    // `TERM` and `results` are simple strings there, and so here.
    // A suggestion's score is the share of the index's documents that hold
    // it (`spell_check.c`), best first, written as Redis writes a double; a
    // word only in an INCLUDE dictionary scores 0.
    let resp3 = crate::commands::handler::effective_resp_for_reply(ctx) == 3;
    let score = |docs: usize| -> Vec<u8> {
        let digits = crate::commands::helpers::format_score(docs as f64 / total_docs);
        if resp3 {
            format!(",{digits}\r\n").into_bytes()
        } else {
            resp_bulk_str(&digits)
        }
    };
    let mut out: Vec<Vec<u8>> = Vec::new();

    // The query's words as the index has them, so that `world_x` and
    // `users\.u_1` are the words the vocabulary holds.
    for token_clean in crate::vector::tokenize_for_vocab(&query) {
        if token_clean.is_empty()
            || vocab.contains_key(&token_clean)
            || excluded.contains(&token_clean)
        {
            continue; // correctly spelled — skip
        }

        let near = |word: &str| levenshtein(&token_clean, word) <= max_distance;
        let mut candidates: Vec<(usize, &str)> = vocab
            .iter()
            .filter(|(word, _)| near(word))
            .map(|(word, &docs)| (docs, word.as_str()))
            .collect();
        for word in &included {
            if !vocab.contains_key(word) && near(word) {
                candidates.push((0, word.as_str()));
            }
        }
        // Best first; words of one score in order, to be the same each time.
        candidates.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));

        let suggestions: Vec<Vec<u8>> = candidates
            .iter()
            .map(|&(docs, word)| {
                if resp3 {
                    resp_map(vec![resp_bulk_str(word), score(docs)])
                } else {
                    resp_array(vec![score(docs), resp_bulk_str(word)])
                }
            })
            .collect();

        if resp3 {
            out.push(resp_bulk_str(&token_clean));
            out.push(resp_array(suggestions));
        } else {
            out.push(resp_array(vec![
                resp_simple("TERM"),
                resp_bulk_str(&token_clean),
                resp_array(suggestions),
            ]));
        }
    }

    if resp3 {
        return Some(resp_map(vec![resp_simple("results"), resp_map(out)]));
    }
    Some(resp_array(out))
}

// ── Helpers for filtered search ──────────────────────────────────────────────

/// [`nearest_shown`] asks an HNSW search for at most one vector in this
/// many; past that it measures every vector instead. An HNSW search for that
/// many costs about what measuring every vector does.
pub(crate) const SEARCH_SHARE: usize = 16;

/// The nearest `k` an index search finds, less the hits a query leaves out
/// ([`crate::commands::search::hit_left_out`]: a document past its `field`'s
/// deadline, or one deleted or written over whose vector is still in the
/// index), nearest first, and whether `deadline` passed first.
///
/// `search(n)` is the nearest `n`. While hits were left out and the search
/// found all it was asked for, an HNSW search is asked for more, at least
/// twice as many each round, up to one [`SEARCH_SHARE`]th of the index. Then
/// (on a FLAT index, after the first search: each of its searches measures
/// every vector) every vector of `live` is measured, exactly, keeping the
/// nearest twice as many as the search would have been asked for next, and
/// four times as many again each pass, until `k` of them are shown. The
/// search used to be asked for more until it was asked for the whole index
/// (an HNSW search with as many candidates), past the query's `TIMEOUT`.
///
/// Cut short by the deadline, it is the shown ones found so far, nearest
/// first.
#[allow(clippy::too_many_arguments)]
pub(crate) fn nearest_shown(
    db: &crate::store::Db,
    on: &crate::vector::IndexOn,
    field: &str,
    k: usize,
    now: &crate::types::QueryClock,
    deadline: Option<std::time::Instant>,
    live: &LiveIndex,
    query: &[f32],
    metric: &crate::vector::DistanceMetric,
    search: impl FnMut(usize) -> Vec<crate::vector::KnnResult>,
) -> (Vec<crate::vector::KnnResult>, bool) {
    // How many more an index search may be asked for: none on FLAT; on HNSW
    // a share of the index, read only once a search has left hits out.
    let room = || match live {
        LiveIndex::Flat(_) => 0,
        LiveIndex::Hnsw(idx) => idx.live_count.load(std::sync::atomic::Ordering::Relaxed),
    };
    let measure = |n: usize| {
        let mut scorer = FilteredKnn::new(live, query, n, metric);
        let cut = scorer.offer_every(deadline);
        (scorer.nearest(), cut)
    };
    shown_of(k, room, deadline, search, measure, |key| {
        crate::commands::search::hit_left_out(db, on, key, field, now)
    })
}

/// [`nearest_shown`], with the search asked for at most `room()` /
/// [`SEARCH_SHARE`], the exact nearest `n` of every vector, nearest first,
/// as `measure(n)`, and what is hidden as `hidden`. The first search reads
/// no clock (a query with nothing hidden is that search alone); every round
/// and pass after it reads it once before it starts, and a pass every
/// [`DEADLINE_EVERY`] documents it looks at.
///
/// [`DEADLINE_EVERY`]: crate::commands::search::DEADLINE_EVERY
fn shown_of(
    k: usize,
    room: impl Fn() -> usize,
    deadline: Option<std::time::Instant>,
    mut search: impl FnMut(usize) -> Vec<crate::vector::KnnResult>,
    mut measure: impl FnMut(usize) -> (Vec<crate::vector::KnnResult>, bool),
    hidden: impl Fn(&str) -> bool,
) -> (Vec<crate::vector::KnnResult>, bool) {
    use crate::commands::search::{deadline_due, past_deadline};
    let mut want = k;
    loop {
        let mut found = search(want);
        let all = found.len() >= want;
        let before = found.len();
        found.retain(|hit| !hidden(&hit.key));
        let left_out = before - found.len();
        if left_out == 0 || found.len() >= k || !all {
            found.truncate(k);
            return (found, false);
        }
        want = want.saturating_add(left_out.max(want));
        if past_deadline(deadline) {
            return (found, true);
        }
        if want.saturating_mul(SEARCH_SHARE) > room() {
            break;
        }
    }
    // A pass measures again what the last one did: a write between them can
    // change their order, so each key is judged once, by key, not by place.
    let mut judged = ahash::AHashSet::new();
    let (mut shown, mut seen) = (Vec::new(), 0u32);
    let mut keep = want.saturating_mul(2);
    loop {
        let (measured, cut) = measure(keep);
        let had = measured.len();
        for hit in measured {
            if judged.contains(hit.key.as_str()) {
                continue;
            }
            if deadline_due(deadline, &mut seen) {
                shown.sort_by(|a: &crate::vector::KnnResult, b| a.score.total_cmp(&b.score));
                return (shown, true);
            }
            judged.insert(hit.key.clone());
            if !hidden(&hit.key) {
                shown.push(hit);
                if shown.len() >= k {
                    break;
                }
            }
        }
        // Nearest first, though a write may have moved a hit between passes.
        shown.sort_by(|a, b| a.score.total_cmp(&b.score));
        if shown.len() >= k || cut || had < keep {
            shown.truncate(k);
            return (shown, cut);
        }
        keep = keep.saturating_mul(4);
        if past_deadline(deadline) {
            return (shown, true);
        }
    }
}

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
    /// The nearest of a FLAT index, from [`FilteredKnn::offer_every`].
    found: Option<Vec<crate::vector::KnnResult>>,
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
            found: None,
        }
    }

    /// Measure the document at `key`, if the index holds a vector for it.
    #[inline]
    pub(crate) fn offer(&mut self, key: &str) {
        let dim = self.query.vector().len();
        let distance = match self.live {
            LiveIndex::Flat(idx) => {
                match idx.with_vector(key, |v| (v.len() == dim).then(|| self.query.to_raw(v))) {
                    Some(Some(d)) => d,
                    _ => return,
                }
            }
            LiveIndex::Hnsw(idx) => {
                // A vector waiting to be linked is the key's latest.
                if let Some(d) = idx.backlog.with_waiting(key, |w| {
                    (w.vector.len() == dim).then(|| self.query.to_unit(&w.vector))
                }) {
                    if let Some(d) = d {
                        self.best.offer(d, key);
                    }
                    return;
                }
                let Some(id) = idx.key_to_id.read().get(key).copied() else {
                    return;
                };
                let nodes = idx.nodes.read();
                match idx.vector_of(&nodes, id, key) {
                    Some(v) if v.len() == dim => self.query.to_unit(v),
                    _ => return,
                }
            }
        };
        self.best.offer(distance, key);
    }

    /// Measure every vector the index holds, until `deadline` has passed
    /// (the clock read every [`DEADLINE_EVERY`] vectors): then true.
    ///
    /// The index's locks are held for the whole walk, so the caller must not
    /// hold a shard's lock (see [`FilteredKnn`]).
    ///
    /// [`DEADLINE_EVERY`]: crate::commands::search::DEADLINE_EVERY
    pub(crate) fn offer_every(&mut self, deadline: Option<std::time::Instant>) -> bool {
        let dim = self.query.vector().len();
        let mut seen = 0u32;
        match self.live {
            LiveIndex::Flat(idx) => {
                // The blocks in order, the lock let go between runs.
                let (found, cut) = idx.nearest(&self.query, self.best.k, deadline);
                self.found = Some(found);
                return cut;
            }
            LiveIndex::Hnsw(idx) => {
                // `nodes` before `key_to_id`, as the graph search and
                // `hnsw_insert` take them: the other way round, a query and
                // an insert can each hold one and wait for the other.
                let nodes = idx.nodes.read();
                let ids = idx.key_to_id.read();
                for (key, &id) in ids.iter() {
                    if crate::commands::search::deadline_due(deadline, &mut seen) {
                        return true;
                    }
                    if let Some(v) = idx.vector_of(&nodes, id, key)
                        && v.len() == dim
                    {
                        self.best.offer(self.query.to_unit(v), key);
                    }
                }
            }
        }
        false
    }

    /// The nearest, nearest first.
    pub(crate) fn nearest(self) -> Vec<crate::vector::KnnResult> {
        match self.found {
            Some(found) => found,
            None => self.best.into_sorted(),
        }
    }
}

/// The `k` smallest distances offered, with their keys: a max-heap whose
/// top is the one to beat.
pub(crate) struct Best {
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
    pub(crate) fn new(k: usize) -> Self {
        Self {
            k,
            heap: std::collections::BinaryHeap::with_capacity(k.min(4096) + 1),
        }
    }

    #[inline]
    pub(crate) fn offer(&mut self, distance: f32, key: &str) {
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

    pub(crate) fn into_sorted(self) -> Vec<crate::vector::KnnResult> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|Far(score, key)| crate::vector::KnnResult { key, score })
            .collect()
    }
}

// ── Filtered KNN from postings ───────────────────────────────────────────────
//
// When postings name exactly the documents a filter matches (see
// `search::plan`), the nearest of them are found one of two ways:
//
// * few documents: each is measured (`knn_measured`) — exact, and as cheap
//   as it gets for a few thousand vectors;
// * many: the graph is searched, keeping only the documents the filter
//   matches (`vector::knn_hnsw_admitting`). The search walks every node, so
//   it costs about what an unfiltered one does while the filter lets most
//   through; as it lets fewer through, the walk gets longer. It is given as
//   many distances as measuring every document would cost, and falls back
//   to that when they run out.
//
// `HYBRID_POLICY ADHOC_BF` always measures; `BATCHES` always searches the
// graph, however long the walk. A FLAT index is always measured.
//
// Each way keeps the query's deadline: cut short, it answers with the
// nearest it found so far and says it was cut, and the command then applies
// `ON_TIMEOUT` as it does after a scan.

/// The most documents a filtered KNN measures one by one rather than search
/// the graph for, by default. Measuring costs a distance per document; a
/// search that keeps a fraction `s` of the nodes it meets walks some `ef / s`
/// of them, and more for the graph's detours. The two cost the same at
/// `n = sqrt(c * ef * N)`; `c` is measured. On dbpedia-openai-100K-1536
/// (an Azure D16s_v5, AVX-512) the two cost the same at about 10,000
/// documents for ef 64, 13,500 for 128 and 16,000 for 256: `c` 16, 14 and
/// 10. Measuring a document of 1536 dimensions costs some 0.4 µs there; the
/// graph search kept the recall of an unfiltered one from 10% of documents
/// up.
pub(crate) fn measure_up_to(ef: usize, graph_size: usize) -> usize {
    const C: f64 = 12.0;
    let crossing = (C * ef as f64 * graph_size as f64).sqrt() as usize;
    crossing.max(2000)
}

/// The nearest `knn.k` of the documents `filter` matches, from postings, or
/// `None` if they do not name exactly those documents and the caller is to
/// find them by trying the filter. The second value is whether the deadline
/// cut the search short.
#[allow(clippy::too_many_arguments)]
pub(crate) fn knn_planned(
    ctx: &Ctx,
    def: &crate::vector::IndexDef,
    filter: &crate::vector::filter::Filter,
    only: Option<&ahash::AHashSet<String>>,
    knn: &crate::vector::filter::Knn,
    field: &crate::vector::SchemaField,
    deadline: Option<std::time::Instant>,
    now: &crate::types::QueryClock,
) -> Option<(Vec<crate::vector::KnnResult>, bool)> {
    let FieldType::Vector { metric, algo, .. } = &field.field_type else {
        return None;
    };
    let live = def.indexes.get(&knn.field)?;
    let mut plan = crate::commands::search::plan(ctx, def, filter, only, now)?;
    if !plan.exact {
        return None;
    }
    let ef = knn
        .ef_runtime
        .unwrap_or(match algo {
            VectorAlgo::Hnsw { ef_runtime, .. } => *ef_runtime,
            VectorAlgo::Flat => knn.k * 2,
        })
        .max(knn.k)
        .max(50);
    let db = &ctx.store[def.db_index];
    // A document overwritten by a value of another type stays in the
    // postings until something deletes it; one that comes up among the
    // nearest is taken out, and the search run again without it.
    for _ in 0..4 {
        let (nearest, cut) = knn_among(def, &plan.ids, knn, live, metric, ef, deadline);
        let gone: Vec<&str> = nearest
            .iter()
            .map(|r| r.key.as_str())
            .filter(|key| {
                let raw = crate::key_bytes::bytes_of(key);
                !crate::store::get_shard(db, &*raw)
                    .read()
                    .get(&*raw)
                    .is_some_and(|v| crate::vector::is_document(&def.on, v))
            })
            .collect();
        // A hit whose vector the query is not to see is left out of this
        // query's plan, not out of the index.
        let hidden: Vec<String> = nearest
            .iter()
            .filter(|r| crate::commands::search::field_hidden(db, &r.key, &knn.field, now))
            .map(|r| r.key.clone())
            .collect();
        // Ids given back while the query ran may have been given out again,
        // to documents the filter was not worked out for: then each hit is
        // tried against the filter on its own document, and one that does
        // not match is left out of the plan. Otherwise this is one compare.
        let others: Vec<String> = if def.postings.peek().given_back() == plan.given_back {
            Vec::new()
        } else {
            nearest
                .iter()
                .filter(|r| {
                    only.is_some_and(|only| !only.contains(&r.key))
                        || crate::commands::search::with_doc(db, def, &r.key, now, |fields, _| {
                            crate::vector::filter::eval(filter, fields)
                        }) == Some(false)
                })
                .map(|r| r.key.clone())
                .collect()
        };
        if (gone.is_empty() && hidden.is_empty() && others.is_empty()) || cut {
            let mut nearest = nearest;
            nearest.retain(|r| !hidden.contains(&r.key) && !others.contains(&r.key));
            return Some((nearest, cut));
        }
        for key in hidden.iter().chain(&others) {
            if let Some(id) = def.postings.peek().id(key) {
                plan.ids.unset(id);
            }
        }
        for key in gone {
            if let Some(id) = def.postings.peek().id(key) {
                plan.ids.unset(id);
            }
            crate::vector::maybe_unindex(
                ctx.index_store,
                def.db_index,
                &*crate::key_bytes::bytes_of(key),
            );
        }
    }
    None
}

/// The nearest `knn.k` of the documents `ids`, by the policy above.
fn knn_among(
    def: &crate::vector::IndexDef,
    ids: &crate::vector::postings::Bits,
    knn: &crate::vector::filter::Knn,
    live: &LiveIndex,
    metric: &crate::vector::DistanceMetric,
    ef: usize,
    deadline: Option<std::time::Instant>,
) -> (Vec<crate::vector::KnnResult>, bool) {
    use crate::vector::filter::HybridPolicy;
    use std::sync::atomic::Ordering::Relaxed;
    let n = ids.count();
    if let LiveIndex::Hnsw(graph) = live {
        let size = graph.live_count.load(Relaxed);
        let search = match knn.policy {
            Some(HybridPolicy::AdhocBf) => false,
            Some(HybridPolicy::Batches) => size >= 500,
            None => size >= 500 && n > measure_up_to(ef, size),
        };
        if search {
            let budget = if knn.policy == Some(HybridPolicy::Batches) {
                usize::MAX
            } else {
                n
            };
            let prepared = crate::vector::PreparedQuery::new(metric, &knn.vector);
            let mut admit = |node: &crate::vector::HnswNode| {
                let mut doc = node.doc.load(Relaxed);
                if doc == crate::vector::postings::NO_DOC {
                    // Not known yet (a compaction, a snapshot, or a write
                    // that was queued made the node): looked up once, and
                    // kept. A document written before the query began has
                    // its id: the query's plan applied the queue.
                    match def.postings.peek().id(&node.key) {
                        Some(id) => {
                            node.doc.store(id, Relaxed);
                            doc = id;
                        }
                        None => return false,
                    }
                }
                ids.get(doc)
            };
            if let Some(found) = crate::vector::knn_hnsw_admitting(
                graph,
                prepared.vector(),
                knn.k,
                ef,
                metric,
                &mut admit,
                budget,
                deadline,
            ) {
                return found;
            }
        }
    }
    knn_measured(def, ids, knn, live, metric, deadline)
}

/// The nearest `knn.k` of the documents `ids`, each measured.
pub(crate) fn knn_measured(
    def: &crate::vector::IndexDef,
    ids: &crate::vector::postings::Bits,
    knn: &crate::vector::filter::Knn,
    live: &LiveIndex,
    metric: &crate::vector::DistanceMetric,
    deadline: Option<std::time::Instant>,
) -> (Vec<crate::vector::KnnResult>, bool) {
    let query = crate::vector::PreparedQuery::new(metric, &knn.vector);
    let graph = match live {
        // From each id to its slot, in the order of the ids, as the keys
        // were before: see `vector::flat`.
        LiveIndex::Flat(idx) => {
            return idx.nearest_of_docs(&query, knn.k, ids, &def.postings, deadline);
        }
        LiveIndex::Hnsw(graph) => graph,
    };
    let keys = crate::commands::search::keys_of(def, ids);
    let dim = knn.vector.len();
    let mut best = Best::new(knn.k);
    // The index's locks are let go between runs, for writers.
    const RUN: usize = 256;
    let mut cut = false;
    for (i, run) in keys.chunks(RUN).enumerate() {
        if i > 0 && i % 4 == 0 && crate::commands::search::past_deadline(deadline) {
            cut = true;
            break;
        }
        // A vector waiting to be linked is the key's latest: it is measured
        // where it waits, and the graph not asked.
        let mut run: Vec<&Arc<str>> = run.iter().collect();
        if !graph.backlog.is_empty() {
            let waiting = graph.backlog.read();
            run.retain(|key| match waiting.get(&***key) {
                Some(w) => {
                    if w.vector.len() == dim {
                        best.offer(query.to_unit(&w.vector), key);
                    }
                    false
                }
                None => true,
            });
        }
        // The two locks one after the other, never together: an insert
        // takes the ids' for writing while it holds the nodes'.
        let found: Vec<(usize, &Arc<str>)> = {
            let key_to_id = graph.key_to_id.read();
            run.into_iter()
                .filter_map(|key| key_to_id.get(&**key).map(|&id| (id, key)))
                .collect()
        };
        let nodes = graph.nodes.read();
        for (id, key) in found {
            if let Some(v) = graph.vector_of(&nodes, id, key)
                && v.len() == dim
            {
                best.offer(query.to_unit(v), key);
            }
        }
    }
    (best.into_sorted(), cut)
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
    // The definition, to walk its documents by, with the index store let go
    // of: a clone shares the live indexes.
    let def = def.clone();
    drop(store);

    if work.is_empty() {
        return;
    }

    // ── Walk the index's documents, populate the new vector fields ────────────
    //
    // A key at a time, under its write order, as it is now: see
    // `vector::walk_documents` (#275). The walk used to index the fields as
    // it found them, and a write to the key meanwhile, which indexed what it
    // wrote, could be overwritten in the index by the walk's older copy.
    //
    // An index that gives its documents no ids counts each one the walk
    // meets: made with `SKIPINITIALSCAN`, it has not met them before, and
    // a search of the new field finds them from here on.
    let keeps_keys = !def.gives_ids();
    crate::vector::walk_documents(&ctx.store[def.db_index], &def, |key, inner| {
        if keeps_keys {
            def.docs.add(key);
        }
        // The id a FLAT index records for the document's slot.
        let mut doc = None;
        for w in &work {
            let raw_bytes = match inner.get_bytes(&w.name) {
                Some(b) => b,
                None => continue,
            };
            let Some(vec) = w.data_type.decode(raw_bytes, w.dim) else {
                continue;
            };

            match &w.live {
                LiveIndex::Flat(idx) => {
                    let doc = *doc.get_or_insert_with(|| {
                        if keeps_keys {
                            crate::vector::postings::NO_DOC
                        } else {
                            def.postings
                                .peek()
                                .id(key)
                                .unwrap_or(crate::vector::postings::NO_DOC)
                        }
                    });
                    idx.insert_doc(key, &vec, doc);
                }
                LiveIndex::Hnsw(idx) => {
                    crate::vector::hnsw_insert(idx, key.to_string(), vec, &w.metric);
                }
            }
        }
    });
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
        return Some(resp_error(&crate::commands::search::no_such_index(&index)));
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
        return Some(resp_error(&crate::commands::search::no_such_index(&index)));
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
        return Some(resp_error(&crate::commands::search::no_such_index(index)));
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
        "POSTINGS" => ft_debug_postings(ctx),
        "PAGE" => ft_debug_page(ctx),
        // The subcommands, by name, as Redis's `_FT.DEBUG HELP` lists its.
        "HELP" if cmd.len() == 2 => Some(resp_array(
            ["COMPACT", "POSTINGS", "PAGE", "HELP"]
                .iter()
                .map(|name| resp_bulk_str(name))
                .collect(),
        )),
        _ => Some(resp_error(&format!(
            "ERR unknown FT.DEBUG subcommand '{sub}'"
        ))),
    }
}

/// `FT.DEBUG POSTINGS index ON|OFF`: whether queries answer from the TAG
/// and NUMERIC postings or try every document, for comparing the two.
fn ft_debug_postings(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    let on = match cmd.get(3) {
        Some(w) if cmd.len() == 4 && w.eq_ignore_ascii_case(b"ON") => true,
        Some(w) if cmd.len() == 4 && w.eq_ignore_ascii_case(b"OFF") => false,
        _ => return Some(resp_error("ERR syntax: FT.DEBUG POSTINGS <index> ON|OFF")),
    };
    let name = String::from_utf8_lossy(cmd[2]).into_owned();
    let Some(def) = ctx.index_store.read().get(&name).cloned() else {
        return Some(resp_error(&crate::commands::search::no_such_index(&name)));
    };
    def.postings.write().unused = !on;
    Some(resp_simple("OK"))
}

/// `FT.DEBUG PAGE index ON|OFF`: whether a query whose page the postings
/// know reads only the documents of that page (`search::page_from_postings`)
/// or every document it matches, for comparing the two.
fn ft_debug_page(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    let on = match cmd.get(3) {
        Some(w) if cmd.len() == 4 && w.eq_ignore_ascii_case(b"ON") => true,
        Some(w) if cmd.len() == 4 && w.eq_ignore_ascii_case(b"OFF") => false,
        _ => return Some(resp_error("ERR syntax: FT.DEBUG PAGE <index> ON|OFF")),
    };
    let name = String::from_utf8_lossy(cmd[2]).into_owned();
    let Some(def) = ctx.index_store.read().get(&name).cloned() else {
        return Some(resp_error(&crate::commands::search::no_such_index(&name)));
    };
    def.postings.write().read_all = !on;
    Some(resp_simple("OK"))
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
            None => {
                return Some(resp_error(&crate::commands::search::no_such_index(
                    &index_name,
                )));
            }
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

    /// Every vector of the index measured: the exact order of them all,
    /// from a FLAT index and an HNSW graph; past the deadline, it says so.
    #[test]
    fn every_vector_measured_is_the_exact_order_of_the_index() {
        use crate::vector::{DistanceMetric, HnswIndex, VectorIndex};
        let dim = 37;
        let data: Vec<(String, Vec<f32>)> = (0..3000)
            .map(|i| (format!("doc:{i}"), vector(dim, i as u64 + 1)))
            .collect();
        let all: Vec<&str> = data.iter().map(|(k, _)| k.as_str()).collect();
        for metric in [DistanceMetric::Cosine, DistanceMetric::L2] {
            let flat = Arc::new(VectorIndex::new());
            let graph = Arc::new(HnswIndex::new(16, 64));
            for (key, v) in &data {
                flat.insert(key.clone(), v.clone());
                crate::vector::hnsw_insert(&graph, key.clone(), v.clone(), &metric);
            }
            let q = vector(dim, 9_999);
            let want = exact(&data, &all, &q, all.len(), &metric);
            for live in [
                LiveIndex::Flat(flat.clone()),
                LiveIndex::Hnsw(graph.clone()),
            ] {
                let mut scorer = FilteredKnn::new(&live, &q, usize::MAX, &metric);
                assert!(!scorer.offer_every(None));
                let got = scorer.nearest();
                assert_eq!(got.len(), want.len(), "{metric:?}");
                assert!(got.windows(2).all(|w| w[0].score <= w[1].score));
                let got_keys: Vec<&str> = got.iter().map(|r| r.key.as_str()).collect();
                assert_eq!(
                    got_keys[..10],
                    want[..10].iter().map(String::as_str).collect::<Vec<_>>()[..],
                    "{metric:?}"
                );

                let past = Some(std::time::Instant::now() - std::time::Duration::from_secs(1));
                let mut scorer = FilteredKnn::new(&live, &q, usize::MAX, &metric);
                assert!(scorer.offer_every(past));
                assert!(scorer.nearest().len() < all.len());
            }
        }
    }
}

#[cfg(test)]
mod nearest_shown_tests {
    use super::*;
    use crate::vector::KnnResult;
    use std::time::{Duration, Instant};

    /// `n` hits `d:0` .. at distance 0, 1, ...: the nearest `want` of them.
    fn index(n: usize) -> impl Fn(usize) -> Vec<KnnResult> {
        move |want| {
            (0..want.min(n))
                .map(|i| KnnResult {
                    key: format!("d:{i}"),
                    score: i as f32,
                })
                .collect()
        }
    }

    fn key_number(key: &str) -> usize {
        key[2..].parse().unwrap()
    }

    fn keys(found: &[KnnResult]) -> Vec<usize> {
        found.iter().map(|hit| key_number(&hit.key)).collect()
    }

    /// `shown_of` over `index(n)` with `room` (n: HNSW, 0: FLAT), each pass
    /// taking `pass` -> (what it returned, the sizes the search was asked
    /// for, the passes made).
    fn run(
        n: usize,
        room: usize,
        k: usize,
        deadline: Option<Instant>,
        pass: Duration,
        hidden: impl Fn(&str) -> bool,
    ) -> ((Vec<usize>, bool), Vec<usize>, usize) {
        let search = index(n);
        let mut asked = Vec::new();
        let mut passes = 0;
        let (found, cut) = shown_of(
            k,
            || room,
            deadline,
            |want| {
                asked.push(want);
                search(want)
            },
            |keep| {
                passes += 1;
                std::thread::sleep(pass);
                (search(keep.min(n)), false)
            },
            hidden,
        );
        ((keys(&found), cut), asked, passes)
    }

    const NONE: Duration = Duration::ZERO;

    /// Nothing hidden: one search, of `k`, as before, on either index.
    #[test]
    fn nothing_hidden_is_one_search() {
        for room in [1000, 0] {
            let (found, asked, passes) = run(1000, room, 10, None, NONE, |_| false);
            assert_eq!(found, ((0..10).collect(), false));
            assert_eq!(asked, [10]);
            assert_eq!(passes, 0);
        }
    }

    /// A few of the nearest hidden on HNSW: more asked for, and the nearest
    /// `k` shown, nearest first.
    #[test]
    fn a_few_hidden_are_made_up_for() {
        let (found, asked, passes) = run(1000, 1000, 10, None, NONE, |key| {
            key_number(key).is_multiple_of(3)
        });
        let want: Vec<usize> = (0..100usize)
            .filter(|i| !i.is_multiple_of(3))
            .take(10)
            .collect();
        assert_eq!(found, (want, false));
        assert_eq!(asked, [10, 20]);
        assert_eq!(passes, 0);
    }

    /// On FLAT every search measures every vector: after the first, the
    /// measured pass, not more searches.
    #[test]
    fn flat_goes_to_the_pass_after_one_search() {
        let hidden = |key: &str| key_number(key).is_multiple_of(3);
        let (found, asked, passes) = run(1000, 0, 10, None, NONE, hidden);
        let want: Vec<usize> = (0..100usize)
            .filter(|i| !i.is_multiple_of(3))
            .take(10)
            .collect();
        assert_eq!(found, (want, false));
        assert_eq!(asked, [10]);
        assert_eq!(passes, 1);
    }

    /// Most of the nearest hidden on HNSW: searches, each at least twice the
    /// last, up to a SEARCH_SHAREth of the index, then the measured passes,
    /// and the nearest `k` shown, nearest first.
    #[test]
    fn mostly_hidden_measures_every_vector_past_a_share_of_the_index() {
        let n = 1_000_000;
        let (found, asked, passes) = run(n, n, 10, None, NONE, |key| key_number(key) < 100_000);
        assert_eq!(found, ((100_000..100_010).collect(), false));
        assert!(asked.windows(2).all(|w| w[1] >= 2 * w[0]), "{asked:?}");
        assert!(asked.iter().all(|&a| a * SEARCH_SHARE <= n), "{asked:?}");
        assert!(passes > 0);
    }

    /// The index holds fewer than `k` shown: what it has.
    #[test]
    fn an_index_with_fewer_than_k_shown_ends() {
        let (found, _, _) = run(5, 5, 10, None, NONE, |key| key_number(key) < 2);
        assert_eq!(found, (vec![2, 3, 4], false));
        for room in [1000, 0] {
            let (found, _, passes) = run(1000, room, 10, None, NONE, |_| true);
            assert_eq!(found, (vec![], false));
            assert!(passes > 0);
        }
    }

    /// Past the deadline: no round or pass after the first search, and what
    /// was found is only the shown ones, nearest first.
    #[test]
    fn the_deadline_stops_every_round_after_the_first() {
        let past = Some(Instant::now() - Duration::from_secs(1));
        let hidden = |key: &str| key_number(key) < 15 || key_number(key).is_multiple_of(2);
        for room in [1_000_000, 0] {
            let (found, asked, passes) = run(1_000_000, room, 10, past, NONE, hidden);
            assert_eq!(found, (vec![], true));
            assert_eq!(asked, [10]);
            assert_eq!(passes, 0);
        }
        let (found, asked, _) = run(1_000_000, 1_000_000, 10, past, NONE, |key| {
            key_number(key) < 5
        });
        assert_eq!(found, ((5..10).collect(), true));
        assert_eq!(asked, [10]);
    }

    /// The deadline passing while a pass looks at what it measured: the
    /// shown ones before it, nearest first.
    #[test]
    fn the_deadline_stops_a_pass() {
        // 600 hidden of the nearest 1,200 (every other one): one search, then
        // a pass keeping 2,400, which outlasts the deadline.
        let deadline = Some(Instant::now() + Duration::from_millis(50));
        let hidden = |key: &str| key_number(key).is_multiple_of(2) || key_number(key) < 2000;
        let (found, asked, passes) = run(
            1_000_000,
            0,
            600,
            deadline,
            Duration::from_millis(100),
            hidden,
        );
        let (shown, cut) = found;
        assert!(cut);
        assert_eq!(asked, [600]);
        assert_eq!(passes, 1);
        assert!(shown.len() < 600, "{}", shown.len());
        assert!(shown.iter().all(|&i| !hidden(&format!("d:{i}"))));
        assert!(shown.windows(2).all(|w| w[0] < w[1]));
    }

    /// A pass whose order differs from the last one's (a write between
    /// them) shows each key once.
    #[test]
    fn a_key_is_shown_once_across_passes() {
        let mut pass = 0;
        let (found, _) = shown_of(
            4,
            || 0,
            None,
            |want| index(100)(want),
            |keep| {
                pass += 1;
                let mut hits = index(100)(keep);
                if pass == 2 {
                    hits.swap(7, 20);
                }
                (hits, false)
            },
            |key| key_number(key) != 7 && key_number(key) < 90,
        );
        assert_eq!(keys(&found), [7, 90, 91, 92]);
    }
}
