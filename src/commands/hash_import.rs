//! `HIMPORT` (Redis 8.10): session-based bulk hash import.
//!
//! A client declares a *fieldset* — a named, ordered list of field names —
//! once, then creates many hashes from it by sending only the values:
//!
//! ```text
//! HIMPORT PREPARE u name email age
//! HIMPORT SET user:1 u alice a@example.com 30
//! HIMPORT SET user:2 u bob   b@example.com 25
//! ```
//!
//! Fieldsets belong to the connection: no other client sees them, and they go
//! when the connection closes or sends `RESET`. That is also why `HIMPORT SET`
//! cannot be propagated as written — a replica, or an AOF replayed later, has
//! no such session — so it is propagated as the `DEL` + `HSET` it amounts to
//! (see [`propagation`]).
//!
//! Redis additionally uses the shared field names as a hint to store such keys
//! in its compact-hash encoding. That is a memory optimisation internal to
//! Redis with no effect on any reply; hashes created here are ordinary hashes.

use crate::commands::ctx::Ctx;
use crate::commands::helpers::{parse_utf8, store_value};
use crate::keyspace_events::{KS_HASH, notify};
use crate::resp::write::*;
use crate::types::KiviDBValue;
use ahash::AHashMap;
use parking_lot::RwLock;
use std::sync::{Arc, LazyLock};

/// Field names, in the order `HIMPORT SET` will supply their values.
type Fieldset = Arc<Vec<String>>;

/// Client id → that connection's fieldsets, by name.
///
/// Process-wide rather than a field on the connection: the handler sees a
/// `Ctx`, not the connection loop's state, and the client id is the one thing
/// both have. Entries are dropped by [`forget_client`].
static SESSIONS: LazyLock<RwLock<AHashMap<u64, ClientFieldsets>>> =
    LazyLock::new(|| RwLock::new(AHashMap::new()));

/// One client's prepared fieldsets, by name.
type ClientFieldsets = AHashMap<Vec<u8>, Fieldset>;

/// Discard every fieldset held by `client_id`. Called when the connection
/// closes and on `RESET`.
pub(crate) fn forget_client(client_id: u64) -> usize {
    // Almost no connection ever uses HIMPORT; do not take the write lock for
    // the ones that did not.
    if !SESSIONS.read().contains_key(&client_id) {
        return 0;
    }
    SESSIONS
        .write()
        .remove(&client_id)
        .map_or(0, |sets| sets.len())
}

fn fieldset(client_id: u64, name: &[u8]) -> Option<Fieldset> {
    SESSIONS.read().get(&client_id)?.get(name).cloned()
}

/// `HIMPORT SET key fieldset value [value ...]` as the commands that have the
/// same effect without a session: `DEL key`, then `HSET key field value ...`.
/// Empty when the fieldset is not (or no longer) defined.
pub(crate) fn propagation(client_id: u64, cmd: &[&[u8]]) -> Vec<Vec<Vec<u8>>> {
    if cmd.len() < 5 || !cmd[1].eq_ignore_ascii_case(b"SET") {
        return Vec::new();
    }
    let Some(fields) = fieldset(client_id, cmd[3]) else {
        return Vec::new();
    };
    let values = &cmd[4..];
    if fields.len() != values.len() {
        return Vec::new();
    }
    let mut hset = Vec::with_capacity(2 + 2 * fields.len());
    hset.push(b"HSET".to_vec());
    hset.push(cmd[2].to_vec());
    for (field, value) in fields.iter().zip(values) {
        hset.push(field.as_bytes().to_vec());
        hset.push(value.to_vec());
    }
    vec![vec![b"DEL".to_vec(), cmd[2].to_vec()], hset]
}

/// Whether this `HIMPORT` invocation writes to the keyspace. Only `SET` does;
/// the rest manage session state and are allowed anywhere a read is.
pub(crate) fn is_write(cmd: &[&[u8]]) -> bool {
    cmd.get(1)
        .is_some_and(|sub| sub.eq_ignore_ascii_case(b"SET"))
}

fn wrong_arity(sub: &str) -> Vec<u8> {
    resp_error(&format!(
        "ERR wrong number of arguments for 'himport|{sub}' command"
    ))
}

pub(super) fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    if name != "HIMPORT" {
        return None;
    }
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'himport' command",
        ));
    }
    let sub = String::from_utf8_lossy(cmd[1]).to_ascii_uppercase();
    Some(match sub.as_str() {
        "PREPARE" => {
            // HIMPORT PREPARE fieldset-name field [field ...]
            if cmd.len() < 4 {
                return Some(wrong_arity("prepare"));
            }
            let mut fields = Vec::with_capacity(cmd.len() - 3);
            let mut seen = ahash::AHashSet::with_capacity(cmd.len() - 3);
            for raw in &cmd[3..] {
                let Some(field) = parse_utf8(raw) else {
                    return Some(resp_error("ERR field names must be valid UTF-8"));
                };
                if !seen.insert(field) {
                    return Some(resp_error(&format!(
                        "ERR duplicate field name '{field}' in fieldset"
                    )));
                }
                fields.push(field.to_string());
            }
            // Preparing an existing name replaces its definition.
            SESSIONS
                .write()
                .entry(ctx.client_id)
                .or_default()
                .insert(cmd[2].to_vec(), Arc::new(fields));
            resp_simple("OK")
        }

        "SET" => {
            // HIMPORT SET key fieldset-name value [value ...]
            if cmd.len() < 5 {
                return Some(wrong_arity("set"));
            }
            let Some(key) = parse_utf8(cmd[2]) else {
                return Some(resp_error("ERR must be valid UTF-8"));
            };
            let Some(fields) = fieldset(ctx.client_id, cmd[3]) else {
                return Some(resp_error(&format!(
                    "ERR no such fieldset '{}'; define it with HIMPORT PREPARE on this connection",
                    String::from_utf8_lossy(cmd[3])
                )));
            };
            let values = &cmd[4..];
            if values.len() != fields.len() {
                return Some(resp_error(&format!(
                    "ERR number of values ({}) does not match the fieldset's field count ({})",
                    values.len(),
                    fields.len()
                )));
            }

            let pairs: Vec<(String, Arc<[u8]>)> = fields
                .iter()
                .zip(values)
                .map(|(field, value)| (field.clone(), Arc::from(*value)))
                .collect();
            let hash: crate::types::HashFields = pairs.iter().cloned().collect();

            // Creates or overwrites: whatever was at `key`, of any type, and
            // its TTL, are replaced.
            let db = *ctx.selected_db;
            let replaced = {
                let shard = crate::store::get_shard(&ctx.store[db], key);
                crate::read_lock!(shard).contains_key(key)
            };
            if replaced {
                crate::vector::maybe_unindex(ctx.index_store, db, key);
            }
            store_value(
                ctx.store,
                db,
                key,
                KiviDBValue::Hash(Arc::new(RwLock::new(hash))),
            );
            crate::vector::maybe_index(
                ctx.index_store,
                db,
                key,
                &pairs,
                crate::vector::Write::Whole,
            );
            notify(ctx.pubsub, db, KS_HASH, "hset", key);
            resp_simple("OK")
        }

        "DISCARD" => {
            // HIMPORT DISCARD fieldset-name
            if cmd.len() != 3 {
                return Some(wrong_arity("discard"));
            }
            let mut sessions = SESSIONS.write();
            let removed = sessions
                .get_mut(&ctx.client_id)
                .is_some_and(|sets| sets.remove(cmd[2]).is_some());
            if sessions
                .get(&ctx.client_id)
                .is_some_and(|sets| sets.is_empty())
            {
                sessions.remove(&ctx.client_id);
            }
            resp_integer(removed as i64)
        }

        "DISCARDALL" => {
            if cmd.len() != 2 {
                return Some(wrong_arity("discardall"));
            }
            resp_integer(forget_client(ctx.client_id) as i64)
        }

        "HELP" => resp_array(
            [
                "HIMPORT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "PREPARE <fieldset-name> <field> [<field> ...]",
                "    Define a fieldset on this connection.",
                "SET <key> <fieldset-name> <value> [<value> ...]",
                "    Create (or overwrite) a hash from a fieldset and its values.",
                "DISCARD <fieldset-name>",
                "    Remove one fieldset from this connection.",
                "DISCARDALL",
                "    Remove every fieldset from this connection.",
                "HELP",
                "    Print this help.",
            ]
            .iter()
            .map(|line| resp_simple(line))
            .collect(),
        ),

        other => resp_error(&format!(
            "ERR unknown subcommand '{other}'. Try HIMPORT HELP."
        )),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prepare(client: u64, name: &str, fields: &[&str]) {
        SESSIONS.write().entry(client).or_default().insert(
            name.as_bytes().to_vec(),
            Arc::new(fields.iter().map(|f| f.to_string()).collect()),
        );
    }

    #[test]
    fn set_propagates_as_del_and_hset() {
        let client = 9_000_001;
        prepare(client, "u", &["name", "age"]);
        let cmd: Vec<&[u8]> = vec![b"HIMPORT", b"SET", b"user:1", b"u", b"alice", b"30"];
        let feeds = propagation(client, &cmd);
        let text: Vec<Vec<String>> = feeds
            .iter()
            .map(|c| {
                c.iter()
                    .map(|a| String::from_utf8_lossy(a).into_owned())
                    .collect()
            })
            .collect();
        assert_eq!(
            text,
            vec![
                vec!["DEL", "user:1"],
                vec!["HSET", "user:1", "name", "alice", "age", "30"],
            ]
        );

        // Another connection has no such fieldset.
        assert!(propagation(client + 1, &cmd).is_empty());
        forget_client(client);
        assert!(propagation(client, &cmd).is_empty());
    }

    #[test]
    fn fieldsets_are_per_connection_and_forgotten_with_it() {
        let (a, b) = (9_000_010, 9_000_011);
        prepare(a, "u", &["x"]);
        prepare(a, "v", &["y"]);
        assert!(fieldset(a, b"u").is_some());
        assert!(fieldset(b, b"u").is_none());
        assert_eq!(forget_client(a), 2);
        assert_eq!(forget_client(a), 0);
        assert!(fieldset(a, b"u").is_none());
    }

    #[test]
    fn only_set_is_a_write() {
        assert!(is_write(&[b"HIMPORT", b"set", b"k", b"u", b"v"]));
        assert!(!is_write(&[b"HIMPORT", b"PREPARE", b"u", b"f"]));
        assert!(!is_write(&[b"HIMPORT", b"DISCARDALL"]));
        assert!(!is_write(&[b"HIMPORT"]));
    }
}
