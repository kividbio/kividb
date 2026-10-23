// `COMMAND DOCS`: summary, since, group, complexity, history, doc_flags and
// arguments, for redis-cli and clients that ask on connect. The command used
// to answer with an empty array so those clients would still start. The
// fields are generated from Redis 8.10.2's command JSON, the same source as
// the key specs, so `since` and `complexity` are Redis's and redis-cli can
// build argument hints. Arity and key positions stay on `COMMAND` /
// `COMMAND INFO`.

use super::command_docs_table::TABLE;
use crate::resp::write::{
    resp_array, resp_bulk_str, resp_integer, resp_map_for, resp_set_for, resp_simple,
};
use ahash::AHashMap;
use std::sync::LazyLock;

pub(super) struct History(pub &'static str, pub &'static str);

pub(super) struct Arg {
    pub name: &'static str,
    pub type_: &'static str,
    pub display_text: Option<&'static str>,
    pub key_spec_index: i64,
    pub token: Option<&'static str>,
    pub summary: Option<&'static str>,
    pub since: Option<&'static str>,
    pub deprecated_since: Option<&'static str>,
    pub flags: &'static [&'static str],
    pub arguments: &'static [Arg],
}

pub(super) struct Docs {
    pub summary: &'static str,
    pub since: &'static str,
    pub group: &'static str,
    pub complexity: &'static str,
    pub doc_flags: &'static [&'static str],
    pub deprecated_since: Option<&'static str>,
    pub replaced_by: Option<&'static str>,
    pub history: &'static [History],
    pub arguments: &'static [Arg],
    pub subcommands: &'static [&'static str],
}

static BY_NAME: LazyLock<AHashMap<&'static str, &'static Docs>> =
    LazyLock::new(|| TABLE.iter().map(|(name, docs)| (*name, docs)).collect());

fn lookup(name: &str) -> Option<&'static Docs> {
    BY_NAME.get(name).copied()
}

pub(crate) fn reply(resp: u8, requested: &[&[u8]], all: &[&str]) -> Vec<u8> {
    let names: Vec<String> = if requested.is_empty() {
        let mut names: Vec<String> = all.iter().map(|n| n.to_ascii_uppercase()).collect();
        let mut extra = Vec::new();
        for name in &names {
            if let Some(docs) = lookup(name) {
                extra.extend(docs.subcommands.iter().map(|s| (*s).to_string()));
            }
        }
        names.extend(extra);
        names
    } else {
        requested
            .iter()
            .filter_map(|raw| served_name(raw, all))
            .collect()
    };
    let mut pairs = Vec::with_capacity(names.len() * 2);
    for name in names {
        let Some(docs) = lookup(&name) else {
            continue;
        };
        pairs.push(resp_bulk_str(&name.to_ascii_lowercase()));
        pairs.push(encode_docs(resp, docs));
    }
    resp_map_for(resp, pairs)
}

fn served_name(raw: &[u8], all: &[&str]) -> Option<String> {
    let name = std::str::from_utf8(raw).ok()?.to_ascii_uppercase();
    if all.iter().any(|n| n.eq_ignore_ascii_case(&name)) {
        return Some(name);
    }
    let container = name.split_once('|')?.0;
    if all.iter().any(|n| n.eq_ignore_ascii_case(container)) && lookup(&name).is_some() {
        Some(name)
    } else {
        None
    }
}

fn encode_docs(resp: u8, docs: &Docs) -> Vec<u8> {
    let mut pairs = Vec::new();
    if !docs.summary.is_empty() {
        pairs.push(resp_bulk_str("summary"));
        pairs.push(resp_bulk_str(docs.summary));
    }
    if !docs.since.is_empty() {
        pairs.push(resp_bulk_str("since"));
        pairs.push(resp_bulk_str(docs.since));
    }
    pairs.push(resp_bulk_str("group"));
    pairs.push(resp_bulk_str(docs.group));
    if !docs.complexity.is_empty() {
        pairs.push(resp_bulk_str("complexity"));
        pairs.push(resp_bulk_str(docs.complexity));
    }
    if !docs.doc_flags.is_empty() {
        pairs.push(resp_bulk_str("doc_flags"));
        pairs.push(flag_set(resp, docs.doc_flags));
    }
    if let Some(since) = docs.deprecated_since {
        pairs.push(resp_bulk_str("deprecated_since"));
        pairs.push(resp_bulk_str(since));
    }
    if let Some(replaced) = docs.replaced_by {
        pairs.push(resp_bulk_str("replaced_by"));
        pairs.push(resp_bulk_str(replaced));
    }
    if !docs.history.is_empty() {
        pairs.push(resp_bulk_str("history"));
        pairs.push(resp_set_for(
            resp,
            docs.history
                .iter()
                .map(|h| resp_array(vec![resp_bulk_str(h.0), resp_bulk_str(h.1)]))
                .collect(),
        ));
    }
    if !docs.arguments.is_empty() {
        pairs.push(resp_bulk_str("arguments"));
        pairs.push(resp_array(
            docs.arguments.iter().map(|a| encode_arg(resp, a)).collect(),
        ));
    }
    if !docs.subcommands.is_empty() {
        let mut nested = Vec::new();
        for full in docs.subcommands {
            if let Some(sub) = lookup(full) {
                nested.push(resp_bulk_str(&full.to_ascii_lowercase()));
                nested.push(encode_docs(resp, sub));
            }
        }
        if !nested.is_empty() {
            pairs.push(resp_bulk_str("subcommands"));
            pairs.push(resp_map_for(resp, nested));
        }
    }
    resp_map_for(resp, pairs)
}

fn encode_arg(resp: u8, arg: &Arg) -> Vec<u8> {
    let mut pairs = vec![
        resp_bulk_str("name"),
        resp_bulk_str(arg.name),
        resp_bulk_str("type"),
        resp_bulk_str(arg.type_),
    ];
    if let Some(display) = arg.display_text {
        pairs.push(resp_bulk_str("display_text"));
        pairs.push(resp_bulk_str(display));
    }
    if arg.key_spec_index != -1 {
        pairs.push(resp_bulk_str("key_spec_index"));
        pairs.push(resp_integer(arg.key_spec_index));
    }
    if let Some(token) = arg.token {
        pairs.push(resp_bulk_str("token"));
        pairs.push(resp_bulk_str(token));
    }
    if let Some(summary) = arg.summary {
        pairs.push(resp_bulk_str("summary"));
        pairs.push(resp_bulk_str(summary));
    }
    if let Some(since) = arg.since {
        pairs.push(resp_bulk_str("since"));
        pairs.push(resp_bulk_str(since));
    }
    if let Some(since) = arg.deprecated_since {
        pairs.push(resp_bulk_str("deprecated_since"));
        pairs.push(resp_bulk_str(since));
    }
    if !arg.flags.is_empty() {
        pairs.push(resp_bulk_str("flags"));
        pairs.push(flag_set(resp, arg.flags));
    }
    if arg.type_ == "oneof" || arg.type_ == "block" {
        pairs.push(resp_bulk_str("arguments"));
        pairs.push(resp_array(
            arg.arguments.iter().map(|a| encode_arg(resp, a)).collect(),
        ));
    }
    resp_map_for(resp, pairs)
}

/// Redis emits doc/arg flags as a set of simple strings (`+optional`).
/// redis-cli reads them as STATUS, not bulk.
fn flag_set(resp: u8, flags: &[&str]) -> Vec<u8> {
    resp_set_for(resp, flags.iter().map(|f| resp_simple(f)).collect())
}

#[cfg(test)]
mod tests {
    use super::{lookup, reply};

    #[test]
    fn get_matches_redis_json() {
        let docs = lookup("GET").expect("GET");
        assert_eq!(docs.summary, "Returns the string value of a key.");
        assert_eq!(docs.group, "string");
        assert_eq!(docs.complexity, "O(1)");
        assert_eq!(docs.since, "1.0.0");
        assert_eq!(docs.arguments.len(), 1);
        assert_eq!(docs.arguments[0].name, "key");
        assert_eq!(docs.arguments[0].type_, "key");
    }

    #[test]
    fn since_is_redis_not_one_oh() {
        assert_eq!(lookup("XADD").unwrap().since, "5.0.0");
        assert_eq!(lookup("GETEX").unwrap().since, "6.2.0");
        assert_eq!(lookup("HEXPIRE").unwrap().since, "7.4.0");
        assert_eq!(lookup("ARSET").unwrap().since, "8.8.0");
        assert_eq!(lookup("DIGEST").unwrap().since, "8.4.0");
    }

    #[test]
    fn complexity_is_o1_for_single_element_writes() {
        for name in ["SADD", "HSET", "LPUSH"] {
            let c = lookup(name).unwrap().complexity;
            assert!(c.starts_with("O(1)"), "{name}: {c}");
        }
    }

    #[test]
    fn module_groups() {
        assert_eq!(lookup("FT.SEARCH").unwrap().group, "search");
        assert_eq!(lookup("JSON.SET").unwrap().group, "json");
        assert_eq!(lookup("TS.ADD").unwrap().group, "timeseries");
        assert_eq!(lookup("VADD").unwrap().group, "vectorset");
    }

    #[test]
    fn unknown_names_are_omitted() {
        let out = reply(2, &[b"nosuch"], &["GET"]);
        assert_eq!(out, crate::resp::write::resp_array(vec![]));
    }

    #[test]
    fn setnx_carries_doc_flags() {
        let docs = lookup("SETNX").unwrap();
        assert_eq!(docs.doc_flags, &["deprecated"]);
        assert_eq!(docs.deprecated_since, Some("2.6.12"));
    }
}
