use crate::commands::ctx::Ctx;
use crate::commands::helpers::*;
use crate::commands::json_path::{self, Location, Query, Step};
use crate::resp::write::*;
use crate::store;
use crate::store::Key;
use crate::store::ValueRef;
use crate::types::KiviDBValue;
use crate::{read_lock, write_lock};
use parking_lot::RwLock;
use serde_json::{Number, Value};
use std::sync::Arc;

// ── Paths and replies ────────────────────────────────────────────────────────
//
// Every command takes its path through `json_path::Query`. Two things about a
// path decide the shape of the reply:
//
//   * A JSONPath (`$…`) can match several values, and the command answers for
//     all of them: an array with one entry per match, in document order, with
//     nil where a match is not of the type the command works on.
//   * A legacy path (`.a.b`, `a[0]`) answers for the first match alone, as a
//     plain value, and a match of the wrong type is an error.
//
// Commands that change the document work from the *locations* of the matches.
// They are visited last to first, so that removing or replacing one cannot
// move another that is still to be visited.

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const NOT_A_SELECTION: &str = "ERR the path must select values, not compute one";

fn arg<'a>(cmd: &[&'a [u8]], index: usize) -> Option<&'a str> {
    cmd.get(index).and_then(|bytes| parse_utf8(bytes))
}

fn parse_path(path: &str) -> Result<Query, Vec<u8>> {
    Query::parse(path).map_err(|e| resp_error(&format!("ERR {e}")))
}

fn parse_json(text: Option<&str>) -> Result<Value, Vec<u8>> {
    text.and_then(|text| serde_json::from_str(text).ok())
        .ok_or_else(|| resp_error("ERR invalid JSON value"))
}

fn json_text(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.is_f64() => "number",
        Value::Number(_) => "integer",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Run `f` on the document at `key`. A key that does not exist is `missing`.
fn read_doc(ctx: &Ctx, key: &str, missing: Vec<u8>, f: impl FnOnce(&Value) -> Vec<u8>) -> Vec<u8> {
    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, key);
    let map = read_lock!(shard);
    match map.get(key) {
        Some(ValueRef::Json(tree)) => f(&tree.read()),
        Some(_) => resp_error(WRONGTYPE),
        None => missing,
    }
}

/// [`read_doc`] for a command that changes the document.
fn write_doc(
    ctx: &Ctx,
    key: &str,
    missing: Vec<u8>,
    f: impl FnOnce(&mut Value) -> Vec<u8>,
) -> Vec<u8> {
    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, key);
    let map = read_lock!(shard);
    match map.get(key) {
        Some(ValueRef::Json(tree)) => f(&mut tree.write()),
        Some(_) => resp_error(WRONGTYPE),
        None => missing,
    }
}

/// The reply of a command that reads: `f` for every value the path matches,
/// or for the first if the path is a legacy one. `f` is `None` for a value of
/// the wrong type.
fn each_value(
    resp: u8,
    query: &Query,
    root: &Value,
    f: impl Fn(&Value) -> Option<Vec<u8>>,
) -> Vec<u8> {
    let values = query.values(root);
    if query.is_legacy() {
        values
            .first()
            .and_then(|value| f(value))
            .unwrap_or_else(resp_null_bulk)
    } else {
        resp_array(
            values
                .iter()
                .map(|value| f(value).unwrap_or_else(|| resp_null_for(resp)))
                .collect(),
        )
    }
}

/// The reply of a command that changes values in place: `f` for every node
/// the path matches, or for the first if the path is a legacy one. `f` fails
/// with the error a legacy path reports for a node of the wrong type; for a
/// JSONPath that node's entry is nil.
fn each_node(
    resp: u8,
    query: &Query,
    root: &mut Value,
    mut f: impl FnMut(&mut Value) -> Result<Vec<u8>, &'static str>,
) -> Vec<u8> {
    if !query.selects() {
        return resp_error(NOT_A_SELECTION);
    }
    let locations = query.locations(root);
    if query.is_legacy() {
        return match locations
            .first()
            .and_then(|location| json_path::get_mut(root, location))
        {
            Some(node) => f(node).unwrap_or_else(resp_error),
            None => resp_null_bulk(),
        };
    }
    let mut replies: Vec<Vec<u8>> = locations
        .iter()
        .rev()
        .map(
            |location| match json_path::get_mut(root, location).map(&mut f) {
                Some(Ok(reply)) => reply,
                _ => resp_null_for(resp),
            },
        )
        .collect();
    replies.reverse();
    resp_array(replies)
}

// ── Formatting (JSON.GET INDENT / NEWLINE / SPACE) ───────────────────────────

#[derive(Default)]
struct Format<'a> {
    indent: &'a str,
    newline: &'a str,
    space: &'a str,
}

impl Format<'_> {
    fn render(&self, value: &Value) -> String {
        if self.indent.is_empty() && self.newline.is_empty() && self.space.is_empty() {
            return json_text(value);
        }
        let mut out = String::new();
        self.write(value, 0, &mut out);
        out
    }

    fn line(&self, depth: usize, out: &mut String) {
        out.push_str(self.newline);
        for _ in 0..depth {
            out.push_str(self.indent);
        }
    }

    fn write(&self, value: &Value, depth: usize, out: &mut String) {
        match value {
            Value::Array(items) if !items.is_empty() => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    self.line(depth + 1, out);
                    self.write(item, depth + 1, out);
                }
                self.line(depth, out);
                out.push(']');
            }
            Value::Object(members) if !members.is_empty() => {
                out.push('{');
                for (i, (key, member)) in members.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    self.line(depth + 1, out);
                    out.push_str(&json_text(&Value::String(key.clone())));
                    out.push(':');
                    out.push_str(self.space);
                    self.write(member, depth + 1, out);
                }
                self.line(depth, out);
                out.push('}');
            }
            scalar => out.push_str(&json_text(scalar)),
        }
    }
}

// ── Writing ──────────────────────────────────────────────────────────────────

/// Set `value` at `query` inside an existing document. `Ok(true)` if anything
/// was set, `Ok(false)` if NX/XX or a path that leads nowhere left the
/// document as it was.
fn set_in(
    root: &mut Value,
    query: &Query,
    value: &Value,
    nx: bool,
    xx: bool,
) -> Result<bool, Vec<u8>> {
    if !query.selects() {
        return Err(resp_error(NOT_A_SELECTION));
    }
    if query.is_root() {
        if nx {
            return Ok(false);
        }
        *root = value.clone();
        return Ok(true);
    }
    let locations = query.locations(root);
    if !locations.is_empty() {
        if nx {
            return Ok(false);
        }
        for location in locations.iter().rev() {
            if let Some(node) = json_path::get_mut(root, location) {
                *node = value.clone();
            }
        }
        return Ok(true);
    }
    if xx {
        return Ok(false);
    }
    // Nothing there yet: a new member of an object that is.
    match query.new_member(root) {
        Some((parents, name)) => {
            for parent in &parents {
                if let Some(Value::Object(members)) = json_path::get_mut(root, parent) {
                    members.insert(name.clone(), value.clone());
                }
            }
            Ok(true)
        }
        None if query.is_legacy() => Err(resp_error("ERR path does not exist")),
        None => Ok(false),
    }
}

/// `JSON.SET` for one key. `Ok(true)` if the value was stored.
fn set_key(
    ctx: &Ctx,
    key: &str,
    query: &Query,
    value: &Value,
    nx: bool,
    xx: bool,
) -> Result<bool, Vec<u8>> {
    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, key);
    let mut map = write_lock!(shard);
    match map.get(key) {
        Some(ValueRef::Json(tree)) => set_in(&mut tree.write(), query, value, nx, xx),
        Some(_) => Err(resp_error(WRONGTYPE)),
        None if !query.is_root() => Err(resp_error("ERR new objects must be created at the root")),
        None if xx => Ok(false),
        None => {
            map.insert(
                Key::from(key),
                KiviDBValue::Json(Arc::new(RwLock::new(value.clone()))),
            );
            Ok(true)
        }
    }
}

/// Remove the node at `location`. Removing a member keeps the order of the
/// ones that stay.
fn remove_at(root: &mut Value, location: &Location) -> bool {
    let Some((last, parent)) = location.split_last() else {
        return false;
    };
    match (json_path::get_mut(root, parent), last) {
        (Some(Value::Object(members)), Step::Key(key)) => members.shift_remove(key).is_some(),
        (Some(Value::Array(items)), Step::Index(index)) if *index < items.len() => {
            items.remove(*index);
            true
        }
        _ => false,
    }
}

/// Delete the whole key: the value, its TTL and whatever indexed it.
fn delete_document(ctx: &Ctx, key: &str) -> bool {
    let removed = delete_key(ctx.store, *ctx.selected_db, key);
    if removed {
        crate::vector::maybe_unindex(ctx.index_store, key);
    }
    removed
}

/// RFC 7396 JSON Merge Patch.
fn merge(target: &mut Value, patch: &Value) {
    let Value::Object(patch) = patch else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = Value::Object(serde_json::Map::new());
    }
    let Value::Object(members) = target else {
        return;
    };
    for (key, value) in patch {
        if value.is_null() {
            members.shift_remove(key);
        } else {
            merge(members.entry(key.clone()).or_insert(Value::Null), value);
        }
    }
}

// ── Numbers ──────────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
enum NumOp {
    Add,
    Multiply,
}

/// `current ⊕ operand`. Integers stay integers while the result fits; a
/// result that is not a finite number is refused.
fn arithmetic(current: &Number, operand: &Number, op: NumOp) -> Option<Number> {
    if let (Some(a), Some(b)) = (current.as_i64(), operand.as_i64()) {
        let exact = match op {
            NumOp::Add => a.checked_add(b),
            NumOp::Multiply => a.checked_mul(b),
        };
        if let Some(n) = exact {
            return Some(n.into());
        }
    }
    let (a, b) = (current.as_f64()?, operand.as_f64()?);
    let result = match op {
        NumOp::Add => a + b,
        NumOp::Multiply => a * b,
    };
    // A whole number that an integer can hold exactly is stored as one, so
    // `1.5 + 1.5` is `3` and not `3.0`.
    if result.fract() == 0.0 && result.abs() < 9_007_199_254_740_992.0 {
        return Some((result as i64).into());
    }
    Number::from_f64(result)
}

/// `JSON.NUMINCRBY` / `JSON.NUMMULTBY`.
fn number_command(ctx: &Ctx, resp: u8, op: NumOp, name: &str) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() != 4 {
        return resp_error(&format!(
            "ERR wrong number of arguments for '{}' command",
            name.to_ascii_lowercase()
        ));
    }
    let (Some(key), Some(path)) = (arg(cmd, 1), arg(cmd, 2)) else {
        return resp_error("ERR key and path must be valid UTF-8");
    };
    let operand = match arg(cmd, 3).and_then(|text| serde_json::from_str::<Value>(text).ok()) {
        Some(Value::Number(n)) => n,
        _ => return resp_error("ERR value must be a number"),
    };
    let query = match parse_path(path) {
        Ok(query) => query,
        Err(e) => return e,
    };
    write_doc(ctx, key, resp_null_bulk(), |root| {
        if !query.selects() {
            return resp_error(NOT_A_SELECTION);
        }
        let locations = query.locations(root);
        // Work everything out before changing anything, so a result that is
        // out of range fails the command and leaves the document alone.
        let mut results: Vec<Option<Number>> = Vec::with_capacity(locations.len());
        for location in &locations {
            match json_path::get(root, location) {
                Some(Value::Number(current)) => match arithmetic(current, &operand, op) {
                    Some(next) => results.push(Some(next)),
                    None => return resp_error("ERR result is not a finite number"),
                },
                _ => results.push(None),
            }
        }
        if query.is_legacy() {
            results.truncate(1);
        }
        for (location, result) in locations.iter().zip(&results) {
            if let (Some(node), Some(next)) = (json_path::get_mut(root, location), result) {
                *node = Value::Number(next.clone());
            }
        }
        if query.is_legacy() {
            return match results.first() {
                Some(Some(next)) => resp_bulk_str(&next.to_string()),
                Some(None) => resp_error("ERR path is not a number"),
                None => resp_null_bulk(),
            };
        }
        if resp >= 3 {
            return resp_array(
                results
                    .iter()
                    .map(|result| match result {
                        Some(n) => match n.as_i64() {
                            Some(i) => resp_integer(i),
                            None => resp_double_for(resp, n.as_f64().unwrap_or(0.0)),
                        },
                        None => resp_null_for(resp),
                    })
                    .collect(),
            );
        }
        // RESP2: the new values as one JSON array, null where the match was
        // not a number.
        let values: Vec<Value> = results
            .into_iter()
            .map(|result| result.map_or(Value::Null, Value::Number))
            .collect();
        resp_bulk_str(&json_text(&Value::Array(values)))
    })
}

// ── JSON.RESP ────────────────────────────────────────────────────────────────

/// A JSON value in RESP: null as nil, booleans as `true`/`false`, integers as
/// integers, other numbers and strings as bulk strings, an array as `[`
/// followed by its elements, an object as `{` followed by its members as
/// alternating names and values.
fn as_resp(resp: u8, value: &Value) -> Vec<u8> {
    match value {
        Value::Null => resp_null_for(resp),
        Value::Bool(b) => resp_simple(if *b { "true" } else { "false" }),
        Value::Number(n) => match n.as_i64() {
            Some(i) => resp_integer(i),
            None => resp_bulk_str(&n.to_string()),
        },
        Value::String(s) => resp_bulk_str(s),
        Value::Array(items) => {
            let mut out = vec![resp_simple("[")];
            out.extend(items.iter().map(|item| as_resp(resp, item)));
            resp_array(out)
        }
        Value::Object(members) => {
            let mut out = vec![resp_simple("{")];
            for (key, member) in members {
                out.push(resp_bulk_str(key));
                out.push(as_resp(resp, member));
            }
            resp_array(out)
        }
    }
}

// ── Array helpers ────────────────────────────────────────────────────────────

/// An index that may count from the end, clamped into `0..=len`.
fn clamp_index(index: i64, len: usize) -> usize {
    let len = len as i64;
    let at = if index < 0 { len + index } else { index };
    at.clamp(0, len) as usize
}

fn not_an_array<T>() -> Result<T, &'static str> {
    Err("ERR path is not an array")
}

// ── Commands ─────────────────────────────────────────────────────────────────

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    if !name.starts_with("JSON.") {
        return None;
    }
    // JSON replies routinely nest nils inside arrays (JSON.MGET slots, per-match
    // results for $ paths). Those are invisible to the dispatcher's top-level
    // null normalisation, so each site encodes for the negotiated protocol.
    let resp = crate::commands::handler::effective_resp_for_reply(ctx);
    let ctx: &Ctx = ctx;
    let cmd = &ctx.cmd;

    let wrong_args = || {
        resp_error(&format!(
            "ERR wrong number of arguments for '{}' command",
            name.to_ascii_lowercase()
        ))
    };
    // The key, and the path if there is one (the root if not).
    let key_and_path = |path_at: usize| -> Result<(&str, Query), Vec<u8>> {
        let key = arg(cmd, 1).ok_or_else(|| resp_error("ERR key must be valid UTF-8"))?;
        let path = match cmd.get(path_at) {
            Some(bytes) => {
                parse_utf8(bytes).ok_or_else(|| resp_error("ERR path must be valid UTF-8"))?
            }
            None => ".",
        };
        Ok((key, parse_path(path)?))
    };
    macro_rules! key_and_path {
        ($at:expr) => {
            match key_and_path($at) {
                Ok(parts) => parts,
                Err(e) => return Some(e),
            }
        };
    }

    Some(match name {
        // ── JSON.SET key path value [NX | XX] [FPHA type] ────────────────────
        // NX: only if the path does not exist yet. XX: only if it does.
        // FPHA: the floating-point type to hold arrays of floats in.
        "JSON.SET" => {
            if cmd.len() < 4 {
                return Some(wrong_args());
            }
            let (key, query) = key_and_path!(2);
            let mut value: Value = match arg(cmd, 3).and_then(|s| serde_json::from_str(s).ok()) {
                Some(value) => value,
                None => return Some(resp_error("ERR invalid JSON")),
            };
            let (mut nx, mut xx) = (false, false);
            let mut i = 4;
            while i < cmd.len() {
                if cmd[i].eq_ignore_ascii_case(b"NX") && !xx {
                    nx = true;
                    i += 1;
                } else if cmd[i].eq_ignore_ascii_case(b"XX") && !nx {
                    xx = true;
                    i += 1;
                } else if cmd[i].eq_ignore_ascii_case(b"FPHA") {
                    let Some(ty) = cmd
                        .get(i + 1)
                        .and_then(|arg| crate::commands::json_fpha::FpType::parse(arg))
                    else {
                        return Some(resp_error("ERR syntax error"));
                    };
                    // Before anything is stored: a value that does not fit
                    // the type fails the command and changes nothing.
                    if crate::commands::json_fpha::apply(&mut value, ty).is_err() {
                        return Some(resp_error("ERR value out of range"));
                    }
                    i += 2;
                } else {
                    return Some(resp_error("ERR syntax error"));
                }
            }
            match set_key(ctx, key, &query, &value, nx, xx) {
                Ok(true) => resp_simple("OK"),
                Ok(false) => resp_null_bulk(),
                Err(e) => e,
            }
        }

        // ── JSON.MSET key path value [key path value ...] ────────────────────
        "JSON.MSET" => {
            if cmd.len() < 4 || !(cmd.len() - 1).is_multiple_of(3) {
                return Some(wrong_args());
            }
            // Every argument is checked before any key is touched.
            let mut writes: Vec<(&str, Query, Value)> = Vec::new();
            for triple in cmd[1..].chunks(3) {
                let Some(key) = parse_utf8(triple[0]) else {
                    return Some(resp_error("ERR key must be valid UTF-8"));
                };
                let query = match parse_utf8(triple[1]).map(parse_path) {
                    Some(Ok(query)) => query,
                    Some(Err(e)) => return Some(e),
                    None => return Some(resp_error("ERR path must be valid UTF-8")),
                };
                let value = match parse_json(parse_utf8(triple[2])) {
                    Ok(value) => value,
                    Err(e) => return Some(e),
                };
                writes.push((key, query, value));
            }
            for (key, query, value) in &writes {
                if let Err(e) = set_key(ctx, key, query, value, false, false) {
                    return Some(e);
                }
            }
            resp_simple("OK")
        }

        // ── JSON.GET key [INDENT s] [NEWLINE s] [SPACE s] [path ...] ─────────
        // No path: the whole document. One path: what it matches. Several:
        // an object with one member per path.
        "JSON.GET" => {
            if cmd.len() < 2 {
                return Some(wrong_args());
            }
            let Some(key) = arg(cmd, 1) else {
                return Some(resp_error("ERR key must be valid UTF-8"));
            };
            let mut format = Format::default();
            let mut paths: Vec<(&str, Query)> = Vec::new();
            let mut i = 2;
            while i < cmd.len() {
                let Some(word) = arg(cmd, i) else {
                    return Some(resp_error("ERR path must be valid UTF-8"));
                };
                let option = word.to_ascii_uppercase();
                match option.as_str() {
                    "INDENT" | "NEWLINE" | "SPACE" if i + 1 < cmd.len() => {
                        let Some(text) = arg(cmd, i + 1) else {
                            return Some(resp_error("ERR syntax error"));
                        };
                        match option.as_str() {
                            "INDENT" => format.indent = text,
                            "NEWLINE" => format.newline = text,
                            _ => format.space = text,
                        }
                        i += 2;
                    }
                    "NOESCAPE" => i += 1,
                    _ => {
                        match parse_path(word) {
                            Ok(query) => paths.push((word, query)),
                            Err(e) => return Some(e),
                        }
                        i += 1;
                    }
                }
            }
            read_doc(ctx, key, resp_null_bulk(), |root| {
                // What one path contributes: every match for a JSONPath, the
                // first for a legacy path.
                let matched = |path: &str, query: &Query, all: bool| -> Result<Value, Vec<u8>> {
                    let mut values: Vec<Value> = query
                        .values(root)
                        .into_iter()
                        .map(std::borrow::Cow::into_owned)
                        .collect();
                    if all {
                        return Ok(Value::Array(values));
                    }
                    if values.is_empty() {
                        return Err(resp_error(&format!("ERR Path '{path}' does not exist")));
                    }
                    Ok(values.swap_remove(0))
                };
                let reply = match paths.as_slice() {
                    [] => Ok(root.clone()),
                    [(path, query)] => matched(path, query, !query.is_legacy()),
                    several => {
                        // One JSONPath among them and every member is a list.
                        let all = several.iter().any(|(_, query)| !query.is_legacy());
                        several
                            .iter()
                            .map(|(path, query)| Ok((path.to_string(), matched(path, query, all)?)))
                            .collect::<Result<serde_json::Map<String, Value>, Vec<u8>>>()
                            .map(Value::Object)
                    }
                };
                match reply {
                    Ok(value) => resp_bulk_str(&format.render(&value)),
                    Err(e) => e,
                }
            })
        }

        // ── JSON.MGET key [key ...] path ─────────────────────────────────────
        // A key that does not exist, or holds something else, is nil.
        "JSON.MGET" => {
            if cmd.len() < 3 {
                return Some(wrong_args());
            }
            let query = match arg(cmd, cmd.len() - 1).map(parse_path) {
                Some(Ok(query)) => query,
                Some(Err(e)) => return Some(e),
                None => return Some(resp_error("ERR path must be valid UTF-8")),
            };
            let db = &ctx.store[*ctx.selected_db];
            let replies = cmd[1..cmd.len() - 1]
                .iter()
                .map(|key| {
                    let Some(key) = parse_utf8(key) else {
                        return resp_null_for(resp);
                    };
                    let shard = store::get_shard(db, key);
                    let map = read_lock!(shard);
                    let Some(ValueRef::Json(tree)) = map.get(key) else {
                        return resp_null_for(resp);
                    };
                    let root = tree.read();
                    let values = query.values(&root);
                    if query.is_legacy() {
                        match values.first() {
                            Some(value) => resp_bulk_str(&json_text(value)),
                            None => resp_null_for(resp),
                        }
                    } else {
                        let all: Vec<&Value> = values.iter().map(|v| v.as_ref()).collect();
                        resp_bulk_str(&serde_json::to_string(&all).unwrap_or_default())
                    }
                })
                .collect();
            resp_array(replies)
        }

        // ── JSON.DEL / JSON.FORGET key [path] ────────────────────────────────
        // The number of values deleted. The root deletes the key.
        "JSON.DEL" | "JSON.FORGET" => {
            if cmd.len() < 2 {
                return Some(wrong_args());
            }
            let (key, query) = key_and_path!(2);
            if !query.selects() {
                return Some(resp_error(NOT_A_SELECTION));
            }
            let is_json = read_doc(ctx, key, Vec::new(), |_| b"+".to_vec());
            if is_json != b"+" {
                // Missing (nothing deleted) or of another type (an error).
                return Some(if is_json.is_empty() {
                    resp_integer(0)
                } else {
                    is_json
                });
            }
            if query.is_root() {
                return Some(resp_integer(delete_document(ctx, key) as i64));
            }
            write_doc(ctx, key, resp_integer(0), |root| {
                let locations = query.locations(root);
                let removed = locations
                    .iter()
                    .rev()
                    .filter(|location| remove_at(root, location))
                    .count();
                resp_integer(removed as i64)
            })
        }

        // ── JSON.TYPE key [path] ─────────────────────────────────────────────
        "JSON.TYPE" => {
            if cmd.len() < 2 {
                return Some(wrong_args());
            }
            let (key, query) = key_and_path!(2);
            read_doc(ctx, key, resp_null_bulk(), |root| {
                each_value(resp, &query, root, |value| {
                    Some(resp_bulk_str(type_name(value)))
                })
            })
        }

        // ── JSON.STRLEN / JSON.OBJLEN / JSON.ARRLEN key [path] ───────────────
        "JSON.STRLEN" | "JSON.OBJLEN" | "JSON.ARRLEN" => {
            if cmd.len() < 2 {
                return Some(wrong_args());
            }
            let (key, query) = key_and_path!(2);
            read_doc(ctx, key, resp_null_bulk(), |root| {
                each_value(resp, &query, root, |value| {
                    let len = match (name, value) {
                        ("JSON.STRLEN", Value::String(s)) => s.len(),
                        ("JSON.OBJLEN", Value::Object(members)) => members.len(),
                        ("JSON.ARRLEN", Value::Array(items)) => items.len(),
                        _ => return None,
                    };
                    Some(resp_integer(len as i64))
                })
            })
        }

        // ── JSON.OBJKEYS key [path] ──────────────────────────────────────────
        "JSON.OBJKEYS" => {
            if cmd.len() < 2 {
                return Some(wrong_args());
            }
            let (key, query) = key_and_path!(2);
            read_doc(ctx, key, resp_null_bulk(), |root| {
                each_value(resp, &query, root, |value| {
                    let members = value.as_object()?;
                    Some(resp_array(
                        members.keys().map(|key| resp_bulk_str(key)).collect(),
                    ))
                })
            })
        }

        // ── JSON.STRAPPEND key [path] value ──────────────────────────────────
        // The value is a JSON string: '"world"', not 'world'. Replies with
        // the new length.
        "JSON.STRAPPEND" => {
            if cmd.len() < 3 {
                return Some(wrong_args());
            }
            let has_path = cmd.len() > 3;
            let (key, query) = key_and_path!(if has_path { 2 } else { usize::MAX });
            let suffix = match arg(cmd, if has_path { 3 } else { 2 })
                .and_then(|text| serde_json::from_str::<Value>(text).ok())
            {
                Some(Value::String(suffix)) => suffix,
                _ => return Some(resp_error("ERR value must be a JSON string")),
            };
            write_doc(ctx, key, resp_null_bulk(), |root| {
                each_node(resp, &query, root, |node| match node {
                    Value::String(s) => {
                        s.push_str(&suffix);
                        Ok(resp_integer(s.len() as i64))
                    }
                    _ => Err("ERR path is not a string"),
                })
            })
        }

        // ── JSON.TOGGLE key path ─────────────────────────────────────────────
        // Flips a boolean. A JSONPath replies with the new values as 1 and 0;
        // a legacy path with "true" or "false".
        "JSON.TOGGLE" => {
            if cmd.len() < 3 {
                return Some(wrong_args());
            }
            let (key, query) = key_and_path!(2);
            let legacy = query.is_legacy();
            write_doc(ctx, key, resp_null_bulk(), |root| {
                each_node(resp, &query, root, |node| match node {
                    Value::Bool(b) => {
                        *b = !*b;
                        Ok(if legacy {
                            resp_bulk_str(if *b { "true" } else { "false" })
                        } else {
                            resp_integer(*b as i64)
                        })
                    }
                    _ => Err("ERR path is not a boolean"),
                })
            })
        }

        // ── JSON.NUMINCRBY / JSON.NUMMULTBY key path number ──────────────────
        "JSON.NUMINCRBY" => number_command(ctx, resp, NumOp::Add, name),
        "JSON.NUMMULTBY" => number_command(ctx, resp, NumOp::Multiply, name),

        // ── JSON.ARRAPPEND key path value [value ...] ────────────────────────
        // Replies with the new length.
        "JSON.ARRAPPEND" => {
            if cmd.len() < 4 {
                return Some(wrong_args());
            }
            let (key, query) = key_and_path!(2);
            // Parse every value before touching the document.
            let values: Vec<Value> = match cmd[3..]
                .iter()
                .map(|raw| parse_json(parse_utf8(raw)))
                .collect()
            {
                Ok(values) => values,
                Err(e) => return Some(e),
            };
            write_doc(ctx, key, resp_null_bulk(), |root| {
                each_node(resp, &query, root, |node| match node {
                    Value::Array(items) => {
                        items.extend(values.iter().cloned());
                        Ok(resp_integer(items.len() as i64))
                    }
                    _ => not_an_array(),
                })
            })
        }

        // ── JSON.ARRINSERT key path index value [value ...] ──────────────────
        // Inserts before `index`; a negative index counts from the end.
        "JSON.ARRINSERT" => {
            if cmd.len() < 5 {
                return Some(wrong_args());
            }
            let (key, query) = key_and_path!(2);
            let Some(index) = arg(cmd, 3).and_then(|s| s.parse::<i64>().ok()) else {
                return Some(resp_error("ERR index must be an integer"));
            };
            let values: Vec<Value> = match cmd[4..]
                .iter()
                .map(|raw| parse_json(parse_utf8(raw)))
                .collect()
            {
                Ok(values) => values,
                Err(e) => return Some(e),
            };
            write_doc(ctx, key, resp_null_bulk(), |root| {
                each_node(resp, &query, root, |node| match node {
                    Value::Array(items) => {
                        let len = items.len() as i64;
                        let at = if index < 0 { len + index } else { index };
                        if !(0..=len).contains(&at) {
                            return Err("ERR index out of bounds");
                        }
                        let at = at as usize;
                        items.splice(at..at, values.iter().cloned());
                        Ok(resp_integer(items.len() as i64))
                    }
                    _ => not_an_array(),
                })
            })
        }

        // ── JSON.ARRPOP key [path [index]] ───────────────────────────────────
        // Removes and returns an element, the last by default. An index out
        // of range means the nearest end; an empty array has nothing to pop.
        "JSON.ARRPOP" => {
            if cmd.len() < 2 {
                return Some(wrong_args());
            }
            let (key, query) = key_and_path!(2);
            let index = match cmd.get(3) {
                Some(raw) => match parse_utf8(raw).and_then(|s| s.parse::<i64>().ok()) {
                    Some(index) => index,
                    None => return Some(resp_error("ERR index must be an integer")),
                },
                None => -1,
            };
            write_doc(ctx, key, resp_null_bulk(), |root| {
                each_node(resp, &query, root, |node| match node {
                    Value::Array(items) if items.is_empty() => Ok(resp_null_for(resp)),
                    Value::Array(items) => {
                        let at = clamp_index(index, items.len()).min(items.len() - 1);
                        Ok(resp_bulk_str(&json_text(&items.remove(at))))
                    }
                    _ => not_an_array(),
                })
            })
        }

        // ── JSON.ARRTRIM key path start stop ─────────────────────────────────
        // Keeps the elements from `start` to `stop`, both included. Replies
        // with the new length.
        "JSON.ARRTRIM" => {
            if cmd.len() != 5 {
                return Some(wrong_args());
            }
            let (key, query) = key_and_path!(2);
            let bound = |at: usize| arg(cmd, at).and_then(|s| s.parse::<i64>().ok());
            let (Some(start), Some(stop)) = (bound(3), bound(4)) else {
                return Some(resp_error("ERR start and stop must be integers"));
            };
            write_doc(ctx, key, resp_null_bulk(), |root| {
                each_node(resp, &query, root, |node| match node {
                    Value::Array(items) => {
                        let len = items.len();
                        let from = clamp_index(start, len);
                        // `stop` is inclusive, and past the end means the end.
                        let to = if stop < 0 && (len as i64 + stop) < 0 {
                            0
                        } else {
                            (clamp_index(stop, len) + 1).min(len)
                        };
                        if from >= to {
                            items.clear();
                        } else {
                            items.truncate(to);
                            items.drain(..from);
                        }
                        Ok(resp_integer(items.len() as i64))
                    }
                    _ => not_an_array(),
                })
            })
        }

        // ── JSON.ARRINDEX key path value [start [stop]] ──────────────────────
        // The index of the first element equal to `value` between `start`
        // (included) and `stop` (excluded; 0 means the end), or -1.
        "JSON.ARRINDEX" => {
            if cmd.len() < 4 {
                return Some(wrong_args());
            }
            let (key, query) = key_and_path!(2);
            let needle = match parse_json(arg(cmd, 3)) {
                Ok(needle) => needle,
                Err(_) => return Some(resp_error("ERR invalid JSON scalar")),
            };
            let bound = |at: usize| arg(cmd, at).and_then(|s| s.parse::<i64>().ok());
            let (start, stop) = (bound(4).unwrap_or(0), bound(5).unwrap_or(0));
            read_doc(ctx, key, resp_null_bulk(), |root| {
                let search = |value: &Value| -> Option<i64> {
                    let items = value.as_array()?;
                    let from = clamp_index(start, items.len());
                    let to = if stop == 0 {
                        items.len()
                    } else {
                        clamp_index(stop, items.len())
                    };
                    let found = items
                        .get(from..to.max(from))
                        .and_then(|window| {
                            window
                                .iter()
                                .position(|item| json_path::deep_eq(item, &needle))
                        })
                        .map_or(-1, |at| (at + from) as i64);
                    Some(found)
                };
                if query.is_legacy() {
                    return match query.values(root).first() {
                        Some(value) => match search(value) {
                            Some(found) => resp_integer(found),
                            None => resp_error("ERR path is not an array"),
                        },
                        None => resp_null_bulk(),
                    };
                }
                each_value(resp, &query, root, |value| search(value).map(resp_integer))
            })
        }

        // ── JSON.CLEAR key [path] ────────────────────────────────────────────
        // Empties arrays and objects and sets numbers to 0. Replies with how
        // many values that changed.
        "JSON.CLEAR" => {
            if cmd.len() < 2 || cmd.len() > 3 {
                return Some(wrong_args());
            }
            let (key, query) = key_and_path!(2);
            write_doc(ctx, key, resp_integer(0), |root| {
                if !query.selects() {
                    return resp_error(NOT_A_SELECTION);
                }
                let mut cleared = 0;
                for location in query.locations(root).iter().rev() {
                    let Some(node) = json_path::get_mut(root, location) else {
                        continue;
                    };
                    let changed = match node {
                        Value::Array(items) if !items.is_empty() => {
                            items.clear();
                            true
                        }
                        Value::Object(members) if !members.is_empty() => {
                            members.clear();
                            true
                        }
                        Value::Number(n) if n.as_f64() != Some(0.0) => {
                            *node = Value::Number(0.into());
                            true
                        }
                        _ => false,
                    };
                    cleared += changed as i64;
                }
                resp_integer(cleared)
            })
        }

        // ── JSON.MERGE key path value ────────────────────────────────────────
        // RFC 7396 JSON Merge Patch: members are merged, a null deletes, and
        // anything else replaces.
        "JSON.MERGE" => {
            if cmd.len() != 4 {
                return Some(wrong_args());
            }
            let (key, query) = key_and_path!(2);
            let patch = match parse_json(arg(cmd, 3)) {
                Ok(patch) => patch,
                Err(e) => return Some(e),
            };
            if !query.selects() {
                return Some(resp_error(NOT_A_SELECTION));
            }
            if query.is_root() {
                let exists = read_doc(ctx, key, Vec::new(), |_| b"+".to_vec());
                if exists.is_empty() {
                    // A new document is the patch applied to nothing.
                    let mut document = Value::Null;
                    merge(&mut document, &patch);
                    return Some(match set_key(ctx, key, &query, &document, false, false) {
                        Ok(_) => resp_simple("OK"),
                        Err(e) => e,
                    });
                }
                if exists != b"+" {
                    return Some(exists);
                }
                if patch.is_null() {
                    delete_document(ctx, key);
                    return Some(resp_simple("OK"));
                }
            }
            write_doc(ctx, key, resp_error("ERR key does not exist"), |root| {
                let locations = query.locations(root);
                if locations.is_empty() {
                    return match query.new_member(root) {
                        Some((parents, name)) => {
                            for parent in &parents {
                                if let Some(Value::Object(members)) =
                                    json_path::get_mut(root, parent)
                                    && !patch.is_null()
                                {
                                    let mut value = Value::Null;
                                    merge(&mut value, &patch);
                                    members.insert(name.clone(), value);
                                }
                            }
                            resp_simple("OK")
                        }
                        None => resp_error("ERR path does not exist"),
                    };
                }
                for location in locations.iter().rev() {
                    if patch.is_null() {
                        remove_at(root, location);
                    } else if let Some(node) = json_path::get_mut(root, location) {
                        merge(node, &patch);
                    }
                }
                resp_simple("OK")
            })
        }

        // ── JSON.DEBUG MEMORY key [path] ─────────────────────────────────────
        // The size of the value's JSON text, as an approximation of the
        // memory it takes.
        "JSON.DEBUG" => {
            let sub = arg(cmd, 1).unwrap_or("").to_ascii_uppercase();
            match sub.as_str() {
                "HELP" => resp_array(vec![
                    resp_bulk_str("MEMORY <key> [path] - reports memory usage"),
                    resp_bulk_str("HELP                - this message"),
                ]),
                "MEMORY" => {
                    if cmd.len() < 3 {
                        return Some(resp_error(
                            "ERR wrong number of arguments for 'json.debug|memory' command",
                        ));
                    }
                    let Some(key) = arg(cmd, 2) else {
                        return Some(resp_error("ERR key must be valid UTF-8"));
                    };
                    let query = match parse_path(arg(cmd, 3).unwrap_or(".")) {
                        Ok(query) => query,
                        Err(e) => return Some(e),
                    };
                    read_doc(ctx, key, resp_null_bulk(), |root| {
                        each_value(resp, &query, root, |value| {
                            Some(resp_integer(json_text(value).len() as i64))
                        })
                    })
                }
                _ => resp_error("ERR unknown subcommand - try `JSON.DEBUG HELP`"),
            }
        }

        // ── JSON.RESP key [path] ─────────────────────────────────────────────
        "JSON.RESP" => {
            if cmd.len() < 2 {
                return Some(wrong_args());
            }
            let (key, query) = key_and_path!(2);
            read_doc(ctx, key, resp_null_bulk(), |root| {
                each_value(resp, &query, root, |value| Some(as_resp(resp, value)))
            })
        }

        _ => return None,
    })
}
