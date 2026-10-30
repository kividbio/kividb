// src/cluster/search.rs
//
// Searching a whole cluster.
//
// Each master indexes the keys it holds. FT.SEARCH and FT.AGGREGATE sent to
// a node of a cluster are answered from all of them: the node that receives
// the command (the coordinator) sends it on to every other master, over the
// connections and with the credentials replication uses, marked so that the
// receiving node answers from its own keys rather than sending it on again.
// The coordinator merges what comes back:
//
//   FT.SEARCH     each master is asked for the first offset+count matches,
//                 with their scores and sort keys; the coordinator orders
//                 them all as one node would, and takes the page asked for.
//                 The total is the sum of the masters' totals. A master's
//                 reply says whether its scan ran out of time; the
//                 coordinator ORs those flags and applies ON_TIMEOUT.
//   FT.AGGREGATE  each master sends the rows that match the query, before
//                 any step, and whether it timed out; the coordinator runs
//                 the steps (GROUPBY, REDUCE, APPLY, FILTER, SORTBY, LIMIT)
//                 over all of them, so every reducer is exact, then applies
//                 ON_TIMEOUT. WITHCURSOR keeps the cursor on the coordinator.
//
// Whether a master's part timed out travels only when the coordinator asks
// for it (see `ask_for_timeout_flag`), so that during a rolling upgrade
// each side gets the reply it understands: a master from before the flag
// does not know the question and answers as it always did, and a
// coordinator from before it never asks.
//
// Relevance (BM25) is computed by each master over its own documents, as in
// RediSearch's coordinator. A node answers only for the slots it serves, so
// keys part-way through a migration are not counted twice.

use crate::cluster::ClusterState;
use crate::cluster::node::NodeFlags;
use std::io::{BufRead, BufReader, Write};
use std::net::ToSocketAddrs;
use std::time::Duration;

/// The client name a coordinator's requests are sent under.
pub const FORWARDED: &str = "kividb-cluster-search";

const TIMEOUT: Duration = Duration::from_secs(10);

/// A reply as read off the wire.
#[derive(Debug, Clone)]
pub enum Reply {
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Simple(String),
    Error(String),
    Array(Vec<Reply>),
}

impl Reply {
    pub fn text(&self) -> Option<String> {
        match self {
            Reply::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
            Reply::Simple(s) => Some(s.clone()),
            Reply::Int(n) => Some(n.to_string()),
            _ => None,
        }
    }
}

/// Read one reply. RESP2, as a coordinator's connections speak it.
pub fn read_reply(reader: &mut impl BufRead) -> Result<Reply, String> {
    read_reply_within(reader, 0)
}

/// The deepest a reply from another node may nest, and the longest string
/// in it: what a search or an index's definition can come to, with room.
const REPLY_DEPTH_MAX: u32 = 32;
const REPLY_BULK_MAX: i64 = 512 * 1024 * 1024;

fn read_reply_within(reader: &mut impl BufRead, depth: u32) -> Result<Reply, String> {
    if depth > REPLY_DEPTH_MAX {
        return Err("a reply nested too deep".into());
    }
    let mut line = String::new();
    if reader.read_line(&mut line).map_err(|e| e.to_string())? == 0 {
        return Err("connection closed".into());
    }
    let line = line.trim_end_matches(['\r', '\n']);
    let (kind, rest) = line.split_at(line.len().min(1));
    let number = || {
        rest.parse::<i64>()
            .map_err(|_| format!("bad reply: {line}"))
    };
    match kind {
        "+" => Ok(Reply::Simple(rest.to_string())),
        "-" => Ok(Reply::Error(rest.to_string())),
        ":" => Ok(Reply::Int(number()?)),
        "$" => {
            let len = number()?;
            if len < 0 {
                return Ok(Reply::Bulk(None));
            }
            if len > REPLY_BULK_MAX {
                return Err(format!("bad reply: {line}"));
            }
            let mut body = vec![0u8; len as usize + 2];
            reader.read_exact(&mut body).map_err(|e| e.to_string())?;
            body.truncate(len as usize);
            Ok(Reply::Bulk(Some(body)))
        }
        "*" => {
            let n = number()?;
            if n < 0 {
                return Ok(Reply::Bulk(None));
            }
            // As many as the other node says, but room made only for some.
            let mut items = Vec::with_capacity((n as usize).min(1024));
            for _ in 0..n {
                items.push(read_reply_within(reader, depth + 1)?);
            }
            Ok(Reply::Array(items))
        }
        _ => Err(format!("unexpected reply: {line}")),
    }
}

/// Parse a reply this node produced itself.
pub fn parse_reply(bytes: &[u8]) -> Result<Reply, String> {
    read_reply(&mut BufReader::new(bytes))
}

/// Whether this node answers for `key`: its slot is this node's, or, on a
/// replica, its master's.
pub fn serves(cluster: &ClusterState, key: &str) -> bool {
    let owner = cluster.slot_owner(crate::cluster::key_hash_slot(key.as_bytes()));
    owner.is_some() && (owner == Some(cluster.myself()) || owner == cluster.my_master_id())
}

/// Whether this command came from a coordinator.
pub fn is_forwarded(ctx: &crate::commands::ctx::Ctx) -> bool {
    ctx.cluster.enabled
        && ctx
            .client_registry
            .get(ctx.client_id)
            .is_some_and(|c| c.name == FORWARDED)
}

/// The masters, other than the one whose data this node holds, that a
/// coordinator asks.
fn others(cluster: &ClusterState) -> Vec<(String, u16)> {
    let me = cluster.myself();
    let mine = cluster.my_master_id().unwrap_or(me);
    cluster
        .nodes
        .read()
        .values()
        .filter(|n| {
            n.id != me
                && n.id != mine
                && n.flags.contains(NodeFlags::MASTER)
                && !n.flags.contains(NodeFlags::HANDSHAKE)
                && !n.ip.is_empty()
                && n.client_port != 0
        })
        .map(|n| (n.ip.clone(), n.client_port))
        .collect()
}

/// Whether this node coordinates a search: in a cluster with other masters,
/// for a client (not a coordinator), and not inside a transaction or a
/// script, which hold the server's gates (a search there answers from this
/// node, as before).
pub fn coordinates(ctx: &crate::commands::ctx::Ctx) -> bool {
    ctx.cluster.enabled
        && !crate::write_gate::held_by_this_thread()
        && !crate::command_gate::alone_on_this_thread()
        && !is_forwarded(ctx)
        && !others(ctx.cluster).is_empty()
}

fn encode(parts: &[&[u8]]) -> Vec<u8> {
    let owned: Vec<Vec<u8>> = parts.iter().map(|p| p.to_vec()).collect();
    crate::resp::write::encode_resp_command(&owned)
}

/// [`ask_once`]; and a master that does not have the index (added to the
/// cluster after it was created) is given it, and asked again.
fn ask(
    host: &str,
    port: u16,
    credentials: &(String, String),
    db: usize,
    cmd: &[Vec<u8>],
    definition: Option<&[Vec<u8>]>,
) -> Result<Reply, String> {
    match ask_once(host, port, credentials, db, cmd) {
        Err(e) if e.ends_with("no such index") && definition.is_some() => {
            let create: Vec<&[u8]> = definition.unwrap().iter().map(Vec::as_slice).collect();
            crate::cluster::ddl::run_on(host, port, &credentials.0, &credentials.1, &create)?;
            log_info!("Cluster: gave {host}:{port} the definition of an index it lacked");
            ask_once(host, port, credentials, db, cmd)
        }
        other => other,
    }
}

fn ask_once(
    host: &str,
    port: u16,
    credentials: &(String, String),
    db: usize,
    cmd: &[Vec<u8>],
) -> Result<Reply, String> {
    let addr = (host, port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut it| it.next())
        .ok_or("could not resolve")?;
    let (stream, _) =
        crate::node_tls::connect_node(host, &addr, TIMEOUT).map_err(|e| e.to_string())?;
    let mut reader = BufReader::new(stream);
    let (user, password) = credentials;
    let mut out = Vec::new();
    let mut preamble = 0;
    if !password.is_empty() {
        out.extend(if user.is_empty() {
            // As the default user, by name: `AUTH <password>` is refused by a
            // node that has no password (#135), and with the name it logs in as
            // before.
            encode(&[b"AUTH", b"default", password.as_bytes()])
        } else {
            encode(&[b"AUTH", user.as_bytes(), password.as_bytes()])
        });
        preamble += 1;
    }
    out.extend(encode(&[b"CLIENT", b"SETNAME", FORWARDED.as_bytes()]));
    preamble += 1;
    // A cluster has database 0 only; SELECT would be one more command the
    // user these connections log in as must be allowed.
    if db != 0 {
        out.extend(encode(&[b"SELECT", db.to_string().as_bytes()]));
        preamble += 1;
    }
    let parts: Vec<&[u8]> = cmd.iter().map(Vec::as_slice).collect();
    out.extend(encode(&parts));
    let writer = reader.get_mut();
    writer
        .write_all(&out)
        .and_then(|()| writer.flush())
        .map_err(|e| e.to_string())?;
    for _ in 0..preamble {
        if let Reply::Error(e) = read_reply(&mut reader)? {
            return Err(e);
        }
    }
    match read_reply(&mut reader)? {
        Reply::Error(e) => Err(e),
        reply => Ok(reply),
    }
}

/// Send `cmd` to every other master (see [`others`]) and collect their
/// replies. An error if any of them cannot answer: a search of part of the
/// data is not one of the whole.
pub fn fan_out(ctx: &crate::commands::ctx::Ctx, cmd: &[Vec<u8>]) -> Result<Vec<Reply>, String> {
    let targets = others(ctx.cluster);
    let credentials = ctx.repl_state.master_credentials();
    let db = *ctx.selected_db;
    // The index's definition, for a master that lacks it.
    let definition: Option<Vec<Vec<u8>>> = cmd.get(1).and_then(|name| {
        let name = String::from_utf8_lossy(name);
        let store = ctx.index_store.read();
        let real = crate::vector::alias_target(&store, &name).unwrap_or(name.to_string());
        store.get(&real).map(|def| def.create_args())
    });
    let definition = definition.as_deref();
    let results: Vec<Result<Reply, String>> = std::thread::scope(|scope| {
        let handles: Vec<_> = targets
            .iter()
            .map(|(host, port)| {
                let credentials = &credentials;
                scope.spawn(move || {
                    ask(host, *port, credentials, db, cmd, definition)
                        .map_err(|e| format!("{host}:{port}: {e}"))
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|_| Err("panicked".into())))
            .collect()
    });
    let mut replies = Vec::with_capacity(results.len());
    let mut failed = Vec::new();
    for result in results {
        match result {
            Ok(reply) => replies.push(reply),
            Err(e) => failed.push(e),
        }
    }
    if failed.is_empty() {
        Ok(replies)
    } else {
        Err(format!(
            "ERR the search could not be answered by {} of the cluster's masters: {}",
            failed.len(),
            failed.join("; ")
        ))
    }
}

/// The parameter a coordinator adds to what it asks a master, to be told
/// whether that master's part ran out of time. A parameter, because every
/// version takes `PARAMS` anywhere in FT.SEARCH and FT.AGGREGATE, and one
/// the query does not use changes nothing: a master from before the flag
/// ignores it, and answers in the shape it always did.
const TIMEOUT_FLAG_PARAM: &[u8] = b"__kividb_timeout_flag";

/// Ask the masters for their timeout flag: `PARAMS 2 <name> 1`, last.
pub fn ask_for_timeout_flag(cmd: &mut Vec<Vec<u8>>) {
    cmd.extend([
        b"PARAMS".to_vec(),
        b"2".to_vec(),
        TIMEOUT_FLAG_PARAM.to_vec(),
        b"1".to_vec(),
    ]);
}

/// Whether a coordinator asked this master for its timeout flag (see
/// [`ask_for_timeout_flag`]).
pub fn wants_timeout_flag(cmd: &[&[u8]]) -> bool {
    matches!(
        cmd,
        [.., params, two, name, one]
            if params.eq_ignore_ascii_case(b"PARAMS")
                && *two == b"2"
                && *name == TIMEOUT_FLAG_PARAM
                && *one == b"1"
    )
}

/// A master's FT.SEARCH reply to a coordinator that asked for the flag:
/// the RESP2 page, then whether that master's scan ran out of time.
pub fn encode_search(page: Vec<u8>, timed_out: bool) -> Vec<u8> {
    use crate::resp::write::{resp_array, resp_integer};
    resp_array(vec![page, resp_integer(i64::from(timed_out))])
}

/// The page and timeout flag in a master's reply to [`encode_search`]. A
/// master from before the flag answers with the page alone, which begins
/// with its total, an integer, where the flagged reply begins with the
/// page, an array: that page did not time out.
pub fn decode_search(reply: Reply) -> Result<(Reply, bool), String> {
    match reply {
        Reply::Array(mut items) if matches!(items.as_slice(), [Reply::Array(_), Reply::Int(_)]) => {
            let timed_out = matches!(items.pop(), Some(Reply::Int(n)) if n != 0);
            Ok((items.remove(0), timed_out))
        }
        Reply::Array(items) if matches!(items.first(), Some(Reply::Int(_))) => {
            Ok((Reply::Array(items), false))
        }
        _ => Err("ERR a master answered the search with something else".into()),
    }
}

/// The rows of an aggregation, as a master sends them to its coordinator:
/// every column of every row, then, if the coordinator asked for it (see
/// [`ask_for_timeout_flag`]), whether that master's scan ran out of time.
pub fn encode_rows(rows: &[ahash::AHashMap<String, String>], timed_out: Option<bool>) -> Vec<u8> {
    use crate::resp::write::{resp_array, resp_bulk_str, resp_integer};
    let body = resp_array(
        rows.iter()
            .map(|row| {
                resp_array(
                    row.iter()
                        .flat_map(|(k, v)| [resp_bulk_str(k), resp_bulk_str(v)])
                        .collect(),
                )
            })
            .collect(),
    );
    match timed_out {
        Some(timed_out) => resp_array(vec![body, resp_integer(i64::from(timed_out))]),
        None => body,
    }
}

/// The rows and timeout flag in a master's reply to [`encode_rows`]. A
/// master from before the flag sends the rows alone: an array of arrays,
/// where the flagged reply ends with an integer. Those rows did not time
/// out.
pub fn decode_rows(reply: Reply) -> (Vec<ahash::AHashMap<String, String>>, bool) {
    let (rows, timed_out) = match reply {
        Reply::Array(mut wrap) if matches!(wrap.as_slice(), [Reply::Array(_), Reply::Int(_)]) => {
            let timed_out = matches!(wrap.pop(), Some(Reply::Int(n)) if n != 0);
            (wrap.remove(0), timed_out)
        }
        rows => (rows, false),
    };
    let rows = match rows {
        Reply::Array(rows) => rows
            .into_iter()
            .filter_map(|row| match row {
                Reply::Array(cells) => Some(
                    cells
                        .chunks(2)
                        .filter_map(|pair| Some((pair.first()?.text()?, pair.get(1)?.text()?)))
                        .collect(),
                ),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    (rows, timed_out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replies_are_read_whole_and_rows_survive_the_trip() {
        let reply = parse_reply(b"*3\r\n:2\r\n$3\r\nabc\r\n*2\r\n$-1\r\n+OK\r\n").unwrap();
        let Reply::Array(items) = reply else { panic!() };
        assert!(matches!(items[0], Reply::Int(2)));
        assert_eq!(items[1].text().as_deref(), Some("abc"));

        let mut row = ahash::AHashMap::new();
        row.insert("__key".to_string(), "doc:1".to_string());
        row.insert("price".to_string(), "10".to_string());
        let (back, timed_out) =
            decode_rows(parse_reply(&encode_rows(&[row.clone()], Some(false))).unwrap());
        assert_eq!(back, vec![row.clone()]);
        assert!(!timed_out);

        let (empty, timed_out) = decode_rows(parse_reply(&encode_rows(&[], Some(true))).unwrap());
        assert!(empty.is_empty());
        assert!(timed_out);

        let page = crate::resp::write::resp_array(vec![crate::resp::write::resp_integer(2)]);
        let (inner, timed_out) =
            decode_search(parse_reply(&encode_search(page, true)).unwrap()).unwrap();
        assert!(timed_out);
        let Reply::Array(items) = inner else { panic!() };
        assert!(matches!(items[0], Reply::Int(2)));
    }

    /// A master from before the timeout flag answers with the page or the
    /// rows alone: read as not timed out, in every shape they take.
    #[test]
    fn a_reply_without_the_flag_is_one_that_did_not_time_out() {
        use crate::resp::write::{resp_array, resp_bulk_str, resp_integer};
        // Pages: none found; a key and its score; one with its fields too.
        for page in [
            resp_array(vec![resp_integer(0)]),
            resp_array(vec![
                resp_integer(1),
                resp_bulk_str("doc:1"),
                resp_bulk_str("1"),
            ]),
            resp_array(vec![
                resp_integer(7),
                resp_bulk_str("doc:1"),
                resp_bulk_str("0.5"),
                resp_array(vec![resp_bulk_str("t"), resp_bulk_str("hello")]),
            ]),
        ] {
            let alone = parse_reply(&page).unwrap();
            let (inner, timed_out) = decode_search(alone.clone()).unwrap();
            assert!(!timed_out);
            assert_eq!(format!("{inner:?}"), format!("{alone:?}"));
            // The same page with the flag is told apart from it.
            let (flagged, timed_out) =
                decode_search(parse_reply(&encode_search(page, true)).unwrap()).unwrap();
            assert!(timed_out);
            assert_eq!(format!("{flagged:?}"), format!("{alone:?}"));
        }
        assert!(decode_search(Reply::Simple("OK".into())).is_err());
        assert!(decode_search(Reply::Array(vec![Reply::Bulk(Some(b"x".to_vec()))])).is_err());

        // Rows: none, one, two.
        let mut row = ahash::AHashMap::new();
        row.insert("__key".to_string(), "doc:1".to_string());
        for rows in [vec![], vec![row.clone()], vec![row.clone(), row.clone()]] {
            let (back, timed_out) = decode_rows(parse_reply(&encode_rows(&rows, None)).unwrap());
            assert_eq!(back, rows);
            assert!(!timed_out);
            let (back, timed_out) =
                decode_rows(parse_reply(&encode_rows(&rows, Some(true))).unwrap());
            assert_eq!(back, rows);
            assert!(timed_out);
        }
    }

    #[test]
    fn only_a_coordinator_that_asks_is_told_whether_a_part_timed_out() {
        let asks = |cmd: &[Vec<u8>]| -> bool {
            let parts: Vec<&[u8]> = cmd.iter().map(Vec::as_slice).collect();
            wants_timeout_flag(&parts)
        };
        let mut cmd: Vec<Vec<u8>> = vec![b"FT.SEARCH".to_vec(), b"idx".to_vec(), b"*".to_vec()];
        assert!(!asks(&cmd));
        cmd.extend([
            b"PARAMS".to_vec(),
            b"2".to_vec(),
            b"a".to_vec(),
            b"1".to_vec(),
        ]);
        assert!(!asks(&cmd));
        ask_for_timeout_flag(&mut cmd);
        assert!(asks(&cmd));
    }
}
