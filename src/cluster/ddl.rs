// src/cluster/ddl.rs
//
// Search index definitions across a cluster.
//
// Each node keeps its own indexes and indexes its own keys. In a cluster the
// keys of one index are spread over every master, so every master needs the
// definition. A command that defines, changes or drops an index (or an alias,
// or a dictionary) run on one master is therefore run, after it has succeeded
// there, on every other master too, over an ordinary client connection with
// the credentials replication uses (`masteruser` / `masterauth`). Replicas get
// it from their masters, as they always have.
//
// A repeat is harmless: the errors that mean "already done" (an index that
// already exists, one already dropped) count as success on the other nodes,
// and a command that fails here only for one of those reasons is still sent
// on. Running it again is how a node that missed it — down at the time, or
// added to the cluster since — is brought into line.
//
// Searching is not spread: FT.SEARCH and FT.AGGREGATE answer from the keys of
// the node they are sent to.

use crate::cluster::ClusterState;
use crate::cluster::node::NodeFlags;
use std::io::{BufRead, BufReader, Write};
use std::net::ToSocketAddrs;
use std::time::Duration;

/// The client name the forwarded copies are sent under, so that the node
/// receiving one runs it without sending it on again.
pub const FORWARDED: &str = "kividb-cluster-ddl";

const TIMEOUT: Duration = Duration::from_secs(5);

/// Whether `name` changes the index definitions every master must share.
pub fn is_definition_command(name: &str) -> bool {
    matches!(
        name,
        "FT.CREATE"
            | "FT.ALTER"
            | "FT.DROPINDEX"
            | "FT.DROP"
            | "FT.ALIASADD"
            | "FT.ALIASUPDATE"
            | "FT.ALIASDEL"
            | "FT.SYNUPDATE"
            | "FT.DICTADD"
            | "FT.DICTDEL"
    )
}

/// An error that means the command had already been applied.
fn already_done(error: &str) -> bool {
    let e = error.to_ascii_lowercase();
    e.contains("already exists") || e.contains("unknown index") || e.contains("does not exist")
}

/// Whether the reply this node gave means the command should go to the rest
/// of the cluster: it succeeded, or failed only because it had been done.
pub fn should_forward(reply: &[u8]) -> bool {
    match reply.strip_prefix(b"-") {
        None => true,
        Some(error) => already_done(&String::from_utf8_lossy(error)),
    }
}

/// Send the command in `ctx`, which this node answered with `reply`, to the
/// other masters, and give the client `reply` — or, if some of them did not
/// take it, an error that says so.
///
/// Not from a replica, nor for a command its master or the AOF replays,
/// nor for a copy another master forwarded. Nor inside a transaction or a
/// script: those hold the server's gates, and waiting on the network under
/// them would stall every write here; such a command applies to this node.
pub fn spread(ctx: &crate::commands::ctx::Ctx, reply: Vec<u8>) -> Vec<u8> {
    if ctx.apply_replication
        || ctx.repl_state.is_slave()
        || !should_forward(&reply)
        || crate::write_gate::held_by_this_thread()
        || crate::command_gate::alone_on_this_thread()
        || ctx
            .client_registry
            .get(ctx.client_id)
            .is_some_and(|c| c.name == FORWARDED)
    {
        return reply;
    }
    match forward(ctx.cluster, ctx.repl_state, &ctx.cmd) {
        None => reply,
        Some(error) => crate::resp::write::resp_error(&error),
    }
}

/// Run `cmd` on every other master. `None` if they all applied it (or had
/// already); otherwise the error to give the client.
pub fn forward(
    cluster: &ClusterState,
    repl_state: &crate::replication::ReplState,
    cmd: &[&[u8]],
) -> Option<String> {
    let me = cluster.myself();
    let targets: Vec<(String, u16)> = cluster
        .nodes
        .read()
        .values()
        .filter(|n| {
            n.id != me
                && n.flags.contains(NodeFlags::MASTER)
                && !n.flags.intersects(NodeFlags::HANDSHAKE | NodeFlags::FAIL)
                && !n.ip.is_empty()
                && n.client_port != 0
        })
        .map(|n| (n.ip.clone(), n.client_port))
        .collect();
    if targets.is_empty() {
        return None;
    }
    let (user, password) = repl_state.master_credentials();
    let failed: Vec<String> = std::thread::scope(|scope| {
        let handles: Vec<_> = targets
            .iter()
            .map(|(host, port)| {
                let (user, password) = (&user, &password);
                scope.spawn(move || {
                    run_on(host, *port, user, password, cmd)
                        .err()
                        .map(|e| format!("{host}:{port} ({e})"))
                })
            })
            .collect();
        handles
            .into_iter()
            .filter_map(|h| h.join().unwrap_or_else(|_| Some("panicked".into())))
            .collect()
    });
    if failed.is_empty() {
        return None;
    }
    let name = String::from_utf8_lossy(cmd[0]).to_ascii_uppercase();
    Some(format!(
        "ERR {name} was applied on this node but not on {} of the other {} master(s): {}. \
         Run it again once they are reachable; where it was applied, a repeat changes nothing",
        failed.len(),
        targets.len(),
        failed.join("; ")
    ))
}

fn encode(parts: &[&[u8]]) -> Vec<u8> {
    let owned: Vec<Vec<u8>> = parts.iter().map(|p| p.to_vec()).collect();
    crate::resp::write::encode_resp_command(&owned)
}

/// Read one reply and say whether it was an error, skipping over the body
/// of an aggregate or bulk one (FT.DICTADD answers with an integer, the rest
/// with a status).
fn read_reply(reader: &mut impl BufRead) -> Result<Result<(), String>, String> {
    let mut line = String::new();
    if reader.read_line(&mut line).map_err(|e| e.to_string())? == 0 {
        return Err("connection closed".into());
    }
    let line = line.trim_end();
    let (kind, rest) = line.split_at(line.len().min(1));
    match kind {
        "-" => Ok(Err(rest.to_string())),
        "+" | ":" | "_" | "#" | "," => Ok(Ok(())),
        "$" => {
            let len: i64 = rest.parse().map_err(|_| "bad reply")?;
            if len >= 0 {
                // Read and thrown away, a piece at a time.
                let want = len as u64 + 2;
                let got = std::io::copy(
                    &mut std::io::Read::take(&mut *reader, want),
                    &mut std::io::sink(),
                )
                .map_err(|e| e.to_string())?;
                if got != want {
                    return Err("connection closed".into());
                }
            }
            Ok(Ok(()))
        }
        "*" | "%" | "~" => {
            let n: i64 = rest.parse().map_err(|_| "bad reply")?;
            let n = if kind == "%" { n.saturating_mul(2) } else { n };
            for _ in 0..n.max(0) {
                read_reply(reader)??;
            }
            Ok(Ok(()))
        }
        _ => Err(format!("unexpected reply: {line}")),
    }
}

pub(crate) fn run_on(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    cmd: &[&[u8]],
) -> Result<(), String> {
    let addr = (host, port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut it| it.next())
        .ok_or("could not resolve")?;
    let (stream, _) =
        crate::node_tls::connect_node(host, &addr, TIMEOUT).map_err(|e| e.to_string())?;
    let mut reader = BufReader::new(stream);

    let mut out = Vec::new();
    let mut steps = 0;
    if !password.is_empty() {
        out.extend(if user.is_empty() {
            // As the default user, by name: `AUTH <password>` is refused by a
            // node that has no password (#135), and with the name it logs in as
            // before.
            encode(&[b"AUTH", b"default", password.as_bytes()])
        } else {
            encode(&[b"AUTH", user.as_bytes(), password.as_bytes()])
        });
        steps += 1;
    }
    out.extend(encode(&[b"CLIENT", b"SETNAME", FORWARDED.as_bytes()]));
    out.extend(encode(cmd));
    let writer = reader.get_mut();
    writer
        .write_all(&out)
        .and_then(|()| writer.flush())
        .map_err(|e| e.to_string())?;
    for step in 0..steps + 2 {
        match read_reply(&mut reader)? {
            Ok(()) => {}
            Err(e) if step == steps + 1 && already_done(&e) => {}
            // Taken for a master in this node's view, it has just become a
            // replica: its master gives it the definition.
            Err(e) if step == steps + 1 && e.starts_with("READONLY") => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_repeat_is_not_a_failure() {
        assert!(should_forward(b"+OK\r\n"));
        assert!(should_forward(b":2\r\n"));
        assert!(should_forward(b"-Index already exists\r\n"));
        assert!(should_forward(b"-Unknown Index name\r\n"));
        assert!(!should_forward(b"-ERR syntax error\r\n"));
    }

    #[test]
    fn replies_of_every_shape_are_read_whole() {
        let mut r = std::io::Cursor::new(b"*2\r\n$3\r\nabc\r\n:1\r\n-ERR no\r\n+OK\r\n".to_vec());
        assert_eq!(read_reply(&mut r).unwrap(), Ok(()));
        assert_eq!(read_reply(&mut r).unwrap(), Err("ERR no".to_string()));
        assert_eq!(read_reply(&mut r).unwrap(), Ok(()));
    }
}
