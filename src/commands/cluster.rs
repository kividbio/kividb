// src/commands/cluster.rs
//
// CLUSTER subcommands. Step 2 implements the read-only surface plus
// CLUSTER MYID. Mutating subcommands (ADDSLOTS, MEET, FORGET, SETSLOT,
// etc.) come in Step 4 and beyond.

use crate::cluster::{
    CLUSTER_SLOTS, ClusterState, MigrationState, NodeFlags, NodeId, key_hash_slot, node_id_str,
};
use crate::commands::ctx::Ctx;
use crate::resp::write::*;
use std::sync::Arc;

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;

    match name {
        "CLUSTER" => {
            if cmd.len() < 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'cluster' command",
                ));
            }
            let sub = String::from_utf8_lossy(cmd[1]).to_ascii_uppercase();
            Some(handle_cluster(ctx, &sub))
        }
        "ASKING" => Some(cmd_asking(ctx)),
        "READONLY" => Some(cmd_readonly(ctx)),
        "READWRITE" => Some(cmd_readwrite(ctx)),
        _ => None,
    }
}

fn handle_cluster(ctx: &mut Ctx, sub: &str) -> Vec<u8> {
    let state: &Arc<ClusterState> = ctx.cluster;

    // Subcommands that work even when cluster is disabled.
    match sub {
        // Read-only — work in both standalone and cluster modes
        "INFO" => return cmd_info(state),
        "MYID" => return cmd_myid(state),
        "KEYSLOT" => return cmd_keyslot(&ctx.cmd),
        "COUNTKEYSINSLOT" => return cmd_countkeysinslot(ctx),
        "GETKEYSINSLOT" => return cmd_getkeysinslot(ctx),
        "NODES" => return cmd_nodes(state),
        "SLOTS" => return cmd_slots(state),
        "SHARDS" => return cmd_shards(state),

        // Mutating — require cluster mode (each handler checks via state.enabled)
        "ADDSLOTS" => return cmd_addslots(state, &ctx.cmd),
        "DELSLOTS" => return cmd_delslots(state, &ctx.cmd),
        "ADDSLOTSRANGE" => return cmd_addslotsrange(state, &ctx.cmd),
        "DELSLOTSRANGE" => return cmd_delslotsrange(state, &ctx.cmd),
        "FLUSHSLOTS" => return cmd_flushslots(state),
        "BUMPEPOCH" => return cmd_bumpepoch(state),
        "SET-CONFIG-EPOCH" => return cmd_set_config_epoch(state, &ctx.cmd),
        "SAVECONFIG" => return cmd_saveconfig(state),

        _ => {}
    }

    // Anything past this point requires cluster mode for now.
    // (ADDSLOTS / SETSLOT / MEET come in later steps; until they exist we
    // return the same error redis-cli expects, instead of "unknown".)
    if !state.enabled {
        return resp_error("ERR This instance has cluster support disabled");
    }

    match sub {
        "MEET" => cmd_meet(ctx),
        "FORGET" => cmd_forget(ctx),
        "COUNT-FAILURE-REPORTS" => cmd_count_failure_reports(ctx),
        "LINKS" => cmd_links(ctx),
        "REPLICAS" | "SLAVES" => cmd_replicas(ctx),

        "REPLICATE" => cmd_replicate(ctx),
        "FAILOVER" => cmd_failover(ctx),
        "SETSLOT" => cmd_setslot(ctx),

        "RESET" => cmd_reset(ctx),
        "MYSHARDID" => cmd_myshardid(ctx),
        "SLOT-STATS" => cmd_slot_stats(ctx),

        "MIGRATION" => cmd_migration(ctx),
        // Node-to-node half of CLUSTER MIGRATION; not for clients.
        "SYNCSLOTS" => cmd_syncslots(ctx),
        other => resp_error(&format!("ERR Unknown CLUSTER subcommand '{other}'")),
    }
}

// ── CLUSTER MYID ─────────────────────────────────────────────────────────────

fn cmd_myid(state: &ClusterState) -> Vec<u8> {
    resp_bulk_str(node_id_str(&state.myself()))
}

// ── CLUSTER INFO ─────────────────────────────────────────────────────────────
//
// Returns a bulk string with key:value lines, separated by \r\n.
// Same format Redis uses, so redis-cli prints it cleanly.

fn cmd_info(state: &ClusterState) -> Vec<u8> {
    // Refresh first: this is what operators and orchestration poll, and it
    // should never trail a change they just made.
    state.refresh_state();
    let cluster_enabled = if state.enabled { 1 } else { 0 };
    let cluster_state = state.cluster_state_str();
    // Slot counts by the health of each slot's owner, and the cluster's size
    // as Redis defines it: masters that own at least one slot. These used to
    // report every assigned slot as ok and every master as part of the size.
    let (slots_assigned, slots_ok, slots_pfail, slots_fail) = state.slot_health();
    let size = state.voting_master_count();
    let current_epoch = state.current_epoch();
    let nodes = state.nodes.read();
    let known_nodes = nodes.len();

    let mut out = String::with_capacity(512);
    out.push_str(&format!("cluster_enabled:{cluster_enabled}\r\n"));
    out.push_str(&format!("cluster_state:{cluster_state}\r\n"));
    out.push_str(&format!("cluster_slots_assigned:{slots_assigned}\r\n"));
    out.push_str(&format!("cluster_slots_ok:{slots_ok}\r\n"));
    out.push_str(&format!("cluster_slots_pfail:{slots_pfail}\r\n"));
    out.push_str(&format!("cluster_slots_fail:{slots_fail}\r\n"));
    out.push_str(&format!("cluster_known_nodes:{known_nodes}\r\n"));
    out.push_str(&format!("cluster_size:{size}\r\n"));
    out.push_str(&format!("cluster_current_epoch:{current_epoch}\r\n"));
    out.push_str(&format!(
        "cluster_my_epoch:{}\r\n",
        nodes
            .get(&state.myself())
            .map(|n| n.config_epoch)
            .unwrap_or(0)
    ));
    out.push_str(&format!(
        "cluster_stats_messages_sent:{}\r\n",
        state
            .stats_messages_sent
            .load(std::sync::atomic::Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "cluster_stats_messages_received:{}\r\n",
        state
            .stats_messages_received
            .load(std::sync::atomic::Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "cluster_stats_bus_connects:{}\r\n",
        state
            .stats_bus_connects
            .load(std::sync::atomic::Ordering::Relaxed)
    ));
    out.push_str("total_cluster_links_buffer_limit_exceeded:0\r\n");

    resp_bulk_str(&out)
}

// ── CLUSTER KEYSLOT ──────────────────────────────────────────────────────────

fn cmd_keyslot(cmd: &[&[u8]]) -> Vec<u8> {
    if cmd.len() != 3 {
        return resp_error("ERR wrong number of arguments for 'cluster|keyslot' command");
    }
    let slot = key_hash_slot(cmd[2]);
    resp_integer(slot as i64)
}

// ── CLUSTER COUNTKEYSINSLOT ──────────────────────────────────────────────────
//
// Walk the keyspace of the currently selected DB and count keys hashing to
// the given slot. O(N) over the DB. Cluster mode normally restricts the
// keyspace to the slots we own, but in standalone mode this still works
// and returns a count for any slot — useful for testing.

fn cmd_countkeysinslot(ctx: &Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 3 {
        return resp_error("ERR wrong number of arguments for 'cluster|countkeysinslot' command");
    }
    let slot: u16 = match parse_slot_arg(ctx.cmd[2]) {
        Ok(s) => s,
        Err(e) => return e,
    };

    let db = &ctx.store[*ctx.selected_db];
    let mut count: i64 = 0;
    for shard in db.iter() {
        let map = crate::read_lock!(shard);
        for k in map.keys() {
            if key_hash_slot(k.as_bytes()) == slot {
                count += 1;
            }
        }
    }
    resp_integer(count)
}

// ── CLUSTER GETKEYSINSLOT ────────────────────────────────────────────────────

fn cmd_getkeysinslot(ctx: &Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 4 {
        return resp_error("ERR wrong number of arguments for 'cluster|getkeysinslot' command");
    }
    let slot: u16 = match parse_slot_arg(ctx.cmd[2]) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let count: usize = match std::str::from_utf8(ctx.cmd[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_error("ERR value is not an integer or out of range"),
    };

    let db = &ctx.store[*ctx.selected_db];
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(count.min(64));
    'outer: for shard in db.iter() {
        let map = crate::read_lock!(shard);
        for k in map.keys() {
            if key_hash_slot(k.as_bytes()) == slot {
                out.push(resp_bulk_str(k));
                if out.len() >= count {
                    break 'outer;
                }
            }
        }
    }
    resp_array(out)
}

// ── CLUSTER NODES ────────────────────────────────────────────────────────────
//
// Bulk string with one line per node. Format must match Redis byte-for-byte
// or clients (redis-cli, redis-py cluster client, jedis) will misparse it.
// Reuse the same line format as nodes.conf — that's by design in Redis too.

fn cmd_nodes(state: &ClusterState) -> Vec<u8> {
    use std::fmt::Write;

    let nodes = state.nodes.read();
    let slots = state.slots.read();
    let migrations = state.migrations.read();
    let myself = state.myself();

    let mut out = String::with_capacity(256 * nodes.len().max(1));

    let mut sorted: Vec<&crate::cluster::ClusterNode> = nodes.values().collect();
    sorted.sort_by_key(|n| n.id);

    for node in sorted {
        let _ = write!(
            out,
            "{} {}:{}@{}{}",
            node_id_str(&node.id),
            node.ip,
            node.client_facing_port(),
            node.cluster_port,
            crate::cluster::persist::hostname_suffix(&node.hostname)
        );
        out.push(' ');
        out.push_str(&format_flags_for_nodes(node.flags));
        out.push(' ');
        match node.master_id {
            Some(m) => out.push_str(node_id_str(&m)),
            None => out.push('-'),
        }
        let _ = write!(
            out,
            " {} {} {} {}",
            node.ping_sent_ms,
            node.pong_received_ms,
            node.config_epoch,
            link_state(node)
        );

        if node.flags.contains(NodeFlags::MASTER) {
            for (a, b) in compute_slot_ranges(&slots, node.id) {
                if a == b {
                    let _ = write!(out, " {a}");
                } else {
                    let _ = write!(out, " {a}-{b}");
                }
            }
        }
        // As Redis, on this node's own line: the slots it is moving out
        // (`[slot->-dest]`) or in (`[slot-<-src]`). redis-cli --cluster check
        // and fix find open slots by them.
        if node.id == myself {
            let mut open: Vec<(u16, MigrationState)> =
                migrations.iter().map(|(slot, m)| (*slot, *m)).collect();
            open.sort_by_key(|(slot, _)| *slot);
            for (slot, migration) in open {
                let _ = match migration {
                    MigrationState::Migrating { dest_id } => {
                        write!(out, " [{slot}->-{}]", node_id_str(&dest_id))
                    }
                    MigrationState::Importing { src_id } => {
                        write!(out, " [{slot}-<-{}]", node_id_str(&src_id))
                    }
                };
            }
        }
        out.push('\n');
    }

    resp_bulk_str(&out)
}

// ── CLUSTER SLOTS ────────────────────────────────────────────────────────────
//
// Returns an array of [start, end, [master_ip, master_port, master_id], ...].
// One element per contiguous range of slots owned by a single master.

fn cmd_slots(state: &ClusterState) -> Vec<u8> {
    let nodes = state.nodes.read();
    let slots = state.slots.read();

    // Build one (start, end, owner_id) entry per contiguous range.
    let mut ranges: Vec<(usize, usize, NodeId)> = Vec::new();
    let mut start: Option<(usize, NodeId)> = None;
    for i in 0..CLUSTER_SLOTS {
        match (slots[i], start) {
            (Some(owner), None) => start = Some((i, owner)),
            (Some(owner), Some((s, prev_owner))) if owner != prev_owner => {
                ranges.push((s, i - 1, prev_owner));
                start = Some((i, owner));
            }
            (None, Some((s, prev_owner))) => {
                ranges.push((s, i - 1, prev_owner));
                start = None;
            }
            _ => {}
        }
    }
    if let Some((s, owner)) = start {
        ranges.push((s, CLUSTER_SLOTS - 1, owner));
    }

    let mut entries: Vec<Vec<u8>> = Vec::with_capacity(ranges.len());
    for (a, b, owner_id) in ranges {
        let owner = match nodes.get(&owner_id) {
            Some(n) => n,
            None => continue,
        };
        let mut entry = vec![
            resp_integer(a as i64),
            resp_integer(b as i64),
            slots_node_entry(state, owner),
        ];
        // Append replicas of this master.
        for replica in nodes
            .values()
            .filter(|n| n.flags.contains(NodeFlags::REPLICA) && n.master_id == Some(owner_id))
        {
            entry.push(slots_node_entry(state, replica));
        }
        entries.push(resp_array(entry));
    }

    resp_array(entries)
}

/// One node in a CLUSTER SLOTS range, as Redis 7 gives it: the endpoint
/// `cluster-preferred-endpoint-type` asks for (null for `unknown-endpoint`),
/// port, id, and then what the endpoint does not already say — the address
/// in place of a hostname, the hostname in place of an address. That last
/// part is left out when there is nothing in it, which is the reply before
/// Redis 7 and what every client reads.
fn slots_node_entry(state: &ClusterState, node: &crate::cluster::ClusterNode) -> Vec<u8> {
    use crate::cluster::Endpoint;
    let preferred = state.preferred_endpoint();
    let mut fields = vec![
        match preferred {
            Endpoint::Unknown => resp_null_bulk(),
            _ => resp_bulk_str(&state.endpoint_of(node)),
        },
        resp_integer(node.client_facing_port() as i64),
        resp_bulk_str(node_id_str(&node.id)),
    ];
    let mut metadata: Vec<Vec<u8>> = Vec::new();
    if preferred != Endpoint::Ip {
        metadata.push(resp_bulk_str("ip"));
        metadata.push(resp_bulk_str(&node.ip));
    }
    if preferred != Endpoint::Hostname && !node.hostname.is_empty() {
        metadata.push(resp_bulk_str("hostname"));
        metadata.push(resp_bulk_str(&node.hostname));
    }
    if !metadata.is_empty() {
        fields.push(resp_array(metadata));
    }
    resp_array(fields)
}

// ── CLUSTER SHARDS ───────────────────────────────────────────────────────────
//
// Redis 7+. Groups nodes by shard (master + its replicas) and returns the
// slot ranges they cover. Each shard is a map: { slots: [...], nodes: [...] }.

fn cmd_shards(state: &ClusterState) -> Vec<u8> {
    let nodes = state.nodes.read();
    let slots = state.slots.read();

    let mut shards: Vec<Vec<u8>> = Vec::new();
    for master in nodes
        .values()
        .filter(|n| n.flags.contains(NodeFlags::MASTER))
    {
        let mut slot_pairs: Vec<Vec<u8>> = Vec::new();
        for (a, b) in compute_slot_ranges(&slots, master.id) {
            slot_pairs.push(resp_integer(a as i64));
            slot_pairs.push(resp_integer(b as i64));
        }

        let mut node_descs: Vec<Vec<u8>> = Vec::new();
        node_descs.push(node_description(state, master));
        for replica in nodes
            .values()
            .filter(|n| n.flags.contains(NodeFlags::REPLICA) && n.master_id == Some(master.id))
        {
            node_descs.push(node_description(state, replica));
        }

        shards.push(resp_array(vec![
            resp_bulk_str("slots"),
            resp_array(slot_pairs),
            resp_bulk_str("nodes"),
            resp_array(node_descs),
        ]));
    }

    resp_array(shards)
}

/// The link-state column of `CLUSTER NODES`: whether we can currently reach
/// the node. Our own row is always `connected`.
fn link_state(node: &crate::cluster::ClusterNode) -> &'static str {
    if !node.flags.contains(NodeFlags::MYSELF)
        && node
            .flags
            .intersects(NodeFlags::PFAIL | NodeFlags::FAIL | NodeFlags::HANDSHAKE)
    {
        "disconnected"
    } else {
        "connected"
    }
}

fn node_description(state: &ClusterState, node: &crate::cluster::ClusterNode) -> Vec<u8> {
    let role = if node.flags.contains(NodeFlags::MASTER) {
        "master"
    } else if node.flags.contains(NodeFlags::REPLICA) {
        "replica"
    } else {
        "unknown"
    };
    // `loading` means a node that is still syncing its dataset, which is not
    // something a suspected-unreachable node is known to be doing.
    let health = if node.flags.contains(NodeFlags::FAIL) {
        "fail"
    } else {
        "online"
    };
    let mut fields = vec![
        resp_bulk_str("id"),
        resp_bulk_str(node_id_str(&node.id)),
        resp_bulk_str("port"),
        resp_integer(node.client_facing_port() as i64),
        resp_bulk_str("ip"),
        resp_bulk_str(&node.ip),
        resp_bulk_str("endpoint"),
        resp_bulk_str(&state.endpoint_of(node)),
    ];
    if !node.hostname.is_empty() {
        fields.push(resp_bulk_str("hostname"));
        fields.push(resp_bulk_str(&node.hostname));
    }
    fields.extend([
        resp_bulk_str("role"),
        resp_bulk_str(role),
        resp_bulk_str("replication-offset"),
        resp_integer(0),
        resp_bulk_str("health"),
        resp_bulk_str(health),
    ]);
    resp_array(fields)
}

// ── Shared helpers ───────────────────────────────────────────────────────────

fn parse_slot_arg(arg: &[u8]) -> Result<u16, Vec<u8>> {
    let s = std::str::from_utf8(arg).map_err(|_| resp_error("ERR invalid slot"))?;
    let n: i64 = s
        .parse()
        .map_err(|_| resp_error("ERR value is not an integer or out of range"))?;
    if !(0..CLUSTER_SLOTS as i64).contains(&n) {
        return Err(resp_error(&format!(
            "ERR Invalid slot {n}, must be between 0 and {}",
            CLUSTER_SLOTS - 1
        )));
    }
    Ok(n as u16)
}

fn format_flags_for_nodes(f: NodeFlags) -> String {
    let mut parts: Vec<&str> = Vec::new();
    if f.contains(NodeFlags::MYSELF) {
        parts.push("myself");
    }
    if f.contains(NodeFlags::MASTER) {
        parts.push("master");
    }
    if f.contains(NodeFlags::REPLICA) {
        parts.push("slave");
    }
    if f.contains(NodeFlags::PFAIL) {
        parts.push("fail?");
    }
    if f.contains(NodeFlags::FAIL) {
        parts.push("fail");
    }
    if f.contains(NodeFlags::HANDSHAKE) {
        parts.push("handshake");
    }
    if f.contains(NodeFlags::NOADDR) {
        parts.push("noaddr");
    }
    if parts.is_empty() {
        return "noflags".into();
    }
    parts.join(",")
}

fn compute_slot_ranges(
    slots: &[Option<NodeId>; CLUSTER_SLOTS],
    owner: NodeId,
) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut start: Option<usize> = None;
    for (i, slot) in slots.iter().enumerate().take(CLUSTER_SLOTS) {
        let owned = matches!(*slot, Some(o) if o == owner);
        match (start, owned) {
            (None, true) => start = Some(i),
            (Some(s), false) => {
                ranges.push((s, i - 1));
                start = None;
            }
            _ => {}
        }
    }
    if let Some(s) = start {
        ranges.push((s, CLUSTER_SLOTS - 1));
    }
    ranges
}

// ── Mutating subcommands (Step 3) ─────────────────────────────────────────────
//
// All of these:
//   1. Require cluster mode (state.enabled). The release_/claim_ helpers
//      check this themselves; we still need an explicit check for the
//      argument-only commands (FLUSHSLOTS, BUMPEPOCH, SAVECONFIG).
//   2. Persist to nodes.conf on success. A failed mutation does not
//      trigger a save.
//   3. Return Redis-compatible error strings. The format matters because
//      cluster orchestrators (redis-cli --cluster, redis-trib) parse them.

fn require_cluster_mode(state: &ClusterState) -> Option<Vec<u8>> {
    if !state.enabled {
        Some(resp_error("ERR This instance has cluster support disabled"))
    } else {
        None
    }
}

/// CLUSTER ADDSLOTS slot [slot ...]
fn cmd_addslots(state: &ClusterState, cmd: &[&[u8]]) -> Vec<u8> {
    if let Some(err) = require_cluster_mode(state) {
        return err;
    }
    if cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'cluster|addslots' command");
    }

    // Parse all slots first, before mutating anything. Same pattern as the
    // range mutators: validate-then-apply so we don't leave half-claimed
    // state on a parse error.
    let mut slots: Vec<u16> = Vec::with_capacity(cmd.len() - 2);
    for arg in &cmd[2..] {
        match parse_slot_arg(arg) {
            Ok(s) => slots.push(s),
            Err(e) => return e,
        }
    }
    // Reject duplicates inside the same call. Real Redis returns this error.
    {
        let mut seen = std::collections::HashSet::with_capacity(slots.len());
        for &s in &slots {
            if !seen.insert(s) {
                return resp_error(&format!("ERR Slot {s} specified multiple times"));
            }
        }
    }
    for s in &slots {
        if let Err(e) = state.claim_slot(*s) {
            return resp_error(&format!("ERR {e}"));
        }
    }
    if let Err(e) = state.save_to_disk() {
        return resp_error(&format!(
            "ERR ADDSLOTS persisted in memory but disk save failed: {e}"
        ));
    }
    resp_simple("OK")
}

/// CLUSTER DELSLOTS slot [slot ...]
fn cmd_delslots(state: &ClusterState, cmd: &[&[u8]]) -> Vec<u8> {
    if let Some(err) = require_cluster_mode(state) {
        return err;
    }
    if cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'cluster|delslots' command");
    }
    let mut slots: Vec<u16> = Vec::with_capacity(cmd.len() - 2);
    for arg in &cmd[2..] {
        match parse_slot_arg(arg) {
            Ok(s) => slots.push(s),
            Err(e) => return e,
        }
    }
    {
        let mut seen = std::collections::HashSet::with_capacity(slots.len());
        for &s in &slots {
            if !seen.insert(s) {
                return resp_error(&format!("ERR Slot {s} specified multiple times"));
            }
        }
    }
    for s in &slots {
        if let Err(e) = state.release_slot(*s) {
            return resp_error(&format!("ERR {e}"));
        }
    }
    if let Err(e) = state.save_to_disk() {
        return resp_error(&format!(
            "ERR DELSLOTS persisted in memory but disk save failed: {e}"
        ));
    }
    resp_simple("OK")
}

/// CLUSTER ADDSLOTSRANGE start-slot end-slot [start-slot end-slot ...]
fn cmd_addslotsrange(state: &ClusterState, cmd: &[&[u8]]) -> Vec<u8> {
    if let Some(err) = require_cluster_mode(state) {
        return err;
    }
    // Need at least: CLUSTER ADDSLOTSRANGE start end -> 4 args
    if cmd.len() < 4 || !(cmd.len() - 2).is_multiple_of(2) {
        return resp_error("ERR wrong number of arguments for 'cluster|addslotsrange' command");
    }
    // Parse all (start, end) pairs first.
    let mut ranges: Vec<(u16, u16)> = Vec::new();
    let mut i = 2;
    while i + 1 < cmd.len() {
        let start = match parse_slot_arg(cmd[i]) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let end = match parse_slot_arg(cmd[i + 1]) {
            Ok(s) => s,
            Err(e) => return e,
        };
        if start > end {
            return resp_error(&format!("ERR Invalid range {start}-{end}"));
        }
        ranges.push((start, end));
        i += 2;
    }

    // Apply each range. claim_slot_range is itself atomic per-range; if a
    // later range fails, earlier ones stay claimed. That matches Redis's
    // observable behavior.
    for (start, end) in ranges {
        if let Err(e) = state.claim_slot_range(start, end) {
            return resp_error(&format!("ERR {e}"));
        }
    }
    if let Err(e) = state.save_to_disk() {
        return resp_error(&format!(
            "ERR ADDSLOTSRANGE persisted in memory but disk save failed: {e}"
        ));
    }
    resp_simple("OK")
}

/// CLUSTER DELSLOTSRANGE start-slot end-slot [start-slot end-slot ...]
fn cmd_delslotsrange(state: &ClusterState, cmd: &[&[u8]]) -> Vec<u8> {
    if let Some(err) = require_cluster_mode(state) {
        return err;
    }
    if cmd.len() < 4 || !(cmd.len() - 2).is_multiple_of(2) {
        return resp_error("ERR wrong number of arguments for 'cluster|delslotsrange' command");
    }
    let mut ranges: Vec<(u16, u16)> = Vec::new();
    let mut i = 2;
    while i + 1 < cmd.len() {
        let start = match parse_slot_arg(cmd[i]) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let end = match parse_slot_arg(cmd[i + 1]) {
            Ok(s) => s,
            Err(e) => return e,
        };
        if start > end {
            return resp_error(&format!("ERR Invalid range {start}-{end}"));
        }
        ranges.push((start, end));
        i += 2;
    }
    for (start, end) in ranges {
        if let Err(e) = state.release_slot_range(start, end) {
            return resp_error(&format!("ERR {e}"));
        }
    }
    if let Err(e) = state.save_to_disk() {
        return resp_error(&format!(
            "ERR DELSLOTSRANGE persisted in memory but disk save failed: {e}"
        ));
    }
    resp_simple("OK")
}

/// CLUSTER FLUSHSLOTS — clear all our slot assignments.
fn cmd_flushslots(state: &ClusterState) -> Vec<u8> {
    if let Some(err) = require_cluster_mode(state) {
        return err;
    }
    state.flush_slots();
    if let Err(e) = state.save_to_disk() {
        return resp_error(&format!(
            "ERR FLUSHSLOTS persisted in memory but disk save failed: {e}"
        ));
    }
    resp_simple("OK")
}

/// CLUSTER BUMPEPOCH — increment current_epoch and return the new value.
/// Real Redis only bumps if it's actually higher than known epochs in the
/// cluster; with no gossip yet, ours always succeeds. Returns "BUMPED <n>"
/// or "STILL <n>" — matching Redis's wire format exactly so cluster scripts
/// that parse this string keep working.
fn cmd_bumpepoch(state: &ClusterState) -> Vec<u8> {
    if let Some(err) = require_cluster_mode(state) {
        return err;
    }
    let new = state.bump_epoch();
    // Save to disk so the bump survives restart.
    if let Err(e) = state.save_to_disk() {
        return resp_error(&format!(
            "ERR BUMPEPOCH persisted in memory but disk save failed: {e}"
        ));
    }
    resp_simple(&format!("BUMPED {new}"))
}

/// CLUSTER SET-CONFIG-EPOCH <epoch>
/// Only valid when our current config_epoch is 0 (i.e. fresh node).
fn cmd_set_config_epoch(state: &ClusterState, cmd: &[&[u8]]) -> Vec<u8> {
    if let Some(err) = require_cluster_mode(state) {
        return err;
    }
    if cmd.len() != 3 {
        return resp_error("ERR wrong number of arguments for 'cluster|set-config-epoch' command");
    }
    let new_epoch: u64 = match std::str::from_utf8(cmd[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_error("ERR value is not an integer or out of range"),
    };
    if let Err(e) = state.set_my_config_epoch(new_epoch) {
        return resp_error(&format!("ERR {e}"));
    }
    if let Err(e) = state.save_to_disk() {
        return resp_error(&format!(
            "ERR SET-CONFIG-EPOCH persisted in memory but disk save failed: {e}"
        ));
    }
    resp_simple("OK")
}

/// CLUSTER SAVECONFIG — force a write of nodes.conf.
fn cmd_saveconfig(state: &ClusterState) -> Vec<u8> {
    if let Some(err) = require_cluster_mode(state) {
        return err;
    }
    match state.save_to_disk() {
        Ok(()) => resp_simple("OK"),
        Err(e) => resp_error(&format!("ERR {e}")),
    }
}

// ── CLUSTER MEET ip port [cluster-bus-port] ───────────────────────────────────

fn cmd_meet(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if let Some(err) = require_cluster_mode(ctx.cluster) {
        return err;
    }
    if cmd.len() < 4 || cmd.len() > 5 {
        return resp_error("ERR wrong number of arguments for 'cluster|meet' command");
    }
    let ip = match std::str::from_utf8(cmd[2]) {
        Ok(s) => s.to_string(),
        Err(_) => return resp_error("ERR invalid IP"),
    };
    let client_port: u16 = match std::str::from_utf8(cmd[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(p) => p,
        None => return resp_error("ERR invalid port"),
    };
    let bus_port: u16 = if cmd.len() == 5 {
        match std::str::from_utf8(cmd[4])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(p) => p,
            None => return resp_error("ERR invalid cluster bus port"),
        }
    } else {
        client_port + crate::cluster::bus::BUS_PORT_OFFSET
    };

    let Some(bus) = ctx.cluster.bus_runtime.get() else {
        return resp_error("ERR the cluster bus is not running");
    };

    // Insert a HANDSHAKE placeholder so CLUSTER NODES immediately reflects
    // the in-flight handshake even before the PONG comes back.
    let placed =
        crate::cluster::bus::dialer::insert_handshake_placeholder(ctx.cluster, &ip, client_port);

    // Fire off the actual handshake. We don't await it from the command
    // handler — the command returns OK as soon as the request is queued,
    // matching Redis's behavior.
    let state = std::sync::Arc::clone(ctx.cluster);
    bus.spawn(async move {
        if let Err(e) =
            crate::cluster::bus::dialer::send_meet(Arc::clone(&state), ip.clone(), bus_port).await
        {
            log_warn!("Cluster: MEET {ip}:{client_port} failed: {e}");
            // Nobody answered; do not leave a handshake row behind.
            if placed {
                crate::cluster::bus::dialer::remove_handshake_placeholder(&state, &ip, client_port);
            }
        }
    });

    resp_simple("OK")
}

// ── CLUSTER FORGET node-id ────────────────────────────────────────────────────

fn cmd_forget(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if let Some(err) = require_cluster_mode(ctx.cluster) {
        return err;
    }
    if cmd.len() != 3 {
        return resp_error("ERR wrong number of arguments for 'cluster|forget' command");
    }
    let id = match crate::cluster::node::parse_node_id(cmd[2]) {
        Some(id) => id,
        None => return resp_error("ERR Invalid node id"),
    };
    if id == ctx.cluster.myself() {
        return resp_error("ERR I tried hard but I can't forget myself...");
    }
    if ctx.cluster.my_master_id() == Some(id) {
        return resp_error("ERR Can't forget my master!");
    }

    // Removes the node, releases its slots, and bans it from being re-added
    // by gossip for a minute — long enough to send FORGET to every node.
    if !ctx.cluster.forget_node(id) {
        return resp_error("ERR Unknown node");
    }
    if let Err(e) = ctx.cluster.save_to_disk() {
        return resp_error(&format!(
            "ERR FORGET applied in memory but disk save failed: {e}"
        ));
    }
    resp_simple("OK")
}

// ── CLUSTER COUNT-FAILURE-REPORTS node-id ─────────────────────────────────────

fn cmd_count_failure_reports(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if let Some(err) = require_cluster_mode(ctx.cluster) {
        return err;
    }
    if cmd.len() != 3 {
        return resp_error(
            "ERR wrong number of arguments for 'cluster|count-failure-reports' command",
        );
    }
    let id = match crate::cluster::node::parse_node_id(cmd[2]) {
        Some(id) => id,
        None => return resp_error("ERR Invalid node id"),
    };
    let count = ctx.cluster.failure_report_count(id);
    resp_integer(count as i64)
}

// ── CLUSTER LINKS ─────────────────────────────────────────────────────────────
//
// Returns one entry per peer connection. We don't yet track real bus link
// state (each PING is a fresh TCP connect — we'd need a connection cache to
// expose link uptime properly), so we report what we know: peer id, address,
// last ping/pong timestamps. This is enough for cluster monitoring tools.

fn cmd_links(ctx: &mut Ctx) -> Vec<u8> {
    if let Some(err) = require_cluster_mode(ctx.cluster) {
        return err;
    }
    let nodes = ctx.cluster.nodes.read();
    let mut out: Vec<Vec<u8>> = Vec::new();

    for n in nodes.values() {
        if n.id == ctx.cluster.myself() || n.flags.contains(crate::cluster::NodeFlags::HANDSHAKE) {
            continue;
        }
        // We expose two "links" per peer (Redis convention: one for-receiving,
        // one for-sending) but ours are the same — each PING uses a fresh
        // connection. Report a single direction.
        let entry = resp_array(vec![
            resp_bulk_str("direction"),
            resp_bulk_str("to"),
            resp_bulk_str("node"),
            resp_bulk_str(crate::cluster::node_id_str(&n.id)),
            resp_bulk_str("create-time"),
            resp_integer(n.ping_sent_ms as i64),
            resp_bulk_str("events"),
            resp_bulk_str("rw"),
            resp_bulk_str("send-buffer-allocated"),
            resp_integer(0),
            resp_bulk_str("send-buffer-used"),
            resp_integer(0),
        ]);
        out.push(entry);
    }
    resp_array(out)
}

// ── CLUSTER REPLICAS / SLAVES node-id ─────────────────────────────────────────

fn cmd_replicas(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if let Some(err) = require_cluster_mode(ctx.cluster) {
        return err;
    }
    if cmd.len() != 3 {
        return resp_error("ERR wrong number of arguments");
    }
    let master_id = match crate::cluster::node::parse_node_id(cmd[2]) {
        Some(id) => id,
        None => return resp_error("ERR Invalid node id"),
    };

    let nodes = ctx.cluster.nodes.read();
    if !nodes.contains_key(&master_id) {
        return resp_error("ERR Unknown node");
    }

    // An array, as Redis replies: one bulk string per replica, in the format
    // of a `CLUSTER NODES` line. It was one string of the lines (#138).
    let mut out = Vec::new();
    for n in nodes.values() {
        if n.flags.contains(crate::cluster::NodeFlags::REPLICA) && n.master_id == Some(master_id) {
            out.push(resp_bulk_str(&format!(
                "{} {}:{}@{}{} {} {} {} {} {} connected",
                crate::cluster::node_id_str(&n.id),
                n.ip,
                n.client_facing_port(),
                n.cluster_port,
                crate::cluster::persist::hostname_suffix(&n.hostname),
                format_flags_for_nodes(n.flags),
                crate::cluster::node_id_str(&master_id),
                n.ping_sent_ms,
                n.pong_received_ms,
                n.config_epoch,
            )));
        }
    }
    resp_array(out)
}

// ── CLUSTER REPLICATE node-id ────────────────────────────────────────────────
//
// Configure this node as a replica of the given master. Internally this is
// equivalent to REPLICAOF <master_ip> <master_port>, plus updating our own
// cluster node entry to set the REPLICA flag and master_id field.
//
// We reuse the existing replication client machinery — kicking it off via
// the same code path REPLICAOF uses ensures the handshake/RDB-sync/stream
// flow is identical between standalone and cluster replication.

fn cmd_replicate(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if let Some(err) = require_cluster_mode(ctx.cluster) {
        return err;
    }
    if cmd.len() != 3 {
        return resp_error("ERR wrong number of arguments for 'cluster|replicate' command");
    }
    let master_id = match crate::cluster::node::parse_node_id(cmd[2]) {
        Some(id) => id,
        None => return resp_error("ERR Invalid node id"),
    };

    // Update cluster topology — returns the master's address so we can
    // hand it to the replication client.
    let (master_ip, master_port) = match ctx.cluster.set_replica_of(master_id) {
        Ok(v) => v,
        Err(e) => return resp_error(&format!("ERR {e}")),
    };

    // Persist nodes.conf with the new replica state.
    if let Err(e) = ctx.cluster.save_to_disk() {
        return resp_error(&format!(
            "ERR REPLICATE applied in memory but disk save failed: {e}"
        ));
    }

    // Start (or restart) the replication client — same path REPLICAOF uses.
    // It flips the ReplState role synchronously, so INFO replication agrees
    // with the cluster view before this command returns.
    crate::replication::replica_client::ReplicaStarter::from_ctx(ctx).start(master_ip, master_port);

    resp_simple("OK")
}

// ── CLUSTER FAILOVER [FORCE | TAKEOVER] ──────────────────────────────────────
//
// Manually trigger a failover from a replica. Three modes:
//   default   — ask the master to pause writes, catch up to it, then win
//               an election; nothing acknowledged is lost
//   FORCE     — skip the handshake (the master may be unreachable) but still
//               win an election
//   TAKEOVER  — skip the election too (dangerous; bypasses quorum)
//
// This must run on a REPLICA; master receiving it returns an error.

fn cmd_failover(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if let Some(err) = require_cluster_mode(ctx.cluster) {
        return err;
    }
    let mode = if cmd.len() == 2 {
        "default"
    } else if cmd.len() == 3 {
        match std::str::from_utf8(cmd[2]) {
            Ok(s) => match s.to_ascii_uppercase().as_str() {
                "FORCE" => "force",
                "TAKEOVER" => "takeover",
                _ => return resp_error("ERR syntax: CLUSTER FAILOVER [FORCE | TAKEOVER]"),
            },
            Err(_) => return resp_error("ERR invalid argument"),
        }
    } else {
        return resp_error("ERR wrong number of arguments for 'cluster|failover' command");
    };

    // Must be a replica with a known master.
    let master_id = match ctx.cluster.my_master_id() {
        Some(id) => id,
        None => return resp_error("ERR You should send CLUSTER FAILOVER to a replica"),
    };
    if !ctx.cluster.nodes.read().contains_key(&master_id) {
        return resp_error("ERR Master is unknown. Please use CLUSTER FAILOVER FORCE.");
    }
    let Some(bus) = ctx.cluster.bus_runtime.get() else {
        return resp_error("ERR the cluster bus is not running");
    };

    match mode {
        "takeover" => {
            // Bypass quorum entirely: bump epoch, claim slots, broadcast.
            let epoch = ctx.cluster.bump_epoch();
            ctx.cluster.promote_self(master_id, epoch);
            ctx.repl_state.become_master();
            log_info!("Cluster: Manual TAKEOVER promoted us to master at epoch {epoch}");
            // Announce it now, not on the next heartbeat.
            let state = Arc::clone(ctx.cluster);
            bus.spawn(async move {
                crate::cluster::bus::heartbeat::send_one_round(&state).await;
            });
            resp_simple("OK")
        }
        "force" => {
            // No handshake with the master — it may be unreachable — but still
            // a real election: the other masters have to agree.
            let state = Arc::clone(ctx.cluster);
            bus.spawn(async move {
                crate::cluster::bus::heartbeat::start_forced_election(&state, master_id, None)
                    .await;
            });
            resp_simple("OK")
        }
        _ => {
            // The safe form. The master is asked to stop taking writes and
            // tell us where its stream ends; we take over only once we have
            // read up to that point, so nothing acknowledged is lost. That
            // needs the master to be there to ask.
            if !ctx
                .repl_state
                .slave_link_up
                .load(std::sync::atomic::Ordering::Acquire)
            {
                return resp_error(
                    "ERR Master is down or failed, please use CLUSTER FAILOVER FORCE",
                );
            }
            let state = Arc::clone(ctx.cluster);
            bus.spawn(async move {
                if let Err(e) =
                    crate::cluster::bus::heartbeat::run_manual_failover(state, master_id).await
                {
                    log_warn!("Cluster: Manual failover aborted: {e}");
                }
            });
            resp_simple("OK")
        }
    }
}

// ── ASKING ───────────────────────────────────────────────────────────────────
//
// Set a per-connection single-shot flag: the next command bypasses normal
// slot-ownership and migration-state checks. Cleared automatically after
// the next command runs, whether it succeeded or not.

fn cmd_asking(ctx: &mut Ctx) -> Vec<u8> {
    ctx.client_registry.update(ctx.client_id, |info| {
        info.asking = true;
    });
    resp_simple("OK")
}

// ── READONLY / READWRITE ─────────────────────────────────────────────────────
//
// READONLY enables reads against a replica's slots (slots owned by our master).
// READWRITE flips it back. The flag persists until explicitly toggled.
//
// We don't *currently* serve replica reads — Step 7c-followup if you want
// that. The flag is plumbed; the routing layer can grow conditional logic
// later: "if asking_flag OR (readonly_flag AND slot owned by my master),
// accept the read."

fn cmd_readonly(ctx: &mut Ctx) -> Vec<u8> {
    ctx.client_registry.update(ctx.client_id, |info| {
        info.readonly = true;
    });
    resp_simple("OK")
}

fn cmd_readwrite(ctx: &mut Ctx) -> Vec<u8> {
    ctx.client_registry.update(ctx.client_id, |info| {
        info.readonly = false;
    });
    resp_simple("OK")
}

// ── CLUSTER SETSLOT slot <IMPORTING node-id | MIGRATING node-id | NODE node-id | STABLE> ──

fn cmd_setslot(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if let Some(err) = require_cluster_mode(ctx.cluster) {
        return err;
    }
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'cluster|setslot' command");
    }
    let slot: u16 = match parse_slot_arg(cmd[2]) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let action = match std::str::from_utf8(cmd[3]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return resp_error("ERR invalid action"),
    };

    match action.as_str() {
        "MIGRATING" => {
            if cmd.len() != 5 {
                return resp_error(
                    "ERR wrong number of arguments for 'cluster|setslot|migrating' command",
                );
            }
            let dest = match crate::cluster::node::parse_node_id(cmd[4]) {
                Some(id) => id,
                None => return resp_error("ERR Invalid node id"),
            };
            if let Err(e) = ctx.cluster.set_migrating(slot, dest) {
                return resp_error(&format!("ERR {e}"));
            }
            let _ = ctx.cluster.save_to_disk();
            resp_simple("OK")
        }
        "IMPORTING" => {
            if cmd.len() != 5 {
                return resp_error(
                    "ERR wrong number of arguments for 'cluster|setslot|importing' command",
                );
            }
            let src = match crate::cluster::node::parse_node_id(cmd[4]) {
                Some(id) => id,
                None => return resp_error("ERR Invalid node id"),
            };
            if let Err(e) = ctx.cluster.set_importing(slot, src) {
                return resp_error(&format!("ERR {e}"));
            }
            let _ = ctx.cluster.save_to_disk();
            resp_simple("OK")
        }
        "STABLE" => {
            if cmd.len() != 4 {
                return resp_error(
                    "ERR wrong number of arguments for 'cluster|setslot|stable' command",
                );
            }
            ctx.cluster.clear_migration(slot);
            let _ = ctx.cluster.save_to_disk();
            resp_simple("OK")
        }
        "NODE" => {
            if cmd.len() != 5 {
                return resp_error(
                    "ERR wrong number of arguments for 'cluster|setslot|node' command",
                );
            }
            let new_owner = match crate::cluster::node::parse_node_id(cmd[4]) {
                Some(id) => id,
                None => return resp_error("ERR Invalid node id"),
            };
            // Handing a slot to another node while keys from it are still here
            // strands them: clients are redirected away, and the keys sit
            // unreachable until the slot comes back. Finish the migration.
            if new_owner != ctx.cluster.myself()
                && ctx.cluster.owns_slot(slot)
                && slot_has_keys(ctx, slot)
            {
                return resp_error(&format!(
                    "ERR Can't assign hashslot {slot} to a different node while I still hold \
                     keys for this hash slot."
                ));
            }
            if let Err(e) = ctx.cluster.finalize_slot(slot, new_owner) {
                return resp_error(&format!("ERR {e}"));
            }
            let _ = ctx.cluster.save_to_disk();
            resp_simple("OK")
        }
        other => resp_error(&format!("ERR Unknown SETSLOT subcommand '{other}'")),
    }
}

// ── CLUSTER MIGRATION (Redis 8.4) ────────────────────────────────────────────
//
// CLUSTER MIGRATION IMPORT start-slot end-slot [start-slot end-slot ...]
// CLUSTER MIGRATION CANCEL <ID task-id | ALL>
// CLUSTER MIGRATION STATUS [ID task-id | ALL]
//
// Atomic slot migration: see `cluster::migration`.

fn cmd_migration(ctx: &mut Ctx) -> Vec<u8> {
    use crate::cluster::migration;
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'cluster|migration' command");
    }
    let registry = &ctx.cluster.slot_migrations;
    let action = String::from_utf8_lossy(cmd[2]).to_ascii_uppercase();
    // `ID <task-id>` or `ALL`, for CANCEL and STATUS.
    let selector = |args: &[&[u8]]| -> Result<Option<String>, Vec<u8>> {
        match args {
            [all] if all.eq_ignore_ascii_case(b"ALL") => Ok(None),
            [id, task] if id.eq_ignore_ascii_case(b"ID") => {
                Ok(Some(String::from_utf8_lossy(task).into_owned()))
            }
            _ => Err(resp_error("ERR syntax error")),
        }
    };

    match action.as_str() {
        "IMPORT" => {
            let ranges = match migration::parse_ranges(&cmd[3..]) {
                Ok(r) => r,
                Err(e) => return resp_error(&format!("ERR {e}")),
            };
            match migration::start_import(migration::Env::from_ctx(ctx), ranges) {
                Ok(id) => resp_bulk_str(&id),
                Err(e) => resp_error(&format!("ERR {e}")),
            }
        }
        "CANCEL" => match selector(&cmd[3..]) {
            Ok(id) => resp_integer(registry.cancel(id.as_deref()) as i64),
            Err(e) => e,
        },
        "STATUS" => {
            // With no argument, the same as ALL.
            let id = if cmd.len() == 3 {
                None
            } else {
                match selector(&cmd[3..]) {
                    Ok(id) => id,
                    Err(e) => return e,
                }
            };
            let resp = crate::commands::handler::effective_resp_for_reply(ctx);
            let text = |s: &str| resp_bulk_str(s);
            let number = |n: u64| resp_integer(n.min(i64::MAX as u64) as i64);
            let tasks: Vec<Vec<u8>> = registry
                .all()
                .iter()
                .filter(|task| id.as_deref().is_none_or(|id| task.id == id))
                .map(|task| {
                    resp_map_for(
                        resp,
                        vec![
                            text("id"),
                            text(&task.id),
                            text("slots"),
                            text(&migration::ranges_text(&task.ranges)),
                            text("source"),
                            text(node_id_str(&task.source)),
                            text("dest"),
                            text(node_id_str(&task.dest)),
                            text("operation"),
                            text(match task.operation {
                                migration::Operation::Import => "import",
                                migration::Operation::Migrate => "migrate",
                            }),
                            text("state"),
                            text(task.state.name()),
                            text("last_error"),
                            text(&task.last_error),
                            text("retries"),
                            number(task.retries),
                            text("create_time"),
                            number(task.create_time_ms),
                            text("start_time"),
                            number(task.start_time_ms),
                            text("end_time"),
                            number(task.end_time_ms),
                            text("write_pause_ms"),
                            number(task.write_pause_ms),
                            text("capture_pause_ms"),
                            number(task.capture_pause_ms),
                        ],
                    )
                })
                .collect();
            resp_array(tasks)
        }
        other => resp_error(&format!(
            "ERR unknown CLUSTER MIGRATION subcommand '{other}'"
        )),
    }
}

// CLUSTER SYNCSLOTS START <task-id> <dest-node-id> start end [start end ...]
//     destination → source: begin sending these slots to me
// CLUSTER SYNCSLOTS CANCEL <task-id>
//     destination → source: stop
// CLUSTER SYNCSLOTS APPLY <task-id> <command> [arg ...]
//     source → destination: a key, or a write, belonging to the slots
// CLUSTER SYNCSLOTS TAKEOVER <task-id>
//     source → destination: that was everything; the slots are yours
// CLUSTER SYNCSLOTS FAIL <task-id> <reason>
//     source → destination: I have given up on this attempt

fn cmd_syncslots(ctx: &mut Ctx) -> Vec<u8> {
    use crate::cluster::migration;
    if ctx.cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'cluster|syncslots' command");
    }
    let action = String::from_utf8_lossy(ctx.cmd[2]).to_ascii_uppercase();
    let task_id = String::from_utf8_lossy(ctx.cmd[3]).into_owned();
    let done = |result: Result<(), String>| match result {
        Ok(()) => resp_simple("OK"),
        Err(e) => resp_error(&format!("ERR {e}")),
    };

    match action.as_str() {
        "START" => {
            if ctx.cmd.len() < 7 {
                return resp_error("ERR syntax error");
            }
            let Some(dest) = crate::cluster::node::parse_node_id(ctx.cmd[4]) else {
                return resp_error("ERR Invalid node id");
            };
            let ranges = match migration::parse_ranges(&ctx.cmd[5..]) {
                Ok(r) => r,
                Err(e) => return resp_error(&format!("ERR {e}")),
            };
            done(migration::start_source(
                migration::Env::from_ctx(ctx),
                &task_id,
                dest,
                ranges,
            ))
        }
        "CANCEL" => {
            ctx.cluster.slot_migrations.cancel(Some(&task_id));
            resp_simple("OK")
        }
        "TAKEOVER" => done(migration::take_over(ctx.cluster, &task_id)),
        "FAIL" => {
            let reason = ctx
                .cmd
                .get(4)
                .map(|r| String::from_utf8_lossy(r).into_owned())
                .unwrap_or_default();
            migration::source_failed(ctx.cluster, &task_id, &reason);
            resp_simple("OK")
        }
        "APPLY" => syncslots_apply(ctx, &task_id),
        other => resp_error(&format!(
            "ERR unknown CLUSTER SYNCSLOTS subcommand '{other}'"
        )),
    }
}

/// Run the command a migration's source sent, as a write of this node's own:
/// it goes to this node's replicas and AOF like any other. The slot does not
/// belong to this node yet, so the ownership check is bypassed — for this
/// command only, and only for keys inside the task's slots.
fn syncslots_apply(ctx: &mut Ctx, task_id: &str) -> Vec<u8> {
    use crate::cluster::migration;
    if ctx.cmd.len() < 5 {
        return resp_error("ERR syntax error");
    }
    if let Err(e) = migration::accept_apply(ctx.cluster, task_id) {
        return resp_error(&format!("ERR {e}"));
    }
    let inner: Vec<&[u8]> = ctx.cmd[4..].to_vec();
    let name = String::from_utf8_lossy(inner[0]).to_ascii_uppercase();
    if !crate::replication::command_mutates_for_replication(&name, &inner) {
        return resp_error("ERR only writes can be applied to an importing slot");
    }
    let Some(task) = ctx.cluster.slot_migrations.get(task_id) else {
        return resp_error("ERR no such import in progress");
    };
    let keys = crate::keyspec::key_positions(&name, &inner);
    let inside = !keys.is_empty()
        && keys
            .iter()
            .all(|pos| migration::in_ranges(&task.ranges, key_hash_slot(inner[pos])));
    if !inside {
        return resp_error("ERR the command's keys are not in the importing slots");
    }

    let saved_cmd = std::mem::replace(&mut ctx.cmd, inner);
    let saved_bypass = std::mem::replace(&mut ctx.bypass_cluster_check, true);
    let result = {
        // A write like any other: whole, as far as a cut is concerned, and
        // in order with the other writes to its keys.
        let _command = crate::command_gate::enter();
        let _gate = crate::write_gate::enter();
        let _order = crate::write_order::lock_keys(
            *ctx.selected_db,
            crate::keyspec::key_positions(&name, &ctx.cmd)
                .iter()
                .map(|pos| ctx.cmd[pos]),
        );
        let result = crate::commands::handler::dispatch_command_handlers(ctx, &name)
            .unwrap_or_else(|| resp_error("ERR unknown command"));
        crate::commands::handler::apply_write_side_effects(ctx, &name, &result);
        result
    };
    ctx.cmd = saved_cmd;
    ctx.bypass_cluster_check = saved_bypass;

    if result.starts_with(b"-") {
        result
    } else {
        resp_simple("OK")
    }
}

/// Whether any key in the selected database hashes to `slot`.
fn slot_has_keys(ctx: &Ctx, slot: u16) -> bool {
    ctx.store[*ctx.selected_db].iter().any(|shard| {
        crate::read_lock!(shard)
            .keys()
            .any(|k| key_hash_slot(k.as_bytes()) == slot)
    })
}

// ── MIGRATE host port key|"" db timeout [COPY] [REPLACE]
//            [AUTH password | AUTH2 username password] [KEYS key [key ...]] ──
//
// Move keys to another node: serialise each, RESTORE it there, and delete it
// here once the destination has acknowledged it. This is what a cluster
// resharding is made of — `redis-cli --cluster reshard` and `rebalance` drive
// it with the `KEYS` form, a batch of keys from one slot at a time.
//
// It used to handle one key, of type string only, sent as a plain `SET`: the
// TTL was dropped in transit, every other type was refused, and the `KEYS`
// form the resharding tools rely on was rejected outright.

pub fn handle_migrate(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    if name != "MIGRATE" {
        return None;
    }
    Some(migrate(ctx))
}

/// One key on its way out: name, serialised value, absolute expiry (0 = none).
struct Outgoing {
    key: String,
    payload: Vec<u8>,
    expires_at_ms: u64,
}

fn migrate(ctx: &mut Ctx) -> Vec<u8> {
    use std::io::Write;
    use std::net::ToSocketAddrs;
    use std::time::Duration;

    let cmd = &ctx.cmd;
    if cmd.len() < 6 {
        return resp_error("ERR wrong number of arguments for 'migrate' command");
    }
    let Ok(host) = std::str::from_utf8(cmd[1]) else {
        return resp_error("ERR invalid host");
    };
    let Some(port) = std::str::from_utf8(cmd[2])
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
    else {
        return resp_error("ERR invalid port");
    };
    let Some(dest_db) = std::str::from_utf8(cmd[4])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    else {
        return resp_error("ERR value is not an integer or out of range");
    };
    let timeout = match std::str::from_utf8(cmd[5])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        // Redis treats a non-positive timeout as one second.
        Some(ms) if ms > 0 => Duration::from_millis(ms as u64),
        Some(_) => Duration::from_secs(1),
        None => return resp_error("ERR value is not an integer or out of range"),
    };

    // ── Options ──────────────────────────────────────────────────────────
    let mut copy = false;
    let mut replace = false;
    let mut auth: Option<Vec<Vec<u8>>> = None;
    let mut keys: Vec<&[u8]> = Vec::new();
    let mut i = 6;
    while i < cmd.len() {
        let option = cmd[i].to_ascii_uppercase();
        match option.as_slice() {
            b"COPY" => copy = true,
            b"REPLACE" => replace = true,
            b"AUTH" => {
                let Some(password) = cmd.get(i + 1) else {
                    return resp_error("ERR syntax error");
                };
                auth = Some(vec![b"AUTH".to_vec(), password.to_vec()]);
                i += 1;
            }
            b"AUTH2" => {
                let (Some(user), Some(password)) = (cmd.get(i + 1), cmd.get(i + 2)) else {
                    return resp_error("ERR syntax error");
                };
                auth = Some(vec![b"AUTH".to_vec(), user.to_vec(), password.to_vec()]);
                i += 2;
            }
            b"KEYS" => {
                if !cmd[3].is_empty() {
                    return resp_error(
                        "ERR When using MIGRATE KEYS option, the key argument must be set to \
                         the empty string",
                    );
                }
                // KEYS is the last option: everything after it is a key.
                keys = cmd[i + 1..].to_vec();
                break;
            }
            _ => return resp_error("ERR syntax error"),
        }
        i += 1;
    }
    if keys.is_empty() {
        if cmd[3].is_empty() {
            return resp_error("ERR syntax error");
        }
        keys.push(cmd[3]);
    }

    // ── Serialise what exists ────────────────────────────────────────────
    let db = *ctx.selected_db;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let mut outgoing: Vec<Outgoing> = Vec::with_capacity(keys.len());
    for key in &keys {
        let Ok(key) = std::str::from_utf8(key) else {
            continue;
        };
        if crate::commands::helpers::lazy_expire_key(ctx.store, ctx.expiry_store, db, key) {
            continue;
        }
        let value = {
            let shard = crate::store::get_shard(&ctx.store[db], key);
            match crate::read_lock!(shard).get(key) {
                Some(v) => v.to_value(),
                None => continue,
            }
        };
        let expires_at_ms = {
            let shard = crate::commands::helpers::get_expiry_shard(ctx.expiry_store, db, key);
            crate::read_lock!(shard)
                .get(key)
                .and_then(|at| at.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        };
        if expires_at_ms != 0 && expires_at_ms <= now_ms {
            continue;
        }
        let payload = match crate::snapshot::dump_payload(&value) {
            Ok(p) => p,
            Err(e) => return resp_error(&format!("ERR could not serialise '{key}': {e}")),
        };
        outgoing.push(Outgoing {
            key: key.to_string(),
            payload,
            expires_at_ms,
        });
    }
    if outgoing.is_empty() {
        return resp_simple("NOKEY");
    }

    // ── Send ─────────────────────────────────────────────────────────────
    let addr = format!("{host}:{port}");
    let Some(socket_addr) = addr.to_socket_addrs().ok().and_then(|mut it| it.next()) else {
        return resp_error(&format!("IOERR could not resolve {addr}"));
    };
    // TLS with `tls-cluster`, as every connection between nodes.
    let mut sock = match crate::node_tls::connect_node(host, &socket_addr, timeout) {
        Ok((stream, _)) => stream,
        Err(e) => {
            return resp_error(&format!(
                "IOERR error or timeout connecting to the client: {e}"
            ));
        }
    };

    let mut pipeline = Vec::new();
    // Replies that precede the per-key ones and must each be +OK.
    let mut preamble: Vec<&str> = Vec::new();
    if let Some(auth) = &auth {
        pipeline.extend_from_slice(&crate::resp::write::encode_resp_command(auth));
        preamble.push("AUTH");
    }
    pipeline.extend_from_slice(&crate::resp::write::encode_resp_command(&[
        b"SELECT".to_vec(),
        dest_db.to_string().into_bytes(),
    ]));
    preamble.push("SELECT");
    for item in &outgoing {
        // ASKING is single-shot and must immediately precede the RESTORE: the
        // destination is still only *importing* the slot, and would answer a
        // bare RESTORE with a redirect back to us.
        pipeline.extend_from_slice(&crate::resp::write::encode_resp_command(&[
            b"ASKING".to_vec()
        ]));
        let mut restore = vec![
            b"RESTORE".to_vec(),
            item.key.as_bytes().to_vec(),
            item.expires_at_ms.to_string().into_bytes(),
            item.payload.clone(),
        ];
        if replace {
            restore.push(b"REPLACE".to_vec());
        }
        if item.expires_at_ms != 0 {
            // Absolute, so the key expires when it would have here however
            // long the transfer takes.
            restore.push(b"ABSTTL".to_vec());
        }
        pipeline.extend_from_slice(&crate::resp::write::encode_resp_command(&restore));
    }
    if let Err(e) = sock.write_all(&pipeline).and_then(|()| sock.flush()) {
        return resp_error(&format!(
            "IOERR error or timeout writing to target instance: {e}"
        ));
    }

    // ── Read the replies ─────────────────────────────────────────────────
    let mut reader = std::io::BufReader::new(sock);
    for what in preamble {
        match read_status_reply(&mut reader) {
            Ok(Ok(())) => {}
            Ok(Err(reply)) => {
                return resp_error(&format!(
                    "ERR Target instance replied with error to {what}: {reply}"
                ));
            }
            Err(e) => {
                return resp_error(&format!(
                    "IOERR error or timeout reading to target instance: {e}"
                ));
            }
        }
    }
    let mut moved: Vec<&str> = Vec::with_capacity(outgoing.len());
    let mut failure: Option<String> = None;
    for item in &outgoing {
        // ASKING's reply does not matter; RESTORE's does. A destination that
        // is not in cluster mode answers ASKING with an error and still
        // performs the RESTORE.
        let asking = read_status_reply(&mut reader);
        let restore = match asking {
            Err(e) => Err(e),
            Ok(_) => read_status_reply(&mut reader),
        };
        match restore {
            Ok(Ok(())) => moved.push(item.key.as_str()),
            Ok(Err(reply)) => {
                failure.get_or_insert(format!("ERR Target instance replied with error: {reply}"));
            }
            Err(e) => {
                // The connection is gone; nothing after this can be confirmed.
                failure.get_or_insert(format!(
                    "IOERR error or timeout reading to target instance: {e}"
                ));
                break;
            }
        }
    }

    // ── Delete what the destination confirmed ────────────────────────────
    if !copy {
        for key in &moved {
            crate::commands::helpers::delete_key(ctx.store, ctx.expiry_store, db, key);
            crate::vector::maybe_unindex(ctx.index_store, key);
        }
    }

    match failure {
        None => resp_simple("OK"),
        Some(error) => {
            // A partial failure still moved — and deleted — some keys. The
            // error reply means nothing is propagated on our behalf, so tell
            // replicas and the AOF about those deletions directly; otherwise
            // they keep keys this node no longer has.
            if !copy && !moved.is_empty() && crate::replication::write_is_observed(ctx) {
                let mut del = vec![b"DEL".to_vec()];
                del.extend(moved.iter().map(|k| k.as_bytes().to_vec()));
                crate::replication::replicate_one_write(ctx, &del);
            }
            resp_error(&error)
        }
    }
}

/// Read one reply that is expected to be a status. `Ok(Ok(()))` for any `+`
/// reply, `Ok(Err(text))` for an error reply (or anything else), `Err` when
/// the connection fails or times out.
fn read_status_reply(
    reader: &mut std::io::BufReader<Box<dyn crate::node_tls::ReadWrite>>,
) -> std::io::Result<Result<(), String>> {
    use std::io::BufRead;
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "connection closed by target instance",
        ));
    }
    let line = line.trim_end_matches(['\r', '\n']);
    Ok(match line.strip_prefix('+') {
        Some(_) => Ok(()),
        None => Err(line.trim_start_matches('-').to_string()),
    })
}

// ── CLUSTER RESET [HARD | SOFT] ──────────────────────────────────────────────

fn cmd_reset(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if let Some(err) = require_cluster_mode(ctx.cluster) {
        return err;
    }
    let hard = if cmd.len() == 2 {
        false // default is SOFT
    } else if cmd.len() == 3 {
        match std::str::from_utf8(cmd[2]) {
            Ok(s) => match s.to_ascii_uppercase().as_str() {
                "HARD" => true,
                "SOFT" => false,
                _ => return resp_error("ERR syntax: CLUSTER RESET [HARD | SOFT]"),
            },
            Err(_) => return resp_error("ERR invalid argument"),
        }
    } else {
        return resp_error("ERR wrong number of arguments for 'cluster|reset' command");
    };

    // Redis refuses to reset a master that holds keys: the reset drops its
    // slots, and the keys would be left belonging to nothing. (A replica's
    // dataset is flushed by the reset instead.)
    if ctx.cluster.my_master_id().is_none() && !hard {
        let has_keys = ctx
            .store
            .iter()
            .any(|db| db.iter().any(|shard| !crate::read_lock!(shard).is_empty()));
        if has_keys {
            return resp_error(
                "ERR CLUSTER RESET can't be called with master nodes containing keys",
            );
        }
    }

    // FLUSHALL the data on hard reset, mirroring Redis behavior.
    if hard {
        let new_id = ctx.cluster.hard_reset();
        let flushed = crate::store::clear_store(ctx.store, ctx.expiry_store);
        let indexes = crate::vector::drop_all_indexes(ctx.index_store);
        crate::vector::drop_all_dicts(ctx.dict_store);
        crate::store::clear_ts_label_index(ctx.ts_index);
        if flushed.should_trim() || indexes > 0 {
            crate::commands::memory::purge_allocator();
        }
        log_info!(
            "Cluster: Hard reset performed — new node ID {}",
            node_id_str(&new_id)
        );
    } else {
        ctx.cluster.soft_reset();
        log_info!("Cluster: Soft reset performed");
    }

    // Drop our replica relationship if any.
    ctx.repl_state.become_master();

    resp_simple("OK")
}

// ── CLUSTER MYSHARDID ────────────────────────────────────────────────────────
//
// Shard ID = master node's ID. For a master that's our own ID. For a replica
// it's our master_id. Returns the 40-hex node ID as a bulk string.

fn cmd_myshardid(ctx: &mut Ctx) -> Vec<u8> {
    if let Some(err) = require_cluster_mode(ctx.cluster) {
        return err;
    }
    let shard_id = match ctx.cluster.my_master_id() {
        Some(master_id) => master_id, // we're a replica
        None => ctx.cluster.myself(), // we're a master
    };
    resp_bulk_str(crate::cluster::node_id_str(&shard_id))
}

// ── CLUSTER SLOT-STATS ───────────────────────────────────────────────────────
//
// Two forms:
//   CLUSTER SLOT-STATS SLOTSRANGE <start> <end>
//   CLUSTER SLOT-STATS ORDERBY <metric> [LIMIT <n>] [ASC | DESC]
//
// We track `key-count` honestly (computed by walking the keyspace) and report
// 0 for cpu-usec / network-bytes-in / network-bytes-out — these would require
// per-slot instrumentation we don't currently have.
//
// Returns an array of slot stat entries. Each entry is a 2-element array:
//   [slot, [field1, value1, field2, value2, ...]]

fn cmd_slot_stats(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if let Some(err) = require_cluster_mode(ctx.cluster) {
        return err;
    }
    if cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'cluster|slot-stats' command");
    }
    let mode = match std::str::from_utf8(cmd[2]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return resp_error("ERR syntax error"),
    };

    // Compute key counts per slot, only for slots we own.
    // Walk every key in the selected DB once.
    let key_counts = compute_key_counts_per_slot(ctx);

    let entries: Vec<(u16, u64)> = match mode.as_str() {
        "SLOTSRANGE" => {
            if cmd.len() != 5 {
                return resp_error("ERR syntax: CLUSTER SLOT-STATS SLOTSRANGE start-slot end-slot");
            }
            let start: u16 = match parse_slot_arg(cmd[3]) {
                Ok(s) => s,
                Err(e) => return e,
            };
            let end: u16 = match parse_slot_arg(cmd[4]) {
                Ok(s) => s,
                Err(e) => return e,
            };
            if start > end {
                return resp_error(&format!("ERR Invalid range {start}-{end}"));
            }
            (start..=end)
                .filter(|s| ctx.cluster.owns_slot(*s))
                .map(|s| (s, key_counts.get(&s).copied().unwrap_or(0)))
                .collect()
        }
        "ORDERBY" => {
            if cmd.len() < 4 {
                return resp_error(
                    "ERR syntax: CLUSTER SLOT-STATS ORDERBY metric [LIMIT n] [ASC | DESC]",
                );
            }
            let metric = match std::str::from_utf8(cmd[3]) {
                Ok(s) => s.to_ascii_lowercase(),
                Err(_) => return resp_error("ERR invalid metric"),
            };
            // Only "key-count" is meaningful for us.
            if metric != "key-count" {
                return resp_error(&format!(
                    "ERR ORDERBY metric '{metric}' not supported (use 'key-count')"
                ));
            }
            let mut limit: usize = 16384;
            // Busiest first unless asked otherwise, as in Redis: the
            // command is for finding the hot slots. It was ascending (#139).
            let mut descending = true;
            let mut i = 4;
            while i < cmd.len() {
                let arg = match std::str::from_utf8(cmd[i]) {
                    Ok(s) => s.to_ascii_uppercase(),
                    Err(_) => return resp_error("ERR syntax error"),
                };
                match arg.as_str() {
                    "LIMIT" => {
                        i += 1;
                        if i >= cmd.len() {
                            return resp_error("ERR LIMIT requires a value");
                        }
                        limit = match std::str::from_utf8(cmd[i])
                            .ok()
                            .and_then(|s| s.parse().ok())
                        {
                            Some(n) => n,
                            None => return resp_error("ERR invalid LIMIT"),
                        };
                    }
                    "ASC" => descending = false,
                    "DESC" => descending = true,
                    other => {
                        return resp_error(&format!("ERR unexpected token '{other}'"));
                    }
                }
                i += 1;
            }
            // Take all our owned slots, sort by count, truncate.
            let mut all: Vec<(u16, u64)> = (0..crate::cluster::CLUSTER_SLOTS as u16)
                .filter(|s| ctx.cluster.owns_slot(*s))
                .map(|s| (s, key_counts.get(&s).copied().unwrap_or(0)))
                .collect();
            if descending {
                all.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
            } else {
                all.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
            }
            all.truncate(limit);
            all
        }
        other => return resp_error(&format!("ERR Unknown subcommand '{other}'")),
    };

    let resp_entries: Vec<Vec<u8>> = entries
        .into_iter()
        .map(|(slot, key_count)| {
            resp_array(vec![
                resp_integer(slot as i64),
                resp_array(vec![
                    resp_bulk_str("key-count"),
                    resp_integer(key_count as i64),
                    resp_bulk_str("cpu-usec"),
                    resp_integer(0),
                    resp_bulk_str("network-bytes-in"),
                    resp_integer(0),
                    resp_bulk_str("network-bytes-out"),
                    resp_integer(0),
                ]),
            ])
        })
        .collect();
    resp_array(resp_entries)
}

/// Walk the selected DB once and return a map of slot → key-count, but only
/// for slots we own. Used by CLUSTER SLOT-STATS.
///
/// O(N) over all keys in the DB. For a multi-million-key DB this is slow,
/// but SLOT-STATS is an operator command, not a hot-path one — fine for now.
fn compute_key_counts_per_slot(ctx: &Ctx) -> std::collections::HashMap<u16, u64> {
    use crate::cluster::key_hash_slot;

    let mut counts: std::collections::HashMap<u16, u64> = std::collections::HashMap::new();
    let db = &ctx.store[*ctx.selected_db];
    for shard in db.iter() {
        let map = crate::read_lock!(shard);
        for k in map.keys() {
            let slot = key_hash_slot(k.as_bytes());
            if ctx.cluster.owns_slot(slot) {
                *counts.entry(slot).or_insert(0) += 1;
            }
        }
    }
    counts
}
