// src/cluster/persist.rs
//
// nodes.conf — the on-disk record of cluster topology.
//
// Format matches Redis closely enough that operators familiar with Redis
// cluster files can read ours. Each line describes one node:
//
//   <id> <ip:client_port@cluster_port> <flags> <master_id|-> <ping> <pong> <epoch> <link> <slot ranges...>
//
// Plus a final line with our current_epoch:
//
//   vars currentEpoch <N>
//
// A few simplifications versus Redis:
//   - We don't track separate "configEpoch" and "currentEpoch" per node yet;
//     each node's epoch is its own claim version.
//   - Link state ("connected" / "disconnected") is always written as
//     "connected" — we'll fill this in once the gossip layer (Step 5)
//     exists. For now it's a placeholder, but the column must be present
//     so the format stays stable.
//
// The whole file is rewritten atomically: write to nodes.conf.tmp, then
// rename. This is the standard way to avoid a half-written file if the
// process crashes mid-save.

use crate::cluster::node::{ClusterNode, NodeFlags, NodeId, node_id_str, parse_node_id};
use crate::cluster::slot::CLUSTER_SLOTS;
use ahash::AHashMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::Ordering;

/// What we read out of nodes.conf. Used to repopulate ClusterState on startup.
pub struct LoadedConfig {
    pub myself: NodeId,
    pub nodes: AHashMap<NodeId, ClusterNode>,
    pub slots: Box<[Option<NodeId>; CLUSTER_SLOTS]>,
    pub current_epoch: u64,
    pub last_vote_epoch: u64,
}

/// Read nodes.conf from disk. Returns Ok(None) if the file does not exist
/// (first startup), Err on parse failure, Ok(Some(_)) on success.
pub fn load(path: &Path) -> std::io::Result<Option<LoadedConfig>> {
    let contents = match fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };

    let mut nodes: AHashMap<NodeId, ClusterNode> = AHashMap::new();
    let mut myself: Option<NodeId> = None;
    let mut current_epoch: u64 = 0;
    let mut last_vote_epoch: u64 = 0;
    // Box<[Option<NodeId>; 16384]> via the same dance as in mod.rs
    let mut slots: Box<[Option<NodeId>; CLUSTER_SLOTS]> = vec![None; CLUSTER_SLOTS]
        .into_boxed_slice()
        .try_into()
        .expect("vec is exactly CLUSTER_SLOTS long");

    for (lineno, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // The "vars" line is special: vars currentEpoch <N> [other vars...]
        if line.starts_with("vars ") {
            let mut tokens = line.split_whitespace();
            tokens.next();
            while let (Some(k), Some(v)) = (tokens.next(), tokens.next()) {
                match k {
                    "currentEpoch" => current_epoch = v.parse().unwrap_or(0),
                    "lastVoteEpoch" => last_vote_epoch = v.parse().unwrap_or(0),
                    _ => {}
                }
            }
            continue;
        }

        // Node line. Minimum 8 fields before any slot ranges:
        //   id addr flags master ping pong epoch link
        let tokens: Vec<&str> = line.split_whitespace().collect();
        if tokens.len() < 8 {
            return Err(io_err(format!(
                "nodes.conf line {}: expected >=8 fields, got {}",
                lineno + 1,
                tokens.len()
            )));
        }

        let id = parse_node_id(tokens[0].as_bytes())
            .ok_or_else(|| io_err(format!("invalid node id on line {}", lineno + 1)))?;

        // `ip:port@cport`, then `,hostname` when the node announces one,
        // as Redis writes it.
        let (addr, hostname) = tokens[1].split_once(',').unwrap_or((tokens[1], ""));
        let (ip, client_port, cluster_port) = parse_addr(addr)
            .ok_or_else(|| io_err(format!("invalid address on line {}", lineno + 1)))?;

        // PFAIL and FAIL are verdicts about reachability *now*; one recorded
        // before a restart says nothing about the present, and a stale FAIL
        // would have this node refuse to ping a healthy peer. Handshake
        // placeholders are likewise only meaningful within one process.
        let flags = parse_flags(tokens[2]) & !(NodeFlags::PFAIL | NodeFlags::FAIL);
        if flags.contains(NodeFlags::HANDSHAKE) {
            continue;
        }
        let master_id = if tokens[3] == "-" {
            None
        } else {
            parse_node_id(tokens[3].as_bytes())
        };
        let ping_sent_ms: u64 = tokens[4].parse().unwrap_or(0);
        let pong_received_ms: u64 = tokens[5].parse().unwrap_or(0);
        let config_epoch: u64 = tokens[6].parse().unwrap_or(0);
        // tokens[7] is the link state ("connected"/"disconnected") — ignored on load.

        // Remaining tokens are slot specifications: "N" or "N-M" or
        // special migration markers like "[N-<-srcid]" / "[N->-dstid]".
        // Migration markers are Step 7's problem; for now we recognize
        // them and skip them, so a partly-migrated cluster file still loads.
        for &tok in &tokens[8..] {
            if tok.starts_with('[') {
                continue; // migration marker, ignored for now
            }
            assign_slot_spec(tok, id, &mut slots).ok_or_else(|| {
                io_err(format!(
                    "invalid slot spec '{}' on line {}",
                    tok,
                    lineno + 1
                ))
            })?;
        }

        if flags.contains(NodeFlags::MYSELF) {
            myself = Some(id);
        }

        nodes.insert(
            id,
            ClusterNode {
                id,
                ip,
                client_port,
                cluster_port,
                flags,
                master_id,
                config_epoch,
                ping_sent_ms,
                pong_received_ms,
                // Not persisted: a FAIL verdict from before a restart is
                // stale by definition and is re-established by gossip.
                fail_since_ms: 0,
                hostname: hostname.to_string(),
                // Not persisted: the peer says it again on its next heartbeat.
                announced_client_port: 0,
            },
        );
    }

    let myself = myself.ok_or_else(|| io_err("nodes.conf: no node has the 'myself' flag"))?;

    Ok(Some(LoadedConfig {
        myself,
        nodes,
        slots,
        current_epoch,
        last_vote_epoch,
    }))
}

/// Write the cluster state to disk atomically.
/// state_locked_view is what the caller has already locked and prepared —
/// we don't reach back into ClusterState to avoid lock-acquisition surprises.
pub fn save(
    path: &Path,
    nodes: &AHashMap<NodeId, ClusterNode>,
    slots: &[Option<NodeId>; CLUSTER_SLOTS],
    current_epoch: u64,
    last_vote_epoch: u64,
) -> std::io::Result<()> {
    let tmp_path = path.with_extension("conf.tmp");

    // Build the file contents in memory first. The whole file is small
    // (kilobytes for any realistic cluster size) so this is not a concern.
    let mut out: Vec<u8> = Vec::with_capacity(4096);

    // Sort nodes by ID for deterministic output — useful for tests and
    // for diffing nodes.conf across restarts.
    let mut sorted_nodes: Vec<&ClusterNode> = nodes.values().collect();
    sorted_nodes.sort_by_key(|n| n.id);

    for node in sorted_nodes {
        if node.flags.contains(NodeFlags::HANDSHAKE) {
            continue;
        }
        // <id>
        out.extend_from_slice(node_id_str(&node.id).as_bytes());
        out.push(b' ');

        // <ip:client_port@cluster_port>
        write!(
            out,
            "{}:{}@{}{} ",
            node.ip,
            node.client_port,
            node.cluster_port,
            hostname_suffix(&node.hostname)
        )
        .unwrap();

        // <flags> — configuration only; see `load` for why verdicts are dropped.
        out.extend_from_slice(
            format_flags(node.flags & !(NodeFlags::PFAIL | NodeFlags::FAIL)).as_bytes(),
        );
        out.push(b' ');

        // <master_id|->
        match node.master_id {
            Some(m) => out.extend_from_slice(node_id_str(&m).as_bytes()),
            None => out.push(b'-'),
        }
        out.push(b' ');

        // <ping> <pong> <epoch>
        write!(
            out,
            "{} {} {} ",
            node.ping_sent_ms, node.pong_received_ms, node.config_epoch
        )
        .unwrap();

        // <link state> — placeholder until gossip layer
        out.extend_from_slice(b"connected");

        // <slot ranges> for masters only
        if node.flags.contains(NodeFlags::MASTER) {
            for range in compute_slot_ranges(slots, node.id) {
                out.push(b' ');
                if range.0 == range.1 {
                    write!(out, "{}", range.0).unwrap();
                } else {
                    write!(out, "{}-{}", range.0, range.1).unwrap();
                }
            }
        }

        out.push(b'\n');
    }

    // Trailing vars line
    writeln!(
        out,
        "vars currentEpoch {} lastVoteEpoch {}",
        current_epoch, last_vote_epoch
    )
    .unwrap();

    write_atomically(path, &tmp_path, &out)
}

thread_local! {
    static DEFER_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Run `f` with the saves it makes written and renamed into place at once,
/// and synced afterwards on another thread. For a save that something is
/// waiting on with client writes paused: a process that dies loses nothing
/// by it, only a machine that loses power in the moment between. A sync on
/// a busy disk took seconds.
pub fn with_deferred_sync<R>(f: impl FnOnce() -> R) -> R {
    DEFER_SYNC.with(|d| d.set(true));
    let result = f();
    DEFER_SYNC.with(|d| d.set(false));
    result
}

/// Write to `.tmp` and rename it into place. The caller syncs it (see
/// [`save_from_state`]).
fn write_atomically(path: &Path, tmp_path: &Path, out: &[u8]) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    let written = write_direct(tmp_path, out).is_ok();
    #[cfg(not(target_os = "linux"))]
    let written = false;
    if !written {
        let mut f = fs::File::create(tmp_path)?;
        f.write_all(out)?;
    }
    fs::rename(tmp_path, path)
}

/// Write `out` past the page cache (`O_DIRECT`), padded with blank lines to
/// the 4 KiB it needs. A process writing a great deal — a node taking in a
/// slot migration, its AOF growing by hundreds of megabytes — has every
/// buffered write it makes held back by the kernel until the disk catches
/// up: nodes.conf's few kilobytes waited 5.7 s, with the migration's source
/// pausing client writes for the answer. A direct write is not held back.
/// An error (a filesystem without `O_DIRECT`) and the caller writes the
/// ordinary way.
#[cfg(target_os = "linux")]
fn write_direct(tmp_path: &Path, out: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    const ALIGN: usize = 4096;
    let len = out.len().div_ceil(ALIGN).max(1) * ALIGN;
    let layout = std::alloc::Layout::from_size_align(len, ALIGN)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    // SAFETY: `layout` is non-zero in size; the buffer is filled before it
    // is read, and freed once, below.
    let ptr = unsafe { std::alloc::alloc(layout) };
    if ptr.is_null() {
        return Err(std::io::Error::other("out of memory"));
    }
    let buf = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
    buf[..out.len()].copy_from_slice(out);
    buf[out.len()..].fill(b'\n');
    let result = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_DIRECT)
        .open(tmp_path)
        .and_then(|mut f| f.write_all(buf));
    unsafe { std::alloc::dealloc(ptr, layout) };
    result
}

/// What `save` would write, minus the two timestamp columns.
///
/// Every heartbeat refreshes a peer's ping/pong times, so comparing whole
/// files would call every save a change. What must survive a restart is the
/// topology — ids, addresses, roles, epochs, slots — and that changes rarely.
fn topology_fingerprint(
    nodes: &AHashMap<NodeId, ClusterNode>,
    slots: &[Option<NodeId>; CLUSTER_SLOTS],
    current_epoch: u64,
    last_vote_epoch: u64,
) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    let mut sorted: Vec<&ClusterNode> = nodes.values().collect();
    sorted.sort_by_key(|n| n.id);
    for n in sorted {
        n.id.hash(&mut h);
        n.ip.hash(&mut h);
        n.client_port.hash(&mut h);
        n.cluster_port.hash(&mut h);
        // FAIL / PFAIL / HANDSHAKE are not persisted (see `load`).
        (n.flags & !(NodeFlags::PFAIL | NodeFlags::FAIL | NodeFlags::HANDSHAKE))
            .bits()
            .hash(&mut h);
        n.master_id.hash(&mut h);
        n.config_epoch.hash(&mut h);
        // Saved too: a hostname learnt on its own used to wait for some other
        // change before it reached the file, and a restart forgot it.
        n.hostname.hash(&mut h);
    }
    slots.hash(&mut h);
    current_epoch.hash(&mut h);
    last_vote_epoch.hash(&mut h);
    h.finish()
}

/// Convenience wrapper that takes the live ClusterState and saves it.
/// Locks slots and nodes in a fixed order; safe to call from any thread.
///
/// A no-op when the topology is unchanged since the last successful save. The
/// bus calls this after every message it processes; writing and fsyncing an
/// identical file a dozen times a second per node was most of this module's
/// actual work.
pub fn save_from_state(state: &crate::cluster::ClusterState) -> std::io::Result<()> {
    let path = match state.config_path.as_ref() {
        Some(p) => p,
        None => return Ok(()),
    };
    // One writer at a time: two threads saving concurrently share the temp
    // file's name and would interleave into it. Held to write the file and
    // put it in place, not to sync it (below): a sync on a busy disk took
    // seconds, and a slot handover's takeover, with the source's writes
    // paused, waited behind the bus's save for all of them.
    let asked = std::time::Instant::now();
    let saving = state.save_lock.lock();
    let locked = asked.elapsed().as_millis();
    let (nodes_snap, slots_snap, epoch, last_vote) = {
        let nodes = state.nodes.read();
        let slots = state.slots.read();
        let epoch = state.current_epoch.load(Ordering::Relaxed);
        let last_vote = state.last_vote_epoch.load(Ordering::Relaxed);
        let mut slots_copy: Box<[Option<NodeId>; CLUSTER_SLOTS]> = vec![None; CLUSTER_SLOTS]
            .into_boxed_slice()
            .try_into()
            .expect("len matches");
        slots_copy.copy_from_slice(&**slots);
        (nodes.clone(), slots_copy, epoch, last_vote)
    };
    let fingerprint = topology_fingerprint(&nodes_snap, &slots_snap, epoch, last_vote);
    if state.last_saved_fingerprint.load(Ordering::Relaxed) == fingerprint && path.exists() {
        return Ok(());
    }
    save(path, &nodes_snap, &slots_snap, epoch, last_vote)?;
    state
        .last_saved_fingerprint
        .store(fingerprint, Ordering::Relaxed);
    drop(saving);
    if asked.elapsed() > std::time::Duration::from_millis(100) {
        log_info!(
            "Cluster: slow nodes.conf save: {locked} ms for the lock, {} ms in all",
            asked.elapsed().as_millis()
        );
    }
    if DEFER_SYNC.with(|d| d.get()) {
        let path = path.clone();
        std::thread::spawn(move || {
            if let Ok(f) = fs::OpenOptions::new().write(true).open(&path) {
                let _ = f.sync_all();
            }
        });
        Ok(())
    } else {
        // Opened for writing: Windows will not flush a file opened to read.
        fs::OpenOptions::new().write(true).open(path)?.sync_all()
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn io_err(msg: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg.into())
}

/// Parse "ip:port@busport". The cluster bus port is conventionally
/// client_port + 10000 in Redis.
fn parse_addr(s: &str) -> Option<(String, u16, u16)> {
    let (host_port, bus) = s.split_once('@')?;
    let bus: u16 = bus.parse().ok()?;
    // ip can contain ':' in IPv6 — split from the right.
    let colon = host_port.rfind(':')?;
    let ip = &host_port[..colon];
    let port: u16 = host_port[colon + 1..].parse().ok()?;
    Some((ip.to_string(), port, bus))
}

/// Parse a comma-separated flag list. Unknown flags are silently ignored,
/// so newer nodes.conf files written by a future version still load.
fn parse_flags(s: &str) -> NodeFlags {
    let mut f = NodeFlags::empty();
    if s == "noflags" {
        return f;
    }
    for tok in s.split(',') {
        match tok {
            "myself" => f |= NodeFlags::MYSELF,
            "master" => f |= NodeFlags::MASTER,
            "slave" | "replica" => f |= NodeFlags::REPLICA,
            "fail?" => f |= NodeFlags::PFAIL,
            "fail" => f |= NodeFlags::FAIL,
            "handshake" => f |= NodeFlags::HANDSHAKE,
            "noaddr" => f |= NodeFlags::NOADDR,
            _ => {}
        }
    }
    f
}

fn format_flags(f: NodeFlags) -> String {
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

/// "N" or "N-M" -> mark slots as owned by `owner`.
fn assign_slot_spec(
    spec: &str,
    owner: NodeId,
    slots: &mut [Option<NodeId>; CLUSTER_SLOTS],
) -> Option<()> {
    match spec.split_once('-') {
        None => {
            let n: usize = spec.parse().ok()?;
            if n >= CLUSTER_SLOTS {
                return None;
            }
            slots[n] = Some(owner);
        }
        Some((a, b)) => {
            let a: usize = a.parse().ok()?;
            let b: usize = b.parse().ok()?;
            if a > b || b >= CLUSTER_SLOTS {
                return None;
            }
            for slot in slots.iter_mut().take(b + 1).skip(a) {
                *slot = Some(owner);
            }
        }
    }
    Some(())
}

/// Walk the slots array and return contiguous ranges owned by `owner`.
/// `(5, 5)` means a single slot 5; `(5, 10)` means inclusive range 5..=10.
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

// ── Tests ────────────────────────────────────────────────────────────────────

/// `,hostname` after a node's address in nodes.conf and CLUSTER NODES, as
/// Redis writes it; nothing when it has none.
pub fn hostname_suffix(hostname: &str) -> String {
    if hostname.is_empty() {
        String::new()
    } else {
        format!(",{hostname}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::node::generate_node_id;
    use tempfile::NamedTempFile;

    #[test]
    fn round_trip_empty_master() {
        let me = generate_node_id();
        let mut nodes = AHashMap::new();
        nodes.insert(
            me,
            ClusterNode {
                id: me,
                ip: "127.0.0.1".into(),
                client_port: 6380,
                cluster_port: 16380,
                flags: NodeFlags::MYSELF | NodeFlags::MASTER,
                master_id: None,
                config_epoch: 1,
                ping_sent_ms: 0,
                pong_received_ms: 0,
                fail_since_ms: 0,
                hostname: String::new(),
                announced_client_port: 0,
            },
        );
        let slots: Box<[Option<NodeId>; CLUSTER_SLOTS]> = vec![None; CLUSTER_SLOTS]
            .into_boxed_slice()
            .try_into()
            .unwrap();

        let tmp = NamedTempFile::new().unwrap();
        save(tmp.path(), &nodes, &slots, 7, 0).unwrap();

        let loaded = load(tmp.path()).unwrap().unwrap();
        assert_eq!(loaded.myself, me);
        assert_eq!(loaded.current_epoch, 7);
        assert_eq!(loaded.nodes.len(), 1);
    }

    #[test]
    fn round_trip_with_slots() {
        let me = generate_node_id();
        let mut nodes = AHashMap::new();
        nodes.insert(
            me,
            ClusterNode {
                id: me,
                ip: "127.0.0.1".into(),
                client_port: 6380,
                cluster_port: 16380,
                flags: NodeFlags::MYSELF | NodeFlags::MASTER,
                master_id: None,
                config_epoch: 0,
                ping_sent_ms: 0,
                pong_received_ms: 0,
                fail_since_ms: 0,
                hostname: String::new(),
                announced_client_port: 0,
            },
        );
        // Assign slots 0..=99, 200..=200, 5000..=5474 to me.
        let mut slots: Box<[Option<NodeId>; CLUSTER_SLOTS]> = vec![None; CLUSTER_SLOTS]
            .into_boxed_slice()
            .try_into()
            .unwrap();
        for i in 0..=99 {
            slots[i] = Some(me);
        }
        slots[200] = Some(me);
        for i in 5000..=5474 {
            slots[i] = Some(me);
        }

        let tmp = NamedTempFile::new().unwrap();
        save(tmp.path(), &nodes, &slots, 0, 0).unwrap();

        let loaded = load(tmp.path()).unwrap().unwrap();
        // All the slots we wrote should still be ours.
        for i in 0..=99 {
            assert_eq!(loaded.slots[i], Some(me));
        }
        assert_eq!(loaded.slots[200], Some(me));
        for i in 5000..=5474 {
            assert_eq!(loaded.slots[i], Some(me));
        }
        // Spot-check a non-owned slot
        assert_eq!(loaded.slots[100], None);
    }

    #[test]
    fn missing_file_returns_none() {
        let path = Path::new("/tmp/this_file_definitely_does_not_exist_kividb_test_42424242");
        let _ = fs::remove_file(path); // best effort
        assert!(load(path).unwrap().is_none());
    }

    #[test]
    fn slot_range_compaction() {
        let me = generate_node_id();
        let mut slots: Box<[Option<NodeId>; CLUSTER_SLOTS]> = vec![None; CLUSTER_SLOTS]
            .into_boxed_slice()
            .try_into()
            .unwrap();
        // Pattern: 0, 1, 2,  5,  10, 11
        for i in [0, 1, 2, 5, 10, 11] {
            slots[i] = Some(me);
        }
        let ranges = compute_slot_ranges(&slots, me);
        assert_eq!(ranges, vec![(0, 2), (5, 5), (10, 11)]);
    }
}
