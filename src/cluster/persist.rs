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
// The whole file is rewritten atomically: written to a temporary file next
// to it, synced, renamed over it, and the directory synced. A crash at any
// point leaves the old file or the new one, never part of one. A file that
// is cut short all the same (it does not end with its `vars` line) is
// refused at startup (#808).

use crate::cluster::node::{
    ClusterNode, NodeFlags, NodeId, generate_node_id, node_id_str, parse_node_id,
};
use crate::cluster::slot::CLUSTER_SLOTS;
use ahash::AHashMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

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
    check_complete(&contents)?;

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

        // `ip:port@cport`, then `,hostname` (empty when the node announces
        // none) and `,key=value` fields, as Redis writes them: `shard-id=`.
        let mut fields = tokens[1].split(',');
        let addr = fields.next().unwrap_or("");
        let hostname = fields.next().unwrap_or("");
        let shard_id = fields
            .filter_map(|field| field.strip_prefix("shard-id="))
            .find_map(|id| parse_node_id(id.as_bytes()));
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
                // A file an earlier version wrote has none: a master gets a
                // new one, as Redis gives it one, and its replicas take it
                // below.
                shard_id: shard_id.unwrap_or_else(generate_node_id),
            },
        );
    }

    // A replica is in its master's shard, whatever the file says of it (as
    // Redis reads it: the master's line may come after the replica's).
    let shard_of_master: Vec<(NodeId, NodeId)> = nodes
        .values()
        .filter_map(|n| {
            let master = nodes.get(&n.master_id?)?;
            Some((n.id, master.shard_id))
        })
        .collect();
    for (id, shard_id) in shard_of_master {
        if let Some(n) = nodes.get_mut(&id) {
            n.shard_id = shard_id;
        }
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

/// Whether `contents` is a whole nodes.conf: one whose last line is its
/// `vars` line, ended by a newline, as every file this server has written
/// is (and as Redis writes it). One that is not was cut short: by a machine
/// that went down before a write reached the disk, or a disk that lost its
/// tail, and it may lack nodes, slots and the epochs. It is refused, with
/// the reason, and the file is left as it is. An empty file is refused too,
/// as empty. Redis takes one for a new node: here the file is deleted for
/// that.
fn check_complete(contents: &str) -> std::io::Result<()> {
    // Blank lines after it are the padding of a direct write.
    let last = contents
        .split_inclusive('\n')
        .rfind(|line| !line.trim().is_empty());
    match last {
        Some(line) if line.ends_with('\n') && line.starts_with("vars ") => Ok(()),
        Some(_) => Err(io_err(
            "nodes.conf is incomplete: it does not end with its 'vars' line (cut short by a \
             crash or a disk fault?)",
        )),
        None => Err(io_err(
            "nodes.conf is empty (cut short by a crash or a disk fault?)",
        )),
    }
}

/// Write the cluster state to `path`: to a temporary file, synced, renamed
/// over it, and the directory synced. For the tests; the server saves
/// through [`save_from_state`].
pub fn save(
    path: &Path,
    nodes: &AHashMap<NodeId, ClusterNode>,
    slots: &[Option<NodeId>; CLUSTER_SLOTS],
    current_epoch: u64,
    last_vote_epoch: u64,
) -> std::io::Result<()> {
    let tmp = temp_path(path, 0);
    write_temp(&tmp, &render(nodes, slots, current_epoch, last_vote_epoch))?;
    sync_file(&tmp)?;
    fs::rename(&tmp, path)?;
    sync_dir(path)
}

/// What nodes.conf holds for this state.
fn render(
    nodes: &AHashMap<NodeId, ClusterNode>,
    slots: &[Option<NodeId>; CLUSTER_SLOTS],
    current_epoch: u64,
    last_vote_epoch: u64,
) -> Vec<u8> {
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

        // <ip:client_port@cluster_port>,<hostname>,shard-id=<id>: the
        // hostname empty when the node announces none, as Redis writes it.
        write!(
            out,
            "{}:{}@{},{},shard-id={} ",
            node.ip,
            node.client_port,
            node.cluster_port,
            node.hostname,
            node_id_str(&node.shard_id)
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
    out
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

/// The temporary file save number `generation` is written to, beside
/// `path`: `nodes.conf.tmp-<generation>`. Each save has its own, so that one
/// can be synced while another is written.
fn temp_path(path: &Path, generation: u64) -> std::path::PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".tmp-{generation}"));
    path.with_file_name(name)
}

/// Remove the temporary files of saves a crash cut short (see
/// [`temp_path`]), and the one of the versions before them
/// (`nodes.conf.tmp`). Called at startup, before anything is saved.
pub fn remove_leftover_temps(path: &Path) {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return;
    };
    let dir = match path.parent() {
        Some(d) if !d.as_os_str().is_empty() => d,
        _ => Path::new("."),
    };
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let ours = format!("{name}.tmp-");
    let old = Path::new(name).with_extension("conf.tmp");
    for entry in entries.flatten() {
        let file = entry.file_name();
        let Some(file) = file.to_str() else { continue };
        let leftover = file
            .strip_prefix(&ours)
            .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
            || Path::new(file) == old;
        if leftover {
            let _ = fs::remove_file(entry.path());
        }
    }
}

/// For the tests of a save that fails, and never set otherwise: named in
/// `KIVIDB_TEST_FAILPOINTS`, `nodes-conf-fail` fails every save while the
/// file `fail.nodes-conf` is in the working directory, and
/// `nodes-conf-dir-sync-fail` fails the sync of the directory once for each
/// time `fail.nodes-conf-dir-sync` is made there. A file that holds `EINVAL`
/// or `ENOTSUP` fails it with that error, as a filesystem would.
fn injected(name: &str, file: &str, once: bool) -> std::io::Result<()> {
    if !crate::aof::injected(name) {
        return Ok(());
    }
    let hit = if once {
        let text = fs::read_to_string(file).unwrap_or_default();
        fs::remove_file(file).is_ok().then_some(text)
    } else {
        Path::new(file).exists().then(String::new)
    };
    match hit.as_deref().map(str::trim) {
        None => Ok(()),
        #[cfg(unix)]
        Some("EINVAL") => Err(std::io::Error::from_raw_os_error(libc::EINVAL)),
        #[cfg(unix)]
        Some("ENOTSUP") => Err(std::io::Error::from_raw_os_error(libc::ENOTSUP)),
        Some(_) => Err(std::io::Error::other(format!("{name} (a test's)"))),
    }
}

/// Write `out` to the temporary file `tmp`. Not synced: see [`sync_file`].
fn write_temp(tmp: &Path, out: &[u8]) -> std::io::Result<()> {
    injected("nodes-conf-fail", "fail.nodes-conf", false)?;
    #[cfg(target_os = "linux")]
    let written = write_direct(tmp, out).is_ok();
    #[cfg(not(target_os = "linux"))]
    let written = false;
    if !written {
        let mut f = fs::File::create(tmp)?;
        f.write_all(out)?;
    }
    Ok(())
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
        // And a shard id taken from a master's heartbeat, for the same reason:
        // a replica restarted before anything else changed came back in the
        // shard it had made up for its master (#563).
        n.shard_id.hash(&mut h);
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
    save_then(state, &[], None::<fn()>)
}

/// [`save_from_state`], with `mine` saved as this node's slots, and then,
/// once that is saved, `publish`: the change that makes them so in
/// `state`. Nothing is published if the save fails: the file is synced
/// before `publish` runs, unless the sync is deferred
/// ([`with_deferred_sync`]). `publish` runs before any other save can be
/// made, so none is made in between that leaves the slots out
/// (`ClusterState::take_over_slots`). It may take `slots` and `migrations`,
/// not `save_lock` or `nodes`.
pub fn save_from_state_then(
    state: &crate::cluster::ClusterState,
    mine: &[u16],
    publish: impl FnOnce(),
) -> std::io::Result<()> {
    save_then(state, mine, Some(publish))
}

/// Sync the file at `path`.
fn sync_file(path: &Path) -> std::io::Result<()> {
    // Opened for writing: Windows will not flush a file opened to read.
    fs::OpenOptions::new().write(true).open(path)?.sync_all()
}

/// Sync the directory `path` is in, so that a rename into it is on disk.
fn sync_dir(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let dir = match path.parent() {
            Some(d) if !d.as_os_str().is_empty() => d,
            _ => Path::new("."),
        };
        let synced = injected("nodes-conf-dir-sync-fail", "fail.nodes-conf-dir-sync", true)
            .and_then(|()| fs::File::open(dir)?.sync_all());
        dir_sync_outcome(synced)
    }
    // Windows: a directory cannot be opened as a file here, so the rename
    // is not synced. NTFS journals it, but the journal is written out
    // lazily: a power loss soon after the save can leave the file before it.
    #[cfg(not(unix))]
    {
        let _ = path;
        injected("nodes-conf-dir-sync-fail", "fail.nodes-conf-dir-sync", true)
    }
}

/// What a sync of the directory that returned `synced` means for the save.
/// A filesystem that cannot sync a directory at all (some FUSE, CIFS and 9p
/// mounts answer `EINVAL` or `ENOTSUP`) does not fail every save: the
/// rename stands as the filesystem keeps it, and that is said once, as a
/// warning. Any other error fails the save.
fn dir_sync_outcome(synced: std::io::Result<()>) -> std::io::Result<()> {
    match synced {
        Err(e) if cannot_sync_dirs(&e) => {
            static WARNED: std::sync::atomic::AtomicBool =
                std::sync::atomic::AtomicBool::new(false);
            if !WARNED.swap(true, Ordering::Relaxed) {
                log_warn!(
                    "Cluster: this filesystem cannot sync the directory of the cluster config \
                     file ({e}): a save is renamed into place, but the rename is only as \
                     durable as the filesystem makes it"
                );
            }
            Ok(())
        }
        other => other,
    }
}

/// Whether `e` says the filesystem does not sync directories, rather than
/// that the sync failed.
fn cannot_sync_dirs(e: &std::io::Error) -> bool {
    if matches!(
        e.kind(),
        std::io::ErrorKind::InvalidInput | std::io::ErrorKind::Unsupported
    ) {
        return true;
    }
    #[cfg(unix)]
    if let Some(code) = e.raw_os_error() {
        return code == libc::EINVAL || code == libc::ENOTSUP || code == libc::EOPNOTSUPP;
    }
    false
}

/// Put `tmp`, written and synced, in place of `path`. The temporary file
/// is removed if it cannot be.
fn put_in_place(tmp: &Path, path: &Path) -> std::io::Result<()> {
    if let Err(e) = fs::rename(tmp, path) {
        let _ = fs::remove_file(tmp);
        return Err(e);
    }
    Ok(())
}

fn save_then(
    state: &crate::cluster::ClusterState,
    mine: &[u16],
    publish: Option<impl FnOnce()>,
) -> std::io::Result<()> {
    let path = match state.config_path.as_ref() {
        Some(p) => p,
        None => {
            if let Some(publish) = publish {
                publish();
            }
            return Ok(());
        }
    };
    // Held to take the state and write it out, and to put a file in place,
    // so that what is in place is always the newest save. Not held, by a
    // save with nothing to publish (the bus's), to sync it: a sync on a busy
    // disk can take seconds, and a slot handover's takeover, with the
    // source's writes paused, would wait behind the bus's save. Each save
    // has a temporary file of its own for that (see `temp_path`).
    let asked = std::time::Instant::now();
    let saving = state.save_lock.lock();
    let locked = asked.elapsed().as_millis();
    // Numbered before the state is taken: a save numbered after a node was
    // added to `joining` has it (see `wait_for_save`).
    let generation = state.saves.written.fetch_add(1, Ordering::SeqCst) + 1;
    let (nodes_snap, slots_snap, epoch, last_vote) = {
        let nodes = state.nodes.read();
        let slots = state.slots.read();
        // SeqCst, against a vote's compare-and-swap of `last_vote_epoch`
        // and its load of `written`: either this save sees the vote, or the
        // vote waits for a save numbered after this one.
        let epoch = state.current_epoch.load(Ordering::SeqCst);
        let last_vote = state.last_vote_epoch.load(Ordering::SeqCst);
        let mut slots_copy: Box<[Option<NodeId>; CLUSTER_SLOTS]> = vec![None; CLUSTER_SLOTS]
            .into_boxed_slice()
            .try_into()
            .expect("len matches");
        slots_copy.copy_from_slice(&**slots);
        let me = state.myself();
        for &slot in mine {
            slots_copy[slot as usize] = Some(me);
        }
        let mut nodes = nodes.clone();
        // Nodes met and about to be members (`ClusterState::admit_met_node`).
        for (_, node) in state.joining.lock().iter() {
            nodes.entry(node.id).or_insert_with(|| node.clone());
        }
        (nodes, slots_copy, epoch, last_vote)
    };
    let fingerprint = topology_fingerprint(&nodes_snap, &slots_snap, epoch, last_vote);
    // Unchanged since the save in place, synced, and no other on its way to
    // replace it: nothing to write. With one on its way (being synced), it
    // is written all the same: the older one, put in place after, must not
    // be what stands (a `CLUSTER FORGET` that returns the topology to what
    // is in place is saved, not skipped). `in_flight` is read first: a save
    // that fails resets the fingerprint before it leaves `in_flight`.
    if state.saves.in_flight.load(Ordering::SeqCst) == 0
        && state.last_saved_fingerprint.load(Ordering::SeqCst) == fingerprint
        && path.exists()
    {
        state.saves.note_saved(generation, false);
        if let Some(publish) = publish {
            publish();
        }
        return Ok(());
    }
    state.saves.in_flight.fetch_add(1, Ordering::SeqCst);
    let tmp = temp_path(path, generation);
    let written = write_temp(&tmp, &render(&nodes_snap, &slots_snap, epoch, last_vote));
    if let Err(e) = written {
        let _ = fs::remove_file(&tmp);
        state.last_saved_fingerprint.store(0, Ordering::SeqCst);
        state.saves.in_flight.fetch_sub(1, Ordering::SeqCst);
        state.saves.note_failed(generation, &e);
        return Err(e);
    }
    let deferred = DEFER_SYNC.with(|d| d.get());
    let result = match publish {
        // A change to publish is synced and put in place with the lock held,
        // so that a sync that fails publishes nothing, and no other save is
        // put in place before it is published.
        Some(publish) => {
            let placed = (|| {
                if !deferred {
                    sync_file(&tmp)?;
                }
                // For the tests of a crash between the write and the rename.
                crate::aof::failpoint_noting("nodes-conf-written", &tmp.to_string_lossy());
                put_in_place(&tmp, path)?;
                state.saves.in_place.store(generation, Ordering::SeqCst);
                if !deferred {
                    sync_dir(path)?;
                    // Only once it is on disk: a save of the same topology
                    // made later is skipped on the strength of it.
                    state
                        .last_saved_fingerprint
                        .store(fingerprint, Ordering::SeqCst);
                }
                Ok(())
            })();
            match placed {
                Ok(()) if !deferred => {
                    state.saves.in_flight.fetch_sub(1, Ordering::SeqCst);
                    state.saves.note_saved(generation, true);
                    publish();
                }
                Ok(()) => {
                    // In place, not synced yet: the fingerprint of the save
                    // before it does not tell what the file holds.
                    state.last_saved_fingerprint.store(0, Ordering::SeqCst);
                    publish();
                }
                Err(ref e) => {
                    state.last_saved_fingerprint.store(0, Ordering::SeqCst);
                    state.saves.in_flight.fetch_sub(1, Ordering::SeqCst);
                    let _ = fs::remove_file(&tmp);
                    state.saves.note_failed(generation, e);
                }
            }
            drop(saving);
            if placed.is_ok() && deferred {
                // The file and its rename, synced on another thread: see
                // `with_deferred_sync`. Until they are, the save is in
                // flight, and does not count as done for a save something
                // waits on (a vote): that waits for this sync, or makes a
                // save of its own. No fingerprint is kept for it, so the
                // next save is written whatever it holds.
                let path = path.clone();
                let saves = std::sync::Arc::clone(&state.saves);
                crate::cpus::spawn(move || {
                    let synced = sync_file(&path).and_then(|()| sync_dir(&path));
                    saves.in_flight.fetch_sub(1, Ordering::SeqCst);
                    match synced {
                        Ok(()) => saves.note_saved(generation, true),
                        Err(e) => saves.note_failed(generation, &e),
                    }
                });
            }
            placed
        }
        // Synced without the lock, then put in place unless a newer save
        // has been meanwhile.
        None => {
            drop(saving);
            let placed = (|| {
                sync_file(&tmp)?;
                crate::aof::failpoint_noting("nodes-conf-written", &tmp.to_string_lossy());
                let saving = state.save_lock.lock();
                if state.saves.in_place.load(Ordering::SeqCst) < generation {
                    put_in_place(&tmp, path)?;
                    state.saves.in_place.store(generation, Ordering::SeqCst);
                    drop(saving);
                    sync_dir(path)?;
                    // Only once it is on disk, and if it is still the newest
                    // in place: a save of the same topology made later is
                    // skipped on the strength of it.
                    let _saving = state.save_lock.lock();
                    if state.saves.in_place.load(Ordering::SeqCst) == generation {
                        state
                            .last_saved_fingerprint
                            .store(fingerprint, Ordering::SeqCst);
                    }
                    Ok(true)
                } else {
                    // A newer save is in place: it has all this one had,
                    // and it notes itself saved once it is synced.
                    drop(saving);
                    let _ = fs::remove_file(&tmp);
                    Ok(false)
                }
            })();
            match &placed {
                Ok(true) => {
                    state.saves.in_flight.fetch_sub(1, Ordering::SeqCst);
                    state.saves.note_saved(generation, true);
                }
                Ok(false) => {
                    state.saves.in_flight.fetch_sub(1, Ordering::SeqCst);
                    state.saves.signal();
                }
                Err(e) => {
                    // Reset before it leaves `in_flight`: see the skip above.
                    state.last_saved_fingerprint.store(0, Ordering::SeqCst);
                    state.saves.in_flight.fetch_sub(1, Ordering::SeqCst);
                    let _ = fs::remove_file(&tmp);
                    state.saves.note_failed(generation, e);
                }
            }
            match placed {
                // Overtaken: done once the newer save is synced, and not
                // before, so that what waits on this one (a command's reply)
                // is not answered before its change is on disk.
                Ok(false) => wait_synced(
                    &state.saves,
                    generation,
                    std::time::Instant::now() + OVERTAKEN_SAVE_WAIT,
                )
                .map_err(std::io::Error::other),
                placed => placed.map(|_| ()),
            }
        }
    };
    if asked.elapsed() > std::time::Duration::from_millis(100) {
        log_info!(
            "Cluster: slow nodes.conf save: {locked} ms for the lock, {} ms in all",
            asked.elapsed().as_millis()
        );
    }
    result
}

/// Where this node's saves of nodes.conf stand: see [`save_from_state`].
#[derive(Default)]
pub struct Saves {
    /// Saves begun, numbered from 1.
    pub(crate) written: AtomicU64,
    /// The newest put in place; an older one is not put in place over it.
    pub(crate) in_place: AtomicU64,
    /// Written and not yet put in place, failed or given up.
    pub(crate) in_flight: AtomicU64,
    /// The newest save that is on disk, synced (or found to be what is).
    synced: AtomicU64,
    /// Saves something waits on (a vote, a node met) being made: the bus's
    /// own wait for them, or the disk's journal commits the two in turn.
    pub(crate) urgent: AtomicU64,
    /// Whether the last save written failed (see `note_failed`).
    failing: std::sync::atomic::AtomicBool,
    /// The newest that failed, and why.
    failed: parking_lot::Mutex<(u64, String)>,
    /// Signalled whenever a save ends.
    ended: parking_lot::Condvar,
    ended_lock: parking_lot::Mutex<()>,
}

impl Saves {
    /// `written`: the save was written and put in place (not skipped as
    /// unchanged), and so tells that saves work again.
    fn note_saved(&self, generation: u64, written: bool) {
        self.synced.fetch_max(generation, Ordering::SeqCst);
        if written && self.failing.swap(false, Ordering::SeqCst) {
            log_warn!("Cluster: the cluster config file is saved again");
        }
        self.signal();
    }

    /// Logged once, as an error, when saves start failing (a disk that is
    /// full or read-only fails every one, and the bus saves often), and once
    /// more when one works again.
    fn note_failed(&self, generation: u64, e: &std::io::Error) {
        {
            let mut failed = self.failed.lock();
            if generation > failed.0 {
                *failed = (generation, e.to_string());
            }
        }
        if !self.failing.swap(true, Ordering::SeqCst) {
            log_error!(
                "Cluster: could not save the cluster config file: {e}. Until it can be, \
                 changes of topology are not on disk, no node met is made a member, and no \
                 vote is cast"
            );
        }
        self.signal();
    }

    fn signal(&self) {
        let _held = self.ended_lock.lock();
        self.ended.notify_all();
    }
}

/// Wait until a save numbered from `from` on is in place, synced, and make
/// one, here, if none has begun: `Err` with the reason if the first such
/// save fails, or none is done by `deadline`. For a node to be admitted
/// (`ClusterState::admit_met_node`): every node waiting is in the save
/// begun next, so a burst of `CLUSTER MEET`s is saved in a few writes, not
/// one each, and one is not kept waiting behind the bus's saves.
pub fn wait_for_save(
    state: &crate::cluster::ClusterState,
    from: u64,
    deadline: std::time::Instant,
) -> Result<(), String> {
    if state.config_path.is_none() {
        return Ok(());
    }
    let saves = &state.saves;
    if saves.written.load(Ordering::SeqCst) < from {
        // Its outcome is noted in `saves`, below.
        saves.urgent.fetch_add(1, Ordering::SeqCst);
        state.refresh_state();
        let _ = save_from_state(state);
        saves.urgent.fetch_sub(1, Ordering::SeqCst);
    }
    wait_synced(saves, from, deadline)
}

/// How long a save overtaken by a newer one (see `save_then`) waits for
/// that one to be synced.
const OVERTAKEN_SAVE_WAIT: std::time::Duration = std::time::Duration::from_secs(20);

/// Wait until a save numbered from `from` on is in place, synced: `Err`
/// with the reason if the first such save to end fails, or none is done by
/// `deadline`.
fn wait_synced(saves: &Saves, from: u64, deadline: std::time::Instant) -> Result<(), String> {
    let mut held = saves.ended_lock.lock();
    loop {
        if saves.synced.load(Ordering::SeqCst) >= from {
            return Ok(());
        }
        {
            let failed = saves.failed.lock();
            if failed.0 >= from {
                return Err(failed.1.clone());
            }
        }
        if saves.ended.wait_until(&mut held, deadline).timed_out()
            && saves.synced.load(Ordering::SeqCst) < from
        {
            return Err("the save did not end in time".into());
        }
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
                shard_id: generate_node_id(),
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
                shard_id: generate_node_id(),
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

    /// The shard id is kept, as Redis keeps it: `ip:port@cport,<hostname>,
    /// shard-id=<id>`. A replica is in its master's shard whatever its line
    /// says (#563).
    #[test]
    fn shard_ids_survive_a_restart() {
        let (me, replica) = (generate_node_id(), generate_node_id());
        let shard = generate_node_id();
        let mut nodes = AHashMap::new();
        let mut master = ClusterNode::new(me, "127.0.0.1".into(), 6380, 16380);
        master.flags = NodeFlags::MYSELF | NodeFlags::MASTER;
        master.hostname = "a.example".into();
        master.shard_id = shard;
        nodes.insert(me, master);
        let mut r = ClusterNode::new(replica, "127.0.0.1".into(), 6381, 16381);
        r.flags = NodeFlags::REPLICA;
        r.master_id = Some(me);
        nodes.insert(replica, r); // its own, random: not what is kept
        let slots: Box<[Option<NodeId>; CLUSTER_SLOTS]> = vec![None; CLUSTER_SLOTS]
            .into_boxed_slice()
            .try_into()
            .unwrap();

        let tmp = NamedTempFile::new().unwrap();
        save(tmp.path(), &nodes, &slots, 0, 0).unwrap();
        let written = std::fs::read_to_string(tmp.path()).unwrap();
        assert!(
            written.contains(&format!(
                "127.0.0.1:6380@16380,a.example,shard-id={} ",
                node_id_str(&shard)
            )),
            "{written}"
        );
        assert!(
            written.contains("127.0.0.1:6381@16381,,shard-id="),
            "{written}"
        );

        let loaded = load(tmp.path()).unwrap().unwrap();
        assert_eq!(loaded.nodes[&me].shard_id, shard);
        assert_eq!(loaded.nodes[&me].hostname, "a.example");
        assert_eq!(loaded.nodes[&replica].shard_id, shard);
        assert_eq!(loaded.nodes[&replica].hostname, "");
    }

    /// A shard id taken in on its own changes what is saved, so it reaches
    /// the file without waiting for another change (#563).
    #[test]
    fn a_new_shard_id_is_a_change_to_save() {
        let me = generate_node_id();
        let mut node = ClusterNode::new(me, "127.0.0.1".into(), 6380, 16380);
        node.flags = NodeFlags::MYSELF | NodeFlags::MASTER;
        let mut nodes = AHashMap::new();
        nodes.insert(me, node);
        let slots: Box<[Option<NodeId>; CLUSTER_SLOTS]> = vec![None; CLUSTER_SLOTS]
            .into_boxed_slice()
            .try_into()
            .unwrap();
        let before = topology_fingerprint(&nodes, &slots, 0, 0);
        assert_eq!(topology_fingerprint(&nodes, &slots, 0, 0), before);
        nodes.get_mut(&me).unwrap().shard_id = generate_node_id();
        assert_ne!(topology_fingerprint(&nodes, &slots, 0, 0), before);
    }

    /// A file an earlier version wrote has no shard ids: a master gets a new
    /// one, and its replica takes it, whichever line comes first.
    #[test]
    fn a_file_without_shard_ids_puts_replicas_in_their_masters_shard() {
        let (me, master) = (generate_node_id(), generate_node_id());
        let file = format!(
            "{me} 127.0.0.1:6381@16381 myself,slave {master} 0 0 0 connected\n\
             {master} 127.0.0.1:6380@16380,a.example master - 0 0 1 connected 0-99\n\
             vars currentEpoch 1 lastVoteEpoch 0\n",
            me = node_id_str(&me),
            master = node_id_str(&master),
        );
        let tmp = NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), file).unwrap();
        let loaded = load(tmp.path()).unwrap().unwrap();
        assert_eq!(loaded.nodes[&me].shard_id, loaded.nodes[&master].shard_id);
        assert_ne!(loaded.nodes[&master].shard_id, master);
        assert_eq!(loaded.nodes[&master].hostname, "a.example");
        assert_eq!(loaded.slots[0], Some(master));
    }

    /// A file is whole when it ends with its `vars` line, blank lines (the
    /// padding of a direct write) after it or not; cut short anywhere, or
    /// empty, it is refused, and the reason says so (#808).
    #[test]
    fn a_file_cut_short_is_refused() {
        let me = generate_node_id();
        let mut node = ClusterNode::new(me, "127.0.0.1".into(), 6380, 16380);
        node.flags = NodeFlags::MYSELF | NodeFlags::MASTER;
        let mut nodes = AHashMap::new();
        nodes.insert(me, node);
        let mut slots: Box<[Option<NodeId>; CLUSTER_SLOTS]> = vec![None; CLUSTER_SLOTS]
            .into_boxed_slice()
            .try_into()
            .unwrap();
        slots[0] = Some(me);
        let tmp = NamedTempFile::new().unwrap();
        save(tmp.path(), &nodes, &slots, 3, 2).unwrap();
        let whole = std::fs::read_to_string(tmp.path()).unwrap();
        assert!(whole.contains("vars currentEpoch 3 lastVoteEpoch 2\n"));
        let padded = format!("{whole}\n\n\n");
        std::fs::write(tmp.path(), &padded).unwrap();
        assert_eq!(load(tmp.path()).unwrap().unwrap().last_vote_epoch, 2);

        let vars = whole.find("vars ").unwrap();
        let line_end = whole[..vars - 1].rfind('\n').map_or(0, |i| i + 1);
        // Its newline: on Linux, blank lines of padding follow.
        let vars_end = vars + whole[vars..].find('\n').unwrap();
        for cut in [
            "",
            "\n\n",
            &whole[..vars],
            &whole[..vars + 5],
            &whole[..vars_end],
            &whole[..line_end + 10],
        ] {
            std::fs::write(tmp.path(), cut).unwrap();
            let e = load(tmp.path())
                .err()
                .unwrap_or_else(|| panic!("{cut:?} loaded"));
            let e = e.to_string();
            assert!(
                e.contains("incomplete") || e.contains("empty"),
                "{cut:?}: {e}"
            );
        }
    }

    /// Every save writes a file of its own beside nodes.conf; those a crash
    /// left, and 1.2's `nodes.conf.tmp`, are removed at startup, and nothing
    /// else is.
    #[test]
    fn leftover_temporary_files_are_removed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nodes.conf");
        assert_eq!(temp_path(&path, 7), dir.path().join("nodes.conf.tmp-7"));
        for name in [
            "nodes.conf",
            "nodes.conf.tmp",
            "nodes.conf.tmp-1",
            "nodes.conf.tmp-42",
            "nodes.conf.tmp-x",
            "other.conf.tmp-1",
            "nodes.conf.bak",
        ] {
            std::fs::write(dir.path().join(name), "x").unwrap();
        }
        remove_leftover_temps(&path);
        let mut left: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        left.sort();
        assert_eq!(
            left,
            [
                "nodes.conf",
                "nodes.conf.bak",
                "nodes.conf.tmp-x",
                "other.conf.tmp-1"
            ]
        );
    }

    /// A directory sync the filesystem does not support (`EINVAL`,
    /// `ENOTSUP`) lets the save stand; any other error fails it.
    #[test]
    fn a_directory_sync_the_filesystem_does_not_support_lets_the_save_stand() {
        use std::io::{Error, ErrorKind};
        assert!(dir_sync_outcome(Ok(())).is_ok());
        assert!(dir_sync_outcome(Err(Error::from(ErrorKind::InvalidInput))).is_ok());
        assert!(dir_sync_outcome(Err(Error::from(ErrorKind::Unsupported))).is_ok());
        #[cfg(unix)]
        for code in [libc::EINVAL, libc::ENOTSUP, libc::EOPNOTSUPP] {
            assert!(dir_sync_outcome(Err(Error::from_raw_os_error(code))).is_ok());
        }
        #[cfg(unix)]
        for code in [libc::EIO, libc::ENOSPC, libc::EROFS, libc::EACCES] {
            let e = dir_sync_outcome(Err(Error::from_raw_os_error(code))).unwrap_err();
            assert_eq!(e.raw_os_error(), Some(code));
        }
        assert!(dir_sync_outcome(Err(Error::other("a test's"))).is_err());
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
