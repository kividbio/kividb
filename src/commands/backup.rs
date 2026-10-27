//! `BACKUP` (Redis 8.10): an online backup in two parts.
//!
//! ```text
//! BACKUP START     open a backup: write a BASE snapshot, start collecting INCR
//! BACKUP LIST      the immutable files so far (BASE; after SEAL also INCR + manifest)
//! BACKUP SEAL      freeze it: the backup now holds everything up to this moment
//! BACKUP CLEANUP   remove a sealed (or failed) backup's files
//! BACKUP ABORT     cancel a backup that has not been sealed
//! BACKUP STATUS    state, error, start and end time
//! ```
//!
//! A sealed backup is three files in the backup directory:
//!
//! * **BASE** — `appendonly.aof.N.base.kdb`: the dataset at the instant
//!   `BACKUP START` took its cut, in KiviDB's snapshot format.
//! * **INCR** — `appendonly.aof.N.incr.aof`: every write from that instant up
//!   to `BACKUP SEAL`, as commands.
//! * **Manifest** — `appendonly.aof.manifest`, naming the two.
//!
//! Restoring loads BASE and replays INCR (`preload-file aof:<manifest>`), so
//! the restored dataset is the one that existed at the seal, not at the start.
//! Because start and seal are separate, a control plane can start the backups
//! of a cluster's nodes one at a time and then seal them together.
//!
//! # How it is taken
//!
//! Redis builds this on its multi-part AOF and a forked rewrite. Here the same
//! stream that feeds a replica feeds a backup: under one closing of the write
//! gate the dataset is frozen *and* a tap is attached to the write stream, so
//! the snapshot and the stream meet exactly — no write is in both, none in
//! neither. That is the property a full resync relies on
//! (see `replication::begin_full_sync`), reused. It works with AOF on or off,
//! and on a replica as well as on a master.

use crate::commands::ctx::Ctx;
use crate::replication::{ReplPayload, ReplState};
use crate::resp::write::*;
use parking_lot::Mutex;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub const MANIFEST_NAME: &str = "appendonly.aof.manifest";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Phase {
    Idle,
    /// The BASE snapshot is being written.
    Snapshotting,
    /// BASE is done and listed; writes are accumulating in INCR.
    Incrementing,
    /// Complete and immutable.
    Sealed,
    /// Failed or aborted; a new backup may be started.
    Failed,
}

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Phase::Idle => "idle",
            Phase::Snapshotting => "snapshotting",
            Phase::Incrementing => "incrementing",
            Phase::Sealed => "sealed",
            Phase::Failed => "failed",
        }
    }
}

struct Backup {
    phase: Phase,
    error: String,
    start_time: u64,
    end_time: u64,
    /// Which backup this is; names its files and tells a stale worker or
    /// timer that the backup it belonged to is gone.
    seq: u64,
    dir: PathBuf,
    /// Files that are complete and will not change, in the order to list them.
    files: Vec<PathBuf>,
    /// The stream tap feeding this backup's INCR, while it is open.
    tap: Option<u64>,
    /// Finished collecting INCR: the outcome, for whoever is sealing.
    incr_done: Option<std::sync::mpsc::Receiver<std::io::Result<()>>>,
}

static BACKUP: std::sync::LazyLock<Mutex<Backup>> = std::sync::LazyLock::new(|| {
    Mutex::new(Backup {
        phase: Phase::Idle,
        error: String::new(),
        start_time: 0,
        end_time: 0,
        seq: 0,
        dir: PathBuf::new(),
        files: Vec::new(),
        tap: None,
        incr_done: None,
    })
});

static NEXT_SEQ: AtomicU64 = AtomicU64::new(1);
static NEXT_TAP: AtomicU64 = AtomicU64::new(1);

/// `backup_in_progress` for `INFO persistence`.
static IN_PROGRESS: AtomicBool = AtomicBool::new(false);

pub(crate) fn in_progress() -> bool {
    IN_PROGRESS.load(Ordering::Relaxed)
}

fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn base_name(seq: u64) -> String {
    format!("appendonly.aof.{seq}.base.kdb")
}

fn incr_name(seq: u64) -> String {
    format!("appendonly.aof.{seq}.incr.aof")
}

/// The INCR file while it is still being written. Not in `BACKUP LIST`, which
/// reports only files that will not change.
fn incr_partial(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!(".{}.partial", incr_name(seq)))
}

fn manifest_text(seq: u64) -> String {
    format!(
        "file {} seq {seq} type b\nfile {} seq {seq} type i\n",
        base_name(seq),
        incr_name(seq)
    )
}

/// The files a manifest names: `(base, incr)`, relative to the manifest.
pub fn parse_manifest(text: &str) -> std::io::Result<(String, Option<String>)> {
    let mut base = None;
    let mut incr = None;
    for line in text.lines() {
        let mut words = line.split_whitespace();
        if words.next() != Some("file") {
            continue;
        }
        let Some(name) = words.next() else { continue };
        // `seq N type b|i`, in either order.
        let rest: Vec<&str> = words.collect();
        let kind = rest
            .windows(2)
            .find(|pair| pair[0] == "type")
            .map(|pair| pair[1]);
        match kind {
            Some("b") => base = Some(name.to_string()),
            Some("i") => incr = Some(name.to_string()),
            _ => {}
        }
    }
    match base {
        Some(base) => Ok((base, incr)),
        None => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "backup manifest names no base file",
        )),
    }
}

fn write_durably(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

fn absolute(path: &Path) -> PathBuf {
    std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf())
}

/// Detach a tap from the stream. Taken under the write gate so that the
/// stream the tap has seen ends between two whole writes.
fn close_tap(repl_state: &ReplState, tap: u64) {
    let _cut = crate::write_gate::cut();
    repl_state.detach_tap(tap);
}

/// Record a failure, unless the backup has moved on without this caller.
fn fail(seq: u64, error: String) {
    let mut backup = BACKUP.lock();
    if backup.seq == seq && matches!(backup.phase, Phase::Snapshotting | Phase::Incrementing) {
        backup.phase = Phase::Failed;
        backup.error = error;
        backup.tap = None;
        IN_PROGRESS.store(false, Ordering::Relaxed);
    }
}

/// What the worker thread needs from the server.
struct Handles {
    store: crate::store::Store,
    expiry_store: crate::store::ExpiryStore,
    index_store: crate::vector::IndexStore,
    repl_state: Arc<ReplState>,
}

/// The backup's worker: write BASE, then append the stream to INCR until the
/// tap is closed.
fn run_backup(handles: Handles, seq: u64, dir: PathBuf, tap: u64) {
    let (tx, rx) = std::sync::mpsc::channel::<ReplPayload>();
    let (done_tx, done_rx) = std::sync::mpsc::channel::<std::io::Result<()>>();

    // ── The cut ──────────────────────────────────────────────────────────
    // With the write gate closed: attach the tap and capture the dataset.
    // Everything already written is in the capture; everything written after
    // goes to the tap.
    let frozen = {
        let _cut = crate::write_gate::cut();
        handles.repl_state.attach_tap(tap, tx);
        crate::snapshot::freeze_dataset(
            &handles.store,
            Some(&handles.expiry_store),
            Some(&handles.index_store),
        )
    };
    {
        let mut backup = BACKUP.lock();
        if backup.seq != seq || backup.phase != Phase::Snapshotting {
            // Aborted while we were getting started.
            drop(backup);
            close_tap(&handles.repl_state, tap);
            return;
        }
        backup.tap = Some(tap);
        backup.incr_done = Some(done_rx);
    }

    // ── BASE ─────────────────────────────────────────────────────────────
    let base_path = dir.join(base_name(seq));
    let written = frozen
        .and_then(|frozen| frozen.into_payload())
        .and_then(|image| write_durably(&base_path, &image));
    if let Err(e) = written {
        close_tap(&handles.repl_state, tap);
        return fail(seq, format!("could not write the base snapshot: {e}"));
    }
    {
        let mut backup = BACKUP.lock();
        if backup.seq != seq || backup.phase != Phase::Snapshotting {
            // Aborted while BASE was being written: it is nobody's now.
            drop(backup);
            let _ = std::fs::remove_file(&base_path);
            close_tap(&handles.repl_state, tap);
            return;
        }
        backup.files.push(absolute(&base_path));
        backup.phase = Phase::Incrementing;
    }

    // ── INCR ─────────────────────────────────────────────────────────────
    // Until the tap is closed (SEAL or ABORT), which ends the channel.
    let collect = || -> std::io::Result<()> {
        let mut incr = std::io::BufWriter::new(std::fs::File::create(incr_partial(&dir, seq))?);
        // Ends when the tap is detached and the channel has been drained.
        for payload in rx.iter() {
            incr.write_all(&payload)?;
        }
        incr.flush()?;
        incr.get_ref().sync_all()
    };
    let outcome = collect();
    if let Err(e) = &outcome {
        // The disk failed mid-backup. Stop taking the stream.
        close_tap(&handles.repl_state, tap);
        fail(seq, format!("could not write the incremental file: {e}"));
    }
    let _ = done_tx.send(outcome);
}

fn start(ctx: &Ctx) -> Vec<u8> {
    let dir = PathBuf::from(&ctx.config.backupdirname);
    let mut backup = BACKUP.lock();
    match backup.phase {
        Phase::Idle => {}
        // What a failed or aborted backup left behind is not a backup.
        Phase::Failed => remove_files(&mut backup),
        Phase::Sealed => {
            return resp_error(
                "ERR a sealed backup is waiting for BACKUP CLEANUP before another can start",
            );
        }
        Phase::Snapshotting | Phase::Incrementing => {
            return resp_error("ERR a backup is already in progress");
        }
    }
    // The directory must be empty: what is in it afterwards is the backup.
    if let Err(e) = std::fs::create_dir_all(&dir) {
        return resp_error(&format!(
            "ERR cannot create the backup directory '{}': {e}",
            dir.display()
        ));
    }
    match std::fs::read_dir(&dir) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                return resp_error(&format!(
                    "ERR the backup directory '{}' is not empty",
                    dir.display()
                ));
            }
        }
        Err(e) => {
            return resp_error(&format!(
                "ERR cannot read the backup directory '{}': {e}",
                dir.display()
            ));
        }
    }

    let seq = NEXT_SEQ.fetch_add(1, Ordering::Relaxed);
    *backup = Backup {
        phase: Phase::Snapshotting,
        error: String::new(),
        start_time: unix_secs(),
        end_time: 0,
        seq,
        dir: dir.clone(),
        files: Vec::new(),
        tap: None,
        incr_done: None,
    };
    IN_PROGRESS.store(true, Ordering::Relaxed);
    drop(backup);

    let handles = Handles {
        store: Arc::clone(ctx.store),
        expiry_store: Arc::clone(ctx.expiry_store),
        index_store: Arc::clone(ctx.index_store),
        repl_state: Arc::clone(ctx.repl_state),
    };
    let tap = NEXT_TAP.fetch_add(1, Ordering::Relaxed);
    let spawned = std::thread::Builder::new()
        .name("kividb-backup".into())
        .spawn(move || run_backup(handles, seq, dir, tap));
    if let Err(e) = spawned {
        fail(seq, format!("could not start the backup thread: {e}"));
        return resp_error(&format!("ERR could not start the backup: {e}"));
    }
    resp_simple("OK")
}

fn seal(ctx: &Ctx) -> Vec<u8> {
    let (seq, dir, tap, done) = {
        let mut backup = BACKUP.lock();
        match backup.phase {
            Phase::Incrementing => {}
            Phase::Snapshotting => {
                return resp_error("ERR the base snapshot is still being written; try again");
            }
            _ => return resp_error("ERR no backup is in progress"),
        }
        let (Some(tap), Some(done)) = (backup.tap.take(), backup.incr_done.take()) else {
            return resp_error("ERR the backup is already being sealed");
        };
        (backup.seq, backup.dir.clone(), tap, done)
    };

    // Close the stream at a clean boundary and wait for the worker to have
    // written, flushed and fsynced everything up to it.
    close_tap(ctx.repl_state, tap);
    let collected = done
        .recv_timeout(std::time::Duration::from_secs(120))
        .map_err(|_| std::io::Error::other("timed out waiting for the incremental file"))
        .and_then(|outcome| outcome);

    let incr_path = dir.join(incr_name(seq));
    let manifest_path = dir.join(MANIFEST_NAME);
    let sealed = collected
        .and_then(|()| std::fs::rename(incr_partial(&dir, seq), &incr_path))
        .and_then(|()| write_durably(&manifest_path, manifest_text(seq).as_bytes()));
    if let Err(e) = sealed {
        fail(seq, format!("could not seal the backup: {e}"));
        // `fail` only acts on a backup still in progress, which this is.
        return resp_error(&format!("ERR could not seal the backup: {e}"));
    }

    let ttl = sealed_ttl(ctx.config);
    {
        let mut backup = BACKUP.lock();
        if backup.seq != seq {
            return resp_error("ERR the backup was replaced while it was being sealed");
        }
        backup.files.push(absolute(&incr_path));
        backup.files.push(absolute(&manifest_path));
        backup.phase = Phase::Sealed;
        backup.end_time = unix_secs();
        IN_PROGRESS.store(false, Ordering::Relaxed);
    }
    if ttl > 0 {
        remove_after(seq, std::time::Duration::from_secs(ttl));
    }
    resp_simple("OK")
}

/// `backup-sealed-ttl`: clean the sealed backup `seq` up by ourselves, after
/// `wait`, if nobody has.
fn remove_after(seq: u64, wait: std::time::Duration) {
    let _ = std::thread::Builder::new()
        .name("kividb-backup-ttl".into())
        .spawn(move || {
            std::thread::sleep(wait);
            let mut backup = BACKUP.lock();
            if backup.seq == seq && backup.phase == Phase::Sealed {
                remove_files(&mut backup);
            }
        });
}

/// At startup: the backup the directory holds, from before a restart (#150).
/// Its state was only in memory, so a sealed backup could neither be cleaned
/// up nor make way for another, and its `backup-sealed-ttl` never ran out.
///
/// A manifest whose base and incremental files are there is a sealed backup,
/// sealed when the manifest was written: `BACKUP LIST` lists it, `BACKUP
/// CLEANUP` removes it, and the TTL counts from its seal. Any other file a
/// backup writes is what one the restart cut short left behind: a failed
/// backup, which `BACKUP CLEANUP` or the next `BACKUP START` removes.
///
/// Called by the server binary only: the library's in-process test servers
/// share this state, and take up no backup from disk.
#[allow(dead_code)]
pub(crate) fn recover(config: &crate::config::Config) {
    let dir = PathBuf::from(&config.backupdirname);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return;
    };
    let leftovers = entries.flatten().any(|entry| {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        name.starts_with("appendonly.aof.") || name.starts_with(".appendonly.aof.")
    });
    if !leftovers {
        return;
    }
    let modified = |path: &Path| {
        std::fs::metadata(path)
            .and_then(|meta| meta.modified())
            .ok()
            .and_then(|at| at.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0, |at| at.as_secs())
    };
    let manifest_path = dir.join(MANIFEST_NAME);
    let sealed = std::fs::read_to_string(&manifest_path)
        .and_then(|text| parse_manifest(&text))
        .ok()
        .and_then(|(base, incr)| {
            let seq = base
                .strip_prefix("appendonly.aof.")?
                .strip_suffix(".base.kdb")?
                .parse::<u64>()
                .ok()?;
            let (base, incr) = (dir.join(base), dir.join(incr?));
            (base.is_file() && incr.is_file()).then_some((seq, base, incr))
        });
    let mut backup = BACKUP.lock();
    match sealed {
        Some((seq, base, incr)) => {
            NEXT_SEQ.fetch_max(seq + 1, Ordering::Relaxed);
            *backup = Backup {
                phase: Phase::Sealed,
                error: String::new(),
                start_time: modified(&base),
                end_time: modified(&manifest_path),
                seq,
                dir,
                files: vec![absolute(&base), absolute(&incr), absolute(&manifest_path)],
                tap: None,
                incr_done: None,
            };
            let ttl = sealed_ttl(config);
            if ttl > 0 {
                let age = unix_secs().saturating_sub(backup.end_time);
                remove_after(seq, std::time::Duration::from_secs(ttl.saturating_sub(age)));
            }
            log_info!(
                "Backup: a sealed backup is in '{}' (BACKUP LIST, BACKUP CLEANUP)",
                backup.dir.display()
            );
        }
        None => {
            backup.phase = Phase::Failed;
            backup.error = "interrupted by a restart".into();
            backup.dir = dir;
            log_warn!(
                "Backup: '{}' holds a backup that a restart interrupted; BACKUP CLEANUP or the \
                 next BACKUP START removes it",
                backup.dir.display()
            );
        }
    }
}

fn abort(ctx: &Ctx) -> Vec<u8> {
    let tap = {
        let mut backup = BACKUP.lock();
        if !matches!(backup.phase, Phase::Snapshotting | Phase::Incrementing) {
            return resp_error(match backup.phase {
                Phase::Sealed => "ERR the backup is sealed; use BACKUP CLEANUP",
                _ => "ERR no backup is in progress",
            });
        }
        backup.phase = Phase::Failed;
        backup.error = "aborted".into();
        backup.incr_done = None;
        IN_PROGRESS.store(false, Ordering::Relaxed);
        backup.tap.take()
    };
    // A worker still writing BASE closes its own tap when it sees the abort.
    if let Some(tap) = tap {
        close_tap(ctx.repl_state, tap);
    }
    resp_simple("OK")
}

/// Delete the backup's files and return to idle. Only files a backup writes
/// (BASE, INCR, the manifest, and their partial and temporary forms): anything
/// else someone put in the directory since `BACKUP START` is left alone.
fn remove_files(backup: &mut Backup) {
    if !backup.dir.as_os_str().is_empty()
        && let Ok(entries) = std::fs::read_dir(&backup.dir)
    {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("appendonly.aof.") || name.starts_with(".appendonly.aof.") {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
    backup.files.clear();
    backup.phase = Phase::Idle;
    backup.error.clear();
    backup.start_time = 0;
    backup.end_time = 0;
}

fn cleanup() -> Vec<u8> {
    let mut backup = BACKUP.lock();
    match backup.phase {
        Phase::Sealed | Phase::Failed => {
            remove_files(&mut backup);
            resp_simple("OK")
        }
        Phase::Idle => resp_error("ERR there is no backup to clean up"),
        Phase::Snapshotting | Phase::Incrementing => {
            resp_error("ERR a backup is in progress; seal or abort it first")
        }
    }
}

fn list() -> Vec<u8> {
    let backup = BACKUP.lock();
    resp_array(
        backup
            .files
            .iter()
            .map(|path| resp_bulk_str(&path.to_string_lossy()))
            .collect(),
    )
}

fn status(ctx: &Ctx) -> Vec<u8> {
    let resp = crate::commands::handler::effective_resp_for_reply(ctx);
    let backup = BACKUP.lock();
    resp_map_for(
        resp,
        vec![
            resp_bulk_str("state"),
            resp_bulk_str(backup.phase.name()),
            resp_bulk_str("error"),
            resp_bulk_str(&backup.error),
            resp_bulk_str("start_time"),
            resp_bulk_str(&backup.start_time.to_string()),
            resp_bulk_str("end_time"),
            resp_bulk_str(&backup.end_time.to_string()),
        ],
    )
}

pub(super) fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    if name != "BACKUP" {
        return None;
    }
    if ctx.cmd.len() != 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'backup' command",
        ));
    }
    let sub = String::from_utf8_lossy(ctx.cmd[1]).to_ascii_uppercase();
    Some(match sub.as_str() {
        "START" => start(ctx),
        "SEAL" => seal(ctx),
        "ABORT" => abort(ctx),
        "CLEANUP" => cleanup(),
        "LIST" => list(),
        "STATUS" => status(ctx),
        "HELP" => resp_array(
            [
                "BACKUP <subcommand>. Subcommands are:",
                "START",
                "    Open a backup: write a base snapshot and start collecting writes.",
                "LIST",
                "    Return the paths of the backup files that are complete.",
                "SEAL",
                "    Freeze the backup at this moment and write its manifest.",
                "CLEANUP",
                "    Remove the files of a sealed or failed backup.",
                "ABORT",
                "    Cancel a backup that has not been sealed.",
                "STATUS",
                "    Return the backup's state, error and start/end times.",
                "HELP",
                "    Print this help.",
            ]
            .iter()
            .map(|line| resp_simple(line))
            .collect(),
        ),
        other => resp_error(&format!(
            "ERR unknown subcommand '{other}'. Try BACKUP HELP."
        )),
    })
}

/// `backup-sealed-ttl` as `CONFIG SET` left it, or as configured.
static SEALED_TTL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(u64::MAX);

pub(crate) fn sealed_ttl(config: &crate::config::Config) -> u64 {
    match SEALED_TTL.load(std::sync::atomic::Ordering::Relaxed) {
        u64::MAX => config.backup_sealed_ttl,
        set => set,
    }
}

pub(crate) fn set_sealed_ttl(seconds: u64) {
    SEALED_TTL.store(seconds, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_manifest_names_its_base_and_incremental_files() {
        let (base, incr) = parse_manifest(&manifest_text(3)).unwrap();
        assert_eq!(base, "appendonly.aof.3.base.kdb");
        assert_eq!(incr.as_deref(), Some("appendonly.aof.3.incr.aof"));

        // The Redis layout, with its fields in either order and no INCR.
        let (base, incr) = parse_manifest("file appendonly.aof.7.base.rdb type b seq 7\n").unwrap();
        assert_eq!(base, "appendonly.aof.7.base.rdb");
        assert_eq!(incr, None);

        assert!(parse_manifest("").is_err());
        assert!(parse_manifest("file x.incr.aof seq 1 type i\n").is_err());
    }

    #[test]
    fn the_partial_incremental_file_is_not_one_of_the_listed_names() {
        let dir = Path::new("backupdir");
        assert_ne!(incr_partial(dir, 1), dir.join(incr_name(1)));
        assert_eq!(Phase::Incrementing.name(), "incrementing");
    }
}
