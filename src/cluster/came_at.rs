// src/cluster/came_at.rs
//
// When each slot came to its owner, as this node knows it: the config epoch
// of the claim that moved the slot to the owner it has in this node's slot
// table. A master asked for its vote compares it with the epoch a candidate
// claims a slot of its master at (the epoch of the last claim of its master
// the candidate read in its replication stream, `followed_imports`): a slot
// that came to the master after that epoch came with keys the candidate may
// not hold (the slot was handed away and imported back while the candidate
// was cut off), and the vote is refused for it (`ClusterState::cast_vote`).
//
// Set wherever this node's table gives a slot an owner: a peer's claim taken
// in (`apply_peer_slot_claims_from`, the claim's epoch), and this node's own
// takeovers (an import's, `CLUSTER SETSLOT <slot> NODE` to itself, `CLUSTER
// ADDSLOTS`, a promotion: the epoch it takes them at). A slot given to
// another node here by hand or by the end of a handover from this node
// (`CLUSTER SETSLOT <slot> NODE <other>`, `hand_over_slots`) is noted at the
// owner's config epoch known then, provisionally: the owner's own claim of
// it, when it comes, sets it.
//
// Written down, synced, in `<cluster-config-file>.came-at`, with each slot's
// owner, before the saves of the cluster config (`persist::save_then`, as
// `.not-held`; not by a takeover's own save, whose source holds writes
// meanwhile), and read back at start-up. A slot the file does not have with
// the owner nodes.conf has (no file; a file
// that does not read; one written before the save that moved the slot) is
// taken to have come at its owner's config epoch in nodes.conf: no earlier
// than it did, so a candidate that read its master's newest claim is not
// refused, and one whose last claim read is older than its master's epoch
// is.

use super::{CLUSTER_SLOTS, ClusterState, NodeId, migration, node_id_str};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

pub(crate) struct CameAt {
    inner: Mutex<Inner>,
    /// How many slots are provisional: none, and a claim of slots their
    /// owner holds here takes no lock ([`Self::owner_claimed`]).
    provisional_count: AtomicUsize,
    /// Read at start-up: saves made before do not write the file.
    loaded: AtomicBool,
}

struct Inner {
    epochs: Vec<u64>,
    /// Noted at a lower bound of the epoch the slot came at (it was given
    /// to its owner here, by hand or at the end of a handover from this
    /// node, or read back without the file having it), until the owner's
    /// own claim of it says when.
    provisional: Vec<bool>,
}

impl Inner {
    fn set_provisional(&mut self, i: usize, value: bool, count: &AtomicUsize) {
        if self.provisional[i] != value {
            self.provisional[i] = value;
            if value {
                count.fetch_add(1, Ordering::SeqCst);
            } else {
                count.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }
}

impl Default for CameAt {
    fn default() -> Self {
        CameAt {
            inner: Mutex::new(Inner {
                epochs: vec![0; CLUSTER_SLOTS],
                provisional: vec![false; CLUSTER_SLOTS],
            }),
            provisional_count: AtomicUsize::new(0),
            loaded: AtomicBool::new(false),
        }
    }
}

/// The last line of the file: one without it was cut short.
const END: &str = "end";

impl CameAt {
    /// `slots` came to their owner at `epoch`.
    pub(crate) fn set(&self, slots: impl IntoIterator<Item = u16>, epoch: u64) {
        let mut inner = self.inner.lock();
        for slot in slots {
            let i = slot as usize;
            if i < CLUSTER_SLOTS {
                inner.epochs[i] = epoch;
                inner.set_provisional(i, false, &self.provisional_count);
            }
        }
    }

    /// `slot` was given to an owner here, which will claim it at `epoch` or
    /// later: a lower bound of when it came (see [`provisional_epoch`]),
    /// until the owner's own claim of it says when ([`Self::owner_claimed`]).
    pub(crate) fn set_provisional(&self, slot: u16, epoch: u64) {
        let mut inner = self.inner.lock();
        let i = slot as usize;
        if i < CLUSTER_SLOTS {
            inner.epochs[i] = epoch;
            inner.set_provisional(i, true, &self.provisional_count);
        }
    }

    /// The owner of the slots `start..=end` that `owners` gives it claims
    /// them at `epoch`: each provisional one came at the claim's epoch (no
    /// earlier than the lower bound noted: a claim of the slot made after
    /// it came is at that epoch or later). One lock for the range, none
    /// while no slot is provisional.
    pub(crate) fn owner_claimed(
        &self,
        owners: &[Option<NodeId>],
        owner: NodeId,
        start: u16,
        end: u16,
        epoch: u64,
    ) {
        if self.provisional_count.load(Ordering::SeqCst) == 0 {
            return;
        }
        let mut inner = self.inner.lock();
        let end = (end as usize).min(CLUSTER_SLOTS - 1);
        for i in start as usize..=end {
            if inner.provisional[i] && owners[i] == Some(owner) {
                inner.epochs[i] = inner.epochs[i].max(epoch);
                inner.set_provisional(i, false, &self.provisional_count);
            }
        }
    }

    pub(crate) fn get(&self, slot: u16) -> u64 {
        self.inner
            .lock()
            .epochs
            .get(slot as usize)
            .copied()
            .unwrap_or(0)
    }

    /// `CLUSTER RESET`.
    pub(crate) fn clear(&self) {
        let mut inner = self.inner.lock();
        inner.epochs.iter_mut().for_each(|e| *e = 0);
        inner.provisional.iter_mut().for_each(|p| *p = false);
        self.provisional_count.store(0, Ordering::SeqCst);
    }

    /// The file's text, with `owners` the slot table: runs of
    /// `a-b epoch owner` for the owned slots, `a-b epoch owner provisional`
    /// for a provisional run, then `end`.
    fn text(&self, owners: &[Option<NodeId>]) -> String {
        let inner = self.inner.lock();
        let mut out = String::new();
        type Run = (usize, usize, u64, NodeId, bool);
        let flush = |run: Option<Run>, out: &mut String| {
            if let Some((a, b, e, o, p)) = run {
                let tail = if p { " provisional" } else { "" };
                out.push_str(&format!("{a}-{b} {e} {}{tail}\n", node_id_str(&o)));
            }
        };
        let mut run: Option<Run> = None;
        for (slot, owner) in owners.iter().enumerate().take(CLUSTER_SLOTS) {
            let epoch = inner.epochs[slot];
            let prov = inner.provisional[slot];
            run = match (run, owner) {
                (Some((a, b, e, o, p)), Some(owner))
                    if e == epoch && o == *owner && p == prov && b + 1 == slot =>
                {
                    Some((a, slot, e, o, p))
                }
                (prev, Some(owner)) => {
                    flush(prev, &mut out);
                    Some((slot, slot, epoch, *owner, prov))
                }
                (prev, None) => {
                    flush(prev, &mut out);
                    None
                }
            };
        }
        flush(run, &mut out);
        out.push_str(END);
        out.push('\n');
        out
    }
}

/// When a slot came, to which owner, and whether that is provisional.
type Came = (u64, NodeId, bool);

/// The file read back: for each slot, when it came and to which owner, or
/// `Err` if it does not read.
fn parse(text: &str) -> Result<Vec<Option<Came>>, ()> {
    let mut came = vec![None; CLUSTER_SLOTS];
    let mut ended = false;
    for line in text.lines() {
        if ended {
            return Err(());
        }
        if line == END {
            ended = true;
            continue;
        }
        let mut words = line.split(' ');
        let (Some(range), Some(epoch), Some(owner), prov, None) = (
            words.next(),
            words.next(),
            words.next(),
            words.next(),
            words.next(),
        ) else {
            return Err(());
        };
        let prov = match prov {
            None => false,
            Some("provisional") => true,
            Some(_) => return Err(()),
        };
        let epoch: u64 = epoch.parse().map_err(|_| ())?;
        let owner = super::node::parse_node_id(owner.as_bytes()).ok_or(())?;
        let (a, b) = range.split_once('-').ok_or(())?;
        let (a, b): (usize, usize) = (a.parse().map_err(|_| ())?, b.parse().map_err(|_| ())?);
        if a > b || b >= CLUSTER_SLOTS {
            return Err(());
        }
        came[a..=b]
            .iter_mut()
            .for_each(|c| *c = Some((epoch, owner, prov)));
    }
    if !ended {
        return Err(());
    }
    Ok(came)
}

fn path(cluster: &ClusterState) -> Option<std::path::PathBuf> {
    cluster.config_path.as_ref().map(|p| {
        let mut name = p.as_os_str().to_owned();
        name.push(".came-at");
        std::path::PathBuf::from(name)
    })
}

/// The text last written, or read back at start-up: a save that would write
/// the same writes nothing. Held only to compare and to note.
static WRITTEN: Mutex<Option<String>> = Mutex::new(None);

/// One writer of the file at a time (a save of the bus's, a command's).
static WRITING: Mutex<()> = Mutex::new(());

/// The file written before it is put in place.
fn tmp_path(path: &std::path::Path) -> std::path::PathBuf {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    std::path::PathBuf::from(tmp)
}

/// Write the file down, synced, if it changed since it was last written.
pub(crate) fn save(cluster: &ClusterState) {
    let Some(path) = path(cluster) else {
        return;
    };
    if !cluster.came_at.loaded.load(Ordering::SeqCst) {
        return;
    }
    let _writing = WRITING.lock();
    // The table before the epochs, the order the two are taken in where a
    // slot moves (`apply_peer_slot_claims_from`).
    let owners: Vec<Option<NodeId>> = cluster.slots.read().to_vec();
    let text = cluster.came_at.text(&owners);
    if WRITTEN.lock().as_deref() == Some(text.as_str()) {
        return;
    }
    match write_file(&path, &text) {
        Ok(()) => *WRITTEN.lock() = Some(text),
        Err(e) => {
            // The file in place is older than what is noted here: no
            // longer to be read back as this node's history.
            *WRITTEN.lock() = None;
            log_warn!(
                "Cluster: could not write down when each slot came to its owner ({}): {e}; \
                 the file in place is emptied, and read back as none",
                path.display()
            );
        }
    }
}

/// Write `text` to `path`, synced, by way of a temporary file. If that
/// fails, the file in place is emptied (best effort): one that does not
/// read is read back as no file, each slot at its owner's config epoch,
/// never as a history older than this node's.
fn write_file(path: &std::path::Path, text: &str) -> std::io::Result<()> {
    let tmp = tmp_path(path);
    let saved = (|| {
        use std::io::Write;
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&tmp, path)
    })();
    if saved.is_err()
        && let Ok(file) = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(path)
    {
        let _ = file.sync_all();
    }
    if let Some(dir) = path.parent()
        && let Ok(dir) = std::fs::File::open(dir)
    {
        let _ = dir.sync_all();
    }
    saved
}

/// The lower bound noted for a slot given here to `new_owner` from
/// `previous` (`CLUSTER SETSLOT <slot> NODE`, the end of a handover from
/// this node), with `epochs` each node's config epoch here: the new owner's
/// claim of it is at its config epoch, which is no older than this node
/// knows, and above the previous owner's (a claim is taken in only over an
/// owner at an older epoch).
pub(crate) fn provisional_epoch(
    epochs: &ahash::AHashMap<NodeId, u64>,
    new_owner: NodeId,
    previous: Option<NodeId>,
) -> u64 {
    let new = epochs.get(&new_owner).copied().unwrap_or(0);
    let above_previous = previous
        .filter(|p| *p != new_owner)
        .and_then(|p| epochs.get(&p))
        .map_or(0, |e| e + 1);
    new.max(above_previous)
}

/// At start-up, once nodes.conf is read: each slot at the epoch the file
/// has for it, if the file has it with the owner nodes.conf has; else (no
/// file, one that does not read, or another owner: the file was written
/// before a save of nodes.conf that moved the slot) at its owner's config
/// epoch in nodes.conf, which is no earlier than it came.
pub(crate) fn load(cluster: &ClusterState) {
    // A file being written when the node stopped is not the file: it is
    // removed, as a spare of nodes.conf is.
    if let Some(path) = path(cluster) {
        let _ = std::fs::remove_file(tmp_path(&path));
    }
    let read = path(cluster).and_then(|path| match std::fs::read(&path) {
        Ok(bytes) => {
            let text = String::from_utf8(bytes).ok();
            let parsed = text.as_deref().ok_or(()).and_then(parse);
            if parsed.is_ok() {
                // What it holds is what a save of the same state writes.
                *WRITTEN.lock() = text.clone();
            }
            if parsed.is_err() {
                log_warn!(
                    "Cluster: {} does not read: each slot is taken to have come to its owner at \
                     the owner's config epoch",
                    path.display()
                );
            }
            parsed.ok()
        }
        Err(_) => None,
    });
    // A slot the file does not have with its owner here is noted at that
    // owner's config epoch, provisionally: the owner's own claim of it says
    // when it came.
    let came: Vec<(u64, bool)> = {
        let owners: Vec<Option<NodeId>> = cluster.slots.read().to_vec();
        let nodes = cluster.nodes.read();
        owners
            .iter()
            .enumerate()
            .map(|(slot, o)| {
                let Some(owner) = o else {
                    return (0, false);
                };
                match read.as_ref().and_then(|came| came[slot]) {
                    Some((epoch, of, prov)) if of == *owner => (epoch, prov),
                    _ => (nodes.get(owner).map_or(0, |n| n.config_epoch), true),
                }
            })
            .collect()
    };
    {
        let mut inner = cluster.came_at.inner.lock();
        inner.epochs = came.iter().map(|(e, _)| *e).collect();
        inner.provisional = came.iter().map(|(_, p)| *p).collect();
        cluster
            .came_at
            .provisional_count
            .store(came.iter().filter(|(_, p)| *p).count(), Ordering::SeqCst);
    }
    cluster.came_at.loaded.store(true, Ordering::SeqCst);
}

/// For the tests of a voter that does not make the check, never set otherwise:
/// `KIVIDB_TEST_VOTER_SKIPS_CAME_AT`, set, has this node vote without the
/// check of [`ClusterState::moved_after_claimed`].
pub(crate) fn test_voter_skips() -> bool {
    static SET: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *SET.get_or_init(|| std::env::var_os("KIVIDB_TEST_VOTER_SKIPS_CAME_AT").is_some())
}

/// The refusals this node, a candidate, has had in its election under way
/// for slots that came to its master after the epoch it claimed them at:
/// per slot, the masters that refused it.
#[derive(Default)]
pub(crate) struct Refusals {
    epoch: u64,
    by_slot: ahash::AHashMap<u16, ahash::AHashSet<NodeId>>,
}

impl ClusterState {
    /// `voter` refused the slots `moved` (each with the epoch it says the
    /// slot came at), which this node claimed for its master in the
    /// election under way: of them, those this node gives up, the masters
    /// that refused them leaving too few others to make a quorum. A slot is
    /// kept while enough masters that have not refused it remain: one
    /// master's view (an over-estimate after it started without its file)
    /// does not take a slot from a replica the others grant it to. Every
    /// request this node sends still claims a slot it keeps, so a vote it
    /// counts for the slot is one given with the slot claimed. Outside an
    /// election, every slot given is given up.
    pub(crate) fn moved_refusals_give_up(
        &self,
        voter: NodeId,
        master: NodeId,
        moved: &[(u16, u64)],
    ) -> Vec<(u16, u64)> {
        let Some(epoch) = self
            .election
            .lock()
            .as_ref()
            .filter(|e| e.failed_master_id == master && !e.completed)
            .map(|e| e.epoch)
        else {
            return moved.to_vec();
        };
        // The masters that can vote: every master that owns slots here but
        // the failed one; a quorum of all of them is needed.
        let available = self.voting_master_count().saturating_sub(1);
        let quorum = self.election_quorum(master);
        let mut refusals = self.moved_refusals.lock();
        if refusals.epoch != epoch {
            *refusals = Refusals {
                epoch,
                by_slot: Default::default(),
            };
        }
        moved
            .iter()
            .filter(|(slot, _)| {
                let refusers = refusals.by_slot.entry(*slot).or_default();
                refusers.insert(voter);
                refusers.len() + quorum > available
            })
            .copied()
            .collect()
    }

    /// Of the slots a candidate `claims` for `master_id`, the first that
    /// `master_id` owns here and that came to it at a newer epoch than the
    /// claim's ([`CameAt`]): its slot, when it came, and the claim's epoch.
    pub(crate) fn moved_after_claimed(
        &self,
        master_id: NodeId,
        claims: &[crate::cluster::bus::SlotClaim],
    ) -> Option<(u16, u64, u64)> {
        let slots = self.slots.read();
        for c in claims {
            let end = c.end.min((CLUSTER_SLOTS - 1) as u16);
            for slot in c.start..=end {
                if slots[slot as usize] == Some(master_id) {
                    let came = self.came_at.get(slot);
                    if came > c.epoch {
                        return Some((slot, came, c.epoch));
                    }
                }
            }
        }
        None
    }

    /// Those slots ([`Self::moved_after_claimed`]), each at the epoch it came
    /// at: what the `UPDATE` that answers the refusal tells the candidate.
    pub(crate) fn moved_claims_of(
        &self,
        master_id: NodeId,
        claims: &[crate::cluster::bus::SlotClaim],
    ) -> Vec<crate::cluster::bus::SlotClaim> {
        let slots = self.slots.read();
        let mut moved: Vec<(u16, u64)> = Vec::new();
        for c in claims {
            let end = c.end.min((CLUSTER_SLOTS - 1) as u16);
            for slot in c.start..=end {
                if slots[slot as usize] == Some(master_id) {
                    let came = self.came_at.get(slot);
                    if came > c.epoch {
                        moved.push((slot, came));
                    }
                }
            }
        }
        moved.sort_unstable();
        moved.dedup();
        let mut out: Vec<crate::cluster::bus::SlotClaim> = Vec::new();
        for (slot, epoch) in moved {
            match out.last_mut() {
                Some(last) if last.epoch == epoch && last.end + 1 == slot => last.end = slot,
                _ => out.push(crate::cluster::bus::SlotClaim {
                    start: slot,
                    end: slot,
                    epoch,
                }),
            }
        }
        out
    }
}

/// The slots of `claims`, for a log line.
pub(crate) fn claims_text(claims: &[crate::cluster::bus::SlotClaim]) -> String {
    migration::ranges_text(&claims.iter().map(|c| (c.start, c.end)).collect::<Vec<_>>())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The file reads back as written, with each slot's owner; one cut
    /// short, or with anything after its end, does not read.
    #[test]
    fn when_slots_came_reads_back() {
        let c = CameAt::default();
        let (a, b) = (
            crate::cluster::generate_node_id(),
            crate::cluster::generate_node_id(),
        );
        let mut owners = vec![None; CLUSTER_SLOTS];
        for slot in owners.iter_mut().take(21).skip(10) {
            *slot = Some(a);
        }
        owners[30] = Some(a);
        owners[40] = Some(b);
        c.set(10..=20, 5);
        c.set([30], 7);
        c.set_provisional(40, 3);
        c.set([50], 9);
        let text = c.text(&owners);
        assert_eq!(
            text,
            format!(
                "10-20 5 {a}\n30-30 7 {a}\n40-40 3 {b} provisional\nend\n",
                a = node_id_str(&a),
                b = node_id_str(&b)
            )
        );
        let came = parse(&text).unwrap();
        assert_eq!(came[15], Some((5, a, false)));
        assert_eq!(came[30], Some((7, a, false)));
        assert_eq!(came[40], Some((3, b, true)));
        assert!(parse(&format!("1-2 3 {} other\nend\n", node_id_str(&a))).is_err());
        assert_eq!(came[50], None, "no owner, not written");
        assert!(parse(&text[..text.len() - 4]).is_err());
        assert!(parse(&format!("{text}1-2 3 {}\n", node_id_str(&a))).is_err());
        assert!(parse(&format!("1-16384 3 {}\nend\n", node_id_str(&a))).is_err());
        assert!(parse("1-2 3\nend\n").is_err());
    }

    /// A provisional note is set by the owner's own claim, once, and only
    /// for slots of that owner; a set one is not; the count follows.
    #[test]
    fn a_provisional_note_is_set_by_the_owners_claim() {
        let c = CameAt::default();
        let (a, b) = (
            crate::cluster::generate_node_id(),
            crate::cluster::generate_node_id(),
        );
        let mut owners = vec![None; CLUSTER_SLOTS];
        owners[1] = Some(a);
        owners[2] = Some(a);
        owners[3] = Some(b);
        c.set_provisional(1, 4);
        c.set_provisional(3, 4);
        assert_eq!(c.provisional_count.load(Ordering::SeqCst), 2);
        c.owner_claimed(&owners, a, 0, 10, 9);
        assert_eq!((c.get(1), c.get(3)), (9, 4), "only the owner's");
        c.owner_claimed(&owners, a, 0, 10, 12);
        assert_eq!(c.get(1), 9, "once");
        c.set([2], 5);
        c.owner_claimed(&owners, a, 2, 2, 8);
        assert_eq!(c.get(2), 5);
        assert_eq!(c.provisional_count.load(Ordering::SeqCst), 1);
        c.clear();
        assert_eq!(c.get(1), 0);
        assert_eq!(c.provisional_count.load(Ordering::SeqCst), 0);
    }

    /// A slot given to another owner here is noted no earlier than that
    /// owner's epoch known here, and above the previous owner's.
    #[test]
    fn a_provisional_note_is_above_the_previous_owner() {
        let (a, b) = (
            crate::cluster::generate_node_id(),
            crate::cluster::generate_node_id(),
        );
        let mut epochs = ahash::AHashMap::new();
        epochs.insert(a, 3);
        epochs.insert(b, 7);
        assert_eq!(provisional_epoch(&epochs, a, Some(b)), 8);
        assert_eq!(provisional_epoch(&epochs, b, Some(a)), 7);
        assert_eq!(provisional_epoch(&epochs, a, None), 3);
    }

    /// A write that fails empties the file in place, which then does not
    /// read: it is read back as no file, never as an older history.
    #[test]
    fn a_failed_write_leaves_no_file_that_reads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nodes.conf.came-at");
        let a = crate::cluster::generate_node_id();
        let text = format!("0-9 5 {}\nend\n", node_id_str(&a));
        write_file(&path, &text).unwrap();
        assert!(parse(&std::fs::read_to_string(&path).unwrap()).is_ok());
        // The temporary file cannot be made: a directory has its name.
        std::fs::create_dir(tmp_path(&path)).unwrap();
        assert!(write_file(&path, &text.replace(" 5 ", " 6 ")).is_err());
        assert!(parse(&std::fs::read_to_string(&path).unwrap()).is_err());
    }
}
