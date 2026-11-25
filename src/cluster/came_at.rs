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
// the owner nodes.conf has (no file: a node of an earlier release; a file
// that does not read; one written before the save that moved the slot) is
// taken to have come at its owner's config epoch in nodes.conf: no earlier
// than it did, so a candidate that read its master's newest claim is not
// refused, and one whose last claim read is older than its master's epoch
// is.

use super::{CLUSTER_SLOTS, ClusterState, NodeId, migration, node_id_str};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) struct CameAt {
    inner: Mutex<Inner>,
    /// Read at start-up: saves made before do not write the file.
    loaded: AtomicBool,
}

struct Inner {
    epochs: Vec<u64>,
    /// Noted at the owner's epoch known then, until its own claim comes.
    provisional: Vec<bool>,
}

impl Default for CameAt {
    fn default() -> Self {
        CameAt {
            inner: Mutex::new(Inner {
                epochs: vec![0; CLUSTER_SLOTS],
                provisional: vec![false; CLUSTER_SLOTS],
            }),
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
                inner.provisional[i] = false;
            }
        }
    }

    /// `slot` was given to an owner here at `epoch`, that owner's config
    /// epoch known then: its own claim of it sets it ([`Self::owner_claimed`]).
    pub(crate) fn set_provisional(&self, slot: u16, epoch: u64) {
        let mut inner = self.inner.lock();
        let i = slot as usize;
        if i < CLUSTER_SLOTS {
            inner.epochs[i] = epoch;
            inner.provisional[i] = true;
        }
    }

    /// The owner of `slot` here claims it at `epoch`: a provisional note is
    /// set to it.
    pub(crate) fn owner_claimed(&self, slot: u16, epoch: u64) {
        let mut inner = self.inner.lock();
        let i = slot as usize;
        if i < CLUSTER_SLOTS && inner.provisional[i] {
            inner.epochs[i] = inner.epochs[i].max(epoch);
            inner.provisional[i] = false;
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
    }

    /// The file's text, with `owners` the slot table: runs of
    /// `a-b epoch owner` for the owned slots, then `end`.
    fn text(&self, owners: &[Option<NodeId>]) -> String {
        let inner = self.inner.lock();
        let mut out = String::new();
        let flush = |run: Option<(usize, usize, u64, NodeId)>, out: &mut String| {
            if let Some((a, b, e, o)) = run {
                out.push_str(&format!("{a}-{b} {e} {}\n", node_id_str(&o)));
            }
        };
        let mut run: Option<(usize, usize, u64, NodeId)> = None;
        for (slot, owner) in owners.iter().enumerate().take(CLUSTER_SLOTS) {
            let epoch = inner.epochs[slot];
            run = match (run, owner) {
                (Some((a, b, e, o)), Some(owner)) if e == epoch && o == *owner && b + 1 == slot => {
                    Some((a, slot, e, o))
                }
                (prev, Some(owner)) => {
                    flush(prev, &mut out);
                    Some((slot, slot, epoch, *owner))
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

/// The file read back: for each slot, when it came and to which owner, or
/// `Err` if it does not read.
fn parse(text: &str) -> Result<Vec<Option<(u64, NodeId)>>, ()> {
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
        let (Some(range), Some(epoch), Some(owner), None) =
            (words.next(), words.next(), words.next(), words.next())
        else {
            return Err(());
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
            .for_each(|c| *c = Some((epoch, owner)));
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
/// the same writes nothing.
static WRITTEN: Mutex<Option<String>> = Mutex::new(None);

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
    let mut written = WRITTEN.lock();
    // The table before the epochs, the order the two are taken in where a
    // slot moves (`apply_peer_slot_claims_from`).
    let owners: Vec<Option<NodeId>> = cluster.slots.read().to_vec();
    let text = cluster.came_at.text(&owners);
    if written.as_deref() == Some(text.as_str()) {
        return;
    }
    let tmp = tmp_path(&path);
    let saved = (|| {
        use std::io::Write;
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&tmp, &path)
    })();
    if let Some(dir) = path.parent()
        && let Ok(dir) = std::fs::File::open(dir)
    {
        let _ = dir.sync_all();
    }
    match saved {
        Ok(()) => *written = Some(text),
        Err(e) => log_warn!(
            "Cluster: could not write down when each slot came to its owner ({}): {e}",
            path.display()
        ),
    }
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
    let epochs: Vec<u64> = {
        let owners: Vec<Option<NodeId>> = cluster.slots.read().to_vec();
        let nodes = cluster.nodes.read();
        owners
            .iter()
            .enumerate()
            .map(|(slot, o)| {
                let Some(owner) = o else {
                    return 0;
                };
                match read.as_ref().and_then(|came| came[slot]) {
                    Some((epoch, of)) if of == *owner => epoch,
                    _ => nodes.get(owner).map_or(0, |n| n.config_epoch),
                }
            })
            .collect()
    };
    {
        let mut inner = cluster.came_at.inner.lock();
        inner.epochs = epochs;
        inner.provisional.iter_mut().for_each(|p| *p = false);
    }
    cluster.came_at.loaded.store(true, Ordering::SeqCst);
}

/// For the tests of a voter of an earlier release, never set otherwise:
/// `KIVIDB_TEST_VOTER_SKIPS_CAME_AT`, set, has this node vote without the
/// check of [`ClusterState::moved_after_claimed`].
pub(crate) fn test_voter_skips() -> bool {
    static SET: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *SET.get_or_init(|| std::env::var_os("KIVIDB_TEST_VOTER_SKIPS_CAME_AT").is_some())
}

impl ClusterState {
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
                "10-20 5 {a}\n30-30 7 {a}\n40-40 3 {b}\nend\n",
                a = node_id_str(&a),
                b = node_id_str(&b)
            )
        );
        let came = parse(&text).unwrap();
        assert_eq!(came[15], Some((5, a)));
        assert_eq!(came[30], Some((7, a)));
        assert_eq!(came[40], Some((3, b)));
        assert_eq!(came[50], None, "no owner, not written");
        assert!(parse(&text[..text.len() - 4]).is_err());
        assert!(parse(&format!("{text}1-2 3 {}\n", node_id_str(&a))).is_err());
        assert!(parse(&format!("1-16384 3 {}\nend\n", node_id_str(&a))).is_err());
        assert!(parse("1-2 3\nend\n").is_err());
    }

    /// A provisional note is set by the owner's own claim, and only once;
    /// a set one is not.
    #[test]
    fn a_provisional_note_is_set_by_the_owners_claim() {
        let c = CameAt::default();
        c.set_provisional(1, 4);
        c.owner_claimed(1, 9);
        assert_eq!(c.get(1), 9);
        c.owner_claimed(1, 12);
        assert_eq!(c.get(1), 9);
        c.set([2], 5);
        c.owner_claimed(2, 8);
        assert_eq!(c.get(2), 5);
        c.clear();
        assert_eq!(c.get(1), 0);
    }
}
