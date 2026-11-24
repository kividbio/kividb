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
// Written down, synced, in `<cluster-config-file>.came-at` before every save
// of the cluster config (`persist::save_then`, as `.not-held`), and read back
// at start-up. Without the file (a node of an earlier release, a file that
// does not read), each owned slot is taken to have come at its owner's
// config epoch in nodes.conf: no earlier than it did, so a candidate that
// read its master's latest claim is not refused, and one whose last claim
// read is older than its master's epoch is.

use super::{CLUSTER_SLOTS, ClusterState, NodeId, migration};
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

    /// The file's text: runs of `a-b epoch`, then `end`.
    fn text(&self) -> String {
        let inner = self.inner.lock();
        let mut out = String::new();
        let mut run: Option<(usize, usize, u64)> = None;
        for (slot, &epoch) in inner.epochs.iter().enumerate() {
            run = match run {
                Some((a, b, e)) if e == epoch && b + 1 == slot => Some((a, slot, e)),
                Some((a, b, e)) => {
                    if e != 0 {
                        out.push_str(&format!("{a}-{b} {e}\n"));
                    }
                    Some((slot, slot, epoch))
                }
                None => Some((slot, slot, epoch)),
            };
        }
        if let Some((a, b, e)) = run
            && e != 0
        {
            out.push_str(&format!("{a}-{b} {e}\n"));
        }
        out.push_str(END);
        out.push('\n');
        out
    }
}

/// The file read back: each slot's epoch, or `Err` if it does not read.
fn parse(text: &str) -> Result<Vec<u64>, ()> {
    let mut epochs = vec![0; CLUSTER_SLOTS];
    let mut ended = false;
    for line in text.lines() {
        if ended {
            return Err(());
        }
        if line == END {
            ended = true;
            continue;
        }
        let (range, epoch) = line.split_once(' ').ok_or(())?;
        let epoch: u64 = epoch.parse().map_err(|_| ())?;
        let (a, b) = range.split_once('-').ok_or(())?;
        let (a, b): (usize, usize) = (a.parse().map_err(|_| ())?, b.parse().map_err(|_| ())?);
        if a > b || b >= CLUSTER_SLOTS {
            return Err(());
        }
        epochs[a..=b].iter_mut().for_each(|e| *e = epoch);
    }
    if !ended {
        return Err(());
    }
    Ok(epochs)
}

fn path(cluster: &ClusterState) -> Option<std::path::PathBuf> {
    cluster.config_path.as_ref().map(|p| {
        let mut name = p.as_os_str().to_owned();
        name.push(".came-at");
        std::path::PathBuf::from(name)
    })
}

/// Write the file down, synced, if it changed since it was last written.
pub(crate) fn save(cluster: &ClusterState) {
    static WRITTEN: Mutex<Option<String>> = Mutex::new(None);
    let Some(path) = path(cluster) else {
        return;
    };
    if !cluster.came_at.loaded.load(Ordering::SeqCst) {
        return;
    }
    let mut written = WRITTEN.lock();
    let text = cluster.came_at.text();
    if written.as_deref() == Some(text.as_str()) {
        return;
    }
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = std::path::PathBuf::from(tmp);
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

/// At start-up, once nodes.conf is read: the file read back, or, without
/// one that reads, each owned slot at its owner's config epoch.
pub(crate) fn load(cluster: &ClusterState) {
    let read = path(cluster).and_then(|path| match std::fs::read(&path) {
        Ok(bytes) => {
            let parsed = String::from_utf8(bytes)
                .ok()
                .ok_or(())
                .and_then(|t| parse(&t));
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
    let epochs = match read {
        Some(epochs) => epochs,
        None => {
            let owners: Vec<Option<NodeId>> = cluster.slots.read().to_vec();
            let nodes = cluster.nodes.read();
            owners
                .iter()
                .map(|o| {
                    o.and_then(|id| nodes.get(&id).map(|n| n.config_epoch))
                        .unwrap_or(0)
                })
                .collect()
        }
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

    /// The file reads back as written; one cut short, or with anything
    /// after its end, does not read.
    #[test]
    fn when_slots_came_reads_back() {
        let c = CameAt::default();
        c.set(10..=20, 5);
        c.set([30], 7);
        c.set_provisional(40, 3);
        let text = c.text();
        assert_eq!(text, "10-20 5\n30-30 7\n40-40 3\nend\n");
        let epochs = parse(&text).unwrap();
        assert_eq!(epochs[15], 5);
        assert_eq!(epochs[30], 7);
        assert_eq!(epochs[40], 3);
        assert_eq!(epochs[0], 0);
        assert!(parse(&text[..text.len() - 4]).is_err());
        assert!(parse(&format!("{text}1-2 3\n")).is_err());
        assert!(parse("1-16384 3\nend\n").is_err());
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
