// src/cluster/came_at.rs
//
// When each slot came to its owner, as this node knows it: the config epoch
// of the claim that moved the slot to the owner it has in this node's slot
// table. A master asked for its vote compares it with the epoch a candidate
// claims a slot of its master at (the epoch of the last claim of its master
// the candidate read in its replication stream, `followed_imports`): a slot
// that came to the master after that epoch (it was handed away and imported
// back since) is refused (`ClusterState::cast_vote`).
//
// Each entry has a class ([`Class`]):
//
// * `Heard`: set from a claim this node took in (`apply_peer_slot_claims_from`,
//   the claim's epoch), or by this node's own takeovers (an import's,
//   `CLUSTER SETSLOT <slot> NODE` to itself, `CLUSTER ADDSLOTS`, a promotion:
//   the epoch it takes them at).
// * `Lower`: a slot given to another node here by hand or by the end of a
//   handover from this node (`CLUSTER SETSLOT <slot> NODE <other>`,
//   `hand_over_slots`): noted at a lower bound of the epoch the new owner
//   claims it at ([`provisional_epoch`]).
// * `Fallback`: read back at start-up without an entry for the slot with its
//   current owner (no file; a file that does not read; one written before
//   the save that moved the slot): its owner's config epoch in nodes.conf.
//
// The owner's own claim of a `Lower` or `Fallback` slot makes it `Heard`, at
// that claim's epoch ([`CameAt::owner_claimed`]). A refusal for a `Heard` or
// `Lower` entry is decisive for the candidate; one for a `Fallback` entry
// counts toward a quorum only (`ClusterState::moved_refusals_give_up`).
//
// Written down, synced, in `<cluster-config-file>.came-at`, with each slot's
// owner and class, before the saves of the cluster config
// (`persist::save_then`, as `.not-held`; not by the saves of a takeover,
// whose source holds writes meanwhile), and read back at start-up.

use super::{CLUSTER_SLOTS, ClusterState, NodeFlags, NodeId, migration, node_id_str};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// How this node came to know when a slot came to its owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Class {
    /// From a claim of the slot this node took in, or its own takeover.
    Heard,
    /// A lower bound noted when the slot was given to its owner here.
    Lower,
    /// Its owner's config epoch, read back with no entry for the slot.
    Fallback,
}

impl Class {
    /// A refusal for it is decisive for the candidate.
    pub(crate) fn decisive(self) -> bool {
        self != Class::Fallback
    }

    fn word(self) -> &'static str {
        match self {
            Class::Heard => "",
            Class::Lower => " provisional",
            Class::Fallback => " fallback",
        }
    }
}

pub(crate) struct CameAt {
    inner: Mutex<Inner>,
    /// How many slots are `Lower` or `Fallback`: none, and a claim of slots
    /// their owner holds here takes no lock ([`Self::owner_claimed`]).
    settling: AtomicUsize,
    /// Read at start-up: saves made before do not write the file.
    loaded: AtomicBool,
}

struct Inner {
    epochs: Vec<u64>,
    class: Vec<Class>,
}

impl Inner {
    fn set_class(&mut self, i: usize, class: Class, count: &AtomicUsize) {
        let was = self.class[i] != Class::Heard;
        let is = class != Class::Heard;
        self.class[i] = class;
        match (was, is) {
            (false, true) => {
                count.fetch_add(1, Ordering::SeqCst);
            }
            (true, false) => {
                count.fetch_sub(1, Ordering::SeqCst);
            }
            _ => {}
        }
    }
}

impl Default for CameAt {
    fn default() -> Self {
        CameAt {
            inner: Mutex::new(Inner {
                epochs: vec![0; CLUSTER_SLOTS],
                class: vec![Class::Heard; CLUSTER_SLOTS],
            }),
            settling: AtomicUsize::new(0),
            loaded: AtomicBool::new(false),
        }
    }
}

/// The last line of the file: one without it was cut short.
const END: &str = "end";

impl CameAt {
    /// `slots` came to their owner at `epoch`, as this node heard it.
    pub(crate) fn set(&self, slots: impl IntoIterator<Item = u16>, epoch: u64) {
        let mut inner = self.inner.lock();
        for slot in slots {
            let i = slot as usize;
            if i < CLUSTER_SLOTS {
                inner.epochs[i] = epoch;
                inner.set_class(i, Class::Heard, &self.settling);
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
            inner.set_class(i, Class::Lower, &self.settling);
        }
    }

    /// The owner of the slots `start..=end` that `owners` gives it claims
    /// them at `epoch`: each `Lower` or `Fallback` one came at that claim's
    /// epoch, and is `Heard` from now on. Any claim of the slot by its owner
    /// will do: the owner claims it at its config epoch, which only grows
    /// and is at or above the epoch it took the slot at; and a claim of the
    /// slot by the owner reaches this node only once the owner has it (its
    /// claims of the time before do not list the slot, the slot having
    /// another owner then). One lock for the range, none while no slot is
    /// `Lower` or `Fallback`.
    pub(crate) fn owner_claimed(
        &self,
        owners: &[Option<NodeId>],
        owner: NodeId,
        start: u16,
        end: u16,
        epoch: u64,
    ) {
        if self.settling.load(Ordering::SeqCst) == 0 {
            return;
        }
        let mut inner = self.inner.lock();
        let end = (end as usize).min(CLUSTER_SLOTS - 1);
        for i in start as usize..=end {
            if inner.class[i] != Class::Heard && owners[i] == Some(owner) {
                inner.epochs[i] = epoch;
                inner.set_class(i, Class::Heard, &self.settling);
            }
        }
    }

    /// When `slot` came to its owner here, and how this node knows it.
    pub(crate) fn get(&self, slot: u16) -> (u64, Class) {
        let inner = self.inner.lock();
        let i = (slot as usize).min(CLUSTER_SLOTS - 1);
        (inner.epochs[i], inner.class[i])
    }

    /// `CLUSTER RESET`.
    pub(crate) fn clear(&self) {
        let mut inner = self.inner.lock();
        inner.epochs.iter_mut().for_each(|e| *e = 0);
        inner.class.iter_mut().for_each(|c| *c = Class::Heard);
        self.settling.store(0, Ordering::SeqCst);
    }

    /// The file's text, with `owners` the slot table: runs of
    /// `a-b epoch owner`, `a-b epoch owner provisional` (a `Lower` run) or
    /// `a-b epoch owner fallback`, for the owned slots, then `end`.
    fn text(&self, owners: &[Option<NodeId>]) -> String {
        let inner = self.inner.lock();
        let mut out = String::new();
        type Run = (usize, usize, u64, NodeId, Class);
        let flush = |run: Option<Run>, out: &mut String| {
            if let Some((a, b, e, o, c)) = run {
                out.push_str(&format!("{a}-{b} {e} {}{}\n", node_id_str(&o), c.word()));
            }
        };
        let mut run: Option<Run> = None;
        for (slot, owner) in owners.iter().enumerate().take(CLUSTER_SLOTS) {
            let epoch = inner.epochs[slot];
            let class = inner.class[slot];
            run = match (run, owner) {
                (Some((a, b, e, o, c)), Some(owner))
                    if e == epoch && o == *owner && c == class && b + 1 == slot =>
                {
                    Some((a, slot, e, o, c))
                }
                (prev, Some(owner)) => {
                    flush(prev, &mut out);
                    Some((slot, slot, epoch, *owner, class))
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

/// When a slot came, to which owner, and how this node knew it.
type Came = (u64, NodeId, Class);

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
        let (Some(range), Some(epoch), Some(owner), class, None) = (
            words.next(),
            words.next(),
            words.next(),
            words.next(),
            words.next(),
        ) else {
            return Err(());
        };
        let class = match class {
            None => Class::Heard,
            Some("provisional") => Class::Lower,
            Some("fallback") => Class::Fallback,
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
            .for_each(|c| *c = Some((epoch, owner, class)));
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

/// A write failed, and the file in place could be neither emptied, removed
/// nor moved aside: it holds a history older than this node's. Every save
/// tries again, and says so, until a write succeeds.
static STALE_IN_PLACE: AtomicBool = AtomicBool::new(false);

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
        Ok(()) => {
            *WRITTEN.lock() = Some(text);
            STALE_IN_PLACE.store(false, Ordering::SeqCst);
        }
        Err((e, left)) => {
            // What is in place is older than what this node has: the next
            // save writes again, whatever it compares with.
            *WRITTEN.lock() = None;
            if left == Left::Readable {
                STALE_IN_PLACE.store(true, Ordering::SeqCst);
            }
            log_warn!(
                "Cluster: could not write down when each slot came to its owner ({}): {e}; {}",
                path.display(),
                match left {
                    Left::Readable => {
                        "the file in place could be neither emptied, removed nor moved aside, \
                         and holds an older history: remove it before this node next starts"
                    }
                    Left::Unreadable => "the file in place reads back as none",
                }
            );
        }
    }
}

/// What a failed write leaves in place.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Left {
    /// Nothing that reads: emptied, removed, or moved aside.
    Unreadable,
    /// The file as it was.
    Readable,
}

/// Write `text` to `path`, synced, by way of a temporary file. If that
/// fails, the file in place is emptied, or else removed, or else moved
/// aside (best effort, in that order): one that does not read is read back
/// as no file, each slot at its owner's config epoch (`Fallback`).
fn write_file(path: &std::path::Path, text: &str) -> Result<(), (std::io::Error, Left)> {
    let tmp = tmp_path(path);
    let saved = (|| {
        use std::io::Write;
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&tmp, path)
    })();
    let left = match &saved {
        Ok(()) => Left::Unreadable,
        Err(_) => {
            let emptied = std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(path)
                .and_then(|f| f.sync_all());
            let gone = || match std::fs::remove_file(path) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
                _ => Ok(()),
            };
            let aside = || {
                let mut name = path.as_os_str().to_owned();
                name.push(".unread");
                std::fs::rename(path, std::path::PathBuf::from(name))
            };
            if emptied.is_ok() || gone().is_ok() || aside().is_ok() {
                Left::Unreadable
            } else {
                Left::Readable
            }
        }
    };
    if let Some(dir) = path.parent()
        && let Ok(dir) = std::fs::File::open(dir)
    {
        let _ = dir.sync_all();
    }
    saved.map_err(|e| (e, left))
}

/// The lower bound noted for a slot given here to `new_owner` from
/// `previous` (`CLUSTER SETSLOT <slot> NODE`, the end of a handover from
/// this node), with `epochs` each node's config epoch here: the new owner's
/// claim of it is at its config epoch, which is no older than this node
/// knows, and above the previous owner's (a claim is taken in only over an
/// owner at an older epoch). It can be above the epoch the new owner claims
/// the slot at when the new owner took it without raising its epoch above
/// the previous owner's here (`CLUSTER SETSLOT <slot> NODE` given on a
/// third node before the new owner's own): the new owner's first claim of
/// the slot settles it ([`CameAt::owner_claimed`]).
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

/// At start-up, once nodes.conf is read: each slot as the file has it, if
/// the file has it with the owner nodes.conf has; else (no file, one that
/// does not read, or another owner: the file was written before a save of
/// nodes.conf that moved the slot) at its owner's config epoch in
/// nodes.conf, which is no earlier than it came, as `Fallback`.
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
    let came: Vec<(u64, Class)> = {
        let owners: Vec<Option<NodeId>> = cluster.slots.read().to_vec();
        let nodes = cluster.nodes.read();
        owners
            .iter()
            .enumerate()
            .map(|(slot, o)| {
                let Some(owner) = o else {
                    return (0, Class::Heard);
                };
                match read.as_ref().and_then(|came| came[slot]) {
                    Some((epoch, of, class)) if of == *owner => (epoch, class),
                    _ => (
                        nodes.get(owner).map_or(0, |n| n.config_epoch),
                        Class::Fallback,
                    ),
                }
            })
            .collect()
    };
    {
        let mut inner = cluster.came_at.inner.lock();
        inner.epochs = came.iter().map(|(e, _)| *e).collect();
        inner.class = came.iter().map(|(_, c)| *c).collect();
        cluster.came_at.settling.store(
            came.iter().filter(|(_, c)| *c != Class::Heard).count(),
            Ordering::SeqCst,
        );
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
/// for slots a master took to have come at its owner's epoch (`Fallback`):
/// per slot, the masters that refused it.
#[derive(Default)]
pub(crate) struct Refusals {
    epoch: u64,
    by_slot: ahash::AHashMap<u16, ahash::AHashSet<NodeId>>,
}

/// The slots a master refused for when they came, as its `UPDATE` says:
/// those it knows the coming of (`Heard`, `Lower`: decisive) and those it
/// takes to have come at their owner's epoch (`Fallback`), each at that
/// epoch.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub(crate) struct Moved {
    pub(crate) decisive: Vec<crate::cluster::bus::SlotClaim>,
    pub(crate) fallback: Vec<crate::cluster::bus::SlotClaim>,
}

impl Moved {
    pub(crate) fn is_empty(&self) -> bool {
        self.decisive.is_empty() && self.fallback.is_empty()
    }
}

/// `slots` (each with its epoch) as claims, in runs.
fn runs(mut slots: Vec<(u16, u64)>) -> Vec<crate::cluster::bus::SlotClaim> {
    slots.sort_unstable();
    slots.dedup();
    let mut out: Vec<crate::cluster::bus::SlotClaim> = Vec::new();
    for (slot, epoch) in slots {
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

impl ClusterState {
    /// `voter` refused the slots `moved` (each with the epoch it says the
    /// slot came at) for when they came, taking each to have come at its
    /// owner's config epoch (`Fallback`): of them, those this node, a
    /// candidate in the election under way, gives up, the masters that
    /// refused them so leaving too few others to make a quorum. Counted
    /// among the others: the masters that own slots here, but the failed
    /// one and those marked failing or failed. A master without the history
    /// of a slot refuses whatever one with it refuses, and more; such a
    /// refusal alone does not take the slot from a replica the others grant
    /// it to. Every request this node sends still claims a slot it keeps, so
    /// a vote it counts for the slot is one given with the slot claimed.
    /// Outside an election, every slot given is given up.
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
        let owners: ahash::AHashSet<NodeId> = self.slots.read().iter().flatten().copied().collect();
        let available = {
            let nodes = self.nodes.read();
            owners
                .iter()
                .filter(|id| {
                    **id != master
                        && nodes.get(*id).is_some_and(|n| {
                            !n.flags.intersects(NodeFlags::PFAIL | NodeFlags::FAIL)
                        })
                })
                .count()
        };
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
                    let (came, _) = self.came_at.get(slot);
                    if came > c.epoch {
                        return Some((slot, came, c.epoch));
                    }
                }
            }
        }
        None
    }

    /// Those slots ([`Self::moved_after_claimed`]), each at the epoch it came
    /// at, decisive or not: what the `UPDATE` that answers the refusal tells
    /// the candidate.
    pub(crate) fn moved_claims_of(
        &self,
        master_id: NodeId,
        claims: &[crate::cluster::bus::SlotClaim],
    ) -> Moved {
        let slots = self.slots.read();
        let (mut decisive, mut fallback): (Vec<(u16, u64)>, Vec<(u16, u64)>) = Default::default();
        for c in claims {
            let end = c.end.min((CLUSTER_SLOTS - 1) as u16);
            for slot in c.start..=end {
                if slots[slot as usize] == Some(master_id) {
                    let (came, class) = self.came_at.get(slot);
                    if came > c.epoch {
                        if class.decisive() {
                            decisive.push((slot, came));
                        } else {
                            fallback.push((slot, came));
                        }
                    }
                }
            }
        }
        Moved {
            decisive: runs(decisive),
            fallback: runs(fallback),
        }
    }
}

/// The slots of `claims`, for a log line.
pub(crate) fn claims_text(claims: &[crate::cluster::bus::SlotClaim]) -> String {
    migration::ranges_text(&claims.iter().map(|c| (c.start, c.end)).collect::<Vec<_>>())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The file reads back as written, with each slot's owner and class;
    /// one cut short, or with anything after its end, does not read.
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
        owners[41] = Some(b);
        c.set(10..=20, 5);
        c.set([30], 7);
        c.set_provisional(40, 3);
        c.set([50], 9);
        {
            let mut inner = c.inner.lock();
            inner.epochs[41] = 6;
            inner.set_class(41, Class::Fallback, &c.settling);
        }
        let text = c.text(&owners);
        assert_eq!(
            text,
            format!(
                "10-20 5 {a}\n30-30 7 {a}\n40-40 3 {b} provisional\n41-41 6 {b} fallback\nend\n",
                a = node_id_str(&a),
                b = node_id_str(&b)
            )
        );
        let came = parse(&text).unwrap();
        assert_eq!(came[15], Some((5, a, Class::Heard)));
        assert_eq!(came[30], Some((7, a, Class::Heard)));
        assert_eq!(came[40], Some((3, b, Class::Lower)));
        assert_eq!(came[41], Some((6, b, Class::Fallback)));
        assert!(parse(&format!("1-2 3 {} other\nend\n", node_id_str(&a))).is_err());
        assert_eq!(came[50], None, "no owner, not written");
        assert!(parse(&text[..text.len() - 4]).is_err());
        assert!(parse(&format!("{text}1-2 3 {}\n", node_id_str(&a))).is_err());
        assert!(parse(&format!("1-16384 3 {}\nend\n", node_id_str(&a))).is_err());
        assert!(parse("1-2 3\nend\n").is_err());
        assert!(Class::Heard.decisive() && Class::Lower.decisive());
        assert!(!Class::Fallback.decisive());
    }

    /// A `Lower` or `Fallback` note is set by the owner's own claim, to its
    /// epoch (above or below the note), once, and only for slots of that
    /// owner; it is `Heard` from then on; the count follows.
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
        owners[4] = Some(a);
        c.set_provisional(1, 4);
        c.set_provisional(3, 4);
        c.set_provisional(4, 9);
        assert_eq!(c.settling.load(Ordering::SeqCst), 3);
        c.owner_claimed(&owners, a, 0, 10, 7);
        assert_eq!(
            (c.get(1), c.get(3), c.get(4)),
            ((7, Class::Heard), (4, Class::Lower), (7, Class::Heard)),
            "only the owner's, to its claim's epoch"
        );
        c.owner_claimed(&owners, a, 0, 10, 12);
        assert_eq!(c.get(1), (7, Class::Heard), "once");
        c.set([2], 5);
        c.owner_claimed(&owners, a, 2, 2, 8);
        assert_eq!(c.get(2), (5, Class::Heard));
        assert_eq!(c.settling.load(Ordering::SeqCst), 1);
        c.clear();
        assert_eq!(c.get(1), (0, Class::Heard));
        assert_eq!(c.settling.load(Ordering::SeqCst), 0);
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

    /// A write that fails leaves no file that reads: it is emptied, else
    /// removed, else moved aside.
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
        let (_, left) = write_file(&path, &text.replace(" 5 ", " 6 ")).unwrap_err();
        assert_eq!(left, Left::Unreadable);
        assert!(parse(&std::fs::read_to_string(&path).unwrap()).is_err());
    }
}
