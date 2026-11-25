// src/cluster/followed_imports.rs
//
// The atomic slot imports of the master this node follows, and what is
// left of them once it is promoted (#741).
//
// An import (`CLUSTER MIGRATION IMPORT`) brings the source's keys of its
// slots to the destination, and the destination's replicas get them with
// its other writes. An import that does not complete deletes what it
// brought, and keeps the keys the destination held of the slots before it
// (#745). But a destination can die first. One of its replicas is promoted
// then, with what had arrived: keys of slots it does not own, which nothing
// deleted, and which a later import of the slots kept beside what it
// brought, as keys held before. So keys the source had deleted since were
// served again.
//
// So a master tells the replicas that ask for it (`REPLCONF capa
// slot-imports`) which imports it has under way, and of which of their
// slots it held keys before each began (`KIVIDB.IMPORTS`: see
// `replication::SLOT_IMPORTS_MARKER`). That is not part of the stream: no
// offset counts it, and it is in no backlog, AOF or snapshot. A replica of
// an earlier release never asks for it and is never sent it, and a replica
// of a master that does not send it (an earlier release) knows of no import
// and behaves as before.
//
// * A replica sees, in the stream's order, the moment its master looked at
//   the keys it held of an import's slots, and notes its own keys of them
//   then: the same keys, or more, never fewer (a key can only leave those
//   slots meanwhile, by a deletion still on its way).
// * A replica that attaches is told the imports as they stand. One it did
//   not see begin (it attached part way, or was sent the dataset in full)
//   it knows by its slots only: every key of a slot that had keys before is
//   kept.
// * What it knows is written beside the cluster config, as slots, never
//   key names (`<cluster-config-file>.followed-imports`). After a restart,
//   every key of a slot that had keys before is kept too.
// * An import its master completed, or ended and cleaned up itself, is no
//   longer listed, and is forgotten: what it brought is the slots' data, or
//   gone already. Whatever this node's slot table says of them, it does not
//   delete those keys.
// * Promoted, this node deletes what the imports still listed brought:
//   every key of a slot of which nothing was held before, and in the others
//   the keys not held before. Never a slot that is its own, nor one that
//   has come to it since it first looked, even and gone again (keys
//   written to it meanwhile are acknowledged writes): such a slot is left
//   out of the import for good. Of an import read back from disk, whose
//   slots' past here is not known, only keys the owner has. Only once
//   nothing here is importing, migrating or deleting the slot, and the
//   slot's owner, a master that is up, says it owns it and is not moving
//   it (`CLUSTER SYNCSLOTS ORPHANCHECK`), which it is asked again before
//   each batch is deleted (`ORPHANKEYS`). The deletes reach this node's
//   replicas and AOF as its own writes do; its replicas delete nothing on
//   their own. It stops when this node becomes a replica, and otherwise
//   tries again every second until it is done. Meanwhile an import of those
//   slots is refused. An owner of an earlier release, which cannot be
//   asked, leaves the keys as an earlier release did: `CLUSTER ORPHANSLOTS`
//   lists them, and `CLUSTER DROPORPHANS` deletes those the owner has.
// * Until the deletion is done, the imports stay listed in this node's own
//   markers, so that a replica of it promoted next carries on with it.
// * `CLUSTER RESET` forgets them all.
//
// Redis 8 does the same with `CLUSTER SYNCSLOTS CONF ASM-TASK`, which a
// destination sends its replicas in the stream (and keeps in its RDB); a
// replica promoted with an import unfinished deletes every key of the
// import's slots it does not own, without asking the owner, and so does an
// import as it begins. Here the keys held before are kept, and the owner is
// asked.

use super::migration::{self, Env, HeldBefore, Ranges, in_ranges, slot_runs, slots_of};
use super::{ClusterState, NodeId, key_hash_slot, node_id_str, orphans};
use crate::cpus::BuilderExt as _;
use crate::replication::{SLOT_IMPORTS_MARKER, SlotImportsEvent};
use parking_lot::{Condvar, Mutex};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

/// How often a promoted node tries again to delete what is left.
const RETRY: Duration = Duration::from_secs(1);
/// How often it says again why it cannot yet.
const RELOG: Duration = Duration::from_secs(30);

/// An import of the master this node follows, or followed.
#[derive(Clone)]
pub(crate) struct Followed {
    pub(crate) id: String,
    pub(crate) ranges: Ranges,
    /// The slots of which every key is kept: those the master said it held
    /// keys of before the import, and after a restart those this node held
    /// keys of. `None` while the master has not looked: the source has been
    /// asked for nothing, and every key of the slots is one held before.
    pub(crate) kept: Option<Ranges>,
    /// This node's own keys of the slots at the moment its master looked at
    /// its own, seen in the stream's order: the keys held before, or more.
    /// `None` if it did not see that moment.
    pub(crate) held: Option<Arc<HeldBefore>>,
    /// Per slot, how many times it had come to this node
    /// (`SlotDeletions::came_here`) when this node first looked: a slot
    /// that has come here since is left out of the import for good
    /// ([`settle`]). `None` until then.
    came: Option<Vec<(u16, u32)>>,
    /// Known since this process started, so that whether a slot came here
    /// meanwhile is known. Read back from disk after a restart it is not: a
    /// slot may have been this node's, taken writes, and gone again before
    /// the restart, and of such an import only the keys the owner has are
    /// deleted.
    pub(crate) seen_since_start: bool,
}

/// What a promoted node deletes of a slot of an import.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Plan {
    /// Nothing: every key may have been there before.
    Keep,
    /// Every key: none was there before.
    All,
    /// The keys not noted as held before.
    Unheld,
}

impl Followed {
    fn plan(&self, slot: u16) -> Plan {
        let Some(kept) = &self.kept else {
            return Plan::Keep;
        };
        match &self.held {
            Some(held) if held.has_slot(slot) => {
                if held.whole {
                    Plan::Keep
                } else {
                    Plan::Unheld
                }
            }
            _ if in_ranges(kept, slot) => Plan::Keep,
            _ => Plan::All,
        }
    }

    /// The slots of which every key stays after a restart: names are not
    /// written down.
    fn kept_on_disk(&self) -> Option<Ranges> {
        let kept = self.kept.as_ref()?;
        let mut slots: Vec<u16> = slots_of(kept).collect();
        if let Some(held) = &self.held {
            slots.extend(held.slots.iter().copied());
        }
        slots.sort_unstable();
        slots.dedup();
        Some(slot_runs(slots))
    }

    /// Whether the cleanup still has anything to delete of it.
    fn deletes_any(&self) -> bool {
        slots_of(&self.ranges).any(|slot| self.plan(slot) != Plan::Keep)
    }
}

/// The imports of this node's master, or of its old master once promoted.
/// See the top of this file.
///
/// Locks: `imports` after `ClusterState::slots` and `migrations`, before
/// `SlotDeletions::counts`; nothing else is taken holding it.
#[derive(Default)]
pub(crate) struct FollowedImports {
    imports: Mutex<Vec<Followed>>,
    /// Wakes the cleanup at once, after a promotion.
    wake: Condvar,
    woken: Mutex<bool>,
    /// Each look at `imports` to be written down is numbered, holding its
    /// lock; the file is written out of the lock, and never with an older
    /// look than the last one written (see [`write_down`]).
    looks: std::sync::atomic::AtomicU64,
    written: Mutex<u64>,
    /// The imports into this node's master whose data this node holds in
    /// full, as its master's stream told (`ARRIVED` markers, see
    /// [`encode_arrived`]): what a vote's `UPDATE` may give it of its
    /// master's claims (see `ClusterState::master_claims_arrived`).
    arrived: Mutex<Vec<Arrived>>,
    /// Of the slots this node's table shows as its master's, those it does
    /// not hold: see [`NotHeld`].
    not_held: Mutex<NotHeld>,
    /// The file of [`NotHeld`] has been read at start-up: saves of the
    /// cluster config made before (at start-up, before the cluster starts)
    /// do not write it, which would remove it unread.
    not_held_loaded: std::sync::atomic::AtomicBool,
}

/// What this node, a replica, does not hold of the slots its table shows as
/// its master's. The table shows the master's slots as the cluster tells
/// them, for routing and redirects; what the node holds is what its
/// replication stream has confirmed.
///
/// For a master whose stream carries the slot-imports markers, a slot it
/// gained in this node's table counts as held only once the stream has
/// confirmed it: a `TAKEN` of the master at that claim's epoch or later
/// (the claim it sends before any write to the slots, and the one kept and
/// sent ahead of a full sync's dataset), or an `ARRIVED` at that very epoch
/// (every record of the import came before it). The `TAKEN` confirms the
/// gain whether this node reads it before it hears of the gain or after:
/// the last one read is kept ([`NotHeld::stream_claim`]), and a gain of a
/// slot it lists, at its epoch or before, is held at once. So a replica
/// whose master's config epoch moved after a takeover (a collision,
/// `CLUSTER BUMPEPOCH`: each sends a `TAKEN` at the new epoch) holds the
/// slots at the newer epoch it hears them claimed at. A gain learned only from a
/// heartbeat of the master, or from a vote's `UPDATE`, is not held: across
/// a cut of the replication link alone, the bus still brings the master's
/// claims while the stream brings none of the keys. A candidate claims only
/// the slots of its master it holds (`ClusterState::held_claims_of_master`),
/// and a promotion takes only those (`ClusterState::promote_self`).
///
/// A gain is noted here together with the table change, under the slot
/// table's lock, and the slots not held are written down, synced, in
/// `<cluster-config-file>.not-held` before every save of the cluster config
/// (`persist::save_then`; nothing is written when they have not changed):
/// no save of nodes.conf shows a gain the file does not have. At start-up they are read back:
/// a replica that heard its master gain slots, and restarted before its
/// stream confirmed them, does not hold them after the restart either. No
/// file: every slot of the master in the table, read from nodes.conf, is
/// held, as the dataset loaded with it has them (nothing was noted, as on a
/// node of an earlier release). A file that does not read (cut short,
/// damaged): every slot of the master in the table is taken as not held,
/// at epoch 0, until the stream confirms it (any `TAKEN` of the master
/// does). An earlier release that reads the same nodes.conf ignores the
/// file. What the master gains after a restart is not held until the
/// stream confirms it. A master whose stream carries no markers (1.2):
/// every slot of it in the table is held, from the first session with it
/// (until then, after a restart, its gains are not held either: they are
/// not in the dataset the node loaded).
#[derive(Default)]
struct NotHeld {
    /// The master this is about: everything is forgotten when this node
    /// follows another.
    master: Option<NodeId>,
    /// That master's stream carries no markers (a session with it said so).
    plain: bool,
    /// The slots not held, with the epoch of the claim that gave each one to
    /// the master here.
    slots: std::collections::BTreeMap<u16, u64>,
    /// A full sync began, and the claim the master sends ahead of the
    /// dataset (the last `TAKEN` it keeps) is still to be read: every slot it
    /// lists is held, at whatever epoch this node heard the master gain it,
    /// since the dataset has every write to those slots up to it. Ended by
    /// the first marker after the dataset (that `TAKEN`, or the imports
    /// marker, which the master sends after it).
    sync_claim_due: bool,
    /// The last claim of the master read in its stream (a `TAKEN`: every
    /// slot it owned then, at its config epoch then), while this node's
    /// dataset is that stream's: forgotten at a full sync (the claim sent
    /// ahead of the dataset sets it again) and when this node follows
    /// another master, kept across a partial resync (the stream continues).
    /// In memory only: after a restart, the stream's claims set it again.
    /// See [`NotHeld::stream_holds`].
    stream_claim: Option<(u64, Ranges)>,
    /// The epoch of the newest claim of the master read in its stream
    /// (`TAKEN`), kept across a full sync and a restart (written down with
    /// the slots not held): the epoch this node holds the master's slots as
    /// of, and claims them at when it stands for election
    /// ([`FollowedImports::claim_epochs`]). `None` before the first one.
    confirmed: Option<u64>,
    /// Started without the file (none was written: a node of an earlier
    /// release, or no claim read before it stopped), or with one that has
    /// no confirmed epoch: until a claim is read, the master's slots are
    /// claimed at its config epoch in nodes.conf, the view the dataset
    /// loaded with it has, as every slot of it is held then.
    baseline: bool,
}

impl NotHeld {
    fn about(&mut self, master: NodeId) {
        if self.master != Some(master) {
            *self = NotHeld {
                master: Some(master),
                ..NotHeld::default()
            };
        }
    }

    /// The epoch to claim a slot of the master at, standing for election.
    fn claim_epochs(
        &self,
        master: NodeId,
        view_epoch: u64,
        arrived: Vec<(u64, Ranges)>,
    ) -> ClaimEpochs {
        if self.master != Some(master) || self.plain {
            return ClaimEpochs::at(view_epoch);
        }
        match self.confirmed {
            Some(confirmed) => ClaimEpochs {
                base: confirmed,
                arrived: arrived
                    .into_iter()
                    .filter(|(e, _)| *e > confirmed)
                    .collect(),
            },
            None if self.baseline => ClaimEpochs::at(view_epoch),
            None => ClaimEpochs { base: 0, arrived },
        }
    }

    /// Whether this node holds `slot` as of a claim of it by its master at
    /// `epoch`: the last claim read in the master's stream lists it, at
    /// `epoch` or later ([`claim_holds`]).
    fn stream_holds(&self, slot: u16, epoch: u64) -> bool {
        self.stream_claim
            .as_ref()
            .is_some_and(|claim| claim_holds(claim, slot, epoch))
    }
}

/// The epochs a replica claims its master's slots at when it stands for
/// election ([`FollowedImports::claim_epochs`]).
pub(crate) struct ClaimEpochs {
    /// The epoch of the newest claim of the master read in its stream.
    base: u64,
    /// `ARRIVED` markers at newer epochs: a slot one covers is claimed at
    /// its epoch, as this node holds it as of a takeover at that epoch.
    arrived: Vec<(u64, Ranges)>,
}

impl ClaimEpochs {
    fn at(epoch: u64) -> Self {
        ClaimEpochs {
            base: epoch,
            arrived: Vec::new(),
        }
    }

    /// The epoch to claim `slot` at.
    pub(crate) fn epoch(&self, slot: u16) -> u64 {
        self.arrived
            .iter()
            .filter(|(_, r)| in_ranges(r, slot))
            .map(|(e, _)| *e)
            .fold(self.base, u64::max)
    }
}

/// Whether `claim`, the last claim of this node's master read in its stream
/// (`TAKEN`, at its epoch, of its slots), stands for a claim of `slot` by
/// the master at `epoch`: it lists the slot, at `epoch` or later.
///
/// Every write to the slot before the `TAKEN` came before it in the stream,
/// which this node applied, whatever became of its link since. The master's
/// config epoch only grows, and every way it takes a slot whose keys come
/// from another node takes an epoch above every one it knows (an import's
/// takeover, `CLUSTER SETSLOT <slot> NODE` to itself, a claim back after a
/// failed destination, a promotion). So a claim of the slot at the
/// `TAKEN`'s epoch or before is of the slot as the stream had it: the
/// takeover the stream confirmed, then only the master's own writes. A claim
/// at a newer epoch may be of a later takeover, whose keys this node may not
/// have (its link down meanwhile): not held.
pub(crate) fn claim_holds(claim: &(u64, Ranges), slot: u16, epoch: u64) -> bool {
    epoch <= claim.0 && in_ranges(&claim.1, slot)
}

/// An `ARRIVED` marker read in the stream of `master`: every record of an
/// import of `ranges` into it came before, for it to take them over at
/// config epoch `epoch`.
///
/// Forgotten when this node's dataset is no longer that stream's as of the
/// marker: at a full sync (the dataset is replaced; the marker is not sent
/// again with it), and when this node follows another master. Kept across a
/// link drop and a partial resync with the same master: the dataset is the
/// same stream, continued. Forgotten too once a `TAKEN` of the master at
/// that epoch or later has been taken in: the slots are the master's here
/// from then on, which the `UPDATE` filter accepts in any case.
#[derive(Clone, Debug)]
struct Arrived {
    master: String,
    epoch: u64,
    ranges: Ranges,
}

/// At most this many [`Arrived`] are kept, the oldest forgotten first: each
/// stands for one takeover attempt not yet followed by a claim.
const ARRIVED_KEPT: usize = 64;

impl FollowedImports {
    /// Of the imports into `master` whose data this node holds in full
    /// ([`Arrived`]): the config epoch each was to be taken over at, and its
    /// slots.
    pub(crate) fn arrived_into(&self, master: &str) -> Vec<(u64, Ranges)> {
        self.arrived
            .lock()
            .iter()
            .filter(|a| a.master == master)
            .map(|a| (a.epoch, a.ranges.clone()))
            .collect()
    }

    /// The last claim of `master` read in its stream, while this node follows
    /// it ([`NotHeld::stream_claim`]).
    pub(crate) fn stream_claim_of(&self, master: NodeId) -> Option<(u64, Ranges)> {
        let not_held = self.not_held.lock();
        if not_held.master != Some(master) {
            return None;
        }
        not_held.stream_claim.clone()
    }

    /// The epochs a replica of `master` claims its slots at when it stands
    /// for election, `view_epoch` being the master's config epoch here: the
    /// epoch of the newest claim of it read in its stream (`confirmed`),
    /// and for a slot an `ARRIVED` at a newer epoch covers, that epoch. For
    /// a master whose stream carries no markers, or that this node has
    /// no session with yet, `view_epoch`. Before the first claim read: `view_epoch` if this node
    /// started holding every slot of its master as nodes.conf has them (no
    /// file, see [`NotHeld::baseline`]); else 0, which a voter that knows
    /// when the master's slots came to it refuses for each of them.
    pub(crate) fn claim_epochs(&self, master: NodeId, view_epoch: u64) -> ClaimEpochs {
        let arrived = self.arrived_into(node_id_str(&master));
        self.not_held
            .lock()
            .claim_epochs(master, view_epoch, arrived)
    }

    /// What [`NotHeld`] is to be written down as: `None`, nothing (no
    /// file).
    fn not_held_text(&self) -> Option<String> {
        let not_held = self.not_held.lock();
        let master = not_held.master?;
        if not_held.plain || (not_held.slots.is_empty() && not_held.confirmed.is_none()) {
            return None;
        }
        let mut out = format!("master {}\n", node_id_str(&master));
        let mut run: Option<(u16, u16, u64)> = None;
        for (&slot, &epoch) in &not_held.slots {
            run = match run {
                Some((a, b, e)) if e == epoch && b + 1 == slot => Some((a, slot, e)),
                Some((a, b, e)) => {
                    out.push_str(&format!("{a}-{b} {e}\n"));
                    Some((slot, slot, epoch))
                }
                None => Some((slot, slot, epoch)),
            };
        }
        if let Some((a, b, e)) = run {
            out.push_str(&format!("{a}-{b} {e}\n"));
        }
        if let Some(confirmed) = not_held.confirmed {
            out.push_str(&format!("{CONFIRMED} {confirmed}\n"));
        }
        out.push_str(NOT_HELD_END);
        out.push('\n');
        Some(out)
    }

    /// Slots this node's table has just moved to `master`, the master it
    /// follows, each with the epoch of the claim (`gains`), from a claim not
    /// of its stream: not held, unless the last claim read in the stream
    /// lists them at that epoch or later, or an `ARRIVED` of the master at
    /// that very epoch covers them (see [`NotHeld`]).
    pub(crate) fn note_gains(&self, master: NodeId, gains: &[(u16, u64)]) {
        if gains.is_empty() {
            return;
        }
        let arrived = self.arrived_into(node_id_str(&master));
        let mut not_held = self.not_held.lock();
        not_held.about(master);
        if not_held.plain {
            return;
        }
        for &(slot, epoch) in gains {
            if not_held.stream_holds(slot, epoch)
                || arrived
                    .iter()
                    .any(|(e, r)| *e == epoch && in_ranges(r, slot))
            {
                not_held.slots.remove(&slot);
            } else {
                not_held.slots.insert(slot, epoch);
            }
        }
    }

    /// The stream of `master` confirmed `ranges`: a `TAKEN` at `epoch`
    /// (`exact` false: every gain at that epoch or before; the one sent ahead
    /// of a full sync's dataset, every gain of its slots), or an `ARRIVED` at
    /// it (`exact`: gains at that very epoch).
    fn confirm_held(&self, master: NodeId, epoch: u64, ranges: &[(u16, u16)], exact: bool) {
        let mut not_held = self.not_held.lock();
        if not_held.master != Some(master) {
            return;
        }
        if !exact {
            not_held.stream_claim = Some((epoch, ranges.to_vec()));
            not_held.confirmed = Some(not_held.confirmed.map_or(epoch, |c| c.max(epoch)));
        }
        let epoch = if !exact && std::mem::take(&mut not_held.sync_claim_due) {
            u64::MAX
        } else {
            epoch
        };
        not_held.slots.retain(|slot, gained_at| {
            !(in_ranges(ranges, *slot)
                && if exact {
                    *gained_at == epoch
                } else {
                    *gained_at <= epoch
                })
        });
    }

    /// Of the slots this node's table shows as `master`'s, those it does not
    /// hold (see [`NotHeld`]), in order. Only for the master it follows.
    pub(crate) fn not_held_of(&self, master: NodeId) -> Vec<u16> {
        let not_held = self.not_held.lock();
        if not_held.master != Some(master) {
            return Vec::new();
        }
        not_held.slots.keys().copied().collect()
    }

    /// This node is no one's replica any more (promoted, reset).
    pub(crate) fn forget_not_held(&self) {
        *self.not_held.lock() = NotHeld::default();
    }

    /// The imports marker was read: the claim sent ahead of a full sync's
    /// dataset, if any, came before it.
    fn sync_claim_read(&self) {
        self.not_held.lock().sync_claim_due = false;
    }

    /// A session with `master` began, a full sync (`full`) or not; its
    /// stream carries markers (`marked`), or not (1.2: every slot of it in
    /// the table is held, as before).
    fn session_with(&self, master: Option<NodeId>, marked: bool, full: bool) {
        let mut not_held = self.not_held.lock();
        match master {
            Some(master) => not_held.about(master),
            None => {
                *not_held = NotHeld::default();
                return;
            }
        }
        not_held.plain = !marked;
        not_held.sync_claim_due = marked && full;
        if full || !marked {
            not_held.stream_claim = None;
        }
        if not_held.plain {
            not_held.slots.clear();
        }
    }

    /// Note an `ARRIVED` marker of `master`.
    fn note_arrived(&self, master: String, epoch: u64, ranges: Ranges) {
        let mut arrived = self.arrived.lock();
        arrived.retain(|a| !(a.master == master && a.epoch == epoch));
        arrived.push(Arrived {
            master,
            epoch,
            ranges,
        });
        if arrived.len() > ARRIVED_KEPT {
            let over = arrived.len() - ARRIVED_KEPT;
            arrived.drain(..over);
        }
    }

    /// The imports known, as they stand.
    pub(crate) fn list(&self) -> Vec<Followed> {
        self.imports.lock().clone()
    }

    /// What to write down of `imports`, taken holding their lock: its
    /// number, and the file's text (`None`: no import, no file).
    fn look(&self, imports: &[Followed]) -> (u64, Option<String>) {
        let n = self.looks.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        let text = (!imports.is_empty()).then(|| {
            imports
                .iter()
                .map(|f| {
                    format!(
                        "{} {} {}\n",
                        f.id,
                        ranges_word(&f.ranges),
                        kept_word(&f.kept_on_disk())
                    )
                })
                .collect()
        });
        (n, text)
    }

    /// Forget every import (`CLUSTER RESET`): this node is no longer
    /// anyone's replica, nor in the cluster whose slots they were.
    pub(crate) fn forget_all(&self, cluster: &ClusterState, why: &str) {
        let look = {
            let mut imports = self.imports.lock();
            if imports.is_empty() {
                return;
            }
            log_info!(
                "Cluster: forgetting the {} import(s) into this node's master it knew of ({}): \
                 {why}; their keys are left as they are",
                imports.len(),
                listed_ranges(&imports)
            );
            imports.clear();
            self.look(&imports)
        };
        write_down(cluster, look);
    }

    fn wake(&self) {
        *self.woken.lock() = true;
        self.wake.notify_all();
    }

    /// Wait for `timeout`, or a wake.
    fn wait(&self, timeout: Duration) {
        let mut woken = self.woken.lock();
        if !*woken {
            self.wake.wait_for(&mut woken, timeout);
        }
        *woken = false;
    }
}

// ── What a replica does not hold, on disk ────────────────────────────────────

/// The last line of `<cluster-config-file>.not-held`: a file without it was
/// cut short.
const NOT_HELD_END: &str = "end";

fn not_held_path(cluster: &ClusterState) -> Option<std::path::PathBuf> {
    cluster.config_path.as_ref().map(|p| {
        let mut name = p.as_os_str().to_owned();
        name.push(".not-held");
        std::path::PathBuf::from(name)
    })
}

/// Write [`NotHeld`] down, synced, if it changed since it was last written
/// (none: remove the file). One writer at a time, the last state written.
pub(crate) fn save_not_held(cluster: &ClusterState) {
    static WRITTEN: Mutex<Option<Option<String>>> = Mutex::new(None);
    let Some(path) = not_held_path(cluster) else {
        return;
    };
    if !cluster
        .followed_imports
        .not_held_loaded
        .load(std::sync::atomic::Ordering::SeqCst)
    {
        return;
    }
    let mut written = WRITTEN.lock();
    let text = cluster.followed_imports.not_held_text();
    if written.as_ref() == Some(&text) {
        return;
    }
    let saved = match &text {
        None => match std::fs::remove_file(&path) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        },
        Some(text) => {
            let mut tmp = path.as_os_str().to_owned();
            tmp.push(".tmp");
            let tmp = std::path::PathBuf::from(tmp);
            write_synced(&tmp, text.as_bytes()).and_then(|()| std::fs::rename(&tmp, &path))
        }
    };
    if let Some(dir) = path.parent()
        && let Ok(dir) = std::fs::File::open(dir)
    {
        let _ = dir.sync_all();
    }
    match saved {
        Ok(()) => *written = Some(text),
        Err(e) => log_warn!(
            "Cluster: could not write down which slots of its master this node does not hold \
             ({}): {e}",
            path.display()
        ),
    }
}

/// [`FollowedImports::forget_not_held`], written down.
pub(crate) fn forget_not_held(cluster: &ClusterState) {
    cluster.followed_imports.forget_not_held();
    save_not_held(cluster);
}

/// The file read back (see [`NotHeld`]): its master and slots, or `Err` if
/// it does not read.
fn parse_not_held(text: &str) -> Result<(NodeId, std::collections::BTreeMap<u16, u64>), ()> {
    let mut lines = text.lines();
    let master = lines
        .next()
        .and_then(|l| l.strip_prefix("master "))
        .and_then(|id| super::node::parse_node_id(id.as_bytes()))
        .ok_or(())?;
    let mut slots = std::collections::BTreeMap::new();
    let mut ended = false;
    for line in lines {
        if ended {
            return Err(());
        }
        if line == NOT_HELD_END {
            ended = true;
            continue;
        }
        if line.starts_with(CONFIRMED) {
            continue;
        }
        let (range, epoch) = line.split_once(' ').ok_or(())?;
        let epoch: u64 = epoch.parse().map_err(|_| ())?;
        let (a, b) = range.split_once('-').ok_or(())?;
        let (a, b): (u16, u16) = (a.parse().map_err(|_| ())?, b.parse().map_err(|_| ())?);
        if a > b || b as usize >= super::CLUSTER_SLOTS {
            return Err(());
        }
        for slot in a..=b {
            slots.insert(slot, epoch);
        }
    }
    if !ended {
        return Err(());
    }
    Ok((master, slots))
}

/// The line of `<cluster-config-file>.not-held` that gives
/// [`NotHeld::confirmed`].
const CONFIRMED: &str = "confirmed";

/// [`NotHeld::confirmed`] as written down in `text`, if it is.
fn parse_confirmed(text: &str) -> Option<u64> {
    text.lines()
        .find_map(|l| l.strip_prefix(CONFIRMED)?.strip_prefix(' ')?.parse().ok())
}

/// At start-up: what this node, a replica, does not hold of its master's
/// slots, as written down (see [`NotHeld`]).
fn load_not_held(cluster: &ClusterState) {
    struct Loaded<'a>(&'a ClusterState);
    impl Drop for Loaded<'_> {
        fn drop(&mut self) {
            self.0
                .followed_imports
                .not_held_loaded
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let _loaded = Loaded(cluster);
    let Some(path) = not_held_path(cluster) else {
        return;
    };
    let text = match std::fs::read(&path) {
        Ok(bytes) => String::from_utf8(bytes).ok(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // No file: every slot of the master is held as nodes.conf has
            // them, and claimed at its epoch there (`NotHeld::baseline`).
            if let Some(master) = cluster.my_master_id() {
                let mut not_held = cluster.followed_imports.not_held.lock();
                not_held.about(master);
                not_held.baseline = true;
            }
            return;
        }
        Err(_) => None,
    };
    let Some(master) = cluster.my_master_id() else {
        return;
    };
    let read = text.as_deref().ok_or(()).and_then(parse_not_held);
    // The master's slots in the table, read before the list is locked (the
    // order every other path takes the two in).
    let of_master: Vec<u16> = {
        let owners = cluster.slots.read();
        (0..super::CLUSTER_SLOTS as u16)
            .filter(|&slot| owners[slot as usize] == Some(master))
            .collect()
    };
    let mut not_held = cluster.followed_imports.not_held.lock();
    not_held.about(master);
    match read {
        Ok((of, slots)) if of == master => {
            not_held.slots = slots
                .into_iter()
                .filter(|(slot, _)| of_master.binary_search(slot).is_ok())
                .collect();
            not_held.confirmed = text.as_deref().and_then(parse_confirmed);
            not_held.baseline = not_held.confirmed.is_none();
            if !not_held.slots.is_empty() {
                log_info!(
                    "Cluster: this node does not hold slots {} of its master yet: its master \
                     took them, and its replication stream has not confirmed them",
                    migration::ranges_text(&slot_runs(not_held.slots.keys().copied()))
                );
            }
        }
        // Of another master: nothing of this one.
        Ok(_) => {}
        Err(()) => {
            not_held.slots = of_master.iter().map(|&slot| (slot, 0)).collect();
            log_warn!(
                "Cluster: {} does not read: every slot of this node's master is taken as not \
                 held until its replication stream confirms it",
                path.display()
            );
        }
    }
}

// ── The marker ───────────────────────────────────────────────────────────────

/// `a-b,c-d`; `-` for none.
fn ranges_word(ranges: &[(u16, u16)]) -> String {
    if ranges.is_empty() {
        return "-".into();
    }
    ranges
        .iter()
        .map(|(a, b)| format!("{a}-{b}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// [`ranges_word`] read back.
fn parse_ranges_word(word: &str) -> Option<Ranges> {
    if word == "-" {
        return Some(Vec::new());
    }
    let mut ranges = Vec::new();
    for part in word.split(',') {
        let (a, b) = part.split_once('-')?;
        let (a, b): (u16, u16) = (a.parse().ok()?, b.parse().ok()?);
        if a > b || b as usize >= super::CLUSTER_SLOTS {
            return None;
        }
        ranges.push((a, b));
    }
    Some(ranges)
}

/// `*` for not looked at yet, else [`ranges_word`].
fn kept_word(kept: &Option<Ranges>) -> String {
    match kept {
        None => "*".into(),
        Some(kept) => ranges_word(kept),
    }
}

fn parse_kept_word(word: &str) -> Option<Option<Ranges>> {
    if word == "*" {
        Some(None)
    } else {
        parse_ranges_word(word).map(Some)
    }
}

/// One import as a marker lists it: id, slots, kept.
type Listed = (String, Ranges, Option<Ranges>);

/// The marker of `listed`, `LIVE` or `SYNC`.
pub(crate) fn encode_marker(kind: &str, listed: &[Listed]) -> Vec<u8> {
    let mut parts: Vec<Vec<u8>> = vec![SLOT_IMPORTS_MARKER.to_vec(), kind.as_bytes().to_vec()];
    for (id, ranges, kept) in listed {
        parts.push(id.as_bytes().to_vec());
        parts.push(ranges_word(ranges).into_bytes());
        parts.push(kept_word(kept).into_bytes());
    }
    crate::resp::write::encode_resp_command(&parts)
}

/// A `TAKEN` marker: the master that sends it owns `ranges` at config epoch
/// `epoch`, what its heartbeats claim. It sends it as it takes slots over,
/// before it serves any write to them, so that a replica that has applied
/// such a write knows whose slots they are.
pub(crate) fn encode_taken(epoch: u64, ranges: &[(u16, u16)]) -> Vec<u8> {
    crate::resp::write::encode_resp_command(&[
        SLOT_IMPORTS_MARKER.to_vec(),
        b"TAKEN".to_vec(),
        epoch.to_string().into_bytes(),
        ranges_word(ranges).into_bytes(),
    ])
}

/// A `TAKEN` marker read back: its epoch and slots. `None` if it is not one.
fn parse_taken(args: &[&[u8]]) -> Option<(u64, Ranges)> {
    let [name, kind, epoch, ranges] = args else {
        return None;
    };
    if !name.eq_ignore_ascii_case(SLOT_IMPORTS_MARKER) || !kind.eq_ignore_ascii_case(b"TAKEN") {
        return None;
    }
    let epoch = std::str::from_utf8(epoch).ok()?.parse().ok()?;
    let ranges = parse_ranges_word(std::str::from_utf8(ranges).ok()?)?;
    Some((epoch, ranges))
}

/// An `ARRIVED` marker: every record of an import of `ranges` into the
/// master that sends it has come before it in the stream, for the master to
/// take them over at config epoch `epoch`. Sent at the takeover, after the
/// last record of the import applied, before the master waits for its
/// replicas to acknowledge what arrived: a replica that has read it holds
/// every key of the slots. It does not say that the slots were taken: that
/// is `TAKEN`'s, and a takeover refused leaves them with the source.
pub(crate) fn encode_arrived(epoch: u64, ranges: &[(u16, u16)]) -> Vec<u8> {
    crate::resp::write::encode_resp_command(&[
        SLOT_IMPORTS_MARKER.to_vec(),
        b"ARRIVED".to_vec(),
        epoch.to_string().into_bytes(),
        ranges_word(ranges).into_bytes(),
    ])
}

/// An `ARRIVED` marker read back: its epoch and slots. `None` if it is not
/// one.
fn parse_arrived(args: &[&[u8]]) -> Option<(u64, Ranges)> {
    let [name, kind, epoch, ranges] = args else {
        return None;
    };
    if !name.eq_ignore_ascii_case(SLOT_IMPORTS_MARKER) || !kind.eq_ignore_ascii_case(b"ARRIVED") {
        return None;
    }
    let epoch = std::str::from_utf8(epoch).ok()?.parse().ok()?;
    let ranges = parse_ranges_word(std::str::from_utf8(ranges).ok()?)?;
    Some((epoch, ranges))
}

/// The master whose stream this is, if it is the one this node follows: a
/// marker of another is not taken in.
fn origin_if_followed(cluster: &ClusterState, what: &str) -> Option<NodeId> {
    let master = cluster.my_master_id()?;
    let origin = cluster
        .hooks
        .get()
        .and_then(|hooks| hooks.replica_starter.repl_state.stream_origin());
    if !origin.as_deref().is_some_and(|o| o == node_id_str(&master)) {
        log_info!(
            "Cluster: {what} in the replication stream, of {}, is not taken in: this node \
             follows {} now",
            origin.as_deref().unwrap_or("an unknown master"),
            node_id_str(&master)
        );
        return None;
    }
    Some(master)
}

/// Note an `ARRIVED` marker of this node's master (see [`encode_arrived`]).
fn note_masters_arrived(cluster: &ClusterState, epoch: u64, ranges: Ranges) {
    if ranges.is_empty() {
        return;
    }
    let Some(master) = origin_if_followed(cluster, "an import's arrival") else {
        return;
    };
    log_info!(
        "Cluster: this node holds what arrived of slots {} on its master, for a takeover at \
         epoch {epoch}",
        migration::ranges_text(&ranges)
    );
    cluster
        .followed_imports
        .confirm_held(master, epoch, &ranges, true);
    save_not_held(cluster);
    cluster
        .followed_imports
        .note_arrived(node_id_str(&master).to_string(), epoch, ranges);
}

/// Take in a `TAKEN` marker of this node's master: its claim of `ranges` at
/// `epoch`, as its heartbeat would bring it, applied in the marker's place
/// in the stream.
fn take_in_masters_claim(cluster: &ClusterState, epoch: u64, ranges: &[(u16, u16)]) {
    if cluster.my_master_id().is_none() || ranges.is_empty() {
        return;
    }
    // The claim of the master whose stream this is: taken in only while it
    // is the master this node follows.
    let Some(master) = origin_if_followed(cluster, "a claim of slots") else {
        return;
    };
    // Taken into the table first, then its slots held: the stream confirmed
    // them. (Held also for a test of a replica that takes the claim into its
    // table only from the heartbeats.)
    let ignored = test_ignores_stream_claims();
    if !ignored {
        cluster.take_in_claims_of_master(master, epoch, ranges);
    }
    cluster
        .followed_imports
        .confirm_held(master, epoch, ranges, false);
    save_not_held(cluster);
    if ignored {
        return;
    }
    // What arrived for a takeover at this epoch or before is the master's
    // here now, or was taken from it since by a newer claim.
    let master = node_id_str(&master);
    cluster
        .followed_imports
        .arrived
        .lock()
        .retain(|a| !(a.master == master && a.epoch <= epoch));
}

/// For the tests of a replica that has not taken in its master's claim:
/// `KIVIDB_TEST_IGNORE_STREAM_CLAIMS`, set, has this node take in no `TAKEN`
/// marker. Never set otherwise.
fn test_ignores_stream_claims() -> bool {
    static SET: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *SET.get_or_init(|| std::env::var_os("KIVIDB_TEST_IGNORE_STREAM_CLAIMS").is_some())
}

/// A marker read back: whether it is `LIVE`, and what it lists. `None` if
/// it does not read as one.
fn parse_marker(args: &[&[u8]]) -> Option<(bool, Vec<Listed>)> {
    let (name, rest) = args.split_first()?;
    if !name.eq_ignore_ascii_case(SLOT_IMPORTS_MARKER) {
        return None;
    }
    let (kind, rest) = rest.split_first()?;
    let live = if kind.eq_ignore_ascii_case(b"LIVE") {
        true
    } else if kind.eq_ignore_ascii_case(b"SYNC") {
        false
    } else {
        return None;
    };
    if rest.len() % 3 != 0 {
        return None;
    }
    let mut listed = Vec::new();
    for item in rest.chunks(3) {
        let text = |b: &[u8]| std::str::from_utf8(b).ok().map(str::to_string);
        let id = text(item[0])?;
        let ranges = parse_ranges_word(&text(item[1])?)?;
        let kept = parse_kept_word(&text(item[2])?)?;
        if ranges.is_empty() {
            return None;
        }
        listed.push((id, ranges, kept));
    }
    Some((live, listed))
}

/// `a`'s slots and `b`'s; `None` only if both are.
fn union(a: &Option<Ranges>, b: &Option<Ranges>) -> Option<Ranges> {
    if a.is_none() && b.is_none() {
        return None;
    }
    let mut slots: Vec<u16> = a
        .iter()
        .chain(b.iter())
        .flat_map(|r| slots_of(r).collect::<Vec<_>>())
        .collect();
    slots.sort_unstable();
    slots.dedup();
    Some(slot_runs(slots))
}

/// The imports a marker (`live`, `listed`) shows its master has just looked
/// at, which this node, knowing them as `before`, is to note its own keys
/// of now: once each, in a `LIVE` marker only (one sent in its place among
/// the writes).
fn to_note(before: &[Followed], live: bool, listed: &[Listed]) -> Vec<(String, Ranges)> {
    if !live {
        return Vec::new();
    }
    listed
        .iter()
        .filter(|(id, _, kept)| {
            kept.is_some()
                && before
                    .iter()
                    .any(|k| k.id == *id && k.kept.is_none() && k.held.is_none())
        })
        .map(|(id, ranges, _)| (id.clone(), ranges.clone()))
        .collect()
}

/// What this node now knows, given what it knew (`before`), a marker
/// (`listed`), and what it noted for it (`noted`, see [`to_note`]). The
/// slots of an import known already are only ever fewer: one that came to
/// this node was left out for good ([`settle`]).
fn reconcile(
    before: &[Followed],
    listed: Vec<Listed>,
    noted: &[(String, Arc<HeldBefore>)],
) -> Vec<Followed> {
    listed
        .into_iter()
        .map(|(id, ranges, kept)| {
            let known = before.iter().find(|f| f.id == id);
            let held = noted
                .iter()
                .find(|(n, _)| *n == id)
                .map(|(_, held)| Arc::clone(held))
                .or_else(|| known.and_then(|k| k.held.clone()));
            let ranges = match known {
                Some(k) => slot_runs(slots_of(&ranges).filter(|&s| in_ranges(&k.ranges, s))),
                None => ranges,
            };
            Followed {
                id,
                ranges,
                kept: union(&known.and_then(|k| k.kept.clone()), &kept),
                held,
                came: known.and_then(|k| k.came.clone()),
                seen_since_start: known.is_none_or(|k| k.seen_since_start),
            }
        })
        .filter(|f| !f.ranges.is_empty())
        .collect()
}

// ── Events from the replication client ───────────────────────────────────────

/// What the replication client tells (see [`SlotImportsEvent`]).
pub(crate) fn on_event(
    cluster: &ClusterState,
    store: &crate::store::Store,
    event: SlotImportsEvent,
) {
    let followed = &cluster.followed_imports;
    match event {
        SlotImportsEvent::Session { marked, full } => {
            followed.session_with(cluster.my_master_id(), marked, full);
            save_not_held(cluster);
            // What arrived (`ARRIVED`) is in this node's dataset while that
            // is the stream it was read in: not after a full sync, nor once
            // this node follows another master; still after a partial
            // resync with the same one.
            {
                let master = cluster.my_master_id().map(|m| node_id_str(&m).to_string());
                let mut arrived = followed.arrived.lock();
                if full || master.is_none() {
                    arrived.clear();
                } else {
                    arrived.retain(|a| Some(&a.master) == master.as_ref());
                }
            }
            let mut imports = followed.imports.lock();
            if (marked && !full) || imports.is_empty() {
                return;
            }
            log_info!(
                "Cluster: forgetting the {} import(s) into this node's master it knew of ({}): {}",
                imports.len(),
                listed_ranges(&imports),
                if marked {
                    "the master sends its dataset in full, and tells of them again"
                } else {
                    "the master does not tell of its imports (an earlier release)"
                }
            );
            imports.clear();
            let look = followed.look(&imports);
            drop(imports);
            write_down(cluster, look);
        }
        SlotImportsEvent::Marker(args) => {
            if let Some((epoch, ranges)) = parse_taken(args) {
                take_in_masters_claim(cluster, epoch, &ranges);
                return;
            }
            if let Some((epoch, ranges)) = parse_arrived(args) {
                note_masters_arrived(cluster, epoch, ranges);
                return;
            }
            followed.sync_claim_read();
            let Some((live, listed)) = parse_marker(args) else {
                log_warn!(
                    "Cluster: the master sent a slot-imports marker this node cannot read; \
                     ignored"
                );
                return;
            };
            // Noted here, at the marker's place in the stream: the caller
            // applies nothing until this returns (it holds the apply lock),
            // so this node's keys of the slots are those it had as its
            // master looked. Not holding the list's lock. This walks the
            // import's slots only (their shards), as the master's own look
            // does, at most `HELD_BEFORE_MAX_KEYS` names or 64 MiB; the
            // replica applies and acknowledges nothing meanwhile.
            let wanted = to_note(&followed.imports.lock(), live, &listed);
            let noted: Vec<(String, Arc<HeldBefore>)> = wanted
                .into_iter()
                .map(|(id, ranges)| (id, Arc::new(HeldBefore::note(store, &ranges))))
                .collect();
            let mut imports = followed.imports.lock();
            let now = reconcile(&imports, listed, &noted);
            for f in now.iter().filter(|f| f.kept.is_some() && f.held.is_none()) {
                if !imports.iter().any(|k| k.id == f.id) {
                    log_info!(
                        "Cluster: this node's master is importing slots {} (task {}), begun \
                         before this node saw it: if this node is promoted before it ends, \
                         every key of the slots its master held keys of before is kept",
                        migration::ranges_text(&f.ranges),
                        f.id
                    );
                }
            }
            let changed = now.len() != imports.len()
                || now.iter().zip(imports.iter()).any(|(a, b)| {
                    a.id != b.id || a.kept != b.kept || a.held.is_some() != b.held.is_some()
                });
            *imports = now;
            if changed {
                // Written out of the list's lock (in order: see `look`).
                let look = followed.look(&imports);
                drop(imports);
                write_down(cluster, look);
            }
        }
        SlotImportsEvent::Promoted => {
            // Each slot's count of comings as of now, the promotion's
            // claim included: one that comes later is left out.
            settle(cluster);
            let left: Vec<Followed> = followed
                .imports
                .lock()
                .iter()
                .filter(|f| f.deletes_any())
                .cloned()
                .collect();
            if !left.is_empty() {
                log_warn!(
                    "Cluster: promoted with {} import(s) into its old master unfinished ({}): \
                     what they brought is deleted once each slot's owner says it owns the slot \
                     and is not moving it; the keys this node held before are kept",
                    left.len(),
                    listed_ranges(&left)
                );
            }
            // Its replicas are told at once, before any can attach.
            migration::save_imports(cluster);
            followed.wake();
        }
    }
}

/// The slots of `imports`, for a log line.
fn listed_ranges(imports: &[Followed]) -> String {
    imports
        .iter()
        .map(|f| migration::ranges_text(&f.ranges))
        .collect::<Vec<_>>()
        .join(", ")
}

// ── Master side ──────────────────────────────────────────────────────────────

/// Tell this node's replicas which imports it has under way (`under_way`,
/// its own), and which of its old master's it still has to clean up. Called
/// by `migration::save_imports`, one at a time, in the order of the changes.
pub(super) fn send_markers(cluster: &ClusterState, under_way: &[Listed]) {
    let Some(hooks) = cluster.hooks.get() else {
        return;
    };
    let repl_state = &hooks.replica_starter.repl_state;
    let mut listed: Vec<Listed> = under_way.to_vec();
    for f in cluster.followed_imports.imports.lock().iter() {
        if !listed.iter().any(|(id, _, _)| *id == f.id) {
            listed.push((f.id.clone(), f.ranges.clone(), f.kept_on_disk()));
        }
    }
    repl_state.send_slot_imports(
        encode_marker("LIVE", &listed),
        encode_marker("SYNC", &listed),
    );
}

/// `CLUSTER MIGRATION IMPORT` of `ranges`: refused while what an import into
/// this node's old master brought of them is still to be deleted.
pub(crate) fn refuse_while_cleaning(
    cluster: &ClusterState,
    ranges: &[(u16, u16)],
) -> Result<(), String> {
    let me = cluster.myself();
    let table = cluster.slots.read();
    for f in cluster.followed_imports.imports.lock().iter() {
        if let Some(slot) = slots_of(ranges).find(|&slot| {
            in_ranges(&f.ranges, slot)
                && table[slot as usize] != Some(me)
                && f.plan(slot) != Plan::Keep
        }) {
            return Err(format!(
                "the keys an import into this node's old master brought of slot {slot} (task {}, \
                 cut short when that master failed) are still to be deleted: they would stay \
                 beside what this import brings, and keys the source deleted since would be \
                 back. The log says why they are not deleted yet; send it again once they are",
                f.id
            ));
        }
    }
    Ok(())
}

/// How many slots of its old master's imports a promoted node has still to
/// clean up (`INFO cluster`).
pub(crate) fn slots_to_clean(cluster: &ClusterState) -> usize {
    // A replica: none, as for orphans; its keys are its master's.
    if cluster.my_master_id().is_some() {
        return 0;
    }
    let me = cluster.myself();
    let table = cluster.slots.read();
    let mut slots: Vec<u16> = cluster
        .followed_imports
        .imports
        .lock()
        .iter()
        .flat_map(|f| {
            slots_of(&f.ranges)
                .filter(|&slot| table[slot as usize] != Some(me) && f.plan(slot) != Plan::Keep)
                .collect::<Vec<_>>()
        })
        .collect();
    slots.sort_unstable();
    slots.dedup();
    slots.len()
}

// ── On disk ──────────────────────────────────────────────────────────────────

/// `<cluster-config-file>.followed-imports`.
fn path(cluster: &ClusterState) -> Option<std::path::PathBuf> {
    cluster.config_path.as_ref().map(|p| {
        let mut name = p.as_os_str().to_owned();
        name.push(".followed-imports");
        std::path::PathBuf::from(name)
    })
}

/// Write a [`FollowedImports::look`] down (no import: remove the file): one
/// line each, `<id> <slots> <kept>`, the slots of which every key stays
/// after a restart. Out of the list's lock; a look older than the last one
/// written is not written.
fn write_down(cluster: &ClusterState, (n, text): (u64, Option<String>)) {
    let Some(path) = path(cluster) else {
        return;
    };
    let mut written = cluster.followed_imports.written.lock();
    if *written >= n {
        return;
    }
    *written = n;
    let written = match text {
        None => match std::fs::remove_file(&path) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        },
        Some(text) => {
            let mut tmp = path.as_os_str().to_owned();
            tmp.push(".tmp");
            let tmp = std::path::PathBuf::from(tmp);
            write_synced(&tmp, text.as_bytes()).and_then(|()| std::fs::rename(&tmp, &path))
        }
    };
    if let Err(e) = written {
        log_warn!(
            "Cluster: could not write down the imports of this node's master in {}: {e}",
            path.display()
        );
    }
}

fn write_synced(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

/// One line of the file read back.
fn parse_line(line: &str) -> Option<Followed> {
    let mut words = line.split_ascii_whitespace();
    let id = words.next()?.to_string();
    let ranges = parse_ranges_word(words.next()?)?;
    let kept = parse_kept_word(words.next()?)?;
    if ranges.is_empty() || words.next().is_some() {
        return None;
    }
    Some(Followed {
        id,
        ranges,
        kept,
        held: None,
        came: None,
        seen_since_start: false,
    })
}

/// At startup: what this node knew when it stopped. A line it cannot read
/// is left out, and with it that import: nothing of it is deleted, its
/// keys stay as an earlier release left them (`CLUSTER ORPHANSLOTS`).
fn load(cluster: &ClusterState) {
    let Some(path) = path(cluster) else {
        return;
    };
    let Ok(text) = std::fs::read_to_string(&path) else {
        return;
    };
    let mut imports = Vec::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        match parse_line(line) {
            Some(f) => imports.push(f),
            None => log_warn!(
                "Cluster: a line of {} this node cannot read, left out: {line}",
                path.display()
            ),
        }
    }
    if !imports.is_empty() {
        log_info!(
            "Cluster: {} import(s) into this node's master were under way when it stopped ({}): \
             every key of a slot that had keys before is kept",
            imports.len(),
            listed_ranges(&imports)
        );
    }
    *cluster.followed_imports.imports.lock() = imports;
}

// ── The cleanup ──────────────────────────────────────────────────────────────

/// From `ClusterState::start`: read what was written down, hook into the
/// replication client, and start the cleanup thread.
pub(crate) fn start(cluster: &Arc<ClusterState>, repl_state: &crate::replication::ReplState) {
    load(cluster);
    load_not_held(cluster);
    // Given to a master this node replicates from (`REPLCONF node-id`).
    repl_state.set_my_node_id(Some(node_id_str(&cluster.myself()).to_string()));
    if !crate::replication::hides_slot_imports_capability() {
        repl_state
            .sends_slot_imports
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
    let weak = Arc::downgrade(cluster);
    let spawned = std::thread::Builder::new()
        .name("kividb-import-cleanup".into())
        .spawn_released(move || run(&weak));
    if let Err(e) = spawned {
        log_error!(
            "Cluster: could not start the thread that deletes what an unfinished import into \
             a failed master brought: {e}; CLUSTER ORPHANSLOTS lists those keys"
        );
    }
}

/// The hook installed in the replication client.
pub(crate) fn hook(cluster: &Arc<ClusterState>) -> crate::replication::SlotImportsFn {
    let weak = Arc::downgrade(cluster);
    Box::new(move |event| {
        let Some(cluster) = weak.upgrade() else {
            return;
        };
        let Some(hooks) = cluster.hooks.get() else {
            return;
        };
        on_event(&cluster, &hooks.replica_starter.store, event);
    })
}

/// How one round of the cleanup of an import went.
enum Outcome {
    /// Nothing is left to delete: why, for the log.
    Done(String),
    /// Not now: why.
    Wait(String),
}

fn run(cluster: &Weak<ClusterState>) {
    let mut asker = migration::NodeAsker::default();
    let mut waiting: ahash::AHashMap<String, (String, Instant)> = Default::default();
    loop {
        let Some(cluster) = cluster.upgrade() else {
            return;
        };
        cluster.followed_imports.wait(RETRY);
        let Some(hooks) = cluster.hooks.get() else {
            continue;
        };
        if cluster.followed_imports.imports.lock().is_empty() {
            continue;
        }
        let starter = &hooks.replica_starter;
        let env = Env {
            store: Arc::clone(&starter.store),
            index_store: Arc::clone(&starter.index_store),
            repl_state: Arc::clone(&starter.repl_state),
            hub_tx: starter.hub_tx.clone(),
            cluster: Arc::clone(&cluster),
            config: Arc::clone(&starter.config),
        };
        let generation = env.repl_state.generation();
        let master = || {
            cluster.my_master_id().is_none()
                && !env.repl_state.is_slave()
                && env.repl_state.generation() == generation
        };
        if !master() {
            waiting.clear();
            settle(&cluster);
            continue;
        }
        // On a replica too: a slot that comes here is left out for good.
        settle(&cluster);
        let imports = cluster.followed_imports.list();
        if imports.is_empty() {
            continue;
        }
        let mut done: Vec<String> = Vec::new();
        for f in &imports {
            match clean(&env, &mut asker, f) {
                Outcome::Done(what) => {
                    log_info!(
                        "Cluster: the unfinished import {} of slots {} into this node's old \
                         master: {what}",
                        f.id,
                        migration::ranges_text(&f.ranges)
                    );
                    waiting.remove(&f.id);
                    done.push(f.id.clone());
                }
                Outcome::Wait(why) => {
                    let due = waiting
                        .get(&f.id)
                        .is_none_or(|(last, at)| *last != why || at.elapsed() >= RELOG);
                    if due {
                        log_info!(
                            "Cluster: what the unfinished import {} of slots {} into this \
                             node's old master brought is not deleted yet: {why}",
                            f.id,
                            migration::ranges_text(&f.ranges)
                        );
                        waiting.insert(f.id.clone(), (why, Instant::now()));
                    }
                }
            }
        }
        if done.is_empty() {
            continue;
        }
        let look = {
            let mut list = cluster.followed_imports.imports.lock();
            // Not if this node has been a replica since: the list is its
            // new master's.
            if !master() {
                continue;
            }
            list.retain(|f| !done.contains(&f.id));
            cluster.followed_imports.look(&list)
        };
        write_down(&cluster, look);
        migration::save_imports(&cluster);
    }
}

/// Leave out of every import, for good, each slot that is this node's or
/// has come to it (`SlotDeletions::came_here`: assigned, imported, taken
/// over) since this node first looked: keys written to it meanwhile are
/// acknowledged writes, and after it leaves again they would look like what
/// the import brought (#741 review). Also notes the counts of an import not
/// looked at yet. Written down, and the replicas told, at once.
fn settle(cluster: &ClusterState) {
    // Nothing to do, and nothing to wait for: most of the time, and every
    // promotion of a node that knew of no import (called from it).
    if cluster.followed_imports.imports.lock().is_empty() {
        return;
    }
    let me = cluster.myself();
    let mut gone: Vec<(String, Vec<u16>)> = Vec::new();
    let look = {
        // In the documented order: `slots`, `migrations`, the list, the
        // counts. Whatever gives this node a slot counts it holding one of
        // the first two, so the counts read here are as of the table read.
        let table = cluster.slots.read();
        let _migrations = cluster.migrations.read();
        let mut imports = cluster.followed_imports.imports.lock();
        if imports.is_empty() {
            return;
        }
        for f in imports.iter_mut() {
            let slots: Vec<u16> = slots_of(&f.ranges).collect();
            let now = cluster.slot_deletions.came_counts(&slots);
            let base = f.came.get_or_insert_with(|| now.clone());
            let left: Vec<u16> = now
                .iter()
                .filter(|(slot, count)| {
                    table[*slot as usize] == Some(me)
                        || base.iter().any(|(s, c)| s == slot && c != count)
                })
                .map(|&(slot, _)| slot)
                .collect();
            if left.is_empty() {
                continue;
            }
            f.ranges = slot_runs(slots.into_iter().filter(|s| !left.contains(s)));
            gone.push((f.id.clone(), left));
        }
        if gone.is_empty() {
            return;
        }
        imports.retain(|f| !f.ranges.is_empty());
        cluster.followed_imports.look(&imports)
    };
    for (id, slots) in &gone {
        log_info!(
            "Cluster: {} of the unfinished import {id} into this node's old master came to this \
             node: left out of it for good; none of their keys is deleted for it",
            migration::ranges_text(&slot_runs(slots.iter().copied()))
        );
    }
    write_down(cluster, look);
    migration::save_imports(cluster);
}

/// One round of the cleanup of import `f`, on a master.
fn clean(env: &Env, asker: &mut migration::NodeAsker, f: &Followed) -> Outcome {
    let cluster = &env.cluster;
    if f.kept.is_none() {
        return Outcome::Done(
            "it had brought nothing: its master had not asked the source yet".into(),
        );
    }
    let me = cluster.myself();
    let slots: Vec<u16> = {
        let table = cluster.slots.read();
        slots_of(&f.ranges)
            .filter(|&slot| table[slot as usize] != Some(me) && f.plan(slot) != Plan::Keep)
            .collect()
    };
    if slots.is_empty() {
        return Outcome::Done(
            "nothing to delete: every key of its slots was held before, or the slots are this \
             node's"
                .into(),
        );
    }
    if let Err(why) = orphans::check_droppable(cluster, &slots) {
        return Outcome::Wait(why);
    }
    let owners: Vec<(u16, NodeId)> = {
        let table = cluster.slots.read();
        slots
            .iter()
            .filter_map(|&slot| table[slot as usize].map(|owner| (slot, owner)))
            .collect()
    };
    if owners.len() != slots.len() {
        return Outcome::Wait("a slot of it has no owner".into());
    }
    if let Err(why) = orphans::confirm_with_owners(env, asker, &owners) {
        if why.contains("unknown CLUSTER SYNCSLOTS subcommand") || why.contains("nknown subcommand")
        {
            log_warn!(
                "Cluster: the owner of slots of the unfinished import {} into this node's old \
                 master runs an earlier release, which cannot say whether it owns them: what \
                 the import brought is left, as an earlier release left it (CLUSTER \
                 ORPHANSLOTS lists those keys; CLUSTER DROPORPHANS deletes those the owner \
                 has): {why}",
                f.id
            );
            return Outcome::Done("left to CLUSTER DROPORPHANS".into());
        }
        return Outcome::Wait(why);
    }
    let owner_of: ahash::AHashMap<u16, NodeId> = owners.iter().copied().collect();
    let held = f.held.clone();
    let Some(came) = f.came.clone() else {
        return Outcome::Wait("its slots' counts are not noted yet".into());
    };
    let mut confirm = |keys: &[crate::store::Key]| -> Result<Vec<bool>, String> {
        // None of the batch's slots has come to this node since it first
        // looked (`settle`), even and gone again: such a slot's keys may
        // be acknowledged writes. One that comes during the batch is left
        // by the deletion itself (`DeleteRun`).
        let mut batch: Vec<u16> = keys.iter().map(|k| key_hash_slot(k.as_bytes())).collect();
        batch.sort_unstable();
        batch.dedup();
        for (slot, count) in cluster.slot_deletions.came_counts(&batch) {
            if !came.iter().any(|&(s, c)| s == slot && c == count) {
                return Err(format!(
                    "slot {slot} came to this node since it first looked: it is left out"
                ));
            }
        }
        // The owners once more, for this batch: each still owns its slots
        // and is not moving them. And, of an import read back from disk
        // (whether a slot was this node's before the restart is not known),
        // only the keys the owner has.
        let has = orphans::confirm_keys(env, asker, &owner_of, keys)?;
        Ok(keys
            .iter()
            .zip(has)
            .map(|(key, has)| {
                let brought = match f.plan(key_hash_slot(key.as_bytes())) {
                    Plan::All => true,
                    Plan::Unheld => held.as_ref().is_some_and(|held| !held.keys.contains(key)),
                    Plan::Keep => false,
                };
                brought && (f.seen_since_start || has)
            })
            .collect())
    };
    let what = format!("the unfinished import {} into this node's old master", f.id);
    match migration::drop_confirmed_as(
        cluster,
        &env.store,
        &env.repl_state,
        &env.hub_tx,
        &slots,
        &mut confirm,
        &what,
        "import-cleanup-deleting",
    ) {
        Err(why) => Outcome::Wait(why),
        Ok(dropped) => match dropped.halted {
            Some(why) => Outcome::Wait(format!(
                "stopped after deleting {} key(s): {why}",
                dropped.deleted
            )),
            None => Outcome::Done(format!(
                "deleted the {} key(s) it brought, and kept {} this node held before it, once \
                 the owners ({}) said they own the slots",
                dropped.deleted,
                dropped.kept,
                owners_text(&owners)
            )),
        },
    }
}

fn owners_text(owners: &[(u16, NodeId)]) -> String {
    let mut ids: Vec<String> = owners
        .iter()
        .map(|(_, owner)| node_id_str(owner).to_string())
        .collect();
    ids.sort();
    ids.dedup();
    ids.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_marker_reads_back() {
        let listed: Vec<Listed> = vec![
            ("a".repeat(40), vec![(0, 10), (20, 20)], None),
            ("b".repeat(40), vec![(96, 96)], Some(vec![])),
            ("c".repeat(40), vec![(5000, 5100)], Some(vec![(5001, 5002)])),
        ];
        for kind in ["LIVE", "SYNC"] {
            let encoded = encode_marker(kind, &listed);
            let (_, args) = crate::resp::parse::parse_resp_command(&encoded).unwrap();
            let (live, back) = parse_marker(&args).unwrap();
            assert_eq!(live, kind == "LIVE");
            assert_eq!(back, listed);
        }
        let empty = encode_marker("SYNC", &[]);
        let (_, args) = crate::resp::parse::parse_resp_command(&empty).unwrap();
        assert_eq!(parse_marker(&args), Some((false, vec![])));
    }

    /// A `TAKEN` marker reads back as its epoch and slots, and is not read
    /// as a list of imports; one that is not whole, or not valid, is none.
    #[test]
    fn a_taken_marker_reads_back() {
        let bytes = encode_taken(12, &[(0, 100), (5000, 5000)]);
        let (_, args) = crate::resp::parse::parse_resp_command(&bytes).unwrap();
        assert_eq!(parse_taken(&args), Some((12, vec![(0, 100), (5000, 5000)])));
        assert!(parse_marker(&args).is_none());
        let words = |w: &[&str]| -> Option<(u64, Ranges)> {
            let w: Vec<&[u8]> = w.iter().map(|s| s.as_bytes()).collect();
            parse_taken(&w)
        };
        assert_eq!(
            words(&["KIVIDB.IMPORTS", "TAKEN", "7", "-"]),
            Some((7, vec![]))
        );
        assert_eq!(words(&["KIVIDB.IMPORTS", "TAKEN", "7"]), None);
        assert_eq!(words(&["KIVIDB.IMPORTS", "TAKEN", "x", "0-1"]), None);
        assert_eq!(words(&["KIVIDB.IMPORTS", "TAKEN", "7", "5-1"]), None);
        assert_eq!(words(&["KIVIDB.IMPORTS", "TAKEN", "7", "0-16384"]), None);
        assert_eq!(words(&["KIVIDB.IMPORTS", "LIVE", "7", "0-1"]), None);
    }

    /// An `ARRIVED` marker reads back as its epoch and slots, and is not
    /// read as a `TAKEN` one, nor a `TAKEN` one as it.
    #[test]
    fn an_arrived_marker_reads_back() {
        let bytes = encode_arrived(9, &[(10, 20), (30, 30)]);
        let (_, args) = crate::resp::parse::parse_resp_command(&bytes).unwrap();
        assert_eq!(parse_arrived(&args), Some((9, vec![(10, 20), (30, 30)])));
        assert_eq!(parse_taken(&args), None);
        assert!(parse_marker(&args).is_none());
        let taken = encode_taken(9, &[(10, 20)]);
        let (_, args) = crate::resp::parse::parse_resp_command(&taken).unwrap();
        assert_eq!(parse_arrived(&args), None);
    }

    /// An `ARRIVED` of a master at an epoch is kept once, the newest kept
    /// when there are too many, and each master's apart.
    #[test]
    fn arrived_imports_are_kept_by_master_and_epoch() {
        let f = FollowedImports::default();
        f.note_arrived("a".into(), 5, vec![(0, 9)]);
        f.note_arrived("a".into(), 5, vec![(0, 9)]);
        f.note_arrived("b".into(), 6, vec![(10, 19)]);
        assert_eq!(f.arrived_into("a"), vec![(5, vec![(0, 9)])]);
        assert_eq!(f.arrived_into("b"), vec![(6, vec![(10, 19)])]);
        for epoch in 0..(ARRIVED_KEPT as u64 + 10) {
            f.note_arrived("c".into(), 100 + epoch, vec![(20, 29)]);
        }
        assert_eq!(f.arrived.lock().len(), ARRIVED_KEPT);
        assert!(f.arrived_into("a").is_empty());
        assert!(
            f.arrived_into("c")
                .iter()
                .any(|(e, _)| *e == 100 + ARRIVED_KEPT as u64 + 9)
        );
    }

    /// A slot its master gained here, not from its stream, is not held
    /// until the stream confirms it: a `TAKEN` at that epoch or later, or an
    /// `ARRIVED` at that very epoch. A master without markers: all held.
    /// Another master: nothing noted of the first.
    #[test]
    fn a_gain_of_the_master_is_held_once_its_stream_confirms_it() {
        let f = FollowedImports::default();
        let (m, other) = (
            crate::cluster::generate_node_id(),
            crate::cluster::generate_node_id(),
        );
        f.note_gains(m, &[(10, 5), (11, 5), (20, 6)]);
        assert_eq!(f.not_held_of(m), vec![10, 11, 20]);
        assert!(f.not_held_of(other).is_empty());
        f.confirm_held(m, 4, &[(10, 11)], false);
        assert_eq!(f.not_held_of(m), vec![10, 11, 20], "an older TAKEN");
        f.confirm_held(m, 5, &[(10, 10)], false);
        assert_eq!(f.not_held_of(m), vec![11, 20]);
        f.confirm_held(m, 7, &[(20, 20)], true);
        assert_eq!(
            f.not_held_of(m),
            vec![11, 20],
            "an ARRIVED at another epoch"
        );
        f.confirm_held(m, 6, &[(20, 20)], true);
        assert_eq!(f.not_held_of(m), vec![11]);
        // A gain an ARRIVED at its epoch covers is held at once.
        f.note_arrived(node_id_str(&m).to_string(), 8, vec![(30, 31)]);
        f.note_gains(m, &[(30, 8), (31, 9)]);
        assert_eq!(f.not_held_of(m), vec![11, 31]);
        // A session with a master without markers: everything held.
        f.session_with(Some(m), false, false);
        assert!(f.not_held_of(m).is_empty());
        f.note_gains(m, &[(40, 9)]);
        assert!(f.not_held_of(m).is_empty());
        // Another master: what was noted of the first is forgotten.
        // A full sync: the claim sent ahead of the dataset confirms the
        // slots it lists at any epoch; the next one by the epoch rule.
        f.session_with(Some(m), true, true);
        f.note_gains(m, &[(41, 12), (42, 12), (43, 12)]);
        f.confirm_held(m, 3, &[(41, 42)], false);
        assert_eq!(f.not_held_of(m), vec![43]);
        f.confirm_held(m, 3, &[(43, 43)], false);
        assert_eq!(f.not_held_of(m), vec![43]);
        f.session_with(Some(m), true, true);
        f.sync_claim_read();
        f.confirm_held(m, 3, &[(43, 43)], false);
        assert_eq!(
            f.not_held_of(m),
            vec![43],
            "the claim came after the imports marker"
        );
        f.session_with(Some(m), true, false);
        f.note_gains(m, &[(50, 9)]);
        f.note_gains(other, &[(60, 3)]);
        assert!(f.not_held_of(m).is_empty());
        assert_eq!(f.not_held_of(other), vec![60]);
        f.forget_not_held();
        assert!(f.not_held_of(other).is_empty());
    }

    /// The last claim read in the stream holds a gain of a slot it lists at
    /// its epoch or before, heard after the claim: not one at a newer epoch,
    /// nor of a slot it does not list. Kept across a partial resync,
    /// forgotten at a full sync until the claim sent ahead of the dataset,
    /// and for another master.
    #[test]
    fn a_gain_heard_after_the_streams_claim_is_held_by_it() {
        let f = FollowedImports::default();
        let (m, other) = (
            crate::cluster::generate_node_id(),
            crate::cluster::generate_node_id(),
        );
        f.session_with(Some(m), true, false);
        // A takeover at 4, then the master's epoch moved to 5.
        f.note_arrived(node_id_str(&m).to_string(), 4, vec![(0, 99)]);
        f.confirm_held(m, 4, &[(0, 99), (500, 599)], false);
        f.confirm_held(m, 5, &[(0, 99), (500, 599)], false);
        assert_eq!(f.stream_claim_of(m), Some((5, vec![(0, 99), (500, 599)])));
        assert_eq!(f.stream_claim_of(other), None);
        f.note_gains(m, &[(10, 5), (11, 4), (12, 6), (200, 5), (501, 3)]);
        assert_eq!(
            f.not_held_of(m),
            vec![12, 200],
            "a newer epoch, or unlisted"
        );
        // A partial resync: kept.
        f.session_with(Some(m), true, false);
        f.note_gains(m, &[(20, 5)]);
        assert_eq!(f.not_held_of(m), vec![12, 200]);
        // A full sync: forgotten, until the claim sent ahead of the dataset.
        f.session_with(Some(m), true, true);
        assert_eq!(f.stream_claim_of(m), None);
        f.note_gains(m, &[(30, 5)]);
        assert_eq!(f.not_held_of(m), vec![12, 30, 200]);
        f.confirm_held(m, 7, &[(0, 99)], false);
        assert_eq!(f.not_held_of(m), vec![200]);
        f.note_gains(m, &[(40, 7), (41, 8)]);
        assert_eq!(f.not_held_of(m), vec![41, 200]);
        // A master without markers: none.
        f.session_with(Some(m), false, false);
        assert_eq!(f.stream_claim_of(m), None);
        // Another master: forgotten.
        f.session_with(Some(m), true, false);
        f.confirm_held(m, 9, &[(0, 99)], false);
        f.note_gains(other, &[(50, 1)]);
        assert_eq!(f.stream_claim_of(m), None);
        assert_eq!(f.not_held_of(other), vec![50]);
    }

    /// The newest claim read is the epoch a replica claims its master's
    /// slots at, written down and read back with the slots not held; an
    /// `ARRIVED` at a newer epoch is the epoch of the slots it covers.
    /// Before any claim: 0, or the master's epoch in nodes.conf for a node
    /// that started without the file; the master's epoch here for a master
    /// without markers, or one this node has no session with.
    #[test]
    fn a_replica_claims_its_masters_slots_at_the_newest_claim_it_read() {
        let f = FollowedImports::default();
        let (m, other) = (
            crate::cluster::generate_node_id(),
            crate::cluster::generate_node_id(),
        );
        assert_eq!(f.claim_epochs(m, 9).epoch(1), 9, "no session");
        f.session_with(Some(m), true, false);
        assert_eq!(f.claim_epochs(m, 9).epoch(1), 0, "no claim read");
        f.confirm_held(m, 5, &[(0, 99)], false);
        f.confirm_held(m, 4, &[(0, 99)], false);
        assert_eq!(f.claim_epochs(m, 9).epoch(1), 5);
        f.note_arrived(node_id_str(&m).to_string(), 7, vec![(200, 299)]);
        f.note_arrived(node_id_str(&m).to_string(), 3, vec![(300, 399)]);
        let epochs = f.claim_epochs(m, 9);
        assert_eq!(
            (epochs.epoch(1), epochs.epoch(250), epochs.epoch(350)),
            (5, 7, 5)
        );
        assert_eq!(f.claim_epochs(other, 9).epoch(1), 9);
        let text = f.not_held_text().unwrap();
        assert_eq!(
            text,
            format!("master {}\nconfirmed 5\nend\n", node_id_str(&m))
        );
        assert_eq!(parse_confirmed(&text), Some(5));
        let (of, slots) = parse_not_held(&text).unwrap();
        assert_eq!((of, slots.len()), (m, 0));
        // A full sync keeps it; a master without markers: its config epoch.
        f.session_with(Some(m), true, true);
        assert_eq!(f.claim_epochs(m, 9).epoch(1), 5);
        f.session_with(Some(m), false, false);
        assert_eq!(f.claim_epochs(m, 9).epoch(1), 9);
        // Started without the file: the master's epoch in nodes.conf.
        let f = FollowedImports::default();
        {
            let mut not_held = f.not_held.lock();
            not_held.about(m);
            not_held.baseline = true;
        }
        assert_eq!(f.claim_epochs(m, 6).epoch(1), 6);
        f.confirm_held(m, 4, &[(0, 99)], false);
        assert_eq!(f.claim_epochs(m, 6).epoch(1), 4);
    }

    /// The slots not held read back as written; a file cut short, or with
    /// anything after its end, does not read.
    #[test]
    fn the_slots_not_held_read_back() {
        let f = FollowedImports::default();
        let m = crate::cluster::generate_node_id();
        f.note_gains(m, &[(10, 5), (11, 5), (12, 6), (20, 6)]);
        let text = f.not_held_text().unwrap();
        assert_eq!(
            text,
            format!(
                "master {}\n10-11 5\n12-12 6\n20-20 6\nend\n",
                node_id_str(&m)
            )
        );
        let (of, slots) = parse_not_held(&text).unwrap();
        assert_eq!(of, m);
        assert_eq!(
            slots.into_iter().collect::<Vec<_>>(),
            vec![(10, 5), (11, 5), (12, 6), (20, 6)]
        );
        assert!(parse_not_held(&text[..text.len() - 4]).is_err());
        assert!(parse_not_held(&format!("{text}1-2 3\n")).is_err());
        assert!(parse_not_held("master x\nend\n").is_err());
        f.forget_not_held();
        assert_eq!(f.not_held_text(), None);
    }

    #[test]
    fn a_marker_that_does_not_read_is_none() {
        for bad in [
            vec![&b"KIVIDB.IMPORTS"[..]],
            vec![&b"KIVIDB.IMPORTS"[..], b"NOW"],
            vec![&b"KIVIDB.IMPORTS"[..], b"LIVE", b"id", b"1-2"],
            vec![&b"KIVIDB.IMPORTS"[..], b"LIVE", b"id", b"2-1", b"*"],
            vec![&b"KIVIDB.IMPORTS"[..], b"LIVE", b"id", b"1-16384", b"*"],
            vec![&b"KIVIDB.IMPORTS"[..], b"LIVE", b"id", b"-", b"*"],
            vec![&b"KIVIDB.IMPORTS"[..], b"LIVE", b"id", b"1-2", b"x"],
            vec![&b"SET"[..], b"LIVE"],
        ] {
            assert_eq!(parse_marker(&bad), None, "{bad:?}");
        }
    }

    fn store_with(keys: &[&str]) -> crate::store::Store {
        let store = crate::store::new_store(1);
        for key in keys {
            set(&store, key);
        }
        store
    }

    fn set(store: &crate::store::Store, key: &str) {
        let _ = crate::store::get_shard(&store[0], key)
            .write()
            .insert_str(key, "v");
    }

    /// `n` keys of `slot`.
    fn keys_of(slot: u16, prefix: &str, n: usize) -> Vec<String> {
        let tag = (0..)
            .map(|i| format!("t{i}"))
            .find(|t| key_hash_slot(t.as_bytes()) == slot)
            .unwrap();
        (0..n).map(|i| format!("{{{tag}}}:{prefix}:{i}")).collect()
    }

    /// A marker as `on_event` takes it: what to note, noted, reconciled.
    fn step(
        before: &[Followed],
        live: bool,
        listed: Vec<Listed>,
        store: &crate::store::Store,
    ) -> Vec<Followed> {
        let noted: Vec<(String, Arc<HeldBefore>)> = to_note(before, live, &listed)
            .into_iter()
            .map(|(id, ranges)| (id, Arc::new(HeldBefore::note(store, &ranges))))
            .collect();
        reconcile(before, listed, &noted)
    }

    fn listed(kept: Option<Ranges>) -> Vec<Listed> {
        vec![("t".repeat(40), vec![(96, 96), (5461, 5461)], kept)]
    }

    #[test]
    fn the_keys_held_are_noted_when_the_master_is_seen_to_look() {
        let held = keys_of(5461, "held", 3);
        let refs: Vec<&str> = held.iter().map(String::as_str).collect();
        let store = store_with(&refs);
        let begun = step(&[], true, listed(None), &store);
        assert_eq!(begun.len(), 1);
        assert!(begun[0].held.is_none());
        assert_eq!(begun[0].plan(96), Plan::Keep);
        // Seen to look, in the stream's order: noted now.
        let noted = step(&begun, true, listed(Some(vec![(5461, 5461)])), &store);
        let f = &noted[0];
        let h = f.held.as_ref().expect("noted");
        assert_eq!(h.keys.len(), 3);
        assert_eq!(f.plan(96), Plan::All);
        assert_eq!(f.plan(5461), Plan::Unheld);
        // And not again, at a later marker, with what arrived meanwhile.
        let brought = keys_of(5461, "brought", 2);
        for k in &brought {
            set(&store, k);
        }
        let later = step(&noted, true, listed(Some(vec![(5461, 5461)])), &store);
        assert_eq!(later[0].held.as_ref().unwrap().keys.len(), 3);
        // Once on disk, the slots that had keys keep them all.
        assert_eq!(f.kept_on_disk(), Some(vec![(5461, 5461)]));
        // Ended: no longer listed, forgotten.
        assert!(step(&later, true, vec![], &store).is_empty());
    }

    #[test]
    fn an_import_not_seen_begin_keeps_every_key_of_the_slots_that_had_any() {
        let store = store_with(&[]);
        // Attached part way, or sent the dataset in full.
        for live in [false, true] {
            let late = step(&[], live, listed(Some(vec![(5461, 5461)])), &store);
            assert!(late[0].held.is_none());
            assert_eq!(late[0].plan(96), Plan::All);
            assert_eq!(late[0].plan(5461), Plan::Keep);
        }
        // Known before, by a SYNC after a partial resync: what was noted
        // stays.
        let begun = step(&[], true, listed(None), &store);
        let noted = step(&begun, true, listed(Some(vec![])), &store);
        assert!(noted[0].held.is_some());
        let again = step(&noted, false, listed(Some(vec![])), &store);
        assert!(again[0].held.is_some());
        // A master that says it has not looked, of an import known to have
        // been: kept as looked at.
        let back = step(&noted, false, listed(None), &store);
        assert_eq!(back[0].kept, Some(vec![]));
    }

    #[test]
    fn too_many_keys_to_note_keep_their_slots_whole() {
        let held = Arc::new(HeldBefore {
            keys: Default::default(),
            slots: vec![5461],
            whole: true,
        });
        let f = Followed {
            id: "x".into(),
            ranges: vec![(96, 96), (5461, 5461)],
            kept: Some(vec![]),
            held: Some(held),
            came: None,
            seen_since_start: true,
        };
        assert_eq!(f.plan(5461), Plan::Keep);
        assert_eq!(f.plan(96), Plan::All);
    }

    /// A slot left out of an import (it came to this node) stays out,
    /// whatever the master's markers list; an import left with none is
    /// forgotten. And an import read back from disk stays one whose past
    /// here is not known.
    #[test]
    fn a_slot_left_out_of_an_import_stays_out() {
        let store = store_with(&[]);
        let begun = step(&[], true, listed(None), &store);
        assert!(begun[0].seen_since_start);
        let mut fewer = begun.clone();
        fewer[0].ranges = vec![(5461, 5461)];
        let again = step(&fewer, true, listed(Some(vec![])), &store);
        assert_eq!(again[0].ranges, vec![(5461, 5461)]);
        fewer[0].ranges = vec![];
        assert!(step(&fewer, false, listed(Some(vec![])), &store).is_empty());
        let mut read_back = parse_line(&format!("{} 96-96 -", "t".repeat(40))).unwrap();
        assert!(!read_back.seen_since_start);
        read_back.ranges = vec![(96, 96), (5461, 5461)];
        let still = step(&[read_back], false, listed(Some(vec![])), &store);
        assert!(!still[0].seen_since_start);
    }

    #[test]
    fn a_line_of_the_file_reads_back() {
        for f in [
            Followed {
                id: "a".repeat(40),
                ranges: vec![(1, 2), (7, 7)],
                kept: None,
                held: None,
                came: None,
                seen_since_start: true,
            },
            Followed {
                id: "b".repeat(40),
                ranges: vec![(96, 96)],
                kept: Some(vec![]),
                held: None,
                came: None,
                seen_since_start: true,
            },
            Followed {
                id: "c".repeat(40),
                ranges: vec![(96, 96), (5461, 5461)],
                kept: Some(vec![(5461, 5461)]),
                held: None,
                came: None,
                seen_since_start: true,
            },
        ] {
            let line = format!(
                "{} {} {}",
                f.id,
                ranges_word(&f.ranges),
                kept_word(&f.kept_on_disk())
            );
            let back = parse_line(&line).unwrap();
            assert_eq!((back.id, back.ranges, back.kept), (f.id, f.ranges, f.kept));
        }
        assert!(parse_line("id").is_none());
        assert!(parse_line("id 1-2").is_none());
        assert!(parse_line("id 1-2 * extra").is_none());
    }
}
