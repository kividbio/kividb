//! A shard's keys with a time to live, kept in two parts so that active
//! expiry looks only at those that may come due soon (#618).
//!
//! Every entry with a time to live is in one of two lists, and knows where
//! (`Entry::pos`, the top bit saying which list). The **far** list holds
//! only entries whose time ends at the list's horizon or later: none of
//! them can be due before then, so until the horizon a pass for due keys
//! goes over the **near** list alone. The near list may hold anything.
//!
//! Before, there was one list, and every active-expiry cycle went over all
//! of it in every shard, ten times a second: with 5 million keys set to live
//! an hour and none due, the expiry threads took more than half a core on an
//! idle server, and the more such keys the more it took.
//!
//! Now the far list is gone through once per horizon instead (a sweep,
//! [`TtlList::sweep`]): about every [`HORIZON_MS`], it moves the entries
//! that end before the next horizon to the near list, and those of the near
//! list that end after it to the far list. A key given a time that ends
//! before the horizon a sweep is moving to goes straight to the near list,
//! so a short time to live is seen by the next pass whenever it is set.
//!
//! The order within each list means nothing: a key that leaves, or loses
//! its time to live, has the last one of its list moved into its place.
//!
//! The lists also keep the sum of when their entries' times end
//! ([`TtlList::deadline_sum`]), kept as entries come, go and are given new
//! times: what `INFO keyspace` makes `avg_ttl` of, in one step a shard.
//! It went over every entry, holding each shard while it did: 45-70 ms at
//! 5 million keys with a time to live, and writes to a shard waited
//! that long (#622).

use super::entry::{Entry, prefetch_expiry};

/// The bit of an entry's place that says it is in the far list.
const FAR: usize = 1 << 31;

/// How far ahead of the time of a sweep the horizon it moves to is. The
/// longer, the less often each far entry is looked at (once per horizon);
/// the shorter, the fewer entries the near list holds that are not due yet
/// (those ending within a horizon, gone over every cycle). With times to
/// live spread evenly over an hour, 20 s keeps both under 1% of the keys a
/// second.
pub(super) const HORIZON_MS: u64 = 20_000;

/// How long before its horizon a shard is swept: so that a sweep is done
/// before the horizon passes, which would make a pass go over the far list
/// as well until the next one.
const LEAD_MS: u64 = 2_000;

/// How far ahead a pass over a list asks for entries' lines.
const AHEAD: usize = 8;

/// When the time to live of `entry`, which has one, ends.
#[inline(always)]
fn deadline(entry: Entry) -> u64 {
    entry.expires().unwrap_or(0)
}

/// See the module.
pub(super) struct TtlList {
    near: Vec<Entry>,
    far: Vec<Entry>,
    /// No entry of `far` ends before this (milliseconds since the Unix
    /// epoch). 0 until the first sweep: everything is far, and a pass goes
    /// over all of it.
    horizon: u64,
    /// The horizon a sweep under way moves to; `horizon` while there is
    /// none. What an entry given a time to live is placed by: one that ends
    /// before it goes to the near list.
    next: u64,
    /// Whether a sweep is under way.
    sweeping: bool,
    /// While a sweep is under way, `far[..checked]` have been looked at, and
    /// all end at `next` or later. 0 otherwise.
    checked: usize,
    /// While a sweep is under way, how much of `near` it has looked at.
    near_checked: usize,
    /// The sum of when the times of all the entries, near and far, end.
    /// Every entry comes in by `push` (or `replaced`), goes out by `take`
    /// (or `replaced`, or `clear`), and has its time changed by `retimed`
    /// (or `replaced`): those keep it. A sweep moves entries between the
    /// lists and leaves it as it is. 128 bits: 2^32 entries of times up
    /// to 2^64 do not overflow it.
    deadlines: u128,
}

impl TtlList {
    pub(super) const fn new() -> Self {
        TtlList {
            near: Vec::new(),
            far: Vec::new(),
            horizon: 0,
            next: 0,
            sweeping: false,
            checked: 0,
            near_checked: 0,
            deadlines: 0,
        }
    }

    /// The sum of when the times of all the entries end (milliseconds
    /// since the Unix epoch): over [`TtlList::len`], their mean.
    #[inline]
    pub(super) fn deadline_sum(&self) -> u128 {
        self.deadlines
    }

    /// How many entries there are.
    #[inline]
    pub(super) fn len(&self) -> usize {
        self.near.len() + self.far.len()
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.near.is_empty() && self.far.is_empty()
    }

    /// The room the two lists have.
    #[inline]
    pub(super) fn capacity(&self) -> usize {
        self.near.capacity() + self.far.capacity()
    }

    /// The entry at place `index` of the two lists, near then far: what a
    /// pick at random from them is (`index` modulo how many there are).
    pub(super) fn pick(&self, index: usize) -> Option<Entry> {
        let len = self.len();
        if len == 0 {
            return None;
        }
        let i = index % len;
        Some(match self.near.get(i) {
            Some(&e) => e,
            None => self.far[i - self.near.len()],
        })
    }

    /// The entry at `pos` (`Entry::pos`), to be replaced by one moved
    /// elsewhere in memory.
    #[inline]
    pub(super) fn at_mut(&mut self, pos: usize) -> &mut Entry {
        if pos & FAR == 0 {
            &mut self.near[pos]
        } else {
            &mut self.far[pos & !FAR]
        }
    }

    /// The entry at `pos` (`Entry::pos`).
    #[cfg(test)]
    pub(super) fn at(&self, pos: usize) -> Entry {
        if pos & FAR == 0 {
            self.near[pos]
        } else {
            self.far[pos & !FAR]
        }
    }

    /// Every entry.
    pub(super) fn iter(&self) -> impl Iterator<Item = &Entry> {
        self.near.iter().chain(self.far.iter())
    }

    /// Take every entry out.
    pub(super) fn clear(&mut self) {
        self.near.clear();
        self.far.clear();
        self.checked = 0;
        self.near_checked = 0;
        self.deadlines = 0;
    }

    /// Give back the room of each list that is `sparse`, down to its
    /// entries and an eighth more.
    pub(super) fn shrink_if(&mut self, sparse: impl Fn(usize, usize) -> bool) {
        for list in [&mut self.near, &mut self.far] {
            if sparse(list.len(), list.capacity()) {
                list.shrink_to(list.len() + list.len() / 8);
            }
        }
    }

    /// Whether either list is `sparse`.
    pub(super) fn is_sparse(&self, sparse: impl Fn(usize, usize) -> bool) -> bool {
        sparse(self.near.len(), self.near.capacity()) || sparse(self.far.len(), self.far.capacity())
    }

    #[inline]
    fn push_near(&mut self, entry: Entry) {
        assert!(self.near.len() < FAR, "keys with a time to live");
        entry.set_pos(self.near.len());
        self.near.push(entry);
    }

    #[inline]
    fn push_far(&mut self, entry: Entry) {
        assert!(self.far.len() < FAR, "keys with a time to live");
        entry.set_pos(FAR | self.far.len());
        self.far.push(entry);
    }

    /// `entry`, which has a time to live, put in the list its time says.
    #[inline]
    pub(super) fn push(&mut self, entry: Entry) {
        let at = deadline(entry);
        self.deadlines += u128::from(at);
        if at < self.next {
            self.push_near(entry);
        } else {
            self.push_far(entry);
        }
    }

    /// Take the entry at `i` out of the near list, the last one moved into
    /// its place.
    #[inline]
    fn take_near(&mut self, i: usize) {
        self.near.swap_remove(i);
        if let Some(&moved) = self.near.get(i) {
            moved.set_pos(i);
        }
    }

    /// Take the entry at `i` out of the far list, the last one moved into
    /// its place. While a sweep is under way, one moved into the part it has
    /// looked at that ends before its horizon goes to the near list, as the
    /// sweep would have put it.
    #[inline]
    fn take_far(&mut self, i: usize) {
        loop {
            self.far.swap_remove(i);
            let Some(&moved) = self.far.get(i) else {
                return;
            };
            if i < self.checked && deadline(moved) < self.next {
                self.push_near(moved);
                continue;
            }
            moved.set_pos(FAR | i);
            return;
        }
    }

    /// `entry` taken out.
    #[inline]
    pub(super) fn take(&mut self, entry: Entry) {
        self.deadlines -= u128::from(deadline(entry));
        let pos = entry.pos();
        if pos & FAR == 0 {
            debug_assert!(self.near[pos] == entry);
            self.take_near(pos);
        } else {
            debug_assert!(self.far[pos & !FAR] == entry);
            self.take_far(pos & !FAR);
        }
    }

    /// `entry`, in the list, has a new time to live, in place of the one
    /// that ended at `was`: to the near list if it is in the far one and
    /// its time now ends before the horizon.
    #[inline]
    pub(super) fn retimed(&mut self, entry: Entry, was: u64) {
        self.deadlines = self.deadlines - u128::from(was) + u128::from(deadline(entry));
        self.relist(entry);
    }

    /// `entry`, in the list, to the near list if it is in the far one and
    /// its time ends before the horizon. Neither comes nor goes.
    #[inline]
    fn relist(&mut self, entry: Entry) {
        let pos = entry.pos();
        if pos & FAR != 0 && deadline(entry) < self.next {
            self.take_far(pos & !FAR);
            self.push_near(entry);
        }
    }

    /// The list made right for `new` taking `old`'s place in the table:
    /// either of them may have a time to live.
    #[inline(always)]
    pub(super) fn replaced(&mut self, old: Entry, new: Entry) {
        match (old.has_ttl(), new.has_ttl()) {
            (false, false) => {}
            (true, true) => {
                let pos = old.pos();
                new.set_pos(pos);
                *self.at_mut(pos) = new;
                self.retimed(new, deadline(old));
            }
            (true, false) => self.take(old),
            (false, true) => self.push(new),
        }
    }

    /// The entries whose time to live ended before `now` (milliseconds
    /// since the Unix epoch), at most `limit` of them: one pass over the near
    /// list, and the far one only if `now` is past the horizon, stopping at
    /// the last one it takes. Returns them, and how many entries it looked
    /// at.
    pub(super) fn due(&self, now: u64, limit: usize) -> (Vec<crate::store::Key>, usize) {
        let mut due = Vec::new();
        let mut looked = 0;
        // No far entry ends before the horizon.
        let lists: &[&Vec<Entry>] = if now > self.horizon {
            &[&self.near, &self.far]
        } else {
            &[&self.near]
        };
        for list in lists {
            for (i, e) in list.iter().enumerate() {
                // Each entry is somewhere else in memory: a few are asked
                // for ahead, so that their lines come in together.
                if let Some(ahead) = list.get(i + AHEAD) {
                    prefetch_expiry(*ahead);
                }
                if deadline(*e) < now {
                    due.push(crate::store::Key::new(e.key()));
                    if due.len() == limit {
                        return (due, looked + i + 1);
                    }
                }
            }
            looked += list.len();
        }
        (due, looked)
    }

    /// Whether the shard should be swept at `now`: one is under way, the
    /// horizon is close, or it is further ahead than a sweep now would put
    /// it. That is a clock stepped back after a sweep: until it caught up,
    /// every key given a time to live would go to the near list, and every
    /// pass would look at it.
    #[inline]
    pub(super) fn sweep_due(&self, now: u64) -> bool {
        self.sweeping
            || now.saturating_add(LEAD_MS) >= self.horizon
            || self.horizon > now.saturating_add(HORIZON_MS + LEAD_MS)
    }

    /// Sweep, looking at `steps` entries at most: start one, moving the
    /// horizon to [`HORIZON_MS`] after `now`, if none is under way, and
    /// move the entries that end before the new horizon to the near list,
    /// and the others to the far list. Returns whether the sweep is done,
    /// and the horizon moved.
    pub(super) fn sweep(&mut self, now: u64, steps: usize) -> bool {
        if !self.sweeping {
            self.sweeping = true;
            let next = now.saturating_add(HORIZON_MS);
            // A horizon further ahead (the clock stepped back) is brought
            // back to this one: the far list's entries end at the old one or
            // later, so at this one or later too.
            self.horizon = self.horizon.min(next);
            self.next = next;
            self.checked = 0;
            self.near_checked = 0;
        }
        let next = self.next;
        let mut left = steps;
        // Those of the near list that end at the new horizon or later: the
        // far list takes them, at its end, where the sweep looks at them
        // again.
        while self.near_checked < self.near.len() {
            if left == 0 {
                return false;
            }
            left -= 1;
            if let Some(&ahead) = self.near.get(self.near_checked + AHEAD) {
                prefetch_expiry(ahead);
            }
            let e = self.near[self.near_checked];
            if deadline(e) >= next {
                self.take_near(self.near_checked);
                self.push_far(e);
            } else {
                self.near_checked += 1;
            }
        }
        while self.checked < self.far.len() {
            if left == 0 {
                return false;
            }
            left -= 1;
            if let Some(&ahead) = self.far.get(self.checked + AHEAD) {
                prefetch_expiry(ahead);
            }
            let e = self.far[self.checked];
            if deadline(e) < next {
                // The last entry comes in its place, to be looked at next.
                self.take_far(self.checked);
                self.push_near(e);
            } else {
                self.checked += 1;
            }
        }
        self.horizon = next;
        self.sweeping = false;
        self.checked = 0;
        self.near_checked = 0;
        true
    }

    /// Check what the lists say of themselves: every entry knows its place,
    /// and none in the far list ends before the horizon, nor before the
    /// next one in the part a sweep has looked at.
    #[cfg(test)]
    pub(super) fn check(&self) {
        let sum: u128 = self.iter().map(|e| u128::from(deadline(*e))).sum();
        assert_eq!(self.deadlines, sum, "sum of the times");
        for (i, e) in self.near.iter().enumerate() {
            assert_eq!(e.pos(), i);
        }
        for (i, e) in self.far.iter().enumerate() {
            assert_eq!(e.pos(), FAR | i);
            assert!(deadline(*e) >= self.horizon, "far entry before the horizon");
            if i < self.checked {
                assert!(
                    deadline(*e) >= self.next,
                    "swept entry before the next horizon"
                );
            }
        }
    }

    #[cfg(test)]
    pub(super) fn near_len(&self) -> usize {
        self.near.len()
    }
}
