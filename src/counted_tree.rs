//! A large sorted set's (score, member) order, with subtree counts for rank.

use std::mem;

use ordered_float::OrderedFloat;

use crate::member_slab::{MemberHandle, MemberSlab, SlabFull};

/// Most entries a node holds; a split leaves halves of at least `MIN`.
const MAX: usize = 31;
const MIN: usize = MAX / 2;

/// Past this many entries, `nth` finds its place from the root.
const STEP_LIMIT: usize = 32;

type Entry = (f64, MemberHandle);

/// A B-tree of (score, member) entries whose member bytes live once in its slab.
#[derive(Clone, Default)]
pub struct CountedTree {
    /// Private, so every handle in the tree was issued by this slab.
    slab: MemberSlab,
    root: Node,
}

#[derive(Clone, Default)]
struct Node {
    entries: Vec<Entry>,
    /// Empty for a leaf, else one more than `entries`.
    children: Vec<Node>,
    /// Entries in this subtree.
    len: usize,
}

enum Insert {
    Present,
    Added,
    Split(Entry, Node),
}

fn bytes_of(slab: &MemberSlab, handle: MemberHandle) -> &[u8] {
    // A member leaves the slab only after its entry leaves the tree.
    slab.get(handle)
        .expect("a tree entry's member is in its slab")
}

impl CountedTree {
    pub fn len(&self) -> usize {
        self.root.len
    }

    /// The bytes of a member this tree holds.
    #[inline]
    pub fn member(&self, handle: MemberHandle) -> &[u8] {
        bytes_of(&self.slab, handle)
    }

    /// Members surely storable before the slab is full.
    pub fn spare(&self) -> usize {
        self.slab.spare()
    }

    /// Stores a member not yet in the tree and adds its entry.
    pub fn insert_new(&mut self, score: f64, member: &[u8]) -> Result<MemberHandle, SlabFull> {
        let handle = self.slab.insert(member)?;
        let linked = self.link(score, handle);
        debug_assert!(linked, "the member was not in the tree");
        Ok(handle)
    }

    /// Moves a member's entry from `old` to `new`; its bytes stay where they are.
    pub fn rescore(&mut self, handle: MemberHandle, old: f64, new: f64) {
        let unlinked = self.unlink(old, handle);
        debug_assert_eq!(unlinked, Some(handle));
        let linked = self.link(new, handle);
        debug_assert!(linked);
    }

    /// Takes the member's entry out and frees its bytes.
    pub fn remove(&mut self, handle: MemberHandle, score: f64) {
        let unlinked = self.unlink(score, handle);
        debug_assert_eq!(unlinked, Some(handle));
        self.slab.remove(handle);
    }

    /// Takes out the entry at `rank`, handing it to `take` before its bytes are freed.
    pub fn remove_at<R>(
        &mut self,
        rank: usize,
        take: impl FnOnce(MemberHandle, &[u8], f64) -> R,
    ) -> Option<R> {
        // The last entry is reached without summing child counts on the way.
        let (score, handle) = if Some(rank) == self.len().checked_sub(1) {
            self.root.pop_last()?
        } else {
            self.root.remove_at(rank)?
        };
        self.shrink_root();
        let out = take(handle, bytes_of(&self.slab, handle), score);
        self.slab.remove(handle);
        Some(out)
    }

    /// Adds an entry for `handle`; false if it is already present.
    fn link(&mut self, score: f64, handle: MemberHandle) -> bool {
        let member = bytes_of(&self.slab, handle);
        match self.root.insert(&self.slab, score, member, handle) {
            Insert::Present => return false,
            Insert::Added => {}
            Insert::Split(middle, right) => {
                let left = mem::take(&mut self.root);
                self.root = Node {
                    len: left.len + 1 + right.len,
                    entries: vec![middle],
                    children: vec![left, right],
                };
            }
        }
        true
    }

    /// Takes the entry out of the tree, leaving its member in the slab.
    fn unlink(&mut self, score: f64, handle: MemberHandle) -> Option<MemberHandle> {
        let member = bytes_of(&self.slab, handle);
        let unlinked = self.root.remove(&self.slab, score, member)?;
        self.shrink_root();
        Some(unlinked)
    }

    fn shrink_root(&mut self) {
        if self.root.entries.is_empty()
            && let Some(only) = self.root.children.pop()
        {
            self.root = only;
        }
    }

    /// The entry's 0-based rank by ascending score, then member.
    pub fn rank(&self, score: f64, member: &[u8]) -> Option<usize> {
        let mut node = &self.root;
        let mut rank = 0;
        loop {
            match node.search(&self.slab, score, member) {
                Ok(i) => return Some(rank + i + node.count_before(i + 1)),
                Err(i) => {
                    rank += i + node.count_before(i);
                    node = node.children.get(i)?;
                }
            }
        }
    }

    /// Entries before (score, member), or up to it too if `inclusive`.
    pub fn rank_of(&self, score: f64, member: &[u8], inclusive: bool) -> usize {
        let mut node = &self.root;
        let mut rank = 0;
        loop {
            match node.search(&self.slab, score, member) {
                Ok(i) => return rank + i + node.count_before(i + 1) + usize::from(inclusive),
                Err(i) => {
                    rank += i + node.count_before(i);
                    match node.children.get(i) {
                        Some(child) => node = child,
                        None => return rank,
                    }
                }
            }
        }
    }

    /// The entries of ranks `start..end`, in order, from either end.
    pub fn range(&self, start: usize, end: usize) -> Iter<'_> {
        let end = end.min(self.len());
        let start = start.min(end);
        Iter {
            tree: self,
            front: start,
            left: end - start,
            front_path: Vec::new(),
            back_path: Vec::new(),
        }
    }

    /// Bytes held by the slab and the nodes, at their capacity.
    pub fn heap_bytes(&self) -> usize {
        self.slab.usage().heap_bytes() + self.root.heap_bytes()
    }

    /// Lowers the member limit, so a full set can be tested.
    #[cfg(test)]
    pub(crate) fn set_member_limit(&mut self, members: u32) {
        self.slab.set_member_limit(members);
    }

    /// Members the slab holds.
    #[cfg(test)]
    pub(crate) fn stored(&self) -> usize {
        self.slab.len()
    }
}

/// See [`CountedTree::range`]. Each end finds its place from the root when first used.
pub struct Iter<'a> {
    tree: &'a CountedTree,
    /// Rank of the next entry from the front.
    front: usize,
    left: usize,
    /// Nodes down to the next entry from the front, and that entry's index.
    front_path: Vec<(&'a Node, usize)>,
    /// Nodes down to the next entry from the back, and one past its index.
    back_path: Vec<(&'a Node, usize)>,
}

impl<'a> Iter<'a> {
    fn item(&self, entry: Entry) -> (&'a [u8], f64) {
        let (score, handle) = entry;
        let tree = self.tree;
        (bytes_of(&tree.slab, handle), score)
    }

    /// Descends to the entry of rank `rank`; `back` ends one past it.
    fn seek(&self, mut rank: usize, back: bool) -> Vec<(&'a Node, usize)> {
        let mut path = Vec::new();
        let tree = self.tree;
        let mut node = &tree.root;
        'down: loop {
            if node.children.is_empty() {
                path.push((node, rank + usize::from(back)));
                return path;
            }
            for (i, child) in node.children.iter().enumerate() {
                if rank < child.len {
                    // Child `i` lies between entries `i - 1` and `i`.
                    path.push((node, i));
                    node = child;
                    continue 'down;
                }
                if rank == child.len {
                    path.push((node, i + usize::from(back)));
                    return path;
                }
                rank -= child.len + 1;
            }
            return path;
        }
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a [u8], f64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        if self.front_path.is_empty() {
            self.front_path = self.seek(self.front, false);
        }
        while let Some(&mut (node, ref mut at)) = self.front_path.last_mut() {
            let i = *at;
            let Some(&entry) = node.entries.get(i) else {
                self.front_path.pop();
                continue;
            };
            *at += 1;
            // Then the leftmost entries of the child after this one.
            let mut next = node.children.get(i + 1);
            while let Some(child) = next {
                self.front_path.push((child, 0));
                next = child.children.first();
            }
            self.left -= 1;
            self.front += 1;
            return Some(self.item(entry));
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }

    fn count(self) -> usize {
        self.left
    }

    fn last(mut self) -> Option<Self::Item> {
        self.next_back()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if n >= self.left {
            self.left = 0;
            return None;
        }
        if n < STEP_LIMIT && !self.front_path.is_empty() {
            for _ in 0..n {
                self.next();
            }
        } else {
            self.front += n;
            self.left -= n;
            self.front_path.clear();
        }
        self.next()
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        if self.back_path.is_empty() {
            self.back_path = self.seek(self.front + self.left - 1, true);
        }
        while let Some(&mut (node, ref mut at)) = self.back_path.last_mut() {
            let Some(i) = at.checked_sub(1) else {
                self.back_path.pop();
                continue;
            };
            *at = i;
            let entry = node.entries[i];
            // Then the rightmost entries of the child before this one.
            let mut next = node.children.get(i);
            while let Some(child) = next {
                self.back_path.push((child, child.entries.len()));
                next = child.children.last();
            }
            self.left -= 1;
            return Some(self.item(entry));
        }
        None
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        if n >= self.left {
            self.left = 0;
            return None;
        }
        if n < STEP_LIMIT && !self.back_path.is_empty() {
            for _ in 0..n {
                self.next_back();
            }
        } else {
            self.left -= n;
            self.back_path.clear();
        }
        self.next_back()
    }
}

impl ExactSizeIterator for Iter<'_> {}

impl Node {
    fn search(&self, slab: &MemberSlab, score: f64, member: &[u8]) -> Result<usize, usize> {
        self.entries.binary_search_by(|&(entry_score, handle)| {
            OrderedFloat(entry_score)
                .cmp(&OrderedFloat(score))
                .then_with(|| bytes_of(slab, handle).cmp(member))
        })
    }

    /// Entries in the first `children` subtrees.
    fn count_before(&self, children: usize) -> usize {
        self.children.iter().take(children).map(|c| c.len).sum()
    }

    fn insert(
        &mut self,
        slab: &MemberSlab,
        score: f64,
        member: &[u8],
        handle: MemberHandle,
    ) -> Insert {
        let i = match self.search(slab, score, member) {
            Ok(_) => return Insert::Present,
            Err(i) => i,
        };
        if self.children.is_empty() {
            self.entries.insert(i, (score, handle));
        } else {
            match self.children[i].insert(slab, score, member, handle) {
                Insert::Present => return Insert::Present,
                Insert::Added => {}
                Insert::Split(middle, right) => {
                    self.entries.insert(i, middle);
                    self.children.insert(i + 1, right);
                }
            }
        }
        self.len += 1;
        if self.entries.len() > MAX {
            let (middle, right) = self.split();
            return Insert::Split(middle, right);
        }
        Insert::Added
    }

    /// Moves the upper half into a new node, returning it and the middle entry.
    fn split(&mut self) -> (Entry, Node) {
        let mut entries = Vec::with_capacity(MAX + 1);
        entries.extend(self.entries.drain(MIN + 2..));
        let middle = self.entries.remove(MIN + 1);
        let mut children = Vec::new();
        if !self.children.is_empty() {
            children.reserve_exact(MAX + 2);
            children.extend(self.children.drain(MIN + 2..));
        }
        let len = entries.len() + children.iter().map(|c: &Node| c.len).sum::<usize>();
        self.len -= len + 1;
        (
            middle,
            Node {
                entries,
                children,
                len,
            },
        )
    }

    /// Unlinks the entry and returns its member's handle.
    fn remove(&mut self, slab: &MemberSlab, score: f64, member: &[u8]) -> Option<MemberHandle> {
        let handle = match self.search(slab, score, member) {
            Ok(i) if self.children.is_empty() => self.entries.remove(i).1,
            Ok(i) => {
                // The predecessor takes the removed entry's place.
                let before = self.children[i].pop_last()?;
                let handle = mem::replace(&mut self.entries[i], before).1;
                self.refill(i);
                handle
            }
            Err(i) => {
                let handle = self.children.get_mut(i)?.remove(slab, score, member)?;
                self.refill(i);
                handle
            }
        };
        self.len -= 1;
        Some(handle)
    }

    /// Unlinks the entry at `rank` in this subtree.
    fn remove_at(&mut self, mut rank: usize) -> Option<Entry> {
        if rank >= self.len {
            return None;
        }
        let entry = if self.children.is_empty() {
            self.entries.remove(rank)
        } else {
            // Below this subtree's len, so the walk stops at a child or its entry.
            let mut i = 0;
            while rank > self.children[i].len {
                rank -= self.children[i].len + 1;
                i += 1;
            }
            let entry = if rank < self.children[i].len {
                self.children[i].remove_at(rank)?
            } else {
                let before = self.children[i].pop_last()?;
                mem::replace(&mut self.entries[i], before)
            };
            self.refill(i);
            entry
        };
        self.len -= 1;
        Some(entry)
    }

    fn pop_last(&mut self) -> Option<Entry> {
        let entry = match self.children.len().checked_sub(1) {
            None => self.entries.pop()?,
            Some(last) => {
                let entry = self.children[last].pop_last()?;
                self.refill(last);
                entry
            }
        };
        self.len -= 1;
        Some(entry)
    }

    /// Brings child `i` back to `MIN` entries from a sibling, or merges it.
    fn refill(&mut self, i: usize) {
        if self.children[i].entries.len() >= MIN {
            return;
        }
        if i > 0 && self.children[i - 1].entries.len() > MIN {
            let (before, after) = self.children.split_at_mut(i);
            let (left, child) = (&mut before[i - 1], &mut after[0]);
            if let Some(moved) = left.entries.pop() {
                let parent = mem::replace(&mut self.entries[i - 1], moved);
                child.entries.insert(0, parent);
                let mut count = 1;
                if let Some(grandchild) = left.children.pop() {
                    count += grandchild.len;
                    child.children.insert(0, grandchild);
                }
                left.len -= count;
                child.len += count;
            }
        } else if i + 1 < self.children.len() && self.children[i + 1].entries.len() > MIN {
            let (before, after) = self.children.split_at_mut(i + 1);
            let (child, right) = (&mut before[i], &mut after[0]);
            let moved = right.entries.remove(0);
            let parent = mem::replace(&mut self.entries[i], moved);
            child.entries.push(parent);
            let mut count = 1;
            if !right.children.is_empty() {
                let grandchild = right.children.remove(0);
                count += grandchild.len;
                child.children.push(grandchild);
            }
            right.len -= count;
            child.len += count;
        } else {
            // Both siblings are at most MIN, so together they fit in MAX.
            let at = i.saturating_sub(1);
            let right = self.children.remove(at + 1);
            let parent = self.entries.remove(at);
            let left = &mut self.children[at];
            left.entries.push(parent);
            left.entries.extend(right.entries);
            left.children.extend(right.children);
            left.len += 1 + right.len;
        }
    }

    /// Bytes this subtree's vectors hold, at their capacity.
    fn heap_bytes(&self) -> usize {
        self.entries.capacity() * size_of::<Entry>()
            + self.children.capacity() * size_of::<Node>()
            + self.children.iter().map(Node::heap_bytes).sum::<usize>()
    }
}

#[cfg(test)]
impl CountedTree {
    /// Checks counts, fill, leaf depth, order, and that each member is live.
    pub(crate) fn check(&self) {
        fn walk(
            node: &Node,
            slab: &MemberSlab,
            depth: usize,
            leaf_depth: &mut Option<usize>,
            out: &mut Vec<(f64, Vec<u8>)>,
        ) -> usize {
            assert!(node.entries.len() <= MAX, "overfull node");
            if depth > 0 {
                assert!(node.entries.len() >= MIN, "underfull node");
            }
            let mut len = node.entries.len();
            if node.children.is_empty() {
                assert_eq!(*leaf_depth.get_or_insert(depth), depth, "leaf depths");
                for &(score, handle) in &node.entries {
                    out.push((score, slab.get(handle).expect("live member").to_vec()));
                }
            } else {
                assert_eq!(node.children.len(), node.entries.len() + 1);
                assert!(!node.entries.is_empty(), "internal node without entries");
                for (i, child) in node.children.iter().enumerate() {
                    len += walk(child, slab, depth + 1, leaf_depth, out);
                    if let Some(&(score, handle)) = node.entries.get(i) {
                        out.push((score, slab.get(handle).expect("live member").to_vec()));
                    }
                }
            }
            assert_eq!(node.len, len, "subtree count");
            len
        }
        let mut walked = Vec::new();
        let len = walk(&self.root, &self.slab, 0, &mut None, &mut walked);
        assert_eq!(len, self.len());
        assert_eq!(
            self.slab.len(),
            self.len(),
            "the slab holds only the tree's members"
        );
        for pair in walked.windows(2) {
            let (a, b) = (&pair[0], &pair[1]);
            assert!(
                (OrderedFloat(a.0), &a.1) < (OrderedFloat(b.0), &b.1),
                "order"
            );
        }
        assert!(
            walked
                .iter()
                .all(|(s, _)| *s != 0.0 || s.is_sign_positive())
        );
        // Bits, so a NaN score compares equal to itself.
        let bits = |v: &[(f64, Vec<u8>)]| -> Vec<(u64, Vec<u8>)> {
            v.iter().map(|(s, m)| (s.to_bits(), m.clone())).collect()
        };
        let ranged: Vec<(f64, Vec<u8>)> = self
            .range(0, self.len())
            .map(|(m, s)| (s, m.to_vec()))
            .collect();
        assert_eq!(bits(&ranged), bits(&walked), "full range");
        let mut back: Vec<(f64, Vec<u8>)> = self
            .range(0, self.len())
            .rev()
            .map(|(m, s)| (s, m.to_vec()))
            .collect();
        back.reverse();
        assert_eq!(bits(&back), bits(&walked), "full range, backwards");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::member_slab::SMALL_MAX;

    type Model = BTreeSet<(OrderedFloat<f64>, Vec<u8>)>;

    /// Adds an entry if absent, -0 kept as 0, as the set's callers do.
    fn add(tree: &mut CountedTree, score: f64, member: &[u8]) -> bool {
        let score = score + 0.0;
        if tree.rank(score, member).is_some() {
            return false;
        }
        tree.insert_new(score, member).unwrap();
        true
    }

    /// Takes the entry out if present.
    fn take(tree: &mut CountedTree, score: f64, member: &[u8]) -> bool {
        match tree.rank(score, member) {
            Some(rank) => tree.remove_at(rank, |_, _, _| ()).is_some(),
            None => false,
        }
    }

    fn entries(tree: &CountedTree, start: usize, stop: usize) -> Vec<(f64, Vec<u8>)> {
        tree.range(start, stop.saturating_add(1))
            .map(|(member, score)| (score, member.to_vec()))
            .collect()
    }

    fn model_range(model: &Model, start: usize, stop: usize) -> Vec<(f64, Vec<u8>)> {
        let take = stop.saturating_add(1).saturating_sub(start);
        let range = model.iter().skip(start).take(take);
        range
            .map(|(score, member)| (score.0, member.clone()))
            .collect()
    }

    #[test]
    fn scores_order_entries_first_then_member_bytes() {
        let mut tree = CountedTree::default();
        let added: [(f64, &[u8]); 12] = [
            (f64::INFINITY, b"inf"),
            (1.5, b"b"),
            (1.5, b"a"),
            (1.5, b""),
            (1.5, b"\xff"),
            (1.5, b"\0"),
            (1.5, "\u{e9}".as_bytes()),
            (-0.0, b"zero"),
            (-1.0, b"neg"),
            (f64::NEG_INFINITY, b"-inf"),
            (f64::NAN, b"nan"),
            (1e300, b"big"),
        ];
        for (score, member) in added {
            assert!(add(&mut tree, score, member));
        }
        let want: [(f64, &[u8]); 11] = [
            (f64::NEG_INFINITY, b"-inf"),
            (-1.0, b"neg"),
            (0.0, b"zero"),
            (1.5, b""),
            (1.5, b"\0"),
            (1.5, b"a"),
            (1.5, b"b"),
            (1.5, "\u{e9}".as_bytes()),
            (1.5, b"\xff"),
            (1e300, b"big"),
            (f64::INFINITY, b"inf"),
        ];
        let got = entries(&tree, 0, usize::MAX);
        assert_eq!(got.len(), want.len() + 1);
        for ((score, member), (want_score, want_member)) in got.iter().zip(&want) {
            assert_eq!(
                (score.to_bits(), &member[..]),
                (want_score.to_bits(), *want_member)
            );
        }
        // NaN sorts last, as OrderedFloat orders it.
        assert!(got[11].0.is_nan() && got[11].1 == b"nan");
        assert_eq!(tree.rank(f64::NAN, b"nan"), Some(11));
        tree.check();

        // -0 and 0 are one score.
        assert!(!add(&mut tree, 0.0, b"zero"));
        assert_eq!(tree.rank(-0.0, b"zero"), Some(2));
        assert!(!add(&mut tree, 1.5, b"a"), "already present");
        assert_eq!(
            (tree.len(), tree.slab.len()),
            (12, 12),
            "nothing stored twice"
        );

        assert_eq!(tree.rank(1.5, b"c"), None);
        assert_eq!(tree.rank(2.0, b"a"), None, "the member has another score");
        assert!(!take(&mut tree, 2.0, b"a"));
        assert!(take(&mut tree, 1.5, b"a"));
        assert!(!take(&mut tree, 1.5, b"a"));
        assert_eq!(tree.rank(1.5, b"b"), Some(5));
        assert!(take(&mut tree, -0.0, b"zero"));
        assert_eq!(tree.rank(1.5, b"b"), Some(4));
        tree.check();
    }

    #[test]
    fn empty_and_single_entry_trees() {
        let mut tree = CountedTree::default();
        assert_eq!(tree.len(), 0);
        assert_eq!(tree.rank(1.0, b"a"), None);
        assert_eq!(tree.rank_of(1.0, b"a", true), 0);
        assert!(!take(&mut tree, 1.0, b"a"));
        assert!(entries(&tree, 0, usize::MAX).is_empty());
        assert_eq!(tree.range(0, 5).next_back(), None);
        tree.check();

        assert!(add(&mut tree, 1.0, b"only"));
        assert_eq!((tree.len(), tree.rank(1.0, b"only")), (1, Some(0)));
        let only = vec![(1.0, b"only".to_vec())];
        assert_eq!(entries(&tree, 0, 0), only);
        assert_eq!(entries(&tree, 0, usize::MAX), only);
        assert!(entries(&tree, 1, 5).is_empty());
        assert!(entries(&tree, 1, 0).is_empty(), "start past stop");
        assert!(take(&mut tree, 1.0, b"only"));
        assert_eq!(tree.len(), 0);
        tree.check();
    }

    /// Ascending, descending and equal-score runs: splits and merges at each edge.
    #[test]
    fn sorted_runs_in_and_out_keep_every_invariant() {
        const N: usize = 3_000;
        for run in 0..3 {
            let key = |i: usize| match run {
                0 => (i as f64, format!("m{i}").into_bytes()),
                1 => (-(i as f64), format!("m{i}").into_bytes()),
                _ => (7.0, format!("{i:05}").into_bytes()),
            };
            let rank = |i: usize| if run == 1 { N - 1 - i } else { i };
            let mut tree = CountedTree::default();
            for i in 0..N {
                let (score, member) = key(i);
                assert!(add(&mut tree, score, &member));
            }
            tree.check();
            for i in (0..N).step_by(97) {
                let (score, member) = key(i);
                assert_eq!(tree.rank(score, &member), Some(rank(i)));
            }
            for start in [0, N / 2 - 5, N - 10] {
                let got = entries(&tree, start, start + 9);
                let want: Vec<_> = (start..start + 10)
                    .map(|r| key(if run == 1 { N - 1 - r } else { r }))
                    .collect();
                assert_eq!(got, want, "run {run}, ranks from {start}");
            }
            // First half removed in insertion order, the rest in reverse.
            for i in (0..N / 2).chain((N / 2..N).rev()) {
                let (score, member) = key(i);
                assert!(take(&mut tree, score, &member));
                if i % 250 == 0 {
                    tree.check();
                }
            }
            assert_eq!(tree.len(), 0);
            assert!(tree.root.children.is_empty() && tree.root.entries.is_empty());
            tree.check();
        }
    }

    /// Random operations, with ties and score changes, checked against a BTreeSet.
    #[test]
    fn random_operations_agree_with_a_sorted_model() {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = |n: usize| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % n as u64) as usize
        };
        let tied = [f64::NEG_INFINITY, -2.5, -0.0, 0.0, 1.0, f64::INFINITY];
        let mut tree = CountedTree::default();
        let mut model = Model::new();
        for step in 0..60_000 {
            // Grow, shrink, grow: splits and merges on every level.
            let inserts = if (step / 20_000) % 2 == 0 { 60 } else { 10 };
            let score = if next(3) == 0 {
                tied[next(tied.len())]
            } else {
                next(2_000) as f64 - 1_000.5
            };
            let member: Vec<u8> = match next(200) {
                0 => vec![next(256) as u8; SMALL_MAX + 1 + next(100)],
                1..=100 => (0..next(3)).map(|_| [0, b'a', 0xff][next(3)]).collect(),
                _ => (0..next(16)).map(|_| next(256) as u8).collect(),
            };
            let key = (OrderedFloat(score + 0.0), member);
            let len = model.len();
            match next(100) {
                r if r < inserts => {
                    let added = add(&mut tree, score, &key.1);
                    assert_eq!(added, model.insert(key));
                }
                r if r < 85 && len > 0 => {
                    // A score change, as the set makes it: the bytes stay.
                    let (old, member) = model.iter().nth(next(len)).cloned().unwrap();
                    let new = OrderedFloat(score + 0.0);
                    if !model.contains(&(new, member.clone())) {
                        let rank = tree.rank(old.0, &member).unwrap();
                        let handle = tree.remove_at(rank, |handle, _, _| handle).unwrap();
                        assert_eq!(tree.slab.get(handle), None, "freed");
                        let handle = tree.insert_new(old.0, &member).unwrap();
                        tree.rescore(handle, old.0, new.0);
                        model.remove(&(old, member.clone()));
                        model.insert((new, member));
                    }
                }
                r if r < 90 => {
                    assert_eq!(take(&mut tree, score, &key.1), model.remove(&key));
                }
                r if r < 96 => {
                    let key = match len {
                        0 => key,
                        _ => model.iter().nth(next(len)).cloned().unwrap(),
                    };
                    let want = model.contains(&key).then(|| model.range(..&key).count());
                    assert_eq!(tree.rank(key.0.0, &key.1), want);
                }
                _ => {
                    let start = next(len + 2);
                    let stop = start + next(40);
                    assert_eq!(
                        entries(&tree, start, stop),
                        model_range(&model, start, stop)
                    );
                }
            }
            assert_eq!(tree.len(), model.len());
            if step % 2_000 == 0 {
                tree.check();
            }
        }
        tree.check();
        assert_eq!(
            entries(&tree, 0, usize::MAX),
            model_range(&model, 0, usize::MAX)
        );
        for (score, member) in &model {
            assert!(take(&mut tree, score.0, member));
        }
        assert_eq!(tree.len(), 0);
        tree.check();
    }

    /// Bounds by key, present or not, and walks from both ends, skips included.
    #[test]
    fn ranks_of_keys_and_two_ended_walks_agree_with_a_model() {
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = |n: usize| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % n as u64) as usize
        };
        let mut tree = CountedTree::default();
        let mut model = Model::new();
        for size in [0usize, 1, 31, 32, 33, 500, 5_000] {
            while model.len() < size {
                let key = (
                    OrderedFloat(next(50) as f64),
                    vec![next(256) as u8; 1 + next(3)],
                );
                if model.insert(key.clone()) {
                    tree.insert_new(key.0.0, &key.1).unwrap();
                }
            }
            tree.check();
            let all = model_range(&model, 0, usize::MAX);
            for _ in 0..200 {
                let probe = (
                    OrderedFloat(next(52) as f64 - 1.0),
                    vec![next(256) as u8; next(4)],
                );
                let below = model.range(..&probe).count();
                let at_most = below + usize::from(model.contains(&probe));
                assert_eq!(tree.rank_of(probe.0.0, &probe.1, false), below);
                assert_eq!(tree.rank_of(probe.0.0, &probe.1, true), at_most);
            }
            for _ in 0..50 {
                let start = next(size + 2);
                let end = start + next(size + 2);
                let mut front = start.min(size);
                let mut back = end.min(size);
                let mut it = tree.range(start, end);
                assert_eq!(it.len(), back - front);
                while front < back {
                    // Skips either side of the step limit.
                    let skip = [0, 0, 1, 2, 31, 32, 40][next(7)];
                    let from_back = next(2) == 1;
                    let got = match (from_back, skip) {
                        (false, 0) if next(2) == 0 => it.next(),
                        (true, 0) if next(2) == 0 => it.next_back(),
                        (false, _) => it.nth(skip),
                        (true, _) => it.nth_back(skip),
                    };
                    let want = if skip >= back - front {
                        front = back;
                        None
                    } else if from_back {
                        back -= skip + 1;
                        Some(all[back].clone())
                    } else {
                        front += skip + 1;
                        Some(all[front - 1].clone())
                    };
                    assert_eq!(got.map(|(m, s)| (s, m.to_vec())), want);
                    assert_eq!(it.len(), back - front);
                }
                assert_eq!(it.next(), None);
                assert_eq!(it.next_back(), None);
            }
            // Counting and the last entry need no walk.
            assert_eq!(tree.range(3, size).count(), size.saturating_sub(3));
            assert_eq!(
                tree.range(0, size).last().map(|(m, s)| (s, m.to_vec())),
                all.last().cloned()
            );
        }
    }
}
