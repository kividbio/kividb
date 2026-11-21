//! A large sorted set's (score, member) order: a B+tree counted for rank.
//!
//! Leaves hold the entries: a score and an opaque reference to the member.
//! Internal nodes hold each child's entry count and a score between children.
//! A separator is untied when the scores either side of it differ:
//! routing then compares scores only. A tied separator is its next child's
//! first score, and that child's first member decides.

use std::cmp::Ordering;
use std::ops::Bound;

use ordered_float::OrderedFloat;

/// Entries in a leaf, and children of an internal node, at most.
const CAP: usize = 32;
/// A split leaves halves of at least this many.
const MIN: usize = CAP / 2;

/// Entries under a child: u64, so the tree sets no member limit.
type Count = u64;

/// Slots in use first, then empty ones.
type Slots<T> = [Option<T>; CAP];

/// A separator's score, and whether it is tied.
type Sep = (f64, bool);

/// A missing slot reads as no member; `check` proves slots in use are full.
const EMPTY: &[u8] = &[];

/// What the tree reads through a reference it holds.
pub(crate) trait Member {
    /// The member's bytes: equal scores order by them.
    fn member(&self) -> &[u8];
}

/// Entries in (score, member) order, counted for rank.
pub(crate) struct CountedTree<R> {
    /// None until the first entry, and again once the last is gone.
    root: Option<Box<Inner<R>>>,
    len: usize,
    census: Census,
}

/// Nodes allocated, so heap bytes are exact without a walk.
#[derive(Default)]
struct Census {
    leaves: usize,
    inners: usize,
}

struct Leaf<R> {
    len: u8,
    /// Apart from the references, so a search reads only scores.
    scores: [f64; CAP],
    refs: Slots<R>,
}

struct Inner<R> {
    keys: Keys,
    children: Children<R>,
}

/// An internal node's children in use, their counts and the separators.
#[derive(Default)]
struct Keys {
    len: u8,
    /// Bit `s` set: separator `s` is tied.
    ties: u32,
    counts: [Count; CAP],
    /// `seps[s]` lies between child `s` and child `s + 1`.
    seps: [f64; CAP - 1],
}

/// One tag for all of a node's children, which share a level.
enum Children<R> {
    Leaves(Slots<Box<Leaf<R>>>),
    Inners(Slots<Box<Inner<R>>>),
}

/// A node's new upper half, and the separator before it.
struct Split<N> {
    sep: Sep,
    count: Count,
    right: Box<N>,
}

/// A node built whole, and what its parent needs of it.
struct Built<N> {
    node: Box<N>,
    count: Count,
    first: f64,
    last: f64,
}

/// A child, borrowed for a walk down.
enum Child<'a, R> {
    Leaf(&'a Leaf<R>),
    Inner(&'a Inner<R>),
}

impl<'a, R> Child<'a, R> {
    /// The first leaf of this subtree, or the last.
    fn end_leaf(self, last: bool) -> Option<&'a Leaf<R>> {
        match self {
            Child::Leaf(leaf) => Some(leaf),
            Child::Inner(inner) => inner.end_leaf(last),
        }
    }
}

/// The entry a removal takes.
enum Target<'a, R> {
    /// The entry of this score that holds this very reference.
    Entry(f64, &'a R),
    /// The entry of this rank, within the subtree.
    Rank(Count),
}

/// An entry taken out, and its subtree's new first score if that changed.
struct Taken<R> {
    score: f64,
    r: R,
    first: Option<f64>,
}

/// How a score change was made.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Rescore {
    /// The entry kept its rank: its score was written where it is.
    InPlace,
    /// The entry was taken out and put back at its new rank.
    Moved,
}

/// What a score change below asks of the nodes above.
enum Rescoring {
    /// Made in place. If the subtree's first or last entry changed, the score
    /// beside it, for the separator between them further up.
    InPlace {
        first: Option<f64>,
        last: Option<f64>,
    },
    /// The new key passes a neighbour: the entry must move.
    Moves,
}

/// Entries of a range of ranks, in order, from either end.
/// Each end finds its leaf from the root, and again on leaving it.
pub struct Iter<'a, R> {
    tree: &'a CountedTree<R>,
    /// Rank of the next entry from the front; one past the back's.
    start: usize,
    end: usize,
    /// The front's leaf, and where rank `start` is in it.
    front: Option<(&'a Leaf<R>, usize)>,
    /// The back's leaf, and one past where rank `end - 1` is in it.
    back: Option<(&'a Leaf<R>, usize)>,
}

/// What rebalancing needs of a leaf or an internal node.
trait Node {
    /// Entries of a leaf, children of an internal node.
    fn fill(&self) -> usize;
    /// Moves `left`'s last entry or child here: its count, and the new separator.
    fn take_last_of(&mut self, left: &mut Self, between: Sep) -> Option<(Count, Sep)>;
    /// Moves `right`'s first `k` entries or children here, and `right` keeps one:
    /// their count, and the new separator.
    fn take_front_of(&mut self, right: &mut Self, k: usize, between: Sep) -> Option<(Count, Sep)>;
    /// Moves all of `right` onto the end; false if nothing moved.
    fn absorb(&mut self, right: &mut Self, between: Sep) -> bool;
}

impl<R> Default for CountedTree<R> {
    fn default() -> Self {
        Self {
            root: None,
            len: 0,
            census: Census::default(),
        }
    }
}

impl<R> CountedTree<R> {
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Bytes of the nodes, as allocated.
    pub(crate) fn heap_bytes(&self) -> usize {
        self.census.leaves * size_of::<Leaf<R>>() + self.census.inners * size_of::<Inner<R>>()
    }

    /// The entry of 0-based rank `rank`.
    pub(crate) fn nth(&self, rank: usize) -> Option<(f64, &R)> {
        let (leaf, i) = self.leaf_holding(rank)?;
        leaf.get(i)
    }

    /// Every entry, in order.
    pub(crate) fn iter(&self) -> Iter<'_, R> {
        self.range_by_rank(0, self.len)
    }

    /// The entries of ranks `start..end`, as far as the tree holds them.
    pub(crate) fn range_by_rank(&self, start: usize, end: usize) -> Iter<'_, R> {
        let end = end.min(self.len);
        Iter {
            tree: self,
            start: start.min(end),
            end,
            front: None,
            back: None,
        }
    }

    /// The leaf holding rank `rank`, and the entry's index in it.
    fn leaf_holding(&self, rank: usize) -> Option<(&Leaf<R>, usize)> {
        let mut rank = rank as Count;
        let mut node = self.root.as_deref()?;
        loop {
            let c = node.child_holding(&mut rank)?;
            match node.child(c)? {
                Child::Inner(inner) => node = inner,
                Child::Leaf(leaf) => return Some((leaf, rank as usize)),
            }
        }
    }

    /// Entries scored below `score`, or up to it too if `inclusive`.
    /// Separators and leaves hold the scores, so no member is read.
    pub(crate) fn score_rank(&self, score: f64, inclusive: bool) -> usize {
        let Some(mut node) = self.root.as_deref() else {
            return 0;
        };
        let mut before: Count = 0;
        loop {
            let keys = &node.keys;
            let (lo, hi) = equal_run(&keys.seps[..keys.len().saturating_sub(1)], score);
            // Entries before a separator score at most its score, after it at least.
            let c = if inclusive { hi } else { lo };
            before += keys.counts[..c].iter().sum::<Count>();
            match node.child(c) {
                Some(Child::Inner(inner)) => node = inner,
                Some(Child::Leaf(leaf)) => {
                    let (lo, hi) = equal_run(&leaf.scores[..leaf.len()], score);
                    return before as usize + if inclusive { hi } else { lo };
                }
                None => return before as usize,
            }
        }
    }
}

impl<'a, R> Iterator for Iter<'a, R> {
    type Item = (f64, &'a R);

    fn next(&mut self) -> Option<Self::Item> {
        if self.start == self.end {
            return None;
        }
        let (leaf, i) = match self.front {
            Some((leaf, i)) if i < leaf.len() => (leaf, i),
            // Not placed yet, or past its leaf: down from the root.
            _ => self.tree.leaf_holding(self.start)?,
        };
        self.front = Some((leaf, i + 1));
        self.start += 1;
        leaf.get(i)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let left = self.end - self.start;
        (left, Some(left))
    }

    fn count(self) -> usize {
        self.len()
    }

    fn last(mut self) -> Option<Self::Item> {
        self.next_back()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if n >= self.len() {
            self.start = self.end;
            return None;
        }
        self.start += n;
        // Past its leaf, `next` finds the front again.
        self.front = self.front.map(|(leaf, i)| (leaf, i + n));
        self.next()
    }
}

impl<R> DoubleEndedIterator for Iter<'_, R> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.start == self.end {
            return None;
        }
        let (leaf, i) = match self.back {
            Some((leaf, i)) if i > 0 => (leaf, i - 1),
            _ => self.tree.leaf_holding(self.end - 1)?,
        };
        self.back = Some((leaf, i));
        self.end -= 1;
        leaf.get(i)
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        if n >= self.len() {
            self.end = self.start;
            return None;
        }
        self.end -= n;
        // Before its leaf, `next_back` finds the back again.
        self.back = self
            .back
            .and_then(|(leaf, i)| Some((leaf, i.checked_sub(n)?)));
        self.next_back()
    }
}

impl<R> ExactSizeIterator for Iter<'_, R> {}

impl<R: Member> CountedTree<R> {
    /// Adds the entry; if an equal one is present, `r` comes back.
    pub(crate) fn insert(&mut self, score: f64, r: R) -> Result<(), R> {
        let census = &mut self.census;
        let mut root = match self.root.take() {
            Some(root) => root,
            None => Inner::first(census),
        };
        let split = match root.insert(score, r, true, census) {
            Ok(split) => split,
            Err(r) => {
                self.root = Some(root);
                return Err(r);
            }
        };
        self.root = Some(match split {
            None => root,
            Some(split) => Inner::above(root, split, census),
        });
        self.len += 1;
        Ok(())
    }

    /// The tree of `sorted`, each key above the one before; else `sorted` comes back.
    /// Built from the leaves up, every node as full as the rest.
    pub(crate) fn from_sorted(sorted: Vec<(f64, R)>) -> Result<Self, Vec<(f64, R)>> {
        // Members are read only where scores tie.
        let ascending = sorted.windows(2).all(|pair| {
            let ((a, a_ref), (b, b_ref)) = (&pair[0], &pair[1]);
            let scores = OrderedFloat(*a).cmp(&OrderedFloat(*b));
            scores
                .then_with(|| a_ref.member().cmp(b_ref.member()))
                .is_lt()
        });
        if !ascending {
            return Err(sorted);
        }
        let len = sorted.len();
        if len == 0 {
            return Ok(Self::default());
        }
        let mut entries = sorted.into_iter();
        let mut leaves = Vec::with_capacity(len.div_ceil(CAP));
        for size in spread(len) {
            let mut leaf = Leaf::empty();
            for (i, (score, r)) in (0..size).zip(&mut entries) {
                leaf.scores[i] = score;
                leaf.refs[i] = Some(r);
                leaf.len += 1;
            }
            let (first, last) = (leaf.scores[0], leaf.scores[leaf.len() - 1]);
            let count = leaf.len() as Count;
            leaves.push(Built {
                node: leaf,
                count,
                first,
                last,
            });
        }
        let mut census = Census {
            leaves: leaves.len(),
            inners: 0,
        };
        let mut level = group(leaves, Children::Leaves, &mut census);
        while level.len() > 1 {
            level = group(level, Children::Inners, &mut census);
        }
        Ok(CountedTree {
            root: level.pop().map(|top| top.node),
            len,
            census,
        })
    }

    /// `Ok(rank)` of an entry present, else `Err(rank)` it would take.
    pub(crate) fn rank(&self, score: f64, member: &[u8]) -> Result<usize, usize> {
        let Some(mut node) = self.root.as_deref() else {
            return Err(0);
        };
        let mut before: Count = 0;
        loop {
            let c = node.route(score, member);
            before += node.keys.counts[..c].iter().sum::<Count>();
            let base = before as usize;
            match node.child(c) {
                Some(Child::Inner(inner)) => node = inner,
                Some(Child::Leaf(leaf)) => {
                    return leaf
                        .search(score, member)
                        .map(|i| base + i)
                        .map_err(|i| base + i);
                }
                None => return Err(base),
            }
        }
    }

    /// Entries before `(score, member)`, or up to it too if `inclusive`.
    pub(crate) fn key_rank(&self, score: f64, member: &[u8], inclusive: bool) -> usize {
        // No member sorts before the empty one: the score alone decides.
        if member.is_empty() && !inclusive {
            return self.score_rank(score, false);
        }
        match self.rank(score, member) {
            Ok(rank) => rank + usize::from(inclusive),
            Err(rank) => rank,
        }
    }

    /// Ranks `start..end` of the entries from `from` to `to`, for `range_by_rank`.
    pub(crate) fn rank_range(
        &self,
        from: Bound<(f64, &[u8])>,
        to: Bound<(f64, &[u8])>,
    ) -> (usize, usize) {
        let start = match from {
            Bound::Included((score, member)) => self.key_rank(score, member, false),
            Bound::Excluded((score, member)) => self.key_rank(score, member, true),
            Bound::Unbounded => 0,
        };
        let end = match to {
            Bound::Included((score, member)) => self.key_rank(score, member, true),
            Bound::Excluded((score, member)) => self.key_rank(score, member, false),
            Bound::Unbounded => self.len,
        };
        // Bounds that cross hold nothing.
        (start, end.max(start))
    }
}

impl<R: Member + PartialEq> CountedTree<R> {
    /// Takes out the entry of `score` holding `r` itself, and gives that back.
    pub(crate) fn remove(&mut self, score: f64, r: &R) -> Option<R> {
        self.take_out(Target::Entry(score, r)).map(|(_, r)| r)
    }

    /// Takes out the entry of 0-based rank `rank`.
    pub(crate) fn remove_at(&mut self, rank: usize) -> Option<(f64, R)> {
        self.take_out(Target::Rank(rank as Count))
    }

    /// Moves the entry of `old` holding `r` itself to `new`: in place if it
    /// keeps its rank, else out and back in. `None`, with nothing changed, if
    /// no such entry is held or another holds `new` and the same member.
    pub(crate) fn rescore(&mut self, old: f64, new: f64, r: &R) -> Option<Rescore> {
        let root = self.root.as_deref_mut()?;
        match root.rescore((old, new), r, None, None)? {
            Rescoring::InPlace { .. } => Some(Rescore::InPlace),
            Rescoring::Moves => {
                // Refused before anything moves, so a refusal changes nothing.
                if self.rank(new, r.member()).is_ok() {
                    return None;
                }
                let held = self.remove(old, r)?;
                // The new key was free, so the reference goes back in.
                self.insert(new, held).ok().map(|()| Rescore::Moved)
            }
        }
    }

    fn take_out(&mut self, target: Target<'_, R>) -> Option<(f64, R)> {
        let taken = self.root.as_deref_mut()?.remove(target, &mut self.census)?;
        self.len -= 1;
        self.shrink_root();
        Some((taken.score, taken.r))
    }

    /// Frees an empty tree; a root over one internal node gives way to it.
    fn shrink_root(&mut self) {
        if self.len == 0 {
            self.root = None;
            self.census = Census::default();
            return;
        }
        if let Some(root) = self.root.as_mut()
            && root.keys.len() == 1
            && let Children::Inners(kids) = &mut root.children
            && let Some(only) = kids[0].take()
        {
            *root = only;
            self.census.inners -= 1;
        }
    }
}

impl<R> Leaf<R> {
    fn empty() -> Box<Self> {
        Box::new(Leaf {
            len: 0,
            scores: [0.0; CAP],
            refs: [const { None }; CAP],
        })
    }

    fn len(&self) -> usize {
        usize::from(self.len)
    }

    fn get(&self, i: usize) -> Option<(f64, &R)> {
        Some((*self.scores.get(i)?, self.refs.get(i)?.as_ref()?))
    }

    fn last(&self) -> Option<(f64, &R)> {
        self.get(self.len().checked_sub(1)?)
    }

    /// Puts the entry at `i`; the leaf has room.
    fn put(&mut self, i: usize, score: f64, r: R) {
        let n = self.len();
        self.scores.copy_within(i..n, i + 1);
        self.scores[i] = score;
        put(&mut self.refs, i, n, r);
        self.len += 1;
    }

    /// Takes the entry at `i` out, closing the gap.
    fn take(&mut self, i: usize) -> Option<(f64, R)> {
        let n = self.len();
        if i >= n {
            return None;
        }
        let r = self.refs[i].take()?;
        let score = self.scores[i];
        self.scores.copy_within(i + 1..n, i);
        self.refs[i..n].rotate_left(1);
        self.len -= 1;
        Some((score, r))
    }

    /// Moves the entries from `MIN` on into a new leaf.
    fn split_off(&mut self) -> Box<Self> {
        let n = self.len();
        let mut right = Self::empty();
        right.scores[..n - MIN].copy_from_slice(&self.scores[MIN..n]);
        right.refs = take_tail(&mut self.refs, MIN, n);
        right.len = (n - MIN) as u8;
        self.len = MIN as u8;
        right
    }
}

impl<R: Member> Leaf<R> {
    fn member(&self, i: usize) -> &[u8] {
        self.refs.get(i).map_or(EMPTY, bytes)
    }

    /// True if `(score, member)` sorts after every entry here.
    fn ends_before(&self, score: f64, member: &[u8]) -> bool {
        self.last()
            .is_some_and(|last| against(score, member, last).is_gt())
    }

    fn search(&self, score: f64, member: &[u8]) -> Result<usize, usize> {
        let (lo, hi) = equal_run(&self.scores[..self.len()], score);
        self.refs[lo..hi]
            .binary_search_by(|r| bytes(r).cmp(member))
            .map(|i| lo + i)
            .map_err(|i| lo + i)
    }

    /// Adds the entry in order; a full leaf splits, giving its upper half.
    fn insert(&mut self, score: f64, r: R) -> Result<Option<Split<Self>>, R> {
        let i = match self.search(score, r.member()) {
            Ok(_) => return Err(r),
            Err(i) => i,
        };
        if self.len() < CAP {
            self.put(i, score, r);
            return Ok(None);
        }
        let mut right = self.split_off();
        if i <= MIN {
            self.put(i, score, r);
        } else {
            right.put(i - MIN, score, r);
        }
        Ok(Some(Split {
            sep: sep_between(self.scores[self.len() - 1], right.scores[0]),
            count: right.len() as Count,
            right,
        }))
    }
}

impl<R: Member + PartialEq> Leaf<R> {
    fn remove(&mut self, target: Target<'_, R>) -> Option<Taken<R>> {
        let i = match target {
            Target::Entry(score, r) => {
                let i = self.search(score, r.member()).ok()?;
                // Equal bytes are not enough: it must be the reference held.
                (self.refs[i].as_ref() == Some(r)).then_some(i)?
            }
            Target::Rank(rank) => rank as usize,
        };
        let (score, r) = self.take(i)?;
        let first = (i == 0 && self.len > 0).then(|| self.scores[0]);
        Some(Taken { score, r, first })
    }

    /// Writes `new` over the score of the entry of `old` holding `r`, if the
    /// new key stays between the entry's neighbours in the whole tree:
    /// `before` and `after` are the subtrees beside this leaf.
    fn rescore(
        &mut self,
        (old, new): (f64, f64),
        r: &R,
        before: Option<Child<'_, R>>,
        after: Option<Child<'_, R>>,
    ) -> Option<Rescoring> {
        let member = r.member();
        let i = self.search(old, member).ok()?;
        if self.refs[i].as_ref() != Some(r) {
            return None;
        }
        let (at_start, at_end) = (i == 0, i + 1 == self.len());
        let below = if at_start {
            before.and_then(|side| side.end_leaf(true)?.last())
        } else {
            self.get(i - 1)
        };
        let above = if at_end {
            after.and_then(|side| side.end_leaf(false)?.get(0))
        } else {
            self.get(i + 1)
        };
        let past_below = below.is_none_or(|e| against(new, member, e).is_gt());
        if !past_below || above.is_some_and(|e| against(new, member, e).is_ge()) {
            return Some(Rescoring::Moves);
        }
        // Separators above see this leaf's first and last scores.
        let first = below.filter(|_| at_start).map(|(score, _)| score);
        let last = above.filter(|_| at_end).map(|(score, _)| score);
        self.scores[i] = new;
        Some(Rescoring::InPlace { first, last })
    }
}

impl<R> Node for Leaf<R> {
    fn fill(&self) -> usize {
        self.len()
    }

    fn take_last_of(&mut self, left: &mut Self, _: Sep) -> Option<(Count, Sep)> {
        // `left` held more than MIN, so it keeps a last entry.
        let (score, r) = left.take(left.len().checked_sub(1)?)?;
        self.put(0, score, r);
        Some((1, sep_between(left.scores[left.len() - 1], score)))
    }

    fn take_front_of(&mut self, right: &mut Self, k: usize, _: Sep) -> Option<(Count, Sep)> {
        let (n, m) = (self.len(), right.len());
        if k == 0 || k >= m || n + k > CAP {
            return None;
        }
        self.scores[n..n + k].copy_from_slice(&right.scores[..k]);
        shift(&mut self.refs[n..n + k], &mut right.refs[..k]);
        right.scores.copy_within(k..m, 0);
        right.refs[..m].rotate_left(k);
        self.len += k as u8;
        right.len -= k as u8;
        Some((
            k as Count,
            sep_between(self.scores[n + k - 1], right.scores[0]),
        ))
    }

    fn absorb(&mut self, right: &mut Self, _: Sep) -> bool {
        let (n, m) = (self.len(), right.len());
        self.scores[n..n + m].copy_from_slice(&right.scores[..m]);
        shift(&mut self.refs[n..n + m], &mut right.refs[..m]);
        self.len += right.len;
        right.len = 0;
        true
    }
}

impl<R> Inner<R> {
    /// A root over one empty leaf, for the first entry.
    fn first(census: &mut Census) -> Box<Self> {
        census.leaves += 1;
        census.inners += 1;
        let mut kids = [const { None }; CAP];
        kids[0] = Some(Leaf::empty());
        Box::new(Inner {
            keys: Keys {
                len: 1,
                ..Keys::default()
            },
            children: Children::Leaves(kids),
        })
    }

    /// A new root over the old one and its new upper half.
    fn above(left: Box<Self>, split: Split<Self>, census: &mut Census) -> Box<Self> {
        census.inners += 1;
        let mut keys = Keys {
            len: 2,
            ..Keys::default()
        };
        keys.counts[0] = left.keys.total();
        keys.counts[1] = split.count;
        keys.set_sep(0, split.sep);
        let mut kids = [const { None }; CAP];
        kids[0] = Some(left);
        kids[1] = Some(split.right);
        Box::new(Inner {
            keys,
            children: Children::Inners(kids),
        })
    }

    fn child(&self, c: usize) -> Option<Child<'_, R>> {
        match &self.children {
            Children::Leaves(kids) => kids.get(c)?.as_deref().map(Child::Leaf),
            Children::Inners(kids) => kids.get(c)?.as_deref().map(Child::Inner),
        }
    }

    /// The first leaf below, or the last: it holds the smallest or largest key.
    fn end_leaf(&self, last: bool) -> Option<&Leaf<R>> {
        let mut node = self;
        loop {
            let c = if last {
                node.keys.len().checked_sub(1)?
            } else {
                0
            };
            match node.child(c)? {
                Child::Leaf(leaf) => return Some(leaf),
                Child::Inner(inner) => node = inner,
            }
        }
    }

    /// The child holding `rank`, which becomes a rank within it.
    fn child_holding(&self, rank: &mut Count) -> Option<usize> {
        for (c, &count) in self.keys.counts[..self.keys.len()].iter().enumerate() {
            if *rank < count {
                return Some(c);
            }
            *rank -= count;
        }
        None
    }
}

impl<R: Member> Inner<R> {
    /// The child whose keys `(score, member)` falls among.
    fn route(&self, score: f64, member: &[u8]) -> usize {
        let seps = &self.keys.seps[..self.keys.len().saturating_sub(1)];
        let (lo, hi) = equal_run(seps, score);
        // Past an untied separator; past a tied one unless below its child.
        partition(lo, hi, |s| {
            !self.keys.tied(s) || member >= self.children.first_member(s + 1)
        })
    }

    /// Adds the entry below; a full node splits, giving its upper half.
    /// `edge`: this node is last at every level above it.
    fn insert(
        &mut self,
        score: f64,
        r: R,
        edge: bool,
        census: &mut Census,
    ) -> Result<Option<Split<Self>>, R> {
        let c = self.route(score, r.member());
        // A key past every other goes to the last child at every level.
        let edge = edge && c + 1 == self.keys.len();
        let keys = &mut self.keys;
        let half = match &mut self.children {
            Children::Leaves(kids) => {
                if edge {
                    lend(keys, kids, c, |leaf| leaf.ends_before(score, r.member()));
                }
                let Some(leaf) = kids.get_mut(c).and_then(Option::as_mut) else {
                    return Err(r);
                };
                let split = leaf.insert(score, r)?;
                census.leaves += usize::from(split.is_some());
                adopt(keys, kids, c, split)
                    .map(|(sep, right, kids)| (sep, right, Children::Leaves(kids)))
            }
            Children::Inners(kids) => {
                if edge {
                    lend(keys, kids, c, |inner| {
                        let last = inner.end_leaf(true);
                        last.is_some_and(|leaf| leaf.ends_before(score, r.member()))
                    });
                }
                let Some(inner) = kids.get_mut(c).and_then(Option::as_mut) else {
                    return Err(r);
                };
                let split = inner.insert(score, r, edge, census)?;
                adopt(keys, kids, c, split)
                    .map(|(sep, right, kids)| (sep, right, Children::Inners(kids)))
            }
        };
        let Some((sep, keys, children)) = half else {
            return Ok(None);
        };
        census.inners += 1;
        let right = Box::new(Inner { keys, children });
        Ok(Some(Split {
            sep,
            count: right.keys.total(),
            right,
        }))
    }
}

impl<R: Member + PartialEq> Inner<R> {
    /// Takes the target out below, then refills the child it left short.
    fn remove(&mut self, target: Target<'_, R>, census: &mut Census) -> Option<Taken<R>> {
        let (c, target) = match target {
            Target::Entry(score, r) => (self.route(score, r.member()), Target::Entry(score, r)),
            Target::Rank(mut rank) => (self.child_holding(&mut rank)?, Target::Rank(rank)),
        };
        let keys = &mut self.keys;
        let taken = match &mut self.children {
            Children::Leaves(kids) => {
                let taken = kids.get_mut(c)?.as_deref_mut()?.remove(target)?;
                keys.removed(c, taken.first);
                census.leaves -= usize::from(refill(keys, kids, c));
                taken
            }
            Children::Inners(kids) => {
                let taken = kids.get_mut(c)?.as_deref_mut()?.remove(target, census)?;
                keys.removed(c, taken.first);
                census.inners -= usize::from(refill(keys, kids, c));
                taken
            }
        };
        // This subtree's first entry changed only if its first child's did.
        let first = taken.first.filter(|_| c == 0);
        Some(Taken { first, ..taken })
    }

    /// Moves the entry of `old` holding `r` to `new` below, in place if it
    /// keeps its rank; `before` and `after`: the subtrees beside this one.
    fn rescore(
        &mut self,
        (old, new): (f64, f64),
        r: &R,
        before: Option<Child<'_, R>>,
        after: Option<Child<'_, R>>,
    ) -> Option<Rescoring> {
        let c = self.route(old, r.member());
        let n = self.keys.len();
        // The child's siblings are borrowed beside it: the nearest neighbours.
        let mut step = match &mut self.children {
            Children::Leaves(kids) => {
                let (left, child, right) = beside(kids, c)?;
                let before = left.map(Child::Leaf).or(before);
                let after = right.map(Child::Leaf).or(after);
                child.rescore((old, new), r, before, after)?
            }
            Children::Inners(kids) => {
                let (left, child, right) = beside(kids, c)?;
                let before = left.map(Child::Inner).or(before);
                let after = right.map(Child::Inner).or(after);
                child.rescore((old, new), r, before, after)?
            }
        };
        // The separator beside a changed first or last entry is the nearest one.
        if let Rescoring::InPlace { first, last } = &mut step {
            if c > 0
                && let Some(below) = first.take()
            {
                self.keys.set_sep(c - 1, sep_between(below, new));
            }
            if c + 1 < n
                && let Some(above) = last.take()
            {
                self.keys.set_sep(c, sep_between(new, above));
            }
        }
        Some(step)
    }
}

impl<R> Node for Inner<R> {
    fn fill(&self) -> usize {
        self.keys.len()
    }

    fn take_last_of(&mut self, left: &mut Self, between: Sep) -> Option<(Count, Sep)> {
        let (n, ln) = (self.keys.len(), left.keys.len());
        if !self.children.move_one(n, 0, &mut left.children, ln, ln - 1) {
            return None;
        }
        let (count, up) = left.keys.remove(ln - 1, ln - 2);
        self.keys.insert(0, count, 0, between);
        Some((count, up))
    }

    fn take_front_of(&mut self, right: &mut Self, k: usize, between: Sep) -> Option<(Count, Sep)> {
        let (n, rn) = (self.keys.len(), right.keys.len());
        if n == 0 || k == 0 || k >= rn || n + k > CAP {
            return None;
        }
        if !self.children.take_front(n, &mut right.children, k, rn) {
            return None;
        }
        Some(self.keys.take_front(&mut right.keys, k, between))
    }

    fn absorb(&mut self, right: &mut Self, between: Sep) -> bool {
        let (n, rn) = (self.keys.len(), right.keys.len());
        if !self.children.take_front(n, &mut right.children, rn, rn) {
            return false;
        }
        self.keys.append(&right.keys, between);
        true
    }
}

impl<R> Children<R> {
    /// Moves child `from_at` of `from` to `at` here.
    fn move_one(
        &mut self,
        n: usize,
        at: usize,
        from: &mut Self,
        from_n: usize,
        from_at: usize,
    ) -> bool {
        match (self, from) {
            (Children::Leaves(to), Children::Leaves(from)) => {
                move_slot(to, n, at, from, from_n, from_at)
            }
            (Children::Inners(to), Children::Inners(from)) => {
                move_slot(to, n, at, from, from_n, from_at)
            }
            // Children of one level share a kind, as `check` proves.
            _ => false,
        }
    }

    /// Moves `from`'s first `k` children after this node's first `n`;
    /// the rest of `from`'s first `from_n` move up.
    fn take_front(&mut self, n: usize, from: &mut Self, k: usize, from_n: usize) -> bool {
        match (self, from) {
            (Children::Leaves(to), Children::Leaves(from)) => {
                shift(&mut to[n..n + k], &mut from[..k]);
                from[..from_n].rotate_left(k);
            }
            (Children::Inners(to), Children::Inners(from)) => {
                shift(&mut to[n..n + k], &mut from[..k]);
                from[..from_n].rotate_left(k);
            }
            _ => return false,
        }
        true
    }
}

impl<R: Member> Children<R> {
    /// The first member under child `c`.
    fn first_member(&self, c: usize) -> &[u8] {
        match self {
            Children::Leaves(kids) => kids
                .get(c)
                .and_then(Option::as_deref)
                .map_or(EMPTY, |leaf| leaf.member(0)),
            Children::Inners(kids) => kids
                .get(c)
                .and_then(Option::as_deref)
                .map_or(EMPTY, |inner| inner.children.first_member(0)),
        }
    }
}

impl Keys {
    fn len(&self) -> usize {
        usize::from(self.len)
    }

    fn tied(&self, s: usize) -> bool {
        (self.ties >> s) & 1 == 1
    }

    fn sep(&self, s: usize) -> Sep {
        (self.seps[s], self.tied(s))
    }

    fn set_sep(&mut self, s: usize, (score, tied): Sep) {
        self.seps[s] = score;
        self.ties = (self.ties & !(1 << s)) | (u32::from(tied) << s);
    }

    fn total(&self) -> Count {
        self.counts[..self.len()].iter().sum()
    }

    /// Adds a child's count at `i` and a separator at `s`; there is room.
    fn insert(&mut self, i: usize, count: Count, s: usize, (score, tied): Sep) {
        let n = self.len();
        self.counts.copy_within(i..n, i + 1);
        self.counts[i] = count;
        self.seps.copy_within(s..n - 1, s + 1);
        self.seps[s] = score;
        let below = self.ties & ((1 << s) - 1);
        self.ties = below | (u32::from(tied) << s) | ((self.ties ^ below) << 1);
        self.len += 1;
    }

    /// Drops the count at `i` and the separator at `s`, giving both back.
    fn remove(&mut self, i: usize, s: usize) -> (Count, Sep) {
        let n = self.len();
        let (count, sep) = (self.counts[i], self.sep(s));
        self.counts.copy_within(i + 1..n, i);
        self.seps.copy_within(s + 1..n - 1, s);
        let below = self.ties & ((1 << s) - 1);
        self.ties = below | ((self.ties >> (s + 1)) << s);
        self.len -= 1;
        (count, sep)
    }

    /// Takes in `right`'s counts and separators after these, `between` first.
    fn append(&mut self, right: &Keys, between: Sep) {
        let (n, m) = (self.len(), right.len());
        self.counts[n..n + m].copy_from_slice(&right.counts[..m]);
        self.seps[n..n + m - 1].copy_from_slice(&right.seps[..m - 1]);
        self.set_sep(n - 1, between);
        self.ties |= right.ties << n;
        self.len += right.len;
    }

    /// Takes in the keys of `right`'s first `k` children after these, `between` first.
    /// Gives their count, and the separator now before `right`.
    fn take_front(&mut self, right: &mut Keys, k: usize, between: Sep) -> (Count, Sep) {
        let (n, m) = (self.len(), right.len());
        self.counts[n..n + k].copy_from_slice(&right.counts[..k]);
        self.seps[n..n + k - 1].copy_from_slice(&right.seps[..k - 1]);
        self.set_sep(n - 1, between);
        self.ties |= (right.ties & ((1 << (k - 1)) - 1)) << n;
        let up = right.sep(k - 1);
        right.counts.copy_within(k..m, 0);
        right.seps.copy_within(k..m - 1, 0);
        right.ties >>= k;
        self.len += k as u8;
        right.len -= k as u8;
        (self.counts[n..n + k].iter().sum(), up)
    }

    /// Counts an entry gone from child `c`, whose first score may now be `first`.
    fn removed(&mut self, c: usize, first: Option<f64>) {
        self.counts[c] -= 1;
        // A tied separator must stay its next child's first score.
        if let (Some(s), Some(first)) = (c.checked_sub(1), first)
            && self.tied(s)
            && OrderedFloat(self.seps[s]) != OrderedFloat(first)
        {
            // The scores before it are at most the old one, so below `first`.
            self.set_sep(s, (first, false));
        }
    }

    /// Moves children from `MIN` on into new keys, giving the separator between.
    fn split_off(&mut self) -> (Sep, Keys) {
        let n = self.len();
        let mut right = Keys {
            len: (n - MIN) as u8,
            ties: self.ties >> MIN,
            ..Keys::default()
        };
        right.counts[..n - MIN].copy_from_slice(&self.counts[MIN..n]);
        right.seps[..n - MIN - 1].copy_from_slice(&self.seps[MIN..n - 1]);
        let between = self.sep(MIN - 1);
        self.ties &= (1 << (MIN - 1)) - 1;
        self.len = MIN as u8;
        (between, right)
    }
}

/// Counts the new entry under child `c`, and takes in the child's upper half.
/// A full node first moves its own upper half out, and gives it back.
fn adopt<N>(
    keys: &mut Keys,
    kids: &mut Slots<Box<N>>,
    c: usize,
    split: Option<Split<N>>,
) -> Option<(Sep, Keys, Slots<Box<N>>)> {
    keys.counts[c] += 1;
    let split = split?;
    keys.counts[c] -= split.count;
    if keys.len() < CAP {
        place(keys, kids, c, split);
        return None;
    }
    let (sep, mut right) = keys.split_off();
    let mut right_kids = take_tail(kids, MIN, CAP);
    if c < MIN {
        place(keys, kids, c, split);
    } else {
        place(&mut right, &mut right_kids, c - MIN, split);
    }
    Some((sep, right, right_kids))
}

/// Before an append to full child `c`, fills the child before it from `c`'s front.
/// Nodes left behind the right edge end full; halving would leave them half full.
fn lend<N: Node>(
    keys: &mut Keys,
    kids: &mut Slots<Box<N>>,
    c: usize,
    past: impl FnOnce(&N) -> bool,
) {
    let Some(b) = c.checked_sub(1) else {
        return;
    };
    let Some((left, right)) = pair(kids, b) else {
        return;
    };
    let room = CAP.saturating_sub(left.fill());
    if room == 0 || right.fill() < CAP || !past(right) {
        return;
    }
    // `left` holds at least MIN, so `right` keeps at least MIN.
    if let Some((count, sep)) = left.take_front_of(right, room, keys.sep(b)) {
        keys.counts[b] += count;
        keys.counts[c] -= count;
        keys.set_sep(b, sep);
    }
}

/// Puts `split.right` after child `c`, with its separator and count; there is room.
fn place<N>(keys: &mut Keys, kids: &mut Slots<Box<N>>, c: usize, split: Split<N>) {
    put(kids, c + 1, keys.len(), split.right);
    keys.insert(c + 1, split.count, c, split.sep);
}

/// Brings child `c` back to `MIN` from a sibling above it, else merges a pair.
/// True if a node was freed.
fn refill<N: Node>(keys: &mut Keys, kids: &mut Slots<Box<N>>, c: usize) -> bool {
    let n = keys.len();
    let fill = |i: usize| kids.get(i).and_then(Option::as_deref).map(N::fill);
    let (here, left, right) = (fill(c), c.checked_sub(1).and_then(fill), fill(c + 1));
    if here.is_none_or(|here| here >= MIN) {
        return false;
    }
    if left.is_some_and(|left| left > MIN) {
        if let Some((left, child)) = pair(kids, c - 1)
            && let Some((count, sep)) = child.take_last_of(left, keys.sep(c - 1))
        {
            keys.counts[c - 1] -= count;
            keys.counts[c] += count;
            keys.set_sep(c - 1, sep);
        }
        return false;
    }
    if right.is_some_and(|right| right > MIN) {
        if let Some((child, right)) = pair(kids, c)
            && let Some((count, sep)) = child.take_front_of(right, 1, keys.sep(c))
        {
            keys.counts[c] += count;
            keys.counts[c + 1] -= count;
            keys.set_sep(c, sep);
        }
        return false;
    }
    // Neither sibling is above MIN, so a merged pair fits one node.
    let a = match (left, right) {
        (Some(_), _) => c - 1,
        (None, Some(_)) => c,
        (None, None) => return false,
    };
    let Some((left, right)) = pair(kids, a) else {
        return false;
    };
    if !left.absorb(right, keys.sep(a)) {
        return false;
    }
    let (count, _) = keys.remove(a + 1, a);
    keys.counts[a] += count;
    kids[a + 1..n].rotate_left(1);
    kids[n - 1] = None;
    true
}

/// Children `a` and `a + 1`, both borrowed.
fn pair<N>(kids: &mut Slots<Box<N>>, a: usize) -> Option<(&mut N, &mut N)> {
    let (head, tail) = kids.split_at_mut_checked(a + 1)?;
    Some((
        head.last_mut()?.as_deref_mut()?,
        tail.first_mut()?.as_deref_mut()?,
    ))
}

/// How `(score, member)` sorts against the entry `(s, r)`, whose member is
/// read only if the scores tie.
fn against<R: Member>(score: f64, member: &[u8], (s, r): (f64, &R)) -> Ordering {
    let scores = OrderedFloat(score).cmp(&OrderedFloat(s));
    scores.then_with(|| member.cmp(r.member()))
}

/// Child `c` to change, and the siblings either side of it to read.
fn beside<N>(kids: &mut Slots<Box<N>>, c: usize) -> Option<(Option<&N>, &mut N, Option<&N>)> {
    let (head, tail) = kids.split_at_mut(c);
    let (here, rest) = tail.split_first_mut()?;
    let left = head.last().and_then(|kid| kid.as_deref());
    let right = rest.first().and_then(|kid| kid.as_deref());
    Some((left, here.as_deref_mut()?, right))
}

/// The separator before `first`, tied if `last` before it has its score.
fn sep_between(last: f64, first: f64) -> Sep {
    (first, OrderedFloat(last) == OrderedFloat(first))
}

/// Sizes of the fewest nodes that hold `n`, as even as can be.
/// Past one node, each holds at least `MIN`: `n` exceeds `(nodes - 1) * CAP`.
fn spread(n: usize) -> impl Iterator<Item = usize> {
    let nodes = n.div_ceil(CAP).max(1);
    (0..nodes).map(move |i| n / nodes + usize::from(i < n % nodes))
}

/// Parents of `kids` in order, as few and as evenly filled as can be.
fn group<N, R>(
    kids: Vec<Built<N>>,
    children: fn(Slots<Box<N>>) -> Children<R>,
    census: &mut Census,
) -> Vec<Built<Inner<R>>> {
    let n = kids.len();
    let mut kids = kids.into_iter();
    let mut parents = Vec::with_capacity(n.div_ceil(CAP));
    for size in spread(n) {
        let mut keys = Keys::default();
        let mut slots = [const { None }; CAP];
        let (mut first, mut last) = (0.0, 0.0);
        for (c, kid) in (0..size).zip(&mut kids) {
            if c == 0 {
                first = kid.first;
            } else {
                keys.set_sep(c - 1, sep_between(last, kid.first));
            }
            last = kid.last;
            keys.counts[c] = kid.count;
            slots[c] = Some(kid.node);
            keys.len += 1;
        }
        census.inners += 1;
        let count = keys.total();
        let node = Box::new(Inner {
            keys,
            children: children(slots),
        });
        parents.push(Built {
            node,
            count,
            first,
            last,
        });
    }
    parents
}

/// Puts `item` at `at` among the first `n` slots, which have room.
fn put<T>(slots: &mut Slots<T>, at: usize, n: usize, item: T) {
    slots[at..=n].rotate_right(1);
    slots[at] = Some(item);
}

/// Moves slot `from_at` of `from`'s first `from_n` to `at` of `to`'s first `n`.
fn move_slot<T>(
    to: &mut Slots<T>,
    n: usize,
    at: usize,
    from: &mut Slots<T>,
    from_n: usize,
    from_at: usize,
) -> bool {
    let Some(item) = from[from_at].take() else {
        return false;
    };
    from[from_at..from_n].rotate_left(1);
    put(to, at, n, item);
    true
}

/// Takes slots `at..n` out, as the start of a new array.
fn take_tail<T>(slots: &mut Slots<T>, at: usize, n: usize) -> Slots<T> {
    let mut tail = [const { None }; CAP];
    shift(&mut tail[..n - at], &mut slots[at..n]);
    tail
}

/// Moves each of `from` into `to`, leaving `from` empty.
fn shift<T>(to: &mut [Option<T>], from: &mut [Option<T>]) {
    for (to, from) in to.iter_mut().zip(from) {
        *to = from.take();
    }
}

fn bytes<R: Member>(slot: &Option<R>) -> &[u8] {
    slot.as_ref().map_or(EMPTY, R::member)
}

/// The run of `scores` equal to `score`, as its start and end.
fn equal_run(scores: &[f64], score: f64) -> (usize, usize) {
    let score = OrderedFloat(score);
    let lo = scores.partition_point(|&s| OrderedFloat(s) < score);
    let hi = lo + scores[lo..].partition_point(|&s| OrderedFloat(s) == score);
    (lo, hi)
}

/// The first of `lo..hi` where `pred` fails: it holds before, fails after.
fn partition(mut lo: usize, mut hi: usize, pred: impl Fn(usize) -> bool) -> usize {
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if pred(mid) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// The sorted set's order, which the tree must agree with.
#[cfg(test)]
fn order(a: (f64, &[u8]), b: (f64, &[u8])) -> Ordering {
    crate::zset_table::order(a.0, a.1, b.0, b.1)
}

/// What a subtree holds, for `check`.
#[cfg(test)]
struct Span<'a> {
    count: Count,
    depth: usize,
    first: (f64, &'a [u8]),
    last: (f64, &'a [u8]),
}

#[cfg(test)]
impl<R: Member> CountedTree<R> {
    /// Checks counts, fill, depth, order, separators and the census.
    pub(crate) fn check(&self) {
        let Some(root) = self.root.as_deref() else {
            let held = (self.len, self.census.leaves, self.census.inners);
            assert_eq!(held, (0, 0, 0), "an empty tree holds nothing");
            return;
        };
        let mut seen = Census::default();
        let span = root.check(true, &mut seen);
        assert_eq!(span.count, self.len as Count, "len");
        let census = (self.census.leaves, self.census.inners);
        assert_eq!((seen.leaves, seen.inners), census, "census");
    }

    /// Levels of nodes, the leaves' included; 0 when empty.
    pub(crate) fn height(&self) -> usize {
        let mut levels = 0;
        let mut node = self.root.as_deref().map(Child::Inner);
        while let Some(here) = node {
            levels += 1;
            node = match here {
                Child::Inner(inner) => inner.child(0),
                Child::Leaf(_) => None,
            };
        }
        levels
    }
}

#[cfg(test)]
impl<R: Member> Leaf<R> {
    /// `alone`: the root's only child, which may hold fewer than `MIN`.
    fn check(&self, alone: bool) -> Span<'_> {
        let n = self.len();
        assert!((1..=CAP).contains(&n) && (alone || n >= MIN), "leaf of {n}");
        let mut slots = self.refs.iter().enumerate();
        assert!(slots.all(|(i, r)| r.is_some() == (i < n)), "leaf slots");
        let key = |i: usize| (self.scores[i], self.member(i));
        for i in 1..n {
            assert_eq!(order(key(i - 1), key(i)), Ordering::Less, "leaf order");
        }
        Span {
            count: n as Count,
            depth: 0,
            first: key(0),
            last: key(n - 1),
        }
    }
}

#[cfg(test)]
impl<R: Member> Inner<R> {
    fn check(&self, root: bool, seen: &mut Census) -> Span<'_> {
        let keys = &self.keys;
        let n = keys.len();
        assert!((1..=CAP).contains(&n) && (root || n >= MIN), "inner of {n}");
        let leaves = matches!(self.children, Children::Leaves(_));
        assert!(n > 1 || (root && leaves), "one child: the root over a leaf");
        assert_eq!(keys.ties >> (n - 1), 0, "tie bits past the separators");
        let full = (0..CAP).all(|c| self.child(c).is_some() == (c < n));
        assert!(full, "inner slots");
        seen.inners += 1;
        let mut spans = Vec::with_capacity(n);
        for c in 0..n {
            spans.push(match self.child(c) {
                Some(Child::Leaf(leaf)) => {
                    seen.leaves += 1;
                    leaf.check(root && n == 1)
                }
                Some(Child::Inner(inner)) => inner.check(false, seen),
                None => unreachable!("slots checked"),
            });
        }
        for (c, span) in spans.iter().enumerate() {
            assert_eq!(keys.counts[c], span.count, "count of child {c}");
            assert_eq!(span.depth, spans[0].depth, "leaves at one depth");
        }
        for (s, pair) in spans.windows(2).enumerate() {
            let (last, first) = (pair[0].last, pair[1].first);
            assert_eq!(order(last, first), Ordering::Less, "order at {s}");
            let sep = OrderedFloat(keys.seps[s]);
            let (low, high) = (OrderedFloat(last.0), OrderedFloat(first.0));
            if keys.tied(s) {
                assert_eq!(sep, high, "tied separator {s} is the next first score");
            } else {
                assert!(low < sep && sep <= high, "separator {s} between the scores");
            }
        }
        Span {
            count: keys.total(),
            depth: spans[0].depth + 1,
            first: spans[0].first,
            last: spans[n - 1].last,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::BTreeSet;
    use std::ops::RangeBounds;
    use std::ptr::NonNull;

    use super::*;

    type Tree = CountedTree<Box<[u8]>>;
    type Model = BTreeSet<(OrderedFloat<f64>, Vec<u8>)>;

    /// Past 4 KiB, as the earlier tree's long members were.
    const LONG: usize = 4_089;

    impl Member for Box<[u8]> {
        fn member(&self) -> &[u8] {
            self
        }
    }

    /// Equal bytes and another reference, as two table entries would be.
    #[derive(Debug, PartialEq)]
    struct Tagged(Box<[u8]>, u32);

    impl Member for Tagged {
        fn member(&self) -> &[u8] {
            &self.0
        }
    }

    fn xorshift(mut seed: u64) -> impl FnMut(usize) -> usize {
        move |n: usize| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % n as u64) as usize
        }
    }

    fn sizes() -> (usize, usize) {
        (size_of::<Leaf<Box<[u8]>>>(), size_of::<Inner<Box<[u8]>>>())
    }

    /// Leaves and internal nodes that hold `n` entries, at fewest.
    fn fewest_nodes(n: usize) -> (usize, usize) {
        let leaves = n.div_ceil(CAP);
        let (mut level, mut inners) = (leaves, 0);
        while level > 0 {
            level = level.div_ceil(CAP);
            inners += level;
            if level == 1 {
                break;
            }
        }
        (leaves, inners)
    }

    /// The model's keys, in order, as a bulk build takes them.
    fn built(model: &Model) -> Tree {
        let sorted = model.iter().map(|(s, m)| (s.0, m[..].into())).collect();
        let Ok(tree) = Tree::from_sorted(sorted) else {
            panic!("keys in order refused");
        };
        tree
    }

    /// An 8-byte reference, as the table's is; its member is empty.
    struct Raw(NonNull<u8>);

    impl Member for Raw {
        fn member(&self) -> &[u8] {
            EMPTY
        }
    }

    /// Adds an entry if absent, -0 kept as 0, as the set's callers do.
    fn add(tree: &mut Tree, score: f64, member: &[u8]) -> bool {
        tree.insert(score + 0.0, member.into()).is_ok()
    }

    /// Takes the entry out by its rank, if present.
    fn take(tree: &mut Tree, score: f64, member: &[u8]) -> bool {
        match tree.rank(score, member) {
            Ok(rank) => tree.remove_at(rank).is_some(),
            Err(_) => false,
        }
    }

    /// Takes the entry out by its reference: a box's bytes are its identity.
    fn remove(tree: &mut Tree, score: f64, member: &[u8]) -> bool {
        tree.remove(score, &Box::from(member)).is_some()
    }

    fn rank(tree: &Tree, score: f64, member: &[u8]) -> Option<usize> {
        tree.rank(score, member).ok()
    }

    /// Up to 7 bytes of four values, zero among them: often prefixes.
    fn binary(next: &mut impl FnMut(usize) -> usize) -> Vec<u8> {
        (0..next(8)).map(|_| [0, 1, b'a', 0xff][next(4)]).collect()
    }

    /// Kind 0 is unbounded; 1 and 2 include the key, 3 and 4 exclude it.
    fn bound(kind: usize, (score, member): &(f64, Vec<u8>)) -> Bound<(f64, &[u8])> {
        match kind {
            0 => Bound::Unbounded,
            1 | 2 => Bound::Included((*score, member)),
            _ => Bound::Excluded((*score, member)),
        }
    }

    /// Bounds at `(score, member)`, by key and by score, as the model counts them.
    fn bounds_agree(tree: &Tree, model: &Model, score: f64, member: &[u8]) {
        let key = (OrderedFloat(score), member.to_vec());
        let before = model.range(..&key).count();
        let up_to = model.range(..=&key).count();
        assert_eq!(
            tree.key_rank(score, member, false),
            before,
            "before {key:?}"
        );
        assert_eq!(tree.key_rank(score, member, true), up_to, "up to {key:?}");
        let below = model.iter().filter(|(s, _)| *s < key.0).count();
        let at_most = model.iter().filter(|(s, _)| *s <= key.0).count();
        assert_eq!(tree.score_rank(score, false), below, "below {score}");
        assert_eq!(tree.score_rank(score, true), at_most, "at most {score}");
    }

    /// A range's ranks, and its entries both ways, as the model holds them.
    fn range_agrees(
        tree: &Tree,
        model: &Model,
        from: Bound<(f64, &[u8])>,
        to: Bound<(f64, &[u8])>,
    ) {
        let key = |(score, member): (f64, &[u8])| (OrderedFloat(score), member.to_vec());
        let (lo, hi) = (from.map(key), to.map(key));
        let start = model
            .iter()
            .filter(|&k| !(lo.as_ref(), Bound::Unbounded).contains(k))
            .count();
        let end = model
            .iter()
            .filter(|&k| (Bound::Unbounded, hi.as_ref()).contains(k))
            .count();
        let end = end.max(start);
        assert_eq!(tree.rank_range(from, to), (start, end), "{lo:?} to {hi:?}");
        let inside: Vec<_> = model
            .iter()
            .filter(|&k| (lo.as_ref(), hi.as_ref()).contains(k))
            .map(|(score, member)| (score.0, member.clone()))
            .collect();
        assert_eq!(inside.len(), end - start, "{lo:?} to {hi:?}");
        let forward: Vec<_> = tree.range_by_rank(start, end).map(owned).collect();
        assert_eq!(forward, inside);
        let back = tree.range_by_rank(start, end).rev().map(owned);
        assert!(back.eq(inside.iter().rev().cloned()));
    }

    fn entries(tree: &Tree, start: usize, stop: usize) -> Vec<(f64, Vec<u8>)> {
        let range = tree.range_by_rank(start, stop.saturating_add(1));
        range.map(owned).collect()
    }

    fn owned<R: Member>((score, r): (f64, &R)) -> (f64, Vec<u8>) {
        (score, r.member().to_vec())
    }

    fn model_range(model: &Model, start: usize, stop: usize) -> Vec<(f64, Vec<u8>)> {
        let take = stop.saturating_add(1).saturating_sub(start);
        let range = model.iter().skip(start).take(take);
        range
            .map(|(score, member)| (score.0, member.clone()))
            .collect()
    }

    /// Takes rank `at` out of both, checking the tree gives the model's entry.
    fn take_nth(tree: &mut Tree, model: &mut Model, at: usize) {
        let want = model.iter().nth(at).cloned();
        let got = tree
            .remove_at(at)
            .map(|(s, r)| (OrderedFloat(s), r.to_vec()));
        assert_eq!(got, want, "rank {at}");
        if let Some(key) = want {
            model.remove(&key);
        }
    }

    /// A key as the model tests draw it: ties, prefixes and long members.
    fn draw(next: &mut impl FnMut(usize) -> usize) -> (OrderedFloat<f64>, Vec<u8>) {
        let tied = [f64::NEG_INFINITY, -2.5, -0.0, 0.0, 1.0, f64::INFINITY];
        let score = if next(3) == 0 {
            tied[next(tied.len())]
        } else {
            next(2_000) as f64 - 1_000.5
        };
        let member = match next(200) {
            0 => vec![next(256) as u8; LONG + next(100)],
            1..=100 => (0..next(3)).map(|_| [0, b'a', 0xff][next(3)]).collect(),
            _ => (0..next(16)).map(|_| next(256) as u8).collect(),
        };
        (OrderedFloat(score + 0.0), member)
    }

    /// The tree holds the model's entries both ways, each at its rank, and checks out.
    fn agrees(tree: &Tree, model: &Model) {
        assert_eq!(tree.len(), model.len());
        let mut all = model_range(model, 0, usize::MAX);
        assert_eq!(tree.iter().map(owned).collect::<Vec<_>>(), all);
        for (at, (score, member)) in all.iter().enumerate() {
            assert_eq!(tree.rank(*score, member), Ok(at));
            assert_eq!(tree.nth(at).map(owned).as_ref(), Some(&all[at]));
        }
        all.reverse();
        assert_eq!(tree.iter().rev().map(owned).collect::<Vec<_>>(), all);
        tree.check();
    }

    /// Ranks 0..n in order: full leaves, but for the last two.
    fn in_order(n: usize) -> (Tree, Vec<(f64, Vec<u8>)>) {
        let mut tree = Tree::default();
        for i in 0..n {
            assert!(add(&mut tree, i as f64, &i.to_be_bytes()));
        }
        let all = entries(&tree, 0, usize::MAX);
        assert_eq!(all.len(), n);
        (tree, all)
    }

    /// Entries of the root's leaves, or children of its internal nodes.
    fn fills(tree: &Tree) -> Vec<usize> {
        let Some(root) = tree.root.as_deref() else {
            return Vec::new();
        };
        let fill = |child| match child {
            Child::Leaf(leaf) => leaf.len(),
            Child::Inner(inner) => inner.keys.len(),
        };
        (0..root.keys.len())
            .filter_map(|c| root.child(c))
            .map(fill)
            .collect()
    }

    #[test]
    fn scores_order_entries_first_then_member_bytes() {
        let mut tree = Tree::default();
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
        assert_eq!(rank(&tree, f64::NAN, b"nan"), Some(11));
        tree.check();

        // -0 and 0 are one score.
        assert!(!add(&mut tree, 0.0, b"zero"));
        assert_eq!(rank(&tree, -0.0, b"zero"), Some(2));
        assert!(!add(&mut tree, 1.5, b"a"), "already present");
        assert_eq!(tree.len(), 12, "nothing stored twice");
        let back = tree.insert(1.5, b"a"[..].into());
        assert_eq!(
            back.err().as_deref(),
            Some(&b"a"[..]),
            "the reference comes back"
        );

        assert_eq!(rank(&tree, 1.5, b"c"), None);
        assert_eq!(tree.rank(1.5, b"c"), Err(7), "after b, before \u{e9}");
        assert_eq!(rank(&tree, 2.0, b"a"), None, "the member has another score");
        assert!(!take(&mut tree, 2.0, b"a"));
        assert!(take(&mut tree, 1.5, b"a"));
        assert!(!take(&mut tree, 1.5, b"a"));
        assert_eq!(rank(&tree, 1.5, b"b"), Some(5));
        assert!(take(&mut tree, -0.0, b"zero"));
        assert_eq!(rank(&tree, 1.5, b"b"), Some(4));
        assert!(remove(&mut tree, f64::NAN, b"nan"));
        assert!(!remove(&mut tree, f64::NAN, b"nan"));
        assert_eq!(tree.len(), 9);
        tree.check();
    }

    #[test]
    fn empty_and_single_entry_trees() {
        let mut tree = Tree::default();
        assert_eq!((tree.len(), tree.heap_bytes()), (0, 0));
        assert_eq!(rank(&tree, 1.0, b"a"), None);
        assert_eq!(tree.key_rank(1.0, b"a", true), 0);
        assert!(!take(&mut tree, 1.0, b"a"));
        assert!(!remove(&mut tree, 1.0, b"a"));
        assert_eq!(tree.remove_at(0), None);
        assert_eq!(tree.nth(0).map(|(score, _)| score), None);
        assert!(entries(&tree, 0, usize::MAX).is_empty());
        assert_eq!(tree.range_by_rank(0, 5).next_back(), None);
        let mut it = tree.iter();
        assert_eq!(
            (it.len(), it.next(), it.nth(2), it.nth_back(3)),
            (0, None, None, None)
        );
        assert_eq!((tree.iter().count(), tree.iter().last()), (0, None));
        tree.check();

        assert!(add(&mut tree, 1.0, b"only"));
        assert_eq!((tree.len(), rank(&tree, 1.0, b"only")), (1, Some(0)));
        let only = vec![(1.0, b"only".to_vec())];
        assert_eq!(entries(&tree, 0, 0), only);
        assert_eq!(entries(&tree, 0, usize::MAX), only);
        assert!(entries(&tree, 1, 5).is_empty());
        assert!(entries(&tree, 1, 0).is_empty(), "start past stop");
        assert_eq!(tree.range_by_rank(3, 1).len(), 0, "start past end");
        // Either end gives the one entry, and then neither does.
        for back_first in [false, true] {
            let mut it = tree.iter();
            assert_eq!(it.len(), 1);
            let got = if back_first {
                it.next_back()
            } else {
                it.next()
            };
            assert_eq!(got.map(owned), Some(only[0].clone()));
            assert_eq!((it.len(), it.next(), it.next_back()), (0, None, None));
        }
        // Skipping none gives it; skipping it gives nothing.
        for (n, want) in [(0, Some(only[0].clone())), (1, None)] {
            assert_eq!(tree.iter().nth(n).map(owned), want);
            assert_eq!(tree.iter().nth_back(n).map(owned), want);
        }
        assert_eq!(tree.iter().last().map(owned), Some(only[0].clone()));
        assert_eq!(tree.key_rank(0.5, b"z", false), 0);
        assert_eq!(tree.key_rank(1.0, b"only", false), 0);
        assert_eq!(tree.key_rank(1.0, b"only", true), 1);
        assert_eq!(tree.key_rank(2.0, b"", false), 1);
        assert_eq!(tree.remove_at(1), None, "past the end");
        assert!(take(&mut tree, 1.0, b"only"));
        assert_eq!((tree.len(), tree.heap_bytes()), (0, 0));
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
            let rank_at = |i: usize| if run == 1 { N - 1 - i } else { i };
            let mut tree = Tree::default();
            for i in 0..N {
                let (score, member) = key(i);
                assert!(add(&mut tree, score, &member));
            }
            tree.check();
            for i in (0..N).step_by(97) {
                let (score, member) = key(i);
                assert_eq!(rank(&tree, score, &member), Some(rank_at(i)));
            }
            for start in [0, N / 2 - 5, N - 10] {
                let got = entries(&tree, start, start + 9);
                let want: Vec<_> = (start..start + 10)
                    .map(|r| key(if run == 1 { N - 1 - r } else { r }))
                    .collect();
                assert_eq!(got, want, "run {run}, ranks from {start}");
            }
            // Appends fill each leaf they pass; descending splits leave halves.
            let kept = if run == 1 { MIN } else { CAP };
            assert_eq!(tree.census.leaves, 2 + (N - 33) / kept, "run {run}");
            // First half removed in insertion order, the rest in reverse.
            for i in (0..N / 2).chain((N / 2..N).rev()) {
                let (score, member) = key(i);
                assert!(take(&mut tree, score, &member));
                if i % 250 == 0 {
                    tree.check();
                }
            }
            assert_eq!(tree.len(), 0);
            assert!(tree.root.is_none() && tree.heap_bytes() == 0);
            tree.check();
        }
    }

    /// Random inserts, with ties and long members, checked against a BTreeSet.
    #[test]
    fn random_inserts_agree_with_a_sorted_model() {
        let mut next = xorshift(0x9e37_79b9_7f4a_7c15);
        let mut tree = Tree::default();
        let mut model = Model::new();
        for step in 0..60_000 {
            let key = draw(&mut next);
            let len = model.len();
            match next(100) {
                r if r < 60 => {
                    let added = add(&mut tree, key.0.0, &key.1);
                    assert_eq!(added, model.insert(key));
                }
                r if r < 90 => {
                    // A key held, or the one drawn, present or not.
                    let key = if r < 75 && len > 0 {
                        model.iter().nth(next(len)).cloned().unwrap()
                    } else {
                        key
                    };
                    let below = model.range(..&key).count();
                    let want = if model.contains(&key) {
                        Ok(below)
                    } else {
                        Err(below)
                    };
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
        // Random order leaves leaves about ln 2 full.
        let fill = tree.len() as f64 / (tree.census.leaves * CAP) as f64;
        assert!((0.6..0.8).contains(&fill), "fill {fill}");
    }

    /// Random operations, with ties and score changes, checked against a BTreeSet.
    #[test]
    fn random_operations_agree_with_a_sorted_model() {
        let mut next = xorshift(0x9e37_79b9_7f4a_7c15);
        let mut tree = Tree::default();
        let mut model = Model::new();
        for step in 0..60_000 {
            // Grow, shrink, grow: splits and merges on every level.
            let inserts = if (step / 20_000) % 2 == 0 { 60 } else { 10 };
            let key = draw(&mut next);
            let len = model.len();
            match next(100) {
                r if r < inserts => {
                    let added = add(&mut tree, key.0.0, &key.1);
                    assert_eq!(added, model.insert(key));
                }
                r if r < 85 && len > 0 => {
                    // A score change, as the set makes it: the same reference moves.
                    let (old, member) = model.iter().nth(next(len)).cloned().unwrap();
                    let new = key.0;
                    if !model.contains(&(new, member.clone())) {
                        let rank = tree.rank(old.0, &member).unwrap();
                        let (held, r) = tree.remove_at(rank).unwrap();
                        assert_eq!((held.to_bits(), &r[..]), (old.0.to_bits(), &member[..]));
                        assert!(tree.insert(new.0, r).is_ok());
                        model.remove(&(old, member.clone()));
                        model.insert((new, member));
                    }
                }
                r if r < 90 => {
                    assert_eq!(take(&mut tree, key.0.0, &key.1), model.remove(&key));
                }
                r if r < 96 => {
                    let key = match len {
                        0 => key,
                        _ => model.iter().nth(next(len)).cloned().unwrap(),
                    };
                    let want = model.contains(&key).then(|| model.range(..&key).count());
                    assert_eq!(rank(&tree, key.0.0, &key.1), want);
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
        assert_eq!((tree.len(), tree.heap_bytes()), (0, 0));
        tree.check();
    }

    /// Bounds by key, present or not, and walks from both ends, skips included.
    #[test]
    fn ranks_of_keys_and_two_ended_walks_agree_with_a_model() {
        let mut next = xorshift(0x2545_f491_4f6c_dd1d);
        let mut tree = Tree::default();
        let mut model = Model::new();
        for size in [0usize, 1, 31, 32, 33, 500, 5_000] {
            while model.len() < size {
                let key = (
                    OrderedFloat(next(50) as f64),
                    vec![next(256) as u8; 1 + next(3)],
                );
                if model.insert(key.clone()) {
                    assert!(tree.insert(key.0.0, key.1.into()).is_ok());
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
                assert_eq!(tree.key_rank(probe.0.0, &probe.1, false), below);
                assert_eq!(tree.key_rank(probe.0.0, &probe.1, true), at_most);
            }
            for _ in 0..50 {
                let start = next(size + 2);
                let end = start + next(size + 2);
                let mut front = start.min(size);
                let mut back = end.min(size);
                let mut it = tree.range_by_rank(start, end);
                assert_eq!(it.len(), back - front);
                while front < back {
                    // Skips either side of a leaf.
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
                    assert_eq!(got.map(owned), want);
                    assert_eq!(it.len(), back - front);
                }
                assert_eq!(it.next(), None);
                assert_eq!(it.next_back(), None);
            }
            // Counting and the last entry need no walk.
            assert_eq!(tree.range_by_rank(3, size).count(), size.saturating_sub(3));
            let last = tree.range_by_rank(0, size).last().map(owned);
            assert_eq!(last.as_ref(), all.last());
            assert_eq!(tree.nth(size).map(|(score, _)| score), None);
        }
    }

    /// One score for 5,000 members: every separator is tied, so members route.
    #[test]
    fn five_thousand_equal_scores_route_by_member() {
        let mut next = xorshift(0xd1b5_4a32_d192_ed03);
        let mut tree = Tree::default();
        let mut model = Model::new();
        while model.len() < 5_000 {
            // Few byte values: members are often prefixes of each other.
            let member: Vec<u8> = (0..next(9)).map(|_| [0, 1, b'a', 0xff][next(4)]).collect();
            let added = add(&mut tree, 3.0, &member);
            assert_eq!(added, model.insert((OrderedFloat(3.0), member)));
        }
        assert!(tree.census.inners > 1, "more than one internal level");
        agrees(&tree, &model);
        // Absent members between them, and scores either side.
        for _ in 0..2_000 {
            let score = OrderedFloat([2.0, 3.0, 4.0][next(3)]);
            let member: Vec<u8> = (0..next(10)).map(|_| [0, 1, b'a', b'b'][next(4)]).collect();
            let probe = (score, member);
            let below = model.range(..&probe).count();
            let want = if model.contains(&probe) {
                Ok(below)
            } else {
                Err(below)
            };
            assert_eq!(tree.rank(score.0, &probe.1), want);
        }
        // Walks from random ranks, as a ZRANGE within one score makes them.
        let all = model_range(&model, 0, usize::MAX);
        for _ in 0..200 {
            let start = next(all.len());
            let end = start + next(200);
            let want = &all[start..end.min(all.len())];
            let forward: Vec<_> = tree.range_by_rank(start, end).map(owned).collect();
            assert_eq!(forward, want);
            let back = tree.range_by_rank(start, end).rev().map(owned);
            assert!(back.eq(want.iter().rev().cloned()));
        }
    }

    /// From every rank to the end, and from the start to every rank, backwards.
    #[test]
    fn walks_from_every_rank_both_ways() {
        let mut next = xorshift(0x6a09_e667_f3bc_c908);
        let (ordered, ordered_all) = in_order(600);
        let mut random = Tree::default();
        let mut model = Model::new();
        while model.len() < 600 {
            let key = (
                OrderedFloat(next(40) as f64),
                vec![next(256) as u8; next(3)],
            );
            assert_eq!(add(&mut random, key.0.0, &key.1), model.insert(key));
        }
        random.check();
        let random_all = model_range(&model, 0, usize::MAX);
        for (tree, all) in [(&ordered, &ordered_all), (&random, &random_all)] {
            let n = all.len();
            for at in 0..=n {
                let forward = tree.range_by_rank(at, n).map(owned);
                assert!(forward.eq(all[at..].iter().cloned()), "from {at}");
                let back = tree.range_by_rank(0, at).rev().map(owned);
                assert!(back.eq(all[..at].iter().rev().cloned()), "back from {at}");
                // A short window either way, across a leaf now and then.
                let window = &all[at..(at + 20).min(n)];
                let forward = tree.range_by_rank(at, at + 20).map(owned);
                assert!(forward.eq(window.iter().cloned()));
                let back = tree.range_by_rank(at, at + 20).rev().map(owned);
                assert!(back.eq(window.iter().rev().cloned()));
            }
        }
    }

    /// Ends taken in turn meet without repeating or skipping an entry.
    #[test]
    fn alternating_ends_meet_without_gaps() {
        let (tree, all) = in_order(1_000);
        let patterns: [&[bool]; 4] = [
            &[false, true],
            &[false, false, true],
            &[true, true, true, false],
            &[true],
        ];
        for (start, end) in [
            (0, 1_000),
            (0, 1),
            (0, 2),
            (15, 17),
            (16, 48),
            (7, 993),
            (500, 501),
        ] {
            for pattern in patterns {
                let mut it = tree.range_by_rank(start, end);
                let (mut front, mut back) = (Vec::new(), Vec::new());
                for &from_back in pattern.iter().cycle() {
                    let got = if from_back { it.next_back() } else { it.next() };
                    let Some(got) = got else { break };
                    if from_back {
                        back.push(owned(got));
                    } else {
                        front.push(owned(got));
                    }
                    assert_eq!(it.len(), end - start - front.len() - back.len());
                }
                assert_eq!((it.next(), it.next_back()), (None, None));
                back.reverse();
                front.append(&mut back);
                assert_eq!(front, all[start..end], "{start}..{end}, {pattern:?}");
            }
        }
    }

    /// Steps and skips from both ends, as a slice's iterator takes them.
    #[test]
    fn nth_and_nth_back_agree_with_a_slice_iterator() {
        let mut next = xorshift(0xbb67_ae85_84ca_a73b);
        let (ordered, ordered_all) = in_order(2_000);
        let mut random = Tree::default();
        let mut model = Model::new();
        while model.len() < 2_000 {
            let key = (OrderedFloat(next(100) as f64), vec![next(256) as u8]);
            assert_eq!(add(&mut random, key.0.0, &key.1), model.insert(key));
        }
        random.check();
        let random_all = model_range(&model, 0, usize::MAX);
        // In order, leaves end every 16 ranks: these skips land on each side.
        let skips = [0, 1, 14, 15, 16, 17, 31, 32, 33, 100, 1_000, 5_000];
        for (tree, all) in [(&ordered, &ordered_all), (&random, &random_all)] {
            for _ in 0..300 {
                let start = next(all.len() + 1);
                let end = start + next(all.len() + 1 - start);
                let mut it = tree.range_by_rank(start, end);
                let mut want = all[start..end].iter();
                loop {
                    let skip = skips[next(skips.len())];
                    let (got, expected) = match next(4) {
                        0 => (it.next(), want.next()),
                        1 => (it.next_back(), want.next_back()),
                        2 => (it.nth(skip), want.nth(skip)),
                        _ => (it.nth_back(skip), want.nth_back(skip)),
                    };
                    assert_eq!(got.map(owned).as_ref(), expected);
                    assert_eq!(it.len(), want.len());
                    if want.len() == 0 && next(4) == 0 {
                        break;
                    }
                }
                assert_eq!((it.next(), it.next_back(), it.nth(1)), (None, None, None));
            }
        }
    }

    /// An entry reference not Clone, with its member in the reference itself.
    #[derive(Debug, PartialEq)]
    struct Id([u8; 8]);

    thread_local! {
        /// Members read through an `Id`, to show what a bound reads.
        static READS: Cell<usize> = const { Cell::new(0) };
    }

    impl Member for Id {
        fn member(&self) -> &[u8] {
            READS.with(|reads| reads.set(reads.get() + 1));
            &self.0
        }
    }

    /// The iterator is a few words, borrows the references and allocates nothing.
    #[test]
    fn iteration_allocates_nothing_and_lends_the_references() {
        assert_eq!(size_of::<Iter<'_, NonNull<u8>>>(), 56);
        let Some(before) = crate::memory_use::allocated_by_this_thread() else {
            return;
        };
        let allocated = || crate::memory_use::allocated_by_this_thread().unwrap() - before;
        let mut next = xorshift(0x3c6e_f372_fe94_f82b);
        let mut tree = CountedTree::default();
        for i in 0..20_000u64 {
            let id = Id((next(1 << 20) as u64 * 20_000 + i).to_be_bytes());
            assert!(tree.insert(next(500) as f64, id).is_ok());
        }
        tree.check();
        // References only: no member allocates, so the nodes are all there is.
        assert_eq!(allocated(), tree.heap_bytes() as isize);
        let held = allocated();
        let mut it = tree.iter();
        loop {
            let got = if next(2) == 0 {
                it.next()
            } else {
                it.nth(next(40))
            };
            let Some((score, r)) = got else { break };
            let at = tree.len() - it.len() - 1;
            let (want_score, want) = tree.nth(at).unwrap();
            // The very reference the tree holds, not a copy.
            assert!(score == want_score && std::ptr::eq(r, want), "rank {at}");
            assert_eq!(allocated(), held, "a step from the front allocates nothing");
        }
        let mut it = tree.range_by_rank(17, 19_000);
        loop {
            let got = if next(2) == 0 {
                it.next_back()
            } else {
                it.nth_back(next(40))
            };
            let Some((score, r)) = got else { break };
            let at = 17 + it.len();
            let (want_score, want) = tree.nth(at).unwrap();
            assert!(score == want_score && std::ptr::eq(r, want), "rank {at}");
            assert_eq!(allocated(), held, "a step from the back allocates nothing");
        }
        while tree.remove_at(0).is_some() {}
        assert_eq!((tree.heap_bytes(), allocated()), (0, 0), "every node freed");
    }

    /// The same members, taken out in random order by reference and by rank.
    #[test]
    fn equal_scores_are_removed_by_member() {
        let mut next = xorshift(0x5851_f42d_4c95_7f2d);
        let mut tree = Tree::default();
        let mut model = Model::new();
        while model.len() < 5_000 {
            let member: Vec<u8> = (0..next(9)).map(|_| [0, 1, b'a', 0xff][next(4)]).collect();
            let added = add(&mut tree, 3.0, &member);
            assert_eq!(added, model.insert((OrderedFloat(3.0), member)));
        }
        agrees(&tree, &model);
        while let Some(key) = model.iter().nth(next(model.len().max(1))).cloned() {
            if next(2) == 0 {
                assert!(remove(&mut tree, key.0.0, &key.1));
            } else {
                let at = tree.rank(key.0.0, &key.1).unwrap();
                let taken = tree.remove_at(at).map(|(_, r)| r.to_vec());
                assert_eq!(taken.as_ref(), Some(&key.1));
            }
            model.remove(&key);
            assert_eq!(rank(&tree, key.0.0, &key.1), None);
            tree.check();
            if model.len().is_multiple_of(500) {
                agrees(&tree, &model);
            }
        }
        assert_eq!((tree.len(), tree.heap_bytes()), (0, 0));
    }

    /// The first, last and middle entries, by rank and by reference.
    #[test]
    fn removes_first_last_and_middle_entries() {
        let mut tree = Tree::default();
        let mut model = Model::new();
        for i in 0..200 {
            let key = (OrderedFloat(i as f64), format!("m{i:03}").into_bytes());
            assert!(add(&mut tree, key.0.0, &key.1));
            model.insert(key);
        }
        for round in 0..30 {
            let len = model.len();
            let at = [0, len - 1, len / 2][round % 3];
            if round % 2 == 0 {
                take_nth(&mut tree, &mut model, at);
            } else {
                let (score, member) = model.iter().nth(at).cloned().unwrap();
                let r = tree.remove(score.0, &Box::from(&member[..]));
                assert_eq!(r.as_deref(), Some(&member[..]), "round {round}");
                model.remove(&(score, member));
            }
            agrees(&tree, &model);
        }
        assert_eq!(tree.remove_at(model.len()), None, "past the end");
        assert!(!remove(&mut tree, 1_000.0, b"m000"), "absent");
        assert!(
            !remove(&mut tree, 51.0, b"m050"),
            "present with another score"
        );
        agrees(&tree, &model);
    }

    /// The root's only leaf may hold fewer than MIN, down to none.
    #[test]
    fn removing_from_a_single_leaf() {
        let (leaf, inner) = sizes();
        let mut tree = Tree::default();
        let mut model = Model::new();
        for i in 0..20 {
            let key = (OrderedFloat(f64::from(i)), vec![b'm', i]);
            assert!(add(&mut tree, key.0.0, &key.1));
            model.insert(key);
        }
        assert_eq!(fills(&tree), [20]);
        for at in [0, 18, 9, 0, 15] {
            take_nth(&mut tree, &mut model, at);
            agrees(&tree, &model);
            assert_eq!(tree.heap_bytes(), leaf + inner);
        }
        assert_eq!(fills(&tree), [15]);
        while let Some((score, r)) = tree.remove_at(0) {
            assert!(model.remove(&(OrderedFloat(score), r.to_vec())));
            tree.check();
        }
        assert!(model.is_empty() && tree.root.is_none());
        assert_eq!((tree.len(), tree.heap_bytes()), (0, 0));
        assert!(add(&mut tree, 1.0, b"again"));
        assert_eq!(tree.heap_bytes(), leaf + inner);
        tree.check();
    }

    /// A short leaf borrows from a sibling above MIN, else merges with one.
    #[test]
    fn leaves_borrow_then_merge() {
        let (leaf, inner) = sizes();
        for run in 0..4 {
            // Odd runs descend; the later two share one score.
            let descending = run % 2 == 1;
            let key = |i: u8| {
                let i = if descending { 99 - i } else { i };
                let score = if run < 2 { f64::from(i) } else { 7.0 };
                (OrderedFloat(score), vec![b'm', i])
            };
            let mut tree = Tree::default();
            let mut model = Model::new();
            for i in 0..33 {
                let key = key(i);
                assert!(add(&mut tree, key.0.0, &key.1));
                model.insert(key);
            }
            // The leaf the inserts go into keeps the extra entry.
            let split: [usize; 2] = if descending { [17, 16] } else { [16, 17] };
            assert_eq!(fills(&tree), split, "run {run}");
            // From the leaf of 16: a borrow, then a merge.
            for want in [vec![16usize, 16], vec![31]] {
                let at = if descending { model.len() - 1 } else { 0 };
                take_nth(&mut tree, &mut model, at);
                assert_eq!(fills(&tree), want, "run {run}");
                agrees(&tree, &model);
            }
            assert_eq!(tree.heap_bytes(), leaf + inner, "the merged leaf freed");
        }
    }

    /// Internal nodes rotate a child from a sibling, then merge; the root collapses.
    #[test]
    fn internal_nodes_borrow_merge_and_the_root_collapses() {
        let (leaf, inner) = sizes();
        for run in 0..4 {
            let descending = run % 2 == 1;
            let key = |i: usize| {
                let i = if descending { 9_999 - i } else { i };
                let score = if run < 2 { i as f64 } else { 7.0 };
                (OrderedFloat(score), format!("m{i:04}").into_bytes())
            };
            let mut tree = Tree::default();
            let mut model = Model::new();
            // One past a full root of half-full leaves: the root splits.
            // The largest key goes first, so no insert appends.
            let largest = if descending { 0 } else { 528 };
            for i in std::iter::once(largest).chain((0..529).filter(|&i| i != largest)) {
                let key = key(i);
                assert!(add(&mut tree, key.0.0, &key.1));
                model.insert(key);
            }
            let split: Vec<usize> = if descending {
                vec![17, 16]
            } else {
                vec![16, 17]
            };
            assert_eq!((fills(&tree), tree.census.inners), (split, 3));
            let mut removed = 0;
            while tree.census.inners > 1 {
                let at = if descending { model.len() - 1 } else { 0 };
                take_nth(&mut tree, &mut model, at);
                removed += 1;
                tree.check();
                if removed == 1 {
                    // A leaf merge left one side 15: it took a child across.
                    assert_eq!((fills(&tree), tree.census.inners), (vec![16, 16], 3));
                }
            }
            // Then 16 more emptied the first leaf, and both halves merged.
            assert_eq!(removed, 17, "run {run}");
            assert_eq!(fills(&tree).len(), 31, "the root is the merged node");
            assert_eq!(tree.heap_bytes(), tree.census.leaves * leaf + inner);
            agrees(&tree, &model);
        }
    }

    /// Only the reference held is taken: equal bytes are not that reference.
    #[test]
    fn removal_takes_only_the_reference_held() {
        let tagged = |i: u32| Tagged(i.to_be_bytes().into(), i);
        let mut tree = CountedTree::default();
        for i in 0..100 {
            assert!(tree.insert(f64::from(i % 7), tagged(i)).is_ok());
        }
        tree.check();
        let other = Tagged(5u32.to_be_bytes().into(), 99);
        assert_eq!(
            tree.remove(5.0, &other),
            None,
            "same bytes, another reference"
        );
        assert_eq!(tree.remove(4.0, &tagged(5)), None, "another score");
        assert_eq!(tree.len(), 100);
        assert_eq!(tree.remove(5.0, &tagged(5)), Some(tagged(5)));
        assert_eq!(tree.len(), 99);
        assert!(tree.rank(5.0, &5u32.to_be_bytes()).is_err());
        tree.check();
    }

    /// Taking every entry out frees every node; memory never grows meanwhile.
    #[test]
    fn removing_every_entry_frees_every_node() {
        let mut next = xorshift(0x3c6e_f372_fe94_f82b);
        let mut tree = Tree::default();
        let mut model = Model::new();
        while model.len() < 3_000 {
            let member = (0..8).map(|_| next(256) as u8).collect::<Vec<u8>>();
            let key = (OrderedFloat(next(1_000) as f64), member);
            if model.insert(key.clone()) {
                assert!(add(&mut tree, key.0.0, &key.1));
            }
        }
        let mut doomed: Vec<_> = model.iter().cloned().collect();
        for i in (1..doomed.len()).rev() {
            doomed.swap(i, next(i + 1));
        }
        let mut heap = tree.heap_bytes();
        for key in &doomed {
            assert!(remove(&mut tree, key.0.0, &key.1));
            assert!(tree.heap_bytes() <= heap, "a removal never allocates");
            heap = tree.heap_bytes();
            tree.check();
        }
        assert!(tree.root.is_none());
        assert_eq!((tree.len(), tree.heap_bytes()), (0, 0));
    }

    /// Grow and shrink over a few thousand keys, checking after every change.
    #[test]
    fn every_mutation_keeps_every_invariant() {
        let mut next = xorshift(0x0123_4567_89ab_cdef);
        let mut tree = Tree::default();
        let mut model = Model::new();
        for step in 0..20_000 {
            let growing = (step / 2_500) % 2 == 0;
            let key = (
                OrderedFloat(next(300) as f64),
                vec![[0, b'a', 0xff][next(3)]; next(4)],
            );
            let len = model.len();
            match next(100) {
                r if r < if growing { 75 } else { 25 } => {
                    let added = add(&mut tree, key.0.0, &key.1);
                    assert_eq!(added, model.insert(key));
                }
                r if r < 90 => {
                    // A key held, or the one drawn, by reference.
                    let key = if r % 2 == 0 && len > 0 {
                        model.iter().nth(next(len)).cloned().unwrap()
                    } else {
                        key
                    };
                    assert_eq!(remove(&mut tree, key.0.0, &key.1), model.remove(&key));
                }
                _ => take_nth(&mut tree, &mut model, next(len + 1)),
            }
            tree.check();
            assert_eq!(tree.len(), model.len());
            if step % 1_000 == 999 {
                agrees(&tree, &model);
            }
        }
    }

    /// Sizes with an 8-byte reference, as the table's entry reference is.
    #[test]
    fn node_sizes_with_an_eight_byte_reference() {
        type Ref = NonNull<u8>;
        assert_eq!(size_of::<Option<Ref>>(), 8);
        assert_eq!(size_of::<Leaf<Ref>>(), 520);
        assert_eq!(size_of::<Inner<Ref>>(), 776);
    }

    /// Heap bytes are the nodes allocated: one root, then splits.
    #[test]
    fn heap_bytes_count_every_node() {
        let (leaf, inner) = sizes();
        let mut tree = Tree::default();
        let add_next = |tree: &mut Tree| {
            let i = tree.len();
            assert!(add(tree, i as f64, &i.to_be_bytes()));
        };
        add_next(&mut tree);
        assert_eq!(tree.heap_bytes(), leaf + inner, "a root over one leaf");
        while tree.len() < CAP + 1 {
            add_next(&mut tree);
        }
        assert_eq!(tree.heap_bytes(), 2 * leaf + inner, "the first leaf split");
        // In order, the root fills with 32 full leaves at 1,024 entries.
        while tree.len() < CAP * CAP {
            add_next(&mut tree);
        }
        assert_eq!(tree.heap_bytes(), CAP * leaf + inner);
        add_next(&mut tree);
        assert_eq!(tree.heap_bytes(), 33 * leaf + 3 * inner, "the root split");
        tree.check();
    }

    /// Bounds of nothing, and of one entry: below, at and above it.
    #[test]
    fn bounds_of_empty_and_single_entry_trees() {
        let probes: [(f64, Vec<u8>); 8] = [
            (0.5, b"".to_vec()),
            (1.0, b"".to_vec()),
            (1.0, b"\0".to_vec()),
            (1.0, b"a".to_vec()),
            (1.0, b"a\0".to_vec()),
            (1.0, b"a\0\0".to_vec()),
            (1.0, b"b".to_vec()),
            (2.0, b"".to_vec()),
        ];
        let mut tree = Tree::default();
        let mut model = Model::new();
        for held in [false, true] {
            if held {
                assert!(add(&mut tree, 1.0, b"a\0"));
                model.insert((OrderedFloat(1.0), b"a\0".to_vec()));
            }
            for (score, member) in &probes {
                bounds_agree(&tree, &model, *score, member);
            }
            for from in (0..5).flat_map(|kind| probes.iter().map(move |p| bound(kind, p))) {
                for to in (0..5).flat_map(|kind| probes.iter().map(move |p| bound(kind, p))) {
                    range_agrees(&tree, &model, from, to);
                }
            }
        }
        assert_eq!(
            (tree.score_rank(1.0, false), tree.score_rank(1.0, true)),
            (0, 1)
        );
        assert_eq!(tree.key_rank(1.0, b"a", true), 0, "a prefix sorts first");
        assert_eq!(
            tree.key_rank(1.0, b"a\0\0", false),
            1,
            "a longer member after"
        );
        let only: (f64, &[u8]) = (1.0, b"a\0");
        let (inc, exc) = (Bound::Included(only), Bound::Excluded(only));
        assert_eq!(tree.rank_range(inc, inc), (0, 1));
        assert_eq!(tree.rank_range(exc, inc), (1, 1));
        assert_eq!(tree.rank_range(inc, exc), (0, 0));
        let (two, zero) = (
            Bound::Included((2.0, &b""[..])),
            Bound::Excluded((0.0, &b""[..])),
        );
        assert_eq!(tree.rank_range(two, zero), (1, 1), "crossed bounds");
    }

    /// Bounds by key and by score, and ranges both ways, against a model.
    #[test]
    fn key_and_score_bounds_agree_with_a_model() {
        let mut next = xorshift(0x510e_527f_ade6_82d1);
        // Distinct scores: one run, runs across leaves, then mostly unique.
        for scores in [1, 4, 50, 1 << 20] {
            for size in [0usize, 1, 2, 31, 32, 33, 600, 3_000] {
                let mut tree = Tree::default();
                let mut model = Model::new();
                while model.len() < size {
                    let key = (OrderedFloat(next(scores) as f64), binary(&mut next));
                    assert_eq!(add(&mut tree, key.0.0, &key.1), model.insert(key));
                }
                tree.check();
                // Ends and absent scores, then keys held and their neighbours.
                let mut probes: Vec<(f64, Vec<u8>)> = vec![
                    (f64::NEG_INFINITY, Vec::new()),
                    (-0.0, Vec::new()),
                    (-0.5, vec![0]),
                    (f64::INFINITY, vec![0xff; 8]),
                    (f64::NAN, Vec::new()),
                ];
                let all: Vec<_> = model.iter().cloned().collect();
                let sample = if size <= 600 { size } else { 100 };
                for i in 0..sample {
                    let at = if size <= 600 { i } else { next(size) };
                    let (score, member) = all[at].clone();
                    let mut longer = member.clone();
                    longer.push(0);
                    let shorter = member[..member.len().saturating_sub(1)].to_vec();
                    probes.extend([
                        (score.0, Vec::new()),
                        (score.0, shorter),
                        (score.0, longer),
                        (score.0 - 0.5, member.clone()),
                        (score.0 + 0.5, member.clone()),
                        (score.0, member),
                    ]);
                }
                if let (Some(first), Some(last)) = (all.first(), all.last()) {
                    probes.extend([(first.0.0, first.1.clone()), (last.0.0, last.1.clone())]);
                }
                for (score, member) in &probes {
                    bounds_agree(&tree, &model, *score, member);
                }
                for _ in 0..100 {
                    let a = &probes[next(probes.len())];
                    let b = &probes[next(probes.len())];
                    range_agrees(&tree, &model, bound(next(5), a), bound(next(5), b));
                }
            }
        }
    }

    /// Runs of one score over many leaves: bounds by member, and at their ends.
    #[test]
    fn equal_score_runs_across_leaves_bound_by_member() {
        let mut next = xorshift(0x9b05_688c_2b3e_6c1f);
        let mut tree = Tree::default();
        let mut model = Model::new();
        for score in [1.0, 2.0, 3.0] {
            let held = model.len();
            while model.len() < held + 400 {
                let key = (OrderedFloat(score), binary(&mut next));
                assert_eq!(add(&mut tree, score, &key.1), model.insert(key));
            }
        }
        tree.check();
        let leaf_of = |rank| tree.leaf_holding(rank).map(|(leaf, _)| leaf as *const _);
        assert_ne!(leaf_of(400), leaf_of(799), "the middle run spans leaves");
        let middle = (tree.score_rank(2.0, false), tree.score_rank(2.0, true));
        assert_eq!(middle, (400, 800));
        assert_eq!(
            (tree.score_rank(1.5, false), tree.score_rank(1.5, true)),
            (400, 400)
        );
        let from = Bound::Included((2.0, &b""[..]));
        let to = Bound::Excluded((3.0, &b""[..]));
        assert_eq!(tree.rank_range(from, to), (400, 800));
        // Each member of the middle run, and its neighbours.
        let run: Vec<_> = model.iter().filter(|(s, _)| s.0 == 2.0).cloned().collect();
        for (score, member) in &run {
            let mut longer = member.clone();
            longer.push(0);
            let shorter = &member[..member.len().saturating_sub(1)];
            for probe in [&member[..], &longer, shorter] {
                bounds_agree(&tree, &model, score.0, probe);
            }
        }
        // Ranges of members within the score, as ZRANGEBYLEX takes them.
        for _ in 0..300 {
            let (a, b) = ((2.0, binary(&mut next)), (2.0, binary(&mut next)));
            range_agrees(&tree, &model, bound(next(5), &a), bound(next(5), &b));
        }
    }

    /// Bounds after random inserts and removals, each change checked.
    #[test]
    fn bounds_follow_random_inserts_and_removals() {
        let mut next = xorshift(0x1f83_d9ab_fb41_bd6b);
        let mut tree = Tree::default();
        let mut model = Model::new();
        for step in 0..8_000 {
            let inserts = if (step / 2_000) % 2 == 0 { 70 } else { 30 };
            let key = (OrderedFloat(next(60) as f64), binary(&mut next));
            let changed = if next(100) < inserts {
                let added = add(&mut tree, key.0.0, &key.1);
                assert_eq!(added, model.insert(key));
                added
            } else {
                // A key held, or the one drawn.
                let held = next(2) == 0 && !model.is_empty();
                let key = if held {
                    model.iter().nth(next(model.len())).cloned().unwrap()
                } else {
                    key
                };
                let removed = remove(&mut tree, key.0.0, &key.1);
                assert_eq!(removed, model.remove(&key));
                removed
            };
            if changed {
                tree.check();
            }
            if step % 100 == 99 {
                for _ in 0..10 {
                    let probe = (next(62) as f64 - 1.0, binary(&mut next));
                    bounds_agree(&tree, &model, probe.0, &probe.1);
                    let other = (probe.0 + next(4) as f64, binary(&mut next));
                    range_agrees(
                        &tree,
                        &model,
                        bound(next(5), &probe),
                        bound(next(5), &other),
                    );
                }
            }
        }
        assert!(model.len() > 100, "{} held", model.len());
    }

    /// A key bound reads a member a level; a score bound reads none. Neither allocates.
    #[test]
    fn bounds_read_few_members_and_allocate_nothing() {
        let mut next = xorshift(0x5be0_cd19_137e_2179);
        let mut tree = CountedTree::default();
        // One score for all: every separator tied, the most a key bound reads.
        // Probes fall between the members, which are `k * 20_000 + i`.
        let between = |k: usize| (k as u64 * 20_000).to_be_bytes();
        for i in 0..20_000u64 {
            let at = next(1 << 20) as u64 * 20_000 + i;
            assert!(tree.insert(5.0, Id(at.to_be_bytes())).is_ok());
        }
        tree.check();
        let mut levels = 1;
        let mut node = tree.root.as_deref().unwrap();
        while let Some(Child::Inner(inner)) = node.child(0) {
            node = inner;
            levels += 1;
        }
        let reads = |bound: &dyn Fn() -> usize| {
            let before = READS.with(Cell::get);
            let rank = bound();
            (rank, READS.with(Cell::get) - before)
        };
        assert_eq!(reads(&|| tree.score_rank(5.0, false)), (0, 0));
        assert_eq!(reads(&|| tree.score_rank(5.0, true)), (20_000, 0));
        assert_eq!(reads(&|| tree.key_rank(5.0, b"", false)), (0, 0));
        // A binary search a level, of at most 32: six reads each.
        let most = 6 * (levels + 1);
        for _ in 0..500 {
            let probe = between(next(1 << 20));
            let (rank, read) = reads(&|| tree.key_rank(5.0, &probe, false));
            assert!(read <= most, "{read} reads, {levels} levels");
            let around = (rank.checked_sub(1).map(|r| tree.nth(r)), tree.nth(rank));
            assert!(around.0.is_none_or(|e| e.is_some_and(|(_, r)| r.0 < probe)));
            assert!(around.1.is_none_or(|(_, r)| r.0 >= probe));
        }
        let Some(before) = crate::memory_use::allocated_by_this_thread() else {
            return;
        };
        for _ in 0..500 {
            let (a, b) = (between(next(1 << 20)), between(next(1 << 20)));
            let from = Bound::Included((5.0, &a[..]));
            let to = Bound::Excluded((5.0, &b[..]));
            let (start, end) = tree.rank_range(from, to);
            assert!(start <= end && end <= tree.len());
            assert!(tree.score_rank(next(10) as f64, next(2) == 0) <= tree.len());
        }
        let after = crate::memory_use::allocated_by_this_thread().unwrap();
        assert_eq!(after, before, "a bound allocates nothing");
    }

    /// Built whole from keys in order: the fewest nodes, and the model's answers.
    #[test]
    fn bulk_builds_agree_with_a_model() {
        let mut next = xorshift(0x6c62_272e_07bb_0142);
        let (leaf, inner) = sizes();
        let sizes = [
            0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 1_000, 10_000, 100_000,
        ];
        for size in sizes {
            // Distinct scores, one score for all, then runs of about 40.
            for scores in [1 << 40, 1, size / 40 + 1] {
                let mut model = Model::new();
                while model.len() < size {
                    // Binary bytes, then up to three of a counter's: zeros among them.
                    let mut member = binary(&mut next);
                    member.extend_from_slice(&next(1 << 24).to_be_bytes()[5 + next(4)..]);
                    model.insert((OrderedFloat(next(scores) as f64), member));
                }
                let tree = built(&model);
                let (leaves, inners) = fewest_nodes(size);
                let census = (tree.census.leaves, tree.census.inners);
                assert_eq!(census, (leaves, inners), "{size} keys");
                assert_eq!(tree.heap_bytes(), leaves * leaf + inners * inner);
                agrees(&tree, &model);
                let all: Vec<_> = model.iter().map(|(s, m)| (s.0, m.clone())).collect();
                let held =
                    |at: usize, absent: f64| all.get(at).cloned().unwrap_or((absent, Vec::new()));
                for _ in 0..40 {
                    let at = (next(size.max(1)), next(size.max(1)));
                    let (one, other) = (held(at.0, 1.0), held(at.1, 2.0));
                    bounds_agree(&tree, &model, one.0, &one.1);
                    bounds_agree(&tree, &model, one.0, &one.1[..one.1.len() / 2]);
                    let kinds = (next(5), next(5));
                    let (from, to) = (bound(kinds.0, &one), bound(kinds.1, &other));
                    range_agrees(&tree, &model, from, to);
                }
            }
        }
    }

    /// Keys out of order, or one twice, come back untouched; nothing is built.
    #[test]
    fn bulk_build_refuses_keys_out_of_order() {
        let boxed = |keys: &[(f64, &[u8])]| -> Vec<(f64, Box<[u8]>)> {
            keys.iter()
                .map(|&(score, member)| (score, member.into()))
                .collect()
        };
        let refused: [&[(f64, &[u8])]; 6] = [
            &[(1.0, b"b"), (1.0, b"a")],
            &[(2.0, b"a"), (1.0, b"b")],
            &[(1.0, b"a"), (1.0, b"a")],
            &[(-0.0, b"a"), (0.0, b"a")],
            &[(1.0, b"a\0"), (1.0, b"a")],
            &[(1.0, b"a"), (1.0, b"\0"), (2.0, b"z")],
        ];
        for keys in refused {
            let given = boxed(keys);
            let at = given.as_ptr();
            let Err(back) = Tree::from_sorted(given) else {
                panic!("{keys:?} taken");
            };
            assert_eq!((back.as_ptr(), &back), (at, &boxed(keys)), "{keys:?}");
        }
        // One pair out of order deep in an equal-score run.
        let mut keys: Vec<(f64, Box<[u8]>)> = (0..5_000u32)
            .map(|i| (f64::from(i / 700), i.to_be_bytes()[..].into()))
            .collect();
        keys.swap(4_000, 4_001);
        let want = keys.clone();
        let Err(back) = Tree::from_sorted(keys) else {
            panic!("a pair out of order taken");
        };
        assert_eq!(back, want);
        // Nothing, one, and a member at two scores: two keys, as `insert` takes them.
        let (leaf, inner) = sizes();
        for (keys, nodes) in [(boxed(&[]), 0), (boxed(&[(1.0, b"a")]), leaf + inner)] {
            let Ok(tree) = Tree::from_sorted(keys) else {
                panic!("in order refused");
            };
            assert_eq!(tree.heap_bytes(), nodes);
            tree.check();
        }
        let Ok(tree) = Tree::from_sorted(boxed(&[(1.0, b"a"), (2.0, b"a")])) else {
            panic!("in order refused");
        };
        assert_eq!(
            entries(&tree, 0, 1),
            [(1.0, b"a".to_vec()), (2.0, b"a".to_vec())]
        );
        tree.check();
    }

    /// A built tree takes inserts and removals as any other, checked after each.
    #[test]
    fn bulk_built_trees_take_inserts_and_removals() {
        let mut next = xorshift(0x8422_2325_cbf2_9ce4);
        for size in [33, 1_000, 5_000] {
            let scores = size / 30 + 2;
            let mut model = Model::new();
            while model.len() < size {
                model.insert((OrderedFloat(next(scores) as f64), binary(&mut next)));
            }
            let mut tree = built(&model);
            for step in 0..3_000 {
                let key = (OrderedFloat(next(scores) as f64), binary(&mut next));
                let changed = if next(2) == 0 {
                    let added = add(&mut tree, key.0.0, &key.1);
                    assert_eq!(added, model.insert(key));
                    added
                } else {
                    // A key held, or the one drawn.
                    let held = next(2) == 0 && !model.is_empty();
                    let key = if held {
                        model.iter().nth(next(model.len())).cloned().unwrap()
                    } else {
                        key
                    };
                    let removed = remove(&mut tree, key.0.0, &key.1);
                    assert_eq!(removed, model.remove(&key));
                    removed
                };
                if changed {
                    tree.check();
                }
                if step % 500 == 499 {
                    agrees(&tree, &model);
                }
            }
        }
    }

    /// The nodes are all a built tree allocates, as later changes add and free them.
    #[test]
    fn bulk_built_memory_is_exact_as_nodes_come_and_go() {
        let Some(start) = crate::memory_use::allocated_by_this_thread() else {
            return;
        };
        let allocated = || crate::memory_use::allocated_by_this_thread().unwrap() - start;
        let mut next = xorshift(0xcbf2_9ce4_8422_2325);
        // Even members held, 64 to a score; odd ones fall between them.
        let key = |i: u64| (f64::from((i / 128) as u32), Id(i.to_be_bytes()));
        let n = 100_000;
        let sorted: Vec<_> = (0..n).map(|i| key(2 * i)).collect();
        let Ok(mut tree) = CountedTree::from_sorted(sorted) else {
            panic!("in order refused");
        };
        // The input's buffer is gone: the nodes are all that is left.
        let (leaves, inners) = fewest_nodes(n as usize);
        let nodes = leaves * size_of::<Leaf<Id>>() + inners * size_of::<Inner<Id>>();
        assert_eq!((tree.heap_bytes(), allocated()), (nodes, nodes as isize));
        tree.check();
        for _ in 0..20_000 {
            let (score, id) = key(next(2 * n as usize) as u64);
            if tree.remove(score, &id).is_none() {
                assert!(tree.insert(score, id).is_ok());
            }
            assert_eq!(allocated(), tree.heap_bytes() as isize);
        }
        tree.check();
        while tree.remove_at(0).is_some() {}
        assert_eq!((tree.heap_bytes(), allocated()), (0, 0), "every node freed");
    }

    /// Bytes a member of a built tree, with the table's 8-byte reference.
    #[test]
    fn bulk_built_bytes_per_member_with_an_eight_byte_reference() {
        let (leaf, inner) = (size_of::<Leaf<Raw>>(), size_of::<Inner<Raw>>());
        assert_eq!((leaf, inner), (520, 776));
        for n in [1_000, 10_000, 100_000, 1_000_000] {
            let sorted = (0..n)
                .map(|i| (i as f64, Raw(NonNull::dangling())))
                .collect();
            let Ok(tree) = CountedTree::from_sorted(sorted) else {
                panic!("in order refused");
            };
            let (leaves, inners) = fewest_nodes(n);
            assert_eq!(tree.heap_bytes(), leaves * leaf + inners * inner);
            let last = tree.nth(n - 1).map(|(score, r)| (score, r.0));
            assert_eq!(last, Some(((n - 1) as f64, NonNull::dangling())));
            // 520 bytes a 32-entry leaf, and a little for the levels above.
            let per_member = tree.heap_bytes() as f64 / n as f64;
            assert!((16.25..17.5).contains(&per_member), "{n}: {per_member}");
        }
    }

    /// Fills of each level's nodes, root first, left to right.
    fn levels<R>(tree: &CountedTree<R>) -> Vec<Vec<usize>> {
        let mut levels = Vec::new();
        let mut nodes: Vec<&Inner<R>> = tree.root.as_deref().into_iter().collect();
        while !nodes.is_empty() {
            levels.push(nodes.iter().map(|node| node.keys.len()).collect());
            let (mut leaves, mut inners) = (Vec::new(), Vec::new());
            for node in nodes {
                for c in 0..node.keys.len() {
                    match node.child(c) {
                        Some(Child::Leaf(leaf)) => leaves.push(leaf.len()),
                        Some(Child::Inner(inner)) => inners.push(inner),
                        None => {}
                    }
                }
            }
            if !leaves.is_empty() {
                levels.push(leaves);
            }
            nodes = inners;
        }
        levels
    }

    /// Below the root, every node is full but the last two of its level.
    fn full_behind_the_edge<R>(tree: &CountedTree<R>) {
        for (depth, fills) in levels(tree).iter().enumerate().skip(1) {
            let behind = &fills[..fills.len().saturating_sub(2)];
            assert!(
                behind.iter().all(|&fill| fill == CAP),
                "level {depth}: {fills:?}"
            );
        }
    }

    /// Fills down the last child from the root, the leaf's last.
    fn spine<R>(tree: &CountedTree<R>) -> Vec<usize> {
        let mut fills = Vec::new();
        let mut node = tree.root.as_deref();
        while let Some(inner) = node {
            let n = inner.keys.len();
            fills.push(n);
            node = match inner.child(n - 1) {
                Some(Child::Inner(inner)) => Some(inner),
                Some(Child::Leaf(leaf)) => {
                    fills.push(leaf.len());
                    None
                }
                None => None,
            };
        }
        fills
    }

    /// Adds a key past every other: true if a node split or lent, not just grew.
    fn append(tree: &mut Tree, model: &mut Model, key: (OrderedFloat<f64>, Vec<u8>)) -> bool {
        assert!(model.last() < Some(&key), "{key:?} is past the largest");
        let census = (tree.census.leaves, tree.census.inners);
        let mut grown = spine(tree);
        if let Some(leaf) = grown.last_mut() {
            *leaf += 1;
        }
        assert!(add(tree, key.0.0, &key.1));
        model.insert(key);
        (tree.census.leaves, tree.census.inners) != census || spine(tree) != grown
    }

    /// Takes out the highest or the lowest entry, as the model does.
    fn pop(tree: &mut Tree, model: &mut Model, high: bool) -> bool {
        let Some(key) = (if high {
            model.pop_last()
        } else {
            model.pop_first()
        }) else {
            assert_eq!(tree.len(), 0);
            return false;
        };
        let at = if high { model.len() } else { 0 };
        let got = tree
            .remove_at(at)
            .map(|(s, r)| (OrderedFloat(s), r.to_vec()));
        assert_eq!(got, Some(key));
        true
    }

    /// Appends fill every node they pass, at every level, checked after each.
    #[test]
    fn appends_fill_every_node_but_the_last_two() {
        let mut next = xorshift(0x5be0_cd19_137e_2179);
        for run in 0..3 {
            // Rising scores; one score, rising members; members that extend the last.
            let n = if run == 2 { 600 } else { 3_000 };
            let keys: Vec<_> = (0..n)
                .map(|i| match run {
                    0 => (OrderedFloat(i as f64 - 1_000.5), binary(&mut next)),
                    1 => (OrderedFloat(7.0), (i as u32).to_be_bytes().to_vec()),
                    _ => (OrderedFloat(-1.0), vec![0; i]),
                })
                .collect();
            let mut tree = Tree::default();
            let mut model = Model::new();
            for key in keys {
                append(&mut tree, &mut model, key);
                tree.check();
                full_behind_the_edge(&tree);
            }
            assert_eq!(tree.census.leaves, 2 + (n - 33) / CAP, "run {run}");
            assert_eq!(tree.census.leaves, n.div_ceil(CAP), "as few as hold them");
            agrees(&tree, &model);
            let all: Vec<_> = model.iter().map(|(s, m)| (s.0, m.clone())).collect();
            for _ in 0..60 {
                let (one, other) = (&all[next(n)], &all[next(n)]);
                bounds_agree(&tree, &model, one.0, &one.1);
                bounds_agree(&tree, &model, one.0, &one.1[..one.1.len() / 2]);
                range_agrees(&tree, &model, bound(next(5), one), bound(next(5), other));
            }
        }
    }

    /// Only a key past the largest fills the leaf behind; others split as before.
    #[test]
    fn only_a_key_past_the_largest_lends() {
        let (leaf, inner) = sizes();
        for tied in [false, true] {
            let key = |i: u8| (if tied { 7.0 } else { f64::from(i) }, vec![b'm', i]);
            // A split at 33 leaves 16 and 17; 15 more fill the last leaf.
            let start = || {
                let mut tree = Tree::default();
                for (score, member) in (0..48).map(key) {
                    assert!(add(&mut tree, score, &member));
                }
                assert_eq!(fills(&tree), [16, 32]);
                tree
            };
            let (largest, member) = key(47);
            let below = if tied {
                (7.0, vec![b'm', 46, 0])
            } else {
                (46.5, b"z".to_vec())
            };
            let cases = [
                ((largest, member.clone()), false, vec![16, 32]),
                (below, true, vec![16, 16, 17]),
                ((largest, [&member[..], &[0]].concat()), true, vec![32, 17]),
                ((largest + 0.5, Vec::new()), true, vec![32, 17]),
                ((-1.0, b"a".to_vec()), true, vec![17, 32]),
            ];
            for ((score, member), added, want) in cases {
                let mut tree = start();
                assert_eq!(add(&mut tree, score, &member), added);
                assert_eq!(fills(&tree), want, "tied {tied}: {score} {member:?}");
                let leaves = want.len();
                assert_eq!(tree.heap_bytes(), leaves * leaf + inner);
                tree.check();
            }
        }
    }

    /// Past a full leaf's last key, but not the largest: the leaf splits as before.
    #[test]
    fn a_key_past_a_full_middle_leaf_splits_it() {
        let mut tree = Tree::default();
        // The largest first, so no insert appends: halves, then odd scores fill the middle.
        let evens = (0..48).map(|i| 2 * i);
        let odds = (16..32).map(|i| 2 * i + 1);
        for score in std::iter::once(1_000).chain(evens).chain(odds) {
            assert!(add(&mut tree, f64::from(score), b"m"));
        }
        assert_eq!(fills(&tree), [16, 32, 17]);
        assert!(add(&mut tree, 63.5, b"m"));
        assert_eq!(fills(&tree), [16, 16, 17, 17]);
        tree.check();
    }

    /// An internal node lends its front only for a key past the largest.
    #[test]
    fn internal_nodes_lend_only_for_a_key_past_the_largest() {
        // Even scores appended until the root's second child is full.
        let start = || {
            let mut tree = Tree::default();
            let mut i = 0;
            while (fills(&tree), spine(&tree).len()) != (vec![16, 32], 3) {
                assert!(add(&mut tree, f64::from(2 * i), b"m"));
                i += 1;
            }
            // Odd scores below the largest fill the last leaf, and lend nothing.
            let largest = f64::from(2 * (i - 1));
            for k in 0..15 {
                assert!(add(&mut tree, largest - 1.0 - f64::from(2 * k), b"m"));
            }
            assert_eq!(
                (fills(&tree), spine(&tree)),
                (vec![16, 32], vec![2, 32, 32])
            );
            tree.check();
            (tree, largest)
        };
        let (mut tree, largest) = start();
        let census = (tree.census.leaves, tree.census.inners);
        // Below the largest: the leaf splits, then the full node.
        assert!(add(&mut tree, largest - 31.0, b"m"));
        assert_eq!(fills(&tree), [16, 16, 17]);
        assert_eq!(
            (tree.census.leaves, tree.census.inners),
            (census.0 + 1, census.1 + 1)
        );
        tree.check();
        // Past it: the root's children rotate, then the leaves; nothing is allocated.
        let (mut tree, largest) = start();
        assert!(add(&mut tree, largest + 2.0, b"m"));
        assert_eq!(
            (fills(&tree), spine(&tree)),
            (vec![32, 16], vec![2, 16, 17])
        );
        assert_eq!((tree.census.leaves, tree.census.inners), census);
        tree.check();
    }

    /// Appends grow three internal levels, checked at each split or lend.
    #[test]
    fn appends_through_three_levels_check_each_structural_change() {
        let mut tree = Tree::default();
        let mut model = Model::new();
        let mut depths = Vec::new();
        let mut track = |tree: &Tree| {
            let depth = spine(tree).len();
            if depths.last() != Some(&depth) {
                depths.push(depth);
            }
        };
        for i in 0..60_000u32 {
            // Runs of 40 a score cross leaves of 32: separators tie.
            let key = (OrderedFloat(f64::from(i / 40)), i.to_be_bytes().to_vec());
            if append(&mut tree, &mut model, key) {
                tree.check();
            }
            track(&tree);
        }
        full_behind_the_edge(&tree);
        assert_eq!(levels(&tree)[1][0], CAP, "the root's first child filled");
        assert_eq!(tree.census.leaves, 60_000usize.div_ceil(CAP));
        agrees(&tree, &model);
        // Highest and lowest by turns: the root collapses a level at a time.
        let mut high = true;
        let mut census = (tree.census.leaves, tree.census.inners);
        while pop(&mut tree, &mut model, high) {
            let after = (tree.census.leaves, tree.census.inners);
            if after != census || tree.len().is_multiple_of(997) {
                tree.check();
            }
            census = after;
            high = !high;
            track(&tree);
        }
        assert_eq!(depths, [2, 3, 4, 3, 2, 0]);
        assert_eq!(tree.heap_bytes(), 0);
    }

    /// Appends grow the root; pops from both ends collapse it; checked after each.
    #[test]
    fn appends_grow_the_root_and_pops_collapse_it() {
        let (leaf, inner) = sizes();
        for tied in [false, true] {
            let mut tree = Tree::default();
            let mut model = Model::new();
            let mut depths = Vec::new();
            let mut track = |tree: &Tree| {
                let depth = spine(tree).len();
                if depths.last() != Some(&depth) {
                    depths.push(depth);
                }
            };
            // Past a root of 32 full leaves.
            for i in 0..CAP * CAP + 100 {
                let i = i as u16;
                let key = if tied {
                    (OrderedFloat(3.0), i.to_be_bytes().to_vec())
                } else {
                    (OrderedFloat(f64::from(i)), b"m".to_vec())
                };
                append(&mut tree, &mut model, key);
                tree.check();
                track(&tree);
            }
            full_behind_the_edge(&tree);
            let nodes = tree.census.leaves * leaf + tree.census.inners * inner;
            assert_eq!(tree.heap_bytes(), nodes);
            agrees(&tree, &model);
            let mut high = true;
            while pop(&mut tree, &mut model, high) {
                tree.check();
                if model.len().is_multiple_of(200) {
                    agrees(&tree, &model);
                }
                high = !high;
                track(&tree);
            }
            assert_eq!(depths, [2, 3, 2, 0], "tied {tied}");
            assert_eq!((tree.len(), tree.heap_bytes()), (0, 0));
        }
    }

    /// Random changes and appends after a long run of appends, checked after each.
    #[test]
    fn random_changes_after_a_long_ordered_run() {
        let mut next = xorshift(0x1f83_d9ab_fb41_bd6b);
        let mut tree = Tree::default();
        let mut model = Model::new();
        for i in 0..6_000u32 {
            // Even scores, three a score: odd ones and new members fall between.
            let key = (OrderedFloat(f64::from(i / 3 * 2)), i.to_be_bytes().to_vec());
            append(&mut tree, &mut model, key);
        }
        full_behind_the_edge(&tree);
        agrees(&tree, &model);
        for step in 0..4_000 {
            match next(6) {
                // Anywhere, ties included, a few past the largest.
                0 | 1 => {
                    let key = (OrderedFloat(next(4_200) as f64), binary(&mut next));
                    assert_eq!(add(&mut tree, key.0.0, &key.1), model.insert(key));
                }
                2 if !model.is_empty() => {
                    let at = next(model.len());
                    take_nth(&mut tree, &mut model, at);
                }
                3 => {
                    pop(&mut tree, &mut model, next(2) == 0);
                }
                _ => {
                    let (score, member) = model.last().cloned().unwrap_or_default();
                    let key = if next(2) == 0 {
                        (score, [&member[..], &[next(256) as u8]].concat())
                    } else {
                        (OrderedFloat(score.0 + 1.0), binary(&mut next))
                    };
                    append(&mut tree, &mut model, key);
                }
            }
            tree.check();
            if step % 500 == 0 {
                agrees(&tree, &model);
            }
        }
        agrees(&tree, &model);
        let all: Vec<_> = model.iter().map(|(s, m)| (s.0, m.clone())).collect();
        for _ in 0..60 {
            let (one, other) = (&all[next(all.len())], &all[next(all.len())]);
            bounds_agree(&tree, &model, one.0 + 1.0, &one.1);
            range_agrees(&tree, &model, bound(next(5), one), bound(next(5), other));
        }
    }

    /// Appends, random changes and pops allocate and free exactly the nodes counted.
    #[test]
    fn appends_allocate_exactly_the_nodes_counted() {
        let Some(start) = crate::memory_use::allocated_by_this_thread() else {
            return;
        };
        let allocated = || crate::memory_use::allocated_by_this_thread().unwrap() - start;
        let (leaf, inner) = (size_of::<Leaf<Id>>(), size_of::<Inner<Id>>());
        let mut next = xorshift(0x9b05_688c_2b3e_6c1f);
        // Even counters, 64 to a score: equal-score runs cross leaves.
        let key = |i: u64| (f64::from((i / 128) as u32), Id(i.to_be_bytes()));
        let n = 40_000;
        let mut tree = CountedTree::default();
        for i in 0..n {
            let (score, id) = key(2 * i);
            assert!(tree.insert(score, id).is_ok());
            assert_eq!(allocated(), tree.heap_bytes() as isize);
        }
        tree.check();
        full_behind_the_edge(&tree);
        let (leaves, inners) = (tree.census.leaves, tree.census.inners);
        assert_eq!(leaves, (n as usize).div_ceil(CAP));
        assert_eq!(tree.heap_bytes(), leaves * leaf + inners * inner);
        for _ in 0..10_000 {
            // A few past the largest, appended.
            let (score, id) = key(next(2 * n as usize + 512) as u64);
            if tree.remove(score, &id).is_none() {
                assert!(tree.insert(score, id).is_ok());
            }
            assert_eq!(allocated(), tree.heap_bytes() as isize);
        }
        tree.check();
        let mut high = true;
        while tree
            .remove_at(if high {
                tree.len().saturating_sub(1)
            } else {
                0
            })
            .is_some()
        {
            assert_eq!(allocated(), tree.heap_bytes() as isize);
            high = !high;
        }
        assert_eq!((tree.heap_bytes(), allocated()), (0, 0), "every node freed");
    }

    /// Every node's address, level by level: unchanged if no node came or went.
    fn nodes<R>(tree: &CountedTree<R>) -> Vec<usize> {
        let mut addresses = Vec::new();
        let mut inners: Vec<&Inner<R>> = tree.root.as_deref().into_iter().collect();
        while !inners.is_empty() {
            let mut below = Vec::new();
            for node in inners {
                addresses.push(std::ptr::from_ref(node).addr());
                for c in 0..node.keys.len() {
                    match node.child(c) {
                        Some(Child::Leaf(leaf)) => addresses.push(std::ptr::from_ref(leaf).addr()),
                        Some(Child::Inner(inner)) => below.push(inner),
                        None => {}
                    }
                }
            }
            inners = below;
        }
        addresses
    }

    fn key_at(model: &Model, rank: usize) -> (OrderedFloat<f64>, Vec<u8>) {
        model.iter().nth(rank).cloned().expect("a rank held")
    }

    /// What moving `key` to `new` must do, by the model: keep its rank between
    /// its neighbours, move past one, or be refused.
    fn expected(model: &Model, key: &(OrderedFloat<f64>, Vec<u8>), new: f64) -> Option<Rescore> {
        let moved = (OrderedFloat(new), key.1.clone());
        if !model.contains(key) || (moved != *key && model.contains(&moved)) {
            return None;
        }
        let below = model.range(..key).next_back();
        let above = model.range((Bound::Excluded(key), Bound::Unbounded)).next();
        let kept = below.is_none_or(|b| *b < moved) && above.is_none_or(|a| moved < *a);
        Some(if kept {
            Rescore::InPlace
        } else {
            Rescore::Moved
        })
    }

    /// Moves `key` to `new` in both; the tree must do as the model expects.
    fn rescore_both(
        tree: &mut Tree,
        model: &mut Model,
        key: &(OrderedFloat<f64>, Vec<u8>),
        new: f64,
    ) -> Option<Rescore> {
        let want = expected(model, key, new);
        let got = tree.rescore(key.0.0, new, &Box::from(&key.1[..]));
        assert_eq!(got, want, "{key:?} to {new}");
        if got.is_some() {
            model.remove(key);
            model.insert((OrderedFloat(new), key.1.clone()));
        }
        got
    }

    /// Ranks, entries both ways and the checks, then bounds and ranges at `key`.
    fn agrees_at(tree: &Tree, model: &Model, key: &(f64, Vec<u8>), other: &(f64, Vec<u8>)) {
        agrees(tree, model);
        bounds_agree(tree, model, key.0, &key.1);
        for kinds in [(1, 3), (3, 1), (0, 2), (4, 0)] {
            range_agrees(tree, model, bound(kinds.0, key), bound(kinds.1, other));
            range_agrees(tree, model, bound(kinds.0, other), bound(kinds.1, key));
        }
    }

    /// Scores 10 apart, members in the same order, appended.
    fn spaced(n: usize) -> (Tree, Model) {
        let mut tree = Tree::default();
        let mut model = Model::new();
        for i in 0..n {
            let key = (
                OrderedFloat(10.0 * i as f64),
                format!("m{i:05}").into_bytes(),
            );
            assert!(add(&mut tree, key.0.0, &key.1));
            model.insert(key);
        }
        (tree, model)
    }

    /// A key that stays between its neighbours keeps its rank: no node changes.
    #[test]
    fn rescores_that_keep_the_rank_are_made_in_place() {
        let (mut tree, mut model) = spaced(2_000);
        let Some(root) = tree.root.as_deref() else {
            panic!("a root");
        };
        // The first entry under the root's second child: the root holds its separator.
        let split = root.keys.counts[0] as usize;
        let score = |rank: usize| key_at(&model, rank).0.0;
        let cases = [
            // Inside a leaf; a leaf's first, below its separator; a leaf's last.
            (5, 53.0),
            (32, 315.0),
            (63, 639.5),
            // Tied with the last of the leaf before, then the first of the next.
            (96, score(95)),
            (127, score(128)),
            // Either side of the root's separator, then tied across it.
            (split, score(split) - 5.0),
            (split - 1, score(split - 1) + 5.0),
            (0, f64::NEG_INFINITY),
            (1_999, f64::INFINITY),
            (500, score(500)),
            // -0 sorts as 0: callers keep it as 0, and the tree keeps what it is given.
            (0, 0.0),
            (0, -0.0),
            (0, 0.0),
        ];
        for (rank, new) in cases {
            let key = key_at(&model, rank);
            let held = (nodes(&tree), tree.heap_bytes());
            let made = rescore_both(&mut tree, &mut model, &key, new);
            assert_eq!(made, Some(Rescore::InPlace), "rank {rank} to {new}");
            assert_eq!((nodes(&tree), tree.heap_bytes()), held, "rank {rank}");
            assert_eq!(tree.rank(new, &key.1), Ok(rank));
            assert_eq!(
                tree.nth(rank).map(|(s, _)| s.to_bits()),
                Some(new.to_bits())
            );
            agrees_at(&tree, &model, &(new, key.1.clone()), &(key.0.0, key.1));
        }
        // NaN, which callers never pass, sorts above every score, as an insert's does.
        let (largest, member) = key_at(&model, 1_999);
        let r = Box::from(&member[..]);
        assert_eq!(
            tree.rescore(largest.0, f64::NAN, &r),
            Some(Rescore::InPlace)
        );
        tree.check();
        assert_eq!(tree.rank(f64::NAN, &member), Ok(1_999));
        assert_eq!(
            tree.rescore(f64::NAN, largest.0, &r),
            Some(Rescore::InPlace)
        );
        agrees(&tree, &model);
    }

    /// A key past a neighbour moves; one not held, or taken, is refused untouched.
    #[test]
    fn rescores_past_a_neighbour_move_and_others_are_refused() {
        let (mut tree, mut model) = spaced(2_000);
        let n = model.len();
        // Past the next in its leaf; a leaf's last past the next leaf's first, and
        // back; the smallest past the largest, and back; far across the root.
        for (rank, to) in [
            (5, 6),
            (31, 32),
            (32, 31),
            (0, n - 1),
            (n - 1, 0),
            (100, 1_900),
        ] {
            let key = key_at(&model, rank);
            let target = key_at(&model, to).0.0;
            let new = if to > rank {
                target + 1.0
            } else {
                target - 1.0
            };
            let made = rescore_both(&mut tree, &mut model, &key, new);
            assert_eq!(made, Some(Rescore::Moved), "rank {rank} past {to}");
            agrees_at(&tree, &model, &(new, key.1.clone()), &(key.0.0, key.1));
        }
        // The same member just below and just above: a rescore onto either is refused.
        let (score, member) = key_at(&model, 700);
        for twin in [score.0 - 5.0, score.0 + 5.0] {
            assert!(add(&mut tree, twin, &member));
            model.insert((OrderedFloat(twin), member.clone()));
        }
        let absent = [
            ((OrderedFloat(5.0), b"absent".to_vec()), 6.0),
            ((OrderedFloat(score.0 + 1.0), member.clone()), 2.0),
            ((score, member.clone()), score.0 - 5.0),
            ((score, member.clone()), score.0 + 5.0),
        ];
        for (key, new) in absent {
            let held = (nodes(&tree), tree.heap_bytes());
            assert_eq!(rescore_both(&mut tree, &mut model, &key, new), None);
            assert_eq!((nodes(&tree), tree.heap_bytes()), held, "{key:?} untouched");
        }
        agrees(&tree, &model);
        // Equal bytes are not the reference held.
        let mut tagged = CountedTree::default();
        for i in 0..40 {
            assert!(
                tagged
                    .insert(f64::from(i), Tagged(vec![b'm', i as u8].into(), 1))
                    .is_ok()
            );
        }
        let other = Tagged(vec![b'm', 7].into(), 2);
        assert_eq!(tagged.rescore(7.0, 7.5, &other), None);
        let held = Tagged(vec![b'm', 7].into(), 1);
        assert_eq!(tagged.rescore(7.0, 7.5, &held), Some(Rescore::InPlace));
        assert_eq!(tagged.rescore(7.5, 50.0, &held), Some(Rescore::Moved));
        tagged.check();
    }

    /// Ranks where a child other than the first begins, by its parent's depth.
    fn boundaries<R>(tree: &CountedTree<R>) -> Vec<(usize, usize)> {
        let mut found = Vec::new();
        let mut level: Vec<_> = tree
            .root
            .as_deref()
            .map(|root| (root, 0))
            .into_iter()
            .collect();
        let mut depth = 0;
        while !level.is_empty() {
            let mut below = Vec::new();
            for (node, mut rank) in level {
                for c in 0..node.keys.len() {
                    if c > 0 {
                        found.push((depth, rank));
                    }
                    if let Some(Child::Inner(inner)) = node.child(c) {
                        below.push((inner, rank));
                    }
                    rank += node.keys.counts[c] as usize;
                }
            }
            level = below;
            depth += 1;
        }
        found
    }

    /// A subtree's first and last entries find their neighbours in the next
    /// subtree, at separators the root, the nodes below it and the leaves'
    /// parents hold.
    #[test]
    fn rescores_find_neighbours_across_every_level() {
        let (mut tree, mut model) = spaced(40_000);
        assert_eq!(spine(&tree).len(), 4, "three internal levels");
        for depth in 0..3 {
            // The first in place above the entry before; the last in place below
            // the entry after; then each past its neighbour.
            let cases = [(true, 1.0), (false, -0.5), (true, -1.0), (false, 1.0)];
            for (first, offset) in cases {
                let at = boundaries(&tree)
                    .into_iter()
                    .find(|&(d, _)| d == depth)
                    .map(|(_, rank)| rank)
                    .expect("a separator at each depth");
                let (rank, beside) = if first { (at, at - 1) } else { (at - 1, at) };
                let key = key_at(&model, rank);
                let new = key_at(&model, beside).0.0 + offset;
                let kept = (first && offset > 0.0) || (!first && offset < 0.0);
                let made = if kept {
                    Rescore::InPlace
                } else {
                    Rescore::Moved
                };
                let got = rescore_both(&mut tree, &mut model, &key, new);
                assert_eq!(got, Some(made), "depth {depth}, rank {rank} to {new}");
                tree.check();
                let rank = model.range(..&(OrderedFloat(new), key.1.clone())).count();
                assert_eq!(tree.rank(new, &key.1), Ok(rank));
                bounds_agree(&tree, &model, new, &key.1);
            }
        }
        agrees(&tree, &model);
    }

    /// Few scores, many members: each run crosses leaves, and members decide.
    #[test]
    fn equal_score_rescores_keep_or_move_by_member() {
        let mut next = xorshift(0x243f_6a88_85a3_08d3);
        let mut tree = Tree::default();
        let mut model = Model::new();
        while model.len() < 1_500 {
            let mut member = binary(&mut next);
            member.extend_from_slice(&(next(1 << 16) as u32).to_be_bytes()[2..]);
            let key = (OrderedFloat(f64::from(next(3) as u32 + 1)), member);
            assert_eq!(add(&mut tree, key.0.0, &key.1), model.insert(key));
        }
        // Each run's first to the score before, its last to the score after:
        // its member against the next run's decides.
        for score in [1.0, 2.0, 3.0] {
            let first = model
                .range((OrderedFloat(score), Vec::new())..)
                .next()
                .cloned();
            let last = model
                .range(..(OrderedFloat(score + 1.0), Vec::new()))
                .next_back()
                .cloned();
            for (key, new) in [(first, score - 1.0), (last, score + 1.0)] {
                let key = key.expect("a run");
                rescore_both(&mut tree, &mut model, &key, new);
                agrees_at(&tree, &model, &(new, key.1.clone()), &(key.0.0, key.1));
            }
        }
        let scores = [0.5, 1.0, 2.0, 3.0, 3.5, f64::NEG_INFINITY, f64::INFINITY];
        let mut made = [0, 0];
        for _ in 0..800 {
            let key = key_at(&model, next(model.len()));
            let new = scores[next(scores.len())];
            match rescore_both(&mut tree, &mut model, &key, new) {
                Some(Rescore::InPlace) => made[0] += 1,
                Some(Rescore::Moved) => made[1] += 1,
                None => panic!("{key:?} is held"),
            }
            agrees_at(&tree, &model, &(new, key.1.clone()), &(key.0.0, key.1));
        }
        assert!(made.iter().all(|&made| made > 50), "{made:?}");
    }

    /// One member, rescored again and again, near its place and far from it.
    #[test]
    fn one_member_rescored_again_and_again() {
        let mut next = xorshift(0x1319_8a2e_0370_7344);
        let (mut tree, mut model) = spaced(3_000);
        let member = b"m01234".to_vec();
        let mut score = 12_340.0;
        let mut made = [0, 0];
        for step in 0..2_000 {
            // Others' scores are 10 apart: a step of up to 10 often stays between.
            let new = if next(2) == 0 {
                score + next(21) as f64 - 10.0
            } else {
                (next(3_000) * 10) as f64
            };
            let key = (OrderedFloat(score), member.clone());
            match rescore_both(&mut tree, &mut model, &key, new) {
                Some(Rescore::InPlace) => made[0] += 1,
                Some(Rescore::Moved) => made[1] += 1,
                None => panic!("held at {score}"),
            }
            score = new;
            tree.check();
            let rank = model.range(..&(OrderedFloat(new), member.clone())).count();
            assert_eq!(tree.rank(new, &member), Ok(rank));
            assert_eq!(tree.nth(rank).map(owned), Some((new, member.clone())));
            if step % 100 == 0 {
                agrees(&tree, &model);
            }
        }
        agrees(&tree, &model);
        assert!(made.iter().all(|&made| made > 200), "{made:?}");
    }

    /// Inserts, removals and rescores, held and not, checked after each.
    #[test]
    fn random_inserts_removals_and_rescores_agree_with_a_model() {
        let mut next = xorshift(0xa409_3822_299f_31d0);
        let mut tree = Tree::default();
        let mut model = Model::new();
        let tied = [f64::NEG_INFINITY, -2.5, 0.0, -0.0, 1.0, f64::INFINITY];
        let mut made = [0, 0, 0];
        for _ in 0..5_000 {
            match next(5) {
                0 | 1 => {
                    let key = (OrderedFloat(next(400) as f64 - 200.0), binary(&mut next));
                    assert_eq!(add(&mut tree, key.0.0, &key.1), model.insert(key));
                }
                2 if !model.is_empty() => {
                    let at = next(model.len());
                    take_nth(&mut tree, &mut model, at);
                }
                _ => {
                    // Now and then a member not held.
                    let key = if model.is_empty() || next(10) == 0 {
                        (OrderedFloat(next(400) as f64 - 200.0), binary(&mut next))
                    } else {
                        key_at(&model, next(model.len()))
                    };
                    // Scores in ties, anywhere, or a nudge that mostly keeps the rank.
                    let new = match next(3) {
                        0 => tied[next(tied.len())],
                        1 => next(800) as f64 / 2.0 - 200.0,
                        _ => key.0.0 + 0.25,
                    };
                    match rescore_both(&mut tree, &mut model, &key, new) {
                        Some(Rescore::InPlace) => made[0] += 1,
                        Some(Rescore::Moved) => made[1] += 1,
                        None => made[2] += 1,
                    }
                    agrees_at(&tree, &model, &(new, key.1.clone()), &(key.0.0, key.1));
                }
            }
            tree.check();
        }
        assert!(made.iter().all(|&made| made > 50), "{made:?}");
    }

    /// Rescores, in place and moved, allocate and free exactly the nodes counted.
    #[test]
    fn rescores_allocate_exactly_the_nodes_counted() {
        let mut next = xorshift(0x0822_9f31_d008_2efa);
        let n = 30_000;
        let id = |i: usize| Id((i as u64).to_be_bytes());
        // Each member's score: 64 to a score at first, so runs cross leaves.
        let mut scores: Vec<f64> = (0..n).map(|i| (i / 64) as f64).collect();
        // Counted from here: the tree's nodes alone.
        let Some(start) = crate::memory_use::allocated_by_this_thread() else {
            return;
        };
        let allocated = || crate::memory_use::allocated_by_this_thread().unwrap() - start;
        let mut tree = CountedTree::default();
        for (i, &score) in scores.iter().enumerate() {
            assert!(tree.insert(score, id(i)).is_ok());
        }
        assert_eq!(allocated(), tree.heap_bytes() as isize);
        let mut made = [0, 0];
        for _ in 0..20_000 {
            let i = next(n);
            let new = match next(3) {
                0 => scores[i],
                1 => scores[i] + next(3) as f64 - 1.0,
                _ => next(n / 64) as f64,
            };
            let held = (allocated(), tree.census.leaves, tree.census.inners);
            match tree.rescore(scores[i], new, &id(i)) {
                Some(Rescore::InPlace) => {
                    made[0] += 1;
                    let now = (allocated(), tree.census.leaves, tree.census.inners);
                    assert_eq!(now, held, "in place, no node came or went");
                }
                Some(Rescore::Moved) => made[1] += 1,
                None => panic!("member {i} held at {}", scores[i]),
            }
            scores[i] = new;
            assert_eq!(allocated(), tree.heap_bytes() as isize);
        }
        tree.check();
        assert!(made.iter().all(|&made| made > 1_000), "{made:?}");
        while tree.remove_at(0).is_some() {}
        assert_eq!((tree.heap_bytes(), allocated()), (0, 0), "every node freed");
    }

    /// A rescore that keeps the rank allocates and frees nothing: every node
    /// stays where it was, through every leaf's first and last entry.
    #[test]
    fn an_in_place_rescore_allocates_and_frees_nothing() {
        let Some(start) = crate::memory_use::allocated_by_this_thread() else {
            return;
        };
        let allocated = || crate::memory_use::allocated_by_this_thread().unwrap() - start;
        let mut next = xorshift(0x3707_3443_a409_3822);
        let n = 5_000;
        let id = |i: usize| Id((i as u64).to_be_bytes());
        // Scores 2 apart: within a half of its own, no key passes another.
        let home = |i: usize| (2 * i) as f64;
        let mut tree = CountedTree::default();
        for i in 0..n {
            assert!(tree.insert(home(i), id(i)).is_ok());
        }
        let mut scores: Vec<f64> = (0..n).map(home).collect();
        // The addresses first, so that their own buffer is in both counts.
        let addresses = nodes(&tree);
        let held = (allocated(), tree.heap_bytes());
        let every = (0..n).map(|i| (i, 0.5));
        let random = (0..3_000).map(|_| (next(n), [-0.5, 0.0, 0.5][next(3)]));
        for (i, step) in every.chain(random) {
            let new = home(i) + step;
            assert_eq!(tree.rescore(scores[i], new, &id(i)), Some(Rescore::InPlace));
            scores[i] = new;
            assert_eq!((allocated(), tree.heap_bytes()), held, "member {i}");
            assert_eq!(
                nodes(&tree),
                addresses,
                "member {i}: every node where it was"
            );
        }
        tree.check();
        let all: Vec<_> = tree.iter().map(|(score, id)| (score, id.0)).collect();
        let want: Vec<_> = (0..n).map(|i| (scores[i], id(i).0)).collect();
        assert_eq!(all, want);
    }
}
