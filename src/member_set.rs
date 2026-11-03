//! The members of a set (`SADD`): a hash table that can also hand out a
//! member at random without looking at the others.
//!
//! The standard library's `HashSet` cannot: the only way to a random member
//! is to walk the table. `SPOP` and `SRANDMEMBER` did, cloning every member
//! on the way, and on a set of a hundred thousand members answered a few
//! thousand times a second where every other command answered a million.
//!
//! This is the same table (hashbrown's, as the standard library's is), held
//! directly so that a bucket can be asked for by its number: a random member
//! is a random bucket, tried again while it is empty, as Redis picks one. A
//! table is never less than a quarter full for long (it is shrunk below
//! that: see [`MemberSet::remove`]), so that is a handful of tries.
//!
//! **Small sets of integers (#297).** A set whose members are all the
//! canonical text of an `i64` (no `+`, no leading zero, no `-0`), no more of
//! them than `set-max-intset-entries`, is a sorted array of the narrowest of
//! `i16`, `i32` and `i64` that holds them: two allocations in all, where the
//! table takes one per member. Formatting such a number gives back exactly
//! the bytes that were added, so nobody can tell the two forms apart but by
//! their size. A member that is not such a number, or one more than the
//! limit, turns the array into the table, for good; the table never turns
//! back. Either form changes only through `&mut self`, so under the set's
//! own lock, as before.

use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use hashbrown::HashTable;

/// `set-max-intset-entries` until `CONFIG SET` or the configuration file
/// gives another.
pub const DEFAULT_INTSET_MAX: usize = 64;

/// The most members a set of integers keeps as an array (#297), as
/// `set-max-intset-entries` says. 0: no arrays.
static INTSET_MAX: AtomicUsize = AtomicUsize::new(DEFAULT_INTSET_MAX);

#[inline]
pub fn intset_max() -> usize {
    INTSET_MAX.load(Ordering::Relaxed)
}

/// Takes effect at a set's next new member. An array already longer stays
/// one until then, and a table never turns back.
pub fn set_intset_max(limit: usize) {
    INTSET_MAX.store(limit, Ordering::Relaxed);
}

/// The `i64` whose text `bytes` is, written as it would be written: the only
/// members an array of integers may hold.
#[inline]
pub fn canonical_i64(bytes: &[u8]) -> Option<i64> {
    let (negative, digits) = match bytes {
        [b'-', rest @ ..] => (true, rest),
        _ => (false, bytes),
    };
    match digits {
        [] => return None,
        [b'0'] if !negative => return Some(0),
        [b'0', ..] => return None,
        _ if digits.len() > 19 => return None,
        _ => {}
    }
    let mut n: i64 = 0;
    for &d in digits {
        if !d.is_ascii_digit() {
            return None;
        }
        let d = (d - b'0') as i64;
        n = n.checked_mul(10)?;
        n = if negative {
            n.checked_sub(d)?
        } else {
            n.checked_add(d)?
        };
    }
    Some(n)
}

/// An integer member's text, made without allocating.
#[derive(Clone, Copy)]
pub struct IntText {
    buf: [u8; 20],
    start: u8,
}

const PAIRS: [u8; 200] = {
    let mut out = [0u8; 200];
    let mut i = 0;
    while i < 100 {
        out[i * 2] = b'0' + (i / 10) as u8;
        out[i * 2 + 1] = b'0' + (i % 10) as u8;
        i += 1;
    }
    out
};

impl IntText {
    #[inline]
    pub fn new(value: i64) -> Self {
        let mut buf = [0u8; 20];
        let mut at = buf.len();
        let mut n = value.unsigned_abs();
        // Two digits at a time: half the divisions.
        while n >= 100 {
            let pair = (n % 100) as usize * 2;
            n /= 100;
            at -= 2;
            buf[at] = PAIRS[pair];
            buf[at + 1] = PAIRS[pair + 1];
        }
        if n >= 10 {
            let pair = n as usize * 2;
            at -= 2;
            buf[at] = PAIRS[pair];
            buf[at + 1] = PAIRS[pair + 1];
        } else {
            at -= 1;
            buf[at] = b'0' + n as u8;
        }
        if value < 0 {
            at -= 1;
            buf[at] = b'-';
        }
        IntText {
            buf,
            start: at as u8,
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf[self.start as usize..]
    }
}

/// A member, as a set hands it out: the table's shared bytes, or an array's
/// integer as text. Either is its bytes (`Deref`).
pub enum Member<'a> {
    Shared(&'a Arc<[u8]>),
    Int(IntText),
}

impl Member<'_> {
    /// The member as shared bytes: the table's own, or a new copy of an
    /// integer's text.
    pub fn to_shared(&self) -> Arc<[u8]> {
        match self {
            Member::Shared(bytes) => Arc::clone(bytes),
            Member::Int(text) => Arc::from(text.as_bytes()),
        }
    }
}

impl Deref for Member<'_> {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        match self {
            Member::Shared(bytes) => bytes,
            Member::Int(text) => text.as_bytes(),
        }
    }
}

impl AsRef<[u8]> for Member<'_> {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl std::fmt::Debug for Member<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", String::from_utf8_lossy(self))
    }
}

/// A set of byte strings. Each member of the table is shared (`Arc`), as the
/// commands that copy members between sets and into replies want it.
#[derive(Clone)]
pub struct MemberSet {
    repr: Repr,
}

#[derive(Clone)]
enum Repr {
    Ints(Ints),
    Table(Table),
}

macro_rules! each_width {
    ($ints:expr, $v:ident => $body:expr) => {
        match $ints {
            Ints::W16($v) => $body,
            Ints::W32($v) => $body,
            Ints::W64($v) => $body,
        }
    };
}

impl Default for MemberSet {
    fn default() -> Self {
        Self::new()
    }
}

impl MemberSet {
    pub fn new() -> Self {
        let repr = if intset_max() > 0 {
            Repr::Ints(Ints::W16(Vec::new()))
        } else {
            Repr::Table(Table::default())
        };
        Self { repr }
    }

    pub fn with_capacity(members: usize) -> Self {
        if members <= intset_max() {
            return Self::new();
        }
        Self {
            repr: Repr::Table(Table::with_capacity(members)),
        }
    }

    /// Whether the members are kept as an array of integers.
    pub fn is_compact(&self) -> bool {
        matches!(self.repr, Repr::Ints(_))
    }

    /// What the array has allocated, room to spare included; `None` for the
    /// table. For `MEMORY USAGE`.
    pub fn array_bytes(&self) -> Option<usize> {
        match &self.repr {
            Repr::Ints(ints) => Some(each_width!(ints, v => allocated(v))),
            Repr::Table(_) => None,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match &self.repr {
            Repr::Ints(ints) => ints.len(),
            Repr::Table(t) => t.table.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> usize {
        match &self.repr {
            Repr::Ints(ints) => ints.capacity(),
            Repr::Table(t) => t.table.capacity(),
        }
    }

    #[inline]
    pub fn contains(&self, member: &[u8]) -> bool {
        match &self.repr {
            Repr::Ints(ints) => canonical_i64(member).is_some_and(|n| ints.find(n).is_ok()),
            Repr::Table(t) => t.get(member).is_some(),
        }
    }

    #[inline]
    pub fn get(&self, member: &[u8]) -> Option<Member<'_>> {
        match &self.repr {
            Repr::Ints(ints) => {
                let n = canonical_i64(member)?;
                ints.find(n).ok().map(|_| Member::Int(IntText::new(n)))
            }
            Repr::Table(t) => t.get(member).map(Member::Shared),
        }
    }

    /// Adds `member`. False if it was there already.
    pub fn insert(&mut self, member: Arc<[u8]>) -> bool {
        if let Some(added) = self.insert_int(&member) {
            return added;
        }
        self.table().insert(member)
    }

    /// Adds `member`, making the shared copy of it only if it is new.
    /// False if it was there already.
    pub fn insert_bytes(&mut self, member: &[u8]) -> bool {
        if let Some(added) = self.insert_int(member) {
            return added;
        }
        self.table().insert_bytes(member)
    }

    /// The array's insert: `None` when the member has to go to the table.
    #[inline]
    fn insert_int(&mut self, member: &[u8]) -> Option<bool> {
        let Repr::Ints(ints) = &mut self.repr else {
            return None;
        };
        let n = canonical_i64(member)?;
        match ints.find(n) {
            Ok(_) => Some(false),
            Err(_) if ints.len() >= intset_max() => None,
            Err(at) => {
                ints.insert_at(at, n);
                Some(true)
            }
        }
    }

    /// The table, made from the array if the members are still one.
    fn table(&mut self) -> &mut Table {
        if let Repr::Ints(ints) = &self.repr {
            let mut table = Table::with_capacity(ints.len() + 1);
            for i in 0..ints.len() {
                table.insert(Arc::from(IntText::new(ints.get(i)).as_bytes()));
            }
            self.repr = Repr::Table(table);
        }
        match &mut self.repr {
            Repr::Table(table) => table,
            Repr::Ints(_) => unreachable!(),
        }
    }

    /// Removes `member`. False if it was not there.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        match &mut self.repr {
            Repr::Ints(ints) => match canonical_i64(member).map(|n| ints.find(n)) {
                Some(Ok(at)) => {
                    ints.remove_at(at);
                    true
                }
                _ => false,
            },
            Repr::Table(t) => t.take(member).is_some(),
        }
    }

    /// Removes `member` and hands it back.
    pub fn take(&mut self, member: &[u8]) -> Option<Arc<[u8]>> {
        match &mut self.repr {
            Repr::Ints(ints) => {
                let n = canonical_i64(member)?;
                let at = ints.find(n).ok()?;
                ints.remove_at(at);
                Some(Arc::from(member))
            }
            Repr::Table(t) => t.take(member),
        }
    }

    pub fn clear(&mut self) {
        match &mut self.repr {
            Repr::Ints(ints) => *ints = Ints::W16(Vec::new()),
            Repr::Table(t) => t.table.clear(),
        }
    }

    pub fn reserve(&mut self, more: usize) {
        // An array grows as it is added to: it may never need the room.
        if let Repr::Table(t) = &mut self.repr {
            let hasher = &t.hasher;
            t.table.reserve(more, |have| hasher.hash_one(&**have));
        }
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&[u8]) -> bool) {
        match &mut self.repr {
            Repr::Ints(ints) => ints.retain(|n| keep(IntText::new(n).as_bytes())),
            Repr::Table(t) => {
                t.table.retain(|have| keep(have));
                t.shrink_if_sparse();
            }
        }
    }

    /// Every member's bytes. The form is looked at once, not once a member:
    /// the loop of `SMEMBERS`, a snapshot and the digest.
    #[inline]
    pub fn for_each(&self, mut f: impl FnMut(&[u8])) {
        match &self.repr {
            Repr::Table(t) => t.table.iter().for_each(|m| f(m)),
            Repr::Ints(ints) => {
                each_width!(ints, v => v.iter().for_each(|&n| f(IntText::new(wide(n)).as_bytes())))
            }
        }
    }

    pub fn iter(&self) -> Iter<'_> {
        match &self.repr {
            Repr::Ints(Ints::W16(v)) => Iter::W16(v.iter()),
            Repr::Ints(Ints::W32(v)) => Iter::W32(v.iter()),
            Repr::Ints(Ints::W64(v)) => Iter::W64(v.iter()),
            Repr::Table(t) => Iter::Table(t.table.iter()),
        }
    }

    /// A member, any one as likely as another.
    pub fn random(&self, rng: &mut impl rand::Rng) -> Option<Member<'_>> {
        match &self.repr {
            Repr::Ints(ints) if ints.len() == 0 => None,
            Repr::Ints(ints) => Some(Member::Int(IntText::new(
                ints.get(rng.gen_range(0..ints.len())),
            ))),
            Repr::Table(t) => t
                .table
                .get_bucket(t.random_bucket(rng)?)
                .map(Member::Shared),
        }
    }

    /// Removes a member, any one as likely as another.
    pub fn pop_random(&mut self, rng: &mut impl rand::Rng) -> Option<Arc<[u8]>> {
        match &mut self.repr {
            Repr::Ints(ints) if ints.len() == 0 => None,
            Repr::Ints(ints) => {
                let n = ints.remove_at(rng.gen_range(0..ints.len()));
                Some(Arc::from(IntText::new(n).as_bytes()))
            }
            Repr::Table(t) => t.pop_random(rng),
        }
    }

    /// Up to `count` different members, chosen at random, without removing
    /// them.
    ///
    /// A few of many are picked one at a time, picking again when one comes
    /// up twice. Most of them, or all, are found by walking the table once,
    /// which is then no more work than the answer is long.
    pub fn random_distinct(&self, count: usize, rng: &mut impl rand::Rng) -> Vec<Member<'_>> {
        use rand::seq::IteratorRandom;
        let len = self.len();
        if count >= len {
            return self.iter().collect();
        }
        let t = match &self.repr {
            // Only the chosen positions are formatted, not the whole array.
            Repr::Ints(ints) => {
                return rand::seq::index::sample(rng, len, count)
                    .into_iter()
                    .map(|i| Member::Int(IntText::new(ints.get(i))))
                    .collect();
            }
            Repr::Table(t) if count * 3 <= len => t,
            _ => return self.iter().choose_multiple(rng, count),
        };
        let mut seen: ahash::AHashSet<usize> = ahash::AHashSet::with_capacity(count);
        let mut picked = Vec::with_capacity(count);
        while picked.len() < count {
            let Some(at) = t.random_bucket(rng) else {
                break;
            };
            if seen.insert(at)
                && let Some(member) = t.table.get_bucket(at)
            {
                picked.push(Member::Shared(member));
            }
        }
        picked
    }
}

/// The members of an array, in ascending order, at the width they need.
#[derive(Clone)]
enum Ints {
    W16(Vec<i16>),
    W32(Vec<i32>),
    W64(Vec<i64>),
}

/// Any width as an `i64`: a generic call, so one macro body serves all three.
#[inline]
fn wide<T: Into<i64>>(n: T) -> i64 {
    n.into()
}

fn allocated<T>(v: &Vec<T>) -> usize {
    v.capacity() * std::mem::size_of::<T>()
}

impl Ints {
    #[inline]
    fn len(&self) -> usize {
        each_width!(self, v => v.len())
    }

    fn capacity(&self) -> usize {
        each_width!(self, v => v.capacity())
    }

    #[inline]
    fn get(&self, i: usize) -> i64 {
        each_width!(self, v => wide(v[i]))
    }

    /// Where `n` is (`Ok`) or would go (`Err`).
    #[inline]
    fn find(&self, n: i64) -> Result<usize, usize> {
        match self {
            Ints::W16(v) => match i16::try_from(n) {
                Ok(n) => v.binary_search(&n),
                Err(_) => Err(if n < 0 { 0 } else { v.len() }),
            },
            Ints::W32(v) => match i32::try_from(n) {
                Ok(n) => v.binary_search(&n),
                Err(_) => Err(if n < 0 { 0 } else { v.len() }),
            },
            Ints::W64(v) => v.binary_search(&n),
        }
    }

    /// Puts `n` at `at` (from [`Ints::find`]), widening the array first if
    /// `n` does not fit. Room for exactly one more: adding never leaves
    /// spare room.
    fn insert_at(&mut self, at: usize, n: i64) {
        if let Ints::W16(v) = self
            && i16::try_from(n).is_err()
        {
            *self = Ints::W32(v.iter().map(|&x| x as i32).collect());
        }
        if let Ints::W32(v) = self
            && i32::try_from(n).is_err()
        {
            *self = Ints::W64(v.iter().map(|&x| x as i64).collect());
        }
        each_width!(self, v => {
            v.reserve_exact(1);
            v.insert(at, n as _);
        })
    }

    fn remove_at(&mut self, at: usize) -> i64 {
        each_width!(self, v => {
            let n = wide(v.remove(at));
            if v.capacity() > 2 * v.len() + 4 {
                v.shrink_to_fit();
            }
            n
        })
    }

    fn retain(&mut self, mut keep: impl FnMut(i64) -> bool) {
        each_width!(self, v => {
            v.retain(|&x| keep(wide(x)));
            v.shrink_to_fit();
        })
    }
}

/// The table, as every set was before #297.
#[derive(Clone, Default)]
struct Table {
    table: HashTable<Arc<[u8]>>,
    hasher: ahash::RandomState,
}

impl Table {
    fn with_capacity(members: usize) -> Self {
        Self {
            table: HashTable::with_capacity(members),
            hasher: ahash::RandomState::new(),
        }
    }

    #[inline]
    fn hash(&self, member: &[u8]) -> u64 {
        self.hasher.hash_one(member)
    }

    #[inline]
    fn get(&self, member: &[u8]) -> Option<&Arc<[u8]>> {
        self.table.find(self.hash(member), |have| &**have == member)
    }

    fn insert(&mut self, member: Arc<[u8]>) -> bool {
        let hash = self.hash(&member);
        let hasher = &self.hasher;
        match self.table.entry(
            hash,
            |have| **have == *member,
            |have| hasher.hash_one(&**have),
        ) {
            hashbrown::hash_table::Entry::Occupied(_) => false,
            hashbrown::hash_table::Entry::Vacant(place) => {
                place.insert(member);
                true
            }
        }
    }

    fn insert_bytes(&mut self, member: &[u8]) -> bool {
        let hash = self.hash(member);
        let hasher = &self.hasher;
        match self.table.entry(
            hash,
            |have| &**have == member,
            |have| hasher.hash_one(&**have),
        ) {
            hashbrown::hash_table::Entry::Occupied(_) => false,
            hashbrown::hash_table::Entry::Vacant(place) => {
                place.insert(Arc::from(member));
                true
            }
        }
    }

    fn take(&mut self, member: &[u8]) -> Option<Arc<[u8]>> {
        let hash = self.hash(member);
        let found = self.table.find_entry(hash, |have| &**have == member).ok()?;
        let (taken, _) = found.remove();
        self.shrink_if_sparse();
        Some(taken)
    }

    /// A table that has lost most of its members is made smaller: it would
    /// otherwise keep the memory of its largest size for ever, and a random
    /// bucket of it would nearly always be an empty one.
    fn shrink_if_sparse(&mut self) {
        let buckets = self.table.num_buckets();
        if buckets > 64 && self.table.len() * 8 < buckets {
            let hasher = &self.hasher;
            self.table
                .shrink_to(self.table.len() * 2, |have| hasher.hash_one(&**have));
        }
    }

    /// The bucket a random member is in, if there is a member at all.
    fn random_bucket(&self, rng: &mut impl rand::Rng) -> Option<usize> {
        if self.table.is_empty() {
            return None;
        }
        let buckets = self.table.num_buckets();
        for _ in 0..64 {
            let at = rng.gen_range(0..buckets);
            if self.table.get_bucket(at).is_some() {
                return Some(at);
            }
        }
        // A table made large and filled little (`with_capacity`): sixty-four
        // empty buckets in a row. Walk to a member instead.
        use rand::seq::IteratorRandom;
        let member = self.table.iter().choose(rng)?;
        self.table
            .find_bucket_index(self.hash(member), |have| Arc::ptr_eq(have, member))
    }

    fn pop_random(&mut self, rng: &mut impl rand::Rng) -> Option<Arc<[u8]>> {
        let at = self.random_bucket(rng)?;
        let (taken, _) = self.table.get_bucket_entry(at).ok()?.remove();
        self.shrink_if_sparse();
        Some(taken)
    }
}

/// The members of a set, in no particular order (an array's ascending).
pub enum Iter<'a> {
    W16(std::slice::Iter<'a, i16>),
    W32(std::slice::Iter<'a, i32>),
    W64(std::slice::Iter<'a, i64>),
    Table(hashbrown::hash_table::Iter<'a, Arc<[u8]>>),
}

impl<'a> Iterator for Iter<'a> {
    type Item = Member<'a>;

    #[inline]
    fn next(&mut self) -> Option<Member<'a>> {
        let n = match self {
            Iter::W16(it) => *it.next()? as i64,
            Iter::W32(it) => *it.next()? as i64,
            Iter::W64(it) => *it.next()?,
            Iter::Table(it) => return it.next().map(Member::Shared),
        };
        Some(Member::Int(IntText::new(n)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Iter::W16(it) => it.size_hint(),
            Iter::W32(it) => it.size_hint(),
            Iter::W64(it) => it.size_hint(),
            Iter::Table(it) => it.size_hint(),
        }
    }
}

impl ExactSizeIterator for Iter<'_> {}

impl<'a> IntoIterator for &'a MemberSet {
    type Item = Member<'a>;
    type IntoIter = Iter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for MemberSet {
    type Item = Arc<[u8]>;
    type IntoIter = IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        match self.repr {
            Repr::Table(t) => IntoIter(Handed::Table(t.table.into_iter())),
            Repr::Ints(ints) => IntoIter(Handed::Ints(ints, 0)),
        }
    }
}

/// The members of a set, handed over: an array's each a new shared copy.
pub struct IntoIter(Handed);

enum Handed {
    Ints(Ints, usize),
    Table(hashbrown::hash_table::IntoIter<Arc<[u8]>>),
}

impl Iterator for IntoIter {
    type Item = Arc<[u8]>;
    fn next(&mut self) -> Option<Arc<[u8]>> {
        match &mut self.0 {
            Handed::Table(it) => it.next(),
            Handed::Ints(ints, at) => {
                if *at >= ints.len() {
                    return None;
                }
                let n = ints.get(*at);
                *at += 1;
                Some(Arc::from(IntText::new(n).as_bytes()))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.0 {
            Handed::Table(it) => it.size_hint(),
            Handed::Ints(ints, at) => {
                let left = ints.len() - *at;
                (left, Some(left))
            }
        }
    }
}

impl Extend<Arc<[u8]>> for MemberSet {
    fn extend<I: IntoIterator<Item = Arc<[u8]>>>(&mut self, members: I) {
        for member in members {
            self.insert(member);
        }
    }
}

impl FromIterator<Arc<[u8]>> for MemberSet {
    fn from_iter<I: IntoIterator<Item = Arc<[u8]>>>(members: I) -> Self {
        let mut set = Self::new();
        set.extend(members);
        set
    }
}

impl std::fmt::Debug for MemberSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(i: usize) -> Arc<[u8]> {
        Arc::from(format!("member-{i}").as_bytes())
    }

    #[test]
    fn it_is_a_set() {
        let mut set = MemberSet::new();
        assert!(set.is_empty());
        for i in 0..1000 {
            assert!(set.insert(member(i)));
        }
        for i in 0..1000 {
            assert!(!set.insert(member(i)), "{i} was added twice");
            assert!(set.contains(&member(i)));
        }
        assert_eq!(set.len(), 1000);
        assert!(!set.insert_bytes(&member(3)));
        assert!(set.insert_bytes(b"from bytes"));
        assert!(!set.insert_bytes(b"from bytes"));
        assert!(set.remove(b"from bytes"));
        assert!(!set.contains(b"nobody"));
        assert!(set.remove(&member(7)));
        assert!(!set.remove(&member(7)));
        assert_eq!(set.take(&member(8)).as_deref(), Some(&member(8)[..]));
        assert_eq!(set.len(), 998);
        set.retain(|m| m.ends_with(b"0"));
        assert_eq!(set.len(), 100);
        let copy: MemberSet = set.iter().map(|m| m.to_shared()).collect();
        assert_eq!(copy.len(), 100);
        assert!(set.iter().all(|m| copy.contains(&m)));
    }

    #[test]
    fn popping_at_random_empties_it_and_gives_each_member_once() {
        let mut rng = rand::thread_rng();
        let mut set: MemberSet = (0..5000).map(member).collect();
        let mut popped = std::collections::HashSet::new();
        while let Some(m) = set.pop_random(&mut rng) {
            assert!(popped.insert(m), "a member was popped twice");
        }
        assert_eq!(popped.len(), 5000);
        assert!(set.is_empty());
        assert!(set.random(&mut rng).is_none());
        // What it grew to was given back as it emptied.
        assert!(set.capacity() < 256, "{}", set.capacity());
    }

    #[test]
    fn a_random_member_is_any_of_them() {
        let mut rng = rand::thread_rng();
        for set in [
            (0..10).map(member).collect::<MemberSet>(),
            (0..10)
                .map(|i| Arc::from(i.to_string().as_bytes()))
                .collect(),
        ] {
            let mut times = std::collections::HashMap::new();
            for _ in 0..20_000 {
                *times
                    .entry(set.random(&mut rng).unwrap().to_shared())
                    .or_insert(0u32) += 1;
            }
            assert_eq!(times.len(), 10);
            // Two thousand each, give or take: none is left out or favoured.
            assert!(
                times.values().all(|n| (1500..2500).contains(n)),
                "{times:?}"
            );
        }
    }

    #[test]
    fn several_at_random_are_different_and_no_more_than_there_are() {
        let mut rng = rand::thread_rng();
        let set: MemberSet = (0..1000).map(member).collect();
        for count in [0, 1, 5, 300, 400, 999, 1000, 5000] {
            let picked = set.random_distinct(count, &mut rng);
            assert_eq!(picked.len(), count.min(1000), "asked for {count}");
            let different: std::collections::HashSet<&[u8]> = picked.iter().map(|m| &**m).collect();
            assert_eq!(different.len(), picked.len(), "asked for {count}");
            assert!(picked.iter().all(|m| set.contains(m)));
        }
    }

    // ── #297: the array of integers ─────────────────────────────────────────

    fn bytes(set: &MemberSet) -> Vec<Vec<u8>> {
        let mut all: Vec<Vec<u8>> = set.iter().map(|m| m.to_vec()).collect();
        all.sort();
        all
    }

    #[test]
    fn only_the_canonical_text_of_an_i64_is_an_integer() {
        for (text, want) in [
            ("0", Some(0)),
            ("7", Some(7)),
            ("-7", Some(-7)),
            ("9223372036854775807", Some(i64::MAX)),
            ("-9223372036854775808", Some(i64::MIN)),
            ("9223372036854775808", None),
            ("-9223372036854775809", None),
            ("-0", None),
            ("00", None),
            ("01", None),
            ("-01", None),
            ("+1", None),
            (" 1", None),
            ("1 ", None),
            ("1.0", None),
            ("1e3", None),
            ("", None),
            ("-", None),
            ("99999999999999999999", None),
        ] {
            assert_eq!(canonical_i64(text.as_bytes()), want, "{text:?}");
            if let Some(n) = want {
                assert_eq!(IntText::new(n).as_bytes(), text.as_bytes());
            }
        }
    }

    #[test]
    fn integers_that_are_not_canonical_keep_their_bytes() {
        let mut set = MemberSet::new();
        for m in ["1", "2", "-3", "0"] {
            assert!(set.insert_bytes(m.as_bytes()));
        }
        assert!(set.is_compact());
        // "01" is not "1": a member of its own, and the end of the array.
        assert!(!set.contains(b"01"));
        assert!(set.insert_bytes(b"01"));
        assert!(!set.is_compact());
        for m in ["+1", "-0", "1.0", "00"] {
            assert!(set.insert_bytes(m.as_bytes()), "{m}");
        }
        assert_eq!(set.len(), 9);
        let want: Vec<Vec<u8>> = {
            let mut w: Vec<Vec<u8>> = ["1", "2", "-3", "0", "01", "+1", "-0", "1.0", "00"]
                .iter()
                .map(|m| m.as_bytes().to_vec())
                .collect();
            w.sort();
            w
        };
        assert_eq!(bytes(&set), want);
        assert!(set.contains(b"1") && set.contains(b"01") && set.contains(b"-0"));
    }

    #[test]
    fn the_array_widens_and_keeps_its_order() {
        let mut set = MemberSet::new();
        for n in [5i64, -5, 40_000, -40_000, i64::MAX, i64::MIN, 0] {
            assert!(set.insert_bytes(n.to_string().as_bytes()));
            assert!(!set.insert_bytes(n.to_string().as_bytes()));
        }
        assert!(set.is_compact());
        let in_order: Vec<i64> = set.iter().map(|m| canonical_i64(&m).unwrap()).collect();
        assert_eq!(in_order, [i64::MIN, -40_000, -5, 0, 5, 40_000, i64::MAX]);
        // A number wider than the array is never mistaken for one in it.
        let mut narrow = MemberSet::new();
        narrow.insert_bytes(b"1");
        assert!(!narrow.contains(b"65537") && !narrow.contains(b"-4294967297"));
        assert!(!narrow.remove(b"65537"));
    }

    #[test]
    fn one_more_than_the_limit_makes_it_a_table() {
        let limit = intset_max();
        let mut set = MemberSet::new();
        for i in 0..limit {
            assert!(set.insert_bytes(i.to_string().as_bytes()));
        }
        assert!(set.is_compact());
        // One already there changes nothing.
        assert!(!set.insert_bytes(b"0"));
        assert!(set.is_compact());
        assert!(set.insert_bytes(limit.to_string().as_bytes()));
        assert!(!set.is_compact());
        assert_eq!(set.len(), limit + 1);
        for i in 0..=limit {
            assert!(set.contains(i.to_string().as_bytes()), "{i}");
        }
        // The table does not turn back.
        for i in 0..=limit {
            assert!(set.remove(i.to_string().as_bytes()));
        }
        assert!(set.is_empty() && !set.is_compact());
        assert!(set.insert_bytes(b"1"));
        assert!(!set.is_compact());
    }

    #[test]
    fn removing_from_the_array() {
        let mut rng = rand::thread_rng();
        let mut set: MemberSet = ["3", "1", "2"]
            .iter()
            .map(|m| Arc::from(m.as_bytes()))
            .collect();
        assert!(set.is_compact());
        assert!(!set.remove(b"01") && !set.remove(b"4") && !set.remove(b"x"));
        assert_eq!(set.take(b"2").as_deref(), Some(&b"2"[..]));
        assert!(!set.contains(b"2"));
        set.retain(|m| m != b"3");
        assert_eq!(bytes(&set), [b"1".to_vec()]);
        assert_eq!(set.pop_random(&mut rng).as_deref(), Some(&b"1"[..]));
        assert!(set.is_empty() && set.pop_random(&mut rng).is_none());
        assert!(set.is_compact());
    }

    #[test]
    fn handed_over_or_copied_it_is_the_same_members() {
        let set: MemberSet = (0..10)
            .map(|i| Arc::from((i * 7 - 30).to_string().as_bytes()))
            .collect();
        assert!(set.is_compact());
        let mut rng = rand::thread_rng();
        let all = bytes(&set);
        let mut handed: Vec<Vec<u8>> = set.clone().into_iter().map(|m| m.to_vec()).collect();
        handed.sort();
        assert_eq!(handed, all);
        let mut picked: Vec<Vec<u8>> = set
            .random_distinct(4, &mut rng)
            .iter()
            .map(|m| m.to_vec())
            .collect();
        picked.sort();
        picked.dedup();
        assert_eq!(picked.len(), 4);
        assert!(picked.iter().all(|m| set.contains(m)));
        assert_eq!(set.random_distinct(100, &mut rng).len(), 10);
        assert_eq!(set.get(b"-30").as_deref(), Some(&b"-30"[..]));
        assert!(set.get(b"-030").is_none());
    }
}
