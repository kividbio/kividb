//! The members of a set (`SADD`), kept in one of three forms by what they
//! are, as Redis keeps a set as an intset, a listpack or a hash table.
//!
//! - **Integers (#297).** A set whose members are all the canonical text of
//!   an `i64` (no `+`, no leading zero, no `-0`), no more of them than
//!   `set-max-intset-entries`, is a sorted array of the narrowest of `i16`,
//!   `i32` and `i64` that holds them. Formatting such a number gives back
//!   exactly the bytes that were added, so nobody can tell it from the
//!   others but by its size.
//! - **Packed.** A set of no more than `set-max-listpack-entries` members,
//!   none longer than `set-max-listpack-value` bytes, is one block holding
//!   the members end to end ([`PackedSet`]): a lookup scans a byte a member.
//! - **Table.** Any other set is a [`CollectionTable`] (#764): each member
//!   one block of the entry pool, or none at all for a member of up to
//!   seven bytes, which lives in its slot. It gives `SSCAN` its cursor
//!   (`table_scan`), a uniformly random member in O(1), and grows and
//!   shrinks a few members at a time, so no write pays for moving them all.
//!   A resize that the writes leave unfinished is finished by the
//!   background ([`after_write`]), and the array a finished one leaves is
//!   freed there too.
//!
//! A set moves from the array to the block, or to the table, when a member
//! does not fit where it is, and from the block to the table; never back,
//! as in Redis. Either form changes only through `&mut self`, so under the
//! set's own lock, as before. Snapshots, the AOF, replication and `DUMP`
//! see members as bytes whatever the form: none of them changes.

use std::ops::{ControlFlow, Deref};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};

use crate::collection_table::{CollectionTable, INLINE_MEMBER, MemberIter, MemberRef, Members};
use crate::packed_set::{self, PackedSet};

/// `set-max-intset-entries` until `CONFIG SET` or the configuration file
/// gives another: Redis's.
pub const DEFAULT_INTSET_MAX: usize = 512;
/// `set-max-listpack-entries` until set otherwise: Redis's, and the most a
/// block holds ([`packed_set::MAX_ENTRIES`]).
pub const DEFAULT_PACKED_ENTRIES: usize = packed_set::MAX_ENTRIES;
/// `set-max-listpack-value` until set otherwise: Redis's, and the longest
/// member a block holds ([`packed_set::MAX_LEN`]).
pub const DEFAULT_PACKED_VALUE: usize = packed_set::MAX_LEN;

/// The most members a set of integers keeps as an array (#297), as
/// `set-max-intset-entries` says. 0: no arrays.
///
/// Each new member moves those after it, so an add costs time in proportion
/// to the length: at 512 that is a move of at most 4 KB.
static INTSET_MAX: AtomicUsize = AtomicUsize::new(DEFAULT_INTSET_MAX);
/// The most members a set keeps in one block, as `set-max-listpack-entries`
/// says. 0: no blocks.
static PACKED_ENTRIES: AtomicUsize = AtomicUsize::new(DEFAULT_PACKED_ENTRIES);
/// The longest member a set keeps in one block, as `set-max-listpack-value`
/// says.
static PACKED_VALUE: AtomicUsize = AtomicUsize::new(DEFAULT_PACKED_VALUE);

#[cfg(test)]
thread_local! {
    /// Limits of a test's own, on its thread only: tests run side by side,
    /// and the settings are the process's.
    static OWN_LIMITS: std::cell::Cell<Option<[usize; 3]>> = const { std::cell::Cell::new(None) };
}

/// The limits on this thread alone, for a test: `None` puts the settings
/// back.
#[cfg(test)]
pub(crate) fn own_limits(limits: Option<[usize; 3]>) {
    OWN_LIMITS.with(|own| own.set(limits));
}

#[cfg(test)]
fn own_limit(which: usize) -> Option<usize> {
    OWN_LIMITS.with(|own| own.get()).map(|limits| limits[which])
}

#[inline]
pub fn intset_max() -> usize {
    #[cfg(test)]
    if let Some(limit) = own_limit(0) {
        return limit;
    }
    INTSET_MAX.load(Ordering::Relaxed)
}

/// Takes effect at a set's next new member. An array already longer stays
/// one until then, and nothing turns back into an array.
///
/// No more than 2^30 has effect, as in Redis (`intsetMaxEntries`).
pub fn set_intset_max(limit: usize) {
    INTSET_MAX.store(limit.min(1 << 30), Ordering::Relaxed);
}

#[inline]
pub fn packed_entries() -> usize {
    #[cfg(test)]
    if let Some(limit) = own_limit(1) {
        return limit;
    }
    PACKED_ENTRIES.load(Ordering::Relaxed)
}

/// `set-max-listpack-entries`: no more than a block holds. Returns what was
/// kept, for `CONFIG GET` to report.
pub fn set_packed_entries(limit: usize) -> usize {
    let limit = limit.min(packed_set::MAX_ENTRIES);
    PACKED_ENTRIES.store(limit, Ordering::Relaxed);
    limit
}

#[inline]
pub fn packed_value() -> usize {
    #[cfg(test)]
    if let Some(limit) = own_limit(2) {
        return limit;
    }
    PACKED_VALUE.load(Ordering::Relaxed)
}

/// `set-max-listpack-value`: no longer than a block holds. Returns what was
/// kept, for `CONFIG GET` to report.
pub fn set_packed_value(limit: usize) -> usize {
    let limit = limit.min(packed_set::MAX_LEN);
    PACKED_VALUE.store(limit, Ordering::Relaxed);
    limit
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
    if digits.len() <= 18 {
        // Eighteen digits cannot overflow: no checks but that each is one.
        let mut n: u64 = 0;
        for &d in digits {
            let d = d.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            n = n * 10 + d as u64;
        }
        let n = n as i64;
        return Some(if negative { -n } else { n });
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
    value: i64,
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
            value,
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf[self.start as usize..]
    }

    #[inline]
    pub fn value(&self) -> i64 {
        self.value
    }
}

/// The length of `n`'s text.
fn int_text_len(n: i64) -> usize {
    let digits = n.unsigned_abs().checked_ilog10().unwrap_or(0) as usize + 1;
    digits + (n < 0) as usize
}

/// A member, as a collection hands it out: shared bytes (a sorted set's),
/// borrowed bytes (a block's, or a table entry's), a table's member of up
/// to seven bytes copied out of its slot, or an array's integer as text.
/// Each is its bytes (`Deref`).
pub enum Member<'a> {
    Shared(&'a Arc<[u8]>),
    Bytes(&'a [u8]),
    Inline([u8; INLINE_MEMBER], u8),
    Int(IntText),
}

impl Member<'_> {
    /// The member as shared bytes: the sorted set's own, or a new copy.
    pub fn to_shared(&self) -> Arc<[u8]> {
        match self {
            Member::Shared(bytes) => Arc::clone(bytes),
            _ => Arc::from(&**self),
        }
    }
}

impl<'a> From<MemberRef<'a>> for Member<'a> {
    #[inline]
    fn from(member: MemberRef<'a>) -> Self {
        match member {
            MemberRef::Bytes(bytes) => Member::Bytes(bytes),
            MemberRef::Inline(bytes, len) => Member::Inline(bytes, len),
        }
    }
}

impl Deref for Member<'_> {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        match self {
            Member::Shared(bytes) => bytes,
            Member::Bytes(bytes) => bytes,
            Member::Inline(bytes, len) => &bytes[..usize::from(*len)],
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

/// A set of byte strings.
#[derive(Clone)]
pub struct MemberSet {
    repr: Repr,
}

// Kept inline in every set's shared block: no bigger than it was before the
// table (#764), so that no set grows. The table's header (80 bytes) is
// boxed: one block a large set.
const _: () = assert!(std::mem::size_of::<MemberSet>() <= 64);

/// Two levels, so that the box's one spare value of its own (a null
/// pointer) can tell it from the other two.
#[derive(Clone)]
enum Repr {
    Small(Small),
    Table(Box<Table>),
}

#[derive(Clone)]
enum Small {
    /// The members that `with_capacity` said to expect (0: not said): a set
    /// that leaves the array goes where that many fit, not only those held.
    Ints(Ints, u32),
    Packed(PackedSet),
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

/// What a set is kept as, by Redis's names for its encodings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Encoding {
    Intset,
    Listpack,
    Hashtable,
}

impl Encoding {
    pub fn name(self) -> &'static str {
        match self {
            Encoding::Intset => "intset",
            Encoding::Listpack => "listpack",
            Encoding::Hashtable => "hashtable",
        }
    }
}

impl Default for MemberSet {
    fn default() -> Self {
        Self::new()
    }
}

impl MemberSet {
    /// An empty set. It is an empty array, which allocates nothing: its
    /// first member decides where it goes.
    pub fn new() -> Self {
        Self {
            repr: Repr::Small(Small::Ints(Ints::W16(Vec::new()), 0)),
        }
    }

    /// An empty set that is to hold about `members`: the table at once if
    /// that is more than an array or a block would hold.
    pub fn with_capacity(members: usize) -> Self {
        if members > intset_max().max(packed_entries()) {
            return Self {
                repr: Repr::Table(Table::with_capacity(members)),
            };
        }
        Self {
            repr: Repr::Small(Small::Ints(Ints::W16(Vec::new()), members as u32)),
        }
    }

    /// A set of `members`, which may repeat, in the form they need, chosen
    /// from all of them at once: no array or block is filled only to be
    /// turned into a table.
    pub fn from_shared(members: Vec<Arc<[u8]>>) -> Self {
        let n = members.len();
        if n == 0 {
            return Self::new();
        }
        if n <= intset_max() {
            let numbers: Option<Vec<i64>> = members.iter().map(|m| canonical_i64(m)).collect();
            if let Some(mut numbers) = numbers {
                numbers.sort_unstable();
                numbers.dedup();
                return Self {
                    repr: Repr::Small(Small::Ints(Ints::from_sorted(&numbers), 0)),
                };
            }
        }
        if n <= packed_entries() && members.iter().all(|m| m.len() <= packed_value()) {
            let mut block = PackedSet::new();
            for m in &members {
                block.insert(m);
            }
            return Self {
                repr: Repr::Small(Small::Packed(block)),
            };
        }
        let mut table = Table::with_capacity(n);
        for m in members {
            table.insert(&m);
        }
        // Repeats taken out, few enough for a block after all.
        let len = table.members.len();
        if len < n
            && len <= packed_entries()
            && table.members.iter().all(|m| m.len() <= packed_value())
        {
            let mut block = PackedSet::new();
            for m in table.members.iter() {
                block.push(&m);
            }
            return Self {
                repr: Repr::Small(Small::Packed(block)),
            };
        }
        Self {
            repr: Repr::Table(table),
        }
    }

    /// An empty table with room for `members`, however few: for a set to be
    /// filled with an unknown number of members that may be many, then put
    /// in the form it needs ([`Self::into_fitting`]).
    pub fn new_table(members: usize) -> Self {
        Self {
            repr: Repr::Table(Table::with_capacity(members)),
        }
    }

    /// A table that a block or an array would hold, made that (as
    /// [`Self::from_shared`] chooses); a table with room for over four
    /// times its members, made again at its size; any other set as it is.
    pub fn into_fitting(self) -> Self {
        let Repr::Table(t) = &self.repr else {
            return self;
        };
        let n = t.members.len();
        if n <= intset_max() {
            let numbers: Option<Vec<i64>> = t.members.iter().map(|m| canonical_i64(&m)).collect();
            if let Some(mut numbers) = numbers {
                numbers.sort_unstable();
                return Self {
                    repr: Repr::Small(Small::Ints(Ints::from_sorted(&numbers), 0)),
                };
            }
        }
        if n <= packed_entries() && t.members.iter().all(|m| m.len() <= packed_value()) {
            let mut block = PackedSet::new();
            for m in t.members.iter() {
                block.push(&m);
            }
            return Self {
                repr: Repr::Small(Small::Packed(block)),
            };
        }
        if t.members.capacity() > 4 * n {
            let mut table = Table::with_capacity(n);
            for m in t.members.iter() {
                table.push_new(&m);
            }
            return Self {
                repr: Repr::Table(table),
            };
        }
        self
    }

    /// What the set is kept as (`OBJECT ENCODING`).
    #[inline]
    pub fn encoding(&self) -> Encoding {
        match &self.repr {
            Repr::Small(Small::Ints(..)) => Encoding::Intset,
            Repr::Small(Small::Packed(_)) => Encoding::Listpack,
            Repr::Table(_) => Encoding::Hashtable,
        }
    }

    /// Whether the members are kept as an array of integers.
    pub fn is_compact(&self) -> bool {
        matches!(self.repr, Repr::Small(Small::Ints(..)))
    }

    /// Whether the members are a table. A set in one block or an array is
    /// read whole by `SSCAN`, as Redis reads a listpack or an intset.
    #[inline]
    pub fn is_table(&self) -> bool {
        matches!(self.repr, Repr::Table(_))
    }

    /// Where the array's members are; `None` for the others.
    #[cfg(test)]
    pub(crate) fn array_ptr(&self) -> Option<*const u8> {
        match &self.repr {
            Repr::Small(Small::Ints(ints, _)) => Some(each_width!(ints, v => v.as_ptr().cast())),
            _ => None,
        }
    }

    /// What an array or a block has allocated, room to spare included: the
    /// whole of the set's memory but its own header. `None` for the table.
    pub fn compact_bytes(&self) -> Option<usize> {
        match &self.repr {
            Repr::Small(Small::Ints(ints, _)) => Some(each_width!(ints, v => allocated(v))),
            Repr::Small(Small::Packed(block)) => Some(block.allocation_size()),
            Repr::Table(_) => None,
        }
    }

    /// How much work freeing the set is, as Redis's `lazyfreeGetFreeEffort`
    /// counts it: an array or a block is one allocation, a table one per
    /// member.
    pub fn free_effort(&self) -> usize {
        match &self.repr {
            Repr::Table(t) => t.members.len(),
            _ => 1,
        }
    }

    /// Bytes that freeing the set gives back, at least (for eviction):
    /// exactly what it holds, in every form (`per_member` is kept for the
    /// callers that pass one for every type).
    pub fn freeing_at_least(&self, _per_member: usize) -> usize {
        self.allocation_size()
    }

    #[inline]
    pub fn len(&self) -> usize {
        match &self.repr {
            Repr::Table(t) => t.members.len(),
            Repr::Small(Small::Packed(block)) => block.len(),
            Repr::Small(Small::Ints(ints, _)) => ints.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Members there is room for without growing.
    pub fn capacity(&self) -> usize {
        match &self.repr {
            Repr::Table(t) => t.members.capacity(),
            Repr::Small(Small::Packed(block)) => block.len(),
            Repr::Small(Small::Ints(ints, _)) => ints.capacity(),
        }
    }

    /// Everything the set holds but its own header, exactly as `used_memory`
    /// counts it, in O(1): for the table its box, group arrays, overflow
    /// groups and members' entries at their pool class's size
    /// ([`CollectionTable::allocation_size`]); for an array or a block, what
    /// it has allocated, room to spare included. For `MEMORY USAGE`.
    pub fn allocation_size(&self) -> usize {
        match &self.repr {
            Repr::Table(t) => std::mem::size_of::<Table>() + t.members.allocation_size(),
            Repr::Small(Small::Packed(block)) => block.allocation_size(),
            Repr::Small(Small::Ints(ints, _)) => each_width!(ints, v => allocated(v)),
        }
    }

    #[inline]
    pub fn contains(&self, member: &[u8]) -> bool {
        match &self.repr {
            Repr::Table(t) => t.members.contains(member),
            Repr::Small(Small::Packed(block)) => block.contains(member),
            Repr::Small(Small::Ints(ints, _)) => {
                canonical_i64(member).is_some_and(|n| ints.find(n).is_ok())
            }
        }
    }

    /// Whether `member`, of another set, is one of these: an integer is
    /// looked for in an array as the number it is, without reading its
    /// text again.
    #[inline]
    pub fn contains_member(&self, member: &Member<'_>) -> bool {
        match &self.repr {
            Repr::Table(t) => t.members.contains(member),
            Repr::Small(Small::Packed(block)) => block.contains(member),
            Repr::Small(Small::Ints(ints, _)) => match member {
                Member::Int(n) => ints.find(n.value()).is_ok(),
                _ => canonical_i64(member).is_some_and(|n| ints.find(n).is_ok()),
            },
        }
    }

    #[inline]
    pub fn get(&self, member: &[u8]) -> Option<Member<'_>> {
        match &self.repr {
            Repr::Table(t) => t.members.get(member).map(Member::from),
            Repr::Small(Small::Packed(block)) => {
                block.find(member).map(|i| Member::Bytes(block.get(i)))
            }
            Repr::Small(Small::Ints(ints, _)) => {
                let n = canonical_i64(member)?;
                ints.find(n).ok().map(|_| Member::Int(IntText::new(n)))
            }
        }
    }

    /// Adds `member`. False if it was there already.
    pub fn insert(&mut self, member: Arc<[u8]>) -> bool {
        self.insert_bytes(&member)
    }

    /// Adds `member`, copying its bytes in. False if it was there already.
    #[inline]
    pub fn insert_bytes(&mut self, member: &[u8]) -> bool {
        match &mut self.repr {
            Repr::Table(t) => t.insert(member),
            _ => self.insert_compact(member),
        }
    }

    /// Adds a member read from a snapshot, the AOF's base, a replication
    /// stream or a `RESTORE` payload. A member too long for the table
    /// ([`crate::collection_table::MAX_LEN`]; a command's argument is far
    /// shorter) is an error, never dropped: the load fails.
    pub fn insert_loaded(&mut self, member: &[u8]) -> std::io::Result<bool> {
        if member.len() > crate::collection_table::MAX_LEN {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "a set member of {} bytes: longer than the {} a set holds",
                    member.len(),
                    crate::collection_table::MAX_LEN
                ),
            ));
        }
        Ok(self.insert_bytes(member))
    }

    /// Adds a member of another set.
    #[inline]
    pub fn insert_member(&mut self, member: &Member<'_>) -> bool {
        self.insert_bytes(member)
    }

    /// The insert of an array or a block.
    fn insert_compact(&mut self, member: &[u8]) -> bool {
        match &mut self.repr {
            Repr::Small(Small::Packed(block)) => {
                if block.find(member).is_some() {
                    return false;
                }
                if block.len() < packed_entries() && member.len() <= packed_value() {
                    block.push(member);
                    return true;
                }
                self.block_to_table(1);
            }
            Repr::Small(Small::Ints(ints, expect)) => {
                let number = canonical_i64(member);
                if let Some(n) = number {
                    match ints.find(n) {
                        Ok(_) => return false,
                        Err(at) if ints.len() < intset_max() => {
                            ints.insert_at(at, n);
                            return true;
                        }
                        Err(_) => {}
                    }
                }
                // New (an array holds no other text), and the array cannot
                // take it: the block if all of them fit there, else the
                // table.
                let n = ints.len();
                let want = (n + 1).max(*expect as usize);
                let longest = match n {
                    0 => 0,
                    _ => int_text_len(ints.get(0)).max(int_text_len(ints.get(n - 1))),
                };
                let fits = want <= packed_entries()
                    && member.len() <= packed_value()
                    && longest <= packed_value();
                if fits {
                    let mut block = PackedSet::new();
                    ints.each(|n| block.push(IntText::new(n).as_bytes()));
                    block.push(member);
                    self.repr = Repr::Small(Small::Packed(block));
                    return true;
                }
                let mut table = Table::with_capacity(want);
                ints.each(|n| table.push_new(IntText::new(n).as_bytes()));
                self.repr = Repr::Table(table);
            }
            Repr::Table(_) => unreachable!("a table inserts by itself"),
        }
        let Repr::Table(table) = &mut self.repr else {
            unreachable!()
        };
        table.push_new(member);
        true
    }

    /// The block made a table, in one pass, with room for `more`.
    fn block_to_table(&mut self, more: usize) {
        let Repr::Small(Small::Packed(block)) = &self.repr else {
            return;
        };
        let mut table = Table::with_capacity(block.len() + more);
        for m in block.iter() {
            table.push_new(m);
        }
        self.repr = Repr::Table(table);
    }

    /// Removes `member`. False if it was not there.
    #[inline]
    pub fn remove(&mut self, member: &[u8]) -> bool {
        match &mut self.repr {
            Repr::Table(t) => t.members.remove(member),
            Repr::Small(Small::Packed(block)) => block.remove(member),
            Repr::Small(Small::Ints(ints, _)) => {
                match canonical_i64(member).map(|n| ints.find(n)) {
                    Some(Ok(at)) => {
                        ints.remove_at(at);
                        true
                    }
                    _ => false,
                }
            }
        }
    }

    /// Removes `member` and hands it back.
    pub fn take(&mut self, member: &[u8]) -> Option<Arc<[u8]>> {
        match &mut self.repr {
            Repr::Table(t) => t.members.remove_with(member, |m| Arc::from(&*m)),
            Repr::Small(Small::Packed(block)) => {
                let at = block.find(member)?;
                block.remove_at(at);
                Some(Arc::from(member))
            }
            Repr::Small(Small::Ints(ints, _)) => {
                let n = canonical_i64(member)?;
                let at = ints.find(n).ok()?;
                ints.remove_at(at);
                Some(Arc::from(member))
            }
        }
    }

    pub fn clear(&mut self) {
        match &mut self.repr {
            Repr::Table(t) => t.members.clear(),
            Repr::Small(Small::Packed(block)) => block.clear(),
            Repr::Small(Small::Ints(ints, _)) => *ints = Ints::W16(Vec::new()),
        }
    }

    pub fn reserve(&mut self, more: usize) {
        // An array or a block grows as it is added to: it may never need
        // the room.
        if let Repr::Table(t) = &mut self.repr {
            t.members.reserve(more);
        }
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&[u8]) -> bool) {
        match &mut self.repr {
            // A table shrinks as it empties, a few members a write.
            Repr::Table(t) => t.members.retain(keep),
            Repr::Small(Small::Packed(block)) => block.retain(keep),
            Repr::Small(Small::Ints(ints, _)) => ints.retain(|n| keep(IntText::new(n).as_bytes())),
        }
    }

    /// Every member's bytes. The form is looked at once, not once a member:
    /// the loop of `SMEMBERS`, a snapshot and the digest.
    #[inline]
    pub fn for_each(&self, mut f: impl FnMut(&[u8])) {
        match &self.repr {
            Repr::Table(t) => t.members.iter().for_each(|m| f(&m)),
            Repr::Small(Small::Packed(block)) => block.iter().for_each(f),
            Repr::Small(Small::Ints(ints, _)) => {
                each_width!(ints, v => v.iter().for_each(|&n| f(IntText::new(wide(n)).as_bytes())))
            }
        }
    }

    /// Every member, until `f` says to stop. The form is looked at once,
    /// outside the loop: over a table this is the loop over its slots.
    #[inline]
    pub fn try_each<B>(&self, mut f: impl FnMut(Member<'_>) -> ControlFlow<B>) -> ControlFlow<B> {
        match &self.repr {
            Repr::Table(t) => {
                for m in t.members.iter() {
                    f(Member::from(m))?;
                }
            }
            Repr::Small(Small::Packed(block)) => {
                for m in block.iter() {
                    f(Member::Bytes(m))?;
                }
            }
            Repr::Small(Small::Ints(ints, _)) => each_width!(ints, v => {
                for &n in v.iter() {
                    f(Member::Int(IntText::new(wide(n))))?;
                }
            }),
        }
        ControlFlow::Continue(())
    }

    /// The members of this set that `other` holds too (`common`), or that it
    /// does not (`!common`), when both are arrays or both are blocks: two
    /// sorted arrays are merged; a block's members are looked up in the
    /// other block by the fingerprints they were filed under, so none is
    /// computed. `None` for any other pair, which goes member by member.
    pub fn try_each_against<B>(
        &self,
        other: &MemberSet,
        common: bool,
        mut f: impl FnMut(Member<'_>) -> ControlFlow<B>,
    ) -> Option<ControlFlow<B>> {
        match (&self.repr, &other.repr) {
            (Repr::Small(Small::Ints(a, _)), Repr::Small(Small::Ints(b, _))) => {
                let (mut i, mut j) = (0, 0);
                let (la, lb) = (a.len(), b.len());
                while i < la {
                    let x = a.get(i);
                    while j < lb && b.get(j) < x {
                        j += 1;
                    }
                    let there = j < lb && b.get(j) == x;
                    if there == common
                        && let ControlFlow::Break(stop) = f(Member::Int(IntText::new(x)))
                    {
                        return Some(ControlFlow::Break(stop));
                    }
                    if common && j == lb {
                        break;
                    }
                    i += 1;
                }
                Some(ControlFlow::Continue(()))
            }
            (Repr::Small(Small::Packed(a)), Repr::Small(Small::Packed(b))) => {
                for i in 0..a.len() {
                    let m = a.get(i);
                    if b.find_fingerprinted(m, a.fingerprint_at(i)).is_some() == common
                        && let ControlFlow::Break(stop) = f(Member::Bytes(m))
                    {
                        return Some(ControlFlow::Break(stop));
                    }
                }
                Some(ControlFlow::Continue(()))
            }
            _ => None,
        }
    }

    pub fn iter(&self) -> Iter<'_> {
        match &self.repr {
            Repr::Small(Small::Ints(Ints::W16(v), _)) => Iter::W16(v.iter()),
            Repr::Small(Small::Ints(Ints::W32(v), _)) => Iter::W32(v.iter()),
            Repr::Small(Small::Ints(Ints::W64(v), _)) => Iter::W64(v.iter()),
            Repr::Small(Small::Packed(block)) => Iter::Packed(block.iter()),
            Repr::Table(t) => Iter::Table(t.members.iter()),
        }
    }

    /// A member, any one as likely as another.
    pub fn random(&self, rng: &mut impl rand::Rng) -> Option<Member<'_>> {
        match &self.repr {
            Repr::Table(t) => t.members.random(rng).map(Member::from),
            Repr::Small(Small::Packed(block)) if block.is_empty() => None,
            Repr::Small(Small::Packed(block)) => {
                Some(Member::Bytes(block.get(rng.gen_range(0..block.len()))))
            }
            Repr::Small(Small::Ints(ints, _)) if ints.len() == 0 => None,
            Repr::Small(Small::Ints(ints, _)) => Some(Member::Int(IntText::new(
                ints.get(rng.gen_range(0..ints.len())),
            ))),
        }
    }

    /// Removes a member, any one as likely as another.
    pub fn pop_random(&mut self, rng: &mut impl rand::Rng) -> Option<Arc<[u8]>> {
        let mut popped = None;
        self.pop_random_with(rng, |member| popped = Some(member.to_shared()));
        popped
    }

    /// Removes a member, any one as likely as another, and shows it to
    /// `f` first: a member of a block or an array is not copied out to be
    /// handed over. False if there was none.
    pub fn pop_random_with(
        &mut self,
        rng: &mut impl rand::Rng,
        f: impl FnOnce(Member<'_>),
    ) -> bool {
        match &mut self.repr {
            // One pick, shown to `f` on its way out: nothing copied.
            Repr::Table(t) => t
                .members
                .remove_random(rng, |member| f(Member::from(member)))
                .is_some(),
            Repr::Small(Small::Packed(block)) => {
                if block.is_empty() {
                    return false;
                }
                let at = rng.gen_range(0..block.len());
                f(Member::Bytes(block.get(at)));
                block.remove_at(at);
                true
            }
            Repr::Small(Small::Ints(ints, _)) => {
                if ints.len() == 0 {
                    return false;
                }
                let n = ints.remove_at(rng.gen_range(0..ints.len()));
                f(Member::Int(IntText::new(n)));
                true
            }
        }
    }

    /// Up to `count` different members, chosen at random, without removing
    /// them.
    ///
    /// A few of many (no more than a third) are picked one at a time,
    /// picking again when one comes up twice: O(count). More of them are
    /// chosen in one walk of the table (`choose_multiple`): O(len), which is
    /// then no more than three times the answer. A block or an array is
    /// asked for the chosen places only.
    pub fn random_distinct(&self, count: usize, rng: &mut impl rand::Rng) -> Vec<Member<'_>> {
        use rand::seq::IteratorRandom;
        let len = self.len();
        if count >= len {
            return self.iter().collect();
        }
        let t = match &self.repr {
            Repr::Small(Small::Ints(ints, _)) => {
                return rand::seq::index::sample(rng, len, count)
                    .into_iter()
                    .map(|i| Member::Int(IntText::new(ints.get(i))))
                    .collect();
            }
            Repr::Small(Small::Packed(block)) => {
                return rand::seq::index::sample(rng, len, count)
                    .into_iter()
                    .map(|i| Member::Bytes(block.get(i)))
                    .collect();
            }
            Repr::Table(t) if count * 3 <= len => t,
            _ => return self.iter().choose_multiple(rng, count),
        };
        // Each member is told apart by where it is: an entry by its address,
        // a member in its slot by its bytes (it has no other copy).
        let mut seen: ahash::AHashSet<(usize, u64)> = ahash::AHashSet::with_capacity(count);
        let mut picked = Vec::with_capacity(count);
        while picked.len() < count {
            let Some(member) = t.members.random(rng) else {
                break;
            };
            let identity = match member {
                MemberRef::Bytes(bytes) => (bytes.as_ptr() as usize, 0),
                MemberRef::Inline(bytes, len) => {
                    let mut word = [0u8; 8];
                    word[..INLINE_MEMBER].copy_from_slice(&bytes);
                    word[INLINE_MEMBER] = len;
                    (0, u64::from_le_bytes(word))
                }
            };
            if seen.insert(identity) {
                picked.push(Member::from(member));
            }
        }
        picked
    }

    /// The table, for `SSCAN`'s cursor (`table_scan`); `None` for an array
    /// or a block, which `SSCAN` returns whole.
    #[inline]
    pub fn table(&self) -> Option<&CollectionTable<Members>> {
        match &self.repr {
            Repr::Table(t) => Some(&t.members),
            Repr::Small(_) => None,
        }
    }

    /// Whether the background has work on this set (a resize to finish, or
    /// a finished one's array to free) that it has not been handed yet; if
    /// so, it is now marked handed. Near `maxmemory` the work is done here
    /// instead ([`Self::settle_if_tight`]) and nothing is handed over.
    #[inline]
    fn hand_over(&mut self) -> bool {
        let Repr::Table(t) = &mut self.repr else {
            return false;
        };
        if !(t.members.resizing() || t.members.holds_retired()) {
            return false;
        }
        if self.settle_if_tight() {
            return false;
        }
        let Repr::Table(t) = &mut self.repr else {
            return false;
        };
        let due = !t.queued;
        t.queued = true;
        due
    }

    /// Near `maxmemory`, finish a resize in progress and free the old
    /// arrays now, on this thread, as a table that grows all at once does
    /// (and as sets did before #764): the arrays a resize holds are there
    /// only until it ends, and must never be what makes the server refuse a
    /// write or evict a key. Returns whether it did. See [`tight`].
    #[cold]
    fn settle_if_tight(&mut self) -> bool {
        let Repr::Table(t) = &mut self.repr else {
            return false;
        };
        if !tight(t.members.transient_bytes()) {
            return false;
        }
        while !t.members.step_resize(usize::MAX) {}
        drop(t.members.take_retired());
        true
    }

    /// One step of the background's work on the set: about `budget` units
    /// of a resize in progress. Returns whether the set has no more for it,
    /// and the array a finished resize left, to be freed elsewhere.
    fn background_step(
        &mut self,
        budget: usize,
    ) -> (bool, Option<crate::collection_table::Retired>) {
        let Repr::Table(t) = &mut self.repr else {
            return (true, None);
        };
        let finished = t.members.step_resize(budget);
        let retired = t.members.take_retired();
        if finished {
            t.queued = false;
        }
        (finished, retired)
    }

    /// Whether the set is handed to the background (tests).
    #[cfg(test)]
    pub(crate) fn is_queued(&self) -> bool {
        matches!(&self.repr, Repr::Table(t) if t.queued)
    }

    /// Finish whatever resize is in progress now, and free what it leaves
    /// (tests).
    #[cfg(test)]
    pub(crate) fn finish_resize(&mut self) {
        while !self.background_step(usize::MAX).0 {}
    }
}

// ── the background's part (#764 step 2) ───────────────────────────────────────
//
// A table grows and shrinks a few members a write (`WRITE_STEP`), so a set
// whose writes stop part-way through a resize would hold both arrays until
// its next write, maybe for ever. A write that leaves its set mid-resize, or
// with a finished resize's old array kept back (`defer_free`: freeing a big
// one took about a millisecond), hands the set to the background
// ([`after_write`]); the active-expire thread finishes the resize in bounded
// steps, each under the set's own write lock ([`step_background`]), and
// gives the old arrays to `lazyfree`.
//
// The background holds the sets weakly: a key deleted, overwritten,
// expired, evicted, flushed or moved meanwhile is not kept alive by it, and
// it never waits for a set's lock (`try_write`), so a reader that holds one
// for long delays only that set's resize, and nothing waits on the
// background. A set it finds itself the last holder of (its key gone while
// it held it) goes to `lazyfree` whole.

type Shared = Arc<RwLock<MemberSet>>;

/// Sets handed to the background, not yet done with.
static HANDED: Mutex<Vec<Weak<RwLock<MemberSet>>>> = Mutex::new(Vec::new());
/// How many sets [`HANDED`] holds, read without its lock.
static HANDED_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Units of a resize's moves done under one hold of a set's lock (a unit is
/// a group visited or a member moved). Finishing the growth of a 3.2M-member
/// set, 4,096 of them held the lock for up to 0.49 ms, 1,024 for up to 0.27
/// ms (a move into pages of the new array touched for the first time costs
/// a page fault), so a writer of that set waits no longer than that.
pub const BACKGROUND_STEP: usize = 1024;

/// After a write to `set` (whose write lock `inner` is): hands it to the
/// background if it has work there. A few instructions when not.
#[inline]
pub fn after_write(set: &Shared, inner: &mut MemberSet) {
    if inner.hand_over() {
        hand(Arc::downgrade(set));
    }
}

#[cold]
fn hand(set: Weak<RwLock<MemberSet>>) {
    let mut handed = HANDED.lock();
    handed.push(set);
    HANDED_COUNT.store(handed.len(), Ordering::Release);
}

/// `set` made a key's value: shared, and handed to the background if a
/// resize it went through while it was built is still in progress. What
/// every set made whole goes through (a load, `RESTORE`, a `*STORE`
/// command, a new key of `SADD`).
pub fn shared(mut set: MemberSet) -> Shared {
    let due = set.hand_over();
    let shared = Arc::new(RwLock::new(set));
    if due {
        hand(Arc::downgrade(&shared));
    }
    shared
}

/// Whether the arrays a resize holds (`transient` bytes) could take memory
/// past `maxmemory`: a limit is set and memory is within that many bytes of
/// it. Read only when a table holds such arrays.
fn tight(transient: usize) -> bool {
    let limit = crate::RUNTIME_MAXMEMORY.load(Ordering::Relaxed);
    limit != 0 && crate::memory_use::counted().saturating_add(transient) >= limit
}

/// The process is over `maxmemory` (a command's check): finish every resize
/// handed to the background and free what they leave, now, so that only
/// what the sets settle at is counted when the check is made again. A set
/// whose lock is held is left (it is never waited for). Returns whether
/// anything was given back.
pub fn release_transients() -> bool {
    let sets = std::mem::take(&mut *HANDED.lock());
    let mut kept = Vec::with_capacity(sets.len());
    let mut released = false;
    for weak in sets {
        let Some(set) = weak.upgrade() else {
            continue;
        };
        match set.try_write() {
            Some(mut inner) => {
                if let Repr::Table(t) = &mut inner.repr {
                    released |= t.members.transient_bytes() > 0;
                    while !t.members.step_resize(usize::MAX) {}
                    drop(t.members.take_retired());
                    t.queued = false;
                }
            }
            None => kept.push(Arc::downgrade(&set)),
        }
        if let Some(last) = Arc::into_inner(set) {
            crate::lazyfree::free_later(last, 0);
        }
    }
    let mut handed = HANDED.lock();
    kept.append(&mut handed);
    *handed = kept;
    HANDED_COUNT.store(handed.len(), Ordering::Release);
    released
}

/// Whether any set waits for the background.
#[inline]
pub fn background_pending() -> bool {
    HANDED_COUNT.load(Ordering::Acquire) > 0
}

/// What one call of [`step_background`] did.
#[derive(Debug, Default, Clone, Copy)]
pub struct BackgroundWork {
    /// Steps taken, each under one hold of a set's lock.
    pub steps: usize,
    /// The longest a set's lock was held for one of them.
    pub longest_hold: std::time::Duration,
    /// Sets finished with.
    pub finished: usize,
}

/// Work on the sets handed to the background for about `budget` of time,
/// a [`BACKGROUND_STEP`] at a time, each under the set's write lock taken
/// without waiting. The sets left over are kept for the next call.
pub fn step_background(budget: std::time::Duration) -> BackgroundWork {
    let started = std::time::Instant::now();
    let mut work = BackgroundWork::default();
    let sets = std::mem::take(&mut *HANDED.lock());
    let mut kept = Vec::with_capacity(sets.len());
    let mut sets = sets.into_iter();
    for weak in sets.by_ref() {
        if started.elapsed() >= budget {
            kept.push(weak);
            break;
        }
        // Gone (deleted, overwritten, flushed...): nothing to do.
        let Some(set) = weak.upgrade() else {
            continue;
        };
        let mut done = false;
        while started.elapsed() < budget {
            // A reader holds it (a long `SMEMBERS`, say): next time.
            let Some(mut inner) = set.try_write() else {
                break;
            };
            let hold = std::time::Instant::now();
            let (finished, retired) = inner.background_step(BACKGROUND_STEP);
            drop(inner);
            work.longest_hold = work.longest_hold.max(hold.elapsed());
            work.steps += 1;
            if let Some(retired) = retired {
                // Near the limit, given back now: `lazyfree` frees it a
                // moment later, and the next command's check would count it.
                if tight(retired.bytes()) {
                    drop(retired);
                } else {
                    crate::lazyfree::free_later(retired, 0);
                }
            }
            if finished {
                done = true;
                break;
            }
        }
        // Let go of it without ever freeing it here. If its key went while
        // this held it (deleted, flushed, `UNLINK`ed...), this is the last
        // holder, and the set goes to `lazyfree` whole: `into_inner` tells
        // that atomically, where a count read first could see another
        // holder that lets go just after, leaving the free to this thread.
        if let Some(last) = Arc::into_inner(set) {
            crate::lazyfree::free_later(last, 0);
            continue;
        }
        if done {
            work.finished += 1;
        } else {
            kept.push(weak);
        }
    }
    kept.extend(sets);
    let mut handed = HANDED.lock();
    // Sets handed over while these were worked on come after them.
    kept.append(&mut handed);
    *handed = kept;
    HANDED_COUNT.store(handed.len(), Ordering::Release);
    work
}

/// The members of an array, in ascending order, at the width they need.
#[derive(Clone)]
enum Ints {
    W16(Vec<i16>),
    W32(Vec<i32>),
    W64(Vec<i64>),
}

#[inline]
fn search<T: Ord + Copy>(v: &[T], n: T) -> Result<usize, usize> {
    v.binary_search(&n)
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
    /// `numbers`, ascending and distinct, at the narrowest width.
    fn from_sorted(numbers: &[i64]) -> Self {
        let (Some(&low), Some(&high)) = (numbers.first(), numbers.last()) else {
            return Ints::W16(Vec::new());
        };
        if i16::try_from(low).is_ok() && i16::try_from(high).is_ok() {
            Ints::W16(numbers.iter().map(|&n| n as i16).collect())
        } else if i32::try_from(low).is_ok() && i32::try_from(high).is_ok() {
            Ints::W32(numbers.iter().map(|&n| n as i32).collect())
        } else {
            Ints::W64(numbers.to_vec())
        }
    }

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

    fn each(&self, mut f: impl FnMut(i64)) {
        each_width!(self, v => v.iter().for_each(|&n| f(wide(n))))
    }

    /// Where `n` is (`Ok`) or would go (`Err`).
    #[inline]
    fn find(&self, n: i64) -> Result<usize, usize> {
        match self {
            Ints::W16(v) => match i16::try_from(n) {
                Ok(n) => search(v, n),
                Err(_) => Err(if n < 0 { 0 } else { v.len() }),
            },
            Ints::W32(v) => match i32::try_from(n) {
                Ok(n) => search(v, n),
                Err(_) => Err(if n < 0 { 0 } else { v.len() }),
            },
            Ints::W64(v) => search(v, n),
        }
    }

    /// Puts `n` at `at` (from [`Ints::find`]), widening the array first if
    /// `n` does not fit. Room is made a quarter at a time, as a block's.
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
            if v.len() == v.capacity() {
                v.reserve_exact((v.len() / 4).max(4));
            }
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

/// The table (#764), boxed (its header is 80 bytes; a set's is 64), and
/// whether it has been handed to the background ([`after_write`]).
struct Table {
    members: CollectionTable<Members>,
    queued: bool,
}

impl Table {
    fn with_capacity(members: usize) -> Box<Self> {
        let mut members = CollectionTable::with_capacity(members);
        // A finished resize's old array is freed by the background, not by
        // the write that finished it.
        members.defer_free(true);
        Box::new(Table {
            members,
            queued: false,
        })
    }

    /// Adds `member`; false if it was there. A member is at most
    /// `collection_table::MAX_LEN` (4 GiB) long: a command's argument is at
    /// most 512 MB, and nothing longer is ever offered.
    #[inline]
    fn insert(&mut self, member: &[u8]) -> bool {
        self.members.insert(member).unwrap_or(false)
    }

    /// Adds a member known not to be one.
    fn push_new(&mut self, member: &[u8]) {
        self.insert(member);
    }
}

impl Clone for Table {
    /// A table of its own (`COPY`): every member copied, nothing shared.
    fn clone(&self) -> Self {
        let mut members = self.members.clone();
        members.defer_free(true);
        Table {
            members,
            queued: false,
        }
    }
}

/// The members of a set, in no particular order (an array's ascending, a
/// block's in the order they were added).
pub enum Iter<'a> {
    W16(std::slice::Iter<'a, i16>),
    W32(std::slice::Iter<'a, i32>),
    W64(std::slice::Iter<'a, i64>),
    Packed(packed_set::Iter<'a>),
    Table(MemberIter<'a>),
}

impl<'a> Iterator for Iter<'a> {
    type Item = Member<'a>;

    #[inline]
    fn next(&mut self) -> Option<Member<'a>> {
        let n = match self {
            Iter::Table(it) => return it.next().map(Member::from),
            Iter::Packed(it) => return it.next().map(Member::Bytes),
            Iter::W16(it) => *it.next()? as i64,
            Iter::W32(it) => *it.next()? as i64,
            Iter::W64(it) => *it.next()?,
        };
        Some(Member::Int(IntText::new(n)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Iter::W16(it) => it.size_hint(),
            Iter::W32(it) => it.size_hint(),
            Iter::W64(it) => it.size_hint(),
            Iter::Packed(it) => it.size_hint(),
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
            // Each member a copy of its own: the table's entries are its.
            Repr::Table(t) => IntoIter(Handed::Table(
                t.members
                    .iter()
                    .map(|m| Arc::from(&*m))
                    .collect::<Vec<_>>()
                    .into_iter(),
            )),
            Repr::Small(Small::Packed(block)) => IntoIter(Handed::Packed(block, 0)),
            Repr::Small(Small::Ints(ints, _)) => IntoIter(Handed::Ints(ints, 0)),
        }
    }
}

/// The members of a set, handed over: a block's or an array's each a new
/// shared copy.
pub struct IntoIter(Handed);

enum Handed {
    Ints(Ints, usize),
    Packed(PackedSet, usize),
    Table(std::vec::IntoIter<Arc<[u8]>>),
}

impl Iterator for IntoIter {
    type Item = Arc<[u8]>;
    fn next(&mut self) -> Option<Arc<[u8]>> {
        match &mut self.0 {
            Handed::Table(it) => it.next(),
            Handed::Packed(block, at) => {
                if *at >= block.len() {
                    return None;
                }
                let member = Arc::from(block.get(*at));
                *at += 1;
                Some(member)
            }
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
        let left = match &self.0 {
            Handed::Table(it) => return it.size_hint(),
            Handed::Packed(block, at) => block.len() - *at,
            Handed::Ints(ints, at) => ints.len() - *at,
        };
        (left, Some(left))
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
    /// The form is chosen from all the members at once (see
    /// [`MemberSet::from_shared`]); a set known to be more than an array or
    /// a block holds is made the table at once.
    fn from_iter<I: IntoIterator<Item = Arc<[u8]>>>(members: I) -> Self {
        let members = members.into_iter();
        let (at_least, _) = members.size_hint();
        if at_least > intset_max().max(packed_entries()) {
            let mut set = Self::with_capacity(at_least);
            set.extend(members);
            return set;
        }
        Self::from_shared(members.collect())
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
            ("999999999999999999", Some(999_999_999_999_999_999)),
            ("-999999999999999999", Some(-999_999_999_999_999_999)),
            ("1000000000000000000", Some(1_000_000_000_000_000_000)),
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
            ("1/", None),
            ("1:", None),
            ("", None),
            ("-", None),
            ("99999999999999999999", None),
        ] {
            assert_eq!(canonical_i64(text.as_bytes()), want, "{text:?}");
            if let Some(n) = want {
                assert_eq!(IntText::new(n).as_bytes(), text.as_bytes());
                assert_eq!(int_text_len(n), text.len());
            }
        }
    }

    #[test]
    fn integers_that_are_not_canonical_keep_their_bytes() {
        defaults();
        let mut set = MemberSet::new();
        for m in ["1", "2", "-3", "0"] {
            assert!(set.insert_bytes(m.as_bytes()));
        }
        assert!(set.is_compact());
        // "01" is not "1": a member of its own, and the end of the array.
        assert!(!set.contains(b"01"));
        assert!(set.insert_bytes(b"01"));
        assert!(!set.is_compact());
        assert_eq!(set.encoding(), Encoding::Listpack);
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
        defaults();
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
    fn one_more_than_the_limit_leaves_the_array() {
        defaults();
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
        // Nothing turns back.
        let was = set.encoding();
        for i in 0..=limit {
            assert!(set.remove(i.to_string().as_bytes()));
        }
        assert!(set.is_empty() && set.encoding() == was);
        assert!(set.insert_bytes(b"1"));
        assert_eq!(set.encoding(), was);
    }

    #[test]
    fn removing_from_the_array() {
        defaults();
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
        defaults();
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

    // ── the block ───────────────────────────────────────────────────────────

    #[test]
    fn short_members_are_one_block_until_one_does_not_fit() {
        defaults();
        let mut set = MemberSet::new();
        assert!(set.insert_bytes(b"a"));
        assert_eq!(set.encoding(), Encoding::Listpack);
        for i in 1..packed_entries() {
            assert!(set.insert_bytes(format!("m{i}").as_bytes()));
        }
        assert_eq!(set.encoding(), Encoding::Listpack);
        assert_eq!(set.len(), packed_entries());
        // At the limit a member there is still found, and changes nothing.
        assert!(!set.insert_bytes(b"a"));
        assert_eq!(set.encoding(), Encoding::Listpack);
        assert!(set.insert_bytes(b"one more"));
        assert_eq!(set.encoding(), Encoding::Hashtable);
        assert_eq!(set.len(), packed_entries() + 1);
        assert!(set.contains(b"a") && set.contains(b"one more"));

        // A member too long for a block makes it the table at once.
        let mut set = MemberSet::new();
        set.insert_bytes(b"short");
        set.insert_bytes(&[b'x'; DEFAULT_PACKED_VALUE + 1]);
        assert_eq!(set.encoding(), Encoding::Hashtable);
        assert_eq!(set.len(), 2);
        // And straight from the array, too.
        let mut set = MemberSet::new();
        set.insert_bytes(b"1");
        set.insert_bytes(&[b'x'; DEFAULT_PACKED_VALUE + 1]);
        assert_eq!(set.encoding(), Encoding::Hashtable);
        assert!(set.contains(b"1"));
    }

    #[test]
    fn built_at_once_it_takes_the_form_it_would_have_grown_into() {
        defaults();
        let ints: Vec<Arc<[u8]>> = (0..100)
            .map(|i| Arc::from(i.to_string().as_bytes()))
            .collect();
        let strings: Vec<Arc<[u8]>> = (0..100).map(member).collect();
        let many: Vec<Arc<[u8]>> = (0..1000).map(member).collect();
        for (members, want) in [
            (ints, Encoding::Intset),
            (strings, Encoding::Listpack),
            (many, Encoding::Hashtable),
        ] {
            let mut grown = MemberSet::new();
            for m in &members {
                grown.insert(Arc::clone(m));
            }
            let mut doubled = members.clone();
            doubled.extend(members.iter().cloned());
            let built = MemberSet::from_shared(doubled);
            let collected: MemberSet = members.iter().cloned().collect();
            for set in [&grown, &built, &collected] {
                assert_eq!(set.encoding(), want);
                assert_eq!(set.len(), members.len());
                assert!(members.iter().all(|m| set.contains(m)));
            }
        }
        // Room asked for past every limit: the table from the start.
        assert_eq!(
            MemberSet::with_capacity(100_000).encoding(),
            Encoding::Hashtable
        );
    }

    #[test]
    fn a_block_pops_and_picks_any_member() {
        defaults();
        let mut rng = rand::thread_rng();
        let set: MemberSet = (0..10).map(member).collect();
        assert_eq!(set.encoding(), Encoding::Listpack);
        let mut times = std::collections::HashMap::new();
        for _ in 0..20_000 {
            *times
                .entry(set.random(&mut rng).unwrap().to_vec())
                .or_insert(0u32) += 1;
        }
        assert_eq!(times.len(), 10);
        assert!(
            times.values().all(|n| (1500..2500).contains(n)),
            "{times:?}"
        );
        let picked = set.random_distinct(4, &mut rng);
        let different: std::collections::HashSet<&[u8]> = picked.iter().map(|m| &**m).collect();
        assert_eq!(different.len(), 4);
        let mut set = set;
        let mut popped = std::collections::HashSet::new();
        while let Some(m) = set.pop_random(&mut rng) {
            assert!(popped.insert(m));
        }
        assert_eq!(popped.len(), 10);
    }

    #[test]
    fn arrays_merge_and_blocks_look_up_by_fingerprint() {
        defaults();
        let a: MemberSet = (0..40)
            .map(|i| Arc::from((i * 3).to_string().as_bytes()))
            .collect();
        let b: MemberSet = (0..40)
            .map(|i| Arc::from((i * 5 - 100_000).to_string().as_bytes()))
            .collect();
        let c: MemberSet = (0..40)
            .map(|i| Arc::from((i * 5).to_string().as_bytes()))
            .collect();
        let mut common = Vec::new();
        let done = a.try_each_against(&c, true, |m| {
            common.push(m.to_vec());
            ControlFlow::<()>::Continue(())
        });
        assert_eq!(done, Some(ControlFlow::Continue(())));
        let want: Vec<Vec<u8>> = (0..120)
            .filter(|n| n % 15 == 0)
            .map(|n: i64| n.to_string().into_bytes())
            .collect();
        assert_eq!(common, want);
        let mut none = 0;
        a.try_each_against(&b, true, |_| {
            none += 1;
            ControlFlow::<()>::Continue(())
        });
        assert_eq!(none, 0);
        let strings: MemberSet = (0..4).map(member).collect();
        assert!(
            a.try_each_against(&strings, true, |_| ControlFlow::<()>::Continue(()))
                .is_none()
        );
        // And what one has that the other does not, merged likewise.
        let mut only = Vec::new();
        a.try_each_against(&c, false, |m| {
            only.push(m.to_vec());
            ControlFlow::<()>::Continue(())
        });
        let want: Vec<Vec<u8>> = (0..40)
            .map(|i: i64| i * 3)
            .filter(|n| n % 5 != 0)
            .map(|n| n.to_string().into_bytes())
            .collect();
        assert_eq!(only, want);
        // Two blocks, both ways.
        let x: MemberSet = (0..30).map(member).collect();
        let y: MemberSet = (20..60).map(member).collect();
        for common in [true, false] {
            let mut got = Vec::new();
            x.try_each_against(&y, common, |m| {
                got.push(m.to_vec());
                ControlFlow::<()>::Continue(())
            });
            let want: Vec<Vec<u8>> = (0..30)
                .filter(|i| (*i >= 20) == common)
                .map(|i| member(i).to_vec())
                .collect();
            assert_eq!(got, want, "common {common}");
        }
    }

    /// `random` and `pop_random` pick every member of an array and of a
    /// block as often as any other: a chi-square test, with bounds wide
    /// enough that a fair pick fails it about once in a million runs.
    #[test]
    fn random_picks_are_uniform_in_every_form() {
        defaults();
        let mut rng = rand::rngs::StdRng::seed_from_u64(0x0005_17e5);
        let ints: MemberSet = (0..20)
            .map(|i| Arc::from(i.to_string().as_bytes()))
            .collect();
        let block: MemberSet = (0..20).map(member).collect();
        let table: MemberSet = (0..600).map(member).collect();
        assert_eq!(ints.encoding(), Encoding::Intset);
        assert_eq!(block.encoding(), Encoding::Listpack);
        assert_eq!(table.encoding(), Encoding::Hashtable);
        // 19 degrees of freedom: P(chi2 > 60) is about 3e-6; 599: P(chi2 > 760) about 1e-6.
        for (set, bound, draws) in [
            (&ints, 60.0, 100_000),
            (&block, 60.0, 100_000),
            (&table, 760.0, 600_000),
        ] {
            let k = set.len();
            let chi2 = |counts: &std::collections::HashMap<Vec<u8>, u32>, total: u32| {
                assert_eq!(counts.len(), k, "a member never came up");
                let expected = total as f64 / k as f64;
                counts
                    .values()
                    .map(|&n| (n as f64 - expected).powi(2) / expected)
                    .sum::<f64>()
            };
            let mut counts = std::collections::HashMap::new();
            for _ in 0..draws {
                *counts
                    .entry(set.random(&mut rng).unwrap().to_vec())
                    .or_insert(0u32) += 1;
            }
            let x = chi2(&counts, draws);
            assert!(x < bound, "{:?} random: chi2 {x}", set.encoding());
            // The first member popped from a fresh copy, many times over.
            let pops = if k > 100 { 60_000 } else { 40_000 };
            let mut counts = std::collections::HashMap::new();
            for _ in 0..pops {
                let mut copy = set.clone();
                *counts
                    .entry(copy.pop_random(&mut rng).unwrap().to_vec())
                    .or_insert(0u32) += 1;
            }
            let x = chi2(&counts, pops);
            assert!(x < bound, "{:?} pop: chi2 {x}", set.encoding());
        }
    }

    /// A model: a long random run of adds, removes, lookups, pops and
    /// retains, over members that are integers, short strings and long
    /// ones, with the limits changed part-way, checked against a
    /// `HashSet` at every step. Every move between the forms is crossed.
    #[test]
    fn it_agrees_with_a_hash_set_across_every_form() {
        use std::collections::HashSet;
        let mut rng = rand::rngs::StdRng::seed_from_u64(0x5e7_5e7);
        let mut moves = HashSet::new();
        for round in 0..60 {
            // Limits of every size, Redis's among them.
            let (ints, entries, value) = match round % 6 {
                0 => (512, 128, 64),
                1 => (4, 8, 10),
                2 => (0, 16, 64),
                3 => (64, 0, 64),
                4 => (20, 128, 3),
                _ => (512, 32, 64),
            };
            let mut limits = [ints, entries, value];
            own_limits(Some(limits));
            let mut set = if round % 2 == 0 {
                MemberSet::new()
            } else {
                MemberSet::with_capacity(rng.gen_range(0..300))
            };
            let mut model: HashSet<Vec<u8>> = HashSet::new();
            let pool = rng.gen_range(5..400usize);
            let kinds = rng.gen_range(0..4u8);
            let pick = |rng: &mut rand::rngs::StdRng| -> Vec<u8> {
                let i = rng.gen_range(0..pool);
                match (kinds, rng.gen_range(0..10)) {
                    (0, _) | (1, 0..=8) => (i as i64 * 7919 - 1000).to_string().into_bytes(),
                    (1, _) | (2, 0..=8) => format!("m:{i}").into_bytes(),
                    (_, 0..=6) => format!("member:{i:08}").into_bytes(),
                    _ => format!("{i}{}", "x".repeat(i % 90)).into_bytes(),
                }
            };
            for step in 0..3000 {
                let before = set.encoding();
                match rng.gen_range(0..20) {
                    0..=8 => {
                        let m = pick(&mut rng);
                        let added = if step % 2 == 0 {
                            set.insert_bytes(&m)
                        } else {
                            set.insert(Arc::from(&m[..]))
                        };
                        assert_eq!(added, model.insert(m), "round {round} step {step}");
                    }
                    9..=12 => {
                        let m = pick(&mut rng);
                        assert_eq!(
                            set.remove(&m),
                            model.remove(&m),
                            "round {round} step {step}"
                        );
                    }
                    13..=15 => {
                        let m = pick(&mut rng);
                        assert_eq!(set.contains(&m), model.contains(&m));
                        assert_eq!(set.get(&m).map(|g| g.to_vec()), model.get(&m).cloned());
                    }
                    16 => {
                        if let Some(m) = set.pop_random(&mut rng) {
                            assert!(model.remove(&m[..]), "popped what was not there");
                        } else {
                            assert!(model.is_empty());
                        }
                    }
                    17 => {
                        let cut = rng.gen_range(0..10u8);
                        set.retain(|m| m.iter().map(|&b| b as u32).sum::<u32>() % 10 != cut as u32);
                        model.retain(|m| {
                            m.iter().map(|&b| b as u32).sum::<u32>() % 10 != cut as u32
                        });
                    }
                    18 => {
                        if let Some(m) = set.random(&mut rng) {
                            assert!(model.contains(&m[..]));
                        }
                    }
                    _ => {
                        // A limit changed under a live set.
                        limits[1] = rng.gen_range(0..=128);
                        own_limits(Some(limits));
                    }
                }
                assert_eq!(set.len(), model.len(), "round {round} step {step}");
                let after = set.encoding();
                moves.insert((before, after));
                // Never back.
                assert!(after as u8 >= before as u8, "{before:?} -> {after:?}");
                if step % 100 == 0 {
                    let held: HashSet<Vec<u8>> = set.iter().map(|m| m.to_vec()).collect();
                    assert_eq!(held, model, "round {round} step {step}");
                    let mut each = HashSet::new();
                    set.for_each(|m| {
                        each.insert(m.to_vec());
                    });
                    assert_eq!(each, model);
                }
            }
        }
        own_limits(None);
        use Encoding::*;
        for crossed in [
            (Intset, Listpack),
            (Intset, Hashtable),
            (Listpack, Hashtable),
        ] {
            assert!(moves.contains(&crossed), "never crossed {crossed:?}");
        }
    }

    // ── #764: the table and the background ──────────────────────────────────

    fn table_of(set: &Shared) -> (bool, bool, usize) {
        let inner = set.read();
        let t = inner.table().expect("a table");
        (t.resizing(), t.holds_retired(), inner.allocation_size())
    }

    /// Steps the background until `set` has nothing left there, as the
    /// expiry thread would; returns the longest hold of its lock.
    fn finish_in_background(set: &Shared) -> std::time::Duration {
        let mut longest = std::time::Duration::ZERO;
        for _ in 0..1_000_000 {
            let (resizing, retired, _) = table_of(set);
            if !resizing && !retired && !set.read().is_queued() {
                return longest;
            }
            let work = step_background(std::time::Duration::from_millis(1));
            longest = longest.max(work.longest_hold);
        }
        panic!("the background never finished the set");
    }

    /// A set whose writes stop just after a growth started (3M members,
    /// past the doubling of 2^19 groups) is finished by the background with
    /// no further write, a step at a time, and its old array freed; after
    /// most of it is removed, the shrinks the removes leave are finished
    /// too, and the memory comes back.
    #[test]
    fn the_background_finishes_a_resize_the_writes_left() {
        defaults();
        let n: usize = if cfg!(debug_assertions) {
            400_000
        } else {
            3_000_000
        };
        let set = shared(MemberSet::new());
        let mut i = 0usize;
        // Filled with no background meanwhile: as if its writes outran it.
        {
            let mut inner = set.write();
            while i < n || !inner.table().is_some_and(|t| t.resizing()) {
                inner.insert_bytes(format!("member:{i:010}").as_bytes());
                i += 1;
            }
            after_write(&set, &mut inner);
            assert!(inner.is_queued());
        }
        let (resizing, _, mid) = table_of(&set);
        assert!(resizing);
        let longest = finish_in_background(&set);
        let (_, _, done) = table_of(&set);
        assert!(done < mid, "{done} bytes after, {mid} during");
        assert_eq!(set.read().len(), i);
        eprintln!("growth to {i} members finished; longest hold {longest:?}");
        // Down to a twentieth: shrinks, which the background ends.
        {
            let mut inner = set.write();
            for j in 0..i {
                if j % 20 != 0 {
                    assert!(inner.remove(format!("member:{j:010}").as_bytes()));
                }
            }
            after_write(&set, &mut inner);
        }
        let longest = finish_in_background(&set);
        let (_, _, shrunk) = table_of(&set);
        let inner = set.read();
        assert_eq!(inner.len(), i.div_ceil(20));
        assert!(
            shrunk * 8 < done,
            "{shrunk} bytes after the shrink, {done} before"
        );
        // What it holds is what a table built for that many holds, give or
        // take the arrays' rounding: the old ones are gone.
        let mut fresh = MemberSet::new_table(inner.len());
        inner.for_each(|m| {
            fresh.insert_bytes(m);
        });
        assert!(
            shrunk <= 2 * fresh.allocation_size(),
            "{shrunk} against {}",
            fresh.allocation_size()
        );
        eprintln!("shrink finished; longest hold {longest:?}");
    }

    /// A set whose key went while it waited for the background is not kept
    /// alive by it, nor touched after it is freed; one held by a reader is
    /// left for later, never waited for.
    #[test]
    fn the_background_holds_sets_weakly_and_never_waits() {
        defaults();
        let grown = || {
            let set = shared(MemberSet::new());
            let mut inner = set.write();
            let mut i = 0;
            while i < 1000 || !inner.table().is_some_and(|t| t.resizing()) {
                inner.insert_bytes(format!("m{i}").as_bytes());
                i += 1;
            }
            after_write(&set, &mut inner);
            drop(inner);
            set
        };
        // Deleted while handed over: the background finds it gone.
        let gone = grown();
        let weak = Arc::downgrade(&gone);
        drop(gone);
        assert!(weak.upgrade().is_none());
        step_background(std::time::Duration::from_millis(50));
        assert!(
            !HANDED.lock().iter().any(|w| w.ptr_eq(&weak)),
            "a deleted set stayed handed over"
        );
        // Read-locked: skipped, kept for later, then finished.
        let held = grown();
        let reader = held.read();
        let work = step_background(std::time::Duration::from_millis(5));
        assert!(reader.table().unwrap().resizing());
        assert!(
            HANDED
                .lock()
                .iter()
                .any(|w| w.ptr_eq(&Arc::downgrade(&held)))
        );
        let _ = work;
        drop(reader);
        finish_in_background(&held);
        assert!(
            !HANDED
                .lock()
                .iter()
                .any(|w| w.ptr_eq(&Arc::downgrade(&held)))
        );
        // A set built mid-growth and made a key's value is handed over at
        // once (a load, `RESTORE`, a `*STORE` command).
        let mut built = MemberSet::new_table(0);
        let mut i = 0;
        while i < 1000 || !built.table().unwrap().resizing() {
            built.insert_bytes(format!("x{i}").as_bytes());
            i += 1;
        }
        let built = shared(built);
        assert!(built.read().is_queued());
        finish_in_background(&built);
        let mut copy = built.read().clone();
        copy.finish_resize();
        assert_eq!(copy.len(), i);
    }

    /// A table built for many that holds few is put in the form its
    /// members would have grown into, as `from_shared` chooses.
    #[test]
    fn a_table_that_came_out_small_fits_its_members() {
        defaults();
        for (members, want) in [
            (
                (0..100).map(|i| i.to_string()).collect::<Vec<_>>(),
                Encoding::Intset,
            ),
            (
                (0..100).map(|i| format!("m{i}")).collect(),
                Encoding::Listpack,
            ),
            (
                (0..600).map(|i| format!("m{i}")).collect(),
                Encoding::Hashtable,
            ),
        ] {
            let mut set = MemberSet::new_table(100_000);
            for m in &members {
                set.insert_bytes(m.as_bytes());
            }
            let set = set.into_fitting();
            assert_eq!(set.encoding(), want);
            assert_eq!(set.len(), members.len());
            // Made again at its size, not left with room for 100,000.
            assert!(set.capacity() <= 4 * set.len(), "{}", set.capacity());
            assert!(members.iter().all(|m| set.contains(m.as_bytes())));
        }
    }

    /// Redis's limits on this thread, whatever another test sets.
    fn defaults() {
        own_limits(Some([
            DEFAULT_INTSET_MAX,
            DEFAULT_PACKED_ENTRIES,
            DEFAULT_PACKED_VALUE,
        ]));
    }

    use rand::{Rng, SeedableRng};
}
