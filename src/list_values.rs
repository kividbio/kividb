//! The elements of a list (`LPUSH`, `RPUSH`), in order, kept in one of two
//! forms:
//!
//! - **Packed.** Elements end to end in chunks of up to
//!   [`packed_list::CHUNK_ENTRIES`] ([`PackedList`]): two bytes an element
//!   besides its own (four for one of 128 bytes or more), and one
//!   allocation a chunk. Every new list is packed.
//! - **Plain.** Each element a block of its own (`Arc<[u8]>`) in a
//!   `VecDeque`, as every list was before. A list becomes plain the first
//!   time an element longer than [`packed_list::MAX_LEN`] is pushed, set or
//!   inserted: its elements are moved over once, in order, and the write is
//!   done there. It never goes back; an emptied list is deleted, and the
//!   next is packed again.
//!
//! Callers see elements as [`Value`]s (bytes, whatever the form) and change
//! the list only through what is here: pushes and pops at either end, an
//! element by its index, ranges read either way, an insert by a pivot,
//! removal by value (`LREM`), a trim to a range (`LTRIM`). Elements popped
//! are handed to the caller as [`Popped`] for as long as the call lasts.
//! Snapshots, the AOF, replication and `DUMP` read elements as bytes, in
//! order: none of them sees the form, and none of their formats changed.

use std::collections::VecDeque;
use std::sync::Arc;

pub use crate::element::{Popped, Value};
use crate::packed_list::{self, PackedList};

/// A list's elements.
#[derive(Clone, Default)]
pub struct ListValues {
    repr: Repr,
}

#[derive(Clone)]
enum Repr {
    Packed(PackedList),
    /// Each element a block of its own.
    Plain(VecDeque<Arc<[u8]>>),
}

impl Default for Repr {
    fn default() -> Self {
        Repr::Packed(PackedList::new())
    }
}

/// How a list is kept, by Redis's names for its encodings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    /// One chunk.
    Listpack,
    /// Several chunks, or elements kept apart.
    Quicklist,
}

impl Encoding {
    pub fn name(self) -> &'static str {
        match self {
            Encoding::Listpack => "listpack",
            Encoding::Quicklist => "quicklist",
        }
    }
}

impl ListValues {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn len(&self) -> usize {
        match &self.repr {
            Repr::Packed(p) => p.len(),
            Repr::Plain(d) => d.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        match &self.repr {
            Repr::Packed(p) => p.is_empty(),
            Repr::Plain(d) => d.is_empty(),
        }
    }

    /// Whether the elements are packed in chunks.
    #[inline]
    pub fn is_packed(&self) -> bool {
        matches!(self.repr, Repr::Packed(_))
    }

    /// `OBJECT ENCODING`: `listpack` while the list is one chunk,
    /// `quicklist` once it is more, or its elements are kept apart.
    pub fn encoding(&self) -> Encoding {
        match &self.repr {
            Repr::Packed(p) if p.is_one_chunk() => Encoding::Listpack,
            _ => Encoding::Quicklist,
        }
    }

    /// The bytes of a packed list's allocations (its chunks, and the deque
    /// that holds them); `None` for a plain one.
    pub fn packed_bytes(&self) -> Option<usize> {
        match &self.repr {
            Repr::Packed(p) => Some(p.allocation_size()),
            Repr::Plain(_) => None,
        }
    }

    /// How much work freeing the list is, as Redis's
    /// `lazyfreeGetFreeEffort` counts it: a quicklist's nodes, a plain
    /// list's elements.
    pub fn free_effort(&self) -> usize {
        match &self.repr {
            Repr::Packed(p) => p.chunk_count(),
            Repr::Plain(d) => d.len(),
        }
    }

    /// The elements, moved over once into blocks of their own; with room
    /// for one more.
    #[cold]
    #[inline(never)]
    fn make_plain(&mut self) -> &mut VecDeque<Arc<[u8]>> {
        if let Repr::Packed(p) = &self.repr {
            let mut plain = VecDeque::with_capacity(p.len() + 1);
            plain.extend(p.iter().map(Arc::<[u8]>::from));
            self.repr = Repr::Plain(plain);
        }
        match &mut self.repr {
            Repr::Plain(d) => d,
            Repr::Packed(_) => unreachable!(),
        }
    }

    /// Room for `more` elements, where that means anything.
    #[inline]
    pub fn reserve(&mut self, more: usize) {
        if let Repr::Plain(d) = &mut self.repr {
            d.reserve(more);
        }
    }

    #[inline]
    pub fn push_back(&mut self, element: &[u8]) {
        match &mut self.repr {
            Repr::Packed(p) => {
                if !p.push_back(element) {
                    self.make_plain().push_back(Arc::from(element));
                }
            }
            Repr::Plain(d) => d.push_back(Arc::from(element)),
        }
    }

    #[inline]
    pub fn push_front(&mut self, element: &[u8]) {
        match &mut self.repr {
            Repr::Packed(p) => {
                if !p.push_front(element) {
                    self.make_plain().push_front(Arc::from(element));
                }
            }
            Repr::Plain(d) => d.push_front(Arc::from(element)),
        }
    }

    /// `RPUSH`: `elements` after the last, in order.
    pub fn extend_back(&mut self, elements: &[&[u8]]) {
        if let [element] = elements {
            return self.push_back(element);
        }
        let done = match &mut self.repr {
            Repr::Packed(p) => p.extend_back(elements),
            Repr::Plain(_) => 0,
        };
        if done < elements.len() {
            let d = self.make_plain();
            d.reserve(elements.len() - done);
            d.extend(elements[done..].iter().map(|e| Arc::<[u8]>::from(*e)));
        }
    }

    /// `LPUSH`: each of `elements` before the first, in turn, so the last
    /// of them ends up first.
    pub fn extend_front(&mut self, elements: &[&[u8]]) {
        if let [element] = elements {
            return self.push_front(element);
        }
        let done = match &mut self.repr {
            Repr::Packed(p) => p.extend_front(elements),
            Repr::Plain(_) => 0,
        };
        if done < elements.len() {
            let d = self.make_plain();
            for element in &elements[done..] {
                d.push_front(Arc::from(*element));
            }
        }
    }

    /// Add an element the caller holds as shared bytes: kept as they are
    /// where elements are kept apart, else its bytes.
    #[inline]
    pub fn push_back_shared(&mut self, element: Arc<[u8]>) {
        match &mut self.repr {
            Repr::Packed(p) => {
                if !p.push_back(&element) {
                    self.make_plain().push_back(element);
                }
            }
            Repr::Plain(d) => d.push_back(element),
        }
    }

    #[inline]
    pub fn push_front_shared(&mut self, element: Arc<[u8]>) {
        match &mut self.repr {
            Repr::Packed(p) => {
                if !p.push_front(&element) {
                    self.make_plain().push_front(element);
                }
            }
            Repr::Plain(d) => d.push_front(element),
        }
    }

    #[inline]
    pub fn front(&self) -> Option<Value<'_>> {
        match &self.repr {
            Repr::Packed(p) => p.front().map(Value::Bytes),
            Repr::Plain(d) => d.front().map(Value::Shared),
        }
    }

    #[inline]
    pub fn back(&self) -> Option<Value<'_>> {
        match &self.repr {
            Repr::Packed(p) => p.back().map(Value::Bytes),
            Repr::Plain(d) => d.back().map(Value::Shared),
        }
    }

    /// Take the first element off, handing it to `take`. `None`, and
    /// `take` not called, if the list is empty.
    #[inline]
    pub fn pop_front_with<R>(&mut self, take: impl FnOnce(Popped<'_>) -> R) -> Option<R> {
        match &mut self.repr {
            Repr::Packed(p) => {
                let taken = take(Popped::Bytes(p.front()?));
                p.drop_front();
                Some(taken)
            }
            Repr::Plain(d) => d.pop_front().map(|e| take(Popped::Shared(e))),
        }
    }

    /// Take the last element off, handing it to `take`.
    #[inline]
    pub fn pop_back_with<R>(&mut self, take: impl FnOnce(Popped<'_>) -> R) -> Option<R> {
        match &mut self.repr {
            Repr::Packed(p) => {
                let taken = take(Popped::Bytes(p.back()?));
                p.drop_back();
                Some(taken)
            }
            Repr::Plain(d) => d.pop_back().map(|e| take(Popped::Shared(e))),
        }
    }

    /// `LMOVE k k` / `RPOPLPUSH k k`: the element at one end moved to the
    /// other (`from_left` to `to_left`; the same end changes nothing),
    /// handed to `take` first. In a packed list its frame is moved within
    /// the list, copied once; in a plain one its block is. `None` if the
    /// list is empty.
    pub fn rotate<R>(
        &mut self,
        from_left: bool,
        to_left: bool,
        take: impl FnOnce(Value<'_>) -> R,
    ) -> Option<R> {
        match &mut self.repr {
            Repr::Packed(p) => {
                if from_left == to_left {
                    let element = if from_left { p.front() } else { p.back() };
                    return element.map(|e| take(Value::Bytes(e)));
                }
                p.rotate(!from_left, |e| take(Value::Bytes(e)))
            }
            Repr::Plain(d) => {
                if from_left == to_left {
                    let element = if from_left { d.front() } else { d.back() };
                    return element.map(|e| take(Value::Shared(e)));
                }
                let element = if from_left {
                    d.pop_front()?
                } else {
                    d.pop_back()?
                };
                let taken = take(Value::Shared(&element));
                if to_left {
                    d.push_front(element);
                } else {
                    d.push_back(element);
                }
                Some(taken)
            }
        }
    }

    /// The packed list's shape checked against its chunks (tests).
    #[cfg(test)]
    fn check_shape(&self) {
        if let Repr::Packed(p) = &self.repr {
            p.check_shape();
        }
    }

    /// Element `i`, if there is one.
    #[inline]
    pub fn get(&self, i: usize) -> Option<Value<'_>> {
        match &self.repr {
            Repr::Packed(p) => p.get(i).map(Value::Bytes),
            Repr::Plain(d) => d.get(i).map(Value::Shared),
        }
    }

    /// Put `element` in element `i`'s place. `false` if there is no
    /// element `i`.
    pub fn set(&mut self, i: usize, element: &[u8]) -> bool {
        if i >= self.len() {
            return false;
        }
        match &mut self.repr {
            Repr::Packed(p) if PackedList::takes(element) => p.set(i, element),
            _ => {
                self.make_plain()[i] = Arc::from(element);
                true
            }
        }
    }

    /// Put `element` before element `i`; after the last when `i == len()`.
    /// `false` if `i > len()`.
    pub fn insert(&mut self, i: usize, element: &[u8]) -> bool {
        if i > self.len() {
            return false;
        }
        match &mut self.repr {
            Repr::Packed(p) if PackedList::takes(element) => p.insert(i, element),
            _ => {
                self.make_plain().insert(i, Arc::from(element));
                true
            }
        }
    }

    /// The index of the first element equal to `element`.
    pub fn position(&self, element: &[u8]) -> Option<usize> {
        match &self.repr {
            Repr::Packed(p) => p.position(element),
            Repr::Plain(d) => d.iter().position(|v| v.as_ref() == element),
        }
    }

    /// `LREM`: take out the elements equal to `element`, `count` of them
    /// at most from the first on (`count > 0`), from the last back (`count
    /// < 0`), or all (`0`). Returns how many went.
    pub fn remove_matching(&mut self, element: &[u8], count: i64) -> usize {
        let limit = count.unsigned_abs() as usize;
        match &mut self.repr {
            Repr::Packed(p) => p.remove_matching(element, count),
            Repr::Plain(d) => {
                let mut removed = 0usize;
                if count == 0 {
                    d.retain(|v| {
                        if v.as_ref() == element {
                            removed += 1;
                            false
                        } else {
                            true
                        }
                    });
                } else if count > 0 {
                    let mut i = 0;
                    while i < d.len() && removed < limit {
                        if d[i].as_ref() == element {
                            d.remove(i);
                            removed += 1;
                        } else {
                            i += 1;
                        }
                    }
                } else {
                    let mut i = d.len();
                    while i > 0 && removed < limit {
                        i -= 1;
                        if d[i].as_ref() == element {
                            d.remove(i);
                            removed += 1;
                        }
                    }
                }
                removed
            }
        }
    }

    /// Keep the elements from `start` up to, not including, `end`, and
    /// nothing else. `start <= end <= len()`.
    pub fn retain_range(&mut self, start: usize, end: usize) {
        debug_assert!(start <= end && end <= self.len());
        match &mut self.repr {
            Repr::Packed(p) => p.keep_range(start, end),
            Repr::Plain(d) => {
                // From the back first, then the front.
                d.drain(end..);
                d.drain(..start);
            }
        }
    }

    pub fn clear(&mut self) {
        match &mut self.repr {
            Repr::Packed(p) => p.clear(),
            Repr::Plain(d) => d.clear(),
        }
    }

    /// Hand `each` the elements from `from` up to, not including, `to`,
    /// first to last, while it says to go on (`true`): cheaper than
    /// [`range`](Self::range) where every element is read. `false` if it
    /// said to stop. `from <= to <= len()`.
    #[inline]
    pub fn for_each_in(&self, from: usize, to: usize, mut each: impl FnMut(&[u8]) -> bool) -> bool {
        match &self.repr {
            Repr::Packed(p) => p.for_each_in(from, to, each),
            Repr::Plain(d) => d.range(from..to).all(|e| each(e)),
        }
    }

    /// As [`for_each_in`](Self::for_each_in), last to first.
    #[inline]
    pub fn for_each_in_rev(
        &self,
        from: usize,
        to: usize,
        mut each: impl FnMut(&[u8]) -> bool,
    ) -> bool {
        match &self.repr {
            Repr::Packed(p) => p.for_each_in_rev(from, to, each),
            Repr::Plain(d) => d.range(from..to).rev().all(|e| each(e)),
        }
    }

    /// Every element, first to last.
    #[inline]
    pub fn iter(&self) -> Iter<'_> {
        self.range(0, self.len())
    }

    /// The elements from `from` up to, not including, `to`, first to last.
    /// `from <= to <= len()`.
    #[inline]
    pub fn range(&self, from: usize, to: usize) -> Iter<'_> {
        match &self.repr {
            Repr::Packed(p) => Iter::Packed(p.range(from, to)),
            Repr::Plain(d) => Iter::Plain(d.range(from..to)),
        }
    }

    /// The elements from `from` up to, not including, `to`, last to first.
    #[inline]
    pub fn range_rev(&self, from: usize, to: usize) -> RevIter<'_> {
        match &self.repr {
            Repr::Packed(p) => RevIter::Packed(p.range_rev(from, to)),
            Repr::Plain(d) => RevIter::Plain(d.range(from..to).rev()),
        }
    }
}

/// A list's elements in order, as they are read: [`ListValues::builder`].
/// Bulk loads (a snapshot, `RESTORE`, `SORT ... STORE`) lay elements down
/// here one after another: full chunks, each exactly as long as it needs,
/// not as many pushes. The first element too long for a chunk moves what
/// was laid down so far into blocks of their own, once, and the rest
/// follow it there.
pub struct Builder {
    form: Form,
    hint: usize,
}

enum Form {
    Packed(packed_list::Builder),
    Plain(VecDeque<Arc<[u8]>>),
}

impl ListValues {
    /// A list to be filled in order, about `hint` elements long.
    pub fn builder(hint: usize) -> Builder {
        Builder {
            form: Form::Packed(packed_list::Builder::new()),
            hint,
        }
    }
}

impl Builder {
    #[cold]
    #[inline(never)]
    fn make_plain(&mut self) -> &mut VecDeque<Arc<[u8]>> {
        if let Form::Packed(b) = &mut self.form {
            let packed = std::mem::take(b).finish();
            let mut plain = VecDeque::with_capacity(self.hint.max(packed.len() + 1));
            plain.extend(packed.iter().map(Arc::<[u8]>::from));
            self.form = Form::Plain(plain);
        }
        match &mut self.form {
            Form::Plain(d) => d,
            Form::Packed(_) => unreachable!(),
        }
    }

    #[inline]
    pub fn push(&mut self, element: &[u8]) {
        match &mut self.form {
            Form::Packed(b) => {
                if !b.push(element) {
                    self.make_plain().push_back(Arc::from(element));
                }
            }
            Form::Plain(d) => d.push_back(Arc::from(element)),
        }
    }

    #[inline]
    pub fn push_shared(&mut self, element: Arc<[u8]>) {
        match &mut self.form {
            Form::Packed(b) => {
                if !b.push(&element) {
                    self.make_plain().push_back(element);
                }
            }
            Form::Plain(d) => d.push_back(element),
        }
    }

    pub fn finish(self) -> ListValues {
        ListValues {
            repr: match self.form {
                Form::Packed(b) => Repr::Packed(b.finish()),
                Form::Plain(d) => Repr::Plain(d),
            },
        }
    }
}

impl FromIterator<Arc<[u8]>> for ListValues {
    fn from_iter<I: IntoIterator<Item = Arc<[u8]>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = ListValues::builder(iter.size_hint().0);
        for element in iter {
            builder.push_shared(element);
        }
        builder.finish()
    }
}

impl<'a> FromIterator<&'a [u8]> for ListValues {
    fn from_iter<I: IntoIterator<Item = &'a [u8]>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = ListValues::builder(iter.size_hint().0);
        for element in iter {
            builder.push(element);
        }
        builder.finish()
    }
}

impl std::fmt::Debug for ListValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

/// Elements first to last.
pub enum Iter<'a> {
    Packed(packed_list::Iter<'a>),
    Plain(std::collections::vec_deque::Iter<'a, Arc<[u8]>>),
}

impl<'a> Iterator for Iter<'a> {
    type Item = Value<'a>;

    #[inline]
    fn next(&mut self) -> Option<Value<'a>> {
        match self {
            Iter::Packed(it) => it.next().map(Value::Bytes),
            Iter::Plain(it) => it.next().map(Value::Shared),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Iter::Packed(it) => it.size_hint(),
            Iter::Plain(it) => it.size_hint(),
        }
    }
}

impl ExactSizeIterator for Iter<'_> {}

/// Elements last to first.
pub enum RevIter<'a> {
    Packed(packed_list::RevIter<'a>),
    Plain(std::iter::Rev<std::collections::vec_deque::Iter<'a, Arc<[u8]>>>),
}

impl<'a> Iterator for RevIter<'a> {
    type Item = Value<'a>;

    #[inline]
    fn next(&mut self) -> Option<Value<'a>> {
        match self {
            RevIter::Packed(it) => it.next().map(Value::Bytes),
            RevIter::Plain(it) => it.next().map(Value::Shared),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            RevIter::Packed(it) => it.size_hint(),
            RevIter::Plain(it) => it.size_hint(),
        }
    }
}

impl ExactSizeIterator for RevIter<'_> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packed_list::{CHUNK_ENTRIES, MAX_LEN, PackedList};

    /// The list holds exactly `model`, read every way it can be read.
    fn assert_same(list: &ListValues, model: &VecDeque<Vec<u8>>, what: &str) {
        assert_eq!(list.len(), model.len(), "{what}");
        assert_eq!(list.is_empty(), model.is_empty(), "{what}");
        let expected: Vec<&[u8]> = model.iter().map(Vec::as_slice).collect();
        let forward: Vec<&[u8]> = list.iter().map(Value::bytes).collect();
        assert_eq!(forward, expected, "{what}");
        let mut backward: Vec<&[u8]> = list.range_rev(0, list.len()).map(Value::bytes).collect();
        backward.reverse();
        assert_eq!(backward, expected, "{what}");
        assert_eq!(
            list.front().map(Value::bytes),
            expected.first().copied(),
            "{what}"
        );
        assert_eq!(
            list.back().map(Value::bytes),
            expected.last().copied(),
            "{what}"
        );
        let step = (model.len() / 41).max(1);
        for i in (0..model.len()).step_by(step) {
            assert_eq!(
                list.get(i).map(Value::bytes),
                Some(expected[i]),
                "{what}: {i}"
            );
        }
        assert!(list.get(model.len()).is_none(), "{what}");
        if !list.is_packed() || model.len() > CHUNK_ENTRIES {
            assert_eq!(list.encoding(), Encoding::Quicklist, "{what}");
        }
        assert_eq!(list.packed_bytes().is_some(), list.is_packed(), "{what}");
    }

    fn element(i: usize) -> Vec<u8> {
        format!("element:{i:08}").into_bytes()
    }

    #[test]
    fn the_handle_is_no_bigger_than_a_chunk_and_a_tag() {
        // A list of several chunks keeps its length and its run behind a
        // box: the list is one chunk's size (a chunk's buffer keeps its
        // length and capacity in 32 bits each), and the two forms one word
        // more than the larger.
        assert_eq!(std::mem::size_of::<PackedList>(), 24);
        assert!(std::mem::size_of::<ListValues>() <= 40);
    }

    #[test]
    fn a_new_list_is_packed_and_one_chunk() {
        let mut list = ListValues::new();
        assert!(list.is_packed());
        assert_eq!(list.encoding(), Encoding::Listpack);
        assert_eq!(list.packed_bytes(), Some(0));
        for i in 0..CHUNK_ENTRIES {
            list.push_back(&element(i));
        }
        assert_eq!(list.encoding(), Encoding::Listpack);
        list.push_front(b"one more");
        assert_eq!(list.encoding(), Encoding::Quicklist);
        assert!(list.is_packed());
        assert_eq!(list.free_effort(), 2);
    }

    #[test]
    fn a_long_element_moves_the_list_to_blocks_of_their_own() {
        let long = vec![b'L'; MAX_LEN + 1];
        let longest = vec![b'l'; MAX_LEN];
        for how in [
            "push_back",
            "push_front",
            "set",
            "insert",
            "extend_back",
            "extend_front",
        ] {
            let mut list = ListValues::new();
            let mut model = VecDeque::new();
            for i in 0..100 {
                list.push_back(&element(i));
                model.push_back(element(i));
            }
            list.push_back(&longest);
            model.push_back(longest.clone());
            assert!(list.is_packed(), "{how}");
            match how {
                "push_back" => {
                    list.push_back(&long);
                    model.push_back(long.clone());
                }
                "push_front" => {
                    list.push_front(&long);
                    model.push_front(long.clone());
                }
                "set" => {
                    assert!(list.set(40, &long));
                    model[40] = long.clone();
                }
                "insert" => {
                    assert!(list.insert(70, &long));
                    model.insert(70, long.clone());
                }
                "extend_back" => {
                    list.extend_back(&[&b"a"[..], &long[..], &b"b"[..]]);
                    model.extend([b"a".to_vec(), long.clone(), b"b".to_vec()]);
                }
                _ => {
                    list.extend_front(&[&b"a"[..], &long[..], &b"b"[..]]);
                    for e in [b"a".to_vec(), long.clone(), b"b".to_vec()] {
                        model.push_front(e);
                    }
                }
            }
            assert!(!list.is_packed(), "{how}");
            assert_same(&list, &model, how);
            // And it stays so, short elements and all.
            list.push_back(b"short");
            model.push_back(b"short".to_vec());
            assert!(!list.is_packed(), "{how}");
            assert_same(&list, &model, how);
        }
    }

    #[test]
    fn a_set_or_insert_out_of_range_changes_nothing() {
        let long = vec![b'L'; MAX_LEN + 1];
        let mut list: ListValues = [&b"a"[..], b"b"].into_iter().collect();
        assert!(!list.set(2, &long));
        assert!(!list.insert(3, &long));
        assert!(list.is_packed());
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn a_bulk_load_lays_down_full_chunks_and_falls_back_once() {
        let elements: Vec<Vec<u8>> = (0..1000).map(element).collect();
        let list: ListValues = elements.iter().map(Vec::as_slice).collect();
        assert!(list.is_packed());
        assert_eq!(list.free_effort(), 1000usize.div_ceil(CHUNK_ENTRIES));
        // Exactly the frames, and the deque of chunks.
        // Elements of 16 bytes: one byte of length on each side.
        let frames: usize = elements.iter().map(|e| e.len() + 2).sum();
        let bytes = list.packed_bytes().unwrap();
        assert!(
            bytes >= frames && bytes <= frames + 64 * 32,
            "{bytes} {frames}"
        );
        let model: VecDeque<Vec<u8>> = elements.iter().cloned().collect();
        assert_same(&list, &model, "bulk");
        // A long element half way: what came before is moved over once.
        let mut with_long = elements.clone();
        with_long.insert(500, vec![b'L'; MAX_LEN + 1]);
        let mut b = ListValues::builder(with_long.len());
        for e in &with_long {
            b.push(e);
        }
        let list = b.finish();
        assert!(!list.is_packed());
        assert_same(&list, &with_long.into_iter().collect(), "bulk with long");
        // Shared elements likewise.
        let shared: ListValues = elements.iter().map(|e| Arc::<[u8]>::from(&e[..])).collect();
        assert_same(&shared, &model, "shared");
        let empty: ListValues = std::iter::empty::<&[u8]>().collect();
        assert!(empty.is_empty() && empty.is_packed());
    }

    #[test]
    fn extend_lays_elements_down_as_pushes_would() {
        for start in [0usize, 1, 63, 64, 65, 200] {
            for n in [0usize, 1, 10, 63, 64, 65, 130, 500] {
                let batch: Vec<Vec<u8>> = (0..n).map(|i| element(10_000 + i)).collect();
                let refs: Vec<&[u8]> = batch.iter().map(Vec::as_slice).collect();
                let mut back = ListValues::new();
                let mut front = ListValues::new();
                let mut model_back = VecDeque::new();
                let mut model_front = VecDeque::new();
                for i in 0..start {
                    back.push_back(&element(i));
                    front.push_back(&element(i));
                    model_back.push_back(element(i));
                    model_front.push_back(element(i));
                }
                back.extend_back(&refs);
                front.extend_front(&refs);
                for e in &batch {
                    model_back.push_back(e.clone());
                    model_front.push_front(e.clone());
                }
                assert_same(&back, &model_back, &format!("back {start} {n}"));
                assert_same(&front, &model_front, &format!("front {start} {n}"));
                // Buffers of exactly the frames (in their block's class) for a
                // batch that fills its chunks.
                if start == 0 && n == 64 {
                    let frames: usize = batch.iter().map(|e| e.len() + 2).sum();
                    let block = crate::packed_list::block_bytes(frames);
                    assert_eq!(back.packed_bytes(), Some(block));
                    assert_eq!(front.packed_bytes(), Some(block));
                }
            }
        }
    }

    #[test]
    fn lrem_and_ltrim_in_every_shape() {
        for packed in [true, false] {
            for len in [1usize, 10, 64, 65, 200, 1000] {
                for count in [0i64, 1, 2, 5, 1000, -1, -3, -1000] {
                    let mut list = ListValues::new();
                    let mut model: VecDeque<Vec<u8>> = VecDeque::new();
                    if !packed {
                        list.push_back(&[b'L'; MAX_LEN + 1]);
                        list.pop_front_with(|_| ());
                    }
                    for i in 0..len {
                        let e = if i % 3 == 0 {
                            b"x".to_vec()
                        } else {
                            element(i)
                        };
                        list.push_back(&e);
                        model.push_back(e);
                    }
                    let removed = list.remove_matching(b"x", count);
                    // The model, as Redis does it.
                    let mut want = 0;
                    let limit = if count == 0 {
                        usize::MAX
                    } else {
                        count.unsigned_abs() as usize
                    };
                    if count >= 0 {
                        let mut i = 0;
                        while i < model.len() && want < limit {
                            if model[i] == b"x" {
                                model.remove(i);
                                want += 1;
                            } else {
                                i += 1;
                            }
                        }
                    } else {
                        let mut i = model.len();
                        while i > 0 && want < limit {
                            i -= 1;
                            if model[i] == b"x" {
                                model.remove(i);
                                want += 1;
                            }
                        }
                    }
                    let what = format!("lrem packed={packed} len={len} count={count}");
                    assert_eq!(removed, want, "{what}");
                    assert_same(&list, &model, &what);
                    assert_eq!(list.is_packed(), packed);
                    // Then trims of what is left.
                    let n = model.len();
                    for (from, to) in [
                        (0, n),
                        (1.min(n), n),
                        (0, n.saturating_sub(1)),
                        (n / 3, n / 2),
                        (n / 2, n / 2),
                    ] {
                        let mut l = list.clone();
                        l.retain_range(from, to);
                        let m: VecDeque<Vec<u8>> = model.range(from..to).cloned().collect();
                        assert_same(&l, &m, &format!("{what} trim {from}..{to}"));
                    }
                }
            }
        }
    }

    #[test]
    fn it_matches_a_model_over_long_random_runs() {
        for seed in 0..24u64 {
            let mut state = 0x9E37_79B9_7F4A_7C15u64 ^ (seed * 0x1000_0001);
            let mut next = move || {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state
            };
            let mut list = ListValues::new();
            let mut model: VecDeque<Vec<u8>> = VecDeque::new();
            // Some runs meet a long element early, some late, some never.
            let long_every = [0u64, 400, 2_500, 9_000][seed as usize % 4];
            for step in 0..6_000u64 {
                let r = next();
                let mut value = vec![b'a' + (r >> 8) as u8 % 6; ((r >> 16) % 24) as usize];
                if long_every > 0 && (r >> 40) % long_every == 0 {
                    value = vec![b'L'; MAX_LEN + 1 + (r % 50) as usize];
                }
                // Phases: growing, shrinking, about level.
                let bias = match (step / 700) % 3 {
                    0 => 3,
                    1 => 0,
                    _ => 1,
                };
                let what = format!("seed {seed} step {step}");
                match r % 20 {
                    0..=2 => {
                        list.push_back(&value);
                        model.push_back(value);
                    }
                    3..=4 => {
                        list.push_front(&value);
                        model.push_front(value);
                    }
                    5 => {
                        let batch: Vec<Vec<u8>> = (0..(r >> 24) % 90)
                            .map(|i| vec![b'a' + (i % 5) as u8; (i % 13) as usize])
                            .collect();
                        let refs: Vec<&[u8]> = batch.iter().map(Vec::as_slice).collect();
                        if r & 1 == 0 {
                            list.extend_back(&refs);
                            model.extend(batch);
                        } else {
                            list.extend_front(&refs);
                            for e in batch {
                                model.push_front(e);
                            }
                        }
                    }
                    6..=8 if bias < 3 => {
                        let got = list.pop_front_with(|p| p.to_vec());
                        assert_eq!(got, model.pop_front(), "{what}");
                    }
                    9..=11 if bias < 2 => {
                        let got = list.pop_back_with(|p| p.to_vec());
                        assert_eq!(got, model.pop_back(), "{what}");
                    }
                    12 if !model.is_empty() => {
                        let i = (r >> 32) as usize % model.len();
                        assert!(list.set(i, &value));
                        model[i] = value;
                    }
                    13 => {
                        let i = (r >> 32) as usize % (model.len() + 1);
                        assert!(list.insert(i, &value));
                        model.insert(i, value);
                    }
                    14 => {
                        let count = ((r >> 32) % 7) as i64 - 3;
                        let removed = list.remove_matching(&value, count);
                        let limit = if count == 0 {
                            usize::MAX
                        } else {
                            count.unsigned_abs() as usize
                        };
                        let mut want = 0;
                        if count >= 0 {
                            let mut i = 0;
                            while i < model.len() && want < limit {
                                if model[i] == value {
                                    model.remove(i);
                                    want += 1;
                                } else {
                                    i += 1;
                                }
                            }
                        } else {
                            let mut i = model.len();
                            while i > 0 && want < limit {
                                i -= 1;
                                if model[i] == value {
                                    model.remove(i);
                                    want += 1;
                                }
                            }
                        }
                        assert_eq!(removed, want, "{what}");
                    }
                    15 if !model.is_empty() && bias < 3 => {
                        let n = model.len();
                        let from = (r >> 32) as usize % (n / 8 + 1);
                        let to = n - (r >> 48) as usize % (n / 8 + 1);
                        let to = to.max(from);
                        list.retain_range(from, to);
                        model = model.range(from..to).cloned().collect();
                    }
                    16 => {
                        assert_eq!(
                            list.position(&value),
                            model.iter().position(|e| *e == value),
                            "{what}"
                        );
                    }
                    17 if !model.is_empty() => {
                        let n = model.len();
                        let from = (r >> 32) as usize % n;
                        let to = from + (r >> 48) as usize % (n - from + 1);
                        let got: Vec<&[u8]> = list.range(from, to).map(Value::bytes).collect();
                        let want: Vec<&[u8]> = model.range(from..to).map(Vec::as_slice).collect();
                        assert_eq!(got, want, "{what}");
                        assert_eq!(list.range(from, to).len(), to - from);
                        let mut got: Vec<&[u8]> =
                            list.range_rev(from, to).map(Value::bytes).collect();
                        got.reverse();
                        assert_eq!(got, want, "{what}");
                    }
                    18 => {
                        // A rotation, as `RPOPLPUSH k k` and `LMOVE k k`.
                        let (from_left, to_left) = ((r >> 33) & 1 == 1, (r >> 34) & 1 == 1);
                        let got = list.rotate(from_left, to_left, |v| v.to_vec());
                        let want = if from_left {
                            model.pop_front()
                        } else {
                            model.pop_back()
                        };
                        assert_eq!(got, want, "{what}");
                        if let Some(e) = want {
                            if to_left {
                                model.push_front(e);
                            } else {
                                model.push_back(e);
                            }
                        }
                    }
                    _ => {
                        if let Some(i) =
                            (!model.is_empty()).then(|| (r >> 32) as usize % model.len())
                        {
                            assert_eq!(
                                list.get(i).map(Value::bytes),
                                Some(model[i].as_slice()),
                                "{what}"
                            );
                        }
                    }
                }
                // An emptied list is deleted, and the next one made new.
                if model.is_empty() && list.is_empty() && r % 3 == 0 {
                    list = ListValues::new();
                }
                // `locate` against a plain walk, at every step.
                list.check_shape();
                if step % 97 == 0 {
                    assert_same(&list, &model, &what);
                }
            }
            assert_same(&list, &model, &format!("seed {seed} end"));
        }
    }

    /// The list commands' operations at random against a `VecDeque`, with
    /// elements of 1 to 200 bytes: one- and two-byte lengths, chunks in slab
    /// blocks of both kinds and in the system allocator's (`Buf`), in phases
    /// that cross the chunk-count boundaries both ways, and a timeline
    /// (`LPUSH`, then `LTRIM 0 cap-1`) between them.
    #[test]
    fn it_matches_a_model_with_elements_of_1_to_200_bytes() {
        for seed in 0..12u64 {
            let mut state = 0x2545_F491_4F6C_DD1Du64 ^ (seed.wrapping_mul(0x9E37_79B9) + 1);
            let mut next = move || {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state
            };
            let mut list = ListValues::new();
            let mut model: VecDeque<Vec<u8>> = VecDeque::new();
            let cap = CHUNK_ENTRIES * (1 + seed as usize % 5) + 7;
            for step in 0..5_000u64 {
                let r = next();
                let len = 1 + (r >> 8) as usize % 200;
                let mut value: Vec<u8> = (0..len)
                    .map(|i| b'a' + ((r >> 20) as usize + i) as u8 % 7)
                    .collect();
                // Now and then an element the list holds, for LREM and LPOS.
                if !model.is_empty() && (r >> 40) % 4 == 0 {
                    value = model[(r >> 44) as usize % model.len()].clone();
                }
                let what = format!("seed {seed} step {step}");
                // Phases of 500 steps: growing, shrinking, a timeline.
                let phase = (step / 500) % 3;
                if phase == 2 {
                    list.push_front(&value);
                    model.push_front(value);
                    if model.len() > cap {
                        list.retain_range(0, cap);
                        model.truncate(cap);
                    }
                } else {
                    match r % 16 {
                        0..=3 if phase == 0 => {
                            list.push_back(&value);
                            model.push_back(value);
                        }
                        4..=5 if phase == 0 || r % 5 == 0 => {
                            list.push_front(&value);
                            model.push_front(value);
                        }
                        0..=5 => {
                            let got = list.pop_back_with(|p| p.to_vec());
                            assert_eq!(got, model.pop_back(), "{what}");
                        }
                        6..=7 => {
                            let got = list.pop_front_with(|p| p.to_vec());
                            assert_eq!(got, model.pop_front(), "{what}");
                        }
                        8 if !model.is_empty() => {
                            let i = (r >> 32) as usize % model.len();
                            assert!(list.set(i, &value), "{what}");
                            model[i] = value;
                        }
                        9 => {
                            let i = (r >> 32) as usize % (model.len() + 1);
                            assert!(list.insert(i, &value), "{what}");
                            model.insert(i, value);
                        }
                        10 => {
                            let count = ((r >> 32) % 5) as i64 - 2;
                            let removed = list.remove_matching(&value, count);
                            let limit = if count == 0 {
                                usize::MAX
                            } else {
                                count.unsigned_abs() as usize
                            };
                            let mut want = 0;
                            if count >= 0 {
                                let mut i = 0;
                                while i < model.len() && want < limit {
                                    if model[i] == value {
                                        model.remove(i);
                                        want += 1;
                                    } else {
                                        i += 1;
                                    }
                                }
                            } else {
                                let mut i = model.len();
                                while i > 0 && want < limit {
                                    i -= 1;
                                    if model[i] == value {
                                        model.remove(i);
                                        want += 1;
                                    }
                                }
                            }
                            assert_eq!(removed, want, "{what}");
                        }
                        11 if !model.is_empty() => {
                            let n = model.len();
                            let from = (r >> 32) as usize % (n / 6 + 1);
                            let to = (n - (r >> 48) as usize % (n / 6 + 1)).max(from);
                            list.retain_range(from, to);
                            model = model.range(from..to).cloned().collect();
                        }
                        12 => {
                            let (from_left, to_left) = ((r >> 33) & 1 == 1, (r >> 34) & 1 == 1);
                            let got = list.rotate(from_left, to_left, |v| v.to_vec());
                            let want = if from_left {
                                model.pop_front()
                            } else {
                                model.pop_back()
                            };
                            assert_eq!(got, want, "{what}");
                            if let Some(e) = want {
                                if to_left {
                                    model.push_front(e);
                                } else {
                                    model.push_back(e);
                                }
                            }
                        }
                        13 => {
                            assert_eq!(
                                list.position(&value),
                                model.iter().position(|e| *e == value),
                                "{what}"
                            );
                        }
                        14 if !model.is_empty() => {
                            let n = model.len();
                            let from = (r >> 32) as usize % n;
                            let to = from + (r >> 48) as usize % (n - from + 1);
                            let got: Vec<&[u8]> = list.range(from, to).map(Value::bytes).collect();
                            let want: Vec<&[u8]> =
                                model.range(from..to).map(Vec::as_slice).collect();
                            assert_eq!(got, want, "{what}");
                        }
                        _ => {
                            if !model.is_empty() {
                                let i = (r >> 32) as usize % model.len();
                                assert_eq!(
                                    list.get(i).map(Value::bytes),
                                    Some(model[i].as_slice()),
                                    "{what}"
                                );
                            }
                        }
                    }
                }
                if model.is_empty() && list.is_empty() && r % 3 == 0 {
                    list = ListValues::new();
                }
                list.check_shape();
                if step % 61 == 0 {
                    assert_same(&list, &model, &what);
                    // A copy (`COPY`, a snapshot's fork) is the same list.
                    assert_same(&list.clone(), &model, &what);
                }
            }
            assert_same(&list, &model, &format!("seed {seed} end"));
            assert!(
                list.is_packed(),
                "seed {seed}: elements of 200 bytes are packed"
            );
        }
        // Chunks crossed 8 KiB both ways: from a slab block into the system
        // allocator's as they grew, and back as `LTRIM` and `LREM` gave
        // their room back.
        let (up, down) = crate::packed_list::crossings::counts();
        assert!(up > 0 && down > 0, "crossings of 8 KiB: {up} up, {down} down");
    }
}
