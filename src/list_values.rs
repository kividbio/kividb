//! The elements of a list (`LPUSH`, `RPUSH`), in order.
//!
//! Callers see elements as [`Value`]s (bytes, whatever the list keeps them
//! as) and change the list only through what is here: pushes and pops at
//! either end, an element by its index, ranges read either way, an insert
//! by a pivot, removal by value (`LREM`), a trim to a range (`LTRIM`).
//! Elements popped are handed to the caller as [`Popped`] for as long as
//! the call lasts. Snapshots, the AOF, replication and `DUMP` read elements
//! as bytes, in order: none of them sees how they are kept.

use std::collections::VecDeque;
use std::sync::Arc;

pub use crate::element::{Popped, Value};

/// A list's elements.
#[derive(Clone, Default)]
pub struct ListValues {
    repr: Repr,
}

#[derive(Clone)]
enum Repr {
    /// Each element a block of its own.
    Plain(VecDeque<Arc<[u8]>>),
}

impl Default for Repr {
    fn default() -> Self {
        Repr::Plain(VecDeque::new())
    }
}

impl ListValues {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn len(&self) -> usize {
        match &self.repr {
            Repr::Plain(d) => d.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Room for `more` elements, where that means anything.
    #[inline]
    pub fn reserve(&mut self, more: usize) {
        match &mut self.repr {
            Repr::Plain(d) => d.reserve(more),
        }
    }

    #[inline]
    pub fn push_back(&mut self, element: &[u8]) {
        match &mut self.repr {
            Repr::Plain(d) => d.push_back(Arc::from(element)),
        }
    }

    #[inline]
    pub fn push_front(&mut self, element: &[u8]) {
        match &mut self.repr {
            Repr::Plain(d) => d.push_front(Arc::from(element)),
        }
    }

    /// Add an element the caller holds as shared bytes: kept as they are
    /// where elements are kept apart.
    #[inline]
    pub fn push_back_shared(&mut self, element: Arc<[u8]>) {
        match &mut self.repr {
            Repr::Plain(d) => d.push_back(element),
        }
    }

    #[inline]
    pub fn push_front_shared(&mut self, element: Arc<[u8]>) {
        match &mut self.repr {
            Repr::Plain(d) => d.push_front(element),
        }
    }

    #[inline]
    pub fn front(&self) -> Option<Value<'_>> {
        match &self.repr {
            Repr::Plain(d) => d.front().map(Value::Shared),
        }
    }

    #[inline]
    pub fn back(&self) -> Option<Value<'_>> {
        match &self.repr {
            Repr::Plain(d) => d.back().map(Value::Shared),
        }
    }

    /// Take the first element off, handing it to `take`. `None`, and
    /// `take` not called, if the list is empty.
    #[inline]
    pub fn pop_front_with<R>(&mut self, take: impl FnOnce(Popped<'_>) -> R) -> Option<R> {
        match &mut self.repr {
            Repr::Plain(d) => d.pop_front().map(|e| take(Popped::Shared(e))),
        }
    }

    /// Take the last element off, handing it to `take`.
    #[inline]
    pub fn pop_back_with<R>(&mut self, take: impl FnOnce(Popped<'_>) -> R) -> Option<R> {
        match &mut self.repr {
            Repr::Plain(d) => d.pop_back().map(|e| take(Popped::Shared(e))),
        }
    }

    /// Element `i`, if there is one.
    #[inline]
    pub fn get(&self, i: usize) -> Option<Value<'_>> {
        match &self.repr {
            Repr::Plain(d) => d.get(i).map(Value::Shared),
        }
    }

    /// Put `element` in element `i`'s place. `false` if there is no
    /// element `i`.
    pub fn set(&mut self, i: usize, element: &[u8]) -> bool {
        match &mut self.repr {
            Repr::Plain(d) => match d.get_mut(i) {
                Some(slot) => {
                    *slot = Arc::from(element);
                    true
                }
                None => false,
            },
        }
    }

    /// Put `element` before element `i`; after the last when `i == len()`.
    /// `false` if `i > len()`.
    pub fn insert(&mut self, i: usize, element: &[u8]) -> bool {
        match &mut self.repr {
            Repr::Plain(d) => {
                if i > d.len() {
                    return false;
                }
                d.insert(i, Arc::from(element));
                true
            }
        }
    }

    /// The index of the first element equal to `element`.
    pub fn position(&self, element: &[u8]) -> Option<usize> {
        match &self.repr {
            Repr::Plain(d) => d.iter().position(|v| v.as_ref() == element),
        }
    }

    /// `LREM`: take out the elements equal to `element`, `count` of them
    /// at most from the first on (`count > 0`), from the last back (`count
    /// < 0`), or all (`0`). Returns how many went.
    pub fn remove_matching(&mut self, element: &[u8], count: i64) -> usize {
        let limit = count.unsigned_abs() as usize;
        match &mut self.repr {
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
            Repr::Plain(d) => {
                // From the back first, then the front.
                d.drain(end..);
                d.drain(..start);
            }
        }
    }

    pub fn clear(&mut self) {
        match &mut self.repr {
            Repr::Plain(d) => d.clear(),
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
            Repr::Plain(d) => Iter::Plain(d.range(from..to)),
        }
    }

    /// The elements from `from` up to, not including, `to`, last to first.
    #[inline]
    pub fn range_rev(&self, from: usize, to: usize) -> RevIter<'_> {
        match &self.repr {
            Repr::Plain(d) => RevIter::Plain(d.range(from..to).rev()),
        }
    }
}

/// A list's elements in order, as they are read: [`ListValues::builder`].
/// Bulk loads (a snapshot, `RESTORE`, `SORT ... STORE`) lay elements down
/// here one after another.
pub struct Builder {
    list: ListValues,
}

impl ListValues {
    /// A list to be filled in order, about `hint` elements long.
    pub fn builder(hint: usize) -> Builder {
        Builder {
            list: ListValues {
                repr: Repr::Plain(VecDeque::with_capacity(hint)),
            },
        }
    }
}

impl Builder {
    #[inline]
    pub fn push(&mut self, element: &[u8]) {
        self.list.push_back(element);
    }

    #[inline]
    pub fn push_shared(&mut self, element: Arc<[u8]>) {
        self.list.push_back_shared(element);
    }

    pub fn finish(self) -> ListValues {
        self.list
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
    Plain(std::collections::vec_deque::Iter<'a, Arc<[u8]>>),
}

impl<'a> Iterator for Iter<'a> {
    type Item = Value<'a>;

    #[inline]
    fn next(&mut self) -> Option<Value<'a>> {
        match self {
            Iter::Plain(it) => it.next().map(Value::Shared),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Iter::Plain(it) => it.size_hint(),
        }
    }
}

impl ExactSizeIterator for Iter<'_> {}

/// Elements last to first.
pub enum RevIter<'a> {
    Plain(std::iter::Rev<std::collections::vec_deque::Iter<'a, Arc<[u8]>>>),
}

impl<'a> Iterator for RevIter<'a> {
    type Item = Value<'a>;

    #[inline]
    fn next(&mut self) -> Option<Value<'a>> {
        match self {
            RevIter::Plain(it) => it.next().map(Value::Shared),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            RevIter::Plain(it) => it.size_hint(),
        }
    }
}

impl ExactSizeIterator for RevIter<'_> {}
