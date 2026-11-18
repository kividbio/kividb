//! An element as a collection hands it out: bytes it shares (`Arc`), or
//! bytes that lie in one of its blocks. Either is its bytes (`Deref`).
//!
//! Collections that keep some elements apart and others packed together
//! (a list's chunks, a hash's block) hand out a [`Value`] so that their
//! callers need not know which: whoever wants the element beyond the
//! collection's lock takes it with [`Value::to_shared`], which is a count
//! for shared bytes and a copy for packed ones.

use std::ops::Deref;
use std::sync::Arc;

/// An element's bytes, borrowed from its collection.
#[derive(Clone, Copy)]
pub enum Value<'a> {
    Shared(&'a Arc<[u8]>),
    Bytes(&'a [u8]),
}

impl<'a> Value<'a> {
    /// The element as shared bytes: the collection's own, or a new copy.
    #[inline]
    pub fn to_shared(self) -> Arc<[u8]> {
        match self {
            Value::Shared(bytes) => Arc::clone(bytes),
            Value::Bytes(bytes) => Arc::from(bytes),
        }
    }

    /// The bytes, for as long as the collection is borrowed.
    #[inline]
    pub fn bytes(self) -> &'a [u8] {
        match self {
            Value::Shared(bytes) => bytes,
            Value::Bytes(bytes) => bytes,
        }
    }
}

impl Deref for Value<'_> {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        match self {
            Value::Shared(bytes) => bytes,
            Value::Bytes(bytes) => bytes,
        }
    }
}

impl AsRef<[u8]> for Value<'_> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl PartialEq<[u8]> for Value<'_> {
    #[inline]
    fn eq(&self, other: &[u8]) -> bool {
        **self == *other
    }
}

impl std::fmt::Debug for Value<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", String::from_utf8_lossy(self))
    }
}

/// An element taken out of its collection: the shared bytes it was kept
/// as, given over whole, or its bytes in a block, to be read (copied)
/// before the block is changed.
pub enum Popped<'a> {
    Shared(Arc<[u8]>),
    Bytes(&'a [u8]),
}

impl Popped<'_> {
    /// The element as shared bytes: as it was kept, or a new copy.
    #[inline]
    pub fn into_shared(self) -> Arc<[u8]> {
        match self {
            Popped::Shared(bytes) => bytes,
            Popped::Bytes(bytes) => Arc::from(bytes),
        }
    }
}

impl Deref for Popped<'_> {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        match self {
            Popped::Shared(bytes) => bytes,
            Popped::Bytes(bytes) => bytes,
        }
    }
}
