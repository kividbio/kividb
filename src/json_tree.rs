//! The tree a JSON document is held in.
//!
//! The same six kinds of node as `serde_json::Value`, laid out to take less
//! memory:
//!
//! * a node is 32 bytes, where a `serde_json::Value` with `preserve_order`
//!   is 72: an object's member table is boxed, and so does not set the
//!   size of every number and string in the document;
//! * an array whose elements are all numbers of one kind is held as one
//!   buffer of them ([`Packed`]): integers as `i64`, floats as `f64`. An
//!   embedding of 1,536 floats takes 12 KB this way, against 110 KB as
//!   generic nodes.
//!
//! What a document reads back is what it was given, whatever form an array is
//! held in: a packed array holds only one kind of number, so integers stay
//! integers and floats floats (`1.0` stays `1.0`). Text goes in
//! through serde_json's parser ([`Deserialize`]) and out through its
//! serializer ([`Serialize`]), as it did when the tree was serde_json's own:
//! the bytes a document is written as (`JSON.GET`, a snapshot, `DUMP`, an AOF
//! rewrite, a full sync) are the same.
//!
//! An array is packed when it is built from parsed text: a whole array given
//! to `JSON.SET`, `JSON.MSET`, `JSON.MERGE`, `JSON.ARRAPPEND` or
//! `JSON.ARRINSERT`, or read from a snapshot. An edit of a packed array that
//! keeps it all numbers of its kind is made in the buffer; any other edit turns
//! it into generic nodes first, and it stays so until the whole array is set
//! again.

use indexmap::IndexMap;
use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
pub use serde_json::Number;
use std::borrow::Cow;
use std::fmt;

/// An object's members, in the order they were added.
pub type Map = IndexMap<String, Value>;

/// A node of a JSON document.
#[derive(Clone, Debug, Default)]
pub enum Value {
    #[default]
    Null,
    Bool(bool),
    Number(Number),
    String(String),
    Array(Array),
    Object(Box<Map>),
}

/// The fewest elements an array is packed with: below it, a packed buffer
/// saves too little to be worth a second form.
pub const PACK_MIN: usize = 8;

/// An array: generic nodes, or a packed buffer of numbers.
#[derive(Clone, Debug)]
pub enum Array {
    Items(Vec<Value>),
    Packed(Box<Packed>),
}

/// The numbers of a packed array, all of one kind.
#[derive(Clone, Debug)]
pub enum Packed {
    /// Integers that fit an `i64`.
    I64(Vec<i64>),
    /// Floats.
    F64(Vec<f64>),
}

/// Whether arrays are packed. Off only in tests that compare the two forms
/// (`KIVIDB_TEST_JSON_UNPACKED`), so that a server can be run holding every
/// array as generic nodes.
fn packing_enabled() -> bool {
    static OFF: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    !*OFF.get_or_init(|| std::env::var_os("KIVIDB_TEST_JSON_UNPACKED").is_some())
}

fn float(value: f64) -> Number {
    // A packed float came from a finite `Number`, and arithmetic stores only
    // finite results.
    Number::from_f64(value).unwrap_or_else(|| Number::from(0))
}

impl Packed {
    /// The packed form of `items`, if they qualify: at least [`PACK_MIN`] of
    /// them, every one a number, all integers that fit an `i64` or all
    /// floats.
    fn of(items: &[Value]) -> Option<Packed> {
        if items.len() < PACK_MIN || !packing_enabled() {
            return None;
        }
        let Value::Number(first) = &items[0] else {
            return None;
        };
        if first.is_f64() {
            let mut floats = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::Number(n) if n.is_f64() => floats.push(n.as_f64()?),
                    _ => return None,
                }
            }
            Some(Packed::F64(floats))
        } else {
            let mut ints = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::Number(n) if !n.is_f64() => ints.push(n.as_i64()?),
                    _ => return None,
                }
            }
            Some(Packed::I64(ints))
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Packed::I64(v) => v.len(),
            Packed::F64(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Element `index` as a JSON number. `index` must be in range.
    pub fn number(&self, index: usize) -> Number {
        match self {
            Packed::I64(v) => Number::from(v[index]),
            Packed::F64(v) => float(v[index]),
        }
    }

    /// Element `index` as a float, as `as_f64` of its number gives it.
    pub fn f64_at(&self, index: usize) -> f64 {
        match self {
            Packed::I64(v) => v[index] as f64,
            Packed::F64(v) => v[index],
        }
    }

    /// The buffer's capacity in bytes.
    pub fn buffer_bytes(&self) -> usize {
        match self {
            Packed::I64(v) => v.capacity() * 8,
            Packed::F64(v) => v.capacity() * 8,
        }
    }

    fn fits(&self, value: &Value) -> bool {
        let Value::Number(n) = value else {
            return false;
        };
        match self {
            Packed::I64(_) => !n.is_f64() && n.as_i64().is_some(),
            Packed::F64(_) => n.is_f64(),
        }
    }

    /// Whether every one of `values` goes in.
    fn make_room_for<'v>(&mut self, values: impl IntoIterator<Item = &'v Value>) -> bool {
        values.into_iter().all(|value| self.fits(value))
    }

    /// Put `value` at `index`, or insert it there. Only for a value
    /// [`Packed::make_room_for`] has accepted.
    fn place(&mut self, index: usize, value: &Value, insert: bool) {
        let Value::Number(n) = value else {
            return;
        };
        fn put<T>(v: &mut Vec<T>, index: usize, x: T, insert: bool) {
            if insert {
                v.insert(index, x);
            } else {
                v[index] = x;
            }
        }
        match self {
            Packed::I64(v) => put(v, index, n.as_i64().unwrap_or(0), insert),
            Packed::F64(v) => put(v, index, n.as_f64().unwrap_or(0.0), insert),
        }
    }

    fn remove(&mut self, index: usize) -> Number {
        let n = self.number(index);
        match self {
            Packed::I64(v) => drop(v.remove(index)),
            Packed::F64(v) => drop(v.remove(index)),
        }
        n
    }

    fn keep(&mut self, from: usize, to: usize) {
        fn cut<T>(v: &mut Vec<T>, from: usize, to: usize) {
            v.truncate(to);
            v.drain(..from);
        }
        match self {
            Packed::I64(v) => cut(v, from, to),
            Packed::F64(v) => cut(v, from, to),
        }
    }

    fn clear(&mut self) {
        match self {
            Packed::I64(v) => v.clear(),
            Packed::F64(v) => v.clear(),
        }
    }

    fn unpack(&self) -> Vec<Value> {
        (0..self.len())
            .map(|i| Value::Number(self.number(i)))
            .collect()
    }
}

impl Default for Array {
    fn default() -> Self {
        Array::Items(Vec::new())
    }
}

impl From<Vec<Value>> for Array {
    /// Generic nodes, as they are: for arrays built to be printed, or
    /// compared, rather than stored.
    fn from(items: Vec<Value>) -> Self {
        Array::Items(items)
    }
}

impl Array {
    /// `items` as a stored array: packed if they qualify ([`Packed`]).
    pub fn packing(items: Vec<Value>) -> Self {
        match Packed::of(&items) {
            Some(packed) => Array::Packed(Box::new(packed)),
            None => Array::Items(items),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Array::Items(items) => items.len(),
            Array::Packed(packed) => packed.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Element `index`, borrowed from a generic array or made from a packed
    /// one.
    pub fn get(&self, index: usize) -> Option<Cow<'_, Value>> {
        match self {
            Array::Items(items) => items.get(index).map(Cow::Borrowed),
            Array::Packed(packed) if index < packed.len() => {
                Some(Cow::Owned(Value::Number(packed.number(index))))
            }
            Array::Packed(_) => None,
        }
    }

    pub fn iter(&self) -> Iter<'_> {
        match self {
            Array::Items(items) => Iter::Items(items.iter()),
            Array::Packed(packed) => Iter::Packed(packed, 0..packed.len()),
        }
    }

    /// The generic nodes, if that is how the array is held.
    pub fn items(&self) -> Option<&[Value]> {
        match self {
            Array::Items(items) => Some(items),
            Array::Packed(_) => None,
        }
    }

    pub fn packed(&self) -> Option<&Packed> {
        match self {
            Array::Packed(packed) => Some(packed),
            Array::Items(_) => None,
        }
    }

    /// The elements as generic nodes, which they are from here on: for an
    /// edit a packed array cannot take.
    pub fn items_mut(&mut self) -> &mut Vec<Value> {
        if let Array::Packed(packed) = self {
            *self = Array::Items(packed.unpack());
        }
        match self {
            Array::Items(items) => items,
            Array::Packed(_) => unreachable!("unpacked above"),
        }
    }

    /// Element `index` becomes `value`. `index` must be in range.
    pub fn set(&mut self, index: usize, value: Value) {
        if let Array::Packed(packed) = self
            && packed.make_room_for([&value])
        {
            packed.place(index, &value, false);
            return;
        }
        self.items_mut()[index] = value;
    }

    /// `values` added at the end.
    pub fn append(&mut self, values: &[Value]) {
        if let Array::Packed(packed) = self
            && packed.make_room_for(values)
        {
            let end = packed.len();
            for (i, value) in values.iter().enumerate() {
                packed.place(end + i, value, true);
            }
            return;
        }
        self.items_mut().extend(values.iter().cloned());
    }

    /// `values` inserted before element `at` (`at` at most the length).
    pub fn insert(&mut self, at: usize, values: &[Value]) {
        if let Array::Packed(packed) = self
            && packed.make_room_for(values)
        {
            for (i, value) in values.iter().enumerate() {
                packed.place(at + i, value, true);
            }
            return;
        }
        self.items_mut().splice(at..at, values.iter().cloned());
    }

    /// Removes element `index`, which must be in range, and returns it.
    pub fn remove(&mut self, index: usize) -> Value {
        match self {
            Array::Items(items) => items.remove(index),
            Array::Packed(packed) => Value::Number(packed.remove(index)),
        }
    }

    /// Keeps elements `from..to` only (`from <= to <= len`).
    pub fn keep(&mut self, from: usize, to: usize) {
        match self {
            Array::Items(items) => {
                items.truncate(to);
                items.drain(..from);
            }
            Array::Packed(packed) => packed.keep(from, to),
        }
    }

    pub fn clear(&mut self) {
        match self {
            Array::Items(items) => items.clear(),
            Array::Packed(packed) => packed.clear(),
        }
    }
}

/// The elements of an [`Array`], in order.
pub enum Iter<'a> {
    Items(std::slice::Iter<'a, Value>),
    Packed(&'a Packed, std::ops::Range<usize>),
}

impl<'a> Iterator for Iter<'a> {
    type Item = Cow<'a, Value>;

    fn next(&mut self) -> Option<Cow<'a, Value>> {
        match self {
            Iter::Items(items) => items.next().map(Cow::Borrowed),
            Iter::Packed(packed, range) => range
                .next()
                .map(|i| Cow::Owned(Value::Number(packed.number(i)))),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = match self {
            Iter::Items(items) => items.len(),
            Iter::Packed(_, range) => range.len(),
        };
        (len, Some(len))
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Iter::Items(items) => items.next_back().map(Cow::Borrowed),
            Iter::Packed(packed, range) => range
                .next_back()
                .map(|i| Cow::Owned(Value::Number(packed.number(i)))),
        }
    }
}

impl ExactSizeIterator for Iter<'_> {}

impl Value {
    /// An empty object.
    pub fn object() -> Value {
        Value::Object(Box::default())
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn is_object(&self) -> bool {
        matches!(self, Value::Object(_))
    }

    pub fn is_array(&self) -> bool {
        matches!(self, Value::Array(_))
    }

    pub fn is_number(&self) -> bool {
        matches!(self, Value::Number(_))
    }

    pub fn as_object(&self) -> Option<&Map> {
        match self {
            Value::Object(members) => Some(members),
            _ => None,
        }
    }

    pub fn as_object_mut(&mut self) -> Option<&mut Map> {
        match self {
            Value::Object(members) => Some(members),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&Array> {
        match self {
            Value::Array(items) => Some(items),
            _ => None,
        }
    }

    pub fn as_array_mut(&mut self) -> Option<&mut Array> {
        match self {
            Value::Array(items) => Some(items),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Number(n) => n.as_f64(),
            _ => None,
        }
    }
}

// ── Equality ─────────────────────────────────────────────────────────────────

impl PartialEq for Value {
    /// As serde_json compares its values: an object's members in any order,
    /// an array's elements in order, whichever form either array is held in.
    fn eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Number(a), Value::Number(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Array(a), Value::Array(b)) => a == b,
            (Value::Object(a), Value::Object(b)) => a == b,
            _ => false,
        }
    }
}

impl PartialEq for Array {
    fn eq(&self, other: &Array) -> bool {
        self.len() == other.len() && self.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}

impl PartialEq<serde_json::Value> for Value {
    fn eq(&self, other: &serde_json::Value) -> bool {
        match (self, other) {
            (Value::Null, serde_json::Value::Null) => true,
            (Value::Bool(a), serde_json::Value::Bool(b)) => a == b,
            (Value::Number(a), serde_json::Value::Number(b)) => a == b,
            (Value::String(a), serde_json::Value::String(b)) => a == b,
            (Value::Array(a), serde_json::Value::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(a, b)| *a == *b)
            }
            (Value::Object(a), serde_json::Value::Object(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .all(|(key, value)| b.get(key).is_some_and(|other| value == other))
            }
            _ => false,
        }
    }
}

// ── Conversions ──────────────────────────────────────────────────────────────

impl From<serde_json::Value> for Value {
    /// A serde_json value as a stored document: arrays packed where they
    /// qualify.
    fn from(value: serde_json::Value) -> Value {
        match value {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => Value::Number(n),
            serde_json::Value::String(s) => Value::String(s),
            serde_json::Value::Array(items) => {
                Value::Array(Array::packing(items.into_iter().map(Value::from).collect()))
            }
            serde_json::Value::Object(members) => Value::Object(Box::new(
                members
                    .into_iter()
                    .map(|(key, value)| (key, Value::from(value)))
                    .collect(),
            )),
        }
    }
}

impl From<&Value> for serde_json::Value {
    fn from(value: &Value) -> serde_json::Value {
        match value {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Number(n) => serde_json::Value::Number(n.clone()),
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Array(items) => {
                serde_json::Value::Array(items.iter().map(|item| (&*item).into()).collect())
            }
            Value::Object(members) => serde_json::Value::Object(
                members
                    .iter()
                    .map(|(key, value)| (key.clone(), value.into()))
                    .collect(),
            ),
        }
    }
}

impl From<Number> for Value {
    fn from(n: Number) -> Value {
        Value::Number(n)
    }
}

// ── Text ─────────────────────────────────────────────────────────────────────

impl Serialize for Value {
    /// The calls serde_json's own `Value` makes, so that any serializer
    /// writes a document as it wrote serde_json's.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Value::Null => serializer.serialize_unit(),
            Value::Bool(b) => serializer.serialize_bool(*b),
            Value::Number(n) => n.serialize(serializer),
            Value::String(s) => serializer.serialize_str(s),
            Value::Array(items) => items.serialize(serializer),
            Value::Object(members) => {
                let mut map = serializer.serialize_map(Some(members.len()))?;
                for (key, value) in members.iter() {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
        }
    }
}

impl Serialize for Array {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        match self {
            Array::Items(items) => {
                for item in items {
                    seq.serialize_element(item)?;
                }
            }
            // A number serializes as `serialize_u64`/`serialize_i64` for an
            // integer and `serialize_f64` for a float, as it does in a
            // generic array.
            Array::Packed(packed) => match &**packed {
                Packed::I64(v) => {
                    for &n in v {
                        seq.serialize_element(&Number::from(n))?;
                    }
                }
                Packed::F64(v) => {
                    for &f in v {
                        seq.serialize_element(&f)?;
                    }
                }
            },
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Value {
    /// What serde_json's own `Value` makes of the same text, with arrays
    /// packed where they qualify.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Value, D::Error> {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("any valid JSON value")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Value, E> {
                Ok(Value::Bool(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Value, E> {
                Ok(Value::Number(value.into()))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Value, E> {
                Ok(Value::Number(value.into()))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Value, E> {
                Ok(Number::from_f64(value).map_or(Value::Null, Value::Number))
            }

            fn visit_str<E>(self, value: &str) -> Result<Value, E> {
                Ok(Value::String(value.to_owned()))
            }

            fn visit_string<E>(self, value: String) -> Result<Value, E> {
                Ok(Value::String(value))
            }

            fn visit_none<E>(self) -> Result<Value, E> {
                Ok(Value::Null)
            }

            fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
                Deserialize::deserialize(deserializer)
            }

            fn visit_unit<E>(self) -> Result<Value, E> {
                Ok(Value::Null)
            }

            fn visit_seq<V: SeqAccess<'de>>(self, mut visitor: V) -> Result<Value, V::Error> {
                let mut items = Vec::new();
                while let Some(item) = visitor.next_element()? {
                    items.push(item);
                }
                Ok(Value::Array(Array::packing(items)))
            }

            fn visit_map<V: MapAccess<'de>>(self, mut visitor: V) -> Result<Value, V::Error> {
                let mut members = Map::new();
                while let Some((key, value)) = visitor.next_entry::<String, Value>()? {
                    members.insert(key, value);
                }
                Ok(Value::Object(Box::new(members)))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

impl fmt::Display for Value {
    /// Compact JSON, as `serde_json::Value`'s `to_string` gives it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = serde_json::to_string(self).map_err(|_| fmt::Error)?;
        f.write_str(&text)
    }
}

impl std::str::FromStr for Value {
    type Err = serde_json::Error;

    fn from_str(text: &str) -> Result<Value, serde_json::Error> {
        serde_json::from_str(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Value {
        serde_json::from_str(text).unwrap()
    }

    fn reference(text: &str) -> String {
        serde_json::to_string(&serde_json::from_str::<serde_json::Value>(text).unwrap()).unwrap()
    }

    #[test]
    fn a_node_is_32_bytes() {
        assert!(
            std::mem::size_of::<Value>() <= 32,
            "{}",
            std::mem::size_of::<Value>()
        );
        assert_eq!(std::mem::size_of::<Array>(), 24);
    }

    #[test]
    fn arrays_pack_by_kind() {
        let kind = |text: &str| match parse(text) {
            Value::Array(Array::Packed(p)) => match *p {
                Packed::I64(_) => "i64",
                Packed::F64(_) => "f64",
            },
            Value::Array(Array::Items(_)) => "items",
            _ => "other",
        };
        assert_eq!(kind("[1,2,3,4,5,6,7,8]"), "i64");
        assert_eq!(kind("[1,2,3,4,5,6,7]"), "items");
        assert_eq!(kind("[0.5,1.5,2.5,3.5,4.5,5.5,6.5,-0.0]"), "f64");
        assert_eq!(kind("[0.1,1.5,2.5,3.5,4.5,5.5,6.5,7.5]"), "f64");
        assert_eq!(kind("[1.0,1,2,3,4,5,6,7]"), "items");
        assert_eq!(kind("[1,2,3,4,5,6,7,18446744073709551615]"), "items");
        assert_eq!(kind("[1,2,3,4,5,6,7,\"8\"]"), "items");
        assert_eq!(kind("[1e300,1,2,3,4,5,6,7.5]"), "items");
        assert_eq!(kind("[1e300,1.5,2.5,3.5,4.5,5.5,6.5,7.5]"), "f64");
    }

    #[test]
    fn packed_arrays_print_as_parsed_text() {
        let texts = [
            "[1,2,3,4,5,6,7,8,-9223372036854775808,9223372036854775807]",
            "[0.5,1.0,-0.0,0.0,1e21,1e-7,6.4e-05,3.4028234663852886e38,1.401298464324817e-45]",
            "[0.1,0.2,0.30000000000000004,1e300,-1e-300,5e-324,2.5,3.25]",
            "[0.10000000149011612,0.20000000298023224,1.5,2.5,3.5,4.5,5.5,6.5]",
            "[1.50,1e5,100000.0,2E-3,-0.0,12.0,7.000,8e0]",
            "{\"a\":[1,2,3,4,5,6,7,8],\"b\":{\"c\":[0.25,0.5,0.75,1.0,1.25,1.5,1.75,2.0]}}",
        ];
        for text in texts {
            let value = parse(text);
            assert!(
                matches!(&value, Value::Array(Array::Packed(_)) | Value::Object(_)),
                "{text}"
            );
            assert_eq!(
                serde_json::to_string(&value).unwrap(),
                reference(text),
                "{text}"
            );
            assert_eq!(value.to_string(), reference(text), "{text}");
            let serde: serde_json::Value = serde_json::from_str(text).unwrap();
            assert_eq!(value, serde, "{text}");
            assert_eq!(serde_json::Value::from(&value), serde, "{text}");
            assert_eq!(Value::from(serde), value, "{text}");
        }
    }

    #[test]
    fn edits_that_fit_stay_packed_and_others_unpack() {
        let mut value = parse("[0.5,1.5,2.5,3.5,4.5,5.5,6.5,7.5]");
        let Value::Array(array) = &mut value else {
            unreachable!()
        };
        array.set(0, Value::Number(Number::from_f64(0.25).unwrap()));
        assert!(matches!(array.packed(), Some(Packed::F64(_))));
        array.append(&[Value::Number(Number::from_f64(0.1).unwrap())]);
        assert!(matches!(array.packed(), Some(Packed::F64(_))));
        array.insert(1, &[Value::Number(Number::from_f64(-0.0).unwrap())]);
        assert!(matches!(array.packed(), Some(Packed::F64(_))));
        assert_eq!(
            value.to_string(),
            "[0.25,-0.0,1.5,2.5,3.5,4.5,5.5,6.5,7.5,0.1]"
        );
        let Value::Array(array) = &mut value else {
            unreachable!()
        };
        array.set(2, Value::Number(Number::from(3)));
        assert!(array.packed().is_none());
        assert_eq!(
            value.to_string(),
            "[0.25,-0.0,3,2.5,3.5,4.5,5.5,6.5,7.5,0.1]"
        );

        let mut ints = parse("[1,2,3,4,5,6,7,8]");
        let Value::Array(array) = &mut ints else {
            unreachable!()
        };
        array.append(&[Value::Number(Number::from(-9))]);
        array.keep(1, 8);
        assert!(matches!(array.packed(), Some(Packed::I64(_))));
        assert_eq!(array.remove(0), Value::Number(Number::from(2)));
        array.append(&[Value::Number(Number::from(u64::MAX))]);
        assert!(array.packed().is_none());
        assert_eq!(ints.to_string(), "[3,4,5,6,7,8,18446744073709551615]");
    }
}
