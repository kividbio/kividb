// The TEXT index: which documents hold which word, and what each document's
// TEXT fields put in, in numbers rather than strings.
//
// Every distinct term (a word, or a stem written `+stem`, see `stem`) is
// kept once, as a number: its id. A document is known by its id in the
// index (the one `postings::Postings` gives every document of an index that
// has a TEXT, TAG or NUMERIC field), so that:
//
// - a term's postings are, for each TEXT field it is in, the sorted ids of
//   the documents holding it there, or a bitmap of them once that is the
//   smaller (as for TAG values);
// - a document's terms (its forward entry) are one slice of numbers: for
//   each TEXT field it holds words in, in the schema's order, a marker
//   naming the field, then the field's terms in the order they come, each
//   word followed by its stem if it was indexed with one. Positions, for
//   phrases and for relevance, are the order: the nth word of a document is
//   at position n, and a stem is at its word's;
// - the BM25 statistics (documents, their total length, each term's
//   document frequency) are counters kept with them.
//
// Before, every posting was a copy of the document's key in a set of
// strings, and every word of every document a string of its own, for each
// of the inverted index, the forward index, the document frequencies and
// the vocabulary: with stems, that was some 30 allocations a short document.
//
// A word, once seen, keeps its id while the index lives, as the vocabulary
// that `FT.SPELLCHECK` reads and `FT.INFO`'s `num_terms` counts always kept
// it. A stem no document holds any more gives its id back.

use super::postings::Bits;
use ahash::RandomState;
use hashbrown::HashTable;

/// What a stem is written with, ahead of the word (as `stem::STEM_PREFIX`).
const STEM_PREFIX: char = super::stem::STEM_PREFIX;

/// In a document's entry: the start of a field's terms. The low bits are the
/// field's number (see [`TextIndex::set`]).
const FIELD_MARK: u32 = 1 << 31;
/// In a document's entry: the term is a stem.
const STEM_BIT: u32 = 1 << 30;
const TERM_MASK: u32 = STEM_BIT - 1;

/// A term of a document's entry: its id, and whether it is a stem.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Token(pub u32);

impl Token {
    #[inline]
    pub fn term(self) -> u32 {
        self.0 & TERM_MASK
    }

    #[inline]
    pub fn is_stem(self) -> bool {
        self.0 & STEM_BIT != 0
    }
}

/// The fields of a document's entry: each field's number and its terms
/// (each a [`Token`]).
pub fn fields(entry: &[u32]) -> impl Iterator<Item = (u16, &[u32])> + '_ {
    let mut rest = entry;
    std::iter::from_fn(move || {
        let (&mark, tail) = rest.split_first()?;
        debug_assert!(mark & FIELD_MARK != 0);
        let end = tail
            .iter()
            .position(|t| t & FIELD_MARK != 0)
            .unwrap_or(tail.len());
        let (terms, next) = tail.split_at(end);
        rest = next;
        Some(((mark & !FIELD_MARK) as u16, terms))
    })
}

// ── Sets of documents ────────────────────────────────────────────────────────

/// The documents that hold a term in one field: a sorted list while they are
/// few, a bitmap once the list would be the larger of the two.
#[derive(Debug, Clone)]
enum DocSet {
    Sparse(Vec<u32>),
    Dense { bits: Bits, len: u32 },
}

impl DocSet {
    fn len(&self) -> usize {
        match self {
            DocSet::Sparse(ids) => ids.len(),
            DocSet::Dense { len, .. } => *len as usize,
        }
    }

    /// Add `doc`. `slots` is how many ids there are, which a bitmap costs.
    fn insert(&mut self, doc: u32, slots: usize) -> bool {
        match self {
            DocSet::Sparse(ids) => {
                // Documents mostly come in the order of their ids.
                match ids.last() {
                    Some(&last) if last < doc => ids.push(doc),
                    None => ids.push(doc),
                    _ => match ids.binary_search(&doc) {
                        Ok(_) => return false,
                        Err(at) => ids.insert(at, doc),
                    },
                }
                // A list of n ids costs 32n bits; a bitmap one bit a slot.
                if ids.len() >= 64 && ids.len() * 32 > slots {
                    let mut bits = Bits::new();
                    for &id in ids.iter() {
                        bits.set(id);
                    }
                    *self = DocSet::Dense {
                        len: ids.len() as u32,
                        bits,
                    };
                }
                true
            }
            DocSet::Dense { bits, len } => {
                if bits.get(doc) {
                    return false;
                }
                bits.set(doc);
                *len += 1;
                true
            }
        }
    }

    fn remove(&mut self, doc: u32, slots: usize) -> bool {
        match self {
            DocSet::Sparse(ids) => match ids.binary_search(&doc) {
                Ok(at) => {
                    ids.remove(at);
                    if ids.capacity() > 16 && ids.len() * 4 < ids.capacity() {
                        ids.shrink_to(ids.len() * 2);
                    }
                    true
                }
                Err(_) => false,
            },
            DocSet::Dense { bits, len } => {
                if !bits.get(doc) {
                    return false;
                }
                bits.unset(doc);
                *len -= 1;
                // Back to a list well below the point it became a bitmap, so
                // that a set at the boundary does not flip back and forth.
                if (*len as usize) * 64 < slots {
                    *self = DocSet::Sparse(bits.iter().collect());
                }
                true
            }
        }
    }

    fn or_into(&self, out: &mut Bits) {
        match self {
            DocSet::Sparse(ids) => ids.iter().for_each(|&id| out.set(id)),
            DocSet::Dense { bits, .. } => out.or_with(bits),
        }
    }

    fn bytes(&self) -> usize {
        match self {
            DocSet::Sparse(ids) => ids.capacity() * 4,
            DocSet::Dense { bits, .. } => bits.bytes(),
        }
    }
}

// ── Terms ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
struct Term {
    /// Empty for an id given back.
    text: Box<str>,
    /// How many documents hold it, in any field.
    docs: u32,
    /// By field number: the documents holding it there.
    postings: Vec<(u16, DocSet)>,
}

impl Term {
    fn is_stem(&self) -> bool {
        self.text.starts_with(STEM_PREFIX)
    }
}

/// What the TEXT index holds, in bytes (`FT.INFO`).
#[derive(Debug, Default, Clone, Copy)]
pub struct TextBytes {
    pub terms: usize,
    pub postings: usize,
    pub docs: usize,
}

/// The TEXT index of one search index. See the top of this file.
#[derive(Debug, Clone, Default)]
pub struct TextIndex {
    hasher: RandomState,
    /// Term text → id, the text being kept in `terms`.
    lookup: HashTable<u32>,
    terms: Vec<Term>,
    /// Ids given back by stems no document holds any more.
    free: Vec<u32>,
    /// Terms that are words, not stems: the vocabulary.
    words: usize,
    /// By document id: its entry (see the top of this file).
    docs: Vec<Option<Box<[u32]>>>,
    /// Documents with at least one word.
    doc_count: usize,
    /// Their lengths, summed: each word counted by its field's weight.
    total_len: usize,
    /// Postings: (field, term, document) triples.
    records: usize,
}

impl TextIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// The id of the term `text`, if the index has it.
    pub fn term(&self, text: &str) -> Option<u32> {
        let hash = self.hasher.hash_one(text);
        self.lookup
            .find(hash, |&id| &*self.terms[id as usize].text == text)
            .copied()
    }

    /// The text of term `id`.
    pub fn text(&self, id: u32) -> &str {
        self.terms.get(id as usize).map_or("", |t| &t.text)
    }

    /// How many documents hold term `id`.
    pub fn df(&self, id: u32) -> usize {
        self.terms.get(id as usize).map_or(0, |t| t.docs as usize)
    }

    /// How many documents hold the term `text`.
    pub fn df_of(&self, text: &str) -> usize {
        self.term(text).map_or(0, |id| self.df(id))
    }

    /// Documents with at least one word.
    pub fn doc_count(&self) -> usize {
        self.doc_count
    }

    /// The length of every document, summed.
    pub fn total_len(&self) -> usize {
        self.total_len
    }

    /// Words (not stems) the index has seen: `FT.INFO`'s `num_terms`.
    pub fn num_terms(&self) -> usize {
        self.words
    }

    /// (field, term, document) postings: `FT.INFO`'s `num_records`.
    pub fn num_records(&self) -> usize {
        self.records
    }

    /// Every word the index has seen, for `FT.SPELLCHECK`.
    pub fn words(&self) -> impl Iterator<Item = &str> + '_ {
        self.terms
            .iter()
            .filter(|t| !t.text.is_empty() && !t.is_stem())
            .map(|t| &*t.text)
    }

    /// Every word (not stem) some document holds, with its id.
    pub fn live_words(&self) -> impl Iterator<Item = (u32, &str)> + '_ {
        self.terms
            .iter()
            .enumerate()
            .filter(|(_, t)| t.docs > 0 && !t.is_stem())
            .map(|(id, t)| (id as u32, &*t.text))
    }

    /// The entry of document `doc`: empty if it holds no word.
    #[inline]
    pub fn doc(&self, doc: u32) -> &[u32] {
        self.docs
            .get(doc as usize)
            .and_then(Option::as_deref)
            .unwrap_or(&[])
    }

    /// The words (not stems) document `doc` holds in field `field`, in order.
    pub fn field_words(&self, doc: u32, field: u16) -> Option<Vec<String>> {
        fields(self.doc(doc))
            .find(|(f, _)| *f == field)
            .map(|(_, terms)| {
                terms
                    .iter()
                    .map(|&t| Token(t))
                    .filter(|t| !t.is_stem())
                    .map(|t| self.text(t.term()).to_string())
                    .collect()
            })
    }

    /// Into `out`, the documents holding term `text` in any of `fields`.
    pub fn docs_with(&self, text: &str, fields: &[u16], out: &mut Bits) {
        if let Some(id) = self.term(text) {
            for (field, set) in &self.terms[id as usize].postings {
                if fields.contains(field) {
                    set.or_into(out);
                }
            }
        }
    }

    /// Into `out`, the documents holding, in any of `fields`, a word (not a
    /// stem) that `pick` accepts. Reads every term.
    pub fn docs_matching(&self, fields: &[u16], pick: impl Fn(&str) -> bool, out: &mut Bits) {
        for term in &self.terms {
            if term.docs == 0 || term.is_stem() || !pick(&term.text) {
                continue;
            }
            for (field, set) in &term.postings {
                if fields.contains(field) {
                    set.or_into(out);
                }
            }
        }
    }

    fn intern(&mut self, text: &str) -> u32 {
        let hash = self.hasher.hash_one(text);
        if let Some(&id) = self
            .lookup
            .find(hash, |&id| &*self.terms[id as usize].text == text)
        {
            return id;
        }
        let term = Term {
            text: text.into(),
            docs: 0,
            postings: Vec::new(),
        };
        if !term.is_stem() {
            self.words += 1;
        }
        let id = match self.free.pop() {
            Some(id) => {
                self.terms[id as usize] = term;
                id
            }
            None => {
                self.terms.push(term);
                (self.terms.len() - 1) as u32
            }
        };
        let (terms, hasher) = (&self.terms, &self.hasher);
        self.lookup
            .insert_unique(hash, id, |&id| hasher.hash_one(&*terms[id as usize].text));
        id
    }

    /// A stem no document holds gives its id back.
    fn release(&mut self, id: u32) {
        let term = &mut self.terms[id as usize];
        term.postings = Vec::new();
        if !term.is_stem() {
            return;
        }
        let text = std::mem::take(&mut term.text);
        let hash = self.hasher.hash_one(&*text);
        if let Ok(entry) = self.lookup.find_entry(hash, |&other| other == id) {
            entry.remove();
        }
        self.free.push(id);
    }

    /// Make what document `doc` holds in some TEXT fields `changes`: for each,
    /// the field's number and its terms, a word followed by its stem when it
    /// has one, as `stem::index_words` gives them (empty: the field holds no
    /// word). Its other fields keep what they hold. `unit` is what a word of
    /// a field counts toward the document's length (its weight).
    ///
    /// Field numbers are those of the index's TEXT fields in the schema's
    /// order, which is the order positions are counted in.
    pub fn set(&mut self, doc: u32, changes: &[(u16, Vec<String>)], unit: impl Fn(u16) -> u32) {
        let at = doc as usize;
        if at >= self.docs.len() {
            self.docs.resize(at + 1, None);
        }
        let old_entry = self.docs[at].take();
        let old: &[u32] = old_entry.as_deref().unwrap_or(&[]);
        if old.is_empty() && changes.iter().all(|(_, terms)| terms.is_empty()) {
            return;
        }

        // The new entry: the old one's fields that were not written, and the
        // written ones, in field order.
        let mut segments: Vec<(u16, Vec<u32>)> = fields(old)
            .filter(|(f, _)| !changes.iter().any(|(c, _)| c == f))
            .map(|(f, terms)| (f, terms.to_vec()))
            .collect();
        for (field, terms) in changes {
            if terms.is_empty() {
                continue;
            }
            let ids = terms
                .iter()
                .map(|t| {
                    let id = self.intern(t);
                    if t.starts_with(STEM_PREFIX) {
                        id | STEM_BIT
                    } else {
                        id
                    }
                })
                .collect();
            segments.push((*field, ids));
        }
        segments.sort_by_key(|(f, _)| *f);
        let mut new: Vec<u32> = Vec::with_capacity(segments.iter().map(|(_, t)| t.len() + 1).sum());
        for (field, terms) in &segments {
            new.push(FIELD_MARK | u32::from(*field));
            new.extend_from_slice(terms);
        }

        // What changed: (field, term) postings, and terms in the document.
        let pairs = |entry: &[u32]| -> Vec<u64> {
            let mut out: Vec<u64> = fields(entry)
                .flat_map(|(f, terms)| {
                    terms
                        .iter()
                        .map(move |&t| (u64::from(f) << 32) | u64::from(Token(t).term()))
                })
                .collect();
            out.sort_unstable();
            out.dedup();
            out
        };
        let length = |entry: &[u32]| -> usize {
            fields(entry)
                .map(|(f, terms)| {
                    terms.iter().filter(|&&t| !Token(t).is_stem()).count() * unit(f) as usize
                })
                .sum()
        };
        let (old_pairs, new_pairs) = (pairs(old), pairs(&new));
        let slots = self.docs.len();
        merge_diff(&old_pairs, &new_pairs, |pair, came| {
            let (field, id) = ((pair >> 32) as u16, pair as u32);
            let term = &mut self.terms[id as usize];
            if came {
                let at = match term.postings.iter().position(|(f, _)| *f == field) {
                    Some(at) => at,
                    None => {
                        term.postings.push((field, DocSet::Sparse(Vec::new())));
                        term.postings.len() - 1
                    }
                };
                if term.postings[at].1.insert(doc, slots) {
                    self.records += 1;
                }
            } else {
                if let Some(at) = term.postings.iter().position(|(f, _)| *f == field) {
                    if term.postings[at].1.remove(doc, slots) {
                        self.records -= 1;
                    }
                    if term.postings[at].1.len() == 0 {
                        term.postings.swap_remove(at);
                    }
                }
            }
        });
        // A term in two fields is one document of its frequency.
        let terms_of = |entry: &[u32]| -> Vec<u32> {
            let mut out: Vec<u32> = fields(entry)
                .flat_map(|(_, terms)| terms.iter().map(|&t| Token(t).term()))
                .collect();
            out.sort_unstable();
            out.dedup();
            out
        };
        let (old_terms, new_terms) = (terms_of(old), terms_of(&new));
        let mut unheld: Vec<u32> = Vec::new();
        merge_diff(&old_terms, &new_terms, |id, came| {
            let term = &mut self.terms[id as usize];
            if came {
                term.docs += 1;
            } else {
                term.docs = term.docs.saturating_sub(1);
                if term.docs == 0 {
                    unheld.push(id);
                }
            }
        });
        for id in unheld {
            self.release(id);
        }

        let (old_len, new_len) = (length(old), length(&new));
        match (old_len > 0, new_len > 0) {
            (false, true) => self.doc_count += 1,
            (true, false) => self.doc_count = self.doc_count.saturating_sub(1),
            _ => {}
        }
        self.total_len = self
            .total_len
            .saturating_sub(old_len)
            .saturating_add(new_len);
        if !new.is_empty() {
            self.docs[at] = Some(new.into_boxed_slice());
        }
    }

    /// Document `doc` has left the index: take out everything it put in.
    pub fn remove(&mut self, doc: u32, unit: impl Fn(u16) -> u32) {
        let fields: Vec<(u16, Vec<String>)> = fields(self.doc(doc))
            .map(|(f, _)| (f, Vec::new()))
            .collect();
        if !fields.is_empty() {
            self.set(doc, &fields, unit);
        }
    }

    /// Bytes held: the terms and their lookup table, the postings, and the
    /// documents' entries.
    pub fn bytes(&self) -> TextBytes {
        let terms = self.terms.capacity() * std::mem::size_of::<Term>()
            + self.terms.iter().map(|t| t.text.len()).sum::<usize>()
            + self.lookup.capacity() * 5
            + self.free.capacity() * 4;
        let postings = self
            .terms
            .iter()
            .map(|t| {
                t.postings.capacity() * std::mem::size_of::<(u16, DocSet)>()
                    + t.postings.iter().map(|(_, s)| s.bytes()).sum::<usize>()
            })
            .sum();
        let docs = self.docs.capacity() * 16
            + self
                .docs
                .iter()
                .flatten()
                .map(|d| d.len() * 4)
                .sum::<usize>();
        TextBytes {
            terms,
            postings,
            docs,
        }
    }
}

/// Walk two sorted, deduplicated lists: `each(x, true)` for what is only in
/// `new`, `each(x, false)` for what is only in `old`.
fn merge_diff<T: Copy + Ord>(old: &[T], new: &[T], mut each: impl FnMut(T, bool)) {
    let (mut i, mut j) = (0, 0);
    while i < old.len() || j < new.len() {
        match (old.get(i), new.get(j)) {
            (Some(&a), Some(&b)) if a == b => {
                i += 1;
                j += 1;
            }
            (Some(&a), Some(&b)) if a < b => {
                each(a, false);
                i += 1;
            }
            (Some(_), Some(&b)) => {
                each(b, true);
                j += 1;
            }
            (Some(&a), None) => {
                each(a, false);
                i += 1;
            }
            (None, Some(&b)) => {
                each(b, true);
                j += 1;
            }
            (None, None) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(s: &str) -> Vec<String> {
        s.split(' ').map(str::to_string).collect()
    }

    fn docs(index: &TextIndex, term: &str, field: u16) -> Vec<u32> {
        let mut out = Bits::new();
        index.docs_with(term, &[field], &mut out);
        out.iter().collect()
    }

    #[test]
    fn a_document_written_again_and_removed_leaves_nothing() {
        let mut index = TextIndex::new();
        index.set(3, &[(0, words("dogs +dog run"))], |_| 1);
        index.set(5, &[(0, words("dogs +dog")), (1, words("cat"))], |_| 2);
        assert_eq!(docs(&index, "dogs", 0), vec![3, 5]);
        assert_eq!(docs(&index, "+dog", 0), vec![3, 5]);
        assert_eq!(docs(&index, "cat", 1), vec![5]);
        assert_eq!(index.df_of("dogs"), 2);
        assert_eq!(index.doc_count(), 2);
        // 2 words, and 2 words at weight 2.
        assert_eq!(index.total_len(), 2 + 4);
        assert_eq!(index.num_records(), 3 + 3);
        assert_eq!(index.num_terms(), 3);

        // Field 0 of document 5 written again: field 1 keeps its word.
        index.set(5, &[(0, words("run"))], |_| 2);
        assert_eq!(docs(&index, "dogs", 0), vec![3]);
        assert_eq!(docs(&index, "run", 0), vec![3, 5]);
        assert_eq!(docs(&index, "cat", 1), vec![5]);
        assert_eq!(index.field_words(5, 1), Some(vec!["cat".to_string()]));

        index.remove(3, |_| 1);
        index.remove(5, |_| 2);
        assert_eq!(index.doc_count(), 0);
        assert_eq!(index.total_len(), 0);
        assert_eq!(index.num_records(), 0);
        assert_eq!(index.df_of("dogs"), 0);
        // The stem gave its id back; the words stay in the vocabulary.
        assert_eq!(index.term("+dog"), None);
        assert_eq!(index.num_terms(), 3);
        let mut vocab: Vec<&str> = index.words().collect();
        vocab.sort_unstable();
        assert_eq!(vocab, ["cat", "dogs", "run"]);
    }

    #[test]
    fn positions_are_the_order_of_the_words() {
        let mut index = TextIndex::new();
        index.set(0, &[(1, words("b +b2")), (0, words("a"))], |_| 1);
        let entry: Vec<(u16, Vec<(String, bool)>)> = fields(index.doc(0))
            .map(|(f, terms)| {
                (
                    f,
                    terms
                        .iter()
                        .map(|&t| (index.text(Token(t).term()).to_string(), Token(t).is_stem()))
                        .collect(),
                )
            })
            .collect();
        assert_eq!(
            entry,
            vec![
                (0, vec![("a".to_string(), false)]),
                (1, vec![("b".to_string(), false), ("+b2".to_string(), true)]),
            ]
        );
    }

    #[test]
    fn many_documents_become_a_bitmap_and_back() {
        let mut index = TextIndex::new();
        for doc in 0..1000 {
            let w = if doc % 2 == 0 { "even" } else { "odd" };
            index.set(doc, &[(0, words(&format!("all {w}")))], |_| 1);
        }
        assert_eq!(docs(&index, "all", 0).len(), 1000);
        assert_eq!(docs(&index, "even", 0).len(), 500);
        for doc in 0..990 {
            index.remove(doc, |_| 1);
        }
        assert_eq!(docs(&index, "all", 0), (990..1000).collect::<Vec<_>>());
        let mut out = Bits::new();
        index.docs_matching(&[0], |w| w.starts_with("ev"), &mut out);
        assert_eq!(out.count(), 5);
    }
}
