pub mod filter;
pub mod postings;
pub mod simd;
use crate::store::ValueRef;
use ahash::{AHashMap, AHashSet};
use parking_lot::RwLock;
use rand::Rng;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize};

/// True while snapshot startup is rebuilding vector / FT live indexes from keys.
/// [`crate::commands::handler::execute_command`] returns `-LOADING` for commands not on the
/// same allowlist as replica bulk-sync (PING, INFO, SHUTDOWN, etc.).
pub static SNAPSHOT_INDEX_REBUILD: AtomicBool = AtomicBool::new(false);

/// Set for the duration of an AOF replay, so [`maybe_index`] and
/// [`maybe_unindex`] do nothing and the indexes are built by a single
/// [`backfill_from_db_progress`] pass once the dataset is whole.
///
/// Replaying an indexed dataset otherwise pays a full HNSW insertion per
/// record — 1.18M of them for glove-25, serially, before the listener is even
/// bound, which is the difference between a ~3 minute start and a ~3 second
/// one. It is also wasted work: a vector indexed by one record and overwritten
/// or deleted by a later one was inserted into the graph for nothing.
///
/// Safe because no write command reads the index to decide its result, so the
/// state the replay produces is identical either way. Reads cannot observe the
/// gap — the replay finishes before any client is accepted.
pub static DEFER_INDEXING: AtomicBool = AtomicBool::new(false);

/// One relaxed load on the write path, in the same shape as `MONITOR_COUNT`.
#[inline]
fn indexing_deferred() -> bool {
    DEFER_INDEXING.load(Relaxed)
}

#[derive(Debug, Clone, PartialEq)]
pub enum DistanceMetric {
    L2,
    Cosine,
    InnerProduct,
}

impl DistanceMetric {
    pub fn metric_from_str(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "L2" => Some(Self::L2),
            "COSINE" => Some(Self::Cosine),
            "IP" | "INNERPRODUCT" => Some(Self::InnerProduct),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum VectorAlgo {
    Flat,
    Hnsw {
        m: usize,
        ef_construction: usize,
        ef_runtime: usize,
    },
}

/// The element type of a vector field's blobs, `TYPE` in `FT.CREATE`.
///
/// Only the blobs are in this type: a hash field's value and a query's
/// vector parameter. The index holds every vector as `f32`, so a FLOAT64
/// field costs what a FLOAT32 one does and FLOAT16 saves nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VectorType {
    #[default]
    Float32,
    Float64,
    Float16,
    BFloat16,
}

impl VectorType {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "FLOAT32" => Some(Self::Float32),
            "FLOAT64" => Some(Self::Float64),
            "FLOAT16" => Some(Self::Float16),
            "BFLOAT16" => Some(Self::BFloat16),
            _ => None,
        }
    }

    /// As `FT.CREATE` and `FT.INFO` spell it.
    pub fn name(self) -> &'static str {
        match self {
            Self::Float32 => "FLOAT32",
            Self::Float64 => "FLOAT64",
            Self::Float16 => "FLOAT16",
            Self::BFloat16 => "BFLOAT16",
        }
    }

    /// Bytes per element.
    pub fn size(self) -> usize {
        match self {
            Self::Float32 => 4,
            Self::Float64 => 8,
            Self::Float16 | Self::BFloat16 => 2,
        }
    }

    /// A blob of `dim` little-endian elements, as `f32`. `None` if it is not
    /// `dim` elements long.
    pub fn decode(self, blob: &[u8], dim: usize) -> Option<Vec<f32>> {
        if blob.len() != dim * self.size() {
            return None;
        }
        Some(match self {
            Self::Float32 => blob
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes(*b))
                .collect(),
            Self::Float64 => blob
                .as_chunks::<8>()
                .0
                .iter()
                .map(|b| f64::from_le_bytes(*b) as f32)
                .collect(),
            Self::Float16 => blob
                .as_chunks::<2>()
                .0
                .iter()
                .map(|b| crate::commands::json_fpha::f16_bits_to_f32(u16::from_le_bytes(*b)))
                .collect(),
            Self::BFloat16 => blob
                .as_chunks::<2>()
                .0
                .iter()
                .map(|b| f32::from_bits(u32::from(u16::from_le_bytes(*b)) << 16))
                .collect(),
        })
    }

    /// One element as this type's little-endian bytes: a JSON document's
    /// number array is turned into the blob a hash would hold.
    pub fn encode(self, value: f64, out: &mut Vec<u8>) {
        match self {
            Self::Float32 => out.extend_from_slice(&(value as f32).to_le_bytes()),
            Self::Float64 => out.extend_from_slice(&value.to_le_bytes()),
            Self::Float16 => out.extend_from_slice(
                &crate::commands::json_fpha::f32_to_f16_bits(value as f32).to_le_bytes(),
            ),
            Self::BFloat16 => {
                let rounded = crate::commands::json_fpha::f32_to_bf16(value as f32);
                out.extend_from_slice(&((rounded.to_bits() >> 16) as u16).to_le_bytes());
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum FieldType {
    Text,
    Tag,
    Numeric,
    Vector {
        algo: VectorAlgo,
        dim: usize,
        metric: DistanceMetric,
        data_type: VectorType,
    },
}

#[derive(Debug, Clone)]
pub struct SchemaField {
    /// Where the value is read from: the hash field's name.
    pub name: String,
    pub field_type: FieldType,
    pub options: FieldOptions,
}

impl SchemaField {
    /// The name queries, `RETURN` and `SORTBY` know the field by: the one
    /// given with `AS`, or else its own.
    pub fn attribute(&self) -> &str {
        self.options.alias.as_deref().unwrap_or(&self.name)
    }

    /// The type as `FT.CREATE` and `FT.INFO` spell it.
    pub fn type_name(&self) -> &'static str {
        match self.field_type {
            FieldType::Text => "TEXT",
            FieldType::Tag => "TAG",
            FieldType::Numeric => "NUMERIC",
            FieldType::Vector { .. } => "VECTOR",
        }
    }
}

/// What may follow a field's type in a schema.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldOptions {
    /// `AS name`.
    pub alias: Option<String>,
    pub sortable: bool,
    /// `SORTABLE UNF`: sort on the value as stored, not a normalised form.
    pub unf: bool,
    /// Not searchable; still returned, and sortable if `SORTABLE`.
    pub noindex: bool,
    /// TEXT: how much a match in this field counts towards the score.
    pub weight: f64,
    pub nostem: bool,
    pub phonetic: Option<String>,
    /// TAG: what separates one tag from the next in a value.
    pub separator: char,
    /// TAG: `Abc` and `abc` are different tags.
    pub case_sensitive: bool,
    pub with_suffix_trie: bool,
    pub index_empty: bool,
    pub index_missing: bool,
}

impl Default for FieldOptions {
    fn default() -> Self {
        Self {
            alias: None,
            sortable: false,
            unf: false,
            noindex: false,
            weight: 1.0,
            nostem: false,
            phonetic: None,
            separator: ',',
            case_sensitive: false,
            with_suffix_trie: false,
            index_empty: false,
            index_missing: false,
        }
    }
}

impl FieldOptions {
    /// Read the options that follow a field's type, from `args[*i]` on, and
    /// stop at the first word that is not one — the next field's name.
    ///
    /// An option is recognised only on a type it applies to. `WEIGHT` after a
    /// TAG field is therefore not consumed, and is reported by the caller as
    /// what it is: an option in the wrong place.
    pub fn consume(
        &mut self,
        field: &str,
        field_type: &FieldType,
        args: &[&[u8]],
        i: &mut usize,
    ) -> Result<(), String> {
        let text = matches!(field_type, FieldType::Text);
        let tag = matches!(field_type, FieldType::Tag);
        let vector = matches!(field_type, FieldType::Vector { .. });
        while let Some(word) = args.get(*i).and_then(|a| std::str::from_utf8(a).ok()) {
            let mut value = |what: &str| -> Result<String, String> {
                let value = args
                    .get(*i + 1)
                    .and_then(|a| std::str::from_utf8(a).ok())
                    .ok_or_else(|| format!("{what} of field `{field}` needs a value"))?;
                *i += 1;
                Ok(value.to_string())
            };
            match word.to_ascii_uppercase().as_str() {
                "SORTABLE" if !vector => {
                    self.sortable = true;
                    if args
                        .get(*i + 1)
                        .is_some_and(|a| a.eq_ignore_ascii_case(b"UNF"))
                    {
                        self.unf = true;
                        *i += 1;
                    }
                }
                "NOINDEX" if !vector => self.noindex = true,
                "INDEXMISSING" => self.index_missing = true,
                "INDEXEMPTY" if text || tag => self.index_empty = true,
                "WITHSUFFIXTRIE" if text || tag => self.with_suffix_trie = true,
                "WEIGHT" if text => {
                    let raw = value("WEIGHT")?;
                    self.weight = raw
                        .parse::<f64>()
                        .ok()
                        .filter(|w| w.is_finite() && *w >= 0.0)
                        .ok_or_else(|| format!("Invalid WEIGHT `{raw}` for field `{field}`"))?;
                }
                "NOSTEM" if text => self.nostem = true,
                "PHONETIC" if text => self.phonetic = Some(value("PHONETIC")?),
                "SEPARATOR" if tag => {
                    let raw = value("SEPARATOR")?;
                    let mut chars = raw.chars();
                    match (chars.next(), chars.next()) {
                        (Some(c), None) => self.separator = c,
                        _ => {
                            return Err(format!(
                                "Tag separator of field `{field}` must be a single character"
                            ));
                        }
                    }
                }
                "CASESENSITIVE" if tag => self.case_sensitive = true,
                _ => break,
            }
            *i += 1;
        }
        Ok(())
    }

    /// The options as `FT.CREATE` would be given them — and as `FT.INFO`
    /// lists them, after the type.
    pub fn words(&self, field_type: &FieldType) -> Vec<String> {
        let mut out = Vec::new();
        match field_type {
            FieldType::Text => {
                out.push("WEIGHT".to_string());
                out.push(format_weight(self.weight));
                if self.nostem {
                    out.push("NOSTEM".into());
                }
                if let Some(matcher) = &self.phonetic {
                    out.push("PHONETIC".into());
                    out.push(matcher.clone());
                }
            }
            FieldType::Tag => {
                out.push("SEPARATOR".to_string());
                out.push(self.separator.to_string());
                if self.case_sensitive {
                    out.push("CASESENSITIVE".into());
                }
            }
            FieldType::Numeric | FieldType::Vector { .. } => {}
        }
        if self.with_suffix_trie {
            out.push("WITHSUFFIXTRIE".into());
        }
        if self.index_empty {
            out.push("INDEXEMPTY".into());
        }
        if self.index_missing {
            out.push("INDEXMISSING".into());
        }
        if self.sortable {
            out.push("SORTABLE".into());
            if self.unf {
                out.push("UNF".into());
            }
        }
        if self.noindex {
            out.push("NOINDEX".into());
        }
        out
    }
}

impl IndexDef {
    /// The `FT.CREATE` that makes this definition, for a node that lacks
    /// it: one added to a cluster after the index was created, say (see
    /// `cluster::search` and `cluster::migration`). Synonym groups are not
    /// part of it.
    pub fn create_args(&self) -> Vec<Vec<u8>> {
        let mut a: Vec<String> = vec![
            "FT.CREATE".into(),
            self.name.clone(),
            "ON".into(),
            match self.on {
                IndexOn::Hash => "HASH",
                IndexOn::Json => "JSON",
            }
            .into(),
        ];
        if !self.prefixes.is_empty() {
            a.push("PREFIX".into());
            a.push(self.prefixes.len().to_string());
            a.extend(self.prefixes.iter().cloned());
        }
        if let Some(words) = &self.stopwords {
            a.push("STOPWORDS".into());
            a.push(words.len().to_string());
            a.extend(words.iter().cloned());
        }
        a.push("SCHEMA".into());
        for field in &self.fields {
            a.push(field.name.clone());
            if let Some(alias) = &field.options.alias {
                a.push("AS".into());
                a.push(alias.clone());
            }
            a.push(field.type_name().into());
            if let FieldType::Vector {
                algo,
                dim,
                metric,
                data_type,
            } = &field.field_type
            {
                let mut attrs: Vec<String> = vec![
                    "TYPE".into(),
                    data_type.name().into(),
                    "DIM".into(),
                    dim.to_string(),
                    "DISTANCE_METRIC".into(),
                    match metric {
                        DistanceMetric::L2 => "L2",
                        DistanceMetric::Cosine => "COSINE",
                        DistanceMetric::InnerProduct => "IP",
                    }
                    .into(),
                ];
                match algo {
                    VectorAlgo::Flat => a.push("FLAT".into()),
                    VectorAlgo::Hnsw {
                        m,
                        ef_construction,
                        ef_runtime,
                    } => {
                        a.push("HNSW".into());
                        attrs.extend([
                            "M".into(),
                            m.to_string(),
                            "EF_CONSTRUCTION".into(),
                            ef_construction.to_string(),
                            "EF_RUNTIME".into(),
                            ef_runtime.to_string(),
                        ]);
                    }
                }
                a.push(attrs.len().to_string());
                a.extend(attrs);
            }
            a.extend(field.options.words(&field.field_type));
        }
        a.into_iter().map(String::into_bytes).collect()
    }
}

fn format_weight(weight: f64) -> String {
    if weight.fract() == 0.0 {
        format!("{}", weight as i64)
    } else {
        weight.to_string()
    }
}

/// Words that belong after a field's type, and the types they belong to.
/// Used to explain a schema that has one in the wrong place.
pub fn field_option_owner(word: &str) -> Option<&'static str> {
    match word.to_ascii_uppercase().as_str() {
        "WEIGHT" | "NOSTEM" | "PHONETIC" => Some("TEXT"),
        "SEPARATOR" | "CASESENSITIVE" => Some("TAG"),
        "SORTABLE" | "UNF" | "NOINDEX" => Some("TEXT, TAG and NUMERIC"),
        "WITHSUFFIXTRIE" | "INDEXEMPTY" => Some("TEXT and TAG"),
        "INDEXMISSING" => Some("every type of"),
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub enum IndexOn {
    Hash,
    Json,
}

/// Holds the live index data — either FLAT (brute force) or HNSW (graph).
#[derive(Debug, Clone)]
pub enum LiveIndex {
    Flat(Arc<VectorIndex>),
    Hnsw(Arc<HnswIndex>),
}

/// A synonym group: a set of terms that are treated as equivalent during search.
/// Key = group ID (string), Value = set of synonym terms.
pub type SynonymGroups = AHashMap<String, Vec<String>>;

/// What separates one word from the next, in a document and in a query —
/// Redis's default separators. Everything else, `_` included, is part of a
/// word.
#[inline]
pub(crate) fn is_token_separator(c: char) -> bool {
    c.is_whitespace()
        || matches!(
            c,
            ',' | '.'
                | '/'
                | '('
                | ')'
                | '{'
                | '}'
                | '['
                | ']'
                | ':'
                | ';'
                | '\\'
                | '~'
                | '!'
                | '@'
                | '#'
                | '$'
                | '%'
                | '^'
                | '&'
                | '*'
                | '-'
                | '='
                | '+'
                | '|'
                | '\''
                | '`'
                | '"'
                | '<'
                | '>'
                | '?'
        )
}

/// The words of a text: split at separators, lower-cased.
///
/// One place that defines "what is a word" — used for indexing, for matching
/// and for spell-checking, so that they always agree.
///
/// It used to split at whitespace only and then drop the punctuation, which
/// made `wi-fi` the word `wifi` and `red,green` the word `redgreen`: a search
/// for `green` found nothing. Punctuation separates words, as in Redis.
///
/// A backslash makes the character after it part of the word, separator or
/// not, and is itself dropped: `users\.u_1` is the one word `users.u_1`, in a
/// document as in a query, as in Redis. LangGraph's store keeps namespaces
/// apart this way; splitting there made a namespace match the ones under it.
pub fn tokenize_for_vocab(text: &str) -> impl Iterator<Item = String> + '_ {
    // Most text has no backslash: it is split as it always was, at the cost
    // tokenizing had before escapes were read. Looking for one is a byte
    // search over the text.
    if text.contains('\\') {
        Words::Escaped(text)
    } else {
        Words::Plain(
            text.split(is_token_separator)
                .filter(|word| !word.is_empty())
                .map(str::to_lowercase),
        )
    }
}

/// The words of a text, for [`tokenize_for_vocab`]. `Plain` is the split a
/// text without a backslash has always had; it keeps the separator test a
/// function the compiler inlines, as a `fn` pointer would not.
enum Words<'a, P> {
    Plain(P),
    /// What is left of a text that has a backslash in it.
    Escaped(&'a str),
}

impl<P: Iterator<Item = String>> Iterator for Words<'_, P> {
    type Item = String;

    fn next(&mut self) -> Option<String> {
        match self {
            Words::Plain(words) => words.next(),
            Words::Escaped(rest) => loop {
                // A word starts at the first character that is not a
                // separator, or at a backslash, which escapes what follows.
                let start = rest.find(|c| c == '\\' || !is_token_separator(c))?;
                let text = &rest[start..];
                let mut word = String::new();
                let mut chars = text.char_indices();
                let mut end = text.len();
                while let Some((i, c)) = chars.next() {
                    if c == '\\' {
                        word.extend(chars.next().map(|(_, escaped)| escaped));
                    } else if is_token_separator(c) {
                        end = i;
                        break;
                    } else {
                        word.push(c);
                    }
                }
                *rest = &text[end..];
                if !word.is_empty() {
                    return Some(word.to_lowercase());
                }
            },
        }
    }
}

/// Returns true if the token is a common English stopword that should not
/// be indexed or matched. Stopwords are never added to the inverted index,
/// vocab, or BM25 stats — querying for them returns no results rather than
/// matching everything.
///
/// List matches Redis's default English stopword set.
pub fn is_stopword(tok: &str) -> bool {
    matches!(
        tok,
        "a" | "an"
            | "and"
            | "are"
            | "as"
            | "at"
            | "be"
            | "but"
            | "by"
            | "for"
            | "if"
            | "in"
            | "into"
            | "is"
            | "it"
            | "no"
            | "not"
            | "of"
            | "on"
            | "or"
            | "such"
            | "that"
            | "the"
            | "their"
            | "then"
            | "there"
            | "these"
            | "they"
            | "this"
            | "to"
            | "was"
            | "will"
            | "with"
    )
}

#[derive(Debug, Clone)]
pub struct IndexDef {
    pub name: String,
    /// Which logical database (0-based) this index applies to.
    pub db_index: usize,
    pub on: IndexOn,
    pub prefixes: Vec<String>,
    pub fields: Vec<SchemaField>,
    /// Live index data for each VECTOR field, keyed by field name.
    /// A schema can declare multiple VECTOR fields; each one gets its own
    /// FLAT or HNSW structure here. Non-vector fields (TEXT/TAG/NUMERIC)
    /// are evaluated at scan time and don't appear in this map.
    pub indexes: AHashMap<String, LiveIndex>,
    /// `STOPWORDS n word…` of `FT.CREATE`: the words this index does not
    /// index. `None` is the default list; an empty list is "none at all".
    pub stopwords: Option<Arc<Vec<String>>>,
    pub synonyms: SynonymGroups,
    /// Indexed vocabulary — every distinct token observed in any TEXT field
    /// of any indexed key. Used by FT.SPELLCHECK to find candidates near
    /// a misspelled query term without re-scanning the keyspace.
    ///
    /// Append-only between rebuilds (see backfill_index for the rebuild path).
    /// Wrapped in Arc<RwLock<>> so the IndexStore's outer lock can be
    /// dropped before reading or writing the vocab — keeps the fast path
    /// (HSET / DEL / FT.SEARCH) free of vocab lock contention.
    pub vocab: Arc<RwLock<BTreeSet<String>>>,
    // ── BM25 corpus statistics ─────────────────────────────────────────────
    //
    // These three are everything BM25 needs beyond per-document data:
    //
    //   doc_count    — total documents indexed. The "N" in the IDF formula.
    //   total_tokens — sum of token counts across every document. Dividing
    //                  this by doc_count gives avgdl (average document length).
    //   doc_freq     — for each term, how many *documents* contain it (df(t)).
    //                  Note: this is documents, not occurrences. "rust rust"
    //                  in one doc contributes df("rust") += 1, not += 2.
    //
    // All three live behind their own Arc<RwLock> so the IndexStore's outer
    // lock can be released before touching them — same pattern as `vocab`.
    // AtomicUsize for doc_count/total_tokens avoids a lock on the read path
    // (BM25 reads them once per query, not per document).
    pub doc_count: Arc<AtomicUsize>,
    pub total_tokens: Arc<AtomicUsize>,
    pub doc_freq: Arc<RwLock<AHashMap<String, usize>>>,
    pub inverted: Arc<RwLock<AHashMap<String, AHashSet<String>>>>,
    //                                     ^^^^^^  ^^^^^^^^^^^^^^^^^^
    //                                     token   set of doc keys containing it
    /// What each document's TEXT fields put into the inverted index and the
    /// statistics above, by key and field, stopwords left out: what is taken
    /// back out when a field is written again or the document goes. Without
    /// it nothing was: an update added a document's words again, a delete
    /// left them counted, and BM25 scores drifted until a restart (#162).
    pub forward: Arc<RwLock<ForwardIndex>>,
    /// The TAG and NUMERIC postings, and the document ids they are kept in
    /// (see [`postings`]). Empty, and given no document, for an index with
    /// no TAG or NUMERIC field.
    pub postings: postings::SharedPostings,
}

/// By document key, by TEXT field: the words it put in (see
/// [`IndexDef::forward`]).
pub type ForwardIndex = AHashMap<String, AHashMap<String, Vec<String>>>;

impl IndexDef {
    /// Make `tokens` — stopwords already left out — what TEXT field `field`
    /// of the document at `key` holds: the inverted index, the vocabulary
    /// and the BM25 statistics become what they would be had the document
    /// always held them.
    pub fn set_text(&self, key: &str, field: &str, tokens: Vec<String>) {
        self.change_text(key, |fields| {
            if tokens.is_empty() {
                fields.remove(field);
            } else {
                fields.insert(field.to_string(), tokens);
            }
        });
    }

    /// The document at `key` is gone: take out everything its TEXT fields
    /// put in.
    pub fn remove_text(&self, key: &str) {
        self.change_text(key, |fields| fields.clear());
    }

    fn change_text(&self, key: &str, change: impl FnOnce(&mut AHashMap<String, Vec<String>>)) {
        use std::sync::atomic::Ordering::Relaxed;
        let mut forward = self.forward.write();
        let old = forward.remove(key).unwrap_or_default();
        let mut new = old.clone();
        change(&mut new);
        if old.is_empty() && new.is_empty() {
            return;
        }
        let words = |fields: &AHashMap<String, Vec<String>>| -> (usize, AHashSet<String>) {
            let total = fields.values().map(Vec::len).sum();
            let unique = fields.values().flatten().cloned().collect();
            (total, unique)
        };
        let postings = |fields: &AHashMap<String, Vec<String>>| -> AHashSet<String> {
            fields
                .iter()
                .flat_map(|(field, tokens)| tokens.iter().map(move |t| format!("{field}\x00{t}")))
                .collect()
        };
        let (old_total, old_unique) = words(&old);
        let (new_total, new_unique) = words(&new);
        let (old_postings, new_postings) = (postings(&old), postings(&new));
        {
            let mut inverted = self.inverted.write();
            for gone in old_postings.difference(&new_postings) {
                if let Some(set) = inverted.get_mut(gone) {
                    set.remove(key);
                    if set.is_empty() {
                        inverted.remove(gone);
                    }
                }
            }
            for came in new_postings.difference(&old_postings) {
                inverted
                    .entry(came.clone())
                    .or_default()
                    .insert(key.to_string());
            }
        }
        {
            let mut doc_freq = self.doc_freq.write();
            for gone in old_unique.difference(&new_unique) {
                if let Some(n) = doc_freq.get_mut(gone) {
                    *n = n.saturating_sub(1);
                    if *n == 0 {
                        doc_freq.remove(gone);
                    }
                }
            }
            for came in new_unique.difference(&old_unique) {
                *doc_freq.entry(came.clone()).or_insert(0) += 1;
            }
        }
        if !new_unique.is_empty() {
            let mut vocab = self.vocab.write();
            for word in &new_unique {
                if !vocab.contains(word) {
                    vocab.insert(word.clone());
                }
            }
        }
        // Only changed here, under the forward index's lock: a load and a
        // store do not race.
        match (old_total > 0, new_total > 0) {
            (false, true) => {
                self.doc_count.fetch_add(1, Relaxed);
            }
            (true, false) => {
                let n = self.doc_count.load(Relaxed);
                self.doc_count.store(n.saturating_sub(1), Relaxed);
            }
            _ => {}
        }
        let tokens = self.total_tokens.load(Relaxed);
        self.total_tokens.store(
            tokens.saturating_sub(old_total).saturating_add(new_total),
            Relaxed,
        );
        if !new.is_empty() {
            forward.insert(key.to_string(), new);
        }
    }

    /// Whether the index has a TAG or a NUMERIC field: whether it keeps
    /// postings (see [`postings`]).
    #[inline]
    pub fn keeps_postings(&self) -> bool {
        self.fields
            .iter()
            .any(|f| matches!(f.field_type, FieldType::Tag | FieldType::Numeric))
    }

    /// Whether `key` is under one of the index's prefixes.
    #[inline]
    pub fn covers(&self, key: &str) -> bool {
        self.prefixes.is_empty() || self.prefixes.iter().any(|p| key.starts_with(p.as_str()))
    }

    /// Whether `token` is one of this index's stopwords.
    pub fn is_stopword(&self, token: &str) -> bool {
        match &self.stopwords {
            Some(words) => words.iter().any(|w| w == token),
            None => is_stopword(token),
        }
    }

    /// The field queries know as `attribute` — by the name given with `AS`,
    /// or its own if it has none.
    pub fn field_by_attribute(&self, attribute: &str) -> Option<&SchemaField> {
        self.fields.iter().find(|f| f.attribute() == attribute)
    }

    /// All VECTOR fields declared in the schema, in declaration order.
    /// Use this when you need to iterate over every vector field — for
    /// indexing, unindexing, info dumps, and so on.
    pub fn vector_fields(&self) -> impl Iterator<Item = &SchemaField> {
        self.fields
            .iter()
            .filter(|f| matches!(f.field_type, FieldType::Vector { .. }))
    }

    /// Look up a specific VECTOR field by name. Returns None if there's no
    /// such field, or if the field exists but isn't a VECTOR type.
    pub fn vector_field_by_name(&self, name: &str) -> Option<&SchemaField> {
        self.fields
            .iter()
            .find(|f| f.name == name && matches!(f.field_type, FieldType::Vector { .. }))
    }

    /// Deprecated. Returns the first VECTOR field. Kept temporarily for
    /// callsites that haven't been ported to multi-vector. New code should
    /// use `vector_fields()` or `vector_field_by_name()`.
    #[deprecated(note = "use vector_fields() or vector_field_by_name() instead")]
    pub fn vector_field(&self) -> Option<&SchemaField> {
        self.vector_fields().next()
    }
}
pub type IndexStore = Arc<RwLock<AHashMap<String, IndexDef>>>;

// ── Aliases ──────────────────────────────────────────────────────────────────
//
// An alias is an entry in the index store whose key is the alias and whose
// `name` is the index it stands for. The entry carries no schema and no index
// data — it is a name, resolved to the real index before a command runs (see
// `commands::vector::handle`).
//
// Aliases used to be full copies of the index definition. The copy shared the
// index's data through `Arc`s but was indexed into as if it were a second
// index, so every document counted twice toward the corpus statistics BM25
// scores with; it kept its own stale schema after `FT.ALTER`; it held the old
// graph alive after a compaction replaced it; it was listed by `FT._LIST` and
// survived `FT.DROPINDEX`; and it was written to snapshots as a second index
// of the same name, so the alias itself did not survive a restart.

/// Whether the entry stored under `key` is an alias rather than an index.
#[inline]
pub fn is_alias(key: &str, def: &IndexDef) -> bool {
    key != def.name
}

/// The index `name` refers to — itself, or the index it is an alias of.
pub fn resolve_index_name(store: &AHashMap<String, IndexDef>, name: &str) -> Option<String> {
    let def = store.get(name)?;
    // An alias whose index has since gone resolves to nothing.
    store.contains_key(&def.name).then(|| def.name.clone())
}

/// The index `name` is an alias of — `None` if it is an index itself, or
/// nothing at all. Allocation-free for the common case of a real index name.
pub fn alias_target(store: &AHashMap<String, IndexDef>, name: &str) -> Option<String> {
    let def = store.get(name)?;
    (is_alias(name, def) && store.contains_key(&def.name)).then(|| def.name.clone())
}

/// The aliases of `index`, sorted.
pub fn index_aliases(store: &AHashMap<String, IndexDef>, index: &str) -> Vec<String> {
    let mut aliases: Vec<String> = store
        .iter()
        .filter(|(key, def)| def.name == index && is_alias(key, def))
        .map(|(key, _)| key.clone())
        .collect();
    aliases.sort_unstable();
    aliases
}

/// Point `alias` at `index`, replacing whatever `alias` pointed at before.
/// The caller has checked that `index` exists and that `alias` is not itself
/// an index.
pub fn set_alias(store: &mut AHashMap<String, IndexDef>, alias: &str, index: &IndexDef) {
    let marker = IndexSchema {
        name: index.name.clone(),
        db_index: index.db_index,
        on: index.on.clone(),
        prefixes: Vec::new(),
        fields: Vec::new(),
        aliases: Vec::new(),
        stopwords: None,
        synonyms: Default::default(),
    }
    .into_def();
    store.insert(alias.to_string(), marker);
}

/// Remove `index` and every alias of it. False if there is no such index.
pub fn remove_index(store: &mut AHashMap<String, IndexDef>, index: &str) -> bool {
    if store.get(index).is_none_or(|def| is_alias(index, def)) {
        return false;
    }
    store.retain(|_, def| def.name != index);
    true
}

pub fn new_index_store() -> IndexStore {
    Arc::new(RwLock::new(AHashMap::new()))
}

/// Drop every search index, including HNSW/FLAT graphs, inverted indexes,
/// vocab, and aliases. Redis Stack does this on `FLUSHALL`; KiviDB stores
/// indexes outside the key HashMaps, so flushing keys alone leaves the
/// graphs resident (the 1.36 GB "empty after vector bench" case).
pub fn drop_all_indexes(index_store: &IndexStore) -> usize {
    let mut g = index_store.write();
    let n = g.len();
    *g = AHashMap::new();
    n
}

/// Drop indexes whose schema was created in `db_index`. Used by `FLUSHDB`.
pub fn drop_indexes_for_db(index_store: &IndexStore, db_index: usize) -> usize {
    let mut g = index_store.write();
    let before = g.len();
    g.retain(|_, def| def.db_index != db_index);
    before - g.len()
}

/// Lightweight, serialisation-friendly description of an index.
/// Contains only the schema (name, db, prefixes, field types) — no live graph
/// data. On snapshot load, convert to a full `IndexDef` via `into_def()` and
/// then call `backfill_from_db` to rebuild all live indexes from the key data.
#[derive(Debug, Clone)]
pub struct IndexSchema {
    pub name: String,
    pub db_index: usize,
    pub on: IndexOn,
    pub prefixes: Vec<String>,
    pub fields: Vec<SchemaField>,
    /// Aliases that resolve to this index (`FT.ALIASADD`).
    pub aliases: Vec<String>,
    /// See [`IndexDef::stopwords`].
    pub stopwords: Option<Vec<String>>,
    /// The index's synonym groups (`FT.SYNUPDATE`).
    pub synonyms: SynonymGroups,
}

impl IndexSchema {
    /// Build a full `IndexDef` with empty live indexes.
    /// Call `backfill_from_db` afterwards to populate them.
    pub fn into_def(self) -> IndexDef {
        let mut indexes: AHashMap<String, LiveIndex> = AHashMap::new();
        for f in &self.fields {
            if let FieldType::Vector { algo, .. } = &f.field_type {
                let live = match algo {
                    VectorAlgo::Flat => LiveIndex::Flat(Arc::new(VectorIndex::new())),
                    VectorAlgo::Hnsw {
                        m, ef_construction, ..
                    } => LiveIndex::Hnsw(Arc::new(HnswIndex::new(*m, *ef_construction))),
                };
                indexes.insert(f.name.clone(), live);
            }
        }
        let postings = Arc::new(RwLock::new(postings::Postings::new(&self.fields)));
        IndexDef {
            name: self.name,
            db_index: self.db_index,
            on: self.on,
            prefixes: self.prefixes,
            fields: self.fields,
            indexes,
            stopwords: self.stopwords.map(Arc::new),
            synonyms: self.synonyms,
            vocab: Arc::new(RwLock::new(BTreeSet::new())),
            doc_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            total_tokens: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            doc_freq: Arc::new(RwLock::new(AHashMap::new())),
            inverted: Arc::new(RwLock::new(AHashMap::new())),
            forward: Arc::new(RwLock::new(AHashMap::new())),
            postings,
        }
    }
}

/// Index the words of some TEXT fields of every document the index covers —
/// for fields added to an index that already has documents (`FT.ALTER`).
/// Without it a query for a word in the new field finds only documents
/// written after the field was added.
pub fn backfill_text_fields(db: &crate::store::Db, def: &IndexDef, fields: &[String]) {
    for shard in db.iter() {
        let map = shard.read();
        for (key, value) in map.iter() {
            let covered =
                def.prefixes.is_empty() || def.prefixes.iter().any(|p| key.starts_with(p.as_str()));
            if !covered {
                continue;
            }
            let Some(inner) = doc_fields(&def.on, &def.fields, value) else {
                continue;
            };
            for field in fields {
                let Some(text) = inner.get(field).and_then(|b| std::str::from_utf8(b).ok()) else {
                    continue;
                };
                let tokens: Vec<String> = tokenize_for_vocab(text)
                    .filter(|tok| !def.is_stopword(tok))
                    .collect();
                def.set_text(key, field, tokens);
            }
        }
    }
}

/// Start keeping postings for TAG and NUMERIC fields added to an index
/// (`FT.ALTER`), and fill them in from the documents it covers — all of its
/// postings, which is as cheap as only the new ones. With `scan` false
/// (`SKIPINITIALSCAN`), documents are only given ids, so that `NOT` and `*`
/// still know every document.
pub fn backfill_postings(db: &crate::store::Db, def: &IndexDef, added: &[SchemaField], scan: bool) {
    def.postings.write().add_fields(added);
    for shard in db.iter() {
        let map = shard.read();
        for (key, value) in map.iter() {
            if !def.covers(key) {
                continue;
            }
            let Some(inner) = doc_fields(&def.on, &def.fields, value) else {
                continue;
            };
            let mut postings = def.postings.write();
            if scan {
                postings.write_doc(key, &inner);
            } else {
                postings.ensure(key);
            }
        }
    }
}

/// Build every index again from the documents of its database: after
/// `SWAPDB`, which swaps what two databases hold under the indexes of each.
pub fn rebuild_indexes(index_store: &IndexStore, store: &crate::store::Store) {
    let rebuilt: Vec<IndexDef> = {
        let mut indexes = index_store.write();
        let names: Vec<String> = indexes
            .iter()
            .filter(|(name, def)| !is_alias(name, def))
            .map(|(name, _)| name.clone())
            .collect();
        let mut rebuilt = Vec::with_capacity(names.len());
        for name in names {
            let def = &indexes[&name];
            let fresh = IndexSchema {
                name: def.name.clone(),
                db_index: def.db_index,
                on: def.on.clone(),
                prefixes: def.prefixes.clone(),
                fields: def.fields.clone(),
                aliases: Vec::new(),
                stopwords: def.stopwords.as_deref().cloned(),
                synonyms: def.synonyms.clone(),
            }
            .into_def();
            indexes.insert(name, fresh.clone());
            rebuilt.push(fresh);
        }
        rebuilt
    };
    for def in rebuilt {
        if let Some(db) = store.get(def.db_index) {
            backfill_from_db(db, &def);
        }
    }
}

/// Backfill an `IndexDef` by scanning all matching keys in `db`.
/// This is the DB-access core shared by `backfill_index` (called on FT.CREATE)
/// and snapshot restoration (called on startup after keys are loaded).
pub fn backfill_from_db(db: &crate::store::Db, def: &IndexDef) {
    backfill_from_db_progress(db, def, false, false);
}

/// Like [`backfill_from_db`], but logs progress every [`BACKFILL_PROGRESS_KEYS`] matching keys
/// when `log_progress` is true (used after KDB snapshot restore).
///
/// When `skip_hnsw_if_populated` is true (snapshot startup only), HNSW vector inserts are
/// skipped for any field whose graph already has vectors (e.g. loaded from `KIVIHN1`), while
/// FLAT fields and TEXT / inverted-index work still run.
const BACKFILL_PROGRESS_KEYS: usize = 50_000;

pub fn backfill_from_db_progress(
    db: &crate::store::Db,
    def: &IndexDef,
    log_progress: bool,
    skip_hnsw_if_populated: bool,
) {
    struct VecFieldWork {
        name: String,
        dim: usize,
        data_type: VectorType,
        metric: DistanceMetric,
        live: LiveIndex,
        /// The graph came with the snapshot: its vectors are not inserted
        /// again. Decided once, before the walk — asked per key, the first
        /// vector this walk inserts would make the graph look loaded and the
        /// rest would be skipped, which left a graph rebuilt after an AOF
        /// replay holding one vector.
        loaded: bool,
    }
    let mut vec_work: Vec<VecFieldWork> = Vec::new();
    for vf in def.vector_fields() {
        let (dim, data_type, metric) = match &vf.field_type {
            FieldType::Vector {
                dim,
                data_type,
                metric,
                ..
            } => (*dim, *data_type, metric.clone()),
            _ => continue,
        };
        let live = match def.indexes.get(&vf.name) {
            Some(LiveIndex::Flat(a)) => LiveIndex::Flat(Arc::clone(a)),
            Some(LiveIndex::Hnsw(a)) => LiveIndex::Hnsw(Arc::clone(a)),
            None => continue,
        };
        let loaded = skip_hnsw_if_populated
            && matches!(&live, LiveIndex::Hnsw(idx) if idx.live_count.load(Relaxed) > 0);
        vec_work.push(VecFieldWork {
            name: vf.name.clone(),
            dim,
            data_type,
            metric,
            live,
            loaded,
        });
    }

    let text_field_names: Vec<String> = def
        .fields
        .iter()
        .filter(|f| matches!(f.field_type, FieldType::Text) && !f.options.noindex)
        .map(|f| f.name.clone())
        .collect();
    let keeps_postings = def.keeps_postings();

    let mut keys_matched: usize = 0;

    for shard in db.iter() {
        #[cfg(target_os = "linux")]
        let map = shard.read();
        #[cfg(not(target_os = "linux"))]
        let map = shard.read();

        for (key, value) in map.iter() {
            let matches =
                def.prefixes.is_empty() || def.prefixes.iter().any(|p| key.starts_with(p.as_str()));
            if !matches {
                continue;
            }
            keys_matched += 1;
            if log_progress && keys_matched.is_multiple_of(BACKFILL_PROGRESS_KEYS) {
                log_verbose!(
                    "Index backfill `{}`: processed ~{} matching keys…",
                    def.name,
                    keys_matched
                );
            }
            let Some(inner) = doc_fields(&def.on, &def.fields, value) else {
                continue;
            };
            let doc = if keeps_postings {
                def.postings.write().write_doc(key, &inner)
            } else {
                postings::NO_DOC
            };

            for w in &vec_work {
                let raw_bytes = match inner.get(&w.name) {
                    Some(b) => b.clone(),
                    None => continue,
                };
                let Some(vec) = w.data_type.decode(&raw_bytes, w.dim) else {
                    continue;
                };
                match &w.live {
                    LiveIndex::Flat(idx) => idx.insert(key.to_string(), vec),
                    LiveIndex::Hnsw(idx) => {
                        if w.loaded {
                            continue;
                        }
                        hnsw_insert_doc(idx, key.to_string(), vec, &w.metric, doc);
                    }
                }
            }

            for fname in &text_field_names {
                if let Some(raw) = inner.get(fname)
                    && let Ok(s) = std::str::from_utf8(raw)
                {
                    let tokens: Vec<String> = tokenize_for_vocab(s)
                        .filter(|tok| !def.is_stopword(tok))
                        .collect();
                    def.set_text(key, fname, tokens);
                }
            }
        }
    }
}

// ── Compaction request channel ────────────────────────────────────────────────
//
// HNSW deletes leave tombstones. When the tombstone ratio crosses a threshold
// we want to rebuild the graph in the background. Rather than spawning a
// task per deletion (or doing the rebuild inline on the request path), we
// send a `CompactionRequest` on a sync_channel. A dedicated background OS
// thread drains the channel and rebuilds one index at a time.
//
// This is the same pattern used for AOF and replica client — a long-lived
// thread with an mpsc channel, no shared lock contention with the fast path.

#[derive(Debug, Clone)]
pub struct CompactionRequest {
    pub index_name: String,
    /// The specific VECTOR field whose graph to rebuild. Multi-vector
    /// indexes track tombstones per field, so compaction is per field too.
    pub field_name: String,
}
/// Sender side of the compaction channel. Cloned into every server worker
/// via Ctx; calls to `try_send` from the fast path never block.
pub type CompactionTx = std::sync::mpsc::SyncSender<CompactionRequest>;

/// Receiver side, owned by the dedicated compaction thread.
pub type CompactionRx = std::sync::mpsc::Receiver<CompactionRequest>;

/// Channel capacity. Small on purpose: if more than this number of compactions
/// are pending, the system is so overwhelmed that dropping additional requests
/// (the `try_send` Err path) is the right behaviour — they'll be re-queued
/// next time the threshold is crossed.
pub const COMPACTION_CHANNEL_CAPACITY: usize = 32;

pub fn new_compaction_channel() -> (CompactionTx, CompactionRx) {
    std::sync::mpsc::sync_channel(COMPACTION_CHANNEL_CAPACITY)
}

/// Tombstone ratio above which we trigger background compaction.
/// 0.20 = 20% of slots are ghosts — chosen as a balance between rebuild
/// frequency (CPU) and recall degradation. hnswlib uses 0.5 by default;
/// we're more aggressive because rebuild is a tokio task, not a stop-the-world.
pub const COMPACTION_THRESHOLD: f64 = 0.20;

/// Minimum tombstone count before we even consider compacting.
/// Without this, a 10-vector index with 3 deletes (30% ratio) would compact
/// constantly. We want to amortize over real workloads.
pub const COMPACTION_MIN_TOMBSTONES: usize = 64;

// ── Dictionary store ──────────────────────────────────────────────────────────
//
// Named global dictionaries for FT.DICTADD/DEL/DUMP and FT.SPELLCHECK.
// Key = dictionary name (String), Value = set of words in that dict.
// Global — not tied to any index, shared across all connections.
//
// We use AHashSet<String> per dict for O(1) membership checks during
// spell-check. The outer Arc<RwLock<...>> is the same pattern as IndexStore.

pub type DictStore = Arc<RwLock<AHashMap<String, ahash::AHashSet<String>>>>;

/// The dictionaries (`FT.DICTADD`) that go with each index store, so that
/// what saves and loads an index store's indexes can save and load its
/// dictionaries too. By the store's address; a server has one of each.
static DICT_STORES: RwLock<Vec<(usize, DictStore)>> = RwLock::new(Vec::new());

pub fn register_dict_store(index_store: &IndexStore, dicts: &DictStore) {
    let id = Arc::as_ptr(index_store) as usize;
    let mut stores = DICT_STORES.write();
    stores.retain(|(other, _)| *other != id);
    stores.push((id, Arc::clone(dicts)));
}

pub fn dict_store_for(index_store: &IndexStore) -> Option<DictStore> {
    let id = Arc::as_ptr(index_store) as usize;
    DICT_STORES
        .read()
        .iter()
        .find(|(other, _)| *other == id)
        .map(|(_, dicts)| Arc::clone(dicts))
}

/// Put back the dictionaries a snapshot held.
pub fn restore_dictionaries(index_store: &IndexStore, dictionaries: Vec<(String, Vec<String>)>) {
    if dictionaries.is_empty() {
        return;
    }
    let Some(dicts) = dict_store_for(index_store) else {
        return;
    };
    let mut dicts = dicts.write();
    for (name, terms) in dictionaries {
        dicts.entry(name).or_default().extend(terms);
    }
}

pub fn new_dict_store() -> DictStore {
    Arc::new(RwLock::new(AHashMap::new()))
}

/// Drop FT.DICTADD dictionaries. They are not Redis keys either.
pub fn drop_all_dicts(dict_store: &DictStore) {
    let mut g = dict_store.write();
    *g = AHashMap::new();
}

#[derive(Debug, Default)]
pub struct VectorIndex {
    pub vectors: RwLock<AHashMap<String, Vec<f32>>>,
}

impl VectorIndex {
    pub fn new() -> Self {
        Self {
            vectors: RwLock::new(AHashMap::new()),
        }
    }

    pub fn insert(&self, key: String, vec: Vec<f32>) {
        self.vectors.write().insert(key, vec);
    }
}

/// One node in the HNSW graph.
/// Each node lives in layers 0..=top_layer.
/// `neighbors[layer]` is the list of neighbour node ids at that layer.
///
/// A node's key and vector do not change while it is in the graph (deleting
/// it empties both, under the graph's write lock). Its neighbour lists have a
/// lock of their own, so that inserts link themselves into different parts of
/// the graph at the same time, as hnswlib's do.
#[derive(Debug)]
pub struct HnswNode {
    pub key: String,
    pub vector: Vec<f32>,
    pub neighbors: parking_lot::Mutex<Vec<Vec<usize>>>, // neighbors[layer] = [node_id, ...]
    /// The id its document has in the index's postings, or
    /// [`postings::NO_DOC`] until it is looked up: a node made by a
    /// compaction, or read from a snapshot, learns it the first time a
    /// filtered search asks (see [`knn_hnsw_admitting`]). A document keeps
    /// its id for as long as it is in the index, and leaving the index
    /// deletes its node first, so a node never names another's.
    pub doc: AtomicU32,
}

impl HnswNode {
    pub fn new(key: String, vector: Vec<f32>, neighbors: Vec<Vec<usize>>) -> Self {
        Self {
            key,
            vector,
            neighbors: parking_lot::Mutex::new(neighbors),
            doc: AtomicU32::new(postings::NO_DOC),
        }
    }

    /// The slot a deleted node leaves.
    pub fn tombstone() -> Self {
        Self::new(String::new(), Vec::new(), Vec::new())
    }

    /// Empty the slot: the node is deleted.
    fn clear(&mut self) {
        self.vector = Vec::new();
        self.key = String::new();
        *self.neighbors.get_mut() = Vec::new();
    }

    /// How many layers the node is in (none once deleted).
    fn layer_count(&self) -> usize {
        self.neighbors.lock().len()
    }
}

/// The nodes of a graph, by id. Appending takes only a shared reference, and
/// ids never move, so an insert adds its node under the graph's read lock.
pub type HnswNodes = boxcar::Vec<HnswNode>;

/// The graph held for reading.
type NodesGuard<'a> = parking_lot::RwLockReadGuard<'a, HnswNodes>;

/// The HNSW graph index.
#[derive(Debug, Default)]
pub struct HnswIndex {
    /// Searches and inserts hold this for reading; only deleting a node, which
    /// empties its slot, takes it for writing.
    pub nodes: RwLock<HnswNodes>,
    pub key_to_id: RwLock<AHashMap<String, usize>>,
    pub entry_point: RwLock<Option<usize>>, // node id of current entry point
    pub m: usize,                           // max neighbours per layer (except L0)
    pub m0: usize,                          // max neighbours at layer 0 (= 2*M)
    pub ef_construction: usize,
    pub ml: f64, // level multiplier = 1/ln(M)

    /// Number of nodes currently in `nodes` that are live (not tombstoned).
    /// We maintain this explicitly so `FT.INFO` and the compaction threshold
    /// check don't have to walk the whole Vec.
    pub live_count: AtomicUsize,

    /// Number of tombstoned slots currently in `nodes`.
    /// Invariant: nodes.len() == live_count + tombstone_count (at rest).
    pub tombstone_count: AtomicUsize,

    /// True while a background compaction is already scheduled/running for
    /// this index. Prevents piling up duplicate compaction tasks on a
    /// write-heavy delete stream.
    pub compaction_in_progress: AtomicBool,
}

impl HnswIndex {
    pub fn new(m: usize, ef_construction: usize) -> Self {
        Self {
            nodes: RwLock::new(HnswNodes::new()),
            key_to_id: RwLock::new(AHashMap::new()),
            entry_point: RwLock::new(None),
            m,
            m0: m * 2,
            ef_construction,
            ml: 1.0 / (m as f64).ln(),
            live_count: AtomicUsize::new(0),
            tombstone_count: AtomicUsize::new(0),
            compaction_in_progress: AtomicBool::new(false),
        }
    }

    /// Reconstruct an `HnswIndex` from a serialised snapshot (all structural
    /// data already parsed by the snapshot reader). The graph topology is
    /// preserved exactly — no backfill or re-insertion is needed.
    pub fn restore(
        m: usize,
        ef_construction: usize,
        nodes: Vec<HnswNode>,
        key_to_id: AHashMap<String, usize>,
        entry_point: Option<usize>,
        live_count: usize,
        tombstone_count: usize,
    ) -> Self {
        Self {
            nodes: RwLock::new(nodes.into_iter().collect()),
            key_to_id: RwLock::new(key_to_id),
            entry_point: RwLock::new(entry_point),
            m,
            m0: m * 2,
            ef_construction,
            ml: 1.0 / (m as f64).ln(),
            live_count: AtomicUsize::new(live_count),
            tombstone_count: AtomicUsize::new(tombstone_count),
            compaction_in_progress: AtomicBool::new(false),
        }
    }

    /// Draw a random top layer for a new node.
    /// Uses the same formula as the original HNSW paper.
    pub fn random_level(&self) -> usize {
        let mut rng = rand::thread_rng();
        let f: f64 = rng.r#gen::<f64>();
        (-f.ln() * self.ml).floor() as usize
    }

    /// Ratio of tombstones to total slots. Returns 0.0 for empty indexes.
    /// Cheap: two atomic loads.
    pub fn tombstone_ratio(&self) -> f64 {
        use std::sync::atomic::Ordering::Relaxed;
        let tomb = self.tombstone_count.load(Relaxed);
        let live = self.live_count.load(Relaxed);
        let total = tomb + live;
        if total == 0 {
            0.0
        } else {
            tomb as f64 / total as f64
        }
    }
}

/// Install index definitions carried by a KDB image (short write lock).
///
/// HNSW graphs saved in a snapshot are spliced straight into the `IndexDef`, so the
/// backfill that follows skips those fields entirely and the graph's precision
/// is the one that was saved rather than one rebuilt from scratch.
///
/// Lives here rather than beside the startup code because both readers of a KDB
/// image need it: `dump.kdb` at boot, and the AOF preamble during replay.
pub fn register_index_schemas(
    index_store: &IndexStore,
    schemas: &[IndexSchema],
    hnsw_graphs: &std::collections::HashMap<
        String,
        std::collections::HashMap<String, Arc<HnswIndex>>,
    >,
) {
    if schemas.is_empty() {
        return;
    }
    let mut idx = index_store.write();
    for schema in schemas {
        let mut def = schema.clone().into_def();
        if let Some(field_graphs) = hnsw_graphs.get(&def.name) {
            for (field_name, hnsw_arc) in field_graphs {
                if let Some(live) = def.indexes.get_mut(field_name) {
                    *live = LiveIndex::Hnsw(Arc::clone(hnsw_arc));
                }
            }
        }
        for alias in &schema.aliases {
            if !idx.contains_key(alias) {
                set_alias(&mut idx, alias, &def);
            }
        }
        idx.insert(schema.name.clone(), def);
    }
}

// ── Documents ─────────────────────────────────────────────────────────────────
//
// An index is `ON HASH` or `ON JSON`. Everything that reads a document for
// an index — indexing it, matching a query against it, sorting by one of its
// fields — reads the same thing: the document's fields by the name the
// schema reads them under, as bytes. For a hash that is the hash. For a JSON
// document it is what the schema's paths select, written as a hash would
// hold it: a string as it is, a number as text, an array of strings as one
// value with the field's separator between them, an array of numbers under
// a VECTOR field as little-endian floats.

/// A document's fields, as its index reads them.
pub type Fields = crate::types::HashFields;

/// [`Fields`] of one document: a hash's own, locked for reading, or the
/// projection of a JSON document.
pub enum DocFields<'a> {
    Hash(parking_lot::RwLockReadGuard<'a, Fields>),
    /// A hash some of whose fields have a TTL.
    HashEx(parking_lot::RwLockReadGuard<'a, crate::types::HashInner>),
    Json(Fields),
}

impl std::ops::Deref for DocFields<'_> {
    type Target = Fields;
    fn deref(&self) -> &Fields {
        match self {
            DocFields::Hash(fields) => fields,
            DocFields::HashEx(hash) => &hash.fields,
            DocFields::Json(fields) => fields,
        }
    }
}

/// Whether `value` is the kind of document an index `ON on` covers.
pub fn is_document(on: &IndexOn, value: crate::store::ValueRef<'_>) -> bool {
    matches!(
        (on, value),
        (IndexOn::Hash, crate::store::ValueRef::Hash(_))
            | (IndexOn::Hash, crate::store::ValueRef::HashEx(_))
            | (IndexOn::Json, crate::store::ValueRef::Json(_))
    )
}

/// The fields of `value` as an index `ON on` with this schema reads them.
/// `None` if it is not a document of that kind.
pub fn doc_fields<'a>(
    on: &IndexOn,
    schema: &[SchemaField],
    value: crate::store::ValueRef<'a>,
) -> Option<DocFields<'a>> {
    match (on, value) {
        (IndexOn::Hash, crate::store::ValueRef::Hash(hash)) => Some(DocFields::Hash(hash.read())),
        // A hash is no less a document for one of its fields having a TTL:
        // it used to drop out of every query the moment one was given one.
        (IndexOn::Hash, crate::store::ValueRef::HashEx(hash)) => {
            Some(DocFields::HashEx(hash.read()))
        }
        (IndexOn::Json, crate::store::ValueRef::Json(doc)) => {
            Some(DocFields::Json(project_json(schema, &doc.read())))
        }
        _ => None,
    }
}

/// A schema field's JSONPath, parsed. Paths are few and documents many, so
/// each thread remembers the ones it has parsed.
fn json_path(path: &str) -> Option<Arc<crate::commands::json_path::Query>> {
    use crate::commands::json_path::Query;
    thread_local! {
        static PARSED: std::cell::RefCell<AHashMap<String, Option<Arc<Query>>>> =
            std::cell::RefCell::new(AHashMap::new());
    }
    PARSED.with(|parsed| {
        let mut parsed = parsed.borrow_mut();
        if let Some(query) = parsed.get(path) {
            return query.clone();
        }
        if parsed.len() >= 1024 {
            parsed.clear();
        }
        let query = Query::parse(path).ok().map(Arc::new);
        parsed.insert(path.to_string(), query.clone());
        query
    })
}

/// What a JSON document holds for each field of `schema`, keyed by the
/// field's path.
pub fn project_json(schema: &[SchemaField], doc: &serde_json::Value) -> Fields {
    let mut fields = Fields::with_capacity_and_hasher(schema.len(), Default::default());
    for field in schema {
        let Some(query) = json_path(&field.name) else {
            continue;
        };
        let values = query.values(doc);
        if let Some(bytes) = json_field_bytes(field, &values) {
            fields.insert(field.name.clone(), bytes);
        }
    }
    fields
}

/// The strings among `values`, with those inside arrays.
fn json_strings<'a>(
    values: &'a [std::borrow::Cow<'a, serde_json::Value>],
    booleans: bool,
) -> Vec<std::borrow::Cow<'a, str>> {
    use serde_json::Value;
    use std::borrow::Cow;
    let mut out = Vec::new();
    let mut take = |value: &'a Value| match value {
        Value::String(s) => out.push(Cow::Borrowed(s.as_str())),
        Value::Bool(b) if booleans => out.push(Cow::Borrowed(if *b { "true" } else { "false" })),
        _ => {}
    };
    for value in values {
        match value.as_ref() {
            Value::Array(items) => items.iter().for_each(&mut take),
            other => take(other),
        }
    }
    out
}

fn json_field_bytes(
    field: &SchemaField,
    values: &[std::borrow::Cow<'_, serde_json::Value>],
) -> Option<Arc<[u8]>> {
    use serde_json::Value;
    let first = values.first()?;
    match &field.field_type {
        FieldType::Text => {
            let words = json_strings(values, false);
            (!words.is_empty()).then(|| Arc::from(words.join(" ").into_bytes()))
        }
        FieldType::Tag => {
            let tags = json_strings(values, true);
            let separator = field.options.separator.to_string();
            (!tags.is_empty()).then(|| Arc::from(tags.join(&separator).into_bytes()))
        }
        FieldType::Numeric => {
            // The first number the path selects.
            let number = values.iter().find_map(|value| match value.as_ref() {
                Value::Number(n) => Some(n),
                Value::Array(items) => items.iter().find_map(|item| match item {
                    Value::Number(n) => Some(n),
                    _ => None,
                }),
                _ => None,
            })?;
            Some(Arc::from(number.to_string().into_bytes()))
        }
        // The blob a hash would hold, in the field's type, so that one
        // decoder reads both.
        FieldType::Vector { dim, data_type, .. } => {
            let Value::Array(items) = first.as_ref() else {
                return None;
            };
            if items.len() != *dim {
                return None;
            }
            let mut bytes = Vec::with_capacity(dim * data_type.size());
            for item in items {
                data_type.encode(item.as_f64()?, &mut bytes);
            }
            Some(Arc::from(bytes))
        }
    }
}

/// What `FT.SEARCH … RETURN` shows for `path` of a JSON document: a string
/// as it is, anything else as JSON. `None` if the path selects nothing.
pub fn json_return_value(doc: &serde_json::Value, path: &str) -> Option<Vec<u8>> {
    let query = json_path(path)?;
    let values = query.values(doc);
    Some(match values.first()?.as_ref() {
        serde_json::Value::String(s) => s.clone().into_bytes(),
        other => serde_json::to_vec(other).ok()?,
    })
}

/// A JSON document was written: bring the `ON JSON` indexes of database
/// `db` that cover its key up to date with it.
pub fn maybe_index_json(index_store: &IndexStore, db: usize, key: &str, doc: &serde_json::Value) {
    if indexing_deferred() {
        return;
    }
    let store = index_store.read();
    for (name, def) in store.iter() {
        if is_alias(name, def)
            || !matches!(def.on, IndexOn::Json)
            || def.db_index != db
            || !def.covers(key)
        {
            continue;
        }
        let fields: Vec<(String, Arc<[u8]>)> = project_json(&def.fields, doc).into_iter().collect();
        index_fields(def, key, &fields, Write::Whole);
    }
}

/// [`maybe_index_json`] for whatever is at `key` now, if it is a JSON
/// document and any index is `ON JSON`.
/// A hash was put under `key` whole (`RESTORE`, and so `MIGRATE` and slot
/// migration): the `ON HASH` indexes that cover it are brought up to date
/// with all of its fields. A restored hash used to be in no index, so that a
/// key moved to another node of a cluster could not be found there.
pub fn reindex_json_key(
    index_store: &IndexStore,
    store: &crate::store::Store,
    db: usize,
    key: &str,
) {
    if indexing_deferred()
        || !index_store
            .read()
            .values()
            .any(|def| matches!(def.on, IndexOn::Json))
    {
        return;
    }
    let Some(db_ref) = store.get(db) else {
        return;
    };
    let value = crate::store::get_shard(db_ref, key)
        .read()
        .get(key)
        .map(ValueRef::to_value);
    if let Some(crate::types::KiviDBValue::Json(doc)) = value {
        maybe_index_json(index_store, db, key, &doc.read());
    }
}

/// Bring every index of database `db` that covers `key` up to date with what
/// is there now, whatever it is: for a command that changed a document
/// other than by writing fields of it (`HDEL`, `HINCRBY`, a field that
/// expired), or put one under a key (`RENAME`, `COPY`, `MOVE`, `RESTORE`).
/// A key that holds no document of an index any more leaves it.
///
/// With [`Write::Refresh`], a vector already in the graph is left there: the
/// write did not touch it, and inserting it again would cost an insertion.
pub fn reindex_key(
    index_store: &IndexStore,
    store: &crate::store::Store,
    db: usize,
    key: &str,
    how: Write,
) {
    if indexing_deferred() {
        return;
    }
    let indexes = index_store.read();
    if !indexes
        .iter()
        .any(|(name, def)| !is_alias(name, def) && def.db_index == db && def.covers(key))
    {
        return;
    }
    let Some(db_ref) = store.get(db) else {
        return;
    };
    let value = crate::store::get_shard(db_ref, key)
        .read()
        .get(key)
        .map(ValueRef::to_value);
    for (name, def) in indexes.iter() {
        if is_alias(name, def) || def.db_index != db || !def.covers(key) {
            continue;
        }
        match (&def.on, &value) {
            (IndexOn::Hash, Some(crate::types::KiviDBValue::Hash(hash))) => {
                let pairs: Vec<(String, Arc<[u8]>)> = hash
                    .read()
                    .iter()
                    .map(|(field, value)| (field.to_string(), Arc::clone(value)))
                    .collect();
                index_fields(def, key, &pairs, how);
            }
            (IndexOn::Hash, Some(crate::types::KiviDBValue::HashEx(hash))) => {
                let pairs: Vec<(String, Arc<[u8]>)> = hash
                    .read()
                    .fields
                    .iter()
                    .map(|(field, value)| (field.to_string(), Arc::clone(value)))
                    .collect();
                index_fields(def, key, &pairs, how);
            }
            (IndexOn::Json, Some(crate::types::KiviDBValue::Json(doc))) => {
                let fields: Vec<(String, Arc<[u8]>)> =
                    project_json(&def.fields, &doc.read()).into_iter().collect();
                index_fields(def, key, &fields, how);
            }
            _ => unindex_from(def, key),
        }
    }
}

/// How a write is applied to an index (see [`index_fields`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Write {
    /// The fields given were written; the document's others are as they
    /// were (`HSET`).
    Fields,
    /// The fields given are all the document has: a field of the schema not
    /// among them is one it does not hold (a new hash, a JSON document, a
    /// value put under the key whole).
    Whole,
    /// [`Write::Whole`], but a vector the graph already holds for the key is
    /// left there (see [`reindex_key`]).
    Refresh,
}

/// Some fields of a hash in database `db` were written: bring the `ON HASH`
/// indexes of that database that cover its key up to date with them. `how`
/// is [`Write::Whole`] for a hash the write created.
pub fn maybe_index(
    index_store: &IndexStore,
    db: usize,
    key: &str,
    fields: &[(String, Arc<[u8]>)],
    how: Write,
) {
    if indexing_deferred() {
        return;
    }
    let store = index_store.read();

    for (name, def) in store.iter() {
        if is_alias(name, def)
            || !matches!(def.on, IndexOn::Hash)
            || def.db_index != db
            || !def.covers(key)
        {
            continue;
        }
        index_fields(def, key, fields, how);
    }
}

/// Index `fields` of the document at `key` in `def`: the words of its TEXT
/// fields, its TAG and NUMERIC values, and its vectors.
fn index_fields(def: &IndexDef, key: &str, fields: &[(String, Arc<[u8]>)], how: Write) {
    let whole = how != Write::Fields;
    {
        // ── Spell-check vocabulary, inverted index, BM25 statistics ─────────
        //
        // Each TEXT field written replaces what it held before: its words,
        // stopwords left out (see `IndexDef::set_text`).
        for f in &def.fields {
            if !matches!(f.field_type, FieldType::Text) || f.options.noindex {
                continue;
            }
            let Some((_, raw)) = fields.iter().find(|(n, _)| n == &f.name) else {
                if whole {
                    def.set_text(key, &f.name, Vec::new());
                }
                continue;
            };
            let tokens: Vec<String> = match std::str::from_utf8(raw) {
                Ok(s) => tokenize_for_vocab(s)
                    .filter(|tok| !def.is_stopword(tok))
                    .collect(),
                Err(_) => Vec::new(),
            };
            def.set_text(key, &f.name, tokens);
        }
    }

    // ── TAG and NUMERIC postings ─────────────────────────────────────────────
    //
    // Every document of the index is given an id, whatever it holds: `*`
    // and `NOT` are in terms of all of them.
    let doc = if def.keeps_postings() {
        def.postings.write().write(key, fields, whole)
    } else {
        postings::NO_DOC
    };

    {
        // ── Update live vector indexes ────────────────────────────────────────
        //
        // Walk every VECTOR field in this index's schema. Each one may have
        // a value in the incoming HSET fields; insert into that field's
        // LiveIndex. Fields without a value in this HSET are simply skipped.
        for f in &def.fields {
            let (dim, data_type, metric) = match &f.field_type {
                FieldType::Vector {
                    dim,
                    data_type,
                    metric,
                    ..
                } => (*dim, *data_type, metric.clone()),
                _ => continue,
            };
            let Some(live) = def.indexes.get(&f.name) else {
                continue;
            };
            let vec = fields
                .iter()
                .find(|(n, _)| n == &f.name)
                .and_then(|(_, b)| data_type.decode(b, dim));
            let Some(vec) = vec else {
                // The document holds no vector there (any more).
                if whole {
                    unindex_vector(live, key);
                }
                continue;
            };
            match live {
                LiveIndex::Flat(idx) => idx.insert(key.to_string(), vec),
                LiveIndex::Hnsw(idx) => {
                    if how == Write::Refresh && idx.key_to_id.read().contains_key(key) {
                        continue;
                    }
                    hnsw_insert_doc(idx, key.to_string(), vec, &metric, doc);
                }
            }
        }
    }
}

/// Take the vector of the document at `key` out of a vector index: from a
/// FLAT index, at once; from a graph, by deleting its node in place.
fn unindex_vector(live: &LiveIndex, key: &str) {
    match live {
        LiveIndex::Flat(idx) => {
            idx.vectors.write().remove(key);
        }
        LiveIndex::Hnsw(idx) => {
            let node_id = idx.key_to_id.write().remove(key);
            if let Some(node_id) = node_id {
                let mut nodes = idx.nodes.write();
                if let Some(node) = nodes.get_mut(node_id)
                    && !node.vector.is_empty()
                {
                    node.clear();
                    idx.tombstone_count.fetch_add(1, Relaxed);
                    idx.live_count.fetch_sub(1, Relaxed);
                }
            }
        }
    }
}

/// The document at `key` has left `def`: its vectors, then its postings
/// (the order a filtered search relies on: see [`HnswNode::doc`]), then its
/// words.
fn unindex_from(def: &IndexDef, key: &str) {
    for live in def.indexes.values() {
        unindex_vector(live, key);
    }
    if def.keeps_postings() {
        def.postings.write().remove(key);
    }
    def.remove_text(key);
}

// ── Keys that expire on being read ───────────────────────────────────────────
//
// A key found expired by a read is deleted there and then (`helpers::
// delete_key_if_expired`), where there is no index store to hand. Each
// server's store is paired with its index store here, so that the key leaves
// its indexes too: before, it stayed in the graph and was still found by a
// KNN query.

static INDEX_STORES: RwLock<Vec<(usize, IndexStore)>> = RwLock::new(Vec::new());

fn store_id(store: &crate::store::Store) -> usize {
    Arc::as_ptr(store) as *const () as usize
}

/// Pair a server's store with its index store.
pub fn register_index_store(store: &crate::store::Store, index_store: &IndexStore) {
    let id = store_id(store);
    let mut stores = INDEX_STORES.write();
    stores.retain(|(other, _)| *other != id);
    stores.push((id, Arc::clone(index_store)));
}

/// `key` of database `db` expired and was deleted by a read.
pub fn unindex_expired(store: &crate::store::Store, db: usize, key: &str) {
    let id = store_id(store);
    let index_store = INDEX_STORES
        .read()
        .iter()
        .find(|(other, _)| *other == id)
        .map(|(_, index_store)| Arc::clone(index_store));
    if let Some(index_store) = index_store {
        maybe_unindex(&index_store, db, key);
    }
}

// ── Distance functions ───────────────────────────────────────────────────────
//
// Each is one pass of a kernel in `simd`: AVX-512, AVX2 with FMA, or NEON,
// picked at run time, or eight independent sums where there is none. The
// sequential `iter().map().sum()` these replace could not be vectorised: the
// compiler may not reorder float additions.

pub fn dist_l2_sq(a: &[f32], b: &[f32]) -> f32 {
    simd::l2_sq(a, b)
}

pub fn dist_cosine(a: &[f32], b: &[f32]) -> f32 {
    let (dot, na, nb) = simd::dot_norms(a, b);
    let denom = na.sqrt() * nb.sqrt();
    if denom == 0.0 {
        return 1.0;
    }
    1.0 - (dot / denom)
}

pub fn dist_ip(a: &[f32], b: &[f32]) -> f32 {
    1.0 - simd::dot(a, b)
}

pub fn distance(metric: &DistanceMetric, a: &[f32], b: &[f32]) -> f32 {
    match metric {
        DistanceMetric::L2 => dist_l2_sq(a, b),
        DistanceMetric::Cosine => dist_cosine(a, b),
        DistanceMetric::InnerProduct => dist_ip(a, b),
    }
}

/// Fast cosine distance for pre-normalized (unit-length) vectors.
/// Cosine vectors are normalized to unit length at ingestion, so the
/// denominator is always 1.0 and we skip the two sqrt() calls entirely.
/// For L2 and InnerProduct the behaviour is identical to `distance`.
#[inline(always)]
pub(crate) fn distance_prenorm(metric: &DistanceMetric, a: &[f32], b: &[f32]) -> f32 {
    match metric {
        DistanceMetric::Cosine => dist_ip(a, b), // |a|=|b|=1 → 1 – dot(a,b)
        DistanceMetric::L2 => dist_l2_sq(a, b),
        DistanceMetric::InnerProduct => dist_ip(a, b),
    }
}

/// Normalize a vector to unit length in-place (for cosine ingestion).
fn normalize_to_unit(mut v: Vec<f32>) -> Vec<f32> {
    let mag = simd::norm_sq(&v).sqrt();
    if mag > 0.0 {
        for x in &mut v {
            *x /= mag;
        }
    }
    v
}

/// A query made ready to be measured against many stored vectors: for a
/// cosine, scaled to unit length once, so that each distance after is one
/// pass that leaves out the query's own norm.
pub struct PreparedQuery<'a> {
    metric: &'a DistanceMetric,
    vector: std::borrow::Cow<'a, [f32]>,
}

impl<'a> PreparedQuery<'a> {
    pub fn new(metric: &'a DistanceMetric, query: &'a [f32]) -> Self {
        let vector = match metric {
            DistanceMetric::Cosine => std::borrow::Cow::Owned(normalize_to_unit(query.to_vec())),
            _ => std::borrow::Cow::Borrowed(query),
        };
        Self { metric, vector }
    }

    /// The query as it is measured: of unit length for a cosine.
    pub fn vector(&self) -> &[f32] {
        &self.vector
    }

    /// The distance to a vector stored as it was given (a FLAT index):
    /// what [`distance`] gives on the query as it was.
    #[inline]
    pub fn to_raw(&self, stored: &[f32]) -> f32 {
        match self.metric {
            DistanceMetric::Cosine => {
                let (dot, nb) = simd::dot_norm_b(&self.vector, stored);
                if nb == 0.0 {
                    // A zero vector on either side, as `dist_cosine` has it.
                    return 1.0;
                }
                1.0 - dot / nb.sqrt()
            }
            DistanceMetric::L2 => dist_l2_sq(&self.vector, stored),
            DistanceMetric::InnerProduct => dist_ip(&self.vector, stored),
        }
    }

    /// The distance to a vector an HNSW graph holds: of unit length for a
    /// cosine.
    #[inline]
    pub fn to_unit(&self, stored: &[f32]) -> f32 {
        distance_prenorm(self.metric, &self.vector, stored)
    }
}

// A result from KNN search
#[derive(Debug)]
pub struct KnnResult {
    pub key: String,
    pub score: f32,
}

// ── The f32 ordering problem ─────────────────────────────────────────────────
//
// Rust's BinaryHeap requires Ord. f32 does NOT implement Ord because NaN
// breaks total ordering (NaN != NaN). We wrap f32 in a newtype and implement
// Ord ourselves, treating NaN as greater than everything (i.e. worst score).
// This is a common Rust pattern whenever you need to sort/heap floats.

// #[derive(PartialEq)]
// struct OrdF32(f32);

// impl Eq for OrdF32 {}

// impl PartialOrd for OrdF32 {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         Some(self.cmp(other))
//     }
// }

// impl Ord for OrdF32 {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         // total_cmp is stable since Rust 1.62 — handles NaN/inf correctly
//         self.0.total_cmp(&other.0)
//     }
// }

pub fn knn_flat(
    index: &VectorIndex,
    query: &[f32],
    k: usize,
    metric: &DistanceMetric,
) -> Vec<KnnResult> {
    // Collect all distances first, then partial-sort.
    // Avoids the heap tie-breaking problem entirely.
    let vectors = index.vectors.read();
    let prepared = PreparedQuery::new(metric, query);

    let mut scored: Vec<(f32, &str)> = vectors
        .iter()
        .filter(|(_, vec)| vec.len() == query.len())
        .map(|(key, vec)| (prepared.to_raw(vec), key.as_str()))
        .collect();

    // partial_sort_unstable would be ideal but isn't in std.
    // select_nth_unstable partitions so that element k is in its sorted position
    // and everything before it is <= it — O(n) average, no full sort needed.
    // This is the standard competitive KNN trick for FLAT search.
    if scored.len() > k {
        scored.select_nth_unstable_by(k, |a, b| a.0.total_cmp(&b.0));
        scored.truncate(k);
    }

    // Now sort the kept k results ascending by distance
    scored.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));

    scored
        .into_iter()
        .map(|(score, key)| KnnResult {
            key: key.to_string(),
            score,
        })
        .collect()
}

/// Algorithm 4 from the HNSW paper — heuristic neighbor selection.
///
/// The greedy approach (take the M closest) clusters neighbors in the same
/// direction in high-dimensional spaces. This hurts recall badly at 100D+.
///
/// The heuristic: accept a candidate only if it is closer to the query `q`
/// than it is to every already-accepted result neighbor. This ensures the
/// M selected neighbors "fan out" across the local neighborhood rather than
/// piling up in one direction.
///
/// `candidates` — (dist_to_q, node_id), sorted ascending by dist_to_q.
/// `keep_pruned` — if true, fill remaining slots with the discarded set so
///                 we always return exactly m_max neighbors when possible.
/// Choose up to `m_max` of `candidates` as a node's neighbours. Like the
/// search, it offers its read lock to a waiting insert every few candidates.
fn select_neighbors_heuristic(
    nodes: &mut NodesGuard<'_>,
    candidates: &[(f32, usize)],
    m_max: usize,
    metric: &DistanceMetric,
    keep_pruned: bool,
) -> Vec<usize> {
    let mut result: Vec<(f32, usize)> = Vec::with_capacity(m_max);
    let mut discarded: Vec<(f32, usize)> = Vec::new();

    'cand: for (i, &(d_to_q, cand_id)) in candidates.iter().enumerate() {
        if result.len() >= m_max {
            break;
        }
        if i > 0 && i.is_multiple_of(EXPANSIONS_PER_YIELD) {
            parking_lot::RwLockReadGuard::bump(nodes);
        }
        let cand_vec = &nodes[cand_id].vector;
        if cand_vec.is_empty() {
            continue;
        }
        // Key heuristic check: is the candidate closer to q than to any
        // already-selected result neighbor?  If a result neighbor is closer
        // to the candidate than q is, the candidate is "dominated" — the
        // result neighbor already covers that direction better.
        for &(_, sel_id) in &result {
            let d_cand_to_sel = distance_prenorm(metric, cand_vec, &nodes[sel_id].vector);
            if d_cand_to_sel < d_to_q {
                discarded.push((d_to_q, cand_id));
                continue 'cand;
            }
        }
        result.push((d_to_q, cand_id));
    }

    // Fill remaining capacity from the pruned set so we don't leave a node
    // with fewer connections than intended (sparse graphs hurt recall too).
    if keep_pruned {
        for &(d, id) in &discarded {
            if result.len() >= m_max {
                break;
            }
            result.push((d, id));
        }
    }

    result.into_iter().map(|(_, id)| id).collect()
}

/// Insert a new vector into the HNSW index.
pub fn hnsw_insert(index: &Arc<HnswIndex>, key: String, vector: Vec<f32>, metric: &DistanceMetric) {
    hnsw_insert_doc(index, key, vector, metric, postings::NO_DOC);
}

/// [`hnsw_insert`] of the vector of the document whose id is `doc`.
pub fn hnsw_insert_doc(
    index: &Arc<HnswIndex>,
    key: String,
    vector: Vec<f32>,
    metric: &DistanceMetric,
    doc: u32,
) {
    use std::sync::atomic::Ordering::Relaxed;

    // ── Phase 0: tombstone old node if key exists ─────────────────────────
    {
        let key_to_id = index.key_to_id.read();
        if let Some(&old_id) = key_to_id.get(&key) {
            drop(key_to_id);
            let mut nodes = index.nodes.write();
            if let Some(node) = nodes.get_mut(old_id)
                && !node.vector.is_empty()
            {
                node.clear();
                index.tombstone_count.fetch_add(1, Relaxed);
                index.live_count.fetch_sub(1, Relaxed);
            }
            // don't return — we still need to insert the new node
        }
    }

    // Cosine vectors are stored as unit-length so that all distance calls can
    // skip the expensive sqrt/divide and use pure dot product instead.
    let vector = if matches!(metric, DistanceMetric::Cosine) {
        normalize_to_unit(vector)
    } else {
        vector
    };

    let level = {
        use rand::Rng;
        let f: f64 = rand::thread_rng().r#gen::<f64>();
        (-f.ln() * index.ml).floor() as usize
    };

    // ── Phase 1: snapshot entry point ────────────────────────────────────
    let ep_snapshot = *index.entry_point.read();

    // ── Phase 2: pure read traversal — NO global write lock held ─────────
    // This is the hot path. Concurrent inserts and searches run freely here.
    //
    // precomputed_backlinks: for each forward neighbor predicted to overflow
    // after receiving a backlink, we pre-score its current neighbors against
    // its own vector during Phase 2 (read lock, concurrent). Phase 6 inserts
    // new_id into that sorted list and runs a bounded heuristic (K=4) — only
    // O(M×K) pairwise distance calls under the write lock instead of O(M²).
    // Tuple: (nb_id, layer, nb_vec_owned, pre_scored_sorted, snapshot_len).
    // A forward neighbour that will overflow when it receives its back-link,
    // pruned during the search: (neighbour id, layer, its neighbours as the
    // search saw them, the list to replace them with). The new node is not in
    // the graph yet, so the list names it NEW_NODE.
    type PrecomputedBacklink = (usize, usize, Vec<usize>, Vec<usize>);
    const NEW_NODE: usize = usize::MAX;
    let mut precomputed_backlinks: Vec<PrecomputedBacklink> = Vec::new();

    // The whole insert holds the graph for reading: it appends its node and
    // links it in under the neighbours' own locks, so inserts do not wait for
    // one another, nor searches for them. Before, each insert took the write
    // lock to link itself in, and a writer that waits holds back every reader
    // that comes after it: 6,000 inserts a second on 64 cores, 51 of 62
    // workers waiting.
    let mut nodes = index.nodes.read();
    let neighbors_per_layer: Vec<Vec<usize>> = if let Some(mut ep) = ep_snapshot {
        // Check if entry point is tombstoned — find a live replacement
        if nodes.get(ep).map(|n| n.vector.is_empty()).unwrap_or(true) {
            let k2id = index.key_to_id.read();
            match k2id.values().next().copied() {
                Some(id) => ep = id,
                None => {
                    // No live nodes at all — we become the entry point
                    drop(k2id);
                    let neighbors = (0..=level).map(|_| Vec::new()).collect();
                    let node = HnswNode::new(key.clone(), vector, neighbors);
                    node.doc.store(doc, Relaxed);
                    let new_id = nodes.push(node);
                    index.key_to_id.write().insert(key, new_id);
                    index.live_count.fetch_add(1, Relaxed);
                    *index.entry_point.write() = Some(new_id);
                    return;
                }
            }
        }

        let ep_level = nodes[ep].layer_count().saturating_sub(1);

        // Greedy descent from top layer to level+1 (just finding better ep)
        let mut curr_ep = ep;
        let mut per_layer: Vec<Vec<usize>> = Vec::with_capacity(level + 1);
        for lc in (level + 1..=ep_level).rev() {
            let results = hnsw_search_layer_concurrent(&mut nodes, &vector, curr_ep, 1, lc, metric);
            if let Some(&(_, best)) = results.first() {
                curr_ep = best;
            }
        }

        // Search and collect neighbors for layers 0..=min(level, ep_level)
        for lc in (0..=level.min(ep_level)).rev() {
            let m_max = if lc == 0 { index.m0 } else { index.m };
            let candidates = hnsw_search_layer_concurrent(
                &mut nodes,
                &vector,
                curr_ep,
                index.ef_construction,
                lc,
                metric,
            );
            if let Some(&(_, best)) = candidates.first() {
                curr_ep = best;
            }
            let selected = select_neighbors_heuristic(&mut nodes, &candidates, m_max, metric, true);
            per_layer.push(selected);
        }

        // Reverse first so per_layer[0] = layer-0 neighbors, then pad with
        // empty vecs for layers above ep_level when level > ep_level.
        // (Padding before reversing would place the empty entries at [0].)
        per_layer.reverse(); // per_layer[0] = layer 0 neighbors
        while per_layer.len() <= level {
            per_layer.push(vec![]);
        }

        // Prune, before taking any neighbour's lock, every forward neighbour
        // that the back-link will overflow; linking then only swaps the
        // pruned list in. Pruning reads up to M x 4 vectors from all over the
        // graph, for some 32 neighbours an insert.
        for (lc, forward_neighbors) in per_layer.iter().enumerate() {
            let m_max = if lc == 0 { index.m0 } else { index.m };
            for &nb_id in forward_neighbors {
                let nb_node = &nodes[nb_id];
                if nb_node.vector.is_empty() {
                    continue;
                }
                let lc_neighbors = match nb_node.neighbors.lock().get(lc) {
                    Some(n) if n.len() >= m_max => n.clone(),
                    _ => continue, // won't overflow
                };
                let nb_vec = &nb_node.vector;
                let vector_of = |id: usize| {
                    if id == NEW_NODE {
                        &vector[..]
                    } else {
                        &nodes[id].vector[..]
                    }
                };
                let mut scored: Vec<(f32, usize)> = lc_neighbors
                    .iter()
                    .filter_map(|&id| {
                        let n = &nodes[id];
                        if n.vector.is_empty() {
                            return None;
                        }
                        Some((distance_prenorm(metric, nb_vec, &n.vector), id))
                    })
                    .collect();
                scored.push((distance_prenorm(metric, nb_vec, &vector), NEW_NODE));
                scored.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));

                // Bounded Algorithm-4 heuristic: check each candidate
                // against at most K=4 already-accepted neighbours.
                for i in 0..scored.len() {
                    let (d_i, id_i) = scored[i];
                    let vec_i = vector_of(id_i);
                    let mut checks = 0usize;
                    for j in 0..i {
                        let (d_j, id_j) = scored[j];
                        if d_j.is_infinite() {
                            continue;
                        }
                        if distance_prenorm(metric, vec_i, vector_of(id_j)) < d_i {
                            scored[i].0 = f32::INFINITY;
                            break;
                        }
                        checks += 1;
                        if checks >= 4 {
                            break;
                        }
                    }
                }
                let mut pruned = Vec::with_capacity(m_max);
                let kept = scored.iter().filter(|(d, _)| !d.is_infinite());
                let dropped = scored.iter().filter(|(d, _)| d.is_infinite());
                pruned.extend(kept.chain(dropped).take(m_max).map(|&(_, id)| id));
                precomputed_backlinks.push((nb_id, lc, lc_neighbors, pruned));
                parking_lot::RwLockReadGuard::bump(&mut nodes);
            }
        }

        per_layer
    } else {
        // First node ever — empty neighbor lists
        (0..=level).map(|_| vec![]).collect()
    };

    // Append the node. Nothing finds it until a neighbour links back to it
    // below, or a reader looks its key up after the next line.
    let forward_links = neighbors_per_layer.clone();
    let node = HnswNode::new(key.clone(), vector, neighbors_per_layer);
    node.doc.store(doc, Relaxed);
    let new_id = nodes.push(node);
    index.key_to_id.write().insert(key, new_id);
    index.live_count.fetch_add(1, Relaxed);

    // Entry point bump if the new node has higher level.
    {
        let mut ep = index.entry_point.write();
        match *ep {
            None => *ep = Some(new_id),
            Some(current_ep) => {
                let current_level = nodes[current_ep].layer_count().saturating_sub(1);
                if level > current_level {
                    *ep = Some(new_id);
                }
            }
        }
    }

    // Back-links, each under that neighbour's lock alone.
    for (lc, forward) in forward_links.iter().enumerate() {
        let m_max = if lc == 0 { index.m0 } else { index.m };
        for &nb_id in forward {
            let nb = &nodes[nb_id];
            if nb.vector.is_empty() {
                continue;
            }
            let mut links = nb.neighbors.lock();
            while links.len() <= lc {
                links.push(Vec::new());
            }
            let list = &mut links[lc];
            list.push(new_id);
            if list.len() <= m_max {
                continue;
            }

            // Fast path: the list pruned during the search, if the
            // neighbour's list is still the one it pruned (with new_id just
            // pushed on the end).
            let fresh = precomputed_backlinks.iter().find(|(pid, plc, seen, _)| {
                *pid == nb_id && *plc == lc && list[..list.len() - 1] == seen[..]
            });
            if let Some((_, _, _, pruned)) = fresh {
                list.clear();
                list.extend(
                    pruned
                        .iter()
                        .map(|&id| if id == NEW_NODE { new_id } else { id }),
                );
                continue;
            }

            // Stale (a concurrent insert changed the list): keep the m_max
            // nearest.
            let candidates: Vec<usize> = std::mem::take(list);
            BACKLINK_SCRATCH.with(|cell| {
                let mut scored = cell.borrow_mut();
                scored.clear();
                scored.reserve(candidates.len());
                for &id in &candidates {
                    let n = &nodes[id];
                    if n.vector.is_empty() {
                        continue;
                    }
                    scored.push((distance_prenorm(metric, &nb.vector, &n.vector), id));
                }
                scored.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
                scored.truncate(m_max);
                list.reserve(scored.len());
                list.extend(scored.iter().map(|&(_, id)| id));
            });
        }
    }
}
thread_local! {
    static BACKLINK_SCRATCH: std::cell::RefCell<Vec<(f32, usize)>> =
        std::cell::RefCell::new(Vec::with_capacity(64));
}
/// Per-thread scratch buffers for HNSW search.
/// Allocated once per thread, reused across every search call.
/// clear() is O(n) but avoids malloc/free on the hot path.
struct SearchBuffers {
    visited: AHashSet<usize>,
    candidates:
        std::collections::BinaryHeap<std::cmp::Reverse<(ordered_float::OrderedFloat<f32>, usize)>>,
    dynamic: std::collections::BinaryHeap<(ordered_float::OrderedFloat<f32>, usize)>,
    neighbor_scratch: Vec<usize>,
}
impl SearchBuffers {
    fn new() -> Self {
        Self {
            visited: AHashSet::with_capacity(256),
            candidates: std::collections::BinaryHeap::with_capacity(256),
            dynamic: std::collections::BinaryHeap::with_capacity(256),
            neighbor_scratch: Vec::with_capacity(64),
        }
    }

    fn clear(&mut self) {
        self.visited.clear();
        self.candidates.clear();
        self.dynamic.clear();
        self.neighbor_scratch.clear();
    }
}

thread_local! {
    static SEARCH_BUFFERS: std::cell::RefCell<SearchBuffers> =
        std::cell::RefCell::new(SearchBuffers::new());
}

/// Greedy beam search within one layer.
/// Returns up to `ef` candidates as (distance, node_id), sorted ascending.
/// How many candidates a search expands between offers of its read lock to a
/// waiting insert (see [`hnsw_search_layer_concurrent`]).
const EXPANSIONS_PER_YIELD: usize = 4;

/// Search one layer of the graph from `ep`, the graph held for reading.
///
/// Every few candidates the read lock is handed to a delete waiting for the
/// write lock, if there is one, and taken back: a writer that waits holds back
/// every reader that comes after it, so a long search would hold up all of
/// them. Ids stay valid across the yield: nodes are only appended, and deleted
/// ones are tombstoned in place.
fn hnsw_search_layer_concurrent(
    nodes: &mut NodesGuard<'_>,
    query: &[f32],
    ep: usize,
    ef: usize,
    layer: usize,
    metric: &DistanceMetric,
) -> Vec<(f32, usize)> {
    let ep_dist = {
        let n = &nodes[ep];
        if n.vector.is_empty() {
            return vec![];
        }
        distance_prenorm(metric, query, &n.vector)
    };

    SEARCH_BUFFERS.with(|cell| {
        let mut bufs = cell.borrow_mut();
        bufs.clear();

        let of_ep = ordered_float::OrderedFloat(ep_dist);
        bufs.visited.insert(ep);
        bufs.candidates.push(std::cmp::Reverse((of_ep, ep)));
        bufs.dynamic.push((of_ep, ep));

        let mut expanded = 0usize;
        while let Some(std::cmp::Reverse((c_dist, c_id))) = bufs.candidates.pop() {
            let worst = bufs.dynamic.peek().map(|(d, _)| d.0).unwrap_or(f32::MAX);
            if bufs.dynamic.len() >= ef && c_dist.0 > worst {
                break;
            }
            expanded += 1;
            if expanded.is_multiple_of(EXPANSIONS_PER_YIELD) {
                parking_lot::RwLockReadGuard::bump(nodes);
            }

            // Copy neighbors into the thread-local scratch buffer.
            bufs.neighbor_scratch.clear();
            {
                let links = nodes[c_id].neighbors.lock();
                if let Some(list) = links.get(layer) {
                    bufs.neighbor_scratch.extend_from_slice(list);
                }
            }

            // Index-based iteration so we can mutate other bufs fields inside.
            for i in 0..bufs.neighbor_scratch.len() {
                let nb_id = bufs.neighbor_scratch[i];
                if !bufs.visited.insert(nb_id) {
                    continue;
                }

                let nb_dist = {
                    let n = &nodes[nb_id];
                    if n.vector.is_empty() {
                        continue;
                    }
                    distance_prenorm(metric, query, &n.vector)
                };

                let worst = bufs.dynamic.peek().map(|(d, _)| d.0).unwrap_or(f32::MAX);
                if nb_dist < worst || bufs.dynamic.len() < ef {
                    let of = ordered_float::OrderedFloat(nb_dist);
                    bufs.candidates.push(std::cmp::Reverse((of, nb_id)));
                    bufs.dynamic.push((of, nb_id));
                    if bufs.dynamic.len() > ef {
                        bufs.dynamic.pop();
                    }
                }
            }
        }

        // Drain dynamic into result vec — reuse the allocation
        let mut result: Vec<(f32, usize)> = bufs.dynamic.drain().map(|(d, id)| (d.0, id)).collect();
        result.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
        result
    })
}

/// KNN search using the HNSW graph.
pub fn knn_hnsw(
    index: &HnswIndex,
    query: &[f32],
    k: usize,
    ef: usize,
    metric: &DistanceMetric,
) -> Vec<KnnResult> {
    let ef = ef.max(k).max(50);

    // Normalize the query for cosine so distances match the pre-normalized stored vectors.
    let query_buf: Vec<f32>;
    let query: &[f32] = if matches!(metric, DistanceMetric::Cosine) {
        query_buf = normalize_to_unit(query.to_vec());
        &query_buf
    } else {
        query
    };

    let mut ep_id = match *index.entry_point.read() {
        Some(id) => id,
        None => return vec![],
    };

    let mut nodes = index.nodes.read();

    // Handle tombstoned entry point
    if nodes
        .get(ep_id)
        .map(|n| n.vector.is_empty())
        .unwrap_or(true)
    {
        let k2id = index.key_to_id.read();
        match k2id.values().next().copied() {
            Some(id) => ep_id = id,
            None => return vec![],
        }
    }

    let live_count = index.live_count.load(std::sync::atomic::Ordering::Relaxed);

    // Small graph: brute force
    if live_count < 500 {
        let mut scored: Vec<(f32, usize)> = nodes
            .iter()
            .filter_map(|(i, n)| {
                if n.vector.is_empty() || n.vector.len() != query.len() {
                    return None;
                }
                Some((distance_prenorm(metric, query, &n.vector), i))
            })
            .collect();
        scored.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
        scored.truncate(k);
        return scored
            .into_iter()
            .map(|(score, id)| KnnResult {
                key: nodes[id].key.clone(),
                score,
            })
            .collect();
    }

    // Large graph: HNSW traversal
    let ep_level = nodes[ep_id].layer_count().saturating_sub(1);
    let mut curr_ep = ep_id;

    for lc in (1..=ep_level).rev() {
        let candidates = hnsw_search_layer_concurrent(&mut nodes, query, curr_ep, 1, lc, metric);
        if let Some(&(_, best)) = candidates.first() {
            curr_ep = best;
        }
    }

    let mut results = hnsw_search_layer_concurrent(&mut nodes, query, curr_ep, ef, 0, metric);

    // Fallback search from original entry point if we got too few results
    if results.len() < k && curr_ep != ep_id {
        let mut more = hnsw_search_layer_concurrent(&mut nodes, query, ep_id, ef, 0, metric);
        results.append(&mut more);
        let mut seen: AHashMap<usize, f32> = AHashMap::new();
        for (dist, id) in results {
            seen.entry(id)
                .and_modify(|d| {
                    if dist < *d {
                        *d = dist;
                    }
                })
                .or_insert(dist);
        }
        results = seen.into_iter().map(|(id, dist)| (dist, id)).collect();
        results.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
    }

    results.truncate(k);
    results
        .into_iter()
        .filter(|(_, id)| !nodes[*id].vector.is_empty())
        .map(|(score, id)| KnnResult {
            key: nodes[id].key.clone(),
            score,
        })
        .collect()
}

/// KNN over the graph, among only the nodes `admit` lets in: the search
/// walks every node, as an unfiltered one does, but only those let in are
/// kept as results, and it goes on until it holds `ef` of them or has
/// nowhere nearer to go. hnswlib filters the same way.
///
/// `query` is as the graph holds vectors: of unit length for a cosine (see
/// [`PreparedQuery::vector`]). After `budget` distances the search gives up
/// and returns `None`: a filter that lets few nodes in makes the walk long,
/// and measuring the few it lets in is then the cheaper way.
pub fn knn_hnsw_admitting(
    index: &HnswIndex,
    query: &[f32],
    k: usize,
    ef: usize,
    metric: &DistanceMetric,
    admit: &mut dyn FnMut(&HnswNode) -> bool,
    budget: usize,
) -> Option<Vec<KnnResult>> {
    use ordered_float::OrderedFloat;
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let ef = ef.max(k);
    let mut ep_id = (*index.entry_point.read())?;
    let mut nodes = index.nodes.read();
    if nodes.get(ep_id).is_none_or(|n| n.vector.is_empty()) {
        ep_id = index.key_to_id.read().values().next().copied()?;
    }

    // Down to layer 1, nearest first, as an unfiltered search goes.
    let ep_level = nodes[ep_id].layer_count().saturating_sub(1);
    let mut curr = ep_id;
    for lc in (1..=ep_level).rev() {
        let found = hnsw_search_layer_concurrent(&mut nodes, query, curr, 1, lc, metric);
        if let Some(&(_, best)) = found.first() {
            curr = best;
        }
    }

    // Layer 0, keeping only what is let in.
    let mut spent = 0usize;
    let mut visited = postings::Bits::new();
    let mut candidates: BinaryHeap<Reverse<(OrderedFloat<f32>, usize)>> = BinaryHeap::new();
    let mut results: BinaryHeap<(OrderedFloat<f32>, usize)> = BinaryHeap::new();
    let mut neighbors: Vec<usize> = Vec::with_capacity(64);

    let start = &nodes[curr];
    if start.vector.is_empty() {
        return Some(Vec::new());
    }
    let d = OrderedFloat(distance_prenorm(metric, query, &start.vector));
    spent += 1;
    visited.set(curr as u32);
    candidates.push(Reverse((d, curr)));
    if admit(start) {
        results.push((d, curr));
    }
    let mut bound = if results.len() >= ef {
        results.peek().map_or(f32::INFINITY, |(d, _)| d.0)
    } else {
        f32::INFINITY
    };

    let mut expanded = 0usize;
    while let Some(Reverse((d, id))) = candidates.pop() {
        if d.0 > bound && results.len() >= ef {
            break;
        }
        expanded += 1;
        if expanded.is_multiple_of(EXPANSIONS_PER_YIELD) {
            parking_lot::RwLockReadGuard::bump(&mut nodes);
        }
        neighbors.clear();
        if let Some(list) = nodes[id].neighbors.lock().first() {
            neighbors.extend_from_slice(list);
        }
        for &nb in &neighbors {
            if visited.get(nb as u32) {
                continue;
            }
            visited.set(nb as u32);
            let node = &nodes[nb];
            if node.vector.is_empty() || node.vector.len() != query.len() {
                continue;
            }
            spent += 1;
            if spent > budget {
                return None;
            }
            let dist = distance_prenorm(metric, query, &node.vector);
            if results.len() < ef || dist < bound {
                candidates.push(Reverse((OrderedFloat(dist), nb)));
                if admit(node) {
                    results.push((OrderedFloat(dist), nb));
                    if results.len() > ef {
                        results.pop();
                    }
                    if results.len() >= ef {
                        bound = results.peek().map_or(f32::INFINITY, |(d, _)| d.0);
                    }
                }
            }
        }
    }

    let mut found: Vec<(f32, usize)> = results.into_iter().map(|(d, id)| (d.0, id)).collect();
    found.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
    found.truncate(k);
    Some(
        found
            .into_iter()
            .filter(|(_, id)| !nodes[*id].vector.is_empty())
            .map(|(score, id)| KnnResult {
                key: nodes[id].key.clone(),
                score,
            })
            .collect(),
    )
}

/// Called when a key is deleted. Removes it from any matching indexes.
/// For FLAT this is O(1). For HNSW we tombstone — mark deleted but
/// leave the graph structure intact (full graph surgery is expensive).
///
/// Only the indexes of database `db`: the same key in another database is
/// another document. A `DEL` in one database used to take the document of
/// the same name out of an index of another.
pub fn maybe_unindex(index_store: &IndexStore, db: usize, key: &str) {
    if indexing_deferred() {
        return;
    }
    let store = index_store.read();

    for (name, def) in store.iter() {
        if is_alias(name, def) || def.db_index != db || !def.covers(key) {
            continue;
        }
        // Every vector field's, its postings, and what its TEXT fields put
        // in: the words it held, from the inverted index and the BM25
        // statistics.
        unindex_from(def, key);
    }
}
// ── HNSW compaction ───────────────────────────────────────────────────────────
//
// Rebuilds an HNSW index from scratch, dropping all tombstoned slots.
// Runs on the dedicated compaction thread, NEVER on a server worker.
//
// Concurrency model: shadow-index with atomic Arc swap.
//   1. Read snapshot: clone the current Arc<HnswIndex>. Cheap (refcount bump).
//      Searches and inserts continue using this Arc — they are unaffected.
//   2. Rebuild: walk the snapshot's nodes, re-insert each live one into a
//      fresh HnswIndex. No locks held on the snapshot during this loop;
//      each node read takes the snapshot's RwLock briefly then releases.
//   3. Swap: take a write lock on IndexStore, replace LiveIndex::Hnsw(old_arc)
//      with LiveIndex::Hnsw(new_arc). Drop the lock immediately.
//   4. Old Arc has its refcount drop to zero once all in-flight searches
//      finish their reads. Memory is reclaimed automatically.
//
// Writes (HSET) that arrived during step 2 went into the OLD graph. After
// the swap they are visible to no-one — this is the only correctness gotcha
// of shadow-index. We mitigate by re-checking after swap: any keys present
// in the IndexStore's data hashes but missing from the new graph get
// re-inserted in a "catch-up" pass.

pub fn compact_hnsw(
    index_store: &IndexStore,
    db_store: &crate::store::Store,
    index_name: &str,
    field_name: &str,
) -> Result<usize, &'static str> {
    use std::sync::atomic::Ordering;

    // ── Step 1: snapshot the Arc<HnswIndex> for the named field ──────────
    let (old_arc, metric, dim, data_type, prefixes, m, ef_construction) = {
        let store = index_store.read();
        let def = match store.get(index_name) {
            Some(d) => d,
            None => return Err("index not found"),
        };

        let old_arc = match def.indexes.get(field_name) {
            Some(LiveIndex::Hnsw(arc)) => Arc::clone(arc),
            Some(_) => return Err("field is not HNSW"),
            None => return Err("no such vector field on index"),
        };

        if old_arc
            .compaction_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err("compaction already in progress");
        }

        let vf = match def.vector_field_by_name(field_name) {
            Some(f) => f,
            None => {
                old_arc
                    .compaction_in_progress
                    .store(false, Ordering::Release);
                return Err("vector field metadata missing");
            }
        };

        let (dim, data_type, metric) = match &vf.field_type {
            FieldType::Vector {
                dim,
                data_type,
                metric,
                ..
            } => (*dim, *data_type, metric.clone()),
            _ => {
                old_arc
                    .compaction_in_progress
                    .store(false, Ordering::Release);
                return Err("vector field type mismatch");
            }
        };

        let m = old_arc.m;
        let ef_construction = old_arc.ef_construction;
        let prefixes = def.prefixes.clone();
        (
            old_arc,
            metric,
            dim,
            data_type,
            prefixes,
            m,
            ef_construction,
        )
    };

    struct CompactionGuard<'a>(&'a HnswIndex);
    impl<'a> Drop for CompactionGuard<'a> {
        fn drop(&mut self) {
            self.0
                .compaction_in_progress
                .store(false, std::sync::atomic::Ordering::Release);
        }
    }
    let _guard = CompactionGuard(&old_arc);

    // ── Step 2: build the new index from live nodes only ──────────────────
    let live_pairs: Vec<(String, Vec<f32>)> = {
        let nodes = old_arc.nodes.read();
        nodes
            .iter()
            .filter_map(|(_, n)| {
                if n.vector.is_empty() {
                    return None;
                }
                Some((n.key.clone(), n.vector.clone()))
            })
            .collect()
    };

    let new_index = Arc::new(HnswIndex::new(m, ef_construction));
    for (key, vec) in &live_pairs {
        hnsw_insert(&new_index, key.clone(), vec.clone(), &metric);
    }

    // ── Step 3: swap into the named slot in `indexes` ─────────────────────
    let (doc_on, doc_schema) = {
        let mut store = index_store.write();
        let def = match store.get_mut(index_name) {
            Some(d) => d,
            None => return Err("index dropped during compaction"),
        };
        match def.indexes.get(field_name) {
            Some(LiveIndex::Hnsw(current)) => {
                if !Arc::ptr_eq(current, &old_arc) {
                    return Err("index was replaced during compaction");
                }
            }
            _ => return Err("field changed type during compaction"),
        }
        def.indexes.insert(
            field_name.to_string(),
            LiveIndex::Hnsw(Arc::clone(&new_index)),
        );
        (def.on.clone(), def.fields.clone())
    };

    // ── Step 4: catch-up pass for writes that landed in old_arc ───────────
    let mut catchup = 0usize;
    {
        let already: ahash::AHashSet<String> = {
            let kti = new_index.key_to_id.read();
            kti.keys().cloned().collect()
        };

        let db_ref = &db_store[0];
        for shard in db_ref.iter() {
            #[cfg(target_os = "linux")]
            let map = shard.read();
            #[cfg(not(target_os = "linux"))]
            let map = shard.read();

            for (key, value) in map.iter() {
                let matches =
                    prefixes.is_empty() || prefixes.iter().any(|p| key.starts_with(p.as_str()));
                if !matches || already.contains(key) {
                    continue;
                }
                let Some(inner) = doc_fields(&doc_on, &doc_schema, value) else {
                    continue;
                };
                let raw = match inner.get(field_name) {
                    Some(b) => b,
                    None => continue,
                };
                let Some(v) = data_type.decode(raw, dim) else {
                    continue;
                };
                hnsw_insert(&new_index, key.to_string(), v, &metric);
                catchup += 1;
            }
        }
    }

    Ok(live_pairs.len() + catchup)
}

/// Spawn the dedicated compaction worker thread.
/// Receives requests on `rx` and processes them serially.
pub fn spawn_compaction_worker(
    index_store: IndexStore,
    db_store: crate::store::Store,
    rx: CompactionRx,
) {
    std::thread::Builder::new()
        .name("hnsw-compaction".into())
        .spawn(move || {
            // Serial processing: one compaction at a time across all indexes.
            // If parallel compaction is ever needed (multiple busy indexes),
            // the fix is a thread pool here — not a tokio runtime.
            while let Ok(req) = rx.recv() {
                match compact_hnsw(&index_store, &db_store, &req.index_name, &req.field_name) {
                    Ok(n) => {
                        log_info!(
                            "HNSW compaction complete for '{}' field '{}': {n} live vectors",
                            req.index_name,
                            req.field_name
                        );
                    }
                    Err(e) => {
                        log_warn!(
                            "HNSW compaction skipped for '{}' field '{}': {e}",
                            req.index_name,
                            req.field_name
                        );
                    }
                }
            }
            // Channel closed — shutdown.
        })
        .expect("failed to spawn HNSW compaction worker");
}

/// Check tombstone ratio for any indexes touched by this key delete and,
/// if they've crossed the threshold, send a compaction request.
///
/// Called immediately after `maybe_unindex` from the command handlers.
/// `try_send` never blocks — if the channel is full (32 pending requests)
/// or no receiver exists yet (during startup), we silently drop. The next
/// delete will re-check and re-queue.
pub fn maybe_request_compaction(
    index_store: &IndexStore,
    compaction_tx: &CompactionTx,
    deleted_key: &str,
) {
    use std::sync::atomic::Ordering::Relaxed;

    let store = index_store.read();
    for (name, def) in store.iter() {
        if is_alias(name, def) {
            continue;
        }
        let matches = def.prefixes.is_empty()
            || def
                .prefixes
                .iter()
                .any(|p| deleted_key.starts_with(p.as_str()));
        if !matches {
            continue;
        }
        // Each HNSW vector field is its own graph with its own tombstones.
        // Check each one independently — one field may need compaction
        // while another is fresh.
        for (field_name, live) in def.indexes.iter() {
            let idx = match live {
                LiveIndex::Hnsw(i) => i,
                _ => continue,
            };
            let tomb = idx.tombstone_count.load(Relaxed);
            if tomb < COMPACTION_MIN_TOMBSTONES {
                continue;
            }
            if idx.tombstone_ratio() < COMPACTION_THRESHOLD {
                continue;
            }
            if idx.compaction_in_progress.load(Relaxed) {
                continue;
            }
            let _ = compaction_tx.try_send(CompactionRequest {
                index_name: name.clone(),
                field_name: field_name.clone(),
            });
        }
    }
}

#[cfg(test)]
mod prepared_query_tests {
    use super::*;

    fn vector(dim: usize, seed: u64) -> Vec<f32> {
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        (0..dim)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                ((state >> 40) as f32 / (1u64 << 24) as f32) * 4.0 - 2.0
            })
            .collect()
    }

    /// The scalar code the kernels replaced.
    fn scalar_cosine(a: &[f32], b: &[f32]) -> f32 {
        let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
        let ma = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let mb = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        if ma * mb == 0.0 {
            return 1.0;
        }
        1.0 - dot / (ma * mb)
    }

    #[test]
    fn a_prepared_query_measures_what_distance_does() {
        let metrics = [
            DistanceMetric::Cosine,
            DistanceMetric::L2,
            DistanceMetric::InnerProduct,
        ];
        for dim in [1usize, 3, 7, 8, 15, 16, 25, 128, 512, 1536, 2048] {
            let q = vector(dim, dim as u64);
            let s = vector(dim, 7 + dim as u64);
            for metric in &metrics {
                let prepared = PreparedQuery::new(metric, &q);
                let want = distance(metric, &q, &s);
                let tol = 1e-4 * want.abs().max(1.0) * (dim as f32).sqrt();
                let got = prepared.to_raw(&s);
                assert!(
                    (got - want).abs() <= tol,
                    "{metric:?} {dim}: {got} vs {want}"
                );
            }
            // Against a vector the graph holds: of unit length for a cosine.
            let cosine = DistanceMetric::Cosine;
            let unit = normalize_to_unit(s.clone());
            let got = PreparedQuery::new(&cosine, &q).to_unit(&unit);
            let want = scalar_cosine(&q, &s);
            assert!(
                (got - want).abs() <= 1e-5,
                "unit cosine {dim}: {got} vs {want}"
            );
            assert!(
                (dist_cosine(&q, &s) - want).abs() <= 1e-5,
                "dist_cosine {dim}"
            );
        }
    }

    #[test]
    fn a_zero_vector_is_at_cosine_distance_one() {
        let cosine = DistanceMetric::Cosine;
        let zero = vec![0.0f32; 16];
        let v = vector(16, 3);
        assert_eq!(dist_cosine(&zero, &v), 1.0);
        assert_eq!(PreparedQuery::new(&cosine, &zero).to_raw(&v), 1.0);
        assert_eq!(PreparedQuery::new(&cosine, &v).to_raw(&zero), 1.0);
        assert_eq!(
            PreparedQuery::new(&cosine, &zero).to_unit(&normalize_to_unit(v)),
            1.0
        );
    }
}

#[cfg(test)]
mod flush_index_tests {
    use super::*;

    #[test]
    fn drop_all_indexes_drops_hnsw_arc() {
        let store = new_index_store();
        let def = IndexSchema {
            name: "idx".into(),
            db_index: 0,
            on: IndexOn::Hash,
            prefixes: vec!["v:".into()],
            fields: vec![SchemaField {
                name: "vec".into(),
                field_type: FieldType::Vector {
                    algo: VectorAlgo::Hnsw {
                        m: 16,
                        ef_construction: 64,
                        ef_runtime: 10,
                    },
                    dim: 2,
                    metric: DistanceMetric::L2,
                    data_type: VectorType::Float32,
                },
                options: FieldOptions::default(),
            }],
            aliases: Vec::new(),
            stopwords: None,
            synonyms: Default::default(),
        }
        .into_def();
        let graph = match def.indexes.get("vec") {
            Some(LiveIndex::Hnsw(arc)) => Arc::clone(arc),
            _ => panic!("expected HNSW field"),
        };
        hnsw_insert(&graph, "v:1".into(), vec![1.0, 0.0], &DistanceMetric::L2);
        store.write().insert("idx".into(), def);

        let weak = Arc::downgrade(&graph);
        drop(graph);
        assert_eq!(drop_all_indexes(&store), 1);
        assert!(store.read().is_empty());
        assert!(
            weak.upgrade().is_none(),
            "HNSW graph must be freed when FLUSHALL drops the index"
        );
    }

    #[test]
    fn drop_indexes_for_db_keeps_other_dbs() {
        let store = new_index_store();
        let mk = |name: &str, db: usize| {
            IndexSchema {
                name: name.into(),
                db_index: db,
                on: IndexOn::Hash,
                prefixes: vec![],
                fields: vec![SchemaField {
                    name: "t".into(),
                    field_type: FieldType::Text,
                    options: FieldOptions::default(),
                }],
                aliases: Vec::new(),
                stopwords: None,
                synonyms: Default::default(),
            }
            .into_def()
        };
        store.write().insert("a".into(), mk("a", 0));
        store.write().insert("b".into(), mk("b", 1));
        assert_eq!(drop_indexes_for_db(&store, 1), 1);
        let g = store.read();
        assert!(g.contains_key("a"));
        assert!(!g.contains_key("b"));
    }
}

#[cfg(test)]
mod alias_tests {
    use super::*;

    fn index(name: &str) -> IndexDef {
        IndexSchema {
            name: name.into(),
            db_index: 0,
            on: IndexOn::Hash,
            prefixes: vec!["doc:".into()],
            fields: vec![SchemaField {
                name: "t".into(),
                field_type: FieldType::Text,
                options: FieldOptions::default(),
            }],
            aliases: Vec::new(),
            stopwords: None,
            synonyms: Default::default(),
        }
        .into_def()
    }

    #[test]
    fn an_alias_resolves_to_its_index_and_goes_with_it() {
        let mut store = AHashMap::new();
        store.insert("idx".to_string(), index("idx"));
        let def = store["idx"].clone();
        set_alias(&mut store, "b", &def);
        set_alias(&mut store, "a", &def);

        assert_eq!(resolve_index_name(&store, "a").as_deref(), Some("idx"));
        assert_eq!(resolve_index_name(&store, "idx").as_deref(), Some("idx"));
        assert_eq!(resolve_index_name(&store, "nope"), None);
        assert_eq!(index_aliases(&store, "idx"), ["a", "b"]);
        assert!(is_alias("a", &store["a"]));
        assert!(!is_alias("idx", &store["idx"]));

        // An alias is not an index: it cannot be dropped as one.
        assert!(!remove_index(&mut store, "a"));
        assert!(remove_index(&mut store, "idx"));
        assert!(store.is_empty(), "aliases go with their index");
    }

    /// A document is indexed once, however many names the index has.
    #[test]
    fn documents_are_not_indexed_through_aliases() {
        let store = new_index_store();
        store.write().insert("idx".to_string(), index("idx"));
        {
            let mut g = store.write();
            let def = g["idx"].clone();
            set_alias(&mut g, "alias", &def);
        }
        maybe_index(
            &store,
            0,
            "doc:1",
            &[("t".to_string(), Arc::from(&b"hello world"[..]))],
            Write::Fields,
        );
        let g = store.read();
        assert_eq!(
            g["idx"]
                .doc_count
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            g["alias"]
                .doc_count
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn aliases_are_restored_with_their_schema() {
        let store = new_index_store();
        let schema = IndexSchema {
            name: "idx".into(),
            db_index: 0,
            on: IndexOn::Hash,
            prefixes: vec![],
            fields: vec![],
            aliases: vec!["a".into()],
            stopwords: None,
            synonyms: Default::default(),
        };
        register_index_schemas(&store, &[schema], &Default::default());
        let g = store.read();
        assert_eq!(resolve_index_name(&g, "a").as_deref(), Some("idx"));
        assert_eq!(index_aliases(&g, "idx"), ["a"]);
    }
}

#[cfg(test)]
mod concurrent_insert_tests {
    use super::*;
    use rand::{Rng, SeedableRng};

    fn random_vectors(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        (0..n)
            .map(|_| (0..dim).map(|_| rng.r#gen::<f32>() - 0.5).collect())
            .collect()
    }

    /// Inserts link themselves in under their neighbours' locks alone, while
    /// searches run and keys are overwritten (which takes the write lock).
    /// The graph that comes out must be whole, and every vector findable.
    #[test]
    fn concurrent_inserts_searches_and_overwrites_leave_a_whole_graph() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 1500;
        const DIM: usize = 16;
        let metric = DistanceMetric::Cosine;
        let index = Arc::new(HnswIndex::new(16, 100));
        let vectors: Vec<Vec<Vec<f32>>> = (0..THREADS)
            .map(|t| random_vectors(PER_THREAD, DIM, t as u64))
            .collect();

        std::thread::scope(|s| {
            for (t, vs) in vectors.iter().enumerate() {
                let index = &index;
                let metric = &metric;
                s.spawn(move || {
                    for (i, v) in vs.iter().enumerate() {
                        hnsw_insert(index, format!("k:{t}:{i}"), v.clone(), metric);
                        if i % 10 == 0 {
                            knn_hnsw(index, v, 5, 50, metric);
                        }
                        // Overwrite an earlier key now and then: a tombstone
                        // under the write lock, then a fresh insert.
                        if i % 100 == 99 {
                            let j = i - 50;
                            hnsw_insert(index, format!("k:{t}:{j}"), vs[j].clone(), metric);
                        }
                    }
                });
            }
        });

        let nodes = index.nodes.read();
        let count = nodes.count();
        let overwrites = THREADS * (PER_THREAD / 100);
        assert_eq!(count, THREADS * PER_THREAD + overwrites);
        assert_eq!(index.live_count.load(Relaxed), THREADS * PER_THREAD);
        assert_eq!(index.tombstone_count.load(Relaxed), overwrites);
        for (id, node) in nodes.iter() {
            let links = node.neighbors.lock();
            for (lc, list) in links.iter().enumerate() {
                let m_max = if lc == 0 { index.m0 } else { index.m };
                assert!(
                    list.len() <= m_max,
                    "node {id} layer {lc}: {} links",
                    list.len()
                );
                for &nb in list {
                    assert!(nb < count && nb != id, "node {id} links to {nb}");
                }
            }
        }
        drop(nodes);

        // Every vector is found as its own nearest neighbour (nearly always:
        // HNSW is approximate).
        let mut found = 0;
        for (t, vs) in vectors.iter().enumerate() {
            for (i, v) in vs.iter().enumerate() {
                let hits = knn_hnsw(&index, v, 1, 100, &metric);
                if hits.first().is_some_and(|h| h.key == format!("k:{t}:{i}")) {
                    found += 1;
                }
            }
        }
        let total = THREADS * PER_THREAD;
        assert!(found * 100 >= total * 98, "found {found} of {total}");
    }
}
