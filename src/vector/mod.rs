pub mod filter;
pub mod postings;
pub mod score;
pub mod simd;
pub mod slots;
pub mod stem;
pub mod text;
use crate::store::ValueRef;
use ahash::AHashMap;
use parking_lot::RwLock;
use rand::Rng;
use std::sync::Arc;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize};

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
        a.extend(self.language.words());
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
    /// `LANGUAGE` and `LANGUAGE_FIELD`: what TEXT fields are stemmed in.
    pub language: stem::LanguageOptions,
    /// With `LANGUAGE_FIELD`: the documents whose words were stemmed in a
    /// language other than the index's, and that language. For a write of
    /// some fields of a hash that leaves its language field as it was.
    pub doc_languages: Arc<RwLock<AHashMap<String, stem::Language>>>,
    /// The TEXT fields' inverted and forward index, and their BM25
    /// statistics, in terms of the document ids of [`IndexDef::postings`]
    /// (see [`text`]). Every document of an index with a TEXT field has an
    /// id, whether or not the index has a TAG or NUMERIC field.
    pub text: Arc<RwLock<text::TextIndex>>,
    /// The TAG and NUMERIC postings, and the document ids they are kept in
    /// (see [`postings`]). For an index with no TAG or NUMERIC field, only
    /// the ids, and only if it has a TEXT field.
    pub postings: postings::SharedPostings,
}

impl IndexDef {
    /// The indexed TEXT fields, in the schema's order: a field's place among
    /// them is its number in [`IndexDef::text`].
    pub fn text_fields(&self) -> impl Iterator<Item = &SchemaField> {
        self.fields
            .iter()
            .filter(|f| matches!(f.field_type, FieldType::Text) && !f.options.noindex)
    }

    /// The number of TEXT field `name` in [`IndexDef::text`]: `None` if it
    /// is not an indexed TEXT field.
    pub fn text_field(&self, name: &str) -> Option<u16> {
        self.text_fields()
            .position(|f| f.name == name)
            .map(|i| i as u16)
    }

    /// By TEXT field number: what an occurrence of a word there counts, its
    /// `WEIGHT`, truncated, and at least 1 (RediSearch's frequencies are
    /// whole numbers).
    pub fn text_units(&self) -> Vec<u32> {
        self.text_fields()
            .map(|f| (f.options.weight as u32).max(1))
            .collect()
    }

    /// Whether the index gives its documents ids: it has a TEXT, TAG or
    /// NUMERIC field.
    #[inline]
    pub fn gives_ids(&self) -> bool {
        self.fields.iter().any(|f| match f.field_type {
            FieldType::Tag | FieldType::Numeric => true,
            FieldType::Text => !f.options.noindex,
            _ => false,
        })
    }

    /// Make what the TEXT fields numbered in `changes` of document `doc` hold
    /// the words given, stopwords already left out, each followed by its stem
    /// if it has one (`stem::index_words`): the inverted index, the
    /// vocabulary and the BM25 statistics become what they would be had the
    /// document always held them.
    pub fn write_text(&self, doc: u32, changes: &[(u16, Vec<String>)]) {
        if changes.is_empty() {
            return;
        }
        let units = self.text_units();
        self.text.write().set(doc, changes, |f| {
            units.get(f as usize).copied().unwrap_or(1)
        });
    }

    /// The TEXT index's part of a write, run by the postings with the
    /// document's id, under their lock (see [`postings::WithId`]): what
    /// [`IndexDef::write_text`] does with `changes`, after the fields
    /// numbered in `restemmed` (with whether they stem) are stemmed again
    /// in `language` from the words they hold. `None` if there is nothing
    /// to do.
    fn text_write(
        &self,
        mut changes: Vec<(u16, Vec<String>)>,
        restemmed: Vec<(u16, bool)>,
        language: stem::Language,
    ) -> Option<postings::WithId> {
        if changes.is_empty() && restemmed.is_empty() {
            return None;
        }
        let text = Arc::clone(&self.text);
        let units = self.text_units();
        Some(Box::new(move |doc| {
            let mut text = text.write();
            for (number, stems) in restemmed {
                if let Some(tokens) = restem(&text, doc, number, stems, language) {
                    changes.push((number, tokens));
                }
            }
            if !changes.is_empty() {
                text.set(doc, &changes, |f| {
                    units.get(f as usize).copied().unwrap_or(1)
                });
            }
        }))
    }

    /// The TEXT index's part of a removal: take out everything the
    /// document's TEXT fields put in, by the postings, under their lock,
    /// before its id is given back (see [`postings::WithId`]).
    fn text_removal(&self) -> Option<postings::WithId> {
        self.text_fields().next()?;
        let text = Arc::clone(&self.text);
        let units = self.text_units();
        Some(Box::new(move |doc| {
            text.write()
                .remove(doc, |f| units.get(f as usize).copied().unwrap_or(1));
        }))
    }

    /// The document at `key` is gone: forget the language it was in.
    fn forget_language(&self, key: &str) {
        if self.language.field.is_some() {
            self.doc_languages.write().remove(key);
        }
    }

    /// The language the words of the document at `key` are stemmed in, for a
    /// write of `fields` (all of the document's if `whole`): the one its
    /// language field names, or for a write that leaves that field as it
    /// was, the one it had. Kept for the next write.
    fn doc_language(
        &self,
        key: &str,
        fields: &[(String, Arc<[u8]>)],
        whole: bool,
    ) -> stem::Language {
        let Some(name) = &self.language.field else {
            return self.language.language;
        };
        let given = fields.iter().find(|(n, _)| n == name).map(|(_, v)| &**v);
        if given.is_none() && !whole {
            return self
                .doc_languages
                .read()
                .get(key)
                .copied()
                .unwrap_or(self.language.language);
        }
        let language = self.language.of(given);
        let mut languages = self.doc_languages.write();
        if language == self.language.language {
            languages.remove(key);
        } else {
            languages.insert(key.to_string(), language);
        }
        language
    }

    /// The language of the document at `key`, whose fields are all of `doc`.
    fn language_of(&self, key: &str, doc: &Fields) -> stem::Language {
        let Some(name) = &self.language.field else {
            return self.language.language;
        };
        let language = self.language.of(doc.get(name.as_str()).map(|v| &**v));
        let mut languages = self.doc_languages.write();
        if language == self.language.language {
            languages.remove(key);
        } else {
            languages.insert(key.to_string(), language);
        }
        language
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
        language: Default::default(),
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
    /// See [`IndexDef::language`].
    pub language: stem::LanguageOptions,
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
        let postings = postings::PostingsLock::new(postings::Postings::new(&self.fields));
        IndexDef {
            name: self.name,
            db_index: self.db_index,
            on: self.on,
            prefixes: self.prefixes,
            fields: self.fields,
            indexes,
            stopwords: self.stopwords.map(Arc::new),
            synonyms: self.synonyms,
            language: self.language,
            doc_languages: Arc::new(RwLock::new(AHashMap::new())),
            text: Arc::new(RwLock::new(text::TextIndex::new())),
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
            let language = def.language_of(key, &inner);
            let mut changes = Vec::new();
            for field in fields {
                let Some(text) = inner.get(field).and_then(|b| std::str::from_utf8(b).ok()) else {
                    continue;
                };
                let Some(schema) = def.fields.iter().find(|f| &f.name == field) else {
                    continue;
                };
                let Some(number) = def.text_field(field) else {
                    continue;
                };
                changes.push((number, stem::index_words(def, schema, language, text)));
            }
            if !changes.is_empty() {
                let doc = def.postings.write().ensure(key);
                def.write_text(doc, &changes);
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
            def.postings.done(postings);
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
                language: def.language.clone(),
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
/// When `keep_loaded_hnsw` is true (startup only), an HNSW graph that already holds vectors
/// (loaded from the snapshot's `KIVIHN1` section) is brought up to date with the keys rather
/// than built again (see `loaded` below). FLAT fields and TEXT / inverted-index work run as
/// for any backfill.
const BACKFILL_PROGRESS_KEYS: usize = 50_000;

pub fn backfill_from_db_progress(
    db: &crate::store::Db,
    def: &IndexDef,
    log_progress: bool,
    keep_loaded_hnsw: bool,
) {
    struct VecFieldWork {
        name: String,
        dim: usize,
        data_type: VectorType,
        metric: DistanceMetric,
        live: LiveIndex,
        /// The graph came with the snapshot. It is the graph as it was at
        /// the checkpoint, and the keys are as they are now: the writes
        /// replayed from the AOF since (whose indexing a replay defers) are
        /// in the keys only. So a vector the graph holds as the key holds it
        /// is left where it is, one it lacks or holds otherwise is inserted,
        /// and after the walk a node whose key no longer holds a vector is
        /// deleted. The graph used to be taken as it was, and the vectors
        /// written after the checkpoint were in no graph after a restart.
        ///
        /// Decided once, before the walk — asked per key, the first vector
        /// this walk inserts would make the graph look loaded, which left a
        /// graph rebuilt after an AOF replay holding one vector.
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
        let loaded = keep_loaded_hnsw
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

    let text_fields: Vec<&SchemaField> = def.text_fields().collect();
    let gives_ids = def.gives_ids();

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
            let doc = if gives_ids {
                let mut postings = def.postings.write();
                let doc = postings.write_doc(key, &inner);
                def.postings.done(postings);
                doc
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
                        if w.loaded && holds_vector(idx, key, &vec, &w.metric) {
                            continue;
                        }
                        hnsw_insert_doc(idx, key.to_string(), vec, &w.metric, doc);
                    }
                }
            }

            if text_fields.is_empty() {
                continue;
            }
            let language = def.language_of(key, &inner);
            let mut changes = Vec::new();
            for (number, field) in text_fields.iter().enumerate() {
                if let Some(raw) = inner.get(&field.name)
                    && let Ok(s) = std::str::from_utf8(raw)
                {
                    changes.push((number as u16, stem::index_words(def, field, language, s)));
                }
            }
            def.write_text(doc, &changes);
        }
    }

    // A loaded graph's nodes whose keys were deleted since the checkpoint, or
    // no longer hold a vector in the field. Each is looked at under its
    // key's shard lock, so that a write to the key made meanwhile (by a
    // master's stream) is either seen here or indexed after this deletes.
    for w in vec_work.iter().filter(|w| w.loaded) {
        let LiveIndex::Hnsw(idx) = &w.live else {
            continue;
        };
        let keys: Vec<String> = idx.key_to_id.read().keys().cloned().collect();
        for key in keys {
            let map = crate::store::get_shard(db, &key).read();
            let holds = def.covers(&key)
                && map
                    .peek(&key)
                    .and_then(|value| doc_fields(&def.on, &def.fields, value))
                    .and_then(|inner| {
                        inner
                            .get(&w.name)
                            .map(|raw| w.data_type.decode(raw, w.dim).is_some())
                    })
                    .unwrap_or(false);
            if !holds {
                unindex_vector(&w.live, &key);
            }
        }
    }
}

/// Whether `index` holds `vector` for `key`, as an insert of it would have
/// stored it (a cosine vector at unit length, by the same arithmetic).
fn holds_vector(index: &HnswIndex, key: &str, vector: &[f32], metric: &DistanceMetric) -> bool {
    let Some(id) = index.key_to_id.read().get(key).copied() else {
        return false;
    };
    let nodes = index.nodes.read();
    let Some(held) = index.vector_of(&nodes, id, key) else {
        return false;
    };
    if matches!(metric, DistanceMetric::Cosine) {
        held == normalize_to_unit(vector.to_vec()).as_slice()
    } else {
        held == vector
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

/// One node in the HNSW graph: its key, its document id, and its links
/// above layer 0. Its vector, its layer-0 links and whether it is live are
/// kept apart, in the graph's slots at its id ([`slots::Slots`]): those are
/// what a search reads for every node it meets, and there they are found
/// from the id alone.
///
/// A node's key and vector do not change while it is in the graph.
/// Deleting it, or filling its slot again for a new node, happens under the
/// graph's write lock. Its links have a lock of their own (`upper`), so that
/// inserts link themselves into different parts of the graph at the same
/// time, as hnswlib's do; layer 0's are read without it.
#[derive(Debug)]
pub struct HnswNode {
    pub key: String,
    /// The node's lock, held to change its links at any layer. It guards
    /// the lists above layer 0: `upper[l - 1]` is layer `l`'s.
    upper: parking_lot::Mutex<Vec<Vec<u32>>>,
    /// The top layer the node is in.
    level: usize,
    /// The id its document has in the index's postings, or
    /// [`postings::NO_DOC`] until it is looked up: a node made by a
    /// compaction, or read from a snapshot, learns it the first time a
    /// filtered search asks (see [`knn_hnsw_admitting`]). A document keeps
    /// its id for as long as it is in the index, and leaving the index
    /// deletes its node first, so a node never names another's.
    pub doc: AtomicU32,
}

impl HnswNode {
    fn new(key: String, level: usize, upper: Vec<Vec<u32>>, doc: u32) -> Self {
        Self {
            key,
            upper: parking_lot::Mutex::new(upper),
            level,
            doc: AtomicU32::new(doc),
        }
    }

    /// The node of a slot that holds none (deleted, or never filled).
    fn empty() -> Self {
        Self::new(String::new(), 0, Vec::new(), postings::NO_DOC)
    }

    /// How many layers the node is in.
    pub fn layer_count(&self) -> usize {
        self.level + 1
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
    /// Searches and inserts hold this for reading. Deleting a node, and
    /// filling a deleted node's slot again, take it for writing: a reader
    /// may still hold the id. Vectors are read only while it is held.
    pub nodes: RwLock<HnswNodes>,
    /// The vectors, layer-0 links and live flags, by id: made with the
    /// first vector, which sets the dimension.
    slots: std::sync::OnceLock<slots::Slots>,
    pub key_to_id: RwLock<AHashMap<String, usize>>,
    pub entry_point: RwLock<Option<usize>>, // node id of current entry point
    pub m: usize,                           // max neighbours per layer (except L0)
    pub m0: usize,                          // max neighbours at layer 0 (= 2*M); see `slack`
    pub ef_construction: usize,
    pub ml: f64, // level multiplier = 1/ln(M)

    /// Number of nodes currently in `nodes` that are live (not tombstoned).
    /// We maintain this explicitly so `FT.INFO` and the compaction threshold
    /// check don't have to walk the whole Vec.
    pub live_count: AtomicUsize,

    /// Number of deleted slots in `nodes` not filled again yet.
    /// Invariant: nodes.count() == live_count + tombstone_count (at rest).
    pub tombstone_count: AtomicUsize,

    /// True while a background compaction is already scheduled/running for
    /// this index. Prevents piling up duplicate compaction tasks on a
    /// write-heavy delete stream.
    pub compaction_in_progress: AtomicBool,

    /// Deleted slots, which the next inserts fill before the graph grows.
    free: parking_lot::Mutex<Vec<u32>>,

    /// Bumped each time a slot is filled again. A search holds ids across
    /// the moments it lets its read lock go; if this moved meanwhile, an id
    /// it kept may name another node now, and it measures its results again
    /// before it answers.
    reused: AtomicU64,
}

impl HnswIndex {
    pub fn new(m: usize, ef_construction: usize) -> Self {
        Self {
            nodes: RwLock::new(HnswNodes::new()),
            slots: std::sync::OnceLock::new(),
            key_to_id: RwLock::new(AHashMap::new()),
            entry_point: RwLock::new(None),
            m,
            m0: m * 2,
            ef_construction,
            ml: 1.0 / (m as f64).ln(),
            live_count: AtomicUsize::new(0),
            tombstone_count: AtomicUsize::new(0),
            compaction_in_progress: AtomicBool::new(false),
            free: parking_lot::Mutex::new(Vec::new()),
            reused: AtomicU64::new(0),
        }
    }

    /// Reconstruct an `HnswIndex` from a serialised snapshot: for each slot
    /// in order, `None` for a deleted one, or the node's key, vector (as the
    /// graph held it) and links by layer. The graph topology is preserved
    /// exactly — no backfill or re-insertion is needed. Deleted slots are
    /// filled again by the next inserts.
    pub fn restore(
        m: usize,
        ef_construction: usize,
        nodes: Vec<Option<RestoredNode>>,
        entry_point: Option<usize>,
    ) -> Self {
        let mut index = Self::new(m, ef_construction);
        let dim = nodes.iter().flatten().map(|n| n.vector.len()).next();
        let slots = dim.map(|dim| {
            index
                .slots
                .get_or_init(|| slots::Slots::new(dim, index.m0 + slack(index.m0)))
        });
        let count = nodes.len();
        {
            let graph = index.nodes.get_mut();
            let mut key_to_id = index.key_to_id.write();
            let mut free = index.free.lock();
            for (id, entry) in nodes.into_iter().enumerate() {
                if let Some(slots) = slots {
                    slots.reserve(id);
                }
                let node = match (entry, slots) {
                    (Some(n), Some(slots)) if n.vector.len() == slots.dim() => {
                        let mut layers = n.neighbors.into_iter().map(|list| {
                            list.into_iter()
                                .filter(|&nb| nb < count && nb != id)
                                .map(|nb| nb as u32)
                                .collect::<Vec<u32>>()
                        });
                        let layer0 = layers.next().unwrap_or_default();
                        let upper: Vec<Vec<u32>> = layers.collect();
                        // SAFETY: the graph is not shared yet.
                        unsafe { slots.write_vector(id, &n.vector) };
                        slots.write_links(id, layer0);
                        slots.flag(id).store(1, Release);
                        key_to_id.insert(n.key.clone(), id);
                        index.live_count.fetch_add(1, Relaxed);
                        HnswNode::new(n.key, upper.len(), upper, postings::NO_DOC)
                    }
                    _ => {
                        free.push(id as u32);
                        index.tombstone_count.fetch_add(1, Relaxed);
                        HnswNode::empty()
                    }
                };
                graph.push(node);
            }
            // The lowest ids are filled first.
            free.reverse();
        }
        let entry_point = match entry_point {
            Some(id) if index.is_live(id) => Some(id),
            _ => index.key_to_id.read().values().next().copied(),
        };
        *index.entry_point.write() = entry_point;
        index
    }

    /// The slots, once the first vector has made them.
    #[inline]
    pub fn slots(&self) -> Option<&slots::Slots> {
        self.slots.get()
    }

    /// Whether slot `id` holds a live node.
    #[inline]
    pub fn is_live(&self, id: usize) -> bool {
        self.slots.get().is_some_and(|s| s.has(id) && s.is_live(id))
    }

    /// The vector of node `id` if it is live. The caller holds the graph
    /// for reading (see [`HnswIndex::nodes`]).
    #[inline]
    pub fn vector(&self, id: usize) -> Option<&[f32]> {
        let s = self.slots.get()?;
        (s.has(id) && s.is_live(id)).then(|| s.vector(id))
    }

    /// The vector of node `id` if it is live and is `key`'s: a slot found
    /// through `key_to_id` may have been filled again for another key
    /// between the lookup and the read lock.
    #[inline]
    pub fn vector_of<'a>(
        &'a self,
        nodes: &'a HnswNodes,
        id: usize,
        key: &str,
    ) -> Option<&'a [f32]> {
        let v = self.vector(id)?;
        nodes.get(id).filter(|n| n.key == key).map(|_| v)
    }

    /// The links of node `id` at `layer`, appended to `out`. The caller
    /// holds the graph for reading.
    pub fn links(&self, nodes: &HnswNodes, id: usize, layer: usize, out: &mut Vec<u32>) {
        if layer == 0 {
            if let Some(s) = self.slots.get()
                && s.has(id)
            {
                s.read_links(id, out);
            }
        } else if let Some(node) = nodes.get(id)
            && let Some(list) = node.upper.lock().get(layer - 1)
        {
            out.extend_from_slice(list);
        }
    }

    /// Bytes the slots take: the vectors, layer-0 links and flags.
    pub fn slot_bytes(&self) -> usize {
        self.slots.get().map_or(0, |s| s.bytes())
    }

    /// Delete node `id`: its slot is emptied and kept for the next insert.
    /// Under the graph's write lock.
    fn delete(&self, nodes: &mut HnswNodes, id: usize) {
        if !self.is_live(id) {
            return;
        }
        let Some(s) = self.slots.get() else { return };
        s.flag(id).store(0, Release);
        s.write_links(id, std::iter::empty());
        if let Some(node) = nodes.get_mut(id) {
            *node = HnswNode::empty();
        }
        self.free.lock().push(id as u32);
        self.tombstone_count.fetch_add(1, Relaxed);
        self.live_count.fetch_sub(1, Relaxed);
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

/// A node as a snapshot holds it: see [`HnswIndex::restore`].
pub struct RestoredNode {
    pub key: String,
    pub vector: Vec<f32>,
    /// Links by layer, layer 0 first.
    pub neighbors: Vec<Vec<usize>>,
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

    // ── The words of the TEXT fields written ────────────────────────────────
    //
    // Each TEXT field written replaces what it held before: its words,
    // stopwords left out, with their stems (see `IndexDef::write_text`).
    // Read before any lock is taken.
    let mut changes: Vec<(u16, Vec<String>)> = Vec::new();
    let mut restemmed: Vec<(u16, bool)> = Vec::new();
    let mut language = def.language.language;
    if def.text_fields().next().is_some() {
        let before = (def.language.field.is_some() && !whole).then(|| {
            def.doc_languages
                .read()
                .get(key)
                .copied()
                .unwrap_or(def.language.language)
        });
        language = def.doc_language(key, fields, whole);
        for (number, f) in def.text_fields().enumerate() {
            let number = number as u16;
            let Some((_, raw)) = fields.iter().find(|(n, _)| n == &f.name) else {
                if whole {
                    changes.push((number, Vec::new()));
                } else if before.is_some_and(|before| before != language) {
                    // The document's language changed and this field did
                    // not: its words are stemmed again in the new one.
                    restemmed.push((number, !f.options.nostem));
                }
                continue;
            };
            let tokens: Vec<String> = match std::str::from_utf8(raw) {
                Ok(s) => stem::index_words(def, f, language, s),
                Err(_) => Vec::new(),
            };
            changes.push((number, tokens));
        }
    }

    // ── Document id, TAG and NUMERIC postings ───────────────────────────────
    //
    // Every document of the index is given an id, whatever it holds: `*`
    // and `NOT` are in terms of all of them, and the TEXT index is too. A
    // write that finds a query reading them is queued, not kept waiting (see
    // `postings`): its vector then learns its id when a filtered search
    // first asks. Its words (inverted index, vocabulary, BM25 statistics) go
    // with it: the postings write them with the id they give it, under their
    // lock, at once or when the queue is applied (see `postings::WithId`).
    let doc = if def.gives_ids() {
        let words = def.text_write(changes, restemmed, language);
        def.postings.write_fields(key, fields, whole, words)
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

/// The words TEXT field `number` of document `doc` holds in `text`, stemmed
/// again in `language` if the field `stems`: `None` if it holds none.
fn restem(
    text: &text::TextIndex,
    doc: u32,
    number: u16,
    stems: bool,
    language: stem::Language,
) -> Option<Vec<String>> {
    let words = text.field_words(doc, number)?;
    let mut tokens = Vec::with_capacity(words.len() * 2);
    for word in words {
        let stemmed = if stems {
            stem::index_stem(language, &word)
        } else {
            None
        };
        tokens.push(word);
        if let Some(s) = stemmed {
            tokens.push(format!("{}{s}", stem::STEM_PREFIX));
        }
    }
    Some(tokens)
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
                if nodes.get(node_id).is_some_and(|n| n.key == key) {
                    idx.delete(&mut nodes, node_id);
                }
            }
        }
    }
}

/// The document at `key` has left `def`: its vectors, then its words, then
/// its postings and its id (the order a filtered search relies on: see
/// [`HnswNode::doc`]; the words are in terms of the id).
fn unindex_from(def: &IndexDef, key: &str) {
    for live in def.indexes.values() {
        unindex_vector(live, key);
    }
    // The words are taken out by the postings, before they give the id back.
    if def.gives_ids() {
        def.postings.remove(key, def.text_removal());
    }
    def.forget_language(key);
}

// ── Keys that expire on being read ───────────────────────────────────────────
//
// A key found expired by a read is deleted there and then (`helpers::
// delete_key_if_expired`), where there is no index store to hand. Each
// server's store is paired with its index store here, so that the key leaves
// its indexes too: before, it stayed in the graph and was still found by a
// KNN query.

static INDEX_STORES: RwLock<Vec<(usize, IndexStore)>> = RwLock::new(Vec::new());

pub(crate) fn store_id(store: &crate::store::Store) -> usize {
    Arc::as_ptr(store) as *const () as usize
}

/// Pair a server's store with its index store.
pub fn register_index_store(store: &crate::store::Store, index_store: &IndexStore) {
    let id = store_id(store);
    let mut stores = INDEX_STORES.write();
    stores.retain(|(other, _)| *other != id);
    stores.push((id, Arc::clone(index_store)));
}

/// `key` of database `db` of the store at address `id` ([`store_id`])
/// expired and was deleted: out of its indexes too.
pub fn unindex_expired_in(id: usize, db: usize, key: &str) {
    let index_store = INDEX_STORES
        .read()
        .iter()
        .find(|(other, _)| *other == id)
        .map(|(_, index_store)| Arc::clone(index_store));
    if let Some(index_store) = index_store {
        maybe_unindex(&index_store, db, key);
    }
}

// ── Keys on their way out of the indexes ─────────────────────────────────────
//
// An expiry takes a key out of the store under its shard's lock, and out of
// its indexes once that lock is let go of: taken out under it, a vector would
// wait there for a KNN query's hold of the graph, and every write to the
// shard behind it. In between, the key is gone from the store and its
// document still in the postings. A count read off the postings (FT.SEARCH
// ... LIMIT 0 0) leaves out the keys past their time that nothing has deleted
// yet, found in the shards' lists of keys with a time to live; a key the
// expiry had taken was in no list, and counted (#249). So the expiry marks
// the key leaving before it lets go of the shard, and unmarks it once the
// key has left its indexes (its postings' removal applied, or queued for
// whoever takes their lock next); the count leaves out the keys marked too.
//
// Only where the key's database has an index: elsewhere nothing is marked.
// The marks' lock is taken last, with nothing waited for under it.

/// Keys marked leaving: the store's address ([`store_id`]), the database,
/// the key.
static LEAVING: parking_lot::Mutex<Vec<(usize, usize, Box<str>)>> =
    parking_lot::Mutex::new(Vec::new());

/// Whether `index_store` has an index of database `db`: whether a key of it
/// that expires is marked leaving ([`mark_leaving`]).
pub fn has_index_of(index_store: &IndexStore, db: usize) -> bool {
    index_store.read().values().any(|def| def.db_index == db)
}

/// The index store of the store at address `id` ([`store_id`]), if it has an
/// index of database `db`.
pub fn indexes_of(id: usize, db: usize) -> Option<IndexStore> {
    let index_store = INDEX_STORES
        .read()
        .iter()
        .find(|(other, _)| *other == id)
        .map(|(_, index_store)| Arc::clone(index_store))?;
    has_index_of(&index_store, db).then_some(index_store)
}

/// `key` of database `db` of the store at address `id` is gone from the
/// store and not yet from its indexes: under the shard's lock that took it.
pub fn mark_leaving(id: usize, db: usize, key: &str) {
    LEAVING.lock().push((id, db, key.into()));
}

/// `key` has left its indexes: what [`mark_leaving`] marked, unmarked.
pub fn unmark_leaving(id: usize, db: usize, key: &str) {
    let mut leaving = LEAVING.lock();
    if let Some(at) = leaving
        .iter()
        .position(|(i, d, k)| *i == id && *d == db && **k == *key)
    {
        leaving.swap_remove(at);
    }
}

/// The keys of database `db` of the store at address `id` marked leaving.
pub fn leaving(id: usize, db: usize) -> Vec<Box<str>> {
    LEAVING
        .lock()
        .iter()
        .filter(|(i, d, _)| *i == id && *d == db)
        .map(|(_, _, key)| key.clone())
        .collect()
}

#[cfg(test)]
mod leaving_tests {
    use super::*;

    #[test]
    fn a_key_is_leaving_from_its_mark_to_its_unmark() {
        // An address no store has.
        let id = usize::MAX - 249;
        assert!(leaving(id, 0).is_empty());
        mark_leaving(id, 0, "a");
        mark_leaving(id, 0, "b");
        mark_leaving(id, 1, "a");
        let mut keys = leaving(id, 0);
        keys.sort();
        assert_eq!(keys, vec![Box::from("a"), Box::from("b")]);
        unmark_leaving(id, 0, "a");
        assert_eq!(leaving(id, 0), vec![Box::from("b")]);
        assert_eq!(leaving(id, 1), vec![Box::from("a")]);
        unmark_leaving(id, 0, "b");
        unmark_leaving(id, 1, "a");
        assert!(leaving(id, 0).is_empty());
        assert!(leaving(id, 1).is_empty());
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
/// search, it offers its read lock to a waiting writer every few candidates.
fn select_neighbors_heuristic(
    slots: &slots::Slots,
    nodes: &mut NodesGuard<'_>,
    candidates: &[(f32, u32)],
    m_max: usize,
    metric: &DistanceMetric,
    keep_pruned: bool,
) -> Vec<u32> {
    let mut result: Vec<(f32, u32)> = Vec::with_capacity(m_max);
    let mut discarded: Vec<(f32, u32)> = Vec::new();
    let next_lines = PREFETCH_NEXT;

    'cand: for (i, &(d_to_q, cand_id)) in candidates.iter().enumerate() {
        if result.len() >= m_max {
            break;
        }
        if i > 0 && i.is_multiple_of(EXPANSIONS_PER_YIELD) {
            parking_lot::RwLockReadGuard::bump(nodes);
        }
        if let Some(&(_, next)) = candidates.get(i + 1) {
            slots.prefetch(next as usize, next_lines);
        }
        let cand = cand_id as usize;
        if !slots.is_live(cand) {
            continue;
        }
        let cand_vec = slots.vector(cand);
        // Key heuristic check: is the candidate closer to q than to any
        // already-selected result neighbor?  If a result neighbor is closer
        // to the candidate than q is, the candidate is "dominated" — the
        // result neighbor already covers that direction better.
        for &(_, sel_id) in &result {
            let d_cand_to_sel = distance_prenorm(metric, cand_vec, slots.vector(sel_id as usize));
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

/// The `m_max` of `ids` nearest the vector `of`, nearest first: the
/// neighbours a node keeps when one more is linked to it than it has room
/// for and the list pruned during the search is out of date.
fn nearest_of(
    slots: &slots::Slots,
    metric: &DistanceMetric,
    of: &[f32],
    ids: &[u32],
    m_max: usize,
) -> Vec<u32> {
    BACKLINK_SCRATCH.with(|cell| {
        let mut scored = cell.borrow_mut();
        scored.clear();
        scored.reserve(ids.len());
        for &id in ids {
            if !slots.is_live(id as usize) {
                continue;
            }
            scored.push((distance_prenorm(metric, of, slots.vector(id as usize)), id));
        }
        scored.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
        scored.truncate(m_max);
        scored.iter().map(|&(_, id)| id).collect()
    })
}

/// Insert a new vector into the HNSW index.
pub fn hnsw_insert(index: &Arc<HnswIndex>, key: String, vector: Vec<f32>, metric: &DistanceMetric) {
    hnsw_insert_doc(index, key, vector, metric, postings::NO_DOC);
}

/// A forward neighbour that will overflow when it receives its back-link,
/// pruned during the search: (neighbour id, layer, its neighbours as the
/// search saw them, the list to replace them with). The new node is not in
/// the graph yet, so the list names it [`NEW_NODE`].
type PrecomputedBacklink = (u32, usize, Vec<u32>, Vec<u32>);
const NEW_NODE: u32 = u32::MAX;

/// [`hnsw_insert`] of the vector of the document whose id is `doc`.
pub fn hnsw_insert_doc(
    index: &Arc<HnswIndex>,
    key: String,
    vector: Vec<f32>,
    metric: &DistanceMetric,
    doc: u32,
) {
    // ── Phase 0: delete the key's old node, if it has one ─────────────────
    // Its slot goes to the free list; this insert may well fill it again.
    let old_id = index.key_to_id.write().remove(&key);
    if let Some(old_id) = old_id {
        let mut nodes = index.nodes.write();
        if nodes.get(old_id).is_some_and(|n| n.key == key) {
            index.delete(&mut nodes, old_id);
        }
    }

    // Cosine vectors are stored as unit-length so that all distance calls can
    // skip the expensive sqrt/divide and use pure dot product instead.
    let vector = if matches!(metric, DistanceMetric::Cosine) {
        normalize_to_unit(vector)
    } else {
        vector
    };
    // The first vector sets the graph's dimension; one of another (which
    // the schema rules out) has nowhere to go.
    let slots = index
        .slots
        .get_or_init(|| slots::Slots::new(vector.len(), index.m0 + slack(index.m0)));
    if vector.len() != slots.dim() {
        return;
    }

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
    // For each forward neighbour predicted to overflow when it receives its
    // back-link, its neighbours are scored against it here, under the read
    // lock, so that linking (under the neighbour's lock) only swaps the
    // pruned list in.
    let mut precomputed_backlinks: Vec<PrecomputedBacklink> = Vec::new();

    // The whole insert holds the graph for reading: it appends its node and
    // links it in under the neighbours' own locks, so inserts do not wait for
    // one another, nor searches for them. Before, each insert took the write
    // lock to link itself in, and a writer that waits holds back every reader
    // that comes after it: 6,000 inserts a second on 64 cores, 51 of 62
    // workers waiting.
    let mut nodes = index.nodes.read();
    let mut neighbors_per_layer: Vec<Vec<u32>> = if let Some(mut ep) = ep_snapshot {
        // Check if entry point is tombstoned — find a live replacement
        if !index.is_live(ep) {
            let k2id = index.key_to_id.read();
            match k2id.values().next().copied() {
                Some(id) => ep = id,
                None => {
                    // No live nodes at all — we become the entry point
                    drop(k2id);
                    let mut links: Vec<Vec<u32>> = (0..=level).map(|_| Vec::new()).collect();
                    let new_id = publish(
                        index,
                        slots,
                        &mut nodes,
                        key.clone(),
                        &vector,
                        level,
                        doc,
                        &mut links,
                    );
                    index.key_to_id.write().insert(key, new_id);
                    index.live_count.fetch_add(1, Relaxed);
                    *index.entry_point.write() = Some(new_id);
                    return;
                }
            }
        }

        let ep_level = nodes.get(ep).map_or(0, |n| n.level);

        // Greedy descent from top layer to level+1 (just finding better ep)
        let mut curr_ep = ep as u32;
        let mut per_layer: Vec<Vec<u32>> = Vec::with_capacity(level + 1);
        for lc in (level + 1..=ep_level).rev() {
            let results = search_layer(index, slots, &mut nodes, &vector, curr_ep, 1, lc, metric);
            if let Some(&(_, best)) = results.first() {
                curr_ep = best;
            }
        }

        // Search and collect neighbors for layers 0..=min(level, ep_level)
        for lc in (0..=level.min(ep_level)).rev() {
            let m_max = if lc == 0 { index.m0 } else { index.m };
            let candidates = search_layer(
                index,
                slots,
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
            let selected =
                select_neighbors_heuristic(slots, &mut nodes, &candidates, m_max, metric, true);
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
        let mut seen: Vec<u32> = Vec::with_capacity(index.m0);
        for (lc, forward_neighbors) in per_layer.iter().enumerate() {
            let m_max = if lc == 0 { index.m0 } else { index.m };
            for &nb_id in forward_neighbors {
                let nb = nb_id as usize;
                if !slots.is_live(nb) {
                    continue;
                }
                seen.clear();
                index.links(&nodes, nb, lc, &mut seen);
                if seen.len() < m_max + slack(m_max) {
                    continue; // won't overflow
                }
                let nb_vec = slots.vector(nb);
                let vector_of = |id: u32| {
                    if id == NEW_NODE {
                        &vector[..]
                    } else {
                        slots.vector(id as usize)
                    }
                };
                let mut scored: Vec<(f32, u32)> = seen
                    .iter()
                    .filter(|&&id| slots.is_live(id as usize))
                    .map(|&id| {
                        (
                            distance_prenorm(metric, nb_vec, slots.vector(id as usize)),
                            id,
                        )
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
                precomputed_backlinks.push((nb_id, lc, seen.clone(), pruned));
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
    let new_id = publish(
        index,
        slots,
        &mut nodes,
        key.clone(),
        &vector,
        level,
        doc,
        &mut neighbors_per_layer,
    );
    index.key_to_id.write().insert(key, new_id);
    index.live_count.fetch_add(1, Relaxed);

    // Entry point bump if the new node has higher level.
    {
        let mut ep = index.entry_point.write();
        match *ep {
            None => *ep = Some(new_id),
            Some(current_ep) => {
                let current_level = nodes.get(current_ep).map_or(0, |n| n.level);
                if level > current_level || !index.is_live(current_ep) {
                    *ep = Some(new_id);
                }
            }
        }
    }

    // Back-links, each under that neighbour's lock alone.
    let mut list: Vec<u32> = Vec::with_capacity(index.m0 + 1);
    for (lc, forward) in neighbors_per_layer.iter().enumerate() {
        let m_max = if lc == 0 { index.m0 } else { index.m };
        for &nb_id in forward {
            let nb = nb_id as usize;
            if nb == new_id || !slots.is_live(nb) {
                continue;
            }
            let Some(nb_node) = nodes.get(nb) else {
                continue;
            };
            let mut upper = nb_node.upper.lock();
            if lc == 0 {
                if slots.push_link(nb, new_id as u32) {
                    continue;
                }
                list.clear();
                slots.read_links(nb, &mut list);
            } else {
                while upper.len() < lc {
                    upper.push(Vec::new());
                }
                let links = &mut upper[lc - 1];
                if links.len() < m_max + slack(m_max) {
                    links.push(new_id as u32);
                    continue;
                }
                list.clear();
                list.extend_from_slice(links);
            }

            // Fast path: the list pruned during the search, if the
            // neighbour's list is still the one it pruned.
            let fresh = precomputed_backlinks
                .iter()
                .find(|(pid, plc, seen, _)| *pid == nb_id && *plc == lc && *seen == list);
            let replacement: Vec<u32> = if let Some((_, _, _, pruned)) = fresh {
                pruned
                    .iter()
                    .map(|&id| if id == NEW_NODE { new_id as u32 } else { id })
                    .collect()
            } else {
                // Stale (a concurrent insert changed the list): keep the
                // m_max nearest.
                list.push(new_id as u32);
                nearest_of(slots, metric, slots.vector(nb), &list, m_max)
            };
            if lc == 0 {
                slots.write_links(nb, replacement);
            } else {
                upper[lc - 1] = replacement;
            }
        }
    }
}

/// How many links past its most a node's list may take before it is pruned
/// back to its most, so that a pruning is paid once for several back-links
/// rather than for each.
///
/// A list is full nearly always, so with no slack every back-link pruned it
/// again: an insert pruned some 32 neighbours, each measuring the
/// neighbour's links against one another, and that was half the distances
/// an insert measured. Pruning to below the most instead (leaving room)
/// saves as much, but leaves nodes with fewer links than before, and recall
/// at ef 64 fell by 0.004 in graphs built by the server. With slack a node
/// has at least the links it had, and up to 4 more: 16 bytes a node.
/// Measured on dbpedia-openai-100K-1536 (M 16, EF_CONSTRUCTION 128, an
/// 8-CPU D16s_v5): see the commit that brought it.
const PRUNE_SLACK: usize = 4;

/// [`PRUNE_SLACK`], for a list of at most `m_max` links.
fn slack(m_max: usize) -> usize {
    PRUNE_SLACK.min(m_max / 4)
}

/// Put a new node in the graph: in a deleted node's slot if there is one,
/// under the write lock (a search may still hold that id), or else in a
/// new slot, which no reader can reach before its node is there. `links`
/// are its links by layer; a link to the slot it takes is dropped (a node
/// met live in the search may have been deleted since, and its slot be the
/// one given to this node).
#[allow(clippy::too_many_arguments)]
fn publish(
    index: &HnswIndex,
    slots: &slots::Slots,
    nodes: &mut NodesGuard<'_>,
    key: String,
    vector: &[f32],
    level: usize,
    doc: u32,
    links: &mut [Vec<u32>],
) -> usize {
    let reuse = index.free.lock().pop();
    if let Some(id) = reuse {
        for list in links.iter_mut() {
            list.retain(|&x| x != id);
        }
        let id = id as usize;
        let upper = links[1..].to_vec();
        let layer0 = &links[0];
        parking_lot::RwLockReadGuard::unlocked(nodes, || {
            let mut graph = index.nodes.write();
            // SAFETY: the graph is held for writing, so no one reads the
            // slot; it was deleted, so no one writes it.
            unsafe { slots.write_vector(id, vector) };
            slots.write_links(id, layer0.iter().copied());
            if let Some(node) = graph.get_mut(id) {
                *node = HnswNode::new(key, level, upper, doc);
            }
            slots.flag(id).store(1, Release);
            index.reused.fetch_add(1, Release);
            index.tombstone_count.fetch_sub(1, Relaxed);
        });
        id
    } else {
        let upper = links[1..].to_vec();
        let layer0 = &links[0];
        nodes.push_with(|id| {
            slots.reserve(id);
            // SAFETY: a new slot: nothing links to it and no key names it.
            unsafe { slots.write_vector(id, vector) };
            slots.write_links(id, layer0.iter().copied());
            slots.flag(id).store(1, Release);
            HnswNode::new(key, level, upper, doc)
        })
    }
}

thread_local! {
    static BACKLINK_SCRATCH: std::cell::RefCell<Vec<(f32, u32)>> =
        std::cell::RefCell::new(Vec::with_capacity(64));
}

type MinHeap =
    std::collections::BinaryHeap<std::cmp::Reverse<(ordered_float::OrderedFloat<f32>, u32)>>;
type MaxHeap = std::collections::BinaryHeap<(ordered_float::OrderedFloat<f32>, u32)>;

/// Per-thread scratch buffers for HNSW search.
/// Allocated once per thread, reused across every search call.
struct SearchBuffers {
    visited: slots::Visited,
    candidates: MinHeap,
    dynamic: MaxHeap,
    neighbor_scratch: Vec<u32>,
}
impl SearchBuffers {
    fn new() -> Self {
        Self {
            visited: slots::Visited::default(),
            candidates: std::collections::BinaryHeap::with_capacity(256),
            dynamic: std::collections::BinaryHeap::with_capacity(256),
            neighbor_scratch: Vec::with_capacity(64),
        }
    }

    /// Empty, for a graph of `nodes` slots.
    fn clear(&mut self, nodes: usize) {
        self.visited.reset(nodes);
        self.candidates.clear();
        self.dynamic.clear();
        self.neighbor_scratch.clear();
    }
}

thread_local! {
    static SEARCH_BUFFERS: std::cell::RefCell<SearchBuffers> =
        std::cell::RefCell::new(SearchBuffers::new());
}

/// How many candidates a search expands between offers of its read lock to a
/// waiting writer (see [`search_layer`]).
const EXPANSIONS_PER_YIELD: usize = 4;

/// Cache lines of each neighbour's vector asked for as soon as the
/// neighbours of a node are known, before any is measured, and of the next
/// neighbour's while one is measured.
///
/// The hardware brings the rest of a vector in once its first lines are
/// read. Measured on dbpedia-openai-100K-1536 (D16s_v5, ef 128): nothing
/// asked for, 778 queries a second on one thread and 3,872 on eight; the
/// whole of the next vector, 766 and 3,526 (the lines asked for push out
/// those being read); 4 and 8 lines, 958 and 4,353. From 1 to 4 lines up
/// front and 2 to 8 of the next measured within a few percent of that.
const PREFETCH_FIRST: usize = 4;
const PREFETCH_NEXT: usize = 8;

/// Search one layer of the graph from `ep`, the graph held for reading.
/// Returns up to `ef` candidates as (distance, node_id), sorted ascending.
///
/// Every few candidates the read lock is handed to a writer waiting for it,
/// if there is one, and taken back: a writer that waits holds back every
/// reader that comes after it, so a long search would hold up all of them.
/// Ids stay valid across the yield: slots never move. One may name another
/// node after it, if a deleted slot was filled again meanwhile; the search
/// then only measures a node it did not mean to (see [`HnswIndex::reused`]).
///
/// The neighbours of each node expanded are read without a lock, and their
/// vectors asked for before any is measured: the vectors are the search's
/// cost, and most come from memory.
#[allow(clippy::too_many_arguments)]
fn search_layer(
    index: &HnswIndex,
    slots: &slots::Slots,
    nodes: &mut NodesGuard<'_>,
    query: &[f32],
    ep: u32,
    ef: usize,
    layer: usize,
    metric: &DistanceMetric,
) -> Vec<(f32, u32)> {
    use ordered_float::OrderedFloat;
    use std::cmp::Reverse;
    let ep_id = ep as usize;
    if !slots.has(ep_id) || !slots.is_live(ep_id) {
        return vec![];
    }
    let ep_dist = distance_prenorm(metric, query, slots.vector(ep_id));
    let (first, next) = (PREFETCH_FIRST, PREFETCH_NEXT);

    SEARCH_BUFFERS.with(|cell| {
        let mut guard = cell.borrow_mut();
        let bufs = &mut *guard;
        bufs.clear(nodes.count());

        let of_ep = OrderedFloat(ep_dist);
        bufs.visited.insert(ep);
        bufs.candidates.push(Reverse((of_ep, ep)));
        bufs.dynamic.push((of_ep, ep));

        let mut expanded = 0usize;
        while let Some(Reverse((c_dist, c_id))) = bufs.candidates.pop() {
            let worst = bufs.dynamic.peek().map(|(d, _)| d.0).unwrap_or(f32::MAX);
            if bufs.dynamic.len() >= ef && c_dist.0 > worst {
                break;
            }
            expanded += 1;
            if expanded.is_multiple_of(EXPANSIONS_PER_YIELD) {
                parking_lot::RwLockReadGuard::bump(nodes);
            }

            // The neighbours not met yet, each asked for ahead of its turn.
            bufs.neighbor_scratch.clear();
            index.links(nodes, c_id as usize, layer, &mut bufs.neighbor_scratch);
            let mut fresh = 0;
            for i in 0..bufs.neighbor_scratch.len() {
                let id = bufs.neighbor_scratch[i];
                if bufs.visited.insert(id) {
                    bufs.neighbor_scratch[fresh] = id;
                    fresh += 1;
                    slots.prefetch(id as usize, first);
                }
            }

            for i in 0..fresh {
                if i + 1 < fresh {
                    slots.prefetch(bufs.neighbor_scratch[i + 1] as usize, next);
                }
                let nb_id = bufs.neighbor_scratch[i];
                if !slots.is_live(nb_id as usize) {
                    continue;
                }
                let nb_dist = distance_prenorm(metric, query, slots.vector(nb_id as usize));

                let worst = bufs.dynamic.peek().map(|(d, _)| d.0).unwrap_or(f32::MAX);
                if nb_dist < worst || bufs.dynamic.len() < ef {
                    let of = OrderedFloat(nb_dist);
                    bufs.candidates.push(Reverse((of, nb_id)));
                    bufs.dynamic.push((of, nb_id));
                    if bufs.dynamic.len() > ef {
                        bufs.dynamic.pop();
                    }
                }
            }
        }

        let mut result: Vec<(f32, u32)> = bufs.dynamic.drain().map(|(d, id)| (d.0, id)).collect();
        result.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
        result
    })
}

/// The `k` nearest of `found` (sorted ascending), as results: the live
/// ones, with their keys. If a deleted slot was filled again since the
/// search took note of `reused`, an id it holds may name another node now,
/// and each is measured again first.
#[allow(clippy::too_many_arguments)]
fn into_results(
    index: &HnswIndex,
    slots: &slots::Slots,
    nodes: &HnswNodes,
    query: &[f32],
    metric: &DistanceMetric,
    mut found: Vec<(f32, u32)>,
    k: usize,
    reused: u64,
) -> Vec<KnnResult> {
    if index.reused.load(Acquire) != reused {
        for r in found.iter_mut() {
            let id = r.1 as usize;
            r.0 = if slots.is_live(id) {
                distance_prenorm(metric, query, slots.vector(id))
            } else {
                f32::INFINITY
            };
        }
        found.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
    }
    found.truncate(k);
    found
        .into_iter()
        .filter(|(_, id)| slots.is_live(*id as usize))
        .filter_map(|(score, id)| {
            nodes.get(id as usize).map(|n| KnnResult {
                key: n.key.clone(),
                score,
            })
        })
        .collect()
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

    let Some(slots) = index.slots() else {
        return vec![];
    };
    if query.len() != slots.dim() {
        return vec![];
    }

    let mut ep_id = match *index.entry_point.read() {
        Some(id) => id,
        None => return vec![],
    };

    let mut nodes = index.nodes.read();
    let reused = index.reused.load(Acquire);

    // Handle tombstoned entry point
    if !index.is_live(ep_id) {
        let k2id = index.key_to_id.read();
        match k2id.values().next().copied() {
            Some(id) => ep_id = id,
            None => return vec![],
        }
    }

    let live_count = index.live_count.load(std::sync::atomic::Ordering::Relaxed);

    // Small graph: brute force
    if live_count < 500 {
        let mut scored: Vec<(f32, u32)> = nodes
            .iter()
            .filter(|(i, _)| slots.is_live(*i))
            .map(|(i, _)| (distance_prenorm(metric, query, slots.vector(i)), i as u32))
            .collect();
        scored.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
        return into_results(index, slots, &nodes, query, metric, scored, k, reused);
    }

    // Large graph: HNSW traversal
    let ep_level = nodes.get(ep_id).map_or(0, |n| n.level);
    let mut curr_ep = ep_id as u32;

    for lc in (1..=ep_level).rev() {
        let candidates = search_layer(index, slots, &mut nodes, query, curr_ep, 1, lc, metric);
        if let Some(&(_, best)) = candidates.first() {
            curr_ep = best;
        }
    }

    let mut results = search_layer(index, slots, &mut nodes, query, curr_ep, ef, 0, metric);

    // Fallback search from original entry point if we got too few results
    if results.len() < k && curr_ep as usize != ep_id {
        let mut more = search_layer(index, slots, &mut nodes, query, ep_id as u32, ef, 0, metric);
        results.append(&mut more);
        let mut seen: AHashMap<u32, f32> = AHashMap::new();
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

    into_results(index, slots, &nodes, query, metric, results, k, reused)
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
///
/// When `deadline` has passed, the search stops with the nearest of the
/// nodes let in so far, and the second value is true: a query's `TIMEOUT`
/// holds here as in a scan. `BATCHES` gives the walk no budget, and a filter
/// that lets few nodes in makes it walk the whole graph.
#[allow(clippy::too_many_arguments)]
pub fn knn_hnsw_admitting(
    index: &HnswIndex,
    query: &[f32],
    k: usize,
    ef: usize,
    metric: &DistanceMetric,
    admit: &mut dyn FnMut(&HnswNode) -> bool,
    budget: usize,
    deadline: Option<std::time::Instant>,
) -> Option<(Vec<KnnResult>, bool)> {
    use ordered_float::OrderedFloat;
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let ef = ef.max(k);
    let slots = index.slots()?;
    if query.len() != slots.dim() {
        return Some((Vec::new(), false));
    }
    let mut ep_id = (*index.entry_point.read())?;
    let mut nodes = index.nodes.read();
    let reused = index.reused.load(Acquire);
    if !index.is_live(ep_id) {
        ep_id = index.key_to_id.read().values().next().copied()?;
    }

    // Down to layer 1, nearest first, as an unfiltered search goes.
    let ep_level = nodes.get(ep_id).map_or(0, |n| n.level);
    let mut curr = ep_id as u32;
    for lc in (1..=ep_level).rev() {
        let found = search_layer(index, slots, &mut nodes, query, curr, 1, lc, metric);
        if let Some(&(_, best)) = found.first() {
            curr = best;
        }
    }

    // Layer 0, keeping only what is let in.
    let (first, next) = (PREFETCH_FIRST, PREFETCH_NEXT);
    let mut spent = 0usize;
    let mut visited = slots::Visited::default();
    visited.reset(nodes.count());
    let mut candidates: BinaryHeap<Reverse<(OrderedFloat<f32>, u32)>> = BinaryHeap::new();
    let mut results: BinaryHeap<(OrderedFloat<f32>, u32)> = BinaryHeap::new();
    let mut neighbors: Vec<u32> = Vec::with_capacity(64);

    if !index.is_live(curr as usize) {
        return Some((Vec::new(), false));
    }
    let d = OrderedFloat(distance_prenorm(metric, query, slots.vector(curr as usize)));
    spent += 1;
    visited.insert(curr);
    candidates.push(Reverse((d, curr)));
    if let Some(n) = nodes.get(curr as usize)
        && admit(n)
    {
        results.push((d, curr));
    }
    let mut bound = if results.len() >= ef {
        results.peek().map_or(f32::INFINITY, |(d, _)| d.0)
    } else {
        f32::INFINITY
    };

    let mut expanded = 0usize;
    let mut cut = false;
    while let Some(Reverse((d, id))) = candidates.pop() {
        if d.0 > bound && results.len() >= ef {
            break;
        }
        expanded += 1;
        // The clock every DEADLINE_EXPANSIONS nodes expanded: a few
        // thousand distances apart.
        if expanded.is_multiple_of(DEADLINE_EXPANSIONS)
            && deadline.is_some_and(|d| std::time::Instant::now() >= d)
        {
            cut = true;
            break;
        }
        if expanded.is_multiple_of(EXPANSIONS_PER_YIELD) {
            parking_lot::RwLockReadGuard::bump(&mut nodes);
        }
        neighbors.clear();
        slots.read_links(id as usize, &mut neighbors);
        let mut fresh = 0;
        for i in 0..neighbors.len() {
            let nb = neighbors[i];
            if visited.insert(nb) {
                neighbors[fresh] = nb;
                fresh += 1;
                slots.prefetch(nb as usize, first);
            }
        }
        for i in 0..fresh {
            if i + 1 < fresh {
                slots.prefetch(neighbors[i + 1] as usize, next);
            }
            let nb = neighbors[i];
            if !slots.is_live(nb as usize) {
                continue;
            }
            spent += 1;
            if spent > budget {
                return None;
            }
            let dist = distance_prenorm(metric, query, slots.vector(nb as usize));
            if results.len() < ef || dist < bound {
                candidates.push(Reverse((OrderedFloat(dist), nb)));
                if let Some(n) = nodes.get(nb as usize)
                    && admit(n)
                {
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

    let mut found: Vec<(f32, u32)> = results.into_iter().map(|(d, id)| (d.0, id)).collect();
    found.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
    if index.reused.load(Acquire) != reused {
        // A slot filled again meanwhile holds another document now: it is
        // let in again or not.
        found.retain(|(_, id)| match nodes.get(*id as usize) {
            Some(n) => admit(n),
            None => false,
        });
    }
    let nearest = into_results(index, slots, &nodes, query, metric, found, k, reused);
    Some((nearest, cut))
}

/// Nodes a filtered graph search expands between reads of the clock
/// (`knn_hnsw_admitting`).
const DEADLINE_EXPANSIONS: usize = 64;

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
            .filter_map(|(id, n)| Some((n.key.clone(), old_arc.vector(id)?.to_vec())))
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
            language: Default::default(),
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
                language: Default::default(),
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
            language: Default::default(),
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
        assert_eq!(g["idx"].text.read().doc_count(), 1);
        assert_eq!(g["alias"].text.read().doc_count(), 0);
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
            language: Default::default(),
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
        // An overwrite deletes the key's node and the slot is filled again
        // by a later insert: the graph grows by fewer slots than inserts.
        let tombstones = index.tombstone_count.load(Relaxed);
        assert_eq!(index.live_count.load(Relaxed), THREADS * PER_THREAD);
        assert!(tombstones <= overwrites, "{tombstones} empty slots");
        assert_eq!(count, THREADS * PER_THREAD + tombstones);
        let mut list = Vec::new();
        for (id, node) in nodes.iter() {
            for lc in 0..node.layer_count() {
                list.clear();
                index.links(&nodes, id, lc, &mut list);
                let m_max = if lc == 0 { index.m0 } else { index.m };
                assert!(
                    list.len() <= m_max + slack(m_max),
                    "node {id} layer {lc}: {} links",
                    list.len()
                );
                for &nb in &list {
                    let nb = nb as usize;
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

#[cfg(test)]
mod slot_reuse_tests {
    use super::*;

    fn point(i: usize) -> Vec<f32> {
        vec![i as f32, (i % 7) as f32, 1.0, 2.0]
    }

    /// A graph past the size searched by brute force.
    fn graph(n: usize) -> Arc<HnswIndex> {
        let index = Arc::new(HnswIndex::new(8, 64));
        for i in 0..n {
            hnsw_insert(&index, format!("k{i}"), point(i), &DistanceMetric::L2);
        }
        index
    }

    #[test]
    fn a_deleted_nodes_slot_is_filled_by_the_next_insert() {
        let metric = DistanceMetric::L2;
        let index = graph(600);
        let live = LiveIndex::Hnsw(Arc::clone(&index));
        unindex_vector(&live, "k10");
        assert_eq!(index.tombstone_count.load(Relaxed), 1);
        assert_eq!(index.live_count.load(Relaxed), 599);
        let slots_before = index.nodes.read().count();

        hnsw_insert(&index, "new".into(), vec![10.25, 3.0, 1.0, 2.0], &metric);
        assert_eq!(index.nodes.read().count(), slots_before, "no new slot");
        assert_eq!(index.tombstone_count.load(Relaxed), 0);
        assert_eq!(index.live_count.load(Relaxed), 600);
        assert_eq!(index.key_to_id.read().get("new"), Some(&10));

        let hits = knn_hnsw(&index, &[10.25, 3.0, 1.0, 2.0], 1, 64, &metric);
        assert_eq!(hits[0].key, "new");
        assert!(hits[0].score < 1e-6);
        let all = knn_hnsw(&index, &point(10), 600, 600, &metric);
        assert!(
            all.iter().all(|h| h.key != "k10"),
            "the deleted key is gone"
        );
        // Every result is measured against its own vector.
        for h in &all {
            let i: usize = match h.key.strip_prefix('k') {
                Some(i) => i.parse().unwrap(),
                None => continue,
            };
            let want = dist_l2_sq(&point(10), &point(i));
            assert!(
                (h.score - want).abs() < 1e-3,
                "{}: {} vs {want}",
                h.key,
                h.score
            );
        }
    }

    #[test]
    fn an_overwritten_key_keeps_its_slot() {
        let metric = DistanceMetric::L2;
        let index = graph(600);
        let slots_before = index.nodes.read().count();
        for round in 0..3 {
            hnsw_insert(
                &index,
                "k5".into(),
                vec![1000.0 + round as f32, 0.0, 0.0, 0.0],
                &metric,
            );
        }
        assert_eq!(index.nodes.read().count(), slots_before);
        assert_eq!(index.live_count.load(Relaxed), 600);
        assert_eq!(index.tombstone_count.load(Relaxed), 0);
        let hits = knn_hnsw(&index, &[1002.0, 0.0, 0.0, 0.0], 1, 64, &metric);
        assert_eq!(hits[0].key, "k5");
        assert!(hits[0].score < 1e-3);
    }

    #[test]
    fn a_restored_graph_fills_its_deleted_slots_first() {
        let metric = DistanceMetric::L2;
        let node = |key: &str, v: [f32; 2], links: Vec<usize>| {
            Some(RestoredNode {
                key: key.into(),
                vector: v.to_vec(),
                neighbors: vec![links],
            })
        };
        // The entry point saved is a deleted slot; a link points past the
        // graph.
        let nodes = vec![
            node("a", [1.0, 0.0], vec![2, 9]),
            None,
            node("c", [0.0, 1.0], vec![0]),
        ];
        let index = Arc::new(HnswIndex::restore(8, 64, nodes, Some(1)));
        assert_eq!(index.live_count.load(Relaxed), 2);
        assert_eq!(index.tombstone_count.load(Relaxed), 1);
        let ep = index.entry_point.read().unwrap();
        assert!(index.is_live(ep));
        let mut links = Vec::new();
        index.links(&index.nodes.read(), 0, 0, &mut links);
        assert_eq!(links, [2]);

        hnsw_insert(&index, "b".into(), vec![1.0, 1.0], &metric);
        assert_eq!(index.nodes.read().count(), 3);
        assert_eq!(index.key_to_id.read().get("b"), Some(&1));
        let hits = knn_hnsw(&index, &[1.0, 1.0], 3, 50, &metric);
        assert_eq!(hits[0].key, "b");
        assert_eq!(hits.len(), 3);
    }

    #[test]
    fn deletes_and_inserts_racing_searches_leave_every_answer_right() {
        let metric = DistanceMetric::L2;
        let index = graph(1000);
        std::thread::scope(|s| {
            for t in 0..4 {
                let index = &index;
                let metric = &metric;
                s.spawn(move || {
                    for r in 0..200 {
                        let i = (t * 250 + r) % 1000;
                        let live = LiveIndex::Hnsw(Arc::clone(index));
                        unindex_vector(&live, &format!("k{i}"));
                        hnsw_insert(index, format!("k{i}"), point(i), metric);
                    }
                });
            }
            for _ in 0..2 {
                let index = &index;
                let metric = &metric;
                s.spawn(move || {
                    for r in 0..300 {
                        let q = point(r * 3 % 1000);
                        for h in knn_hnsw(index, &q, 10, 64, metric) {
                            let i: usize = h.key[1..].parse().unwrap();
                            let want = dist_l2_sq(&q, &point(i));
                            assert!((h.score - want).abs() < 1e-3, "{}: {}", h.key, h.score);
                        }
                    }
                });
            }
        });
        assert_eq!(index.live_count.load(Relaxed), 1000);
        assert_eq!(
            index.nodes.read().count(),
            1000 + index.tombstone_count.load(Relaxed)
        );
    }
}
