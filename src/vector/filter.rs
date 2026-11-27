// The query language of FT.SEARCH, FT.AGGREGATE and FT.HYBRID: parsing a
// query into a filter (and, if it has one, a KNN clause), and evaluating the
// filter against a document.
//
//   *                               everything
//   hello world                     both words, in any TEXT field
//   hello | world                   either
//   -hello                          not
//   ~hello                          optional: restricts nothing
//   "hello world"                   the two words, adjacent, in that order
//   hel*   *llo   *ell*   %helo%    prefix, suffix, infix, fuzzy
//                                   (a bare word also finds its stem:
//                                   see `Parser::exact` and `stem`)
//   @title:hello                    in that field
//   @title|body:(hello | world)     in either field; any expression inside
//   @tag:{a | b}   @tag:{pre*}      TAG: any of; prefix
//   @n:[1 10]  @n:[(1 +inf]  @n:[5]  NUMERIC: range, exclusive bound, equal
//   @n>=5  @n==5  @n!=5  @n<5       NUMERIC comparisons
//   @v:[VECTOR_RANGE 0.2 $vec]=>{$YIELD_DISTANCE_AS: d}
//   ( … )                           grouping
//   <filter>=>[KNN 10 @v $vec AS d EF_RUNTIME 100]
//   $name                           wherever a value goes: from PARAMS
//
// What cannot be parsed is an error. It used to become "match everything":
// `(-@k:{tenantB})=>[KNN …]` returned tenant B's documents, and a
// `VECTOR_RANGE` query returned nothing at all without saying why.

use super::stem::Language;
use super::{DistanceMetric, FieldType, IndexDef, IndexOn, SchemaField, VectorType};
use ahash::AHashMap;

/// `PARAMS` of a query: name → value.
pub type Params = AHashMap<String, Vec<u8>>;

// ── Documents ─────────────────────────────────────────────────────────────────

/// What a filter is evaluated against: a document's fields, by the name the
/// schema reads them under (a hash field name).
pub trait Doc {
    fn field_bytes(&self, field: &str) -> Option<&[u8]>;
}

impl Doc for crate::types::HashFields {
    fn field_bytes(&self, field: &str) -> Option<&[u8]> {
        self.get_bytes(field)
    }
}

impl Doc for AHashMap<String, Vec<u8>> {
    fn field_bytes(&self, field: &str) -> Option<&[u8]> {
        self.get(field).map(|v| v.as_slice())
    }
}

// ── Filter AST ────────────────────────────────────────────────────────────────

/// How a word of a query is matched against the words of a TEXT field.
#[derive(Debug, Clone, PartialEq)]
pub enum TextMatch {
    Exact(String),
    Prefix(String),
    Suffix(String),
    Infix(String),
    /// Within this many edits.
    Fuzzy(String, usize),
    /// A word whose stem the index holds is this one: the `+stem` a query's
    /// word is expanded with (see `stem`).
    Stem(Box<StemMatch>),
    /// The bare stem a query's word is expanded with, where it differs from
    /// the word: matched as [`TextMatch::Exact`] is.
    Expanded(String),
    /// The phonetic code (`<JN`) a query's word is expanded with, on the
    /// fields that are `PHONETIC`: a document's word of the same code is a
    /// match (see `phonetic`).
    Phonetic(String),
}

/// What [`TextMatch::Stem`] needs to tell whether a document's word has the
/// stem: the language its words were stemmed in.
#[derive(Debug, Clone, PartialEq)]
pub struct StemMatch {
    pub stem: String,
    /// The index's language.
    pub language: Language,
    /// `LANGUAGE_FIELD`: where a document's own language is.
    pub language_field: Option<String>,
    /// Fields of the clause that are `NOSTEM`: they hold no stems.
    pub nostem: Vec<String>,
}

impl StemMatch {
    /// Whether a word of a document in `language` was indexed with this stem.
    fn has(&self, language: Language, token: &str) -> bool {
        super::stem::index_stem(language, token).is_some_and(|s| s == self.stem)
    }
}

impl TextMatch {
    pub(crate) fn matches(&self, token: &str) -> bool {
        match self {
            TextMatch::Exact(t) | TextMatch::Expanded(t) => token == t,
            TextMatch::Phonetic(t) => {
                let min = crate::commands::search_tools::config_number("MIN_PHONETIC_TERM_LEN", 3);
                super::phonetic::term_of(token, min as usize).as_deref() == Some(t.as_str())
            }
            TextMatch::Stem(stem) => stem.has(stem.language, token),
            TextMatch::Prefix(p) => token.starts_with(p.as_str()),
            TextMatch::Suffix(s) => token.ends_with(s.as_str()),
            TextMatch::Infix(s) => token.contains(s.as_str()),
            TextMatch::Fuzzy(t, distance) => within_edits(t, token, *distance),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Filter {
    /// `@field:{a | b}` — the field holds any of the values (or, for a value
    /// written `pre*`, a tag starting with it). `field` is where the value is
    /// read from; `separator` and `case_sensitive` are the field's.
    Tag {
        field: String,
        values: Vec<String>,
        prefixes: Vec<String>,
        separator: char,
        case_sensitive: bool,
    },
    /// `@field:[lo hi]`. -inf and +inf are `f64::NEG_INFINITY` / `INFINITY`.
    Numeric {
        field: String,
        lo: f64,
        lo_exclusive: bool,
        hi: f64,
        hi_exclusive: bool,
    },
    /// `@field:[lon lat radius unit]`, or `GEOFILTER` (#1004): a point of
    /// the GEO `field` within `radius_m` metres of (`lon`, `lat`). `json`:
    /// the index is on JSON, whose fields hold several points.
    Geo {
        field: String,
        lon: f64,
        lat: f64,
        radius_m: f64,
        json: bool,
        /// The unit the radius was given in, lower case: for `FT.EXPLAIN`.
        unit: String,
    },
    /// `@field != v`
    NumericNot {
        field: String,
        value: f64,
    },
    /// A word, in any of `fields`.
    Text {
        fields: Vec<String>,
        term: TextMatch,
    },
    /// Words, adjacent and in order, in one of `fields`.
    Phrase {
        fields: Vec<String>,
        terms: Vec<String>,
    },
    /// `@field:[VECTOR_RANGE radius $vec]` — the field's vector is within
    /// `radius` of `vector`.
    VectorRange {
        field: String,
        radius: f64,
        vector: Vec<f32>,
        metric: DistanceMetric,
        /// The element type of the field's blobs, to read a document's.
        data_type: VectorType,
        /// `$YIELD_DISTANCE_AS`: the name to report the distance under.
        yield_as: Option<String>,
    },
    And(Box<Filter>, Box<Filter>),
    Or(Box<Filter>, Box<Filter>),
    Not(Box<Filter>),
    /// `~expr`: matches whether or not `expr` does.
    Optional(Box<Filter>),
    /// `*` — match everything
    MatchAll,
    MatchNone,
}

impl Filter {
    fn and(self, other: Filter) -> Filter {
        match (self, other) {
            (Filter::MatchAll, other) | (other, Filter::MatchAll) => other,
            (a, b) => Filter::And(Box::new(a), Box::new(b)),
        }
    }

    /// The first `VECTOR_RANGE` clause that a match must satisfy, if any: the
    /// one whose distance a result can be given.
    pub fn vector_range(&self) -> Option<&Filter> {
        match self {
            Filter::VectorRange { .. } => Some(self),
            Filter::And(l, r) => l.vector_range().or_else(|| r.vector_range()),
            _ => None,
        }
    }

    /// `INFIELDS` of FT.SEARCH: each text clause looks only in those of its
    /// fields that are among `fields` ([`text_fields_named`]: TEXT fields, by
    /// the names clauses hold them under), and one left with none matches
    /// nothing. As in Redis,
    /// where the option narrows every clause's field mask: `@title:x`
    /// with `INFIELDS 1 body` matches nothing, and `-x` with `INFIELDS 1
    /// title` is every document without `x` in its title. Tag, numeric and
    /// vector clauses are left as they are.
    pub fn in_fields(self, fields: &[String]) -> Filter {
        let narrowed = |mut own: Vec<String>| {
            own.retain(|f| fields.contains(f));
            own
        };
        match self {
            Filter::Text { fields: own, term } => {
                let own = narrowed(own);
                if own.is_empty() {
                    Filter::MatchNone
                } else {
                    Filter::Text { fields: own, term }
                }
            }
            Filter::Phrase { fields: own, terms } => {
                let own = narrowed(own);
                if own.is_empty() {
                    Filter::MatchNone
                } else {
                    Filter::Phrase { fields: own, terms }
                }
            }
            Filter::And(l, r) => {
                Filter::And(Box::new(l.in_fields(fields)), Box::new(r.in_fields(fields)))
            }
            Filter::Or(l, r) => {
                Filter::Or(Box::new(l.in_fields(fields)), Box::new(r.in_fields(fields)))
            }
            Filter::Not(inner) => Filter::Not(Box::new(inner.in_fields(fields))),
            Filter::Optional(inner) => Filter::Optional(Box::new(inner.in_fields(fields))),
            other => other,
        }
    }

    /// Whether any clause is a text clause — whether relevance means anything
    /// for this query.
    pub fn has_text(&self) -> bool {
        match self {
            Filter::Text { .. } | Filter::Phrase { .. } => true,
            Filter::And(l, r) | Filter::Or(l, r) => l.has_text() || r.has_text(),
            Filter::Not(inner) | Filter::Optional(inner) => inner.has_text(),
            _ => false,
        }
    }
}

/// A GEO clause's radius in metres: positive, in `m`, `km`, `mi` or `ft`
/// (any case), as Redis 8.10 takes it. An infinite one reaches every point.
pub fn geo_radius(radius: f64, unit: &str) -> Result<f64, String> {
    let radius_m = crate::geohash::to_meters(radius, unit)
        .ok_or_else(|| "Invalid GeoFilter unit".to_string())?;
    if radius.is_nan() || radius <= 0.0 {
        return Err("Invalid GeoFilter radius".into());
    }
    Ok(radius_m)
}

/// A GEO clause's centre: a finite longitude within ±180 and latitude
/// within ±90, as Redis 8.10 takes it (a latitude past what a geohash holds
/// is a centre no point is near, not an error).
pub fn geo_lon_lat(lon: f64, lat: f64) -> Result<(f64, f64), String> {
    if !(-180.0..=180.0).contains(&lon) || !(-90.0..=90.0).contains(&lat) {
        return Err("Invalid GeoFilter lat/lon".into());
    }
    Ok((lon, lat))
}

/// The TEXT fields of `def` that `INFIELDS` names (by attribute, the name
/// `AS` gave a field), by the names text clauses hold them under: read from
/// the schema, so each is there once and the list is never longer than the
/// schema, however many names are given. A name that is no TEXT field of
/// the index is no field to look in.
pub fn text_fields_named(def: &IndexDef, names: &[String]) -> Vec<String> {
    let names: ahash::AHashSet<&str> = names.iter().map(String::as_str).collect();
    def.fields
        .iter()
        .filter(|f| matches!(f.field_type, FieldType::Text) && names.contains(f.attribute()))
        .map(|f| f.name.clone())
        .collect()
}

/// `=>[KNN k @field $vec …]`, with everything resolved.
#[derive(Debug, Clone)]
pub struct Knn {
    pub k: usize,
    /// The field the vectors are read from.
    pub field: String,
    pub vector: Vec<f32>,
    /// The name the distance is reported under.
    pub alias: String,
    pub ef_runtime: Option<usize>,
    /// `HYBRID_POLICY`: how a filtered KNN is carried out, if the query says.
    pub policy: Option<HybridPolicy>,
    /// `BATCH_SIZE`. Accepted, for `BATCHES`, and not used: see
    /// [`HybridPolicy::Batches`].
    pub batch_size: Option<usize>,
}

/// `HYBRID_POLICY` of a filtered KNN.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HybridPolicy {
    /// Measure every document the filter lets through: exact.
    AdhocBf,
    /// RediSearch asks the graph for batches of nearest neighbours and
    /// filters each. Here it is the graph search that keeps only what the
    /// filter lets through (`vector::knn_hnsw_admitting`), with no limit on
    /// how far it walks: the same idea, approximate as it is, without
    /// batches.
    Batches,
}

impl HybridPolicy {
    fn parse(word: &str) -> Result<Self, String> {
        match word.to_ascii_uppercase().as_str() {
            "ADHOC_BF" => Ok(Self::AdhocBf),
            "BATCHES" => Ok(Self::Batches),
            _ => Err(format!(
                "Error parsing vector similarity parameters: invalid hybrid policy `{word}`"
            )),
        }
    }
}

/// What a query's words are looked for as, beyond themselves: `VERBATIM`
/// and `LANGUAGE` of FT.SEARCH and FT.AGGREGATE.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueryOptions {
    /// Words are not expanded with their stems.
    pub verbatim: bool,
    /// The language to stem the query's words in: the index's if `None`.
    pub language: Option<Language>,
    /// The query's `DIALECT`, or the engine's default; 0 when not known
    /// (the later dialects' reading, which is what a query read by the
    /// engine itself has). In dialect 1, `-` and `~` take the rest of the
    /// query as their operand: see `Parser::unary_clause`.
    pub dialect: u8,
}

/// A parsed query.
#[derive(Debug, Clone)]
pub struct Query {
    pub filter: Filter,
    pub knn: Option<Knn>,
}

// ── Parser ────────────────────────────────────────────────────────────────────

/// Parse `query` against the schema of `def`, taking `$name` values from
/// `params`. `Err` is the message for the client.
pub fn parse_query(query: &str, params: &Params, def: &IndexDef) -> Result<Query, String> {
    parse_query_with(query, params, def, QueryOptions::default())
}

/// [`parse_query`], with `VERBATIM` and `LANGUAGE` as given.
pub fn parse_query_with(
    query: &str,
    params: &Params,
    def: &IndexDef,
    options: QueryOptions,
) -> Result<Query, String> {
    let mut parser = Parser {
        src: query,
        pos: 0,
        params,
        def,
        clauses: 0,
        depth: 0,
        verbatim: options.verbatim,
        language: options.language.unwrap_or(def.language.language),
        dialect1: options.dialect == 1,
        stop_tilde: false,
        stop_minus: false,
        only_stopwords: false,
    };
    parser.skip_ws();
    let filter = if parser.at_end() {
        Filter::MatchAll
    } else {
        parser.or_expr(None)?
    };
    parser.skip_ws();
    let mut knn = None;
    if parser.eat_str("=>[") {
        let mut clause = parser.knn_clause()?;
        parser.skip_ws();
        if parser.eat_str("=>{") {
            for (name, value) in parser.attributes()? {
                match name.as_str() {
                    "yield_distance_as" => clause.alias = value,
                    "ef_runtime" => clause.ef_runtime = Some(parser.count(&value)?),
                    "hybrid_policy" => clause.policy = Some(HybridPolicy::parse(&value)?),
                    "batch_size" => clause.batch_size = Some(parser.count(&value)?),
                    _ => {}
                }
            }
        }
        check_policy(&clause)?;
        knn = Some(clause);
        parser.skip_ws();
    }
    if !parser.at_end() {
        return Err(parser.syntax_error());
    }
    Ok(Query { filter, knn })
}

/// `BATCH_SIZE` belongs to `BATCHES`.
fn check_policy(clause: &Knn) -> Result<(), String> {
    if clause.batch_size.is_some() && clause.policy == Some(HybridPolicy::AdhocBf) {
        return Err(
            "Error parsing vector similarity parameters: 'batch size' is irrelevant for 'ADHOC_BF' policy"
                .into(),
        );
    }
    Ok(())
}

struct Parser<'a> {
    src: &'a str,
    pos: usize,
    params: &'a Params,
    def: &'a IndexDef,
    /// How many clauses have been read.
    clauses: usize,
    /// How deep in parentheses, `-` and `~` the parse is.
    depth: u32,
    /// `VERBATIM`: words are not expanded with their stems.
    verbatim: bool,
    /// The language the query's words are stemmed in.
    language: Language,
    /// Dialect 1: `-` and `~` apply to the rest of the query.
    dialect1: bool,
    /// Dialect 1, inside the operand of a `~` or `-`: a later `~` starts a
    /// clause of its own, not part of the operand.
    stop_tilde: bool,
    /// The same for `-`, inside the operand of one.
    stop_minus: bool,
    /// The last run of clauses read was nothing but stopwords.
    only_stopwords: bool,
}

/// The most clauses a query may have, and the deepest it may nest. A query is
/// parsed, matched and freed by calls that go as deep as its tree, and `a b c`
/// or `a|b|c` is a tree as deep as it is long: without a limit, a long query
/// from a client would run the thread out of stack, which takes the server
/// down.
const CLAUSES_MAX: usize = 1024;
const DEPTH_MAX: u32 = 64;

/// Characters that end a bare word.
fn is_word_end(c: char) -> bool {
    c.is_whitespace() || matches!(c, '(' | ')' | '|' | '{' | '}' | '[' | ']' | '"' | '@' | '~')
}

impl<'a> Parser<'a> {
    fn rest(&self) -> &'a str {
        &self.src[self.pos..]
    }

    fn at_end(&self) -> bool {
        self.pos >= self.src.len()
    }

    fn peek(&self) -> Option<char> {
        self.rest().chars().next()
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.peek()?;
        self.pos += c.len_utf8();
        Some(c)
    }

    fn skip_ws(&mut self) {
        while self.peek().is_some_and(char::is_whitespace) {
            self.bump();
        }
    }

    fn eat(&mut self, c: char) -> bool {
        if self.peek() == Some(c) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn eat_str(&mut self, s: &str) -> bool {
        if self.rest().starts_with(s) {
            self.pos += s.len();
            true
        } else {
            false
        }
    }

    fn syntax_error(&self) -> String {
        self.syntax_error_at(self.pos)
    }

    fn syntax_error_at(&self, pos: usize) -> String {
        let near: String = self.src[pos.min(self.src.len())..]
            .chars()
            .take(16)
            .collect();
        format!("Syntax error at offset {pos} near {near}")
    }

    /// `a | b | c`
    fn or_expr(&mut self, scope: Option<&[String]>) -> Result<Filter, String> {
        let mut left = self.and_expr(scope)?;
        loop {
            self.skip_ws();
            if !self.eat('|') {
                return Ok(left);
            }
            self.skip_ws();
            let right = self.and_expr(scope)?;
            left = Filter::Or(Box::new(left), Box::new(right));
        }
    }

    /// `a b c` — one after another means all of them.
    fn and_expr(&mut self, scope: Option<&[String]>) -> Result<Filter, String> {
        let start = self.pos;
        let mut clauses: Vec<Filter> = Vec::new();
        // Words that were dropped because they are stopwords.
        let mut dropped = 0usize;
        loop {
            self.skip_ws();
            match self.peek() {
                None | Some(')') | Some('|') => break,
                Some('=') if self.rest().starts_with("=>[") => break,
                // Dialect 1: inside the operand of a `~` or `-`, one more
                // `~` (or `-`, in the operand of a `-`) ends it.
                Some('~') if self.stop_tilde && clauses.len() + dropped > 0 => break,
                Some('-') if self.stop_minus && clauses.len() + dropped > 0 => break,
                _ => {}
            }
            match self.unary(scope)? {
                Some(clause) => clauses.push(clause),
                None => dropped += 1,
            }
        }
        if clauses.is_empty() {
            // Nothing but stopwords: they are in no index, so nothing matches.
            return if dropped > 0 {
                self.only_stopwords = true;
                Ok(Filter::MatchNone)
            } else {
                Err(self.syntax_error_at(start))
            };
        }
        self.only_stopwords = false;
        let mut clauses = clauses.into_iter();
        let first = clauses.next().unwrap_or(Filter::MatchAll);
        Ok(clauses.fold(first, Filter::and))
    }

    /// `-x`, `~x`, or a clause, with any `=>{…}` attributes after it.
    /// `None` is a stopword: a word that restricts nothing.
    fn unary(&mut self, scope: Option<&[String]>) -> Result<Option<Filter>, String> {
        self.clauses += 1;
        if self.clauses > CLAUSES_MAX || self.depth >= DEPTH_MAX {
            return Err("Syntax error: the query is too long or nests too deep".into());
        }
        self.depth += 1;
        let clause = self.unary_clause(scope);
        self.depth -= 1;
        clause
    }

    fn unary_clause(&mut self, scope: Option<&[String]>) -> Result<Option<Filter>, String> {
        // Dialect 1: `-` and `~` take the rest of the query, up to the next
        // `)` (a `|` is part of it), as their operand: `~a b` is an optional
        // `a b`, `-a b` is not (`a b`), and `a ~b c | d` is `a` with an
        // optional `b c | d`. A later `~` starts a clause of its own, and in
        // the operand of a `-`, so does a later `-`.
        if self.dialect1 && matches!(self.peek(), Some('-' | '~')) {
            let minus = self.eat('-');
            if !minus {
                self.eat('~');
            }
            self.skip_ws();
            let (was_tilde, was_minus) = (self.stop_tilde, self.stop_minus);
            self.stop_tilde = true;
            self.stop_minus |= minus;
            self.only_stopwords = false;
            let operand = self.or_expr(scope);
            let only_stopwords = self.only_stopwords;
            self.stop_tilde = was_tilde;
            self.stop_minus = was_minus;
            let operand = operand?;
            return Ok(Some(
                if only_stopwords && matches!(operand, Filter::MatchNone) {
                    // "not, or optionally, a stopword": every document.
                    Filter::MatchAll
                } else if minus {
                    Filter::Not(Box::new(operand))
                } else {
                    Filter::Optional(Box::new(operand))
                },
            ));
        }
        if self.eat('-') {
            self.skip_ws();
            return Ok(Some(match self.unary(scope)? {
                Some(inner) => Filter::Not(Box::new(inner)),
                // "not a stopword": every document.
                None => Filter::MatchAll,
            }));
        }
        if self.eat('~') {
            self.skip_ws();
            return Ok(Some(match self.unary(scope)? {
                Some(inner) => Filter::Optional(Box::new(inner)),
                None => Filter::MatchAll,
            }));
        }
        let Some(mut clause) = self.primary(scope)? else {
            return Ok(None);
        };
        self.skip_ws();
        if self.eat_str("=>{") {
            for (name, value) in self.attributes()? {
                if name == "yield_distance_as"
                    && let Filter::VectorRange { yield_as, .. } = &mut clause
                {
                    *yield_as = Some(value.clone());
                }
                // `$phonetic`: whether the clause's words are also looked for by
                // their phonetic codes (default: on the PHONETIC fields).
                if name == "phonetic" {
                    match value.to_ascii_lowercase().as_str() {
                        "false" => clause = without_phonetic(clause),
                        "true" => {
                            if !has_phonetic(&clause) {
                                return Err("field does not support phonetics".into());
                            }
                        }
                        _ => return Err(self.syntax_error()),
                    }
                }
                // $weight, $slop, $inorder, $epsilon: accepted; they tune
                // ranking or approximation, not what matches.
            }
        }
        Ok(Some(clause))
    }

    fn primary(&mut self, scope: Option<&[String]>) -> Result<Option<Filter>, String> {
        match self.peek() {
            Some('(') => {
                self.bump();
                self.skip_ws();
                // A `~` or `-` inside the parentheses is of the group, not
                // one that ends the operand of the `~` or `-` outside.
                let flags = (self.stop_tilde, self.stop_minus);
                (self.stop_tilde, self.stop_minus) = (false, false);
                let inner = self.or_expr(scope);
                (self.stop_tilde, self.stop_minus) = flags;
                let inner = inner?;
                self.skip_ws();
                if !self.eat(')') {
                    return Err(self.syntax_error());
                }
                Ok(Some(inner))
            }
            Some('@') => self.field_clause().map(Some),
            Some('"') => self.phrase(scope).map(Some),
            Some('*')
                if self.rest()[1..].starts_with("=>")
                    || self.rest()[1..].chars().next().is_none_or(is_word_end) =>
            {
                self.bump();
                Ok(Some(Filter::MatchAll))
            }
            Some(c) if !is_word_end(c) => self.word(scope),
            _ => Err(self.syntax_error()),
        }
    }

    /// `@a|b:…`, or `@n>=5` and its relatives.
    fn field_clause(&mut self) -> Result<Filter, String> {
        let start = self.pos;
        self.bump(); // '@'
        let mut names: Vec<String> = Vec::new();
        loop {
            let name = self.field_name();
            if name.is_empty() {
                return Err(self.syntax_error_at(start));
            }
            names.push(name);
            if !self.eat('|') {
                break;
            }
        }
        let mut fields: Vec<&SchemaField> = Vec::with_capacity(names.len());
        for name in &names {
            match self.def.field_by_attribute(name) {
                Some(field) => fields.push(field),
                None => return Err(format!("Unknown field at offset {start} near {name}")),
            }
        }

        // Comparison operators, on one NUMERIC field.
        for (op, lo_exclusive, hi_exclusive, which) in [
            ("==", false, false, 0),
            ("!=", false, false, 1),
            (">=", false, false, 2),
            ("<=", false, false, 3),
            (">", true, false, 2),
            ("<", false, true, 3),
        ] {
            if !self.eat_str(op) {
                continue;
            }
            let value = self.number_token()?;
            let [field] = fields.as_slice() else {
                return Err(self.syntax_error_at(start));
            };
            if !matches!(field.field_type, FieldType::Numeric) {
                return Ok(Filter::MatchNone);
            }
            let field = field.name.clone();
            return Ok(match which {
                0 => Filter::Numeric {
                    field,
                    lo: value,
                    lo_exclusive: false,
                    hi: value,
                    hi_exclusive: false,
                },
                1 => Filter::NumericNot { field, value },
                2 => Filter::Numeric {
                    field,
                    lo: value,
                    lo_exclusive,
                    hi: f64::INFINITY,
                    hi_exclusive: false,
                },
                _ => Filter::Numeric {
                    field,
                    lo: f64::NEG_INFINITY,
                    lo_exclusive: false,
                    hi: value,
                    hi_exclusive,
                },
            });
        }

        if !self.eat(':') {
            return Err(self.syntax_error());
        }
        self.skip_ws();
        match self.peek() {
            Some('{') => {
                self.bump();
                let (values, prefixes) = self.tag_values()?;
                let mut out = Filter::MatchNone;
                let mut any = false;
                for field in &fields {
                    // Braces are the syntax of a TAG field. On any other,
                    // there is nothing they could match.
                    if !matches!(field.field_type, FieldType::Tag) {
                        continue;
                    }
                    let normalise = |v: &String| {
                        if field.options.case_sensitive {
                            v.clone()
                        } else {
                            v.to_lowercase()
                        }
                    };
                    let clause = Filter::Tag {
                        field: field.name.clone(),
                        values: values.iter().map(normalise).collect(),
                        prefixes: prefixes.iter().map(normalise).collect(),
                        separator: field.options.separator,
                        case_sensitive: field.options.case_sensitive,
                    };
                    out = if any {
                        Filter::Or(Box::new(out), Box::new(clause))
                    } else {
                        clause
                    };
                    any = true;
                }
                Ok(out)
            }
            Some('[') => {
                self.bump();
                let [field] = fields.as_slice() else {
                    return Err(self.syntax_error_at(start));
                };
                self.bracket_clause(field, start)
            }
            _ => {
                // Words: they are looked for in the TEXT fields named. A TAG
                // or NUMERIC field has no words — its syntax is `{…}` or
                // `[…]` — so on those nothing matches.
                let scope: Vec<String> = fields
                    .iter()
                    .filter(|f| matches!(f.field_type, FieldType::Text) && !f.options.noindex)
                    .map(|f| f.name.clone())
                    .collect();
                let clause = match self.unary(Some(&scope))? {
                    Some(clause) => clause,
                    None => Filter::MatchNone,
                };
                Ok(if scope.is_empty() {
                    Filter::MatchNone
                } else {
                    clause
                })
            }
        }
    }

    fn field_name(&mut self) -> String {
        let mut name = String::new();
        while let Some(c) = self.peek() {
            if c == '\\' {
                self.bump();
                if let Some(escaped) = self.bump() {
                    name.push(escaped);
                }
            } else if c.is_alphanumeric() || matches!(c, '_' | '.' | '$' | '-') {
                // `-` and `.` appear in field names; `-` cannot begin a
                // comparison, so it is safe to take.
                name.push(c);
                self.bump();
            } else {
                break;
            }
        }
        name
    }

    /// After `@field:{`. Values are separated by `|`; one ending in `*` is a
    /// prefix; `\` escapes; `"…"` is taken whole; `$name` is a parameter.
    fn tag_values(&mut self) -> Result<(Vec<String>, Vec<String>), String> {
        let start = self.pos;
        let mut values = Vec::new();
        let mut prefixes = Vec::new();
        let mut current = String::new();
        let mut quoted = false;
        let mut finish = |current: &mut String, quoted: &mut bool, this: &Self| {
            let raw = std::mem::take(current);
            let trimmed = if *quoted { raw.as_str() } else { raw.trim() };
            *quoted = false;
            if trimmed.is_empty() {
                return Ok(());
            }
            if let Some(name) = trimmed.strip_prefix('$') {
                values.push(this.param_text(name)?);
            } else if let Some(prefix) = trimmed.strip_suffix('*') {
                if prefix.is_empty() {
                    return Err(this.syntax_error_at(start));
                }
                prefixes.push(prefix.to_string());
            } else {
                values.push(trimmed.to_string());
            }
            Ok::<(), String>(())
        };
        loop {
            match self.bump() {
                None => return Err(self.syntax_error_at(start)),
                Some('}') => {
                    finish(&mut current, &mut quoted, self)?;
                    break;
                }
                Some('|') => finish(&mut current, &mut quoted, self)?,
                Some('\\') => {
                    if let Some(escaped) = self.bump() {
                        current.push(escaped);
                    }
                }
                Some('"') if current.trim().is_empty() => {
                    current.clear();
                    loop {
                        match self.bump() {
                            None => return Err(self.syntax_error_at(start)),
                            Some('"') => break,
                            Some('\\') => {
                                if let Some(escaped) = self.bump() {
                                    current.push(escaped);
                                }
                            }
                            Some(c) => current.push(c),
                        }
                    }
                    quoted = true;
                }
                Some(c) => current.push(c),
            }
        }
        if values.is_empty() && prefixes.is_empty() {
            return Err(self.syntax_error_at(start));
        }
        Ok((values, prefixes))
    }

    /// After `@field:[`: a numeric range, a single number, or a vector range.
    fn bracket_clause(&mut self, field: &SchemaField, start: usize) -> Result<Filter, String> {
        let Some(len) = self.rest().find(']') else {
            return Err(self.syntax_error_at(start));
        };
        let inner = &self.rest()[..len];
        let inner_start = self.pos;
        self.pos += len + 1;
        let tokens: Vec<&str> = inner.split_whitespace().collect();

        // `[lon lat radius unit]`: a GEO field's syntax, and no other's.
        if let [lon, lat, radius, unit] = tokens.as_slice() {
            if !matches!(field.field_type, FieldType::Geo) {
                return Err(format!(
                    "Expected a GEO field at offset {start} near {}",
                    field.attribute()
                ));
            }
            // `nan` is no number here, as in Redis.
            let number = |token: &str| match self.number(token) {
                Ok(v) if !v.is_nan() => Ok(v),
                _ => Err(self.syntax_error_at(inner_start)),
            };
            let (lon, lat, radius) = (number(lon)?, number(lat)?, number(radius)?);
            let unit = match unit.strip_prefix('$') {
                Some(name) => self.param_text(name)?,
                None => unit.to_string(),
            };
            let radius_m = geo_radius(radius, &unit)?;
            let lon_lat = geo_lon_lat(lon, lat)?;
            return Ok(Filter::Geo {
                field: field.name.clone(),
                lon: lon_lat.0,
                lat: lon_lat.1,
                radius_m,
                json: matches!(self.def.on, IndexOn::Json),
                unit: unit.to_ascii_lowercase(),
            });
        }

        if tokens
            .first()
            .is_some_and(|t| t.eq_ignore_ascii_case("VECTOR_RANGE"))
        {
            let [_, radius, vector] = tokens.as_slice() else {
                return Err(self.syntax_error_at(inner_start));
            };
            let FieldType::Vector {
                dim,
                metric,
                data_type,
                ..
            } = &field.field_type
            else {
                return Err(format!(
                    "Error parsing vector similarity query: '{}' is not a vector field",
                    field.attribute()
                ));
            };
            let radius = self.number(radius)?;
            if radius.is_nan() || radius < 0.0 {
                return Err("Error parsing vector similarity query: invalid radius".into());
            }
            let Some(name) = vector.strip_prefix('$') else {
                return Err(self.syntax_error_at(inner_start));
            };
            return Ok(Filter::VectorRange {
                field: field.name.clone(),
                radius,
                vector: self.vector_param(name, *dim, *data_type)?,
                metric: metric.clone(),
                data_type: *data_type,
                yield_as: None,
            });
        }

        let bound = |token: &str, this: &Self| -> Result<(f64, bool), String> {
            match token.strip_prefix('(') {
                Some(rest) => Ok((this.number(rest)?, true)),
                None => Ok((this.number(token)?, false)),
            }
        };
        let (lo, lo_exclusive, hi, hi_exclusive) = match tokens.as_slice() {
            [single] => {
                let (v, _) = bound(single, self)?;
                (v, false, v, false)
            }
            [lo, hi] => {
                let (lo, lo_exclusive) = bound(lo, self)?;
                let (hi, hi_exclusive) = bound(hi, self)?;
                (lo, lo_exclusive, hi, hi_exclusive)
            }
            _ => return Err(self.syntax_error_at(inner_start)),
        };
        if !matches!(field.field_type, FieldType::Numeric) {
            return Ok(Filter::MatchNone);
        }
        Ok(Filter::Numeric {
            field: field.name.clone(),
            lo,
            lo_exclusive,
            hi,
            hi_exclusive,
        })
    }

    /// A number written in a query: a literal, `inf`, or a `$parameter`.
    fn number(&self, token: &str) -> Result<f64, String> {
        let text = match token.strip_prefix('$') {
            Some(name) => self.param_text(name)?,
            None => token.to_string(),
        };
        match text.to_ascii_lowercase().as_str() {
            "-inf" | "-infinity" => Ok(f64::NEG_INFINITY),
            "inf" | "+inf" | "infinity" | "+infinity" => Ok(f64::INFINITY),
            other => other
                .parse::<f64>()
                .map_err(|_| format!("Syntax error: '{text}' is not a number")),
        }
    }

    /// The number after a comparison operator.
    fn number_token(&mut self) -> Result<f64, String> {
        self.skip_ws();
        let start = self.pos;
        while self
            .peek()
            .is_some_and(|c| !c.is_whitespace() && !matches!(c, ')' | '|' | '(' | '@'))
        {
            self.bump();
        }
        if self.pos == start {
            return Err(self.syntax_error());
        }
        self.number(&self.src[start..self.pos])
    }

    fn count(&self, token: &str) -> Result<usize, String> {
        let text = match token.strip_prefix('$') {
            Some(name) => self.param_text(name)?,
            None => token.to_string(),
        };
        text.parse::<usize>()
            .map_err(|_| format!("Syntax error: '{text}' is not a non-negative integer"))
    }

    fn param(&self, name: &str) -> Result<&'a [u8], String> {
        self.params
            .get(name)
            .map(|v| v.as_slice())
            .ok_or_else(|| format!("No such parameter `{name}`"))
    }

    fn param_text(&self, name: &str) -> Result<String, String> {
        String::from_utf8(self.param(name)?.to_vec())
            .map_err(|_| format!("Parameter `{name}` is not text"))
    }

    /// A vector given as a parameter: `dim` little-endian elements of the
    /// field's type.
    fn vector_param(&self, name: &str, dim: usize, ty: VectorType) -> Result<Vec<f32>, String> {
        let raw = self.param(name)?;
        ty.decode(raw, dim).ok_or_else(|| {
            format!(
                "Error parsing vector similarity query: query vector blob size ({}) does not \
                 match index's expected size ({}).",
                raw.len(),
                dim * ty.size()
            )
        })
    }

    /// The TEXT fields a word is looked for in: the ones in scope, or all of
    /// the index's.
    fn text_fields(&self, scope: Option<&[String]>) -> Vec<String> {
        match scope {
            Some(fields) => fields.to_vec(),
            None => self
                .def
                .fields
                .iter()
                .filter(|f| matches!(f.field_type, FieldType::Text) && !f.options.noindex)
                .map(|f| f.name.clone())
                .collect(),
        }
    }

    /// `"…"`: the words, adjacent and in order.
    fn phrase(&mut self, scope: Option<&[String]>) -> Result<Filter, String> {
        let start = self.pos;
        self.bump(); // '"'
        let mut text = String::new();
        loop {
            match self.bump() {
                None => return Err(self.syntax_error_at(start)),
                Some('"') => break,
                // The escape is kept: the tokenizer reads it, so that `\.`
                // stays inside its word as it does in the document.
                Some('\\') => {
                    if let Some(escaped) = self.bump() {
                        text.push('\\');
                        text.push(escaped);
                    }
                }
                Some(c) => text.push(c),
            }
        }
        let text = match text.strip_prefix('$') {
            Some(name) if self.params.contains_key(name) => self.param_text(name)?,
            _ => text,
        };
        // Stopwords stay in a phrase: they hold their place between words.
        let terms: Vec<String> = super::tokenize_for_vocab(&text).collect();
        let fields = self.text_fields(scope);
        Ok(match terms.len() {
            0 => Filter::MatchNone,
            1 => Filter::Text {
                fields,
                term: TextMatch::Exact(terms.into_iter().next().unwrap_or_default()),
            },
            _ => Filter::Phrase { fields, terms },
        })
    }

    /// A word as it is to match: itself, or any word of the synonym groups
    /// it is in (`FT.SYNUPDATE`), as RediSearch expands a query's terms. The
    /// groups were kept and searched with nothing (#130). Being the query's,
    /// the expansion finds documents indexed before the group was made, as
    /// RediSearch's rescan does.
    ///
    /// Then, as RediSearch's default expander does, the word is also looked
    /// for as its stem: `word | +stem`, and `| stem` if the stem differs
    /// from the word (`preferences | +prefer | prefer`). Not under
    /// `VERBATIM`, nor for a clause in `NOSTEM` fields only, nor for a word
    /// that is a number given as a parameter (`expand` false). A clause that
    /// names its fields is expanded in those that stem; one that names none,
    /// in all of them, as in RediSearch.
    fn exact(&self, fields: &[String], scoped: bool, word: String, expand: bool) -> Filter {
        let base = self.synonyms(fields, word.clone());
        if !expand || self.verbatim {
            return base;
        }
        let nostem = |name: &String| {
            self.def
                .fields
                .iter()
                .any(|f| f.name == *name && f.options.nostem)
        };
        // The word's phonetic code, on the PHONETIC fields of the clause:
        // `word | <CODE | +stem`. A word is expanded whatever its length;
        // only a word of `MIN_PHONETIC_TERM_LEN` bytes or more has a code in
        // the index.
        let base = {
            let phonetic: Vec<String> = fields
                .iter()
                .filter(|name| {
                    self.def.fields.iter().any(|f| {
                        f.name == **name
                            && matches!(f.field_type, FieldType::Text)
                            && f.options.phonetic.is_some()
                    })
                })
                .cloned()
                .collect();
            match super::phonetic::term_of(&word, 0) {
                Some(code) if !phonetic.is_empty() => Filter::Or(
                    Box::new(base),
                    Box::new(Filter::Text {
                        fields: phonetic,
                        term: TextMatch::Phonetic(code),
                    }),
                ),
                _ => base,
            }
        };
        let stem_fields: Vec<String> = if scoped {
            fields.iter().filter(|f| !nostem(f)).cloned().collect()
        } else {
            fields.to_vec()
        };
        if stem_fields.is_empty() {
            return base;
        }
        let Some(stem) = self.language.stem(&word) else {
            return base;
        };
        let stemmed = Filter::Text {
            fields: stem_fields.clone(),
            term: TextMatch::Stem(Box::new(StemMatch {
                stem: stem.clone(),
                language: self.def.language.language,
                language_field: self.def.language.field.clone(),
                nostem: stem_fields.iter().filter(|f| nostem(f)).cloned().collect(),
            })),
        };
        let mut node = Filter::Or(Box::new(base), Box::new(stemmed));
        if stem != word {
            node = Filter::Or(
                Box::new(node),
                Box::new(Filter::Text {
                    fields: stem_fields,
                    term: TextMatch::Expanded(stem),
                }),
            );
        }
        node
    }

    /// The word, or any word of the synonym groups it is in.
    fn synonyms(&self, fields: &[String], word: String) -> Filter {
        let mut words = vec![word];
        for group in self.def.synonyms.values() {
            if group
                .iter()
                .any(|term| term.eq_ignore_ascii_case(&words[0]))
            {
                for term in group {
                    let term = term.to_lowercase();
                    if !words.contains(&term) {
                        words.push(term);
                    }
                }
            }
        }
        words
            .into_iter()
            .map(|word| Filter::Text {
                fields: fields.to_vec(),
                term: TextMatch::Exact(word),
            })
            .reduce(|a, b| Filter::Or(Box::new(a), Box::new(b)))
            .unwrap_or(Filter::MatchNone)
    }

    /// A bare word, with its wildcards. `None` for a stopword.
    fn word(&mut self, scope: Option<&[String]>) -> Result<Option<Filter>, String> {
        let start = self.pos;
        let mut raw = String::new();
        while let Some(c) = self.peek() {
            if c == '\\' {
                // Kept for the tokenizer, as in a phrase.
                self.bump();
                if let Some(escaped) = self.bump() {
                    raw.push('\\');
                    raw.push(escaped);
                }
            } else if is_word_end(c) || (c == '=' && self.rest().starts_with("=>")) {
                break;
            } else {
                raw.push(c);
                self.bump();
            }
        }
        if raw.is_empty() {
            return Err(self.syntax_error_at(start));
        }
        let fields = self.text_fields(scope);
        // A parameter's value is one word, taken as it is but for its
        // escapes: `users.u_1` is not split at the dot, as in Redis.
        if let Some(name) = raw.strip_prefix('$') {
            let value = self.param_text(name)?;
            let mut word = String::with_capacity(value.len());
            let mut chars = value.chars();
            while let Some(c) = chars.next() {
                word.extend(if c == '\\' { chars.next() } else { Some(c) });
            }
            let word = word.to_lowercase();
            if word.is_empty() || self.def.is_stopword(&word) {
                return Ok(None);
            }
            // A number given as a parameter is not stemmed, as in RediSearch.
            let expand = word.parse::<f64>().is_err();
            return Ok(Some(self.exact(&fields, scope.is_some(), word, expand)));
        }

        // %word% — one edit per pair of percent signs, up to three.
        let fuzz = raw.chars().take_while(|c| *c == '%').count();
        if fuzz > 0 {
            let inner = raw.trim_matches('%');
            if fuzz > 3 || inner.is_empty() || !raw.ends_with(&"%".repeat(fuzz)) {
                return Err(self.syntax_error_at(start));
            }
            // The word as the tokenizer has it, so that `%users\.u_1%` is
            // compared with the word `users.u_1` a document holds.
            let mut words = super::tokenize_for_vocab(inner);
            let word = match (words.next(), words.next()) {
                (Some(word), None) => word,
                _ => inner.to_lowercase(),
            };
            return Ok(Some(Filter::Text {
                fields,
                term: TextMatch::Fuzzy(word, fuzz),
            }));
        }

        // An escaped `*` (`\*`) is a character of the word, not a wildcard.
        let leading = raw.starts_with('*');
        let trailing = raw.len() > 1
            && raw.ends_with('*')
            && raw[..raw.len() - 1]
                .chars()
                .rev()
                .take_while(|c| *c == '\\')
                .count()
                .is_multiple_of(2);
        let core = &raw[usize::from(leading)..raw.len() - usize::from(trailing)];
        let mut tokens: Vec<String> = super::tokenize_for_vocab(core).collect();
        if leading || trailing {
            let [token] = tokens.as_slice() else {
                return Err(self.syntax_error_at(start));
            };
            let token = token.clone();
            let term = match (leading, trailing) {
                (true, true) => TextMatch::Infix(token),
                (true, false) => TextMatch::Suffix(token),
                _ => TextMatch::Prefix(token),
            };
            return Ok(Some(Filter::Text { fields, term }));
        }

        // A word the tokenizer splits (`foo-bar`) is its parts, all of them.
        tokens.retain(|t| !self.def.is_stopword(t));
        let mut tokens = tokens.into_iter();
        let Some(first) = tokens.next() else {
            return Ok(None);
        };
        let text = |token: String| self.exact(&fields, scope.is_some(), token, true);
        Ok(Some(
            tokens.fold(text(first), |acc, token| acc.and(text(token))),
        ))
    }

    /// After `=>[`: `KNN k @field $vec [AS alias] [EF_RUNTIME n] …]`.
    fn knn_clause(&mut self) -> Result<Knn, String> {
        let start = self.pos;
        let Some(len) = self.rest().find(']') else {
            return Err(self.syntax_error_at(start));
        };
        let inner = &self.rest()[..len];
        self.pos += len + 1;
        let tokens: Vec<&str> = inner.split_whitespace().collect();
        let [knn, k, field, vector, options @ ..] = tokens.as_slice() else {
            return Err(self.syntax_error_at(start));
        };
        if !knn.eq_ignore_ascii_case("KNN") {
            // A vector clause this server does not know. Saying so beats
            // answering with an empty result.
            return Err(format!(
                "Syntax error at offset {start} near {knn}: unsupported vector query"
            ));
        }
        let k = self.count(k)?;
        let (Some(attribute), Some(vector_name)) =
            (field.strip_prefix('@'), vector.strip_prefix('$'))
        else {
            return Err(self.syntax_error_at(start));
        };
        let Some(schema_field) = self.def.field_by_attribute(attribute) else {
            return Err(format!("Unknown field at offset {start} near {attribute}"));
        };
        let FieldType::Vector { dim, data_type, .. } = &schema_field.field_type else {
            return Err(format!(
                "Error parsing vector similarity query: '{attribute}' is not a vector field"
            ));
        };
        let mut clause = Knn {
            k,
            field: schema_field.name.clone(),
            vector: self.vector_param(vector_name, *dim, *data_type)?,
            alias: format!("__{attribute}_score"),
            ef_runtime: None,
            policy: None,
            batch_size: None,
        };
        let mut options = options.iter();
        while let Some(option) = options.next() {
            let mut value = || options.next().ok_or_else(|| self.syntax_error_at(start));
            match option.to_ascii_uppercase().as_str() {
                "AS" => clause.alias = value()?.to_string(),
                "EF_RUNTIME" => clause.ef_runtime = Some(self.count(value()?)?),
                "HYBRID_POLICY" => clause.policy = Some(HybridPolicy::parse(value()?)?),
                "BATCH_SIZE" => clause.batch_size = Some(self.count(value()?)?),
                // For a range query; a KNN has no use for it.
                "EPSILON" => {
                    value()?;
                }
                _ => return Err(self.syntax_error_at(start)),
            }
        }
        Ok(clause)
    }

    /// After `=>{`: `$name: value; $name: value }`. Names are returned in
    /// lower case, without the `$`.
    fn attributes(&mut self) -> Result<Vec<(String, String)>, String> {
        let start = self.pos;
        let Some(len) = self.rest().find('}') else {
            return Err(self.syntax_error_at(start));
        };
        let inner = &self.rest()[..len];
        self.pos += len + 1;
        let mut out = Vec::new();
        for pair in inner.split(';') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            let Some((name, value)) = pair.split_once(':') else {
                return Err(self.syntax_error_at(start));
            };
            let Some(name) = name.trim().strip_prefix('$') else {
                return Err(self.syntax_error_at(start));
            };
            let value = value.trim();
            let value = match value.strip_prefix('$') {
                Some(param) => self.param_text(param)?,
                None => value.to_string(),
            };
            out.push((name.to_ascii_lowercase(), value));
        }
        Ok(out)
    }
}

/// `clause` without the phonetic codes its words were expanded with
/// (`$phonetic: false`).
fn without_phonetic(clause: Filter) -> Filter {
    match clause {
        Filter::Or(left, right) => {
            if matches!(
                &*right,
                Filter::Text {
                    term: TextMatch::Phonetic(_),
                    ..
                }
            ) {
                return without_phonetic(*left);
            }
            Filter::Or(
                Box::new(without_phonetic(*left)),
                Box::new(without_phonetic(*right)),
            )
        }
        Filter::And(left, right) => Filter::And(
            Box::new(without_phonetic(*left)),
            Box::new(without_phonetic(*right)),
        ),
        Filter::Not(inner) => Filter::Not(Box::new(without_phonetic(*inner))),
        Filter::Optional(inner) => Filter::Optional(Box::new(without_phonetic(*inner))),
        other => other,
    }
}

/// Whether `clause` has a phonetic code in it (a word of a PHONETIC field).
fn has_phonetic(clause: &Filter) -> bool {
    match clause {
        Filter::Text {
            term: TextMatch::Phonetic(_),
            ..
        } => true,
        Filter::Or(l, r) | Filter::And(l, r) => has_phonetic(l) || has_phonetic(r),
        Filter::Not(inner) | Filter::Optional(inner) => has_phonetic(inner),
        _ => false,
    }
}

// ── Evaluator ─────────────────────────────────────────────────────────────────

/// Whether `a` can be turned into `b` with at most `max` single-character
/// insertions, deletions or substitutions.
fn within_edits(a: &str, b: &str, max: usize) -> bool {
    let (a, b): (Vec<char>, Vec<char>) = (a.chars().collect(), b.chars().collect());
    if a.len().abs_diff(b.len()) > max {
        return false;
    }
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.iter().enumerate() {
        let mut row = vec![i + 1];
        for (j, cb) in b.iter().enumerate() {
            let cost = usize::from(ca != cb);
            row.push((prev[j] + cost).min(prev[j + 1] + 1).min(row[j] + 1));
        }
        prev = row;
    }
    prev[b.len()] <= max
}

/// The vector a document holds in `field`, if it is `dim` elements of `ty`.
pub fn doc_vector(doc: &dyn Doc, field: &str, dim: usize, ty: VectorType) -> Option<Vec<f32>> {
    ty.decode(doc.field_bytes(field)?, dim)
}

/// The distance a `VECTOR_RANGE` clause measures for this document, if the
/// document has a vector there.
pub fn range_distance(filter: &Filter, doc: &dyn Doc) -> Option<f32> {
    let Filter::VectorRange {
        field,
        vector,
        metric,
        data_type,
        ..
    } = filter
    else {
        return None;
    };
    let stored = doc_vector(doc, field, vector.len(), *data_type)?;
    Some(super::distance(metric, vector, &stored))
}

/// The tags a TAG field's value holds.
pub fn split_tags(value: &str, separator: char) -> impl Iterator<Item = &str> {
    value
        .split(separator)
        .map(str::trim)
        .filter(|tag| !tag.is_empty())
}

/// Evaluate a filter against a document's fields.
pub fn eval(filter: &Filter, doc: &dyn Doc) -> bool {
    match filter {
        Filter::MatchAll => true,
        Filter::MatchNone => false,

        Filter::Tag {
            field,
            values,
            prefixes,
            separator,
            case_sensitive,
        } => {
            let Some(text) = doc
                .field_bytes(field)
                .and_then(|b| std::str::from_utf8(b).ok())
            else {
                return false;
            };
            split_tags(text, *separator).any(|tag| {
                let lowered;
                let tag = if *case_sensitive {
                    tag
                } else {
                    lowered = tag.to_lowercase();
                    lowered.as_str()
                };
                values.iter().any(|v| v == tag)
                    || prefixes.iter().any(|p| tag.starts_with(p.as_str()))
            })
        }

        Filter::Numeric {
            field,
            lo,
            lo_exclusive,
            hi,
            hi_exclusive,
        } => {
            let Some(n) = numeric_value(doc, field) else {
                return false;
            };
            (if *lo_exclusive { n > *lo } else { n >= *lo })
                && (if *hi_exclusive { n < *hi } else { n <= *hi })
        }

        Filter::NumericNot { field, value } => {
            numeric_value(doc, field).is_some_and(|n| n != *value)
        }

        Filter::Geo {
            field,
            lon,
            lat,
            radius_m,
            json,
            ..
        } => {
            crate::geohash::centre_searchable(*lat)
                && doc.field_bytes(field).is_some_and(|raw| {
                    super::geo::points(raw, *json).is_ok_and(|points| {
                        points.iter().any(|&(point_lon, point_lat)| {
                            crate::geohash::hash_within(
                                crate::geohash::geohash_bits(point_lon, point_lat),
                                *lon,
                                *lat,
                                *radius_m,
                            )
                        })
                    })
                })
        }

        Filter::Text {
            fields,
            term: TextMatch::Stem(stem),
        } => {
            // The language the document's words were stemmed in.
            let language = stem
                .language_field
                .as_deref()
                .and_then(|f| doc.field_bytes(f))
                .and_then(|b| std::str::from_utf8(b).ok())
                .and_then(Language::parse)
                .unwrap_or(stem.language);
            fields
                .iter()
                .filter(|field| !stem.nostem.contains(field))
                .any(|field| {
                    doc.field_bytes(field)
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .is_some_and(|text| {
                            super::tokenize_for_vocab(text).any(|tok| stem.has(language, &tok))
                        })
                })
        }

        Filter::Text { fields, term } => fields.iter().any(|field| {
            doc.field_bytes(field)
                .and_then(|b| std::str::from_utf8(b).ok())
                .is_some_and(|text| super::tokenize_for_vocab(text).any(|tok| term.matches(&tok)))
        }),

        Filter::Phrase { fields, terms } => fields.iter().any(|field| {
            doc.field_bytes(field)
                .and_then(|b| std::str::from_utf8(b).ok())
                .is_some_and(|text| {
                    let tokens: Vec<String> = super::tokenize_for_vocab(text).collect();
                    tokens
                        .windows(terms.len())
                        .any(|window| window == terms.as_slice())
                })
        }),

        Filter::VectorRange { radius, .. } => {
            range_distance(filter, doc).is_some_and(|distance| f64::from(distance) <= *radius)
        }

        Filter::And(l, r) => eval(l, doc) && eval(r, doc),
        Filter::Or(l, r) => eval(l, doc) || eval(r, doc),
        Filter::Not(inner) => !eval(inner, doc),
        Filter::Optional(_) => true,
    }
}

fn numeric_value(doc: &dyn Doc, field: &str) -> Option<f64> {
    std::str::from_utf8(doc.field_bytes(field)?)
        .ok()?
        .trim()
        .parse::<f64>()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::super::{FieldOptions, IndexOn, VectorAlgo};
    use super::*;

    fn field(name: &str, field_type: FieldType) -> SchemaField {
        SchemaField {
            name: name.into(),
            field_type,
            options: FieldOptions::default(),
        }
    }

    fn schema() -> IndexDef {
        let mut tags = field("tags", FieldType::Tag);
        tags.options.separator = ';';
        let mut code = field("code", FieldType::Tag);
        code.options.case_sensitive = true;
        let mut renamed = field("stored_as", FieldType::Text);
        renamed.options.alias = Some("alias".into());
        super::super::IndexSchema {
            name: "idx".into(),
            db_index: 0,
            on: IndexOn::Hash,
            prefixes: vec![],
            fields: vec![
                field("title", FieldType::Text),
                field("body", FieldType::Text),
                field("k", FieldType::Tag),
                tags,
                code,
                field("n", FieldType::Numeric),
                renamed,
                field(
                    "v",
                    FieldType::Vector {
                        algo: VectorAlgo::Flat,
                        dim: 2,
                        metric: DistanceMetric::L2,
                        data_type: VectorType::Float32,
                    },
                ),
            ],
            aliases: vec![],
            stopwords: None,
            synonyms: Default::default(),
            language: Default::default(),
            default_score: 1.0,
            score_field: None,
            nofreqs: false,
        }
        .into_def()
    }

    /// `INFIELDS`' names, read against the schema: by attribute, TEXT only,
    /// each field once however often it is named.
    #[test]
    fn infields_names_resolve_against_the_schema() {
        let def = schema();
        let named = |names: &[&str]| {
            let names: Vec<String> = names.iter().map(|n| n.to_string()).collect();
            text_fields_named(&def, &names)
        };
        assert_eq!(named(&["title"; 1000]), ["title"]);
        assert_eq!(named(&["body", "title", "body"]), ["title", "body"]);
        assert_eq!(named(&["alias"]), ["stored_as"]);
        assert!(named(&["stored_as", "k", "n", "v", "nosuch", "TITLE"]).is_empty());
    }

    fn doc(pairs: &[(&str, &str)]) -> AHashMap<String, Vec<u8>> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
            .collect()
    }

    fn vec_bytes(v: &[f32]) -> Vec<u8> {
        v.iter().flat_map(|f| f.to_le_bytes()).collect()
    }

    fn matches(query: &str, pairs: &[(&str, &str)]) -> bool {
        let def = schema();
        let parsed =
            parse_query(query, &Params::new(), &def).unwrap_or_else(|e| panic!("{query}: {e}"));
        eval(&parsed.filter, &doc(pairs))
    }

    fn rejected(query: &str) -> String {
        parse_query(query, &Params::new(), &schema())
            .err()
            .unwrap_or_else(|| panic!("{query} was accepted"))
    }

    #[test]
    fn words() {
        let d = [("title", "Hello, World"), ("body", "the quick brown fox")];
        assert!(matches("*", &d));
        assert!(matches("hello", &d));
        assert!(matches("hello world", &d));
        assert!(matches("hello fox", &d));
        assert!(!matches("hello cat", &d));
        assert!(matches("hello | cat", &d));
        assert!(matches("cat | dog | fox", &d));
        assert!(!matches("cat | dog", &d));
        assert!(matches("-cat", &d));
        assert!(!matches("-hello", &d));
        assert!(matches("hello -cat", &d));
        assert!(!matches("hello -fox", &d));
        assert!(matches("hello ~cat", &d));
        assert!(matches("(hello | cat) (fox | dog)", &d));
        assert!(!matches("(hello | cat) (dog)", &d));
    }

    #[test]
    fn fields_scope_words() {
        let d = [("title", "hello world"), ("body", "quick brown fox")];
        assert!(matches("@title:hello", &d));
        assert!(!matches("@body:hello", &d));
        assert!(matches("@title|body:fox", &d));
        assert!(matches("@title:(hello world)", &d));
        assert!(matches("@title:(hello | fox)", &d));
        assert!(!matches("@title:(hello fox)", &d));
        assert!(matches("@title:hello @body:fox", &d));
        assert!(matches("@title:hello | @body:cat", &d));
        assert!(matches("-@title:cat", &d));
        // An attribute name given with AS is the name queries use.
        let d = [("stored_as", "under another name")];
        assert!(matches("@alias:another", &d));
        assert!(rejected("@stored_as:another").contains("Unknown field"));
    }

    #[test]
    fn phrases_and_wildcards() {
        let d = [("title", "the quick brown fox jumps")];
        assert!(matches("\"quick brown\"", &d));
        assert!(!matches("\"brown quick\"", &d));
        assert!(matches("@title:\"brown fox jumps\"", &d));
        assert!(matches("qui*", &d));
        assert!(!matches("zzz*", &d));
        assert!(matches("*umps", &d));
        assert!(matches("*row*", &d));
        assert!(matches("%quikc%%", &d) || matches("%%quikc%%", &d));
        assert!(matches("%quic%", &d));
        assert!(!matches("%qu%", &d));
    }

    #[test]
    fn words_are_found_by_their_stems() {
        let d = [("title", "I prefer email"), ("body", "running dogs")];
        assert!(matches("@title:preferences", &d));
        assert!(matches("preferred", &d));
        assert!(matches("@body:dog", &d));
        assert!(matches("@body:run", &d));
        assert!(matches("runs", &d));
        assert!(!matches("@title:dog", &d));
        let verbatim = |query: &str| {
            let def = schema();
            let options = QueryOptions {
                verbatim: true,
                language: None,
                dialect: 0,
            };
            let parsed = parse_query_with(query, &Params::new(), &def, options).unwrap();
            eval(&parsed.filter, &doc(&d))
        };
        assert!(!verbatim("@title:preferences"));
        assert!(verbatim("@title:prefer"));
        assert!(!verbatim("@body:dog"));
    }

    #[test]
    fn stopwords_restrict_nothing_but_match_nothing_alone() {
        let d = [("title", "the quick brown fox")];
        assert!(matches("the quick", &d));
        assert!(!matches("the", &d));
        // Punctuation separates words on both sides.
        let d = [("title", "wi-fi,router/modem")];
        assert!(matches("wi", &d));
        assert!(matches("router", &d));
        assert!(matches("wi-fi", &d));
    }

    #[test]
    fn tags() {
        let d = [
            ("k", "tenantA, Shared"),
            ("tags", "red;dark blue"),
            ("code", "AbC"),
        ];
        assert!(matches("@k:{tenantA}", &d));
        assert!(matches("@k:{TENANTA}", &d));
        assert!(matches("@k:{shared}", &d));
        assert!(matches("@k:{nobody | shared}", &d));
        assert!(!matches("@k:{tenantB}", &d));
        assert!(!matches("@k:{tenant}", &d));
        assert!(matches("@k:{tenant*}", &d));
        // The field's own separator, and a value with a space in it.
        assert!(matches("@tags:{dark blue}", &d));
        assert!(matches("@tags:{dark\\ blue}", &d));
        assert!(matches("@tags:{\"dark blue\"}", &d));
        assert!(!matches("@tags:{blue}", &d));
        // CASESENSITIVE
        assert!(matches("@code:{AbC}", &d));
        assert!(!matches("@code:{abc}", &d));
        // The cases seen from clients, LiteLLM's among them: NOT and OR around tag clauses.
        assert!(!matches("-@k:{tenantA}", &d));
        assert!(matches("(-@k:{tenantB})", &d));
        assert!(matches("(@k:{tenantB}|@k:{tenantA})", &d));
        assert!(matches("(@k:{tenantA} | @k:{nobody})", &d));
        assert!(!matches("@k:{tenantB} | @k:{nobody}", &d));
        // A tag field has no words: without braces nothing matches.
        assert!(!matches("@k:tenantA", &d));
        assert!(!matches("(@k:tenantA)", &d));
        // …and braces on a field that is not a tag field match nothing.
        assert!(!matches("@title:{tenantA}", &[("title", "tenantA")]));
    }

    #[test]
    fn numbers() {
        let d = [("n", "10")];
        assert!(matches("@n:[0 10]", &d));
        assert!(matches("@n:[10 10]", &d));
        assert!(!matches("@n:[0 (10]", &d));
        assert!(!matches("@n:[(10 20]", &d));
        assert!(matches("@n:[-inf +inf]", &d));
        assert!(matches("@n:[10]", &d));
        assert!(!matches("@n:[11]", &d));
        assert!(matches("@n==10", &d));
        assert!(matches("@n!=11", &d));
        assert!(!matches("@n!=10", &d));
        assert!(matches("@n>=10", &d));
        assert!(!matches("@n>10", &d));
        assert!(matches("@n<=10", &d));
        assert!(!matches("@n<10", &d));
        assert!(matches("@n>5 @n<20", &d));
        assert!(!matches("@n:[0 10]", &[("n", "not a number")]));
        assert!(!matches("@n:[0 10]", &[]));
    }

    #[test]
    fn parameters() {
        let def = schema();
        let mut params = Params::new();
        params.insert("who".into(), b"tenantA".to_vec());
        params.insert("lo".into(), b"5".to_vec());
        params.insert("word".into(), b"hello".to_vec());
        let d = doc(&[("k", "tenantA"), ("n", "10"), ("title", "hello")]);
        for query in [
            "@k:{$who}",
            "@n:[$lo 20]",
            "@n>=$lo",
            "@title:$word",
            "$word",
        ] {
            let parsed =
                parse_query(query, &params, &def).unwrap_or_else(|e| panic!("{query}: {e}"));
            assert!(eval(&parsed.filter, &d), "{query}");
        }
        let err = parse_query("@k:{$missing}", &params, &def).unwrap_err();
        assert!(err.contains("No such parameter"), "{err}");
    }

    #[test]
    fn vector_range_and_knn() {
        let def = schema();
        let mut params = Params::new();
        params.insert("q".into(), vec_bytes(&[0.0, 0.0]));
        params.insert("r".into(), b"2".to_vec());
        params.insert("k".into(), b"7".to_vec());

        let near: AHashMap<String, Vec<u8>> = [
            ("v".to_string(), vec_bytes(&[1.0, 0.0])),
            ("k".to_string(), b"a".to_vec()),
        ]
        .into();
        let far: AHashMap<String, Vec<u8>> = [("v".to_string(), vec_bytes(&[3.0, 0.0]))].into();

        let parsed = parse_query(
            "@v:[VECTOR_RANGE $r $q]=>{$YIELD_DISTANCE_AS: dist}",
            &params,
            &def,
        )
        .unwrap();
        assert!(eval(&parsed.filter, &near));
        assert!(!eval(&parsed.filter, &far));
        assert!(!eval(&parsed.filter, &doc(&[])));
        let range = parsed.filter.vector_range().expect("a range clause");
        assert_eq!(range_distance(range, &near), Some(1.0));
        assert!(matches!(range, Filter::VectorRange { yield_as: Some(a), .. } if a == "dist"));

        // With a filter beside it, as LiteLLM writes it.
        let parsed = parse_query(
            "@v:[VECTOR_RANGE 2 $q]=>{$yield_distance_as: d} (@k:{a})",
            &params,
            &def,
        )
        .unwrap();
        assert!(eval(&parsed.filter, &near));
        assert!(parsed.filter.vector_range().is_some());

        let parsed =
            parse_query("(@k:{a})=>[KNN $k @v $q AS d EF_RUNTIME 50]", &params, &def).unwrap();
        let knn = parsed.knn.expect("a KNN clause");
        assert_eq!(
            (knn.k, knn.field.as_str(), knn.alias.as_str()),
            (7, "v", "d")
        );
        assert_eq!(knn.ef_runtime, Some(50));
        assert_eq!(knn.vector, [0.0, 0.0]);
        assert!(eval(&parsed.filter, &near));

        let parsed = parse_query("*=>[KNN 3 @v $q]", &params, &def).unwrap();
        assert!(matches!(parsed.filter, Filter::MatchAll));
        assert_eq!(parsed.knn.unwrap().alias, "__v_score");

        // A vector of the wrong size, a field that is not a vector, a vector
        // clause that is not KNN: errors, not empty results.
        params.insert("short".into(), vec![0u8; 4]);
        for bad in [
            "*=>[KNN 3 @v $short]",
            "*=>[KNN 3 @title $q]",
            "*=>[KNN 3 @nosuch $q]",
            "*=>[KNN 3 @v $nosuch]",
            "*=>[RANGE 3 @v $q]",
            "@v:[VECTOR_RANGE 2 $short]",
            "@title:[VECTOR_RANGE 2 $q]",
            "@v:[VECTOR_RANGE -1 $q]",
        ] {
            assert!(
                parse_query(bad, &params, &def).is_err(),
                "{bad} was accepted"
            );
        }
    }

    #[test]
    fn what_cannot_be_parsed_is_an_error() {
        for bad in [
            "(hello",
            "hello)",
            "@title:",
            "@:hello",
            "@k:{",
            "@k:{}",
            "@n:[1 2 3]",
            "@n:[1",
            "@n:[x y]",
            "\"unterminated",
            "hello |",
            "| hello",
            "()",
            "@nosuch:hello",
            "@nosuch:{a}",
            "*=>[KNN",
            "hello =>{$weight: 2",
        ] {
            let err = rejected(bad);
            assert!(
                err.starts_with("Syntax error") || err.starts_with("Unknown field"),
                "{bad}: {err}"
            );
        }
        assert!(rejected("@nosuch:hello").contains("near nosuch"));
    }

    #[test]
    fn a_query_too_long_or_too_deep_is_an_error() {
        assert!(rejected(&"(".repeat(100_000)).contains("too deep"));
        assert!(rejected(&"-".repeat(100_000)).contains("too deep"));
        assert!(rejected(&vec!["hello"; 100_000].join(" ")).contains("too long"));
        assert!(rejected(&vec!["hello"; 100_000].join("|")).contains("too long"));
    }

    #[test]
    fn edit_distance() {
        assert!(within_edits("hello", "hello", 0));
        assert!(within_edits("hello", "helo", 1));
        assert!(within_edits("hello", "hallo", 1));
        assert!(!within_edits("hello", "help", 1));
        assert!(within_edits("hello", "help", 2));
        assert!(!within_edits("a", "abcd", 2));
    }
}
