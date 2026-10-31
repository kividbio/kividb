// Stemming of TEXT fields, as RediSearch does it.
//
// A word of a TEXT field is indexed as itself and, if its stem differs, as
// the stem too, written with a `+` in front (`preferences` puts in
// `preferences` and `+prefer`). A word of a query is looked for as itself,
// as its stem with the `+`, and as the bare stem if that differs from the
// word: `@t:preferences` is `preferences | +prefer | prefer`, which finds a
// document holding `prefer`, `prefers` or `preferences`. `VERBATIM` turns
// that off for a query, `NOSTEM` for a field.
//
// The stem is not indexed for a word shorter than `MINSTEMLEN` bytes
// (`search-min-stem-len`, 4 by default), or for a field marked `NOSTEM`. The
// language is the index's (`LANGUAGE`, English by default), or a document's
// own if the index names a field that holds it (`LANGUAGE_FIELD`). The
// stemmers are Snowball's, from `rust-stemmers`: Arabic, Danish, Dutch,
// English, Finnish, French, German, Greek, Hungarian, Italian, Norwegian,
// Portuguese, Romanian, Russian, Spanish, Swedish, Tamil and Turkish. The
// other languages Redis 8.10 accepts (Armenian, Basque, Catalan, Chinese,
// Hindi, Indonesian, Irish, Lithuanian, Malay, Nepali, Serbian, Tagalog,
// Yiddish) are accepted, and their words are indexed and looked for as they
// are, without stems.
//
// `+` separates words in a text, so a word of a document never starts with
// one (but for one written with a backslash, `\+word`, as in RediSearch,
// where it is the same).

use super::{FieldType, IndexDef, SchemaField};
use std::borrow::Cow;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

/// What a stem is written with in the index, ahead of the word.
pub const STEM_PREFIX: char = '+';

/// Whether an indexed term is a stem (`+prefer`) rather than a word.
#[inline]
pub fn is_stem(term: &str) -> bool {
    term.starts_with(STEM_PREFIX)
}

/// The shortest word, in bytes, that has its stem indexed: `MINSTEMLEN`,
/// `search-min-stem-len`.
static MIN_STEM_LEN: AtomicUsize = AtomicUsize::new(4);

/// The lowest `MINSTEMLEN` may be set to, as in Redis.
pub const MIN_STEM_LEN_FLOOR: usize = 2;

pub fn min_stem_len() -> usize {
    MIN_STEM_LEN.load(Relaxed)
}

/// Set `MINSTEMLEN`. Words already indexed keep the stems they were given.
pub fn set_min_stem_len(len: usize) {
    MIN_STEM_LEN.store(len.max(MIN_STEM_LEN_FLOOR), Relaxed);
}

/// A language of `LANGUAGE`: the ones RediSearch accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Language {
    Arabic,
    Armenian,
    Basque,
    Catalan,
    Chinese,
    Danish,
    Dutch,
    #[default]
    English,
    Finnish,
    French,
    German,
    Greek,
    Hindi,
    Hungarian,
    Indonesian,
    Irish,
    Italian,
    Lithuanian,
    Malay,
    Nepali,
    Norwegian,
    Portuguese,
    Romanian,
    Russian,
    Serbian,
    Spanish,
    Swedish,
    Tagalog,
    Tamil,
    Turkish,
    Yiddish,
}

const LANGUAGES: [(Language, &str); 31] = [
    (Language::Arabic, "arabic"),
    (Language::Armenian, "armenian"),
    (Language::Basque, "basque"),
    (Language::Catalan, "catalan"),
    (Language::Chinese, "chinese"),
    (Language::Danish, "danish"),
    (Language::Dutch, "dutch"),
    (Language::English, "english"),
    (Language::Finnish, "finnish"),
    (Language::French, "french"),
    (Language::German, "german"),
    (Language::Greek, "greek"),
    (Language::Hindi, "hindi"),
    (Language::Hungarian, "hungarian"),
    (Language::Indonesian, "indonesian"),
    (Language::Irish, "irish"),
    (Language::Italian, "italian"),
    (Language::Lithuanian, "lithuanian"),
    (Language::Malay, "malay"),
    (Language::Nepali, "nepali"),
    (Language::Norwegian, "norwegian"),
    (Language::Portuguese, "portuguese"),
    (Language::Romanian, "romanian"),
    (Language::Russian, "russian"),
    (Language::Serbian, "serbian"),
    (Language::Spanish, "spanish"),
    (Language::Swedish, "swedish"),
    (Language::Tagalog, "tagalog"),
    (Language::Tamil, "tamil"),
    (Language::Turkish, "turkish"),
    (Language::Yiddish, "yiddish"),
];

impl Language {
    /// The language a name stands for, in any case: `None` for one Redis
    /// does not know.
    pub fn parse(name: &str) -> Option<Language> {
        LANGUAGES
            .iter()
            .find(|(_, n)| n.eq_ignore_ascii_case(name))
            .map(|(l, _)| *l)
    }

    /// Its name, as `FT.INFO` shows it.
    pub fn name(self) -> &'static str {
        LANGUAGES
            .iter()
            .find(|(l, _)| *l == self)
            .map_or("english", |(_, n)| n)
    }

    fn algorithm(self) -> Option<rust_stemmers::Algorithm> {
        use rust_stemmers::Algorithm as A;
        Some(match self {
            Language::Arabic => A::Arabic,
            Language::Danish => A::Danish,
            Language::Dutch => A::Dutch,
            Language::English => A::English,
            Language::Finnish => A::Finnish,
            Language::French => A::French,
            Language::German => A::German,
            Language::Greek => A::Greek,
            Language::Hungarian => A::Hungarian,
            Language::Italian => A::Italian,
            Language::Norwegian => A::Norwegian,
            Language::Portuguese => A::Portuguese,
            Language::Romanian => A::Romanian,
            Language::Russian => A::Russian,
            Language::Spanish => A::Spanish,
            Language::Swedish => A::Swedish,
            Language::Tamil => A::Tamil,
            Language::Turkish => A::Turkish,
            _ => return None,
        })
    }

    /// Whether words of this language are stemmed here.
    pub fn stems(self) -> bool {
        self.algorithm().is_some()
    }

    /// The stem of `word` (lower-cased already), whatever its length: what a
    /// query's word is expanded with. `None` if the language has no stemmer.
    pub fn stem(self, word: &str) -> Option<String> {
        self.stem_of(word).map(Cow::into_owned)
    }

    /// [`Language::stem`], borrowing the word where it is its own stem.
    fn stem_of(self, word: &str) -> Option<Cow<'_, str>> {
        let algorithm = self.algorithm()?;
        Some(rust_stemmers::Stemmer::create(algorithm).stem(word))
    }
}

/// The stem `word` is indexed with, without the `+`: `None` if it is not
/// (too short, the same as the word, or a language with no stemmer).
pub fn index_stem(language: Language, word: &str) -> Option<String> {
    index_stem_of(language, word).map(Cow::into_owned)
}

/// [`index_stem`], without copying a word that is its own stem.
fn index_stem_of(language: Language, word: &str) -> Option<Cow<'_, str>> {
    if word.len() < min_stem_len() {
        return None;
    }
    language.stem_of(word).filter(|stem| stem != word)
}

/// The index's settings for stemming: `LANGUAGE` and `LANGUAGE_FIELD` of
/// `FT.CREATE`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LanguageOptions {
    /// The language of every document, or of one whose language field does
    /// not name one.
    pub language: Language,
    /// The field a document's language is read from.
    pub field: Option<String>,
}

impl LanguageOptions {
    /// The words `FT.CREATE` takes them as, ahead of `SCHEMA`.
    pub fn words(&self) -> Vec<String> {
        let mut out = Vec::new();
        if self.language != Language::English {
            out.push("LANGUAGE".into());
            out.push(self.language.name().into());
        }
        if let Some(field) = &self.field {
            out.push("LANGUAGE_FIELD".into());
            out.push(field.clone());
        }
        out
    }

    /// The language of a document whose language field holds `value`: the
    /// index's when it holds none, or none Redis knows (as in Redis, which
    /// indexes such a document in the default language).
    pub fn of(&self, value: Option<&[u8]>) -> Language {
        value
            .and_then(|v| std::str::from_utf8(v).ok())
            .and_then(Language::parse)
            .unwrap_or(self.language)
    }
}

/// What TEXT field `field` of a document in `language` puts into the index
/// for `text`: its words, stopwords left out, each followed by its stem with
/// the `+` when it has one (see [`index_stem`]).
pub fn index_words(
    def: &IndexDef,
    field: &SchemaField,
    language: Language,
    text: &str,
) -> Vec<String> {
    let stems = matches!(field.field_type, FieldType::Text) && !field.options.nostem;
    let mut out = Vec::new();
    for word in super::tokenize_for_vocab(text) {
        if def.is_stopword(&word) {
            continue;
        }
        let term = stems
            .then(|| index_stem_of(language, &word))
            .flatten()
            .map(|stem| {
                let mut term = String::with_capacity(stem.len() + 1);
                term.push(STEM_PREFIX);
                term.push_str(&stem);
                term
            });
        out.push(word);
        out.extend(term);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn languages_are_read_in_any_case() {
        assert_eq!(Language::parse("German"), Some(Language::German));
        assert_eq!(Language::parse("ENGLISH"), Some(Language::English));
        assert_eq!(Language::parse("klingon"), None);
        assert_eq!(Language::German.name(), "german");
        assert!(Language::English.stems());
        assert!(!Language::Chinese.stems());
    }

    #[test]
    fn short_words_and_unchanged_stems_are_not_indexed() {
        assert_eq!(
            index_stem(Language::English, "preferences").as_deref(),
            Some("prefer")
        );
        assert_eq!(
            index_stem(Language::English, "dogs").as_deref(),
            Some("dog")
        );
        // Its own stem.
        assert_eq!(index_stem(Language::English, "email"), None);
        // Shorter than MINSTEMLEN (4).
        assert_eq!(index_stem(Language::English, "ran"), None);
        assert_eq!(index_stem(Language::Chinese, "words"), None);
        // A query's word is stemmed whatever its length.
        assert_eq!(Language::English.stem("ran").as_deref(), Some("ran"));
    }
}
