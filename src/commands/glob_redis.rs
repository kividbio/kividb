//! Glob matching, as Redis does it (`stringmatchlen`): `*` any run of bytes,
//! `?` any one, `[abc]` one of these, `[^abc]` none of them, `[a-z]` one in a
//! range, and `\x` the byte `x` whatever it is. On bytes, not characters.
//!
//! A pattern with more than a thousand `*` to be gone through matches
//! nothing: that is Redis's guard against patterns built to take for ever,
//! and what `KEYS` and `SCAN … MATCH` answer has to be what Redis answers.

/// How many `*` deep a match may go.
const MAX_NESTING: usize = 1000;

/// Whether `text` matches `pattern`.
pub(crate) fn matches(pattern: &[u8], text: &[u8]) -> bool {
    // Everything, the empty string included — which Redis's matcher would
    // not say of `*`, and which the commands that use it say for it.
    if pattern == b"*" {
        return true;
    }
    let mut skip_longer = false;
    match_from(pattern, text, &mut skip_longer, 0)
}

/// A pattern made ready to match many texts: what a scan of the keyspace
/// matches every key it looks at against.
///
/// Most patterns given are a run of plain bytes with a `*` after it
/// (`schedule:*`), before it, or on both sides: those are told by comparing
/// bytes, without going through the pattern a byte at a time for each key.
/// Any other pattern is matched as [`matches`] does. Either way the answer
/// is the one [`matches`] gives.
pub(crate) struct Glob {
    pattern: Vec<u8>,
    shape: Shape,
}

enum Shape {
    /// `*`: anything.
    All,
    /// No `*`, `?` or `[`: these bytes and nothing else.
    Exact(Vec<u8>),
    /// `a*`
    Prefix(Vec<u8>),
    /// `*b`
    Suffix(Vec<u8>),
    /// `a*b`
    Both(Vec<u8>, Vec<u8>),
    /// `*a*`
    Contains(Box<memchr::memmem::Finder<'static>>),
    /// Anything else.
    General,
}

impl Glob {
    pub(crate) fn new(pattern: &[u8]) -> Glob {
        let shape = match literal_parts(pattern) {
            None => Shape::General,
            Some(mut parts) => match parts.len() {
                1 => Shape::Exact(parts.pop().unwrap_or_default()),
                2 => {
                    let back = parts.pop().unwrap_or_default();
                    let front = parts.pop().unwrap_or_default();
                    match (front.is_empty(), back.is_empty()) {
                        (true, true) => Shape::All,
                        (false, true) => Shape::Prefix(front),
                        (true, false) => Shape::Suffix(back),
                        (false, false) => Shape::Both(front, back),
                    }
                }
                3 if parts[0].is_empty() && parts[2].is_empty() && !parts[1].is_empty() => {
                    Shape::Contains(Box::new(
                        memchr::memmem::Finder::new(&parts[1]).into_owned(),
                    ))
                }
                _ => Shape::General,
            },
        };
        Glob {
            pattern: pattern.to_vec(),
            shape,
        }
    }

    /// Whether `text` matches: what [`matches`] says.
    #[inline]
    pub(crate) fn matches(&self, text: &[u8]) -> bool {
        // An empty text is the one place the matcher's trailing `*`s are
        // not simply "anything": it says what it says.
        if text.is_empty() {
            return matches(&self.pattern, text);
        }
        match &self.shape {
            Shape::All => true,
            Shape::Exact(bytes) => text == &bytes[..],
            Shape::Prefix(front) => text.starts_with(front),
            Shape::Suffix(back) => text.ends_with(back),
            Shape::Both(front, back) => {
                text.len() >= front.len() + back.len()
                    && text.starts_with(front)
                    && text.ends_with(back)
            }
            Shape::Contains(finder) => finder.find(text).is_some(),
            Shape::General => matches(&self.pattern, text),
        }
    }
}

/// The plain bytes of `pattern` between its `*`s (a run of them as one),
/// escapes taken out: `None` if it has a `?` or a `[`.
fn literal_parts(pattern: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut parts = vec![Vec::new()];
    let mut after_star = false;
    let mut i = 0;
    while i < pattern.len() {
        match pattern[i] {
            b'*' => {
                if !after_star {
                    parts.push(Vec::new());
                }
                after_star = true;
                i += 1;
                continue;
            }
            b'?' | b'[' => return None,
            // `\x` is `x`; a `\` the pattern ends with is itself.
            b'\\' if i + 1 < pattern.len() => {
                parts.last_mut()?.push(pattern[i + 1]);
                i += 2;
            }
            byte => {
                parts.last_mut()?.push(byte);
                i += 1;
            }
        }
        after_star = false;
    }
    Some(parts)
}

fn match_from(mut pattern: &[u8], mut text: &[u8], skip_longer: &mut bool, nesting: usize) -> bool {
    if nesting > MAX_NESTING {
        return false;
    }
    while !pattern.is_empty() && !text.is_empty() {
        match pattern[0] {
            b'*' => {
                while pattern.len() > 1 && pattern[1] == b'*' {
                    pattern = &pattern[1..];
                }
                if pattern.len() == 1 {
                    return true;
                }
                while !text.is_empty() {
                    if match_from(&pattern[1..], text, skip_longer, nesting + 1) {
                        return true;
                    }
                    if *skip_longer {
                        return false;
                    }
                    text = &text[1..];
                }
                // The rest of the pattern matches nowhere in the rest of the
                // text: no earlier `*` will do better by taking more.
                *skip_longer = true;
                return false;
            }
            b'?' => text = &text[1..],
            b'[' => {
                pattern = &pattern[1..];
                let negate = pattern.first() == Some(&b'^');
                if negate {
                    pattern = &pattern[1..];
                }
                let mut found = false;
                loop {
                    if pattern.len() >= 2 && pattern[0] == b'\\' {
                        pattern = &pattern[1..];
                        found |= pattern[0] == text[0];
                    } else if pattern.first() == Some(&b']') {
                        break;
                    } else if pattern.is_empty() {
                        // An unclosed class: taken as closed where the
                        // pattern ends.
                        return (found != negate) && text.len() == 1;
                    } else if pattern.len() >= 3 && pattern[1] == b'-' {
                        let (low, high) = (pattern[0].min(pattern[2]), pattern[0].max(pattern[2]));
                        pattern = &pattern[2..];
                        found |= (low..=high).contains(&text[0]);
                    } else {
                        found |= pattern[0] == text[0];
                    }
                    pattern = &pattern[1..];
                }
                if found == negate {
                    return false;
                }
                text = &text[1..];
            }
            first => {
                let literal = if first == b'\\' && pattern.len() >= 2 {
                    pattern = &pattern[1..];
                    pattern[0]
                } else {
                    first
                };
                if literal != text[0] {
                    return false;
                }
                text = &text[1..];
            }
        }
        pattern = &pattern[1..];
        if text.is_empty() {
            while pattern.first() == Some(&b'*') {
                pattern = &pattern[1..];
            }
            break;
        }
    }
    pattern.is_empty() && text.is_empty()
}

#[cfg(test)]
mod tests {
    use super::matches;

    fn m(pattern: &str, text: &str) -> bool {
        matches(pattern.as_bytes(), text.as_bytes())
    }

    #[test]
    fn stars_and_question_marks() {
        assert!(m("*", ""));
        assert!(m("*", "anything"));
        assert!(m("h?llo", "hello"));
        assert!(!m("h?llo", "hllo"));
        assert!(m("h*llo", "heeeello"));
        assert!(m("h*llo", "hllo"));
        assert!(m("a*b*c", "aXXbYYc"));
        assert!(!m("a*b*c", "aXXbYY"));
        assert!(m("foo*", "foobar"));
        assert!(!m("foo", "foobar"));
        assert!(m("", ""));
        assert!(!m("", "a"));
    }

    #[test]
    fn classes_and_escapes() {
        assert!(m("h[ae]llo", "hello"));
        assert!(m("h[ae]llo", "hallo"));
        assert!(!m("h[ae]llo", "hillo"));
        assert!(m("h[^e]llo", "hallo"));
        assert!(!m("h[^e]llo", "hello"));
        assert!(m("h[a-b]llo", "hbllo"));
        assert!(!m("h[a-b]llo", "hcllo"));
        // A range given backwards is the same range.
        assert!(m("h[b-a]llo", "hallo"));
        assert!(m("a\\*b", "a*b"));
        assert!(!m("a\\*b", "aXb"));
        assert!(m("a[\\]]b", "a]b"));
        assert!(m("foo\\", "foo\\"));
    }

    #[test]
    fn a_pattern_built_to_take_for_ever_matches_nothing() {
        // Redis's own regression: it answers, at once, that there is no
        // match — where the two would, given the time, match.
        let text = "a".repeat(50_000);
        let pattern = "*?".repeat(50_000);
        assert!(!m(&pattern, &text));
        // Many stars that do not have to be gone through one inside
        // another are no trouble.
        assert!(m(&"*".repeat(5000), "abc"));
        assert!(!m(&format!("{}b", "a*".repeat(30)), &"a".repeat(200)));
    }

    fn shape(pattern: &[u8]) -> &'static str {
        match super::Glob::new(pattern).shape {
            super::Shape::All => "all",
            super::Shape::Exact(_) => "exact",
            super::Shape::Prefix(_) => "prefix",
            super::Shape::Suffix(_) => "suffix",
            super::Shape::Both(..) => "both",
            super::Shape::Contains(_) => "contains",
            super::Shape::General => "general",
        }
    }

    #[test]
    fn plain_patterns_are_told_by_their_bytes() {
        assert_eq!(shape(b"*"), "all");
        assert_eq!(shape(b"***"), "all");
        assert_eq!(shape(b"schedule:*"), "prefix");
        assert_eq!(shape(b"schedule:**"), "prefix");
        assert_eq!(shape(b"*:1"), "suffix");
        assert_eq!(shape(b"*:1*"), "contains");
        assert_eq!(shape(b"a*b"), "both");
        assert_eq!(shape(b"abc"), "exact");
        assert_eq!(shape(b""), "exact");
        // An escaped `*`, `?` or `[` is a plain byte.
        assert_eq!(shape(b"a\\*b*"), "prefix");
        assert_eq!(shape(b"a\\?*"), "prefix");
        assert_eq!(shape(b"a\\[*"), "prefix");
        assert_eq!(shape(b"foo\\"), "exact");
        // Anything else is gone through as the matcher goes through it.
        assert_eq!(shape(b"a?*"), "general");
        assert_eq!(shape(b"a[bc]*"), "general");
        assert_eq!(shape(b"a*b*c"), "general");
        assert_eq!(shape(b"**"), "all");

        let glob = super::Glob::new(b"schedule:*");
        assert!(glob.matches(b"schedule:1"));
        assert!(glob.matches(b"schedule:"));
        assert!(!glob.matches(b"user:1"));
        assert!(!glob.matches(b"schedule"));
        let glob = super::Glob::new(b"a\\*b*");
        assert!(glob.matches(b"a*bc"));
        assert!(!glob.matches(b"aXbc"));
        let glob = super::Glob::new(b"ab*ba");
        assert!(!glob.matches(b"aba"), "the two ends do not overlap");
        assert!(glob.matches(b"abba"));
        // Binary keys.
        let glob = super::Glob::new(b"\xff\x00*");
        assert!(glob.matches(b"\xff\x00\x01"));
        assert!(!glob.matches(b"\xff\x01"));
        assert!(super::Glob::new(b"*\x00*").matches(b"a\x00b"));
    }

    /// Every pattern of up to four bytes from `a`, `b`, `*`, `?`, `[` and
    /// `\`, against every text of up to three bytes: the made-ready pattern
    /// answers as the matcher does.
    #[test]
    fn a_made_ready_pattern_answers_as_the_matcher_does() {
        fn all(alphabet: &[u8], longest: usize) -> Vec<Vec<u8>> {
            let mut out = vec![Vec::new()];
            let mut last = vec![Vec::new()];
            for _ in 0..longest {
                let mut next = Vec::new();
                for word in &last {
                    for &byte in alphabet {
                        let mut longer = word.clone();
                        longer.push(byte);
                        next.push(longer);
                    }
                }
                out.extend(next.iter().cloned());
                last = next;
            }
            out
        }
        let patterns = all(b"ab*?[\\", 4);
        let texts = all(b"ab*\\\xff", 3);
        for pattern in &patterns {
            let glob = super::Glob::new(pattern);
            for text in &texts {
                assert_eq!(
                    glob.matches(text),
                    matches(pattern, text),
                    "{:?} against {:?}",
                    String::from_utf8_lossy(pattern),
                    String::from_utf8_lossy(text)
                );
            }
        }
    }
}
