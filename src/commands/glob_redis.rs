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
}
