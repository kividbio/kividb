// Phonetic matching: a TEXT field made with `PHONETIC dm:en` (or `dm:fr`,
// `dm:pt`, `dm:es`) indexes, with each word of three bytes or more
// (`MIN_PHONETIC_TERM_LEN`), a term `<CODE` where CODE is the word's Double
// Metaphone: a query word on the field is looked for as the word, as `<CODE`
// and as its stem. The terms count as a stem does: they are no word of the
// document, and are not in its length.
//
// The code is the primary code of the Double Metaphone Lawrence Philips
// published (C/C++ Users Journal, 2000), at most four characters, as the
// letters A-Z are read: any other byte (a digit, an accent's bytes) is
// passed over. All four `dm:` variants give the same code.

// The rules follow the published algorithm one by one, some of them with
// the same result as the one before: they are kept apart, as published.
#![allow(clippy::if_same_then_else)]

/// What a phonetic term is written with, ahead of the code.
pub const PHONETIC_PREFIX: char = '<';

/// The longest code.
const MAX_CODE: usize = 4;

/// The term a word has in the index of a PHONETIC field, if it has one: the
/// word is at least `min_len` bytes long, and has a code.
pub fn term_of(word: &str, min_len: usize) -> Option<String> {
    if word.len() < min_len {
        return None;
    }
    let code = double_metaphone(word);
    if code.is_empty() {
        return None;
    }
    let mut term = String::with_capacity(code.len() + 1);
    term.push(PHONETIC_PREFIX);
    term.push_str(&code);
    Some(term)
}

struct Word {
    /// Upper case; bytes that are no letter are as they were.
    v: Vec<u8>,
}

impl Word {
    /// The byte at `i`, or 0 outside the word.
    fn at(&self, i: isize) -> u8 {
        if i < 0 || i as usize >= self.v.len() {
            0
        } else {
            self.v[i as usize]
        }
    }

    fn last(&self) -> isize {
        self.v.len() as isize - 1
    }

    /// Whether the word has one of `any` at `start`, each `len` bytes long.
    fn has(&self, start: isize, len: usize, any: &[&str]) -> bool {
        if start < 0 || start as usize + len > self.v.len() {
            return false;
        }
        let part = &self.v[start as usize..start as usize + len];
        any.iter().any(|c| c.as_bytes() == part)
    }

    fn vowel(&self, i: isize) -> bool {
        matches!(self.at(i), b'A' | b'E' | b'I' | b'O' | b'U' | b'Y')
    }
}

/// The primary Double Metaphone of `word`.
pub fn double_metaphone(word: &str) -> String {
    let mut v: Vec<u8> = word.bytes().collect();
    v.make_ascii_uppercase();
    let w = Word { v };
    let slavo = w.v.contains(&b'W')
        || w.v.contains(&b'K')
        || w.v.windows(2).any(|p| p == b"CZ")
        || w.v.windows(4).any(|p| p == b"WITZ");
    let mut out: Vec<u8> = Vec::with_capacity(8);
    let mut i: isize = if w.has(0, 2, &["GN", "KN", "PN", "WR", "PS"]) {
        1
    } else {
        0
    };
    let last = w.last();
    while out.len() < MAX_CODE && i <= last {
        match w.at(i) {
            b'A' | b'E' | b'I' | b'O' | b'U' | b'Y' => {
                if i == 0 {
                    out.push(b'A');
                }
                i += 1;
            }
            b'B' => {
                out.push(b'P');
                i += if w.at(i + 1) == b'B' { 2 } else { 1 };
            }
            b'C' => i = handle_c(&w, &mut out, i),
            b'D' => i = handle_d(&w, &mut out, i),
            b'F' => {
                out.push(b'F');
                i += if w.at(i + 1) == b'F' { 2 } else { 1 };
            }
            b'G' => i = handle_g(&w, &mut out, i, slavo),
            b'H' => {
                if (i == 0 || w.vowel(i - 1)) && w.vowel(i + 1) {
                    out.push(b'H');
                    i += 2;
                } else {
                    i += 1;
                }
            }
            b'J' => i = handle_j(&w, &mut out, i, slavo),
            b'K' => {
                out.push(b'K');
                i += if w.at(i + 1) == b'K' { 2 } else { 1 };
            }
            b'L' => {
                out.push(b'L');
                i += if w.at(i + 1) == b'L' { 2 } else { 1 };
            }
            b'M' => {
                out.push(b'M');
                i += if condition_m0(&w, i) { 2 } else { 1 };
            }
            b'N' => {
                out.push(b'N');
                i += if w.at(i + 1) == b'N' { 2 } else { 1 };
            }
            b'P' => {
                if w.at(i + 1) == b'H' {
                    out.push(b'F');
                    i += 2;
                } else {
                    out.push(b'P');
                    i += if w.has(i + 1, 1, &["P", "B"]) { 2 } else { 1 };
                }
            }
            b'Q' => {
                out.push(b'K');
                i += if w.at(i + 1) == b'Q' { 2 } else { 1 };
            }
            b'R' => {
                // The primary code has no R at the end of a French name
                // (`Rogier`), where the alternate has.
                if !(i == last
                    && !slavo
                    && w.has(i - 2, 2, &["IE"])
                    && !w.has(i - 4, 2, &["ME", "MA"]))
                {
                    out.push(b'R');
                }
                i += if w.at(i + 1) == b'R' { 2 } else { 1 };
            }
            b'S' => i = handle_s(&w, &mut out, i),
            b'T' => i = handle_t(&w, &mut out, i),
            b'V' => {
                out.push(b'F');
                i += if w.at(i + 1) == b'V' { 2 } else { 1 };
            }
            b'W' => i = handle_w(&w, &mut out, i),
            b'X' => {
                if i == 0 {
                    out.push(b'S');
                    i += 1;
                } else {
                    if !(i == last
                        && (w.has(i - 3, 3, &["IAU", "EAU"]) || w.has(i - 2, 2, &["AU", "OU"])))
                    {
                        out.extend_from_slice(b"KS");
                    }
                    i += if w.has(i + 1, 1, &["C", "X"]) { 2 } else { 1 };
                }
            }
            b'Z' => {
                if w.at(i + 1) == b'H' {
                    out.push(b'J');
                    i += 2;
                } else {
                    out.push(b'S');
                    i += if w.at(i + 1) == b'Z' { 2 } else { 1 };
                }
            }
            _ => i += 1,
        }
    }
    out.truncate(MAX_CODE);
    String::from_utf8(out).unwrap_or_default()
}

fn condition_m0(w: &Word, i: isize) -> bool {
    if w.at(i + 1) == b'M' {
        return true;
    }
    w.has(i - 1, 3, &["UMB"]) && (i + 1 == w.last() || w.has(i + 2, 2, &["ER"]))
}

fn condition_c0(w: &Word, i: isize) -> bool {
    if w.has(i, 4, &["CHIA"]) {
        true
    } else if i <= 1 {
        false
    } else if w.vowel(i - 2) {
        false
    } else if !w.has(i - 1, 3, &["ACH"]) {
        false
    } else {
        let c = w.at(i + 2);
        (c != b'I' && c != b'E') || w.has(i - 2, 6, &["BACHER", "MACHER"])
    }
}

fn handle_c(w: &Word, out: &mut Vec<u8>, i: isize) -> isize {
    if condition_c0(w, i) {
        out.push(b'K');
        i + 2
    } else if i == 0 && w.has(i, 6, &["CAESAR"]) {
        out.push(b'S');
        i + 2
    } else if w.has(i, 2, &["CH"]) {
        handle_ch(w, out, i)
    } else if w.has(i, 2, &["CZ"]) && !w.has(i - 2, 4, &["WICZ"]) {
        out.push(b'S');
        i + 2
    } else if w.has(i + 1, 3, &["CIA"]) {
        out.push(b'X');
        i + 3
    } else if w.has(i, 2, &["CC"]) && !(i == 1 && w.at(0) == b'M') {
        handle_cc(w, out, i)
    } else if w.has(i, 2, &["CK", "CG", "CQ"]) {
        out.push(b'K');
        i + 2
    } else if w.has(i, 2, &["CI", "CE", "CY"]) {
        out.push(b'S');
        i + 2
    } else {
        out.push(b'K');
        if w.has(i + 1, 2, &[" C", " Q", " G"]) {
            i + 3
        } else if w.has(i + 1, 1, &["C", "K", "Q"]) && !w.has(i + 1, 2, &["CE", "CI"]) {
            i + 2
        } else {
            i + 1
        }
    }
}

fn handle_cc(w: &Word, out: &mut Vec<u8>, i: isize) -> isize {
    if w.has(i + 2, 1, &["I", "E", "H"]) && !w.has(i + 2, 2, &["HU"]) {
        if (i == 1 && w.at(i - 1) == b'A') || w.has(i - 1, 5, &["UCCEE", "UCCES"]) {
            out.extend_from_slice(b"KS");
        } else {
            out.push(b'X');
        }
        i + 3
    } else {
        out.push(b'K');
        i + 2
    }
}

fn handle_ch(w: &Word, out: &mut Vec<u8>, i: isize) -> isize {
    if i > 0 && w.has(i, 4, &["CHAE"]) {
        out.push(b'K');
        i + 2
    } else if condition_ch0(w, i) || condition_ch1(w, i) {
        out.push(b'K');
        i + 2
    } else {
        if i > 0 {
            if w.has(0, 2, &["MC"]) {
                out.push(b'K');
            } else {
                out.push(b'X');
            }
        } else {
            out.push(b'X');
        }
        i + 2
    }
}

fn condition_ch0(w: &Word, i: isize) -> bool {
    if i != 0 {
        false
    } else if !w.has(i + 1, 5, &["HARAC", "HARIS"])
        && !w.has(i + 1, 3, &["HOR", "HYM", "HIA", "HEM"])
    {
        false
    } else {
        !w.has(0, 5, &["CHORE"])
    }
}

fn condition_ch1(w: &Word, i: isize) -> bool {
    w.has(0, 4, &["VAN ", "VON "])
        || w.has(0, 3, &["SCH"])
        || w.has(i - 2, 6, &["ORCHES", "ARCHIT", "ORCHID"])
        || w.has(i + 2, 1, &["T", "S"])
        || ((w.has(i - 1, 1, &["A", "O", "U", "E"]) || i == 0)
            && (w.has(
                i + 2,
                1,
                &["L", "R", "N", "M", "B", "H", "F", "V", "W", " "],
            ) || i + 1 == w.last()))
}

fn handle_d(w: &Word, out: &mut Vec<u8>, i: isize) -> isize {
    if w.has(i, 2, &["DG"]) {
        if w.has(i + 2, 1, &["I", "E", "Y"]) {
            out.push(b'J');
            i + 3
        } else {
            out.extend_from_slice(b"TK");
            i + 2
        }
    } else if w.has(i, 2, &["DT", "DD"]) {
        out.push(b'T');
        i + 2
    } else {
        out.push(b'T');
        i + 1
    }
}

fn handle_g(w: &Word, out: &mut Vec<u8>, i: isize, slavo: bool) -> isize {
    if w.at(i + 1) == b'H' {
        handle_gh(w, out, i)
    } else if w.at(i + 1) == b'N' {
        if i == 1 && w.vowel(0) && !slavo {
            out.extend_from_slice(b"KN");
        } else if !w.has(i + 2, 2, &["EY"]) && w.at(i + 1) != b'Y' && !slavo {
            out.push(b'N');
        } else {
            out.extend_from_slice(b"KN");
        }
        i + 2
    } else if w.has(i + 1, 2, &["LI"]) && !slavo {
        out.extend_from_slice(b"KL");
        i + 2
    } else if i == 0
        && (w.at(i + 1) == b'Y'
            || w.has(
                i + 1,
                2,
                &[
                    "ES", "EP", "EB", "EL", "EY", "IB", "IL", "IN", "IE", "EI", "ER",
                ],
            ))
    {
        out.push(b'K');
        i + 2
    } else if (w.has(i + 1, 2, &["ER"]) || w.at(i + 1) == b'Y')
        && !w.has(0, 6, &["DANGER", "RANGER", "MANGER"])
        && !w.has(i - 1, 1, &["E", "I"])
        && !w.has(i - 1, 3, &["RGY", "OGY"])
    {
        out.push(b'K');
        i + 2
    } else if w.has(i + 1, 1, &["E", "I", "Y"]) || w.has(i - 1, 4, &["AGGI", "OGGI"]) {
        if w.has(0, 4, &["VAN ", "VON "]) || w.has(0, 3, &["SCH"]) || w.has(i + 1, 2, &["ET"]) {
            out.push(b'K');
        } else {
            out.push(b'J');
        }
        i + 2
    } else if w.at(i + 1) == b'G' {
        out.push(b'K');
        i + 2
    } else {
        out.push(b'K');
        i + 1
    }
}

fn handle_gh(w: &Word, out: &mut Vec<u8>, i: isize) -> isize {
    if i > 0 && !w.vowel(i - 1) {
        out.push(b'K');
        i + 2
    } else if i == 0 {
        if w.at(i + 2) == b'I' {
            out.push(b'J');
        } else {
            out.push(b'K');
        }
        i + 2
    } else if (i > 1 && w.has(i - 2, 1, &["B", "H", "D"]))
        || (i > 2 && w.has(i - 3, 1, &["B", "H", "D"]))
        || (i > 3 && w.has(i - 4, 1, &["B", "H"]))
    {
        i + 2
    } else {
        if i > 2 && w.at(i - 1) == b'U' && w.has(i - 3, 1, &["C", "G", "L", "R", "T"]) {
            out.push(b'F');
        } else if i > 0 && w.at(i - 1) != b'I' {
            out.push(b'K');
        }
        i + 2
    }
}

fn handle_j(w: &Word, out: &mut Vec<u8>, i: isize, slavo: bool) -> isize {
    if w.has(i, 4, &["JOSE"]) || w.has(0, 4, &["SAN "]) {
        if (i == 0 && w.at(i + 4) == b' ') || w.v.len() == 4 || w.has(0, 4, &["SAN "]) {
            out.push(b'H');
        } else {
            out.push(b'J');
        }
        i + 1
    } else {
        if i == 0 && !w.has(i, 4, &["JOSE"]) {
            out.push(b'J');
        } else if w.vowel(i - 1) && !slavo && (w.at(i + 1) == b'A' || w.at(i + 1) == b'O') {
            out.push(b'J');
        } else if i == w.last() {
            out.push(b'J');
        } else if !w.has(i + 1, 1, &["L", "T", "K", "S", "N", "M", "B", "Z"])
            && !w.has(i - 1, 1, &["S", "K", "L"])
        {
            out.push(b'J');
        }
        if w.at(i + 1) == b'J' { i + 2 } else { i + 1 }
    }
}

fn handle_s(w: &Word, out: &mut Vec<u8>, i: isize) -> isize {
    if w.has(i - 1, 3, &["ISL", "YSL"]) {
        i + 1
    } else if i == 0 && w.has(i, 5, &["SUGAR"]) {
        out.push(b'X');
        i + 1
    } else if w.has(i, 2, &["SH"]) {
        if w.has(i + 1, 4, &["HEIM", "HOEK", "HOLM", "HOLZ"]) {
            out.push(b'S');
        } else {
            out.push(b'X');
        }
        i + 2
    } else if w.has(i, 3, &["SIO", "SIA"]) || w.has(i, 4, &["SIAN"]) {
        out.push(b'S');
        i + 3
    } else if (i == 0 && w.has(i + 1, 1, &["M", "N", "L", "W"])) || w.has(i + 1, 1, &["Z"]) {
        out.push(b'S');
        i + if w.has(i + 1, 1, &["Z"]) { 2 } else { 1 }
    } else if w.has(i, 2, &["SC"]) {
        handle_sc(w, out, i)
    } else {
        // French (`resnais`, `artois`): the alternate only.
        if !(i == w.last() && w.has(i - 2, 2, &["AI", "OI"])) {
            out.push(b'S');
        }
        i + if w.has(i + 1, 1, &["S", "Z"]) { 2 } else { 1 }
    }
}

fn handle_sc(w: &Word, out: &mut Vec<u8>, i: isize) -> isize {
    if w.at(i + 2) == b'H' {
        if w.has(i + 3, 2, &["OO", "ER", "EN", "UY", "ED", "EM"]) {
            if w.has(i + 3, 2, &["ER", "EN"]) {
                out.push(b'X');
            } else {
                out.extend_from_slice(b"SK");
            }
        } else {
            out.push(b'X');
        }
    } else if w.has(i + 2, 1, &["I", "E", "Y"]) {
        out.push(b'S');
    } else {
        out.extend_from_slice(b"SK");
    }
    i + 3
}

fn handle_t(w: &Word, out: &mut Vec<u8>, i: isize) -> isize {
    if w.has(i, 4, &["TION"]) {
        out.push(b'X');
        i + 3
    } else if w.has(i, 3, &["TIA", "TCH"]) {
        out.push(b'X');
        i + 3
    } else if w.has(i, 2, &["TH"]) || w.has(i, 3, &["TTH"]) {
        if w.has(i + 2, 2, &["OM", "AM"]) || w.has(0, 4, &["VAN ", "VON "]) || w.has(0, 3, &["SCH"])
        {
            out.push(b'T');
        } else {
            out.push(b'0');
        }
        i + 2
    } else {
        out.push(b'T');
        i + if w.has(i + 1, 1, &["T", "D"]) { 2 } else { 1 }
    }
}

fn handle_w(w: &Word, out: &mut Vec<u8>, i: isize) -> isize {
    if w.has(i, 2, &["WR"]) {
        out.push(b'R');
        i + 2
    } else {
        // As Philips' C has it: a word starting with W and a vowel (or WH) gives
        // an A and goes on to the rules below from the same letter, so that
        // `witz` is `ATS`.
        if i == 0 && (w.vowel(i + 1) || w.has(i, 2, &["WH"])) {
            out.push(b'A');
        }
        if (i == w.last() && w.vowel(i - 1))
            || w.has(i - 1, 5, &["EWSKI", "EWSKY", "OWSKI", "OWSKY"])
            || w.has(0, 3, &["SCH"])
        {
            i + 1
        } else if w.has(i, 4, &["WICZ", "WITZ"]) {
            out.extend_from_slice(b"TS");
            i + 4
        } else {
            i + 1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The codes Redis 8.10.2 gave for these words (`FT.EXPLAIN` of a query
    /// on a `PHONETIC` field).
    #[test]
    fn codes_are_those_redis_gives() {
        for (word, code) in [
            ("john", "JN"),
            ("jon", "JN"),
            ("smith", "SM0"),
            ("schmidt", "XMT"),
            ("thomas", "TMS"),
            ("knight", "NT"),
            ("wright", "RT"),
            ("xavier", "SF"),
            ("jose", "HS"),
            ("cohen", "KHN"),
            ("caesar", "SSR"),
            ("chemistry", "KMST"),
            ("michael", "MKL"),
            ("bough", "P"),
            ("thumb", "0M"),
            ("laugh", "LF"),
            ("zhang", "JNK"),
            ("pneumonia", "NMN"),
            ("witz", "ATS"),
            ("christopher", "KRST"),
            ("geoffrey", "JFR"),
            ("abc", "APK"),
            ("aeiou", "A"),
            ("j0hn", "JN"),
            ("gonçalves", "KNLF"),
            ("muñoz", "MS"),
        ] {
            assert_eq!(double_metaphone(word), code, "{word}");
        }
        assert_eq!(term_of("ab", 3), None);
        assert_eq!(term_of("john", 3).as_deref(), Some("<JN"));
        assert_eq!(term_of("123", 3), None);
    }
}
