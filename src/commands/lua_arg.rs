//! The implicit `arg` table of Lua 5.1's vararg functions (#820).
//!
//! Redis's Lua 5.1 is built with `LUA_COMPAT_VARARG`: a function declared
//! with `...` has a local named `arg`. When the function's body never uses
//! `...`, `arg` is a table of the extra arguments with their count in `n`;
//! when it does, `arg` is `nil`. Older scripts read their arguments that way:
//!
//! ```lua
//! local function join(...)
//!   local r = ""
//!   for _, v in ipairs(arg) do r = r .. " " .. tostring(v) end
//!   return r
//! end
//! ```
//!
//! Lua 5.4 has no such local, so such a script fails with "attempted to
//! access nonexistent global variable 'arg'". This adds the local to the
//! source before it is compiled, at the start of each vararg function whose
//! text mentions `arg`, on its own line (no line is added, so error line
//! numbers stay what they were). Nothing is added to a script that does not
//! mention `arg` inside a vararg function, which is nearly every script, and
//! nothing is done when a script runs: the compiled function is cached as
//! before, and the cost is one scan of the text when it is compiled.
//!
//! The scan knows only as much of Lua as it needs: strings, comments, long
//! brackets, the words that open and close a block, and the parameter list
//! of each function. A script it cannot follow (a string left open) is
//! returned as it was, for the compiler to report.

use std::borrow::Cow;

/// What goes at the start of a vararg function that uses `...` itself:
/// `arg` is a local, and nil.
const ARG_NIL: &str = " local arg;";
/// What goes at the start of one that does not: the extra arguments and their
/// count, as Lua 5.1 builds it.
const ARG_TABLE: &str = " local arg={n=select('#',...),...};";

/// `source`, with `arg` defined in the vararg functions that mention it.
pub(super) fn with_implicit_arg(source: &str) -> Cow<'_, str> {
    // Nearly every script is settled here: it has no vararg function, or
    // never says `arg`.
    if !source.contains("...") || !source.contains("arg") {
        return Cow::Borrowed(source);
    }
    match insertions(source) {
        Some(edits) if !edits.is_empty() => {
            let mut out = String::with_capacity(
                source.len() + edits.iter().map(|(_, text)| text.len()).sum::<usize>(),
            );
            let mut from = 0;
            for (at, text) in edits {
                out.push_str(&source[from..at]);
                out.push_str(text);
                from = at;
            }
            out.push_str(&source[from..]);
            Cow::Owned(out)
        }
        _ => Cow::Borrowed(source),
    }
}

#[derive(PartialEq, Clone, Copy)]
enum Token<'a> {
    Name(&'a str),
    /// `...`
    Dots,
    /// Any other single character that is not part of a name, number,
    /// string or comment.
    Punct(u8),
    /// A number or a string.
    Literal,
}

struct Lexer<'a> {
    src: &'a str,
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Lexer<'a> {
    /// The length of a long bracket opening at `at` (`[==[`), as the number of
    /// `=` in it, if there is one.
    fn long_bracket(&self, at: usize) -> Option<usize> {
        if self.bytes.get(at) != Some(&b'[') {
            return None;
        }
        let mut level = 0;
        while self.bytes.get(at + 1 + level) == Some(&b'=') {
            level += 1;
        }
        (self.bytes.get(at + 1 + level) == Some(&b'[')).then_some(level)
    }

    /// Past the end of the long bracket of `level` that starts at `at`;
    /// `None` if it is never closed.
    fn skip_long(&self, at: usize, level: usize) -> Option<usize> {
        let mut closing = vec![b']'];
        closing.extend(std::iter::repeat_n(b'=', level));
        closing.push(b']');
        let from = at + level + 2;
        self.bytes[from..]
            .windows(closing.len())
            .position(|w| w == closing.as_slice())
            .map(|p| from + p + closing.len())
    }

    /// The next token and where it starts; `Err` for text this cannot follow,
    /// `Ok(None)` at the end.
    fn next(&mut self) -> Result<Option<(usize, Token<'a>)>, ()> {
        loop {
            let Some(&b) = self.bytes.get(self.at) else {
                return Ok(None);
            };
            let start = self.at;
            match b {
                b' ' | b'\t' | b'\r' | b'\n' | 0x0b | 0x0c => self.at += 1,
                b'-' if self.bytes.get(self.at + 1) == Some(&b'-') => {
                    self.at += 2;
                    if let Some(level) = self.long_bracket(self.at) {
                        self.at = self.skip_long(self.at, level).ok_or(())?;
                    } else {
                        while self.bytes.get(self.at).is_some_and(|&c| c != b'\n') {
                            self.at += 1;
                        }
                    }
                }
                b'"' | b'\'' => {
                    self.at += 1;
                    loop {
                        match self.bytes.get(self.at) {
                            None | Some(b'\n') => return Err(()),
                            Some(b'\\') => self.at += 2,
                            Some(&c) if c == b => {
                                self.at += 1;
                                break;
                            }
                            Some(_) => self.at += 1,
                        }
                    }
                    return Ok(Some((start, Token::Literal)));
                }
                b'[' if self.long_bracket(self.at).is_some() => {
                    let level = self.long_bracket(self.at).ok_or(())?;
                    self.at = self.skip_long(self.at, level).ok_or(())?;
                    return Ok(Some((start, Token::Literal)));
                }
                b'0'..=b'9' => {
                    self.number();
                    return Ok(Some((start, Token::Literal)));
                }
                b'.' => {
                    if self.bytes[self.at..].starts_with(b"...") {
                        self.at += 3;
                        return Ok(Some((start, Token::Dots)));
                    }
                    if self.bytes[self.at..].starts_with(b"..") {
                        // Concatenation: not a `.` that selects a field.
                        self.at += 2;
                        return Ok(Some((start, Token::Punct(b'+'))));
                    }
                    if self.bytes.get(self.at + 1).is_some_and(u8::is_ascii_digit) {
                        self.number();
                        return Ok(Some((start, Token::Literal)));
                    }
                    self.at += 1;
                    return Ok(Some((start, Token::Punct(b'.'))));
                }
                b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
                    while self
                        .bytes
                        .get(self.at)
                        .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_')
                    {
                        self.at += 1;
                    }
                    return Ok(Some((start, Token::Name(&self.src[start..self.at]))));
                }
                _ => {
                    self.at += 1;
                    return Ok(Some((start, Token::Punct(b))));
                }
            }
        }
    }

    /// Past a number: digits, letters, dots, and a sign after an exponent.
    fn number(&mut self) {
        while let Some(&c) = self.bytes.get(self.at) {
            let exponent = matches!(c, b'e' | b'E' | b'p' | b'P');
            self.at += 1;
            if exponent && matches!(self.bytes.get(self.at), Some(b'+' | b'-')) {
                self.at += 1;
            }
            if !(c.is_ascii_alphanumeric() || c == b'.' || c == b'_') {
                self.at -= 1;
                break;
            }
        }
    }
}

/// A function being read.
struct Frame {
    /// Declared with `...`.
    vararg: bool,
    /// Its body uses `...` itself.
    uses_dots: bool,
    /// `arg` is said in its body, or in a function inside it.
    uses_arg: bool,
    /// Where its body starts: after the `)` of its parameters.
    body_at: usize,
}

/// What an `end` closes.
enum Block {
    Function(Frame),
    /// `if` or `do` (a `while` or `for` has its `do`).
    Other,
}

/// The places to add `arg`, in order, with what to add. `None` for text that
/// could not be followed.
fn insertions(source: &str) -> Option<Vec<(usize, &'static str)>> {
    let mut lex = Lexer {
        src: source,
        bytes: source.as_bytes(),
        at: 0,
    };
    let mut blocks: Vec<Block> = Vec::new();
    let mut edits: Vec<(usize, &'static str)> = Vec::new();
    // The token before this one, to tell a field `t.arg` from the name.
    let mut before = Token::Punct(b' ');
    while let Some((_, token)) = lex.next().ok()? {
        match token {
            Token::Name("function") => {
                // A name or nothing, then the parameters.
                let mut vararg = false;
                loop {
                    match lex.next().ok()?? {
                        (_, Token::Punct(b'(')) => break,
                        (_, Token::Name(_) | Token::Punct(b'.' | b':')) => {}
                        _ => return None,
                    }
                }
                loop {
                    match lex.next().ok()?? {
                        (_, Token::Punct(b')')) => break,
                        (_, Token::Dots) => vararg = true,
                        (_, Token::Name(_) | Token::Punct(b',')) => {}
                        _ => return None,
                    }
                }
                blocks.push(Block::Function(Frame {
                    vararg,
                    uses_dots: false,
                    uses_arg: false,
                    body_at: lex.at,
                }));
                before = Token::Punct(b')');
                continue;
            }
            Token::Name("if" | "do") => blocks.push(Block::Other),
            Token::Name("end") => {
                if let Block::Function(frame) = blocks.pop()?
                    && frame.vararg
                    && frame.uses_arg
                {
                    edits.push((
                        frame.body_at,
                        if frame.uses_dots { ARG_NIL } else { ARG_TABLE },
                    ));
                }
            }
            Token::Dots => {
                if let Some(frame) = innermost(&mut blocks) {
                    frame.uses_dots = true;
                }
            }
            Token::Name("arg") if !matches!(before, Token::Punct(b'.' | b':')) => {
                for block in blocks.iter_mut() {
                    if let Block::Function(frame) = block {
                        frame.uses_arg = true;
                    }
                }
            }
            _ => {}
        }
        before = token;
    }
    // A function never closed is left for the compiler to refuse.
    blocks.is_empty().then_some(edits).map(|mut edits| {
        edits.sort_by_key(|(at, _)| *at);
        edits
    })
}

/// The function being read, if any.
fn innermost(blocks: &mut [Block]) -> Option<&mut Frame> {
    blocks.iter_mut().rev().find_map(|block| match block {
        Block::Function(frame) => Some(frame),
        Block::Other => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rewrite(source: &str) -> String {
        with_implicit_arg(source).into_owned()
    }

    #[test]
    fn a_script_without_arg_in_a_vararg_function_is_returned_as_it_was() {
        for source in [
            "return 1",
            "return redis.call('GET', KEYS[1])",
            "local function f(...) return select('#', ...) end return f(1)",
            "local args = {} local function f(a) return arg end return 1",
            "return ARGV[1]",
        ] {
            assert!(
                matches!(with_implicit_arg(source), Cow::Borrowed(_)),
                "{source}"
            );
        }
        // Said in a function that is not vararg, nor inside one: not ours.
        let source = "local function f(a) return arg end return 1";
        assert!(matches!(with_implicit_arg(source), Cow::Borrowed(_)));
    }

    #[test]
    fn a_vararg_function_that_never_says_dots_gets_the_table() {
        let out = rewrite("local function f(...) return #arg end return f(1, 2)");
        assert_eq!(
            out,
            "local function f(...) local arg={n=select('#',...),...}; return #arg end return f(1, 2)"
        );
    }

    #[test]
    fn a_vararg_function_that_says_dots_has_a_nil_arg() {
        let out = rewrite("local function f(...) local t = {...} return arg end");
        assert!(out.contains("(...) local arg; local t"), "{out}");
    }

    #[test]
    fn a_function_inside_one_sees_the_outer_arg() {
        let out = rewrite("local function f(...) return function() return arg.n end end");
        assert!(out.contains("f(...) local arg={n="), "{out}");
        // The inner one is not vararg: nothing added to it.
        assert_eq!(out.matches("local arg").count(), 1, "{out}");
    }

    #[test]
    fn fields_strings_comments_and_names_are_not_arg() {
        for source in [
            "local function f(...) return t.arg, t:arg(), 'arg', \"arg\", [[arg]] end",
            "local function f(...) -- arg\n return 1 end",
            "local function f(...) --[[ arg ]] return 1 end",
            "local function f(...) return args, argv, margin, arg_1 end",
        ] {
            assert!(
                matches!(with_implicit_arg(source), Cow::Borrowed(_)),
                "{source}"
            );
        }
    }

    #[test]
    fn blocks_and_methods_are_followed() {
        let source = "\
local t = {}
function t.a.b:c(x, ...)
  for i = 1, 2 do
    if x then
      while x do x = nil end
    elseif i then
      repeat x = 1 until x
    end
  end
  return arg
end
return t";
        let out = rewrite(source);
        // One edit, on the same line as the parameters, so no line moves.
        assert_eq!(out.lines().count(), source.lines().count());
        assert!(
            out.contains("function t.a.b:c(x, ...) local arg={n=select('#',...),...};\n"),
            "{out}"
        );
    }

    #[test]
    fn text_that_cannot_be_followed_is_left_for_the_compiler() {
        for source in [
            "local function f(...) return arg end local s = 'open",
            "local function f(...) return arg",
            "local function f(...) return arg end end",
            "local function f(...) return arg end --[[ open",
        ] {
            assert!(
                matches!(with_implicit_arg(source), Cow::Borrowed(_)),
                "{source}"
            );
        }
    }
}
