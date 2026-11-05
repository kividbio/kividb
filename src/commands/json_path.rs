//! JSONPath, as the JSON commands understand it.
//!
//! A path is parsed once into a [`Query`] and then evaluated against a
//! document. A query is one of two things:
//!
//! * a **selection** — a plain path such as `$.a[*].b`, `$..price` or
//!   `$.items[?(@.price < 10)]`. It names nodes of the document, so besides
//!   their values it can report *where* they are ([`Query::locations`]), which
//!   is what the commands that change the document work from.
//! * a **projection** — anything that computes a value instead: `$.a + 1`,
//!   `$.arr.length()`, `length($.arr)`, `$.obj~`. It has values but no
//!   locations.
//!
//! Supported, following the Redis documentation for the JSON data type up to
//! Redis 8.10:
//!
//! ```text
//! $  @                    the document root, and (in a filter) the current node
//! .name  ['name']  [0]    a member, an element (negative indexes count from the end)
//! .*  [*]                 every child
//! ..name  ..*  ..[…]      the same, at any depth
//! [a,b]  [start:end:step] several selectors at once; an array slice
//! [?expr]  [?(expr)]      the children for which `expr` holds
//! path~                   the member names of the objects matched (terminal)
//!
//! ==  !=  <  <=  >  >=  =~        comparison (`=~` is a regular expression search)
//! &&  ||  !  ( )                  logic
//! +  -  *  /  %                   arithmetic, surrounded by spaces
//! in  nin                         membership in an array
//! subsetof  anyof  noneof         relations between two arrays
//! size  sizeof  empty             the size of a string, array or object
//!
//! length() count() value() keys() match() search() concat() abs() ceiling()
//! floor() first() last() index() min() max() avg() sum() stddev() append()
//! subsetof() anyof() noneof()     as `f(x, …)` or as `x.f(…)`
//! ```
//!
//! Evaluation never fails. An operand that cannot be evaluated — a type that
//! does not fit, a division by zero, an index out of range, the wrong number
//! of arguments — is *Nothing*: a filter condition built on it is false, and a
//! projection that comes to it yields no values.
//!
//! A path that does not start with `$` is a legacy path (`.a.b`, `a[0]`,
//! `["a"]`): the same syntax rooted at the document, for which the commands
//! report the first match instead of all of them.

use serde_json::{Number, Value};
use std::borrow::Cow;

// ── Locations ────────────────────────────────────────────────────────────────

/// One step from a node to one of its children.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Step {
    Key(String),
    Index(usize),
}

/// Where a node is: the steps that lead to it from the root.
pub type Location = Vec<Step>;

pub fn get<'a>(root: &'a Value, location: &[Step]) -> Option<&'a Value> {
    let mut node = root;
    for step in location {
        node = match step {
            Step::Key(key) => node.as_object()?.get(key)?,
            Step::Index(index) => node.as_array()?.get(*index)?,
        };
    }
    Some(node)
}

pub fn get_mut<'a>(root: &'a mut Value, location: &[Step]) -> Option<&'a mut Value> {
    let mut node = root;
    for step in location {
        node = match step {
            Step::Key(key) => node.as_object_mut()?.get_mut(key)?,
            Step::Index(index) => node.as_array_mut()?.get_mut(*index)?,
        };
    }
    Some(node)
}

// ── Syntax tree ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum Selector {
    Name(String),
    Index(i64),
    Slice(Option<i64>, Option<i64>, Option<i64>),
    Wildcard,
    Filter(Box<Expr>),
}

#[derive(Debug, Clone)]
struct Segment {
    /// `..` — applies to the node and to everything beneath it.
    descendant: bool,
    selectors: Vec<Selector>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Origin {
    Root,
    Current,
}

#[derive(Debug, Clone)]
struct Path {
    origin: Origin,
    segments: Vec<Segment>,
}

impl Path {
    /// A path that can match at most one node: names and indexes only. Its
    /// result is that node's value; any other path's result is a list of
    /// nodes, however many it happens to hold.
    fn is_singular(&self) -> bool {
        self.segments.iter().all(|segment| {
            !segment.descendant
                && matches!(
                    segment.selectors.as_slice(),
                    [Selector::Name(_)] | [Selector::Index(_)]
                )
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Regex,
    In,
    Nin,
    SubsetOf,
    AnyOf,
    NoneOf,
    SizeOf,
    Empty,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Func {
    Length,
    Count,
    Value,
    Keys,
    Match,
    Search,
    Concat,
    Abs,
    Ceiling,
    Floor,
    First,
    Last,
    Index,
    Min,
    Max,
    Avg,
    Sum,
    Stddev,
    Append,
    SubsetOf,
    AnyOf,
    NoneOf,
}

impl Func {
    fn named(name: &str) -> Option<Func> {
        Some(match name {
            "length" => Func::Length,
            "count" => Func::Count,
            "value" => Func::Value,
            "keys" => Func::Keys,
            "match" => Func::Match,
            "search" => Func::Search,
            "concat" => Func::Concat,
            "abs" => Func::Abs,
            "ceiling" => Func::Ceiling,
            "floor" => Func::Floor,
            "first" => Func::First,
            "last" => Func::Last,
            "index" => Func::Index,
            "min" => Func::Min,
            "max" => Func::Max,
            "avg" => Func::Avg,
            "sum" => Func::Sum,
            "stddev" => Func::Stddev,
            "append" => Func::Append,
            "subsetof" => Func::SubsetOf,
            "anyof" => Func::AnyOf,
            "noneof" => Func::NoneOf,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone)]
enum Expr {
    Literal(Value),
    Path(Path),
    /// `path~`
    Keys(Box<Expr>),
    Call(Func, Vec<Expr>),
    Negate(Box<Expr>),
    Plus(Box<Expr>),
    Arith(ArithOp, Box<Expr>, Box<Expr>),
    Compare(CmpOp, Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
}

// ── Parser ───────────────────────────────────────────────────────────────────

struct Parser {
    chars: Vec<char>,
    pos: usize,
    /// How deep in parentheses, operators and literals the parse is.
    depth: u32,
    /// How many terms have been read.
    terms: usize,
}

/// The deepest a path's expressions may nest, and the most terms they may
/// have. Parsed and evaluated by calls that go as deep as the tree, which for
/// `a && b && c` is as deep as it is long: without a limit, a path from a
/// client would run the thread out of stack, which takes the server down.
const DEPTH_MAX: u32 = 64;
const TERMS_MAX: usize = 1024;

type Parsed<T> = Result<T, String>;

/// A character that ends a member name written in dot notation. Everything
/// else is part of the name — including `+`, `-`, `/` and `%`, which is why
/// arithmetic operators have to be surrounded by spaces.
fn ends_name(c: char) -> bool {
    c.is_whitespace()
        || matches!(
            c,
            '.' | '['
                | ']'
                | '('
                | ')'
                | ','
                | '='
                | '!'
                | '<'
                | '>'
                | '&'
                | '|'
                | '*'
                | '~'
                | '\''
                | '"'
                | '?'
                | '{'
                | '}'
        )
}

fn is_word(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

impl Parser {
    fn new(text: &str) -> Self {
        Parser {
            chars: text.chars().collect(),
            pos: 0,
            depth: 0,
            terms: 0,
        }
    }

    /// `parse`, one level deeper, as one more term.
    fn nested<T>(&mut self, parse: impl FnOnce(&mut Self) -> Parsed<T>) -> Parsed<T> {
        self.terms += 1;
        if self.depth >= DEPTH_MAX || self.terms > TERMS_MAX {
            return self.fail("an expression too long or nested too deep");
        }
        self.depth += 1;
        let parsed = parse(self);
        self.depth -= 1;
        parsed
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn peek_at(&self, ahead: usize) -> Option<char> {
        self.chars.get(self.pos + ahead).copied()
    }

    fn eat(&mut self, c: char) -> bool {
        if self.peek() == Some(c) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn looking_at(&self, text: &str) -> bool {
        text.chars()
            .enumerate()
            .all(|(i, c)| self.peek_at(i) == Some(c))
    }

    fn eat_str(&mut self, text: &str) -> bool {
        if self.looking_at(text) {
            self.pos += text.chars().count();
            true
        } else {
            false
        }
    }

    /// `word`, as a whole word.
    fn eat_word(&mut self, word: &str) -> bool {
        if self.looking_at(word) && !self.peek_at(word.len()).is_some_and(is_word) {
            self.pos += word.len();
            true
        } else {
            false
        }
    }

    fn skip_ws(&mut self) {
        while self.peek().is_some_and(char::is_whitespace) {
            self.pos += 1;
        }
    }

    fn fail<T>(&self, what: &str) -> Parsed<T> {
        Err(format!("invalid path: {what} at position {}", self.pos))
    }

    fn expect(&mut self, c: char) -> Parsed<()> {
        if self.eat(c) {
            Ok(())
        } else {
            self.fail(&format!("expected '{c}'"))
        }
    }

    // expression := or
    fn expression(&mut self) -> Parsed<Expr> {
        let mut left = self.conjunction()?;
        loop {
            self.skip_ws();
            if !self.eat_str("||") {
                return Ok(left);
            }
            left = Expr::Or(Box::new(left), Box::new(self.conjunction()?));
        }
    }

    fn conjunction(&mut self) -> Parsed<Expr> {
        let mut left = self.negation()?;
        loop {
            self.skip_ws();
            if !self.eat_str("&&") {
                return Ok(left);
            }
            left = Expr::And(Box::new(left), Box::new(self.negation()?));
        }
    }

    /// `!` binds tighter than `&&` and `||`, and applies to the whole
    /// comparison that follows it.
    fn negation(&mut self) -> Parsed<Expr> {
        self.skip_ws();
        if self.peek() == Some('!') && self.peek_at(1) != Some('=') {
            self.pos += 1;
            return Ok(Expr::Not(Box::new(self.nested(Self::negation)?)));
        }
        self.comparison()
    }

    fn comparison(&mut self) -> Parsed<Expr> {
        let left = self.sum()?;
        self.skip_ws();
        let Some(op) = self.comparison_operator() else {
            return Ok(left);
        };
        let right = self.sum()?;
        Ok(Expr::Compare(op, Box::new(left), Box::new(right)))
    }

    fn comparison_operator(&mut self) -> Option<CmpOp> {
        const SYMBOLS: [(&str, CmpOp); 7] = [
            ("==", CmpOp::Eq),
            ("!=", CmpOp::Ne),
            ("<=", CmpOp::Le),
            (">=", CmpOp::Ge),
            ("=~", CmpOp::Regex),
            ("<", CmpOp::Lt),
            (">", CmpOp::Gt),
        ];
        const WORDS: [(&str, CmpOp); 8] = [
            ("subsetof", CmpOp::SubsetOf),
            ("anyof", CmpOp::AnyOf),
            ("noneof", CmpOp::NoneOf),
            ("sizeof", CmpOp::SizeOf),
            ("size", CmpOp::SizeOf),
            ("empty", CmpOp::Empty),
            ("nin", CmpOp::Nin),
            ("in", CmpOp::In),
        ];
        for (symbol, op) in SYMBOLS {
            if self.eat_str(symbol) {
                return Some(op);
            }
        }
        for (word, op) in WORDS {
            if self.eat_word(word) {
                return Some(op);
            }
        }
        None
    }

    fn sum(&mut self) -> Parsed<Expr> {
        let mut left = self.product()?;
        loop {
            self.skip_ws();
            let op = match self.peek() {
                Some('+') => ArithOp::Add,
                Some('-') => ArithOp::Sub,
                _ => return Ok(left),
            };
            self.pos += 1;
            left = Expr::Arith(op, Box::new(left), Box::new(self.product()?));
        }
    }

    fn product(&mut self) -> Parsed<Expr> {
        let mut left = self.unary()?;
        loop {
            self.skip_ws();
            let op = match self.peek() {
                Some('*') => ArithOp::Mul,
                Some('/') => ArithOp::Div,
                Some('%') => ArithOp::Rem,
                _ => return Ok(left),
            };
            self.pos += 1;
            left = Expr::Arith(op, Box::new(left), Box::new(self.unary()?));
        }
    }

    fn unary(&mut self) -> Parsed<Expr> {
        self.skip_ws();
        if self.eat('-') {
            return Ok(Expr::Negate(Box::new(self.nested(Self::unary)?)));
        }
        if self.eat('+') {
            return Ok(Expr::Plus(Box::new(self.nested(Self::unary)?)));
        }
        self.primary()
    }

    fn primary(&mut self) -> Parsed<Expr> {
        self.nested(Self::primary_term)
    }

    fn primary_term(&mut self) -> Parsed<Expr> {
        self.skip_ws();
        match self.peek() {
            Some('(') => {
                self.pos += 1;
                let inner = self.expression()?;
                self.skip_ws();
                self.expect(')')?;
                Ok(inner)
            }
            Some('$') | Some('@') => self.path(),
            Some('"') | Some('\'') | Some('[') | Some('{') => Ok(Expr::Literal(self.literal()?)),
            Some(c) if c.is_ascii_digit() => Ok(Expr::Literal(self.literal()?)),
            Some(c) if c.is_ascii_alphabetic() => {
                for (word, value) in [
                    ("true", Value::Bool(true)),
                    ("false", Value::Bool(false)),
                    ("null", Value::Null),
                ] {
                    if self.eat_word(word) {
                        return Ok(Expr::Literal(value));
                    }
                }
                let start = self.pos;
                while self.peek().is_some_and(is_word) {
                    self.pos += 1;
                }
                let name: String = self.chars[start..self.pos].iter().collect();
                if self.peek() != Some('(') {
                    return self.fail(&format!("unexpected '{name}'"));
                }
                let Some(func) = Func::named(&name) else {
                    return self.fail(&format!("unknown function '{name}'"));
                };
                let args = self.arguments()?;
                self.methods(Expr::Call(func, args))
            }
            Some(c) => self.fail(&format!("unexpected '{c}'")),
            None => self.fail("unexpected end of path"),
        }
    }

    /// `( expr, expr, … )`
    fn arguments(&mut self) -> Parsed<Vec<Expr>> {
        self.expect('(')?;
        let mut args = Vec::new();
        self.skip_ws();
        if self.eat(')') {
            return Ok(args);
        }
        loop {
            args.push(self.expression()?);
            self.skip_ws();
            if self.eat(')') {
                return Ok(args);
            }
            self.expect(',')?;
        }
    }

    /// `.name(…)` any number of times after a value: each call takes what is
    /// to its left as its first argument.
    fn methods(&mut self, mut receiver: Expr) -> Parsed<Expr> {
        loop {
            match self.peek() {
                Some('.') => {
                    self.pos += 1;
                    let name = self.name();
                    let func = Func::named(&name);
                    match (func, self.peek()) {
                        (Some(func), Some('(')) => {
                            let mut args = vec![receiver];
                            args.extend(self.arguments()?);
                            receiver = Expr::Call(func, args);
                        }
                        _ => return self.fail("only a function call can follow a function call"),
                    }
                }
                Some('~') | Some('[') => {
                    return self.fail("a function call cannot be followed by a selector");
                }
                _ => return Ok(receiver),
            }
        }
    }

    fn name(&mut self) -> String {
        let start = self.pos;
        while self.peek().is_some_and(|c| !ends_name(c)) {
            self.pos += 1;
        }
        self.chars[start..self.pos].iter().collect()
    }

    fn path(&mut self) -> Parsed<Expr> {
        let origin = if self.eat('$') {
            Origin::Root
        } else {
            self.expect('@')?;
            Origin::Current
        };
        let mut segments = Vec::new();
        loop {
            if self.looking_at("..") {
                self.pos += 2;
                let selectors = match self.peek() {
                    Some('[') => self.bracket()?,
                    Some('*') => {
                        self.pos += 1;
                        vec![Selector::Wildcard]
                    }
                    _ => {
                        let name = self.name();
                        if name.is_empty() {
                            return self.fail("expected a name after '..'");
                        }
                        vec![Selector::Name(name)]
                    }
                };
                segments.push(Segment {
                    descendant: true,
                    selectors,
                });
                continue;
            }
            match self.peek() {
                Some('.') => {
                    self.pos += 1;
                    if self.eat('*') {
                        segments.push(Segment {
                            descendant: false,
                            selectors: vec![Selector::Wildcard],
                        });
                        continue;
                    }
                    // `.[0]` and `.["a"]`, which the legacy syntax allows.
                    if self.peek() == Some('[') {
                        continue;
                    }
                    let name = self.name();
                    if name.is_empty() {
                        return self.fail("expected a name after '.'");
                    }
                    // A name followed by `(` is a function, called on the
                    // path so far.
                    if self.peek() == Some('(') {
                        let Some(func) = Func::named(&name) else {
                            return self.fail(&format!("unknown function '{name}'"));
                        };
                        let mut args = vec![Expr::Path(Path { origin, segments })];
                        args.extend(self.arguments()?);
                        return self.methods(Expr::Call(func, args));
                    }
                    segments.push(Segment {
                        descendant: false,
                        selectors: vec![Selector::Name(name)],
                    });
                }
                Some('[') => {
                    let selectors = self.bracket()?;
                    segments.push(Segment {
                        descendant: false,
                        selectors,
                    });
                }
                Some('~') => {
                    self.pos += 1;
                    if matches!(self.peek(), Some('.') | Some('[') | Some('~')) {
                        return self.fail("'~' must be the last thing in a path");
                    }
                    return Ok(Expr::Keys(Box::new(Expr::Path(Path { origin, segments }))));
                }
                _ => return Ok(Expr::Path(Path { origin, segments })),
            }
        }
    }

    /// `[selector, selector, …]`
    fn bracket(&mut self) -> Parsed<Vec<Selector>> {
        self.expect('[')?;
        let mut selectors = Vec::new();
        loop {
            self.skip_ws();
            let selector = match self.peek() {
                Some('*') => {
                    self.pos += 1;
                    Selector::Wildcard
                }
                Some('?') => {
                    self.pos += 1;
                    Selector::Filter(Box::new(self.expression()?))
                }
                Some('"') | Some('\'') => Selector::Name(self.string()?),
                Some(c) if c == '-' || c == ':' || c.is_ascii_digit() => self.index_or_slice()?,
                _ => return self.fail("expected a selector"),
            };
            selectors.push(selector);
            self.skip_ws();
            if self.eat(']') {
                return Ok(selectors);
            }
            self.expect(',')?;
        }
    }

    fn integer(&mut self) -> Parsed<Option<i64>> {
        self.skip_ws();
        let start = self.pos;
        if self.peek() == Some('-') {
            self.pos += 1;
        }
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            self.pos += 1;
        }
        if self.pos == start {
            return Ok(None);
        }
        let text: String = self.chars[start..self.pos].iter().collect();
        match text.parse() {
            Ok(n) => Ok(Some(n)),
            Err(_) => self.fail("index out of range"),
        }
    }

    fn index_or_slice(&mut self) -> Parsed<Selector> {
        let start = self.integer()?;
        self.skip_ws();
        if !self.eat(':') {
            return match start {
                Some(index) => Ok(Selector::Index(index)),
                None => self.fail("expected an index"),
            };
        }
        let end = self.integer()?;
        self.skip_ws();
        let step = if self.eat(':') { self.integer()? } else { None };
        Ok(Selector::Slice(start, end, step))
    }

    fn string(&mut self) -> Parsed<String> {
        let Some(quote) = self.peek().filter(|c| *c == '"' || *c == '\'') else {
            return self.fail("expected a string");
        };
        self.pos += 1;
        let mut out = String::new();
        loop {
            let Some(c) = self.peek() else {
                return self.fail("unterminated string");
            };
            self.pos += 1;
            if c == quote {
                return Ok(out);
            }
            if c != '\\' {
                out.push(c);
                continue;
            }
            let Some(escaped) = self.peek() else {
                return self.fail("unterminated string");
            };
            self.pos += 1;
            match escaped {
                'n' => out.push('\n'),
                't' => out.push('\t'),
                'r' => out.push('\r'),
                'b' => out.push('\u{8}'),
                'f' => out.push('\u{c}'),
                'u' => {
                    let hex: String = self.chars.iter().skip(self.pos).take(4).collect();
                    let code = u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32);
                    match code {
                        Some(c) if hex.len() == 4 => {
                            out.push(c);
                            self.pos += 4;
                        }
                        _ => return self.fail("invalid \\u escape"),
                    }
                }
                other => out.push(other),
            }
        }
    }

    /// A JSON value written in the path: a number, a string in either kind of
    /// quote, `true`, `false`, `null`, an array or an object.
    fn literal(&mut self) -> Parsed<Value> {
        self.nested(Self::literal_value)
    }

    fn literal_value(&mut self) -> Parsed<Value> {
        self.skip_ws();
        match self.peek() {
            Some('"') | Some('\'') => Ok(Value::String(self.string()?)),
            Some('[') => {
                self.pos += 1;
                let mut items = Vec::new();
                self.skip_ws();
                if self.eat(']') {
                    return Ok(Value::Array(items));
                }
                loop {
                    items.push(self.literal()?);
                    self.skip_ws();
                    if self.eat(']') {
                        return Ok(Value::Array(items));
                    }
                    self.expect(',')?;
                }
            }
            Some('{') => {
                self.pos += 1;
                let mut members = serde_json::Map::new();
                self.skip_ws();
                if self.eat('}') {
                    return Ok(Value::Object(members));
                }
                loop {
                    self.skip_ws();
                    let key = self.string()?;
                    self.skip_ws();
                    self.expect(':')?;
                    members.insert(key, self.literal()?);
                    self.skip_ws();
                    if self.eat('}') {
                        return Ok(Value::Object(members));
                    }
                    self.expect(',')?;
                }
            }
            Some(c) if c == '-' || c.is_ascii_digit() => self.number(),
            _ => {
                for (word, value) in [
                    ("true", Value::Bool(true)),
                    ("false", Value::Bool(false)),
                    ("null", Value::Null),
                ] {
                    if self.eat_word(word) {
                        return Ok(value);
                    }
                }
                self.fail("expected a value")
            }
        }
    }

    /// Whether `c`, the next character of a number, is the point before
    /// its fraction or the `e` before its exponent.
    fn continues_float(&self, c: char) -> bool {
        let next = self.peek_at(1);
        match c {
            '.' => next.is_some_and(|d| d.is_ascii_digit()),
            'e' | 'E' => next.is_some_and(|d| d.is_ascii_digit() || d == '-' || d == '+'),
            _ => false,
        }
    }

    fn number(&mut self) -> Parsed<Value> {
        let start = self.pos;
        if self.peek() == Some('-') {
            self.pos += 1;
        }
        let mut float = false;
        while let Some(c) = self.peek() {
            let exponent_sign = matches!(c, '+' | '-')
                && matches!(self.chars.get(self.pos - 1), Some('e') | Some('E'));
            if c.is_ascii_digit() || exponent_sign {
                self.pos += 1;
            } else if self.continues_float(c) {
                float = true;
                self.pos += 1;
            } else {
                break;
            }
        }
        let text: String = self.chars[start..self.pos].iter().collect();
        if !float && let Ok(n) = text.parse::<i64>() {
            return Ok(Value::Number(n.into()));
        }
        match text.parse::<f64>().ok().and_then(Number::from_f64) {
            Some(n) => Ok(Value::Number(n)),
            None => self.fail("invalid number"),
        }
    }
}

// ── Evaluation ───────────────────────────────────────────────────────────────

/// What an expression comes to.
enum Val<'a> {
    /// It could not be evaluated.
    Nothing,
    /// The outcome of a test.
    Bool(bool),
    /// One value: a literal, something computed, or the node a singular path
    /// names.
    One(Cow<'a, Value>),
    /// A list of nodes: what any other path selects, however many.
    Nodes(Vec<Cow<'a, Value>>),
}

static TRUE: Value = Value::Bool(true);
static FALSE: Value = Value::Bool(false);

impl<'a> Val<'a> {
    fn owned(value: Value) -> Self {
        Val::One(Cow::Owned(value))
    }

    /// Whether a condition made of just this holds: a test is its outcome, a
    /// path is "does it select anything".
    fn truthy(&self) -> bool {
        match self {
            Val::Nothing => false,
            Val::Bool(outcome) => *outcome,
            Val::One(_) => true,
            Val::Nodes(nodes) => !nodes.is_empty(),
        }
    }

    /// The values an operator is tried against: it holds if it holds for any.
    fn candidates(&self) -> Vec<&Value> {
        match self {
            Val::Nothing => Vec::new(),
            Val::Bool(outcome) => vec![if *outcome { &TRUE } else { &FALSE }],
            Val::One(value) => vec![value.as_ref()],
            Val::Nodes(nodes) => nodes.iter().map(|node| node.as_ref()).collect(),
        }
    }

    /// The value, if this is exactly one.
    fn single(&self) -> Option<&Value> {
        match self {
            Val::One(value) => Some(value.as_ref()),
            Val::Nodes(nodes) if nodes.len() == 1 => Some(nodes[0].as_ref()),
            Val::Bool(outcome) => Some(if *outcome { &TRUE } else { &FALSE }),
            _ => None,
        }
    }

    /// This as an array: the elements of an array value, or the nodes of a
    /// node list.
    fn elements(&self) -> Option<Vec<&Value>> {
        match self {
            Val::One(value) => value.as_array().map(|items| items.iter().collect()),
            Val::Nodes(nodes) => Some(nodes.iter().map(|node| node.as_ref()).collect()),
            _ => None,
        }
    }

    fn into_values(self) -> Vec<Value> {
        match self {
            Val::Nothing => Vec::new(),
            Val::Bool(outcome) => vec![Value::Bool(outcome)],
            Val::One(value) => vec![value.into_owned()],
            Val::Nodes(nodes) => nodes.into_iter().map(Cow::into_owned).collect(),
        }
    }
}

#[derive(Clone, Copy)]
enum Num {
    Int(i64),
    Float(f64),
}

impl Num {
    fn of(value: &Value) -> Option<Num> {
        let Value::Number(n) = value else {
            return None;
        };
        n.as_i64()
            .map(Num::Int)
            .or_else(|| n.as_f64().map(Num::Float))
    }

    fn float(self) -> f64 {
        match self {
            Num::Int(n) => n as f64,
            Num::Float(f) => f,
        }
    }

    /// As a JSON number; a float that is not finite is *Nothing*.
    fn val<'a>(self) -> Val<'a> {
        match self {
            Num::Int(n) => Val::owned(Value::Number(n.into())),
            Num::Float(f) => match Number::from_f64(f) {
                Some(n) => Val::owned(Value::Number(n)),
                None => Val::Nothing,
            },
        }
    }
}

/// Structural equality, with `1` equal to `1.0`.
pub fn deep_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(_), Value::Number(_)) => match (Num::of(a), Num::of(b)) {
            (Some(Num::Int(x)), Some(Num::Int(y))) => x == y,
            (Some(x), Some(y)) => x.float() == y.float(),
            _ => a == b,
        },
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(x, y)| deep_eq(x, y))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(key, value)| y.get(key).is_some_and(|other| deep_eq(value, other)))
        }
        _ => a == b,
    }
}

fn ordering(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Number(_), Value::Number(_)) => {
            Num::of(a)?.float().partial_cmp(&Num::of(b)?.float())
        }
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// Characters in a string, elements in an array, members in an object.
fn size_of(value: &Value) -> Option<usize> {
    match value {
        Value::String(s) => Some(s.chars().count()),
        Value::Array(items) => Some(items.len()),
        Value::Object(members) => Some(members.len()),
        _ => None,
    }
}

fn regex_search(text: &Value, pattern: &Value, whole: bool) -> bool {
    let (Value::String(text), Value::String(pattern)) = (text, pattern) else {
        return false;
    };
    let pattern = if whole {
        Cow::Owned(format!("^(?:{pattern})$"))
    } else {
        Cow::Borrowed(pattern.as_str())
    };
    regex_lite::Regex::new(&pattern).is_ok_and(|re| re.is_match(text))
}

fn contains(haystack: &[&Value], needle: &Value) -> bool {
    haystack.iter().any(|item| deep_eq(item, needle))
}

struct Node<'a> {
    location: Location,
    value: &'a Value,
}

/// Evaluates against one document. `track` is whether nodes carry their
/// location: only the outermost selection of a query needs it.
struct Evaluator<'a> {
    root: &'a Value,
}

impl<'a> Evaluator<'a> {
    fn child(
        parent: &Node<'a>,
        step: impl FnOnce() -> Step,
        value: &'a Value,
        track: bool,
    ) -> Node<'a> {
        let location = if track {
            let mut location = Vec::with_capacity(parent.location.len() + 1);
            location.extend(parent.location.iter().cloned());
            location.push(step());
            location
        } else {
            Vec::new()
        };
        Node { location, value }
    }

    fn select(&self, path: &'a Path, current: &'a Value, track: bool) -> Vec<Node<'a>> {
        let start = match path.origin {
            Origin::Root => self.root,
            Origin::Current => current,
        };
        let mut nodes = vec![Node {
            location: Vec::new(),
            value: start,
        }];
        for segment in &path.segments {
            let mut next = Vec::new();
            for node in &nodes {
                if segment.descendant {
                    self.descend(node, segment, track, &mut next);
                } else {
                    self.apply(node, segment, track, &mut next);
                }
            }
            nodes = next;
        }
        nodes
    }

    /// `segment` applied to `node` and to everything beneath it, in document
    /// order.
    fn descend(&self, node: &Node<'a>, segment: &'a Segment, track: bool, out: &mut Vec<Node<'a>>) {
        self.apply(node, segment, track, out);
        match node.value {
            Value::Array(items) => {
                for (index, item) in items.iter().enumerate() {
                    let child = Self::child(node, || Step::Index(index), item, track);
                    self.descend(&child, segment, track, out);
                }
            }
            Value::Object(members) => {
                for (key, value) in members {
                    let child = Self::child(node, || Step::Key(key.clone()), value, track);
                    self.descend(&child, segment, track, out);
                }
            }
            _ => {}
        }
    }

    fn apply(&self, node: &Node<'a>, segment: &'a Segment, track: bool, out: &mut Vec<Node<'a>>) {
        for selector in &segment.selectors {
            match (selector, node.value) {
                (Selector::Name(name), Value::Object(members)) => {
                    if let Some(value) = members.get(name) {
                        out.push(Self::child(node, || Step::Key(name.clone()), value, track));
                    }
                }
                (Selector::Index(index), Value::Array(items)) => {
                    let len = items.len() as i64;
                    let at = if *index < 0 { len + index } else { *index };
                    if (0..len).contains(&at) {
                        let at = at as usize;
                        out.push(Self::child(node, || Step::Index(at), &items[at], track));
                    }
                }
                (Selector::Slice(start, end, step), Value::Array(items)) => {
                    for at in slice_indexes(items.len(), *start, *end, *step) {
                        out.push(Self::child(node, || Step::Index(at), &items[at], track));
                    }
                }
                (Selector::Wildcard, Value::Array(items)) => {
                    for (at, item) in items.iter().enumerate() {
                        out.push(Self::child(node, || Step::Index(at), item, track));
                    }
                }
                (Selector::Wildcard, Value::Object(members)) => {
                    for (key, value) in members {
                        out.push(Self::child(node, || Step::Key(key.clone()), value, track));
                    }
                }
                (Selector::Filter(condition), Value::Array(items)) => {
                    for (at, item) in items.iter().enumerate() {
                        if self.eval(condition, item).truthy() {
                            out.push(Self::child(node, || Step::Index(at), item, track));
                        }
                    }
                }
                (Selector::Filter(condition), Value::Object(members)) => {
                    for (key, value) in members {
                        if self.eval(condition, value).truthy() {
                            out.push(Self::child(node, || Step::Key(key.clone()), value, track));
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn eval(&self, expr: &'a Expr, current: &'a Value) -> Val<'a> {
        match expr {
            Expr::Literal(value) => Val::One(Cow::Borrowed(value)),
            Expr::Path(path) => {
                let mut nodes = self.select(path, current, false);
                if path.is_singular() {
                    match nodes.pop() {
                        Some(node) => Val::One(Cow::Borrowed(node.value)),
                        None => Val::Nothing,
                    }
                } else {
                    Val::Nodes(nodes.into_iter().map(|n| Cow::Borrowed(n.value)).collect())
                }
            }
            Expr::Keys(inner) => keys_of(&self.eval(inner, current)),
            Expr::Call(func, args) => {
                let args: Vec<Val<'a>> = args.iter().map(|arg| self.eval(arg, current)).collect();
                call(*func, &args)
            }
            Expr::Negate(inner) => match self.eval(inner, current).single().and_then(Num::of) {
                Some(Num::Int(n)) => match n.checked_neg() {
                    Some(n) => Num::Int(n).val(),
                    None => Num::Float(-(n as f64)).val(),
                },
                Some(Num::Float(f)) => Num::Float(-f).val(),
                None => Val::Nothing,
            },
            Expr::Plus(inner) => match self.eval(inner, current).single().and_then(Num::of) {
                Some(n) => n.val(),
                None => Val::Nothing,
            },
            Expr::Arith(op, left, right) => {
                let (left, right) = (self.eval(left, current), self.eval(right, current));
                match (
                    left.single().and_then(Num::of),
                    right.single().and_then(Num::of),
                ) {
                    (Some(a), Some(b)) => arithmetic(*op, a, b),
                    _ => Val::Nothing,
                }
            }
            Expr::Compare(op, left, right) => {
                let (left, right) = (self.eval(left, current), self.eval(right, current));
                Val::Bool(compare(*op, &left, &right))
            }
            Expr::Not(inner) => Val::Bool(!self.eval(inner, current).truthy()),
            Expr::And(left, right) => {
                Val::Bool(self.eval(left, current).truthy() && self.eval(right, current).truthy())
            }
            Expr::Or(left, right) => {
                Val::Bool(self.eval(left, current).truthy() || self.eval(right, current).truthy())
            }
        }
    }
}

/// The indexes an array slice selects, in the order it selects them.
fn slice_indexes(
    len: usize,
    start: Option<i64>,
    end: Option<i64>,
    step: Option<i64>,
) -> Vec<usize> {
    let len = len as i64;
    let step = step.unwrap_or(1);
    let normalise = |i: i64| if i >= 0 { i } else { len + i };
    let mut out = Vec::new();
    if step > 0 {
        let lower = normalise(start.unwrap_or(0)).clamp(0, len);
        let upper = normalise(end.unwrap_or(len)).clamp(0, len);
        let mut i = lower;
        while i < upper {
            out.push(i as usize);
            i += step;
        }
    } else if step < 0 {
        let upper = normalise(start.unwrap_or(len - 1)).clamp(-1, len - 1);
        let lower = normalise(end.unwrap_or(-len - 1)).clamp(-1, len - 1);
        let mut i = upper;
        while lower < i {
            out.push(i as usize);
            i += step;
        }
    }
    out
}

fn arithmetic<'a>(op: ArithOp, a: Num, b: Num) -> Val<'a> {
    // Division is always in floating point; the rest stay in integers while
    // both operands are integers and the result fits.
    if let (Num::Int(x), Num::Int(y)) = (a, b) {
        let exact = match op {
            ArithOp::Add => x.checked_add(y),
            ArithOp::Sub => x.checked_sub(y),
            ArithOp::Mul => x.checked_mul(y),
            ArithOp::Rem if y == 0 => return Val::Nothing,
            ArithOp::Rem => x.checked_rem(y),
            ArithOp::Div => None,
        };
        if let Some(n) = exact {
            return Num::Int(n).val();
        }
    }
    let (x, y) = (a.float(), b.float());
    let result = match op {
        ArithOp::Add => x + y,
        ArithOp::Sub => x - y,
        ArithOp::Mul => x * y,
        ArithOp::Div | ArithOp::Rem if y == 0.0 => return Val::Nothing,
        ArithOp::Div => x / y,
        ArithOp::Rem => x % y,
    };
    Num::Float(result).val()
}

fn compare(op: CmpOp, left: &Val, right: &Val) -> bool {
    let any_pair = |holds: &dyn Fn(&Value, &Value) -> bool| {
        let right = right.candidates();
        left.candidates()
            .iter()
            .any(|a| right.iter().any(|b| holds(a, b)))
    };
    let ordered = |wanted: &[std::cmp::Ordering]| {
        any_pair(&|a, b| ordering(a, b).is_some_and(|o| wanted.contains(&o)))
    };
    use std::cmp::Ordering::{Equal, Greater, Less};
    match op {
        CmpOp::Eq => any_pair(&deep_eq),
        CmpOp::Ne => any_pair(&|a, b| !deep_eq(a, b)),
        CmpOp::Lt => ordered(&[Less]),
        CmpOp::Le => ordered(&[Less, Equal]),
        CmpOp::Gt => ordered(&[Greater]),
        CmpOp::Ge => ordered(&[Greater, Equal]),
        CmpOp::Regex => any_pair(&|text, pattern| regex_search(text, pattern, false)),
        CmpOp::In | CmpOp::Nin => {
            let found = right.elements().is_some_and(|haystack| {
                left.candidates()
                    .iter()
                    .any(|needle| contains(&haystack, needle))
            });
            found == (op == CmpOp::In)
        }
        CmpOp::SubsetOf | CmpOp::AnyOf | CmpOp::NoneOf => relation(op, left, right),
        CmpOp::SizeOf => {
            let Some(wanted) = right.single().and_then(Num::of) else {
                return false;
            };
            let wanted = wanted.float().trunc();
            left.candidates()
                .iter()
                .any(|value| size_of(value).is_some_and(|size| size as f64 == wanted))
        }
        CmpOp::Empty => {
            let Some(Value::Bool(wanted)) = right.single() else {
                return false;
            };
            left.candidates()
                .iter()
                .any(|value| size_of(value).is_some_and(|size| (size == 0) == *wanted))
        }
    }
}

fn relation(op: CmpOp, left: &Val, right: &Val) -> bool {
    let (Some(left), Some(right)) = (left.elements(), right.elements()) else {
        return false;
    };
    match op {
        CmpOp::SubsetOf => left.iter().all(|item| contains(&right, item)),
        CmpOp::AnyOf => left.iter().any(|item| contains(&right, item)),
        _ => !left.iter().any(|item| contains(&right, item)),
    }
}

/// The member names of every object among the values, as a list of strings.
fn keys_of<'a>(of: &Val) -> Val<'a> {
    let mut names = Vec::new();
    for value in of.candidates() {
        if let Value::Object(members) = value {
            names.extend(
                members
                    .keys()
                    .map(|key| Cow::Owned(Value::String(key.clone()))),
            );
        }
    }
    Val::Nodes(names)
}

fn call<'a>(func: Func, args: &[Val<'a>]) -> Val<'a> {
    // The wrong number of arguments is Nothing, not a best effort with some
    // of them.
    let arity_ok = match func {
        Func::Concat => !args.is_empty(),
        Func::Append => args.len() >= 2,
        Func::Match | Func::Search | Func::Index | Func::SubsetOf | Func::AnyOf | Func::NoneOf => {
            args.len() == 2
        }
        _ => args.len() == 1,
    };
    if !arity_ok {
        return Val::Nothing;
    }
    let integer = |n: usize| Val::owned(Value::Number((n as u64).into()));
    let numbers = |of: &Val| -> Option<Vec<Num>> {
        let numbers: Option<Vec<Num>> = of.elements()?.iter().map(|v| Num::of(v)).collect();
        numbers.filter(|numbers| !numbers.is_empty())
    };
    match func {
        Func::Length => match args[0].single().and_then(size_of) {
            Some(size) => integer(size),
            None => Val::Nothing,
        },
        Func::Count => integer(args[0].candidates().len()),
        Func::Value => match args[0].single() {
            Some(value) => Val::owned(value.clone()),
            None => Val::Nothing,
        },
        Func::Keys => keys_of(&args[0]),
        Func::Match | Func::Search => match (args[0].single(), args[1].single()) {
            (Some(text), Some(pattern)) => {
                Val::Bool(regex_search(text, pattern, func == Func::Match))
            }
            _ => Val::Bool(false),
        },
        Func::Concat => {
            let mut out = String::new();
            for arg in args {
                match arg.single() {
                    Some(Value::String(part)) => out.push_str(part),
                    _ => return Val::Nothing,
                }
            }
            Val::owned(Value::String(out))
        }
        Func::Abs | Func::Ceiling | Func::Floor => match args[0].single().and_then(Num::of) {
            Some(Num::Int(n)) if func == Func::Abs => match n.checked_abs() {
                Some(n) => Num::Int(n).val(),
                None => Val::Nothing,
            },
            Some(Num::Int(n)) => Num::Int(n).val(),
            Some(Num::Float(f)) => Num::Float(match func {
                Func::Abs => f.abs(),
                Func::Ceiling => f.ceil(),
                _ => f.floor(),
            })
            .val(),
            None => Val::Nothing,
        },
        Func::First | Func::Last | Func::Index => {
            let Some(items) = args[0].elements() else {
                return Val::Nothing;
            };
            let len = items.len() as i64;
            let at = match func {
                Func::First => 0,
                Func::Last => len - 1,
                _ => match args[1].single().and_then(Num::of) {
                    Some(n) => {
                        let n = n.float().trunc() as i64;
                        if n < 0 { len + n } else { n }
                    }
                    None => return Val::Nothing,
                },
            };
            if (0..len).contains(&at) {
                Val::owned(items[at as usize].clone())
            } else {
                Val::Nothing
            }
        }
        Func::Min | Func::Max | Func::Sum | Func::Avg | Func::Stddev => {
            let Some(numbers) = numbers(&args[0]) else {
                return Val::Nothing;
            };
            let floats = || numbers.iter().map(|n| n.float());
            let count = numbers.len() as f64;
            match func {
                Func::Min | Func::Max => {
                    let mut best = numbers[0];
                    for n in &numbers[1..] {
                        let better = if func == Func::Min {
                            n.float() < best.float()
                        } else {
                            n.float() > best.float()
                        };
                        if better {
                            best = *n;
                        }
                    }
                    best.val()
                }
                Func::Sum => {
                    let exact = numbers.iter().try_fold(0i64, |total, n| match n {
                        Num::Int(n) => total.checked_add(*n),
                        Num::Float(_) => None,
                    });
                    match exact {
                        Some(total) => Num::Int(total).val(),
                        None => Num::Float(floats().sum()).val(),
                    }
                }
                Func::Avg => Num::Float(floats().sum::<f64>() / count).val(),
                _ => {
                    let mean = floats().sum::<f64>() / count;
                    let variance = floats().map(|f| (f - mean).powi(2)).sum::<f64>() / count;
                    Num::Float(variance.sqrt()).val()
                }
            }
        }
        Func::Append => {
            let Some(items) = args[0].elements() else {
                return Val::Nothing;
            };
            let mut out: Vec<Cow<'a, Value>> = items
                .into_iter()
                .map(|item| Cow::Owned(item.clone()))
                .collect();
            for arg in &args[1..] {
                // Several values are added as one element, not spread.
                let added = match arg {
                    Val::Nothing => return Val::Nothing,
                    Val::Nodes(nodes) if nodes.len() != 1 => {
                        Value::Array(nodes.iter().map(|n| n.as_ref().clone()).collect())
                    }
                    other => match other.single() {
                        Some(value) => value.clone(),
                        None => return Val::Nothing,
                    },
                };
                out.push(Cow::Owned(added));
            }
            Val::Nodes(out)
        }
        Func::SubsetOf => Val::Bool(relation(CmpOp::SubsetOf, &args[0], &args[1])),
        Func::AnyOf => Val::Bool(relation(CmpOp::AnyOf, &args[0], &args[1])),
        Func::NoneOf => Val::Bool(relation(CmpOp::NoneOf, &args[0], &args[1])),
    }
}

// ── Queries ──────────────────────────────────────────────────────────────────

/// A parsed path.
#[derive(Debug, Clone)]
pub struct Query {
    expr: Expr,
    legacy: bool,
}

impl Query {
    pub fn parse(path: &str) -> Result<Query, String> {
        let trimmed = path.trim();
        // `$…` is JSONPath, and so is anything that can only be an
        // expression: legacy names never contain a parenthesis or start with
        // a sign.
        let legacy = !trimmed.starts_with('$')
            && !trimmed.contains('(')
            && !trimmed.starts_with(['-', '+', '!']);
        let text = if !legacy {
            Cow::Borrowed(trimmed)
        } else if trimmed == "." || trimmed.is_empty() {
            Cow::Borrowed("$")
        } else if trimmed.starts_with(['.', '[']) {
            Cow::Owned(format!("${trimmed}"))
        } else {
            Cow::Owned(format!("$.{trimmed}"))
        };
        let mut parser = Parser::new(&text);
        let expr = parser.expression()?;
        parser.skip_ws();
        if parser.peek().is_some() {
            return parser.fail("unexpected trailing characters");
        }
        if legacy && !matches!(expr, Expr::Path(_)) {
            return Err("invalid path: a legacy path cannot contain an expression".into());
        }
        Ok(Query { expr, legacy })
    }

    /// Not written with a leading `$`: the commands answer for the first
    /// match only.
    pub fn is_legacy(&self) -> bool {
        self.legacy
    }

    /// A plain path, which names nodes of the document (as opposed to a
    /// projection, which computes values).
    pub fn selects(&self) -> bool {
        matches!(self.expr, Expr::Path(_))
    }

    /// The path of the whole document: `$` or `.`.
    pub fn is_root(&self) -> bool {
        matches!(&self.expr, Expr::Path(path) if path.origin == Origin::Root && path.segments.is_empty())
    }

    /// Where the selected nodes are, in document order, each once. Empty for
    /// a projection.
    pub fn locations(&self, root: &Value) -> Vec<Location> {
        let Expr::Path(path) = &self.expr else {
            return Vec::new();
        };
        let nodes = Evaluator { root }.select(path, root, true);
        let mut seen = std::collections::HashSet::with_capacity(nodes.len());
        let mut out = Vec::with_capacity(nodes.len());
        for node in nodes {
            if seen.insert(node.location.clone()) {
                out.push(node.location);
            }
        }
        out
    }

    /// The selected values, or the values a projection computes.
    pub fn values<'a>(&'a self, root: &'a Value) -> Vec<Cow<'a, Value>> {
        let evaluator = Evaluator { root };
        match &self.expr {
            Expr::Path(path) => evaluator
                .select(path, root, false)
                .into_iter()
                .map(|node| Cow::Borrowed(node.value))
                .collect(),
            expr => evaluator
                .eval(expr, root)
                .into_values()
                .into_iter()
                .map(Cow::Owned)
                .collect(),
        }
    }

    /// For a path that selects nothing but ends in a member name under a
    /// parent that can be only one node — `$.a.b.new` — the objects that
    /// member could be added to, and its name.
    pub fn new_member(&self, root: &Value) -> Option<(Vec<Location>, String)> {
        let Expr::Path(path) = &self.expr else {
            return None;
        };
        if !path.is_singular() {
            return None;
        }
        let (last, parents) = path.segments.split_last()?;
        let [Selector::Name(name)] = last.selectors.as_slice() else {
            return None;
        };
        let parent = Path {
            origin: path.origin,
            segments: parents.to_vec(),
        };
        let locations: Vec<Location> = Evaluator { root }
            .select(&parent, root, true)
            .into_iter()
            .filter(|node| node.value.is_object())
            .map(|node| node.location)
            .collect();
        if locations.is_empty() {
            None
        } else {
            Some((locations, name.clone()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn values(doc: &Value, path: &str) -> Value {
        let query = Query::parse(path).unwrap_or_else(|e| panic!("{path}: {e}"));
        Value::Array(query.values(doc).into_iter().map(Cow::into_owned).collect())
    }

    fn bikes() -> Value {
        json!({"inventory": {
            "mountain_bikes": [
                {"id": "bike:1", "model": "Phoebe", "price": 1920,
                 "specs": {"material": "carbon", "weight": 13.1}, "colors": ["black", "silver"]},
                {"id": "bike:2", "model": "Quaoar", "price": 2072,
                 "specs": {"material": "aluminium", "weight": 7.9}, "colors": ["black", "white"]},
                {"id": "bike:3", "model": "Weywot", "price": 3264,
                 "specs": {"material": "alloy", "weight": 13.8}}
            ],
            "commuter_bikes": [
                {"id": "bike:4", "model": "Salacia", "price": 1475,
                 "specs": {"material": "aluminium", "weight": 16.6}, "colors": ["black", "silver"]},
                {"id": "bike:5", "model": "Mimas", "price": 3941,
                 "specs": {"material": "alloy", "weight": 11.6}}
            ]
        }})
    }

    #[test]
    fn a_filter_too_long_or_too_deep_is_an_error() {
        assert!(Query::parse("$[?(@.a && (@.b || !@.c))]").is_ok());
        for path in [
            format!("$[?{}@.a{}]", "(".repeat(100_000), ")".repeat(100_000)),
            format!("$[?{}@.a]", "!".repeat(100_000)),
            format!("$[?@.a == {}1]", "-".repeat(100_000)),
            format!("$[?@.a == {}]", "[".repeat(100_000)),
            format!("$[?{}]", vec!["@.a"; 100_000].join(" && ")),
        ] {
            let Err(err) = Query::parse(&path) else {
                panic!("accepted")
            };
            assert!(err.contains("too long or nested too deep"), "{err}");
        }
    }

    #[test]
    fn access_examples() {
        let doc = bikes();
        let models = json!(["Phoebe", "Quaoar", "Weywot"]);
        assert_eq!(values(&doc, "$.inventory.mountain_bikes[*].model"), models);
        assert_eq!(
            values(&doc, r#"$.inventory["mountain_bikes"][*].model"#),
            models
        );
        assert_eq!(values(&doc, "$..mountain_bikes[*].model"), models);
        assert_eq!(
            values(&doc, "$..model"),
            json!(["Phoebe", "Quaoar", "Weywot", "Salacia", "Mimas"])
        );
        assert_eq!(
            values(&doc, "$..mountain_bikes[0:2].model"),
            json!(["Phoebe", "Quaoar"])
        );
        assert_eq!(values(&doc, "$.inventory.*").as_array().unwrap().len(), 2);
        assert_eq!(
            values(&doc, "$..price"),
            json!([1920, 2072, 3264, 1475, 3941])
        );
    }

    #[test]
    fn selectors() {
        let doc = json!({"a": [0, 1, 2, 3, 4, 5], "o": {"x": 1, "y:z": 2, "k e y": 3}});
        assert_eq!(values(&doc, "$.a[-1]"), json!([5]));
        assert_eq!(values(&doc, "$.a[1,3]"), json!([1, 3]));
        assert_eq!(values(&doc, "$.a[1:3]"), json!([1, 2]));
        assert_eq!(values(&doc, "$.a[:2]"), json!([0, 1]));
        assert_eq!(values(&doc, "$.a[4:]"), json!([4, 5]));
        assert_eq!(values(&doc, "$.a[::2]"), json!([0, 2, 4]));
        assert_eq!(values(&doc, "$.a[::-2]"), json!([5, 3, 1]));
        assert_eq!(values(&doc, "$.a[-2:]"), json!([4, 5]));
        assert_eq!(values(&doc, "$.a[:]"), json!([0, 1, 2, 3, 4, 5]));
        assert_eq!(values(&doc, "$.a[10]"), json!([]));
        assert_eq!(values(&doc, "$.o['y:z']"), json!([2]));
        assert_eq!(values(&doc, r#"$.o["k e y"]"#), json!([3]));
        assert_eq!(values(&doc, "$.o.y:z"), json!([2]));
        assert_eq!(values(&doc, "$['o']['x','y:z']"), json!([1, 2]));
        assert_eq!(values(&doc, "$.o.*"), json!([1, 2, 3]));
        assert_eq!(values(&doc, "$"), json!([doc]));
        // Every descendant, root excluded, in document order.
        assert_eq!(
            values(&json!({"a": [1, {"b": 2}]}), "$..*"),
            json!([[1, {"b": 2}], 1, {"b": 2}, 2])
        );
    }

    #[test]
    fn legacy_paths() {
        let doc = json!({"inventory": {"headphones": [{"name": "a"}, {"name": "b"}]}});
        for path in [
            ".inventory.headphones",
            "inventory.headphones",
            r#"inventory["headphones"]"#,
            r#"['inventory']["headphones"]"#,
        ] {
            let query = Query::parse(path).unwrap();
            assert!(query.is_legacy(), "{path}");
            assert_eq!(query.values(&doc).len(), 1, "{path}");
        }
        assert_eq!(values(&doc, "inventory.headphones[-1].name"), json!(["b"]));
        assert!(Query::parse(".").unwrap().is_root());
        assert!(Query::parse("$").unwrap().is_root());
        assert!(!Query::parse("$").unwrap().is_legacy());
        assert!(!Query::parse("$.a").unwrap().is_root());
    }

    #[test]
    fn filter_examples() {
        let doc = bikes();
        assert_eq!(
            values(
                &doc,
                "$..mountain_bikes[?(@.price < 3000 && @.specs.weight < 10)].model"
            ),
            json!(["Quaoar"])
        );
        assert_eq!(
            values(&doc, r#"$..[?(@.specs.material == "alloy")].model"#),
            json!(["Weywot", "Mimas"])
        );
        assert_eq!(
            values(&doc, r#"$..[?(@.specs.material =~ "(?i)al")].model"#),
            json!(["Quaoar", "Weywot", "Salacia", "Mimas"])
        );
        assert_eq!(
            values(&doc, "$.inventory.*[?(@.price<2000)].price"),
            json!([1920, 1475])
        );
        assert_eq!(values(&doc, "$..[*].colors").as_array().unwrap().len(), 3);
        // The pattern can come from the document.
        let doc = json!([{"m": "alloy", "p": "(?i)AL"}, {"m": "carbon", "p": "(?i)al"}]);
        assert_eq!(values(&doc, "$[?(@.m =~ @.p)].m"), json!(["alloy"]));
        // A filter on an object looks at its values; `$` is the root.
        let doc = json!({"limit": 2, "o": {"a": {"n": 1}, "b": {"n": 3}}});
        assert_eq!(values(&doc, "$.o[?(@.n > $.limit)]"), json!([{"n": 3}]));
        assert_eq!(
            values(&doc, "$.o[?(@.n >= 1 || @.missing)].n"),
            json!([1, 3])
        );
    }

    #[test]
    fn negation() {
        let doc = json!([{"a": 1, "b": 1}, {"b": 2}, {"a": 1}, {"c": 3}]);
        assert_eq!(values(&doc, "$[?!@.a]"), json!([{"b": 2}, {"c": 3}]));
        assert_eq!(values(&doc, "$[?!(@.a==1)]"), json!([{"b": 2}, {"c": 3}]));
        assert_eq!(values(&doc, "$[?!@.a && @.b]"), json!([{"b": 2}]));
        assert_eq!(
            values(&doc, "$[?!!@.a]"),
            json!([{"a": 1, "b": 1}, {"a": 1}])
        );
        assert_eq!(values(&doc, "$[?@.b != 1]"), json!([{"b": 2}]));
    }

    #[test]
    fn literal_comparison() {
        let doc =
            json!({"arrs": [[1], [2], [1, 2], [1, [2]]], "objs": [{"x": 1}, {"x": 2}, {"y": 1}]});
        assert_eq!(values(&doc, "$.arrs[?(@ == [1])]"), json!([[1]]));
        assert_eq!(values(&doc, "$.arrs[?(@ == [1,[2]])]"), json!([[1, [2]]]));
        assert_eq!(
            values(&doc, r#"$.objs[?(@ == {"x":1})]"#),
            json!([{"x": 1}])
        );
        assert_eq!(
            values(&doc, r#"$.objs[?({"x":1.0} == @)]"#),
            json!([{"x": 1}])
        );
        assert_eq!(values(&doc, "$.arrs[?(@ < [9])]"), json!([]));
    }

    #[test]
    fn arithmetic_operators() {
        let doc = json!([{"a": 2, "b": 3}, {"a": 5, "b": 2}]);
        let first = json!([{"a": 2, "b": 3}]);
        assert_eq!(values(&doc, "$[?@.a + 1 == 3]"), first);
        assert_eq!(values(&doc, "$[?@.a + @.b * 2 == 8]"), first);
        assert_eq!(values(&doc, "$[?(@.a + @.b) * 2 == 10]"), first);
        assert_eq!(values(&doc, "$[?@.a % 2 == 0]"), first);
        assert_eq!(values(&doc, "$[?-@.a == -2]"), first);
        assert_eq!(values(&doc, "$[?@.a / 0 == 1]"), json!([]));
        // Without spaces it is a name.
        let doc = json!({"a+1": 7, "a": 1});
        assert_eq!(values(&doc, "$.a+1"), json!([7]));
        assert_eq!(values(&doc, "$.a + 1"), json!([2]));
    }

    #[test]
    fn membership_and_relations() {
        let doc = json!({"a": [1, 2, 3, 4], "allow": [2, 3]});
        assert_eq!(values(&doc, "$.a[?@ in [2,4]]"), json!([2, 4]));
        assert_eq!(values(&doc, "$.a[?@ nin [2,4]]"), json!([1, 3]));
        assert_eq!(values(&doc, "$.a[?@ in $.allow]"), json!([2, 3]));
        assert_eq!(values(&doc, "$.a[?@ in 2]"), json!([]));
        assert_eq!(values(&doc, "$.a[?@ nin 2]"), json!([1, 2, 3, 4]));
        assert_eq!(values(&doc, "$.a[?@ in [2.0]]"), json!([2]));

        let doc = json!({"a": [[1, 2], [1, 5], []]});
        assert_eq!(
            values(&doc, "$.a[?@ subsetof [1,2,3]]"),
            json!([[1, 2], []])
        );
        let doc = json!({"a": [[1, 9], [8, 9], []]});
        assert_eq!(values(&doc, "$.a[?@ anyof [1,2,3]]"), json!([[1, 9]]));
        assert_eq!(values(&doc, "$.a[?anyof(@, [1,2,3])]"), json!([[1, 9]]));
        let doc = json!({"a": [[4, 5], [1, 9], []]});
        assert_eq!(values(&doc, "$.a[?@ noneof [1,2,3]]"), json!([[4, 5], []]));
    }

    #[test]
    fn size_and_empty() {
        let doc = json!({"a": [[4, 5], [1], [7, 8, 9]]});
        assert_eq!(values(&doc, "$.a[?@ sizeof 2]"), json!([[4, 5]]));
        assert_eq!(values(&doc, "$.a[?@ size 2.9]"), json!([[4, 5]]));
        assert_eq!(values(&doc, r#"$.a[?@ sizeof "x"]"#), json!([]));
        let doc = json!({"a": [[], [1], "", [2, 3], {}, {"k": 1}]});
        assert_eq!(values(&doc, "$.a[?@ empty true]"), json!([[], "", {}]));
        assert_eq!(
            values(&doc, "$.a[?@ empty false]"),
            json!([[1], [2, 3], {"k": 1}])
        );
        // The words are still usable as names.
        let doc = json!({"size": 3, "in": {"empty": 1}});
        assert_eq!(values(&doc, "$.size"), json!([3]));
        assert_eq!(values(&doc, "$.in.empty"), json!([1]));
    }

    #[test]
    fn keys_operator_and_function() {
        let doc = json!({"obj": {"x": 1, "y": 2}, "books": [{"t": "a"}, {"t": "b"}]});
        assert_eq!(values(&doc, "$.obj~"), json!(["x", "y"]));
        assert_eq!(values(&doc, "$.books~"), json!([]));
        assert_eq!(values(&doc, "$.books[*]~"), json!(["t", "t"]));
        assert_eq!(values(&doc, "$.obj.keys()"), json!(["x", "y"]));
        assert_eq!(values(&doc, "$.obj.keys().count()"), json!([2]));
        assert_eq!(values(&doc, "$~").as_array().unwrap().len(), 2);
        for bad in ["$.obj~.x", "$.obj~~", "$.obj.keys()~"] {
            assert!(Query::parse(bad).is_err(), "{bad}");
        }
        assert_eq!(values(&doc, r#"$["a~b"]"#), json!([]));
    }

    #[test]
    fn functions() {
        let doc = json!({"a": [[1, 2, 3], [1], "abcd", "x"]});
        assert_eq!(
            values(&doc, "$.a[?length(@) > 2]"),
            json!([[1, 2, 3], "abcd"])
        );
        let doc = json!([{"a": 1, "b": 2, "c": 3}, {"a": 1}]);
        assert_eq!(
            values(&doc, "$[?count(@.*) == 3]"),
            json!([{"a": 1, "b": 2, "c": 3}])
        );
        assert_eq!(
            values(&doc, "$[?count(@.missing) == 0]")
                .as_array()
                .unwrap()
                .len(),
            2
        );
        let doc = json!([{"a": 1}, {"a": 2}]);
        assert_eq!(values(&doc, "$[?value(@.a) == 1]"), json!([{"a": 1}]));
        assert_eq!(values(&doc, "$[?value(@.*) == 1]"), json!([{"a": 1}]));
        let doc = json!({"a": ["abc", "xabc", "a", "b"]});
        assert_eq!(
            values(&doc, r#"$.a[?match(@, "a.*")]"#),
            json!(["abc", "a"])
        );
        assert_eq!(
            values(&doc, r#"$.a[?search(@, "b")]"#),
            json!(["abc", "xabc", "b"])
        );
        assert_eq!(values(&doc, r#"$.a[?search(@, "(")]"#), json!([]));
        let doc = json!({"a": [{"x": "a", "y": "b"}, {"x": "a", "y": "c"}]});
        assert_eq!(
            values(&doc, r#"$.a[?concat(@.x, @.y) == "ab"]"#),
            json!([{"x": "a", "y": "b"}])
        );
        assert_eq!(values(&doc, r#"$.a[?concat(@.x, 1) == "a1"]"#), json!([]));
        let doc = json!({"a": [2.1, 3.9, 1.0]});
        assert_eq!(values(&doc, "$.a[?ceiling(@) == 3]"), json!([2.1]));
        let doc = json!({"a": [2.1, 2.9, 3.5]});
        assert_eq!(values(&doc, "$.a[?floor(@) == 2]"), json!([2.1, 2.9]));
        let doc = json!({"a": [{"n": -5}, {"n": 5}, {"n": -3}]});
        assert_eq!(
            values(&doc, "$.a[?abs(@.n) == 5]"),
            json!([{"n": -5}, {"n": 5}])
        );
        let doc = json!({"a": [{"n": [1, 2]}, {"n": [9, 8]}]});
        assert_eq!(
            values(&doc, "$.a[?first(@.n) == 1]"),
            json!([{"n": [1, 2]}])
        );
        assert_eq!(values(&doc, "$.a[?last(@.n) == 8]"), json!([{"n": [9, 8]}]));
        assert_eq!(
            values(&doc, "$.a[?index(@.n, -1) == 2]"),
            json!([{"n": [1, 2]}])
        );
        assert_eq!(values(&doc, "$.a[?index(@.n, 7) == 2]"), json!([]));
        let doc = json!({"a": [{"n": [3, 1, 2]}, {"n": [5, 6]}, {"n": [1, "x"]}, {"n": []}]});
        assert_eq!(
            values(&doc, "$.a[?sum(@.n) == 6]"),
            json!([{"n": [3, 1, 2]}])
        );
        assert_eq!(
            values(&doc, "$.a[?avg(@.n) == 2]"),
            json!([{"n": [3, 1, 2]}])
        );
        assert_eq!(values(&doc, "$.a[?min(@.n) == 5]"), json!([{"n": [5, 6]}]));
        assert_eq!(
            values(&doc, "$.a[?max(@.n) == 3]"),
            json!([{"n": [3, 1, 2]}])
        );
        assert_eq!(
            values(&doc, "$.a[?stddev(@.n) == 0.5]"),
            json!([{"n": [5, 6]}])
        );
        // The wrong number of arguments is Nothing.
        assert_eq!(values(&doc, "$.a[?length(@, 1) > 0]"), json!([]));
        assert!(Query::parse("$.a[?nosuch(@)]").is_err());
        // A name not followed by `(` is a member.
        assert_eq!(
            values(&json!({"a": {"length": 9}}), "$.a.length"),
            json!([9])
        );
    }

    #[test]
    fn projections() {
        let doc = json!({"a": 2, "b": 4, "arr": [1, 2, 3], "obj": {"x": 1}});
        assert_eq!(values(&doc, "$.a + 1"), json!([3]));
        assert_eq!(values(&doc, "$.a * $.b"), json!([8]));
        assert_eq!(values(&doc, "($.a + $.b) / 2"), json!([3.0]));
        assert_eq!(values(&doc, "$.arr.length()"), json!([3]));
        assert_eq!(values(&doc, "length($.arr)"), json!([3]));
        assert_eq!(values(&doc, "$.a / 0"), json!([]));
        assert_eq!(values(&doc, "-$.a"), json!([-2]));
        assert_eq!(values(&doc, "$.missing + 1"), json!([]));
        assert_eq!(values(&doc, "$.arr.sum()"), json!([6]));
        assert_eq!(values(&doc, "$.arr.first()"), json!([1]));
        assert_eq!(values(&doc, "$.a > 1"), json!([true]));
        assert_eq!(values(&doc, "$.arr.append(9)"), json!([1, 2, 3, 9]));
        assert_eq!(values(&doc, "$.arr.append($.missing)"), json!([]));
        let books = json!({"books": [{"t": "a", "price": 30}, {"t": "b", "price": 5}]});
        assert_eq!(
            values(&books, r#"$.books[?(@.price >= 10)].append({"t":"X"})"#),
            json!([{"t": "a", "price": 30}, {"t": "X"}])
        );
        // A plain path, parenthesised or not, selects; anything else computes.
        for (path, selects) in [
            ("$.a", true),
            ("($.a)", true),
            ("$.a + 1", false),
            ("$.obj~", false),
        ] {
            assert_eq!(Query::parse(path).unwrap().selects(), selects, "{path}");
        }
        assert!(Query::parse("$.a + 1").unwrap().locations(&doc).is_empty());
    }

    #[test]
    fn locations_and_new_members() {
        let doc = json!({"a": [{"p": 1}, {"p": 2}], "o": {"x": 1}});
        let query = Query::parse("$.a[?(@.p > 1)].p").unwrap();
        assert_eq!(
            query.locations(&doc),
            vec![vec![
                Step::Key("a".into()),
                Step::Index(1),
                Step::Key("p".into())
            ]]
        );
        assert_eq!(get(&doc, &query.locations(&doc)[0]), Some(&json!(2)));
        // A union naming the same node twice still names one place.
        assert_eq!(Query::parse("$.a[0,0]").unwrap().locations(&doc).len(), 1);

        let query = Query::parse("$.o.new").unwrap();
        assert!(query.locations(&doc).is_empty());
        let (parents, name) = query.new_member(&doc).unwrap();
        assert_eq!(
            (parents, name.as_str()),
            (vec![vec![Step::Key("o".into())]], "new")
        );
        // Not under a parent that does not exist, is not an object, or is
        // not a single place.
        assert!(
            Query::parse("$.missing.new")
                .unwrap()
                .new_member(&doc)
                .is_none()
        );
        assert!(Query::parse("$.a.new").unwrap().new_member(&doc).is_none());
        assert!(
            Query::parse("$.a[*].new")
                .unwrap()
                .new_member(&doc)
                .is_none()
        );

        let mut doc = doc;
        *get_mut(&mut doc, &[Step::Key("a".into()), Step::Index(0)]).unwrap() = json!(7);
        assert_eq!(doc["a"][0], json!(7));
    }

    #[test]
    fn syntax_errors() {
        for bad in [
            "$.",
            "$[",
            "$[?]",
            "$.a[1",
            "$..",
            "$.a b",
            "$[?@.a ==]",
            "$['a",
            "a b c",
        ] {
            assert!(Query::parse(bad).is_err(), "{bad}");
        }
    }
}
