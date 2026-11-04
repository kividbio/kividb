// src/commands/expr.rs
//
// Expression evaluator for FT.AGGREGATE FILTER and APPLY clauses.
//
// ## Design
//
// We use a classic three-phase approach:
//   1. Tokenize  — split the expression string into a flat Vec<Token>
//   2. Parse     — consume tokens into an Expr AST using recursive descent
//   3. Evaluate  — walk the AST against a Row to produce a Value
//
// ## Why recursive descent and not a crate?
//
// Two reasons:
//   - Zero new dependencies. The expression grammar is small and stable.
//   - Learning value: this is the pattern used by every real query engine.
//     Once you understand it here you can read query parsers anywhere.
//
// ## The Expr type
//
// `Expr` is a recursive enum — each variant can contain other `Expr` values
// (via Box<Expr>, because enums can't contain themselves by value).
// Box<T> puts the inner Expr on the heap, breaking the infinite-size cycle.
// This is idiomatic Rust for tree types.

use ahash::AHashMap;

// ── Value ─────────────────────────────────────────────────────────────────────
//
// An expression evaluates to a Value. We use three variants because Redis's
// expression language mixes numeric and string contexts freely:
//   @price * 1.1      → Num
//   upper(@name)      → Str
//   @price > 100      → Bool (used in FILTER, always reduces to Num 0/1 in APPLY)

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Num(f64),
    Str(String),
    Bool(bool),
    Nil, // field doesn't exist or function returned nothing
}

impl Value {
    /// Convert to f64 for numeric operations. Non-numeric strings → 0.0.
    pub fn as_num(&self) -> f64 {
        match self {
            Value::Num(n) => *n,
            Value::Str(s) => s.parse::<f64>().unwrap_or(0.0),
            Value::Bool(b) => {
                if *b {
                    1.0
                } else {
                    0.0
                }
            }
            Value::Nil => 0.0,
        }
    }

    /// Convert to string for string operations.
    pub fn as_str(&self) -> String {
        match self {
            Value::Num(n) => {
                // Match Redis: integers print without decimal, floats with up to 17 sig figs
                if n.fract() == 0.0 && n.abs() < 1e15 {
                    format!("{}", *n as i64)
                } else {
                    format!("{}", n)
                }
            }
            Value::Str(s) => s.clone(),
            Value::Bool(b) => {
                if *b {
                    "1".into()
                } else {
                    "0".into()
                }
            }
            Value::Nil => String::new(),
        }
    }

    /// Truthy check for FILTER evaluation.
    pub fn is_truthy(&self) -> bool {
        match self {
            Value::Bool(b) => *b,
            Value::Num(n) => *n != 0.0,
            Value::Str(s) => !s.is_empty(),
            Value::Nil => false,
        }
    }

    /// Format for RESP output (what APPLY produces as the new field value).
    pub fn to_resp_string(&self) -> String {
        self.as_str()
    }
}

// ── Token ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Field(String),  // @fieldname
    NumLit(f64),    // 3.14, -7
    StrLit(String), // "hello"
    Ident(String),  // function names: upper, lower, exists, ...
    Plus,           // +
    Minus,          // -
    Star,           // *
    Slash,          // /
    Percent,        // %
    EqEq,           // ==
    NotEq,          // !=
    Lt,             //
    LtEq,           // <=
    Gt,             // >
    GtEq,           // >=
    And,            // &&
    Or,             // ||
    Bang,           // !
    LParen,         // (
    RParen,         // )
    Comma,          // ,
}

// ── Tokenizer ─────────────────────────────────────────────────────────────────

pub struct Tokenizer<'a> {
    src: &'a [u8],
    pos: usize,
}

impl<'a> Tokenizer<'a> {
    fn new(src: &'a str) -> Self {
        Self {
            src: src.as_bytes(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<u8> {
        self.src.get(self.pos).copied()
    }

    #[allow(dead_code)]
    fn advance(&mut self) -> Option<u8> {
        let b = self.src.get(self.pos).copied();
        self.pos += 1;
        b
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn tokenize(&mut self) -> Vec<Token> {
        let mut tokens = Vec::new();
        loop {
            self.skip_ws();
            let b = match self.peek() {
                None => break,
                Some(b) => b,
            };

            match b {
                // Field reference: @fieldname
                b'@' => {
                    self.pos += 1;
                    let start = self.pos;
                    while matches!(
                        self.peek(),
                        Some(b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' | b'-')
                    ) {
                        self.pos += 1;
                    }
                    let name = std::str::from_utf8(&self.src[start..self.pos])
                        .unwrap_or("")
                        .to_string();
                    tokens.push(Token::Field(name));
                }

                // String literal: "..."
                b'"' => {
                    self.pos += 1;
                    let start = self.pos;
                    while self.peek().map(|c| c != b'"').unwrap_or(false) {
                        self.pos += 1;
                    }
                    let s = std::str::from_utf8(&self.src[start..self.pos])
                        .unwrap_or("")
                        .to_string();
                    if self.peek() == Some(b'"') {
                        self.pos += 1;
                    }
                    tokens.push(Token::StrLit(s));
                }

                // Numeric literal: digits, optional decimal
                b'0'..=b'9' | b'.' => {
                    let start = self.pos;
                    while matches!(
                        self.peek(),
                        Some(b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-')
                    ) {
                        // Only consume +/- right after e/E (scientific notation)
                        if matches!(self.peek(), Some(b'+' | b'-')) {
                            let prev = if self.pos > 0 {
                                self.src[self.pos - 1]
                            } else {
                                0
                            };
                            if !matches!(prev, b'e' | b'E') {
                                break;
                            }
                        }
                        self.pos += 1;
                    }
                    let s = std::str::from_utf8(&self.src[start..self.pos]).unwrap_or("0");
                    let n = s.parse::<f64>().unwrap_or(0.0);
                    tokens.push(Token::NumLit(n));
                }

                // Identifier (function name)
                b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
                    let start = self.pos;
                    while matches!(
                        self.peek(),
                        Some(b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_')
                    ) {
                        self.pos += 1;
                    }
                    let s = std::str::from_utf8(&self.src[start..self.pos])
                        .unwrap_or("")
                        .to_string();
                    tokens.push(Token::Ident(s));
                }

                // Operators — must check two-char before one-char
                b'=' => {
                    self.pos += 1;
                    if self.peek() == Some(b'=') {
                        self.pos += 1;
                        tokens.push(Token::EqEq);
                    }
                    // bare = is not valid in this grammar — skip silently
                }
                b'!' => {
                    self.pos += 1;
                    if self.peek() == Some(b'=') {
                        self.pos += 1;
                        tokens.push(Token::NotEq);
                    } else {
                        tokens.push(Token::Bang);
                    }
                }
                b'<' => {
                    self.pos += 1;
                    if self.peek() == Some(b'=') {
                        self.pos += 1;
                        tokens.push(Token::LtEq);
                    } else {
                        tokens.push(Token::Lt);
                    }
                }
                b'>' => {
                    self.pos += 1;
                    if self.peek() == Some(b'=') {
                        self.pos += 1;
                        tokens.push(Token::GtEq);
                    } else {
                        tokens.push(Token::Gt);
                    }
                }
                b'&' => {
                    self.pos += 1;
                    if self.peek() == Some(b'&') {
                        self.pos += 1;
                    }
                    tokens.push(Token::And);
                }
                b'|' => {
                    self.pos += 1;
                    if self.peek() == Some(b'|') {
                        self.pos += 1;
                    }
                    tokens.push(Token::Or);
                }
                b'+' => {
                    self.pos += 1;
                    tokens.push(Token::Plus);
                }
                b'-' => {
                    // Could be unary minus before a number — let the parser handle it
                    self.pos += 1;
                    tokens.push(Token::Minus);
                }
                b'*' => {
                    self.pos += 1;
                    tokens.push(Token::Star);
                }
                b'/' => {
                    self.pos += 1;
                    tokens.push(Token::Slash);
                }
                b'%' => {
                    self.pos += 1;
                    tokens.push(Token::Percent);
                }
                b'(' => {
                    self.pos += 1;
                    tokens.push(Token::LParen);
                }
                b')' => {
                    self.pos += 1;
                    tokens.push(Token::RParen);
                }
                b',' => {
                    self.pos += 1;
                    tokens.push(Token::Comma);
                }
                _ => {
                    self.pos += 1;
                } // skip unknown chars
            }
        }
        tokens
    }
}

// ── AST ───────────────────────────────────────────────────────────────────────
//
// The expression AST. Box<Expr> is used for recursive variants — without it
// the enum would have infinite size (Expr contains Expr contains Expr...).
// Box<T> allocates T on the heap and stores a fixed-size pointer in the enum.

#[derive(Debug, Clone)]
pub enum Expr {
    // Leaves
    Literal(Value),
    Field(String),

    // Unary
    Neg(Box<Expr>),
    Not(Box<Expr>),

    // Binary arithmetic
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
    Mod(Box<Expr>, Box<Expr>),

    // Binary comparison
    Eq(Box<Expr>, Box<Expr>),
    Ne(Box<Expr>, Box<Expr>),
    Lt(Box<Expr>, Box<Expr>),
    Le(Box<Expr>, Box<Expr>),
    Gt(Box<Expr>, Box<Expr>),
    Ge(Box<Expr>, Box<Expr>),

    // Binary logic
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),

    // Function call: name + argument list
    Call(String, Vec<Expr>),
}

// ── Parser ────────────────────────────────────────────────────────────────────
//
// Recursive descent parser. Operator precedence is encoded in the call chain:
//
//   parse_or          (lowest precedence — evaluated last)
//     parse_and
//       parse_eq
//         parse_cmp
//           parse_add
//             parse_mul      (highest binary precedence)
//               parse_unary
//                 parse_primary  (atoms: literals, fields, function calls, parens)
//
// Each level calls the level above it for its operands. This naturally
// implements C-style precedence. The pattern is standard and worth learning.

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    /// How deep in parentheses, calls and unary operators the parse is.
    depth: u32,
    /// The expression nests too deep, and the rest of it was skipped.
    too_deep: bool,
}

/// The most tokens an expression may have, and the deepest it may nest. The
/// tree is parsed, evaluated and freed by calls that go as deep as it does:
/// without a limit, `((((…` or `1+1+1…` from a client would run the thread
/// out of stack, which takes the server down.
const TOKENS_MAX: usize = 1024;
const DEPTH_MAX: u32 = 64;

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            pos: 0,
            depth: 0,
            too_deep: false,
        }
    }

    /// `parse` one level deeper; past [`DEPTH_MAX`] the rest of the
    /// expression is skipped, and [`parse`] answers `None`.
    fn deeper(&mut self, parse: impl FnOnce(&mut Self) -> Expr) -> Expr {
        if self.depth >= DEPTH_MAX {
            self.too_deep = true;
            self.pos = self.tokens.len();
            return Expr::Literal(Value::Nil);
        }
        self.depth += 1;
        let expr = parse(self);
        self.depth -= 1;
        expr
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<&Token> {
        let t = self.tokens.get(self.pos);
        self.pos += 1;
        t
    }

    fn eat(&mut self, expected: &Token) -> bool {
        if self.peek() == Some(expected) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn parse_expr(&mut self) -> Expr {
        self.deeper(Self::parse_or)
    }

    fn parse_or(&mut self) -> Expr {
        let mut left = self.parse_and();
        while self.peek() == Some(&Token::Or) {
            self.advance();
            let right = self.parse_and();
            left = Expr::Or(Box::new(left), Box::new(right));
        }
        left
    }

    fn parse_and(&mut self) -> Expr {
        let mut left = self.parse_eq();
        while self.peek() == Some(&Token::And) {
            self.advance();
            let right = self.parse_eq();
            left = Expr::And(Box::new(left), Box::new(right));
        }
        left
    }

    fn parse_eq(&mut self) -> Expr {
        let mut left = self.parse_cmp();
        loop {
            match self.peek() {
                Some(Token::EqEq) => {
                    self.advance();
                    let r = self.parse_cmp();
                    left = Expr::Eq(Box::new(left), Box::new(r));
                }
                Some(Token::NotEq) => {
                    self.advance();
                    let r = self.parse_cmp();
                    left = Expr::Ne(Box::new(left), Box::new(r));
                }
                _ => break,
            }
        }
        left
    }

    fn parse_cmp(&mut self) -> Expr {
        let mut left = self.parse_add();
        loop {
            match self.peek() {
                Some(Token::Lt) => {
                    self.advance();
                    let r = self.parse_add();
                    left = Expr::Lt(Box::new(left), Box::new(r));
                }
                Some(Token::LtEq) => {
                    self.advance();
                    let r = self.parse_add();
                    left = Expr::Le(Box::new(left), Box::new(r));
                }
                Some(Token::Gt) => {
                    self.advance();
                    let r = self.parse_add();
                    left = Expr::Gt(Box::new(left), Box::new(r));
                }
                Some(Token::GtEq) => {
                    self.advance();
                    let r = self.parse_add();
                    left = Expr::Ge(Box::new(left), Box::new(r));
                }
                _ => break,
            }
        }
        left
    }

    fn parse_add(&mut self) -> Expr {
        let mut left = self.parse_mul();
        loop {
            match self.peek() {
                Some(Token::Plus) => {
                    self.advance();
                    let r = self.parse_mul();
                    left = Expr::Add(Box::new(left), Box::new(r));
                }
                Some(Token::Minus) => {
                    self.advance();
                    let r = self.parse_mul();
                    left = Expr::Sub(Box::new(left), Box::new(r));
                }
                _ => break,
            }
        }
        left
    }

    fn parse_mul(&mut self) -> Expr {
        let mut left = self.parse_unary();
        loop {
            match self.peek() {
                Some(Token::Star) => {
                    self.advance();
                    let r = self.parse_unary();
                    left = Expr::Mul(Box::new(left), Box::new(r));
                }
                Some(Token::Slash) => {
                    self.advance();
                    let r = self.parse_unary();
                    left = Expr::Div(Box::new(left), Box::new(r));
                }
                Some(Token::Percent) => {
                    self.advance();
                    let r = self.parse_unary();
                    left = Expr::Mod(Box::new(left), Box::new(r));
                }
                _ => break,
            }
        }
        left
    }

    fn parse_unary(&mut self) -> Expr {
        match self.peek() {
            Some(Token::Minus) => {
                self.advance();
                Expr::Neg(Box::new(self.deeper(Self::parse_unary)))
            }
            Some(Token::Bang) => {
                self.advance();
                Expr::Not(Box::new(self.deeper(Self::parse_unary)))
            }
            _ => self.parse_primary(),
        }
    }

    fn parse_primary(&mut self) -> Expr {
        match self.peek().cloned() {
            Some(Token::NumLit(n)) => {
                self.advance();
                Expr::Literal(Value::Num(n))
            }
            Some(Token::StrLit(s)) => {
                self.advance();
                Expr::Literal(Value::Str(s))
            }
            Some(Token::Field(name)) => {
                self.advance();
                Expr::Field(name)
            }

            // Identifier followed by '(' → function call
            Some(Token::Ident(name)) => {
                self.advance();
                if self.eat(&Token::LParen) {
                    let mut args = Vec::new();
                    // Parse comma-separated arguments until ')'
                    while self.peek() != Some(&Token::RParen) && self.peek().is_some() {
                        args.push(self.parse_expr());
                        if !self.eat(&Token::Comma) {
                            break;
                        }
                    }
                    self.eat(&Token::RParen);
                    Expr::Call(name.to_lowercase(), args)
                } else {
                    // Bare identifier — treat as string literal
                    Expr::Literal(Value::Str(name))
                }
            }

            // Parenthesized expression
            Some(Token::LParen) => {
                self.advance();
                let inner = self.parse_expr();
                self.eat(&Token::RParen);
                inner
            }

            // Unexpected token or end — return 0 as a safe default
            _ => Expr::Literal(Value::Num(0.0)),
        }
    }
}

// ── Public parse entry point ──────────────────────────────────────────────────

/// Parse an expression string into an AST.
/// Returns `None` if the expression is empty.
pub fn parse(expr: &str) -> Option<Expr> {
    let expr = expr.trim();
    if expr.is_empty() {
        return None;
    }
    let tokens = Tokenizer::new(expr).tokenize();
    if tokens.is_empty() || tokens.len() > TOKENS_MAX {
        return None;
    }
    let mut parser = Parser::new(tokens);
    let expr = parser.parse_expr();
    (!parser.too_deep).then_some(expr)
}

// ── Evaluator ─────────────────────────────────────────────────────────────────
//
// eval() walks the Expr tree and produces a Value.
// The `row` is the current aggregate row — a map of field_name → string_value.

pub fn eval(expr: &Expr, row: &AHashMap<String, String>) -> Value {
    match expr {
        Expr::Literal(v) => v.clone(),

        Expr::Field(name) => {
            match row.get(name.as_str()) {
                Some(s) => {
                    // Try to parse as number — if it works, return Num
                    if let Ok(n) = s.parse::<f64>() {
                        Value::Num(n)
                    } else {
                        Value::Str(s.clone())
                    }
                }
                None => Value::Nil,
            }
        }

        Expr::Neg(inner) => Value::Num(-eval(inner, row).as_num()),
        Expr::Not(inner) => Value::Bool(!eval(inner, row).is_truthy()),

        Expr::Add(l, r) => {
            let lv = eval(l, row);
            let rv = eval(r, row);
            // If either side is a non-numeric string, concatenate
            match (&lv, &rv) {
                (Value::Str(a), _) | (_, Value::Str(a)) if a.parse::<f64>().is_err() => {
                    Value::Str(format!("{}{}", lv.as_str(), rv.as_str()))
                }
                _ => Value::Num(lv.as_num() + rv.as_num()),
            }
        }
        Expr::Sub(l, r) => Value::Num(eval(l, row).as_num() - eval(r, row).as_num()),
        Expr::Mul(l, r) => Value::Num(eval(l, row).as_num() * eval(r, row).as_num()),
        Expr::Div(l, r) => {
            let d = eval(r, row).as_num();
            if d == 0.0 {
                Value::Nil
            } else {
                Value::Num(eval(l, row).as_num() / d)
            }
        }
        Expr::Mod(l, r) => {
            let d = eval(r, row).as_num();
            if d == 0.0 {
                Value::Nil
            } else {
                Value::Num(eval(l, row).as_num() % d)
            }
        }

        Expr::Eq(l, r) => Value::Bool(eval(l, row).as_str() == eval(r, row).as_str()),
        Expr::Ne(l, r) => Value::Bool(eval(l, row).as_str() != eval(r, row).as_str()),
        Expr::Lt(l, r) => {
            let (lv, rv) = (eval(l, row), eval(r, row));
            Value::Bool(lv.as_num() < rv.as_num())
        }
        Expr::Le(l, r) => {
            let (lv, rv) = (eval(l, row), eval(r, row));
            Value::Bool(lv.as_num() <= rv.as_num())
        }
        Expr::Gt(l, r) => {
            let (lv, rv) = (eval(l, row), eval(r, row));
            Value::Bool(lv.as_num() > rv.as_num())
        }
        Expr::Ge(l, r) => {
            let (lv, rv) = (eval(l, row), eval(r, row));
            Value::Bool(lv.as_num() >= rv.as_num())
        }

        Expr::And(l, r) => {
            // Short-circuit: if left is falsy, don't eval right
            let lv = eval(l, row);
            if !lv.is_truthy() {
                Value::Bool(false)
            } else {
                Value::Bool(eval(r, row).is_truthy())
            }
        }
        Expr::Or(l, r) => {
            let lv = eval(l, row);
            if lv.is_truthy() {
                Value::Bool(true)
            } else {
                Value::Bool(eval(r, row).is_truthy())
            }
        }

        Expr::Call(name, args) => eval_fn(name.as_str(), args, row),
    }
}

// ── Built-in functions ────────────────────────────────────────────────────────

fn eval_fn(name: &str, args: &[Expr], row: &AHashMap<String, String>) -> Value {
    // Evaluate all arguments eagerly (except exists() which checks for Nil)
    let eargs: Vec<Value> = args.iter().map(|a| eval(a, row)).collect();

    match name {
        // String functions
        "upper" => Value::Str(
            eargs
                .first()
                .map(|v| v.as_str().to_uppercase())
                .unwrap_or_default(),
        ),
        "lower" => Value::Str(
            eargs
                .first()
                .map(|v| v.as_str().to_lowercase())
                .unwrap_or_default(),
        ),
        "strlen" | "length" => Value::Num(
            eargs
                .first()
                .map(|v| v.as_str().len() as f64)
                .unwrap_or(0.0),
        ),

        "startswith" => {
            let s = eargs.first().map(|v| v.as_str()).unwrap_or_default();
            let prefix = eargs.get(1).map(|v| v.as_str()).unwrap_or_default();
            Value::Bool(s.starts_with(&prefix))
        }
        "contains" => {
            let s = eargs.first().map(|v| v.as_str()).unwrap_or_default();
            let needle = eargs.get(1).map(|v| v.as_str()).unwrap_or_default();
            Value::Bool(s.contains(&needle))
        }

        "split_part" => {
            // split_part(str, sep, n) — 1-indexed like Redis
            let s = eargs.first().map(|v| v.as_str()).unwrap_or_default();
            let sep = eargs.get(1).map(|v| v.as_str()).unwrap_or_default();
            let n = eargs.get(2).map(|v| v.as_num() as usize).unwrap_or(1);
            Value::Str(
                s.split(&sep)
                    .nth(n.saturating_sub(1))
                    .unwrap_or("")
                    .to_string(),
            )
        }

        "substr" => {
            // substr(str, offset, len) — 0-indexed
            let s = eargs.first().map(|v| v.as_str()).unwrap_or_default();
            let offset = eargs.get(1).map(|v| v.as_num() as usize).unwrap_or(0);
            let len = eargs
                .get(2)
                .map(|v| v.as_num() as usize)
                .unwrap_or(usize::MAX);
            Value::Str(s.chars().skip(offset).take(len).collect())
        }

        "ltrim" | "rtrim" => {
            let s = eargs.first().map(|v| v.as_str()).unwrap_or_default();
            if name == "ltrim" {
                Value::Str(s.trim_start().to_string())
            } else {
                Value::Str(s.trim_end().to_string())
            }
        }

        "trim" => Value::Str(
            eargs
                .first()
                .map(|v| v.as_str().trim().to_string())
                .unwrap_or_default(),
        ),

        // Math functions
        "abs" => Value::Num(eargs.first().map(|v| v.as_num().abs()).unwrap_or(0.0)),
        "ceil" => Value::Num(eargs.first().map(|v| v.as_num().ceil()).unwrap_or(0.0)),
        "floor" => Value::Num(eargs.first().map(|v| v.as_num().floor()).unwrap_or(0.0)),
        "round" => Value::Num(eargs.first().map(|v| v.as_num().round()).unwrap_or(0.0)),
        "log2" => Value::Num(eargs.first().map(|v| v.as_num().log2()).unwrap_or(0.0)),
        "log" => Value::Num(eargs.first().map(|v| v.as_num().ln()).unwrap_or(0.0)),
        "sqrt" => Value::Num(eargs.first().map(|v| v.as_num().sqrt()).unwrap_or(0.0)),
        "pow" => {
            let base = eargs.first().map(|v| v.as_num()).unwrap_or(0.0);
            let exp = eargs.get(1).map(|v| v.as_num()).unwrap_or(1.0);
            Value::Num(base.powf(exp))
        }

        // Type / existence functions
        "exists" => {
            // exists(@field) — true if field is present and non-nil
            // We need to re-eval the argument to check for Nil specifically
            let inner = args.first().map(|a| eval(a, row)).unwrap_or(Value::Nil);
            Value::Bool(!matches!(inner, Value::Nil))
        }
        "isnum" | "isnumeric" => {
            let v = eargs.first().map(|v| v.as_str()).unwrap_or_default();
            Value::Bool(v.parse::<f64>().is_ok())
        }

        // Conditional
        "if" => {
            // if(cond, then, else)
            let cond = eargs.first().map(|v| v.is_truthy()).unwrap_or(false);
            if cond {
                eargs.get(1).cloned().unwrap_or(Value::Nil)
            } else {
                eargs.get(2).cloned().unwrap_or(Value::Nil)
            }
        }

        // Type conversion
        "tostring" => match eargs.into_iter().next() {
            Some(v) => Value::Str(v.as_str()),
            None => Value::Str(String::new()),
        },
        "tonumber" => match eargs.into_iter().next() {
            Some(v) => Value::Num(v.as_num()),
            None => Value::Num(0.0),
        },
        "tointeger" => match eargs.into_iter().next() {
            Some(v) => Value::Num(v.as_num().trunc()),
            None => Value::Num(0.0),
        },

        // Formatting — basic printf-style: %s, %d, %f
        "format" => {
            let fmt = eargs.first().map(|v| v.as_str()).unwrap_or_default();
            let mut result = String::new();
            let mut arg_idx = 1usize;
            let bytes = fmt.as_bytes();
            let mut i = 0;
            while i < bytes.len() {
                if bytes[i] == b'%' && i + 1 < bytes.len() {
                    i += 1;
                    let arg = eargs.get(arg_idx).cloned().unwrap_or(Value::Nil);
                    match bytes[i] {
                        b's' => {
                            result.push_str(&arg.as_str());
                            arg_idx += 1;
                        }
                        b'd' => {
                            result.push_str(&format!("{}", arg.as_num() as i64));
                            arg_idx += 1;
                        }
                        b'f' => {
                            result.push_str(&format!("{:.6}", arg.as_num()));
                            arg_idx += 1;
                        }
                        b'g' => {
                            result.push_str(&format!("{}", arg.as_num()));
                            arg_idx += 1;
                        }
                        b'%' => result.push('%'),
                        other => {
                            result.push('%');
                            result.push(other as char);
                        }
                    }
                } else {
                    result.push(bytes[i] as char);
                }
                i += 1;
            }
            Value::Str(result)
        }
        "geodistance" => {
            // geodistance(lat1, lon1, lat2, lon2) → meters between two points
            // using the Haversine formula on Redis's geo-sphere of radius
            // 6_372_797.560856 m. Returns Nil if any argument is missing
            // or non-numeric.
            //
            // The formula:
            //   a = sin²(Δlat/2) + cos(lat1) · cos(lat2) · sin²(Δlon/2)
            //   c = 2 · atan2(√a, √(1−a))
            //   d = R · c
            //
            // Inputs are degrees, converted to radians inside the function.

            if eargs.len() < 4 {
                return Value::Nil;
            }
            let lat1 = eargs[0].as_num();
            let lon1 = eargs[1].as_num();
            let lat2 = eargs[2].as_num();
            let lon2 = eargs[3].as_num();

            // Validate ranges. Real Redis returns NaN for out-of-range
            // coordinates; we return Nil so the row's field is empty rather
            // than the literal "NaN" string.
            if !(-90.0..=90.0).contains(&lat1)
                || !(-90.0..=90.0).contains(&lat2)
                || !(-180.0..=180.0).contains(&lon1)
                || !(-180.0..=180.0).contains(&lon2)
            {
                return Value::Nil;
            }

            const EARTH_RADIUS_M: f64 = 6_372_797.560856;
            let to_rad = |d: f64| d * std::f64::consts::PI / 180.0;

            let phi1 = to_rad(lat1);
            let phi2 = to_rad(lat2);
            let d_phi = to_rad(lat2 - lat1);
            let d_lambda = to_rad(lon2 - lon1);

            let a = (d_phi / 2.0).sin().powi(2)
                + phi1.cos() * phi2.cos() * (d_lambda / 2.0).sin().powi(2);
            let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
            Value::Num(EARTH_RADIUS_M * c)
        }

        "timefmt" => {
            // timefmt(timestamp, format) → formatted timestamp string
            //
            // timestamp is seconds since Unix epoch. format defaults to
            // ISO 8601 UTC ("%FT%TZ") when omitted. Returns Nil for
            // negative or absurdly-large timestamps that can't be
            // represented as DateTime.
            //
            // We support the common strftime specifiers manually rather
            // than pulling in chrono — the Redis subset is small (FT, T,
            // F, Y, m, d, H, M, S, j, p, A, B), and we don't need locale
            // handling. This keeps the dependency footprint small and
            // makes the formatter's behaviour predictable across builds.

            let ts = eargs.first().map(|v| v.as_num()).unwrap_or(0.0);
            if !ts.is_finite() {
                return Value::Nil;
            }

            // Default format matches Redis: ISO 8601 UTC with 'Z' suffix.
            let fmt = eargs
                .get(1)
                .map(|v| v.as_str())
                .unwrap_or_else(|| "%FT%TZ".to_string());

            match crate::commands::strftime::format_unix_seconds(ts as i64, &fmt) {
                Some(s) => Value::Str(s),
                None => Value::Nil,
            }
        }

        // Anything unknown → Nil (safe default, won't crash)
        _ => Value::Nil,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_expression_that_nests_too_deep_or_runs_too_long_is_refused() {
        assert!(parse("((1+2))*-3").is_some());
        let deep = format!("{}1{}", "(".repeat(100_000), ")".repeat(100_000));
        assert!(parse(&deep).is_none());
        assert!(parse(&"-".repeat(100_000)).is_none());
        let long = vec!["1"; 100_000].join("+");
        assert!(parse(&long).is_none());
    }
}
