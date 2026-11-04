// src/commands/vset_filter.rs
//
// `VSIM ... FILTER <expression>`: the expressions of Redis's vector sets
// (`modules/vector-sets/expr.c`), over the JSON attributes `VSETATTR` and
// `VADD ... SETATTR` give an element. `FILTER` was taken and ignored, so an
// application that filtered by attribute got elements that did not match
// (#132).
//
// The language is Redis's: numbers, strings in single or double quotes (kept
// as written, escapes and all), `null`, tuples (`[1, "a"]`), selectors
// (`.field`, a top-level field of the attributes), and these operators, by
// precedence:
//
//   ( )                 grouping
//   !  not              not (unary)
//   **                  power (right to left)
//   *  /  %             product
//   +  -                sum
//   >  >=  <  <=  ==  !=  in
//   and  &&
//   or  ||
//
// `in` is membership of a tuple, or a substring of a string. A selector that
// the element's attributes do not have (or an element with none) makes the
// whole expression false, as in Redis.

/// A value an expression works on.
#[derive(Debug, Clone, PartialEq)]
enum Value {
    Num(f64),
    Str(String),
    Tuple(Vec<Value>),
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Open,
    Close,
    Not,
    Pow,
    Mul,
    Div,
    Mod,
    Add,
    Sub,
    Gt,
    Gte,
    Lt,
    Lte,
    Eq,
    Neq,
    In,
    And,
    Or,
}

/// The operators by name, as Redis's table has them.
const OPERATORS: &[(&str, Op)] = &[
    ("(", Op::Open),
    (")", Op::Close),
    ("!", Op::Not),
    ("not", Op::Not),
    ("**", Op::Pow),
    ("*", Op::Mul),
    ("/", Op::Div),
    ("%", Op::Mod),
    ("+", Op::Add),
    ("-", Op::Sub),
    (">", Op::Gt),
    (">=", Op::Gte),
    ("<", Op::Lt),
    ("<=", Op::Lte),
    ("==", Op::Eq),
    ("!=", Op::Neq),
    ("in", Op::In),
    ("and", Op::And),
    ("&&", Op::And),
    ("or", Op::Or),
    ("||", Op::Or),
];

const OPERATOR_CHARS: &str = "+-*%/!()<>=|&";

impl Op {
    fn precedence(self) -> u8 {
        match self {
            Op::Open | Op::Close => 7,
            Op::Not => 6,
            Op::Pow => 5,
            Op::Mul | Op::Div | Op::Mod => 4,
            Op::Add | Op::Sub => 3,
            Op::Gt | Op::Gte | Op::Lt | Op::Lte | Op::Eq | Op::Neq | Op::In => 2,
            Op::And => 1,
            Op::Or => 0,
        }
    }

    fn arity(self) -> usize {
        match self {
            Op::Not => 1,
            Op::Open | Op::Close => 0,
            _ => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Value(Value),
    Selector(String),
    Op(Op),
}

/// A compiled expression: its tokens in the order they are run (postfix).
#[derive(Debug)]
pub(crate) struct Filter(Vec<Token>);

/// Redis's reply to an expression it cannot compile.
pub(crate) const SYNTAX_ERROR: &str = "ERR syntax error in FILTER expression";

fn is_selector_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

fn tokenize(expr: &str) -> Option<Vec<Token>> {
    let chars: Vec<char> = expr.chars().collect();
    let mut tokens: Vec<Token> = Vec::new();
    let mut at = 0;
    let number = |at: &mut usize| -> Option<Value> {
        let start = *at;
        while *at < chars.len()
            && (chars[*at].is_ascii_digit()
                || matches!(chars[*at], '.' | 'e' | 'E')
                || (*at == start && chars[*at] == '-'))
        {
            *at += 1;
        }
        let text: String = chars[start..*at].iter().collect();
        text.parse().ok().map(Value::Num)
    };
    let string = |at: &mut usize| -> Option<Value> {
        let quote = chars[*at];
        *at += 1;
        let start = *at;
        while *at < chars.len() {
            if chars[*at] == '\\' && *at + 1 < chars.len() {
                *at += 2;
                continue;
            }
            if chars[*at] == quote {
                let text: String = chars[start..*at].iter().collect();
                *at += 1;
                return Some(Value::Str(text));
            }
            *at += 1;
        }
        None
    };
    loop {
        while at < chars.len() && chars[at].is_whitespace() {
            at += 1;
        }
        if at == chars.len() {
            return Some(tokens);
        }
        // A minus is a number's sign at the start, or after an operator
        // other than a closing parenthesis.
        let minus_is_number = match tokens.last() {
            None => true,
            Some(Token::Op(op)) => *op != Op::Close,
            Some(_) => false,
        };
        let c = chars[at];
        let token = if c.is_ascii_digit()
            || (minus_is_number
                && c == '-'
                && chars.get(at + 1).is_some_and(|d| d.is_ascii_digit()))
        {
            Token::Value(number(&mut at)?)
        } else if c == '"' || c == '\'' {
            Token::Value(string(&mut at)?)
        } else if c == '.' && chars.get(at + 1).is_some_and(|&d| is_selector_char(d)) {
            at += 1;
            let start = at;
            while at < chars.len() && is_selector_char(chars[at]) {
                at += 1;
            }
            Token::Selector(chars[start..at].iter().collect())
        } else if c == '[' {
            at += 1;
            let mut items = Vec::new();
            loop {
                while at < chars.len() && chars[at].is_whitespace() {
                    at += 1;
                }
                match chars.get(at) {
                    Some(']') => {
                        at += 1;
                        break;
                    }
                    Some(d) if d.is_ascii_digit() || *d == '-' => items.push(number(&mut at)?),
                    Some('"' | '\'') => items.push(string(&mut at)?),
                    _ => return None,
                }
                while at < chars.len() && chars[at].is_whitespace() {
                    at += 1;
                }
                match chars.get(at) {
                    Some(']') => {
                        at += 1;
                        break;
                    }
                    Some(',') => at += 1,
                    _ => return None,
                }
            }
            Token::Value(Value::Tuple(items))
        } else if c.is_ascii_alphabetic() || OPERATOR_CHARS.contains(c) {
            let start = at;
            while at < chars.len()
                && (chars[at].is_ascii_alphabetic() || OPERATOR_CHARS.contains(chars[at]))
            {
                at += 1;
            }
            let run: String = chars[start..at].iter().collect();
            if run == "null" {
                Token::Value(Value::Null)
            } else {
                // The longest operator the run starts with.
                let (name, op) = OPERATORS
                    .iter()
                    .filter(|(name, _)| run.starts_with(name))
                    .max_by_key(|(name, _)| name.len())?;
                at = start + name.chars().count();
                Token::Op(*op)
            }
        } else {
            return None;
        };
        tokens.push(token);
    }
}

impl Filter {
    /// Compile `expr`, or `None` if it is not one.
    pub(crate) fn compile(expr: &str) -> Option<Filter> {
        let mut program: Vec<Token> = Vec::new();
        let mut ops: Vec<Op> = Vec::new();
        for token in tokenize(expr)? {
            match token {
                Token::Op(Op::Open) => ops.push(Op::Open),
                Token::Op(Op::Close) => loop {
                    match ops.pop() {
                        Some(Op::Open) => break,
                        Some(op) => program.push(Token::Op(op)),
                        None => return None,
                    }
                },
                Token::Op(op) => {
                    while let Some(&top) = ops.last() {
                        if top == Op::Open {
                            break;
                        }
                        // Power and not are taken right to left.
                        let right = matches!(op, Op::Pow | Op::Not);
                        if top.precedence() < op.precedence()
                            || (right && top.precedence() == op.precedence())
                        {
                            break;
                        }
                        program.push(Token::Op(ops.pop()?));
                    }
                    ops.push(op);
                }
                other => program.push(other),
            }
        }
        while let Some(op) = ops.pop() {
            if op == Op::Open {
                return None;
            }
            program.push(Token::Op(op));
        }
        // Every operator has its operands, and one value is left.
        let mut depth = 0usize;
        for token in &program {
            match token {
                Token::Op(op) => {
                    depth = depth.checked_sub(op.arity())? + 1;
                }
                _ => depth += 1,
            }
        }
        (depth == 1).then_some(Filter(program))
    }

    /// Whether an element with these attributes (a JSON object, if any)
    /// passes.
    pub(crate) fn matches(&self, attributes: Option<&str>) -> bool {
        let object = attributes
            .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok())
            .and_then(|value| match value {
                serde_json::Value::Object(object) => Some(object),
                _ => None,
            });
        let mut stack: Vec<Value> = Vec::new();
        for token in &self.0 {
            match token {
                Token::Value(value) => stack.push(value.clone()),
                Token::Selector(field) => {
                    let Some(value) = object
                        .as_ref()
                        .and_then(|o| o.get(field))
                        .and_then(from_json)
                    else {
                        return false;
                    };
                    stack.push(value);
                }
                Token::Op(op) => {
                    let Some(b) = stack.pop() else {
                        return false;
                    };
                    let a = if op.arity() == 2 {
                        match stack.pop() {
                            Some(a) => a,
                            None => return false,
                        }
                    } else {
                        Value::Null
                    };
                    stack.push(Value::Num(apply(*op, &a, &b)));
                }
            }
        }
        stack.pop().is_some_and(|value| truthy(&value))
    }
}

/// A JSON attribute as an expression's value; `None` for one it cannot be (an
/// object, or a tuple of anything but numbers and strings).
fn from_json(value: &serde_json::Value) -> Option<Value> {
    Some(match value {
        serde_json::Value::Number(n) => Value::Num(n.as_f64()?),
        serde_json::Value::String(s) => Value::Str(s.clone()),
        serde_json::Value::Bool(b) => Value::Num(f64::from(u8::from(*b))),
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Array(items) => Value::Tuple(
            items
                .iter()
                .map(|item| match item {
                    serde_json::Value::Number(_) | serde_json::Value::String(_) => from_json(item),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?,
        ),
        serde_json::Value::Object(_) => return None,
    })
}

fn number(value: &Value) -> f64 {
    match value {
        Value::Num(n) => *n,
        Value::Str(s) => s.parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Num(n) => *n != 0.0,
        Value::Str(s) => !s.is_empty(),
        Value::Null => false,
        Value::Tuple(_) => true,
    }
}

fn equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Str(a), Value::Str(b)) => a == b,
        (Value::Num(a), Value::Num(b)) => a == b,
        (Value::Null, other) | (other, Value::Null) => *other == Value::Null,
        _ => number(a) == number(b),
    }
}

fn apply(op: Op, a: &Value, b: &Value) -> f64 {
    let yes = |test: bool| f64::from(u8::from(test));
    match op {
        Op::Not => yes(!truthy(b)),
        Op::Pow => number(a).powf(number(b)),
        Op::Mul => number(a) * number(b),
        Op::Div => number(a) / number(b),
        Op::Mod => number(a) % number(b),
        Op::Add => number(a) + number(b),
        Op::Sub => number(a) - number(b),
        Op::Gt => yes(number(a) > number(b)),
        Op::Gte => yes(number(a) >= number(b)),
        Op::Lt => yes(number(a) < number(b)),
        Op::Lte => yes(number(a) <= number(b)),
        Op::Eq => yes(equal(a, b)),
        Op::Neq => yes(!equal(a, b)),
        Op::In => match (a, b) {
            (_, Value::Tuple(items)) => yes(items.iter().any(|item| equal(a, item))),
            (Value::Str(needle), Value::Str(hay)) => yes(hay.contains(needle.as_str())),
            _ => 0.0,
        },
        Op::And => yes(truthy(a) && truthy(b)),
        Op::Or => yes(truthy(a) || truthy(b)),
        Op::Open | Op::Close => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::Filter;

    fn passes(expr: &str, attributes: &str) -> bool {
        Filter::compile(expr)
            .unwrap_or_else(|| panic!("{expr}"))
            .matches(Some(attributes))
    }

    #[test]
    fn redis_s_expressions() {
        let red = r#"{"color":"red","size":3,"tags":["a","b"],"ok":true,"none":null}"#;
        assert!(passes(r#".color == "red""#, red));
        assert!(!passes(r#".color == "blue""#, red));
        assert!(passes(".size > 2 and .size <= 3", red));
        assert!(passes(".size * 2 + 1 == 7", red));
        assert!(passes("2 ** 3 ** 2 == 512", red));
        assert!(passes("-1 < .size", red));
        assert!(passes(r#""a" in .tags"#, red));
        assert!(passes(r#".color in ["red", "green"]"#, red));
        assert!(passes(r#""ed" in .color"#, red));
        assert!(passes(".ok && !(.size < 1)", red));
        assert!(passes("not .none", red));
        assert!(passes(".none == null", red));
        assert!(passes(r#".size == "3""#, red));
        // A field the element does not have: false, whatever else.
        assert!(!passes(".missing == 0 or 1", red));
        // No attributes: true if the expression asks for none.
        assert!(Filter::compile("1").unwrap().matches(None));
        assert!(!Filter::compile(".a").unwrap().matches(None));
        for bad in ["(.a", ".a ==", "== 1", r#""open"#, "1 2", "foo"] {
            assert!(Filter::compile(bad).is_none(), "{bad}");
        }
    }
}
