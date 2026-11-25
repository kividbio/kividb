// src/commands/lua_keys_only.rs
//
// Whether a script keeps to its `KEYS`: read from its source, before it
// runs, once per thread.
//
// A script whose `KEYS` carry no hash tag runs alone (`tag_locks`): it may
// build key names of its own, and nothing it touches is locked against the
// commands of other clients. Most short scripts do neither. A lock, a
// compare-and-delete, a rate limiter: every key they give `redis.call` is
// `KEYS[1]` or `KEYS[2]`, written so. Such a script can be held to its keys
// as a tagged script is held to its tags, and run beside others under each
// key's lock, without anything it does changing.
//
// This says "keeps to its keys" only when the source shows it, token by
// token, and "no" to everything else, which then runs alone as before:
//
// * every `redis.call(...)` and `redis.pcall(...)` names its command with a
//   string literal, of the commands in `key_rule`, and gives each argument
//   that command takes as a key exactly as `KEYS[<number>]`;
// * `redis` is used for nothing else but its other functions
//   (`redis.sha1hex`, `redis.log`, ...): never as a value, never assigned;
// * `KEYS` is used only as `KEYS[<number>]`, where it is read: never
//   assigned, passed on, iterated, measured or shadowed;
// * nothing that reaches the globals, or functions, another way is named
//   (`_G`, `getfenv`, `setfenv`, `loadstring`, `load`, `dofile`,
//   `loadfile`, `rawset`, `rawget`, `setmetatable`, `getmetatable`,
//   `debug`, `require`, `module`, `package`, and `_ENV`, which in Lua 5.4
//   decides what a global name such as `KEYS` is).
//
// The script is still held to its keys while it runs, by the server: a key
// outside them is refused, as a key outside a tagged script's tags is.

/// One token of a Lua 5.1 chunk; strings and comments are read past, never
/// looked into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tok<'a> {
    Name(&'a str),
    /// A short string: its text, and whether it has no escape in it.
    Str(&'a str, bool),
    /// A long string (`[[...]]`).
    LongStr,
    Num(&'a str),
    Op(&'a str),
}

const BANNED: [&str; 16] = [
    "_G",
    "_ENV",
    "getfenv",
    "setfenv",
    "loadstring",
    "load",
    "dofile",
    "loadfile",
    "rawset",
    "rawget",
    "setmetatable",
    "getmetatable",
    "debug",
    "require",
    "module",
    "package",
];

/// After which tokens, outside any bracket, `KEYS[n]` is read as a value
/// and cannot be the start of an assignment to it.
fn reads_what_follows(tok: Option<Tok>) -> bool {
    match tok {
        Some(Tok::Op(op)) => matches!(
            op,
            "=" | "=="
                | "~="
                | "<"
                | ">"
                | "<="
                | ">="
                | ".."
                | "+"
                | "-"
                | "*"
                | "/"
                | "%"
                | "^"
                | "#"
        ),
        Some(Tok::Name(word)) => matches!(
            word,
            "return" | "and" | "or" | "not" | "if" | "elseif" | "while" | "until"
        ),
        _ => false,
    }
}

/// Which arguments of a command are keys: the first only, or every one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyRule {
    First,
    All,
}

/// The commands a script that keeps to its keys may call, and where their
/// keys are. Only commands whose keys are always where this says, whatever
/// their options.
fn key_rule(name: &str) -> Option<KeyRule> {
    let upper = name.to_ascii_uppercase();
    let first = matches!(
        upper.as_str(),
        "GET"
            | "SET"
            | "SETNX"
            | "SETEX"
            | "PSETEX"
            | "GETSET"
            | "GETDEL"
            | "GETEX"
            | "INCR"
            | "INCRBY"
            | "DECR"
            | "DECRBY"
            | "INCRBYFLOAT"
            | "APPEND"
            | "STRLEN"
            | "GETRANGE"
            | "SETRANGE"
            | "TTL"
            | "PTTL"
            | "EXPIRE"
            | "PEXPIRE"
            | "EXPIREAT"
            | "PEXPIREAT"
            | "EXPIRETIME"
            | "PEXPIRETIME"
            | "PERSIST"
            | "TYPE"
            | "HGET"
            | "HSET"
            | "HSETNX"
            | "HMSET"
            | "HMGET"
            | "HDEL"
            | "HEXISTS"
            | "HINCRBY"
            | "HINCRBYFLOAT"
            | "HGETALL"
            | "HKEYS"
            | "HVALS"
            | "HLEN"
            | "HSTRLEN"
            | "LPUSH"
            | "RPUSH"
            | "LPUSHX"
            | "RPUSHX"
            | "LPOP"
            | "RPOP"
            | "LLEN"
            | "LRANGE"
            | "LINDEX"
            | "LSET"
            | "LREM"
            | "LTRIM"
            | "LINSERT"
            | "SADD"
            | "SREM"
            | "SISMEMBER"
            | "SMISMEMBER"
            | "SMEMBERS"
            | "SCARD"
            | "SPOP"
            | "SRANDMEMBER"
            | "ZADD"
            | "ZREM"
            | "ZSCORE"
            | "ZMSCORE"
            | "ZINCRBY"
            | "ZCARD"
            | "ZCOUNT"
            | "ZRANGEBYSCORE"
            | "ZRANGE"
            | "ZREVRANGE"
            | "ZREVRANGEBYSCORE"
            | "ZRANK"
            | "ZREVRANK"
            | "ZREMRANGEBYSCORE"
            | "ZREMRANGEBYRANK"
            | "ZPOPMIN"
            | "ZPOPMAX"
            | "LPOS"
    );
    if first {
        return Some(KeyRule::First);
    }
    matches!(
        upper.as_str(),
        "DEL" | "UNLINK" | "EXISTS" | "MGET" | "TOUCH"
    )
    .then_some(KeyRule::All)
}

/// The tokens of `src`, or `None` if it is not one this can read through.
fn tokens(src: &str) -> Option<Vec<Tok<'_>>> {
    let bytes = src.as_bytes();
    let mut out = Vec::new();
    let mut at = 0;
    // `[`, any number of `=`, `[`: the level of a long bracket, or None.
    let long_open = |at: usize| -> Option<usize> {
        if bytes.get(at) != Some(&b'[') {
            return None;
        }
        let mut level = 0;
        while bytes.get(at + 1 + level) == Some(&b'=') {
            level += 1;
        }
        (bytes.get(at + 1 + level) == Some(&b'[')).then_some(level)
    };
    // Past the long bracket of `level` opened at `at`.
    let long_close = |at: usize, level: usize| -> Option<usize> {
        let mut close = Vec::with_capacity(level + 2);
        close.push(b']');
        close.extend(std::iter::repeat_n(b'=', level));
        close.push(b']');
        let from = at + level + 2;
        let rest = bytes.get(from..)?;
        rest.windows(close.len())
            .position(|window| window == close.as_slice())
            .map(|found| from + found + close.len())
    };
    while at < bytes.len() {
        let c = bytes[at];
        if c.is_ascii_whitespace() {
            at += 1;
            continue;
        }
        if c == b'-' && bytes.get(at + 1) == Some(&b'-') {
            at += 2;
            if let Some(level) = long_open(at) {
                at = long_close(at, level)?;
            } else {
                while at < bytes.len() && bytes[at] != b'\n' {
                    at += 1;
                }
            }
            continue;
        }
        if let Some(level) = long_open(at) {
            at = long_close(at, level)?;
            out.push(Tok::LongStr);
            continue;
        }
        if c == b'"' || c == b'\'' {
            let start = at + 1;
            let mut plain = true;
            let mut end = start;
            loop {
                match bytes.get(end) {
                    None | Some(b'\n') => return None,
                    Some(&b) if b == c => break,
                    Some(b'\\') => {
                        plain = false;
                        end += 2;
                    }
                    Some(_) => end += 1,
                }
            }
            out.push(Tok::Str(src.get(start..end)?, plain));
            at = end + 1;
            continue;
        }
        if c.is_ascii_digit() || (c == b'.' && bytes.get(at + 1).is_some_and(u8::is_ascii_digit)) {
            let start = at;
            while at < bytes.len() {
                let b = bytes[at];
                let signed = (b == b'+' || b == b'-')
                    && matches!(bytes[at - 1], b'e' | b'E')
                    && !src[start..at].starts_with("0x")
                    && !src[start..at].starts_with("0X");
                if b.is_ascii_alphanumeric() || b == b'.' || signed {
                    at += 1;
                } else {
                    break;
                }
            }
            out.push(Tok::Num(&src[start..at]));
            continue;
        }
        if c.is_ascii_alphabetic() || c == b'_' {
            let start = at;
            while at < bytes.len() && (bytes[at].is_ascii_alphanumeric() || bytes[at] == b'_') {
                at += 1;
            }
            out.push(Tok::Name(&src[start..at]));
            continue;
        }
        let three = src.get(at..at + 3);
        let two = src.get(at..at + 2);
        let op = if three == Some("...") {
            "..."
        } else if let Some(two) = two.filter(|two| ["..", "==", "~=", "<=", ">="].contains(two)) {
            two
        } else if b"+-*/%^#<>=(){}[];:,.".contains(&c) {
            &src[at..at + 1]
        } else {
            return None;
        };
        at += op.len();
        out.push(Tok::Op(op));
    }
    Some(out)
}

/// What the tokens are inside of, innermost last.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Within {
    Paren,
    Bracket,
    Brace,
    /// `function`, `if`, `do`, `repeat`: statements.
    Block,
}

/// Whether the script `source` (with or without its `#!` line) keeps to its
/// `KEYS`: see the top.
pub(crate) fn keeps_to_its_keys(source: &str) -> bool {
    let body = if source.starts_with("#!") {
        match source.find('\n') {
            Some(nl) => &source[nl..],
            None => return false,
        }
    } else {
        source
    };
    let Some(toks) = tokens(body) else {
        return false;
    };
    let mut within: Vec<Within> = Vec::new();
    let at = |i: usize| toks.get(i).copied();
    for (i, &tok) in toks.iter().enumerate() {
        let prev = i.checked_sub(1).and_then(at);
        let a_field = matches!(prev, Some(Tok::Op(".") | Tok::Op(":")));
        match tok {
            Tok::Op("(") => within.push(Within::Paren),
            Tok::Op("[") => within.push(Within::Bracket),
            Tok::Op("{") => within.push(Within::Brace),
            Tok::Op(")") if within.pop() != Some(Within::Paren) => return false,
            Tok::Op("]") if within.pop() != Some(Within::Bracket) => return false,
            Tok::Op("}") if within.pop() != Some(Within::Brace) => return false,
            Tok::Name("function" | "if" | "do" | "repeat") if !a_field => {
                within.push(Within::Block)
            }
            Tok::Name("end" | "until") if !a_field && within.pop() != Some(Within::Block) => {
                return false;
            }
            Tok::Name(name) if !a_field && BANNED.contains(&name) => return false,
            Tok::Name("KEYS") if !a_field => {
                if !is_key_read(&toks, i) {
                    return false;
                }
                // Outside any bracket, where a statement may start: read as
                // a value only after what reads it.
                let in_expression = matches!(
                    within.last(),
                    Some(Within::Paren | Within::Bracket | Within::Brace)
                );
                if !in_expression && !reads_what_follows(prev) {
                    return false;
                }
            }
            Tok::Name("redis") if !a_field => {
                let Some(Tok::Op(".")) = at(i + 1) else {
                    return false;
                };
                match at(i + 2) {
                    Some(Tok::Name("call" | "pcall")) => {
                        if !call_keeps_to_keys(&toks, i + 3) {
                            return false;
                        }
                    }
                    Some(Tok::Name(_)) => {}
                    _ => return false,
                }
            }
            _ => {}
        }
    }
    within.is_empty()
}

/// Whether `KEYS` at `i` is `KEYS[<decimal number>]`, not assigned to.
fn is_key_read(toks: &[Tok], i: usize) -> bool {
    matches!(
        (toks.get(i + 1), toks.get(i + 2), toks.get(i + 3)),
        (Some(Tok::Op("[")), Some(Tok::Num(n)), Some(Tok::Op("]")))
            if !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit())
    ) && toks.get(i + 4) != Some(&Tok::Op("="))
}

/// The arguments of the call whose `(` is at `open`: each the tokens
/// between two commas outside any bracket or block, or `None` if the call
/// is not closed.
fn call_arguments<'t, 'a>(toks: &'t [Tok<'a>], open: usize) -> Option<Vec<&'t [Tok<'a>]>> {
    if toks.get(open) != Some(&Tok::Op("(")) {
        return None;
    }
    let mut depth = 0usize;
    let mut args = Vec::new();
    let mut start = open + 1;
    for (i, tok) in toks.iter().enumerate().skip(open + 1) {
        let field = matches!(toks.get(i - 1), Some(Tok::Op(".") | Tok::Op(":")));
        match *tok {
            Tok::Op("(" | "[" | "{") => depth += 1,
            Tok::Name("function" | "if" | "do" | "repeat") if !field => depth += 1,
            Tok::Name("end" | "until") if !field => depth = depth.checked_sub(1)?,
            Tok::Op(")") if depth == 0 => {
                if i > start || !args.is_empty() {
                    args.push(&toks[start..i]);
                }
                return Some(args);
            }
            Tok::Op(")" | "]" | "}") => depth = depth.checked_sub(1)?,
            Tok::Op(",") if depth == 0 => {
                args.push(&toks[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    None
}

/// Whether the `redis.call`/`redis.pcall` whose `(` is at `open` names a
/// command of `key_rule` with a literal, and each of its keys as
/// `KEYS[<number>]`.
fn call_keeps_to_keys(toks: &[Tok], open: usize) -> bool {
    let Some(args) = call_arguments(toks, open) else {
        return false;
    };
    let Some((name, rest)) = args.split_first() else {
        return false;
    };
    let rule = match name {
        [Tok::Str(name, true)] => key_rule(name),
        _ => None,
    };
    let Some(rule) = rule else {
        return false;
    };
    let is_key = |arg: &[Tok]| arg.len() == 4 && arg[0] == Tok::Name("KEYS") && is_key_read(arg, 0);
    match rule {
        KeyRule::First => rest.first().is_none_or(|key| is_key(key)),
        KeyRule::All => rest.iter().all(|key| is_key(key)),
    }
}

#[cfg(test)]
mod tests {
    use super::keeps_to_its_keys as keeps;

    #[test]
    fn scripts_that_name_only_their_keys_keep_to_them() {
        assert!(keeps(
            r#"if redis.call("GET",KEYS[1])==ARGV[1] then return redis.call("DEL",KEYS[1]) else return 0 end"#
        ));
        assert!(keeps(
            "local current = redis.call('INCRBY', KEYS[1], ARGV[1])\n\
             if tonumber(current) == tonumber(ARGV[1]) then\n\
               redis.call('PEXPIRE', KEYS[1], ARGV[2])\n\
             end\n\
             return current"
        ));
        assert!(keeps(
            "#!lua flags=no-writes\nreturn redis.call('get', KEYS[1])"
        ));
        assert!(keeps(
            "if redis.call('exists', KEYS[1], KEYS[2]) == 0 then\n\
               redis.call('hset', KEYS[1], 'a', ARGV[1])\n\
               return redis.pcall('hget', KEYS[1], 'a')\n\
             end\n\
             -- redis.call('SET', 'elsewhere', 1)\n\
             return redis.sha1hex('x') .. \"redis.call('GET', ARGV[1])\""
        ));
        assert!(keeps(
            "local k = #KEYS[1] return redis.call('STRLEN', KEYS[1]) + k"
        ));
        assert!(keeps(
            "local t = {KEYS[1], [KEYS[2]] = 1} return redis.call('MGET', KEYS[1], KEYS[2])"
        ));
    }

    #[test]
    fn a_key_built_or_kept_elsewhere_does_not() {
        // Built by the script.
        assert!(!keeps("return redis.call('GET', 'q:' .. ARGV[1])"));
        assert!(!keeps("return redis.call('GET', ARGV[1])"));
        // Through a local, or a loop.
        assert!(!keeps("local k = KEYS[1] return redis.call('GET', k)"));
        assert!(!keeps("for i = 1, #KEYS do redis.call('DEL', KEYS[i]) end"));
        assert!(!keeps("return redis.call('DEL', unpack(KEYS))"));
        // A command by a value, or one whose keys are elsewhere.
        assert!(!keeps("return redis.call(ARGV[1], KEYS[1])"));
        assert!(!keeps("return redis.call('RENAME', KEYS[1], KEYS[2])"));
        assert!(!keeps("return redis.call('SORT', KEYS[1], 'BY', 'w_*')"));
        assert!(!keeps("return redis.call('KEYS', '*')"));
        assert!(!keeps("return redis.call('EVAL', 'return 1', 0)"));
        assert!(!keeps("return redis.call('G\\x45T', KEYS[1])"));
        assert!(!keeps("return redis.call [[GET]]"));
    }

    #[test]
    fn keys_or_redis_changed_or_passed_on_do_not() {
        assert!(!keeps(
            "KEYS[1] = 'other' return redis.call('GET', KEYS[1])"
        ));
        assert!(!keeps("KEYS = {'other'} return redis.call('GET', KEYS[1])"));
        assert!(!keeps(
            "local KEYS = {'other'} return redis.call('GET', KEYS[1])"
        ));
        assert!(!keeps(
            "x, KEYS[1] = 1, 'other' return redis.call('GET', KEYS[1])"
        ));
        assert!(!keeps(
            "local x = 1 KEYS[1] = 'other' return redis.call('GET', KEYS[1])"
        ));
        assert!(!keeps(
            "local f = function() KEYS[1] = 'other' end f() return redis.call('GET', KEYS[1])"
        ));
        assert!(!keeps(
            "if 1 then KEYS[1] = 'o' end return redis.call('GET', KEYS[1])"
        ));
        assert!(!keeps(
            "table.insert(KEYS, 'o') return redis.call('GET', KEYS[1])"
        ));
        assert!(!keeps(
            "local f = function(KEYS) return redis.call('GET', KEYS[1]) end"
        ));
        assert!(!keeps("local r = redis return r.call('GET', 'x')"));
        assert!(!keeps(
            "redis.call = function() end return redis.call('GET', KEYS[1])"
        ));
        assert!(!keeps("local c = redis.call return c('GET', 'x')"));
        assert!(!keeps("return pcall(redis.call, 'GET', 'x')"));
        assert!(!keeps("return redis:call('GET', KEYS[1])"));
        assert!(!keeps("return _G.redis.call('GET', 'x')"));
        assert!(!keeps(
            "return loadstring('return redis.call(\"GET\", \"x\")')()"
        ));
        assert!(!keeps("setmetatable(KEYS, {}) return 1"));
        assert!(!keeps("return rawget(_G, 'KEYS')[1]"));
        assert!(!keeps("return getfenv(1).KEYS[1]"));
        assert!(!keeps(
            "local _ENV = {KEYS = {'o'}, redis = redis} return redis.call('GET', KEYS[1])"
        ));
    }

    #[test]
    fn what_cannot_be_read_through_does_not() {
        assert!(!keeps("return redis.call('GET', KEYS[1]"));
        assert!(!keeps("return 'open"));
        assert!(!keeps("if x then return 1"));
        assert!(!keeps("--[[ open"));
        assert!(!keeps("#!lua"));
        assert!(!keeps("return redis.call('GET', KEYS[1]) end"));
    }
}
