// src/keyspec.rs
//
// Which arguments of a command are keys.
//
// Three places need to know: cluster routing (which slot does this command
// touch?), ACL key-pattern checks (may this user touch these keys?), and
// `COMMAND GETKEYS`. They used to answer separately, and each was wrong in its
// own way:
//
//   * Routing kept its own table, which had drifted. `LMPOP`, `ZMPOP`,
//     `XGROUP`, `XINFO` and `OBJECT` were listed as "key at position 1", so
//     the slot was computed from `numkeys` or the subcommand name and the
//     command was redirected to whichever node owns the slot of the string
//     "2" or "CREATE". Commands missing from the table altogether — `DELEX`,
//     `MSETEX`, `INCREX`, every array command, `ZREMRANGEBY*`, `TS.MADD` —
//     fell through to "no keys" and ran on whatever node received them, slot
//     ownership unchecked.
//   * The ACL check looked at `cmd[1]` and nothing else. A user restricted to
//     `~app:*` could read any key in the database with
//     `MGET app:x anything-else`, and conversely could not run
//     `XGROUP CREATE app:stream ...` because "CREATE" is not under `app:`.
//
// One table, here, used by all three.

/// The key positions of one command invocation, as indices into the command's
/// argument vector (index 0 is the command name).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Keys {
    /// No key arguments: connection, server and introspection commands.
    None,
    /// Exactly one key.
    One(usize),
    /// Exactly two keys.
    Two(usize, usize),
    /// `first, first + step, ...` while `<= last`.
    Range {
        first: usize,
        last: usize,
        step: usize,
    },
    /// Anything that does not fit the shapes above.
    List(Vec<usize>),
}

impl Keys {
    /// The key positions in order.
    pub fn iter(&self) -> KeyIter<'_> {
        match self {
            Keys::None => KeyIter::Range {
                next: 1,
                last: 0,
                step: 1,
            },
            Keys::One(a) => KeyIter::Range {
                next: *a,
                last: *a,
                step: 1,
            },
            Keys::Two(a, b) => KeyIter::Pair(Some(*a), Some(*b)),
            Keys::Range { first, last, step } => KeyIter::Range {
                next: *first,
                last: *last,
                step: (*step).max(1),
            },
            Keys::List(v) => KeyIter::List(v.iter()),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    pub fn first(&self) -> Option<usize> {
        self.iter().next()
    }
}

pub enum KeyIter<'a> {
    Pair(Option<usize>, Option<usize>),
    Range {
        next: usize,
        last: usize,
        step: usize,
    },
    List(std::slice::Iter<'a, usize>),
}

impl Iterator for KeyIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        match self {
            KeyIter::Pair(a, b) => a.take().or_else(|| b.take()),
            KeyIter::Range { next, last, step } => {
                if *next > *last {
                    return None;
                }
                let cur = *next;
                *next += *step;
                Some(cur)
            }
            KeyIter::List(it) => it.next().copied(),
        }
    }
}

/// Where a command's keys are, in the terms `COMMAND` reports: the three
/// numbers a client can apply to any invocation without asking again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    /// The command takes no keys.
    NoKeys,
    /// The key at `first`, then every `step`-th argument up to `last`. A
    /// negative `last` counts from the end: -1 is the last argument.
    Fixed { first: i64, last: i64, step: i64 },
    /// The positions depend on the arguments — a count of keys, or an option
    /// that names one. A client has to ask `COMMAND GETKEYS`.
    Movable,
}

/// The [`Layout`] of `name` (upper case).
///
/// It is read off [`key_positions`] rather than kept as a second table that
/// could disagree with it: the command is tried with argument lists of two
/// lengths, made of words and made of numbers, and the positions that come
/// back either follow one fixed pattern or they do not.
pub fn layout(name: &str) -> Layout {
    // Keys named by an option (`STREAMS`, `STORE`, `KEYS`, a subcommand):
    // made-up arguments never contain it, so these cannot be found by trying.
    if matches!(
        name,
        "XREAD"
            | "XREADGROUP"
            | "SORT"
            | "SORT_RO"
            | "GEORADIUS"
            | "GEORADIUSBYMEMBER"
            | "MIGRATE"
            | "MEMORY"
            | "HIMPORT"
    ) {
        return Layout::Movable;
    }
    // Lengths whose argument counts are multiples of 2 and of 3, so that a
    // command taking pairs or triples sees whole ones.
    const SHORT: usize = 7;
    const LONG: usize = 13;
    let positions = |len: usize, arg: &'static [u8]| -> Vec<usize> {
        let mut cmd: Vec<&[u8]> = vec![arg; len];
        cmd[0] = name.as_bytes();
        key_positions(name, &cmd).iter().collect()
    };
    let (short, long) = (positions(SHORT, b"a"), positions(LONG, b"a"));
    // A command that counts its keys (`numkeys`) answers differently when
    // its arguments are numbers.
    if short != positions(SHORT, b"2") || long != positions(LONG, b"2") {
        return Layout::Movable;
    }
    if short.is_empty() && long.is_empty() {
        return Layout::NoKeys;
    }
    // (first, last, step) of an evenly spaced run, or nothing.
    let run = |positions: &[usize]| -> Option<(i64, i64, i64)> {
        let (first, last) = (*positions.first()? as i64, *positions.last()? as i64);
        let step = match positions {
            [a, b, ..] => *b as i64 - *a as i64,
            _ => 1,
        };
        let even = positions
            .windows(2)
            .all(|pair| pair[1] as i64 - pair[0] as i64 == step);
        (even && step > 0).then_some((first, last, step))
    };
    let (Some((first, short_last, step)), Some(long)) = (run(&short), run(&long)) else {
        return Layout::Movable;
    };
    if (first, step) != (long.0, long.2) {
        return Layout::Movable;
    }
    if short_last == long.1 {
        // The same place however long the command is.
        Layout::Fixed {
            first,
            last: short_last,
            step,
        }
    } else if short_last - SHORT as i64 == long.1 - LONG as i64 {
        // The same distance from the end.
        Layout::Fixed {
            first,
            last: short_last - SHORT as i64,
            step,
        }
    } else {
        Layout::Movable
    }
}

fn parse_usize(arg: &[u8]) -> Option<usize> {
    std::str::from_utf8(arg).ok()?.parse().ok()
}

/// `count` keys starting at `first`, where `count` is read from `cmd[numkeys_at]`.
/// `None`-shaped (no keys) if the count is missing, zero, or runs past the end
/// of the command — the handler will reject the command itself.
fn numkeys(cmd: &[&[u8]], numkeys_at: usize, first: usize) -> Keys {
    match cmd.get(numkeys_at).and_then(|a| parse_usize(a)) {
        Some(n) if n > 0 && n <= cmd.len().saturating_sub(first) => Keys::Range {
            first,
            last: first + n - 1,
            step: 1,
        },
        _ => Keys::None,
    }
}

/// Every argument from `first` to the end.
fn rest(cmd: &[&[u8]], first: usize) -> Keys {
    if cmd.len() > first {
        Keys::Range {
            first,
            last: cmd.len() - 1,
            step: 1,
        }
    } else {
        Keys::None
    }
}

/// Position of an option keyword at or after `from`, case-insensitively.
fn find_option(cmd: &[&[u8]], from: usize, option: &[u8]) -> Option<usize> {
    cmd.iter()
        .enumerate()
        .skip(from)
        .find(|(_, a)| a.eq_ignore_ascii_case(option))
        .map(|(i, _)| i)
}

/// The key positions of `cmd`. `name` is the command name, already uppercased.
///
/// A command this table does not know has no keys as far as it is concerned;
/// the dispatcher will reject a genuinely unknown command on its own.
pub fn key_positions(name: &str, cmd: &[&[u8]]) -> Keys {
    use Keys::*;

    if cmd.len() < 2 {
        return None;
    }

    // The commands of the hot path, before the long match below: what that
    // costs depends on how the compiler lays it out, and one build of 1.2.0
    // spent 2,600 instructions a `DEL` getting to its arm (380 in the build
    // before). Each answers here what its arm below does.
    match name {
        "GET" | "SET" | "INCR" | "DECR" | "INCRBY" | "EXPIRE" | "HGET" | "HSET" | "LPUSH"
        | "RPUSH" | "LPOP" | "RPOP" | "SADD" | "ZADD" => return One(1),
        "DEL" | "UNLINK" | "EXISTS" | "MGET" => return rest(cmd, 1),
        _ => {}
    }

    match name {
        // ── One key, first argument ──────────────────────────────────────
        // Strings
        "GET" | "SET" | "SETNX" | "SETEX" | "PSETEX" | "GETSET" | "GETDEL" | "GETEX"
        | "APPEND" | "STRLEN" | "INCR" | "DECR" | "INCRBY" | "DECRBY" | "INCRBYFLOAT"
        | "INCREX" | "SETRANGE" | "GETRANGE" | "SUBSTR" | "DELEX" | "DIGEST"
        // Bitmaps / HyperLogLog (single-key forms)
        | "SETBIT" | "GETBIT" | "BITCOUNT" | "BITPOS" | "BITFIELD" | "BITFIELD_RO" | "PFADD"
        // Generic
        | "TTL" | "PTTL" | "TYPE" | "PERSIST" | "DUMP" | "RESTORE" | "RESTORE-ASKING"
        | "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" | "EXPIRETIME" | "PEXPIRETIME"
        | "SORT_RO" | "MOVE"
        // Hashes
        | "HGET" | "HSET" | "HMGET" | "HMSET" | "HSETNX" | "HDEL" | "HINCRBY"
        | "HINCRBYFLOAT" | "HGETALL" | "HKEYS" | "HVALS" | "HLEN" | "HEXISTS" | "HRANDFIELD"
        | "HSCAN" | "HSTRLEN" | "HEXPIRE" | "HPEXPIRE" | "HEXPIREAT" | "HPEXPIREAT" | "HTTL"
        | "HPTTL" | "HEXPIRETIME" | "HPEXPIRETIME" | "HPERSIST" | "HGETDEL" | "HGETEX"
        | "HSETEX"
        // Lists
        | "LLEN" | "LRANGE" | "LINDEX" | "LPOS" | "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX"
        | "LPOP" | "RPOP" | "LSET" | "LINSERT" | "LREM" | "LTRIM"
        // Sets
        | "SCARD" | "SMEMBERS" | "SRANDMEMBER" | "SISMEMBER" | "SMISMEMBER" | "SSCAN"
        | "SADD" | "SREM" | "SPOP"
        // Sorted sets
        | "ZCARD" | "ZRANGE" | "ZRANGEBYSCORE" | "ZRANGEBYLEX" | "ZREVRANGE"
        | "ZREVRANGEBYSCORE" | "ZREVRANGEBYLEX" | "ZRANK" | "ZREVRANK" | "ZSCORE" | "ZMSCORE"
        | "ZCOUNT" | "ZLEXCOUNT" | "ZRANDMEMBER" | "ZSCAN" | "ZADD" | "ZREM" | "ZINCRBY"
        | "ZPOPMIN" | "ZPOPMAX" | "ZREMRANGEBYLEX" | "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE"
        // Streams
        | "XLEN" | "XRANGE" | "XREVRANGE" | "XPENDING" | "XADD" | "XDEL" | "XDELEX" | "XTRIM"
        | "XSETID" | "XACK" | "XACKDEL" | "XNACK" | "XAUTOCLAIM" | "XCLAIM" | "XCFGSET" | "XIDMPRECORD"
        // Geo
        | "GEOADD" | "GEODIST" | "GEOPOS" | "GEOHASH" | "GEOSEARCH" | "GEORADIUS_RO"
        | "GEORADIUSBYMEMBER_RO"
        // JSON
        | "JSON.GET" | "JSON.SET" | "JSON.DEL" | "JSON.FORGET" | "JSON.TYPE"
        | "JSON.NUMINCRBY" | "JSON.NUMMULTBY" | "JSON.STRAPPEND" | "JSON.STRLEN"
        | "JSON.ARRAPPEND" | "JSON.ARRINDEX" | "JSON.ARRINSERT" | "JSON.ARRLEN"
        | "JSON.ARRPOP" | "JSON.ARRTRIM" | "JSON.OBJKEYS" | "JSON.OBJLEN" | "JSON.CLEAR"
        | "JSON.TOGGLE" | "JSON.MERGE" | "JSON.RESP"
        // Vector sets
        | "VADD" | "VREM" | "VCARD" | "VDIM" | "VINFO" | "VISMEMBER" | "VEMB" | "VGETATTR"
        | "VSETATTR" | "VLINKS" | "VRANDMEMBER" | "VRANGE" | "VSIM"
        // Time series (single-series forms)
        | "TS.CREATE" | "TS.ALTER" | "TS.ADD" | "TS.INCRBY" | "TS.DECRBY" | "TS.DEL"
        | "TS.GET" | "TS.RANGE" | "TS.REVRANGE" | "TS.INFO" | "TS.READ"
        // Arrays (Redis 8.8)
        | "ARSET" | "ARMSET" | "ARGET" | "ARMGET" | "ARGETRANGE" | "ARDEL" | "ARDELRANGE"
        | "ARLEN" | "ARCOUNT" | "ARINSERT" | "ARRING" | "ARNEXT" | "ARSEEK" | "ARSCAN"
        | "ARGREP" | "AROP" | "ARLASTITEMS" | "ARINFO"
        // Sharded pub/sub: the channel is hashed to a slot exactly like a key.
        | "SPUBLISH" => One(1),

        // ── Two keys: source, destination ────────────────────────────────
        "RENAME" | "RENAMENX" | "COPY" | "LMOVE" | "RPOPLPUSH" | "SMOVE" | "BRPOPLPUSH"
        | "BLMOVE" | "LMOVEM" | "BLMOVEM" | "GEOSEARCHSTORE" | "ZRANGESTORE" | "LCS"
        | "TS.CREATERULE" | "TS.DELETERULE" => {
            if cmd.len() >= 3 {
                Two(1, 2)
            } else {
                None
            }
        }

        // ── Every argument is a key ──────────────────────────────────────
        "MGET" | "DEL" | "UNLINK" | "EXISTS" | "TOUCH" | "PFCOUNT" | "PFMERGE" | "SUNION"
        | "SINTER" | "SDIFF" | "SUNIONSTORE" | "SINTERSTORE" | "SDIFFSTORE" | "WATCH" => {
            rest(cmd, 1)
        }

        // ── Alternating key / value ──────────────────────────────────────
        "MSET" | "MSETNX" => Range {
            first: 1,
            last: cmd.len() - 1,
            step: 2,
        },
        // MSETEX numkeys key value [key value ...] [options]
        "MSETEX" => match cmd.get(1).and_then(|n| parse_usize(n)) {
            Some(n) if n > 0 && n <= cmd.len().saturating_sub(1) / 2 => Range {
                first: 2,
                last: 2 + 2 * (n - 1),
                step: 2,
            },
            _ => None,
        },
        // JSON.MSET key path value [key path value ...]
        "JSON.MSET" => Range {
            first: 1,
            last: cmd.len() - 1,
            step: 3,
        },
        // TS.MADD key timestamp value [key timestamp value ...]
        "TS.MADD" => Range {
            first: 1,
            last: cmd.len() - 1,
            step: 3,
        },
        // JSON.MGET key [key ...] path
        "JSON.MGET" => {
            if cmd.len() >= 3 {
                Range {
                    first: 1,
                    last: cmd.len() - 2,
                    step: 1,
                }
            } else {
                None
            }
        }

        // ── Keys, then a timeout ─────────────────────────────────────────
        "BLPOP" | "BRPOP" | "BZPOPMIN" | "BZPOPMAX" => {
            if cmd.len() >= 3 {
                Range {
                    first: 1,
                    last: cmd.len() - 2,
                    step: 1,
                }
            } else {
                None
            }
        }

        // ── numkeys-prefixed ─────────────────────────────────────────────
        "LMPOP" | "ZMPOP" | "SINTERCARD" | "SUNIONCARD" | "SDIFFCARD" | "ZINTERCARD"
        | "ZUNION" | "ZINTER" | "ZDIFF" | "TS.NRANGE" | "TS.NREVRANGE" => numkeys(cmd, 1, 2),
        "BLMPOP" | "BZMPOP" => numkeys(cmd, 2, 3),
        "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" | "FCALL" | "FCALL_RO" => {
            numkeys(cmd, 2, 3)
        }
        // dest numkeys key [key ...]
        "ZUNIONSTORE" | "ZINTERSTORE" | "ZDIFFSTORE" => match numkeys(cmd, 2, 3) {
            Range { first, last, .. } => {
                let mut keys = vec![1];
                keys.extend(first..=last);
                List(keys)
            }
            _ => One(1),
        },

        // ── Keys behind a keyword ────────────────────────────────────────
        // BITOP operation destkey key [key ...]
        "BITOP" => rest(cmd, 2),
        // XREAD / XREADGROUP ... STREAMS key [key ...] id [id ...]
        "XREAD" | "XREADGROUP" => match find_option(cmd, 1, b"STREAMS") {
            Some(pos) => {
                let n = (cmd.len() - pos - 1) / 2;
                if n == 0 {
                    None
                } else {
                    Range {
                        first: pos + 1,
                        last: pos + n,
                        step: 1,
                    }
                }
            }
            Option::None => None,
        },
        // SORT key [...] [STORE destination]
        "SORT" => match find_option(cmd, 2, b"STORE") {
            Some(pos) if pos + 1 < cmd.len() => Two(1, pos + 1),
            _ => One(1),
        },
        // GEORADIUS key ... [STORE key] [STOREDIST key]
        "GEORADIUS" | "GEORADIUSBYMEMBER" => {
            let mut keys = vec![1];
            for option in [b"STORE".as_slice(), b"STOREDIST".as_slice()] {
                if let Some(pos) = find_option(cmd, 2, option)
                    && pos + 1 < cmd.len()
                {
                    keys.push(pos + 1);
                }
            }
            if keys.len() == 1 { One(1) } else { List(keys) }
        }
        // MIGRATE host port key|"" db timeout [...] [KEYS key [key ...]]
        "MIGRATE" => {
            if cmd.len() < 6 {
                None
            } else if !cmd[3].is_empty() {
                One(3)
            } else {
                match find_option(cmd, 6, b"KEYS") {
                    Some(pos) => rest(cmd, pos + 1),
                    Option::None => None,
                }
            }
        }

        // ── Key after a subcommand ───────────────────────────────────────
        "XGROUP" | "XINFO" | "OBJECT" | "PFDEBUG" | "JSON.DEBUG" => {
            if cmd.len() >= 3 && !cmd[1].eq_ignore_ascii_case(b"HELP") {
                One(2)
            } else {
                None
            }
        }
        // HIMPORT SET key fieldset value [value ...]; the other subcommands
        // manage session state and name no key.
        "HIMPORT" => {
            if cmd.len() >= 3 && cmd[1].eq_ignore_ascii_case(b"SET") {
                One(2)
            } else {
                None
            }
        }
        "MEMORY" => {
            if cmd.len() >= 3 && cmd[1].eq_ignore_ascii_case(b"USAGE") {
                One(2)
            } else {
                None
            }
        }

        // Everything else — connection, server, pub/sub, cluster, index and
        // multi-series commands — names no keys.
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(parts: &[&str]) -> Vec<String> {
        let cmd: Vec<&[u8]> = parts.iter().map(|s| s.as_bytes()).collect();
        let name = parts[0].to_ascii_uppercase();
        key_positions(&name, &cmd)
            .iter()
            .map(|i| parts[i].to_string())
            .collect()
    }

    #[test]
    fn single_key_commands() {
        assert_eq!(keys(&["GET", "k"]), ["k"]);
        assert_eq!(keys(&["SET", "k", "v", "EX", "10"]), ["k"]);
        assert_eq!(keys(&["HSET", "h", "f", "v"]), ["h"]);
        assert_eq!(keys(&["ZREMRANGEBYSCORE", "z", "0", "1"]), ["z"]);
        assert_eq!(keys(&["DELEX", "k", "IFEQ", "v"]), ["k"]);
        assert_eq!(keys(&["INCREX", "k", "BYINT", "1"]), ["k"]);
        assert_eq!(keys(&["ARSET", "arr", "0", "v"]), ["arr"]);
    }

    /// The commands whose first argument is a count or a subcommand, which the
    /// old routing table hashed as if it were the key.
    #[test]
    fn first_argument_is_not_always_the_key() {
        assert_eq!(keys(&["LMPOP", "2", "a", "b", "LEFT"]), ["a", "b"]);
        assert_eq!(keys(&["ZMPOP", "1", "z", "MIN"]), ["z"]);
        assert_eq!(keys(&["BLMPOP", "0", "2", "a", "b", "LEFT"]), ["a", "b"]);
        assert_eq!(
            keys(&["SINTERCARD", "2", "a", "b", "LIMIT", "1"]),
            ["a", "b"]
        );
        assert_eq!(keys(&["SUNIONCARD", "2", "a", "b"]), ["a", "b"]);
        assert_eq!(keys(&["TS.NRANGE", "2", "a", "b", "-", "+"]), ["a", "b"]);
        assert_eq!(keys(&["TS.READ", "a", "$", "BLOCK", "0", "1"]), ["a"]);
        assert_eq!(keys(&["XGROUP", "CREATE", "stream", "g", "$"]), ["stream"]);
        assert_eq!(keys(&["XINFO", "STREAM", "stream"]), ["stream"]);
        assert_eq!(keys(&["OBJECT", "ENCODING", "k"]), ["k"]);
        assert_eq!(keys(&["JSON.DEBUG", "MEMORY", "k", "$"]), ["k"]);
        assert_eq!(keys(&["MEMORY", "USAGE", "k"]), ["k"]);
        assert!(keys(&["MEMORY", "STATS"]).is_empty());
        assert!(keys(&["XGROUP", "HELP"]).is_empty());
        assert_eq!(
            keys(&["HIMPORT", "SET", "user:1", "u", "alice"]),
            ["user:1"]
        );
        assert!(keys(&["HIMPORT", "PREPARE", "u", "name"]).is_empty());
        assert_eq!(
            keys(&["SDIFFCARD", "2", "a", "b", "LIMIT", "1"]),
            ["a", "b"]
        );
        assert_eq!(
            keys(&["LMOVEM", "a", "b", "LEFT", "LEFT", "COUNT", "2", "OBO"]),
            ["a", "b"]
        );
    }

    #[test]
    fn multi_key_commands() {
        assert_eq!(keys(&["MGET", "a", "b", "c"]), ["a", "b", "c"]);
        assert_eq!(keys(&["MSET", "a", "1", "b", "2"]), ["a", "b"]);
        assert_eq!(
            keys(&["MSETEX", "2", "a", "1", "b", "2", "EX", "10"]),
            ["a", "b"]
        );
        assert_eq!(
            keys(&["JSON.MSET", "a", "$", "1", "b", "$", "2"]),
            ["a", "b"]
        );
        assert_eq!(keys(&["JSON.MGET", "a", "b", "$"]), ["a", "b"]);
        assert_eq!(keys(&["JSON.MGET", "a", "$"]), ["a"]);
        assert_eq!(keys(&["TS.MADD", "a", "1", "1", "b", "2", "2"]), ["a", "b"]);
        assert_eq!(keys(&["RENAME", "a", "b"]), ["a", "b"]);
        assert_eq!(keys(&["BLPOP", "a", "b", "0"]), ["a", "b"]);
        assert_eq!(
            keys(&["BITOP", "AND", "dest", "a", "b"]),
            ["dest", "a", "b"]
        );
        assert_eq!(
            keys(&["ZUNIONSTORE", "dest", "2", "a", "b", "WEIGHTS", "1", "2"]),
            ["dest", "a", "b"]
        );
        assert_eq!(keys(&["ZINTER", "2", "a", "b", "WITHSCORES"]), ["a", "b"]);
    }

    #[test]
    fn keys_behind_keywords() {
        assert_eq!(
            keys(&["XREAD", "COUNT", "2", "STREAMS", "s1", "s2", "0", "0"]),
            ["s1", "s2"]
        );
        assert_eq!(
            keys(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s1", ">"]),
            ["s1"]
        );
        assert_eq!(keys(&["SORT", "k", "STORE", "dest"]), ["k", "dest"]);
        assert_eq!(keys(&["SORT", "k", "LIMIT", "0", "1"]), ["k"]);
        assert_eq!(
            keys(&["GEORADIUS", "k", "0", "0", "1", "km", "STORE", "d"]),
            ["k", "d"]
        );
        assert_eq!(
            keys(&["EVAL", "return 1", "2", "a", "b", "arg"]),
            ["a", "b"]
        );
        assert!(keys(&["EVAL", "return 1", "0"]).is_empty());
        assert_eq!(keys(&["MIGRATE", "h", "1", "k", "0", "100"]), ["k"]);
        assert_eq!(
            keys(&["MIGRATE", "h", "1", "", "0", "100", "KEYS", "a", "b"]),
            ["a", "b"]
        );
    }

    #[test]
    fn keyless_commands() {
        for cmd in [
            vec!["PING"],
            vec!["INFO", "replication"],
            vec!["CLUSTER", "NODES"],
            vec!["PUBLISH", "channel", "message"],
            vec!["FT.SEARCH", "idx", "*"],
            vec!["CONFIG", "GET", "port"],
            vec!["SELECT", "1"],
        ] {
            assert!(keys(&cmd).is_empty(), "{cmd:?} must name no keys");
        }
    }

    /// A count that runs past the end of the command must not index out of
    /// bounds — the handler rejects the command, the key walk just sees none.
    #[test]
    fn malformed_counts_yield_no_keys() {
        assert!(keys(&["LMPOP", "5", "a", "LEFT"]).is_empty());
        assert!(keys(&["LMPOP", "nope", "a", "LEFT"]).is_empty());
        assert!(keys(&["LMPOP", "0", "a", "LEFT"]).is_empty());
        assert!(keys(&["MSETEX", "9", "a", "1"]).is_empty());
        assert!(keys(&["EVAL", "x", "3", "a"]).is_empty());
        assert!(keys(&["GET"]).is_empty());
    }

    #[test]
    fn sharded_pubsub_channel_is_slot_addressed() {
        assert_eq!(keys(&["SPUBLISH", "chan", "msg"]), ["chan"]);
    }

    #[test]
    fn layouts_are_read_off_the_key_table() {
        let fixed = |first, last, step| Layout::Fixed { first, last, step };
        for (name, expected) in [
            ("GET", fixed(1, 1, 1)),
            ("SET", fixed(1, 1, 1)),
            ("SETEX", fixed(1, 1, 1)),
            ("RENAME", fixed(1, 2, 1)),
            ("LMOVE", fixed(1, 2, 1)),
            ("DEL", fixed(1, -1, 1)),
            ("MGET", fixed(1, -1, 1)),
            ("MSET", fixed(1, -2, 2)),
            ("BLPOP", fixed(1, -2, 1)),
            ("JSON.MGET", fixed(1, -2, 1)),
            ("JSON.MSET", fixed(1, -3, 3)),
            ("OBJECT", fixed(2, 2, 1)),
            ("XINFO", fixed(2, 2, 1)),
            ("PING", Layout::NoKeys),
            ("INFO", Layout::NoKeys),
            ("PUBLISH", Layout::NoKeys),
            ("EVAL", Layout::Movable),
            ("ZUNIONSTORE", Layout::Movable),
            ("LMPOP", Layout::Movable),
            ("SINTERCARD", Layout::Movable),
            ("XREAD", Layout::Movable),
            ("SORT", Layout::Movable),
            ("MIGRATE", Layout::Movable),
        ] {
            assert_eq!(layout(name), expected, "{name}");
        }
        // Whatever the layout says, applying it to a real invocation has to
        // give the keys the table gives.
        for cmd in [
            vec!["MSET", "a", "1", "b", "2"],
            vec!["DEL", "a", "b", "c"],
            vec!["BLPOP", "a", "b", "0"],
            vec!["JSON.MSET", "a", "$", "1", "b", "$", "2"],
            vec!["RENAME", "a", "b"],
            vec!["SETEX", "a", "10", "v"],
            vec!["XINFO", "STREAM", "s"],
        ] {
            let Layout::Fixed { first, last, step } = layout(cmd[0]) else {
                panic!("{} should have a fixed layout", cmd[0]);
            };
            let last = if last < 0 {
                cmd.len() as i64 + last
            } else {
                last
            };
            let expected: Vec<&str> = (first..=last)
                .step_by(step as usize)
                .map(|i| cmd[i as usize])
                .collect();
            assert_eq!(keys(&cmd), expected, "{cmd:?}");
        }
    }
}
