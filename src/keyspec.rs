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

/// How a command's keys are found. Each command [`COMMANDS`] names has
/// one; any other has [`Shape::NoKeys`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    /// Connection, server, pub/sub, cluster, index and multi-series
    /// commands, and names that are no command.
    NoKeys,
    /// The first argument.
    First,
    /// The first two arguments: a source and a destination.
    SourceAndDestination,
    /// Every argument.
    All,
    /// Every other argument from the first: key, value, key, value.
    Pairs,
    /// `MSETEX numkeys key value [key value ...] [options]`.
    CountedPairs,
    /// Every third argument from the first: key, path, value.
    Triples,
    /// Every argument but the last: keys, then a timeout or a path.
    AllButLast,
    /// A count of keys, then the keys.
    CountFirst,
    /// An argument, a count of keys, then the keys.
    CountSecond,
    /// A destination, a count of keys, then the keys.
    DestinationAndCount,
    /// `BITOP operation destkey key [key ...]`.
    AfterOperation,
    /// `XREAD` / `XREADGROUP ... STREAMS key [key ...] id [id ...]`.
    Streams,
    /// `SORT key [...] [STORE destination]`.
    Sort,
    /// `GEORADIUS key ... [STORE key] [STOREDIST key]`.
    GeoRadius,
    /// `MIGRATE host port key|"" db timeout [...] [KEYS key [key ...]]`.
    Migrate,
    /// The argument after a subcommand, unless that is `HELP`.
    AfterSubcommand,
    /// `HIMPORT SET key fieldset value [value ...]`; the other subcommands
    /// manage session state and name no key.
    HimportSet,
    /// `MEMORY USAGE key`.
    MemoryUsage,
}

const FIRST: &[&str] = &[
    // Strings
    "GET",
    "SET",
    "SETNX",
    "SETEX",
    "PSETEX",
    "GETSET",
    "GETDEL",
    "GETEX",
    "APPEND",
    "STRLEN",
    "INCR",
    "DECR",
    "INCRBY",
    "DECRBY",
    "INCRBYFLOAT",
    "INCREX",
    "SETRANGE",
    "GETRANGE",
    "SUBSTR",
    "DELEX",
    "DIGEST",
    // Bitmaps / HyperLogLog (single-key forms)
    "SETBIT",
    "GETBIT",
    "BITCOUNT",
    "BITPOS",
    "BITFIELD",
    "BITFIELD_RO",
    "PFADD",
    // Generic
    "TTL",
    "PTTL",
    "TYPE",
    "PERSIST",
    "DUMP",
    "RESTORE",
    "RESTORE-ASKING",
    "EXPIRE",
    "PEXPIRE",
    "EXPIREAT",
    "PEXPIREAT",
    "EXPIRETIME",
    "PEXPIRETIME",
    "SORT_RO",
    "MOVE",
    // Hashes
    "HGET",
    "HSET",
    "HMGET",
    "HMSET",
    "HSETNX",
    "HDEL",
    "HINCRBY",
    "HINCRBYFLOAT",
    "HGETALL",
    "HKEYS",
    "HVALS",
    "HLEN",
    "HEXISTS",
    "HRANDFIELD",
    "HSCAN",
    "HSTRLEN",
    "HEXPIRE",
    "HPEXPIRE",
    "HEXPIREAT",
    "HPEXPIREAT",
    "HTTL",
    "HPTTL",
    "HEXPIRETIME",
    "HPEXPIRETIME",
    "HPERSIST",
    "HGETDEL",
    "HGETEX",
    "HSETEX",
    // Lists
    "LLEN",
    "LRANGE",
    "LINDEX",
    "LPOS",
    "LPUSH",
    "RPUSH",
    "LPUSHX",
    "RPUSHX",
    "LPOP",
    "RPOP",
    "LSET",
    "LINSERT",
    "LREM",
    "LTRIM",
    // Sets
    "SCARD",
    "SMEMBERS",
    "SRANDMEMBER",
    "SISMEMBER",
    "SMISMEMBER",
    "SSCAN",
    "SADD",
    "SREM",
    "SPOP",
    // Sorted sets
    "ZCARD",
    "ZRANGE",
    "ZRANGEBYSCORE",
    "ZRANGEBYLEX",
    "ZREVRANGE",
    "ZREVRANGEBYSCORE",
    "ZREVRANGEBYLEX",
    "ZRANK",
    "ZREVRANK",
    "ZSCORE",
    "ZMSCORE",
    "ZCOUNT",
    "ZLEXCOUNT",
    "ZRANDMEMBER",
    "ZSCAN",
    "ZADD",
    "ZREM",
    "ZINCRBY",
    "ZPOPMIN",
    "ZPOPMAX",
    "ZREMRANGEBYLEX",
    "ZREMRANGEBYRANK",
    "ZREMRANGEBYSCORE",
    // Streams
    "XLEN",
    "XRANGE",
    "XREVRANGE",
    "XPENDING",
    "XADD",
    "XDEL",
    "XDELEX",
    "XTRIM",
    "XSETID",
    "XACK",
    "XACKDEL",
    "XNACK",
    "XAUTOCLAIM",
    "XCLAIM",
    "XCFGSET",
    "XIDMPRECORD",
    // Geo
    "GEOADD",
    "GEODIST",
    "GEOPOS",
    "GEOHASH",
    "GEOSEARCH",
    "GEORADIUS_RO",
    "GEORADIUSBYMEMBER_RO",
    // JSON
    "JSON.GET",
    "JSON.SET",
    "JSON.DEL",
    "JSON.FORGET",
    "JSON.TYPE",
    "JSON.NUMINCRBY",
    "JSON.NUMMULTBY",
    "JSON.STRAPPEND",
    "JSON.STRLEN",
    "JSON.ARRAPPEND",
    "JSON.ARRINDEX",
    "JSON.ARRINSERT",
    "JSON.ARRLEN",
    "JSON.ARRPOP",
    "JSON.ARRTRIM",
    "JSON.OBJKEYS",
    "JSON.OBJLEN",
    "JSON.CLEAR",
    "JSON.TOGGLE",
    "JSON.MERGE",
    "JSON.RESP",
    // Vector sets
    "VADD",
    "VREM",
    "VCARD",
    "VDIM",
    "VINFO",
    "VISMEMBER",
    "VEMB",
    "VGETATTR",
    "VSETATTR",
    "VLINKS",
    "VRANDMEMBER",
    "VRANGE",
    "VSIM",
    // Time series (single-series forms)
    "TS.CREATE",
    "TS.ALTER",
    "TS.ADD",
    "TS.INCRBY",
    "TS.DECRBY",
    "TS.DEL",
    "TS.GET",
    "TS.RANGE",
    "TS.REVRANGE",
    "TS.INFO",
    "TS.READ",
    // Arrays (Redis 8.8)
    "ARSET",
    "ARMSET",
    "ARGET",
    "ARMGET",
    "ARGETRANGE",
    "ARDEL",
    "ARDELRANGE",
    "ARLEN",
    "ARCOUNT",
    "ARINSERT",
    "ARRING",
    "ARNEXT",
    "ARSEEK",
    "ARSCAN",
    "ARGREP",
    "AROP",
    "ARLASTITEMS",
    "ARINFO",
    // Sharded pub/sub: the channel is hashed to a slot exactly like a key.
    "SPUBLISH",
];

const SOURCE_AND_DESTINATION: &[&str] = &[
    "RENAME",
    "RENAMENX",
    "COPY",
    "LMOVE",
    "RPOPLPUSH",
    "SMOVE",
    "BRPOPLPUSH",
    "BLMOVE",
    "LMOVEM",
    "BLMOVEM",
    "GEOSEARCHSTORE",
    "ZRANGESTORE",
    "LCS",
    "TS.CREATERULE",
    "TS.DELETERULE",
];

const ALL: &[&str] = &[
    "MGET",
    "DEL",
    "UNLINK",
    "EXISTS",
    "TOUCH",
    "PFCOUNT",
    "PFMERGE",
    "SUNION",
    "SINTER",
    "SDIFF",
    "SUNIONSTORE",
    "SINTERSTORE",
    "SDIFFSTORE",
    "WATCH",
];

const PAIRS: &[&str] = &["MSET", "MSETNX"];

const COUNTED_PAIRS: &[&str] = &["MSETEX"];

const TRIPLES: &[&str] = &["JSON.MSET", "TS.MADD"];

const ALL_BUT_LAST: &[&str] = &["JSON.MGET", "BLPOP", "BRPOP", "BZPOPMIN", "BZPOPMAX"];

const COUNT_FIRST: &[&str] = &[
    "LMPOP",
    "ZMPOP",
    "SINTERCARD",
    "SUNIONCARD",
    "SDIFFCARD",
    "ZINTERCARD",
    "ZUNION",
    "ZINTER",
    "ZDIFF",
    "TS.NRANGE",
    "TS.NREVRANGE",
];

const COUNT_SECOND: &[&str] = &[
    "BLMPOP",
    "BZMPOP",
    "EVAL",
    "EVALSHA",
    "EVAL_RO",
    "EVALSHA_RO",
    "FCALL",
    "FCALL_RO",
];

const DESTINATION_AND_COUNT: &[&str] = &["ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE"];

const AFTER_OPERATION: &[&str] = &["BITOP"];

const STREAMS: &[&str] = &["XREAD", "XREADGROUP"];

const SORT: &[&str] = &["SORT"];

const GEO_RADIUS: &[&str] = &["GEORADIUS", "GEORADIUSBYMEMBER"];

const MIGRATE: &[&str] = &["MIGRATE"];

const AFTER_SUBCOMMAND: &[&str] = &["XGROUP", "XINFO", "OBJECT", "PFDEBUG", "JSON.DEBUG"];

const HIMPORT_SET: &[&str] = &["HIMPORT"];

const MEMORY_USAGE: &[&str] = &["MEMORY"];

/// The commands that name keys, by how their keys are found.
const COMMANDS: &[(&[&str], Shape)] = &[
    (FIRST, Shape::First),
    (SOURCE_AND_DESTINATION, Shape::SourceAndDestination),
    (ALL, Shape::All),
    (PAIRS, Shape::Pairs),
    (COUNTED_PAIRS, Shape::CountedPairs),
    (TRIPLES, Shape::Triples),
    (ALL_BUT_LAST, Shape::AllButLast),
    (COUNT_FIRST, Shape::CountFirst),
    (COUNT_SECOND, Shape::CountSecond),
    (DESTINATION_AND_COUNT, Shape::DestinationAndCount),
    (AFTER_OPERATION, Shape::AfterOperation),
    (STREAMS, Shape::Streams),
    (SORT, Shape::Sort),
    (GEO_RADIUS, Shape::GeoRadius),
    (MIGRATE, Shape::Migrate),
    (AFTER_SUBCOMMAND, Shape::AfterSubcommand),
    (HIMPORT_SET, Shape::HimportSet),
    (MEMORY_USAGE, Shape::MemoryUsage),
];

/// [`COMMANDS`] by name: open addressing over [`SLOTS`] slots, built when
/// the server is compiled.
///
/// This was a `match` on the name: a chain of comparisons of lengths and
/// bytes whose cost moved with where the compiler put it. In a build
/// without link-time optimisation, DEL cost 27% more instructions in one
/// build than in another, from code layout alone, and every command pays
/// this lookup two or three times (the ACL check, cluster routing, the
/// order of writes). A name is now found in one or two slots.
struct Table {
    slots: [Slot; SLOTS],
    /// The name in each slot, compared only when it is longer than eight
    /// bytes: up to eight, the key is the name.
    names: [&'static str; SLOTS],
}

#[derive(Clone, Copy)]
struct Slot {
    key: u64,
    /// The name's length; 0 for a slot nothing is in.
    len: u8,
    shape: Shape,
}

/// A power of two, and about four times the number of names.
const SLOTS: usize = 1024;

/// How many names [`COMMANDS`] has.
#[cfg(test)]
const NAMES: usize = {
    let mut count = 0;
    let mut group = 0;
    while group < COMMANDS.len() {
        count += COMMANDS[group].0.len();
        group += 1;
    }
    count
};

/// The longest name in [`COMMANDS`].
const LONGEST: usize = {
    let mut longest = 0;
    let mut group = 0;
    while group < COMMANDS.len() {
        let mut i = 0;
        while i < COMMANDS[group].0.len() {
            let len = COMMANDS[group].0[i].len();
            if len > longest {
                longest = len;
            }
            i += 1;
        }
        group += 1;
    }
    longest
};

/// A name's bytes as a number, the first the lowest. Up to eight bytes it
/// is the name: the first four and the last four, which overlap where
/// there are fewer than eight and agree where they do; under four, the
/// first, the middle and the last. A longer name gives its first four and
/// its last four, and is compared whole.
#[inline(always)]
const fn key_of(name: &[u8]) -> u64 {
    let len = name.len();
    if len >= 4 {
        let head = u32::from_le_bytes([name[0], name[1], name[2], name[3]]) as u64;
        let tail =
            u32::from_le_bytes([name[len - 4], name[len - 3], name[len - 2], name[len - 1]]) as u64;
        head | tail << if len <= 8 { 8 * (len - 4) } else { 32 }
    } else {
        (name[0] as u64)
            | (name[len / 2] as u64) << (8 * (len / 2))
            | (name[len - 1] as u64) << (8 * (len - 1))
    }
}

/// Where a name with `key` and `len` bytes is looked for first.
#[inline(always)]
const fn slot_of(key: u64, len: usize) -> usize {
    ((key ^ (len as u64) << 58).wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 54) as usize & (SLOTS - 1)
}

const fn same(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

const fn build() -> Table {
    let mut table = Table {
        slots: [Slot {
            key: 0,
            len: 0,
            shape: Shape::NoKeys,
        }; SLOTS],
        names: [""; SLOTS],
    };
    let mut group = 0;
    while group < COMMANDS.len() {
        let (names, shape) = COMMANDS[group];
        let mut i = 0;
        while i < names.len() {
            let name = names[i].as_bytes();
            assert!(!name.is_empty() && name.len() <= u8::MAX as usize);
            let key = key_of(name);
            let mut at = slot_of(key, name.len());
            while table.slots[at].len != 0 {
                assert!(
                    !same(table.names[at].as_bytes(), name),
                    "a name is in COMMANDS twice"
                );
                at = (at + 1) & (SLOTS - 1);
            }
            table.slots[at] = Slot {
                key,
                len: name.len() as u8,
                shape,
            };
            table.names[at] = names[i];
            i += 1;
        }
        group += 1;
    }
    table
}

static TABLE: Table = build();

/// The most slots in a row that hold a name: no lookup, of a name in the
/// table or not, looks at more than this and one empty slot.
const LONGEST_RUN: usize = {
    let table = build();
    let mut longest = 0;
    let mut run = 0;
    // Twice round, for a run that wraps.
    let mut i = 0;
    while i < 2 * SLOTS {
        if table.slots[i & (SLOTS - 1)].len != 0 {
            run += 1;
            if run > longest {
                longest = run;
            }
        } else {
            run = 0;
        }
        i += 1;
    }
    longest
};
const _: () = assert!(
    LONGEST_RUN <= 8,
    "the table's slots bunch up: change the hash"
);

/// How `name`'s keys are found.
#[inline]
fn shape(name: &[u8]) -> Shape {
    let len = name.len();
    if len == 0 || len > LONGEST {
        return Shape::NoKeys;
    }
    let key = key_of(name);
    let mut at = slot_of(key, len);
    loop {
        let slot = &TABLE.slots[at];
        if slot.len == 0 {
            return Shape::NoKeys;
        }
        if slot.key == key
            && slot.len as usize == len
            && (len <= 8 || TABLE.names[at].as_bytes() == name)
        {
            return slot.shape;
        }
        at = (at + 1) & (SLOTS - 1);
    }
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

    match shape(name.as_bytes()) {
        Shape::NoKeys => None,
        Shape::First => One(1),
        Shape::SourceAndDestination => {
            if cmd.len() >= 3 {
                Two(1, 2)
            } else {
                None
            }
        }
        Shape::All => rest(cmd, 1),
        Shape::Pairs => Range {
            first: 1,
            last: cmd.len() - 1,
            step: 2,
        },
        Shape::CountedPairs => match cmd.get(1).and_then(|n| parse_usize(n)) {
            Some(n) if n > 0 && n <= cmd.len().saturating_sub(1) / 2 => Range {
                first: 2,
                last: 2 + 2 * (n - 1),
                step: 2,
            },
            _ => None,
        },
        Shape::Triples => Range {
            first: 1,
            last: cmd.len() - 1,
            step: 3,
        },
        Shape::AllButLast => {
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
        Shape::CountFirst => numkeys(cmd, 1, 2),
        Shape::CountSecond => numkeys(cmd, 2, 3),
        Shape::DestinationAndCount => match numkeys(cmd, 2, 3) {
            Range { first, last, .. } => {
                let mut keys = vec![1];
                keys.extend(first..=last);
                List(keys)
            }
            _ => One(1),
        },
        Shape::AfterOperation => rest(cmd, 2),
        Shape::Streams => match find_option(cmd, 1, b"STREAMS") {
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
        Shape::Sort => match find_option(cmd, 2, b"STORE") {
            Some(pos) if pos + 1 < cmd.len() => Two(1, pos + 1),
            _ => One(1),
        },
        Shape::GeoRadius => {
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
        Shape::Migrate => {
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
        Shape::AfterSubcommand => {
            if cmd.len() >= 3 && !cmd[1].eq_ignore_ascii_case(b"HELP") {
                One(2)
            } else {
                None
            }
        }
        Shape::HimportSet => {
            if cmd.len() >= 3 && cmd[1].eq_ignore_ascii_case(b"SET") {
                One(2)
            } else {
                None
            }
        }
        Shape::MemoryUsage => {
            if cmd.len() >= 3 && cmd[1].eq_ignore_ascii_case(b"USAGE") {
                One(2)
            } else {
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    /// `key_positions` as it was, a `match` on the name: what the table has to
    /// agree with.
    fn reference(name: &str, cmd: &[&[u8]]) -> Keys {
        use Keys::*;

        if cmd.len() < 2 {
            return None;
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

    /// Every name the table has, every name Redis has (subcommands'
    /// containers among them), and names that are no command, each with
    /// argument lists of every length up to 16 made of words, numbers,
    /// empty strings and every keyword a shape looks for: the table finds
    /// what the `match` found, every time.
    #[test]
    fn the_table_finds_what_the_match_found() {
        let mut names: Vec<String> = COMMANDS
            .iter()
            .flat_map(|(names, _)| names.iter().map(|n| n.to_string()))
            .collect();
        assert_eq!(names.len(), NAMES);
        names.extend(crate::commands::arity::all_names().map(str::to_string));
        for odd in [
            "",
            "G",
            "GE",
            "GETX",
            "XGET",
            "get",
            "Get",
            "GET\0",
            "DEL ",
            "JSON.GE",
            "JSON.GETX",
            "GEORADIUSBYMEMBER_R",
            "GEORADIUSBYMEMBER_ROX",
            "TS.MADDX",
            "PING",
            "CLIENT",
            "CONFIG",
            "FT.SEARCH",
            "TS.MRANGE",
            "X",
            "\u{e9}t\u{e9}",
        ] {
            names.push(odd.to_string());
        }
        // Names that differ from one in the table by a byte, in each place.
        let known = names.clone();
        for name in known.iter().filter(|n| !n.is_empty() && n.is_ascii()) {
            for at in 0..name.len() {
                let mut bytes = name.clone().into_bytes();
                bytes[at] ^= 0x01;
                if let Ok(other) = String::from_utf8(bytes) {
                    names.push(other);
                }
            }
        }
        let words: &[&[u8]] = &[
            b"a",
            b"key",
            b"",
            b"0",
            b"1",
            b"2",
            b"3",
            b"5",
            b"-1",
            b"99",
            b"STREAMS",
            b"streams",
            b"STORE",
            b"store",
            b"STOREDIST",
            b"KEYS",
            b"keys",
            b"HELP",
            b"help",
            b"SET",
            b"set",
            b"USAGE",
            b"usage",
            b"$",
        ];
        // A small generator of our own: the same lists on every run.
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move |below: usize| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % below as u64) as usize
        };
        let mut checked = 0usize;
        for name in &names {
            let mut lists: Vec<Vec<&[u8]>> = Vec::new();
            for len in 0..=16 {
                // All one word, and mixed.
                for word in words {
                    lists.push(vec![*word; len]);
                }
                for _ in 0..40 {
                    lists.push((0..len).map(|_| words[next(words.len())]).collect());
                }
            }
            for mut cmd in lists {
                if let Some(first) = cmd.first_mut() {
                    *first = name.as_bytes();
                }
                assert_eq!(
                    key_positions(name, &cmd),
                    reference(name, &cmd),
                    "{name} {:?}",
                    cmd.iter()
                        .map(|a| String::from_utf8_lossy(a))
                        .collect::<Vec<_>>()
                );
                checked += 1;
            }
        }
        assert!(checked > 1_000_000, "{checked} lists");
    }

    /// The table is as small and as spread out as it is meant to be.
    #[test]
    fn the_table_is_sparse() {
        let used = TABLE.slots.iter().filter(|slot| slot.len != 0).count();
        assert_eq!(used, NAMES);
        assert!(used * 3 < SLOTS, "{used} names in {SLOTS} slots");
    }

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
