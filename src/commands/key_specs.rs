//! Which arguments of a command are keys, and what the command does with
//! each: `COMMAND GETKEYS` and `COMMAND GETKEYSANDFLAGS`.
//!
//! Redis describes this for every command with *key specs* — where the keys
//! begin (a fixed argument, or after a keyword) and how far they run (a
//! range, or a count given in another argument) — each with the flags that
//! say how the keys it finds are used. The specs of Redis's own commands are
//! in `key_spec_table`, taken from Redis; this is what reads them, the way
//! Redis does. A command that is not in the table is one of KiviDB's own,
//! and the caller answers for it.

use super::key_spec_table::TABLE;
use ahash::AHashMap;
use std::sync::LazyLock;

pub(super) enum Begin {
    /// The keys begin at this argument.
    Index(usize),
    /// The keys begin after this word, looked for from the given argument
    /// on — or, if that is negative, backwards from that far before the end.
    Keyword(&'static str, i64),
    Unknown,
}

pub(super) enum Find {
    /// `(lastkey, step, limit)`: the keys run to `lastkey` arguments after
    /// the first — or, negative, to that far before the end, of the whole
    /// command or (`limit` not zero) of the first `1/limit` of what is left.
    Range(i64, usize, i64),
    /// `(keynumidx, firstkey, step)`: the number of keys is the argument
    /// `keynumidx` after the beginning, and they start `firstkey` after it.
    KeyNum(usize, usize, usize),
    Unknown,
}

pub(super) struct Spec {
    pub flags: &'static str,
    pub begin: Begin,
    pub find: Find,
}

/// Why a command's keys could not be told.
#[derive(Debug, PartialEq, Eq)]
pub enum NoKeys {
    /// No such command.
    NoSuchCommand,
    /// The command takes no keys.
    NoKeyArguments,
    /// Too few arguments, or too many.
    WrongNumber,
    /// The arguments do not say where the keys are.
    Invalid,
}

impl NoKeys {
    pub fn message(&self) -> &'static str {
        match self {
            NoKeys::NoSuchCommand => "ERR Invalid command specified",
            NoKeys::NoKeyArguments => "ERR The command has no key arguments",
            NoKeys::WrongNumber => "ERR Invalid number of arguments specified for command",
            NoKeys::Invalid => "ERR Invalid arguments specified for command",
        }
    }
}

type Entry = (bool, &'static [Spec]);

static BY_NAME: LazyLock<AHashMap<&'static str, Entry>> = LazyLock::new(|| {
    TABLE
        .iter()
        .map(|(name, optional, specs)| (*name, (*optional, *specs)))
        .collect()
});

/// Whether the keys of `name` (upper case) are somewhere a first, a last and
/// a step cannot say — after a keyword, or as many as an argument counts —
/// so that a client has to ask `COMMAND GETKEYS`. `None` if `name` is not
/// in the table.
pub fn movable(name: &str) -> Option<bool> {
    BY_NAME.get(name).map(|(_, specs)| {
        specs.iter().any(|spec| {
            !matches!(
                (&spec.begin, &spec.find),
                (Begin::Index(_), Find::Range(..))
            )
        })
    })
}

/// The keys of `cmd` — positions in it, each with its flags — if `cmd` is a
/// command of Redis's. `None` if it is not: the table has nothing to say.
pub fn keys_of(cmd: &[&[u8]]) -> Option<Result<Vec<(usize, &'static str)>, NoKeys>> {
    let name = String::from_utf8_lossy(cmd.first()?).to_ascii_uppercase();
    // `MEMORY USAGE key` is the command `MEMORY|USAGE`.
    let full = match cmd.get(1) {
        Some(sub) if super::arity::is_container(&name) => {
            format!(
                "{name}|{}",
                String::from_utf8_lossy(sub).to_ascii_uppercase()
            )
        }
        _ => name.clone(),
    };
    let Some((optional, specs)) = BY_NAME.get(full.as_str()) else {
        // One of Redis's commands, and without keys; or not Redis's at all.
        return (super::arity::of(&full).is_some() || super::arity::is_known(&name))
            .then_some(Err(NoKeys::NoKeyArguments));
    };
    if super::arity::check(&name, cmd).is_some() {
        return Some(Err(NoKeys::WrongNumber));
    }
    let keys = match evaluate(specs, cmd) {
        Ok(keys) => by_the_arguments(&name, cmd, keys),
        Err(()) => Vec::new(),
    };
    Some(if keys.is_empty() && !*optional {
        Err(NoKeys::Invalid)
    } else {
        Ok(keys)
    })
}

/// Read the specs against the arguments. `Err` if a spec that should apply
/// cannot be followed: a key count that is not a number, or that counts more
/// arguments than there are.
fn evaluate(specs: &[Spec], cmd: &[&[u8]]) -> Result<Vec<(usize, &'static str)>, ()> {
    let argc = cmd.len() as i64;
    let mut keys = Vec::new();
    for spec in specs {
        let mut first = match spec.begin {
            Begin::Index(pos) => pos as i64,
            Begin::Keyword(word, from) => {
                let is_word = |at: &i64| cmd[*at as usize].eq_ignore_ascii_case(word.as_bytes());
                let found = if from >= 0 {
                    (from..argc).find(is_word)
                } else {
                    (1..=argc + from).rev().find(is_word)
                };
                match found {
                    Some(at) => at + 1,
                    // The keyword is an option, and it was not given.
                    None => continue,
                }
            }
            Begin::Unknown => continue,
        };
        let (last, step) = match spec.find {
            Find::Range(lastkey, step, limit) => {
                let last = if lastkey >= 0 {
                    first + lastkey
                } else if limit == 0 {
                    argc + lastkey
                } else {
                    first + ((argc - first) / limit + lastkey)
                };
                (last, step)
            }
            Find::KeyNum(index, firstkey, step) => {
                let count = cmd
                    .get((first as usize).saturating_add(index))
                    .and_then(|raw| std::str::from_utf8(raw).ok())
                    .and_then(|s| s.parse::<i64>().ok())
                    .filter(|count| (0..=argc).contains(count))
                    .ok_or(())?;
                first += firstkey as i64;
                // No keys at all is something a command may say.
                if count == 0 {
                    continue;
                }
                (first + (count - 1) * step as i64, step)
            }
            Find::Unknown => continue,
        };
        if last >= argc || last < first || first >= argc {
            return Err(());
        }
        keys.extend(
            (first..=last)
                .step_by(step.max(1))
                .map(|at| (at as usize, spec.flags)),
        );
    }
    Ok(keys)
}

/// What the specs leave to the command itself: flags that depend on the
/// options given, and keys that can be anywhere among the arguments.
fn by_the_arguments(
    name: &str,
    cmd: &[&[u8]],
    mut keys: Vec<(usize, &'static str)>,
) -> Vec<(usize, &'static str)> {
    let has = |from: usize, word: &[u8]| {
        cmd.iter()
            .skip(from)
            .any(|arg| arg.eq_ignore_ascii_case(word))
    };
    match name {
        // The old value is read only if it is asked for, and compared,
        // not returned, under a condition (Redis 8.10's `setGetKeys`).
        "SET" => {
            let compares = [&b"IFEQ"[..], b"IFNE", b"IFDEQ", b"IFDNE"]
                .iter()
                .any(|word| has(3, word));
            let flags = if has(3, b"GET") {
                "RW access update"
            } else if compares {
                "RW update"
            } else {
                "OW update"
            };
            keys.iter_mut().for_each(|key| key.1 = flags);
        }
        "BITFIELD" => {
            let flags = if has(2, b"SET") || has(2, b"INCRBY") {
                "RW access update"
            } else {
                "RO access"
            };
            keys.iter_mut().for_each(|key| key.1 = flags);
        }
        // `STORE destination`, wherever it is.
        "SORT" => {
            let store = (2..cmd.len().saturating_sub(1))
                .rev()
                .find(|at| cmd[*at].eq_ignore_ascii_case(b"STORE"));
            if let Some(at) = store {
                keys.push((at + 1, "OW update"));
            }
        }
        // `MIGRATE host port "" db timeout … KEYS key …`
        "MIGRATE" => {
            if keys.len() > 1 {
                keys.retain(|(at, _)| *at != 3 || !cmd[3].is_empty());
            }
            keys.iter_mut().for_each(|key| key.1 = "RW access delete");
        }
        _ => {}
    }
    keys
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(words: &[&str]) -> Result<Vec<(String, &'static str)>, NoKeys> {
        let cmd: Vec<&[u8]> = words.iter().map(|w| w.as_bytes()).collect();
        keys_of(&cmd).expect("in the table").map(|keys| {
            keys.into_iter()
                .map(|(at, flags)| (words[at].to_string(), flags))
                .collect()
        })
    }

    fn names(words: &[&str]) -> Vec<String> {
        keys(words)
            .unwrap()
            .into_iter()
            .map(|(key, _)| key)
            .collect()
    }

    #[test]
    fn ranges_counts_and_keywords() {
        assert_eq!(names(&["GET", "k"]), ["k"]);
        assert_eq!(names(&["MSET", "a", "1", "b", "2"]), ["a", "b"]);
        assert_eq!(names(&["BLPOP", "a", "b", "0"]), ["a", "b"]);
        assert_eq!(names(&["LCS", "a", "b"]), ["a", "b"]);
        assert_eq!(names(&["ZUNIONSTORE", "d", "2", "a", "b"]), ["d", "a", "b"]);
        assert_eq!(names(&["MEMORY", "USAGE", "k"]), ["k"]);
        assert_eq!(names(&["XGROUP", "CREATE", "k", "g", "$"]), ["k"]);
        assert_eq!(
            names(&[
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "STREAMS",
                "a",
                "b",
                ">",
                ">"
            ]),
            ["a", "b"]
        );
        assert_eq!(
            names(&["GEORADIUS", "k", "0", "0", "1", "km", "STORE", "d"]),
            ["k", "d"]
        );
        assert_eq!(
            names(&["MIGRATE", "h", "1", "", "0", "5", "KEYS", "a", "b"]),
            ["a", "b"]
        );
        assert_eq!(names(&["MIGRATE", "h", "1", "k", "0", "5"]), ["k"]);
    }

    #[test]
    fn flags_follow_the_arguments() {
        assert_eq!(
            keys(&["SET", "k", "v"]).unwrap(),
            [("k".into(), "OW update")]
        );
        assert_eq!(
            keys(&["SET", "k", "v", "GET"]).unwrap(),
            [("k".into(), "RW access update")]
        );
        assert_eq!(
            keys(&["LMOVE", "a", "b", "left", "right"]).unwrap(),
            [("a".into(), "RW access delete"), ("b".into(), "RW insert")]
        );
        assert_eq!(
            keys(&["SORT", "a", "STORE", "b"]).unwrap(),
            [("a".into(), "RO access"), ("b".into(), "OW update")]
        );
    }

    #[test]
    fn what_cannot_be_told() {
        // A script may have no keys; most commands may not.
        assert_eq!(keys(&["EVAL", "return 1", "0"]).unwrap(), []);
        assert_eq!(names(&["EVAL", "return 1", "1", "k"]), ["k"]);
        assert_eq!(
            keys(&["ZINTERSTORE", "zz", "1443677133621497600", "asdf"]),
            Err(NoKeys::Invalid)
        );
        assert_eq!(keys(&["GET"]), Err(NoKeys::WrongNumber));
        assert_eq!(keys(&["PING"]), Err(NoKeys::NoKeyArguments));
        assert_eq!(keys(&["MEMORY", "DOCTOR"]), Err(NoKeys::NoKeyArguments));
        // Not one of Redis's.
        let cmd: Vec<&[u8]> = vec![b"FT.SEARCH", b"idx", b"*"];
        assert!(keys_of(&cmd).is_none());
    }
}
