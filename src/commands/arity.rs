// src/commands/arity.rs
//
// How many arguments each command takes, checked once, before the command
// runs — where Redis checks it, and with Redis's words:
//
//     ERR wrong number of arguments for 'get' command
//     ERR wrong number of arguments for 'config|get' command
//
// Each handler used to check its own, with a message of its own. The table
// is Redis's: `arity_table.rs` is generated from the command definitions in
// its source tree (`src/commands/*.json`). A positive arity is exact, the
// command's name included; a negative one is a minimum. A command the table
// does not have — the query engine's, JSON, time series, KiviDB's own — is
// left to its handler.

use ahash::AHashSet;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::LazyLock;

#[path = "arity_table.rs"]
mod arity_table;

/// The hash of a command's name: a multiply and a rotate for each eight
/// bytes of it. The table is looked in once for every command that runs, and
/// its keys are a few hundred short words, known in advance and not chosen
/// by anyone: nothing stronger is called for, and anything stronger is a
/// cost every command pays.
#[derive(Default)]
struct NameHasher(u64);

impl NameHasher {
    #[inline(always)]
    fn mix(&mut self, word: u64) {
        self.0 = (self.0.rotate_left(5) ^ word).wrapping_mul(0x517c_c1b7_2722_0a95);
    }
}

impl Hasher for NameHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let (eights, rest) = bytes.as_chunks::<8>();
        for eight in eights {
            self.mix(u64::from_le_bytes(*eight));
        }
        let mut last = 0u64;
        for (i, byte) in rest.iter().enumerate() {
            last |= (*byte as u64) << (8 * i);
        }
        self.mix(last ^ ((bytes.len() as u64) << 56));
    }

    #[inline]
    fn write_u8(&mut self, _: u8) {}

    #[inline]
    fn finish(&self) -> u64 {
        // The table takes its slot from the low bits and its tag from the
        // high ones; the multiply leaves the low ones the poorer.
        self.0 ^ (self.0 >> 32)
    }
}

type ByName<V> = HashMap<&'static str, V, BuildHasherDefault<NameHasher>>;

/// The commands whose names are eight bytes or fewer — nearly everything a
/// client sends often — by the name itself, as a number.
///
/// Every command that runs is looked up here, and for a name this short
/// there is nothing to hash and no string to compare: its bytes are the
/// key. A longer name is looked up in [`Arities::commands`].
struct ShortNames {
    slots: Box<[ShortName]>,
}

#[derive(Clone, Copy, Default)]
struct ShortName {
    key: u64,
    /// The name's length; 0 for a slot nothing is in.
    len: u8,
    container: bool,
    arity: i16,
}

impl ShortNames {
    /// A power of two, and several times the number of names: a lookup is
    /// one slot, or two.
    const SLOTS: usize = 1024;
    const LONGEST: usize = 8;

    /// `name`'s bytes, the first the lowest. `name` is of 1 to 8 bytes.
    #[inline(always)]
    fn key(name: &[u8]) -> u64 {
        let len = name.len();
        if len >= 4 {
            // The first four and the last four, which overlap unless there
            // are eight: where they do, they agree.
            let mut head = [0u8; 4];
            head.copy_from_slice(&name[..4]);
            let mut tail = [0u8; 4];
            tail.copy_from_slice(&name[len - 4..]);
            u32::from_le_bytes(head) as u64 | (u32::from_le_bytes(tail) as u64) << (8 * (len - 4))
        } else {
            // One, two or three: the first, the middle and the last, which
            // are the same byte where there are fewer than three.
            (name[0] as u64)
                | (name[len / 2] as u64) << (8 * (len / 2))
                | (name[len - 1] as u64) << (8 * (len - 1))
        }
    }

    #[inline(always)]
    fn slot(key: u64) -> usize {
        (key.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 54) as usize & (Self::SLOTS - 1)
    }

    fn of(commands: &ByName<(i16, bool)>) -> Self {
        let mut slots = vec![ShortName::default(); Self::SLOTS].into_boxed_slice();
        for (name, (arity, container)) in commands {
            if name.len() > Self::LONGEST {
                continue;
            }
            let key = Self::key(name.as_bytes());
            let mut at = Self::slot(key);
            while slots[at].len != 0 {
                at = (at + 1) & (Self::SLOTS - 1);
            }
            slots[at] = ShortName {
                key,
                len: name.len() as u8,
                container: *container,
                arity: *arity,
            };
        }
        ShortNames { slots }
    }

    /// The arity of `name` — of 1 to 8 bytes — and whether it has
    /// subcommands.
    #[inline(always)]
    fn get(&self, name: &[u8]) -> Option<(i16, bool)> {
        let key = Self::key(name);
        let mut at = Self::slot(key);
        loop {
            let slot = &self.slots[at];
            if slot.len == 0 {
                return None;
            }
            // The length too: a name a client sends may have a zero byte
            // at its end, and its number is then that of the name without.
            if slot.key == key && slot.len as usize == name.len() {
                return Some((slot.arity, slot.container));
            }
            at = (at + 1) & (Self::SLOTS - 1);
        }
    }
}

struct Arities {
    /// What a client can send as a command's name: its arity, and whether
    /// it has subcommands of their own arity (`CONFIG`, `CLIENT`, …) —
    /// together, so that the check every command goes through is one lookup.
    commands: ByName<(i16, bool)>,
    /// The short ones among them, again.
    short: ShortNames,
    /// The subcommands, as the table writes them: `CONFIG|GET`.
    subcommands: ByName<i16>,
    containers: AHashSet<&'static str>,
}

static ARITIES: LazyLock<Arities> = LazyLock::new(|| {
    let table = || {
        arity_table::TABLE
            .iter()
            .copied()
            // Forms KiviDB accepts that Redis does not: `HSETEX key seconds
            // field value`, from before the command took its TTL as an option.
            .filter(|(name, _)| *name != "HSETEX")
    };
    let containers: AHashSet<&'static str> = table()
        .filter_map(|(name, _)| name.split_once('|').map(|(container, _)| container))
        .collect();
    let commands: ByName<(i16, bool)> = table()
        .filter(|(name, _)| !name.contains('|'))
        .map(|(name, arity)| (name, (arity, containers.contains(name))))
        .collect();
    let subcommands = table().filter(|(name, _)| name.contains('|')).collect();
    Arities {
        short: ShortNames::of(&commands),
        commands,
        subcommands,
        containers,
    }
});

fn fits(arity: i16, given: usize) -> bool {
    if arity >= 0 {
        given == arity as usize
    } else {
        given >= arity.unsigned_abs() as usize
    }
}

fn wrong_number(name: &str) -> Vec<u8> {
    format!(
        "-ERR wrong number of arguments for '{}' command\r\n",
        name.to_ascii_lowercase()
    )
    .into_bytes()
}

/// The arity of `name` (upper case; `CONFIG|GET` for a subcommand), as
/// Redis writes it: the number of arguments, or minus the least number.
pub fn of(name: &str) -> Option<i16> {
    let arities = &*ARITIES;
    match arities.commands.get(name) {
        Some((arity, _)) => Some(*arity),
        None => arities.subcommands.get(name).copied(),
    }
}

/// The subcommands in the table, as `CONTAINER|SUBCOMMAND`.
pub fn subcommands() -> impl Iterator<Item = &'static str> {
    arity_table::TABLE
        .iter()
        .map(|(name, _)| *name)
        .filter(|name| name.contains('|'))
}

/// Every name the table has, subcommands as `CONFIG|GET`. For tests.
#[cfg(test)]
pub(crate) fn all_names() -> impl Iterator<Item = &'static str> {
    arity_table::TABLE.iter().map(|(name, _)| *name)
}

/// Whether `name` (upper case) is in the table.
pub fn is_known(name: &str) -> bool {
    // `CONFIG|GET` is how a subcommand is written down, not something a
    // client can send as a command's name.
    ARITIES.commands.contains_key(name)
}

/// Whether `name` (upper case) is a command with subcommands.
pub fn is_container(name: &str) -> bool {
    ARITIES.containers.contains(name)
}

/// The reply to `cmd` if it has the wrong number of arguments. `name` is
/// the command's name in upper case.
///
/// Every command that runs comes through here, nearly always to be told
/// nothing: that part is kept small enough to be compiled into the caller.
#[inline(always)]
pub fn check(name: &str, cmd: &[&[u8]]) -> Option<Vec<u8>> {
    let arities = &*ARITIES;
    if (1..=ShortNames::LONGEST).contains(&name.len()) {
        let (arity, container) = arities.short.get(name.as_bytes())?;
        // The right number, and no subcommands to look at.
        if fits(arity, cmd.len()) && !container {
            return None;
        }
        return refusal(arities, name, cmd, arity);
    }
    check_long(arities, name, cmd)
}

#[inline(never)]
fn check_long(arities: &Arities, name: &str, cmd: &[&[u8]]) -> Option<Vec<u8>> {
    let (arity, container) = *arities.commands.get(name)?;
    if fits(arity, cmd.len()) && !container {
        return None;
    }
    refusal(arities, name, cmd, arity)
}

#[cold]
#[inline(never)]
fn refusal(arities: &Arities, name: &str, cmd: &[&[u8]], arity: i16) -> Option<Vec<u8>> {
    if !fits(arity, cmd.len()) {
        return Some(wrong_number(name));
    }
    if let Some(sub) = cmd.get(1) {
        let full = format!(
            "{name}|{}",
            String::from_utf8_lossy(sub).to_ascii_uppercase()
        );
        if let Some(arity) = arities.subcommands.get(full.as_str())
            && !fits(*arity, cmd.len())
        {
            return Some(wrong_number(&full));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The table for short names says what the table of all of them says —
    /// for every name in it, and for names that are not.
    #[test]
    fn the_short_names_are_the_same_commands() {
        let arities = &*ARITIES;
        let mut short = 0;
        for (name, entry) in &arities.commands {
            if name.len() <= ShortNames::LONGEST {
                assert_eq!(arities.short.get(name.as_bytes()), Some(*entry), "{name}");
                short += 1;
            }
        }
        assert!(short > 100, "{short}");
        for name in [
            "G", "GE", "GETT", "XGET", "NOSUCH", "ABCDEFGH", "\0", "GET\0", "\0GET",
        ] {
            assert_eq!(arities.short.get(name.as_bytes()), None, "{name:?}");
        }
        // And through the front door.
        assert_eq!(checked(&["GET\0"]), None);
        assert_eq!(checked(&["GET\0", "a", "b", "c"]), None);
        assert!(checked(&["HGETALL"]).is_some());
        assert!(checked(&["ZRANGEBYSCORE"]).is_some());
    }

    fn checked(words: &[&str]) -> Option<String> {
        let cmd: Vec<&[u8]> = words.iter().map(|w| w.as_bytes()).collect();
        check(&words[0].to_ascii_uppercase(), &cmd)
            .map(|reply| String::from_utf8(reply).unwrap().trim_end().to_string())
    }

    #[test]
    fn exact_and_at_least() {
        assert_eq!(checked(&["GET", "k"]), None);
        assert_eq!(
            checked(&["GET"]).unwrap(),
            "-ERR wrong number of arguments for 'get' command"
        );
        assert_eq!(
            checked(&["GET", "a", "b"]).unwrap(),
            "-ERR wrong number of arguments for 'get' command"
        );
        assert_eq!(checked(&["DEL", "a", "b", "c"]), None);
        assert!(checked(&["DEL"]).is_some());
        assert_eq!(
            checked(&["ZMPOP", "1", "z"]).unwrap(),
            "-ERR wrong number of arguments for 'zmpop' command"
        );
    }

    #[test]
    fn subcommands_have_their_own() {
        assert_eq!(checked(&["CONFIG", "GET", "maxmemory"]), None);
        assert_eq!(
            checked(&["CONFIG", "GET"]).unwrap(),
            "-ERR wrong number of arguments for 'config|get' command"
        );
        assert_eq!(
            checked(&["CONFIG"]).unwrap(),
            "-ERR wrong number of arguments for 'config' command"
        );
        // One it does not know is the handler's to refuse.
        assert_eq!(checked(&["CONFIG", "NOSUCH", "x"]), None);
    }

    #[test]
    fn what_the_table_does_not_have_is_left_alone() {
        assert_eq!(checked(&["FT.SEARCH"]), None);
        assert_eq!(checked(&["JSON.GET"]), None);
        // The form from before it took its TTL as an option.
        assert_eq!(checked(&["HSETEX", "k", "10", "f", "v"]), None);
    }
}
