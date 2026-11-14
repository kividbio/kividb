//! `client-output-buffer-limit`: how much may wait to be written to a client
//! before it is disconnected, by class of client, as in Redis (#774).
//!
//! Each class has a hard limit, a soft limit and the soft limit's seconds. A
//! client with the hard limit or more waiting is disconnected at once; one
//! that has had the soft limit or more waiting for longer than the seconds,
//! without a break, is disconnected then. 0 for a limit turns it off, so
//! `0 0 0` lets the client's output grow without bound.
//!
//! - `normal`: the replies a client is waiting for (a large `KEYS`, say).
//!   Off by default.
//! - `pubsub`: what a subscribed client is waiting for, its messages and its
//!   replies. 32 MB, or 8 MB for 60 seconds, by default.
//! - `slave` (or `replica`): the write stream queued for a replica that is
//!   streaming. Its hard limit is `repl-replica-max-backlog-bytes`, one
//!   setting under two names; during a full sync the replica is held to
//!   `repl-replica-sync-max-backlog-bytes` instead, and not to the soft
//!   limit. 256 MB, or 64 MB for 60 seconds, by default.
//!
//! What counts is the bytes waiting, not the allocator's rounding of them:
//! Redis counts a 31 MB message as 32 MB (jemalloc's size class) and drops
//! its subscriber at the default 32 MB limit, where KiviDB delivers it.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// A class of client, as `client-output-buffer-limit` names them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    Normal = 0,
    Replica = 1,
    PubSub = 2,
}

impl Class {
    pub const ALL: [Class; 3] = [Class::Normal, Class::Replica, Class::PubSub];

    /// As Redis takes it: any case, and `replica` for `slave`.
    pub fn parse(word: &str) -> Option<Class> {
        match word.to_ascii_lowercase().as_str() {
            "normal" => Some(Class::Normal),
            "slave" | "replica" => Some(Class::Replica),
            "pubsub" => Some(Class::PubSub),
            _ => None,
        }
    }

    /// Its name in `CONFIG GET`.
    pub fn name(self) -> &'static str {
        match self {
            Class::Normal => "normal",
            Class::Replica => "slave",
            Class::PubSub => "pubsub",
        }
    }

    /// Its name in a rewritten configuration file, as Redis writes it.
    fn rewrite_name(self) -> &'static str {
        match self {
            Class::Replica => "replica",
            other => other.name(),
        }
    }
}

/// One class's limits. 0 turns a limit off.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Limit {
    pub hard: u64,
    pub soft: u64,
    pub soft_seconds: u64,
}

impl Limit {
    const fn new(hard: u64, soft: u64, soft_seconds: u64) -> Limit {
        Limit {
            hard,
            soft,
            soft_seconds,
        }
    }

    /// Whether a client with `used` bytes waiting is to be disconnected, at
    /// `now` on [`now_secs`]'s clock. `since` is when it went over the soft
    /// limit (0: it is not over it); it is set here when it goes over and
    /// cleared when it is under. As in Redis, the soft limit ends the
    /// connection once it has been over it for more than its seconds.
    pub fn exceeded(&self, used: u64, since: &mut u64, now: u64) -> bool {
        if self.hard != 0 && used >= self.hard {
            return true;
        }
        if self.soft != 0 && used >= self.soft {
            if *since == 0 {
                *since = now;
                false
            } else {
                now.saturating_sub(*since) > self.soft_seconds
            }
        } else {
            *since = 0;
            false
        }
    }
}

/// Redis's defaults, by [`Class`].
pub const DEFAULTS: [Limit; 3] = [
    Limit::new(0, 0, 0),
    Limit::new(256 << 20, 64 << 20, 60),
    Limit::new(32 << 20, 8 << 20, 60),
];

/// All three classes' limits, as the configuration holds them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits(pub [Limit; 3]);

impl Default for Limits {
    fn default() -> Self {
        Limits(DEFAULTS)
    }
}

impl Limits {
    pub fn get(&self, class: Class) -> Limit {
        self.0[class as usize]
    }

    pub fn set(&mut self, class: Class, limit: Limit) {
        self.0[class as usize] = limit;
    }

    /// As `CONFIG GET` gives it: `normal 0 0 0 slave 268435456 67108864 60
    /// pubsub 33554432 8388608 60`.
    pub fn to_config_string(&self) -> String {
        Class::ALL
            .iter()
            .map(|&class| {
                let limit = self.get(class);
                format!(
                    "{} {} {} {}",
                    class.name(),
                    limit.hard,
                    limit.soft,
                    limit.soft_seconds
                )
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// The lines of a rewritten configuration file: one for each class that
    /// is not at Redis's default, with sizes in Redis's units.
    pub fn rewrite_lines(&self) -> String {
        let mut out = String::new();
        for class in Class::ALL {
            let limit = self.get(class);
            if limit != DEFAULTS[class as usize] {
                out.push_str(&format!(
                    "client-output-buffer-limit {} {} {} {}\n",
                    class.rewrite_name(),
                    format_memory(limit.hard),
                    format_memory(limit.soft),
                    limit.soft_seconds
                ));
            }
        }
        out
    }
}

/// Bytes as Redis writes them back to a configuration file: `256mb`, `1gb`,
/// or a plain number.
fn format_memory(bytes: u64) -> String {
    const GB: u64 = 1 << 30;
    const MB: u64 = 1 << 20;
    const KB: u64 = 1 << 10;
    match bytes {
        0 => "0".to_string(),
        b if b % GB == 0 => format!("{}gb", b / GB),
        b if b % MB == 0 => format!("{}mb", b / MB),
        b if b % KB == 0 => format!("{}kb", b / KB),
        b => b.to_string(),
    }
}

/// Redis's three messages for a value it cannot take, word for word.
pub const WRONG_NUMBER: &str = "Wrong number of arguments in buffer limit configuration.";
pub const INVALID_CLASS: &str = "Invalid client class specified in buffer limit configuration.";
pub const INVALID_VALUE: &str =
    "Error in hard, soft or soft_seconds setting in buffer limit configuration.";

/// A value of the setting: `<class> <hard> <soft> <seconds>`, as many times
/// as there are classes to set; a class named twice takes the last. Words
/// are split at each space, as Redis splits them, so a doubled space is an
/// empty word. An empty value sets nothing. Nothing is set unless all of it
/// can be read.
pub fn parse(value: &str) -> Result<Vec<(Class, Limit)>, &'static str> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    let words: Vec<&str> = value.split(' ').collect();
    if !words.len().is_multiple_of(4) {
        return Err(WRONG_NUMBER);
    }
    // Every group's class first, then the numbers, as Redis checks them.
    let classes = words
        .chunks(4)
        .map(|group| Class::parse(group[0]).ok_or(INVALID_CLASS))
        .collect::<Result<Vec<Class>, _>>()?;
    classes
        .into_iter()
        .zip(words.chunks(4))
        .map(|(class, group)| {
            match (
                parse_memory(group[1]),
                parse_memory(group[2]),
                parse_seconds(group[3]),
            ) {
                (Some(hard), Some(soft), Some(soft_seconds)) => Ok((
                    class,
                    Limit {
                        hard,
                        soft,
                        soft_seconds,
                    },
                )),
                _ => Err(INVALID_VALUE),
            }
        })
        .collect()
}

/// A size as Redis reads one: digits, then nothing, `b`, `k`, `kb`, `m`,
/// `mb`, `g` or `gb`, in any case.
fn parse_memory(word: &str) -> Option<u64> {
    let lower = word.to_ascii_lowercase();
    let digits = lower.trim_end_matches(|c: char| c.is_ascii_alphabetic());
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let unit: u64 = match &lower[digits.len()..] {
        "" | "b" => 1,
        "k" => 1000,
        "kb" => 1 << 10,
        "m" => 1000 * 1000,
        "mb" => 1 << 20,
        "g" => 1000 * 1000 * 1000,
        "gb" => 1 << 30,
        _ => return None,
    };
    digits.parse::<u64>().ok()?.checked_mul(unit)
}

/// The seconds as Redis keeps them, in a C `int`: a number past it wraps,
/// and is refused if that makes it negative (`4294967296` is 0, and
/// `2147483648` is refused).
fn parse_seconds(word: &str) -> Option<u64> {
    let digits = word.strip_prefix('+').unwrap_or(word);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let wrapped = digits.parse::<i64>().ok()? as i32;
    u64::try_from(wrapped).ok()
}

/// Seconds since the first call, from 1: 0 is kept to mean "not set".
pub fn now_secs() -> u64 {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    START
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_secs()
        + 1
}

/// The limits of normal and subscribed clients that a server is running
/// with, changed by `CONFIG SET`. (A replica's are in the replication state:
/// see the module's documentation.)
#[derive(Debug)]
pub struct OutputLimits {
    /// hard, soft, seconds; for `Class::Normal` and `Class::PubSub`.
    normal: [AtomicU64; 3],
    pubsub: [AtomicU64; 3],
    /// The smallest of the four sizes that is not 0, or `u64::MAX`: a reply
    /// smaller than this is within every limit, and the connection loop
    /// looks no further.
    trigger: AtomicU64,
}

impl Default for OutputLimits {
    fn default() -> Self {
        let limits = OutputLimits {
            normal: Default::default(),
            pubsub: Default::default(),
            trigger: AtomicU64::new(u64::MAX),
        };
        for class in [Class::Normal, Class::PubSub] {
            limits.set(class, DEFAULTS[class as usize]);
        }
        limits
    }
}

impl OutputLimits {
    fn slots(&self, class: Class) -> Option<&[AtomicU64; 3]> {
        match class {
            Class::Normal => Some(&self.normal),
            Class::PubSub => Some(&self.pubsub),
            Class::Replica => None,
        }
    }

    /// The limits of `Class::Normal` or `Class::PubSub`.
    pub fn get(&self, class: Class) -> Limit {
        match self.slots(class) {
            Some(s) => Limit {
                hard: s[0].load(Relaxed),
                soft: s[1].load(Relaxed),
                soft_seconds: s[2].load(Relaxed),
            },
            None => Limit::default(),
        }
    }

    /// Set the limits of `Class::Normal` or `Class::PubSub`; a replica's are
    /// not kept here, and are ignored.
    pub fn set(&self, class: Class, limit: Limit) {
        let Some(s) = self.slots(class) else {
            return;
        };
        s[0].store(limit.hard, Relaxed);
        s[1].store(limit.soft, Relaxed);
        s[2].store(limit.soft_seconds, Relaxed);
        let trigger = [&self.normal, &self.pubsub]
            .iter()
            .flat_map(|s| [s[0].load(Relaxed), s[1].load(Relaxed)])
            .filter(|&n| n != 0)
            .min()
            .unwrap_or(u64::MAX);
        self.trigger.store(trigger, Relaxed);
    }

    /// Whether `len` bytes of replies could be over a limit: if not, they are
    /// within every one.
    #[inline]
    pub fn may_exceed(&self, len: usize) -> bool {
        len as u64 >= self.trigger.load(Relaxed)
    }
}

/// A connection's replies, held to its class's limits: the clock of its
/// soft limit. The replies to one batch of commands are written before the
/// next batch is read, so what is waiting is that batch.
#[derive(Debug, Default)]
pub struct ReplyLimit {
    since: u64,
}

/// What a connection is to do with a batch of replies.
pub enum Verdict {
    /// Write it.
    Write,
    /// Write it, within this long; past that, it has been over the soft limit
    /// for too long.
    WriteWithin(std::time::Duration),
    /// Close the connection without writing it.
    Close,
}

impl ReplyLimit {
    /// For `len` bytes of replies to a client of `class`. Called only when
    /// [`OutputLimits::may_exceed`].
    pub fn check(&mut self, limits: &OutputLimits, class: Class, len: usize) -> Verdict {
        let limit = limits.get(class);
        let now = now_secs();
        if limit.exceeded(len as u64, &mut self.since, now) {
            return Verdict::Close;
        }
        if self.since == 0 {
            return Verdict::Write;
        }
        // Over the soft limit: the time left before it has been for longer
        // than its seconds.
        let left = (self.since + limit.soft_seconds + 1).saturating_sub(now);
        Verdict::WriteWithin(std::time::Duration::from_secs(left.max(1)))
    }

    /// The replies are written: nothing is waiting.
    pub fn drained(&mut self) {
        self.since = 0;
    }
}

/// Count, and log, a client closed for its output buffer.
pub fn note_disconnect(client_id: u64, class: Class, waiting: u64) {
    crate::stats::OUTPUT_BUFFER_LIMIT_DISCONNECTIONS.fetch_add(1, Relaxed);
    log_warn!(
        "Client id={client_id} closed: {waiting} bytes waiting to be written to it, over \
         client-output-buffer-limit {}",
        class.name()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_read_back_as_in_redis() {
        assert_eq!(
            Limits::default().to_config_string(),
            "normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60"
        );
        assert_eq!(Limits::default().rewrite_lines(), "");
    }

    #[test]
    fn values_are_read_as_redis_reads_them() {
        let parsed = parse("normal 1mb 2gb 5 replica 1g 64m 10 PubSub 0 0 0").unwrap();
        assert_eq!(
            parsed,
            vec![
                (Class::Normal, Limit::new(1 << 20, 2 << 30, 5)),
                (Class::Replica, Limit::new(1_000_000_000, 64_000_000, 10)),
                (Class::PubSub, Limit::new(0, 0, 0)),
            ]
        );
        assert_eq!(parse("").unwrap(), vec![]);
        assert_eq!(parse("normal 1 2"), Err(WRONG_NUMBER));
        assert_eq!(parse("normal 1 2 3 pubsub 1 2"), Err(WRONG_NUMBER));
        assert_eq!(parse("normal 0 0 0 "), Err(WRONG_NUMBER));
        assert_eq!(parse("foo 1 2 3"), Err(INVALID_CLASS));
        assert_eq!(parse("master 1 2 3"), Err(INVALID_CLASS));
        assert_eq!(parse("normal  1   2  5"), Err(INVALID_CLASS));
        assert_eq!(parse("normal  1 0 0"), Err(WRONG_NUMBER));
        for bad in [
            "normal x 2 3",
            "normal -1 2 3",
            "normal 1.5mb 2 3",
            "normal 1 2 -3",
            "normal 1 2 3x",
            "normal 1 2 0x10",
            "normal 1 2 2147483648",
            "normal 1 2 99999999999999999999",
        ] {
            assert_eq!(parse(bad), Err(INVALID_VALUE), "{bad}");
        }
        // A C int: wrapped, as in Redis.
        assert_eq!(parse("normal 1 2 4294967296").unwrap()[0].1.soft_seconds, 0);
        assert_eq!(parse("normal 1 2 +5").unwrap()[0].1.soft_seconds, 5);
        assert_eq!(
            parse("normal 1Kb 00 0").unwrap()[0].1,
            Limit::new(1024, 0, 0)
        );
    }

    #[test]
    fn a_rewrite_names_only_what_changed() {
        let mut limits = Limits::default();
        limits.set(Class::Normal, Limit::new(100 << 20, 0, 0));
        limits.set(Class::Replica, Limit::new(0, 0, 0));
        limits.set(Class::PubSub, Limit::new(1 << 30, 1000, 0));
        assert_eq!(
            limits.rewrite_lines(),
            "client-output-buffer-limit normal 100mb 0 0\n\
             client-output-buffer-limit replica 0 0 0\n\
             client-output-buffer-limit pubsub 1gb 1000 0\n"
        );
    }

    #[test]
    fn the_soft_limit_needs_more_than_its_seconds_without_a_break() {
        let limit = Limit::new(0, 100, 2);
        let mut since = 0;
        assert!(!limit.exceeded(100, &mut since, 10));
        assert!(!limit.exceeded(150, &mut since, 12));
        assert!(limit.exceeded(150, &mut since, 13));
        // A break resets it.
        let mut since = 0;
        assert!(!limit.exceeded(100, &mut since, 10));
        assert!(!limit.exceeded(99, &mut since, 11));
        assert!(!limit.exceeded(100, &mut since, 13));
        assert!(!limit.exceeded(100, &mut since, 15));
        assert!(limit.exceeded(100, &mut since, 16));
        // The hard limit, at once; 0 0 0, never.
        assert!(Limit::new(10, 0, 0).exceeded(10, &mut 0, 1));
        assert!(!Limit::new(0, 0, 0).exceeded(u64::MAX, &mut 0, 1));
    }

    #[test]
    fn small_replies_are_within_every_limit() {
        let limits = OutputLimits::default();
        // The pub/sub soft limit is the smallest by default.
        assert!(!limits.may_exceed((8 << 20) - 1));
        assert!(limits.may_exceed(8 << 20));
        limits.set(Class::PubSub, Limit::default());
        assert!(!limits.may_exceed(1 << 40));
        limits.set(Class::Normal, Limit::new(0, 4096, 1));
        assert!(limits.may_exceed(4096));
    }
}
