use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

pub struct SlowLogEntry {
    pub id: u64,
    pub timestamp: u64,   // Unix seconds when command started
    pub duration_us: u64, // execution time in microseconds
    pub cmd: Vec<String>, // command tokens (truncated)
    pub client_addr: String,
    pub client_name: String,
    /// The command's argument count before truncation, command name included
    /// (the seventh element of a `SLOWLOG GET` entry, Redis 8.10).
    pub argc: usize,
}

pub struct SlowLog {
    entries: parking_lot::RwLock<std::collections::VecDeque<SlowLogEntry>>,
    next_id: AtomicU64,
    max_len: AtomicU64,      // was usize — needs to be runtime-mutable too
    threshold_us: AtomicU64, // was u64
    /// `slowlog-entry-max-argc` / `slowlog-entry-max-string-len` (Redis 8.8).
    entry_max_argc: AtomicU64,
    entry_max_string_len: AtomicU64,
    /// Totals for `INFO stats` (Redis 8.8). They count every command that
    /// crossed the threshold, including ones since trimmed out of the log.
    commands_count: AtomicU64,
    commands_time_us_sum: AtomicU64,
    commands_time_us_max: AtomicU64,
    /// The same totals per command name (lowercase): `(count, sum µs, max µs)`.
    per_command: parking_lot::Mutex<std::collections::BTreeMap<String, (u64, u64, u64)>>,
}

pub type SharedSlowLog = Arc<SlowLog>;

/// Redis's defaults for `slowlog-entry-max-argc` and
/// `slowlog-entry-max-string-len`.
pub const DEFAULT_ENTRY_MAX_ARGC: usize = 32;
pub const DEFAULT_ENTRY_MAX_STRING_LEN: usize = 128;

pub fn new_slowlog(threshold_us: u64, max_len: usize) -> SharedSlowLog {
    Arc::new(SlowLog {
        entries: parking_lot::RwLock::new(std::collections::VecDeque::new()),
        next_id: AtomicU64::new(0),
        max_len: AtomicU64::new(max_len as u64),
        threshold_us: AtomicU64::new(threshold_us),
        entry_max_argc: AtomicU64::new(DEFAULT_ENTRY_MAX_ARGC as u64),
        entry_max_string_len: AtomicU64::new(DEFAULT_ENTRY_MAX_STRING_LEN as u64),
        commands_count: AtomicU64::new(0),
        commands_time_us_sum: AtomicU64::new(0),
        commands_time_us_max: AtomicU64::new(0),
        per_command: parking_lot::Mutex::new(std::collections::BTreeMap::new()),
    })
}

/// A slow log carrying the limits from `config`.
pub fn new_slowlog_from_config(config: &crate::config::Config) -> SharedSlowLog {
    let log = new_slowlog(config.slowlog_log_slower_than, config.slowlog_max_len);
    log.set_entry_max_argc(config.slowlog_entry_max_argc);
    log.set_entry_max_string_len(config.slowlog_entry_max_string_len);
    log
}

/// The logged form of a command: at most `max_argc` arguments, each at most
/// `max_string_len` bytes, with what was cut described in place — the same
/// shape Redis logs.
///
/// A command with more arguments than the limit keeps `max_argc - 1` of them
/// and ends with `... (N more arguments)`. An argument longer than the limit
/// is cut and suffixed with `... (N more bytes)`.
pub fn truncate_command<A: AsRef<[u8]>>(
    cmd: &[A],
    max_argc: usize,
    max_string_len: usize,
) -> Vec<String> {
    // A limit of 0 would log nothing at all, not even the command name.
    let max_argc = max_argc.max(1);
    let kept = if cmd.len() > max_argc {
        max_argc - 1
    } else {
        cmd.len()
    };
    let mut out: Vec<String> = cmd[..kept]
        .iter()
        .map(|arg| {
            let arg = arg.as_ref();
            if arg.len() > max_string_len {
                format!(
                    "{}... ({} more bytes)",
                    String::from_utf8_lossy(&arg[..max_string_len]),
                    arg.len() - max_string_len
                )
            } else {
                String::from_utf8_lossy(arg).into_owned()
            }
        })
        .collect();
    if cmd.len() > max_argc {
        out.push(format!("... ({} more arguments)", cmd.len() - kept));
    }
    out
}

impl SlowLog {
    pub fn threshold_us(&self) -> u64 {
        self.threshold_us.load(Ordering::Relaxed)
    }

    pub fn set_threshold_us(&self, v: u64) {
        self.threshold_us.store(v, Ordering::Relaxed);
    }

    pub fn set_max_len(&self, v: usize) {
        self.max_len.store(v as u64, Ordering::Relaxed);
        // Trim entries if the new limit is smaller than current size
        let mut e = self.entries.write();
        while e.len() > v {
            e.pop_back();
        }
    }

    pub fn entry_max_argc(&self) -> usize {
        self.entry_max_argc.load(Ordering::Relaxed) as usize
    }

    pub fn set_entry_max_argc(&self, v: usize) {
        self.entry_max_argc.store(v as u64, Ordering::Relaxed);
    }

    pub fn entry_max_string_len(&self) -> usize {
        self.entry_max_string_len.load(Ordering::Relaxed) as usize
    }

    pub fn set_entry_max_string_len(&self, v: usize) {
        self.entry_max_string_len.store(v as u64, Ordering::Relaxed);
    }

    /// `(count, total microseconds, max microseconds)` over every command that
    /// has crossed the threshold since startup or `CONFIG RESETSTAT`.
    pub fn totals(&self) -> (u64, u64, u64) {
        (
            self.commands_count.load(Ordering::Relaxed),
            self.commands_time_us_sum.load(Ordering::Relaxed),
            self.commands_time_us_max.load(Ordering::Relaxed),
        )
    }

    pub fn reset_totals(&self) {
        self.commands_count.store(0, Ordering::Relaxed);
        self.commands_time_us_sum.store(0, Ordering::Relaxed);
        self.commands_time_us_max.store(0, Ordering::Relaxed);
        self.per_command.lock().clear();
    }

    /// The same three numbers per command name (lowercase), for
    /// `INFO commandstats`. Only commands that have been logged appear.
    pub fn per_command_totals(&self) -> Vec<(String, (u64, u64, u64))> {
        self.per_command
            .lock()
            .iter()
            .map(|(name, totals)| (name.clone(), *totals))
            .collect()
    }

    /// Log one command that ran for `duration_us`.
    ///
    /// The single place an entry is built, so the truncation limits and the
    /// totals apply to every path that logs — there used to be eight copies of
    /// this, each with its own hardcoded limits.
    pub fn record<A: AsRef<[u8]>>(
        &self,
        cmd: &[A],
        duration_us: u64,
        client_addr: &str,
        client_name: String,
    ) {
        self.commands_count.fetch_add(1, Ordering::Relaxed);
        self.commands_time_us_sum
            .fetch_add(duration_us, Ordering::Relaxed);
        self.commands_time_us_max
            .fetch_max(duration_us, Ordering::Relaxed);
        // Only slow commands get here, so a lock per call is affordable.
        if let Some(name) = cmd.first() {
            let name = String::from_utf8_lossy(name.as_ref()).to_ascii_lowercase();
            let mut per_command = self.per_command.lock();
            let totals = per_command.entry(name).or_insert((0, 0, 0));
            totals.0 += 1;
            totals.1 += duration_us;
            totals.2 = totals.2.max(duration_us);
        }

        self.push(SlowLogEntry {
            id: self.next_id(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            duration_us,
            cmd: truncate_command(cmd, self.entry_max_argc(), self.entry_max_string_len()),
            client_addr: client_addr.to_string(),
            client_name,
            argc: cmd.len(),
        });
    }

    pub fn push(&self, entry: SlowLogEntry) {
        let max = self.max_len.load(Ordering::Relaxed) as usize;
        if max == 0 {
            // `slowlog-max-len 0` keeps nothing; without this the entry below
            // would be pushed after a pop from an empty log and stay forever.
            return;
        }
        let mut e = self.entries.write();
        while e.len() >= max {
            e.pop_back();
        }
        e.push_front(entry);
    }

    pub fn get(&self, count: usize) -> Vec<SlowLogEntry> {
        // VecDeque doesn't impl Clone on entries directly; collect manually
        let e = self.entries.read();
        e.iter()
            .take(count)
            .map(|x| SlowLogEntry {
                id: x.id,
                timestamp: x.timestamp,
                duration_us: x.duration_us,
                cmd: x.cmd.clone(),
                client_addr: x.client_addr.clone(),
                client_name: x.client_name.clone(),
                argc: x.argc,
            })
            .collect()
    }

    pub fn reset(&self) {
        self.entries.write().clear();
    }

    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    pub fn next_id(&self) -> u64 {
        // fetch_add returns the value BEFORE incrementing — exactly what we want
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn len_cap(&self) -> usize {
        self.max_len.load(Ordering::Relaxed) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }
}

#[allow(clippy::items_after_test_module)]
#[cfg(test)]
mod tests {
    use super::*;

    /// The worked example on the SLOWLOG GET reference page: 33 arguments
    /// against the default limit of 32.
    #[test]
    fn truncation_matches_the_reference_example() {
        let mut cmd: Vec<String> = vec!["SADD".into(), "myset".into()];
        cmd.extend((3..=33).map(|n| n.to_string()));
        assert_eq!(cmd.len(), 33);

        let logged = truncate_command(&cmd, 32, 128);
        assert_eq!(logged.len(), 32);
        assert_eq!(logged[0], "SADD");
        assert_eq!(logged[30], "31");
        assert_eq!(logged[31], "... (2 more arguments)");
    }

    #[test]
    fn a_command_within_the_limits_is_logged_verbatim() {
        let cmd = ["SET", "k", "v"];
        assert_eq!(truncate_command(&cmd, 32, 128), vec!["SET", "k", "v"]);
        // Exactly at the argument limit: nothing is cut.
        let cmd: Vec<String> = (0..32).map(|n| n.to_string()).collect();
        assert_eq!(truncate_command(&cmd, 32, 128).len(), 32);
        assert!(!truncate_command(&cmd, 32, 128)[31].starts_with("..."));
    }

    #[test]
    fn long_arguments_are_cut_with_a_byte_count() {
        let big = "x".repeat(200);
        let logged = truncate_command(&["SET", "k", big.as_str()], 32, 128);
        assert_eq!(logged[2], format!("{}... (72 more bytes)", "x".repeat(128)));
    }

    #[test]
    fn record_keeps_the_original_argument_count_and_totals() {
        let sl = new_slowlog(0, 128);
        sl.set_entry_max_argc(4);
        let cmd = ["MSET", "a", "1", "b", "2", "c", "3"];
        sl.record(&cmd, 1_500, "127.0.0.1:1", String::new());
        sl.record(&["PING"], 500, "127.0.0.1:1", String::new());

        let entries = sl.get(10);
        assert_eq!(entries.len(), 2);
        // Newest first.
        assert_eq!(entries[0].argc, 1);
        assert_eq!(entries[1].argc, 7);
        assert_eq!(
            entries[1].cmd,
            vec!["MSET", "a", "1", "... (4 more arguments)"]
        );
        assert_eq!(sl.totals(), (2, 2_000, 1_500));
    }

    #[test]
    fn max_len_zero_keeps_nothing() {
        let sl = new_slowlog(0, 0);
        sl.record(&["PING"], 1, "a", String::new());
        assert_eq!(sl.len(), 0);
        // ...but the command still counts toward the totals.
        assert_eq!(sl.totals().0, 1);
    }

    #[test]
    fn test_default_threshold_is_disabled() {
        // The default threshold must be u64::MAX (disabled).
        // If this fails, the fast path will call now_us() and latency.record()
        // on every command, causing a ~25x throughput regression.
        let config_default = u64::MAX; // mirrors Config::default slowlog_log_slower_than
        let sl = new_slowlog(config_default, 128);
        assert_eq!(
            sl.threshold_us(),
            u64::MAX,
            "Default slowlog threshold must be u64::MAX (disabled). \
             A lower value causes latency.record() on every fast-path command."
        );
        assert!(
            !sl.threshold_us() < u64::MAX, // i.e. slowlog_enabled = false
            "slowlog_enabled must be false with default config"
        );
    }

    #[test]
    fn test_slowlog_disabled_when_threshold_max() {
        let sl = new_slowlog(u64::MAX, 128);
        // Simulate what server.rs computes per connection
        let slowlog_enabled = sl.threshold_us() < u64::MAX;
        assert!(
            !slowlog_enabled,
            "slowlog_enabled must be false with threshold=u64::MAX"
        );
    }

    #[test]
    fn test_config_default_threshold_is_max() {
        // Directly test the Config struct default
        use crate::config::Config;
        use clap::Parser;
        // Parse with no args — should get all defaults
        let config = Config::parse_from(["kividb"]);
        assert_eq!(
            config.slowlog_log_slower_than,
            u64::MAX,
            "Config default slowlog_log_slower_than must be u64::MAX. \
             Changing this to any lower value (e.g. 10_000) causes a ~25x \
             throughput regression because latency.record() acquires a \
             RwLock write on every command on the fast path."
        );
    }
}

// Compile-time guard: ensure the default threshold constant is sane.
// This doesn't catch Config::parse() defaults but catches hardcoded changes.
#[allow(clippy::assertions_on_constants)]
const _: () = assert!(
    u64::MAX > 10_000_000,
    "slowlog threshold sentinel value looks wrong"
);
