use ahash::AHashMap;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Count of connected clients whose authenticated user has a non-wildcard
/// key pattern. When this is zero the key check in execute_command is a
/// single Relaxed load and a branch — effectively free.
pub static RESTRICTED_KEY_CLIENTS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Count of connected clients whose authenticated user's `commands` rule
/// string is not the trivial "+@all" (i.e. some command may be denied), or
/// who may not use every pub/sub channel.
/// Same free-when-zero trick as `RESTRICTED_KEY_CLIENTS`.
pub static RESTRICTED_CMD_CLIENTS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Bumped on every change to the ACL. A connection compares it with the value
/// it last saw before running a command, and re-reads its user's rules when
/// they differ; see [`resync_connection`].
///
/// The two counters above are worked out when a connection authenticates. On
/// their own that meant a rule changed *afterwards* never reached a connection
/// that was already open: revoke a command or a key pattern from a user, and
/// every client already logged in as that user kept the access — indefinitely,
/// on the fast path, where the counters said nobody needed checking. The same
/// gap let a transaction queued before a revocation run after it.
pub static ACL_EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Record that the ACL changed. Call after any mutation of the user table.
pub fn acl_changed() {
    ACL_EPOCH.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// Bring a connection's restriction flags (and the global counters they
/// feed) into line with the ACL as it now stands.
///
/// Returns `false` if the connection's user no longer exists; the connection
/// should then be closed, as Redis does when a user is deleted.
pub fn resync_connection(
    store: &AclStore,
    user: Option<&str>,
    epoch_seen: &mut u64,
    key_restricted: &mut bool,
    cmd_restricted: &mut bool,
) -> bool {
    *epoch_seen = ACL_EPOCH.load(std::sync::atomic::Ordering::Relaxed);
    let Some(name) = user else {
        // Not authenticated: nothing to keep in step.
        return true;
    };
    match store.read().get(name) {
        Some(user) => {
            track_restriction_counters(user, key_restricted, cmd_restricted);
            true
        }
        None => false,
    }
}

// ── ACL user ──────────────────────────────────────────────────────────────────

/// One key pattern of a user: `~pattern` (read and write), `%R~pattern`,
/// `%W~pattern` or `%RW~pattern`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyPattern {
    pub pattern: String,
    pub read: bool,
    pub write: bool,
}

impl KeyPattern {
    /// `~pattern`: both reading and writing.
    pub fn read_write(pattern: &str) -> Self {
        Self {
            pattern: pattern.to_string(),
            read: true,
            write: true,
        }
    }

    fn is_all(&self) -> bool {
        self.pattern == "*" && self.read && self.write
    }
}

impl std::fmt::Display for KeyPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.read, self.write) {
            (true, false) => write!(f, "%R~{}", self.pattern),
            (false, true) => write!(f, "%W~{}", self.pattern),
            _ => write!(f, "~{}", self.pattern),
        }
    }
}

/// A selector: a second, independent set of permissions on a user
/// (`(~cache:* +get)`). A command is allowed if the user's own rules allow it
/// *or* any one selector allows it — command, keys and channels together.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Selector {
    pub commands: String,
    pub keys: Vec<KeyPattern>,
    pub channels: Vec<String>,
}

impl Default for Selector {
    fn default() -> Self {
        Self {
            commands: "-@all".into(),
            keys: Vec::new(),
            channels: Vec::new(),
        }
    }
}

impl std::fmt::Display for Selector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "({})",
            describe_permissions(&self.keys, &self.channels, &self.commands)
        )
    }
}

#[derive(Clone, Debug)]
pub struct AclUser {
    pub name: String,
    /// true = user is active (on), false = disabled (off)
    pub enabled: bool,
    /// true = no password required (nopass flag)
    pub nopass: bool,
    /// Hashed passwords (SHA-256 hex). Empty, without `nopass`, means the user
    /// cannot authenticate at all.
    pub passwords: Vec<String>,
    /// Command permissions: "+@all" / "-@all" / "+cmd" / "-cmd", applied left
    /// to right.
    pub commands: String,
    /// Key patterns. A key is allowed if it matches any of them; the pattern
    /// `*` is "all keys", and no patterns is "no keys".
    pub keys: Vec<KeyPattern>,
    /// Pub/sub channel patterns, held without their `&`. `*` is every
    /// channel, none is no channel.
    pub channels: Vec<String>,
    /// Further permission sets; see [`Selector`].
    pub selectors: Vec<Selector>,
}

impl AclUser {
    /// A user as `ACL SETUSER` first creates it, and as a line of an ACL file
    /// starts out: off, no password, no keys, no channels, no commands.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            enabled: false,
            nopass: false,
            passwords: vec![],
            commands: "-@all".into(),
            keys: Vec::new(),
            channels: Vec::new(),
            selectors: Vec::new(),
        }
    }

    /// The default user — full permissions; `nopass` unless `requirepass` is
    /// set.
    pub fn default_user(requirepass: &str) -> Self {
        let (nopass, passwords) = if requirepass.is_empty() {
            (true, vec![])
        } else {
            // Store SHA-256 of the password
            (false, vec![sha256_hex(requirepass)])
        };
        Self {
            name: "default".into(),
            enabled: true,
            nopass,
            passwords,
            commands: "+@all".into(),
            keys: vec![KeyPattern::read_write("*")],
            channels: vec!["*".into()],
            selectors: Vec::new(),
        }
    }

    /// May use every key (`~*` / `allkeys`).
    pub fn all_keys(&self) -> bool {
        all_keys(&self.keys)
    }

    /// May use every pub/sub channel (`&*` / `allchannels`).
    pub fn all_channels(&self) -> bool {
        all_channels(&self.channels)
    }

    /// Whether `password` authenticates this user.
    pub fn accepts(&self, password: &str) -> bool {
        self.enabled && (self.nopass || self.passwords.contains(&sha256_hex(password)))
    }

    /// The key patterns as `ACL GETUSER` prints them: `~app:* %R~cache:*`.
    pub fn keys_string(&self) -> String {
        join(self.keys.iter())
    }

    /// The channel patterns as `ACL GETUSER` prints them: `&news.*`.
    pub fn channels_string(&self) -> String {
        join(self.channels.iter().map(|c| format!("&{c}")))
    }

    /// Format as an ACL file / `ACL LIST` line. What this writes,
    /// [`parse_acl_file`] reads back to the same user.
    pub fn to_acl_string(&self) -> String {
        let mut out = format!(
            "user {} {}",
            self.name,
            if self.enabled { "on" } else { "off" }
        );
        if self.nopass {
            out.push_str(" nopass");
        }
        // A user with neither `nopass` nor a password cannot log in. It used
        // to be written as `nopass` — which, read back, let anyone in.
        for hash in &self.passwords {
            out.push_str(" #");
            out.push_str(hash);
        }
        out.push(' ');
        out.push_str(&describe_permissions(
            &self.keys,
            &self.channels,
            &self.commands,
        ));
        for selector in &self.selectors {
            out.push(' ');
            out.push_str(&selector.to_string());
        }
        out
    }
}

fn join<T: std::fmt::Display>(items: impl Iterator<Item = T>) -> String {
    items
        .map(|item| item.to_string())
        .collect::<Vec<_>>()
        .join(" ")
}

fn all_keys(keys: &[KeyPattern]) -> bool {
    keys.iter().any(KeyPattern::is_all)
}

fn all_channels(channels: &[String]) -> bool {
    channels.iter().any(|c| c == "*")
}

/// `~a ~b resetchannels &c +@all` — keys, channels, commands, in the order
/// Redis prints them and in a form that can be applied to an empty user.
fn describe_permissions(keys: &[KeyPattern], channels: &[String], commands: &str) -> String {
    let keys = if keys.is_empty() {
        "resetkeys".to_string()
    } else {
        join(keys.iter())
    };
    let channels = if channels.is_empty() {
        "resetchannels".to_string()
    } else {
        join(channels.iter().map(|c| format!("&{c}")))
    };
    format!("{keys} {channels} {commands}")
}

// ── SHA-256 helper ─────────────────────────────────────────────────────────────

pub fn sha256_hex(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(input.as_bytes());
    hash.iter().map(|b| format!("{:02x}", b)).collect()
}

// ── ACL store ─────────────────────────────────────────────────────────────────

pub type AclStore = Arc<RwLock<AHashMap<String, AclUser>>>;

pub fn new_acl_store(requirepass: &str) -> AclStore {
    let mut map = AHashMap::new();
    map.insert("default".into(), AclUser::default_user(requirepass));
    Arc::new(RwLock::new(map))
}

/// Whether a connection that has not sent `AUTH` is the `default` user.
///
/// It is when — and only when — `default` is enabled and needs no password.
/// This is read from the user table, where `--requirepass`, an ACL file,
/// `ACL SETUSER default …` and `CONFIG SET requirepass` all end up. It used
/// to be read from `--requirepass` alone, so a password given to `default`
/// any other way was not asked for: every connection started out logged in.
pub fn default_user_is_open(store: &AclStore) -> bool {
    store
        .read()
        .get("default")
        .is_some_and(|user| user.enabled && user.nopass)
}

/// Redis's reply to `AUTH <password>` when the default user needs none.
pub const AUTH_WITHOUT_PASSWORD: &str = "-ERR AUTH <password> called without any password \
    configured for the default user. Are you sure your configuration is correct?\r\n";

/// The user `username` authenticates as with `password`, if it does.
pub fn authenticate(store: &AclStore, username: &str, password: &str) -> Option<AclUser> {
    let user = store
        .read()
        .get(username)
        .filter(|user| user.accepts(password))
        .cloned();
    if user.is_none() {
        log_verbose!(
            "Authentication failed for user '{username}': unknown user, wrong password, or \
             user disabled. ACL LOG has the client's address"
        );
    }
    user
}

// ── ACL log ───────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct AclLogEntry {
    pub id: u64,
    pub reason: String,         // "auth" or "command" or "key" or "channel"
    pub context: String,        // "toplevel" or "multi" or "lua"
    pub object: String,         // the offending command/key/channel name
    pub username: String,       // which user triggered it
    pub age_seconds: f64,       // seconds since the event (computed at query time)
    pub client_info: String,    // addr:port
    pub timestamp_created: u64, // Unix ms when created
    pub count: u64,             // how many times this event was seen
}

pub struct AclLog {
    entries: RwLock<std::collections::VecDeque<AclLogEntry>>,
    next_id: std::sync::atomic::AtomicU64,
    max_len: usize,
}

impl AclLog {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(std::collections::VecDeque::new()),
            next_id: std::sync::atomic::AtomicU64::new(1),
            max_len: 128,
        }
    }

    pub fn push(&self, entry: AclLogEntry) {
        let mut log = self.entries.write();
        if log.len() >= self.max_len {
            log.pop_front();
        }
        log.push_back(entry);
    }

    /// Record a refused command, key or channel.
    pub fn record(&self, reason: &str, object: &str, username: &str, client_info: &str) {
        self.push(AclLogEntry {
            id: self.next_id(),
            reason: reason.into(),
            context: "toplevel".into(),
            object: object.into(),
            username: username.into(),
            age_seconds: 0.0,
            client_info: client_info.into(),
            timestamp_created: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            count: 1,
        });
    }

    pub fn get(&self, count: usize) -> Vec<AclLogEntry> {
        let log = self.entries.read();
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        log.iter()
            .rev()
            .take(count)
            .map(|e| {
                let mut e = e.clone();
                e.age_seconds = (now_ms.saturating_sub(e.timestamp_created)) as f64 / 1000.0;
                e
            })
            .collect()
    }

    pub fn reset(&self) {
        self.entries.write().clear();
    }

    pub fn next_id(&self) -> u64 {
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

impl Default for AclLog {
    fn default() -> Self {
        Self::new()
    }
}

pub type SharedAclLog = Arc<AclLog>;

pub fn new_acl_log() -> SharedAclLog {
    Arc::new(AclLog::new())
}

// ── ACL file ──────────────────────────────────────────────────────────────────

use std::io::Write;

/// Read the users an ACL file defines.
///
/// Each `user <name> <rule>…` line defines its user from nothing: a line does
/// not add to a user of the same name that already exists in memory. `default`
/// is the one user that exists without a line; if the file has no line for it,
/// it is the default user `requirepass` describes.
///
/// The file is taken whole or not at all: any line that cannot be understood
/// is an error, reported with its line number, and no users are returned.
pub fn parse_acl_file(
    contents: &str,
    requirepass: &str,
) -> Result<AHashMap<String, AclUser>, String> {
    let mut users: AHashMap<String, AclUser> = AHashMap::new();
    let mut errors: Vec<String> = Vec::new();

    for (index, line) in contents.lines().enumerate() {
        let number = index + 1;
        let line = line.trim();
        // Skip comments and blank lines
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut tokens = line.split_whitespace();
        if tokens.next() != Some("user") {
            errors.push(format!(
                "line {number}: a line should start with the 'user' keyword"
            ));
            continue;
        }
        let Some(name) = tokens.next() else {
            errors.push(format!("line {number}: 'user' must be followed by a name"));
            continue;
        };
        if users.contains_key(name) {
            errors.push(format!("line {number}: duplicate user '{name}'"));
            continue;
        }
        let rules = match merge_selector_args(tokens) {
            Ok(rules) => rules,
            Err(e) => {
                errors.push(format!("line {number}: {e}"));
                continue;
            }
        };
        let mut user = AclUser::new(name);
        let mut ok = true;
        for rule in &rules {
            if let Err(e) = apply_rule(&mut user, rule) {
                errors.push(format!("line {number}: error in rule '{rule}': {e}"));
                ok = false;
                break;
            }
        }
        if ok {
            users.insert(name.to_string(), user);
        }
    }

    if !errors.is_empty() {
        return Err(errors.join("; "));
    }
    users
        .entry("default".to_string())
        .or_insert_with(|| AclUser::default_user(requirepass));
    Ok(users)
}

/// Replace the user table with the users the ACL file at `path` defines.
///
/// All or nothing: if the file cannot be read, or any line of it is wrong,
/// the users in memory are left exactly as they were. Otherwise the file is
/// what the users now are — a password that is no longer in it no longer
/// works, a user that is no longer in it no longer exists. (This used to
/// apply each line on top of the user already in memory, so a changed
/// password was *added* and a removed user stayed.)
pub fn load_acl_file(path: &str, store: &AclStore, requirepass: &str) -> Result<(), String> {
    let contents = std::fs::read_to_string(path).map_err(|e| format!("opening '{path}': {e}"))?;
    let users = parse_acl_file(&contents, requirepass).map_err(|e| format!("{path}: {e}"))?;
    *store.write() = users;
    acl_changed();
    Ok(())
}

/// Save all users to the ACL file atomically (write to .tmp then rename).
pub fn save_acl_file(path: &str, store: &AclStore) -> std::io::Result<()> {
    let tmp = format!("{path}.tmp");
    let mut file = std::fs::File::create(&tmp)?;

    let map = store.read();
    // Write default user first, then others alphabetically
    let mut names: Vec<&String> = map.keys().collect();
    names.sort_by_key(|n| (n.as_str() != "default", n.as_str()));

    for name in names {
        if let Some(user) = map.get(name) {
            writeln!(file, "{}", user.to_acl_string())?;
        }
    }
    file.flush()?;
    drop(file);

    // Atomic rename — safe on POSIX, best-effort on Windows
    std::fs::rename(&tmp, path)?;
    Ok(())
}

// ── Rules ─────────────────────────────────────────────────────────────────────

/// Join the pieces of a selector back together. A selector may arrive as one
/// argument, `(~k +get)`, or — typed without quotes, or read from a file
/// split on spaces — as several: `(~k`, `+get)`.
pub fn merge_selector_args<'a>(args: impl Iterator<Item = &'a str>) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = Vec::new();
    let mut open: Option<String> = None;
    for arg in args {
        match open.as_mut() {
            Some(selector) => {
                selector.push(' ');
                selector.push_str(arg);
                if arg.ends_with(')') {
                    out.push(open.take().unwrap_or_default());
                }
            }
            None if arg.starts_with('(') && !arg.ends_with(')') => open = Some(arg.to_string()),
            None => out.push(arg.to_string()),
        }
    }
    match open {
        Some(_) => Err("Unmatched parenthesis in acl selector starting at '('.".to_string()),
        None => Ok(out),
    }
}

const SYNTAX_ERROR: &str = "Syntax error";
const PATTERN_AFTER_ALL: &str = "Adding a pattern after the * pattern (or the 'allkeys' flag) is \
     not valid and does not have any effect. Try 'resetkeys' to start with an empty list of \
     patterns";
const CHANNEL_AFTER_ALL: &str = "Adding a pattern after the * pattern (or the 'allchannels' flag) \
     is not valid and does not have any effect. Try 'resetchannels' to start with an empty list \
     of channels";

/// The rules that keys, channels and commands share between a user and a
/// selector. `Ok(false)` means the rule is not one of them.
fn apply_permission_rule(
    commands: &mut String,
    keys: &mut Vec<KeyPattern>,
    channels: &mut Vec<String>,
    rule: &str,
) -> Result<bool, String> {
    let add_key = |keys: &mut Vec<KeyPattern>, pattern: KeyPattern| -> Result<(), String> {
        if pattern.pattern.is_empty() {
            return Err(SYNTAX_ERROR.to_string());
        }
        if pattern.is_all() {
            // `~*` replaces whatever was there.
            keys.clear();
            keys.push(pattern);
            return Ok(());
        }
        if all_keys(keys) {
            return Err(PATTERN_AFTER_ALL.to_string());
        }
        if !keys.contains(&pattern) {
            keys.push(pattern);
        }
        Ok(())
    };

    match rule {
        "allkeys" | "~*" => add_key(keys, KeyPattern::read_write("*"))?,
        "resetkeys" => keys.clear(),
        "allchannels" | "&*" => {
            channels.clear();
            channels.push("*".to_string());
        }
        "resetchannels" => channels.clear(),
        "allcommands" | "+@all" => *commands = "+@all".into(),
        "nocommands" | "-@all" => *commands = "-@all".into(),
        // Sugar for a read-only user: block every data-mutating command
        // (see the `write` ACL category / `replication::is_write_command`).
        // Does not itself restrict admin/server commands (CONFIG, SHUTDOWN,
        // ...) — combine with explicit `-cmd` rules for full lockdown.
        "readonly" => push_command_rule(commands, "-@write"),
        r if r.starts_with('~') => add_key(keys, KeyPattern::read_write(&r[1..]))?,
        r if r.starts_with('%') => {
            let Some((flags, pattern)) = r[1..].split_once('~') else {
                return Err(SYNTAX_ERROR.to_string());
            };
            let (mut read, mut write) = (false, false);
            for flag in flags.chars() {
                match flag {
                    'R' | 'r' => read = true,
                    'W' | 'w' => write = true,
                    _ => return Err(SYNTAX_ERROR.to_string()),
                }
            }
            if !read && !write {
                return Err(SYNTAX_ERROR.to_string());
            }
            add_key(
                keys,
                KeyPattern {
                    pattern: pattern.to_string(),
                    read,
                    write,
                },
            )?;
        }
        r if r.starts_with('&') => {
            let pattern = &r[1..];
            if pattern.is_empty() {
                return Err(SYNTAX_ERROR.to_string());
            }
            if all_channels(channels) {
                return Err(CHANNEL_AFTER_ALL.to_string());
            }
            if !channels.iter().any(|c| c == pattern) {
                channels.push(pattern.to_string());
            }
        }
        r if r.starts_with('+') || r.starts_with('-') => {
            check_command_rule(r)?;
            push_command_rule(commands, r);
        }
        _ => return Ok(false),
    }
    Ok(true)
}

/// Append a `+…` / `-…` rule, unless it could not change anything.
fn push_command_rule(commands: &mut String, rule: &str) {
    if !(commands == "+@all" && rule.starts_with('+')
        || commands == "-@all" && rule.starts_with('-'))
    {
        commands.push(' ');
        commands.push_str(rule);
    }
}

/// A `+…` / `-…` rule must name a category, a command or a subcommand that
/// exists. One that does not used to be stored and never match — so a typo in
/// `-@dangerous` granted what it was written to deny.
fn check_command_rule(rule: &str) -> Result<(), String> {
    const UNKNOWN: &str = "Unknown command or category name in ACL";
    let name = &rule[1..];
    if let Some(category) = name.strip_prefix('@') {
        let category = category.to_ascii_lowercase();
        return if category == "all"
            || crate::commands::server::acl_cat_commands_opt(&category).is_some()
        {
            Ok(())
        } else {
            Err(UNKNOWN.to_string())
        };
    }
    let mut parts = name.split('|');
    let command = parts.next().unwrap_or_default().to_ascii_uppercase();
    if command.is_empty() || !crate::commands::server::is_registered_command(&command) {
        return Err(UNKNOWN.to_string());
    }
    match (parts.next(), parts.next()) {
        (None, _) => Ok(()),
        (Some(sub), None) if !sub.is_empty() => {
            if CONTAINER_COMMANDS.contains(&command.as_str()) {
                Ok(())
            } else {
                // `+select|0`: allowing a command for one first argument only.
                Err("Allowing first-arg of a subcommand is not supported".to_string())
            }
        }
        _ => Err(SYNTAX_ERROR.to_string()),
    }
}

fn check_password_hash(hash: &str) -> Result<(), String> {
    if hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        Ok(())
    } else {
        Err(
            "The password hash must be exactly 64 characters and contain only lowercase \
             hexadecimal characters"
                .to_string(),
        )
    }
}

/// Parse the inside of `(…)` into a selector.
fn parse_selector(rule: &str) -> Result<Selector, String> {
    let inner = rule
        .strip_prefix('(')
        .and_then(|r| r.strip_suffix(')'))
        .ok_or_else(|| SYNTAX_ERROR.to_string())?;
    let mut selector = Selector::default();
    for part in inner.split_whitespace() {
        let known = apply_permission_rule(
            &mut selector.commands,
            &mut selector.keys,
            &mut selector.channels,
            part,
        )?;
        if !known {
            return Err(SYNTAX_ERROR.to_string());
        }
    }
    Ok(selector)
}

/// Apply a single rule token to a user — shared between SETUSER and the ACL
/// file, so both read rules the same way.
///
/// `Err` is the reason the rule was refused, in Redis's words. A rule that is
/// not understood is an error: it used to be skipped, which turned a
/// misspelt restriction into no restriction.
pub fn apply_rule(user: &mut AclUser, rule: &str) -> Result<(), String> {
    match rule {
        "on" => user.enabled = true,
        "off" => user.enabled = false,
        "nopass" => {
            user.nopass = true;
            user.passwords.clear();
        }
        "resetpass" => {
            user.nopass = false;
            user.passwords.clear();
        }
        "reset" => {
            let name = std::mem::take(&mut user.name);
            *user = AclUser::new(&name);
        }
        "clearselectors" => user.selectors.clear(),
        // Accepted for compatibility with Redis-written files: payloads are
        // always checked.
        "sanitize-payload" | "skip-sanitize-payload" => {}
        r if r.starts_with('(') => user.selectors.push(parse_selector(r)?),
        r if r.starts_with('>') => {
            let hash = sha256_hex(&r[1..]);
            if !user.passwords.contains(&hash) {
                user.passwords.push(hash);
            }
            user.nopass = false;
        }
        r if r.starts_with('<') => {
            let hash = sha256_hex(&r[1..]);
            remove_password(user, &hash)?;
        }
        r if r.starts_with('#') => {
            let hash = &r[1..];
            check_password_hash(hash)?;
            if !user.passwords.iter().any(|h| h == hash) {
                user.passwords.push(hash.to_string());
            }
            user.nopass = false;
        }
        r if r.starts_with('!') => {
            let hash = &r[1..];
            check_password_hash(hash)?;
            remove_password(user, hash)?;
        }
        r => {
            let known =
                apply_permission_rule(&mut user.commands, &mut user.keys, &mut user.channels, r)?;
            if !known {
                return Err(SYNTAX_ERROR.to_string());
            }
        }
    }
    Ok(())
}

fn remove_password(user: &mut AclUser, hash: &str) -> Result<(), String> {
    let before = user.passwords.len();
    user.passwords.retain(|h| h != hash);
    if user.passwords.len() == before {
        return Err(
            "The password you are trying to remove from the user does not exist".to_string(),
        );
    }
    Ok(())
}

// ── Permission checks ─────────────────────────────────────────────────────────

/// Commands whose subcommands can be granted and denied one at a time, as
/// `+config|get` or by a category that lists `CLUSTER|RESET`.
const CONTAINER_COMMANDS: &[&str] = &[
    "ACL", "BACKUP", "CLIENT", "CLUSTER", "COMMAND", "CONFIG", "FUNCTION", "HIMPORT", "HOTKEYS",
    "LATENCY", "MEMORY", "MODULE", "OBJECT", "PUBSUB", "SCRIPT", "SLOWLOG", "XGROUP", "XINFO",
];

/// Whether `name` (upper case) is a command with subcommands.
///
/// Asked of every command that runs, for its statistics: a `match` the
/// compiler turns into a switch on the length, not a walk down the list
/// comparing strings. `CONTAINER_COMMANDS` is the same list, and a test
/// keeps the two the same.
#[inline]
pub fn has_subcommands(name: &str) -> bool {
    matches!(
        name,
        "ACL"
            | "BACKUP"
            | "CLIENT"
            | "CLUSTER"
            | "COMMAND"
            | "CONFIG"
            | "FUNCTION"
            | "HIMPORT"
            | "HOTKEYS"
            | "LATENCY"
            | "MEMORY"
            | "MODULE"
            | "OBJECT"
            | "PUBSUB"
            | "SCRIPT"
            | "SLOWLOG"
            | "XGROUP"
            | "XINFO"
    )
}

#[cfg(test)]
mod has_subcommands_tests {
    use super::{CONTAINER_COMMANDS, has_subcommands};

    #[test]
    fn the_match_and_the_list_agree() {
        for name in CONTAINER_COMMANDS {
            assert!(has_subcommands(name), "{name} is in the list");
        }
        for name in ["GET", "SET", "ACLX", "XINF", "", "CONFI"] {
            assert_eq!(has_subcommands(name), CONTAINER_COMMANDS.contains(&name));
        }
    }
}

/// The name to check a command's permission under: `CLUSTER|RESET` for a
/// command with subcommands, the plain name for the rest. `name` is the
/// upper-cased command name and `first_arg` the argument that follows it.
pub fn acl_command_name<'a>(name: &'a str, first_arg: Option<&[u8]>) -> std::borrow::Cow<'a, str> {
    match first_arg {
        Some(sub) if CONTAINER_COMMANDS.contains(&name) => {
            let sub = String::from_utf8_lossy(sub).to_ascii_uppercase();
            std::borrow::Cow::Owned(format!("{name}|{sub}"))
        }
        _ => std::borrow::Cow::Borrowed(name),
    }
}

/// Whether a list of command rules permits `cmd_name`. The rules are applied
/// left to right, starting from "nothing allowed":
///   "+@all"       → grant all
///   "-@all"       → deny all
///   "+@string"    → grant all string commands
///   "-@string"    → deny all string commands
///   "+get"        → grant GET specifically
///   "-set"        → deny SET specifically
fn rules_allow(commands: &str, cmd_name: &str, acl_cat: fn(&str) -> Vec<&'static str>) -> bool {
    let cmd_lower = cmd_name.to_lowercase();
    // The command itself, for a `command|subcommand` name.
    let base = cmd_lower.split('|').next().unwrap_or(&cmd_lower);
    let names = |c: &str| c.eq_ignore_ascii_case(&cmd_lower) || c.eq_ignore_ascii_case(base);
    // Start from deny-all, apply rules left to right
    let mut allowed = false;

    for rule in commands.split_whitespace() {
        if rule == "+@all" || rule == "allcommands" {
            allowed = true;
        } else if rule == "-@all" || rule == "nocommands" {
            allowed = false;
        } else if let Some(cat) = rule.strip_prefix("+@") {
            if acl_cat(&cat.to_ascii_lowercase()).iter().any(|c| names(c)) {
                allowed = true;
            }
        } else if let Some(cat) = rule.strip_prefix("-@") {
            if acl_cat(&cat.to_ascii_lowercase()).iter().any(|c| names(c)) {
                allowed = false;
            }
        } else if let Some(c) = rule.strip_prefix('+') {
            if names(c) {
                allowed = true;
            }
        } else if let Some(c) = rule.strip_prefix('-')
            && names(c)
        {
            allowed = false;
        }
    }

    allowed
}

/// Whether one of `patterns` lets `key` be used in the way asked.
fn key_matches(patterns: &[KeyPattern], key: &str, read: bool, write: bool) -> bool {
    patterns.iter().any(|p| {
        (!read || p.read)
            && (!write || p.write)
            && (p.pattern == "*" || crate::commands::helpers::glob_match(&p.pattern, key))
    })
}

/// Whether a user may read `key`, through its own patterns or a selector's.
/// For commands that go looking for keys themselves (`TS.QUERYLABELS`).
pub fn may_read_key(user: &AclUser, key: &str) -> bool {
    user.enabled
        && (key_matches(&user.keys, key, true, false)
            || user
                .selectors
                .iter()
                .any(|s| key_matches(&s.keys, key, true, false)))
}

/// Why a command was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Denied {
    /// The command itself (named as it should be reported: `config|set`).
    Command(String),
    Key(String),
    Channel(String),
}

impl Denied {
    /// The `reason` an ACL log entry gives for it.
    pub fn reason(&self) -> &'static str {
        match self {
            Denied::Command(_) => "command",
            Denied::Key(_) => "key",
            Denied::Channel(_) => "channel",
        }
    }

    /// The command, key or channel that was refused.
    pub fn object(&self) -> &str {
        match self {
            Denied::Command(s) | Denied::Key(s) | Denied::Channel(s) => s,
        }
    }

    /// The error a client is sent, without the leading `-`.
    pub fn message(&self, username: &str) -> String {
        match self {
            Denied::Command(command) => {
                format!("NOPERM User {username} has no permissions to run the '{command}' command")
            }
            Denied::Key(key) => {
                format!("NOPERM User {username} has no permissions to access the '{key}' key")
            }
            Denied::Channel(channel) => format!(
                "NOPERM User {username} has no permissions to access the '{channel}' channel"
            ),
        }
    }
}

/// Write commands that put data into a key without giving any of it back:
/// a `%W~` pattern is enough for them. Every other write command also reads
/// what it writes (`INCR` answers with the value, `LPOP` with the element),
/// and needs `%R~` as well.
const WRITE_ONLY_COMMANDS: &[&str] = &[
    "APPEND",
    "DEL",
    "EXPIRE",
    "EXPIREAT",
    "GEOADD",
    "HDEL",
    "HMSET",
    "HSET",
    "HSETNX",
    "JSON.DEL",
    "JSON.SET",
    "LINSERT",
    "LPUSH",
    "LPUSHX",
    "LREM",
    "LSET",
    "LTRIM",
    "MSET",
    "MSETEX",
    "MSETNX",
    "PERSIST",
    "PEXPIRE",
    "PEXPIREAT",
    "PFADD",
    "PSETEX",
    "RESTORE",
    "RPUSH",
    "RPUSHX",
    "SADD",
    "SETEX",
    "SETNX",
    "SETRANGE",
    "SREM",
    "TS.ADD",
    "TS.CREATE",
    "TS.MADD",
    "UNLINK",
    "XACK",
    "XADD",
    "XDEL",
    "XTRIM",
    "ZADD",
    "ZREM",
    "ZREMRANGEBYLEX",
    "ZREMRANGEBYRANK",
    "ZREMRANGEBYSCORE",
];

/// Commands whose first key is written and whose other keys are only read.
const DESTINATION_FIRST: &[&str] = &[
    "BITOP",
    "GEOSEARCHSTORE",
    "SDIFFSTORE",
    "SINTERSTORE",
    "SUNIONSTORE",
    "ZDIFFSTORE",
    "ZINTERSTORE",
    "ZRANGESTORE",
    "ZUNIONSTORE",
];

/// Commands that take from their first key and put into their second.
const SOURCE_THEN_DESTINATION: &[&str] = &[
    "BLMOVE",
    "BLMOVEM",
    "BRPOPLPUSH",
    "LMOVE",
    "LMOVEM",
    "RENAME",
    "RENAMENX",
    "RPOPLPUSH",
    "SMOVE",
];

/// What a command does to its `nth` key: (reads it, writes it).
fn key_access(name: &str, cmd: &[&[u8]], nth: usize) -> (bool, bool) {
    if !crate::replication::is_write_command(name) {
        return (true, false);
    }
    if name == "SET" {
        // `SET key value GET` answers with the old value.
        let returns_old = cmd.iter().skip(3).any(|a| a.eq_ignore_ascii_case(b"GET"));
        return (returns_old, true);
    }
    if name == "COPY" {
        return if nth == 0 {
            (true, false)
        } else {
            (false, true)
        };
    }
    if WRITE_ONLY_COMMANDS.contains(&name) {
        return (false, true);
    }
    if DESTINATION_FIRST.contains(&name) {
        return if nth == 0 {
            (false, true)
        } else {
            (true, false)
        };
    }
    if SOURCE_THEN_DESTINATION.contains(&name) && nth > 0 {
        return (false, true);
    }
    (true, true)
}

/// Commands that name channels, and how: (first channel argument, the
/// arguments are patterns).
fn channel_arguments(name: &str) -> Option<(std::ops::RangeFrom<usize>, bool, bool)> {
    // (arguments, only the first is a channel, they are patterns)
    match name {
        "PUBLISH" | "SPUBLISH" => Some((1.., true, false)),
        "SUBSCRIBE" | "SSUBSCRIBE" => Some((1.., false, false)),
        "PSUBSCRIBE" => Some((1.., false, true)),
        _ => None,
    }
}

/// One set of permissions — the user's own, or a selector's — against one
/// command.
fn permissions_allow(
    commands: &str,
    keys: &[KeyPattern],
    channels: &[String],
    name: &str,
    cmd: &[&[u8]],
) -> Result<(), Denied> {
    let acl_name = acl_command_name(name, cmd.get(1).copied());
    if !rules_allow(
        commands,
        &acl_name,
        crate::commands::server::acl_cat_commands_pub,
    ) {
        return Err(Denied::Command(acl_name.to_lowercase()));
    }

    if let Some((args, first_only, patterns)) = channel_arguments(name) {
        if !all_channels(channels) {
            let take = if first_only { 1 } else { usize::MAX };
            for arg in cmd.iter().skip(args.start).take(take) {
                let channel = String::from_utf8_lossy(arg);
                let allowed = if patterns {
                    // A pattern subscription is allowed only for a pattern
                    // the user was given, letter for letter.
                    channels.iter().any(|c| c == channel.as_ref())
                } else {
                    channels
                        .iter()
                        .any(|c| crate::commands::helpers::glob_match(c, &channel))
                };
                if !allowed {
                    return Err(Denied::Channel(channel.into_owned()));
                }
            }
        }
        // The channel of SPUBLISH / SSUBSCRIBE is listed as a key for cluster
        // routing; it is a channel, and was checked as one.
        return Ok(());
    }

    if !all_keys(keys) {
        for (nth, pos) in crate::keyspec::key_positions(name, cmd).iter().enumerate() {
            let key = String::from_utf8_lossy(cmd[pos]);
            let (read, write) = key_access(name, cmd, nth);
            if !key_matches(keys, &key, read, write) {
                return Err(Denied::Key(key.into_owned()));
            }
        }
    }
    Ok(())
}

/// Whether `user` may run this command: the command, every key it names, and
/// every channel it names, under the user's own rules or under any one of its
/// selectors.
///
/// `name` is the upper-cased command name and `cmd` the whole command, name
/// included. The reason returned is the one the user's own rules give.
pub fn check_permission<A: AsRef<[u8]>>(
    user: &AclUser,
    name: &str,
    cmd: &[A],
) -> Result<(), Denied> {
    // `AUTH` and `HELLO` are how a connection becomes somebody: they are
    // not subject to what the user it is now may run, as in Redis (its
    // `CMD_NO_AUTH` commands). Checked, `HELLO 3 AUTH user pass` was refused
    // for a default user of `-@all +auth` before the `AUTH` in it was ever
    // looked at, and the clients that open with it could not connect.
    if matches!(name, "AUTH" | "HELLO") {
        return Ok(());
    }
    let cmd: Vec<&[u8]> = cmd.iter().map(AsRef::as_ref).collect();
    if !user.enabled {
        return Err(Denied::Command(
            acl_command_name(name, cmd.get(1).copied()).to_lowercase(),
        ));
    }
    let own = permissions_allow(&user.commands, &user.keys, &user.channels, name, &cmd);
    if own.is_ok() {
        return own;
    }
    for selector in &user.selectors {
        if permissions_allow(
            &selector.commands,
            &selector.keys,
            &selector.channels,
            name,
            &cmd,
        )
        .is_ok()
        {
            return Ok(());
        }
    }
    own
}

/// Updates the `RESTRICTED_KEY_CLIENTS`/`RESTRICTED_CMD_CLIENTS` counters and
/// the paired connection-local flags after a connection authenticates or
/// re-authenticates as `user`. Both server loops call this from every place
/// that can change `authenticated_user` (plain `AUTH` and `HELLO ... AUTH`),
/// so the counters — and therefore enforcement in `execute_command` — never
/// fall out of sync with who's actually connected.
pub fn track_restriction_counters(
    user: &AclUser,
    key_restricted: &mut bool,
    cmd_restricted: &mut bool,
) {
    use std::sync::atomic::Ordering::Relaxed;

    // A user with no key patterns at all may use no keys: that is the most
    // restricted a user can be, and it used to be counted as unrestricted.
    let new_key_restricted = !user.all_keys();
    if *key_restricted && !new_key_restricted {
        RESTRICTED_KEY_CLIENTS.fetch_sub(1, Relaxed);
    } else if !*key_restricted && new_key_restricted {
        RESTRICTED_KEY_CLIENTS.fetch_add(1, Relaxed);
    }
    *key_restricted = new_key_restricted;

    let new_cmd_restricted = user.commands != "+@all" || !user.all_channels() || !user.enabled;
    if *cmd_restricted && !new_cmd_restricted {
        RESTRICTED_CMD_CLIENTS.fetch_sub(1, Relaxed);
    } else if !*cmd_restricted && new_cmd_restricted {
        RESTRICTED_CMD_CLIENTS.fetch_add(1, Relaxed);
    }
    *cmd_restricted = new_cmd_restricted;
}

/// A connection stops being any user at all (`RESET` on a server whose
/// default user needs a password): give back whatever it held in the
/// restricted-client counters.
pub fn clear_restriction_counters(key_restricted: &mut bool, cmd_restricted: &mut bool) {
    use std::sync::atomic::Ordering::Relaxed;
    if std::mem::take(key_restricted) {
        RESTRICTED_KEY_CLIENTS.fetch_sub(1, Relaxed);
    }
    if std::mem::take(cmd_restricted) {
        RESTRICTED_CMD_CLIENTS.fetch_sub(1, Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(rules: &[&str]) -> AclUser {
        let mut user = AclUser::new("u");
        for rule in rules {
            apply_rule(&mut user, rule).unwrap_or_else(|e| panic!("{rule}: {e}"));
        }
        user
    }

    fn check(user: &AclUser, cmd: &[&str]) -> Result<(), Denied> {
        check_permission(user, &cmd[0].to_ascii_uppercase(), cmd)
    }

    #[test]
    fn a_user_holds_every_key_pattern_it_is_given() {
        let u = user(&["on", "nopass", "~app:*", "~session:*", "+@all"]);
        assert_eq!(u.keys_string(), "~app:* ~session:*");
        assert!(check(&u, &["SET", "app:x", "1"]).is_ok());
        assert!(check(&u, &["SET", "session:x", "1"]).is_ok());
        assert_eq!(
            check(&u, &["SET", "other:x", "1"]),
            Err(Denied::Key("other:x".into()))
        );
        // Every key of a multi-key command.
        assert!(check(&u, &["MGET", "app:a", "session:b"]).is_ok());
        assert_eq!(
            check(&u, &["MGET", "app:a", "other"]),
            Err(Denied::Key("other".into()))
        );
    }

    #[test]
    fn no_key_patterns_means_no_keys() {
        let u = user(&["on", "nopass", "+@all"]);
        assert_eq!(check(&u, &["GET", "k"]), Err(Denied::Key("k".into())));
        // Commands without keys are not affected.
        assert!(check(&u, &["PING"]).is_ok());
        let (mut key_restricted, mut cmd_restricted) = (false, false);
        track_restriction_counters(&u, &mut key_restricted, &mut cmd_restricted);
        assert!(key_restricted);
        // Undo, so the process-wide counter is left as it was found.
        let open = AclUser::default_user("");
        track_restriction_counters(&open, &mut key_restricted, &mut cmd_restricted);
    }

    #[test]
    fn read_and_write_patterns() {
        let u = user(&["on", "nopass", "%R~ro:*", "%W~wo:*", "%RW~rw:*", "+@all"]);
        assert!(check(&u, &["GET", "ro:a"]).is_ok());
        assert!(check(&u, &["SET", "ro:a", "1"]).is_err());
        assert!(check(&u, &["SET", "wo:a", "1"]).is_ok());
        assert!(check(&u, &["GET", "wo:a"]).is_err());
        // SET … GET reads the old value.
        assert!(check(&u, &["SET", "wo:a", "1", "GET"]).is_err());
        assert!(check(&u, &["SET", "rw:a", "1", "GET"]).is_ok());
        // INCR answers with the value: it needs both.
        assert!(check(&u, &["INCR", "wo:n"]).is_err());
        assert!(check(&u, &["INCR", "rw:n"]).is_ok());
        // COPY reads its source and writes its destination.
        assert!(check(&u, &["COPY", "ro:a", "wo:b"]).is_ok());
        assert!(check(&u, &["COPY", "wo:a", "ro:b"]).is_err());
        assert!(check(&u, &["SUNIONSTORE", "wo:dst", "ro:a", "ro:b"]).is_ok());
        assert_eq!(u.keys_string(), "%R~ro:* %W~wo:* ~rw:*");
    }

    #[test]
    fn channels_are_enforced() {
        let u = user(&["on", "nopass", "~*", "&news.*", "+@all"]);
        assert!(check(&u, &["PUBLISH", "news.uk", "hello"]).is_ok());
        assert_eq!(
            check(&u, &["PUBLISH", "secret", "hello"]),
            Err(Denied::Channel("secret".into()))
        );
        assert!(check(&u, &["SUBSCRIBE", "news.uk", "news.fr"]).is_ok());
        assert!(check(&u, &["SUBSCRIBE", "news.uk", "other"]).is_err());
        // A pattern subscription must be one of the user's patterns exactly.
        assert!(check(&u, &["PSUBSCRIBE", "news.*"]).is_ok());
        assert!(check(&u, &["PSUBSCRIBE", "news.u*"]).is_err());
        assert!(check(&u, &["PSUBSCRIBE", "*"]).is_err());
        // A new user has no channels.
        let none = user(&["on", "nopass", "~*", "+@all"]);
        assert!(check(&none, &["PUBLISH", "c", "m"]).is_err());
        let all = user(&["on", "nopass", "~*", "&*", "+@all"]);
        assert!(check(&all, &["PSUBSCRIBE", "*"]).is_ok());
    }

    #[test]
    fn selectors_are_alternatives() {
        let u = user(&["on", "nopass", "~app:*", "+@all", "(~cache:* +get)"]);
        assert!(check(&u, &["SET", "app:x", "1"]).is_ok());
        assert!(check(&u, &["GET", "cache:x"]).is_ok());
        // The selector allows GET only, and only on cache:*.
        assert_eq!(
            check(&u, &["SET", "cache:x", "1"]),
            Err(Denied::Key("cache:x".into()))
        );
        // Not a mixture: one set of permissions has to cover the whole command.
        assert!(check(&u, &["MGET", "app:x", "cache:x"]).is_err());
        assert_eq!(
            u.to_acl_string(),
            "user u on nopass ~app:* resetchannels +@all (~cache:* resetchannels -@all +get)"
        );
        let mut u = u;
        apply_rule(&mut u, "clearselectors").unwrap();
        assert!(check(&u, &["GET", "cache:x"]).is_err());
    }

    #[test]
    fn a_selector_split_over_several_arguments_is_put_back_together() {
        let merged =
            merge_selector_args(["on", "(~k*", "+get", "+set)", "~a"].into_iter()).unwrap();
        assert_eq!(merged, ["on", "(~k* +get +set)", "~a"]);
        assert!(merge_selector_args(["(~k*", "+get"].into_iter()).is_err());
    }

    #[test]
    fn rules_that_mean_nothing_are_refused() {
        let mut u = AclUser::new("u");
        for bad in [
            "bogus",
            "+@nosuchcategory",
            "-@nosuchcategory",
            "+nosuchcommand",
            "+get|x",
            "#abc",
            "%X~k",
            "~",
            "&",
            "(bogus)",
            "<never-set",
        ] {
            assert!(apply_rule(&mut u, bad).is_err(), "{bad} was accepted");
        }
        apply_rule(&mut u, "~*").unwrap();
        assert!(apply_rule(&mut u, "~more").is_err());
        for good in [
            "+get",
            "-@dangerous",
            "+config|get",
            "+@all",
            "resetkeys",
            "~a",
            "~b",
        ] {
            apply_rule(&mut u, good).unwrap_or_else(|e| panic!("{good}: {e}"));
        }
    }

    #[test]
    fn a_user_without_a_password_is_not_written_as_nopass() {
        let u = user(&["on", "~*", "+@all"]);
        assert_eq!(u.to_acl_string(), "user u on ~* resetchannels +@all");
        assert!(!u.accepts(""));
        assert!(!u.accepts("anything"));
    }

    #[test]
    fn an_acl_file_defines_its_users_from_nothing() {
        let hash = sha256_hex("pw");
        let file = format!(
            "# users\n\nuser default on #{hash} ~* &* +@all\nuser app on #{hash} ~app:* ~session:* &* +@all -flushall\n"
        );
        let users = parse_acl_file(&file, "").unwrap();
        assert_eq!(users.len(), 2);
        assert!(!users["default"].nopass);
        assert!(users["default"].accepts("pw"));
        assert_eq!(users["app"].keys.len(), 2);
        // What is written reads back the same.
        for user in users.values() {
            let again = parse_acl_file(&user.to_acl_string(), "").unwrap();
            assert_eq!(again[&user.name].to_acl_string(), user.to_acl_string());
        }
    }

    #[test]
    fn a_file_without_a_default_line_leaves_default_as_requirepass_made_it() {
        let users = parse_acl_file("user app on nopass ~* +@all\n", "").unwrap();
        assert!(users["default"].nopass);
        let users = parse_acl_file("user app on nopass ~* +@all\n", "secret").unwrap();
        assert!(!users["default"].nopass);
        assert!(users["default"].accepts("secret"));
    }

    #[test]
    fn a_bad_acl_file_is_refused_whole_and_says_where() {
        for (file, needle) in [
            ("user a on nopass\nnonsense line\n", "line 2"),
            ("user a on +@nosuchcategory\n", "line 1"),
            ("user a on\nuser a off\n", "duplicate user 'a'"),
            ("user\n", "line 1"),
            ("user a (~k +get\n", "parenthesis"),
        ] {
            let err = parse_acl_file(file, "").unwrap_err();
            assert!(err.contains(needle), "{file:?}: {err}");
        }
    }

    #[test]
    fn the_default_user_is_open_only_without_a_password() {
        let store = new_acl_store("");
        assert!(default_user_is_open(&store));
        apply_rule(store.write().get_mut("default").unwrap(), ">secret").unwrap();
        assert!(!default_user_is_open(&store));
        assert!(authenticate(&store, "default", "secret").is_some());
        assert!(authenticate(&store, "default", "wrong").is_none());
        assert!(authenticate(&store, "nobody", "secret").is_none());
        apply_rule(store.write().get_mut("default").unwrap(), "nopass").unwrap();
        apply_rule(store.write().get_mut("default").unwrap(), "off").unwrap();
        assert!(!default_user_is_open(&store));
    }
}
