use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

pub const KS_KEYSPACE: u32 = 1 << 0;
pub const KS_KEYEVENT: u32 = 1 << 1;
pub const KS_GENERIC: u32 = 1 << 2;
pub const KS_STRING: u32 = 1 << 3;
pub const KS_LIST: u32 = 1 << 4;
pub const KS_SET: u32 = 1 << 5;
pub const KS_SORTED_SET: u32 = 1 << 6;
pub const KS_EXPIRED: u32 = 1 << 7;
pub const KS_EVICTED: u32 = 1 << 8;
pub const KS_HASH: u32 = 1 << 9;
pub const KS_STREAM: u32 = 1 << 10;
pub const KS_KEY_MISS: u32 = 1 << 11;
/// `a` (Redis 8.10): array commands. One of `A`'s.
pub const KS_ARRAY: u32 = 1 << 19;
/// `d`: the module types' commands (JSON, time series, vector sets). One of
/// `A`'s, as in Redis.
pub const KS_MODULE: u32 = 1 << 20;
/// `n`: a key that was not there has been created. (Not one of `A`'s.)
pub const KS_NEW: u32 = 1 << 16;
/// `o` (Redis 8.2): a key's value has been replaced whole, by `SET`, a
/// `*STORE`, `COPY`, `RENAME` and the like. (Not one of `A`'s.)
pub const KS_OVERWRITTEN: u32 = 1 << 17;
/// `c` (Redis 8.2): it was replaced by a value of another type. (Not one of
/// `A`'s.)
pub const KS_TYPE_CHANGED: u32 = 1 << 18;
// Subkey notifications (Redis 8.8): which fields of a hash a command touched.
// Four channel shapes, each enabled by its own flag, independent of the
// key-level flags above.
pub const KS_SUBKEYSPACE: u32 = 1 << 12;
pub const KS_SUBKEYEVENT: u32 = 1 << 13;
pub const KS_SUBKEYSPACEITEM: u32 = 1 << 14;
pub const KS_SUBKEYSPACEEVENT: u32 = 1 << 15;
const KS_SUBKEY_ANY: u32 =
    KS_SUBKEYSPACE | KS_SUBKEYEVENT | KS_SUBKEYSPACEITEM | KS_SUBKEYSPACEEVENT;

pub static KS_FLAGS: AtomicU32 = AtomicU32::new(0);

pub fn parse_ks_flags(s: &str) -> u32 {
    if s.is_empty() {
        return 0;
    }
    let mut flags: u32 = 0;
    for c in s.chars() {
        match c {
            'K' => flags |= KS_KEYSPACE,
            'E' => flags |= KS_KEYEVENT,
            'g' => flags |= KS_GENERIC,
            '$' => flags |= KS_STRING,
            'l' => flags |= KS_LIST,
            's' => flags |= KS_SET,
            'z' => flags |= KS_SORTED_SET,
            'x' => flags |= KS_EXPIRED,
            'e' => flags |= KS_EVICTED,
            'h' => flags |= KS_HASH,
            't' => flags |= KS_STREAM,
            'm' => flags |= KS_KEY_MISS,
            'a' => flags |= KS_ARRAY,
            'd' => flags |= KS_MODULE,
            'n' => flags |= KS_NEW,
            'o' => flags |= KS_OVERWRITTEN,
            'c' => flags |= KS_TYPE_CHANGED,
            'S' => flags |= KS_SUBKEYSPACE,
            'T' => flags |= KS_SUBKEYEVENT,
            'I' => flags |= KS_SUBKEYSPACEITEM,
            'V' => flags |= KS_SUBKEYSPACEEVENT,
            'A' => {
                flags |= KS_GENERIC
                    | KS_STRING
                    | KS_LIST
                    | KS_SET
                    | KS_SORTED_SET
                    | KS_EXPIRED
                    | KS_EVICTED
                    | KS_HASH
                    | KS_STREAM
                    | KS_ARRAY
                    | KS_MODULE;
            }
            _ => {}
        }
    }
    flags
}

/// The characters `notify-keyspace-events` takes.
const KS_FLAG_CHARS: &str = "KEg$lshzxetmdnaocASTIV";

/// [`parse_ks_flags`], or `None` for a character that is not a flag: `CONFIG
/// SET` and the config file refuse it, as Redis does. It was dropped
/// (`KEAQ!` was `AKE`, #160).
pub fn parse_ks_flags_strict(s: &str) -> Option<u32> {
    s.chars()
        .all(|c| KS_FLAG_CHARS.contains(c))
        .then(|| parse_ks_flags(s))
}

/// Redis's error for a bad `notify-keyspace-events`.
pub const BAD_FLAGS: &str = "Invalid event class character. Use 'Ag$lshzxeKEtmdn'.";

/// An event a command announces, found from the command and its reply: for
/// the commands that publish nothing of their own (#160).
pub struct CommandEvent {
    pub class: u32,
    pub event: String,
    /// Where in the command the key is.
    pub key: usize,
    /// Another database than the one selected (`MOVE`'s `move_to`).
    pub db: Option<usize>,
}

/// The events Redis (or its module) announces for `name`, which ran and
/// answered `reply` (not an error).
pub fn command_events(name: &str, cmd: &[&[u8]], reply: &[u8]) -> Vec<CommandEvent> {
    let int = || {
        reply
            .strip_prefix(b":")
            .and_then(|rest| std::str::from_utf8(rest).ok())
            .and_then(|rest| rest.trim_end().parse::<i64>().ok())
    };
    let one = |class: u32, event: &str, key: usize| {
        vec![CommandEvent {
            class,
            event: event.to_string(),
            key,
            db: None,
        }]
    };
    let has = |word: &[u8]| cmd.iter().skip(2).any(|arg| arg.eq_ignore_ascii_case(word));
    match name {
        // An empty value changes nothing, and Redis says nothing then.
        "SETRANGE" if cmd.len() == 4 && !cmd[3].is_empty() => one(KS_STRING, "setrange", 1),
        "SETBIT" => one(KS_STRING, "setbit", 1),
        "BITFIELD" if has(b"SET") || has(b"INCRBY") => one(KS_STRING, "setbit", 1),
        // An empty result deletes the destination.
        "BITOP" if cmd.len() > 2 => match int() {
            Some(0) => one(KS_GENERIC, "del", 2),
            _ => one(KS_STRING, "set", 2),
        },
        "PFADD" if int() == Some(1) => one(KS_STRING, "pfadd", 1),
        "PFMERGE" => one(KS_STRING, "pfadd", 1),
        // A geo set is a sorted set: `GEOADD` is a `ZADD`.
        "GEOADD" if int() != Some(0) || has(b"CH") || has(b"XX") => one(KS_SORTED_SET, "zadd", 1),
        "MOVE" if int() == Some(1) && cmd.len() == 3 => {
            let to = std::str::from_utf8(cmd[2])
                .ok()
                .and_then(|db| db.parse().ok());
            vec![
                CommandEvent {
                    class: KS_GENERIC,
                    event: "move_from".into(),
                    key: 1,
                    db: None,
                },
                CommandEvent {
                    class: KS_GENERIC,
                    event: "move_to".into(),
                    key: 1,
                    db: to,
                },
            ]
        }
        // The modules' events are their commands' names, for each key.
        "TS.MADD" => (1..cmd.len())
            .step_by(3)
            .flat_map(|key| one(KS_MODULE, "ts.add", key))
            .collect(),
        "TS.CREATERULE" | "TS.DELETERULE" => Vec::new(),
        "JSON.DEL" | "JSON.FORGET" | "JSON.CLEAR" | "VREM" | "VSETATTR" if int() == Some(0) => {
            Vec::new()
        }
        // A JSON write that changed nothing announces nothing, as in
        // RedisJSON (#575).
        _ if name.starts_with("JSON.") && json_changed_nothing(name, reply) => Vec::new(),
        _ if name.starts_with("JSON.")
            || name.starts_with("TS.")
            || matches!(name, "VADD" | "VREM" | "VSETATTR") =>
        {
            // `JSON.FORGET` is RedisJSON's `JSON.DEL`, event and all.
            let event = match name {
                "JSON.FORGET" => "json.del".to_string(),
                _ => name.to_ascii_lowercase(),
            };
            crate::keyspec::key_positions(name, cmd)
                .iter()
                .flat_map(|key| one(KS_MODULE, &event, key))
                .collect()
        }
        _ => Vec::new(),
    }
}

/// Whether the reply of the JSON write `name` says it changed nothing: nil
/// (`JSON.SET`'s `NX` or `XX` not met, a key that is not there), or nil for
/// every value its JSONPath matched (none of the type the command works on,
/// or no match at all), as a RESP array or, for `JSON.NUMINCRBY` and
/// `JSON.NUMMULTBY` in RESP2, as a JSON array. RedisJSON announces an event
/// only once a value was changed.
fn json_changed_nothing(name: &str, reply: &[u8]) -> bool {
    const NILS: [&[u8]; 3] = [b"$-1\r\n", b"_\r\n", b"*-1\r\n"];
    if NILS.contains(&reply) {
        return true;
    }
    let line = |bytes: &[u8]| -> Option<(usize, usize)> {
        let end = bytes.windows(2).position(|w| w == b"\r\n")?;
        let n = std::str::from_utf8(&bytes[1..end]).ok()?.parse().ok()?;
        Some((n, end + 2))
    };
    match reply.first() {
        Some(b'*') => {
            let Some((count, mut at)) = line(reply) else {
                return false;
            };
            for _ in 0..count {
                let Some(nil) = NILS.iter().find(|nil| reply[at..].starts_with(nil)) else {
                    return false;
                };
                at += nil.len();
            }
            at == reply.len()
        }
        Some(b'$') if matches!(name, "JSON.NUMINCRBY" | "JSON.NUMMULTBY") => {
            let Some((len, at)) = line(reply) else {
                return false;
            };
            let Some(text) = reply
                .get(at..at + len)
                .and_then(|text| std::str::from_utf8(text).ok())
            else {
                return false;
            };
            text.strip_prefix('[')
                .and_then(|text| text.strip_suffix(']'))
                .is_some_and(|inner| {
                    inner.is_empty() || inner.split(',').all(|v| v.trim() == "null")
                })
        }
        _ => false,
    }
}

/// The classes of event `A` stands for.
const KS_ALL: u32 = KS_GENERIC
    | KS_STRING
    | KS_LIST
    | KS_SET
    | KS_SORTED_SET
    | KS_EXPIRED
    | KS_EVICTED
    | KS_HASH
    | KS_STREAM
    | KS_ARRAY
    | KS_MODULE;

/// The flags as `CONFIG GET notify-keyspace-events` writes them — in
/// Redis's order, whatever order they were given in: the classes of event
/// (`A` for all of them), then `K` and `E`, then the rest.
pub fn flags_to_string(flags: u32) -> String {
    let mut s = String::new();
    if flags & KS_ALL == KS_ALL {
        s.push('A');
    } else {
        for (flag, c) in [
            (KS_GENERIC, 'g'),
            (KS_STRING, '$'),
            (KS_LIST, 'l'),
            (KS_SET, 's'),
            (KS_HASH, 'h'),
            (KS_SORTED_SET, 'z'),
            (KS_EXPIRED, 'x'),
            (KS_EVICTED, 'e'),
            (KS_STREAM, 't'),
            (KS_ARRAY, 'a'),
            (KS_MODULE, 'd'),
        ] {
            if flags & flag != 0 {
                s.push(c);
            }
        }
    }
    for (flag, c) in [
        (KS_KEYSPACE, 'K'),
        (KS_KEYEVENT, 'E'),
        (KS_KEY_MISS, 'm'),
        (KS_NEW, 'n'),
        (KS_OVERWRITTEN, 'o'),
        (KS_TYPE_CHANGED, 'c'),
        (KS_SUBKEYSPACE, 'S'),
        (KS_SUBKEYEVENT, 'T'),
        (KS_SUBKEYSPACEITEM, 'I'),
        (KS_SUBKEYSPACEEVENT, 'V'),
    ] {
        if flags & flag != 0 {
            s.push(c);
        }
    }
    s
}

/// Where a command replaces a key's value whole, as Redis's `setKey` does:
/// the keys `overwritten` and `type_changed` are about. `(db, position)`,
/// `db` given only when it is not the selected one (`COPY ... DB n`).
pub fn whole_value_targets(name: &str, cmd: &[&[u8]]) -> Vec<(Option<usize>, usize)> {
    let after = |word: &[u8]| {
        cmd.iter()
            .position(|arg| arg.eq_ignore_ascii_case(word))
            .map(|at| at + 1)
            .filter(|at| *at < cmd.len())
    };
    let here = |positions: Vec<usize>| positions.into_iter().map(|at| (None, at)).collect();
    match name {
        "SET" | "SETEX" | "PSETEX" | "SETNX" | "GETSET" | "SINTERSTORE" | "SUNIONSTORE"
        | "SDIFFSTORE" | "ZUNIONSTORE" | "ZINTERSTORE" | "ZDIFFSTORE" | "ZRANGESTORE"
        | "GEOSEARCHSTORE" | "RESTORE"
            if cmd.len() > 1 =>
        {
            here(vec![1])
        }
        "BITOP" | "RENAME" | "RENAMENX" if cmd.len() > 2 => here(vec![2]),
        "MSET" | "MSETNX" => here((1..cmd.len()).step_by(2).collect()),
        // MSETEX numkeys key value [key value ...] [options]
        "MSETEX" => {
            let n = cmd
                .get(1)
                .and_then(|n| std::str::from_utf8(n).ok()?.parse::<usize>().ok())
                .unwrap_or(0);
            here(
                (0..n)
                    .map(|i| 2 + 2 * i)
                    .filter(|at| *at < cmd.len())
                    .collect(),
            )
        }
        "SORT" => here(after(b"STORE").into_iter().collect()),
        "GEORADIUS" | "GEORADIUSBYMEMBER" => here(
            after(b"STORE")
                .or_else(|| after(b"STOREDIST"))
                .into_iter()
                .collect(),
        ),
        "HIMPORT" if cmd.len() > 2 && cmd[1].eq_ignore_ascii_case(b"SET") => here(vec![2]),
        "COPY" if cmd.len() > 2 => {
            let db = after(b"DB")
                .and_then(|at| std::str::from_utf8(cmd[at]).ok()?.parse::<usize>().ok());
            vec![(db, 2)]
        }
        _ => Vec::new(),
    }
}

/// The pub/sub hub of each server in this process, by its store. Expiry
/// happens where there is no connection — in the background task, and deep
/// inside a read that found the key had expired — and this is how it finds
/// where to publish. (A process holds one server, except under test.)
static HUBS: parking_lot::RwLock<Vec<(usize, crate::pubsub::PubSubHub)>> =
    parking_lot::RwLock::new(Vec::new());

fn store_id(store: &crate::store::Store) -> usize {
    Arc::as_ptr(store) as *const () as usize
}

/// Make `hub` the place where events about `store`'s keys are published.
pub fn register_hub(store: &crate::store::Store, hub: &crate::pubsub::PubSubHub) {
    let id = store_id(store);
    let mut hubs = HUBS.write();
    hubs.retain(|(known, _)| *known != id);
    hubs.push((id, hub.clone()));
}

/// The hub registered for `store`.
pub fn hub_for(store: &crate::store::Store) -> Option<crate::pubsub::PubSubHub> {
    hub_for_id(store_id(store))
}

fn hub_for_id(id: usize) -> Option<crate::pubsub::PubSubHub> {
    HUBS.read()
        .iter()
        .find(|(known, _)| *known == id)
        .map(|(_, hub)| hub.clone())
}

/// Writes put new values over `keys`, which were past their time, in
/// database `db` of the store at `store`: `expired` for each, as Redis
/// announces a key a write finds expired, before the write's own events.
pub fn written_over_expired(store: usize, db: usize, keys: &[crate::store::Key]) {
    if KS_FLAGS.load(Ordering::Relaxed) & KS_EXPIRED == 0 {
        return;
    }
    if let Some(hub) = hub_for_id(store) {
        for key in keys {
            notify(&hub, db, KS_EXPIRED, "expired", key);
        }
    }
}

/// A key's TTL ran out and the key was removed: publish `expired`.
///
/// Nothing was published for this before — not when a read found the key
/// expired, and not when the background task removed it.
#[inline]
pub fn notify_expired(store: &crate::store::Store, db: usize, key: &(impl AsRef<[u8]> + ?Sized)) {
    if KS_FLAGS.load(Ordering::Relaxed) & KS_EXPIRED == 0 {
        return;
    }
    if let Some(hub) = hub_for(store) {
        notify(&hub, db, KS_EXPIRED, "expired", key);
    }
}

#[inline]
pub fn notify(
    pubsub: &crate::pubsub::PubSubHub,
    db: usize,
    type_flag: u32,
    event: &str,
    key: &(impl AsRef<[u8]> + ?Sized),
) {
    let flags = KS_FLAGS.load(Ordering::Relaxed);
    if flags == 0 {
        return;
    }
    if flags & type_flag == 0 {
        return;
    }
    let key = key.as_ref();
    if flags & KS_KEYSPACE != 0 {
        // Any bytes: the channel is the prefix and the key as it is.
        let mut ch = format!("__keyspace@{db}__:").into_bytes();
        ch.extend_from_slice(key);
        publish(pubsub, ch, Arc::from(event.as_bytes()));
    }
    if flags & KS_KEYEVENT != 0 {
        let ch = format!("__keyevent@{}__:{}", db, event);
        publish(pubsub, ch.into_bytes(), Arc::from(key));
    }
}

/// A channel and what is published on it.
type Held = Vec<(Vec<u8>, Arc<[u8]>)>;

thread_local! {
    /// Events a command announced while they were held back (see [`hold`]).
    static HELD: std::cell::RefCell<Option<Held>> =
        const { std::cell::RefCell::new(None) };
}

fn publish(pubsub: &crate::pubsub::PubSubHub, channel: Vec<u8>, payload: Arc<[u8]>) {
    let now = HELD.with_borrow_mut(|held| match held {
        Some(held) => {
            held.push((channel, payload));
            None
        }
        None => Some((channel, payload)),
    });
    if let Some((channel, payload)) = now {
        pubsub.publish(&channel, payload);
    }
}

/// Hold back the events the command now running on this thread announces,
/// until [`release`]: so that `overwritten` and `type_changed`, which are
/// only known once it has run, can go before them, as Redis's `setKey`
/// sends them.
pub fn hold() {
    HELD.set(Some(Vec::new()));
}

/// The held events, and holding no longer: announce what goes first, then
/// [`replay`] these.
pub fn release() -> Held {
    HELD.take().unwrap_or_default()
}

pub fn replay(pubsub: &crate::pubsub::PubSubHub, held: Held) {
    for (channel, payload) in held {
        pubsub.publish(&channel, payload);
    }
}

/// Whether a command announces `overwritten` before its own events (the
/// commands that go through Redis's `setKey`) rather than after them
/// (RENAME, COPY and RESTORE, which announce it themselves at the end).
pub fn overwritten_goes_first(name: &str) -> bool {
    !matches!(name, "RENAME" | "RENAMENX" | "COPY" | "RESTORE")
}

/// `<len>:<subkey>[,<len>:<subkey>...]` — length-prefixed, so a subkey may
/// contain any byte, the separators included.
fn encode_subkeys(subkeys: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for (i, subkey) in subkeys.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(subkey.len().to_string().as_bytes());
        out.push(b':');
        out.extend_from_slice(subkey);
    }
    out
}

/// The messages one subkey event produces, as `(channel, payload)`, for the
/// channel shapes enabled in `flags`.
fn subkey_messages(
    flags: u32,
    db: usize,
    event: &str,
    key: &[u8],
    subkeys: &[&[u8]],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    // A channel: `head`, the key, then `tail`.
    let channel = |head: String, tail: &[u8]| {
        let mut ch = head.into_bytes();
        ch.extend_from_slice(key);
        ch.extend_from_slice(tail);
        ch
    };
    let mut out = Vec::new();
    if subkeys.is_empty() {
        return out;
    }
    let list = encode_subkeys(subkeys);
    // An event name containing `|` could not be told apart from the payload.
    let unambiguous = !event.contains('|');

    if flags & KS_SUBKEYSPACE != 0 && unambiguous {
        // `__subkeyspace@<db>__:<key>` → `<event>|<subkeys>`
        let mut payload = event.as_bytes().to_vec();
        payload.push(b'|');
        payload.extend_from_slice(&list);
        out.push((channel(format!("__subkeyspace@{db}__:"), b""), payload));
    }
    if flags & KS_SUBKEYEVENT != 0 {
        // `__subkeyevent@<db>__:<event>` → `<key_len>:<key>|<subkeys>`
        let mut payload = format!("{}:", key.len()).into_bytes();
        payload.extend_from_slice(key);
        payload.push(b'|');
        payload.extend_from_slice(&list);
        out.push((
            format!("__subkeyevent@{db}__:{event}").into_bytes(),
            payload,
        ));
    }
    if flags & KS_SUBKEYSPACEITEM != 0 && !key.contains(&b'\n') {
        // `__subkeyspaceitem@<db>__:<key>\n<subkey>` → `<event>`, one per subkey.
        // Newline separates key from subkey, so a key containing one is skipped.
        for subkey in subkeys {
            let mut tail = vec![b'\n'];
            tail.extend_from_slice(subkey);
            out.push((
                channel(format!("__subkeyspaceitem@{db}__:"), &tail),
                event.as_bytes().to_vec(),
            ));
        }
    }
    if flags & KS_SUBKEYSPACEEVENT != 0 && unambiguous {
        // `__subkeyspaceevent@<db>__:<event>|<key>` → `<subkeys>`
        out.push((
            channel(format!("__subkeyspaceevent@{db}__:{event}|"), b""),
            list,
        ));
    }
    out
}

/// Publish a subkey notification: `event` happened to these fields of `key`.
/// Nothing is published for an empty list, and nothing is computed when no
/// subkey channel is enabled.
#[inline]
pub fn notify_subkeys<'a>(
    pubsub: &crate::pubsub::PubSubHub,
    db: usize,
    event: &str,
    key: &(impl AsRef<[u8]> + ?Sized),
    subkeys: impl IntoIterator<Item = &'a [u8]>,
) {
    let flags = KS_FLAGS.load(Ordering::Relaxed);
    if flags & KS_SUBKEY_ANY == 0 {
        return;
    }
    let subkeys: Vec<&[u8]> = subkeys.into_iter().collect();
    for (channel, payload) in subkey_messages(flags, db, event, key.as_ref(), &subkeys) {
        pubsub.publish(&channel, Arc::from(payload));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nil_replies_change_nothing() {
        for reply in [&b"$-1\r\n"[..], b"_\r\n", b"*0\r\n", b"*2\r\n$-1\r\n_\r\n"] {
            assert!(json_changed_nothing("JSON.TOGGLE", reply), "{reply:?}");
        }
        assert!(json_changed_nothing("JSON.NUMINCRBY", b"$6\r\n[null]\r\n"));
        assert!(json_changed_nothing("JSON.NUMINCRBY", b"$2\r\n[]\r\n"));
    }

    #[test]
    fn a_change_is_a_change() {
        for reply in [
            &b"+OK\r\n"[..],
            b":3\r\n",
            b"*2\r\n$-1\r\n:1\r\n",
            b"$4\r\nnull\r\n",
            b"$6\r\n[null]\r\n",
        ] {
            assert!(!json_changed_nothing("JSON.ARRPOP", reply), "{reply:?}");
        }
        assert!(!json_changed_nothing(
            "JSON.NUMINCRBY",
            b"$8\r\n[null,2]\r\n"
        ));
        assert!(!json_changed_nothing("JSON.NUMINCRBY", b"$1\r\n3\r\n"));
    }

    fn messages(flags: &str, event: &str, key: &str, subkeys: &[&str]) -> Vec<(String, String)> {
        let subkeys: Vec<&[u8]> = subkeys.iter().map(|s| s.as_bytes()).collect();
        subkey_messages(parse_ks_flags(flags), 0, event, key.as_bytes(), &subkeys)
            .into_iter()
            .map(|(ch, payload)| {
                (
                    String::from_utf8(ch).unwrap(),
                    String::from_utf8(payload).unwrap(),
                )
            })
            .collect()
    }

    /// The four channel shapes on the Redis subkey-notifications page.
    #[test]
    fn subkey_channels_and_payloads() {
        let all = messages("STIV", "hset", "myhash", &["field1", "f2"]);
        assert_eq!(
            all,
            [
                (
                    "__subkeyspace@0__:myhash".into(),
                    "hset|6:field1,2:f2".into()
                ),
                (
                    "__subkeyevent@0__:hset".into(),
                    "6:myhash|6:field1,2:f2".into()
                ),
                ("__subkeyspaceitem@0__:myhash\nfield1".into(), "hset".into()),
                ("__subkeyspaceitem@0__:myhash\nf2".into(), "hset".into()),
                (
                    "__subkeyspaceevent@0__:hset|myhash".into(),
                    "6:field1,2:f2".to_string()
                ),
            ]
        );
        // Each shape has its own flag.
        assert_eq!(messages("S", "hdel", "h", &["f"]).len(), 1);
        assert_eq!(
            messages("T", "hdel", "h", &["f"])[0].0,
            "__subkeyevent@0__:hdel"
        );
        // The key-level flags enable none of them.
        assert!(messages("KEA", "hset", "h", &["f"]).is_empty());
    }

    #[test]
    fn subkey_safeguards() {
        // Nothing without a subkey.
        assert!(messages("STIV", "hset", "h", &[]).is_empty());
        // A key with a newline has no item channel.
        let m = messages("STIV", "hset", "a\nb", &["f"]);
        assert!(m.iter().all(|(ch, _)| !ch.starts_with("__subkeyspaceitem")));
        assert_eq!(m.len(), 3);
        // A subkey may contain the separators: the length says where it ends.
        let m = messages("S", "hset", "h", &["a,b|c"]);
        assert_eq!(m[0].1, "hset|5:a,b|c");
    }

    /// A key and a field of any bytes are in the channels as they are.
    #[test]
    fn subkey_channels_of_any_bytes() {
        let key: &[u8] = b"\xff\x00k";
        let field: &[u8] = b"\xfe";
        let m = subkey_messages(parse_ks_flags("SIV"), 0, "hset", key, &[field]);
        assert_eq!(m[0].0, b"__subkeyspace@0__:\xff\x00k".to_vec());
        assert_eq!(m[1].0, b"__subkeyspaceitem@0__:\xff\x00k\n\xfe".to_vec());
        assert_eq!(m[2].0, b"__subkeyspaceevent@0__:hset|\xff\x00k".to_vec());
    }

    #[test]
    fn subkey_flags_round_trip() {
        // Written back in Redis's order: the classes of event first.
        let flags = parse_ks_flags("KEhSTIV");
        assert_eq!(flags_to_string(flags), "hKESTIV");
        assert_eq!(flags_to_string(parse_ks_flags("KA")), "AK");
        assert_eq!(flags_to_string(parse_ks_flags("EKeg$n")), "g$eKEn");
        // `A` is the key-level classes only.
        assert_eq!(parse_ks_flags("A") & KS_SUBKEY_ANY, 0);
    }
}
