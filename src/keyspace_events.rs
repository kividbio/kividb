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
                    | KS_ARRAY;
            }
            _ => {}
        }
    }
    flags
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
    | KS_ARRAY;

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
    let id = store_id(store);
    HUBS.read()
        .iter()
        .find(|(known, _)| *known == id)
        .map(|(_, hub)| hub.clone())
}

/// A key's TTL ran out and the key was removed: publish `expired`.
///
/// Nothing was published for this before — not when a read found the key
/// expired, and not when the background task removed it.
#[inline]
pub fn notify_expired(store: &crate::store::Store, db: usize, key: &str) {
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
    key: &str,
) {
    let flags = KS_FLAGS.load(Ordering::Relaxed);
    if flags == 0 {
        return;
    }
    if flags & type_flag == 0 {
        return;
    }
    if flags & KS_KEYSPACE != 0 {
        let ch = format!("__keyspace@{}__:{}", db, key);
        pubsub.publish(&ch, Arc::from(event.as_bytes()));
    }
    if flags & KS_KEYEVENT != 0 {
        let ch = format!("__keyevent@{}__:{}", db, event);
        pubsub.publish(&ch, Arc::from(key.as_bytes()));
    }
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
    key: &str,
    subkeys: &[&[u8]],
) -> Vec<(String, Vec<u8>)> {
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
        out.push((format!("__subkeyspace@{db}__:{key}"), payload));
    }
    if flags & KS_SUBKEYEVENT != 0 {
        // `__subkeyevent@<db>__:<event>` → `<key_len>:<key>|<subkeys>`
        let mut payload = format!("{}:{key}|", key.len()).into_bytes();
        payload.extend_from_slice(&list);
        out.push((format!("__subkeyevent@{db}__:{event}"), payload));
    }
    if flags & KS_SUBKEYSPACEITEM != 0 && !key.contains('\n') {
        // `__subkeyspaceitem@<db>__:<key>\n<subkey>` → `<event>`, one per subkey.
        // Newline separates key from subkey, so a key containing one is skipped.
        for subkey in subkeys {
            out.push((
                format!(
                    "__subkeyspaceitem@{db}__:{key}\n{}",
                    String::from_utf8_lossy(subkey)
                ),
                event.as_bytes().to_vec(),
            ));
        }
    }
    if flags & KS_SUBKEYSPACEEVENT != 0 && unambiguous {
        // `__subkeyspaceevent@<db>__:<event>|<key>` → `<subkeys>`
        out.push((format!("__subkeyspaceevent@{db}__:{event}|{key}"), list));
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
    key: &str,
    subkeys: impl IntoIterator<Item = &'a [u8]>,
) {
    let flags = KS_FLAGS.load(Ordering::Relaxed);
    if flags & KS_SUBKEY_ANY == 0 {
        return;
    }
    let subkeys: Vec<&[u8]> = subkeys.into_iter().collect();
    for (channel, payload) in subkey_messages(flags, db, event, key, &subkeys) {
        pubsub.publish(&channel, Arc::from(payload));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn messages(flags: &str, event: &str, key: &str, subkeys: &[&str]) -> Vec<(String, String)> {
        let subkeys: Vec<&[u8]> = subkeys.iter().map(|s| s.as_bytes()).collect();
        subkey_messages(parse_ks_flags(flags), 0, event, key, &subkeys)
            .into_iter()
            .map(|(ch, payload)| (ch, String::from_utf8(payload).unwrap()))
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
