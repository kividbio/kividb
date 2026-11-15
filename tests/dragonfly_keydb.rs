// RDB files and DUMP payloads from Dragonfly and KeyDB (#452). Both write
// RDB 9 with more in it than Redis 6.2 put there: Dragonfly, types of its own
// (hashes and sets whose members have TTLs, JSON, its probabilistic types)
// and key flags; KeyDB, the TTLs `EXPIREMEMBER` gives members, as aux fields
// after their key, and a version stamp at the end of its `DUMP` payloads.
// A hash field's TTL loads as KiviDB's field TTL. A set or sorted-set
// member's has nothing to load into: the whole file is refused, naming the
// key. Nothing is skipped.

use kividb::snapshot::{load_redis_rdb_from, restore_payload_checked};
use kividb::types::KiviDBValue;

fn texted<V>(
    map: std::collections::HashMap<kividb::store::Key, V>,
) -> std::collections::HashMap<String, V> {
    map.into_iter()
        .map(|(key, value)| (String::from_utf8(key.to_vec()).unwrap(), value))
        .collect()
}

fn texted_all<V>(
    maps: Vec<std::collections::HashMap<kividb::store::Key, V>>,
) -> Vec<std::collections::HashMap<String, V>> {
    maps.into_iter().map(texted).collect()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn rdb_len(len: usize) -> Vec<u8> {
    match len {
        0..=63 => vec![len as u8],
        64..=16383 => vec![0x40 | (len >> 8) as u8, len as u8],
        _ => {
            let mut v = vec![0x80];
            v.extend_from_slice(&(len as u32).to_be_bytes());
            v
        }
    }
}

fn rdb_str(s: &[u8]) -> Vec<u8> {
    let mut v = rdb_len(s.len());
    v.extend_from_slice(s);
    v
}

fn aux(key: &str, value: &str) -> Vec<u8> {
    let mut v = vec![0xFA];
    v.extend(rdb_str(key.as_bytes()));
    v.extend(rdb_str(value.as_bytes()));
    v
}

/// An RDB 9 file of database 0: `body` between the header and the end.
fn file(body: &[u8]) -> Vec<u8> {
    let mut out = b"REDIS0009".to_vec();
    out.extend(aux("redis-ver", "6.2.11"));
    out.extend_from_slice(&[0xFE, 0]);
    out.extend_from_slice(body);
    out.push(0xFF);
    out.extend_from_slice(&[0; 8]);
    out
}

fn string_key(key: &str, value: &str) -> Vec<u8> {
    [
        &[0u8][..],
        &rdb_str(key.as_bytes()),
        &rdb_str(value.as_bytes()),
    ]
    .concat()
}

fn hash_key(key: &str, fields: &[(&str, &str)]) -> Vec<u8> {
    let mut out = vec![4];
    out.extend(rdb_str(key.as_bytes()));
    out.extend(rdb_len(fields.len()));
    for (f, v) in fields {
        out.extend(rdb_str(f.as_bytes()));
        out.extend(rdb_str(v.as_bytes()));
    }
    out
}

fn set_key(key: &str, members: &[&str]) -> Vec<u8> {
    let mut out = vec![2];
    out.extend(rdb_str(key.as_bytes()));
    out.extend(rdb_len(members.len()));
    for m in members {
        out.extend(rdb_str(m.as_bytes()));
    }
    out
}

/// KeyDB's aux fields for a member's TTL, after its key.
fn keydb_member_ttl(member: &str, at_ms: u64) -> Vec<u8> {
    [
        aux("keydb-subexpire-key", member),
        aux("keydb-subexpire-when", &at_ms.to_string()),
    ]
    .concat()
}

fn refusal(bytes: &[u8]) -> String {
    match load_redis_rdb_from(bytes) {
        Ok(_) => panic!("loaded"),
        Err(e) => e.to_string(),
    }
}

fn field_ttl_ms(value: &KiviDBValue, field: &str) -> Option<u64> {
    let KiviDBValue::HashEx(h) = value else {
        return None;
    };
    let h = h.read();
    h.expiries.get(field).copied()
}

// KeyDB writes the TTL `EXPIREMEMBER` gives a hash field after the hash: it
// loads as the field's TTL, to the millisecond. One already past its time
// is kept as that moment, the field with it, for field expiry to remove (and
// a master to send the `HDEL` of, #415): dropped by each reader's clock, a
// replica's full sync went otherwise than its master. A TTL after a
// key that was dropped as past its own time goes with that key, and does
// not land on the key before it. KeyDB's version stamps (`mvcc-tstamp`,
// before each key under active replication) are read past.
#[test]
fn keydb_hash_field_ttls_load_as_field_ttls() {
    let later = now_ms() + 3_600_000;
    let past = now_ms() - 1_000;
    let mut body = Vec::new();
    body.extend(aux("mvcc-tstamp", "1881066172270510080"));
    body.extend(hash_key("h", &[("a", "1"), ("b", "2"), ("c", "3")]));
    body.extend(keydb_member_ttl("a", later));
    body.extend(keydb_member_ttl("c", past));
    // A TTL for a field the hash does not have changes nothing.
    body.extend(keydb_member_ttl("zz", later));
    body.extend(hash_key("gone", &[("only", "1")]));
    body.extend(keydb_member_ttl("only", past));
    // Past its own time: dropped, and its member TTL with it.
    body.push(0xFC);
    body.extend_from_slice(&past.to_le_bytes());
    body.extend(set_key("expired", &["m"]));
    body.extend(keydb_member_ttl("m", later));
    body.extend(string_key("s", "v"));

    let (dbs, _, _) = load_redis_rdb_from(&file(&body)[..]).unwrap();
    let dbs = texted_all(dbs);
    let db = &dbs[0];
    let mut keys: Vec<&str> = db.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, ["gone", "h", "s"]);
    let due = |key: &str, field: &str| match &db[key] {
        KiviDBValue::HashEx(h) => h
            .read()
            .expiries
            .get(field)
            .is_some_and(|at| *at < now_ms()),
        _ => false,
    };
    let KiviDBValue::HashEx(h) = &db["h"] else {
        panic!("h has no field TTLs");
    };
    {
        let h = h.read();
        let mut fields: Vec<&str> = h.fields.keys().map(String::as_str).collect();
        fields.sort_unstable();
        assert_eq!(fields, ["a", "b", "c"]);
        assert_eq!(h.expiries.len(), 2);
    }
    assert!(due("h", "c"), "c ran out: kept, and due");
    assert!(due("gone", "only"), "only ran out: kept, and due");
    let at = field_ttl_ms(&db["h"], "a").unwrap();
    assert!(at.abs_diff(later) <= 2, "a expires at {at}, not {later}");
}

// A set or sorted-set member KeyDB gave a TTL has nowhere to keep it: the
// file is refused, naming the key, rather than loaded with the member kept
// for ever.
#[test]
fn keydb_set_and_sorted_set_member_ttls_are_refused() {
    let later = now_ms() + 3_600_000;
    let mut body = set_key("s", &["m1", "m2"]);
    body.extend(keydb_member_ttl("m1", later));
    let refused = refusal(&file(&body));
    assert!(
        refused.contains("the set s has a member with its own TTL"),
        "{refused}"
    );

    // RDB_TYPE_ZSET_2: member, binary score.
    let mut body = vec![5];
    body.extend(rdb_str(b"z"));
    body.extend(rdb_len(1));
    body.extend(rdb_str(b"a"));
    body.extend_from_slice(&1.0f64.to_le_bytes());
    body.extend(keydb_member_ttl("a", later));
    let refused = refusal(&file(&body));
    assert!(
        refused.contains("the sorted set z has a member with its own TTL"),
        "{refused}"
    );

    // A KeyDB cron job is refused by name too.
    let mut body = vec![64];
    body.extend(rdb_str(b"job"));
    let refused = refusal(&file(&body));
    assert!(refused.contains("KeyDB cron job"), "{refused}");
}

/// Dragonfly's hash with field TTLs (type 22, as Valkey 9 writes it).
fn dragonfly_hash(key: &str, fields: &[(&str, &str, i64)]) -> Vec<u8> {
    let mut out = vec![22];
    out.extend(rdb_str(key.as_bytes()));
    out.extend(rdb_len(fields.len()));
    for (f, v, at) in fields {
        out.extend(rdb_str(f.as_bytes()));
        out.extend(rdb_str(v.as_bytes()));
        out.extend_from_slice(&at.to_le_bytes());
    }
    out
}

/// Dragonfly's older encodings of a member TTL: seconds from 2023-02-01, as
/// text.
fn dragonfly_member_seconds(at_ms: u64) -> String {
    (at_ms / 1000 - 1_675_209_600).to_string()
}

// Dragonfly's hashes with field TTLs (HSETEX, FIELDEXPIRE) in both its
// encodings, its JSON documents, and a sticky key (STICK) load. A set whose
// members all had no TTL left loads as a set.
#[test]
fn dragonfly_field_ttls_json_and_flags_load() {
    let later = (now_ms() + 3_600_000) / 1000 * 1000;
    let mut body = Vec::new();
    body.extend(dragonfly_hash(
        "h",
        &[("a", "1", later as i64), ("b", "2", -1)],
    ));
    // RDB_TYPE_HASH_WITH_EXPIRY_DEPRECATED (31).
    body.push(31);
    body.extend(rdb_str(b"old"));
    body.extend(rdb_len(2));
    for (f, v, ttl) in [
        ("x", "1", dragonfly_member_seconds(later)),
        ("y", "2", "-1".into()),
    ] {
        body.extend(rdb_str(f.as_bytes()));
        body.extend(rdb_str(v.as_bytes()));
        body.extend(rdb_str(ttl.as_bytes()));
    }
    // RDB_TYPE_JSON (30).
    body.push(30);
    body.extend(rdb_str(b"doc"));
    body.extend(rdb_str(br#"{"a":[1,2,{"b":null}],"c":"d"}"#));
    // RDB_OPCODE_DF_MASK, sticky, before a key.
    body.extend_from_slice(&[0xDC, 1, 0, 0, 0]);
    body.extend(string_key("sticky", "v"));
    // RDB_TYPE_SET_WITH_EXPIRY (32), no member with a TTL.
    body.push(32);
    body.extend(rdb_str(b"s"));
    body.extend(rdb_len(2));
    for m in ["m1", "m2"] {
        body.extend(rdb_str(m.as_bytes()));
        body.extend(rdb_str(b"-1"));
    }

    let (dbs, _, _) = load_redis_rdb_from(&file(&body)[..]).unwrap();
    let dbs = texted_all(dbs);
    let db = &dbs[0];
    let mut keys: Vec<&str> = db.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, ["doc", "h", "old", "s", "sticky"]);
    assert!(field_ttl_ms(&db["h"], "a").unwrap().abs_diff(later) <= 2);
    assert_eq!(field_ttl_ms(&db["h"], "b"), None);
    assert!(field_ttl_ms(&db["old"], "x").unwrap().abs_diff(later) <= 2);
    assert_eq!(field_ttl_ms(&db["old"], "y"), None);
    let KiviDBValue::Json(doc) = &db["doc"] else {
        panic!("doc is not JSON");
    };
    assert_eq!(
        *doc.read(),
        serde_json::json!({"a": [1, 2, {"b": null}], "c": "d"})
    );
    assert!(matches!(&db["s"], KiviDBValue::Set(s) if s.read().len() == 2));
}

// What Dragonfly has and KiviDB does not is refused by what it is: a set
// whose members have TTLs (SADDEX), its probabilistic types, a key with
// memcached flags, and its compressed blocks (with the flag that turns
// them off).
#[test]
fn what_dragonfly_has_and_kividb_does_not_is_refused() {
    let later = now_ms() + 3_600_000;
    let mut body = vec![32];
    body.extend(rdb_str(b"s:ttl"));
    body.extend(rdb_len(2));
    body.extend(rdb_str(b"m1"));
    body.extend(rdb_str(dragonfly_member_seconds(later).as_bytes()));
    body.extend(rdb_str(b"m2"));
    body.extend(rdb_str(b"-1"));
    let refused = refusal(&file(&body));
    assert!(
        refused.contains("the set s:ttl has members with their own TTLs"),
        "{refused}"
    );

    for (type_id, what) in [
        (33u8, "a Bloom filter"),
        (35, "a count-min sketch"),
        (36, "a top-k"),
        (37, "a Cuckoo filter"),
    ] {
        let mut body = vec![type_id];
        body.extend(rdb_str(b"p"));
        body.extend(rdb_len(0));
        let refused = refusal(&file(&body));
        assert!(
            refused.contains(&format!("the key p is {what}")),
            "{refused}"
        );
    }

    let mut body = vec![0xDC, 2, 0, 0, 0, 7, 0, 0, 0];
    body.extend(string_key("mc", "v"));
    let refused = refusal(&file(&body));
    assert!(refused.contains("memcached flags"), "{refused}");

    let mut body = vec![0xCA];
    body.extend(rdb_str(b"\x04\x22\x4d\x18"));
    let refused = refusal(&file(&body));
    assert!(refused.contains("--compression_mode=0"), "{refused}");
}

/// A `DUMP` payload: the value, then the RDB version and Redis's CRC64.
fn sealed(mut body: Vec<u8>, version: u16) -> Vec<u8> {
    body.extend_from_slice(&version.to_le_bytes());
    let crc = kividb::snapshot::redis_crc64(&body);
    body.extend_from_slice(&crc.to_le_bytes());
    body
}

// KeyDB ends a `DUMP` payload with its version stamp (`mvcc-tstamp`), which
// `RESTORE` refused as `Bad data format`; it is read past. Dragonfly's
// payloads of its own types restore. Anything else after the value is still
// not a value.
#[test]
fn keydb_and_dragonfly_dump_payloads_restore() {
    let mut keydb = vec![0];
    keydb.extend(rdb_str(b"v"));
    keydb.extend(aux("mvcc-tstamp", "18446744073709551615"));
    let restored = restore_payload_checked(&sealed(keydb, 9)).unwrap();
    assert!(matches!(&restored, KiviDBValue::String(v) if &v[..] == b"v"));

    let mut other = vec![0];
    other.extend(rdb_str(b"v"));
    other.extend(aux("something-else", "1"));
    assert!(restore_payload_checked(&sealed(other, 9)).is_err());

    let later = (now_ms() + 3_600_000) as i64;
    let mut dragonfly = dragonfly_hash("unused", &[("a", "1", later)]);
    // A payload has no key: drop the key's bytes after the type.
    dragonfly.drain(1..1 + 1 + "unused".len());
    let restored = restore_payload_checked(&sealed(dragonfly, 9)).unwrap();
    assert!(field_ttl_ms(&restored, "a").is_some());

    let mut json = vec![30];
    json.extend(rdb_str(br#"{"x":1}"#));
    let restored = restore_payload_checked(&sealed(json, 9)).unwrap();
    assert!(matches!(&restored, KiviDBValue::Json(_)));
}

// Files saved by real servers: dragonflydb/dragonfly v2.1 (`SAVE RDB`) and
// eqalpha/keydb 6.3.4 (`SAVE`). Dragonfly's: a hash from HSETEX with a
// plain field added, one with a FIELDEXPIRE, JSON, a sticky key, a list, a
// sorted set and a stream; then the same with a SADDEX set. Dragonfly takes
// member TTLs of at most 100,000,000 s, so the captures' member TTLs were
// rewritten to 2100-01-01 (4102444800000), in place, in the encodings
// Dragonfly wrote them in; its files carry no checksum. KeyDB's: a hash with
// two fields given TTLs to 2100 (PEXPIREMEMBERAT, EXPIREMEMBERAT); then the
// same with a set member given one.
#[test]
fn rdb_files_saved_by_dragonfly_and_keydb_load_or_are_refused_whole() {
    const FAR: u64 = 4_102_444_800_000;
    let (dbs, _, _) =
        load_redis_rdb_from(&include_bytes!("fixtures/dragonfly-2.1-types.rdb")[..]).unwrap();
    let dbs = texted_all(dbs);
    let db = &dbs[0];
    let mut keys: Vec<&str> = db.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, ["h:fe", "h:ttl", "j", "l", "s", "st", "sticky", "z"]);
    for (key, field) in [("h:ttl", "a"), ("h:ttl", "b"), ("h:fe", "x")] {
        let at = field_ttl_ms(&db[key], field).unwrap_or_else(|| panic!("{key} {field}"));
        assert!(at.abs_diff(FAR) <= 2, "{key} {field} expires at {at}");
    }
    assert_eq!(field_ttl_ms(&db["h:ttl"], "plain"), None);
    assert_eq!(field_ttl_ms(&db["h:fe"], "y"), None);
    let KiviDBValue::Json(doc) = &db["j"] else {
        panic!("j is not JSON");
    };
    assert_eq!(
        *doc.read(),
        serde_json::json!({"a": [1, 2, {"b": null}], "c": "d"})
    );
    let refused = refusal(include_bytes!("fixtures/dragonfly-2.1-types-saddex.rdb"));
    assert!(
        refused.contains("the set s:ttl has members with their own TTLs"),
        "{refused}"
    );

    let (dbs, _, _) = load_redis_rdb_from(&include_bytes!("fixtures/keydb-6.3.4.rdb")[..]).unwrap();
    let dbs = texted_all(dbs);
    let db = &dbs[0];
    let mut keys: Vec<&str> = db.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, ["h", "s", "z"]);
    for field in ["a", "b"] {
        let at = field_ttl_ms(&db["h"], field).unwrap();
        assert!(at.abs_diff(FAR) <= 2, "h {field} expires at {at}");
    }
    assert_eq!(field_ttl_ms(&db["h"], "c"), None);
    let refused = refusal(include_bytes!("fixtures/keydb-6.3.4-set-member-ttl.rdb"));
    assert!(
        refused.contains("the set set:ttl has a member with its own TTL"),
        "{refused}"
    );
}

// A refusal of what the data holds is told apart from a file that does not
// read: a replica refused so waits a minute or more before it full-syncs the
// same master again, and one whose transfer broke tries again at once.
#[test]
fn a_refusal_of_what_the_data_holds_is_told_apart_from_a_bad_file() {
    let content = |bytes: &[u8]| {
        let e = load_redis_rdb_from(bytes).err().expect("refused");
        kividb::snapshot::is_unrepresentable(&e)
    };
    assert!(content(include_bytes!(
        "fixtures/dragonfly-2.1-types-saddex.rdb"
    )));
    assert!(content(include_bytes!(
        "fixtures/keydb-6.3.4-set-member-ttl.rdb"
    )));
    let mut bloom = vec![33];
    bloom.extend(rdb_str(b"p"));
    assert!(content(&file(&bloom)));

    let whole = include_bytes!("fixtures/keydb-6.3.4.rdb");
    assert!(!content(&whole[..whole.len() / 2]), "cut short");
    let mut unknown = vec![0xF1];
    unknown.extend(rdb_str(b"p"));
    assert!(!content(&file(&unknown)), "an opcode that does not read");
}

// A Dragonfly member TTL is the payload's word: one past what fits in
// milliseconds is refused, not wrapped round (RESTORE reaches it).
#[test]
fn a_dragonfly_member_ttl_out_of_range_is_refused() {
    let mut old_hash = vec![31];
    old_hash.extend(rdb_len(1));
    old_hash.extend(rdb_str(b"f"));
    old_hash.extend(rdb_str(b"v"));
    old_hash.extend(rdb_str(i64::MAX.to_string().as_bytes()));
    assert!(restore_payload_checked(&sealed(old_hash.clone(), 9)).is_err());

    let mut in_file = vec![31];
    in_file.extend(rdb_str(b"h"));
    in_file.extend_from_slice(&old_hash[1..]);
    let refused = refusal(&file(&in_file));
    assert!(refused.contains("out of range"), "{refused}");
}
