// RDB files from servers whose format has parted from Redis's: Valkey 9
// (RDB 80, a `VALKEY080` header). What such a file holds loads whole, or the
// file is refused by name; nothing in it is skipped.

use kividb::snapshot::load_redis_rdb_from;
use kividb::types::KiviDBValue;

/// A loaded database keyed by text, for these tests, whose keys all are:
/// a load gives keys back as bytes (`store::Key`).
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

/// A file with this header, database 0 selected, then `body`, then the end.
fn file(header: &[u8], body: &[u8]) -> Vec<u8> {
    let mut out = header.to_vec();
    out.extend_from_slice(&[0xFE, 0]);
    out.extend_from_slice(body);
    out.push(0xFF);
    out.extend_from_slice(&[0; 8]);
    out
}

/// Valkey 9's `RDB_TYPE_HASH_2` (22): a count, then each field's name, its
/// value and its expiry (8 bytes, little-endian Unix milliseconds, -1 none).
fn valkey_hash(key: &[u8], fields: &[(&[u8], &[u8], i64)]) -> Vec<u8> {
    let mut out = vec![22];
    out.extend(rdb_str(key));
    out.extend(rdb_len(fields.len()));
    for (field, value, at) in fields {
        out.extend(rdb_str(field));
        out.extend(rdb_str(value));
        out.extend_from_slice(&at.to_le_bytes());
    }
    out
}

fn refusal(bytes: &[u8]) -> String {
    match load_redis_rdb_from(bytes) {
        Ok(_) => panic!("loaded"),
        Err(e) => e.to_string(),
    }
}

// A hash with field TTLs, as Valkey 9 saves one: the fields keep their TTLs
// to the millisecond, one without keeps none (#449), and one already past
// its time is kept as that moment, for field expiry to remove (and a master
// to send the `HDEL` of): dropped by each reader's clock, a replica's full
// sync or `RESTORE` went otherwise than its master's (#415).
#[test]
fn a_valkey_9_hash_with_field_ttls_loads_with_them() {
    let later = now_ms() + 3_600_000;
    let body = valkey_hash(
        b"h",
        &[
            (b"a", b"1", later as i64),
            (b"b", b"2", -1),
            (b"gone", b"3", (now_ms() - 1_000) as i64),
        ],
    );
    let (dbs, _, _) = load_redis_rdb_from(&file(b"VALKEY080", &body)[..]).unwrap();
    let dbs = texted_all(dbs);
    let KiviDBValue::HashEx(h) = &dbs[0]["h"] else {
        panic!("h is not a hash with field TTLs");
    };
    let h = h.read();
    let mut fields: Vec<&str> = h.fields.keys().map(String::as_str).collect();
    fields.sort_unstable();
    assert_eq!(fields, ["a", "b", "gone"]);
    assert_eq!(&h.fields["a"][..], b"1");
    assert_eq!(h.expiries.len(), 2, "b has no TTL");
    assert!(h.expiries["gone"] < now_ms(), "gone is due");
    let left = h.expiries["a"].saturating_sub(now_ms()) / 1_000;
    assert!((3_590..=3_600).contains(&left), "a's TTL is {left} s");
}

// Valkey writes a hash all of whose fields have TTLs as type 22 too. When
// every one has run out, the hash is kept with them all due: field expiry
// takes it out, as it takes out each field (see above).
#[test]
fn a_valkey_9_hash_whose_fields_have_all_run_out_is_kept_with_them_due() {
    let past = (now_ms() - 1_000) as i64;
    let mut body = valkey_hash(b"gone", &[(b"a", b"1", past), (b"b", b"2", past)]);
    body.extend([0]);
    body.extend(rdb_str(b"s"));
    body.extend(rdb_str(b"kept"));
    let (dbs, _, _) = load_redis_rdb_from(&file(b"VALKEY080", &body)[..]).unwrap();
    let dbs = texted_all(dbs);
    let mut keys: Vec<&str> = dbs[0].keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, ["gone", "s"]);
    let KiviDBValue::HashEx(h) = &dbs[0]["gone"] else {
        panic!("gone is not a hash with field TTLs");
    };
    assert!(h.read().has_due(now_ms()));
    assert!(h.read().live_fields(now_ms()).is_empty());
}

// What Valkey 9.0 and 9.1 do not write is refused, by what it is: a newer
// Valkey's RDB version (Valkey 9.2 writes 81), a VALKEY header with a
// version Valkey never puts under it, a Redis type in a Valkey file, and
// Valkey's type 22 in a Redis 7.4 file (RDB 12, where 22 is a pre-release
// encoding of Redis's), and a field named twice.
#[test]
fn what_valkey_9_does_not_write_is_refused() {
    let string = [&[0u8][..], &rdb_str(b"k"), &rdb_str(b"v")].concat();
    let newer = refusal(&file(b"VALKEY081", &string));
    assert!(newer.contains("Valkey RDB version 81 is newer"), "{newer}");
    let older = refusal(&file(b"VALKEY011", &string));
    assert!(older.contains("RDB version 11"), "{older}");
    let not_rdb = refusal(&file(b"VALKEYxyz", &string));
    assert!(not_rdb.contains("without an RDB version"), "{not_rdb}");

    // Redis 7.4's RDB_TYPE_HASH_LISTPACK_EX, which Valkey never writes.
    let mut redis_type = vec![25];
    redis_type.extend(rdb_str(b"k"));
    redis_type.extend(&[0u8; 8]);
    redis_type.extend(rdb_str(b""));
    let refused = refusal(&file(b"VALKEY080", &redis_type));
    assert!(
        refused.contains("RDB type 25 is not one Valkey"),
        "{refused}"
    );

    let hash = valkey_hash(b"h", &[(b"a", b"1", -1)]);
    let refused = refusal(&file(b"REDIS0012", &hash));
    assert!(refused.contains("unsupported RDB type 22"), "{refused}");

    let twice = valkey_hash(b"h", &[(b"a", b"1", -1), (b"a", b"2", -1)]);
    let refused = refusal(&file(b"VALKEY080", &twice));
    assert!(refused.contains("duplicate hash fields"), "{refused}");

    // The rest of the string types load under the VALKEY header as under
    // REDIS: it is the same format up to RDB 11's types.
    let (dbs, _, _) = load_redis_rdb_from(&file(b"VALKEY080", &string)[..]).unwrap();
    let dbs = texted_all(dbs);
    assert!(matches!(&dbs[0]["k"], KiviDBValue::String(v) if &v[..] == b"v"));
}

// A Valkey cluster node saves the slot migrations into it that are in
// progress (`RDB_OPCODE_SLOT_IMPORT`). The keys of those slots it holds are
// part of what was being moved, so the file is refused, naming the job and
// its slots, rather than loaded with half of them.
#[test]
fn a_valkey_file_saved_during_a_slot_migration_is_refused() {
    let job = b"0123456789012345678901234567890123456789";
    let mut body = vec![243];
    body.extend(rdb_str(job));
    body.extend(rdb_len(2));
    body.extend(rdb_len(0));
    body.extend(rdb_len(100));
    body.extend(rdb_len(200));
    body.extend(rdb_len(300));
    body.extend([0]);
    body.extend(rdb_str(b"k"));
    body.extend(rdb_str(b"v"));
    let refused = refusal(&file(b"VALKEY080", &body));
    assert!(refused.contains("slot migration"), "{refused}");
    assert!(
        refused.contains("0123456789012345678901234567890123456789"),
        "{refused}"
    );
    assert!(refused.contains("slots 0-100 200-300"), "{refused}");
}

// dump.rdb files written by SAVE on real Valkey 9.0.7 and 9.1.3 servers:
// every core type in the encodings Valkey writes for a small value and a
// large one, hashes with field TTLs (HPEXPIREAT and HSETEX), a stream with
// two consumer groups, pending entries and an idle consumer, keys with a TTL
// in milliseconds and in seconds, keys in databases 3 and 15, and a function
// library. The TTLs are in 2100 (4102444800000), so that they don't run out.
#[test]
fn rdb_files_saved_by_valkey_9_0_and_9_1_load_whole() {
    const FAR: u64 = 4_102_444_800_000;
    let files: [(&str, &[u8]); 2] = [
        ("9.0", include_bytes!("fixtures/valkey-9.0.rdb")),
        ("9.1", include_bytes!("fixtures/valkey-9.1.rdb")),
    ];
    for (label, bytes) in files {
        assert!(bytes.starts_with(b"VALKEY080"), "Valkey {label}");
        let (dbs, expiries, functions) =
            load_redis_rdb_from(bytes).unwrap_or_else(|e| panic!("Valkey {label}: {e}"));
        let dbs = texted_all(dbs);
        let expiries = texted_all(expiries);
        let db = &dbs[0];
        let mut keys: Vec<&str> = db.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "hash:big",
                "hash:small",
                "hashttl:big",
                "hashttl:setex",
                "hashttl:small",
                "list:big",
                "list:small",
                "set:big",
                "set:ints",
                "set:small",
                "str:bin",
                "str:int",
                "str:lzf",
                "str:neg",
                "str:raw",
                "stream:big",
                "stream:small",
                "ttl:far",
                "ttl:list",
                "ttl:sec",
                "zset:big",
                "zset:small",
            ],
            "Valkey {label}"
        );
        for key in ["ttl:far", "ttl:list", "ttl:sec"] {
            assert_eq!(expiries[0].get(key), Some(&FAR), "Valkey {label} {key}");
        }
        assert_eq!(expiries[0].len(), 3, "Valkey {label}");
        for db_index in [3, 15] {
            let mut keys: Vec<&String> = dbs[db_index].keys().collect();
            keys.sort_unstable();
            assert_eq!(
                keys,
                [&format!("db{db_index}:h"), &format!("db{db_index}:s")],
                "Valkey {label}"
            );
        }
        assert!(functions.is_some(), "Valkey {label}: the library was lost");

        // Field TTLs, each to the millisecond: kept as the file has them.
        let ttls = |key: &str| -> Vec<(String, u64)> {
            let KiviDBValue::HashEx(h) = &db[key] else {
                panic!("Valkey {label} {key}: not a hash with field TTLs");
            };
            let h = h.read();
            let mut ttls: Vec<(String, u64)> = h
                .expiries
                .iter()
                .map(|(field, at)| (field.clone(), *at))
                .collect();
            ttls.sort();
            ttls
        };
        for (key, fields, with_ttl) in [
            ("hashttl:small", 3, &["ttl1", "ttl2"][..]),
            ("hashttl:big", 300, &["f7", "f99"][..]),
            ("hashttl:setex", 2, &["a", "b"][..]),
        ] {
            let got = ttls(key);
            let names: Vec<&str> = got.iter().map(|(f, _)| f.as_str()).collect();
            assert_eq!(names, with_ttl, "Valkey {label} {key}");
            for (field, at) in &got {
                assert_eq!(
                    *at, FAR,
                    "Valkey {label} {key} {field}: expires at {at}, not {FAR}"
                );
            }
            let KiviDBValue::HashEx(h) = &db[key] else {
                unreachable!()
            };
            assert_eq!(h.read().fields.len(), fields, "Valkey {label} {key}");
        }

        let KiviDBValue::Stream(st) = &db["stream:small"] else {
            panic!("Valkey {label}: stream:small is not a stream");
        };
        let st = st.read();
        assert_eq!(st.entries.len(), 4, "Valkey {label}");
        let g1 = &st.groups[&b"g1"[..]];
        let mut consumers: Vec<&[u8]> = g1.consumers.keys().map(|c| c.as_bytes()).collect();
        consumers.sort_unstable();
        assert_eq!(
            consumers,
            [&b"alice"[..], b"bob", b"carol"],
            "Valkey {label}"
        );
        assert_eq!(g1.pel.len(), 2, "Valkey {label}: 1-1 (alice) and 4-1 (bob)");
        // Each consumer's own entries, as the loader handed them out.
        let own =
            |c: &[u8]| -> Vec<String> { g1.pel.ids_of(c, ..).map(|id| id.to_string()).collect() };
        assert_eq!(own(b"alice"), ["1-1"], "Valkey {label}");
        assert_eq!(own(b"bob"), ["4-1"], "Valkey {label}");
        assert_eq!(g1.pel.count_of(b"carol"), 0, "Valkey {label}");
        assert_eq!(g1.pel.count_of(b""), 0, "Valkey {label}: none left unowned");
        assert!(st.groups.contains_key(&b"g2"[..]), "Valkey {label}");
        let KiviDBValue::Stream(big) = &db["stream:big"] else {
            panic!("Valkey {label}: stream:big is not a stream");
        };
        assert_eq!(big.read().entries.len(), 300, "Valkey {label}");

        match &db["str:bin"] {
            KiviDBValue::String(v) => {
                assert_eq!(&v[..], (0..=255u8).collect::<Vec<_>>(), "Valkey {label}")
            }
            _ => panic!("Valkey {label}: str:bin is not a string"),
        }
        match &db["list:big"] {
            KiviDBValue::List(l) => assert_eq!(l.read().len(), 400, "Valkey {label}"),
            _ => panic!("Valkey {label}: list:big is not a list"),
        }
        match &db["zset:big"] {
            KiviDBValue::SortedSet(z) => {
                let z = z.read();
                assert_eq!(z.members.len(), 300, "Valkey {label}");
                assert_eq!(z.members.get(&b"z0123"[..]).copied(), Some(184.5));
            }
            _ => panic!("Valkey {label}: zset:big is not a sorted set"),
        }
    }
}

// The same, saved by valkey-bundle 9.0 with its modules loaded (valkey-json,
// valkey-bloom, valkey-search, valkey-ldap): a valkey-json document saves as
// RedisJSON's type, ReJSON-RL, and loads as JSON. The other modules save
// nothing of their own when they hold nothing.
#[test]
fn an_rdb_file_with_a_valkey_json_document_loads() {
    let (dbs, _, _) =
        load_redis_rdb_from(&include_bytes!("fixtures/valkey-9.0-json.rdb")[..]).unwrap();
    let dbs = texted_all(dbs);
    let KiviDBValue::Json(doc) = &dbs[0]["json:doc"] else {
        panic!("json:doc is not JSON");
    };
    assert_eq!(
        *doc.read(),
        serde_json::json!({
            "name": "kivi", "n": 42, "pi": 3.25, "ok": true, "nothing": null,
            "tags": ["a", "b"], "nested": {"x": [1, {"y": "z"}]}
        })
    );
    assert_eq!(
        dbs[0].len(),
        23,
        "the 22 keys of the other files and json:doc"
    );
}
