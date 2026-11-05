// Extensive unit tests for KDB and Redis RDB snapshot formats.
//
// Strategy: call snapshot functions directly in-process — no live server needed.
// Each KDB test:
//   1. Build a HashMap<String, KiviDBValue> with known data
//   2. write_snapshot to a tempfile
//   3. load_snapshot from that tempfile
//   4. Assert the loaded data matches the original exactly
//
// Each RDB test:
//   1. Hand-construct the minimal valid RDB binary for a specific type
//   2. Call load_redis_rdb on a tempfile containing those bytes
//   3. Assert the loaded data matches expectations
//
// Tempfiles use a unique path per test (based on thread id + timestamp)
// so parallel test runs never collide.

use ahash::AHashMap;
use kividb::snapshot::{load_redis_rdb, load_snapshot, write_snapshot};
use kividb::types::{
    KiviDBValue, SortedSetInner, StreamEntry, StreamId, StreamInner, TsInner, VSetEntry,
    VectorSetInner,
};
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Unique temp path per test — avoids collisions during parallel runs.
fn tmp_kdb(tag: &str) -> String {
    let id = std::thread::current().id();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/tmp/kivi_test_{tag}_{id:?}_{ts}.kdb")
}

fn tmp_rdb(tag: &str) -> String {
    let id = std::thread::current().id();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/tmp/kivi_test_{tag}_{id:?}_{ts}.rdb")
}

/// Write `maps` to a KDB file, load it back, return the loaded db 0.
fn roundtrip_kdb(maps: Vec<HashMap<String, KiviDBValue>>) -> HashMap<String, KiviDBValue> {
    let path = tmp_kdb("rt");
    write_snapshot(&path, &maps, None).expect("write_snapshot failed");
    let mut loaded = load_snapshot(&path).expect("load_snapshot failed");
    std::fs::remove_file(&path).ok();
    loaded.dbs.remove(0)
}

/// Build a minimal one-key KDB map, roundtrip it, return the value.
fn roundtrip_one(key: &str, value: KiviDBValue) -> KiviDBValue {
    let mut map = HashMap::new();
    map.insert(key.to_string(), value);
    let mut db0 = roundtrip_kdb(vec![map]);
    db0.remove(key).expect("key missing after roundtrip")
}

// ═════════════════════════════════════════════════════════════════════════════
// KDB — String
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn kdb_string_basic() {
    let v = roundtrip_one(
        "hello",
        KiviDBValue::String(kividb::types::SharedBytes::from(b"world".as_slice())),
    );
    match v {
        KiviDBValue::String(b) => assert_eq!(b.as_ref(), b"world"),
        _ => panic!("wrong type"),
    }
}

#[test]
fn kdb_string_empty() {
    let v = roundtrip_one(
        "e",
        KiviDBValue::String(kividb::types::SharedBytes::from(b"".as_slice())),
    );
    match v {
        KiviDBValue::String(b) => assert!(b.is_empty()),
        _ => panic!("wrong type"),
    }
}

#[test]
fn kdb_string_binary_safe() {
    // Contains null bytes and high bytes — must survive verbatim
    let raw: Vec<u8> = vec![0x00, 0xFF, 0x01, 0x80, 0x7F];
    let v = roundtrip_one(
        "bin",
        KiviDBValue::String(kividb::types::SharedBytes::from(raw.as_slice())),
    );
    match v {
        KiviDBValue::String(b) => assert_eq!(b.as_ref(), &[0x00, 0xFF, 0x01, 0x80, 0x7F]),
        _ => panic!("wrong type"),
    }
}

#[test]
fn kdb_string_large_value() {
    let big = vec![b'x'; 1_000_000];
    let v = roundtrip_one(
        "big",
        KiviDBValue::String(kividb::types::SharedBytes::from(big.as_slice())),
    );
    match v {
        KiviDBValue::String(b) => {
            assert_eq!(b.len(), 1_000_000);
            assert!(b.iter().all(|&x| x == b'x'));
        }
        _ => panic!("wrong type"),
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// KDB — List
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn kdb_list_basic() {
    let deque: VecDeque<Arc<[u8]>> = vec![
        Arc::from(b"alpha".as_slice()),
        Arc::from(b"beta".as_slice()),
        Arc::from(b"gamma".as_slice()),
    ]
    .into();
    let v = roundtrip_one("lst", KiviDBValue::List(Arc::new(RwLock::new(deque))));
    match v {
        KiviDBValue::List(l) => {
            let inner = l.read();
            assert_eq!(inner.len(), 3);
            assert_eq!(inner[0].as_ref(), b"alpha");
            assert_eq!(inner[1].as_ref(), b"beta");
            assert_eq!(inner[2].as_ref(), b"gamma");
        }
        _ => panic!("wrong type"),
    }
}

#[test]
fn kdb_list_order_preserved() {
    // Order is semantically significant in lists — verify it survives
    let items: Vec<Arc<[u8]>> = (0u32..100)
        .map(|i| Arc::from(i.to_string().as_bytes()))
        .collect();
    let deque: VecDeque<Arc<[u8]>> = items.clone().into();
    let v = roundtrip_one("ordered", KiviDBValue::List(Arc::new(RwLock::new(deque))));
    match v {
        KiviDBValue::List(l) => {
            let inner = l.read();
            assert_eq!(inner.len(), 100);
            for (i, item) in inner.iter().enumerate() {
                assert_eq!(item.as_ref(), i.to_string().as_bytes());
            }
        }
        _ => panic!("wrong type"),
    }
}

#[test]
fn kdb_list_empty() {
    let v = roundtrip_one(
        "empty_list",
        KiviDBValue::List(Arc::new(RwLock::new(VecDeque::new()))),
    );
    match v {
        KiviDBValue::List(l) => assert!(l.read().is_empty()),
        _ => panic!("wrong type"),
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// KDB — Hash
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn kdb_hash_basic() {
    let mut hmap = kividb::types::HashFields::default();
    hmap.insert("name".to_string(), Arc::from(b"alice".as_slice()));
    hmap.insert("age".to_string(), Arc::from(b"30".as_slice()));
    hmap.insert("city".to_string(), Arc::from(b"london".as_slice()));

    let v = roundtrip_one("h", KiviDBValue::Hash(Arc::new(RwLock::new(hmap))));
    match v {
        KiviDBValue::Hash(h) => {
            let inner = h.read();
            assert_eq!(inner.len(), 3);
            assert_eq!(inner["name"].as_ref(), b"alice");
            assert_eq!(inner["age"].as_ref(), b"30");
            assert_eq!(inner["city"].as_ref(), b"london");
        }
        _ => panic!("wrong type"),
    }
}

#[test]
fn kdb_hash_many_fields() {
    let mut hmap = kividb::types::HashFields::default();
    for i in 0..500 {
        hmap.insert(
            format!("field:{i}"),
            Arc::from(format!("val:{i}").as_bytes()),
        );
    }
    let v = roundtrip_one("bigmap", KiviDBValue::Hash(Arc::new(RwLock::new(hmap))));
    match v {
        KiviDBValue::Hash(h) => {
            let inner = h.read();
            assert_eq!(inner.len(), 500);
            for i in 0..500 {
                let key = format!("field:{i}");
                let expected = format!("val:{i}");
                assert_eq!(inner[&key].as_ref(), expected.as_bytes());
            }
        }
        _ => panic!("wrong type"),
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// KDB — Set
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn kdb_set_basic() {
    let mut hset = kividb::member_set::MemberSet::new();
    hset.insert(Arc::from(b"apple".as_slice()));
    hset.insert(Arc::from(b"banana".as_slice()));
    hset.insert(Arc::from(b"cherry".as_slice()));

    let v = roundtrip_one("s", KiviDBValue::Set(Arc::new(RwLock::new(hset))));
    match v {
        KiviDBValue::Set(s) => {
            let inner = s.read();
            assert_eq!(inner.len(), 3);
            assert!(inner.contains(b"apple".as_slice()));
            assert!(inner.contains(b"banana".as_slice()));
            assert!(inner.contains(b"cherry".as_slice()));
        }
        _ => panic!("wrong type"),
    }
}

#[test]
fn kdb_set_no_duplicates() {
    // Insert same value twice — set must deduplicate
    let mut hset = kividb::member_set::MemberSet::new();
    hset.insert(Arc::from(b"x".as_slice()));
    hset.insert(Arc::from(b"x".as_slice())); // duplicate

    let v = roundtrip_one("dedup", KiviDBValue::Set(Arc::new(RwLock::new(hset))));
    match v {
        KiviDBValue::Set(s) => assert_eq!(s.read().len(), 1),
        _ => panic!("wrong type"),
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// KDB — SortedSet
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn kdb_sorted_set_basic() {
    let mut ss = SortedSetInner::new();
    ss.insert(Arc::from(b"a".as_slice()), 1.0);
    ss.insert(Arc::from(b"b".as_slice()), 2.5);
    ss.insert(Arc::from(b"c".as_slice()), -1.0);

    let v = roundtrip_one("zs", KiviDBValue::SortedSet(Arc::new(RwLock::new(ss))));
    match v {
        KiviDBValue::SortedSet(s) => {
            let inner = s.read();
            assert_eq!(inner.len(), 3);
            assert_eq!(*inner.members.get(b"a".as_slice()).unwrap(), 1.0);
            assert_eq!(*inner.members.get(b"b".as_slice()).unwrap(), 2.5);
            assert_eq!(*inner.members.get(b"c".as_slice()).unwrap(), -1.0);
        }
        _ => panic!("wrong type"),
    }
}

#[test]
fn kdb_sorted_set_score_precision() {
    // f64 must survive the to_le_bytes round-trip without precision loss
    let mut ss = SortedSetInner::new();
    let score = std::f64::consts::PI; // 3.141592653589793
    ss.insert(Arc::from(b"pi".as_slice()), score);

    let v = roundtrip_one("pi_set", KiviDBValue::SortedSet(Arc::new(RwLock::new(ss))));
    match v {
        KiviDBValue::SortedSet(s) => {
            let inner = s.read();
            let loaded_score = *inner.members.get(b"pi".as_slice()).unwrap();
            assert_eq!(loaded_score.to_bits(), score.to_bits()); // exact bit equality
        }
        _ => panic!("wrong type"),
    }
}

#[test]
fn kdb_sorted_set_extreme_scores() {
    let mut ss = SortedSetInner::new();
    ss.insert(Arc::from(b"min".as_slice()), f64::MIN);
    ss.insert(Arc::from(b"max".as_slice()), f64::MAX);
    ss.insert(Arc::from(b"inf".as_slice()), f64::INFINITY);
    ss.insert(Arc::from(b"neg_inf".as_slice()), f64::NEG_INFINITY);

    let v = roundtrip_one(
        "extremes",
        KiviDBValue::SortedSet(Arc::new(RwLock::new(ss))),
    );
    match v {
        KiviDBValue::SortedSet(s) => {
            let inner = s.read();
            assert_eq!(
                inner.members.get(b"min".as_slice()).unwrap().to_bits(),
                f64::MIN.to_bits()
            );
            assert_eq!(
                inner.members.get(b"max".as_slice()).unwrap().to_bits(),
                f64::MAX.to_bits()
            );
            assert!(inner.members.get(b"inf".as_slice()).unwrap().is_infinite());
            assert!(
                inner
                    .members
                    .get(b"neg_inf".as_slice())
                    .unwrap()
                    .is_infinite()
            );
        }
        _ => panic!("wrong type"),
    }
}

#[test]
fn kdb_sorted_set_score_order_preserved() {
    let mut ss = SortedSetInner::new();
    ss.insert(Arc::from(b"c".as_slice()), 3.0);
    ss.insert(Arc::from(b"a".as_slice()), 1.0);
    ss.insert(Arc::from(b"b".as_slice()), 2.0);

    let v = roundtrip_one(
        "ordered_zs",
        KiviDBValue::SortedSet(Arc::new(RwLock::new(ss))),
    );
    match v {
        KiviDBValue::SortedSet(s) => {
            let inner = s.read();
            // by_score must be in ascending order after load
            let scores: Vec<f64> = inner.by_score.keys().map(|(score, _)| score.0).collect();
            assert_eq!(scores, vec![1.0, 2.0, 3.0]);
        }
        _ => panic!("wrong type"),
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// KDB — Stream
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn kdb_stream_basic() {
    let mut stream = StreamInner::new();
    stream.last_id = StreamId::new(1000, 5);
    stream.entries.insert(
        StreamId::new(1000, 1),
        StreamEntry {
            fields: vec![
                (
                    Arc::from(b"name".as_slice()),
                    Arc::from(b"alice".as_slice()),
                ),
                (
                    Arc::from(b"action".as_slice()),
                    Arc::from(b"login".as_slice()),
                ),
            ],
        },
    );
    stream.entries.insert(
        StreamId::new(1000, 2),
        StreamEntry {
            fields: vec![(Arc::from(b"name".as_slice()), Arc::from(b"bob".as_slice()))],
        },
    );

    let v = roundtrip_one("stream", KiviDBValue::Stream(Arc::new(RwLock::new(stream))));
    match v {
        KiviDBValue::Stream(s) => {
            let inner = s.read();
            assert_eq!(inner.entries.len(), 2);
            assert_eq!(inner.last_id, StreamId::new(1000, 5));

            let e1 = inner.entries.get(&StreamId::new(1000, 1)).unwrap();
            assert_eq!(e1.fields[0].0.as_ref(), b"name");
            assert_eq!(e1.fields[0].1.as_ref(), b"alice");
            assert_eq!(e1.fields[1].0.as_ref(), b"action");
            assert_eq!(e1.fields[1].1.as_ref(), b"login");

            let e2 = inner.entries.get(&StreamId::new(1000, 2)).unwrap();
            assert_eq!(e2.fields[0].1.as_ref(), b"bob");
        }
        _ => panic!("wrong type"),
    }
}

#[test]
fn kdb_stream_entry_order() {
    // BTreeMap orders by StreamId — verify entries come back in ID order
    let mut stream = StreamInner::new();
    stream.last_id = StreamId::new(9999, 0);
    for seq in [5u64, 1, 3, 2, 4] {
        stream.entries.insert(
            StreamId::new(1000, seq),
            StreamEntry {
                fields: vec![(
                    Arc::from(b"seq".as_slice()),
                    Arc::from(seq.to_string().as_bytes()),
                )],
            },
        );
    }

    let v = roundtrip_one(
        "ordered_stream",
        KiviDBValue::Stream(Arc::new(RwLock::new(stream))),
    );
    match v {
        KiviDBValue::Stream(s) => {
            let inner = s.read();
            let seqs: Vec<u64> = inner.entries.keys().map(|id| id.seq).collect();
            assert_eq!(seqs, vec![1, 2, 3, 4, 5]); // BTreeMap order
        }
        _ => panic!("wrong type"),
    }
}

#[test]
fn kdb_stream_consumer_groups_and_meta() {
    use kividb::types::{ConsumerGroup, PelEntry};

    let mut stream = StreamInner::new();
    stream.last_id = StreamId::new(1000, 2);
    stream.max_deleted_id = StreamId::new(900, 1);
    stream.entries_added = 7;
    stream.max_entries = Some(100);
    stream.entries.insert(
        StreamId::new(1000, 1),
        StreamEntry {
            fields: vec![(Arc::from(b"f".as_slice()), Arc::from(b"v0".as_slice()))],
        },
    );
    stream.entries.insert(
        StreamId::new(1000, 2),
        StreamEntry {
            fields: vec![(Arc::from(b"f".as_slice()), Arc::from(b"v1".as_slice()))],
        },
    );
    let mut pel = std::collections::BTreeMap::new();
    pel.insert(
        StreamId::new(1000, 1),
        PelEntry {
            consumer: "c1".into(),
            delivered_at: 1_700_000_000_000,
            delivery_count: 2,
        },
    );
    let mut consumers = AHashMap::new();
    consumers.insert("c1".into(), 1_700_000_000_100);
    stream.groups.insert(
        "g1".into(),
        ConsumerGroup {
            name: "g1".into(),
            last_id: StreamId::new(1000, 1),
            entries_read: 1,
            pel,
            consumers,
            active: Default::default(),
        },
    );

    let v = roundtrip_one("st", KiviDBValue::Stream(Arc::new(RwLock::new(stream))));
    match v {
        KiviDBValue::Stream(s) => {
            let inner = s.read();
            assert_eq!(inner.entries.len(), 2);
            assert_eq!(inner.last_id, StreamId::new(1000, 2));
            assert_eq!(inner.max_deleted_id, StreamId::new(900, 1));
            assert_eq!(inner.entries_added, 7);
            assert_eq!(inner.max_entries, Some(100));
            let g = inner.groups.get("g1").expect("group g1 missing");
            assert_eq!(g.last_id, StreamId::new(1000, 1));
            assert_eq!(g.entries_read, 1);
            assert_eq!(g.pel.len(), 1);
            let pel = g.pel.get(&StreamId::new(1000, 1)).unwrap();
            assert_eq!(pel.consumer, "c1");
            assert_eq!(pel.delivery_count, 2);
            assert_eq!(g.consumers.get("c1"), Some(&1_700_000_000_100));
        }
        _ => panic!("wrong type"),
    }
}

#[test]
fn kdb_stream_legacy_v1_loads_without_groups() {
    use std::io::Write;
    let path = tmp_kdb("stream_v1");
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(b"KIVIDB01").unwrap();
    f.write_all(&1u32.to_le_bytes()).unwrap();
    f.write_all(&1u32.to_le_bytes()).unwrap();
    f.write_all(b"s").unwrap();
    f.write_all(&[6u8]).unwrap(); // TAG_STREAM (v1)
    f.write_all(&0u32.to_le_bytes()).unwrap();
    f.write_all(&0u64.to_le_bytes()).unwrap();
    f.write_all(&0u64.to_le_bytes()).unwrap();
    f.write_all(&0u32.to_le_bytes()).unwrap();
    drop(f);

    let loaded = load_snapshot(&path).expect("legacy TAG_STREAM dump must still load");
    std::fs::remove_file(&path).ok();
    match loaded.dbs[0].get("s") {
        Some(KiviDBValue::Stream(s)) => {
            assert!(s.read().groups.is_empty());
        }
        _ => panic!("expected stream"),
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// KDB — Json
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn kdb_json_basic() {
    let json: serde_json::Value = serde_json::json!({
        "name": "alice",
        "age": 30,
        "active": true,
        "scores": [1, 2, 3],
        "meta": { "city": "london" }
    });
    let v = roundtrip_one(
        "doc",
        KiviDBValue::Json(Arc::new(RwLock::new(json.clone()))),
    );
    match v {
        KiviDBValue::Json(j) => {
            let inner = j.read();
            assert_eq!(*inner, json);
        }
        _ => panic!("wrong type"),
    }
}

#[test]
fn kdb_json_null() {
    let v = roundtrip_one(
        "null_doc",
        KiviDBValue::Json(Arc::new(RwLock::new(serde_json::Value::Null))),
    );
    match v {
        KiviDBValue::Json(j) => assert_eq!(*j.read(), serde_json::Value::Null),
        _ => panic!("wrong type"),
    }
}

#[test]
fn kdb_json_deeply_nested() {
    let mut obj = serde_json::json!({});
    // Build 20 levels of nesting
    for i in (0..20).rev() {
        obj = serde_json::json!({ format!("level_{i}"): obj });
    }
    let v = roundtrip_one(
        "deep",
        KiviDBValue::Json(Arc::new(RwLock::new(obj.clone()))),
    );
    match v {
        KiviDBValue::Json(j) => assert_eq!(*j.read(), obj),
        _ => panic!("wrong type"),
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// KDB — TimeSeries
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn kdb_timeseries_basic() {
    let mut ts = TsInner::new();
    ts.samples.insert(1000, 1.5);
    ts.samples.insert(2000, 2.5);
    ts.samples.insert(3000, 3.5);
    ts.retention = 86_400_000;
    ts.chunk_size = 8192;
    ts.labels.insert("sensor".to_string(), "temp01".to_string());
    ts.labels
        .insert("location".to_string(), "room_a".to_string());
    ts.last_ts = Some(3000);

    let v = roundtrip_one("ts", KiviDBValue::TimeSeries(Arc::new(RwLock::new(ts))));
    match v {
        KiviDBValue::TimeSeries(t) => {
            let inner = t.read();
            assert_eq!(inner.samples.len(), 3);
            assert_eq!(*inner.samples.get(&1000).unwrap(), 1.5);
            assert_eq!(*inner.samples.get(&2000).unwrap(), 2.5);
            assert_eq!(*inner.samples.get(&3000).unwrap(), 3.5);
            assert_eq!(inner.retention, 86_400_000);
            assert_eq!(inner.chunk_size, 8192);
            assert_eq!(inner.labels["sensor"], "temp01");
            assert_eq!(inner.labels["location"], "room_a");
        }
        _ => panic!("wrong type"),
    }
}

#[test]
fn kdb_timeseries_value_precision() {
    // f64 sample values must be bit-exact after roundtrip
    let mut ts = TsInner::new();
    let tricky = f64::from_bits(0x400921FB54442D18); // pi to full precision
    ts.samples.insert(1, tricky);

    let v = roundtrip_one(
        "ts_precise",
        KiviDBValue::TimeSeries(Arc::new(RwLock::new(ts))),
    );
    match v {
        KiviDBValue::TimeSeries(t) => {
            let inner = t.read();
            let loaded = *inner.samples.get(&1).unwrap();
            assert_eq!(loaded.to_bits(), tricky.to_bits());
        }
        _ => panic!("wrong type"),
    }
}

#[test]
fn kdb_timeseries_many_samples() {
    let mut ts = TsInner::new();
    for i in 0u64..10_000 {
        ts.samples.insert(i * 1000, i as f64 * 0.1);
    }

    let v = roundtrip_one("big_ts", KiviDBValue::TimeSeries(Arc::new(RwLock::new(ts))));
    match v {
        KiviDBValue::TimeSeries(t) => {
            let inner = t.read();
            assert_eq!(inner.samples.len(), 10_000);
            // spot-check a few
            assert!((inner.samples[&0] - 0.0).abs() < 1e-10);
            assert!((inner.samples[&5_000_000] - 500.0).abs() < 1e-10);
            assert!((inner.samples[&9_999_000] - 999.9).abs() < 1e-10);
        }
        _ => panic!("wrong type"),
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// KDB — VectorSet
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn kdb_vectorset_basic() {
    let mut vs = VectorSetInner::new(3);
    vs.entries.insert(
        "vec_a".to_string(),
        VSetEntry {
            vector: vec![1.0f32, 2.0, 3.0],
            attrs: None,
        },
    );
    vs.entries.insert(
        "vec_b".to_string(),
        VSetEntry {
            vector: vec![4.0f32, 5.0, 6.0],
            attrs: Some(r#"{"label":"b"}"#.to_string()),
        },
    );

    let v = roundtrip_one("vset", KiviDBValue::VectorSet(Arc::new(RwLock::new(vs))));
    match v {
        KiviDBValue::VectorSet(vs) => {
            let inner = vs.read();
            assert_eq!(inner.dim, 3);
            assert_eq!(inner.entries.len(), 2);

            let a = inner.entries.get("vec_a").unwrap();
            assert_eq!(a.vector, vec![1.0f32, 2.0, 3.0]);
            assert!(a.attrs.is_none());

            let b = inner.entries.get("vec_b").unwrap();
            assert_eq!(b.vector, vec![4.0f32, 5.0, 6.0]);
            assert_eq!(b.attrs.as_deref().unwrap(), r#"{"label":"b"}"#);
        }
        _ => panic!("wrong type"),
    }
}

#[test]
fn kdb_vectorset_high_dim() {
    let dim = 1536usize; // OpenAI embedding dimension
    let vec: Vec<f32> = (0..dim).map(|i| i as f32 / dim as f32).collect();

    let mut vs = VectorSetInner::new(dim);
    vs.entries.insert(
        "embedding".to_string(),
        VSetEntry {
            vector: vec.clone(),
            attrs: None,
        },
    );

    let v = roundtrip_one("highdim", KiviDBValue::VectorSet(Arc::new(RwLock::new(vs))));
    match v {
        KiviDBValue::VectorSet(vs) => {
            let inner = vs.read();
            assert_eq!(inner.dim, dim);
            let loaded_vec = &inner.entries["embedding"].vector;
            assert_eq!(loaded_vec.len(), dim);
            // f32 bit-exact check
            for (orig, loaded) in vec.iter().zip(loaded_vec.iter()) {
                assert_eq!(orig.to_bits(), loaded.to_bits());
            }
        }
        _ => panic!("wrong type"),
    }
}

#[test]
fn kdb_vectorset_f32_precision() {
    // f32 edge cases
    let mut vs = VectorSetInner::new(4);
    vs.entries.insert(
        "edge".to_string(),
        VSetEntry {
            vector: vec![f32::INFINITY, f32::NEG_INFINITY, f32::MIN, f32::MAX],
            attrs: None,
        },
    );

    let v = roundtrip_one(
        "vset_edge",
        KiviDBValue::VectorSet(Arc::new(RwLock::new(vs))),
    );
    match v {
        KiviDBValue::VectorSet(vs) => {
            let inner = vs.read();
            let vec = &inner.entries["edge"].vector;
            assert!(vec[0].is_infinite() && vec[0] > 0.0);
            assert!(vec[1].is_infinite() && vec[1] < 0.0);
            assert_eq!(vec[2].to_bits(), f32::MIN.to_bits());
            assert_eq!(vec[3].to_bits(), f32::MAX.to_bits());
        }
        _ => panic!("wrong type"),
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// KDB — Multiple databases
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn kdb_multiple_databases() {
    let mut db0: HashMap<String, KiviDBValue> = HashMap::new();
    db0.insert(
        "k0".to_string(),
        KiviDBValue::String(kividb::types::SharedBytes::from(b"in_db0".as_slice())),
    );

    let mut db1: HashMap<String, KiviDBValue> = HashMap::new();
    db1.insert(
        "k1".to_string(),
        KiviDBValue::String(kividb::types::SharedBytes::from(b"in_db1".as_slice())),
    );

    let mut db2: HashMap<String, KiviDBValue> = HashMap::new();
    db2.insert(
        "k2".to_string(),
        KiviDBValue::String(kividb::types::SharedBytes::from(b"in_db2".as_slice())),
    );

    let path = tmp_kdb("multidbs");
    write_snapshot(&path, &[db0, db1, db2], None).unwrap();
    let loaded = load_snapshot(&path).unwrap();
    std::fs::remove_file(&path).ok();

    assert_eq!(loaded.dbs.len(), 3);
    match &loaded.dbs[0]["k0"] {
        KiviDBValue::String(b) => assert_eq!(b.as_ref(), b"in_db0"),
        _ => panic!(),
    }
    match &loaded.dbs[1]["k1"] {
        KiviDBValue::String(b) => assert_eq!(b.as_ref(), b"in_db1"),
        _ => panic!(),
    }
    match &loaded.dbs[2]["k2"] {
        KiviDBValue::String(b) => assert_eq!(b.as_ref(), b"in_db2"),
        _ => panic!(),
    }
}

#[test]
fn kdb_same_key_different_databases() {
    // The same key name in different databases must be independent
    let mut db0: HashMap<String, KiviDBValue> = HashMap::new();
    db0.insert(
        "shared".to_string(),
        KiviDBValue::String(kividb::types::SharedBytes::from(b"db0_value".as_slice())),
    );

    let mut db1: HashMap<String, KiviDBValue> = HashMap::new();
    db1.insert(
        "shared".to_string(),
        KiviDBValue::String(kividb::types::SharedBytes::from(b"db1_value".as_slice())),
    );

    let path = tmp_kdb("shared_key");
    write_snapshot(&path, &[db0, db1], None).unwrap();
    let loaded = load_snapshot(&path).unwrap();
    std::fs::remove_file(&path).ok();

    match &loaded.dbs[0]["shared"] {
        KiviDBValue::String(b) => assert_eq!(b.as_ref(), b"db0_value"),
        _ => panic!(),
    }
    match &loaded.dbs[1]["shared"] {
        KiviDBValue::String(b) => assert_eq!(b.as_ref(), b"db1_value"),
        _ => panic!(),
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// KDB — Mixed types in same database
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn kdb_all_types_together() {
    let mut map: HashMap<String, KiviDBValue> = HashMap::new();

    map.insert(
        "str".to_string(),
        KiviDBValue::String(kividb::types::SharedBytes::from(b"hello".as_slice())),
    );

    map.insert(
        "list".to_string(),
        KiviDBValue::List(Arc::new(RwLock::new(
            vec![Arc::from(b"a".as_slice()), Arc::from(b"b".as_slice())].into(),
        ))),
    );

    let mut hmap = kividb::types::HashFields::default();
    hmap.insert("f".to_string(), Arc::from(b"v".as_slice()));
    map.insert(
        "hash".to_string(),
        KiviDBValue::Hash(Arc::new(RwLock::new(hmap))),
    );

    let mut hset = kividb::member_set::MemberSet::new();
    hset.insert(Arc::from(b"member".as_slice()));
    map.insert(
        "set".to_string(),
        KiviDBValue::Set(Arc::new(RwLock::new(hset))),
    );

    let mut ss = SortedSetInner::new();
    ss.insert(Arc::from(b"m".as_slice()), 42.0);
    map.insert(
        "zset".to_string(),
        KiviDBValue::SortedSet(Arc::new(RwLock::new(ss))),
    );

    map.insert(
        "json".to_string(),
        KiviDBValue::Json(Arc::new(RwLock::new(serde_json::json!({"x": 1})))),
    );

    let mut ts = TsInner::new();
    ts.samples.insert(1000, 99.0);
    map.insert(
        "ts".to_string(),
        KiviDBValue::TimeSeries(Arc::new(RwLock::new(ts))),
    );

    let mut stream = StreamInner::new();
    stream.last_id = StreamId::new(1, 0);
    stream.entries.insert(
        StreamId::new(1, 0),
        StreamEntry {
            fields: vec![(Arc::from(b"k".as_slice()), Arc::from(b"v".as_slice()))],
        },
    );
    map.insert(
        "stream".to_string(),
        KiviDBValue::Stream(Arc::new(RwLock::new(stream))),
    );

    let mut vs = VectorSetInner::new(2);
    vs.entries.insert(
        "e".to_string(),
        VSetEntry {
            vector: vec![1.0, 2.0],
            attrs: None,
        },
    );
    map.insert(
        "vset".to_string(),
        KiviDBValue::VectorSet(Arc::new(RwLock::new(vs))),
    );

    let path = tmp_kdb("all_types");
    write_snapshot(&path, &[map], None).unwrap();
    let mut loaded = load_snapshot(&path).unwrap();
    std::fs::remove_file(&path).ok();

    let db = loaded.dbs.remove(0);
    assert_eq!(db.len(), 9);

    assert!(matches!(db["str"], KiviDBValue::String(_)));
    assert!(matches!(db["list"], KiviDBValue::List(_)));
    assert!(matches!(db["hash"], KiviDBValue::Hash(_)));
    assert!(matches!(db["set"], KiviDBValue::Set(_)));
    assert!(matches!(db["zset"], KiviDBValue::SortedSet(_)));
    assert!(matches!(db["json"], KiviDBValue::Json(_)));
    assert!(matches!(db["ts"], KiviDBValue::TimeSeries(_)));
    assert!(matches!(db["stream"], KiviDBValue::Stream(_)));
    assert!(matches!(db["vset"], KiviDBValue::VectorSet(_)));
}

// ═════════════════════════════════════════════════════════════════════════════
// KDB — Error and edge cases
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn kdb_bad_magic_rejected() {
    let path = tmp_kdb("bad_magic");
    std::fs::write(&path, b"WRONGHDR\x01\x00\x00\x00").unwrap();
    let result = load_snapshot(&path);
    std::fs::remove_file(&path).ok();
    assert!(result.is_err());
}

#[test]
fn kdb_empty_store_roundtrip() {
    let path = tmp_kdb("empty");
    let maps: Vec<HashMap<String, KiviDBValue>> = vec![HashMap::new()];
    write_snapshot(&path, &maps, None).unwrap();
    let loaded = load_snapshot(&path).unwrap();
    std::fs::remove_file(&path).ok();
    assert_eq!(loaded.dbs.len(), 1);
    assert!(loaded.dbs[0].is_empty());
}

#[test]
fn kdb_unicode_keys() {
    let mut map: HashMap<String, KiviDBValue> = HashMap::new();
    map.insert(
        "日本語".to_string(),
        KiviDBValue::String(kividb::types::SharedBytes::from(b"japanese".as_slice())),
    );
    map.insert(
        "العربية".to_string(),
        KiviDBValue::String(kividb::types::SharedBytes::from(b"arabic".as_slice())),
    );
    map.insert(
        "🦀".to_string(),
        KiviDBValue::String(kividb::types::SharedBytes::from(b"crab".as_slice())),
    );

    let path = tmp_kdb("unicode");
    write_snapshot(&path, &[map], None).unwrap();
    let mut loaded = load_snapshot(&path).unwrap();
    std::fs::remove_file(&path).ok();

    let db = loaded.dbs.remove(0);
    assert_eq!(db.len(), 3);
    match &db["日本語"] {
        KiviDBValue::String(b) => assert_eq!(b.as_ref(), b"japanese"),
        _ => panic!(),
    }
    match &db["🦀"] {
        KiviDBValue::String(b) => assert_eq!(b.as_ref(), b"crab"),
        _ => panic!(),
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Redis RDB — helpers for constructing minimal valid RDB bytes
// ═════════════════════════════════════════════════════════════════════════════

/// Encode a length using RDB's variable-length encoding (6-bit prefix).
fn rdb_encode_len(len: usize) -> Vec<u8> {
    if len <= 63 {
        vec![len as u8]
    } else if len <= 16383 {
        vec![0x40 | ((len >> 8) as u8), (len & 0xFF) as u8]
    } else {
        let mut v = vec![0x80];
        v.extend_from_slice(&(len as u32).to_be_bytes());
        v
    }
}

/// Encode a string as RDB length-prefixed bytes.
fn rdb_str(s: &[u8]) -> Vec<u8> {
    let mut v = rdb_encode_len(s.len());
    v.extend_from_slice(s);
    v
}

/// Build a complete minimal RDB file wrapping `payload` (type+key+value triples).
/// `payload` is the raw bytes between the DB selector header and the EOF marker.
fn rdb_file(db: u8, payload: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"REDIS"); // magic
    out.extend_from_slice(b"0011"); // version 11
    out.push(0xFE); // SELECT db
    out.extend_from_slice(&rdb_encode_len(db as usize));
    out.push(0xFB); // RESIZE DB hint
    out.extend_from_slice(&rdb_encode_len(1)); // keyspace size
    out.extend_from_slice(&rdb_encode_len(0)); // expires size
    out.extend_from_slice(&payload);
    out.push(0xFF); // EOF
    out.extend_from_slice(&[0u8; 8]); // CRC64 (zeroed — we skip verification)
    out
}

fn write_rdb_file(tag: &str, bytes: Vec<u8>) -> String {
    let path = tmp_rdb(tag);
    std::fs::write(&path, bytes).unwrap();
    path
}

// ═════════════════════════════════════════════════════════════════════════════
// Redis RDB — String (type 0)
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn rdb_string_basic() {
    let mut payload = Vec::new();
    payload.push(0u8); // type: string
    payload.extend(rdb_str(b"greeting"));
    payload.extend(rdb_str(b"hello world"));

    let path = write_rdb_file("rdb_str", rdb_file(0, payload));
    let loaded = load_redis_rdb(&path).unwrap();
    std::fs::remove_file(&path).ok();

    match &loaded[0]["greeting"] {
        KiviDBValue::String(b) => assert_eq!(b.as_ref(), b"hello world"),
        _ => panic!("wrong type"),
    }
}

#[test]
fn rdb_string_integer_encoded() {
    // RDB encodes small integers as INT8/INT16/INT32 — must decode to string representation
    let mut payload = Vec::new();
    payload.push(0u8); // type: string
    payload.extend(rdb_str(b"myint")); // key
    // INT8 encoding: 0xC0 prefix + 1 byte value
    payload.push(0xC0); // is_encoded=true, enc_type=INT8
    payload.push(42u8); // value = 42

    let path = write_rdb_file("rdb_int8", rdb_file(0, payload));
    let loaded = load_redis_rdb(&path).unwrap();
    std::fs::remove_file(&path).ok();

    match &loaded[0]["myint"] {
        KiviDBValue::String(b) => assert_eq!(std::str::from_utf8(b).unwrap(), "42"),
        _ => panic!("wrong type"),
    }
}

#[test]
fn rdb_string_int16_encoded() {
    let mut payload = Vec::new();
    payload.push(0u8);
    payload.extend(rdb_str(b"k"));
    payload.push(0xC1); // INT16
    payload.extend_from_slice(&1000i16.to_le_bytes());

    let path = write_rdb_file("rdb_int16", rdb_file(0, payload));
    let loaded = load_redis_rdb(&path).unwrap();
    std::fs::remove_file(&path).ok();

    match &loaded[0]["k"] {
        KiviDBValue::String(b) => assert_eq!(std::str::from_utf8(b).unwrap(), "1000"),
        _ => panic!("wrong type"),
    }
}

#[test]
fn rdb_string_int32_encoded() {
    let mut payload = Vec::new();
    payload.push(0u8);
    payload.extend(rdb_str(b"k"));
    payload.push(0xC2); // INT32
    payload.extend_from_slice(&100_000i32.to_le_bytes());

    let path = write_rdb_file("rdb_int32", rdb_file(0, payload));
    let loaded = load_redis_rdb(&path).unwrap();
    std::fs::remove_file(&path).ok();

    match &loaded[0]["k"] {
        KiviDBValue::String(b) => assert_eq!(std::str::from_utf8(b).unwrap(), "100000"),
        _ => panic!("wrong type"),
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Redis RDB — Hash (type 4, hashtable encoding)
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn rdb_hash_basic() {
    let mut payload = Vec::new();
    payload.push(4u8); // type: hash
    payload.extend(rdb_str(b"myhash"));
    payload.extend(rdb_encode_len(2)); // 2 fields
    payload.extend(rdb_str(b"name"));
    payload.extend(rdb_str(b"alice"));
    payload.extend(rdb_str(b"age"));
    payload.extend(rdb_str(b"30"));

    let path = write_rdb_file("rdb_hash", rdb_file(0, payload));
    let loaded = load_redis_rdb(&path).unwrap();
    std::fs::remove_file(&path).ok();

    match &loaded[0]["myhash"] {
        KiviDBValue::Hash(h) => {
            let inner = h.read();
            assert_eq!(inner.len(), 2);
            assert_eq!(inner["name"].as_ref(), b"alice");
            assert_eq!(inner["age"].as_ref(), b"30");
        }
        _ => panic!("wrong type"),
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Redis RDB — List (type 1, old linkedlist encoding)
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn rdb_list_basic() {
    let mut payload = Vec::new();
    payload.push(1u8); // type: list
    payload.extend(rdb_str(b"mylist"));
    payload.extend(rdb_encode_len(3)); // 3 elements
    payload.extend(rdb_str(b"alpha"));
    payload.extend(rdb_str(b"beta"));
    payload.extend(rdb_str(b"gamma"));

    let path = write_rdb_file("rdb_list", rdb_file(0, payload));
    let loaded = load_redis_rdb(&path).unwrap();
    std::fs::remove_file(&path).ok();

    match &loaded[0]["mylist"] {
        KiviDBValue::List(l) => {
            let inner = l.read();
            assert_eq!(inner.len(), 3);
            assert_eq!(inner[0].as_ref(), b"alpha");
            assert_eq!(inner[1].as_ref(), b"beta");
            assert_eq!(inner[2].as_ref(), b"gamma");
        }
        _ => panic!("wrong type"),
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Redis RDB — Set (type 2)
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn rdb_set_basic() {
    let mut payload = Vec::new();
    payload.push(2u8); // type: set
    payload.extend(rdb_str(b"myset"));
    payload.extend(rdb_encode_len(2)); // 2 members
    payload.extend(rdb_str(b"apple"));
    payload.extend(rdb_str(b"banana"));

    let path = write_rdb_file("rdb_set", rdb_file(0, payload));
    let loaded = load_redis_rdb(&path).unwrap();
    std::fs::remove_file(&path).ok();

    match &loaded[0]["myset"] {
        KiviDBValue::Set(s) => {
            let inner = s.read();
            assert_eq!(inner.len(), 2);
            assert!(inner.contains(b"apple".as_slice()));
            assert!(inner.contains(b"banana".as_slice()));
        }
        _ => panic!("wrong type"),
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Redis RDB — Sorted Set v1 (type 3, score as double string)
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn rdb_zset_v1_basic() {
    let mut payload = Vec::new();
    payload.push(3u8); // type: zset v1
    payload.extend(rdb_str(b"myzset"));
    payload.extend(rdb_encode_len(2)); // 2 members

    // member "a" with score 1.5
    payload.extend(rdb_str(b"a"));
    let score_str = b"1.5";
    payload.push(score_str.len() as u8); // double string length byte
    payload.extend_from_slice(score_str);

    // member "b" with score 2.5
    payload.extend(rdb_str(b"b"));
    let score_str2 = b"2.5";
    payload.push(score_str2.len() as u8);
    payload.extend_from_slice(score_str2);

    let path = write_rdb_file("rdb_zset_v1", rdb_file(0, payload));
    let loaded = load_redis_rdb(&path).unwrap();
    std::fs::remove_file(&path).ok();

    match &loaded[0]["myzset"] {
        KiviDBValue::SortedSet(s) => {
            let inner = s.read();
            assert_eq!(inner.len(), 2);
            assert!((inner.members.get(b"a".as_slice()).unwrap() - 1.5).abs() < 1e-10);
            assert!((inner.members.get(b"b".as_slice()).unwrap() - 2.5).abs() < 1e-10);
        }
        _ => panic!("wrong type"),
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Redis RDB — Sorted Set v2 (type 5, binary double)
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn rdb_zset_v2_basic() {
    let mut payload = Vec::new();
    payload.push(5u8); // type: zset v2
    payload.extend(rdb_str(b"zv2"));
    payload.extend(rdb_encode_len(2));

    payload.extend(rdb_str(b"x"));
    payload.extend_from_slice(&10.0f64.to_le_bytes());

    payload.extend(rdb_str(b"y"));
    payload.extend_from_slice(&(-3.15f64).to_le_bytes());

    let path = write_rdb_file("rdb_zset_v2", rdb_file(0, payload));
    let loaded = load_redis_rdb(&path).unwrap();
    std::fs::remove_file(&path).ok();

    match &loaded[0]["zv2"] {
        KiviDBValue::SortedSet(s) => {
            let inner = s.read();
            assert!((inner.members.get(b"x".as_slice()).unwrap() - 10.0).abs() < 1e-10);
            assert!((inner.members.get(b"y".as_slice()).unwrap() - (-3.15)).abs() < 1e-10);
        }
        _ => panic!("wrong type"),
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Redis RDB — AUX fields and DB selector
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn rdb_aux_fields_skipped() {
    // AUX fields (0xFA) are metadata — they must be skipped without error
    let mut out = Vec::new();
    out.extend_from_slice(b"REDIS0011");
    // AUX: redis-ver = "7.2.0"
    out.push(0xFA);
    out.extend(rdb_str(b"redis-ver"));
    out.extend(rdb_str(b"7.2.0"));
    // AUX: redis-bits = "64"
    out.push(0xFA);
    out.extend(rdb_str(b"redis-bits"));
    out.extend(rdb_str(b"64"));
    // DB 0
    out.push(0xFE);
    out.extend(rdb_encode_len(0));
    out.push(0xFB);
    out.extend(rdb_encode_len(1));
    out.extend(rdb_encode_len(0));
    // One key
    out.push(0u8);
    out.extend(rdb_str(b"k"));
    out.extend(rdb_str(b"v"));
    out.push(0xFF);
    out.extend_from_slice(&[0u8; 8]);

    let path = write_rdb_file("rdb_aux", out);
    let loaded = load_redis_rdb(&path).unwrap();
    std::fs::remove_file(&path).ok();

    match &loaded[0]["k"] {
        KiviDBValue::String(b) => assert_eq!(b.as_ref(), b"v"),
        _ => panic!("wrong type"),
    }
}

#[test]
fn rdb_multi_db_select() {
    // Keys in different databases must land in the right db slot
    let mut out = Vec::new();
    out.extend_from_slice(b"REDIS0011");

    // DB 0
    out.push(0xFE);
    out.extend(rdb_encode_len(0));
    out.push(0xFB);
    out.extend(rdb_encode_len(1));
    out.extend(rdb_encode_len(0));
    out.push(0u8);
    out.extend(rdb_str(b"db0key"));
    out.extend(rdb_str(b"db0val"));

    // DB 3
    out.push(0xFE);
    out.extend(rdb_encode_len(3));
    out.push(0xFB);
    out.extend(rdb_encode_len(1));
    out.extend(rdb_encode_len(0));
    out.push(0u8);
    out.extend(rdb_str(b"db3key"));
    out.extend(rdb_str(b"db3val"));

    out.push(0xFF);
    out.extend_from_slice(&[0u8; 8]);

    let path = write_rdb_file("rdb_multidb", out);
    let loaded = load_redis_rdb(&path).unwrap();
    std::fs::remove_file(&path).ok();

    // DB 0
    match &loaded[0]["db0key"] {
        KiviDBValue::String(b) => assert_eq!(b.as_ref(), b"db0val"),
        _ => panic!(),
    }
    // DB 3
    match &loaded[3]["db3key"] {
        KiviDBValue::String(b) => assert_eq!(b.as_ref(), b"db3val"),
        _ => panic!(),
    }
    // DB 1 and 2 are empty
    assert!(loaded[1].is_empty());
    assert!(loaded[2].is_empty());
}

#[test]
fn rdb_expiry_prefix_discarded() {
    // Keys with expiry (0xFC prefix) must load but the expiry is discarded
    let mut payload = Vec::new();
    // 0xFC = expiry in ms
    payload.push(0xFC);
    payload.extend_from_slice(&9_999_999_999_999u64.to_le_bytes()); // far future
    payload.push(0u8); // string type
    payload.extend(rdb_str(b"expkey"));
    payload.extend(rdb_str(b"expval"));

    let path = write_rdb_file("rdb_expiry", rdb_file(0, payload));
    let loaded = load_redis_rdb(&path).unwrap();
    std::fs::remove_file(&path).ok();

    // Key must exist (expiry is not enforced at load time)
    match &loaded[0]["expkey"] {
        KiviDBValue::String(b) => assert_eq!(b.as_ref(), b"expval"),
        _ => panic!("wrong type"),
    }
}

#[test]
fn rdb_bad_magic_rejected() {
    let path = write_rdb_file("rdb_bad_magic", b"NOTREDIS0011\xFF".to_vec());
    let result = load_redis_rdb(&path);
    std::fs::remove_file(&path).ok();
    assert!(result.is_err());
}

#[test]
fn rdb_empty_database() {
    // Valid RDB with one empty database
    let mut out = Vec::new();
    out.extend_from_slice(b"REDIS0011");
    out.push(0xFE);
    out.extend(rdb_encode_len(0));
    out.push(0xFB);
    out.extend(rdb_encode_len(0)); // 0 keys
    out.extend(rdb_encode_len(0));
    out.push(0xFF);
    out.extend_from_slice(&[0u8; 8]);

    let path = write_rdb_file("rdb_empty", out);
    let loaded = load_redis_rdb(&path).unwrap();
    std::fs::remove_file(&path).ok();

    assert!(loaded[0].is_empty());
}

// ═════════════════════════════════════════════════════════════════════════════
// Redis RDB — Multiple keys of different types in one file
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn rdb_mixed_types_one_db() {
    let mut payload = Vec::new();

    // String
    payload.push(0u8);
    payload.extend(rdb_str(b"str"));
    payload.extend(rdb_str(b"strval"));

    // Hash
    payload.push(4u8);
    payload.extend(rdb_str(b"hash"));
    payload.extend(rdb_encode_len(1));
    payload.extend(rdb_str(b"field"));
    payload.extend(rdb_str(b"value"));

    // List
    payload.push(1u8);
    payload.extend(rdb_str(b"list"));
    payload.extend(rdb_encode_len(2));
    payload.extend(rdb_str(b"one"));
    payload.extend(rdb_str(b"two"));

    // Set
    payload.push(2u8);
    payload.extend(rdb_str(b"set"));
    payload.extend(rdb_encode_len(1));
    payload.extend(rdb_str(b"member"));

    // Sorted set v2
    payload.push(5u8);
    payload.extend(rdb_str(b"zset"));
    payload.extend(rdb_encode_len(1));
    payload.extend(rdb_str(b"elem"));
    payload.extend_from_slice(&7.0f64.to_le_bytes());

    let path = write_rdb_file("rdb_mixed", rdb_file(0, payload));
    let loaded = load_redis_rdb(&path).unwrap();
    std::fs::remove_file(&path).ok();

    assert_eq!(loaded[0].len(), 5);
    assert!(matches!(loaded[0]["str"], KiviDBValue::String(_)));
    assert!(matches!(loaded[0]["hash"], KiviDBValue::Hash(_)));
    assert!(matches!(loaded[0]["list"], KiviDBValue::List(_)));
    assert!(matches!(loaded[0]["set"], KiviDBValue::Set(_)));
    assert!(matches!(loaded[0]["zset"], KiviDBValue::SortedSet(_)));
}

// ═════════════════════════════════════════════════════════════════════════════
// KDB — HashEx (hash with per-field TTLs)
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn kdb_hashex_basic_no_ttls() {
    // A HashEx with no expiries should still round-trip cleanly.
    use kividb::types::HashInner;

    let mut inner = HashInner::new();
    inner
        .fields
        .insert("a".to_string(), Arc::from(b"1".as_slice()));
    inner
        .fields
        .insert("b".to_string(), Arc::from(b"2".as_slice()));

    let v = roundtrip_one("hx", KiviDBValue::HashEx(Arc::new(RwLock::new(inner))));
    match v {
        KiviDBValue::HashEx(h) => {
            let read = h.read();
            assert_eq!(read.fields.len(), 2);
            assert_eq!(read.expiries.len(), 0);
            assert_eq!(read.fields["a"].as_ref(), b"1");
            assert_eq!(read.fields["b"].as_ref(), b"2");
        }
        _ => panic!("wrong variant after round-trip"),
    }
}

#[test]
fn kdb_hashex_with_field_ttls() {
    use kividb::types::HashInner;

    let mut inner = HashInner::new();
    inner
        .fields
        .insert("a".to_string(), Arc::from(b"1".as_slice()));
    inner
        .fields
        .insert("b".to_string(), Arc::from(b"2".as_slice()));
    // Set field "a" to expire ~5 minutes from now.
    inner.expiries.insert(
        "a".to_string(),
        std::time::Instant::now() + std::time::Duration::from_secs(300),
    );

    let v = roundtrip_one("hxttl", KiviDBValue::HashEx(Arc::new(RwLock::new(inner))));
    match v {
        KiviDBValue::HashEx(h) => {
            let read = h.read();
            assert_eq!(read.fields.len(), 2);
            assert_eq!(read.expiries.len(), 1);
            assert!(read.expiries.contains_key("a"));
            // Re-anchored TTL should still be in the future, in the rough
            // ballpark of 5 minutes (allow generous slack for slow CI).
            let remaining = read.expiries["a"].saturating_duration_since(std::time::Instant::now());
            assert!(remaining.as_secs() >= 250 && remaining.as_secs() <= 305);
        }
        _ => panic!("wrong variant after round-trip"),
    }
}

#[test]
fn kdb_hashex_already_expired_ttl_is_dropped_on_load() {
    // A field that expired while the snapshot was at rest must not come back
    // without an expiry and become permanent.
    use kividb::types::HashInner;

    let mut inner = HashInner::new();
    inner
        .fields
        .insert("a".to_string(), Arc::from(b"1".as_slice()));
    // Manually craft an expiry in the past by anchoring an Instant just
    // before now. (The serializer converts to abs_ms via SystemTime.)
    inner.expiries.insert(
        "a".to_string(),
        std::time::Instant::now() - std::time::Duration::from_secs(10),
    );

    let mut map = HashMap::new();
    map.insert(
        "hxgone".to_string(),
        KiviDBValue::HashEx(Arc::new(RwLock::new(inner))),
    );
    let db0 = roundtrip_kdb(vec![map]);
    assert!(!db0.contains_key("hxgone"));
}

// A dump.rdb written by Redis 7.4 (RDB 12) and by Redis 8.10 (RDB 15), by
// SAVE, loads: its aux fields, a key with a TTL, and the compact encodings
// those versions write for small strings, lists, sets, sorted sets and
// hashes, and a large list. Up to 1.1.0 most compact encodings were read
// under the wrong RDB type numbers, and the listpack decoder misread short
// strings, so such a file did not load (#39).
#[test]
fn rdb_files_written_by_redis_7_4_and_8_10_load() {
    let files: [(&str, &[u8]); 2] = [
        ("7.4", include_bytes!("fixtures/redis-7.4.rdb")),
        ("8.10", include_bytes!("fixtures/redis-8.10.rdb")),
    ];
    for (label, bytes) in files {
        let (dbs, expiries, _) = kividb::snapshot::load_redis_rdb_from(bytes)
            .unwrap_or_else(|e| panic!("Redis {label}: {e}"));
        let db = &dbs[0];
        let mut keys: Vec<&str> = db.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            ["h", "l", "lbig", "n", "s", "si", "ss", "ttl", "z"],
            "Redis {label}"
        );
        let string = |key: &str| match &db[key] {
            KiviDBValue::String(b) => String::from_utf8(b.to_vec()).unwrap(),
            _ => panic!("Redis {label} {key}: wrong type"),
        };
        assert_eq!(string("s"), "hello", "Redis {label}");
        assert_eq!(string("n"), "42", "Redis {label}");
        assert_eq!(string("ttl"), "soon", "Redis {label}");
        assert!(
            expiries[0].contains_key("ttl"),
            "Redis {label}: the TTL was lost"
        );
        assert!(!expiries[0].contains_key("s"), "Redis {label}");

        let list = |key: &str| match &db[key] {
            KiviDBValue::List(l) => l
                .read()
                .iter()
                .map(|v| String::from_utf8(v.to_vec()).unwrap())
                .collect::<Vec<_>>(),
            _ => panic!("Redis {label} {key}: wrong type"),
        };
        assert_eq!(list("l"), ["a", "b", "3"], "Redis {label}");
        let big = list("lbig");
        assert_eq!(big.len(), 300, "Redis {label}");
        assert_eq!(
            big[0],
            format!("item-0-{}", "x".repeat(40)),
            "Redis {label}"
        );
        assert_eq!(
            big[299],
            format!("item-299-{}", "x".repeat(40)),
            "Redis {label}"
        );

        let set = |key: &str| match &db[key] {
            KiviDBValue::Set(s) => {
                let mut members: Vec<String> = s
                    .read()
                    .iter()
                    .map(|m| String::from_utf8(m.to_vec()).unwrap())
                    .collect();
                members.sort();
                members
            }
            _ => panic!("Redis {label} {key}: wrong type"),
        };
        assert_eq!(set("si"), ["1", "2", "3"], "Redis {label}");
        assert_eq!(set("ss"), ["x", "y"], "Redis {label}");

        match &db["z"] {
            KiviDBValue::SortedSet(z) => {
                let z = z.read();
                let score = |m: &[u8]| z.members.get(m).copied();
                assert_eq!(score(b"a"), Some(1.0), "Redis {label}");
                assert_eq!(score(b"b"), Some(2.5), "Redis {label}");
                assert_eq!(z.members.len(), 2, "Redis {label}");
            }
            _ => panic!("Redis {label} z: wrong type"),
        }
        match &db["h"] {
            KiviDBValue::Hash(h) => {
                let h = h.read();
                assert_eq!(h.get("f").map(|v| &v[..]), Some(&b"v"[..]), "Redis {label}");
                assert_eq!(h.get("g").map(|v| &v[..]), Some(&b"7"[..]), "Redis {label}");
                assert_eq!(h.len(), 2, "Redis {label}");
            }
            _ => panic!("Redis {label} h: wrong type"),
        }
    }
}

// A dump.rdb written by Redis 8.10 (RDB 15) holding a stream with a consumer
// group and a pending entry (the later entry's sequence below the master's),
// a second stream, a RedisJSON document and a hash with a field TTL, loads
// (#39). The field's TTL was set ten years out (HEXPIRE h2 315360000): the
// loader drops fields past their time, so a short one makes this test fail
// once it runs out, as the first capture's one hour did.
#[test]
fn an_rdb_file_with_streams_json_and_field_ttls_from_redis_8_10_loads() {
    let (dbs, _, _) = kividb::snapshot::load_redis_rdb_from(
        &include_bytes!("fixtures/redis-8.10-streams-json-ttl.rdb")[..],
    )
    .unwrap();
    let db = &dbs[0];
    let mut keys: Vec<&str> = db.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, ["doc2", "h2", "st", "st2"]);

    let KiviDBValue::Stream(st) = &db["st"] else {
        panic!("st is not a stream");
    };
    let st = st.read();
    assert_eq!(st.entries.len(), 2);
    let first = st.entries.keys().next().unwrap();
    assert_eq!((first.ms, first.seq), (1, 1));
    let group = &st.groups["g"];
    assert_eq!(group.pel.len(), 1);
    assert_eq!(group.pel.values().next().unwrap().consumer, "alice");
    assert!(group.consumers.contains_key("alice"));

    let KiviDBValue::Stream(st2) = &db["st2"] else {
        panic!("st2 is not a stream");
    };
    assert_eq!(st2.read().entries.len(), 1);

    let KiviDBValue::Json(doc) = &db["doc2"] else {
        panic!("doc2 is not JSON");
    };
    assert_eq!(*doc.read(), serde_json::json!({"z": 1}));

    let KiviDBValue::HashEx(h) = &db["h2"] else {
        panic!("h2 is not a hash with field TTLs");
    };
    let h = h.read();
    assert_eq!(h.fields.get("a").map(|v| &v[..]), Some(&b"b"[..]));
    assert!(h.expiries.contains_key("a"), "the field's TTL was lost");
}

// Under an LRU policy Redis saves each key's idle time (IDLE) in front of it,
// and under an LFU policy its access frequency (FREQ), after the key's expiry
// when it has one. The reader took those opcodes for a key's type, so no
// such file loaded (#71). These come from Redis 8.10 under allkeys-lru and
// allkeys-lfu. One key in each was restored with IDLETIME 100000 or FREQ 200,
// so that the LRU file has an idle time five bytes long. The TTLs are
// EXPIREAT 4102444800, in 2100, so that they don't run out.
#[test]
fn rdb_files_saved_by_redis_8_10_under_lru_and_lfu_policies_load() {
    let files: [(&str, &[u8], &str); 2] = [
        (
            "allkeys-lru",
            include_bytes!("fixtures/redis-8.10-lru.rdb"),
            "old",
        ),
        (
            "allkeys-lfu",
            include_bytes!("fixtures/redis-8.10-lfu.rdb"),
            "hot",
        ),
    ];
    for (policy, bytes, restored) in files {
        let (dbs, expiries, _) = kividb::snapshot::load_redis_rdb_from(bytes)
            .unwrap_or_else(|e| panic!("{policy}: {e}"));
        let db = &dbs[0];
        let mut keys: Vec<&str> = db.keys().map(String::as_str).collect();
        keys.sort_unstable();
        let mut expected = ["h", "s", "ttl", restored];
        expected.sort_unstable();
        assert_eq!(keys, expected, "{policy}");
        match &db["s"] {
            KiviDBValue::String(b) => assert_eq!(b.as_ref(), b"hello", "{policy}"),
            _ => panic!("{policy} s: wrong type"),
        }
        for key in ["ttl", restored] {
            assert_eq!(
                expiries[0].get(key),
                Some(&4_102_444_800_000),
                "{policy} {key}: the TTL was lost"
            );
        }
        assert!(!expiries[0].contains_key("s"), "{policy}");
    }
}

// A Redis cluster node saves the slot of the keys that follow (SLOT_INFO).
// This file comes from a Redis 8.10 node holding every slot, with 3 keys
// (#71).
#[test]
fn an_rdb_file_from_a_redis_8_10_cluster_node_loads() {
    let (dbs, expiries, _) = kividb::snapshot::load_redis_rdb_from(
        &include_bytes!("fixtures/redis-8.10-cluster.rdb")[..],
    )
    .unwrap();
    let mut keys: Vec<&str> = dbs[0].keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, ["s", "ttl", "{s}x"]);
    assert_eq!(expiries[0].get("ttl"), Some(&4_102_444_800_000));
    assert!(!expiries[0].contains_key("s"));
}

// IDLE and FREQ can follow either kind of expiry, and a long idle time takes
// more than one byte. A key whose expiry has passed is still dropped, and an
// expiry belongs only to the key right after it.
#[test]
fn rdb_idle_and_freq_before_keys_with_and_without_an_expiry() {
    let key = |p: &mut Vec<u8>, name: &[u8]| {
        p.push(0x00); // string
        p.extend(rdb_str(name));
        p.extend(rdb_str(b"v"));
    };
    let mut p = Vec::new();
    p.push(0xF8); // IDLE, 1000 s: a 14-bit length
    p.extend(rdb_encode_len(1000));
    key(&mut p, b"a");
    p.push(0xF9); // FREQ
    p.push(200);
    key(&mut p, b"b");
    p.push(0xFC); // expiry in milliseconds, in 2100
    p.extend(4_102_444_800_000u64.to_le_bytes());
    p.push(0xF8); // IDLE, 100000 s: a 32-bit length
    p.extend(rdb_encode_len(100_000));
    key(&mut p, b"c");
    p.push(0xFC); // an expiry long past
    p.extend(1_000u64.to_le_bytes());
    p.push(0xF9);
    p.push(5);
    key(&mut p, b"gone");
    p.push(0xFD); // expiry in seconds, in 2100
    p.extend(4_102_444_800u32.to_le_bytes());
    p.push(0xF8);
    p.extend(rdb_encode_len(0));
    key(&mut p, b"d");
    key(&mut p, b"e");

    let (dbs, expiries, _) = kividb::snapshot::load_redis_rdb_from(&rdb_file(0, p)[..]).unwrap();
    let mut keys: Vec<&str> = dbs[0].keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, ["a", "b", "c", "d", "e"]);
    assert_eq!(expiries[0].get("c"), Some(&4_102_444_800_000));
    assert_eq!(expiries[0].get("d"), Some(&4_102_444_800_000));
    for plain in ["a", "b", "e"] {
        assert!(!expiries[0].contains_key(plain), "{plain}");
    }
}

// A module that keeps data of its own writes it as aux data (MODULE_AUX).
// The search module does so for its index definitions (ft_index0) and its
// dictionaries (scdtype00): Redis 8.10 once an index or a dictionary exists,
// and Redis Stack 7.4 with neither, before and after the keys. KiviDB keeps
// its own indexes, so that data is skipped with a warning, and the keys load
// (#71). The first file has an index and a dictionary; the second is Redis
// Stack 7.4.7's with three plain keys. The TTLs are EXPIREAT 4102444800.
#[test]
fn rdb_files_with_the_search_modules_data_load_without_it() {
    let files: [(&str, &[u8], [&str; 3]); 2] = [
        (
            "Redis 8.10 with an index",
            include_bytes!("fixtures/redis-8.10-search.rdb"),
            ["doc:1", "s", "ttl"],
        ),
        (
            "Redis Stack 7.4",
            include_bytes!("fixtures/redis-stack-7.4.rdb"),
            ["h", "s", "ttl"],
        ),
    ];
    for (label, bytes, expected) in files {
        let (dbs, expiries, _) =
            kividb::snapshot::load_redis_rdb_from(bytes).unwrap_or_else(|e| panic!("{label}: {e}"));
        let mut keys: Vec<&str> = dbs[0].keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, expected, "{label}");
        assert_eq!(expiries[0].get("ttl"), Some(&4_102_444_800_000), "{label}");
    }
}

/// A module ID as Redis writes one: 9 characters of 6 bits, then a 10-bit
/// encoding version.
fn module_id(name: &str, version: u64) -> u64 {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    name.bytes().enumerate().fold(version, |id, (i, c)| {
        let index = CHARS.iter().position(|&x| x == c).unwrap() as u64;
        id | index << (64 - 6 * (i + 1))
    })
}

/// A MODULE_AUX record for `name` with one value of each kind a module can
/// save, as Redis lays one out.
fn module_aux(name: &str) -> Vec<u8> {
    let mut p = vec![0xF7, 0x81]; // MODULE_AUX, then a 64-bit length: the ID
    p.extend(module_id(name, 1).to_be_bytes());
    p.extend(rdb_encode_len(2)); // when_opcode: an unsigned integer
    p.extend(rdb_encode_len(1)); // when: before the keys
    p.extend(rdb_encode_len(1)); // a signed integer
    p.extend(rdb_encode_len(300));
    p.extend(rdb_encode_len(2)); // an unsigned integer
    p.extend(rdb_encode_len(7));
    p.extend(rdb_encode_len(3)); // a float
    p.extend(1.5f32.to_le_bytes());
    p.extend(rdb_encode_len(4)); // a double
    p.extend(2.5f64.to_le_bytes());
    p.extend(rdb_encode_len(5)); // a string
    p.extend(rdb_str(b"idx"));
    p.extend(rdb_encode_len(0)); // the end
    p
}

// Aux data of the search module is read past, wherever it is; any other
// module's is refused by its name, as Redis refuses aux data of a module it
// has not loaded (#71).
#[test]
fn rdb_module_aux_data_is_skipped_for_search_and_refused_for_other_modules() {
    let mut p = module_aux("ft_index0");
    p.push(0x00);
    p.extend(rdb_str(b"k"));
    p.extend(rdb_str(b"v"));
    p.extend(module_aux("scdtype00"));
    let (dbs, _, _) = kividb::snapshot::load_redis_rdb_from(&rdb_file(0, p)[..]).unwrap();
    assert_eq!(dbs[0].len(), 1);

    let err = kividb::snapshot::load_redis_rdb_from(&rdb_file(0, module_aux("abcdefghi"))[..])
        .err()
        .expect("another module's aux data is refused");
    assert!(err.to_string().contains("abcdefghi"), "{err}");

    // Redis writes `when` as an unsigned integer; anything else is damage.
    let mut bad = module_aux("ft_index0");
    bad[10] = 5;
    assert!(
        kividb::snapshot::load_redis_rdb_from(&rdb_file(0, bad)[..]).is_err(),
        "a bad when_opcode is refused"
    );
}

// A Redis dump.rdb keeps function libraries as an opcode and the library's
// code (FUNCTION2), one per library. The reader hands them on as the
// FUNCTION DUMP payload Redis itself would give, a version FUNCTION RESTORE
// takes and a CRC included, for the server to load as FUNCTION RESTORE does
// (#71).
// These come from Redis 7.4.11 and 8.10.2, each with two libraries.
#[test]
fn rdb_files_with_function_libraries_from_redis_7_4_and_8_10_load() {
    let files: [(&str, &[u8]); 2] = [
        ("7.4", include_bytes!("fixtures/redis-7.4-functions.rdb")),
        ("8.10", include_bytes!("fixtures/redis-8.10-functions.rdb")),
    ];
    for (label, bytes) in files {
        let (dbs, expiries, functions) = kividb::snapshot::load_redis_rdb_from(bytes)
            .unwrap_or_else(|e| panic!("Redis {label}: {e}"));
        let mut keys: Vec<&str> = dbs[0].keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, ["s", "ttl"], "Redis {label}");
        assert_eq!(
            expiries[0].get("ttl"),
            Some(&4_102_444_800_000),
            "Redis {label}"
        );

        let payload = functions.unwrap_or_else(|| panic!("Redis {label}: no libraries"));
        let text = String::from_utf8_lossy(&payload);
        for library in ["#!lua name=seven", "#!lua name=counter"] {
            assert!(text.contains(library), "Redis {label}: {library}");
        }
        let (versioned, crc) = payload.split_at(payload.len() - 8);
        assert_eq!(
            kividb::snapshot::redis_crc64(versioned).to_le_bytes(),
            crc,
            "Redis {label}: the payload's CRC"
        );
    }

    let (_, _, none) =
        kividb::snapshot::load_redis_rdb_from(&include_bytes!("fixtures/redis-8.10.rdb")[..])
            .unwrap();
    assert!(none.is_none(), "a file without libraries has none");
}

// An opcode the reader does not handle is refused by what it is, not read
// as a key's type (#71).
#[test]
fn rdb_opcodes_kividb_does_not_read_are_refused_by_name() {
    for (opcode, named) in [
        (0xF6, "pre-release"),
        (0xF3, "key metadata"),
        (0xF2, "hash templates"),
        (0xF1, "0xf1"),
        (0xF0, "0xf0"),
    ] {
        let err = kividb::snapshot::load_redis_rdb_from(&rdb_file(0, vec![opcode, 1, 2, 3])[..])
            .err()
            .expect("the opcode is refused");
        assert!(err.to_string().contains(named), "{opcode:#04x}: {err}");
    }
}
