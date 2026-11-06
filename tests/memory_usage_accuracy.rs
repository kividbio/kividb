//! `MEMORY USAGE` of sets and hashes against what their keys take in
//! `used_memory`, measured independently (see `memory_usage`).
//!
//! Each case starts a server process of its own, loads many keys of one
//! shape, and takes the growth of `used_memory` per key. The keys' average
//! `MEMORY USAGE`, with the default sample and with `SAMPLES 0`, must be
//! within the case's [`Tolerance`] of it. `KIVIDB_MEMORY_REPORT=1` prints
//! every case.

mod memory_usage;

use memory_usage::*;
use std::time::Duration;

fn fields_cmd(name: &str, key: &[u8], args: &[&str], fields: impl Iterator<Item = usize>) -> Cmd {
    let fields: Vec<Vec<u8>> = fields.map(|i| b(format!("field:{i}"))).collect();
    let mut cmd = vec![b(name), key.to_vec()];
    cmd.extend(args.iter().map(b));
    cmd.extend([b("FIELDS"), b(fields.len())]);
    cmd.extend(fields);
    cmd
}

fn set_case(name: &str, size: usize, member: fn(usize) -> Vec<u8>) -> Case {
    Case {
        name: format!("set {name}"),
        size,
        len_cmd: b"SCARD",
        make: Box::new(move |k| vec![sadd(k, (0..size).map(member))]),
        settle: None,
        tolerance: Tolerance::EXACT,
    }
}

fn hash_case(
    name: &str,
    size: usize,
    field: fn(usize) -> Vec<u8>,
    value: fn(usize) -> Vec<u8>,
) -> Case {
    Case {
        name: format!("hash {name}"),
        size,
        len_cmd: b"HLEN",
        make: Box::new(move |k| vec![hset(k, (0..size).map(|i| (field(i), value(i))))]),
        settle: None,
        tolerance: Tolerance::EXACT,
    }
}

fn short_field(i: usize) -> Vec<u8> {
    b(format!("field:{i}"))
}
fn short_value(i: usize) -> Vec<u8> {
    b(format!("v{i}"))
}

fn cases() -> Vec<Case> {
    let mut cases = Vec::new();
    for size in [1, 2, 8, 10, 32, 64, 65, 128, 512, 5000] {
        cases.push(set_case("of integers", size, b));
    }
    for size in [1, 8, 65, 512] {
        cases.push(set_case("of 16-byte strings", size, |i| {
            b(format!("member-{i:09}"))
        }));
        cases.push(set_case("of 100-byte strings", size, |i| {
            b(format!("{i:0100}"))
        }));
    }
    // Grown to 1000 and cut to 200: the table keeps its room.
    cases.push(Case {
        name: "set grown then cut".into(),
        size: 200,
        len_cmd: b"SCARD",
        make: Box::new(|k| {
            let mut srem = vec![b("SREM"), k.to_vec()];
            srem.extend((200..1000).map(b));
            vec![sadd(k, (0..1000).map(b)), srem]
        }),
        settle: None,
        tolerance: Tolerance::EXACT,
    });

    for size in [1, 2, 8, 10, 32, 64, 65, 128, 512] {
        cases.push(hash_case("short", size, short_field, short_value));
    }
    for size in [10, 65] {
        cases.push(hash_case(
            "of long fields",
            size,
            |i| b(format!("{i:064}")),
            short_value,
        ));
        cases.push(hash_case("of long values", size, short_field, |i| {
            b(format!("{i:01000}"))
        }));
        cases.push(hash_case("of binary values", size, short_field, |i| {
            (0..=255u8).cycle().skip(i % 256).take(48).collect()
        }));
    }
    // Values written over with longer and shorter ones.
    cases.push(Case {
        name: "hash values replaced".into(),
        size: 65,
        len_cmd: b"HLEN",
        make: Box::new(|k| {
            vec![
                hset(k, (0..65).map(|i| (short_field(i), b(format!("{i:040}"))))),
                hset(k, (0..65).map(|i| (short_field(i), short_value(i)))),
            ]
        }),
        settle: None,
        tolerance: Tolerance::EXACT,
    });
    // 128 fields cut to 65: the map keeps its room.
    cases.push(Case {
        name: "hash grown then cut".into(),
        size: 65,
        len_cmd: b"HLEN",
        make: Box::new(|k| {
            let mut hdel = vec![b("HDEL"), k.to_vec()];
            hdel.extend((65..128).map(short_field));
            vec![
                hset(k, (0..128).map(|i| (short_field(i), short_value(i)))),
                hdel,
            ]
        }),
        settle: None,
        tolerance: Tolerance::EXACT.after_deletions(0.06),
    });

    // 512 fields cut to 65.
    cases.push(Case {
        name: "hash mostly deleted".into(),
        size: 65,
        len_cmd: b"HLEN",
        make: Box::new(|k| {
            let mut hdel = vec![b("HDEL"), k.to_vec()];
            hdel.extend((65..512).map(short_field));
            vec![
                hset(k, (0..512).map(|i| (short_field(i), short_value(i)))),
                hdel,
            ]
        }),
        settle: None,
        tolerance: Tolerance::EXACT.after_deletions(0.10),
    });
    // Field TTLs.
    let ttl_case = |name: &str, size: usize, more: fn(&[u8], usize) -> Vec<Cmd>| Case {
        name: format!("hash with TTLs: {name}"),
        size,
        len_cmd: b"HLEN",
        make: Box::new(move |k| {
            let mut cmds = vec![hset(k, (0..size).map(|i| (short_field(i), short_value(i))))];
            cmds.extend(more(k, size));
            cmds
        }),
        settle: None,
        tolerance: Tolerance::EXACT.with_field_ttls(),
    };
    for size in [1, 8, 65, 512] {
        cases.push(ttl_case("every field", size, |k, n| {
            vec![fields_cmd("HPEXPIRE", k, &["3600000"], 0..n)]
        }));
    }
    cases.push(ttl_case("a few fields", 65, |k, _| {
        vec![fields_cmd("HPEXPIRE", k, &["3600000"], 0..3)]
    }));
    cases.push(ttl_case("half the fields", 65, |k, _| {
        vec![fields_cmd("HPEXPIRE", k, &["3600000"], (0..65).step_by(2))]
    }));
    cases.push(ttl_case("extended", 65, |k, n| {
        vec![
            fields_cmd("HPEXPIRE", k, &["3600000"], 0..n),
            fields_cmd("HPEXPIRE", k, &["7200000", "GT"], 0..n),
        ]
    }));
    cases.push(ttl_case("replaced five times", 65, |k, n| {
        (0..5)
            .map(|t| fields_cmd("HPEXPIRE", k, &[&(3_600_000 + t * 1000).to_string()], 0..n))
            .collect()
    }));
    cases.push(ttl_case("half persisted", 65, |k, n| {
        vec![
            fields_cmd("HPEXPIRE", k, &["3600000"], 0..n),
            fields_cmd("HPERSIST", k, &[], 0..n / 2),
        ]
    }));
    // Half the fields expire and are swept: 130 fields, 65 left.
    cases.push(Case {
        name: "hash with TTLs: half expired and swept".into(),
        size: 65,
        len_cmd: b"HLEN",
        make: Box::new(|k| {
            vec![
                hset(k, (0..130).map(|i| (short_field(i), short_value(i)))),
                fields_cmd("HPEXPIRE", k, &["3600000"], 0..65),
                fields_cmd("HPEXPIRE", k, &["50"], 65..130),
            ]
        }),
        settle: Some(Box::new(|c, keys| {
            // Past their time; reading a whole hash removes its fields that
            // are, as the background sweep does.
            std::thread::sleep(Duration::from_millis(200));
            let reads: Vec<Cmd> = keys.iter().map(|k| vec![b("HGETALL"), k.clone()]).collect();
            c.pipeline(&reads);
        })),
        tolerance: Tolerance::EXACT.with_field_ttls().after_deletions(0.05),
    });
    cases
}

#[test]
fn memory_usage_of_sets_and_hashes_is_what_their_keys_take() {
    check(cases());
}

/// `MEMORY USAGE` of a key that is not there is nil, of an empty-made key
/// there is none (an emptied collection is deleted), and `SAMPLES` takes a
/// count of at least 0.
#[test]
fn samples_are_read_as_redis_reads_them() {
    let server = Server::start();
    let mut c = server.connect();
    assert!(matches!(
        c.cmd(&[b"MEMORY", b"USAGE", b"none"]),
        Reply::Bulk(None)
    ));
    c.int(&[b"SADD", b"s", b"a"]);
    c.int(&[b"SREM", b"s", b"a"]);
    assert!(matches!(
        c.cmd(&[b"MEMORY", b"USAGE", b"s"]),
        Reply::Bulk(None)
    ));

    // A set of 1000 members of one length: any sample is exact, and a larger
    // sample than the set is all of it.
    let members: Vec<Vec<u8>> = (0..1000).map(|i| b(format!("m{i:05}"))).collect();
    c.pipeline(&[sadd(b"s", members.into_iter())]);
    let usage = |c: &mut Conn, samples: Option<&[u8]>| -> i64 {
        let mut cmd: Vec<&[u8]> = vec![b"MEMORY", b"USAGE", b"s"];
        if let Some(n) = samples {
            cmd.extend([&b"SAMPLES"[..], n]);
        }
        c.int(&cmd)
    };
    let default = usage(&mut c, None);
    for n in [&b"0"[..], b"1", b"5", b"999", b"1000", b"100000"] {
        assert_eq!(
            usage(&mut c, Some(n)),
            default,
            "SAMPLES {}",
            String::from_utf8_lossy(n)
        );
    }
    for bad in [&b"-1"[..], b"x", b""] {
        assert!(matches!(
            c.cmd(&[b"MEMORY", b"USAGE", b"s", b"SAMPLES", bad]),
            Reply::Error(_)
        ));
    }
    assert!(matches!(
        c.cmd(&[b"MEMORY", b"USAGE", b"s", b"SAMPLES"]),
        Reply::Error(_)
    ));
}

/// A key's estimate counts a share of its shard's table, at most a
/// half-full table's (`ShardMap::entry_footprint`). It is between a full
/// table's (a bucket and its control byte, 8/7 of them a key: about 10
/// bytes) and a half-full one's (about 21), and at most 52 for a key alone
/// in the smallest table (4 buckets): it moves by at most 42 bytes as other
/// keys come and go. Without the cap, a 65-member set read 13,088 bytes
/// instead of 3,875 after a million other keys were deleted.
const SHARE_MOVES: i64 = 64;

/// Sets and hashes keep their `MEMORY USAGE` while many other keys are
/// added and deleted, before and after the shards' tables are shrunk, and
/// `used_memory` gives the deleted keys back.
#[test]
fn memory_usage_of_a_key_stays_put_when_other_keys_come_and_go() {
    let server = Server::start();
    let mut c = server.connect();
    let names: Vec<Vec<u8>> = (0..64)
        .flat_map(|i| [b(format!("set:{i}")), b(format!("hash:{i}"))])
        .collect();
    let load: Vec<Cmd> = names
        .iter()
        .map(|k| {
            if k.starts_with(b"set") {
                sadd(k, (0..65).map(b))
            } else {
                hset(k, (0..65).map(|i| (short_field(i), short_value(i))))
            }
        })
        .collect();
    c.pipeline(&load);
    let usage = |c: &mut Conn| -> Vec<i64> {
        let cmds: Vec<Cmd> = names
            .iter()
            .map(|k| vec![b("MEMORY"), b("USAGE"), k.clone(), b("SAMPLES"), b("0")])
            .collect();
        c.pipeline(&cmds)
            .into_iter()
            .map(|r| match r {
                Reply::Int(n) => n,
                other => panic!("MEMORY USAGE: {other:?}"),
            })
            .collect()
    };
    let at_first = usage(&mut c);
    let stays_put = |when: &str, now: &[i64]| {
        for ((k, a), n) in names.iter().zip(&at_first).zip(now) {
            assert!(
                (n - a).abs() <= SHARE_MOVES,
                "{when}: {} went from {a} to {n}",
                String::from_utf8_lossy(k)
            );
        }
    };

    // Enough other keys that every shard's table grows many times over.
    let others: Vec<Vec<u8>> = (0..200_000).map(|i| b(format!("other:{i}"))).collect();
    let before = c.settled_used_memory();
    let sets: Vec<Cmd> = others
        .iter()
        .map(|k| vec![b("SET"), k.clone(), b("v")])
        .collect();
    c.pipeline(&sets);
    let grown = c.settled_used_memory();
    assert!(grown > before + 8_000_000, "{before} to {grown}");
    stays_put("with the other keys", &usage(&mut c));

    // Deleted: their entries are given back at once, the tables' room
    // stays until the shards are shrunk.
    let dels: Vec<Cmd> = others.iter().map(|k| vec![b("DEL"), k.clone()]).collect();
    c.pipeline(&dels);
    let deleted = c.used_memory();
    assert!(
        grown - deleted > (grown - before) / 2,
        "deleting gave back {} of {}",
        grown - deleted,
        grown - before
    );
    stays_put(
        "the other keys deleted, the tables not shrunk",
        &usage(&mut c),
    );

    // The shards are shrunk in the background, a sweep a second: waited for
    // (fifteen seconds at most) until `used_memory` is lower and has not
    // moved for two sweeps. Not all the room need come back: a sweep in the
    // middle of the deletions shrinks a table to the keys it has then, and a
    // table under the minimum size is not shrunk again.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let (mut last, mut since) = (c.used_memory(), std::time::Instant::now());
    let shrunk = loop {
        std::thread::sleep(Duration::from_millis(100));
        let now = c.used_memory();
        if now != last {
            (last, since) = (now, std::time::Instant::now());
        } else if now < deleted && since.elapsed() >= Duration::from_millis(2500) {
            break now;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "tables not shrunk: {now}, {deleted} after deleting the other keys"
        );
    };
    assert!(
        shrunk - before < (grown - before) / 4,
        "{shrunk} after shrinking, {before} before the other keys, {grown} with them"
    );
    stays_put("the tables shrunk", &usage(&mut c));

    // Deleting the keys themselves gives back what they report, less their
    // shares of the tables (which stay): within 2%.
    let reported: i64 = usage(&mut c).iter().sum();
    let held = c.settled_used_memory();
    let dels: Vec<Cmd> = names.iter().map(|k| vec![b("DEL"), k.clone()]).collect();
    c.pipeline(&dels);
    let freed = (held - c.settled_used_memory()) as i64;
    assert!(
        (freed - reported).abs() <= reported / 50,
        "deleting the keys gave back {freed}, they reported {reported}"
    );
}
