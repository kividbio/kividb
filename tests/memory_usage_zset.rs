//! `MEMORY USAGE` of sorted sets against what their keys take in
//! `used_memory`, measured independently (see `memory_usage`): sizes, the
//! order members come in (which sets how full the score index's nodes are),
//! scores changed, and members removed.

mod memory_usage;

use memory_usage::*;

/// A sorted set's estimate is exact but for its score index, whose nodes
/// are counted from how many keys it has, not seen (`score_index`): up to
/// eleven members it is one leaf, exact; past that the index is about a
/// third of the key and can be a leaf off ([`Tolerance::ZSET`]).
fn tolerance(size: usize) -> Tolerance {
    match size {
        0..=11 => Tolerance::EXACT,
        _ => Tolerance::ZSET,
    }
}

fn zadd(key: &[u8], pairs: impl Iterator<Item = (f64, Vec<u8>)>) -> Cmd {
    let mut cmd = vec![b("ZADD"), key.to_vec()];
    for (score, member) in pairs {
        cmd.push(b(score));
        cmd.push(member);
    }
    cmd
}

fn member(i: usize) -> Vec<u8> {
    b(format!("member-{i:09}"))
}

/// A score that comes in no order: a multiplicative hash of `i`.
fn scattered(i: usize) -> f64 {
    ((i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40) as f64
}

fn case(name: &str, size: usize, make: Make) -> Case {
    Case {
        name: format!("zset {name}"),
        size,
        len_cmd: b"ZCARD",
        make,
        settle: None,
        tolerance: tolerance(size),
    }
}

fn cases() -> Vec<Case> {
    let mut cases = Vec::new();
    for size in [1, 8, 11, 12, 20, 65, 128, 512, 5000] {
        cases.push(case(
            "ascending scores",
            size,
            Box::new(move |k| vec![zadd(k, (0..size).map(|i| (i as f64, member(i))))]),
        ));
        cases.push(case(
            "descending scores",
            size,
            Box::new(move |k| vec![zadd(k, (0..size).rev().map(|i| (i as f64, member(i))))]),
        ));
        cases.push(case(
            "scattered scores",
            size,
            Box::new(move |k| vec![zadd(k, (0..size).map(|i| (scattered(i), member(i))))]),
        ));
        // Added one member a command, as most writers add them.
        cases.push(case(
            "scattered, one a command",
            size,
            Box::new(move |k| {
                (0..size)
                    .map(|i| zadd(k, std::iter::once((scattered(i), member(i)))))
                    .collect()
            }),
        ));
    }
    for size in [8, 65, 512] {
        cases.push(case(
            "equal scores",
            size,
            Box::new(move |k| vec![zadd(k, (0..size).map(|i| (1.0, member(i))))]),
        ));
        // Every score changed once: the member map keeps its blocks and the
        // score index takes the command's.
        cases.push(case(
            "every score changed",
            size,
            Box::new(move |k| {
                vec![
                    zadd(k, (0..size).map(|i| (i as f64, member(i)))),
                    zadd(k, (0..size).map(|i| (scattered(i), member(i)))),
                ]
            }),
        ));
        cases.push(case(
            "half the scores changed",
            size,
            Box::new(move |k| {
                vec![
                    zadd(k, (0..size).map(|i| (i as f64, member(i)))),
                    zadd(k, (0..size).step_by(2).map(|i| (scattered(i), member(i)))),
                ]
            }),
        ));
    }
    // Members removed: 130 to 65, and 512 to 65.
    for from in [130, 512] {
        cases.push(case(
            &format!("cut from {from}"),
            65,
            Box::new(move |k| {
                let mut zrem = vec![b("ZREM"), k.to_vec()];
                zrem.extend((65..from).map(member));
                vec![zadd(k, (0..from).map(|i| (scattered(i), member(i)))), zrem]
            }),
        ));
    }
    cases
}

#[test]
fn memory_usage_of_sorted_sets_is_what_their_keys_take() {
    check(cases());
}

fn usage_of(c: &mut Conn, keys: &[Vec<u8>], samples: Option<&str>) -> Vec<i64> {
    let cmds: Vec<Cmd> = keys
        .iter()
        .map(|k| {
            let mut cmd = vec![b("MEMORY"), b("USAGE"), k.clone()];
            if let Some(n) = samples {
                cmd.extend([b("SAMPLES"), b(n)]);
            }
            cmd
        })
        .collect();
    c.pipeline(&cmds)
        .into_iter()
        .map(|r| match r {
            Reply::Int(n) => n,
            other => panic!("MEMORY USAGE: {other:?}"),
        })
        .collect()
}

/// Deleting sorted sets gives back what they report (less their shares of
/// the shards' tables, which stay), and making them again takes what they
/// took the first time: nothing of them is left behind or counted twice.
#[test]
fn deleting_and_making_again_sorted_sets_gives_back_what_they_report() {
    let server = Server::start();
    let mut c = server.connect();
    let keys: Vec<Vec<u8>> = (0..2000).map(|i| b(format!("zset:{i}"))).collect();
    let make: Vec<Cmd> = keys
        .iter()
        .enumerate()
        .map(|(n, k)| {
            // Half in order, half at random; a few rescored.
            let score = |i: usize| if n % 2 == 0 { i as f64 } else { scattered(i) };
            zadd(k, (0..65).map(|i| (score(i), member(i))))
        })
        .chain(keys.iter().map(|k| {
            zadd(
                k,
                (0..65).step_by(8).map(|i| (scattered(i) + 0.5, member(i))),
            )
        }))
        .collect();
    c.ok(&[b"SELECT", b"1"]);
    c.pipeline(&make[..10]);
    c.ok(&[b"SELECT", b"0"]);

    let empty = c.settled_used_memory();
    c.pipeline(&make);
    let full = c.settled_used_memory();
    let reported: i64 = usage_of(&mut c, &keys, Some("0")).iter().sum();
    let took = (full - empty) as i64;
    assert!(
        (reported - took).abs() <= took / 10,
        "{} sorted sets took {took} and report {reported}",
        keys.len()
    );

    let dels: Vec<Cmd> = keys.iter().map(|k| vec![b("DEL"), k.clone()]).collect();
    c.pipeline(&dels);
    let freed = (full - c.settled_used_memory()) as i64;
    assert!(
        (freed - reported).abs() <= reported / 10,
        "deleting them gave back {freed}, they reported {reported}"
    );
    // Made again, they take what they took.
    c.pipeline(&make);
    let again = (c.settled_used_memory() - (full - freed as u64)) as i64;
    assert!(
        (again - took).abs() <= took / 100,
        "made again they took {again}, the first time {took}"
    );
}

/// A sorted set's estimate samples its members as a set's does: members of
/// one length give the same estimate from any sample.
#[test]
fn a_sorted_set_is_sampled_as_a_set_is() {
    let server = Server::start();
    let mut c = server.connect();
    c.pipeline(&[zadd(b"z", (0..1000).map(|i| (scattered(i), member(i))))]);
    let key = [b("z")];
    let all = usage_of(&mut c, &key, Some("0"))[0];
    assert_eq!(usage_of(&mut c, &key, None)[0], all);
    for n in ["1", "5", "999", "1000", "100000"] {
        assert_eq!(usage_of(&mut c, &key, Some(n))[0], all, "SAMPLES {n}");
    }
}
