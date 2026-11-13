// src/replication/digest.rs
//
// A fingerprint of the whole dataset: what lets a replica that already holds
// what a restarted master holds carry on from it, rather than be sent all of
// it again.
//
// A master killed (rather than stopped) and started from its AOF cannot say
// where in its old stream its dataset stands, so its replicas used to be
// sent everything. Now it fingerprints its dataset as it starts, before any
// client is served, and keeps its new stream from there. A replica whose
// dataset has the same fingerprint holds exactly that, and is continued from
// the start of the new stream. Any difference at all — a write one of them
// has and the other has not — and the fingerprints differ, and the replica
// is sent everything as before.
//
// The fingerprint is a sum over keys, so it does not depend on the order in
// which keys are stored; members of hashes and sets, whose order also
// differs from node to node, are summed the same way.

use crate::store::ValueRef;
use xxhash_rust::xxh3::{Xxh3, xxh3_64};

/// One field of a hash, with its deadline if it has one, to be summed with
/// the others: the sum is the same whatever order they are stored in.
fn field_digest(field: &[u8], value: &[u8], at: Option<crate::types::FieldDeadline>) -> u64 {
    let mut e = Xxh3::new();
    e.update(&(field.len() as u64).to_le_bytes());
    e.update(field);
    e.update(value);
    let mut sum = e.digest();
    // The deadline as a term of its own (as a key's is in
    // `dataset_digest`): appended to the value's bytes, a value ending in
    // what a deadline is laid out as could not be told from one.
    if let Some(at) = at {
        let mut t = Xxh3::new();
        t.update(b"ttl");
        t.update(&(field.len() as u64).to_le_bytes());
        t.update(field);
        t.update(&at.to_le_bytes());
        sum = sum.wrapping_add(t.digest());
    }
    sum
}

fn bytes_digest(h: &mut Xxh3, bytes: &[u8]) {
    h.update(&(bytes.len() as u64).to_le_bytes());
    h.update(bytes);
}

fn id_digest(h: &mut Xxh3, id: &crate::types::StreamId) {
    h.update(&id.ms.to_le_bytes());
    h.update(&id.seq.to_le_bytes());
}

/// A stream: its entries in order, and its groups, their consumers and the
/// producers it remembers summed, as they are kept in tables whose order
/// differs from process to process.
///
/// What its consumers were last seen and last active at is left out: a
/// consumer's times are those of whoever last ran a command as it, so an
/// AOF replay or a replica applying its master's stream sets them to its
/// own clock, and the digest changed with no data changed (#505). So is
/// when each idempotent ID was recorded, for the same reason: a replay
/// records it at its own time. Each pending entry's delivery time and count
/// are in: they travel as they are (`XCLAIM ... TIME ... RETRYCOUNT`).
fn stream_digest(h: &mut Xxh3, stream: &crate::types::StreamInner) {
    h.update(&(stream.entries.len() as u64).to_le_bytes());
    for (id, entry) in stream.entries.iter() {
        id_digest(h, &id);
        h.update(&(entry.field_count() as u64).to_le_bytes());
        for (field, value) in entry.fields() {
            bytes_digest(h, field);
            bytes_digest(h, value);
        }
    }
    id_digest(h, &stream.last_id);
    id_digest(h, &stream.max_deleted_id);
    h.update(&stream.entries_added.to_le_bytes());
    h.update(&stream.max_entries.unwrap_or(0).to_le_bytes());

    let mut groups = 0u64;
    for (name, group) in stream.groups.iter() {
        let mut g = Xxh3::new();
        bytes_digest(&mut g, name.as_bytes());
        id_digest(&mut g, &group.last_id);
        g.update(&group.entries_read.to_le_bytes());
        g.update(&(group.pel.len() as u64).to_le_bytes());
        for (id, pending) in group.pel.iter() {
            id_digest(&mut g, id);
            bytes_digest(&mut g, pending.consumer.as_bytes());
            g.update(&pending.delivered_at.to_le_bytes());
            g.update(&pending.delivery_count.to_le_bytes());
        }
        let consumers = group
            .consumers
            .keys()
            .fold(0u64, |sum, name| sum.wrapping_add(xxh3_64(name.as_bytes())));
        g.update(&(group.consumers.len() as u64).to_le_bytes());
        g.update(&consumers.to_le_bytes());
        groups = groups.wrapping_add(g.digest());
    }
    h.update(&(stream.groups.len() as u64).to_le_bytes());
    h.update(&groups.to_le_bytes());

    let idmp = &stream.idmp;
    h.update(&idmp.duration_secs.unwrap_or(0).to_le_bytes());
    h.update(&idmp.maxsize.unwrap_or(0).to_le_bytes());
    h.update(&idmp.iids_added.to_le_bytes());
    h.update(&idmp.iids_duplicates.to_le_bytes());
    let mut producers = 0u64;
    for (producer, state) in idmp.producers.iter() {
        let mut p = Xxh3::new();
        bytes_digest(&mut p, producer);
        p.update(&(state.order.len() as u64).to_le_bytes());
        for record in state.order.iter() {
            bytes_digest(&mut p, &record.iid);
            id_digest(&mut p, &record.entry_id);
        }
        producers = producers.wrapping_add(p.digest());
    }
    h.update(&(idmp.producers.len() as u64).to_le_bytes());
    h.update(&producers.to_le_bytes());
}

fn value_digest(h: &mut Xxh3, value: crate::store::ValueRef<'_>) {
    match value {
        ValueRef::String(bytes) => {
            h.update(b"s");
            match crate::commands::hyperloglog::hll_cache_range(&bytes) {
                // A HyperLogLog's cached count is not part of its value:
                // each node fills it in when it counts (#28).
                Some(cache) => {
                    h.update(&bytes[..cache.start]);
                    h.update(&bytes[cache.end..]);
                }
                None => h.update(&bytes),
            }
        }
        ValueRef::List(list) => {
            h.update(b"l");
            for item in list.read().iter() {
                h.update(&(item.len() as u64).to_le_bytes());
                h.update(item);
            }
        }
        ValueRef::Hash(hash) => {
            h.update(b"h");
            let mut sum = 0u64;
            for (field, value) in hash.read().iter() {
                sum = sum.wrapping_add(field_digest(field.as_bytes(), value, None));
            }
            h.update(&sum.to_le_bytes());
        }
        // A hash some of whose fields have a time to live: summed as a
        // plain hash is, each field with its deadline (Unix ms, as it is
        // kept and saved). Not laid out as in a snapshot: that writes the
        // deadlines in the order of their own table, which differs from
        // process to process, so the digest changed across a restart with
        // nothing else changed (#519). A field without a deadline adds
        // what it adds to a plain hash, so a hash whose last time to live
        // was taken away has one digest however it is held.
        ValueRef::HashEx(hash) => {
            h.update(b"h");
            let inner = hash.read();
            let mut sum = 0u64;
            for (field, value) in inner.fields.iter() {
                let at = inner.expiries.get(field).copied();
                sum = sum.wrapping_add(field_digest(field.as_bytes(), value, at));
            }
            h.update(&sum.to_le_bytes());
        }
        ValueRef::Set(set) => {
            h.update(b"S");
            let sum = set
                .read()
                .iter()
                .fold(0u64, |sum, member| sum.wrapping_add(xxh3_64(member)));
            h.update(&sum.to_le_bytes());
        }
        ValueRef::SortedSet(zset) => {
            h.update(b"z");
            for (score, member) in zset.read().by_score.keys() {
                h.update(&score.to_bits().to_le_bytes());
                h.update(&(member.len() as u64).to_le_bytes());
                h.update(member);
            }
        }
        ValueRef::Stream(stream) => {
            h.update(b"x");
            stream_digest(h, &stream.read());
        }
        // The rest as they are laid out in a snapshot. Where that depends on
        // a node's own order, the fingerprints differ and the replica is
        // sent everything: never wrong, only slower.
        // After the four bytes an empty key was laid out as until #441, so
        // that a value's digest is what it was, on nodes of either version.
        other => {
            let mut laid_out = 0u32.to_le_bytes().to_vec();
            if crate::snapshot::write_kdb_value(&mut laid_out, other).is_ok() {
                h.update(b"o");
                h.update(&laid_out);
            }
        }
    }
}

/// `DEBUG DIGEST-VALUE`: a digest of one value, 40 hex digits as Redis's are
/// (Redis's are SHA-1), the same for equal values on any node and whatever
/// order their members are stored in. Only the value: not the key, nor its
/// time to live.
///
/// `redis-cli --cluster fix` compares a key's two copies with it when it
/// finds the key on two nodes, and answered `OK` it failed an assertion and
/// stopped half-way (#295).
pub fn value_hex(value: ValueRef<'_>) -> String {
    let mut h = Xxh3::new();
    value_digest(&mut h, value);
    format!("{:040x}", h.digest128())
}

/// The fingerprint of the dataset. Call with nothing writing to it: at
/// startup, or on a replica between syncs.
pub fn dataset_digest(store: &crate::store::Store) -> u64 {
    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .clamp(1, 8);
    let mut total = 0u64;
    for (db_index, db) in store.iter().enumerate() {
        let shards: Vec<_> = db.iter().collect();
        let per = shards.len().div_ceil(threads).max(1);
        let keys_sum: u64 = std::thread::scope(|scope| {
            let handles: Vec<_> = shards
                .chunks(per)
                .map(|run| {
                    scope.spawn(move || {
                        let mut sum = 0u64;
                        for shard in run {
                            for entry in crate::read_lock!(shard).entries() {
                                let key = entry.key();
                                let mut h = Xxh3::new();
                                h.update(&(db_index as u64).to_le_bytes());
                                h.update(&(key.len() as u64).to_le_bytes());
                                h.update(key);
                                value_digest(&mut h, entry.value());
                                sum = sum.wrapping_add(h.digest());
                                // Its time to live, if it has one.
                                if let Some(ms) = entry.expires() {
                                    let mut h = Xxh3::new();
                                    h.update(b"ttl");
                                    h.update(&(db_index as u64).to_le_bytes());
                                    h.update(key);
                                    h.update(&ms.to_le_bytes());
                                    sum = sum.wrapping_add(h.digest());
                                }
                            }
                        }
                        sum
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap_or(0))
                .fold(0u64, u64::wrapping_add)
        });
        total = total.wrapping_add(keys_sum);
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::KiviDBValue;

    /// Equal values have one digest on any node, whatever their members'
    /// order; different values have different ones.
    #[test]
    fn a_values_digest_follows_the_value() {
        let a = crate::store::new_store(1);
        let b = crate::store::new_store(1);
        let set = |store: &crate::store::Store, k: &str, v: &str| {
            crate::write_lock!(crate::store::get_shard(&store[0], k)).insert_str(k, v.as_bytes());
        };
        set(&a, "k", "one");
        set(&b, "k", "one");
        set(&b, "other", "two");
        let hex = |store: &crate::store::Store, k: &str| {
            let shard = crate::store::get_shard(&store[0], k);
            let map = crate::read_lock!(shard);
            value_hex(map.get(k).unwrap())
        };
        assert_eq!(hex(&a, "k"), hex(&b, "k"));
        assert_ne!(hex(&b, "k"), hex(&b, "other"));
        assert_eq!(hex(&a, "k").len(), 40);
        assert!(hex(&a, "k").bytes().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn the_fingerprint_follows_the_data_not_its_order() {
        let a = crate::store::new_store(1);
        let b = crate::store::new_store(1);
        let set = |store: &crate::store::Store, k: &str, v: &str| {
            crate::write_lock!(crate::store::get_shard(&store[0], k)).insert_str(k, v.as_bytes());
        };
        for i in 0..100 {
            set(&a, &format!("k{i}"), "v");
        }
        for i in (0..100).rev() {
            set(&b, &format!("k{i}"), "v");
        }
        assert_eq!(dataset_digest(&a), dataset_digest(&b));
        set(&b, "k7", "w");
        assert_ne!(dataset_digest(&a), dataset_digest(&b));
        // A time to live is part of it.
        crate::write_lock!(crate::store::get_shard(&a[0], "k7")).insert_str("k7", b"w");
        assert_eq!(dataset_digest(&a), dataset_digest(&b));
        crate::write_lock!(crate::store::get_shard(&a[0], "k7")).set_expires("k7", 99);
        assert_ne!(dataset_digest(&a), dataset_digest(&b));
        crate::write_lock!(crate::store::get_shard(&b[0], "k7")).set_expires("k7", 99);
        assert_eq!(dataset_digest(&a), dataset_digest(&b));
    }

    /// A hash with field TTLs has one digest whatever order its fields and
    /// their deadlines are stored in: after a restart they are in another
    /// (#519). Its deadlines are part of it, and with none left it is
    /// digested as a plain hash with the same fields is.
    #[test]
    fn a_hash_with_field_ttls_is_digested_whatever_its_order() {
        use crate::types::HashInner;
        use std::sync::Arc;
        let store = crate::store::new_store(1);
        let hash_ex = |order: &mut dyn Iterator<Item = usize>, ttls: &[(usize, u64)]| {
            let order: Vec<usize> = order.collect();
            let mut inner = HashInner::new();
            for &i in &order {
                inner
                    .fields
                    .insert(format!("f{i}"), Arc::from(format!("v{i}").as_bytes()));
            }
            for &i in &order {
                if let Some(&(_, at)) = ttls.iter().find(|(f, _)| *f == i) {
                    inner.expiries.insert(format!("f{i}"), at);
                }
            }
            KiviDBValue::HashEx(Arc::new(parking_lot::RwLock::new(inner)))
        };
        let hex = |k: &str, v: KiviDBValue| {
            let shard = crate::store::get_shard(&store[0], k);
            crate::write_lock!(shard).insert(k, v);
            let map = crate::read_lock!(shard);
            value_hex(map.get(k).unwrap())
        };
        let ttls: Vec<(usize, u64)> = (0..50)
            .step_by(3)
            .map(|i| (i, 1_900_000_000_000 + i as u64))
            .collect();
        let forward = hex("a", hash_ex(&mut (0..50), &ttls));
        let backward = hex("b", hash_ex(&mut (0..50).rev(), &ttls));
        assert_eq!(forward, backward);
        // Another deadline, or one more field with one, is another value.
        let mut later = ttls.clone();
        later[0].1 += 1;
        assert_ne!(forward, hex("c", hash_ex(&mut (0..50), &later)));
        let mut more = ttls.clone();
        more.push((1, 1_900_000_000_000));
        assert_ne!(forward, hex("d", hash_ex(&mut (0..50), &more)));
        // No deadline left: as the plain hash.
        let plain: crate::types::HashFields = (0..50)
            .map(|i| (format!("f{i}"), Arc::from(format!("v{i}").as_bytes())))
            .collect();
        let plain = KiviDBValue::Hash(Arc::new(parking_lot::RwLock::new(plain)));
        assert_eq!(hex("e", hash_ex(&mut (0..50).rev(), &[])), hex("f", plain));
    }

    /// A stream has one digest whatever order its groups, consumers and
    /// producers are kept in, and whatever times its consumers were last
    /// seen at: an AOF replay sets those to its own clock (#505). Its
    /// entries, groups, pending entries and consumer names are part of it.
    #[test]
    fn a_streams_digest_leaves_out_its_consumers_times_and_its_order() {
        use crate::types::{ConsumerGroup, PelEntry, StreamId, StreamInner};
        use std::sync::Arc;
        let store = crate::store::new_store(1);
        let make = |names: &[&str], seen: u64, delivered: u64| {
            let mut s = StreamInner::new();
            for ms in 1..=5u64 {
                let value = ms.to_string();
                s.entries
                    .push_tail(StreamId::new(ms, 0), &[(&b"f"[..], value.as_bytes())]);
            }
            s.last_id = StreamId::new(5, 0);
            s.entries_added = 5;
            for name in names {
                let mut g = ConsumerGroup {
                    name: (*name).into(),
                    last_id: StreamId::new(2, 0),
                    entries_read: 2,
                    pel: Default::default(),
                    consumers: Default::default(),
                    active: Default::default(),
                };
                for c in names {
                    g.seen(format!("c-{c}").as_bytes(), seen, true);
                }
                g.pel.insert(
                    StreamId::new(1, 0),
                    PelEntry {
                        consumer: format!("c-{}", names[0]).as_str().into(),
                        delivered_at: delivered,
                        delivery_count: 1,
                    },
                );
                s.groups.insert((*name).into(), g);
            }
            KiviDBValue::Stream(Arc::new(parking_lot::RwLock::new(s)))
        };
        let hex = |k: &str, v: KiviDBValue| {
            let shard = crate::store::get_shard(&store[0], k);
            crate::write_lock!(shard).insert(k, v);
            let map = crate::read_lock!(shard);
            value_hex(map.get(k).unwrap())
        };
        let names: Vec<String> = (0..20).map(|i| format!("g{i}")).collect();
        let forward: Vec<&str> = names.iter().map(String::as_str).collect();
        let mut backward = forward.clone();
        backward.reverse();
        backward.rotate_right(1); // the same first name: it owns the pending entry
        let a = hex("a", make(&forward, 1_000, 500));
        assert_eq!(a, hex("b", make(&backward, 99_000, 500)));
        // A pending entry's delivery time is part of it, as is each group.
        assert_ne!(a, hex("c", make(&forward, 1_000, 501)));
        assert_ne!(a, hex("d", make(&forward[..19], 1_000, 500)));
    }

    /// A HyperLogLog's cached count is each node's own: two that differ only
    /// there are the same data (#28). Its registers are not.
    #[test]
    fn a_hyperloglogs_cached_count_is_not_part_of_the_fingerprint() {
        let (a, b) = (crate::store::new_store(1), crate::store::new_store(1));
        let hll = |cache: u8, register: u8| {
            let mut blob = vec![0u8; 16 + 12288];
            blob[..4].copy_from_slice(b"HYLL");
            blob[4] = 1;
            blob[8] = cache;
            blob[15] = if cache == 0 { 0x80 } else { 0 };
            blob[100] = register;
            KiviDBValue::String(crate::types::SharedBytes::from(blob.as_slice()))
        };
        let put = |store: &crate::store::Store, v: KiviDBValue| {
            crate::write_lock!(crate::store::get_shard(&store[0], "h")).insert("h", v);
        };
        put(&a, hll(0, 1));
        put(&b, hll(7, 1));
        assert_eq!(dataset_digest(&a), dataset_digest(&b));
        put(&b, hll(7, 2));
        assert_ne!(dataset_digest(&a), dataset_digest(&b));

        let sparse = |cache: u8, register: u8| {
            let mut blob = vec![0u8; 18];
            blob[..4].copy_from_slice(b"HYLL");
            blob[4] = 1;
            blob[8] = cache;
            blob[15] = if cache == 0 { 0x80 } else { 0 };
            blob[16] = 0x7f;
            blob[17] = register;
            KiviDBValue::String(crate::types::SharedBytes::from(blob.as_slice()))
        };
        put(&a, sparse(0, 0xff));
        put(&b, sparse(7, 0xff));
        assert_eq!(dataset_digest(&a), dataset_digest(&b));
        put(&b, sparse(7, 0xfe));
        assert_ne!(dataset_digest(&a), dataset_digest(&b));
    }
}
