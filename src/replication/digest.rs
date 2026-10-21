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

use crate::types::KiviDBValue;
use xxhash_rust::xxh3::{Xxh3, xxh3_64};

/// Where a HyperLogLog keeps its cached cardinality, if `bytes` is one.
fn hll_cache(bytes: &[u8]) -> Option<std::ops::Range<usize>> {
    (bytes.len() == 16 + 12288 && bytes.starts_with(b"HYLL")).then_some(8..16)
}

fn value_digest(h: &mut Xxh3, value: &KiviDBValue) {
    match value {
        KiviDBValue::String(bytes) => {
            h.update(b"s");
            match hll_cache(bytes) {
                // A HyperLogLog's cached count is not part of its value:
                // each node fills it in when it counts (#28).
                Some(cache) => {
                    h.update(&bytes[..cache.start]);
                    h.update(&bytes[cache.end..]);
                }
                None => h.update(bytes),
            }
        }
        KiviDBValue::List(list) => {
            h.update(b"l");
            for item in list.read().iter() {
                h.update(&(item.len() as u64).to_le_bytes());
                h.update(item);
            }
        }
        KiviDBValue::Hash(hash) => {
            h.update(b"h");
            let mut sum = 0u64;
            for (field, value) in hash.read().iter() {
                let mut e = Xxh3::new();
                e.update(&(field.len() as u64).to_le_bytes());
                e.update(field.as_bytes());
                e.update(value);
                sum = sum.wrapping_add(e.digest());
            }
            h.update(&sum.to_le_bytes());
        }
        KiviDBValue::Set(set) => {
            h.update(b"S");
            let sum = set
                .read()
                .iter()
                .fold(0u64, |sum, member| sum.wrapping_add(xxh3_64(member)));
            h.update(&sum.to_le_bytes());
        }
        KiviDBValue::SortedSet(zset) => {
            h.update(b"z");
            for (score, member) in zset.read().by_score.keys() {
                h.update(&score.to_bits().to_le_bytes());
                h.update(&(member.len() as u64).to_le_bytes());
                h.update(member);
            }
        }
        // The rest as they are laid out in a snapshot. Where that depends on
        // a node's own order, the fingerprints differ and the replica is
        // sent everything: never wrong, only slower.
        other => {
            let mut laid_out = Vec::new();
            if crate::snapshot::write_kdb_entry(&mut laid_out, "", other).is_ok() {
                h.update(b"o");
                h.update(&laid_out);
            }
        }
    }
}

/// The fingerprint of the dataset. Call with nothing writing to it: at
/// startup, or on a replica between syncs.
pub fn dataset_digest(
    store: &crate::store::Store,
    expiry_store: &crate::store::ExpiryStore,
) -> u64 {
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
                            for (key, value) in crate::read_lock!(shard).iter() {
                                let mut h = Xxh3::new();
                                h.update(&(db_index as u64).to_le_bytes());
                                h.update(&(key.len() as u64).to_le_bytes());
                                h.update(key.as_bytes());
                                value_digest(&mut h, value);
                                sum = sum.wrapping_add(h.digest());
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

        // Times to live, of keys that exist.
        if let Some(exp_db) = expiry_store.get(db_index) {
            for shard in exp_db.iter() {
                let entries: Vec<(String, u64)> = crate::read_lock!(shard)
                    .iter()
                    .map(|(key, at)| {
                        let ms = at
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        (key.to_string(), ms)
                    })
                    .collect();
                for (key, ms) in entries {
                    if crate::read_lock!(crate::store::get_shard(db, &key))
                        .contains_key(key.as_str())
                    {
                        let mut h = Xxh3::new();
                        h.update(b"ttl");
                        h.update(&(db_index as u64).to_le_bytes());
                        h.update(key.as_bytes());
                        h.update(&ms.to_le_bytes());
                        total = total.wrapping_add(h.digest());
                    }
                }
            }
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_fingerprint_follows_the_data_not_its_order() {
        let a = crate::store::new_store(1);
        let b = crate::store::new_store(1);
        let ea = crate::store::new_expiry_store(1);
        let eb = crate::store::new_expiry_store(1);
        let set = |store: &crate::store::Store, k: &str, v: &str| {
            crate::write_lock!(crate::store::get_shard(&store[0], k)).insert(
                k.into(),
                KiviDBValue::String(crate::types::SharedBytes::from(v.as_bytes())),
            );
        };
        for i in 0..100 {
            set(&a, &format!("k{i}"), "v");
        }
        for i in (0..100).rev() {
            set(&b, &format!("k{i}"), "v");
        }
        assert_eq!(dataset_digest(&a, &ea), dataset_digest(&b, &eb));
        set(&b, "k7", "w");
        assert_ne!(dataset_digest(&a, &ea), dataset_digest(&b, &eb));
    }

    /// A HyperLogLog's cached count is each node's own: two that differ only
    /// there are the same data (#28). Its registers are not.
    #[test]
    fn a_hyperloglogs_cached_count_is_not_part_of_the_fingerprint() {
        let (a, b) = (crate::store::new_store(1), crate::store::new_store(1));
        let (ea, eb) = (
            crate::store::new_expiry_store(1),
            crate::store::new_expiry_store(1),
        );
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
            crate::write_lock!(crate::store::get_shard(&store[0], "h")).insert("h".into(), v);
        };
        put(&a, hll(0, 1));
        put(&b, hll(7, 1));
        assert_eq!(dataset_digest(&a, &ea), dataset_digest(&b, &eb));
        put(&b, hll(7, 2));
        assert_ne!(dataset_digest(&a, &ea), dataset_digest(&b, &eb));
    }
}
