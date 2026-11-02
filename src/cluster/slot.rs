// src/cluster/slot.rs
//
// Redis Cluster hash slot computation.
//
// Slot = CRC16/XMODEM(key_or_hashtag) % 16384
//
// CRC16/XMODEM parameters: poly 0x1021, init 0x0000, no reflect, no XOR-out.
// This is NOT the same as CRC-CCITT-FALSE (which has init 0xFFFF).
//
// Hash-tag rule (from Redis src/cluster.c keyHashSlot):
//   1. Find the first '{'.
//   2. Find the first '}' AFTER that '{'.
//   3. If both exist AND there is at least one byte between them,
//      hash only the bytes between the braces.
//   4. Otherwise, hash the whole key.

/// Total number of hash slots in a Redis cluster. Fixed by protocol.
pub const CLUSTER_SLOTS: usize = 16384;

/// CRC16/XMODEM lookup table — 256 entries, generated at compile time.
///
/// Why a const fn instead of a literal table:
///   - No `lazy_static` or `OnceCell` needed; the table is in .rodata.
///   - You can read the algorithm and verify the table at the same time,
///     instead of trusting a 256-entry magic number list.
const CRC16_TABLE: [u16; 256] = {
    let mut table = [0u16; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc: u16 = (i as u16) << 8;
        let mut j = 0;
        while j < 8 {
            if (crc & 0x8000) != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

/// CRC16/XMODEM over a byte slice. Pure function, no allocation.
#[inline]
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in data {
        let idx = ((crc >> 8) as u8 ^ b) as usize;
        crc = (crc << 8) ^ CRC16_TABLE[idx];
    }
    crc
}

/// Extract the hash-tag substring from a key, if any.
/// Returns the slice between the first `{` and the first `}` after it,
/// PROVIDED that slice is non-empty. Otherwise returns the full key.
#[inline]
fn hash_tag(key: &[u8]) -> &[u8] {
    // Find first '{'
    let open = match key.iter().position(|&b| b == b'{') {
        Some(p) => p,
        None => return key,
    };
    // Find first '}' AFTER the '{'
    let close_rel = match key[open + 1..].iter().position(|&b| b == b'}') {
        Some(p) => p,
        None => return key,
    };
    // Empty tag {} -> hash the whole key
    if close_rel == 0 {
        return key;
    }
    &key[open + 1..open + 1 + close_rel]
}

/// Compute the cluster hash slot for a key. Always in range 0..CLUSTER_SLOTS.
#[inline]
pub fn key_hash_slot(key: &[u8]) -> u16 {
    crc16(hash_tag(key)) % (CLUSTER_SLOTS as u16)
}

// ── Tests ─────────────────────────────────────────────────────────────────────
//
// Test vectors are taken directly from Redis itself. If any of these fail,
// DO NOT proceed — every other cluster feature depends on this being correct.

#[cfg(test)]
mod tests {
    use super::*;

    // The single ground-truth vector everyone agrees on: crc16("foo") % 16384 = 12182.
    // This is documented in Redis's own source and reproduced by every cluster client.
    // If this fails, the CRC algorithm is wrong.
    #[test]
    fn key_hash_slot_canonical_foo() {
        assert_eq!(key_hash_slot(b"foo"), 12182);
    }

    #[test]
    fn empty_key_hashes_to_zero() {
        // crc16("") = 0, 0 % 16384 = 0
        assert_eq!(key_hash_slot(b""), 0);
        assert_eq!(crc16(b""), 0);
    }

    #[test]
    fn slot_is_always_in_range() {
        for k in [
            b"a".as_slice(),
            b"abcdef",
            b"{x}",
            b"{}",
            b"verylongkeyname",
            b"{user1000}.following",
            b"{user1000}.followers",
            b"hello world",
        ] {
            assert!(key_hash_slot(k) < CLUSTER_SLOTS as u16);
        }
    }

    #[test]
    fn hash_tag_basic() {
        assert_eq!(hash_tag(b"foo"), b"foo");
        assert_eq!(hash_tag(b"foo{bar}baz"), b"bar");
        assert_eq!(hash_tag(b"{user1000}.following"), b"user1000");
        // Empty tag falls back to full key
        assert_eq!(hash_tag(b"foo{}bar"), b"foo{}bar");
        // Unmatched braces fall back to full key
        assert_eq!(hash_tag(b"foo{bar"), b"foo{bar");
        assert_eq!(hash_tag(b"foo}bar"), b"foo}bar");
        // First {} wins, even if a later {tag} exists
        assert_eq!(hash_tag(b"{}{tag}"), b"{}{tag}");
        // Only the FIRST '{' and the FIRST '}' after it matter
        assert_eq!(hash_tag(b"{first}{second}"), b"first");
    }

    #[test]
    fn hash_tags_collocate() {
        // The defining property of hash tags: any two keys with the same
        // tag MUST hash to the same slot. We don't care WHICH slot — only
        // that they agree.
        let a = key_hash_slot(b"{order:42}.items");
        let b = key_hash_slot(b"{order:42}.total");
        let c = key_hash_slot(b"{order:42}");
        assert_eq!(a, b);
        assert_eq!(b, c);
    }

    #[test]
    fn empty_tag_does_not_collocate() {
        // {}foo and {}bar both have empty tags, so they hash by their full
        // keys, which differ. Almost certainly different slots.
        let a = key_hash_slot(b"{}foo");
        let b = key_hash_slot(b"{}bar");
        assert_ne!(a, b);
    }

    #[test]
    fn different_keys_usually_different_slots() {
        // 16384 slots, so over a small number of distinct keys we expect
        // distinct slots with overwhelming probability. This is a sanity
        // check that the hash isn't degenerate.
        let slots: std::collections::HashSet<u16> = (0..100)
            .map(|i| key_hash_slot(format!("key:{i}").as_bytes()))
            .collect();
        // Birthday-paradox math: 100 keys into 16384 buckets, expected
        // collisions ≈ 0.3. Allow some, but most should be distinct.
        assert!(slots.len() > 90, "got only {} distinct slots", slots.len());
    }
}
