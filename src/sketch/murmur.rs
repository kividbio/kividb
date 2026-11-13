//! MurmurHash2 (32-bit) and MurmurHash64A, written from Austin Appleby's
//! public-domain algorithms.
//!
//! The probabilistic types hash their items with these, with the seeds
//! Redis uses, so that a filter made by Redis answers the same here and
//! `SCANDUMP` gives the same bytes. Which seed goes where was found by
//! checking the bits Redis set (see `bloom.rs`).

const M64: u64 = 0xc6a4_a793_5bd1_e995;
const R64: u32 = 47;

/// MurmurHash64A of `data` with `seed`.
pub fn murmur64a(data: &[u8], seed: u64) -> u64 {
    let mut h = seed ^ (data.len() as u64).wrapping_mul(M64);
    let (blocks, tail) = data.as_chunks::<8>();
    for block in blocks {
        let mut k = u64::from_le_bytes(*block);
        k = k.wrapping_mul(M64);
        k ^= k >> R64;
        k = k.wrapping_mul(M64);
        h ^= k;
        h = h.wrapping_mul(M64);
    }
    if !tail.is_empty() {
        for (i, &byte) in tail.iter().enumerate() {
            h ^= u64::from(byte) << (8 * i);
        }
        h = h.wrapping_mul(M64);
    }
    h ^= h >> R64;
    h = h.wrapping_mul(M64);
    h ^= h >> R64;
    h
}

/// MurmurHash2, 32 bits, of `data` with `seed`. The Count-min sketch and
/// Top-K hash with it; nothing uses it yet.
#[allow(dead_code)]
pub fn murmur2_32(data: &[u8], seed: u32) -> u32 {
    const M: u32 = 0x5bd1_e995;
    let mut h = seed ^ data.len() as u32;
    let (blocks, tail) = data.as_chunks::<4>();
    for block in blocks {
        let mut k = u32::from_le_bytes(*block);
        k = k.wrapping_mul(M);
        k ^= k >> 24;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    if !tail.is_empty() {
        for (i, &byte) in tail.iter().enumerate() {
            h ^= u32::from(byte) << (8 * i);
        }
        h = h.wrapping_mul(M);
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Values Redis's placement implies (checked against the bits it set
    /// and the fingerprints it stored; see the design's transcripts).
    #[test]
    fn known_values() {
        // The Cuckoo filter's hash of "" is 0: fingerprint 1, bucket 0.
        assert_eq!(murmur64a(b"", 0), 0);
        // Top-K fingerprints: seed 1919.
        assert_eq!(murmur2_32(b"a", 1919), 0xf23c_f517);
        assert_eq!(murmur2_32(b"b", 1919), 0x2adf_ed97);
        assert_eq!(murmur2_32(b"c", 1919), 0xaea5_de10);
        // Count-min columns, width 4, rows 0 and 1.
        assert_eq!(murmur2_32(b"a", 0) % 4, 2);
        assert_eq!(murmur2_32(b"a", 1) % 4, 0);
        assert_eq!(murmur2_32(b"b", 0) % 4, 0);
        assert_eq!(murmur2_32(b"b", 1) % 4, 1);
    }

    #[test]
    fn every_tail_length() {
        // Different lengths, and the tail bytes count: no two collide.
        let data = b"0123456789abcdef";
        let hashes: Vec<u64> = (0..=data.len()).map(|n| murmur64a(&data[..n], 7)).collect();
        let mut sorted = hashes.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), hashes.len());
    }
}
