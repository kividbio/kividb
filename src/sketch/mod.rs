//! Probabilistic data types, compatible with Redis 8's built-in ones
//! (formerly RedisBloom): Bloom filters so far; Cuckoo filters, Count-min
//! sketches, Top-K and t-digests follow (#453).
//!
//! One value variant holds them all (`KiviDBValue::Sketch`), so that code
//! that treats every value alike — snapshots, `COPY`, `MEMORY USAGE`,
//! lazy freeing — has one arm for them, and the type is told apart here.
//!
//! They are stored three ways:
//!
//! * in KiviDB's snapshots, under [`TAG_SKETCH`], as a kind, a version of
//!   that kind's encoding, and the encoding (see each type's `write_kdb`);
//! * in Redis's RDB format for `DUMP` (and so `MIGRATE`), and when read
//!   from Redis: see [`redis_module`];
//! * as `BF.SCANDUMP` chunks, which are Redis's too: see [`bloom`].

pub mod bloom;
pub mod murmur;
pub mod redis_module;

use std::io;

/// The snapshot tag of every probabilistic value. Far from the run of the
/// other tags (1 to 15), so that new tags for other types can be numbered
/// on without meeting it.
pub const TAG_SKETCH: u8 = 40;

/// The kinds under [`TAG_SKETCH`], and the version of each one's encoding.
const KIND_BLOOM: u8 = 1;
const BLOOM_VERSION: u8 = 1;

/// A probabilistic value.
#[derive(Clone, Debug, PartialEq)]
pub enum Sketch {
    Bloom(bloom::BloomChain),
}

impl Sketch {
    /// What `TYPE` says: Redis's module type name.
    pub fn type_name(&self) -> &'static str {
        match self {
            Sketch::Bloom(_) => redis_module::BLOOM_TYPE,
        }
    }

    /// What the value takes in memory beyond the `Sketch` itself.
    pub fn memory_bytes(&self) -> usize {
        match self {
            Sketch::Bloom(chain) => chain.memory_bytes(),
        }
    }

    /// How much work freeing it is, in `lazyfree`'s terms: one allocation
    /// per layer.
    pub fn free_effort(&self) -> u64 {
        match self {
            Sketch::Bloom(chain) => chain.layers.len() as u64,
        }
    }
}

/// Whether `bytes` more fit under `maxmemory` (always, without one),
/// reckoned as eviction reckons it: what is counted against the limit.
/// Asked by the commands that make a filter, never while loading one.
pub fn room_for(bytes: u64) -> bool {
    let limit = crate::RUNTIME_MAXMEMORY.load(std::sync::atomic::Ordering::Relaxed);
    limit == 0 || (crate::memory_use::counted() as u64).saturating_add(bytes) <= limit as u64
}

// ── KDB encoding ─────────────────────────────────────────────────────────

/// `sketch` after its [`TAG_SKETCH`]: kind, version, encoding.
pub fn write_kdb(w: &mut impl io::Write, sketch: &Sketch) -> io::Result<()> {
    match sketch {
        Sketch::Bloom(chain) => {
            w.write_all(&[KIND_BLOOM, BLOOM_VERSION])?;
            bloom::write_kdb(w, chain)
        }
    }
}

/// A value written by [`write_kdb`].
pub fn read_kdb(r: &mut impl io::Read) -> io::Result<Sketch> {
    let mut head = [0u8; 2];
    r.read_exact(&mut head)?;
    match head {
        [KIND_BLOOM, BLOOM_VERSION] => Ok(Sketch::Bloom(bloom::read_kdb(r)?)),
        [kind, version] => Err(invalid(&format!(
            "probabilistic value of kind {kind}, version {version}, is not known to this \
             version of KiviDB"
        ))),
    }
}

pub(crate) fn invalid(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what.to_string())
}

pub(crate) fn read_u32(r: &mut impl io::Read) -> io::Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}

pub(crate) fn read_u64(r: &mut impl io::Read) -> io::Result<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}

/// `len` bytes, read a megabyte at a time so that a corrupt length does
/// not allocate more than the stream holds.
pub(crate) fn read_vec(r: &mut impl io::Read, len: usize) -> io::Result<Vec<u8>> {
    const STEP: usize = 1 << 20;
    let mut out = Vec::with_capacity(len.min(STEP));
    while out.len() < len {
        let start = out.len();
        let n = (len - start).min(STEP);
        out.resize(start + n, 0);
        r.read_exact(&mut out[start..])?;
    }
    Ok(out)
}
