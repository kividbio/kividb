//! Scalable Bloom filters (`BF.*`), laid out bit for bit as Redis 8 lays
//! them out.
//!
//! How Redis places an item, sizes a layer and grows a chain was found
//! black-box: a model built from the textbook algorithms (double hashing
//! over MurmurHash64A, a chain of filters each tighter than the last) was
//! checked against the bytes Redis's `BF.SCANDUMP` gave, and it rebuilt
//! them exactly, header and data, for chains of up to ten layers. Nothing
//! here comes from Redis's source. The layout is what makes a filter
//! moved from Redis (an RDB file, `RESTORE`, `BF.LOADCHUNK`) answer the
//! same here, and one moved back answer the same there.
//!
//! * An item hashes to `a = murmur64a(item, SEED)` and
//!   `b = murmur64a(item, a)`; its bits in a layer of `bits` bits are
//!   `(a + i·b) mod bits` for `i` below the layer's `hashes`, bit `x` being
//!   bit `x mod 8` of byte `x / 8`.
//! * A layer for `n` items at error rate `e` has `bpe = −ln e / ln²2`
//!   (with ln²2 as the 15-digit `0.480453013918201`: the full-precision
//!   value gives other `bpe` bytes than Redis stores), `trunc(n·bpe)` bits
//!   rounded up to whole bytes and then to whole 8-byte words, and
//!   `ceil(ln 2 · bpe)` hashes.
//! * A chain's first layer has half the error rate asked for (the rate
//!   itself when it does not scale), each further one half the one before
//!   and `expansion` times the capacity. The rate asked for is first
//!   brought down to 0.25 if it is above.
//! * An item goes into the newest layer once no layer has it; a new layer
//!   is started when the newest is at capacity.

use std::io;

use super::murmur::murmur64a;

/// The seed of an item's first hash.
pub const SEED: u64 = 0xc6a4_a793_5bd1_e995;
/// ln²2 and ln 2 as Redis has them: to 15 digits.
const LN2_SQUARED: f64 = 0.480453013918201;
// Not `LN_2`: the hash count is `ceil` of a product, and the 15-digit value
// is the one that goes with the 15-digit ln²2 above.
#[allow(clippy::approx_constant)]
const LN2: f64 = 0.693147180559945;

/// A capacity from `BF.RESERVE`/`BF.INSERT`: from 1 to 2^30.
pub const MAX_CAPACITY: i64 = 1 << 30;
/// An expansion: from 0 (not scaling) to 32768.
pub const MAX_EXPANSION: i64 = 32768;
/// Error rates above this are taken as this.
pub const MAX_ERROR: f64 = 0.25;

/// `options` bits. Every filter Redis makes since RedisBloom 2 has
/// `NOROUND | FORCE64`; only those are read.
pub const OPT_NOROUND: u32 = 1;
pub const OPT_FORCE64: u32 = 4;
pub const OPT_NONSCALING: u32 = 8;

/// The most bytes one `BF.SCANDUMP` chunk carries.
pub const CHUNK_MAX: u64 = 16 * 1024 * 1024;

/// The most a layer may take. Redis has no limit of its own and aborts
/// when the allocation fails; a layer this size is a mistake, not a filter.
pub const MAX_LAYER_BYTES: u64 = 1 << 36;

/// The header chunk's fixed part and each layer's part.
const HEADER_FIXED: usize = 20;
const HEADER_LAYER: usize = 53;

/// Why a filter could not take an item or be made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BloomError {
    /// A filter that does not scale is at capacity.
    Full,
    /// The layer would be larger than [`MAX_LAYER_BYTES`] or than the
    /// memory left under `maxmemory`.
    TooLarge,
}

/// One layer of a chain: a plain Bloom filter.
#[derive(Clone, Debug, PartialEq)]
pub struct Layer {
    /// How many items it was sized for (Redis's `entries`).
    pub capacity: u64,
    /// How many items were put into it.
    pub size: u64,
    pub error: f64,
    /// Bits per entry.
    pub bpe: f64,
    pub hashes: u32,
    /// Kept as loaded; 0 for every layer Redis makes with `NOROUND`.
    pub n2: u8,
    /// The number of bits: always `data.len() · 8`.
    pub bits: u64,
    pub data: Box<[u8]>,
}

impl Layer {
    /// The bytes a layer for `capacity` items at `error` takes, with its
    /// bits per entry and hashes.
    pub fn shape(capacity: u64, error: f64) -> (f64, u64, u32) {
        let bpe = -error.ln() / LN2_SQUARED;
        // A float-to-integer cast saturates; capacities are at most
        // 2^30 times the expansions, so the product is exact enough.
        let bits = (capacity as f64 * bpe) as u64;
        let bytes = bits.div_ceil(8).div_ceil(8) * 8;
        let hashes = (LN2 * bpe).ceil() as u32;
        (bpe, bytes, hashes)
    }

    /// An empty layer for `capacity` items at `error`.
    pub fn new(capacity: u64, error: f64) -> Result<Layer, BloomError> {
        let (bpe, bytes, hashes) = Self::shape(capacity, error);
        let data = zeroed(bytes)?;
        Ok(Layer {
            capacity,
            size: 0,
            error,
            bpe,
            hashes,
            n2: 0,
            bits: bytes * 8,
            data,
        })
    }

    #[inline]
    fn bit(&self, a: u64, b: u64, i: u64) -> (usize, u8) {
        let x = a.wrapping_add(i.wrapping_mul(b)) % self.bits;
        ((x >> 3) as usize, 1u8 << (x & 7))
    }

    /// Whether every bit of the item hashed to `(a, b)` is set.
    #[inline]
    pub fn contains(&self, a: u64, b: u64) -> bool {
        (0..u64::from(self.hashes)).all(|i| {
            let (at, mask) = self.bit(a, b, i);
            self.data[at] & mask != 0
        })
    }

    /// Set the item's bits.
    #[inline]
    pub fn set(&mut self, a: u64, b: u64) {
        for i in 0..u64::from(self.hashes) {
            let (at, mask) = self.bit(a, b, i);
            self.data[at] |= mask;
        }
    }
}

/// `bytes` zeroed bytes, or why not. Zeroed pages are only touched when
/// written, as with Redis's `calloc`.
fn zeroed(bytes: u64) -> Result<Box<[u8]>, BloomError> {
    if bytes > MAX_LAYER_BYTES || !super::room_for(bytes) {
        return Err(BloomError::TooLarge);
    }
    Ok(vec![0u8; bytes as usize].into_boxed_slice())
}

/// The two hashes of an item.
#[inline]
pub fn hash(item: &[u8]) -> (u64, u64) {
    let a = murmur64a(item, SEED);
    (a, murmur64a(item, a))
}

/// A scalable Bloom filter: a chain of layers.
#[derive(Clone, Debug, PartialEq)]
pub struct BloomChain {
    /// Items added to the chain (`BF.CARD`).
    pub items: u64,
    pub options: u32,
    /// The expansion: each new layer's capacity over the last's.
    pub growth: u32,
    pub layers: Vec<Layer>,
}

/// What a chain is made with.
#[derive(Clone, Copy, Debug)]
pub struct Params {
    pub capacity: u64,
    pub error: f64,
    /// 0 makes a chain that does not scale.
    pub expansion: u32,
    pub nonscaling: bool,
}

impl BloomChain {
    /// A new chain, with its first layer.
    pub fn new(params: Params) -> Result<BloomChain, BloomError> {
        let nonscaling = params.nonscaling || params.expansion == 0;
        let error = params.error.min(MAX_ERROR);
        let first_error = if nonscaling { error } else { error * 0.5 };
        let layer = Layer::new(params.capacity, first_error)?;
        let mut options = OPT_NOROUND | OPT_FORCE64;
        if nonscaling {
            options |= OPT_NONSCALING;
        }
        Ok(BloomChain {
            items: 0,
            options,
            growth: params.expansion,
            layers: vec![layer],
        })
    }

    /// Whether the chain says it does not scale (`BF.INFO` has no
    /// expansion for it). A chain RedisBloom 2.4 made with `EXPANSION 0`
    /// does not say so, and has an expansion of 0: it cannot grow either.
    pub fn nonscaling(&self) -> bool {
        self.options & OPT_NONSCALING != 0
    }

    /// Whether the item may have been added.
    pub fn contains(&self, item: &[u8]) -> bool {
        let (a, b) = hash(item);
        self.layers.iter().rev().any(|layer| layer.contains(a, b))
    }

    /// Add `item`: `Ok(true)` if it was not there, `Ok(false)` if it may
    /// have been.
    pub fn add(&mut self, item: &[u8]) -> Result<bool, BloomError> {
        let (a, b) = hash(item);
        if self.layers.iter().rev().any(|layer| layer.contains(a, b)) {
            return Ok(false);
        }
        let last = self.layers.last().expect("a chain has a layer");
        if last.size >= last.capacity {
            if self.nonscaling() || self.growth == 0 {
                return Err(BloomError::Full);
            }
            let capacity = last
                .capacity
                .checked_mul(u64::from(self.growth))
                .ok_or(BloomError::TooLarge)?;
            let layer = Layer::new(capacity, last.error * 0.5)?;
            self.layers.push(layer);
        }
        let last = self.layers.last_mut().expect("a chain has a layer");
        last.set(a, b);
        last.size += 1;
        self.items += 1;
        Ok(true)
    }

    /// `BF.INFO`'s `Capacity`: what every layer was sized for.
    pub fn capacity(&self) -> u64 {
        self.layers.iter().map(|l| l.capacity).sum()
    }

    /// `BF.INFO`'s `Size`, as Redis reckons it: its structures and the
    /// layers' bytes. (`MEMORY USAGE` gives what the filter takes here.)
    pub fn info_size(&self) -> u64 {
        32 + 64 * self.layers.len() as u64 + self.data_bytes()
    }

    pub fn data_bytes(&self) -> u64 {
        self.layers.iter().map(|l| l.data.len() as u64).sum()
    }

    /// What the chain takes in memory beyond itself: its layers.
    pub fn memory_bytes(&self) -> usize {
        self.layers.capacity() * std::mem::size_of::<Layer>()
            + self.layers.iter().map(|l| l.data.len()).sum::<usize>()
    }

    /// `BF.DEBUG`'s lines.
    pub fn debug_lines(&self) -> Vec<String> {
        let width = if self.options & OPT_FORCE64 != 0 {
            64
        } else {
            32
        };
        let mut lines = vec![format!("size:{}", self.items)];
        for l in &self.layers {
            lines.push(format!(
                "bytes:{} bits:{} hashes:{} hashwidth:{width} capacity:{} size:{} ratio:{}",
                l.data.len(),
                l.bits,
                l.hashes,
                l.capacity,
                l.size,
                format_g(l.error)
            ));
        }
        lines
    }

    // ── SCANDUMP / LOADCHUNK ────────────────────────────────────────────

    /// The header chunk: the chain, then each layer, little-endian.
    pub fn header(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_FIXED + HEADER_LAYER * self.layers.len());
        out.extend_from_slice(&self.items.to_le_bytes());
        out.extend_from_slice(&(self.layers.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.options.to_le_bytes());
        out.extend_from_slice(&self.growth.to_le_bytes());
        for l in &self.layers {
            out.extend_from_slice(&(l.data.len() as u64).to_le_bytes());
            out.extend_from_slice(&l.bits.to_le_bytes());
            out.extend_from_slice(&l.size.to_le_bytes());
            out.extend_from_slice(&l.error.to_le_bytes());
            out.extend_from_slice(&l.bpe.to_le_bytes());
            out.extend_from_slice(&l.hashes.to_le_bytes());
            out.extend_from_slice(&l.capacity.to_le_bytes());
            out.push(l.n2);
        }
        out
    }

    /// A chain from a header chunk, its layers zeroed; `None` if the
    /// header is not one.
    pub fn from_header(raw: &[u8]) -> Option<Result<BloomChain, BloomError>> {
        if raw.len() < HEADER_FIXED {
            return None;
        }
        let u64_at = |at: usize| u64::from_le_bytes(raw[at..at + 8].try_into().unwrap());
        let u32_at = |at: usize| u32::from_le_bytes(raw[at..at + 4].try_into().unwrap());
        let f64_at = |at: usize| f64::from_le_bytes(raw[at..at + 8].try_into().unwrap());
        let items = u64_at(0);
        let count = u32_at(8) as usize;
        let options = u32_at(12);
        let growth = u32_at(16);
        if count == 0 || raw.len() != HEADER_FIXED + HEADER_LAYER * count {
            return None;
        }
        let mut shapes = Vec::with_capacity(count);
        for i in 0..count {
            let at = HEADER_FIXED + HEADER_LAYER * i;
            let shape = LayerShape {
                bytes: u64_at(at),
                bits: u64_at(at + 8),
                size: u64_at(at + 16),
                error: f64_at(at + 24),
                bpe: f64_at(at + 32),
                hashes: u32_at(at + 40),
                capacity: u64_at(at + 44),
                n2: raw[at + 52],
            };
            shapes.push(shape);
        }
        Self::from_shapes(items, options, growth, shapes)
    }

    /// A chain from what a header or a Redis RDB value says, layers
    /// zeroed. `None` if the parts do not fit together, or describe a
    /// layout older than RedisBloom 2 (`NOROUND` and `FORCE64` not set),
    /// which places items otherwise.
    pub fn from_shapes(
        items: u64,
        options: u32,
        growth: u32,
        shapes: Vec<LayerShape>,
    ) -> Option<Result<BloomChain, BloomError>> {
        if options & (OPT_NOROUND | OPT_FORCE64) != OPT_NOROUND | OPT_FORCE64 || shapes.is_empty() {
            return None;
        }
        let mut sum = 0u64;
        for s in &shapes {
            let ok = s.bytes.checked_mul(8) == Some(s.bits)
                && s.bits > 0
                && s.hashes > 0
                && s.n2 == 0
                && s.error > 0.0
                && s.error < 1.0
                && s.bpe.is_finite();
            if !ok {
                return None;
            }
            sum = sum.checked_add(s.size)?;
        }
        if sum != items {
            return None;
        }
        let mut layers = Vec::with_capacity(shapes.len());
        for s in shapes {
            let data = match zeroed(s.bytes) {
                Ok(data) => data,
                Err(e) => return Some(Err(e)),
            };
            layers.push(Layer {
                capacity: s.capacity,
                size: s.size,
                error: s.error,
                bpe: s.bpe,
                hashes: s.hashes,
                n2: s.n2,
                bits: s.bits,
                data,
            });
        }
        Some(Ok(BloomChain {
            items,
            options,
            growth,
            layers,
        }))
    }

    /// `BF.SCANDUMP`'s chunk at `iter`: the next iterator and the bytes.
    /// The iterator after the header is 1, and after a data chunk 1 more
    /// than the offset its last byte ends at; `(0, [])` is the end.
    pub fn scandump(&self, iter: i64) -> (i64, Vec<u8>) {
        if iter == 0 {
            return (1, self.header());
        }
        if iter < 0 {
            return (0, Vec::new());
        }
        let pos = (iter - 1) as u64;
        let mut start = 0u64;
        for l in &self.layers {
            let end = start + l.data.len() as u64;
            if pos < end {
                let from = (pos - start) as usize;
                let len = (end - pos).min(CHUNK_MAX) as usize;
                let next = pos + len as u64 + 1;
                return (next as i64, l.data[from..from + len].to_vec());
            }
            start = end;
        }
        (0, Vec::new())
    }

    /// `BF.LOADCHUNK` of a data chunk: the bytes end where `iter` (as
    /// `BF.SCANDUMP` gave it) says, and must lie in one layer.
    pub fn load_chunk(&mut self, iter: i64, data: &[u8]) -> Result<(), &'static str> {
        let len = data.len() as i64;
        if len > iter {
            return Err("ERR received bad data");
        }
        let offset = iter - 1 - len;
        let mut start = 0i64;
        for l in &mut self.layers {
            let end = start + l.data.len() as i64;
            if offset >= start && offset < end {
                if offset + len > end {
                    return Err("ERR invalid chunk - Too big for current filter");
                }
                let from = (offset - start) as usize;
                l.data[from..from + data.len()].copy_from_slice(data);
                return Ok(());
            }
            start = end;
        }
        Err("ERR invalid offset - no link found")
    }
}

/// A layer as a header or an RDB value describes it.
#[derive(Clone, Copy, Debug)]
pub struct LayerShape {
    pub bytes: u64,
    pub bits: u64,
    pub size: u64,
    pub error: f64,
    pub bpe: f64,
    pub hashes: u32,
    pub capacity: u64,
    pub n2: u8,
}

/// `%g`, as C's printf writes it: six significant digits, the exponent
/// form below 1e-4 and from 1e6, trailing zeros dropped.
pub fn format_g(x: f64) -> String {
    if x == 0.0 {
        return "0".into();
    }
    if !x.is_finite() {
        return if x.is_nan() {
            "nan".into()
        } else if x > 0.0 {
            "inf".into()
        } else {
            "-inf".into()
        };
    }
    let sci = format!("{:.5e}", x);
    let (mantissa, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let trim = |s: &str| -> String {
        if s.contains('.') {
            s.trim_end_matches('0').trim_end_matches('.').to_string()
        } else {
            s.to_string()
        }
    };
    if (-4..6).contains(&exp) {
        let decimals = (5 - exp).max(0) as usize;
        trim(&format!("{:.*}", decimals, x))
    } else {
        let sign = if exp < 0 { '-' } else { '+' };
        format!("{}e{sign}{:02}", trim(mantissa), exp.abs())
    }
}

// ── KiviDB's own encoding (KDB snapshots) ───────────────────────────────

/// The chain in KiviDB's snapshot format: the chain's fields, then each
/// layer's, then its bytes.
pub fn write_kdb(w: &mut impl io::Write, chain: &BloomChain) -> io::Result<()> {
    w.write_all(&chain.items.to_le_bytes())?;
    w.write_all(&chain.options.to_le_bytes())?;
    w.write_all(&chain.growth.to_le_bytes())?;
    w.write_all(&(chain.layers.len() as u32).to_le_bytes())?;
    for l in &chain.layers {
        w.write_all(&l.capacity.to_le_bytes())?;
        w.write_all(&l.size.to_le_bytes())?;
        w.write_all(&l.error.to_le_bytes())?;
        w.write_all(&l.bpe.to_le_bytes())?;
        w.write_all(&l.hashes.to_le_bytes())?;
        w.write_all(&[l.n2])?;
        w.write_all(&l.bits.to_le_bytes())?;
        w.write_all(&(l.data.len() as u64).to_le_bytes())?;
        w.write_all(&l.data)?;
    }
    Ok(())
}

pub fn read_kdb(r: &mut impl io::Read) -> io::Result<BloomChain> {
    let items = super::read_u64(r)?;
    let options = super::read_u32(r)?;
    let growth = super::read_u32(r)?;
    let count = super::read_u32(r)? as usize;
    let mut shapes = Vec::with_capacity(count.min(1024));
    let mut datas = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let capacity = super::read_u64(r)?;
        let size = super::read_u64(r)?;
        let error = f64::from_bits(super::read_u64(r)?);
        let bpe = f64::from_bits(super::read_u64(r)?);
        let hashes = super::read_u32(r)?;
        let mut n2 = [0u8];
        r.read_exact(&mut n2)?;
        let bits = super::read_u64(r)?;
        let bytes = super::read_u64(r)?;
        if bytes > MAX_LAYER_BYTES {
            return Err(super::invalid("Bloom filter layer too large"));
        }
        let data = super::read_vec(r, bytes as usize)?;
        shapes.push(LayerShape {
            bytes,
            bits,
            size,
            error,
            bpe,
            hashes,
            capacity,
            n2: n2[0],
        });
        datas.push(data);
    }
    let mut chain = match BloomChain::from_shapes(items, options, growth, shapes) {
        Some(Ok(chain)) => chain,
        Some(Err(_)) => return Err(super::invalid("Bloom filter too large to load")),
        None => return Err(super::invalid("bad Bloom filter")),
    };
    for (layer, data) in chain.layers.iter_mut().zip(datas) {
        layer.data = data.into_boxed_slice();
    }
    Ok(chain)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    fn params(capacity: u64, error: f64, expansion: u32, nonscaling: bool) -> Params {
        Params {
            capacity,
            error,
            expansion,
            nonscaling,
        }
    }

    /// The bits Redis 8.10 set for one item in a 100-item, 1% filter that
    /// does not scale (960 bits, 7 hashes), from `BF.SCANDUMP`.
    #[test]
    fn one_item_sets_redis_bits() {
        for (item, want) in [
            (&b"hello"[..], [153, 189, 481, 517, 809, 845, 881]),
            (b"a", [74, 347, 364, 637, 654, 927, 944]),
            (b"", [522, 536, 643, 657, 750, 764, 885]),
            (b"\xff", [56, 82, 421, 515, 583, 854, 948]),
            (
                b"item-12345678901234567890",
                [120, 278, 291, 436, 521, 679, 837],
            ),
        ] {
            let mut chain = BloomChain::new(params(100, 0.01, 2, true)).unwrap();
            assert!(chain.add(item).unwrap());
            let layer = &chain.layers[0];
            assert_eq!((layer.bits, layer.hashes), (960, 7));
            let got: Vec<u64> = (0..layer.bits)
                .filter(|x| layer.data[(*x / 8) as usize] >> (x % 8) & 1 == 1)
                .collect();
            assert_eq!(got, want, "{item:?}");
        }
    }

    /// Whole chains, against Redis's `SCANDUMP`: the header verbatim, the
    /// data by its CRC64.
    #[test]
    fn chains_match_redis() {
        let cases: [(&str, Params, &str, usize, u64); 4] = [
            (
                "k",
                params(100, 0.01, 2, false),
                "df030000000000000400000005000000020000009000000000000000800400000000000064000000000000007b14ae47e17a743fe9862fb2350e2640080000006400000000000000003801000000000000c009000000000000c8000000000000007b14ae47e17a643f4af7d49edef0284009000000c80000000000000000b802000000000000c01500000000000090010000000000007b14ae47e17a543faa677a8b87d32b400a0000009001000000000000000006000000000000003000000000000023010000000000007b14ae47e17a443f0bd81f7830b62e400b000000200300000000000000",
                1000,
                0x5b32_4102_7034_689c,
            ),
            (
                "q",
                params(50, 0.001, 3, false),
                "f301000000000000030000000500000003000000680000000000000040030000000000003200000000000000fca9f1d24d62403f2e92f414fca32f400b0000003200000000000000004801000000000000400a0000000000009600000000000000fca9f1d24d62303f4701cd80524331400c000000960000000000000000200400000000000000210000000000002b01000000000000fca9f1d24d62203f77b91ff7a6b432400d000000c20100000000000000",
                500,
                0x2137_c8a6_42c1_2613,
            ),
            (
                "auto",
                params(100, 0.01, 2, false),
                "2b010000000000000200000005000000020000009000000000000000800400000000000064000000000000007b14ae47e17a743fe9862fb2350e2640080000006400000000000000003801000000000000c009000000000000c7000000000000007b14ae47e17a643f4af7d49edef0284009000000c80000000000000000",
                300,
                0x952e_6dcf_f214_dabb,
            ),
            (
                "e",
                params(10, 0.05, 1, false),
                "64000000000000000a0000000500000001000000100000000000000080000000000000000a000000000000009a9999999999993f0bd81f7830b61e40060000000a0000000000000000100000000000000080000000000000000a000000000000009a9999999999893f655cb528c13d2240070000000a0000000000000000100000000000000080000000000000000a000000000000009a9999999999793fc6cc5a156a202540080000000a0000000000000000100000000000000080000000000000000a000000000000009a9999999999693f273d000213032840090000000a00000000000000001800000000000000c0000000000000000a000000000000009a9999999999593f87ada5eebbe52a400a0000000a00000000000000001800000000000000c0000000000000000a000000000000009a9999999999493fe81d4bdb64c82d400b0000000a00000000000000001800000000000000c0000000000000000a000000000000009a9999999999393f2447f8e3865530400c0000000a00000000000000001800000000000000c0000000000000000a000000000000009a9999999999293f54ff4a5adbc631400d0000000a00000000000000001800000000000000c0000000000000000a000000000000009a9999999999193f85b79dd02f3833400e0000000a0000000000000000200000000000000000010000000000000a000000000000009a9999999999093fb56ff04684a934400f0000000a0000000000000000",
                100,
                0xf743_6079_c4d8_f06f,
            ),
        ];
        for (prefix, p, header, n, crc) in cases {
            let mut chain = BloomChain::new(p).unwrap();
            for i in 0..n {
                chain.add(format!("{prefix}{i}").as_bytes()).unwrap();
            }
            assert_eq!(chain.header(), hex(header), "{prefix}: header");
            let data: Vec<u8> = chain
                .layers
                .iter()
                .flat_map(|l| l.data.iter().copied())
                .collect();
            assert_eq!(crate::snapshot::redis_crc64(&data), crc, "{prefix}: data");
            // And back from the header and chunks.
            let mut copy = BloomChain::from_header(&chain.header()).unwrap().unwrap();
            let mut iter = 1;
            loop {
                let (next, bytes) = chain.scandump(iter);
                if next == 0 {
                    break;
                }
                copy.load_chunk(next, &bytes).unwrap();
                iter = next;
            }
            assert_eq!(copy, chain, "{prefix}: SCANDUMP and LOADCHUNK");
        }
    }

    /// Layer shapes Redis's `BF.DEBUG` showed, error rates above 0.25
    /// included.
    #[test]
    fn layer_shapes() {
        for (capacity, error, nonscaling, bytes, hashes) in [
            (1, 0.01, true, 8, 7),
            (10, 0.01, true, 16, 7),
            (100, 0.01, true, 120, 7),
            (1000, 0.01, true, 1200, 7),
            (10000, 0.01, true, 11984, 7),
            (100, 0.001, true, 184, 10),
            (100, 0.1, true, 64, 4),
            (100, 0.5, true, 40, 3),
            (7, 0.05, true, 8, 5),
            (123, 0.0123, true, 144, 7),
            (100, 0.5, false, 56, 4),
            (100, 0.999999, true, 40, 3),
            (1, 1e-300, false, 184, 998),
        ] {
            let chain = BloomChain::new(params(capacity, error, 2, nonscaling)).unwrap();
            let l = &chain.layers[0];
            assert_eq!(
                (l.data.len(), l.hashes),
                (bytes, hashes),
                "{capacity} {error}"
            );
        }
    }

    #[test]
    fn nonscaling_fills_up() {
        let mut chain = BloomChain::new(params(10, 0.01, 0, false)).unwrap();
        assert!(chain.nonscaling());
        let mut added = 0;
        for i in 0..100 {
            match chain.add(format!("x{i}").as_bytes()) {
                Ok(true) => added += 1,
                Ok(false) => {}
                Err(BloomError::Full) => break,
                Err(e) => panic!("{e:?}"),
            }
        }
        assert_eq!(added, 10);
        assert_eq!(chain.info_size(), 112);
    }

    #[test]
    fn scandump_edges() {
        let mut chain = BloomChain::new(params(10, 0.1, 2, true)).unwrap();
        chain.add(b"a").unwrap();
        assert_eq!(chain.scandump(1).0, 9);
        assert_eq!(chain.scandump(2), (9, chain.layers[0].data[1..].to_vec()));
        assert_eq!(chain.scandump(9), (0, vec![]));
        assert_eq!(chain.scandump(-5), (0, vec![]));
        assert_eq!(
            chain.load_chunk(10, b"\x09"),
            Err("ERR invalid offset - no link found")
        );
        assert_eq!(
            chain.load_chunk(9, &[0; 9]),
            Err("ERR invalid offset - no link found")
        );
        assert_eq!(chain.load_chunk(-1, b"x"), Err("ERR received bad data"));
        assert_eq!(chain.load_chunk(8, b"\x09"), Ok(()));
        assert_eq!(chain.layers[0].data[6], 9);
    }

    #[test]
    fn bad_headers() {
        let chain = BloomChain::new(params(5, 0.1, 2, false)).unwrap();
        let header = chain.header();
        assert!(BloomChain::from_header(&header).is_some());
        assert!(BloomChain::from_header(&header[..header.len() - 1]).is_none());
        let mut items = header.clone();
        items[0] = 9;
        assert!(BloomChain::from_header(&items).is_none());
        let mut old = header.clone();
        old[12] = 0;
        assert!(BloomChain::from_header(&old).is_none());
        let mut hashes = header.clone();
        hashes[20 + 40] = 0;
        assert!(BloomChain::from_header(&hashes).is_none());
    }

    #[test]
    fn g_format() {
        for (x, s) in [
            (0.01, "0.01"),
            (0.005, "0.005"),
            (0.000125, "0.000125"),
            (0.25, "0.25"),
            (5e-301, "5e-301"),
            (0.0123, "0.0123"),
            (1e-5, "1e-05"),
            (0.123456789, "0.123457"),
        ] {
            assert_eq!(format_g(x), s);
        }
    }

    #[test]
    fn kdb_round_trip() {
        let mut chain = BloomChain::new(params(5, 0.1, 2, false)).unwrap();
        for i in 0..20 {
            chain.add(format!("{i}").as_bytes()).unwrap();
        }
        let mut out = Vec::new();
        write_kdb(&mut out, &chain).unwrap();
        assert_eq!(read_kdb(&mut out.as_slice()).unwrap(), chain);
    }
}
