//! Scalable Bloom filters (`BF.*`), laid out bit for bit as Redis 8 lays
//! them out.
//!
//! The layout was established by checking a model against the bytes Redis
//! 8.10's `BF.SCANDUMP` and `DUMP` give: it rebuilt them exactly, header
//! and data, for chains of up to ten layers. Where the model came from:
//!
//! * Double hashing over MurmurHash64A, and a chain of filters each
//!   tighter than the last, are the textbook algorithms.
//! * The sizing of a layer (bits per item `−ln e / ln²2`, hashes
//!   `ceil(ln 2 · bits per item)`, with both constants written to 15
//!   digits) is libbloom's, by Jyri J. Virkki, under the BSD 2-Clause
//!   licence: <https://github.com/jvirkki/libbloom>. No code of it is used.
//! * Which hash and seed, that the second hash is seeded with the first,
//!   and the option bits, came from general knowledge of how RedisBloom
//!   works.
//!
//! No Redis or RedisBloom source code was opened or copied for this work,
//! and the names here are our own. Nothing above was taken on trust: every
//! part was confirmed against Redis's bytes. The layout is what makes a
//! filter moved from Redis (an RDB file, `RESTORE`, `BF.LOADCHUNK`) answer
//! the same here, and one moved back answer the same there.
//!
//! * An item hashes to `a = murmur64a(item, FIRST_SEED)` and
//!   `b = murmur64a(item, a)`; its bits in a layer of `bits` bits are
//!   `(a + i·b) mod bits` for `i` below the layer's `hashes`, bit `x` being
//!   bit `x mod 8` of byte `x / 8`.
//! * A layer for `n` items at error rate `e` has `−ln e / D` bits per
//!   item, where `D` is ln²2 to 15 digits (`0.480453013918201`: the
//!   full-precision value gives other bytes than Redis stores);
//!   `trunc(n · bits per item)` bits, rounded up to whole bytes and then
//!   to whole 8-byte words; and `ceil(H · bits per item)` hashes, `H`
//!   being ln 2 to 15 digits.
//! * A chain's first layer has half the error rate asked for (the rate
//!   itself when it does not scale), each further one half the one before
//!   and `expansion` times the capacity. The rate asked for is first
//!   brought down to 0.25 if it is above.
//! * An item goes into the newest layer once no layer has it; a new layer
//!   is started when the newest is at capacity.
//!
//! What a header, a snapshot or an RDB value says about a filter is
//! checked before it is used ([`BloomChain::from_shapes`]): a filter must
//! never be able to divide by zero, loop without end under its shard's
//! lock, or ask for memory it was not made with.
//!
//! Size limits, checked before anything is allocated, whether a filter is
//! made, grows or is loaded: its bytes in all (`bloom-max-filter-bytes`), its
//! layers ([`MAX_LAYERS`]) and the hashes of an item in all its layers
//! ([`MAX_TOTAL_HASHES`]). Redis has none of these; no filter that grows
//! as Redis grows one reaches the last two.
//!
//! A layer's bytes are shared between copies of the filter (a snapshot
//! being written, `COPY`, `DUMP`) until one of them is written to, which
//! then copies that layer: see [`LayerBytes`].

use std::io;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::murmur::murmur64a;

/// The seed of an item's first hash.
pub const FIRST_SEED: u64 = 0xc6a4_a793_5bd1_e995;
/// ln²2 to 15 digits, as libbloom writes it: bits per item are `−ln e`
/// over this.
const SIZING_DIVISOR: f64 = 0.480453013918201;
// ln 2 to 15 digits, as libbloom writes it: hashes are `ceil` of this times
// the bits per item. Not `std::f64::consts::LN_2`: it goes with the 15-digit
// divisor above.
#[allow(clippy::approx_constant)]
const HASH_FACTOR: f64 = 0.693147180559945;
/// The most hashes a layer can have: what the smallest positive error
/// rate gives (`ceil(H · −ln(5e-324) / D)`).
pub const MAX_HASHES: u32 = 1075;

/// A capacity from `BF.RESERVE`/`BF.INSERT`: from 1 to 2^30.
pub const MAX_CAPACITY: i64 = 1 << 30;
/// An expansion: from 0 (not scaling) to 32768.
pub const MAX_EXPANSION: i64 = 32768;
/// Error rates above this are taken as this.
pub const MAX_ERROR: f64 = 0.25;

/// A chain's flags, as its header and RDB value carry them. Every filter
/// Redis makes since RedisBloom 2 has `FLAG_EXACT_SIZE | FLAG_WIDE_HASH`;
/// only those are read, and no other flag.
/// The layer's bits are not rounded to a power of two.
pub const FLAG_EXACT_SIZE: u32 = 1;
/// Items are placed with 64-bit hashes.
pub const FLAG_WIDE_HASH: u32 = 4;
/// The chain does not grow.
pub const FLAG_FIXED: u32 = 8;
const KNOWN_FLAGS: u32 = FLAG_EXACT_SIZE | FLAG_WIDE_HASH | FLAG_FIXED;

/// The error of a write that could not have memory for its filter.
pub const NO_MEMORY: &str = "ERR could not allocate memory for the filter";

/// The most bytes one `BF.SCANDUMP` chunk carries.
pub const CHUNK_MAX: u64 = 16 * 1024 * 1024;

/// The most one layer may take: 4 GiB, a filter of some 3.5 billion items
/// at 1%. A larger one is refused with an error, whether it is asked for
/// (`BF.RESERVE`, growth) or described by what is loaded; it is never
/// allocated. (`bloom-max-filter-bytes`, for the whole filter, is lower
/// unless it is raised.)
pub const MAX_LAYER_BYTES: u64 = 1 << 32;

/// The most layers a filter may have, made or loaded (a `BF.LOADCHUNK`
/// header, a snapshot, an RDB value). A filter that grows stops below it:
/// its error rate halves with each layer, and reaches the smallest double
/// after at most about 1,075 layers.
pub const MAX_LAYERS: usize = 2048;

/// The most hashes an item may take in all of a filter's layers together:
/// what a lookup costs, under its shard's lock. A filter that grows stays
/// below it (some 580,000 at most, from a capacity of 1, `EXPANSION 1` and
/// an error rate of 0.25; see the tests).
pub const MAX_TOTAL_HASHES: u64 = 600_000;

/// `bloom-max-filter-bytes` unless set: the most bytes one filter may take.
pub const DEFAULT_MAX_FILTER_BYTES: u64 = 512 << 20;
/// The highest `bloom-max-filter-bytes` may be set to: 1 TiB.
pub const MAX_MAX_FILTER_BYTES: u64 = 1 << 40;

static MAX_FILTER_BYTES: AtomicU64 = AtomicU64::new(DEFAULT_MAX_FILTER_BYTES);

/// `bloom-max-filter-bytes`.
pub fn max_filter_bytes() -> u64 {
    MAX_FILTER_BYTES.load(Ordering::Relaxed)
}

pub fn set_max_filter_bytes(bytes: u64) {
    MAX_FILTER_BYTES.store(bytes, Ordering::Relaxed);
}

/// The header chunk's fixed part and each layer's part.
const HEADER_FIXED: usize = 20;
const HEADER_LAYER: usize = 53;

/// Why a filter could not take an item or be made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BloomError {
    /// A filter that does not grow is at capacity.
    Full,
    /// A filter that grows cannot: its next layer's error rate is too small
    /// to make a layer of (Redis's `problem inserting into filter`).
    CannotGrow,
    /// The filter would be over a size limit: a layer over
    /// [`MAX_LAYER_BYTES`], the whole over `bloom-max-filter-bytes`, more than
    /// [`MAX_LAYERS`] layers or [`MAX_TOTAL_HASHES`] hashes.
    TooLarge,
    /// There is no room for it under `maxmemory`.
    NoRoom,
    /// The system would not allocate it.
    NoMemory,
}

/// What a filter may come to, as it is made or grows.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// The most bytes the whole filter may take (`bloom-max-filter-bytes`).
    pub max_bytes: u64,
    /// Whether what is allocated must fit under `maxmemory`: for a command
    /// from a client (which has evicted what it could first), not for one
    /// applied from a master or replayed from the AOF, nor for a load.
    pub under_maxmemory: bool,
}

impl Limits {
    /// The limits as set, under `maxmemory` or not.
    pub fn current(under_maxmemory: bool) -> Limits {
        Limits {
            max_bytes: max_filter_bytes(),
            under_maxmemory,
        }
    }
}

/// A layer's bytes. Copies of a filter share them, so that copying one —
/// for a snapshot, `COPY` or `DUMP` — takes no time and no memory; the
/// first write to a shared layer copies it then ([`LayerBytes::make_mut`]),
/// with an allocation that may fail rather than abort.
#[derive(Clone, Debug, PartialEq)]
pub struct LayerBytes(Arc<Box<[u8]>>);

impl LayerBytes {
    pub fn new(bytes: Box<[u8]>) -> LayerBytes {
        LayerBytes(Arc::new(bytes))
    }

    /// The bytes to write to: this filter's own, copied first if another
    /// copy shares them. `NoMemory` if the copy cannot be had.
    pub fn make_mut(&mut self) -> Result<&mut [u8], BloomError> {
        if Arc::get_mut(&mut self.0).is_none() {
            let copy = copy_of(&self.0)?;
            self.0 = Arc::new(copy);
        }
        Ok(Arc::get_mut(&mut self.0).expect("the bytes are this copy's alone"))
    }
}

impl Deref for LayerBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

/// `bytes` copied into an allocation that is asked for, not assumed.
fn copy_of(bytes: &[u8]) -> Result<Box<[u8]>, BloomError> {
    let mut out = Vec::new();
    out.try_reserve_exact(bytes.len())
        .map_err(|_| BloomError::NoMemory)?;
    out.extend_from_slice(bytes);
    Ok(out.into_boxed_slice())
}

/// What a filter has already: its bytes and its hashes, all layers'.
#[derive(Debug, Clone, Copy, Default)]
pub struct Used {
    pub bytes: u64,
    pub hashes: u64,
}

/// One layer of a chain: a plain Bloom filter.
#[derive(Clone, Debug, PartialEq)]
pub struct Layer {
    /// How many items it was sized for.
    pub capacity: u64,
    /// How many items were put into it.
    pub size: u64,
    pub error: f64,
    pub bits_per_item: f64,
    pub hashes: u32,
    /// A byte the header and the RDB value carry after each layer, 0 in
    /// every layer Redis makes; kept as it came.
    pub reserved: u8,
    /// The number of bits: always `data.len() · 8`, and never 0.
    pub bits: u64,
    pub data: LayerBytes,
}

impl Layer {
    /// The bytes a layer for `capacity` items at `error` takes, with its
    /// bits per item and hashes.
    pub fn shape(capacity: u64, error: f64) -> (f64, u64, u32) {
        let bits_per_item = -error.ln() / SIZING_DIVISOR;
        // A float-to-integer cast saturates; capacities are at most
        // 2^30 times the expansions, so the product is exact enough.
        let bits = (capacity as f64 * bits_per_item) as u64;
        let bytes = bits.div_ceil(8).div_ceil(8) * 8;
        let hashes = (HASH_FACTOR * bits_per_item).ceil() as u32;
        (bits_per_item, bytes, hashes)
    }

    /// An empty layer for `capacity` items at `error`, in a filter that
    /// has `used` already. `CannotGrow` if there can be no such layer (no
    /// bits, or more hashes than [`MAX_HASHES`]: an error rate too small);
    /// then the limits, in that order, before anything is allocated.
    pub fn new(capacity: u64, error: f64, limits: Limits, used: Used) -> Result<Layer, BloomError> {
        let (bits_per_item, bytes, hashes) = Self::shape(capacity, error);
        if bytes == 0 || hashes == 0 || hashes > MAX_HASHES {
            return Err(BloomError::CannotGrow);
        }
        if bytes > MAX_LAYER_BYTES
            || used.bytes.saturating_add(bytes) > limits.max_bytes
            || used.hashes.saturating_add(u64::from(hashes)) > MAX_TOTAL_HASHES
        {
            return Err(BloomError::TooLarge);
        }
        if limits.under_maxmemory && !super::room_for(bytes) {
            return Err(BloomError::NoRoom);
        }
        let data = LayerBytes::new(zeroed(bytes)?);
        Ok(Layer {
            capacity,
            size: 0,
            error,
            bits_per_item,
            hashes,
            reserved: 0,
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

    /// Set the item's bits: `NoMemory` if they are shared with a copy and
    /// a copy of its own cannot be had.
    #[inline]
    pub fn set(&mut self, a: u64, b: u64) -> Result<(), BloomError> {
        let (bits, hashes) = (self.bits, self.hashes);
        let data = self.data.make_mut()?;
        for i in 0..u64::from(hashes) {
            let x = a.wrapping_add(i.wrapping_mul(b)) % bits;
            data[(x >> 3) as usize] |= 1u8 << (x & 7);
        }
        Ok(())
    }
}

/// `bytes` zeroed bytes, or why not. The allocation is asked for, not
/// assumed: if the system refuses it, so does this, and the command gets
/// an error rather than the process an abort. The limits are the caller's
/// to check first.
fn zeroed(bytes: u64) -> Result<Box<[u8]>, BloomError> {
    if bytes == 0 || bytes > MAX_LAYER_BYTES {
        return Err(BloomError::TooLarge);
    }
    let layout =
        std::alloc::Layout::array::<u8>(bytes as usize).map_err(|_| BloomError::TooLarge)?;
    // SAFETY: the layout's size is not zero (checked above). The pointer is
    // checked for null, and the block is handed to a `Box<[u8]>` of that
    // many bytes, which frees it with the same layout (bytes, alignment 1).
    unsafe {
        let ptr = std::alloc::alloc_zeroed(layout);
        if ptr.is_null() {
            return Err(BloomError::NoMemory);
        }
        Ok(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
            ptr,
            bytes as usize,
        )))
    }
}

/// The two hashes of an item.
#[inline]
pub fn hash(item: &[u8]) -> (u64, u64) {
    let a = murmur64a(item, FIRST_SEED);
    (a, murmur64a(item, a))
}

/// A scalable Bloom filter: a chain of layers.
#[derive(Clone, Debug, PartialEq)]
pub struct BloomChain {
    /// Items added to the chain (`BF.CARD`).
    pub items: u64,
    pub flags: u32,
    /// Each new layer's capacity over the last's.
    pub expansion: u32,
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

/// Why what a header, a snapshot or an RDB value describes is not taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShapeError {
    /// The parts do not make a filter, or one laid out as Redis lays them
    /// out since RedisBloom 2.
    Bad,
    /// It is over a size limit (see [`BloomError::TooLarge`]).
    TooLarge,
    /// There is no room for its layers under `maxmemory`.
    NoRoom,
    /// The system would not allocate its layers.
    NoMemory,
}

impl From<BloomError> for ShapeError {
    fn from(e: BloomError) -> ShapeError {
        match e {
            BloomError::NoRoom => ShapeError::NoRoom,
            BloomError::NoMemory => ShapeError::NoMemory,
            BloomError::Full | BloomError::CannotGrow | BloomError::TooLarge => {
                ShapeError::TooLarge
            }
        }
    }
}

/// The error rate of a chain's first layer, and whether it scales.
fn first_layer(params: &Params) -> (f64, bool) {
    let nonscaling = params.nonscaling || params.expansion == 0;
    let error = params.error.min(MAX_ERROR);
    (if nonscaling { error } else { error * 0.5 }, nonscaling)
}

/// The bytes of the layers a chain grows by to take `items` more, at most,
/// its newest layer having `left` room and being for `capacity` items at
/// `error`; 0 for a chain that does not grow. Saturates past `u64`.
fn growth_bytes(
    mut left: u64,
    mut capacity: u64,
    mut error: f64,
    expansion: u32,
    items: u64,
) -> u64 {
    if expansion == 0 {
        return 0;
    }
    let mut needed = 0u64;
    let mut items = items;
    let mut layers = 0;
    while items > left && layers < MAX_LAYERS {
        items -= left;
        capacity = capacity.saturating_mul(u64::from(expansion));
        error *= 0.5;
        needed = needed.saturating_add(Layer::shape(capacity, error).1);
        left = capacity;
        layers += 1;
    }
    needed
}

impl BloomChain {
    /// The bytes a chain made with `params` allocates to take `items`
    /// items: its first layer, and the ones it would grow by.
    pub fn bytes_to_make(params: &Params, items: u64) -> u64 {
        let (error, nonscaling) = first_layer(params);
        let first = Layer::shape(params.capacity, error).1;
        let expansion = if nonscaling { 0 } else { params.expansion };
        first.saturating_add(growth_bytes(
            params.capacity,
            params.capacity,
            error,
            expansion,
            items,
        ))
    }

    /// The bytes the chain would allocate to take `items` more, if none of
    /// them is there yet.
    pub fn bytes_to_add(&self, items: u64) -> u64 {
        let last = self.layers.last().expect("a chain has a layer");
        let expansion = if self.nonscaling() { 0 } else { self.expansion };
        growth_bytes(
            last.capacity.saturating_sub(last.size),
            last.capacity,
            last.error,
            expansion,
            items,
        )
    }

    /// What its layers have: bytes and hashes.
    pub fn used(&self) -> Used {
        self.layers.iter().fold(Used::default(), |u, l| Used {
            bytes: u.bytes.saturating_add(l.data.len() as u64),
            hashes: u.hashes.saturating_add(u64::from(l.hashes)),
        })
    }

    /// A new chain, with its first layer.
    pub fn new(params: Params, limits: Limits) -> Result<BloomChain, BloomError> {
        let (first_error, nonscaling) = first_layer(&params);
        let layer = Layer::new(params.capacity, first_error, limits, Used::default()).map_err(
            |e| match e {
                BloomError::CannotGrow => BloomError::TooLarge,
                other => other,
            },
        )?;
        let mut flags = FLAG_EXACT_SIZE | FLAG_WIDE_HASH;
        if nonscaling {
            flags |= FLAG_FIXED;
        }
        Ok(BloomChain {
            items: 0,
            flags,
            expansion: params.expansion,
            layers: vec![layer],
        })
    }

    /// Whether the chain says it does not scale (`BF.INFO` has no
    /// expansion for it). A chain RedisBloom 2.4 made with `EXPANSION 0`
    /// does not say so, and has an expansion of 0: it cannot grow either.
    pub fn nonscaling(&self) -> bool {
        self.flags & FLAG_FIXED != 0
    }

    /// Whether the item may have been added.
    pub fn contains(&self, item: &[u8]) -> bool {
        let (a, b) = hash(item);
        self.layers.iter().rev().any(|layer| layer.contains(a, b))
    }

    /// Add `item`: `Ok(true)` if it was not there, `Ok(false)` if it may
    /// have been.
    pub fn add(&mut self, item: &[u8], limits: Limits) -> Result<bool, BloomError> {
        let (a, b) = hash(item);
        if self.layers.iter().rev().any(|layer| layer.contains(a, b)) {
            return Ok(false);
        }
        let last = self.layers.last().expect("a chain has a layer");
        if last.size >= last.capacity {
            if self.nonscaling() || self.expansion == 0 {
                return Err(BloomError::Full);
            }
            if self.layers.len() >= MAX_LAYERS {
                return Err(BloomError::TooLarge);
            }
            let capacity = last
                .capacity
                .checked_mul(u64::from(self.expansion))
                .ok_or(BloomError::TooLarge)?;
            let layer = Layer::new(capacity, last.error * 0.5, limits, self.used())?;
            self.layers.push(layer);
        }
        let last = self.layers.last_mut().expect("a chain has a layer");
        last.set(a, b)?;
        last.size = last.size.saturating_add(1);
        self.items = self.items.saturating_add(1);
        Ok(true)
    }

    /// `BF.INFO`'s `Capacity`: what every layer was sized for.
    pub fn capacity(&self) -> u64 {
        self.layers
            .iter()
            .fold(0u64, |sum, l| sum.saturating_add(l.capacity))
    }

    /// `BF.INFO`'s `Size`, as Redis reckons it: its structures and the
    /// layers' bytes. (`MEMORY USAGE` gives what the filter takes here.)
    pub fn info_size(&self) -> u64 {
        (32 + 64 * self.layers.len() as u64).saturating_add(self.data_bytes())
    }

    pub fn data_bytes(&self) -> u64 {
        self.layers
            .iter()
            .fold(0u64, |sum, l| sum.saturating_add(l.data.len() as u64))
    }

    /// What the chain takes in memory beyond itself: its layers.
    pub fn memory_bytes(&self) -> usize {
        self.layers.capacity() * std::mem::size_of::<Layer>()
            + self.layers.iter().map(|l| l.data.len()).sum::<usize>()
    }

    /// `BF.DEBUG`'s lines.
    pub fn debug_lines(&self) -> Vec<String> {
        let width = if self.flags & FLAG_WIDE_HASH != 0 {
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
        out.extend_from_slice(&self.flags.to_le_bytes());
        out.extend_from_slice(&self.expansion.to_le_bytes());
        for l in &self.layers {
            out.extend_from_slice(&(l.data.len() as u64).to_le_bytes());
            out.extend_from_slice(&l.bits.to_le_bytes());
            out.extend_from_slice(&l.size.to_le_bytes());
            out.extend_from_slice(&l.error.to_le_bytes());
            out.extend_from_slice(&l.bits_per_item.to_le_bytes());
            out.extend_from_slice(&l.hashes.to_le_bytes());
            out.extend_from_slice(&l.capacity.to_le_bytes());
            out.push(l.reserved);
        }
        out
    }

    /// The bytes the layers of a header chunk take, if it is long enough
    /// to say (saturating): what `BF.LOADCHUNK` of it would allocate.
    pub fn header_bytes(raw: &[u8]) -> u64 {
        if raw.len() < HEADER_FIXED {
            return 0;
        }
        let count = u32::from_le_bytes(raw[8..12].try_into().unwrap()) as usize;
        (0..count)
            .map_while(|i| {
                let at = HEADER_FIXED + HEADER_LAYER * i;
                raw.get(at..at + 8)
                    .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
            })
            .fold(0u64, u64::saturating_add)
    }

    /// A chain from a header chunk, its layers zeroed, within `limits`.
    pub fn from_header(raw: &[u8], limits: Limits) -> Result<BloomChain, ShapeError> {
        if raw.len() < HEADER_FIXED {
            return Err(ShapeError::Bad);
        }
        let u64_at = |at: usize| u64::from_le_bytes(raw[at..at + 8].try_into().unwrap());
        let u32_at = |at: usize| u32::from_le_bytes(raw[at..at + 4].try_into().unwrap());
        let f64_at = |at: usize| f64::from_le_bytes(raw[at..at + 8].try_into().unwrap());
        let items = u64_at(0);
        let count = u32_at(8) as usize;
        let flags = u32_at(12);
        let expansion = u32_at(16);
        let fits = count
            .checked_mul(HEADER_LAYER)
            .and_then(|n| n.checked_add(HEADER_FIXED))
            == Some(raw.len());
        if count == 0 || count > MAX_LAYERS || !fits {
            return Err(ShapeError::Bad);
        }
        let shapes = (0..count)
            .map(|i| {
                let at = HEADER_FIXED + HEADER_LAYER * i;
                LayerShape {
                    bytes: u64_at(at),
                    bits: u64_at(at + 8),
                    size: u64_at(at + 16),
                    error: f64_at(at + 24),
                    bits_per_item: f64_at(at + 32),
                    hashes: u32_at(at + 40),
                    capacity: u64_at(at + 44),
                    reserved: raw[at + 52],
                }
            })
            .collect();
        Self::from_shapes(items, flags, expansion, shapes, None, limits)
    }

    /// A chain from what a header, a snapshot or a Redis RDB value says.
    /// With `data`, the layers are those bytes, already read (one per
    /// layer, each as long as its layer): nothing more is allocated. Without
    /// it they are zeroed, once the whole has been checked against
    /// `limits`.
    ///
    /// Each layer must be one Redis would make: at least one item of
    /// capacity, an error rate in (0, 1), bits per item positive and
    /// finite, `ceil(H · bits per item)` hashes (at most [`MAX_HASHES`]),
    /// bits that are its bytes times 8 and not 0, and a reserved byte of 0;
    /// the items must add up to the chain's, and the flags must be those of
    /// a filter of RedisBloom 2 or later and no others. The whole must be
    /// within the size limits.
    pub fn from_shapes(
        items: u64,
        flags: u32,
        expansion: u32,
        shapes: Vec<LayerShape>,
        data: Option<Vec<Vec<u8>>>,
        limits: Limits,
    ) -> Result<BloomChain, ShapeError> {
        let layout = FLAG_EXACT_SIZE | FLAG_WIDE_HASH;
        if flags & !KNOWN_FLAGS != 0
            || flags & layout != layout
            || shapes.is_empty()
            || shapes.len() > MAX_LAYERS
        {
            return Err(ShapeError::Bad);
        }
        if data.as_ref().is_some_and(|d| d.len() != shapes.len()) {
            return Err(ShapeError::Bad);
        }
        let mut sum = 0u64;
        let mut used = Used::default();
        for s in &shapes {
            let finite = s.bits_per_item.is_finite() && s.bits_per_item > 0.0;
            let ok = finite
                && s.capacity >= 1
                && s.error > 0.0
                && s.error < 1.0
                && s.hashes >= 1
                && s.hashes <= MAX_HASHES
                && f64::from(s.hashes) == (HASH_FACTOR * s.bits_per_item).ceil()
                && s.bits > 0
                && s.bytes.checked_mul(8) == Some(s.bits)
                && s.reserved == 0;
            if !ok {
                return Err(ShapeError::Bad);
            }
            if s.bytes > MAX_LAYER_BYTES {
                return Err(ShapeError::TooLarge);
            }
            used.bytes = used.bytes.saturating_add(s.bytes);
            used.hashes = used.hashes.saturating_add(u64::from(s.hashes));
            sum = sum.checked_add(s.size).ok_or(ShapeError::Bad)?;
        }
        if sum != items {
            return Err(ShapeError::Bad);
        }
        if used.bytes > limits.max_bytes || used.hashes > MAX_TOTAL_HASHES {
            return Err(ShapeError::TooLarge);
        }
        if data.is_none() && limits.under_maxmemory && !super::room_for(used.bytes) {
            return Err(ShapeError::NoRoom);
        }
        let mut datas = data.map(Vec::into_iter);
        let mut layers = Vec::with_capacity(shapes.len());
        for s in shapes {
            let bytes = match datas.as_mut().and_then(Iterator::next) {
                Some(given) if given.len() as u64 == s.bytes => given.into_boxed_slice(),
                Some(_) => return Err(ShapeError::Bad),
                None => zeroed(s.bytes)?,
            };
            let bytes = LayerBytes::new(bytes);
            layers.push(Layer {
                capacity: s.capacity,
                size: s.size,
                error: s.error,
                bits_per_item: s.bits_per_item,
                hashes: s.hashes,
                reserved: s.reserved,
                bits: s.bits,
                data: bytes,
            });
        }
        Ok(BloomChain {
            items,
            flags,
            expansion,
            layers,
        })
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
        let len = i64::try_from(data.len()).unwrap_or(i64::MAX);
        if iter <= 0 || len > iter {
            return Err("ERR received bad data");
        }
        // `iter` is at least 1 and at least `len`: this cannot overflow.
        let offset = iter - 1 - len;
        let mut start = 0i64;
        for l in &mut self.layers {
            let end = start.saturating_add(l.data.len() as i64);
            if offset >= start && offset < end {
                if offset + len > end {
                    return Err("ERR invalid chunk - Too big for current filter");
                }
                let from = (offset - start) as usize;
                let bytes = l.data.make_mut().map_err(|_| NO_MEMORY)?;
                bytes[from..from + data.len()].copy_from_slice(data);
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
    pub bits_per_item: f64,
    pub hashes: u32,
    pub capacity: u64,
    pub reserved: u8,
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
    w.write_all(&chain.flags.to_le_bytes())?;
    w.write_all(&chain.expansion.to_le_bytes())?;
    w.write_all(&(chain.layers.len() as u32).to_le_bytes())?;
    for l in &chain.layers {
        w.write_all(&l.capacity.to_le_bytes())?;
        w.write_all(&l.size.to_le_bytes())?;
        w.write_all(&l.error.to_le_bytes())?;
        w.write_all(&l.bits_per_item.to_le_bytes())?;
        w.write_all(&l.hashes.to_le_bytes())?;
        w.write_all(&[l.reserved])?;
        w.write_all(&l.bits.to_le_bytes())?;
        w.write_all(&(l.data.len() as u64).to_le_bytes())?;
        w.write_all(&l.data)?;
    }
    Ok(())
}

/// The bytes [`write_kdb`] writes for `chain`.
pub fn kdb_len(chain: &BloomChain) -> u64 {
    20 + chain
        .layers
        .iter()
        .map(|l| 53 + l.data.len() as u64)
        .sum::<u64>()
}

/// Why a filter being read is refused for its size: what to do about it.
pub(crate) fn over_limits(used: Used) -> Option<String> {
    let max = max_filter_bytes();
    if used.bytes > max {
        Some(format!(
            "a Bloom filter of more than {} bytes is over bloom-max-filter-bytes ({max}); \
             raise it to load the filter",
            used.bytes
        ))
    } else if used.hashes > MAX_TOTAL_HASHES {
        Some(format!(
            "a Bloom filter whose layers have more than {MAX_TOTAL_HASHES} hashes in all is \
             not supported"
        ))
    } else {
        None
    }
}

/// A chain [`write_kdb`] wrote: its layers are the bytes read, with no
/// second allocation and no `maxmemory` check (a snapshot, or a replica's
/// full sync, is taken whole). The size limits are checked as each layer
/// is reached, before its bytes are read.
pub fn read_kdb(r: &mut impl io::Read) -> io::Result<BloomChain> {
    let items = super::read_u64(r)?;
    let flags = super::read_u32(r)?;
    let expansion = super::read_u32(r)?;
    let count = super::read_u32(r)? as usize;
    if count == 0 || count > MAX_LAYERS {
        return Err(super::invalid("bad number of Bloom filter layers"));
    }
    let mut shapes = Vec::with_capacity(count.min(1024));
    let mut datas = Vec::with_capacity(count.min(1024));
    let mut used = Used::default();
    for _ in 0..count {
        let capacity = super::read_u64(r)?;
        let size = super::read_u64(r)?;
        let error = f64::from_bits(super::read_u64(r)?);
        let bits_per_item = f64::from_bits(super::read_u64(r)?);
        let hashes = super::read_u32(r)?;
        let mut reserved = [0u8];
        r.read_exact(&mut reserved)?;
        let bits = super::read_u64(r)?;
        let bytes = super::read_u64(r)?;
        if bytes > MAX_LAYER_BYTES {
            return Err(super::invalid("Bloom filter layer too large"));
        }
        used.bytes = used.bytes.saturating_add(bytes);
        used.hashes = used.hashes.saturating_add(u64::from(hashes));
        if let Some(why) = over_limits(used) {
            return Err(super::invalid(&why));
        }
        let data = super::read_vec(r, bytes as usize)?;
        shapes.push(LayerShape {
            bytes,
            bits,
            size,
            error,
            bits_per_item,
            hashes,
            capacity,
            reserved: reserved[0],
        });
        datas.push(data);
    }
    let limits = Limits::current(false);
    BloomChain::from_shapes(items, flags, expansion, shapes, Some(datas), limits).map_err(|e| {
        match e {
            ShapeError::Bad => super::invalid("bad Bloom filter"),
            _ => super::invalid("Bloom filter too large"),
        }
    })
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

    /// No limit but the hard ones, and no `maxmemory`.
    fn lim() -> Limits {
        Limits {
            max_bytes: MAX_MAX_FILTER_BYTES,
            under_maxmemory: false,
        }
    }

    fn new_chain(p: Params) -> Result<BloomChain, BloomError> {
        BloomChain::new(p, lim())
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
            let mut chain = new_chain(params(100, 0.01, 2, true)).unwrap();
            assert!(chain.add(item, lim()).unwrap());
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
            let mut chain = new_chain(p).unwrap();
            for i in 0..n {
                chain.add(format!("{prefix}{i}").as_bytes(), lim()).unwrap();
            }
            assert_eq!(chain.header(), hex(header), "{prefix}: header");
            let data: Vec<u8> = chain
                .layers
                .iter()
                .flat_map(|l| l.data.iter().copied())
                .collect();
            assert_eq!(crate::snapshot::redis_crc64(&data), crc, "{prefix}: data");
            // And back from the header and chunks.
            let mut copy = BloomChain::from_header(&chain.header(), lim()).unwrap();
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
            let chain = new_chain(params(capacity, error, 2, nonscaling)).unwrap();
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
        let mut chain = new_chain(params(10, 0.01, 0, false)).unwrap();
        assert!(chain.nonscaling());
        let mut added = 0;
        for i in 0..100 {
            match chain.add(format!("x{i}").as_bytes(), lim()) {
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
        let mut chain = new_chain(params(10, 0.1, 2, true)).unwrap();
        chain.add(b"a", lim()).unwrap();
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
        let chain = new_chain(params(5, 0.1, 2, false)).unwrap();
        let header = chain.header();
        assert!(BloomChain::from_header(&header, lim()).is_ok());
        let bad = Err(ShapeError::Bad);
        assert_eq!(
            BloomChain::from_header(&header[..header.len() - 1], lim()),
            bad
        );
        let mut items = header.clone();
        items[0] = 9;
        assert_eq!(BloomChain::from_header(&items, lim()), bad);
        let mut old = header.clone();
        old[12] = 0;
        assert_eq!(BloomChain::from_header(&old, lim()), bad);
        let mut hashes = header.clone();
        hashes[20 + 40] = 0;
        assert_eq!(BloomChain::from_header(&hashes, lim()), bad);
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
        let mut chain = new_chain(params(5, 0.1, 2, false)).unwrap();
        for i in 0..20 {
            chain.add(format!("{i}").as_bytes(), lim()).unwrap();
        }
        let mut out = Vec::new();
        write_kdb(&mut out, &chain).unwrap();
        assert_eq!(read_kdb(&mut out.as_slice()).unwrap(), chain);
    }

    /// A header for one layer, with each field given.
    #[allow(clippy::too_many_arguments)]
    fn one_layer_header(
        items: u64,
        flags: u32,
        bytes: u64,
        bits: u64,
        error: f64,
        bits_per_item: f64,
        hashes: u32,
        capacity: u64,
    ) -> Vec<u8> {
        let mut h = Vec::new();
        h.extend(items.to_le_bytes());
        h.extend(1u32.to_le_bytes());
        h.extend(flags.to_le_bytes());
        h.extend(2u32.to_le_bytes());
        h.extend(bytes.to_le_bytes());
        h.extend(bits.to_le_bytes());
        h.extend(items.to_le_bytes());
        h.extend(error.to_le_bytes());
        h.extend(bits_per_item.to_le_bytes());
        h.extend(hashes.to_le_bytes());
        h.extend(capacity.to_le_bytes());
        h.push(0);
        h
    }

    /// What a header, a snapshot or an RDB value may say, and must not:
    /// none of these makes a filter.
    #[test]
    fn hostile_shapes_are_refused() {
        let (per_item, _, hashes) = Layer::shape(10, 0.1);
        let flags = FLAG_EXACT_SIZE | FLAG_WIDE_HASH;
        let header = |bytes, bits, error, per_item, hashes, capacity| {
            one_layer_header(0, flags, bytes, bits, error, per_item, hashes, capacity)
        };
        assert!(BloomChain::from_header(&header(8, 64, 0.1, per_item, hashes, 10), lim()).is_ok());
        let bad = Err(ShapeError::Bad);
        for (what, raw) in [
            ("capacity 0", header(8, 64, 0.1, per_item, hashes, 0)),
            (
                "hashes u32::MAX",
                header(8, 64, 0.1, per_item, u32::MAX, 10),
            ),
            (
                "hashes not ceil(H per_item)",
                header(8, 64, 0.1, per_item, hashes + 1, 10),
            ),
            ("bits 0", header(0, 0, 0.1, per_item, hashes, 10)),
            (
                "bits not bytes * 8",
                header(8, 63, 0.1, per_item, hashes, 10),
            ),
            ("bits per item 0", header(8, 64, 0.1, 0.0, hashes, 10)),
            (
                "bits per item NaN",
                header(8, 64, 0.1, f64::NAN, hashes, 10),
            ),
            (
                "bits per item inf",
                header(8, 64, 0.1, f64::INFINITY, hashes, 10),
            ),
            ("error 0", header(8, 64, 0.0, per_item, hashes, 10)),
            ("error 1", header(8, 64, 1.0, per_item, hashes, 10)),
            (
                "bytes * 8 overflows",
                header(u64::MAX, 0, 0.1, per_item, hashes, 10),
            ),
            (
                "unknown flag",
                one_layer_header(0, flags | 2, 8, 64, 0.1, per_item, hashes, 10),
            ),
            (
                "32-bit hashes",
                one_layer_header(0, FLAG_EXACT_SIZE, 8, 64, 0.1, per_item, hashes, 10),
            ),
        ] {
            assert_eq!(BloomChain::from_header(&raw, lim()), bad, "{what}");
        }
        // A 64 GiB layer is refused without being allocated.
        let huge = header(1 << 36, 1 << 39, 0.1, per_item, hashes, 10);
        assert_eq!(
            BloomChain::from_header(&huge, lim()),
            Err(ShapeError::TooLarge)
        );
        // A header that claims 2^32 - 1 layers is not read past its end.
        let mut many = header(8, 64, 0.1, per_item, hashes, 10);
        many[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(BloomChain::from_header(&many, lim()), bad);
    }

    /// At most [`MAX_LAYERS`] layers, whichever way a filter is loaded.
    #[test]
    fn layer_count_is_capped() {
        let (per_item, _, hashes) = Layer::shape(10, 0.1);
        let flags = FLAG_EXACT_SIZE | FLAG_WIDE_HASH;
        let shape = LayerShape {
            bytes: 8,
            bits: 64,
            size: 0,
            error: 0.1,
            bits_per_item: per_item,
            hashes,
            capacity: 10,
            reserved: 0,
        };
        let at_cap = vec![shape; MAX_LAYERS];
        assert!(BloomChain::from_shapes(0, flags, 2, at_cap, None, lim()).is_ok());
        let over = vec![shape; MAX_LAYERS + 1];
        assert_eq!(
            BloomChain::from_shapes(0, flags, 2, over, None, lim()),
            Err(ShapeError::Bad)
        );
        // A header of MAX_LAYERS + 1 well-formed layers.
        let one = one_layer_header(0, flags, 8, 64, 0.1, per_item, hashes, 10);
        let mut raw = one[..HEADER_FIXED].to_vec();
        raw[8..12].copy_from_slice(&((MAX_LAYERS + 1) as u32).to_le_bytes());
        for _ in 0..=MAX_LAYERS {
            raw.extend_from_slice(&one[HEADER_FIXED..]);
        }
        assert_eq!(BloomChain::from_header(&raw, lim()), Err(ShapeError::Bad));
        // A snapshot record that claims MAX_LAYERS + 1 layers is refused
        // before any layer is read.
        let mut kdb = Vec::new();
        kdb.extend(0u64.to_le_bytes());
        kdb.extend(flags.to_le_bytes());
        kdb.extend(2u32.to_le_bytes());
        kdb.extend(((MAX_LAYERS + 1) as u32).to_le_bytes());
        let err = read_kdb(&mut &kdb[..]).unwrap_err();
        assert!(
            err.to_string().contains("number of Bloom filter layers"),
            "{err}"
        );
    }

    /// Loaded bytes are the layers: not copied, and not zeroed again.
    #[test]
    fn loaded_layers_are_the_bytes_read() {
        let mut chain = new_chain(params(5, 0.1, 2, false)).unwrap();
        for i in 0..20 {
            chain.add(format!("{i}").as_bytes(), lim()).unwrap();
        }
        let shapes: Vec<LayerShape> = chain
            .layers
            .iter()
            .map(|l| LayerShape {
                bytes: l.data.len() as u64,
                bits: l.bits,
                size: l.size,
                error: l.error,
                bits_per_item: l.bits_per_item,
                hashes: l.hashes,
                capacity: l.capacity,
                reserved: 0,
            })
            .collect();
        let datas: Vec<Vec<u8>> = chain.layers.iter().map(|l| l.data.to_vec()).collect();
        let pointers: Vec<*const u8> = datas.iter().map(|d| d.as_ptr()).collect();
        let (items, flags, expansion) = (chain.items, chain.flags, chain.expansion);
        let loaded =
            BloomChain::from_shapes(items, flags, expansion, shapes.clone(), Some(datas), lim())
                .unwrap();
        assert_eq!(loaded, chain);
        let kept: Vec<*const u8> = loaded.layers.iter().map(|l| l.data.as_ptr()).collect();
        assert_eq!(kept, pointers);
        // A layer of the wrong length is refused.
        let short = vec![vec![0u8; 1]; shapes.len()];
        assert_eq!(
            BloomChain::from_shapes(items, flags, expansion, shapes, Some(short), lim()),
            Err(ShapeError::Bad)
        );
    }

    #[test]
    fn load_chunk_iterators_out_of_range() {
        let mut chain = new_chain(params(10, 0.1, 2, true)).unwrap();
        for iter in [0, -1, i64::MIN] {
            for data in [&b""[..], b"x"] {
                assert_eq!(
                    chain.load_chunk(iter, data),
                    Err("ERR received bad data"),
                    "{iter}"
                );
            }
        }
        assert_eq!(
            chain.load_chunk(i64::MAX, b"x"),
            Err("ERR invalid offset - no link found")
        );
        assert_eq!(chain.scandump(i64::MIN), (0, vec![]));
        assert_eq!(chain.scandump(i64::MAX), (0, vec![]));
    }

    /// Asking for a layer larger than the cap, or than the system gives, is
    /// an error, not an abort.
    #[test]
    fn too_large_is_an_error() {
        assert_eq!(
            new_chain(params(1 << 30, 1e-30, 2, false)),
            Err(BloomError::TooLarge)
        );
        assert_eq!(zeroed(MAX_LAYER_BYTES + 1), Err(BloomError::TooLarge));
        assert_eq!(zeroed(0), Err(BloomError::TooLarge));
    }

    /// Capacities that add up past u64 are reported, not wrapped.
    #[test]
    fn sums_saturate() {
        let mut chain = new_chain(params(10, 0.1, 2, false)).unwrap();
        let mut second = chain.layers[0].clone();
        second.capacity = u64::MAX;
        chain.layers[0].capacity = u64::MAX;
        chain.layers.push(second);
        assert_eq!(chain.capacity(), u64::MAX);
    }

    /// A layer of hashes [`MAX_HASHES`]: capacity 1 at the smallest error
    /// rate, 200 bytes.
    fn widest_shape() -> LayerShape {
        let error = 5e-324;
        let (bits_per_item, bytes, hashes) = Layer::shape(1, error);
        assert_eq!((bytes, hashes), (200, MAX_HASHES));
        LayerShape {
            bytes,
            bits: bytes * 8,
            size: 0,
            error,
            bits_per_item,
            hashes,
            capacity: 1,
            reserved: 0,
        }
    }

    /// The most layers and hashes a filter can grow to, as Redis grows it:
    /// below the limits, which so refuse nothing Redis makes.
    #[test]
    fn growth_stays_under_the_limits() {
        let (mut layers, mut hashes) = (0usize, 0u64);
        let mut error = MAX_ERROR * 0.5;
        loop {
            let (_, bytes, h) = Layer::shape(1, error);
            if bytes == 0 || h == 0 || h > MAX_HASHES {
                break;
            }
            layers += 1;
            hashes += u64::from(h);
            error *= 0.5;
        }
        assert!(layers > 1000 && layers < MAX_LAYERS, "{layers} layers");
        assert!(
            hashes > 500_000 && hashes < MAX_TOTAL_HASHES,
            "{hashes} hashes"
        );
    }

    /// As in Redis 8.10: `BF.RESERVE g 1e-300 1 EXPANSION 1` takes 91
    /// items in 78 layers, and the 92nd cannot be put in (`problem
    /// inserting into filter`), not `non scaling filter is full`.
    #[test]
    fn a_filter_that_cannot_grow_says_so() {
        let mut chain = new_chain(params(1, 1e-300, 1, false)).unwrap();
        let mut added = 0;
        let err = loop {
            match chain.add(format!("item{}", added + 1).as_bytes(), lim()) {
                Ok(_) => added += 1,
                Err(e) => break e,
            }
        };
        assert_eq!(
            (err, added, chain.layers.len()),
            (BloomError::CannotGrow, 91, 78)
        );
    }

    /// The limits, checked before anything is allocated, whichever way a
    /// filter is made.
    #[test]
    fn size_limits() {
        let small = Limits {
            max_bytes: 1000,
            under_maxmemory: false,
        };
        // Made: 11,984 bytes for 10,000 items at 1%.
        assert_eq!(
            BloomChain::new(params(10_000, 0.01, 2, false), small),
            Err(BloomError::TooLarge)
        );
        // Grown: 144 bytes, then 312, then 696, which is over.
        let mut chain = BloomChain::new(params(100, 0.01, 2, false), small).unwrap();
        let mut refused = None;
        for i in 0..1000 {
            if let Err(e) = chain.add(format!("{i}").as_bytes(), small) {
                refused = Some(e);
                break;
            }
        }
        assert_eq!(refused, Some(BloomError::TooLarge));
        assert!(chain.used().bytes <= 1000);
        // Loaded: a header of layers over the limit in all, though each is
        // under it.
        let mut shapes = vec![widest_shape(); 6];
        let flags = FLAG_EXACT_SIZE | FLAG_WIDE_HASH;
        assert_eq!(
            BloomChain::from_shapes(0, flags, 1, shapes.clone(), None, small).unwrap_err(),
            ShapeError::TooLarge
        );
        shapes.truncate(5);
        assert!(BloomChain::from_shapes(0, flags, 1, shapes, None, small).is_ok());
        // Hashes: 559 layers of 1075 are over MAX_TOTAL_HASHES.
        let wide = vec![widest_shape(); 559];
        assert_eq!(
            BloomChain::from_shapes(0, flags, 1, wide.clone(), None, lim()).unwrap_err(),
            ShapeError::TooLarge
        );
        assert!(BloomChain::from_shapes(0, flags, 1, wide[..558].to_vec(), None, lim()).is_ok());
        // A snapshot record of them is refused before its layers are read.
        let mut kdb = Vec::new();
        kdb.extend(0u64.to_le_bytes());
        kdb.extend(flags.to_le_bytes());
        kdb.extend(1u32.to_le_bytes());
        kdb.extend(559u32.to_le_bytes());
        let s = widest_shape();
        for _ in 0..559 {
            kdb.extend(s.capacity.to_le_bytes());
            kdb.extend(0u64.to_le_bytes());
            kdb.extend(s.error.to_le_bytes());
            kdb.extend(s.bits_per_item.to_le_bytes());
            kdb.extend(s.hashes.to_le_bytes());
            kdb.push(0);
            kdb.extend(s.bits.to_le_bytes());
            kdb.extend(s.bytes.to_le_bytes());
            kdb.extend(vec![0u8; s.bytes as usize]);
        }
        let mut r = &kdb[..];
        let err = read_kdb(&mut r).unwrap_err();
        assert!(err.to_string().contains("hashes in all"), "{err}");
    }

    /// What a command would allocate, reckoned before it runs.
    #[test]
    fn bytes_reckoned_before() {
        let p = params(100, 0.01, 2, false);
        // 144 bytes for the first layer, at 0.5%; 101 items need a second,
        // of 200 at 0.25%: 312 bytes.
        assert_eq!(BloomChain::bytes_to_make(&p, 1), 144);
        assert_eq!(BloomChain::bytes_to_make(&p, 101), 456);
        let mut chain = new_chain(p).unwrap();
        assert_eq!(chain.bytes_to_add(100), 0);
        assert_eq!(chain.bytes_to_add(101), 312);
        let mut i = 0;
        while chain.layers[0].size < 100 {
            chain.add(format!("{i}").as_bytes(), lim()).unwrap();
            i += 1;
        }
        assert_eq!(chain.layers.len(), 1);
        assert_eq!(chain.bytes_to_add(1), 312);
        let fixed = new_chain(params(100, 0.01, 2, true)).unwrap();
        assert_eq!(fixed.bytes_to_add(1000), 0);
        assert_eq!(BloomChain::header_bytes(&chain.header()), 144);
        assert_eq!(BloomChain::header_bytes(b"short"), 0);
    }

    /// A copy shares its layers' bytes until one of the two is written to;
    /// then the one written to has its own, and the other is as it was.
    #[test]
    fn copies_share_layers_until_written() {
        let mut chain = new_chain(params(100, 0.01, 2, false)).unwrap();
        chain.add(b"a", lim()).unwrap();
        let copy = chain.clone();
        assert_eq!(chain.layers[0].data.as_ptr(), copy.layers[0].data.as_ptr());
        chain.add(b"b", lim()).unwrap();
        assert_ne!(chain.layers[0].data.as_ptr(), copy.layers[0].data.as_ptr());
        assert!(chain.contains(b"b") && !copy.contains(b"b"));
        assert!(copy.contains(b"a"));
        // Written to again, the bytes are its own already: not copied.
        let at = chain.layers[0].data.as_ptr();
        chain.add(b"c", lim()).unwrap();
        assert_eq!(chain.layers[0].data.as_ptr(), at);
    }
}
