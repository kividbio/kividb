//! The probabilistic types as Redis saves them: values of module type
//! (`RDB_TYPE_MODULE_2`), each a module ID (nine characters of name, ten
//! bits of encoding version), then the module's own fields, each announced
//! by its kind, up to an end marker.
//!
//! What each type saves was read off `DUMP` payloads of Redis 8.10 and of
//! Redis Stack 6.2, 7.2 and 7.4 (RedisBloom 2.4 to 8.10 save the same).
//! Values read from Redis — an RDB file, a full sync from a Redis master,
//! `RESTORE` and `MIGRATE` — become the same filters here; and `DUMP`
//! writes them so, so that Redis can `RESTORE` what KiviDB dumped.

use std::io::{self, Read};

use super::Sketch;
use super::bloom::{
    BloomChain, FLAG_EXACT_SIZE, FLAG_WIDE_HASH, LayerShape, Limits, MAX_LAYER_BYTES, ShapeError,
    Used,
};
use super::topk::{Slot, TopK};

/// The module type of Bloom filters, and the encoding version written.
pub const BLOOM_TYPE: &str = "MBbloom--";
const BLOOM_ENCVER: u64 = 4;
/// The module type of Top-K sketches, and the encoding version written:
/// what Redis 8.10 writes.
pub const TOPK_TYPE: &str = "TopK-TYPE";
const TOPK_ENCVER: u64 = 0;
/// A place in Redis's heap as it saves it: the fingerprint, the item's
/// length, a pointer (meaningless once saved), the count and four bytes of
/// padding.
const TOPK_SLOT: usize = 24;

/// The RDB version `DUMP` payloads of these types carry: 9 (Redis 6.0),
/// the oldest that has these encodings, so that every Redis since takes
/// them. The values are the same in every version up to 8.10's 15.
const DUMP_RDB_VERSION: u16 = 9;

const RDB_TYPE_MODULE_2: u8 = 7;
const OPCODE_EOF: u64 = 0;
const OPCODE_UINT: u64 = 2;
const OPCODE_DOUBLE: u64 = 4;
const OPCODE_STRING: u64 = 5;

const NAME_CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// The 64-bit ID of module type `name` at encoding version `encver`.
fn module_id(name: &str, encver: u64) -> u64 {
    let mut id = 0u64;
    for (i, c) in name.bytes().enumerate() {
        let index = NAME_CHARS
            .iter()
            .position(|&n| n == c)
            .expect("module type names use the 64 characters") as u64;
        id |= index << (64 - 6 * (i + 1));
    }
    id | encver
}

// ── Writing ─────────────────────────────────────────────────────────────

struct Writer {
    out: Vec<u8>,
}

impl Writer {
    fn len(&mut self, n: u64) {
        if n < 1 << 6 {
            self.out.push(n as u8);
        } else if n < 1 << 14 {
            self.out.push(0x40 | (n >> 8) as u8);
            self.out.push(n as u8);
        } else if n <= u64::from(u32::MAX) {
            self.out.push(0x80);
            self.out.extend_from_slice(&(n as u32).to_be_bytes());
        } else {
            self.out.push(0x81);
            self.out.extend_from_slice(&n.to_be_bytes());
        }
    }

    fn uint(&mut self, v: u64) {
        self.len(OPCODE_UINT);
        self.len(v);
    }

    fn double(&mut self, v: f64) {
        self.len(OPCODE_DOUBLE);
        self.out.extend_from_slice(&v.to_le_bytes());
    }

    fn string(&mut self, s: &[u8]) {
        self.len(OPCODE_STRING);
        self.len(s.len() as u64);
        self.out.extend_from_slice(s);
    }
}

fn write_bloom(w: &mut Writer, chain: &BloomChain) {
    w.uint(chain.items);
    w.uint(chain.layers.len() as u64);
    w.uint(u64::from(chain.flags));
    w.uint(u64::from(chain.expansion));
    for l in &chain.layers {
        w.uint(l.capacity);
        w.double(l.error);
        w.uint(u64::from(l.hashes));
        w.double(l.bits_per_item);
        w.uint(l.bits);
        w.uint(u64::from(l.reserved));
        w.string(&l.data);
        w.uint(l.size);
    }
}

/// A Top-K sketch as Redis saves it (encoding version 0): `k`, width,
/// depth and decay; the buckets (fingerprint and count, little-endian) as
/// one string; the heap's places as one string; then each place's item, a
/// string ending in a zero byte (just the zero byte for an empty place).
/// The pointers Redis's places carry are written as 0: Redis sets them
/// from the items when it loads.
fn write_topk(w: &mut Writer, t: &TopK) {
    w.uint(u64::from(t.k));
    w.uint(u64::from(t.width));
    w.uint(u64::from(t.depth));
    w.double(t.decay);
    w.len(OPCODE_STRING);
    w.len(t.buckets.len() as u64 * 8);
    for b in t.buckets.iter() {
        w.out.extend_from_slice(&b.fp.to_le_bytes());
        w.out.extend_from_slice(&b.count.to_le_bytes());
    }
    w.len(OPCODE_STRING);
    w.len((t.heap.len() * TOPK_SLOT) as u64);
    for s in t.heap.iter() {
        let len = s.item.as_ref().map_or(0, |i| i.len() as u32);
        w.out.extend_from_slice(&s.fp.to_le_bytes());
        w.out.extend_from_slice(&len.to_le_bytes());
        w.out.extend_from_slice(&[0; 8]);
        w.out.extend_from_slice(&s.count.to_le_bytes());
        w.out.extend_from_slice(&[0; 4]);
    }
    for s in t.heap.iter() {
        let item = s.item.as_deref().unwrap_or(&[]);
        w.len(OPCODE_STRING);
        w.len(item.len() as u64 + 1);
        w.out.extend_from_slice(item);
        w.out.push(0);
    }
}

/// Whether `DUMP` writes `sketch` in Redis's format, which Redis can
/// `RESTORE`; the other types are dumped in KiviDB's own.
pub fn dumps_as_redis(sketch: &Sketch) -> bool {
    matches!(sketch, Sketch::Bloom(_) | Sketch::TopK(_))
}

/// The most bytes [`dump_payload`] writes for `sketch`: each field at its
/// longest, and the layers' bytes.
fn dump_len(sketch: &Sketch) -> u64 {
    match sketch {
        Sketch::Bloom(chain) => {
            let layers = chain.layers.len() as u64;
            // Type, module ID, four fields, the end, version and CRC: under
            // 64. Each layer: eight fields of at most 10 bytes, its bytes.
            64 + layers * 80 + chain.data_bytes()
        }
        // Type, module ID, four fields, two strings' heads, the end, version
        // and CRC: under 96. The buckets, the places, and each item with
        // its head and zero byte.
        Sketch::TopK(t) => {
            96 + t.buckets.len() as u64 * 8
                + t.heap
                    .iter()
                    .map(|s| TOPK_SLOT as u64 + 11 + s.item.as_ref().map_or(0, |i| i.len() as u64))
                    .sum::<u64>()
        }
    }
}

/// `sketch` as a Redis `DUMP` payload: the module value, the RDB version
/// and Redis's CRC64 over both. Its memory is asked for at once, not
/// assumed: if it cannot be had, this is an error, not an abort.
pub fn dump_payload(sketch: &Sketch) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    let len = usize::try_from(dump_len(sketch)).map_err(|_| no_memory())?;
    out.try_reserve_exact(len).map_err(|_| no_memory())?;
    let mut w = Writer { out };
    w.out.push(RDB_TYPE_MODULE_2);
    match sketch {
        Sketch::Bloom(chain) => {
            w.len(module_id(BLOOM_TYPE, BLOOM_ENCVER));
            write_bloom(&mut w, chain);
        }
        Sketch::TopK(t) => {
            w.len(module_id(TOPK_TYPE, TOPK_ENCVER));
            write_topk(&mut w, t);
        }
    }
    w.len(OPCODE_EOF);
    let mut out = w.out;
    out.extend_from_slice(&DUMP_RDB_VERSION.to_le_bytes());
    let crc = crate::snapshot::redis_crc64(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    debug_assert!(
        out.len() <= len,
        "dump_len reckons {len}, wrote {}",
        out.len()
    );
    Ok(out)
}

fn no_memory() -> io::Error {
    io::Error::new(
        io::ErrorKind::OutOfMemory,
        "not enough memory to serialize the filter",
    )
}

// ── Reading ─────────────────────────────────────────────────────────────

/// The encoding of an LZF-compressed string, after the length's marker.
const RDB_ENC_LZF: u64 = 3;

/// An RDB length, and whether it is a special encoding's number instead.
fn rdb_length(r: &mut impl Read) -> io::Result<(u64, bool)> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b)?;
    Ok(match b[0] >> 6 {
        0 => (u64::from(b[0] & 0x3f), false),
        1 => {
            let mut next = [0u8; 1];
            r.read_exact(&mut next)?;
            ((u64::from(b[0] & 0x3f) << 8) | u64::from(next[0]), false)
        }
        2 if b[0] == 0x80 => {
            let mut v = [0u8; 4];
            r.read_exact(&mut v)?;
            (u64::from(u32::from_be_bytes(v)), false)
        }
        2 if b[0] == 0x81 => {
            let mut v = [0u8; 8];
            r.read_exact(&mut v)?;
            (u64::from_be_bytes(v), false)
        }
        2 => return Err(super::invalid("an RDB length of an unknown form")),
        _ => (u64::from(b[0] & 0x3f), true),
    })
}

struct Reader<'r, R: Read> {
    r: &'r mut R,
    what: &'static str,
}

impl<R: Read> Reader<'_, R> {
    fn bad(&self, why: &str) -> io::Error {
        super::invalid(&format!("{}: {why}", self.what))
    }

    fn opcode(&mut self, want: u64) -> io::Result<()> {
        let got = crate::snapshot::rdb_read_plain_length(self.r)?;
        if got != want {
            return Err(self.bad(&format!("expected field kind {want}, found {got}")));
        }
        Ok(())
    }

    fn uint(&mut self) -> io::Result<u64> {
        self.opcode(OPCODE_UINT)?;
        crate::snapshot::rdb_read_plain_length(self.r)
    }

    fn uint32(&mut self) -> io::Result<u32> {
        let v = self.uint()?;
        u32::try_from(v).map_err(|_| self.bad("a field is out of range"))
    }

    fn double(&mut self) -> io::Result<f64> {
        self.opcode(OPCODE_DOUBLE)?;
        let mut b = [0u8; 8];
        self.r.read_exact(&mut b)?;
        Ok(f64::from_le_bytes(b))
    }

    fn string(&mut self) -> io::Result<Vec<u8>> {
        self.opcode(OPCODE_STRING)?;
        crate::snapshot::rdb_read_string(self.r)
    }

    /// A string that must be `len` bytes, plain or LZF-compressed as Redis
    /// writes one (`mismatch` if it is another length): its length, or a
    /// compressed one's original length, is checked before anything is
    /// allocated, and only the bytes the payload holds are read.
    fn string_of(&mut self, len: u64, mismatch: &str) -> io::Result<Vec<u8>> {
        self.opcode(OPCODE_STRING)?;
        let want = usize::try_from(len).map_err(|_| self.bad(mismatch))?;
        let (n, encoded) = rdb_length(self.r)?;
        if !encoded {
            if n != len {
                return Err(self.bad(mismatch));
            }
            return super::read_vec(self.r, want);
        }
        if n != RDB_ENC_LZF {
            return Err(self.bad("a string of an unexpected encoding"));
        }
        let (compressed, _) = rdb_length(self.r)?;
        let (original, _) = rdb_length(self.r)?;
        if original != len {
            return Err(self.bad(mismatch));
        }
        // Redis compresses a string only if that makes it shorter.
        if compressed == 0 || compressed >= len {
            return Err(self.bad("a compressed string of a bad length"));
        }
        let compressed = super::read_vec(self.r, compressed as usize)?;
        match lzf::decompress(&compressed, want) {
            Ok(out) if out.len() == want => Ok(out),
            _ => Err(self.bad("a compressed string that does not decompress")),
        }
    }

    fn end(&mut self) -> io::Result<()> {
        self.opcode(OPCODE_EOF)
    }
}

fn read_bloom(r: &mut Reader<'_, impl Read>) -> io::Result<BloomChain> {
    let items = r.uint()?;
    let count = r.uint()?;
    let flags = r.uint32()?;
    let expansion = r.uint32()?;
    if count == 0 || count > super::bloom::MAX_LAYERS as u64 {
        return Err(r.bad("bad number of layers"));
    }
    // Not what the payload says: a small payload can say anything.
    let mut shapes = Vec::with_capacity((count as usize).min(1024));
    let mut datas = Vec::with_capacity((count as usize).min(1024));
    let mut used = Used::default();
    for _ in 0..count {
        let capacity = r.uint()?;
        let error = r.double()?;
        let hashes = r.uint32()?;
        let bits_per_item = r.double()?;
        let bits = r.uint()?;
        let reserved = r.uint()?;
        // The fixed size limits, before the layer's bytes are read, by what
        // the layer says it has.
        let claimed = Used {
            bytes: used.bytes.saturating_add(bits / 8),
            hashes: used.hashes.saturating_add(u64::from(hashes)),
        };
        if bits / 8 > MAX_LAYER_BYTES {
            return Err(r.bad("a layer is too large to load"));
        }
        if let Some(why) = super::bloom::over_limits(claimed) {
            return Err(r.bad(&why));
        }
        // Then by what was read (a compressed string's length is its own
        // word, not the layer's): its bits must be its bytes times 8, and
        // its bytes are what is charged.
        let data = r.string()?;
        if (data.len() as u64).checked_mul(8) != Some(bits) {
            return Err(r.bad("a layer's bytes do not match its bits"));
        }
        used.bytes = used.bytes.saturating_add(data.len() as u64);
        used.hashes = claimed.hashes;
        if let Some(why) = super::bloom::over_limits(used) {
            return Err(r.bad(&why));
        }
        let size = r.uint()?;
        shapes.push(LayerShape {
            bytes: data.len() as u64,
            bits,
            size,
            error,
            bits_per_item,
            hashes,
            capacity,
            reserved: u8::try_from(reserved).map_err(|_| r.bad("a field is out of range"))?,
        });
        datas.push(data);
    }
    r.end()?;
    // The layers are the strings read: nothing more is allocated, and
    // neither `maxmemory` nor `bloom-max-filter-bytes` is consulted (a
    // file, a full sync or a `RESTORE` is taken whole).
    let wide = FLAG_EXACT_SIZE | FLAG_WIDE_HASH;
    let limits = Limits::structural();
    match BloomChain::from_shapes(items, flags, expansion, shapes, Some(datas), limits) {
        Ok(chain) => Ok(chain),
        Err(ShapeError::Bad) if flags & wide != wide => Err(r.bad(
            "a filter made before RedisBloom 2.0 (32-bit hashes) is not supported; recreate it",
        )),
        Err(ShapeError::Bad) => Err(r.bad("the layers do not fit together")),
        Err(_) => Err(r.bad("a layer is too large to load")),
    }
}

/// A Top-K sketch [`write_topk`] (or Redis) wrote: its shape is checked
/// against the size limits before anything sized by it is read; the
/// buckets and places must be exactly what the shape says, and each item
/// its place's length and a zero byte.
fn read_topk(r: &mut Reader<'_, impl Read>) -> io::Result<TopK> {
    let k = r.uint()?;
    let width = r.uint()?;
    let depth = r.uint()?;
    let decay = r.double()?;
    if !super::topk::fits(k, width, depth) || !super::topk::decay_ok(decay) {
        return Err(r.bad("a sketch over the size limits is not supported"));
    }
    let raw = r.string_of(
        width * depth * 8,
        "the buckets do not match the width and depth",
    )?;
    let buckets = super::topk::buckets_from_bytes(&raw)?;
    drop(raw);
    let places = r.string_of(k * TOPK_SLOT as u64, "the heap does not match k")?;
    let mut heap = Vec::with_capacity(k as usize);
    for place in places.as_chunks::<TOPK_SLOT>().0 {
        let word = |at: usize| {
            u32::from_le_bytes([place[at], place[at + 1], place[at + 2], place[at + 3]])
        };
        let (fp, len, count) = (word(0), word(4), word(16));
        let mut item = r.string_of(u64::from(len) + 1, "an item does not match its length")?;
        if item.pop() != Some(0) {
            return Err(r.bad("an item does not end in a zero byte"));
        }
        // An empty place: no item, no fingerprint, no count.
        let empty = item.is_empty() && fp == 0 && count == 0;
        heap.push(Slot {
            fp,
            count,
            item: (!empty).then(|| std::sync::Arc::from(item.into_boxed_slice())),
        });
    }
    r.end()?;
    Ok(TopK::from_parts(
        k as u32,
        width as u32,
        depth as u32,
        decay,
        buckets,
        heap,
        super::topk::RNG_START,
    ))
}

/// The value of module type `name` (encoding version `encver`) from an
/// RDB stream, if it is one of these types; `Ok(None)` for another type.
pub fn read(name: &str, encver: u64, r: &mut impl Read) -> io::Result<Option<Sketch>> {
    match name {
        BLOOM_TYPE => {
            if encver != BLOOM_ENCVER {
                return Err(super::invalid(&format!(
                    "Bloom filter encoding version {encver} is not supported (RedisBloom 2.4 \
                     and later write {BLOOM_ENCVER})"
                )));
            }
            let mut reader = Reader {
                r,
                what: "Bloom filter",
            };
            Ok(Some(Sketch::Bloom(read_bloom(&mut reader)?)))
        }
        TOPK_TYPE => {
            if encver != TOPK_ENCVER {
                return Err(super::invalid(&format!(
                    "Top-K sketch encoding version {encver} is not supported"
                )));
            }
            let mut reader = Reader {
                r,
                what: "Top-K sketch",
            };
            Ok(Some(Sketch::TopK(read_topk(&mut reader)?)))
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sketch::bloom::{BloomChain, Params};

    /// `value` (a module value as [`super::read`] takes it) with the plain
    /// string whose opcode is at `at` LZF-compressed, as Redis writes one:
    /// that one, then the same claiming another original or compressed
    /// length (every one to be refused), then with its compressed bytes
    /// damaged (to be refused, or read within the limits).
    fn lzf_variants(
        value: &[u8],
        at: usize,
        next: &mut impl FnMut() -> u64,
    ) -> (Vec<u8>, Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let mut r = &value[at + 1..];
        let (len, encoded) = rdb_length(&mut r).unwrap();
        assert!(!encoded);
        let (data, rest) = r.split_at(len as usize);
        let compressed = lzf::compress(data).unwrap();
        let build = |original: u64, clen: u64, bytes: &[u8]| {
            let mut w = Writer {
                out: value[..at + 1].to_vec(),
            };
            w.out.push(0xc0 | RDB_ENC_LZF as u8);
            w.len(clen);
            w.len(original);
            w.out.extend_from_slice(bytes);
            w.out.extend_from_slice(rest);
            w.out
        };
        let clen = compressed.len() as u64;
        let good = build(len, clen, &compressed);
        let wrong = [
            (len - 1, clen),
            (len + 1, clen),
            (1 << 40, clen),
            (u64::MAX, clen),
            (len, 0),
            (len, len),
            (len, 1 << 40),
            (len, clen - 1),
        ]
        .into_iter()
        .map(|(original, c)| build(original, c, &compressed))
        .collect();
        let damaged = (0..2000)
            .map(|_| {
                let mut c = compressed.clone();
                if next().is_multiple_of(2) {
                    let i = next() as usize % c.len();
                    c[i] = next() as u8;
                } else {
                    c.truncate(next() as usize % c.len());
                }
                build(len, c.len() as u64, &c)
            })
            .collect();
        (good, wrong, damaged)
    }

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    /// A one-layer value of 64 bits whose bytes are `data`.
    fn one_layer_value(data: &[u8]) -> Vec<u8> {
        let error = 0.1f64;
        let per_item = -error.ln() / 0.480453013918201;
        let mut w = Writer { out: Vec::new() };
        w.uint(0); // items
        w.uint(1); // layers
        w.uint(u64::from(FLAG_EXACT_SIZE | FLAG_WIDE_HASH));
        w.uint(2); // expansion
        w.uint(10); // capacity
        w.double(error);
        w.uint(4); // hashes
        w.double(per_item);
        w.uint(64); // bits
        w.uint(0); // reserved
        w.string(data);
        w.uint(0); // size
        w.len(OPCODE_EOF);
        w.out
    }

    /// A layer's bytes must be its bits over 8 exactly, whatever their
    /// string says, and are refused as soon as that layer is read.
    #[test]
    fn layer_bytes_must_match_bits() {
        let read = |value: Vec<u8>| super::read(BLOOM_TYPE, 4, &mut &value[..]);
        assert!(matches!(read(one_layer_value(&[0u8; 8])), Ok(Some(_))));
        for len in [7, 9, 16, 4096] {
            let err = read(one_layer_value(&vec![0u8; len])).unwrap_err();
            assert!(err.to_string().contains("do not match"), "{len}: {err}");
        }
    }

    /// `DUMP t2` on Redis 8.10.2 after `TOPK.RESERVE t2 2 3 1 1` and
    /// `TOPK.ADD t2 hello`: read, it is the sketch KiviDB builds from the
    /// same commands; written and read again, the same sketch.
    #[test]
    fn topk_dump_from_redis() {
        let payload = hex(TOPK_DUMP);
        let body = &payload[1..payload.len() - 10];
        let mut r = body;
        let id = crate::snapshot::rdb_read_plain_length(&mut r).unwrap();
        assert_eq!(id, module_id(TOPK_TYPE, 0));
        let Some(Sketch::TopK(read)) = super::read(TOPK_TYPE, 0, &mut r).unwrap() else {
            panic!("not a sketch");
        };
        assert!(r.is_empty());
        let mut ours = TopK::new(2, 3, 1, 1.0).unwrap();
        ours.incr_by(b"hello", 1).unwrap();
        assert_eq!(read, ours);
        let written = dump_payload(&Sketch::TopK(ours.clone())).unwrap();
        let mut back = &written[1..written.len() - 10];
        crate::snapshot::rdb_read_plain_length(&mut back).unwrap();
        let Some(Sketch::TopK(again)) = super::read(TOPK_TYPE, 0, &mut back).unwrap() else {
            panic!("not a sketch");
        };
        assert_eq!(again, ours);
        assert!(back.is_empty());
    }

    /// Corrupted Top-K values are refused, or read as a sketch within the
    /// limits; never a panic.
    #[test]
    fn corrupt_topk_values_are_refused_cleanly() {
        let mut t = TopK::new(3, 5, 2, 0.9).unwrap();
        for i in 0..30 {
            t.incr_by(format!("{}", i % 7).as_bytes(), 1).unwrap();
        }
        let mut w = Writer { out: Vec::new() };
        write_topk(&mut w, &t);
        w.len(OPCODE_EOF);
        let good = w.out;
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..4000 {
            let mut bad = good.clone();
            match next() % 3 {
                0 => {
                    let at = next() as usize % bad.len();
                    bad[at] = next() as u8;
                }
                1 => bad.truncate(next() as usize % bad.len()),
                _ => {
                    let mut w = Writer { out: Vec::new() };
                    w.uint(next() >> (next() % 64));
                    w.uint(next() >> (next() % 64));
                    w.uint(next() >> (next() % 64));
                    w.double(0.9);
                    w.string(&[0u8; 16]);
                    w.len(OPCODE_EOF);
                    bad = w.out;
                }
            }
            if let Ok(Some(Sketch::TopK(read))) = super::read(TOPK_TYPE, 0, &mut &bad[..]) {
                assert!(super::super::topk::fits(
                    u64::from(read.k),
                    u64::from(read.width),
                    u64::from(read.depth)
                ));
                assert_eq!(read.heap.len(), read.k as usize);
            }
        }
        // The buckets LZF-compressed, as Redis writes them.
        let mut w = Writer { out: Vec::new() };
        w.uint(3);
        w.uint(5);
        w.uint(2);
        w.double(0.9);
        let (compressed, wrong, damaged) = lzf_variants(&good, w.out.len(), &mut next);
        let Ok(Some(Sketch::TopK(read))) = super::read(TOPK_TYPE, 0, &mut &compressed[..]) else {
            panic!("compressed buckets not read");
        };
        assert_eq!(read.buckets, t.buckets);
        for bad in wrong {
            assert!(super::read(TOPK_TYPE, 0, &mut &bad[..]).is_err());
        }
        for bad in damaged {
            if let Ok(Some(Sketch::TopK(read))) = super::read(TOPK_TYPE, 0, &mut &bad[..]) {
                assert_eq!(read.buckets.len(), 10);
            }
        }
    }

    const TOPK_DUMP: &str = "07814e8a4af9360f100002020203020104000000000000f03f05c30d180503ad4d230100e0070001000005c31e30010000e00d000403ad4d2305201a0898d02582407e000001200b20000000050100050668656c6c6f00000f005531a20539918c1c";

    #[test]
    fn bloom_module_id() {
        // As Redis 8.10 writes it after the type byte.
        assert_eq!(
            module_id(BLOOM_TYPE, 4).to_be_bytes(),
            hex("3016e5a289bef804")[..]
        );
    }

    /// `DUMP bf4` on Redis 8.10: `BF.INSERT bf4 CAPACITY 50 ERROR 0.05
    /// EXPANSION 3 ITEMS a b c`. Read, it is the filter KiviDB builds from
    /// the same command; written, it is the same bytes but the trailer.
    #[test]
    fn bloom_dump_from_redis() {
        let payload = hex(
            "07813016e5a289bef80402030201020502030232049a9999999999993f0206040bd81f7830b61e400241800200053010000000000001200040200000c2000000000008001000000000008400000000000400020000000020000080100000000203000f000ec4213c2476ee3a",
        );
        let body = &payload[1..payload.len() - 10];
        let mut r = body;
        let id = crate::snapshot::rdb_read_plain_length(&mut r).unwrap();
        assert_eq!(id, module_id(BLOOM_TYPE, 4));
        let Some(Sketch::Bloom(read)) = super::read(BLOOM_TYPE, 4, &mut r).unwrap() else {
            panic!("not a Bloom filter");
        };
        assert!(r.is_empty());
        let limits = Limits::structural();
        let mut ours = BloomChain::new(
            Params {
                capacity: 50,
                error: 0.05,
                expansion: 3,
                nonscaling: false,
            },
            limits,
        )
        .unwrap();
        for item in [b"a", b"b", b"c"] {
            ours.add(item, limits).unwrap();
        }
        assert_eq!(read, ours);
        let written = dump_payload(&Sketch::Bloom(ours)).unwrap();
        assert_eq!(written[..written.len() - 10], payload[..payload.len() - 10]);
        // Version 9, and the CRC Redis checks.
        let trailer = &written[written.len() - 10..];
        assert_eq!(&trailer[..2], &[9, 0]);
        let crc = crate::snapshot::redis_crc64(&written[..written.len() - 8]);
        assert_eq!(&trailer[2..], &crc.to_le_bytes());
    }
}
