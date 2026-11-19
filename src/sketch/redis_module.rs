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
use super::cuckoo::{Cuckoo, Shape, SubFilter};

/// The module type of Bloom filters, and the encoding version written.
pub const BLOOM_TYPE: &str = "MBbloom--";
const BLOOM_ENCVER: u64 = 4;
/// The module type of Cuckoo filters, and the encoding version written:
/// what Redis 8.10 writes.
pub const CUCKOO_TYPE: &str = "MBbloomCF";
const CUCKOO_ENCVER: u64 = 4;

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

/// A Cuckoo filter as Redis saves it (encoding version 4): sub-filters,
/// the first one's buckets, items, deletes, bucket size, kick limit and
/// expansion; then each sub-filter's buckets and bytes.
fn write_cuckoo(w: &mut Writer, cf: &Cuckoo) {
    w.uint(cf.filters.len() as u64);
    w.uint(cf.filters[0].buckets);
    w.uint(cf.items);
    w.uint(cf.deletes);
    w.uint(u64::from(cf.bucket_size));
    w.uint(u64::from(cf.max_iterations));
    w.uint(u64::from(cf.expansion));
    for f in &cf.filters {
        w.uint(f.buckets);
        w.string(&f.data);
    }
}

/// Whether `DUMP` writes `sketch` in Redis's format, which Redis can
/// `RESTORE`; the other types are dumped in KiviDB's own.
pub fn dumps_as_redis(sketch: &Sketch) -> bool {
    matches!(sketch, Sketch::Bloom(_) | Sketch::Cuckoo(_))
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
        // Type, module ID, seven fields, the end, version and CRC: under
        // 128. Each sub-filter: two fields and a string head, its bytes.
        Sketch::Cuckoo(cf) => 128 + cf.filters.len() as u64 * 32 + cf.data_bytes(),
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
        Sketch::Cuckoo(cf) => {
            w.len(module_id(CUCKOO_TYPE, CUCKOO_ENCVER));
            write_cuckoo(&mut w, cf);
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

/// The most bytes LZF makes of each one it reads: 264 from a back reference
/// of 3, at most.
const LZF_MOST_PER_BYTE: u64 = 88;

/// An RDB string of at most `max_len` bytes, plain or LZF-compressed: one
/// longer than its declared shape is refused by the length it declares
/// (and a compressed one by what its compressed bytes can make), before
/// anything of that length is reserved; only the bytes the stream holds are
/// read.
fn rdb_read_string_at_most(r: &mut impl Read, max_len: u64) -> io::Result<Vec<u8>> {
    let longer = || super::invalid("a string longer than its declared shape");
    let (n, encoded) = rdb_length(r)?;
    if !encoded {
        if n > max_len {
            return Err(longer());
        }
        return super::read_vec(r, usize::try_from(n).map_err(|_| longer())?);
    }
    if n != RDB_ENC_LZF {
        return Err(super::invalid("a string of an unexpected encoding"));
    }
    let (compressed, _) = rdb_length(r)?;
    let (original, _) = rdb_length(r)?;
    if original > max_len {
        return Err(longer());
    }
    // Redis compresses a string only if that makes it shorter.
    if compressed == 0
        || compressed >= original
        || original > compressed.saturating_mul(LZF_MOST_PER_BYTE)
    {
        return Err(super::invalid("a compressed string of a bad length"));
    }
    let data = super::read_vec(r, compressed as usize)?;
    match lzf::decompress(&data, original as usize) {
        Ok(out) if out.len() as u64 == original => Ok(out),
        _ => Err(super::invalid(
            "a compressed string that does not decompress",
        )),
    }
}

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
    /// writes one (`mismatch` if it is another length): no longer than
    /// that is read or reserved ([`rdb_read_string_at_most`]).
    fn string_of(&mut self, len: u64, mismatch: &str) -> io::Result<Vec<u8>> {
        self.opcode(OPCODE_STRING)?;
        match rdb_read_string_at_most(self.r, len) {
            Ok(s) if s.len() as u64 == len => Ok(s),
            Ok(_) => Err(self.bad(mismatch)),
            Err(e) if e.kind() == io::ErrorKind::InvalidData => Err(self.bad(mismatch)),
            Err(e) => Err(e),
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

/// A Cuckoo filter [`write_cuckoo`] (or Redis) wrote: its shape is checked
/// against the size limits before any sub-filter's bytes are read, and
/// each sub-filter must have the buckets its place in the chain gives and
/// exactly their bytes.
fn read_cuckoo(r: &mut Reader<'_, impl Read>) -> io::Result<Cuckoo> {
    let filters = r.uint()?;
    let buckets = r.uint()?;
    let items = r.uint()?;
    let deletes = r.uint()?;
    let to16 = |v: u64| u16::try_from(v).ok();
    let bucket_size = to16(r.uint()?).ok_or_else(|| r.bad("a field is out of range"))?;
    let max_iterations = to16(r.uint()?).ok_or_else(|| r.bad("a field is out of range"))?;
    let expansion = to16(r.uint()?).ok_or_else(|| r.bad("a field is out of range"))?;
    let shape = Shape {
        items,
        buckets,
        deletes,
        filters,
        bucket_size,
        max_iterations,
        expansion,
    };
    shape
        .check()
        .map_err(|_| r.bad("a filter of this shape is not supported"))?;
    let mut subs = Vec::with_capacity(filters as usize);
    let mut want = buckets;
    for _ in 0..filters {
        let n = r.uint()?;
        if n != want {
            return Err(r.bad("a sub-filter's buckets do not follow from the first's"));
        }
        let data = r.string_of(
            n * u64::from(bucket_size),
            "a sub-filter's bytes do not match its buckets",
        )?;
        subs.push(SubFilter {
            buckets: n,
            data: std::sync::Arc::new(data.into_boxed_slice()),
        });
        want = want.saturating_mul(u64::from(expansion.max(1)));
    }
    r.end()?;
    Ok(Cuckoo {
        items,
        deletes,
        bucket_size,
        max_iterations,
        expansion,
        filters: subs,
    })
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
        CUCKOO_TYPE => {
            if encver != CUCKOO_ENCVER {
                return Err(super::invalid(&format!(
                    "Cuckoo filter encoding version {encver} is not supported"
                )));
            }
            let mut reader = Reader {
                r,
                what: "Cuckoo filter",
            };
            Ok(Some(Sketch::Cuckoo(read_cuckoo(&mut reader)?)))
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

    /// `DUMP d` on Redis 8.10.2 after `CF.RESERVE d 24 BUCKETSIZE 3
    /// EXPANSION 4 MAXITERATIONS 7`, 40 `CF.ADD`s of `x0` to `x39` and `CF.DEL
    /// d x1`: read, it is the filter KiviDB builds from the same commands;
    /// written, the same bytes but the trailer.
    #[test]
    fn cuckoo_dump_from_redis() {
        let payload = hex(CUCKOO_DUMP);
        let body = &payload[1..payload.len() - 10];
        let mut r = body;
        let id = crate::snapshot::rdb_read_plain_length(&mut r).unwrap();
        assert_eq!(id, module_id(CUCKOO_TYPE, 4));
        let Some(Sketch::Cuckoo(read)) = super::read(CUCKOO_TYPE, 4, &mut r).unwrap() else {
            panic!("not a filter");
        };
        assert!(r.is_empty());
        let mut ours = Cuckoo::new(super::super::cuckoo::Params {
            capacity: 24,
            bucket_size: 3,
            max_iterations: 7,
            expansion: 4,
        })
        .unwrap();
        for i in 0..40 {
            ours.insert(super::super::cuckoo::lookup(format!("x{i}").as_bytes()), 32)
                .unwrap();
        }
        ours.delete(super::super::cuckoo::lookup(b"x1")).unwrap();
        assert_eq!(read, ours);
        let written = dump_payload(&Sketch::Cuckoo(ours)).unwrap();
        let mut back = &written[1..written.len() - 10];
        crate::snapshot::rdb_read_plain_length(&mut back).unwrap();
        let Some(Sketch::Cuckoo(again)) = super::read(CUCKOO_TYPE, 4, &mut back).unwrap() else {
            panic!("not a filter");
        };
        assert_eq!(again, read);
    }

    /// Corrupted Cuckoo values are refused, or read as a filter within the
    /// limits; never a panic.
    #[test]
    fn corrupt_cuckoo_values_are_refused_cleanly() {
        let good = hex(CUCKOO_DUMP);
        let good = &good[10..good.len() - 10];
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..4000 {
            let mut bad = good.to_vec();
            match next() % 3 {
                0 => {
                    let at = next() as usize % bad.len();
                    bad[at] = next() as u8;
                }
                1 => bad.truncate(next() as usize % bad.len()),
                _ => {
                    let mut w = Writer { out: Vec::new() };
                    for _ in 0..7 {
                        w.uint(next() >> (next() % 64));
                    }
                    w.uint(next() >> (next() % 64));
                    w.string(&[0u8; 16]);
                    w.len(OPCODE_EOF);
                    bad = w.out;
                }
            }
            if let Ok(Some(Sketch::Cuckoo(read))) = super::read(CUCKOO_TYPE, 4, &mut &bad[..]) {
                assert!(read.data_bytes() <= super::super::cuckoo::MAX_BYTES);
                assert!(!read.filters.is_empty());
            }
        }
        // A sub-filter's bytes LZF-compressed, as Redis writes them.
        let mut cf = Cuckoo::new(super::super::cuckoo::Params {
            capacity: 1024,
            bucket_size: 2,
            max_iterations: 20,
            expansion: 1,
        })
        .unwrap();
        for i in 0..20 {
            let l = super::super::cuckoo::lookup(format!("{i}").as_bytes());
            cf.insert(l, 8).unwrap();
        }
        let mut w = Writer { out: Vec::new() };
        write_cuckoo(&mut w, &cf);
        w.len(OPCODE_EOF);
        let plain = w.out;
        let mut w = Writer { out: Vec::new() };
        for v in [
            cf.filters.len() as u64,
            cf.filters[0].buckets,
            cf.items,
            cf.deletes,
            u64::from(cf.bucket_size),
            u64::from(cf.max_iterations),
            u64::from(cf.expansion),
            cf.filters[0].buckets,
        ] {
            w.uint(v);
        }
        let (compressed, wrong, damaged) = lzf_variants(&plain, w.out.len(), &mut next);
        let Ok(Some(Sketch::Cuckoo(read))) = super::read(CUCKOO_TYPE, 4, &mut &compressed[..])
        else {
            panic!("compressed filter not read");
        };
        assert_eq!(read, cf);
        for bad in wrong {
            assert!(super::read(CUCKOO_TYPE, 4, &mut &bad[..]).is_err());
        }
        for bad in damaged {
            if let Ok(Some(Sketch::Cuckoo(read))) = super::read(CUCKOO_TYPE, 4, &mut &bad[..]) {
                assert_eq!(read.data_bytes(), cf.data_bytes());
            }
        }
    }

    const CUCKOO_DUMP: &str = "07813016e5a289821404020202080227020102030207020402080518ce62a000e214c07bc5b2287cfc65b328da864700003f12ac022005c33c4060010000e00400005c600d004d6005e006000036e0050f0ac200001d0000b5fbdef830401803a10000cf40070b00ea14007200005d00001cb1200d0000000f0038111f3ba6bb5b4f";

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
