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
use super::bloom::{BloomChain, FLAG_EXACT_SIZE, FLAG_WIDE_HASH, LayerShape, ShapeError};

/// The module type of Bloom filters, and the encoding version written.
pub const BLOOM_TYPE: &str = "MBbloom--";
const BLOOM_ENCVER: u64 = 4;

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

/// `sketch` as a Redis `DUMP` payload: the module value, the RDB version
/// and Redis's CRC64 over both.
pub fn dump_payload(sketch: &Sketch) -> Vec<u8> {
    let mut w = Writer { out: Vec::new() };
    w.out.push(RDB_TYPE_MODULE_2);
    match sketch {
        Sketch::Bloom(chain) => {
            w.len(module_id(BLOOM_TYPE, BLOOM_ENCVER));
            write_bloom(&mut w, chain);
        }
    }
    w.len(OPCODE_EOF);
    let mut out = w.out;
    out.extend_from_slice(&DUMP_RDB_VERSION.to_le_bytes());
    let crc = crate::snapshot::redis_crc64(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

// ── Reading ─────────────────────────────────────────────────────────────

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

    fn end(&mut self) -> io::Result<()> {
        self.opcode(OPCODE_EOF)
    }
}

fn read_bloom(r: &mut Reader<'_, impl Read>) -> io::Result<BloomChain> {
    let items = r.uint()?;
    let count = r.uint()?;
    let flags = r.uint32()?;
    let expansion = r.uint32()?;
    if count == 0 || count > 1 << 16 {
        return Err(r.bad("bad number of layers"));
    }
    let mut shapes = Vec::with_capacity(count as usize);
    let mut datas = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let capacity = r.uint()?;
        let error = r.double()?;
        let hashes = r.uint32()?;
        let bits_per_item = r.double()?;
        let bits = r.uint()?;
        let reserved = r.uint()?;
        let data = r.string()?;
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
    // `maxmemory` is not consulted (a file or a full sync is taken whole).
    let wide = FLAG_EXACT_SIZE | FLAG_WIDE_HASH;
    match BloomChain::from_shapes(items, flags, expansion, shapes, Some(datas)) {
        Ok(chain) => Ok(chain),
        Err(ShapeError::TooLarge) => Err(r.bad("a layer is too large to load")),
        Err(ShapeError::Bad) if flags & wide != wide => Err(r.bad(
            "a filter made before RedisBloom 2.0 (32-bit hashes) is not supported; recreate it",
        )),
        Err(ShapeError::Bad) => Err(r.bad("the layers do not fit together")),
    }
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
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sketch::bloom::{BloomChain, Params};

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

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
        let mut ours = BloomChain::new(Params {
            capacity: 50,
            error: 0.05,
            expansion: 3,
            nonscaling: false,
        })
        .unwrap();
        for item in [b"a", b"b", b"c"] {
            ours.add(item).unwrap();
        }
        assert_eq!(read, ours);
        let written = dump_payload(&Sketch::Bloom(ours));
        assert_eq!(written[..written.len() - 10], payload[..payload.len() - 10]);
        // Version 9, and the CRC Redis checks.
        let trailer = &written[written.len() - 10..];
        assert_eq!(&trailer[..2], &[9, 0]);
        let crc = crate::snapshot::redis_crc64(&written[..written.len() - 8]);
        assert_eq!(&trailer[2..], &crc.to_le_bytes());
    }
}
