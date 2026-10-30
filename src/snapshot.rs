use crate::store;
use crate::store::Key;
use crate::store::ValueRef;
use crate::types::SharedBytes;
use crate::types::{
    ConsumerGroup, KiviDBValue, PelEntry, SortedSetInner, StreamEntry, StreamId, StreamInner,
    TsInner,
};
use crate::types::{VSetEntry, VectorSetInner};
use crate::vector::{DistanceMetric, FieldType, IndexOn, IndexSchema, SchemaField, VectorAlgo};
use crate::write_lock;
use ahash::AHashMap;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::sync::Arc;

/// Returned by `load_snapshot` — key-value data plus any index schemas that
/// were embedded in the snapshot file.
pub struct LoadedSnapshot {
    pub dbs: Vec<HashMap<String, KiviDBValue>>,
    /// Index schemas serialised by the previous run. May be empty when loading
    /// an older snapshot that predates index persistence.
    pub index_schemas: Vec<IndexSchema>,
    /// The dictionaries (`FT.DICTADD`): name, terms.
    pub dictionaries: Vec<(String, Vec<String>)>,
    /// HNSW graphs saved in the snapshot (indexed by "index_name" →
    /// field_name → Arc<HnswIndex>). When present for a given field, backfill
    /// is skipped and the saved graph is used directly, preserving precision.
    pub hnsw_graphs: std::collections::HashMap<
        String,
        std::collections::HashMap<String, std::sync::Arc<crate::vector::HnswIndex>>,
    >,
    /// Key expiries, one map per database: key → absolute Unix milliseconds.
    /// Keys whose expiry had already passed at load time are dropped from
    /// `dbs` instead of appearing here. Empty maps for an image written before
    /// expiries were persisted.
    pub expiries: Vec<HashMap<String, u64>>,
    /// The function libraries, as a `FUNCTION DUMP` payload: `None` for an
    /// image that holds none, or one written before they were kept. Loaded
    /// with [`restore_functions`].
    pub functions: Option<Vec<u8>>,
}

/// Per-database key expiries as absolute Unix milliseconds.
pub type ExpiryMaps = Vec<HashMap<String, u64>>;

/// Magic opening the key-expiry section of a KDB image.
const KDB_EXPIRY_MAGIC: &[u8; 8] = b"KIVIEX1\0";
const KDB_INDEX_MAGIC: &[u8; 8] = b"KIVIDX1\0";
const KDB_HNSW_MAGIC: &[u8; 8] = b"KIVIHN1\0";
/// Index aliases. Written last, after the expiry section: a build that does
/// not know this section stops scanning when it meets it, and must already
/// have read everything it does know.
const KDB_ALIAS_MAGIC: &[u8; 8] = b"KIVIAL1\0";
/// What an index's schema says beyond field names and types: `AS` names,
/// `SORTABLE`, `SEPARATOR`, `WEIGHT`, … and the index's `STOPWORDS`. After
/// the alias section, for the same reason that one is after the expiries.
const KDB_FIELD_OPTIONS_MAGIC: &[u8; 8] = b"KIVIFO1\0";
/// Synonym groups of each index, and the dictionaries. After the field
/// options: an older release stops reading at a section it does not know.
const KDB_SYNONYMS_MAGIC: &[u8; 8] = b"KIVISD1\0";
/// The element type of each vector field that is not FLOAT32. Last, for the
/// same reason as the synonyms: the schema section has no room for it, and
/// an older release that stops before it reads the field as FLOAT32.
const KDB_VECTOR_TYPES_MAGIC: &[u8; 8] = b"KIVIVT1\0";
/// The function libraries, as a `FUNCTION DUMP` payload. Last, after the
/// vector types: an older release reads everything else and stops here, as
/// it did before libraries were kept.
const KDB_FUNCTIONS_MAGIC: &[u8; 8] = b"KIVIFN1\0";

// Field-type tags used inside the index schema section.
const SCHEMA_FIELD_TEXT: u8 = 0;
const SCHEMA_FIELD_TAG: u8 = 1;
const SCHEMA_FIELD_NUMERIC: u8 = 2;
const SCHEMA_FIELD_VECTOR: u8 = 3;

const VECTOR_ALGO_FLAT: u8 = 0;
const VECTOR_ALGO_HNSW: u8 = 1;

const TAG_STRING: u8 = 1;
const TAG_LIST: u8 = 2;
const TAG_HASH: u8 = 3;
const TAG_SET: u8 = 4;
const TAG_SORTEDSET: u8 = 5;
const TAG_STREAM: u8 = 6;
const TAG_JSON: u8 = 7;
const TAG_TIMESERIES: u8 = 8;
const TAG_VECTORSET: u8 = 9;
const TAG_HASH_EX: u8 = 10;
const TAG_ARRAY: u8 = 11;
/// Stream entries + last_id, plus max_deleted_id, entries_added, max_entries,
/// and consumer groups (PEL + consumers). `TAG_STREAM` remains readable for
/// dumps written before groups were persisted.
const TAG_STREAM_V2: u8 = 12;
/// `TAG_STREAM_V2` followed by the stream's idempotent-production state.
/// Written only for streams that have any, so a dump that never used
/// `XADD … IDMP` or `XCFGSET` stays readable by older builds.
const TAG_STREAM_V3: u8 = 13;
/// `TAG_TIMESERIES` followed by the series' duplicate policy and compaction
/// rules. Written only for a series that has a non-default policy or any
/// rules, so dumps without them stay readable by older builds.
const TAG_TIMESERIES_V2: u8 = 14;
/// `TAG_TIMESERIES_V2` followed by the series it is a compaction of. Written
/// only for a compaction, so other series stay readable by older builds.
const TAG_TIMESERIES_V3: u8 = 15;

fn write_bytes(w: &mut impl Write, data: &[u8]) -> io::Result<()> {
    w.write_all(&(data.len() as u32).to_le_bytes())?;
    w.write_all(data)
}

fn read_bytes(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    read_exactly(r, len)
}

/// How much room to make ahead for `count` things an image says follow.
///
/// An image is not always this server's own: `RESTORE` takes one from a
/// client, and its checksum is no guard against one made up to lie. So a count
/// is trusted only as far as this, and a container grows past it as what it
/// holds is actually read: an image that claims four billion elements and
/// stops runs out of input long before it runs out of memory.
fn hint(count: usize) -> usize {
    count.min(1 << 16)
}

/// `len` bytes. Room for the lot is made at once only for a length that is
/// small; past that it grows with what is read (see [`hint`]).
fn read_exactly(r: &mut impl Read, len: usize) -> io::Result<Vec<u8>> {
    const AT_ONCE: usize = 1 << 20;
    if len <= AT_ONCE {
        let mut buf = vec![0u8; len];
        r.read_exact(&mut buf)?;
        return Ok(buf);
    }
    let mut buf = Vec::with_capacity(AT_ONCE);
    Read::take(&mut *r, len as u64).read_to_end(&mut buf)?;
    if buf.len() != len {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    Ok(buf)
}

fn write_stream_id(w: &mut impl Write, id: &StreamId) -> io::Result<()> {
    w.write_all(&id.ms.to_le_bytes())?;
    w.write_all(&id.seq.to_le_bytes())
}

fn read_stream_id(r: &mut impl Read) -> io::Result<StreamId> {
    let mut buf = [0u8; 16];
    r.read_exact(&mut buf)?;
    Ok(StreamId::new(
        u64::from_le_bytes(buf[..8].try_into().unwrap()),
        u64::from_le_bytes(buf[8..].try_into().unwrap()),
    ))
}

fn read_u32_le(r: &mut impl Read) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64_le(r: &mut impl Read) -> io::Result<u64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

fn write_stream_entries(w: &mut impl Write, inner: &StreamInner) -> io::Result<()> {
    w.write_all(&(inner.entries.len() as u32).to_le_bytes())?;
    for (id, entry) in inner.entries.iter() {
        write_stream_id(w, id)?;
        w.write_all(&(entry.fields.len() as u32).to_le_bytes())?;
        for (field, val) in entry.fields.iter() {
            write_bytes(w, field)?;
            write_bytes(w, val)?;
        }
    }
    write_stream_id(w, &inner.last_id)
}

fn write_stream_groups(w: &mut impl Write, inner: &StreamInner) -> io::Result<()> {
    write_stream_id(w, &inner.max_deleted_id)?;
    w.write_all(&inner.entries_added.to_le_bytes())?;
    w.write_all(&inner.max_entries.unwrap_or(0).to_le_bytes())?;
    w.write_all(&(inner.groups.len() as u32).to_le_bytes())?;
    for (name, g) in inner.groups.iter() {
        write_bytes(w, name.as_bytes())?;
        write_stream_id(w, &g.last_id)?;
        w.write_all(&g.entries_read.to_le_bytes())?;
        w.write_all(&(g.pel.len() as u32).to_le_bytes())?;
        for (id, pel) in g.pel.iter() {
            write_stream_id(w, id)?;
            write_bytes(w, pel.consumer.as_bytes())?;
            w.write_all(&pel.delivered_at.to_le_bytes())?;
            w.write_all(&pel.delivery_count.to_le_bytes())?;
        }
        w.write_all(&(g.consumers.len() as u32).to_le_bytes())?;
        for (cname, last_seen) in g.consumers.iter() {
            write_bytes(w, cname.as_bytes())?;
            w.write_all(&last_seen.to_le_bytes())?;
        }
    }
    Ok(())
}

fn write_stream_value(w: &mut impl Write, inner: &StreamInner) -> io::Result<()> {
    if inner.idmp.is_unused() {
        w.write_all(&[TAG_STREAM_V2])?;
        write_stream_entries(w, inner)?;
        return write_stream_groups(w, inner);
    }
    w.write_all(&[TAG_STREAM_V3])?;
    write_stream_entries(w, inner)?;
    write_stream_groups(w, inner)?;
    write_stream_idmp(w, inner)
}

/// A retry that arrives after a restart or a failover has to be recognised
/// just as it would have been before, so what a stream remembers about its
/// producers is part of its value. `0` stands for "not set" in the two
/// settings; their valid ranges start at 1.
fn write_stream_idmp(w: &mut impl Write, inner: &StreamInner) -> io::Result<()> {
    let idmp = &inner.idmp;
    w.write_all(&idmp.duration_secs.unwrap_or(0).to_le_bytes())?;
    w.write_all(&idmp.maxsize.unwrap_or(0).to_le_bytes())?;
    w.write_all(&idmp.iids_added.to_le_bytes())?;
    w.write_all(&idmp.iids_duplicates.to_le_bytes())?;
    w.write_all(&(idmp.producers.len() as u32).to_le_bytes())?;
    for (producer, state) in idmp.producers.iter() {
        write_bytes(w, producer)?;
        w.write_all(&(state.order.len() as u32).to_le_bytes())?;
        for record in state.order.iter() {
            write_bytes(w, &record.iid)?;
            write_stream_id(w, &record.entry_id)?;
            w.write_all(&record.added_ms.to_le_bytes())?;
        }
    }
    Ok(())
}

fn read_stream_idmp(r: &mut impl Read, stream: &mut StreamInner) -> io::Result<()> {
    let nonzero = |n: u64| if n == 0 { None } else { Some(n) };
    stream.idmp.duration_secs = nonzero(read_u64_le(r)?);
    stream.idmp.maxsize = nonzero(read_u64_le(r)?);
    stream.idmp.iids_added = read_u64_le(r)?;
    stream.idmp.iids_duplicates = read_u64_le(r)?;
    let producers = read_u32_le(r)? as usize;
    for _ in 0..producers {
        let producer: Arc<[u8]> = Arc::from(read_bytes(r)?.as_slice());
        let records = read_u32_le(r)? as usize;
        let mut state = crate::types::ProducerIdmp::default();
        for _ in 0..records {
            let iid: Arc<[u8]> = Arc::from(read_bytes(r)?.as_slice());
            let entry_id = read_stream_id(r)?;
            let added_ms = read_u64_le(r)?;
            state.by_iid.insert(Arc::clone(&iid), entry_id.clone());
            state.order.push_back(crate::types::IdmpRecord {
                iid,
                entry_id,
                added_ms,
            });
        }
        stream.idmp.producers.insert(producer, state);
    }
    Ok(())
}

fn read_stream_entries(r: &mut impl Read) -> io::Result<StreamInner> {
    let count = read_u32_le(r)? as usize;
    let mut stream = StreamInner::new();
    for _ in 0..count {
        let id = read_stream_id(r)?;
        let fcount = read_u32_le(r)? as usize;
        let mut fields = Vec::with_capacity(hint(fcount));
        for _ in 0..fcount {
            let f = read_bytes(r)?;
            let v = read_bytes(r)?;
            fields.push((Arc::from(f.as_slice()), Arc::from(v.as_slice())));
        }
        stream.entries.insert(id, StreamEntry { fields });
    }
    stream.last_id = read_stream_id(r)?;
    Ok(stream)
}

fn read_stream_groups(r: &mut impl Read, stream: &mut StreamInner) -> io::Result<()> {
    stream.max_deleted_id = read_stream_id(r)?;
    stream.entries_added = read_u64_le(r)?;
    let max_entries = read_u64_le(r)?;
    stream.max_entries = if max_entries == 0 {
        None
    } else {
        Some(max_entries)
    };
    let gcount = read_u32_le(r)? as usize;
    stream.groups.reserve(hint(gcount));
    for _ in 0..gcount {
        let name_bytes = read_bytes(r)?;
        let name = String::from_utf8(name_bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad stream group name"))?;
        let last_id = read_stream_id(r)?;
        let entries_read = read_u64_le(r)?;
        let pel_count = read_u32_le(r)? as usize;
        let mut pel = std::collections::BTreeMap::new();
        for _ in 0..pel_count {
            let id = read_stream_id(r)?;
            let consumer_bytes = read_bytes(r)?;
            let consumer = String::from_utf8(consumer_bytes).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "bad stream PEL consumer")
            })?;
            let delivered_at = read_u64_le(r)?;
            let delivery_count = read_u64_le(r)?;
            pel.insert(
                id,
                PelEntry {
                    consumer,
                    delivered_at,
                    delivery_count,
                },
            );
        }
        let ccount = read_u32_le(r)? as usize;
        let mut consumers = AHashMap::with_capacity(hint(ccount));
        for _ in 0..ccount {
            let cname_bytes = read_bytes(r)?;
            let cname = String::from_utf8(cname_bytes).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "bad stream consumer name")
            })?;
            let last_seen = read_u64_le(r)?;
            consumers.insert(cname, last_seen);
        }
        stream.groups.insert(
            name.clone(),
            ConsumerGroup {
                name,
                last_id,
                entries_read,
                pel,
                consumers,
                active: Default::default(),
            },
        );
    }
    Ok(())
}

fn read_stream_value(r: &mut impl Read, tag: u8) -> io::Result<KiviDBValue> {
    let mut stream = read_stream_entries(r)?;
    if tag == TAG_STREAM_V2 || tag == TAG_STREAM_V3 {
        read_stream_groups(r, &mut stream)?;
    }
    if tag == TAG_STREAM_V3 {
        read_stream_idmp(r, &mut stream)?;
    }
    Ok(KiviDBValue::Stream(Arc::new(parking_lot::RwLock::new(
        stream,
    ))))
}

macro_rules! read_lock {
    ($shard:expr) => {
        $shard.read()
    };
}

/// Leading bytes of every KDB image, snapshot file or AOF preamble alike.
pub const KDB_MAGIC: &[u8; 8] = b"KIVIDB01";

// ── Streaming KDB writer ─────────────────────────────────────────────────────
//
// Split out of `write_snapshot` so the AOF rewrite can emit the same image
// without cloning the dataset into intermediate `HashMap`s first, which
// would double peak memory during a rewrite. Driving the writer shard by
// shard costs nothing extra.
//
// The pieces must be called in this order — header, then per db (entries, db
// end), then the index sections — because that is the order `load_snapshot`
// reads them in.

/// Magic plus the number of database sections that follow.
pub fn write_kdb_header(w: &mut impl Write, db_count: u32) -> io::Result<()> {
    w.write_all(KDB_MAGIC)?;
    w.write_all(&db_count.to_le_bytes())
}

/// Zero-length key: the sentinel that closes a database section.
pub fn write_kdb_db_end(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&0u32.to_le_bytes())
}

/// One key/value pair — the key, a type tag, then the value's own encoding.
pub fn write_kdb_entry(
    w: &mut impl Write,
    key: &str,
    value: crate::store::ValueRef<'_>,
) -> io::Result<()> {
    // Write key first (shared by all types)
    write_bytes(w, key.as_bytes())?;

    match value {
        ValueRef::String(bytes) => {
            w.write_all(&[TAG_STRING])?;
            write_bytes(w, &bytes)?;
        }

        ValueRef::List(arc) => {
            w.write_all(&[TAG_LIST])?;
            let inner = arc.read();
            // u32 element count
            w.write_all(&(inner.len() as u32).to_le_bytes())?;
            for item in inner.iter() {
                write_bytes(w, item)?;
            }
        }

        ValueRef::Hash(arc) => {
            w.write_all(&[TAG_HASH])?;
            let inner = arc.read();
            w.write_all(&(inner.len() as u32).to_le_bytes())?;
            for (field, val) in inner.iter() {
                write_bytes(w, field.as_bytes())?;
                write_bytes(w, val)?;
            }
        }

        ValueRef::Set(arc) => {
            w.write_all(&[TAG_SET])?;
            let inner = arc.read();
            w.write_all(&(inner.len() as u32).to_le_bytes())?;
            for member in inner.iter() {
                write_bytes(w, member)?;
            }
        }

        ValueRef::SortedSet(arc) => {
            w.write_all(&[TAG_SORTEDSET])?;
            let inner = arc.read();
            // Iterate by_score so order is preserved on reload (nice-to-have)
            w.write_all(&(inner.members.len() as u32).to_le_bytes())?;
            for (score, member) in inner.by_score.keys() {
                write_bytes(w, member)?;
                w.write_all(&score.to_bits().to_le_bytes())?; // f64 as raw bits — lossless
            }
        }

        ValueRef::Stream(arc) => {
            write_stream_value(w, &arc.read())?;
        }

        ValueRef::Json(arc) => {
            w.write_all(&[TAG_JSON])?;
            let inner = arc.read();
            // serde_json::to_vec is the fastest compact serialiser you already depend on
            let json_bytes = serde_json::to_vec(&*inner).unwrap_or_else(|_| b"null".to_vec());
            write_bytes(w, &json_bytes)?;
        }

        ValueRef::TimeSeries(arc) => {
            let inner = arc.read();
            // The duplicate policy and the compaction rules were not saved at
            // all: after a restart (or on a replica, after a full sync) every
            // series was back on the default policy and no longer fed its
            // compactions — silently.
            let extended = !inner.rules.is_empty()
                || inner.duplicate_policy != crate::types::DuplicatePolicy::Last
                || inner.source.is_some();
            w.write_all(&[if inner.source.is_some() {
                TAG_TIMESERIES_V3
            } else if extended {
                TAG_TIMESERIES_V2
            } else {
                TAG_TIMESERIES
            }])?;
            w.write_all(&(inner.samples.len() as u32).to_le_bytes())?;
            for (ts, val) in inner.samples.iter() {
                w.write_all(&ts.to_le_bytes())?;
                w.write_all(&val.to_bits().to_le_bytes())?;
            }
            // Persist metadata
            w.write_all(&inner.retention.to_le_bytes())?;
            // A u64 on every target, so a 32-bit build reads a 64-bit one's
            // snapshot (and compiles: usize is 4 bytes there).
            w.write_all(&(inner.chunk_size as u64).to_le_bytes())?;
            // Labels: write as count + (key, value) pairs
            w.write_all(&(inner.labels.len() as u32).to_le_bytes())?;
            for (lk, lv) in inner.labels.iter() {
                write_bytes(w, lk.as_bytes())?;
                write_bytes(w, lv.as_bytes())?;
            }
            if extended {
                write_bytes(w, inner.duplicate_policy.as_str().as_bytes())?;
                w.write_all(&(inner.rules.len() as u32).to_le_bytes())?;
                for rule in inner.rules.iter() {
                    write_bytes(w, rule.dest_key.as_bytes())?;
                    write_bytes(w, rule.aggregation.name().as_bytes())?;
                    w.write_all(&rule.bucket_duration.to_le_bytes())?;
                    w.write_all(&rule.align_timestamp.to_le_bytes())?;
                }
            }
            if let Some(source) = &inner.source {
                write_bytes(w, source.as_bytes())?;
            }
        }

        ValueRef::VectorSet(arc) => {
            w.write_all(&[TAG_VECTORSET])?;
            let inner = arc.read();
            w.write_all(&(inner.dim as u32).to_le_bytes())?;

            // Persist metric as a single byte
            let metric_byte: u8 = match inner.metric {
                crate::vector::DistanceMetric::Cosine => 0,
                crate::vector::DistanceMetric::L2 => 1,
                crate::vector::DistanceMetric::InnerProduct => 2,
            };
            w.write_all(&[metric_byte])?;

            w.write_all(&(inner.entries.len() as u32).to_le_bytes())?;

            for (name, entry) in inner.entries.iter() {
                // Element name
                write_bytes(w, name.as_bytes())?;

                // Vector: write count then raw f32 LE bytes
                let vec_len = entry.vector.len() as u32;
                w.write_all(&vec_len.to_le_bytes())?;
                for f in &entry.vector {
                    w.write_all(&f.to_le_bytes())?;
                }

                // Optional attrs JSON
                match &entry.attrs {
                    Some(s) => {
                        w.write_all(&[1u8])?;
                        write_bytes(w, s.as_bytes())?;
                    }
                    None => {
                        w.write_all(&[0u8])?;
                    }
                }
            }
        }

        ValueRef::HashEx(arc) => {
            w.write_all(&[TAG_HASH_EX])?;
            let inner = arc.read();
            w.write_all(&(inner.fields.len() as u32).to_le_bytes())?;
            for (field, val) in inner.fields.iter() {
                write_bytes(w, field.as_bytes())?;
                write_bytes(w, val)?;
            }
            // Expiries: u32 count followed by (field, unix_ms) pairs.
            // We persist as Unix ms because Instant is monotonic and
            // can't survive a process restart — convert at write time.
            w.write_all(&(inner.expiries.len() as u32).to_le_bytes())?;
            let now_sys = std::time::SystemTime::now();
            let now_mono = std::time::Instant::now();
            let unix_now_ms = now_sys
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            for (field, exp) in inner.expiries.iter() {
                let delta_ms = exp.saturating_duration_since(now_mono).as_millis() as u64;
                let abs_ms = unix_now_ms.saturating_add(delta_ms);
                write_bytes(w, field.as_bytes())?;
                w.write_all(&abs_ms.to_le_bytes())?;
            }
        }

        ValueRef::Array(arc) => {
            w.write_all(&[TAG_ARRAY])?;
            let inner = arc.read();
            // Sparse: (index, value) pairs, not a dense run — an array
            // with one element at index 10^9 must not cost 10^9 slots
            // on disk.
            w.write_all(&(inner.entries.len() as u32).to_le_bytes())?;
            for (idx, val) in inner.entries.iter() {
                w.write_all(&idx.to_le_bytes())?;
                write_bytes(w, val)?;
            }
            // Cursor and ring window are part of the value's observable
            // state (ARNEXT / ARRING), so they have to survive a reload.
            // ring_size 0 encodes "not a ring" — a 0-size ring is
            // rejected at the command layer, so it's free as a sentinel.
            w.write_all(&inner.next_insert.to_le_bytes())?;
            w.write_all(&inner.ring_size.unwrap_or(0).to_le_bytes())?;
        }
    }
    Ok(())
}

/// Index schema and HNSW graph sections, which close a KDB image.
///
/// Both are optional and self-announcing, so a reader that stops after the
/// database sections simply hits EOF and never sees them.
pub fn write_kdb_index_sections(
    w: &mut impl Write,
    index_store: Option<&crate::vector::IndexStore>,
) -> io::Result<()> {
    // ── Index schema section (optional, appended after all DB data) ───────────
    // Magic: b"KIVIDX1\0" (8 bytes) so old readers that stop after db_count
    // databases just hit EOF — they never read these bytes.
    let Some(store) = index_store else {
        return Ok(());
    };
    let guard = store.read();
    if guard.is_empty() {
        return Ok(());
    }

    // Indexes only; their aliases have a section of their own.
    let defs: Vec<&crate::vector::IndexDef> = guard
        .iter()
        .filter(|(key, def)| !crate::vector::is_alias(key, def))
        .map(|(_, def)| def)
        .collect();
    if defs.is_empty() {
        return Ok(());
    }
    w.write_all(KDB_INDEX_MAGIC)?;
    w.write_all(&(defs.len() as u32).to_le_bytes())?;
    for def in &defs {
        write_index_schema(w, def)?;
    }

    // ── HNSW graph section (appended after index schemas) ──────────
    // Magic: b"KIVIHN1\0". Old readers encounter EOF after KIVIDX1 —
    // they never reach this magic and gracefully fall back to backfill.
    let mut hnsw_indexes: Vec<(&str, &str, &crate::vector::HnswIndex)> = Vec::new();
    for def in &defs {
        for (field_name, live) in &def.indexes {
            if let crate::vector::LiveIndex::Hnsw(arc) = live {
                hnsw_indexes.push((&def.name, field_name.as_str(), arc.as_ref()));
            }
        }
    }
    if !hnsw_indexes.is_empty() {
        w.write_all(KDB_HNSW_MAGIC)?;
        w.write_all(&(hnsw_indexes.len() as u32).to_le_bytes())?;
        for (index_name, field_name, hnsw) in &hnsw_indexes {
            write_hnsw_graph(w, index_name, field_name, hnsw)?;
        }
    }
    Ok(())
}

/// Key-expiry section, which closes a KDB image.
///
/// Until this existed a KDB image carried values only. `SAVE` followed by a
/// restart, and every replica full sync, therefore turned each expiring key
/// into a permanent one: a session store reloaded with every session immortal.
///
/// Times are absolute Unix milliseconds — the same form `PEXPIREAT`
/// replicates — so they mean the same thing on whichever node loads them.
/// They are taken from the keys' own entries as the dataset is captured
/// ([`freeze`]), so an expiry cannot outlive its key into an image.
///
/// It is written *last*, after the index sections, deliberately: a build that
/// predates it reads the sections it knows and then stops, so a dump written
/// by this build still loads there (without TTLs, as before) if a release has
/// to be rolled back.
///
/// This writes it from maps already collected, as the tests make images; a
/// capture of the keyspace writes the same layout from the entries it holds
/// ([`FrozenDataset`]).
#[cfg(test)]
pub fn write_kdb_expiry_maps(
    w: &mut impl Write,
    per_db: &[HashMap<String, u64>],
) -> io::Result<()> {
    if per_db.iter().all(|m| m.is_empty()) {
        return Ok(());
    }
    w.write_all(KDB_EXPIRY_MAGIC)?;
    w.write_all(&(per_db.len() as u32).to_le_bytes())?;
    for map in per_db {
        w.write_all(&(map.len() as u32).to_le_bytes())?;
        for (key, at_ms) in map {
            write_bytes(w, key.as_bytes())?;
            w.write_all(&at_ms.to_le_bytes())?;
        }
    }
    Ok(())
}

fn system_time_to_unix_ms(t: std::time::SystemTime) -> u64 {
    t.duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn unix_ms_now() -> u64 {
    system_time_to_unix_ms(std::time::SystemTime::now())
}

/// A complete KDB image of owned values: header, databases, index sections.
/// Values in hand have no time to live: a dataset with TTLs is written from
/// a capture of the keyspace ([`freeze_dataset`]).
pub fn write_kdb_image(
    w: &mut impl Write,
    maps: &[HashMap<String, KiviDBValue>],
    index_store: Option<&crate::vector::IndexStore>,
) -> io::Result<()> {
    write_kdb_header(w, maps.len() as u32)?;
    for map in maps {
        for (k, v) in map {
            write_kdb_entry(w, k, v.view())?;
        }
        write_kdb_db_end(w)?;
    }
    write_kdb_index_sections(w, index_store)?;
    write_kdb_alias_section(w, index_store)?;
    write_kdb_functions_section(w, function_libraries().as_deref())
}

/// The libraries loaded now, as a `FUNCTION DUMP` payload: `None` when there
/// is none, or in a build without Lua, which cannot load any.
fn function_libraries() -> Option<Vec<u8>> {
    #[cfg(feature = "lua")]
    let libraries = crate::commands::library_snapshot();
    #[cfg(not(feature = "lua"))]
    let libraries = None;
    libraries
}

/// The function libraries' section, which closes a KDB image. Absent when
/// there are none, so such an image is what it was before they were kept.
fn write_kdb_functions_section(w: &mut impl Write, libraries: Option<&[u8]>) -> io::Result<()> {
    if let Some(payload) = libraries {
        w.write_all(KDB_FUNCTIONS_MAGIC)?;
        write_bytes(w, payload)?;
    }
    Ok(())
}

/// Load the function libraries an image kept ([`LoadedSnapshot::functions`]),
/// in place of those loaded, as Redis does with the ones in an RDB file: an
/// image without any leaves none.
///
/// A build without Lua cannot load them, and says so: they are not in its
/// next snapshot.
pub fn restore_functions(functions: Option<Vec<u8>>) -> io::Result<()> {
    #[cfg(feature = "lua")]
    crate::commands::restore_library_snapshot(functions.as_deref()).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("the snapshot's function libraries cannot be loaded: {e}"),
        )
    })?;
    #[cfg(not(feature = "lua"))]
    if let Some(payload) = functions {
        let names = function_library_names(&payload)
            .map(|names| format!(" {}", names.join(", ")))
            .unwrap_or_default();
        log_warn!(
            "the snapshot's function libraries{names} are not loaded: this build was made \
             without Lua, and they are not in its next snapshot"
        );
    }
    Ok(())
}

/// The library names in a `FUNCTION DUMP` payload of KiviDB's own: `KVFN`,
/// the format version, the number of libraries, then each one's name,
/// engine, code and function names, length-prefixed, and a checksum. Or in
/// Redis's, as a `dump.rdb`'s libraries are handed on. Read without Lua, so
/// a build without it can say which libraries it leaves out. `None` for a
/// payload of any other shape.
#[cfg_attr(feature = "lua", allow(dead_code))]
fn function_library_names(payload: &[u8]) -> Option<Vec<String>> {
    fn take<'a>(rest: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
        let (head, tail) = rest.split_at_checked(n)?;
        *rest = tail;
        Some(head)
    }
    fn number(rest: &mut &[u8]) -> Option<usize> {
        Some(u32::from_le_bytes(take(rest, 4)?.try_into().ok()?) as usize)
    }
    fn blob<'a>(rest: &mut &'a [u8]) -> Option<&'a [u8]> {
        let len = number(rest)?;
        take(rest, len)
    }
    let Some(mut rest) = payload.strip_prefix(b"KVFN") else {
        return redis_function_dump_names(payload);
    };
    number(&mut rest)?;
    let mut names = Vec::new();
    for _ in 0..number(&mut rest)? {
        names.push(String::from_utf8_lossy(blob(&mut rest)?).into_owned());
        blob(&mut rest)?;
        blob(&mut rest)?;
        for _ in 0..number(&mut rest)? {
            blob(&mut rest)?;
        }
    }
    (rest.len() == 8).then_some(names)
}

/// The library names in a Redis `FUNCTION DUMP` payload: each library's code
/// starts `#!lua name=...`. The version and CRC at the end are not read.
#[cfg_attr(feature = "lua", allow(dead_code))]
fn redis_function_dump_names(payload: &[u8]) -> Option<Vec<String>> {
    let mut rest = payload.get(..payload.len().checked_sub(10)?)?;
    let mut names = Vec::new();
    while let Some((&0xF5, after)) = rest.split_first() {
        rest = after;
        let code = rdb_read_string(&mut rest).ok()?;
        let first_line = code.split(|&b| b == b'\n').next()?;
        let name = String::from_utf8_lossy(first_line)
            .split_whitespace()
            .find_map(|word| word.strip_prefix("name=").map(str::to_string))?;
        names.push(name);
    }
    (rest.is_empty() && !names.is_empty()).then_some(names)
}

/// The index alias section: `(alias, index)` pairs.
pub fn write_kdb_alias_section(
    w: &mut impl Write,
    index_store: Option<&crate::vector::IndexStore>,
) -> io::Result<()> {
    let Some(store) = index_store else {
        return Ok(());
    };
    let guard = store.read();
    let aliases: Vec<(&String, &String)> = guard
        .iter()
        .filter(|(key, def)| crate::vector::is_alias(key, def))
        .map(|(key, def)| (key, &def.name))
        .collect();
    if !aliases.is_empty() {
        w.write_all(KDB_ALIAS_MAGIC)?;
        w.write_all(&(aliases.len() as u32).to_le_bytes())?;
        for (alias, index) in aliases {
            write_bytes(w, alias.as_bytes())?;
            write_bytes(w, index.as_bytes())?;
        }
    }
    drop(guard);
    write_kdb_field_options_section(w, store)?;
    write_kdb_synonyms_section(w, store)?;
    write_kdb_vector_types_section(w, store)
}

/// The vector types section: `index`, `field`, `type name`, for each vector
/// field whose type is not FLOAT32. Absent when there is none, so an image
/// with only FLOAT32 fields is what it was before the section existed.
fn write_kdb_vector_types_section(
    w: &mut impl Write,
    store: &crate::vector::IndexStore,
) -> io::Result<()> {
    let guard = store.read();
    let mut typed: Vec<(&str, &str, &str)> = guard
        .iter()
        .filter(|(key, def)| !crate::vector::is_alias(key, def))
        .flat_map(|(_, def)| {
            def.fields.iter().filter_map(|f| match &f.field_type {
                FieldType::Vector { data_type, .. }
                    if *data_type != crate::vector::VectorType::Float32 =>
                {
                    Some((def.name.as_str(), f.name.as_str(), data_type.name()))
                }
                _ => None,
            })
        })
        .collect();
    if typed.is_empty() {
        return Ok(());
    }
    typed.sort();
    w.write_all(KDB_VECTOR_TYPES_MAGIC)?;
    w.write_all(&(typed.len() as u32).to_le_bytes())?;
    for (index, field, ty) in typed {
        write_bytes(w, index.as_bytes())?;
        write_bytes(w, field.as_bytes())?;
        write_bytes(w, ty.as_bytes())?;
    }
    Ok(())
}

/// Body of the vector types section; the magic has already been consumed.
/// Not best-effort, as the field options are not: a FLOAT64 field read as
/// FLOAT32 indexes none of its documents.
fn read_vector_types_section(r: &mut impl Read, schemas: &mut [IndexSchema]) -> io::Result<()> {
    let text = |bytes: Vec<u8>| {
        String::from_utf8(bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad vector type utf8"))
    };
    for _ in 0..read_u32_le(r)? {
        let index = text(read_bytes(r)?)?;
        let field = text(read_bytes(r)?)?;
        let ty = text(read_bytes(r)?)?;
        let ty = crate::vector::VectorType::parse(&ty).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown vector type `{ty}` for field `{field}`"),
            )
        })?;
        let target = schemas
            .iter_mut()
            .filter(|s| s.name == index)
            .flat_map(|s| s.fields.iter_mut())
            .find(|f| f.name == field);
        if let Some(SchemaField {
            field_type: FieldType::Vector { data_type, .. },
            ..
        }) = target
        {
            *data_type = ty;
        }
    }
    Ok(())
}

/// The synonyms and dictionaries section: each index's synonym groups
/// (`FT.SYNUPDATE`), then every dictionary (`FT.DICTADD`). Neither was saved
/// before: a restart, or a replica's full sync, came back without them.
fn write_kdb_synonyms_section(
    w: &mut impl Write,
    store: &crate::vector::IndexStore,
) -> io::Result<()> {
    let dicts = crate::vector::dict_store_for(store);
    let dicts = dicts.as_ref().map(|dicts| dicts.read());
    let guard = store.read();
    let mut with_synonyms: Vec<&crate::vector::IndexDef> = guard
        .iter()
        .filter(|(key, def)| !crate::vector::is_alias(key, def) && !def.synonyms.is_empty())
        .map(|(_, def)| def)
        .collect();
    with_synonyms.sort_by(|a, b| a.name.cmp(&b.name));
    let mut dictionaries: Vec<(&String, &ahash::AHashSet<String>)> = dicts
        .iter()
        .flat_map(|dicts| dicts.iter())
        .filter(|(_, terms)| !terms.is_empty())
        .collect();
    dictionaries.sort_by(|a, b| a.0.cmp(b.0));
    if with_synonyms.is_empty() && dictionaries.is_empty() {
        return Ok(());
    }

    w.write_all(KDB_SYNONYMS_MAGIC)?;
    w.write_all(&(with_synonyms.len() as u32).to_le_bytes())?;
    for def in with_synonyms {
        write_bytes(w, def.name.as_bytes())?;
        let mut groups: Vec<(&String, &Vec<String>)> = def.synonyms.iter().collect();
        groups.sort();
        w.write_all(&(groups.len() as u32).to_le_bytes())?;
        for (id, terms) in groups {
            write_bytes(w, id.as_bytes())?;
            w.write_all(&(terms.len() as u32).to_le_bytes())?;
            for term in terms {
                write_bytes(w, term.as_bytes())?;
            }
        }
    }
    w.write_all(&(dictionaries.len() as u32).to_le_bytes())?;
    for (name, terms) in dictionaries {
        write_bytes(w, name.as_bytes())?;
        let mut terms: Vec<&String> = terms.iter().collect();
        terms.sort();
        w.write_all(&(terms.len() as u32).to_le_bytes())?;
        for term in terms {
            write_bytes(w, term.as_bytes())?;
        }
    }
    Ok(())
}

/// Body of the synonyms and dictionaries section; the magic has already been
/// consumed. Synonyms go on the schemas read earlier in the image.
fn read_synonyms_section(
    r: &mut impl Read,
    schemas: &mut [IndexSchema],
) -> io::Result<Vec<(String, Vec<String>)>> {
    let text = |bytes: Vec<u8>| {
        String::from_utf8(bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad synonym utf8"))
    };
    let terms = |r: &mut dyn Read| -> io::Result<Vec<String>> {
        let mut r = r;
        let mut out = Vec::new();
        for _ in 0..read_u32_le(&mut r)? {
            out.push(text(read_bytes(&mut r)?)?);
        }
        Ok(out)
    };
    for _ in 0..read_u32_le(r)? {
        let index = text(read_bytes(r)?)?;
        let mut groups = crate::vector::SynonymGroups::new();
        for _ in 0..read_u32_le(r)? {
            let id = text(read_bytes(r)?)?;
            groups.insert(id, terms(r)?);
        }
        if let Some(schema) = schemas.iter_mut().find(|s| s.name == index) {
            schema.synonyms = groups;
        }
    }
    let mut dictionaries = Vec::new();
    for _ in 0..read_u32_le(r)? {
        let name = text(read_bytes(r)?)?;
        dictionaries.push((name, terms(r)?));
    }
    Ok(dictionaries)
}

/// The field options section: for each index that has any, its stopwords and
/// each field's options, written as the words `FT.CREATE` takes them in.
///
/// Words rather than a layout of its own, so that an option added later needs
/// no change here: what `FieldOptions::words` writes, `FieldOptions::consume`
/// reads.
fn write_kdb_field_options_section(
    w: &mut impl Write,
    store: &crate::vector::IndexStore,
) -> io::Result<()> {
    let guard = store.read();
    let plain = crate::vector::FieldOptions::default();
    let defs: Vec<&crate::vector::IndexDef> = guard
        .iter()
        .filter(|(key, def)| !crate::vector::is_alias(key, def))
        .map(|(_, def)| def)
        .filter(|def| def.stopwords.is_some() || def.fields.iter().any(|f| f.options != plain))
        .collect();
    if defs.is_empty() {
        return Ok(());
    }
    w.write_all(KDB_FIELD_OPTIONS_MAGIC)?;
    w.write_all(&(defs.len() as u32).to_le_bytes())?;
    for def in defs {
        write_bytes(w, def.name.as_bytes())?;
        match &def.stopwords {
            None => w.write_all(&[0])?,
            Some(words) => {
                w.write_all(&[1])?;
                w.write_all(&(words.len() as u32).to_le_bytes())?;
                for word in words.iter() {
                    write_bytes(w, word.as_bytes())?;
                }
            }
        }
        w.write_all(&(def.fields.len() as u32).to_le_bytes())?;
        for field in &def.fields {
            write_bytes(w, field.name.as_bytes())?;
            let mut words = field.options.words(&field.field_type);
            if let Some(alias) = &field.options.alias {
                words.insert(0, alias.clone());
                words.insert(0, "AS".to_string());
            }
            w.write_all(&(words.len() as u32).to_le_bytes())?;
            for word in &words {
                write_bytes(w, word.as_bytes())?;
            }
        }
    }
    Ok(())
}

/// Body of the field options section; the magic has already been consumed.
/// The options are put on the schemas read earlier in the image.
fn read_field_options_section(r: &mut impl Read, schemas: &mut [IndexSchema]) -> io::Result<()> {
    let text = |bytes: Vec<u8>| {
        String::from_utf8(bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad field option utf8"))
    };
    let bad = |what: String| io::Error::new(io::ErrorKind::InvalidData, what);
    for _ in 0..read_u32_le(r)? {
        let index = text(read_bytes(r)?)?;
        let mut flag = [0u8];
        r.read_exact(&mut flag)?;
        let stopwords = if flag[0] == 0 {
            None
        } else {
            let mut words = Vec::new();
            for _ in 0..read_u32_le(r)? {
                words.push(text(read_bytes(r)?)?);
            }
            Some(words)
        };
        let mut schema = schemas.iter_mut().find(|s| s.name == index);
        if let Some(schema) = schema.as_deref_mut() {
            schema.stopwords = stopwords;
        }
        for _ in 0..read_u32_le(r)? {
            let field_name = text(read_bytes(r)?)?;
            let mut words: Vec<Vec<u8>> = Vec::new();
            for _ in 0..read_u32_le(r)? {
                words.push(read_bytes(r)?);
            }
            let Some(field) = schema
                .as_deref_mut()
                .and_then(|s| s.fields.iter_mut().find(|f| f.name == field_name))
            else {
                continue;
            };
            let mut words: Vec<&[u8]> = words.iter().map(|w| w.as_slice()).collect();
            let mut options = crate::vector::FieldOptions::default();
            if words.first().is_some_and(|w| *w == b"AS") && words.len() >= 2 {
                options.alias = Some(String::from_utf8_lossy(words[1]).into_owned());
                words.drain(..2);
            }
            let mut at = 0usize;
            options
                .consume(&field_name, &field.field_type, &words, &mut at)
                .map_err(bad)?;
            if at != words.len() {
                return Err(bad(format!(
                    "unknown option `{}` for field `{field_name}`",
                    String::from_utf8_lossy(words[at])
                )));
            }
            field.options = options;
        }
    }
    Ok(())
}

/// Body of the alias section; the magic has already been consumed.
fn read_alias_section(r: &mut impl Read) -> io::Result<Vec<(String, String)>> {
    let count = read_u32_le(r)? as usize;
    let mut aliases = Vec::with_capacity(count.min(1024));
    let text = |bytes: Vec<u8>| {
        String::from_utf8(bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad index alias utf8"))
    };
    for _ in 0..count {
        let alias = text(read_bytes(r)?)?;
        let index = text(read_bytes(r)?)?;
        aliases.push((alias, index));
    }
    Ok(aliases)
}

/// Write a snapshot of owned values (no key expiries: see
/// [`write_kdb_image`]) to `path`, replacing whatever is there atomically.
///
/// The image is written to a sibling temp file, fsynced, and renamed into
/// place. Writing straight into `path` (as this used to) truncates the only
/// good copy first, so a crash or a full disk mid-save left a torn file where
/// the previous snapshot had been.
// Unused by the binary, which writes from a capture of the keyspace; the
// library exports it and the snapshot tests are written against it.
#[allow(dead_code)]
pub fn write_snapshot(
    path: &str,
    maps: &[HashMap<String, KiviDBValue>],
    index_store: Option<&crate::vector::IndexStore>,
) -> io::Result<()> {
    let display_path = std::env::current_dir()
        .map(|d| d.join(path))
        .unwrap_or_else(|_| std::path::PathBuf::from(path));

    log_info!("Saving snapshot → {}", display_path.display());
    // Unique per writer: SAVE, BGSAVE and the shutdown path can overlap, and
    // two of them sharing one temp name would interleave into a corrupt file.
    static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let tmp_path = format!(
        "{path}.{}.{}.tmp",
        std::process::id(),
        TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    let result = (|| -> io::Result<()> {
        let raw = File::create(&tmp_path)?;
        let mut file = BufWriter::with_capacity(1 << 20, raw); // 1 MiB write buffer
        write_kdb_image(&mut file, maps, index_store)?;
        file.flush()?;
        file.get_ref().sync_all()?;
        drop(file);
        std::fs::rename(&tmp_path, path)
    })();
    if let Err(e) = result {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(e);
    }

    log_info!(
        "Snapshot saved  → {} ({} databases)",
        display_path.display(),
        maps.len()
    );
    Ok(())
}

fn write_index_schema(w: &mut impl Write, def: &crate::vector::IndexDef) -> io::Result<()> {
    write_bytes(w, def.name.as_bytes())?;
    w.write_all(&(def.db_index as u32).to_le_bytes())?;
    let on_byte: u8 = match def.on {
        IndexOn::Hash => 0,
        IndexOn::Json => 1,
    };
    w.write_all(&[on_byte])?;

    w.write_all(&(def.prefixes.len() as u32).to_le_bytes())?;
    for p in &def.prefixes {
        write_bytes(w, p.as_bytes())?;
    }

    w.write_all(&(def.fields.len() as u32).to_le_bytes())?;
    for f in &def.fields {
        write_bytes(w, f.name.as_bytes())?;
        match &f.field_type {
            FieldType::Text => w.write_all(&[SCHEMA_FIELD_TEXT])?,
            FieldType::Tag => w.write_all(&[SCHEMA_FIELD_TAG])?,
            FieldType::Numeric => w.write_all(&[SCHEMA_FIELD_NUMERIC])?,
            // The type is in the vector types section.
            FieldType::Vector {
                algo, dim, metric, ..
            } => {
                w.write_all(&[SCHEMA_FIELD_VECTOR])?;
                match algo {
                    VectorAlgo::Flat => {
                        w.write_all(&[VECTOR_ALGO_FLAT])?;
                    }
                    VectorAlgo::Hnsw {
                        m,
                        ef_construction,
                        ef_runtime,
                    } => {
                        w.write_all(&[VECTOR_ALGO_HNSW])?;
                        w.write_all(&(*m as u32).to_le_bytes())?;
                        w.write_all(&(*ef_construction as u32).to_le_bytes())?;
                        w.write_all(&(*ef_runtime as u32).to_le_bytes())?;
                    }
                }
                w.write_all(&(*dim as u32).to_le_bytes())?;
                let metric_byte: u8 = match metric {
                    DistanceMetric::Cosine => 0,
                    DistanceMetric::L2 => 1,
                    DistanceMetric::InnerProduct => 2,
                };
                w.write_all(&[metric_byte])?;
            }
        }
    }
    Ok(())
}

pub fn load_snapshot(path: &str) -> io::Result<LoadedSnapshot> {
    let file = File::open(path)?;
    let mut r = BufReader::new(file);
    load_snapshot_from(&mut r)
}

/// Read a KDB image from anything byte-shaped.
///
/// Split out from [`load_snapshot`] so the AOF preamble — the same format,
/// living at an offset inside `appendonly.aof` rather than in its own file —
/// can be read without being copied out to disk first.
pub fn load_snapshot_from(r: impl Read) -> io::Result<LoadedSnapshot> {
    read_image(r, true).map(|(loaded, _)| loaded)
}

/// Read a KDB image from beginning to end without keeping any of it, and
/// return how many keys it holds.
///
/// This is how a replica finds out that a full-sync payload is whole and
/// readable *before* it gives up the dataset it already has: every key and
/// value is parsed, exactly as loading would, and dropped. It costs the
/// memory of one value at a time.
///
/// Stricter than loading: every database has to end where the writer ends
/// it. (The loader also accepts an image that simply stops between two
/// entries, for the sake of files written before there was an end marker.)
pub fn verify_snapshot(r: impl Read) -> io::Result<usize> {
    read_image(r, false).map(|(_, keys)| keys)
}

/// The reader behind [`load_snapshot_from`] and [`verify_snapshot`]. With
/// `keep` unset nothing is retained; the count of keys read is returned
/// either way.
fn read_image(r: impl Read, keep: bool) -> io::Result<(LoadedSnapshot, usize)> {
    let mut r = r;
    let mut keys_read = 0usize;
    let mut magic = [0u8; 8];
    r.read_exact(&mut magic)?;
    if &magic != KDB_MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
    }

    let mut db_count_buf = [0u8; 4];
    r.read_exact(&mut db_count_buf)?;
    let db_count = u32::from_le_bytes(db_count_buf) as usize;

    let mut result = Vec::with_capacity(hint(db_count));
    // The image stopped between two entries (see below): the databases it
    // says are still to come are not there.
    let mut ended = false;

    for _ in 0..db_count {
        let mut map: HashMap<String, KiviDBValue> = HashMap::new();

        loop {
            // Read key length — if EOF here, we're done
            let mut len_buf = [0u8; 4];
            match r.read_exact(&mut len_buf) {
                // Verifying: an image that stops before a database's end
                // marker is not a whole image.
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof && keep => {
                    ended = true;
                    break;
                }
                Err(e) => return Err(e),
                Ok(_) => {}
            }
            let klen = u32::from_le_bytes(len_buf) as usize;

            // TAG_EOF sentinel (key len == 0 means end of DB section)
            if klen == 0 {
                break;
            }

            let kbuf = read_exactly(&mut r, klen)?;
            let key = String::from_utf8(kbuf)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad key utf8"))?;

            // Read type tag
            let mut tag = [0u8];
            r.read_exact(&mut tag)?;

            let value = match tag[0] {
                TAG_STRING => {
                    let bytes = read_bytes(&mut r)?;
                    KiviDBValue::String(SharedBytes::from(bytes.as_slice()))
                }
                TAG_LIST => {
                    let mut count_buf = [0u8; 4];
                    r.read_exact(&mut count_buf)?;
                    let count = u32::from_le_bytes(count_buf) as usize;
                    let mut deque = VecDeque::with_capacity(hint(count));
                    for _ in 0..count {
                        let item = read_bytes(&mut r)?;
                        deque.push_back(Arc::from(item.as_slice()));
                    }
                    KiviDBValue::List(Arc::new(parking_lot::RwLock::new(deque)))
                }
                TAG_HASH => {
                    let mut count_buf = [0u8; 4];
                    r.read_exact(&mut count_buf)?;
                    let count = u32::from_le_bytes(count_buf) as usize;
                    let mut hmap = crate::types::HashFields::with_capacity_and_hasher(
                        hint(count),
                        Default::default(),
                    );
                    for _ in 0..count {
                        let field_bytes = read_bytes(&mut r)?;
                        let field = String::from_utf8(field_bytes).map_err(|_| {
                            io::Error::new(io::ErrorKind::InvalidData, "bad hash field")
                        })?;
                        let val = read_bytes(&mut r)?;
                        hmap.insert(field, Arc::from(val.as_slice()));
                    }
                    KiviDBValue::Hash(Arc::new(parking_lot::RwLock::new(hmap)))
                }
                TAG_SET => {
                    let mut count_buf = [0u8; 4];
                    r.read_exact(&mut count_buf)?;
                    let count = u32::from_le_bytes(count_buf) as usize;
                    let mut hset = crate::member_set::MemberSet::with_capacity(hint(count));
                    for _ in 0..count {
                        let member = read_bytes(&mut r)?;
                        hset.insert(Arc::from(member.as_slice()));
                    }
                    KiviDBValue::Set(Arc::new(parking_lot::RwLock::new(hset)))
                }
                TAG_SORTEDSET => {
                    let mut count_buf = [0u8; 4];
                    r.read_exact(&mut count_buf)?;
                    let count = u32::from_le_bytes(count_buf) as usize;
                    let mut ss = SortedSetInner::new();
                    for _ in 0..count {
                        let member = read_bytes(&mut r)?;
                        let mut score_buf = [0u8; 8];
                        r.read_exact(&mut score_buf)?;
                        let score = f64::from_le_bytes(score_buf);
                        ss.insert(Arc::from(member.as_slice()), score);
                    }
                    KiviDBValue::SortedSet(Arc::new(parking_lot::RwLock::new(ss)))
                }
                TAG_STREAM | TAG_STREAM_V2 | TAG_STREAM_V3 => read_stream_value(&mut r, tag[0])?,
                TAG_JSON => {
                    let json_bytes = read_bytes(&mut r)?;
                    let val: serde_json::Value =
                        serde_json::from_slice(&json_bytes).unwrap_or(serde_json::Value::Null);
                    KiviDBValue::Json(Arc::new(parking_lot::RwLock::new(val)))
                }
                TAG_TIMESERIES | TAG_TIMESERIES_V2 | TAG_TIMESERIES_V3 => {
                    let mut count_buf = [0u8; 4];
                    r.read_exact(&mut count_buf)?;
                    let count = u32::from_le_bytes(count_buf) as usize;
                    let mut ts = TsInner::new();
                    for _ in 0..count {
                        let mut ts_buf = [0u8; 8];
                        r.read_exact(&mut ts_buf)?;
                        let timestamp = u64::from_le_bytes(ts_buf);
                        let mut v_buf = [0u8; 8];
                        r.read_exact(&mut v_buf)?;
                        let value = f64::from_le_bytes(v_buf);
                        ts.samples.insert(timestamp, value);
                        ts.last_ts = Some(timestamp);
                    }
                    let mut ret_buf = [0u8; 8];
                    r.read_exact(&mut ret_buf)?;
                    ts.retention = u64::from_le_bytes(ret_buf);
                    let mut cs_buf = [0u8; 8];
                    r.read_exact(&mut cs_buf)?;
                    ts.chunk_size =
                        usize::try_from(u64::from_le_bytes(cs_buf)).unwrap_or(usize::MAX);
                    let mut lcount_buf = [0u8; 4];
                    r.read_exact(&mut lcount_buf)?;
                    let lcount = u32::from_le_bytes(lcount_buf) as usize;
                    for _ in 0..lcount {
                        let lk = read_bytes(&mut r)?;
                        let lv = read_bytes(&mut r)?;
                        ts.labels.insert(
                            String::from_utf8_lossy(&lk).into_owned(),
                            String::from_utf8_lossy(&lv).into_owned(),
                        );
                    }
                    if tag[0] != TAG_TIMESERIES {
                        let bad = |what: &str| {
                            io::Error::new(io::ErrorKind::InvalidData, format!("bad series {what}"))
                        };
                        let policy = read_bytes(&mut r)?;
                        ts.duplicate_policy =
                            crate::types::DuplicatePolicy::parse(&String::from_utf8_lossy(&policy))
                                .ok_or_else(|| bad("duplicate policy"))?;
                        let rules = read_u32_le(&mut r)? as usize;
                        for _ in 0..rules {
                            let dest_key = String::from_utf8(read_bytes(&mut r)?)
                                .map_err(|_| bad("rule destination"))?;
                            let aggregation = crate::types::TsAggregation::parse(
                                &String::from_utf8_lossy(&read_bytes(&mut r)?),
                            )
                            .ok_or_else(|| bad("rule aggregation"))?;
                            let bucket_duration = read_u64_le(&mut r)?;
                            let align_timestamp = read_u64_le(&mut r)?;
                            if bucket_duration == 0 {
                                return Err(bad("rule bucket duration"));
                            }
                            ts.rules.push(crate::types::CompactionRule {
                                dest_key,
                                aggregation,
                                bucket_duration,
                                align_timestamp,
                            });
                        }
                        if tag[0] == TAG_TIMESERIES_V3 {
                            ts.source = Some(
                                String::from_utf8(read_bytes(&mut r)?)
                                    .map_err(|_| bad("source"))?,
                            );
                        }
                    }
                    KiviDBValue::TimeSeries(Arc::new(parking_lot::RwLock::new(ts)))
                }
                TAG_VECTORSET => {
                    let mut dim_buf = [0u8; 4];
                    r.read_exact(&mut dim_buf)?;
                    let dim = u32::from_le_bytes(dim_buf) as usize;

                    // Read metric byte
                    let mut metric_buf = [0u8];
                    r.read_exact(&mut metric_buf)?;
                    let metric = match metric_buf[0] {
                        1 => crate::vector::DistanceMetric::L2,
                        2 => crate::vector::DistanceMetric::InnerProduct,
                        _ => crate::vector::DistanceMetric::Cosine,
                    };

                    let mut count_buf = [0u8; 4];
                    r.read_exact(&mut count_buf)?;
                    let count = u32::from_le_bytes(count_buf) as usize;

                    let mut vs = VectorSetInner::with_metric(dim, metric);

                    for _ in 0..count {
                        // Element name
                        let name_bytes = read_bytes(&mut r)?;
                        let name = String::from_utf8(name_bytes).map_err(|_| {
                            io::Error::new(io::ErrorKind::InvalidData, "bad vset element name")
                        })?;

                        // Vector floats
                        let mut vlen_buf = [0u8; 4];
                        r.read_exact(&mut vlen_buf)?;
                        let vec_len = u32::from_le_bytes(vlen_buf) as usize;
                        let mut vector = Vec::with_capacity(hint(vec_len));
                        for _ in 0..vec_len {
                            let mut f_buf = [0u8; 4];
                            r.read_exact(&mut f_buf)?;
                            vector.push(f32::from_le_bytes(f_buf));
                        }

                        // Optional attrs
                        let mut has_attrs = [0u8];
                        r.read_exact(&mut has_attrs)?;
                        let attrs = if has_attrs[0] == 1 {
                            let attr_bytes = read_bytes(&mut r)?;
                            Some(String::from_utf8_lossy(&attr_bytes).into_owned())
                        } else {
                            None
                        };

                        vs.entries.insert(name, VSetEntry { vector, attrs });
                    }

                    KiviDBValue::VectorSet(Arc::new(parking_lot::RwLock::new(vs)))
                }
                TAG_HASH_EX => {
                    let mut count_buf = [0u8; 4];
                    r.read_exact(&mut count_buf)?;
                    let count = u32::from_le_bytes(count_buf) as usize;
                    let mut hmap = crate::types::HashFields::with_capacity_and_hasher(
                        hint(count),
                        Default::default(),
                    );
                    for _ in 0..count {
                        let field_bytes = read_bytes(&mut r)?;
                        let field = String::from_utf8(field_bytes).map_err(|_| {
                            io::Error::new(io::ErrorKind::InvalidData, "bad hash field")
                        })?;
                        let val = read_bytes(&mut r)?;
                        hmap.insert(field, Arc::from(val.as_slice()));
                    }
                    // Expiries: re-anchor each persisted Unix ms back to a
                    // monotonic Instant relative to "now". Fields whose
                    // absolute time is in the past simply don't get added —
                    // the lazy-eviction check on next access handles them.
                    let mut exp_count_buf = [0u8; 4];
                    r.read_exact(&mut exp_count_buf)?;
                    let exp_count = u32::from_le_bytes(exp_count_buf) as usize;
                    let mut exps = AHashMap::with_capacity(hint(exp_count));
                    let now_sys = std::time::SystemTime::now();
                    let now_mono = std::time::Instant::now();
                    let unix_now_ms = now_sys
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    for _ in 0..exp_count {
                        let field_bytes = read_bytes(&mut r)?;
                        let field = String::from_utf8(field_bytes).map_err(|_| {
                            io::Error::new(io::ErrorKind::InvalidData, "bad expiry field")
                        })?;
                        let mut ms_buf = [0u8; 8];
                        r.read_exact(&mut ms_buf)?;
                        let abs_ms = u64::from_le_bytes(ms_buf);
                        if abs_ms > unix_now_ms {
                            let delta = std::time::Duration::from_millis(abs_ms - unix_now_ms);
                            exps.insert(field, now_mono + delta);
                        } else {
                            crate::types::hash_remove(&mut hmap, &field);
                        }
                    }
                    let inner = crate::types::HashInner {
                        fields: hmap,
                        expiries: exps,
                    };
                    KiviDBValue::HashEx(Arc::new(parking_lot::RwLock::new(inner)))
                }
                TAG_ARRAY => {
                    let mut count_buf = [0u8; 4];
                    r.read_exact(&mut count_buf)?;
                    let count = u32::from_le_bytes(count_buf) as usize;
                    let mut arr = crate::types::ArrayInner::new();
                    for _ in 0..count {
                        let mut idx_buf = [0u8; 8];
                        r.read_exact(&mut idx_buf)?;
                        let idx = u64::from_le_bytes(idx_buf);
                        let val = read_bytes(&mut r)?;
                        arr.set(idx, Arc::from(val.as_slice()));
                    }
                    let mut next_buf = [0u8; 8];
                    r.read_exact(&mut next_buf)?;
                    arr.next_insert = u64::from_le_bytes(next_buf);
                    let mut ring_buf = [0u8; 8];
                    r.read_exact(&mut ring_buf)?;
                    let ring = u64::from_le_bytes(ring_buf);
                    arr.ring_size = if ring == 0 { None } else { Some(ring) };
                    KiviDBValue::Array(Arc::new(parking_lot::RwLock::new(arr)))
                }
                unknown => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown type tag {unknown}"),
                    ));
                }
            };

            keys_read += 1;
            let has_value = match &value {
                KiviDBValue::HashEx(hash) => !hash.read().is_empty(),
                _ => true,
            };
            if keep && has_value {
                map.insert(key, value);
            }
        }

        result.push(map);
        if ended {
            break;
        }
    }

    // ── Optional trailing sections ────────────────────────────────────────────
    // Each opens with an 8-byte magic. They are scanned rather than read in a
    // fixed order so that an image carrying a section this build does not know
    // still loads: an unrecognised magic simply ends the scan.
    //
    // Index schemas and HNSW graphs stay best-effort, as they always were — a
    // snapshot that predates them, or whose index section is damaged, falls
    // back to rebuilding. Nothing after a damaged section can be located, so
    // that also ends the scan, and it says so because TTLs live behind it.
    let mut index_schemas = Vec::new();
    let mut hnsw_graphs = std::collections::HashMap::new();
    let mut dictionaries = Vec::new();
    let mut expiries: ExpiryMaps = (0..result.len()).map(|_| HashMap::new()).collect();
    let mut functions = None;
    loop {
        let mut magic = [0u8; 8];
        if r.read_exact(&mut magic).is_err() {
            break;
        }
        if &magic == KDB_INDEX_MAGIC {
            match read_index_schemas(&mut r) {
                Ok(v) => index_schemas = v,
                Err(e) => {
                    log_warn!(
                        "snapshot index section is unreadable ({e}); indexes will be \
                         rebuilt, and any key expiries and function libraries stored \
                         after it are lost"
                    );
                    break;
                }
            }
        } else if &magic == KDB_HNSW_MAGIC {
            match read_hnsw_graphs(&mut r) {
                Ok(v) => hnsw_graphs = v,
                Err(e) => {
                    log_warn!(
                        "snapshot HNSW section is unreadable ({e}); graphs will be \
                         rebuilt, and any key expiries and function libraries stored \
                         after it are lost"
                    );
                    break;
                }
            }
        } else if &magic == KDB_EXPIRY_MAGIC {
            read_expiry_section(&mut r, &mut expiries)?;
        } else if &magic == KDB_ALIAS_MAGIC {
            match read_alias_section(&mut r) {
                Ok(aliases) => {
                    for (alias, index) in aliases {
                        if let Some(schema) = index_schemas.iter_mut().find(|s| s.name == index) {
                            schema.aliases.push(alias);
                        }
                    }
                }
                Err(e) => {
                    log_warn!("snapshot index alias section is unreadable ({e})");
                    break;
                }
            }
        } else if &magic == KDB_FIELD_OPTIONS_MAGIC {
            // Not best-effort: an index that comes back without its options
            // is a different index — a TAG field split at the wrong
            // character matches different documents.
            read_field_options_section(&mut r, &mut index_schemas)?;
        } else if &magic == KDB_SYNONYMS_MAGIC {
            dictionaries = read_synonyms_section(&mut r, &mut index_schemas)?;
        } else if &magic == KDB_VECTOR_TYPES_MAGIC {
            read_vector_types_section(&mut r, &mut index_schemas)?;
        } else if &magic == KDB_FUNCTIONS_MAGIC {
            // Not best-effort: a node that comes back without its libraries
            // fails every FCALL.
            functions = Some(read_bytes(&mut r)?);
        } else {
            break;
        }
    }

    // A key whose expiry passed while the image sat on disk (or in transit to
    // a replica) is gone, not "present without a TTL".
    let now_ms = unix_ms_now();
    for (db, exp) in result.iter_mut().zip(expiries.iter_mut()) {
        exp.retain(|key, at_ms| {
            if !db.contains_key(key) {
                return false;
            }
            if *at_ms <= now_ms {
                db.remove(key);
                return false;
            }
            true
        });
    }

    Ok((
        LoadedSnapshot {
            dbs: result,
            index_schemas,
            dictionaries,
            hnsw_graphs,
            expiries,
            functions,
        },
        keys_read,
    ))
}

/// Body of the expiry section; the magic has already been consumed.
///
/// Unlike the index sections this is not best-effort: a torn expiry section
/// would silently make some keys permanent, so it fails the load instead.
fn read_expiry_section(r: &mut impl Read, expiries: &mut ExpiryMaps) -> io::Result<()> {
    let db_count = read_u32_le(r)? as usize;
    for db_idx in 0..db_count {
        let count = read_u32_le(r)? as usize;
        for _ in 0..count {
            let key = String::from_utf8(read_bytes(r)?)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad expiry key utf8"))?;
            let at_ms = read_u64_le(r)?;
            if let Some(map) = expiries.get_mut(db_idx) {
                map.insert(key, at_ms);
            }
        }
    }
    Ok(())
}

/// Give the keys of `store` the times to live loaded with them (once their
/// values are in). A key that is not there is given none. Returns how many
/// were given one.
pub fn restore_expiries_into_store(expiries: ExpiryMaps, store: &store::Store) -> usize {
    let mut total = 0usize;
    for (db_idx, map) in expiries.into_iter().enumerate() {
        let Some(db) = store.get(db_idx) else {
            break;
        };
        for (key, at_ms) in map {
            if write_lock!(store::get_shard(db, &key)).set_expires(&key, at_ms) {
                total += 1;
            }
        }
    }
    total
}

/// Body of the index schema section; the magic has already been consumed.
fn read_index_schemas(r: &mut impl Read) -> io::Result<Vec<IndexSchema>> {
    let mut count_buf = [0u8; 4];
    r.read_exact(&mut count_buf)?;
    let count = u32::from_le_bytes(count_buf) as usize;

    let mut schemas = Vec::with_capacity(hint(count));
    for _ in 0..count {
        schemas.push(read_one_index_schema(r)?);
    }
    Ok(schemas)
}

fn read_one_index_schema(r: &mut impl Read) -> io::Result<IndexSchema> {
    let name_bytes = read_bytes(r)?;
    let name = String::from_utf8(name_bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad index name utf8"))?;

    let mut db_buf = [0u8; 4];
    r.read_exact(&mut db_buf)?;
    let db_index = u32::from_le_bytes(db_buf) as usize;

    let mut on_buf = [0u8];
    r.read_exact(&mut on_buf)?;
    let on = match on_buf[0] {
        1 => IndexOn::Json,
        _ => IndexOn::Hash,
    };

    let mut pcount_buf = [0u8; 4];
    r.read_exact(&mut pcount_buf)?;
    let pcount = u32::from_le_bytes(pcount_buf) as usize;
    let mut prefixes = Vec::with_capacity(hint(pcount));
    for _ in 0..pcount {
        let pb = read_bytes(r)?;
        prefixes.push(
            String::from_utf8(pb)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad prefix utf8"))?,
        );
    }

    let mut fcount_buf = [0u8; 4];
    r.read_exact(&mut fcount_buf)?;
    let fcount = u32::from_le_bytes(fcount_buf) as usize;
    let mut fields = Vec::with_capacity(hint(fcount));
    for _ in 0..fcount {
        let fname_bytes = read_bytes(r)?;
        let fname = String::from_utf8(fname_bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad field name utf8"))?;

        let mut type_buf = [0u8];
        r.read_exact(&mut type_buf)?;
        let field_type = match type_buf[0] {
            SCHEMA_FIELD_TAG => FieldType::Tag,
            SCHEMA_FIELD_NUMERIC => FieldType::Numeric,
            SCHEMA_FIELD_VECTOR => {
                let mut algo_buf = [0u8];
                r.read_exact(&mut algo_buf)?;
                let algo = match algo_buf[0] {
                    VECTOR_ALGO_HNSW => {
                        let mut m_buf = [0u8; 4];
                        r.read_exact(&mut m_buf)?;
                        let m = u32::from_le_bytes(m_buf) as usize;
                        let mut ef_c_buf = [0u8; 4];
                        r.read_exact(&mut ef_c_buf)?;
                        let ef_construction = u32::from_le_bytes(ef_c_buf) as usize;
                        let mut ef_r_buf = [0u8; 4];
                        r.read_exact(&mut ef_r_buf)?;
                        let ef_runtime = u32::from_le_bytes(ef_r_buf) as usize;
                        VectorAlgo::Hnsw {
                            m,
                            ef_construction,
                            ef_runtime,
                        }
                    }
                    _ => VectorAlgo::Flat,
                };
                let mut dim_buf = [0u8; 4];
                r.read_exact(&mut dim_buf)?;
                let dim = u32::from_le_bytes(dim_buf) as usize;
                let mut metric_buf = [0u8];
                r.read_exact(&mut metric_buf)?;
                let metric = match metric_buf[0] {
                    1 => DistanceMetric::L2,
                    2 => DistanceMetric::InnerProduct,
                    _ => DistanceMetric::Cosine,
                };
                FieldType::Vector {
                    algo,
                    dim,
                    metric,
                    // Set from the vector types section, if the image has one.
                    data_type: crate::vector::VectorType::Float32,
                }
            }
            _ => FieldType::Text,
        };
        fields.push(SchemaField {
            name: fname,
            field_type,
            // Filled in from the options section, if the image has one.
            options: crate::vector::FieldOptions::default(),
        });
    }

    Ok(IndexSchema {
        name,
        db_index,
        on,
        prefixes,
        fields,
        // Filled in from the alias section, if the image has one.
        aliases: Vec::new(),
        stopwords: None,
        synonyms: Default::default(),
    })
}

// ── HNSW graph serialisation helpers ─────────────────────────────────────────
//
// Format (after the KIVIDX1 index-schema section):
//
//   b"KIVIHN1\0"          – 8-byte magic
//   u32                   – number of (index_name, field_name, graph) triples
//   For each triple:
//     write_bytes         – index name
//     write_bytes         – field name
//     u8                  – entry point: 0=None, 1=Some
//     u32                 – entry point node id (if present)
//     u32                 – total node slots (live + tombstone)
//     For each node slot (in order):
//       u8: 0=tombstone, 1=live
//       If live:
//         write_bytes     – key
//         u32             – vector dimension (number of f32 values)
//         [f32 LE]        – vector bytes
//         u32             – number of layers in this node
//         For each layer:
//           u32           – number of neighbor IDs
//           [u32]         – neighbor IDs
//
// Old readers stop after the KIVIDX1 schema section; encountering EOF instead
// of the KIVIHN1 magic is handled by returning an empty map.

fn write_hnsw_graph(
    w: &mut impl Write,
    index_name: &str,
    field_name: &str,
    hnsw: &crate::vector::HnswIndex,
) -> io::Result<()> {
    write_bytes(w, index_name.as_bytes())?;
    write_bytes(w, field_name.as_bytes())?;

    // Inserts go on while the graph is written: they hold it for reading
    // too. The nodes written are those up to the count taken after the entry
    // point (so it is among them); a slot still being filled is written as a
    // tombstone, and links to nodes past the count are left out.
    let nodes = hnsw.nodes.read();
    let entry_point = *hnsw.entry_point.read();
    let count = nodes.count();

    match entry_point {
        None => w.write_all(&[0u8])?,
        Some(id) => {
            w.write_all(&[1u8])?;
            w.write_all(&(id as u32).to_le_bytes())?;
        }
    }

    w.write_all(&(count as u32).to_le_bytes())?;

    for id in 0..count {
        let node = match nodes.get(id) {
            Some(node) if !node.vector.is_empty() => node,
            _ => {
                w.write_all(&[0u8])?; // tombstone slot
                continue;
            }
        };
        w.write_all(&[1u8])?; // live node
        write_bytes(w, node.key.as_bytes())?;
        w.write_all(&(node.vector.len() as u32).to_le_bytes())?;
        for f in &node.vector {
            w.write_all(&f.to_le_bytes())?;
        }
        let neighbors = node.neighbors.lock().clone();
        w.write_all(&(neighbors.len() as u32).to_le_bytes())?;
        for layer_neighbors in &neighbors {
            let kept: Vec<usize> = layer_neighbors
                .iter()
                .copied()
                .filter(|&nb| nb < count)
                .collect();
            w.write_all(&(kept.len() as u32).to_le_bytes())?;
            for nb_id in kept {
                w.write_all(&(nb_id as u32).to_le_bytes())?;
            }
        }
    }

    // M and ef_construction are stored after the nodes so the reader can
    // reconstruct HnswIndex without needing the schema section.
    w.write_all(&(hnsw.m as u32).to_le_bytes())?;
    w.write_all(&(hnsw.ef_construction as u32).to_le_bytes())?;

    Ok(())
}

fn read_hnsw_graphs(
    r: &mut impl Read,
) -> io::Result<
    std::collections::HashMap<
        String,
        std::collections::HashMap<String, std::sync::Arc<crate::vector::HnswIndex>>,
    >,
> {
    // The section magic has already been consumed by the caller's scan.
    let mut count_buf = [0u8; 4];
    r.read_exact(&mut count_buf)?;
    let count = u32::from_le_bytes(count_buf) as usize;

    let mut out: std::collections::HashMap<
        String,
        std::collections::HashMap<String, std::sync::Arc<crate::vector::HnswIndex>>,
    > = std::collections::HashMap::new();

    for _ in 0..count {
        let idx_name_bytes = read_bytes(r)?;
        let idx_name = String::from_utf8(idx_name_bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad hnsw index name"))?;
        let fld_name_bytes = read_bytes(r)?;
        let fld_name = String::from_utf8(fld_name_bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad hnsw field name"))?;

        let mut ep_flag = [0u8];
        r.read_exact(&mut ep_flag)?;
        let entry_point: Option<usize> = if ep_flag[0] == 1 {
            let mut id_buf = [0u8; 4];
            r.read_exact(&mut id_buf)?;
            Some(u32::from_le_bytes(id_buf) as usize)
        } else {
            None
        };

        let mut ncount_buf = [0u8; 4];
        r.read_exact(&mut ncount_buf)?;
        let node_count = u32::from_le_bytes(ncount_buf) as usize;

        let mut nodes: Vec<crate::vector::HnswNode> = Vec::with_capacity(hint(node_count));
        let mut key_to_id: ahash::AHashMap<String, usize> =
            ahash::AHashMap::with_capacity(hint(node_count));
        let mut live_count: usize = 0;
        let mut tombstone_count: usize = 0;

        for node_id in 0..node_count {
            let mut live_flag = [0u8];
            r.read_exact(&mut live_flag)?;

            if live_flag[0] == 0 {
                tombstone_count += 1;
                nodes.push(crate::vector::HnswNode::tombstone());
            } else {
                live_count += 1;
                let key_bytes = read_bytes(r)?;
                let key = String::from_utf8(key_bytes)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad hnsw key"))?;

                let mut vlen_buf = [0u8; 4];
                r.read_exact(&mut vlen_buf)?;
                let vlen = u32::from_le_bytes(vlen_buf) as usize;
                let mut vector = Vec::with_capacity(hint(vlen));
                for _ in 0..vlen {
                    let mut fbuf = [0u8; 4];
                    r.read_exact(&mut fbuf)?;
                    vector.push(f32::from_le_bytes(fbuf));
                }

                let mut lcount_buf = [0u8; 4];
                r.read_exact(&mut lcount_buf)?;
                let layer_count = u32::from_le_bytes(lcount_buf) as usize;
                let mut neighbors: Vec<Vec<usize>> = Vec::with_capacity(hint(layer_count));
                for _ in 0..layer_count {
                    let mut nbcount_buf = [0u8; 4];
                    r.read_exact(&mut nbcount_buf)?;
                    let nb_count = u32::from_le_bytes(nbcount_buf) as usize;
                    let mut nb_list = Vec::with_capacity(hint(nb_count));
                    for _ in 0..nb_count {
                        let mut id_buf = [0u8; 4];
                        r.read_exact(&mut id_buf)?;
                        let nb = u32::from_le_bytes(id_buf) as usize;
                        // A neighbour outside the graph would be read out
                        // of bounds by the first search: drop it.
                        if nb < node_count {
                            nb_list.push(nb);
                        }
                    }
                    neighbors.push(nb_list);
                }

                key_to_id.insert(key.clone(), node_id);
                nodes.push(crate::vector::HnswNode::new(key, vector, neighbors));
            }
        }

        // We need M and ef_construction to reconstruct HnswIndex, but those
        // live in the IndexSchema. Create a placeholder with M=0 so the struct
        // fields are valid — the startup code will look up the real values from
        // the concurrently loaded schema and call HnswIndex::restore().
        // Alternatively, embed M and ef_construction in the HNSW section.
        // For simplicity, store them here.
        let mut m_buf = [0u8; 4];
        r.read_exact(&mut m_buf)?;
        let m = u32::from_le_bytes(m_buf) as usize;
        let mut efc_buf = [0u8; 4];
        r.read_exact(&mut efc_buf)?;
        let ef_construction = u32::from_le_bytes(efc_buf) as usize;

        // An entry point outside the graph would be read out of bounds by
        // the first search: start from any live node instead.
        let entry_point = match entry_point {
            Some(id) if id < node_count => Some(id),
            _ => key_to_id.values().next().copied(),
        };
        let hnsw = crate::vector::HnswIndex::restore(
            m,
            ef_construction,
            nodes,
            key_to_id,
            entry_point,
            live_count,
            tombstone_count,
        );
        out.entry(idx_name)
            .or_default()
            .insert(fld_name, std::sync::Arc::new(hnsw));
    }

    Ok(out)
}

fn rdb_read_length(r: &mut impl Read) -> io::Result<(u64, bool)> {
    // Returns (length, is_encoded)
    // is_encoded=true means the "length" is actually an encoding type for integers
    let mut byte = [0u8];
    r.read_exact(&mut byte)?;
    let enc_type = (byte[0] & 0xC0) >> 6; // top 2 bits

    match enc_type {
        0 => Ok((byte[0] as u64 & 0x3F, false)), // 6-bit length
        1 => {
            let mut next = [0u8];
            r.read_exact(&mut next)?;
            let len = ((byte[0] as u64 & 0x3F) << 8) | next[0] as u64;
            Ok((len, false)) // 14-bit length
        }
        // RDB_32BITLEN and RDB_64BITLEN, big-endian. Every other 10xxxxxx is
        // nothing. 0x81 used to be read as a 32-bit length too, so any
        // length or number past 2^32 (a stream ID's milliseconds, a module
        // ID) misread the rest of the value (#39).
        2 => match byte[0] {
            0x80 => {
                let mut buf = [0u8; 4];
                r.read_exact(&mut buf)?;
                Ok((u32::from_be_bytes(buf) as u64, false))
            }
            0x81 => {
                let mut buf = [0u8; 8];
                r.read_exact(&mut buf)?;
                Ok((u64::from_be_bytes(buf), false))
            }
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown RDB length encoding {other:#04x}"),
            )),
        },
        3 => Ok((byte[0] as u64 & 0x3F, true)), // special encoding
        _ => unreachable!(),
    }
}

fn rdb_read_string(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let (len, is_encoded) = rdb_read_length(r)?;

    if is_encoded {
        match len {
            0 => {
                // INT8
                let mut b = [0u8];
                r.read_exact(&mut b)?;
                return Ok((b[0] as i8).to_string().into_bytes());
            }
            1 => {
                // INT16
                let mut b = [0u8; 2];
                r.read_exact(&mut b)?;
                return Ok(i16::from_le_bytes(b).to_string().into_bytes());
            }
            2 => {
                // INT32
                let mut b = [0u8; 4];
                r.read_exact(&mut b)?;
                return Ok(i32::from_le_bytes(b).to_string().into_bytes());
            }
            3 => {
                // LZF compressed string — decompress before returning.
                // Redis uses LZF for strings longer than ~20 bytes by default,
                // so almost every real dump.rdb will have compressed strings.
                //
                // Wire format: compressed_len, original_len, compressed_bytes
                let (clen, _) = rdb_read_length(r)?;
                let (olen, _) = rdb_read_length(r)?;
                // The lengths are the payload's word: a length near 2^64
                // made room for that many bytes at once, and the process
                // aborted (a `RESTORE`, a replica's full sync). LZF cannot
                // grow its input much over 88 times (264 bytes from a 3-byte
                // back reference); a larger claim is not LZF.
                let compressed = read_exactly(r, usize::try_from(clen).map_err(|_| too_long())?)?;
                if olen > clen.saturating_mul(256) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "LZF original length out of range",
                    ));
                }
                return lzf::decompress(&compressed, olen as usize).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "LZF decompression failed")
                });
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unknown RDB string encoding",
                ));
            }
        }
    }

    // Room for the bytes as they arrive, not for whatever length is claimed.
    read_exactly(r, usize::try_from(len).map_err(|_| too_long())?)
}

fn too_long() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "RDB string length out of range")
}

// Unused by the binary, which wants the expiries; see `write_snapshot`.
#[allow(dead_code)]
pub fn load_redis_rdb(path: &str) -> io::Result<Vec<HashMap<String, KiviDBValue>>> {
    load_redis_rdb_with_expiry(path).map(|(dbs, _, _)| dbs)
}

/// What a Redis RDB file holds: its databases, their keys' expiries, and its
/// function libraries as a Redis `FUNCTION DUMP` payload, if it has any.
pub type RedisRdb = (
    Vec<HashMap<String, KiviDBValue>>,
    ExpiryMaps,
    Option<Vec<u8>>,
);

/// Load a Redis RDB file, keeping each key's expiry.
///
/// [`load_redis_rdb`] parsed the expiry opcodes and threw the value away, so
/// importing a Redis dump — or full-syncing from a Redis master — made every
/// expiring key permanent. Keys already past their expiry are dropped.
pub fn load_redis_rdb_with_expiry(path: &str) -> io::Result<RedisRdb> {
    let file = File::open(path)?;
    load_redis_rdb_from(BufReader::new(file))
}

/// [`load_redis_rdb_with_expiry`] over any byte source.
pub fn load_redis_rdb_from(r: impl Read) -> io::Result<RedisRdb> {
    let mut r = r;
    let now_ms = unix_ms_now();

    // Magic
    let mut magic = [0u8; 5];
    r.read_exact(&mut magic)?;
    if &magic != b"REDIS" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not a Redis RDB file",
        ));
    }

    // Version (4 ASCII digits)
    let mut ver_buf = [0u8; 4];
    r.read_exact(&mut ver_buf)?;
    let _version: u32 = std::str::from_utf8(&ver_buf)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let mut result: Vec<HashMap<String, KiviDBValue>> = (0..16).map(|_| HashMap::new()).collect();
    let mut expiries: ExpiryMaps = (0..16).map(|_| HashMap::new()).collect();

    let mut current_db = 0usize;
    // An expiry belongs to the key after it. Redis may write the key's idle
    // time or access frequency in between, so it waits here for the key.
    let mut pending_expiry: Option<u64> = None;
    // The search module's data is skipped with one warning per file.
    let mut warned_search = false;
    // Each function library's code, in the order the file has them.
    let mut libraries = Vec::new();

    loop {
        let mut type_byte = [0u8];
        r.read_exact(&mut type_byte)?;

        match type_byte[0] {
            0xFA => {
                // Aux field — key/value metadata, just skip both strings
                let _key = rdb_read_string(&mut r)?;
                let _val = rdb_read_string(&mut r)?;
            }
            0xFE => {
                // Database selector
                let (db_num, _) = rdb_read_length(&mut r)?;
                current_db = db_num as usize;
                if current_db >= result.len() {
                    current_db = 0;
                }
            }
            0xFB => {
                // Resize DB hint (RDB v7+) — two lengths, both skippable
                let _ = rdb_read_length(&mut r)?;
                let _ = rdb_read_length(&mut r)?;
            }
            0xFF => {
                // End of RDB — skip 8-byte CRC64
                let mut crc = [0u8; 8];
                let _ = r.read_exact(&mut crc); // ignore error — file may be truncated
                break;
            }
            0xFC => {
                // Expire time in milliseconds (u64 LE), for the next key
                let mut exp_buf = [0u8; 8];
                r.read_exact(&mut exp_buf)?;
                pending_expiry = Some(u64::from_le_bytes(exp_buf));
            }
            0xFD => {
                // Expire time in seconds (u32 LE), for the next key
                let mut exp_buf = [0u8; 4];
                r.read_exact(&mut exp_buf)?;
                pending_expiry = Some(u64::from(u32::from_le_bytes(exp_buf)) * 1000);
            }
            0xF8 => {
                // The next key's idle time in seconds, written under an LRU
                // policy. Not kept: the key's access record is not set from
                // a dump yet (#71).
                let _ = rdb_read_length(&mut r)?;
            }
            0xF9 => {
                // The next key's access frequency, one byte, written under an
                // LFU policy. Not kept either.
                let mut freq = [0u8];
                r.read_exact(&mut freq)?;
            }
            0xF4 => {
                // Slot info, from a cluster node: the slot, its number of
                // keys and of keys with a TTL. Redis skips it outside cluster
                // mode, and the keys say which slot they are in anyway.
                for _ in 0..3 {
                    let _ = rdb_read_length(&mut r)?;
                }
            }
            0xF7 => rdb_skip_module_aux(&mut r, &mut warned_search)?,
            // A function library (`FUNCTION2`): its code.
            0xF5 => libraries.push(rdb_read_string(&mut r)?),
            // Every other opcode from 0xF0 up is refused by what it is, not
            // read as a key's type.
            opcode @ 0xF0..=0xF6 => {
                return Err(rdb_invalid(match opcode {
                    0xF2 => "hash templates (opcode 0xF2) are not supported".to_string(),
                    0xF3 => "module key metadata (opcode 0xF3) is not supported".to_string(),
                    0xF6 => "function libraries in Redis 7.0's pre-release format \
                             (opcode 0xF6) are not supported"
                        .to_string(),
                    other => format!("unknown opcode {other:#04x}"),
                }));
            }
            type_id => {
                // A key: type byte already consumed
                let key_bytes = rdb_read_string(&mut r)?;
                let key = String::from_utf8_lossy(&key_bytes).into_owned();
                let expiry_ms = pending_expiry.take();
                if let Some(val) = rdb_read_value(&mut r, type_id)? {
                    match expiry_ms {
                        // Already past its expiry: dropped.
                        Some(ms) if ms <= now_ms => {}
                        Some(ms) => {
                            expiries[current_db].insert(key.clone(), ms);
                            result[current_db].insert(key, val);
                        }
                        None => {
                            // A plain entry replaces any earlier one for this
                            // key, including that entry's expiry.
                            expiries[current_db].remove(&key);
                            result[current_db].insert(key, val);
                        }
                    }
                }
            }
        }
    }

    Ok((result, expiries, redis_function_dump(&libraries)))
}

/// Function libraries' code as Redis's `FUNCTION DUMP` lays it out: the
/// `FUNCTION2` opcode and an RDB string for each, then an RDB version and a
/// CRC64. That is what `FUNCTION RESTORE` takes from Redis, so the server
/// loads a file's libraries the same way. `None` when there are none.
fn redis_function_dump(libraries: &[Vec<u8>]) -> Option<Vec<u8>> {
    if libraries.is_empty() {
        return None;
    }
    let mut payload = Vec::new();
    for code in libraries {
        payload.push(0xF5);
        rdb_write_length(&mut payload, code.len());
        payload.extend_from_slice(code);
    }
    payload.extend_from_slice(&REDIS_DUMP_VERSION_MAX.to_le_bytes());
    let crc = redis_crc64(&payload);
    payload.extend_from_slice(&crc.to_le_bytes());
    Some(payload)
}

/// A length as an RDB writes one (`rdbSaveLen`).
fn rdb_write_length(out: &mut Vec<u8>, len: usize) {
    match len {
        0..=0x3F => out.push(len as u8),
        0x40..=0x3FFF => out.extend_from_slice(&[0x40 | (len >> 8) as u8, len as u8]),
        _ => match u32::try_from(len) {
            Ok(len) => {
                out.push(0x80);
                out.extend_from_slice(&len.to_be_bytes());
            }
            Err(_) => {
                out.push(0x81);
                out.extend_from_slice(&(len as u64).to_be_bytes());
            }
        },
    }
}

fn rdb_read_value(r: &mut impl Read, type_id: u8) -> io::Result<Option<KiviDBValue>> {
    match type_id {
        0 => {
            // String
            let bytes = rdb_read_string(r)?;
            Ok(Some(KiviDBValue::String(SharedBytes::from(
                bytes.as_slice(),
            ))))
        }
        1 => {
            // Linked list (RDB v7 and earlier) or quicklist
            let (count, _) = rdb_read_length(r)?;
            let mut deque = VecDeque::new();
            for _ in 0..count {
                let item = rdb_read_string(r)?;
                deque.push_back(Arc::from(item.as_slice()));
            }
            Ok(Some(KiviDBValue::List(Arc::new(parking_lot::RwLock::new(
                deque,
            )))))
        }
        2 => {
            // Set (hashtable encoding)
            let (count, _) = rdb_read_length(r)?;
            let mut set = crate::member_set::MemberSet::new();
            for _ in 0..count {
                let member = rdb_read_string(r)?;
                set.insert(Arc::from(member.as_slice()));
            }
            Ok(Some(KiviDBValue::Set(Arc::new(parking_lot::RwLock::new(
                set,
            )))))
        }
        3 => {
            // Sorted set v1 (score as double string)
            let (count, _) = rdb_read_length(r)?;
            let mut ss = SortedSetInner::new();
            for _ in 0..count {
                let member = rdb_read_string(r)?;
                let score = rdb_read_double_v1(r)?;
                ss.insert(Arc::from(member.as_slice()), score);
            }
            Ok(Some(KiviDBValue::SortedSet(Arc::new(
                parking_lot::RwLock::new(ss),
            ))))
        }
        4 => {
            // Hash (hashtable encoding)
            let (count, _) = rdb_read_length(r)?;
            let mut hmap = crate::types::HashFields::default();
            for _ in 0..count {
                let field_bytes = rdb_read_string(r)?;
                let field = String::from_utf8_lossy(&field_bytes).into_owned();
                let val = rdb_read_string(r)?;
                hmap.insert(field, Arc::from(val.as_slice()));
            }
            Ok(Some(KiviDBValue::Hash(Arc::new(parking_lot::RwLock::new(
                hmap,
            )))))
        }
        5 => {
            // Sorted set v2 (score as binary double)
            let (count, _) = rdb_read_length(r)?;
            let mut ss = SortedSetInner::new();
            for _ in 0..count {
                let member = rdb_read_string(r)?;
                let mut score_buf = [0u8; 8];
                r.read_exact(&mut score_buf)?;
                let score = f64::from_le_bytes(score_buf);
                ss.insert(Arc::from(member.as_slice()), score);
            }
            Ok(Some(KiviDBValue::SortedSet(Arc::new(
                parking_lot::RwLock::new(ss),
            ))))
        }
        // The numbers are Redis's (`RDB_TYPE_*` in rdb.h). Up to 1.1.0 most
        // of the compact encodings were filed under the wrong ones, so a
        // `dump.rdb` or a `DUMP` payload from Redis 6 or 7 holding lists,
        // small hashes or small sets did not load (#39).
        10 => {
            // RDB_TYPE_LIST_ZIPLIST (Redis 2.6 to 3.0): one ziplist.
            let blob = rdb_read_string(r)?;
            let mut deque = VecDeque::new();
            rdb_decode_ziplist_into_list(&blob, &mut deque);
            Ok(Some(KiviDBValue::List(Arc::new(parking_lot::RwLock::new(
                deque,
            )))))
        }
        11 => {
            // RDB_TYPE_SET_INTSET: a set of integers, one blob of a header
            // and the sorted values.
            let blob = rdb_read_string(r)?;
            let mut set = crate::member_set::MemberSet::new();
            if blob.len() >= 8 {
                let encoding = u32::from_le_bytes(blob[0..4].try_into().unwrap_or([0; 4]));
                let length = u32::from_le_bytes(blob[4..8].try_into().unwrap_or([0; 4])) as usize;
                let int_size = match encoding {
                    2 => 2usize,
                    4 => 4usize,
                    8 => 8usize,
                    _ => 8,
                };
                let data = &blob[8..];
                for i in 0..length {
                    let start = i * int_size;
                    let end = start + int_size;
                    if end > data.len() {
                        break;
                    }
                    let val: i64 =
                        match int_size {
                            2 => i16::from_le_bytes(data[start..end].try_into().unwrap_or([0; 2]))
                                as i64,
                            4 => i32::from_le_bytes(data[start..end].try_into().unwrap_or([0; 4]))
                                as i64,
                            8 => i64::from_le_bytes(data[start..end].try_into().unwrap_or([0; 8])),
                            _ => 0,
                        };
                    let s = val.to_string();
                    set.insert(Arc::from(s.as_bytes()));
                }
            }
            Ok(Some(KiviDBValue::Set(Arc::new(parking_lot::RwLock::new(
                set,
            )))))
        }
        12 => {
            // RDB_TYPE_ZSET_ZIPLIST (Redis up to 6.2): member, score, member,
            // score..., the scores as text.
            let blob = rdb_read_string(r)?;
            let mut ss = SortedSetInner::new();
            let mut deque = VecDeque::new();
            rdb_decode_ziplist_into_list(&blob, &mut deque);
            let mut iter = deque.into_iter();
            while let (Some(member), Some(score_bytes)) = (iter.next(), iter.next()) {
                let score: f64 = std::str::from_utf8(&score_bytes)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "bad ziplist zset score")
                    })?;
                ss.insert(Arc::from(member.as_ref()), score);
            }
            Ok(Some(KiviDBValue::SortedSet(Arc::new(
                parking_lot::RwLock::new(ss),
            ))))
        }
        13 => {
            // RDB_TYPE_HASH_ZIPLIST (Redis up to 6.2): field, value, ...
            let blob = rdb_read_string(r)?;
            let mut hmap = crate::types::HashFields::default();
            let mut deque = VecDeque::new();
            rdb_decode_ziplist_into_list(&blob, &mut deque);
            let mut iter = deque.into_iter();
            while let (Some(field), Some(val)) = (iter.next(), iter.next()) {
                let field_str = String::from_utf8_lossy(&field).into_owned();
                hmap.insert(field_str, val);
            }
            Ok(Some(KiviDBValue::Hash(Arc::new(parking_lot::RwLock::new(
                hmap,
            )))))
        }
        14 => {
            // RDB_TYPE_LIST_QUICKLIST (Redis 3.2 to 6.2): a count of nodes,
            // each one ziplist.
            let (node_count, _) = rdb_read_length(r)?;
            let mut deque = VecDeque::new();
            for _ in 0..node_count {
                let blob = rdb_read_string(r)?;
                rdb_decode_ziplist_into_list(&blob, &mut deque);
            }
            Ok(Some(KiviDBValue::List(Arc::new(parking_lot::RwLock::new(
                deque,
            )))))
        }
        16 => {
            // RDB_TYPE_HASH_LISTPACK (Redis 7.0+).
            let blob = rdb_read_string(r)?;
            let mut hmap = crate::types::HashFields::default();
            rdb_decode_listpack_into_hash(&blob, &mut hmap)?;
            Ok(Some(KiviDBValue::Hash(Arc::new(parking_lot::RwLock::new(
                hmap,
            )))))
        }
        17 => {
            // RDB_TYPE_ZSET_LISTPACK (Redis 7.0+).
            let blob = rdb_read_string(r)?;
            let mut ss = SortedSetInner::new();
            rdb_decode_listpack_into_zset(&blob, &mut ss)?;
            Ok(Some(KiviDBValue::SortedSet(Arc::new(
                parking_lot::RwLock::new(ss),
            ))))
        }
        18 => {
            // RDB_TYPE_LIST_QUICKLIST_2 (Redis 7.0+): a count of nodes, each
            // its container (1 a plain element, 2 a listpack) and then its
            // bytes, in that order.
            let (node_count, _) = rdb_read_length(r)?;
            let mut deque = VecDeque::new();
            for _ in 0..node_count {
                let (container, _) = rdb_read_length(r)?;
                let blob = rdb_read_string(r)?;
                match container {
                    1 => deque.push_back(Arc::from(blob.as_slice())),
                    2 => rdb_decode_listpack_into_list(&blob, &mut deque)?,
                    other => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("unknown quicklist container {other}"),
                        ));
                    }
                }
            }
            Ok(Some(KiviDBValue::List(Arc::new(parking_lot::RwLock::new(
                deque,
            )))))
        }
        20 => {
            // RDB_TYPE_SET_LISTPACK (Redis 7.2+).
            let blob = rdb_read_string(r)?;
            let mut set = crate::member_set::MemberSet::new();
            let mut deque = VecDeque::new();
            rdb_decode_listpack_into_list(&blob, &mut deque)?;
            for item in deque {
                set.insert(item);
            }
            Ok(Some(KiviDBValue::Set(Arc::new(parking_lot::RwLock::new(
                set,
            )))))
        }
        // RDB_TYPE_STREAM_LISTPACKS, _2 (Redis 7.0), _3 (7.2), _4 (8.6) and
        // _5 (8.10).
        15 | 19 | 21 | 26 | 27 => rdb_read_stream(r, type_id).map(Some),
        // RDB_TYPE_HASH_METADATA and RDB_TYPE_HASH_LISTPACK_EX (Redis 7.4+):
        // hashes whose fields have their own TTLs.
        24 | 25 => rdb_read_hash_with_ttls(r, type_id).map(Some),
        // RDB_TYPE_MODULE_2: a module's value. RedisJSON's is read.
        7 => rdb_read_module_value(r).map(Some),
        unknown => {
            // Unknown type — we can't skip because we don't know the length.
            // Log and return an error. The caller can decide to abort or ignore.
            log_warn!("RDB: unknown type byte {unknown:#04x} — stopping load");
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported RDB type {unknown}"),
            ))
        }
    }
}

fn rdb_invalid(what: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("RDB: {what}"))
}

/// A length as an RDB writes one, not an integer encoding.
fn rdb_read_plain_length(r: &mut impl Read) -> io::Result<u64> {
    match rdb_read_length(r)? {
        (n, false) => Ok(n),
        (_, true) => Err(rdb_invalid("an encoded integer where a length belongs")),
    }
}

/// A time in milliseconds, as `rdbSaveMillisecondTime` writes it: 8 bytes,
/// little-endian, signed (-1 for "never").
fn rdb_read_millisecond_time(r: &mut impl Read) -> io::Result<i64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    Ok(i64::from_le_bytes(buf))
}

/// A stream ID as Redis stores one raw (a radix-tree key, a PEL entry): the
/// milliseconds and the sequence, 8 bytes each, big-endian.
fn raw_stream_id(raw: &[u8]) -> Option<crate::types::StreamId> {
    let raw: &[u8; 16] = raw.try_into().ok()?;
    Some(crate::types::StreamId {
        ms: u64::from_be_bytes(raw[..8].try_into().unwrap()),
        seq: u64::from_be_bytes(raw[8..].try_into().unwrap()),
    })
}

fn rdb_read_raw_stream_id(r: &mut impl Read) -> io::Result<crate::types::StreamId> {
    let mut raw = [0u8; 16];
    r.read_exact(&mut raw)?;
    Ok(raw_stream_id(&raw).unwrap())
}

fn rdb_read_stream_id(r: &mut impl Read) -> io::Result<crate::types::StreamId> {
    Ok(crate::types::StreamId {
        ms: rdb_read_plain_length(r)?,
        seq: rdb_read_plain_length(r)?,
    })
}

/// The elements of a listpack, one at a time.
struct LpCursor<'a>(std::slice::Iter<'a, Vec<u8>>);

impl<'a> LpCursor<'a> {
    fn bytes(&mut self) -> io::Result<&'a [u8]> {
        self.0
            .next()
            .map(Vec::as_slice)
            .ok_or_else(|| rdb_invalid("listpack ends too soon"))
    }

    fn int(&mut self) -> io::Result<i64> {
        std::str::from_utf8(self.bytes()?)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| rdb_invalid("listpack element is not an integer"))
    }

    fn count(&mut self) -> io::Result<u64> {
        u64::try_from(self.int()?).map_err(|_| rdb_invalid("negative count in a listpack"))
    }

    fn is_empty(&self) -> bool {
        self.0.len() == 0
    }
}

/// A stream (`rdbLoadObject`'s stream branch, Redis 5 to 8.10).
///
/// Its entries are in listpacks keyed by a master ID. Each listpack opens
/// with a master entry, `count | deleted | n | field_1 ... field_n | 0`, and
/// every entry after it is `flags | ms - master ms | seq - master seq` (signed), then
/// either its values alone (flag SAMEFIELDS: the master's field names) or
/// `n | field | value ...`, then the number of elements it took (read
/// right to left). Deleted entries stay in the listpack, flagged.
///
/// After the listpacks: the length, the last ID, then (type 19 on) the first
/// ID, the largest deleted ID and the number of entries ever added. Then the
/// consumer groups, each with its last-delivered ID, (type 19 on) its
/// entries read, its pending entries (raw ID, delivery time, count), and its
/// consumers (name, seen time, (type 21 on) active time, and the IDs of the
/// pending entries it owns). Type 27 adds, per group, the IDs of the pending
/// entries `XNACK` released: owned by no consumer, as KiviDB keeps those.
/// Type 26 on ends with idempotent production: duration, size, and for each
/// producer its idempotent IDs and the entries they made, then two counters.
fn rdb_read_stream(r: &mut impl Read, type_id: u8) -> io::Result<KiviDBValue> {
    use crate::types::{
        ConsumerGroup, IdmpRecord, PelEntry, ProducerIdmp, StreamEntry, StreamInner,
    };
    const DELETED: i64 = 1;
    const SAMEFIELDS: i64 = 2;

    let mut stream = StreamInner::new();
    for _ in 0..rdb_read_plain_length(r)? {
        let master = raw_stream_id(&rdb_read_string(r)?)
            .ok_or_else(|| rdb_invalid("stream node key is not a stream ID"))?;
        let elements = lp_elements(&rdb_read_string(r)?)?;
        let mut lp = LpCursor(elements.iter());
        let live = lp.count()?;
        let deleted = lp.count()?;
        let master_fields: Vec<Arc<[u8]>> = (0..lp.count()?)
            .map(|_| lp.bytes().map(Arc::from))
            .collect::<io::Result<_>>()?;
        if lp.int()? != 0 {
            return Err(rdb_invalid("stream master entry is not closed by 0"));
        }
        let (mut seen_live, mut seen_deleted) = (0u64, 0u64);
        while !lp.is_empty() {
            let flags = lp.int()?;
            // Signed differences: an entry's sequence is often below the
            // master's (`5-3`, then `6-0`), so `seq - master seq` is negative.
            let offset =
                |base: u64, diff: i64| u64::try_from(i128::from(base) + i128::from(diff)).ok();
            let ms = offset(master.ms, lp.int()?);
            let seq = offset(master.seq, lp.int()?);
            let (Some(ms), Some(seq)) = (ms, seq) else {
                return Err(rdb_invalid("stream entry ID out of range"));
            };
            let fields: crate::types::FieldsType = if flags & SAMEFIELDS != 0 {
                master_fields
                    .iter()
                    .map(|field| Ok((Arc::clone(field), Arc::from(lp.bytes()?))))
                    .collect::<io::Result<_>>()?
            } else {
                (0..lp.count()?)
                    .map(|_| Ok((Arc::from(lp.bytes()?), Arc::from(lp.bytes()?))))
                    .collect::<io::Result<_>>()?
            };
            lp.int()?; // how many elements the entry took
            if flags & DELETED != 0 {
                seen_deleted += 1;
            } else {
                seen_live += 1;
                stream
                    .entries
                    .insert(crate::types::StreamId { ms, seq }, StreamEntry { fields });
            }
        }
        if (seen_live, seen_deleted) != (live, deleted) {
            return Err(rdb_invalid(
                "stream node's entry counts do not match its entries",
            ));
        }
    }

    let length = rdb_read_plain_length(r)?;
    stream.last_id = rdb_read_stream_id(r)?;
    if type_id >= 19 {
        rdb_read_stream_id(r)?; // the first ID: KiviDB has its entries
        stream.max_deleted_id = rdb_read_stream_id(r)?;
        stream.entries_added = rdb_read_plain_length(r)?;
    } else {
        stream.entries_added = length;
    }
    if length != stream.entries.len() as u64 {
        return Err(rdb_invalid("stream length does not match its entries"));
    }

    for _ in 0..rdb_read_plain_length(r)? {
        let name = String::from_utf8(rdb_read_string(r)?)
            .map_err(|_| rdb_invalid("consumer group name is not UTF-8"))?;
        let last_id = rdb_read_stream_id(r)?;
        let entries_read = if type_id >= 19 {
            rdb_read_plain_length(r)?
        } else {
            estimated_entries_read(&stream, &last_id)
        };
        let mut group = ConsumerGroup {
            name: name.clone(),
            last_id,
            // A group whose count Redis does not know saves -1, which reads
            // back as KiviDB's "not known". Before type 19 there is none, and
            // Redis estimates it from the group's last ID, as this does.
            entries_read,
            pel: Default::default(),
            consumers: Default::default(),
            active: Default::default(),
        };
        for _ in 0..rdb_read_plain_length(r)? {
            let id = rdb_read_raw_stream_id(r)?;
            let delivered_at = rdb_read_millisecond_time(r)?.max(0) as u64;
            let delivery_count = rdb_read_plain_length(r)?;
            // Owned by no one until a consumer below claims it.
            let entry = PelEntry {
                consumer: String::new(),
                delivered_at,
                delivery_count,
            };
            if group.pel.insert(id, entry).is_some() {
                return Err(rdb_invalid("a pending entry is listed twice"));
            }
        }
        for _ in 0..rdb_read_plain_length(r)? {
            let consumer = String::from_utf8(rdb_read_string(r)?)
                .map_err(|_| rdb_invalid("consumer name is not UTF-8"))?;
            let seen = rdb_read_millisecond_time(r)?;
            let active = if type_id >= 21 {
                rdb_read_millisecond_time(r)?
            } else {
                seen
            };
            group.consumers.insert(consumer.clone(), seen.max(0) as u64);
            // -1: never handed an entry.
            if active > 0 {
                group.active.insert(consumer.clone(), active as u64);
            }
            for _ in 0..rdb_read_plain_length(r)? {
                let id = rdb_read_raw_stream_id(r)?;
                match group.pel.get_mut(&id) {
                    Some(entry) if entry.consumer.is_empty() => entry.consumer = consumer.clone(),
                    _ => return Err(rdb_invalid("a consumer owns an entry the group does not")),
                }
            }
        }
        if type_id >= 27 {
            for _ in 0..rdb_read_plain_length(r)? {
                let id = rdb_read_raw_stream_id(r)?;
                match group.pel.get_mut(&id) {
                    Some(entry) if entry.consumer.is_empty() => entry.delivered_at = 0,
                    _ => return Err(rdb_invalid("a released entry is owned or not pending")),
                }
            }
        }
        stream.groups.insert(name, group);
    }

    if type_id >= 26 {
        stream.idmp.duration_secs = Some(rdb_read_plain_length(r)?);
        stream.idmp.maxsize = Some(rdb_read_plain_length(r)?);
        for _ in 0..rdb_read_plain_length(r)? {
            let producer: Arc<[u8]> = Arc::from(rdb_read_string(r)?.as_slice());
            let mut state = ProducerIdmp::default();
            for _ in 0..rdb_read_plain_length(r)? {
                let iid: Arc<[u8]> = Arc::from(rdb_read_string(r)?.as_slice());
                let entry_id = rdb_read_stream_id(r)?;
                state.by_iid.insert(Arc::clone(&iid), entry_id.clone());
                // Redis keeps no time of its own for these: an ID is
                // forgotten by its entry's milliseconds.
                let added_ms = entry_id.ms;
                state.order.push_back(IdmpRecord {
                    iid,
                    entry_id,
                    added_ms,
                });
            }
            if !state.order.is_empty() {
                stream.idmp.producers.insert(producer, state);
            }
        }
        stream.idmp.iids_added = rdb_read_plain_length(r)?;
        stream.idmp.iids_duplicates = rdb_read_plain_length(r)?;
    }
    Ok(KiviDBValue::Stream(Arc::new(parking_lot::RwLock::new(
        stream,
    ))))
}

/// How many of a stream's entries a group whose last-delivered ID is `id`
/// has been delivered, for a stream from before RDB type 19, which did not
/// record it: Redis's `streamEstimateDistanceFromFirstEverEntry`, for a
/// stream that (from such a file) has no deletions on record.
fn estimated_entries_read(stream: &crate::types::StreamInner, id: &crate::types::StreamId) -> u64 {
    use crate::types::ConsumerGroup;
    let key = |id: &crate::types::StreamId| (id.ms, id.seq);
    let added = stream.entries_added;
    let length = stream.entries.len() as u64;
    if added == 0 {
        return 0;
    }
    let last = key(&stream.last_id);
    if key(id) > last {
        return ConsumerGroup::NO_COUNT;
    }
    if length == 0 || key(id) == last {
        return added;
    }
    let first = stream.entries.keys().next().map(key).unwrap();
    match key(id).cmp(&first) {
        std::cmp::Ordering::Less => added - length,
        std::cmp::Ordering::Equal => added - length + 1,
        std::cmp::Ordering::Greater => ConsumerGroup::NO_COUNT,
    }
}

/// A hash whose fields have their own TTLs (Redis 7.4+). Both forms open with
/// the soonest expiry in the hash, in milliseconds.
///
/// - Type 24, a hash table: the field count, then for each field its TTL,
///   its name and its value. A TTL of 0 is none; any other is the expiry
///   less the soonest one, plus 1.
/// - Type 25, a listpack of field, value, expiry triples, each expiry in
///   Unix milliseconds or 0 for none.
///
/// Fields already past their time are dropped, as Redis drops them.
fn rdb_read_hash_with_ttls(r: &mut impl Read, type_id: u8) -> io::Result<KiviDBValue> {
    let soonest = rdb_read_millisecond_time(r)?;
    let mut fields: Vec<(Vec<u8>, Vec<u8>, u64)> = Vec::new();
    if type_id == 24 {
        for _ in 0..rdb_read_plain_length(r)? {
            let ttl = rdb_read_plain_length(r)?;
            let field = rdb_read_string(r)?;
            let value = rdb_read_string(r)?;
            let expires_at = if ttl == 0 {
                0
            } else {
                (ttl - 1)
                    .checked_add(
                        u64::try_from(soonest).map_err(|_| rdb_invalid("bad hash expiry"))?,
                    )
                    .ok_or_else(|| rdb_invalid("bad hash expiry"))?
            };
            fields.push((field, value, expires_at));
        }
    } else {
        let elements = lp_elements(&rdb_read_string(r)?)?;
        if elements.len() % 3 != 0 {
            return Err(rdb_invalid(
                "hash listpack is not field, value, TTL triples",
            ));
        }
        let mut lp = LpCursor(elements.iter());
        while !lp.is_empty() {
            let field = lp.bytes()?.to_vec();
            let value = lp.bytes()?.to_vec();
            fields.push((field, value, lp.count()?));
        }
    }

    let now_ms = unix_ms_now();
    let now = std::time::Instant::now();
    let mut inner = crate::types::HashInner::new();
    for (field, value, expires_at) in fields {
        if expires_at != 0 && expires_at <= now_ms {
            continue;
        }
        let field = String::from_utf8_lossy(&field).into_owned();
        if expires_at != 0 {
            let left = std::time::Duration::from_millis(expires_at - now_ms);
            inner.expiries.insert(field.clone(), now + left);
        }
        inner.fields.insert(field, Arc::from(value.as_slice()));
    }
    Ok(KiviDBValue::HashEx(Arc::new(parking_lot::RwLock::new(
        inner,
    ))))
}

/// The name a module ID carries: 9 characters of 6 bits each, then 10 bits of
/// encoding version (`moduleTypeEncodeId`).
fn module_type_name(id: u64) -> (String, u64) {
    const CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let name = (0..9)
        .map(|i| CHARS[((id >> (64 - 6 * (i + 1))) & 63) as usize] as char)
        .collect();
    (name, id & 1023)
}

/// A module's value (`RDB_TYPE_MODULE_2`): the module ID, then what the
/// module saved, each piece announced by its kind (`RDB_MODULE_OPCODE_*`),
/// up to an end marker. Only RedisJSON's (`ReJSON-RL`) is read: one string,
/// the document, in CBOR from encoding version 4 (Redis 8) and as JSON text
/// before. Other modules' values (time series, Bloom filters, vector sets)
/// are refused.
fn rdb_read_module_value(r: &mut impl Read) -> io::Result<KiviDBValue> {
    let (name, encoding_version) = module_type_name(rdb_read_plain_length(r)?);
    if name != "ReJSON-RL" {
        return Err(rdb_invalid(format!(
            "values of module type {name} are not supported"
        )));
    }
    let strings = rdb_read_module_strings(r)?;
    let [document] = strings.as_slice() else {
        return Err(rdb_invalid("a RedisJSON value is not one string"));
    };
    let json = if encoding_version >= 4 {
        cbor_to_json(document)?
    } else {
        serde_json::from_slice(document).map_err(|e| rdb_invalid(format!("RedisJSON: {e}")))?
    };
    Ok(KiviDBValue::Json(Arc::new(parking_lot::RwLock::new(json))))
}

/// What a module saved, each piece announced by its kind
/// (`RDB_MODULE_OPCODE_*`), up to the end marker. Only the strings are kept.
fn rdb_read_module_strings(r: &mut impl Read) -> io::Result<Vec<Vec<u8>>> {
    let mut strings = Vec::new();
    loop {
        match rdb_read_plain_length(r)? {
            0 => return Ok(strings),
            1 | 2 => {
                rdb_read_plain_length(r)?;
            }
            3 => r.read_exact(&mut [0u8; 4])?,
            4 => r.read_exact(&mut [0u8; 8])?,
            5 => strings.push(rdb_read_string(r)?),
            other => return Err(rdb_invalid(format!("unknown module opcode {other}"))),
        }
    }
}

/// A module's own data (`MODULE_AUX`): the module ID, when it was saved
/// (before or after the keys), then what the module saved.
///
/// The search module's, its index definitions (`ft_index0`) and dictionaries
/// (`scdtype00`), is read past, with a warning the first time it holds any.
/// KiviDB keeps indexes of its own, and `FT.CREATE` indexes the keys already
/// there. Any
/// other module's is refused by its name, as Redis refuses aux data of a
/// module it has not loaded.
fn rdb_skip_module_aux(r: &mut impl Read, warned: &mut bool) -> io::Result<()> {
    let (name, _) = module_type_name(rdb_read_plain_length(r)?);
    if name != "ft_index0" && name != "scdtype00" {
        return Err(rdb_invalid(format!(
            "data of module type {name} (opcode 0xF7) is not supported"
        )));
    }
    // `when_opcode` is always an unsigned integer's, as in Redis.
    if rdb_read_plain_length(r)? != 2 {
        return Err(rdb_invalid("module aux data: bad when_opcode"));
    }
    rdb_read_plain_length(r)?;
    // Redis Stack writes it with nothing in it: a count of 0 and no names.
    let names = rdb_read_module_strings(r)?;
    if !names.is_empty() && !*warned {
        log_warn!(
            "RDB: the search module's index definitions and dictionaries are not loaded; \
             run FT.CREATE and FT.DICTADD again to make them"
        );
        *warned = true;
    }
    Ok(())
}

/// A CBOR document (RFC 8949) as JSON: what RedisJSON 2.8+ saves. Only what
/// a JSON document can be is read: integers, floats, text, arrays, maps with
/// text keys, true, false and null.
fn cbor_to_json(data: &[u8]) -> io::Result<serde_json::Value> {
    let mut at = 0;
    let value = cbor_value(data, &mut at, 0)?;
    if at != data.len() {
        return Err(rdb_invalid("bytes after the CBOR document"));
    }
    Ok(value)
}

/// The byte string (major type 2, of a definite length) at `at`, if that is
/// what is there.
fn cbor_value_bytes<'a>(data: &'a [u8], at: &mut usize) -> io::Result<Option<&'a [u8]>> {
    let bad = || rdb_invalid("malformed CBOR");
    let head = *data.get(*at).ok_or_else(bad)?;
    if head >> 5 != 2 {
        return Ok(None);
    }
    let info = head & 31;
    let (len, skip) = match info {
        0..=23 => (u64::from(info), 1),
        24 => (u64::from(*data.get(*at + 1).ok_or_else(bad)?), 2),
        25 => (
            u64::from(u16::from_be_bytes(
                data.get(*at + 1..*at + 3)
                    .ok_or_else(bad)?
                    .try_into()
                    .unwrap(),
            )),
            3,
        ),
        26 => (
            u64::from(u32::from_be_bytes(
                data.get(*at + 1..*at + 5)
                    .ok_or_else(bad)?
                    .try_into()
                    .unwrap(),
            )),
            5,
        ),
        27 => (
            u64::from_be_bytes(
                data.get(*at + 1..*at + 9)
                    .ok_or_else(bad)?
                    .try_into()
                    .unwrap(),
            ),
            9,
        ),
        _ => return Ok(None),
    };
    let start = *at + skip;
    let end = start
        .checked_add(usize::try_from(len).map_err(|_| bad())?)
        .ok_or_else(bad)?;
    let bytes = data.get(start..end).ok_or_else(bad)?;
    *at = end;
    Ok(Some(bytes))
}

/// The JSON array an RFC 8746 typed array stands for: tag `64 + 16f + 8s +
/// 4e + ll`, `f` a float, `s` signed, `e` little-endian (for one-byte
/// unsigned, clamped), elements of 2^ll bytes (2^(ll+1) for a float).
/// `None` for one it does not describe, or bytes that do not divide into it.
fn typed_array(tag: u64, bytes: &[u8]) -> Option<serde_json::Value> {
    use serde_json::Value;
    let bits = tag - 64;
    let (float, signed, little, ll) = (bits & 16 != 0, bits & 8 != 0, bits & 4 != 0, bits & 3);
    let size = if float { 2usize << ll } else { 1usize << ll };
    if !bytes.len().is_multiple_of(size) {
        return None;
    }
    let word = |chunk: &[u8]| -> u128 {
        let mut n = 0u128;
        if little && size > 1 {
            for &b in chunk.iter().rev() {
                n = (n << 8) | u128::from(b);
            }
        } else {
            for &b in chunk {
                n = (n << 8) | u128::from(b);
            }
        }
        n
    };
    let mut items = Vec::with_capacity(bytes.len() / size);
    for chunk in bytes.chunks_exact(size) {
        let n = word(chunk);
        let value = match (float, size) {
            (false, _) if signed => {
                let shift = 128 - 8 * size as u32;
                Value::from(((n << shift) as i128 >> shift) as i64)
            }
            (false, _) => Value::from(n as u64),
            (true, 2) => Value::from(f64::from(crate::commands::json_fpha::f16_bits_to_f32(
                n as u16,
            ))),
            (true, 4) => Value::from(f64::from(f32::from_bits(n as u32))),
            (true, 8) => Value::from(f64::from_bits(n as u64)),
            // 128-bit floats: nothing JSON has.
            _ => return None,
        };
        // `Value::from(f64)` is null for a NaN or an infinity, which no
        // JSON document holds.
        if value.is_null() {
            return None;
        }
        items.push(value);
    }
    Some(Value::Array(items))
}

fn cbor_value(data: &[u8], at: &mut usize, depth: usize) -> io::Result<serde_json::Value> {
    use serde_json::Value;
    let bad = || rdb_invalid("malformed CBOR");
    if depth > 1024 {
        return Err(rdb_invalid("CBOR nested too deeply"));
    }
    let take = |at: &mut usize, n: usize| -> io::Result<&[u8]> {
        let bytes = data.get(*at..*at + n).ok_or_else(bad)?;
        *at += n;
        Ok(bytes)
    };
    let head = take(at, 1)?[0];
    let (major, info) = (head >> 5, head & 31);
    // The argument: a number, a length, or (major 7) which simple value.
    let argument = |at: &mut usize| -> io::Result<Option<u64>> {
        Ok(Some(match info {
            0..=23 => u64::from(info),
            24 => u64::from(take(at, 1)?[0]),
            25 => u64::from(u16::from_be_bytes(take(at, 2)?.try_into().unwrap())),
            26 => u64::from(u32::from_be_bytes(take(at, 4)?.try_into().unwrap())),
            27 => u64::from_be_bytes(take(at, 8)?.try_into().unwrap()),
            31 => return Ok(None), // indefinite length
            _ => return Err(bad()),
        }))
    };
    let float = |f: f64| {
        serde_json::Number::from_f64(f)
            .map(Value::Number)
            .ok_or_else(bad)
    };
    match major {
        0 => Ok(Value::from(argument(at)?.ok_or_else(bad)?)),
        1 => {
            let n = argument(at)?.ok_or_else(bad)?;
            let n = i64::try_from(n).map_err(|_| bad())?;
            Ok(Value::from(-1 - n))
        }
        3 => {
            let text = match argument(at)? {
                Some(n) => take(at, usize::try_from(n).map_err(|_| bad())?)?.to_vec(),
                None => {
                    let mut text = Vec::new();
                    while data.get(*at) != Some(&0xFF) {
                        match cbor_value(data, at, depth + 1)? {
                            Value::String(s) => text.extend_from_slice(s.as_bytes()),
                            _ => return Err(bad()),
                        }
                    }
                    *at += 1;
                    text
                }
            };
            String::from_utf8(text)
                .map(Value::String)
                .map_err(|_| bad())
        }
        4 => {
            let mut items = Vec::new();
            match argument(at)? {
                Some(n) => {
                    for _ in 0..n {
                        items.push(cbor_value(data, at, depth + 1)?);
                    }
                }
                None => {
                    while data.get(*at) != Some(&0xFF) {
                        items.push(cbor_value(data, at, depth + 1)?);
                    }
                    *at += 1;
                }
            }
            Ok(Value::Array(items))
        }
        5 => {
            let mut map = serde_json::Map::new();
            let mut entry = |at: &mut usize| -> io::Result<()> {
                let Value::String(key) = cbor_value(data, at, depth + 1)? else {
                    return Err(rdb_invalid("CBOR map key is not text"));
                };
                let value = cbor_value(data, at, depth + 1)?;
                map.insert(key, value);
                Ok(())
            };
            match argument(at)? {
                Some(n) => {
                    for _ in 0..n {
                        entry(at)?;
                    }
                }
                None => {
                    while data.get(*at) != Some(&0xFF) {
                        entry(at)?;
                    }
                    *at += 1;
                }
            }
            Ok(Value::Object(map))
        }
        6 => {
            let tag = argument(at)?.ok_or_else(bad)?;
            // A typed array (RFC 8746): how RedisJSON 8 saves an array of
            // numbers all of one kind, `[1, 2]` as 8-bit integers, `[1.5]`
            // as half floats. It made the whole snapshot unreadable (#157).
            if (64..=87).contains(&tag) {
                let bytes = match cbor_value_bytes(data, at)? {
                    Some(bytes) => bytes,
                    None => return Err(bad()),
                };
                return typed_array(tag, bytes).ok_or_else(bad);
            }
            // Any other tag, such as a date or a big number: the value it tags.
            cbor_value(data, at, depth + 1)
        }
        7 => match info {
            20 => Ok(Value::Bool(false)),
            21 => Ok(Value::Bool(true)),
            22 | 23 => Ok(Value::Null),
            25 => {
                let bits = u16::from_be_bytes(take(at, 2)?.try_into().unwrap());
                float(f64::from(crate::commands::json_fpha::f16_bits_to_f32(bits)))
            }
            26 => float(f64::from(f32::from_be_bytes(
                take(at, 4)?.try_into().unwrap(),
            ))),
            27 => float(f64::from_be_bytes(take(at, 8)?.try_into().unwrap())),
            _ => Err(bad()),
        },
        _ => Err(rdb_invalid("CBOR byte strings have no JSON form")),
    }
}

fn rdb_read_double_v1(r: &mut impl Read) -> io::Result<f64> {
    let mut len_byte = [0u8];
    r.read_exact(&mut len_byte)?;
    match len_byte[0] {
        253 => return Ok(f64::NAN),
        254 => return Ok(f64::INFINITY),
        255 => return Ok(f64::NEG_INFINITY),
        _ => {}
    }
    let mut buf = vec![0u8; len_byte[0] as usize];
    r.read_exact(&mut buf)?;
    std::str::from_utf8(&buf)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad double encoding"))
}

fn rdb_decode_ziplist_into_list(blob: &[u8], deque: &mut VecDeque<Arc<[u8]>>) {
    // Ziplist layout: zlbytes(4) zltail(4) zllen(2) entries... ZLEND(0xFF)
    if blob.len() < 11 {
        return;
    }
    let mut pos = 10usize; // skip header (4+4+2)
    while pos < blob.len() {
        if blob[pos] == 0xFF {
            break;
        } // ZLEND

        // Previous entry length (1 or 5 bytes)
        let prev_len_size = if blob[pos] < 254 { 1 } else { 5 };
        pos += prev_len_size;
        if pos >= blob.len() {
            break;
        }

        // Encoding byte
        let enc = blob[pos];
        pos += 1;

        let entry_bytes: Vec<u8> = match (enc & 0xC0) >> 6 {
            0 => {
                // 6-bit string length
                let slen = (enc & 0x3F) as usize;
                let s = blob.get(pos..pos + slen).unwrap_or(&[]).to_vec();
                pos += slen;
                s
            }
            1 => {
                // 14-bit string length
                if pos >= blob.len() {
                    break;
                }
                let slen = (((enc & 0x3F) as usize) << 8) | blob[pos] as usize;
                pos += 1;
                let s = blob.get(pos..pos + slen).unwrap_or(&[]).to_vec();
                pos += slen;
                s
            }
            2 => {
                // 32-bit string length
                if pos + 4 > blob.len() {
                    break;
                }
                let slen =
                    u32::from_be_bytes([blob[pos], blob[pos + 1], blob[pos + 2], blob[pos + 3]])
                        as usize;
                pos += 4;
                let s = blob.get(pos..pos + slen).unwrap_or(&[]).to_vec();
                pos += slen;
                s
            }
            3 => {
                // Integer encoding
                let val: i64 = match enc {
                    0xC0 => {
                        // INT16
                        if pos + 2 > blob.len() {
                            break;
                        }
                        let v = i16::from_le_bytes([blob[pos], blob[pos + 1]]) as i64;
                        pos += 2;
                        v
                    }
                    0xD0 => {
                        // INT32
                        if pos + 4 > blob.len() {
                            break;
                        }
                        let v = i32::from_le_bytes([
                            blob[pos],
                            blob[pos + 1],
                            blob[pos + 2],
                            blob[pos + 3],
                        ]) as i64;
                        pos += 4;
                        v
                    }
                    0xE0 => {
                        // INT64
                        if pos + 8 > blob.len() {
                            break;
                        }
                        let v = i64::from_le_bytes(blob[pos..pos + 8].try_into().unwrap_or([0; 8]));
                        pos += 8;
                        v
                    }
                    0xF0 => {
                        // INT24
                        if pos + 3 > blob.len() {
                            break;
                        }
                        let v = (blob[pos] as i32)
                            | ((blob[pos + 1] as i32) << 8)
                            | ((blob[pos + 2] as i32) << 16);
                        let v = if v & 0x800000 != 0 { v | !0xFFFFFF } else { v };
                        pos += 3;
                        v as i64
                    }
                    0xFE => {
                        // INT8
                        if pos >= blob.len() {
                            break;
                        }
                        let v = blob[pos] as i8 as i64;
                        pos += 1;
                        v
                    }
                    _ if (enc & 0xF0) == 0xF0 => {
                        // 4-bit immediate int (0-12)
                        ((enc & 0x0F) as i64) - 1
                    }
                    _ => break,
                };
                val.to_string().into_bytes()
            }
            _ => break,
        };
        deque.push_back(Arc::from(entry_bytes.as_slice()));
    }
}

fn malformed_listpack() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "malformed listpack")
}

/// One element of a listpack (Redis's `listpack.c`) at `pos`: its value, an
/// integer as its decimal text, and where the next element begins, past the
/// element's back-length. `Ok(None)` at the end marker.
///
/// The encodings, by the first byte: `0xxxxxxx` a 7-bit unsigned integer;
/// `10xxxxxx` a string of up to 63 bytes; `110xxxxx` and one more byte a
/// 13-bit signed integer; `1110xxxx` and one more byte the length of a
/// string of up to 4095 bytes; `0xF0` and four bytes the length of a longer
/// one; `0xF1` to `0xF4` 16-, 24-, 32- and 64-bit signed integers. The
/// back-length after each element is its length again, 7 bits a byte, so
/// one to five bytes.
///
/// It used to read `10xxxxxx` as a 13-bit integer and every back-length as
/// one byte, so the string "a" came back as 353 (#39).
fn lp_read_element(blob: &[u8], pos: usize) -> io::Result<Option<(Vec<u8>, usize)>> {
    let enc = *blob.get(pos).ok_or_else(malformed_listpack)?;
    if enc == 0xFF {
        return Ok(None);
    }
    let byte = |i: usize| blob.get(pos + i).copied().ok_or_else(malformed_listpack);
    let bytes = |from: usize, n: usize| {
        blob.get(pos + from..pos + from + n)
            .ok_or_else(malformed_listpack)
    };
    let int = |v: i64| v.to_string().into_bytes();
    let (value, len) = if enc & 0x80 == 0 {
        (int(i64::from(enc & 0x7F)), 1)
    } else if enc & 0xC0 == 0x80 {
        let n = usize::from(enc & 0x3F);
        (bytes(1, n)?.to_vec(), 1 + n)
    } else if enc & 0xE0 == 0xC0 {
        let raw = (i64::from(enc & 0x1F) << 8) | i64::from(byte(1)?);
        (int(if raw & 0x1000 != 0 { raw - 0x2000 } else { raw }), 2)
    } else if enc & 0xF0 == 0xE0 {
        let n = (usize::from(enc & 0x0F) << 8) | usize::from(byte(1)?);
        (bytes(2, n)?.to_vec(), 2 + n)
    } else {
        match enc {
            0xF0 => {
                let n = u32::from_le_bytes(bytes(1, 4)?.try_into().unwrap()) as usize;
                (bytes(5, n)?.to_vec(), 5 + n)
            }
            0xF1 => (
                int(i16::from_le_bytes(bytes(1, 2)?.try_into().unwrap()).into()),
                3,
            ),
            // 24 bits, sign-extended by shifting them to the top and back.
            0xF2 => {
                let b = bytes(1, 3)?;
                let v = i32::from_le_bytes([0, b[0], b[1], b[2]]) >> 8;
                (int(v.into()), 4)
            }
            0xF3 => (
                int(i32::from_le_bytes(bytes(1, 4)?.try_into().unwrap()).into()),
                5,
            ),
            0xF4 => (int(i64::from_le_bytes(bytes(1, 8)?.try_into().unwrap())), 9),
            _ => return Err(malformed_listpack()),
        }
    };
    let backlen = match len {
        0..=127 => 1,
        128..=16_383 => 2,
        16_384..=2_097_151 => 3,
        2_097_152..=268_435_455 => 4,
        _ => 5,
    };
    let next = pos + len + backlen;
    if next > blob.len() {
        return Err(malformed_listpack());
    }
    Ok(Some((value, next)))
}

/// Every element of a listpack, in order. An error unless the elements run
/// exactly to its end marker, its last byte.
fn lp_elements(blob: &[u8]) -> io::Result<Vec<Vec<u8>>> {
    // The header: total bytes (u32) and element count (u16).
    let mut pos = 6usize;
    if blob.len() < pos + 1 {
        return Err(malformed_listpack());
    }
    let mut out = Vec::new();
    while let Some((value, next)) = lp_read_element(blob, pos)? {
        out.push(value);
        pos = next;
    }
    if pos + 1 != blob.len() {
        return Err(malformed_listpack());
    }
    Ok(out)
}

fn rdb_decode_listpack_into_list(blob: &[u8], deque: &mut VecDeque<Arc<[u8]>>) -> io::Result<()> {
    for value in lp_elements(blob)? {
        deque.push_back(Arc::from(value.as_slice()));
    }
    Ok(())
}

/// field, value, field, value...
fn rdb_decode_listpack_into_hash(
    blob: &[u8],
    hmap: &mut crate::types::HashFields,
) -> io::Result<()> {
    let elements = lp_elements(blob)?;
    if elements.len() % 2 != 0 {
        return Err(malformed_listpack());
    }
    let mut iter = elements.into_iter();
    while let (Some(field), Some(value)) = (iter.next(), iter.next()) {
        let field = String::from_utf8_lossy(&field).into_owned();
        hmap.insert(field, Arc::from(value.as_slice()));
    }
    Ok(())
}

/// member, score, member, score..., a score as an integer or as text.
fn rdb_decode_listpack_into_zset(blob: &[u8], ss: &mut SortedSetInner) -> io::Result<()> {
    let elements = lp_elements(blob)?;
    if elements.len() % 2 != 0 {
        return Err(malformed_listpack());
    }
    let mut iter = elements.into_iter();
    while let (Some(member), Some(score)) = (iter.next(), iter.next()) {
        let score: f64 = std::str::from_utf8(&score)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(malformed_listpack)?;
        ss.insert(Arc::from(member.as_slice()), score);
    }
    Ok(())
}

/// Load a snapshot and populate the store directly.
/// Handles the platform difference in RwLock::write() internally.
pub fn restore_into_store(dbs: Vec<HashMap<String, KiviDBValue>>, store: &store::Store) -> usize {
    let mut total = 0usize;
    for (db_idx, map) in dbs.into_iter().enumerate() {
        if db_idx >= store.len() {
            break;
        }
        let db = &store[db_idx];
        total += map.len();
        for (key, value) in map {
            let shard = crate::store::get_shard(db, &key);
            let mut shard_map = write_lock!(shard);
            shard_map.insert(Key::from(key), value);
        }
    }
    total
}

/// The KDB image a master sends a replica for a full resync.
///
/// Built directly into a buffer. This used to round-trip through a temp file
/// with one fixed name in the working directory, so two replicas attaching at
/// the same moment wrote, read and deleted the *same* file — each could ship
/// the other's half-written image, or find it already removed.
///
/// It carries key expiries and index definitions as well as values. A replica
/// that received values only came up with every TTL stripped and no search
/// indexes, and stayed that way after being promoted.
// The server itself goes through `freeze_dataset` + `into_payload`, so that
// only the freeze happens with writes held back; this one-call form is for
// the library's users and tests.
#[allow(dead_code)]
pub fn full_sync_payload(
    store: &crate::store::Store,
    index_store: Option<&crate::vector::IndexStore>,
) -> std::io::Result<Vec<u8>> {
    freeze_dataset(store, index_store)?.into_payload()
}

/// A dataset captured at one instant, not yet laid out as a payload.
///
/// Capturing has to happen while writes are held back, so it does as little as
/// it can get away with. A string value is an immutable shared buffer: taking
/// a reference to it *is* a point-in-time copy, and the bytes can be written
/// out later. Every other type is mutable in place behind its own lock, so it
/// has to be serialised there and then. The split keeps the pause proportional
/// to the number of keys plus the size of the non-string values, rather than
/// to the total size of the dataset.
pub struct FrozenDataset {
    dbs: Vec<FrozenDb>,
    index_section: Vec<u8>,
    alias_section: Vec<u8>,
    /// The function libraries at the same instant.
    functions: Option<Vec<u8>>,
}

/// What one thread of the capture took: a hold on each string's entry
/// (its key, bytes and time to live, which do not change while held),
/// everything else laid out, and the times to live of everything else.
type FrozenPart = (Vec<store::EntryRef>, Vec<u8>, Vec<(Key, u64)>);

#[derive(Default)]
struct FrozenDb {
    /// One part per thread of the capture (see [`freeze`]).
    strings: Vec<Vec<store::EntryRef>>,
    /// Complete KDB entries for every non-string key, likewise.
    encoded: Vec<Vec<u8>>,
    /// The non-string keys with a time to live, and when it ends (Unix
    /// milliseconds), likewise.
    timed: Vec<Vec<(Key, u64)>>,
}

impl FrozenDb {
    /// How many of the keys captured have a time to live.
    fn timed_count(&self) -> usize {
        let strings = self
            .strings
            .iter()
            .flatten()
            .filter(|e| e.expires().is_some())
            .count();
        strings + self.timed.iter().map(Vec::len).sum::<usize>()
    }
}

/// Capture the dataset. The caller is expected to hold the write gate
/// (`write_gate::cut`) so that nothing changes underneath.
pub fn freeze_dataset(
    store: &crate::store::Store,
    index_store: Option<&crate::vector::IndexStore>,
) -> std::io::Result<FrozenDataset> {
    freeze(store, index_store, false)
}

/// [`freeze_dataset`] for `dump.kdb`: with the vector indexes' graphs, which
/// a replica rebuilds but a restart should not have to.
pub fn freeze_dataset_for_disk(
    store: &crate::store::Store,
    index_store: Option<&crate::vector::IndexStore>,
) -> std::io::Result<FrozenDataset> {
    freeze(store, index_store, true)
}

fn freeze(
    store: &crate::store::Store,
    index_store: Option<&crate::vector::IndexStore>,
    with_graphs: bool,
) -> std::io::Result<FrozenDataset> {
    // Writes are held for all of this, and it is a pass over every key: it
    // is spread over several threads, a run of shards each, and a key is
    // copied as the store keeps it (inline up to 24 bytes) rather than into
    // an allocation of its own. With ten million keys on a node it took
    // seconds on one thread, and grew with the keys.
    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .clamp(1, 8);
    let mut dbs = Vec::with_capacity(store.len());
    for db in store.iter() {
        let shards: Vec<_> = db.iter().collect();
        let keys: usize = shards.iter().map(|shard| read_lock!(shard).len()).sum();
        let per = if keys < 100_000 {
            shards.len().max(1)
        } else {
            shards.len().div_ceil(threads).max(1)
        };
        let parts: Vec<std::io::Result<FrozenPart>> = std::thread::scope(|scope| {
            let handles: Vec<_> = shards
                .chunks(per)
                .map(|run| {
                    scope.spawn(move || {
                        let mut strings = Vec::new();
                        let mut encoded = Vec::new();
                        let mut timed = Vec::new();
                        for shard in run {
                            let map = read_lock!(shard);
                            strings.reserve(map.len());
                            for entry in map.entries() {
                                match entry.value() {
                                    // Eight bytes and a count: no copy of
                                    // the key, nor of the bytes, nor of
                                    // its time to live.
                                    ValueRef::String(_) => strings.push(entry.hold()),
                                    other => {
                                        write_kdb_entry(&mut encoded, entry.key(), other)?;
                                        if let Some(at) = entry.expires() {
                                            timed.push((Key::from(entry.key()), at));
                                        }
                                    }
                                }
                            }
                        }
                        Ok((strings, encoded, timed))
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| {
                    h.join()
                        .unwrap_or_else(|_| Err(std::io::Error::other("capture thread panicked")))
                })
                .collect()
        });
        let mut frozen = FrozenDb::default();
        for part in parts {
            let (strings, encoded, timed) = part?;
            frozen.strings.push(strings);
            frozen.encoded.push(encoded);
            frozen.timed.push(timed);
        }
        dbs.push(frozen);
    }

    // For a replica, schemas only: HNSW graphs are large and a replica
    // rebuilds them from the values it has just been sent.
    let mut index_section = Vec::new();
    if with_graphs {
        write_kdb_index_sections(&mut index_section, index_store)?;
    } else {
        write_kdb_index_schemas_only(&mut index_section, index_store)?;
    }
    let mut alias_section = Vec::new();
    write_kdb_alias_section(&mut alias_section, index_store)?;

    Ok(FrozenDataset {
        dbs,
        index_section,
        alias_section,
        functions: function_libraries(),
    })
}

/// The bytes of a string [`freeze`] held.
fn frozen_string(entry: &store::EntryRef) -> &[u8] {
    match entry.value() {
        ValueRef::String(bytes) => bytes.as_bytes(),
        _ => unreachable!("only strings are held"),
    }
}

impl FrozenDataset {
    /// Lay the captured dataset out as a KDB image.
    pub fn into_payload(self) -> std::io::Result<Vec<u8>> {
        let strings_bytes: usize = self
            .dbs
            .iter()
            .flat_map(|db| db.strings.iter().flatten())
            .map(|e| e.key().len() + frozen_string(e).len() + 9)
            .sum();
        let encoded_bytes: usize = self
            .dbs
            .iter()
            .flat_map(|db| db.encoded.iter())
            .map(Vec::len)
            .sum();
        let mut out = Vec::with_capacity(
            16 + strings_bytes + encoded_bytes + self.index_section.len() + 4 * self.dbs.len(),
        );
        self.write_to(&mut out)?;
        Ok(out)
    }

    /// Write the image to `path`, made durable, under that name. The image is
    /// written as it is laid out, not gathered in memory first: for a dump of
    /// the whole dataset that would be the dataset over again.
    pub fn write_file(self, path: &str) -> std::io::Result<()> {
        let raw = File::create(path)?;
        let mut file = BufWriter::with_capacity(1 << 20, raw);
        self.write_to(&mut file)?;
        file.flush()?;
        file.get_ref().sync_all()
    }

    fn write_to(self, out: &mut impl Write) -> std::io::Result<()> {
        write_kdb_header(out, self.dbs.len() as u32)?;
        for db in &self.dbs {
            for entry in db.strings.iter().flatten() {
                write_bytes(out, entry.key().as_bytes())?;
                out.write_all(&[TAG_STRING])?;
                write_bytes(out, frozen_string(entry))?;
            }
            for encoded in &db.encoded {
                out.write_all(encoded)?;
            }
            write_kdb_db_end(out)?;
        }
        out.write_all(&self.index_section)?;
        self.write_expiries(out)?;
        out.write_all(&self.alias_section)?;
        write_kdb_functions_section(out, self.functions.as_deref())
    }
}

impl FrozenDataset {
    /// The expiry section ([`write_kdb_expiry_maps`]'s layout), from the
    /// times the capture took with the keys.
    fn write_expiries(&self, out: &mut impl Write) -> std::io::Result<()> {
        let counts: Vec<usize> = self.dbs.iter().map(FrozenDb::timed_count).collect();
        if counts.iter().all(|&n| n == 0) {
            return Ok(());
        }
        out.write_all(KDB_EXPIRY_MAGIC)?;
        out.write_all(&(self.dbs.len() as u32).to_le_bytes())?;
        for (db, count) in self.dbs.iter().zip(counts) {
            out.write_all(&(count as u32).to_le_bytes())?;
            for entry in db.strings.iter().flatten() {
                if let Some(at) = entry.expires() {
                    write_bytes(out, entry.key().as_bytes())?;
                    out.write_all(&at.to_le_bytes())?;
                }
            }
            for (key, at) in db.timed.iter().flatten() {
                write_bytes(out, key.as_bytes())?;
                out.write_all(&at.to_le_bytes())?;
            }
        }
        Ok(())
    }
}

/// The index schema section without the HNSW graph section.
fn write_kdb_index_schemas_only(
    w: &mut impl Write,
    index_store: Option<&crate::vector::IndexStore>,
) -> io::Result<()> {
    let Some(store) = index_store else {
        return Ok(());
    };
    let guard = store.read();
    let defs: Vec<&crate::vector::IndexDef> = guard
        .iter()
        .filter(|(key, def)| !crate::vector::is_alias(key, def))
        .map(|(_, def)| def)
        .collect();
    if defs.is_empty() {
        return Ok(());
    }
    w.write_all(KDB_INDEX_MAGIC)?;
    w.write_all(&(defs.len() as u32).to_le_bytes())?;
    for def in defs {
        write_index_schema(w, def)?;
    }
    Ok(())
}

/// Trailer of a `DUMP` payload: a 2-byte format version, then the CRC64 of
/// everything before it.
const DUMP_VERSION: [u8; 2] = [0x0a, 0x00];

/// The key a value is filed under inside a one-entry KDB image. Any non-empty
/// name will do — an empty one is the format's end-of-database marker.
const DUMP_IMAGE_KEY: &str = "v";

/// Serialise one value as a `DUMP` payload: what `DUMP` returns, `RESTORE`
/// accepts, and `MIGRATE` sends.
///
/// Most types use the compact per-value encoding. The two it does not cover —
/// time series and vector sets — are wrapped as a one-entry KDB image instead,
/// so every type can be dumped, restored and migrated. `RESTORE` tells the two
/// forms apart by the image's magic.
pub fn dump_payload(value: &KiviDBValue) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    if serialize_value(value, &mut out).is_err() {
        out.clear();
        write_kdb_header(&mut out, 1)?;
        write_kdb_entry(&mut out, DUMP_IMAGE_KEY, value.view())?;
        write_kdb_db_end(&mut out)?;
    }
    let crc = crc64_digest(&out);
    out.extend_from_slice(&DUMP_VERSION);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

/// The newest RDB version whose `DUMP` payloads `RESTORE` reads: the one
/// Redis 8.10 writes. Redis refuses a payload from a newer version than its
/// own, as this does, since a newer one may hold encodings it cannot read.
const REDIS_DUMP_VERSION_MAX: u16 = 15;

/// Parse a `DUMP` payload back into a value. `None` if the payload is
/// truncated, fails its checksum, or does not decode.
///
/// Two kinds are read: KiviDB's own, and Redis's, so that data can be moved
/// in from Redis with `DUMP`/`RESTORE` or `MIGRATE` (and the tools built on
/// them). Their checksums tell them apart: KiviDB's covers the value, Redis's
/// the value and the 2-byte version after it. The version alone would not:
/// Redis 7.0 writes RDB version 10, the number KiviDB's payloads carry.
///
/// The checksum is verified. It used to be stripped unread, so a payload
/// corrupted in transit — or simply wrong — was handed to the decoder as-is.
#[cfg_attr(not(test), allow(dead_code))]
pub fn restore_payload(payload: &[u8]) -> Option<KiviDBValue> {
    restore_payload_checked(payload).ok()
}

/// Why a `RESTORE` payload was refused, in Redis's words.
#[derive(Debug, PartialEq, Eq)]
pub enum RestoreError {
    /// The trailer is not one this server reads.
    VersionOrChecksum,
    /// The trailer is, and what it seals is not a value.
    BadData,
}

impl RestoreError {
    pub fn message(&self) -> &'static str {
        match self {
            RestoreError::VersionOrChecksum => "ERR DUMP payload version or checksum are wrong",
            RestoreError::BadData => "ERR Bad data format",
        }
    }
}

pub fn restore_payload_checked(payload: &[u8]) -> Result<KiviDBValue, RestoreError> {
    if payload.len() < 10 {
        return Err(RestoreError::VersionOrChecksum);
    }
    let (data, trailer) = payload.split_at(payload.len() - 10);
    if trailer[..2] == DUMP_VERSION && crc64_digest(data).to_le_bytes() == trailer[2..] {
        return restore_kividb_payload(data).ok_or(RestoreError::BadData);
    }
    let (versioned, crc) = payload.split_at(payload.len() - 8);
    let version = u16::from_le_bytes([trailer[0], trailer[1]]);
    // A zero checksum is none, as Redis writes with checksums off.
    let sealed = crc == [0u8; 8] || redis_crc64(versioned).to_le_bytes() == crc;
    if version <= REDIS_DUMP_VERSION_MAX && sealed {
        return restore_redis_payload(data).ok_or(RestoreError::BadData);
    }
    Err(RestoreError::VersionOrChecksum)
}

/// Redis's CRC64, which seals the `DUMP` payloads Redis writes (`crc64.c`:
/// the Jones polynomial, reflected, from 0, with no final XOR). Its check
/// value, the sum of `123456789`, is `0xe9c6d914c4b8d9ca`.
pub fn redis_crc64(data: &[u8]) -> u64 {
    // The Jones polynomial 0xad93d23594c935a9, bit-reversed for the
    // least-significant-bit-first loop.
    const POLY: u64 = 0x95ac_9329_ac4b_c9b5;
    let mut crc: u64 = 0;
    for &byte in data {
        crc ^= u64::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
        }
    }
    crc
}

/// A value as Redis's `DUMP` serialises it: its RDB type, then the value in
/// the RDB encoding of that type, which the `dump.rdb` loader reads. Every
/// byte must belong to the value.
fn restore_redis_payload(data: &[u8]) -> Option<KiviDBValue> {
    let (&type_id, mut rest) = data.split_first()?;
    let value = rdb_read_value(&mut rest, type_id).ok()??;
    rest.is_empty().then_some(value)
}

/// Library sources from a Redis `FUNCTION DUMP` payload.
///
/// Redis writes one `RDB_OPCODE_FUNCTION2` byte and one RDB string per
/// library, then the same version-and-CRC trailer as a `DUMP` payload.
#[cfg(feature = "lua")]
pub(crate) fn redis_function_dump_sources(payload: &[u8]) -> Option<Vec<String>> {
    const RDB_OPCODE_FUNCTION2: u8 = 0xf5;

    if payload.len() < 10 {
        return None;
    }
    let (data, trailer) = payload.split_at(payload.len() - 10);
    let (versioned, crc) = payload.split_at(payload.len() - 8);
    let version = u16::from_le_bytes([trailer[0], trailer[1]]);
    if version > REDIS_DUMP_VERSION_MAX || redis_crc64(versioned).to_le_bytes() != crc {
        return None;
    }

    let mut rest = data;
    let mut sources = Vec::new();
    while let Some((&opcode, after_opcode)) = rest.split_first() {
        if opcode != RDB_OPCODE_FUNCTION2 {
            return None;
        }
        rest = after_opcode;
        sources.push(String::from_utf8(rdb_read_string(&mut rest).ok()?).ok()?);
    }
    (!sources.is_empty()).then_some(sources)
}

/// A value as KiviDB's own `DUMP` serialises it.
fn restore_kividb_payload(data: &[u8]) -> Option<KiviDBValue> {
    if data.starts_with(KDB_MAGIC) {
        load_snapshot_from(data)
            .ok()?
            .dbs
            .into_iter()
            .next()?
            .into_values()
            .next()
    } else {
        deserialize_value(data)
    }
}

/// Serialize a single KiviDBValue to bytes (without file header or key prefix).
/// The format matches what write_snapshot writes per-value, minus the key bytes.
pub fn serialize_value(value: &KiviDBValue, out: &mut Vec<u8>) -> std::io::Result<()> {
    use std::io::Write;
    match value {
        KiviDBValue::String(bytes) => {
            out.write_all(&[TAG_STRING])?;
            write_bytes_vec(out, bytes)?;
        }
        KiviDBValue::List(arc) => {
            out.write_all(&[TAG_LIST])?;
            let inner = arc.read();
            out.write_all(&(inner.len() as u32).to_le_bytes())?;
            for item in inner.iter() {
                write_bytes_vec(out, item)?;
            }
        }
        KiviDBValue::Hash(arc) => {
            out.write_all(&[TAG_HASH])?;
            let inner = arc.read();
            out.write_all(&(inner.len() as u32).to_le_bytes())?;
            for (field, val) in inner.iter() {
                write_bytes_vec(out, field.as_bytes())?;
                write_bytes_vec(out, val)?;
            }
        }
        KiviDBValue::Set(arc) => {
            out.write_all(&[TAG_SET])?;
            let inner = arc.read();
            out.write_all(&(inner.len() as u32).to_le_bytes())?;
            for member in inner.iter() {
                write_bytes_vec(out, member)?;
            }
        }
        KiviDBValue::SortedSet(arc) => {
            out.write_all(&[TAG_SORTEDSET])?;
            let inner = arc.read();
            out.write_all(&(inner.members.len() as u32).to_le_bytes())?;
            for (score, member) in inner.by_score.keys() {
                write_bytes_vec(out, member)?;
                out.write_all(&score.0.to_bits().to_le_bytes())?;
            }
        }
        KiviDBValue::Stream(arc) => {
            write_stream_value(out, &arc.read())?;
        }
        KiviDBValue::Json(arc) => {
            out.write_all(&[TAG_JSON])?;
            let s = arc.read().to_string();
            write_bytes_vec(out, s.as_bytes())?;
        }
        KiviDBValue::TimeSeries(_) | KiviDBValue::VectorSet(_) => {
            // TimeSeries and VectorSet are not portable across instances —
            // return an error so DUMP clearly signals this.
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "DUMP not supported for TimeSeries and VectorSet keys",
            ));
        }
        KiviDBValue::Array(arc) => {
            // Unlike TimeSeries/VectorSet above, an array has no instance-local
            // state (no index handles, no label registry) — it's a plain sparse
            // map plus two scalars, so it round-trips through DUMP/RESTORE.
            out.write_all(&[TAG_ARRAY])?;
            let inner = arc.read();
            out.write_all(&(inner.entries.len() as u32).to_le_bytes())?;
            for (idx, val) in inner.entries.iter() {
                out.write_all(&idx.to_le_bytes())?;
                write_bytes_vec(out, val)?;
            }
            out.write_all(&inner.next_insert.to_le_bytes())?;
            out.write_all(&inner.ring_size.unwrap_or(0).to_le_bytes())?;
        }
        KiviDBValue::HashEx(arc) => {
            out.write_all(&[TAG_HASH_EX])?;
            let inner = arc.read();
            out.write_all(&(inner.fields.len() as u32).to_le_bytes())?;
            for (field, val) in inner.fields.iter() {
                write_bytes_vec(out, field.as_bytes())?;
                write_bytes_vec(out, val)?;
            }
            out.write_all(&(inner.expiries.len() as u32).to_le_bytes())?;
            let now_sys = std::time::SystemTime::now();
            let now_mono = std::time::Instant::now();
            let unix_now_ms = now_sys
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            for (field, exp) in inner.expiries.iter() {
                let delta_ms = exp.saturating_duration_since(now_mono).as_millis() as u64;
                let abs_ms = unix_now_ms.saturating_add(delta_ms);
                write_bytes_vec(out, field.as_bytes())?;
                out.write_all(&abs_ms.to_le_bytes())?;
            }
        }
    }
    Ok(())
}

/// Helper: write length-prefixed bytes into a Vec<u8>.
fn write_bytes_vec(out: &mut Vec<u8>, data: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    out.write_all(&(data.len() as u32).to_le_bytes())?;
    out.write_all(data)
}

/// Deserialize a single value from bytes (the inverse of serialize_value).
/// Returns None if the data is malformed.
pub fn deserialize_value(data: &[u8]) -> Option<KiviDBValue> {
    if data.is_empty() {
        return None;
    }
    let tag = data[0];
    let payload = &data[1..];

    match tag {
        TAG_STRING => {
            let bytes = read_bytes_slice(payload, 0)?.0;
            Some(KiviDBValue::String(SharedBytes::from(bytes)))
        }
        TAG_LIST => {
            if payload.len() < 4 {
                return None;
            }
            let count = u32::from_le_bytes(payload[..4].try_into().ok()?) as usize;
            let mut pos = 4usize;
            let mut deque = VecDeque::new();
            for _ in 0..count {
                let (bytes, next) = read_bytes_slice(payload, pos)?;
                deque.push_back(Arc::from(bytes));
                pos = next;
            }
            Some(KiviDBValue::List(Arc::new(parking_lot::RwLock::new(deque))))
        }
        TAG_ARRAY => {
            if payload.len() < 4 {
                return None;
            }
            let count = u32::from_le_bytes(payload[..4].try_into().ok()?) as usize;
            let mut pos = 4usize;
            let mut arr = crate::types::ArrayInner::new();
            for _ in 0..count {
                let idx = u64::from_le_bytes(payload.get(pos..pos + 8)?.try_into().ok()?);
                pos += 8;
                let (bytes, next) = read_bytes_slice(payload, pos)?;
                arr.set(idx, Arc::from(bytes));
                pos = next;
            }
            arr.next_insert = u64::from_le_bytes(payload.get(pos..pos + 8)?.try_into().ok()?);
            pos += 8;
            let ring = u64::from_le_bytes(payload.get(pos..pos + 8)?.try_into().ok()?);
            arr.ring_size = if ring == 0 { None } else { Some(ring) };
            Some(KiviDBValue::Array(Arc::new(parking_lot::RwLock::new(arr))))
        }
        TAG_HASH => {
            if payload.len() < 4 {
                return None;
            }
            let count = u32::from_le_bytes(payload[..4].try_into().ok()?) as usize;
            let mut pos = 4usize;
            let mut map = crate::types::HashFields::default();
            for _ in 0..count {
                let (field_bytes, next) = read_bytes_slice(payload, pos)?;
                let field = String::from_utf8(field_bytes.to_vec()).ok()?;
                let (val_bytes, next2) = read_bytes_slice(payload, next)?;
                map.insert(field, Arc::from(val_bytes));
                pos = next2;
            }
            Some(KiviDBValue::Hash(Arc::new(parking_lot::RwLock::new(map))))
        }
        TAG_SET => {
            if payload.len() < 4 {
                return None;
            }
            let count = u32::from_le_bytes(payload[..4].try_into().ok()?) as usize;
            let mut pos = 4usize;
            let mut set = crate::member_set::MemberSet::new();
            for _ in 0..count {
                let (bytes, next) = read_bytes_slice(payload, pos)?;
                set.insert(Arc::from(bytes));
                pos = next;
            }
            Some(KiviDBValue::Set(Arc::new(parking_lot::RwLock::new(set))))
        }
        TAG_SORTEDSET => {
            if payload.len() < 4 {
                return None;
            }
            let count = u32::from_le_bytes(payload[..4].try_into().ok()?) as usize;
            let mut pos = 4usize;
            let mut ss = crate::types::SortedSetInner::new();
            for _ in 0..count {
                let (member_bytes, next) = read_bytes_slice(payload, pos)?;
                if next + 8 > payload.len() {
                    return None;
                }
                let score =
                    f64::from_bits(u64::from_le_bytes(payload[next..next + 8].try_into().ok()?));
                ss.insert(Arc::from(member_bytes), score);
                pos = next + 8;
            }
            Some(KiviDBValue::SortedSet(Arc::new(parking_lot::RwLock::new(
                ss,
            ))))
        }
        TAG_JSON => {
            let (bytes, _) = read_bytes_slice(payload, 0)?;
            let s = std::str::from_utf8(bytes).ok()?;
            let v: serde_json::Value = serde_json::from_str(s).ok()?;
            Some(KiviDBValue::Json(Arc::new(parking_lot::RwLock::new(v))))
        }
        TAG_STREAM | TAG_STREAM_V2 | TAG_STREAM_V3 => {
            let mut cur = std::io::Cursor::new(payload);
            read_stream_value(&mut cur, tag).ok()
        }
        TAG_HASH_EX => {
            if payload.len() < 4 {
                return None;
            }
            let count = u32::from_le_bytes(payload[..4].try_into().ok()?) as usize;
            let mut pos = 4usize;
            let mut map = crate::types::HashFields::default();
            for _ in 0..count {
                let (field_bytes, next) = read_bytes_slice(payload, pos)?;
                let field = String::from_utf8(field_bytes.to_vec()).ok()?;
                let (val_bytes, next2) = read_bytes_slice(payload, next)?;
                map.insert(field, Arc::from(val_bytes));
                pos = next2;
            }
            // Expiries
            if pos + 4 > payload.len() {
                return None;
            }
            let exp_count = u32::from_le_bytes(payload[pos..pos + 4].try_into().ok()?) as usize;
            pos += 4;
            let mut exps = AHashMap::new();
            let now_sys = std::time::SystemTime::now();
            let now_mono = std::time::Instant::now();
            let unix_now_ms = now_sys
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            for _ in 0..exp_count {
                let (field_bytes, next) = read_bytes_slice(payload, pos)?;
                let field = String::from_utf8(field_bytes.to_vec()).ok()?;
                if next + 8 > payload.len() {
                    return None;
                }
                let abs_ms = u64::from_le_bytes(payload[next..next + 8].try_into().ok()?);
                pos = next + 8;
                if abs_ms > unix_now_ms {
                    let delta = std::time::Duration::from_millis(abs_ms - unix_now_ms);
                    exps.insert(field, now_mono + delta);
                }
            }
            let inner = crate::types::HashInner {
                fields: map,
                expiries: exps,
            };
            Some(KiviDBValue::HashEx(Arc::new(parking_lot::RwLock::new(
                inner,
            ))))
        }
        _ => None,
    }
}

/// Read a length-prefixed byte slice from `data` at `pos`.
/// Returns (bytes, next_pos) or None if out of bounds.
fn read_bytes_slice(data: &[u8], pos: usize) -> Option<(&[u8], usize)> {
    if pos + 4 > data.len() {
        return None;
    }
    let len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
    let start = pos + 4;
    let end = start + len;
    if end > data.len() {
        return None;
    }
    Some((&data[start..end], end))
}

/// The checksum that seals KiviDB's own `DUMP` and `FUNCTION DUMP` payloads.
///
/// It is not Redis's CRC64, though it was meant to be: it runs the reflected
/// (least-significant-bit-first) loop with the Jones polynomial written the
/// other way round, so its sums never matched Redis's. It stays as it is,
/// since payloads written by earlier releases carry it. Redis's own payloads
/// are checked with [`redis_crc64`].
pub fn crc64_digest(data: &[u8]) -> u64 {
    // Polynomial: 0xad93d23594c935a9 (ECMA-182)
    const POLY: u64 = 0xad93d23594c935a9;
    let mut crc: u64 = 0;
    for &byte in data {
        let mut b = byte as u64;
        for _ in 0..8 {
            if (crc ^ b) & 1 != 0 {
                crc = (crc >> 1) ^ POLY;
            } else {
                crc >>= 1;
            }
            b >>= 1;
        }
    }
    crc
}

#[cfg(test)]
mod hnsw_graph_snapshot_tests {
    use super::{read_hnsw_graphs, write_hnsw_graph};
    use crate::vector::{DistanceMetric, HnswIndex, hnsw_insert, knn_hnsw};
    use std::io::Cursor;
    use std::sync::Arc;
    use std::sync::atomic::Ordering::Relaxed;

    #[test]
    fn kivihn1_section_round_trips_and_knn_matches_original() {
        let idx = Arc::new(HnswIndex::new(8, 64));
        let metric = DistanceMetric::L2;
        for i in 0..25u32 {
            let v = vec![i as f32, (i * 7) as f32, (i % 5) as f32];
            hnsw_insert(&idx, format!("doc:{i}"), v, &metric);
        }

        // The section body: the image loader consumes the `KIVIHN1` magic
        // itself while scanning for sections, then hands over to the reader.
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&(1u32).to_le_bytes());
        write_hnsw_graph(&mut buf, "idx1", "vec", idx.as_ref()).unwrap();

        let mut cur = Cursor::new(buf);
        let graphs = read_hnsw_graphs(&mut cur).unwrap();
        let loaded = graphs.get("idx1").unwrap().get("vec").unwrap();
        assert_eq!(loaded.live_count.load(Relaxed), 25);

        let q = [12.0_f32, 84.0_f32, 3.0_f32];
        let before = knn_hnsw(idx.as_ref(), &q, 3, 64, &metric);
        let after = knn_hnsw(loaded.as_ref(), &q, 3, 64, &metric);
        assert_eq!(before.len(), after.len());
        for (a, b) in before.iter().zip(after.iter()) {
            assert_eq!(
                a.key, b.key,
                "KNN keys differ after HNSW snapshot round-trip"
            );
        }
    }

    /// An image that ends right after its databases — every snapshot written
    /// before HNSW graphs were persisted — loads with no graphs and no error.
    #[test]
    fn image_without_trailing_sections_yields_no_graphs() {
        let mut image: Vec<u8> = Vec::new();
        super::write_kdb_header(&mut image, 1).unwrap();
        super::write_kdb_db_end(&mut image).unwrap();
        let loaded = super::load_snapshot_from(Cursor::new(image)).unwrap();
        assert!(loaded.hnsw_graphs.is_empty());
        assert!(loaded.index_schemas.is_empty());
    }

    /// A section this build does not know ends the scan instead of failing
    /// the load, so an image from a newer build still opens.
    #[test]
    fn unknown_trailing_section_is_ignored() {
        let mut image: Vec<u8> = Vec::new();
        super::write_kdb_header(&mut image, 1).unwrap();
        super::write_kdb_db_end(&mut image).unwrap();
        image.extend_from_slice(b"KIVIZZ9\0");
        image.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let loaded = super::load_snapshot_from(Cursor::new(image)).unwrap();
        assert_eq!(loaded.dbs.len(), 1);
    }
}

#[cfg(test)]
mod stream_idmp_persistence_tests {
    use super::*;

    fn round_trip(inner: &StreamInner) -> (u8, StreamInner) {
        let mut buf = Vec::new();
        write_stream_value(&mut buf, inner).unwrap();
        let tag = buf[0];
        let mut cur = std::io::Cursor::new(&buf[1..]);
        match read_stream_value(&mut cur, tag).unwrap() {
            KiviDBValue::Stream(s) => (tag, s.read().clone()),
            _ => panic!("expected a stream"),
        }
    }

    #[test]
    fn a_stream_that_never_used_idempotency_keeps_the_older_encoding() {
        let (tag, back) = round_trip(&StreamInner::new());
        assert_eq!(tag, TAG_STREAM_V2);
        assert!(back.idmp.is_unused());
    }

    #[test]
    fn idempotency_state_survives_a_dump_and_load() {
        let mut inner = StreamInner::new();
        inner.idmp.duration_secs = Some(300);
        inner.idmp.iids_added = 2;
        inner.idmp.iids_duplicates = 1;
        let mut state = crate::types::ProducerIdmp::default();
        for (iid, ms) in [("a", 1u64), ("b", 2)] {
            let iid: Arc<[u8]> = Arc::from(iid.as_bytes());
            state.by_iid.insert(Arc::clone(&iid), StreamId::new(ms, 0));
            state.order.push_back(crate::types::IdmpRecord {
                iid,
                entry_id: StreamId::new(ms, 0),
                added_ms: 1000 + ms,
            });
        }
        inner.idmp.producers.insert(Arc::from(&b"p1"[..]), state);

        let (tag, back) = round_trip(&inner);
        assert_eq!(tag, TAG_STREAM_V3);
        assert_eq!(back.idmp.duration_secs, Some(300));
        assert_eq!(back.idmp.maxsize, None);
        assert_eq!((back.idmp.iids_added, back.idmp.iids_duplicates), (2, 1));
        let p = &back.idmp.producers[&b"p1"[..]];
        assert_eq!(p.order.len(), 2);
        assert_eq!(p.order[0].added_ms, 1001);
        assert_eq!(p.by_iid[&b"b"[..]], StreamId::new(2, 0));
    }
}

#[cfg(test)]
mod dump_payload_tests {
    use super::*;

    #[test]
    fn every_type_round_trips_including_the_ones_dump_used_to_refuse() {
        let string = KiviDBValue::String(SharedBytes::from(&b"hello"[..]));
        let mut ts = TsInner::new();
        ts.samples.insert(1000, 21.5);
        ts.labels.insert("room".into(), "kitchen".into());
        let series = KiviDBValue::TimeSeries(Arc::new(parking_lot::RwLock::new(ts)));
        let mut vs = VectorSetInner::new(2);
        vs.entries.insert(
            "a".into(),
            VSetEntry {
                vector: vec![1.0, 2.0],
                attrs: None,
            },
        );
        let vset = KiviDBValue::VectorSet(Arc::new(parking_lot::RwLock::new(vs)));

        for value in [string, series, vset] {
            let payload = dump_payload(&value).unwrap();
            let back = restore_payload(&payload).expect("payload restores");
            assert_eq!(back.type_name(), value.type_name());
        }

        let payload = dump_payload(&KiviDBValue::TimeSeries(Arc::new(
            parking_lot::RwLock::new({
                let mut ts = TsInner::new();
                ts.samples.insert(7, 1.5);
                ts.labels.insert("k".into(), "v".into());
                ts
            }),
        )))
        .unwrap();
        match restore_payload(&payload).unwrap() {
            KiviDBValue::TimeSeries(arc) => {
                let ts = arc.read();
                assert_eq!(ts.samples.get(&7), Some(&1.5));
                assert_eq!(ts.labels.get("k").map(String::as_str), Some("v"));
            }
            _ => panic!("expected a time series"),
        }
    }

    #[test]
    fn a_corrupted_payload_is_rejected() {
        let mut payload =
            dump_payload(&KiviDBValue::String(SharedBytes::from(&b"hello"[..]))).unwrap();
        assert!(restore_payload(&payload).is_some());
        payload[1] ^= 0xff;
        assert!(
            restore_payload(&payload).is_none(),
            "checksum must catch a flipped byte"
        );
        assert!(restore_payload(b"short").is_none());
        assert!(restore_payload(&[]).is_none());
    }
}

#[cfg(test)]
mod expiry_section_tests {
    use super::*;

    fn string(v: &str) -> KiviDBValue {
        KiviDBValue::String(SharedBytes::from(v.as_bytes()))
    }

    fn image(maps: &[HashMap<String, KiviDBValue>], expiries: &[HashMap<String, u64>]) -> Vec<u8> {
        let mut out = Vec::new();
        write_kdb_header(&mut out, maps.len() as u32).unwrap();
        for map in maps {
            for (k, v) in map {
                write_kdb_entry(&mut out, k, v.view()).unwrap();
            }
            write_kdb_db_end(&mut out).unwrap();
        }
        write_kdb_expiry_maps(&mut out, expiries).unwrap();
        out
    }

    #[test]
    fn expiries_round_trip_per_database() {
        let far = unix_ms_now() + 3_600_000;
        let db0 = HashMap::from([
            ("a".to_string(), string("1")),
            ("b".to_string(), string("2")),
        ]);
        let db1 = HashMap::from([("c".to_string(), string("3"))]);
        let exp = vec![
            HashMap::from([("a".to_string(), far)]),
            HashMap::from([("c".to_string(), far + 1)]),
        ];
        let loaded = load_snapshot_from(&image(&[db0, db1], &exp)[..]).unwrap();
        assert_eq!(loaded.expiries[0].get("a"), Some(&far));
        assert_eq!(loaded.expiries[0].get("b"), None);
        assert_eq!(loaded.expiries[1].get("c"), Some(&(far + 1)));
    }

    /// The whole point of persisting an expiry is that it is honoured: a key
    /// whose time passed while the image sat on disk must not come back.
    #[test]
    fn expired_keys_are_dropped_at_load() {
        let db0 = HashMap::from([
            ("live".to_string(), string("1")),
            ("dead".to_string(), string("2")),
        ]);
        let exp = vec![HashMap::from([
            ("live".to_string(), unix_ms_now() + 60_000),
            ("dead".to_string(), unix_ms_now().saturating_sub(1)),
        ])];
        let loaded = load_snapshot_from(&image(&[db0], &exp)[..]).unwrap();
        assert!(loaded.dbs[0].contains_key("live"));
        assert!(!loaded.dbs[0].contains_key("dead"));
        assert!(!loaded.expiries[0].contains_key("dead"));
    }

    /// An expiry for a key that is not in the image is dropped rather than
    /// installed, where it would attach to whatever later takes that name.
    #[test]
    fn orphan_expiries_are_not_installed() {
        let db0 = HashMap::from([("a".to_string(), string("1"))]);
        let exp = vec![HashMap::from([(
            "ghost".to_string(),
            unix_ms_now() + 60_000,
        )])];
        let loaded = load_snapshot_from(&image(&[db0], &exp)[..]).unwrap();
        assert!(loaded.expiries[0].is_empty());
    }

    #[test]
    fn an_expired_hash_drops_its_key_expiry() {
        let mut hash = crate::types::HashInner::new();
        hash.fields
            .insert("field".to_string(), Arc::from(&b"value"[..]));
        hash.expiries.insert(
            "field".to_string(),
            std::time::Instant::now() - std::time::Duration::from_secs(1),
        );
        let db0 = HashMap::from([(
            "hxgone".to_string(),
            KiviDBValue::HashEx(Arc::new(parking_lot::RwLock::new(hash))),
        )]);
        let exp = vec![HashMap::from([(
            "hxgone".to_string(),
            unix_ms_now() + 60_000,
        )])];

        let loaded = load_snapshot_from(&image(&[db0], &exp)[..]).unwrap();
        assert!(!loaded.dbs[0].contains_key("hxgone"));
        assert!(!loaded.expiries[0].contains_key("hxgone"));
    }

    /// An image with no expiry section — everything written before this
    /// section existed — still loads, with every key permanent as before.
    #[test]
    fn image_without_an_expiry_section_loads() {
        let db0 = HashMap::from([("a".to_string(), string("1"))]);
        let loaded = load_snapshot_from(&image(&[db0], &[])[..]).unwrap();
        assert_eq!(loaded.dbs[0].len(), 1);
        assert_eq!(loaded.expiries.len(), 1);
        assert!(loaded.expiries[0].is_empty());
    }

    /// Unlike the index sections, a torn expiry section fails the load: half
    /// the TTLs silently missing is worse than a loud error.
    #[test]
    fn a_truncated_expiry_section_fails_the_load() {
        let db0 = HashMap::from([("a".to_string(), string("1"))]);
        let exp = vec![HashMap::from([("a".to_string(), unix_ms_now() + 60_000)])];
        let full = image(&[db0], &exp);
        assert!(load_snapshot_from(&full[..full.len() - 3]).is_err());
    }

    /// `write_snapshot` replaces the file atomically: a save that
    /// fails must leave the previous snapshot intact, not a truncated file.
    #[test]
    fn snapshot_file_is_replaced_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dump.kdb");
        let path_s = path.to_str().unwrap();
        let first = vec![HashMap::from([("a".to_string(), string("1"))])];
        write_snapshot(path_s, &first, None).unwrap();
        let second = vec![HashMap::from([
            ("a".to_string(), string("1")),
            ("b".to_string(), string("2")),
        ])];
        write_snapshot(path_s, &second, None).unwrap();

        assert_eq!(load_snapshot(path_s).unwrap().dbs[0].len(), 2);
        // No temp files left behind.
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }
}

#[cfg(test)]
mod field_option_tests {
    use super::*;
    use crate::vector::{FieldOptions, FieldType, IndexOn, IndexSchema, SchemaField};

    /// Field options and stopwords come back with the index.
    #[test]
    fn field_options_survive_a_snapshot() {
        let field = |name: &str, field_type: FieldType, words: &[&str]| {
            let mut options = FieldOptions::default();
            let words: Vec<&[u8]> = words.iter().map(|w| w.as_bytes()).collect();
            let mut at = 0;
            options.consume(name, &field_type, &words, &mut at).unwrap();
            assert_eq!(at, words.len());
            SchemaField {
                name: name.into(),
                field_type,
                options,
            }
        };
        let mut aliased = field("$.price", FieldType::Numeric, &["SORTABLE", "UNF"]);
        aliased.options.alias = Some("price".into());
        let def = IndexSchema {
            name: "idx".into(),
            db_index: 0,
            on: IndexOn::Hash,
            prefixes: vec!["p:".into()],
            fields: vec![
                field(
                    "title",
                    FieldType::Text,
                    &["WEIGHT", "2.5", "NOSTEM", "SORTABLE"],
                ),
                field("tags", FieldType::Tag, &["SEPARATOR", ";", "CASESENSITIVE"]),
                aliased,
                field("plain", FieldType::Text, &[]),
            ],
            aliases: Vec::new(),
            stopwords: Some(vec!["foo".into(), "bar".into()]),
            synonyms: Default::default(),
        }
        .into_def();
        let expected: Vec<(String, FieldOptions)> = def
            .fields
            .iter()
            .map(|f| (f.name.clone(), f.options.clone()))
            .collect();
        let store = crate::vector::new_index_store();
        store.write().insert("idx".into(), def);
        // An index with nothing to say takes no room in the section.
        let bare = IndexSchema {
            name: "bare".into(),
            db_index: 0,
            on: IndexOn::Hash,
            prefixes: vec![],
            fields: vec![field("t", FieldType::Text, &[])],
            aliases: Vec::new(),
            stopwords: None,
            synonyms: Default::default(),
        }
        .into_def();
        store.write().insert("bare".into(), bare);

        let mut image = Vec::new();
        write_kdb_image(&mut image, &[HashMap::new()], Some(&store)).unwrap();
        let loaded = load_snapshot_from(image.as_slice()).unwrap();

        let idx = loaded
            .index_schemas
            .iter()
            .find(|s| s.name == "idx")
            .unwrap();
        let got: Vec<(String, FieldOptions)> = idx
            .fields
            .iter()
            .map(|f| (f.name.clone(), f.options.clone()))
            .collect();
        assert_eq!(got, expected);
        assert_eq!(
            idx.stopwords,
            Some(vec!["foo".to_string(), "bar".to_string()])
        );
        let bare = loaded
            .index_schemas
            .iter()
            .find(|s| s.name == "bare")
            .unwrap();
        assert_eq!(bare.fields[0].options, FieldOptions::default());
        assert_eq!(bare.stopwords, None);
    }
}

#[cfg(test)]
mod verify_tests {
    use super::*;

    fn image(keys: usize) -> Vec<u8> {
        let mut map: HashMap<String, KiviDBValue> = HashMap::new();
        for i in 0..keys {
            map.insert(
                format!("key:{i}"),
                KiviDBValue::String(SharedBytes::from(format!("value {i}").as_bytes())),
            );
        }
        let mut out = Vec::new();
        write_kdb_image(&mut out, &[map, HashMap::new()], None).unwrap();
        out
    }

    /// A replica must
    /// know a payload is whole before it lets go of what it has.
    #[test]
    fn a_whole_image_verifies_and_a_damaged_one_does_not() {
        let whole = image(500);
        assert_eq!(verify_snapshot(whole.as_slice()).unwrap(), 500);
        assert_eq!(verify_snapshot(image(0).as_slice()).unwrap(), 0);

        // Cut anywhere — in a key, in a value, between two entries, between
        // two databases — it is refused.
        for cut in [
            9,
            12,
            13,
            40,
            200,
            whole.len() / 2,
            whole.len() - 5,
            whole.len() - 1,
        ] {
            assert!(
                verify_snapshot(&whole[..cut]).is_err(),
                "an image cut at {cut} of {} verified",
                whole.len()
            );
        }
        // Not an image at all, and one with a value of no known type.
        assert!(verify_snapshot(&b"not a snapshot at all"[..]).is_err());
        let mut damaged = whole.clone();
        let tag_at = 8 + 4 + 4 + "key:".len() + 1;
        let position = damaged
            .iter()
            .enumerate()
            .skip(tag_at)
            .find(|(_, b)| **b == TAG_STRING)
            .map(|(i, _)| i)
            .unwrap();
        damaged[position] = 0xEE;
        assert!(verify_snapshot(damaged.as_slice()).is_err());

        // Verifying keeps nothing, and loading still loads.
        let loaded = load_snapshot_from(whole.as_slice()).unwrap();
        assert_eq!(loaded.dbs[0].len(), 500);
    }
}

#[cfg(test)]
mod function_section_tests {
    use super::*;

    fn image(libraries: Option<&[u8]>) -> Vec<u8> {
        let mut out = Vec::new();
        write_kdb_header(&mut out, 1).unwrap();
        write_kdb_entry(
            &mut out,
            "k",
            KiviDBValue::String(SharedBytes::from(&b"v"[..])).view(),
        )
        .unwrap();
        write_kdb_db_end(&mut out).unwrap();
        write_kdb_expiry_maps(&mut out, &[HashMap::new()]).unwrap();
        write_kdb_functions_section(&mut out, libraries).unwrap();
        out
    }

    /// The section brings back the libraries' payload as it was written, and
    /// an image without it, as every image before it was, has none (#70).
    #[test]
    fn the_function_section_round_trips_and_is_optional() {
        let loaded = load_snapshot_from(&image(Some(b"KVFN payload"))[..]).unwrap();
        assert_eq!(loaded.functions.as_deref(), Some(&b"KVFN payload"[..]));
        assert_eq!(loaded.dbs[0].len(), 1);
        assert_eq!(
            load_snapshot_from(&image(None)[..]).unwrap().functions,
            None
        );
    }

    /// A torn section fails the load, rather than bring a node up without
    /// its libraries.
    #[test]
    fn a_torn_function_section_fails_the_load() {
        let full = image(Some(b"KVFN payload"));
        assert!(load_snapshot_from(&full[..full.len() - 3]).is_err());
        assert!(verify_snapshot(&full[..full.len() - 3]).is_err());
    }

    /// What a build without Lua leaves out is named in its warning: the
    /// libraries in the payload, read without Lua (#70).
    #[test]
    fn the_libraries_a_build_without_lua_leaves_out_are_named() {
        let blob = |p: &mut Vec<u8>, s: &str| {
            p.extend((s.len() as u32).to_le_bytes());
            p.extend(s.as_bytes());
        };
        let mut payload = b"KVFN".to_vec();
        payload.extend(1u32.to_le_bytes()); // format version
        payload.extend(2u32.to_le_bytes()); // libraries
        for (name, functions) in [("mylib", &["f", "g"][..]), ("other", &[][..])] {
            blob(&mut payload, name);
            blob(&mut payload, "LUA");
            blob(&mut payload, "#!lua name=...");
            payload.extend((functions.len() as u32).to_le_bytes());
            for function in functions {
                blob(&mut payload, function);
            }
        }
        payload.extend([0u8; 8]); // checksum
        assert_eq!(
            function_library_names(&payload),
            Some(vec!["mylib".to_string(), "other".to_string()])
        );
        assert_eq!(function_library_names(b"KVFN payload"), None);

        // Redis's own layout, as a `dump.rdb`'s libraries are handed on: the
        // names are on each library's first line.
        let redis = redis_function_dump(&[
            b"#!lua name=seven\nredis.register_function('seven', ...)".to_vec(),
            b"#!lua name=counter\n...".to_vec(),
        ])
        .unwrap();
        assert_eq!(
            function_library_names(&redis),
            Some(vec!["seven".to_string(), "counter".to_string()])
        );
    }

    /// Lengths as `rdbSaveLen` writes them, at each form's edges, read back
    /// as the reader reads them.
    #[test]
    fn rdb_lengths_round_trip_at_each_forms_edges() {
        for len in [0u64, 0x3F, 0x40, 0x3FFF, 0x4000, 0xFFFF_FFFF, 0x1_0000_0000] {
            // The 64-bit form's edge is past a 32-bit usize.
            let Ok(len) = usize::try_from(len) else {
                continue;
            };
            let mut out = Vec::new();
            rdb_write_length(&mut out, len);
            let (read, encoded) = rdb_read_length(&mut &out[..]).unwrap();
            assert_eq!((read, encoded), (len as u64, false), "{len:#x}");
        }
    }

    /// A build without Lua loads such an image, and leaves the libraries out.
    #[cfg(not(feature = "lua"))]
    #[test]
    fn a_build_without_lua_loads_an_image_with_libraries() {
        assert!(restore_functions(Some(b"KVFN payload".to_vec())).is_ok());
    }
}

#[cfg(test)]
mod redisjson_cbor_tests {
    use super::cbor_to_json;
    use serde_json::json;

    /// What Redis 8.10 saves for `{"a":1,"b":[1,2]}` and `[1.5,2.5]`: the
    /// arrays as typed arrays (RFC 8746), 8-bit integers and half floats.
    /// They made the whole snapshot unreadable (#157).
    #[test]
    fn typed_arrays_redis_writes_are_read() {
        let object = [
            0xa2, 0x61, 0x61, 0x01, 0x61, 0x62, 0xd8, 0x48, 0x42, 0x01, 0x02,
        ];
        assert_eq!(cbor_to_json(&object).unwrap(), json!({"a": 1, "b": [1, 2]}));
        let halves = [0xd8, 0x54, 0x44, 0x00, 0x3e, 0x00, 0x41];
        assert_eq!(cbor_to_json(&halves).unwrap(), json!([1.5, 2.5]));
    }

    #[test]
    fn every_width_and_order_of_typed_array() {
        // uint32, little-endian: tag 70.
        let u32le = [0xd8, 70, 0x48, 1, 0, 0, 0, 0x2c, 1, 0, 0];
        assert_eq!(cbor_to_json(&u32le).unwrap(), json!([1, 300]));
        // sint64, big-endian: tag 75.
        let mut s64be = vec![0xd8, 75, 0x48];
        s64be.extend_from_slice(&(-1i64).to_be_bytes());
        assert_eq!(cbor_to_json(&s64be).unwrap(), json!([-1]));
        // sint16, little-endian: tag 77.
        let s16le = [0xd8, 77, 0x42, 0xfe, 0xff];
        assert_eq!(cbor_to_json(&s16le).unwrap(), json!([-2]));
        // float32, little-endian: tag 85.
        let mut f32le = vec![0xd8, 85, 0x44];
        f32le.extend_from_slice(&0.5f32.to_le_bytes());
        assert_eq!(cbor_to_json(&f32le).unwrap(), json!([0.5]));
        // float64, big-endian: tag 82.
        let mut f64be = vec![0xd8, 82, 0x48];
        f64be.extend_from_slice(&(-2.25f64).to_be_bytes());
        assert_eq!(cbor_to_json(&f64be).unwrap(), json!([-2.25]));
        // Bytes that do not divide into the elements, and 128-bit floats.
        assert!(cbor_to_json(&[0xd8, 70, 0x43, 1, 2, 3]).is_err());
        assert!(cbor_to_json(&[0xd8, 83, 0x40]).is_ok_and(|v| v == json!([])));
        let mut f128 = vec![0xd8, 83, 0x50];
        f128.extend_from_slice(&[0; 16]);
        assert!(cbor_to_json(&f128).is_err());
    }
}
