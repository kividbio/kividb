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

mod rdb_dialect;
pub use rdb_dialect::is_rdb_header;
mod rdb_extensions;
pub use rdb_extensions::is_unrepresentable;

/// Returned by `load_snapshot` — key-value data plus any index schemas that
/// were embedded in the snapshot file.
pub struct LoadedSnapshot {
    pub dbs: Vec<HashMap<Key, KiviDBValue>>,
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
    pub expiries: Vec<HashMap<Key, u64>>,
    /// The function libraries, as a `FUNCTION DUMP` payload: `None` for an
    /// image that holds none, or one written before they were kept. Loaded
    /// with [`restore_functions`].
    pub functions: Option<Vec<u8>>,
}

/// Per-database key expiries as absolute Unix milliseconds.
pub type ExpiryMaps = Vec<HashMap<Key, u64>>;

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
/// The language of each index that is not English or that reads it from a
/// field (`LANGUAGE`, `LANGUAGE_FIELD`). After the function libraries: an
/// older release reads everything else, and stops here.
const KDB_LANGUAGE_MAGIC: &[u8; 8] = b"KIVILG1\0";
/// `SCORE` and `SCORE_FIELD` of each index that sets either otherwise than
/// the default. Last, for the same reason as the language.
const KDB_SCORE_MAGIC: &[u8; 8] = b"KIVISR1\0";

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

/// What a sorted set with a NaN score is refused with, in Redis's words.
const NAN_SCORE: &str = "Zset with NAN score detected";

/// A sorted-set score read from a snapshot, a `DUMP` payload or a Redis
/// RDB file, unless it is NaN. Redis refuses one there, and so does this:
/// no command makes one (`ZADD` and `ZINCRBY` refuse it), the order of the
/// set would put it above `+inf`, and the score ranges take there to be
/// none, as in `ZCOUNT -inf +inf` being `ZCARD` (#414). An infinite score
/// is a score.
fn zset_score(score: f64) -> Option<f64> {
    (!score.is_nan()).then_some(score)
}

/// [`zset_score`] for a reader that reports why.
fn zset_score_or_error(score: f64) -> io::Result<f64> {
    zset_score(score).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, NAN_SCORE))
}
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

/// Whether `byte` is a value's type tag: what follows the empty key as 1.2.0
/// wrote it ([`read_image`]).
fn is_tag(byte: u8) -> bool {
    (TAG_STRING..=TAG_TIMESERIES_V3).contains(&byte)
}

/// What every section after the databases opens with.
const SECTION_MAGIC_PREFIX: &[u8; 4] = b"KIVI";

/// Fill `buf`; `Ok(false)` if the image ends before its first byte and
/// nothing of the block it is part of was read before it (`started`). An
/// image that ends inside the block is torn.
fn fill_or_end(r: &mut impl Read, buf: &mut [u8], started: bool) -> io::Result<bool> {
    let mut at = 0;
    while at < buf.len() {
        match r.read(&mut buf[at..]) {
            Ok(0) if at == 0 && !started => return Ok(false),
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "the snapshot ends inside a section's magic: it is cut short",
                ));
            }
            Ok(n) => at += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

/// `result`, the reading of the `section` section's body, with an image that
/// ends inside it said to be cut short (#474).
///
/// Reaching the end of the image partway through a section is never a clean
/// end, on any reading: a section is either all there or the image is torn.
/// The error stays [`io::ErrorKind::UnexpectedEof`], a fault of the layout,
/// so that [`read_retrying`] still reads a 1.2.0 image again as it says.
fn whole_section<T>(section: &str, result: io::Result<T>) -> io::Result<T> {
    result.map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("the snapshot ends inside its {section} section: it is cut short"),
            )
        } else {
            e
        }
    })
}

/// [`whole_section`] for the index, HNSW graph and alias sections, with one
/// that is all there but cannot be used refused as well, saying which.
///
/// They were read best-effort, and one that could not be used ended the scan
/// with a warning. The images of 1.0 to 1.2.0 hold the key expiries and the
/// function libraries after them, so every TTL and library was dropped with
/// it, and the indexes were not rebuilt either. No writer makes such a
/// section, so refusing one refuses no image KiviDB wrote whole (#474).
fn usable_section<T>(section: &str, result: io::Result<T>) -> io::Result<T> {
    whole_section(section, result).map_err(|e| {
        if e.kind() == io::ErrorKind::InvalidData {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the snapshot's {section} section cannot be used: {e}"),
            )
        } else {
            e
        }
    })
}

/// A snapshot file that goes to the disk as it is written, a few megabytes
/// at a time, rather than all at once when it is synced.
///
/// Written into the page cache at memory speed and synced at the end, a
/// gigabyte of snapshot was a gigabyte queued at the disk at once. On a slow
/// disk that is seconds, and everything else waited behind it: the AOF's
/// once-a-second sync, and the file system's journal, which every append to
/// the AOF needs whenever it updates the file's size or time. A connection
/// appending its records waited out the snapshot in `write`, and its reply
/// with it, and so did every reply waiting for the AOF behind it — 2 to 3
/// seconds at a time on a 150 MB/s disk, more on a slower or busier one
/// (#243). Here each [`PACE_BYTES`] written is handed to the disk, and the
/// piece before it waited for: never more than two pieces queued, so the AOF
/// and the journal get their turns while a snapshot is written, and the
/// snapshot's pages leave the page cache once they are on the disk. A fast
/// disk writes a piece in a few milliseconds, and the snapshot takes about as
/// long as before.
pub(crate) struct PacedFile {
    file: File,
    /// Bytes written so far.
    written: u64,
    /// Up to here, handed to the disk.
    started: u64,
    /// Up to here, on the disk.
    done: u64,
}

/// How much of a snapshot is written before it is handed to the disk.
const PACE_BYTES: u64 = 8 << 20;

impl PacedFile {
    pub(crate) fn new(file: File) -> Self {
        PacedFile {
            file,
            written: 0,
            started: 0,
            done: 0,
        }
    }

    pub(crate) fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Hand what was written since the last call to the disk, and wait for
    /// what was handed to it before. Best effort: whatever this does not do,
    /// the final `sync_all` does, and reports.
    fn pace(&mut self) {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let fd = self.file.as_raw_fd();
            // SAFETY: plain system calls on a descriptor this owns; they
            // touch no memory of ours.
            unsafe {
                libc::sync_file_range(
                    fd,
                    self.started as libc::off64_t,
                    (self.written - self.started) as libc::off64_t,
                    libc::SYNC_FILE_RANGE_WRITE,
                );
                if self.started > self.done {
                    let len = (self.started - self.done) as libc::off64_t;
                    let waited = libc::sync_file_range(
                        fd,
                        self.done as libc::off64_t,
                        len,
                        libc::SYNC_FILE_RANGE_WAIT_BEFORE
                            | libc::SYNC_FILE_RANGE_WRITE
                            | libc::SYNC_FILE_RANGE_WAIT_AFTER,
                    );
                    if waited == 0 {
                        libc::posix_fadvise(
                            fd,
                            self.done as libc::off_t,
                            len as libc::off_t,
                            libc::POSIX_FADV_DONTNEED,
                        );
                    }
                }
            }
        }
        self.done = self.started;
        self.started = self.written;
    }
}

impl Write for PacedFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.file.write(buf)?;
        self.written += n as u64;
        if self.written - self.started >= PACE_BYTES {
            self.pace();
        }
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

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

/// What an entry's key length is written as when the key is empty (#441).
///
/// A key length of 0 ends a database's section ([`write_kdb_db_end`]), and
/// Redis allows the empty key: written with a length of 0, it ended its
/// database there, and the image could not be read past it. No key has this
/// length (keys this long cannot be written, see [`write_key`]), so it stands
/// for the empty key, with no bytes after it. A reader before 1.2.1 takes it
/// for the length of a key of 4 GiB, runs out of image before it has one,
/// and refuses the image.
const EMPTY_KEY_LEN: u32 = u32::MAX;

/// An entry's key: its length, then its bytes; the empty key as
/// [`EMPTY_KEY_LEN`] alone.
fn write_key(w: &mut impl Write, key: &[u8]) -> io::Result<()> {
    if key.is_empty() {
        return w.write_all(&EMPTY_KEY_LEN.to_le_bytes());
    }
    if key.len() >= EMPTY_KEY_LEN as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a key of 4 GiB or more cannot be written to a snapshot",
        ));
    }
    write_bytes(w, key)
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
        // Group and consumer names are any bytes, as in Redis.
        let name = crate::types::StreamName::from(read_bytes(r)?);
        let last_id = read_stream_id(r)?;
        let entries_read = read_u64_le(r)?;
        let pel_count = read_u32_le(r)? as usize;
        let mut pel = std::collections::BTreeMap::new();
        for _ in 0..pel_count {
            let id = read_stream_id(r)?;
            let consumer = crate::types::StreamName::from(read_bytes(r)?);
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
            let cname = crate::types::StreamName::from(read_bytes(r)?);
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
    key: &(impl AsRef<[u8]> + ?Sized),
    value: crate::store::ValueRef<'_>,
) -> io::Result<()> {
    write_key(w, key.as_ref())?;
    write_kdb_value(w, value)
}

/// One value as an entry lays it out after its key: a type tag, then the
/// value's own encoding.
pub fn write_kdb_value(w: &mut impl Write, value: crate::store::ValueRef<'_>) -> io::Result<()> {
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
            // Its links to other series, but in a `DUMP` (`dump_payload`).
            let links = !WITHOUT_LINKS.get();
            let rules: &[crate::types::CompactionRule] = if links { &inner.rules } else { &[] };
            let source = if links { inner.source.as_ref() } else { None };
            // The duplicate policy and the compaction rules were not saved at
            // all: after a restart (or on a replica, after a full sync) every
            // series was back on the default policy and no longer fed its
            // compactions — silently.
            let extended = !rules.is_empty()
                || inner.duplicate_policy != crate::types::DuplicatePolicy::Last
                || source.is_some();
            w.write_all(&[if source.is_some() {
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
                w.write_all(&(rules.len() as u32).to_le_bytes())?;
                for rule in rules.iter() {
                    write_bytes(w, &crate::key_bytes::bytes_of(&rule.dest_key))?;
                    write_bytes(w, rule.aggregation.name().as_bytes())?;
                    w.write_all(&rule.bucket_duration.to_le_bytes())?;
                    w.write_all(&rule.align_timestamp.to_le_bytes())?;
                }
            }
            if let Some(source) = source {
                write_bytes(w, &crate::key_bytes::bytes_of(source))?;
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
            let clock = crate::commands::helpers::FieldClock::now();
            for (field, exp) in inner.expiries.iter() {
                let abs_ms = clock.unix_ms_of(*exp);
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
pub fn write_kdb_expiry_maps<K: AsRef<[u8]>>(
    w: &mut impl Write,
    per_db: &[HashMap<K, u64>],
) -> io::Result<()> {
    if per_db.iter().all(|m| m.is_empty()) {
        return Ok(());
    }
    w.write_all(KDB_EXPIRY_MAGIC)?;
    w.write_all(&(per_db.len() as u32).to_le_bytes())?;
    for map in per_db {
        w.write_all(&(map.len() as u32).to_le_bytes())?;
        for (key, at_ms) in map {
            write_bytes(w, key.as_ref())?;
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
pub fn write_kdb_image<K: AsRef<[u8]>>(
    w: &mut impl Write,
    maps: &[HashMap<K, KiviDBValue>],
    index_store: Option<&crate::vector::IndexStore>,
) -> io::Result<()> {
    write_kdb_header(w, maps.len() as u32)?;
    for map in maps {
        for (k, v) in map {
            write_kdb_entry(w, k, v.view())?;
        }
        write_kdb_db_end(w)?;
    }
    // Functions before the best-effort index sections (see `write_to`).
    write_kdb_functions_section(w, function_libraries().as_deref())?;
    write_kdb_index_sections(w, index_store)?;
    write_kdb_alias_section(w, index_store)?;
    write_kdb_language_section(w, index_store)?;
    write_kdb_score_section(w, index_store)
}

/// The language section: `index`, its language, its language field (empty
/// for none), for each index with either set otherwise than the default.
/// Absent when there is none, so such an image is what it was before.
fn write_kdb_language_section(
    w: &mut impl Write,
    index_store: Option<&crate::vector::IndexStore>,
) -> io::Result<()> {
    let Some(store) = index_store else {
        return Ok(());
    };
    let guard = store.read();
    let mut set: Vec<&crate::vector::IndexDef> = guard
        .iter()
        .filter(|(key, def)| {
            !crate::vector::is_alias(key, def) && def.language != Default::default()
        })
        .map(|(_, def)| def)
        .collect();
    if set.is_empty() {
        return Ok(());
    }
    set.sort_by(|a, b| a.name.cmp(&b.name));
    w.write_all(KDB_LANGUAGE_MAGIC)?;
    w.write_all(&(set.len() as u32).to_le_bytes())?;
    for def in set {
        write_bytes(w, def.name.as_bytes())?;
        write_bytes(w, def.language.language.name().as_bytes())?;
        write_bytes(w, def.language.field.as_deref().unwrap_or("").as_bytes())?;
    }
    Ok(())
}

/// The score section: `index`, its default score, and its `SCORE_FIELD`
/// (empty for none), for each index that sets either otherwise than the
/// default. Absent when there is none.
fn write_kdb_score_section(
    w: &mut impl Write,
    index_store: Option<&crate::vector::IndexStore>,
) -> io::Result<()> {
    let Some(store) = index_store else {
        return Ok(());
    };
    let guard = store.read();
    let mut set: Vec<&crate::vector::IndexDef> = guard
        .iter()
        .filter(|(key, def)| {
            !crate::vector::is_alias(key, def)
                && (def.default_score != 1.0 || def.score_field.is_some())
        })
        .map(|(_, def)| def)
        .collect();
    if set.is_empty() {
        return Ok(());
    }
    set.sort_by(|a, b| a.name.cmp(&b.name));
    w.write_all(KDB_SCORE_MAGIC)?;
    w.write_all(&(set.len() as u32).to_le_bytes())?;
    for def in set {
        write_bytes(w, def.name.as_bytes())?;
        w.write_all(&def.default_score.to_bits().to_le_bytes())?;
        match &def.score_field {
            Some(field) => {
                w.write_all(&[1])?;
                write_bytes(w, field.as_bytes())?;
            }
            None => w.write_all(&[0])?,
        }
    }
    Ok(())
}

/// Body of the score section; the magic has already been consumed.
fn read_score_section(r: &mut impl Read, schemas: &mut [IndexSchema]) -> io::Result<()> {
    let text = |bytes: Vec<u8>| {
        String::from_utf8(bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad index score utf8"))
    };
    for _ in 0..read_u32_le(r)? {
        let index = text(read_bytes(r)?)?;
        let mut score = [0u8; 8];
        r.read_exact(&mut score)?;
        let mut present = [0u8; 1];
        r.read_exact(&mut present)?;
        let field = match present[0] {
            0 => None,
            1 => Some(text(read_bytes(r)?)?),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bad index score field flag",
                ));
            }
        };
        if let Some(schema) = schemas.iter_mut().find(|s| s.name == index) {
            schema.default_score = f64::from_bits(u64::from_le_bytes(score));
            schema.score_field = field;
        }
    }
    Ok(())
}

/// Body of the language section; the magic has already been consumed. Not
/// best-effort: an index in another language indexes other words.
fn read_language_section(r: &mut impl Read, schemas: &mut [IndexSchema]) -> io::Result<()> {
    let text = |bytes: Vec<u8>| {
        String::from_utf8(bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad index language utf8"))
    };
    for _ in 0..read_u32_le(r)? {
        let index = text(read_bytes(r)?)?;
        let name = text(read_bytes(r)?)?;
        let field = text(read_bytes(r)?)?;
        let language = crate::vector::stem::Language::parse(&name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown language `{name}` for index `{index}`"),
            )
        })?;
        if let Some(schema) = schemas.iter_mut().find(|s| s.name == index) {
            schema.language = crate::vector::stem::LanguageOptions {
                language,
                field: (!field.is_empty()).then_some(field),
            };
        }
    }
    Ok(())
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
pub fn write_snapshot<K: AsRef<[u8]>>(
    path: &str,
    maps: &[HashMap<K, KiviDBValue>],
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
        let raw = PacedFile::new(File::create(&tmp_path)?);
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
    load_snapshot_with(|| File::open(path).map(BufReader::new)).map(|(loaded, _)| loaded)
}

/// Read a KDB image from anything byte-shaped.
///
/// Split out from [`load_snapshot`] so the AOF preamble — the same format,
/// living at an offset inside `appendonly.aof` rather than in its own file —
/// can be read without being copied out to disk first.
///
/// It is read once: an image that 1.2.0 wrote with the empty key in a
/// database before its last one is not read (see [`read_retrying`]). The
/// server reads its images with [`load_snapshot`], [`load_snapshot_bytes`]
/// and [`load_snapshot_with`].
pub fn load_snapshot_from(r: impl Read) -> io::Result<LoadedSnapshot> {
    read_image(r, true, &[], &mut Vec::new(), server_databases()).map(|(loaded, _)| loaded)
}

/// The databases a snapshot may have keys in (see [`SNAPSHOT_DATABASES`]).
fn server_databases() -> usize {
    SNAPSHOT_DATABASES.load(std::sync::atomic::Ordering::Relaxed)
}

/// [`load_snapshot_from`] for an image in memory, read again as
/// [`read_retrying`] says if it has to be.
pub fn load_snapshot_bytes(image: &[u8]) -> io::Result<LoadedSnapshot> {
    read_retrying(|| Ok(image), true, server_databases()).map(|(loaded, _, _)| loaded)
}

/// Load the image that `open` opens, opening it again as [`read_retrying`]
/// says if it has to be. Returns the reader of the reading that loaded it.
pub fn load_snapshot_with<R: Read>(
    open: impl FnMut() -> io::Result<R>,
) -> io::Result<(LoadedSnapshot, R)> {
    read_retrying(open, true, server_databases()).map(|(loaded, _, reader)| (loaded, reader))
}

/// Read a KDB image from beginning to end without keeping any of it, and
/// return how many keys it holds.
///
/// This is how a replica finds out that a full-sync payload is whole and
/// readable *before* it gives up the dataset it already has: every key and
/// value is parsed, exactly as loading would, and dropped. It costs the
/// memory of one value at a time.
///
/// Every database has to end where the writer ends it, as in loading.
pub fn verify_snapshot(image: &[u8]) -> io::Result<usize> {
    read_retrying(|| Ok(image), false, server_databases()).map(|(_, keys, _)| keys)
}

/// Readings [`read_retrying`] makes of one image at most.
const READINGS: usize = 32;

/// Read the image `open` opens with [`read_image`], and if that fails, again
/// with the empty key as 1.2.0 wrote it in a database before the last.
///
/// 1.2.0 wrote the empty key with the length that ends a database (#441). In
/// the last database the reader tells the two apart by the byte after it.
/// Before the last one it cannot always: that byte begins the next database,
/// and a first key 1 to 15 bytes long has a length whose first byte is also a
/// type tag. So the image is read as written by 1.2.1 first, and only if that
/// fails, again with such an end read as the empty key, earliest first. A
/// wrong guess reads what follows as a value, which as a rule fails soon
/// after (a length misread can have up to 4 GiB read before it does). 1.2.0
/// wrote at most one empty key per database.
///
/// The image is opened again for each reading, and read no more than
/// [`READINGS`] times. A reading that fails other than on what it read (the
/// disk, the network) ends it. If none loads the image, the first reading's
/// error is returned.
fn read_retrying<R: Read>(
    mut open: impl FnMut() -> io::Result<R>,
    keep: bool,
    databases: usize,
) -> io::Result<(LoadedSnapshot, usize, R)> {
    let mut first_error: Option<io::Error> = None;
    // Readings still to make, latest pushed first: each a reading that
    // failed, with one database more read as holding the empty key. Never
    // more than the readings left.
    let mut plans: Vec<Vec<usize>> = vec![Vec::new()];
    let mut readings = 0;
    while let Some(plan) = plans.pop() {
        if readings == READINGS {
            break;
        }
        readings += 1;
        let mut reader = match open() {
            Ok(reader) => reader,
            Err(e) => return Err(first_error.unwrap_or(e)),
        };
        let mut ends = Vec::new();
        match read_image(&mut reader, keep, &plan, &mut ends, databases) {
            Ok((loaded, keys)) => {
                if !plan.is_empty() {
                    log_warn!(
                        "snapshot was written by KiviDB 1.2.0 with the empty key in database(s) \
                         {plan:?}, where 1.2.0 ends a database (#441): read as the empty key"
                    );
                }
                return Ok((loaded, keys, reader));
            }
            // Read whole, and refused for its databases (#482): this
            // reading's error, whichever reading it is.
            Err(e) if is_past_databases(&e) => return Err(e),
            Err(e) => {
                let layout = matches!(
                    e.kind(),
                    io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
                );
                if first_error.is_none() {
                    if !layout {
                        return Err(e);
                    }
                    first_error = Some(e);
                    push_plans(&mut plans, &plan, &ends, READINGS - readings);
                    continue;
                }
                if !layout {
                    log_warn!(
                        "snapshot: reading it again failed ({e}); what the first reading \
                         found is returned"
                    );
                    break;
                }
                let room = READINGS.saturating_sub(readings + plans.len());
                push_plans(&mut plans, &plan, &ends, room);
            }
        }
    }
    if readings > 1 {
        log_warn!(
            "snapshot could not be read as KiviDB writes it, nor in {} more reading(s) with \
             the empty key where 1.2.0 could have written it (#441){}",
            readings - 1,
            if readings == READINGS {
                ", the most it is read"
            } else {
                ""
            }
        );
    }
    Err(first_error.unwrap_or_else(|| io::Error::other("snapshot could not be read")))
}

/// [`read_retrying`]'s next readings after one with `plan` failed: `plan` and
/// one of the databases in `ends` after the last in it, the earliest `room`
/// of them, pushed so that the earliest is read first.
fn push_plans(plans: &mut Vec<Vec<usize>>, plan: &[usize], ends: &[usize], room: usize) {
    let after = plan.last().map_or(0, |&db| db + 1);
    let next: Vec<usize> = ends
        .iter()
        .copied()
        .filter(|&db| db >= after)
        .take(room)
        .collect();
    for db in next.into_iter().rev() {
        let mut more = plan.to_vec();
        more.push(db);
        plans.push(more);
    }
}

/// The reader behind [`load_snapshot_from`] and [`verify_snapshot`]. With
/// `keep` unset nothing is retained; the count of keys read is returned
/// either way.
///
/// The empty key as 1.2.0 wrote it (#441), a key length of 0 and then the
/// key's type tag, is read where a database's first 0 can only be that: in
/// the last database, where an end is followed by the end of the image or a
/// section, which opens with `KIVI`, never with a tag. And in the databases
/// `legacy` names (see [`read_retrying`]). The other databases whose first
/// 0 was read as their end, and is followed by a byte that is a tag, are
/// added to `ends` (no more than [`READINGS`] of them).
///
/// A reading that takes 1.2.0's empty key reads an image 1.2.0 wrote, which
/// holds only sections 1.2.0 knows: it does not pass over a section it does
/// not know, as other readings do. No reading passes over a section that is
/// cut short or cannot be used.
fn read_image(
    r: impl Read,
    keep: bool,
    legacy: &[usize],
    ends: &mut Vec<usize>,
    databases: usize,
) -> io::Result<(LoadedSnapshot, usize)> {
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
    // The first byte of what follows a database's end (the next database or
    // a section), read to see whether it is a tag.
    let mut next_byte = None;
    let mut strict = !legacy.is_empty();
    // The refusal of a key or index in a database past `databases` (#482).
    // Given once the image has been read through, sections and all: a
    // reading of a 1.2.0 image that took the empty key in the wrong place
    // can come upon a key in such a database before it fails on what it
    // misread, and has to fail on that, so that the right reading is tried.
    let mut past: Option<String> = None;

    for db in 0..db_count {
        let mut map: HashMap<Key, KiviDBValue> = HashMap::new();
        let last = db + 1 == db_count;
        // Whether this database's 1.2.0 empty key has been read.
        let mut legacy_read = false;

        loop {
            // Read key length — if EOF here, we're done
            let mut len_buf = [0u8; 4];
            let read = match next_byte.take() {
                Some(first) => {
                    len_buf[0] = first;
                    r.read_exact(&mut len_buf[1..])
                }
                None => r.read_exact(&mut len_buf),
            };
            match read {
                // An image that stops before a database's end marker is not a
                // whole image, loaded or verified: loading it as far as it
                // went started a server on part of the data, or none (#444).
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "the snapshot ends before its last database does: it is cut short",
                    ));
                }
                Err(e) => return Err(e),
                Ok(_) => {}
            }
            let klen = u32::from_le_bytes(len_buf);

            // TAG_EOF sentinel (key len == 0 means end of DB section), or
            // the empty key as 1.2.0 wrote it, with its tag after it.
            let mut legacy_tag = None;
            if klen == 0 {
                if !legacy_read && legacy.contains(&db) {
                    let mut tag = [0u8];
                    r.read_exact(&mut tag)?;
                    if !is_tag(tag[0]) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "no empty key where 1.2.0 would have written one",
                        ));
                    }
                    legacy_tag = Some(tag[0]);
                } else if !legacy_read && last {
                    let mut next = [0u8];
                    match r.read_exact(&mut next) {
                        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                        Err(e) => return Err(e),
                        Ok(()) if is_tag(next[0]) => {
                            log_warn!(
                                "snapshot holds the empty key in database {db} as KiviDB 1.2.0 \
                                 wrote it, with the length that ends a database (#441): read as \
                                 the empty key"
                            );
                            legacy_tag = Some(next[0]);
                            strict = true;
                        }
                        Ok(()) => {
                            next_byte = Some(next[0]);
                            break;
                        }
                    }
                } else if !legacy_read && !last {
                    let mut next = [0u8];
                    match r.read_exact(&mut next) {
                        // Read again by the next database, which then ends
                        // where the image does.
                        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {}
                        Err(e) => return Err(e),
                        Ok(()) => {
                            if is_tag(next[0]) && ends.len() < READINGS {
                                ends.push(db);
                            }
                            next_byte = Some(next[0]);
                        }
                    }
                    break;
                } else {
                    break;
                }
                legacy_read = true;
            }
            // The empty key: no bytes follow.
            let klen = if klen == EMPTY_KEY_LEN {
                0
            } else {
                klen as usize
            };

            let kbuf = read_exactly(&mut r, klen)?;
            // Any bytes: a key need not be text.
            let key = Key::from(kbuf);

            // Read type tag
            let mut tag = [0u8];
            match legacy_tag {
                Some(legacy_tag) => tag[0] = legacy_tag,
                None => r.read_exact(&mut tag)?,
            }

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
                        let Some(score) = zset_score(f64::from_le_bytes(score_buf)) else {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("{NAN_SCORE} in key '{}'", String::from_utf8_lossy(&key)),
                            ));
                        };
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
                            let dest_key =
                                crate::key_bytes::text_of(&read_bytes(&mut r)?).into_owned();
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
                            ts.source =
                                Some(crate::key_bytes::text_of(&read_bytes(&mut r)?).into_owned());
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
                        // Any bytes, as in Redis.
                        let name = crate::key_bytes::KeyBytes::from(read_bytes(&mut r)?);

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
                    // monotonic Instant relative to "now". A field whose time
                    // has passed is dropped, unless fields are held (under
                    // an AOF, or a replica's load): then it is kept with its
                    // deadline, for the `HDEL` that says it went.
                    let keep_past = crate::expiry::fields_held();
                    let mut exp_count_buf = [0u8; 4];
                    r.read_exact(&mut exp_count_buf)?;
                    let exp_count = u32::from_le_bytes(exp_count_buf) as usize;
                    let mut exps = crate::types::Expiries::with_capacity(hint(exp_count));
                    let clock = crate::commands::helpers::FieldClock::now();
                    for _ in 0..exp_count {
                        let field_bytes = read_bytes(&mut r)?;
                        let field = String::from_utf8(field_bytes).map_err(|_| {
                            io::Error::new(io::ErrorKind::InvalidData, "bad expiry field")
                        })?;
                        let mut ms_buf = [0u8; 8];
                        r.read_exact(&mut ms_buf)?;
                        let abs_ms = u64::from_le_bytes(ms_buf);
                        if let Some(at) = clock.instant_of(abs_ms) {
                            exps.insert(field, at);
                        } else if keep_past && hmap.contains_key(&field) {
                            exps.insert(field, clock.instant_or_past_of(abs_ms));
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

            // A key in a database past the server's (#482): not kept, and
            // the image refused once it is read.
            let past_databases = db >= databases;
            if past_databases && past.is_none() {
                past = database_of_snapshot(db as u64, databases, "keys").err();
            }
            keys_read += 1;
            let has_value = match &value {
                KiviDBValue::HashEx(hash) => !hash.read().is_empty(),
                _ => true,
            };
            if keep && has_value && !past_databases {
                map.insert(key, value);
            }
        }

        result.push(map);
    }

    // ── Optional trailing sections ────────────────────────────────────────────
    // Each opens with an 8-byte magic. They are scanned rather than read in a
    // fixed order so that an image carrying a section this build does not know
    // still loads: an unrecognised magic simply ends the scan.
    //
    // An image that ends inside a section, its magic or its body, is cut
    // short, and refused on every reading (#474): loading it started a server
    // without its TTLs, its function libraries or its indexes.
    //
    // A section that is all there but cannot be used is refused too, the
    // index, HNSW graph and alias sections included (see `usable_section`).
    let mut index_schemas = Vec::new();
    let mut hnsw_graphs = std::collections::HashMap::new();
    let mut dictionaries = Vec::new();
    let mut expiries: ExpiryMaps = (0..result.len()).map(|_| HashMap::new()).collect();
    let mut functions = None;
    loop {
        let mut magic = [0u8; 8];
        let from = match next_byte.take() {
            Some(first) => {
                magic[0] = first;
                1
            }
            None => 0,
        };
        if !fill_or_end(&mut r, &mut magic[from..], from > 0)? {
            break;
        }
        // A section this build does not know is passed over (below); what
        // is not a section at all is not an image this reader understands,
        // and loading what came before it would load part of one (#441).
        if !magic.starts_with(SECTION_MAGIC_PREFIX) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "the image goes on past its databases with bytes that are not a section",
            ));
        }
        if &magic == KDB_INDEX_MAGIC {
            index_schemas = usable_section("index", read_index_schemas(&mut r))?;
        } else if &magic == KDB_HNSW_MAGIC {
            hnsw_graphs = usable_section("HNSW graph", read_hnsw_graphs(&mut r))?;
        } else if &magic == KDB_EXPIRY_MAGIC {
            whole_section("expiry", read_expiry_section(&mut r, &mut expiries))?;
        } else if &magic == KDB_ALIAS_MAGIC {
            let aliases = usable_section("index alias", read_alias_section(&mut r))?;
            for (alias, index) in aliases {
                if let Some(schema) = index_schemas.iter_mut().find(|s| s.name == index) {
                    schema.aliases.push(alias);
                }
            }
        } else if &magic == KDB_FIELD_OPTIONS_MAGIC {
            // Not best-effort: an index that comes back without its options
            // is a different index — a TAG field split at the wrong
            // character matches different documents.
            whole_section(
                "field options",
                read_field_options_section(&mut r, &mut index_schemas),
            )?;
        } else if &magic == KDB_SYNONYMS_MAGIC {
            dictionaries = whole_section(
                "synonyms",
                read_synonyms_section(&mut r, &mut index_schemas),
            )?;
        } else if &magic == KDB_VECTOR_TYPES_MAGIC {
            whole_section(
                "vector types",
                read_vector_types_section(&mut r, &mut index_schemas),
            )?;
        } else if &magic == KDB_FUNCTIONS_MAGIC {
            // Not best-effort: a node that comes back without its libraries
            // fails every FCALL.
            functions = Some(whole_section("functions", read_bytes(&mut r))?);
        } else if &magic == KDB_LANGUAGE_MAGIC {
            whole_section(
                "language",
                read_language_section(&mut r, &mut index_schemas),
            )?;
        } else if &magic == KDB_SCORE_MAGIC {
            whole_section("score", read_score_section(&mut r, &mut index_schemas))?;
        } else if strict {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "an image read with 1.2.0's empty key holds a section 1.2.0 did not write",
            ));
        } else {
            break;
        }
    }

    // A key whose expiry passed while the image sat on disk (or in transit to
    // a replica) is gone, not "present without a TTL". Unless it is loaded
    // under an AOF, or by a replica (`expiry::held`): it is kept, with its
    // time, for the `DEL` that says it went, as Redis keeps it.
    let now_ms = unix_ms_now();
    let keep_past = crate::expiry::held();
    for (db, exp) in result.iter_mut().zip(expiries.iter_mut()) {
        exp.retain(|key, at_ms| {
            if !db.contains_key(key) {
                return false;
            }
            if *at_ms <= now_ms && !keep_past {
                db.remove(key);
                return false;
            }
            true
        });
    }

    // An index of a database past the server's: refused as its keys are
    // (`backfill_from_db` and the search commands take its database as one
    // the store has).
    if past.is_none()
        && let Some(schema) = index_schemas.iter().find(|s| s.db_index >= databases)
    {
        past = database_of_snapshot(
            schema.db_index as u64,
            databases,
            &format!("the index {}", schema.name),
        )
        .err();
    }
    if let Some(why) = past {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            PastDatabases(why),
        ));
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
            let key = Key::from(read_bytes(r)?);
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
            // A snapshot with keys past the store's databases is refused as
            // it is read (see `SNAPSHOT_DATABASES`); this says so if one
            // was not.
            if !map.is_empty() {
                log_error!(
                    "snapshot: the times to live of {} keys in database {db_idx} are dropped: \
                     the store has {} databases",
                    map.len(),
                    store.len()
                );
            }
            continue;
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
        language: Default::default(),
        default_score: 1.0,
        score_field: None,
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

    let mut links: Vec<u32> = Vec::new();
    for id in 0..count {
        let (node, vector) = match (nodes.get(id), hnsw.vector(id)) {
            (Some(node), Some(vector)) => (node, vector),
            _ => {
                w.write_all(&[0u8])?; // tombstone slot
                continue;
            }
        };
        w.write_all(&[1u8])?; // live node
        write_bytes(w, &crate::key_bytes::bytes_of(&node.key))?;
        w.write_all(&(vector.len() as u32).to_le_bytes())?;
        for f in vector {
            w.write_all(&f.to_le_bytes())?;
        }
        let layers = node.layer_count();
        w.write_all(&(layers as u32).to_le_bytes())?;
        for layer in 0..layers {
            links.clear();
            hnsw.links(&nodes, id, layer, &mut links);
            links.retain(|&nb| (nb as usize) < count);
            w.write_all(&(links.len() as u32).to_le_bytes())?;
            for &nb_id in &links {
                w.write_all(&nb_id.to_le_bytes())?;
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

        let mut nodes: Vec<Option<crate::vector::RestoredNode>> =
            Vec::with_capacity(hint(node_count));

        for _ in 0..node_count {
            let mut live_flag = [0u8];
            r.read_exact(&mut live_flag)?;

            if live_flag[0] == 0 {
                nodes.push(None);
            } else {
                let key_bytes = read_bytes(r)?;
                let key = crate::key_bytes::text_of(&key_bytes).into_owned();

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

                nodes.push(Some(crate::vector::RestoredNode {
                    key,
                    vector,
                    neighbors,
                }));
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

        // An entry point outside the graph, or deleted, would be read out
        // of bounds by the first search: `restore` starts from any live
        // node instead.
        let hnsw = crate::vector::HnswIndex::restore(m, ef_construction, nodes, entry_point);
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

/// How many databases a snapshot may have keys in, an RDB file or a KiviDB
/// image: the server's `databases`, set as it starts. A snapshot with keys
/// in a database past them is refused, naming it. An RDB file's were loaded
/// into database 0, merged with that database's keys and overwriting those
/// of the same name (#477); a KiviDB image's were dropped (#482).
pub static SNAPSHOT_DATABASES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(16);

/// The largest number of databases the refusal of a snapshot advises
/// starting with. A database number past it is more likely a damaged file
/// than a server to configure.
const DATABASES_ADVISED_MAX: u64 = 1 << 16;

/// `db`, if it is one of the server's databases (see
/// [`SNAPSHOT_DATABASES`]); otherwise what the refusal of the snapshot says.
fn database_of_snapshot(db: u64, databases: usize, what: &str) -> Result<usize, String> {
    match usize::try_from(db) {
        Ok(index) if index < databases => Ok(index),
        _ if db < DATABASES_ADVISED_MAX => Err(format!(
            "the file has {what} in database {db}, and this server has {databases} databases: \
             start it with databases {} or more",
            db + 1
        )),
        _ => Err(format!(
            "the file has {what} in database {db}, a number more likely of a damaged file \
             than of a server's databases (this server has {databases})"
        )),
    }
}

/// The refusal of a KiviDB image with keys or an index in a database past
/// the server's. Given only for a reading that read the image whole: that
/// reading is the right one, and no other is tried.
#[derive(Debug)]
struct PastDatabases(String);

impl std::fmt::Display for PastDatabases {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PastDatabases {}

fn is_past_databases(e: &io::Error) -> bool {
    e.get_ref().is_some_and(|inner| inner.is::<PastDatabases>())
}

// Unused by the binary, which wants the expiries; see `write_snapshot`.
#[allow(dead_code)]
pub fn load_redis_rdb(path: &str) -> io::Result<Vec<HashMap<Key, KiviDBValue>>> {
    load_redis_rdb_with_expiry(path).map(|(dbs, _, _)| dbs)
}

/// What a Redis RDB file holds: its databases, their keys' expiries, and its
/// function libraries as a Redis `FUNCTION DUMP` payload, if it has any.
pub type RedisRdb = (Vec<HashMap<Key, KiviDBValue>>, ExpiryMaps, Option<Vec<u8>>);

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
///
/// Refused rather than loaded as far as it goes: a file whose version is not
/// a number, one that ends before its checksum or with the wrong one, and a
/// file of a Dragonfly snapshot in Dragonfly's own format. Each used to load,
/// often with no keys at all, and the server started on that (#444).
pub fn load_redis_rdb_from(r: impl Read) -> io::Result<RedisRdb> {
    let mut r = Crc64Reader { inner: r, crc: 0 };
    let now_ms = unix_ms_now();

    // `REDIS` and a 4-digit RDB version, or `VALKEY` and a 3-digit one
    // (Valkey 9): whose numbering the type bytes below follow.
    let mut header = [0u8; 9];
    r.read_exact(&mut header)?;
    let dialect = rdb_dialect::from_header(&header)?;
    // The RDB version, for the checksum that ends a file from version 5 on.
    // A Redis one that is not a number is refused, not read as 0 (#444).
    // Dragonfly and KeyDB write RDB 9, with types and opcodes past Redis
    // 6.2's (see `rdb_extensions`).
    let version: u16 = match dialect {
        rdb_dialect::Dialect::Valkey => rdb_dialect::VALKEY_RDB_VERSION,
        rdb_dialect::Dialect::Redis => std::str::from_utf8(&header[5..])
            .ok()
            .filter(|s| s.bytes().all(|b| b.is_ascii_digit()))
            .and_then(|s| s.parse().ok())
            .filter(|&version| version >= 1)
            .ok_or_else(|| rdb_invalid("not a Redis RDB file: its version is not a number"))?,
    };

    let mut result: Vec<HashMap<Key, KiviDBValue>> = (0..16).map(|_| HashMap::new()).collect();
    let mut expiries: ExpiryMaps = (0..16).map(|_| HashMap::new()).collect();

    let mut current_db = 0usize;
    // An expiry belongs to the key after it. Redis may write the key's idle
    // time or access frequency in between, so it waits here for the key.
    let mut pending_expiry: Option<u64> = None;
    // The search module's data is skipped with one warning per file.
    let mut warned_search = false;
    // Each function library's code, in the order the file has them.
    let mut libraries = Vec::new();
    // The key read last, while it is there: KeyDB writes the TTLs it gave
    // the key's members after it.
    let mut last_key: Option<(usize, Key)> = None;
    let mut keydb_member: Option<Vec<u8>> = None;
    // Keys Dragonfly kept from eviction (`STICK`), which load as any other.
    let mut sticky = 0usize;

    loop {
        let mut type_byte = [0u8];
        r.read_exact(&mut type_byte)?;

        match type_byte[0] {
            0xFA => {
                // Aux field: metadata, skipped, but for two kinds.
                //
                // Dragonfly's own format (`.dfs`) has a file for each shard
                // and one, the summary, with none of the keys. Each is an RDB
                // file. From Dragonfly 1.32 on, each has one of these fields,
                // and its RDB files (`SAVE RDB`) have neither. Earlier ones
                // write neither anywhere: see `preload` in src/aof.rs.
                //
                // KeyDB's member TTLs (`EXPIREMEMBER`) follow their key.
                let aux_key = rdb_read_string(&mut r)?;
                let aux_val = rdb_read_string(&mut r)?;
                match aux_key.as_slice() {
                    b"shard-id" | b"shard-count" => {
                        return Err(rdb_invalid(
                            "this is one file of a Dragonfly snapshot in Dragonfly's own format \
                             (.dfs), which keeps each shard's keys in a file of its own; save one \
                             in RDB format (Dragonfly's SAVE RDB) and load that",
                        ));
                    }
                    b"keydb-subexpire-key" => keydb_member = Some(aux_val),
                    b"keydb-subexpire-when" => {
                        let member = keydb_member
                            .take()
                            .ok_or_else(|| rdb_invalid("a KeyDB member TTL without its member"))?;
                        // A key dropped as past its time takes its members'
                        // TTLs with it.
                        if let Some((db, key)) = last_key.clone()
                            && let Some(value) = result[db].get_mut(&key)
                            && rdb_extensions::apply_keydb_subexpiry(
                                value,
                                &crate::key_bytes::text_of(key.as_ref()),
                                &member,
                                &aux_val,
                            )? == rdb_extensions::SubExpiry::Emptied
                        {
                            result[db].remove(&key);
                            expiries[db].remove(&key);
                            last_key = None;
                        }
                    }
                    _ => {}
                }
            }
            0xFE => {
                // Database selector
                let (db_num, _) = rdb_read_length(&mut r)?;
                let databases = SNAPSHOT_DATABASES.load(std::sync::atomic::Ordering::Relaxed);
                current_db =
                    database_of_snapshot(db_num, databases, "keys").map_err(rdb_invalid)?;
                if current_db >= result.len() {
                    result.resize_with(current_db + 1, HashMap::new);
                    expiries.resize_with(current_db + 1, HashMap::new);
                }
            }
            0xFB => {
                // Resize DB hint (RDB v7+) — two lengths, both skippable
                let _ = rdb_read_length(&mut r)?;
                let _ = rdb_read_length(&mut r)?;
            }
            0xFF => {
                // The end, and from version 5 on, the CRC64 of everything
                // before it. A checksum of 0 is none: Redis writes that with
                // `rdbchecksum no`, and so does Dragonfly.
                if version >= 5 {
                    let sum = r.crc;
                    let mut crc = [0u8; 8];
                    r.read_exact(&mut crc).map_err(|_| {
                        rdb_invalid("the file ends before its checksum: it is cut short")
                    })?;
                    let crc = u64::from_le_bytes(crc);
                    // `rdbchecksum no` skips the check, as in Redis: a file
                    // whose only fault is its checksum can still be loaded.
                    if crc != 0 && crc != sum && rdb_checksum_checked() {
                        return Err(rdb_invalid(
                            "wrong RDB checksum: the file is damaged (to load it all the same, \
                             set rdbchecksum no in the config file)",
                        ));
                    }
                }
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
            // Valkey's RDB_OPCODE_SLOT_IMPORT, a slot migration in progress.
            0xF3 if dialect == rdb_dialect::Dialect::Valkey => {
                return Err(rdb_dialect::refuse_slot_import(&mut r));
            }
            // Dragonfly's RDB_OPCODE_DF_MASK: the next key's flags.
            0xDC if version <= 11 => {
                if rdb_extensions::read_dragonfly_mask(&mut r)? {
                    sticky += 1;
                }
            }
            opcode if rdb_extensions::is_dragonfly_opcode(version, opcode) => {
                return Err(rdb_extensions::refuse_dragonfly_opcode(opcode));
            }
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
                // As the bytes they are: two keys that are not text used
                // to become one (#446).
                let key = Key::from(key_bytes);
                let expiry_ms = pending_expiry.take();
                let value = if rdb_extensions::is_dragonfly_type(version, type_id) {
                    let name = crate::key_bytes::text_of(key.as_ref());
                    rdb_extensions::read_dragonfly_value(&mut r, Some(&name), type_id)?
                } else if type_id == 64 && version <= 11 {
                    return Err(rdb_extensions::refuse_keydb_cron(
                        &crate::key_bytes::text_of(key.as_ref()),
                    ));
                } else {
                    rdb_dialect::read_value(&mut r, type_id, dialect)?
                };
                last_key = None;
                if let Some(val) = value {
                    match expiry_ms {
                        // Already past its expiry: dropped. Not by a replica,
                        // or under an AOF (see `load_snapshot_from`).
                        Some(ms) if ms <= now_ms && !crate::expiry::held() => {}
                        Some(ms) => {
                            expiries[current_db].insert(key.clone(), ms);
                            result[current_db].insert(key.clone(), val);
                            last_key = Some((current_db, key));
                        }
                        None => {
                            // A plain entry replaces any earlier one for this
                            // key, including that entry's expiry.
                            expiries[current_db].remove(&key);
                            result[current_db].insert(key.clone(), val);
                            last_key = Some((current_db, key));
                        }
                    }
                }
            }
        }
    }

    if sticky > 0 {
        log_warn!(
            "RDB: {sticky} keys were sticky in Dragonfly (STICK, never evicted): KiviDB has no \
             such flag, and they load as any other key"
        );
    }
    Ok((result, expiries, redis_function_dump(&libraries)))
}

/// Whether an RDB file's checksum is checked as it is loaded: unless
/// `rdbchecksum no` is set, as in Redis.
fn rdb_checksum_checked() -> bool {
    crate::commands::server::remembered_config("rdbchecksum").as_deref() != Some("no")
}

/// Whether the RDB file at `path` was written by Dragonfly, by its aux
/// fields: `df-ver`, or, before Dragonfly wrote that, `redis-ver
/// 999.999.999`. Only the fields at the start of the file are read.
pub fn rdb_written_by_dragonfly(path: &str) -> io::Result<bool> {
    let mut r = BufReader::new(File::open(path)?);
    let mut magic = [0u8; 9];
    r.read_exact(&mut magic)?;
    if &magic[..5] != b"REDIS" {
        return Ok(false);
    }
    loop {
        let mut opcode = [0u8];
        if r.read_exact(&mut opcode).is_err() || opcode[0] != 0xFA {
            return Ok(false);
        }
        let key = rdb_read_string(&mut r)?;
        let value = rdb_read_string(&mut r)?;
        if key == b"df-ver" || (key == b"redis-ver" && value == b"999.999.999") {
            return Ok(true);
        }
    }
}

/// Redis's CRC64 ([`redis_crc64`]) a byte at a time, from a table.
const REDIS_CRC64_TABLE: [u64; 256] = {
    const POLY: u64 = 0x95ac_9329_ac4b_c9b5;
    let mut table = [0u64; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u64;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

/// [`redis_crc64`] of `data`, carried on from `crc`, the sum of what came
/// before it.
fn redis_crc64_update(mut crc: u64, data: &[u8]) -> u64 {
    for &byte in data {
        crc = REDIS_CRC64_TABLE[((crc ^ u64::from(byte)) & 0xff) as usize] ^ (crc >> 8);
    }
    crc
}

/// A reader that sums what is read through it with Redis's CRC64: what an
/// RDB file's checksum is checked against.
struct Crc64Reader<R> {
    inner: R,
    crc: u64,
}

impl<R: Read> Read for Crc64Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.crc = redis_crc64_update(self.crc, &buf[..n]);
        Ok(n)
    }
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
                let score = zset_score_or_error(rdb_read_double_v1(r)?)?;
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
                let field = crate::key_bytes::text_of(&field_bytes).into_owned();
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
                let score = zset_score_or_error(f64::from_le_bytes(score_buf))?;
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
                let score = zset_score_or_error(score)?;
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
                let field_str = crate::key_bytes::text_of(&field).into_owned();
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
        // Any bytes, as Redis keeps them.
        let name = crate::types::StreamName::from(rdb_read_string(r)?);
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
                consumer: crate::types::StreamName::default(),
                delivered_at,
                delivery_count,
            };
            if group.pel.insert(id, entry).is_some() {
                return Err(rdb_invalid("a pending entry is listed twice"));
            }
        }
        for _ in 0..rdb_read_plain_length(r)? {
            let consumer = crate::types::StreamName::from(rdb_read_string(r)?);
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
/// Fields already past their time are kept with that deadline, for
/// `field_expiry` to remove (and, on a master, to send the `HDEL` of).
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

    // Read once for the hash (see `FieldClock`, #448).
    let clock = crate::commands::helpers::FieldClock::now();
    // A field whose time is up is kept with its deadline, which has passed:
    // this reads `RESTORE`'s Redis payloads too, and what is restored has to
    // be the same on every reader of the stream, whenever each restores it
    // (see the KDB `DUMP` loader). The field goes by `field_expiry`. A
    // deadline further out than the clock can hold is still refused.
    let mut inner = crate::types::HashInner::new();
    for (field, value, expires_at) in fields {
        let field = crate::key_bytes::text_of(&field).into_owned();
        if expires_at != 0 {
            inner.expiries.insert(
                field.clone(),
                rdb_dialect::field_deadline(&clock, expires_at)?,
            );
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
    // RedisGraph's (`graphdata`), which Redis Stack 6.2 writes into every
    // RDB file, graphs or none, is read past too: a graph itself is a key of
    // module type `graphdata`, and that is refused.
    let graph = name == "graphdata";
    if name != "ft_index0" && name != "scdtype00" && !graph {
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
    if !graph && !names.is_empty() && !*warned {
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
        let field = crate::key_bytes::text_of(&field).into_owned();
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
        let score = zset_score_or_error(score)?;
        ss.insert(Arc::from(member.as_slice()), score);
    }
    Ok(())
}

/// Load a snapshot and populate the store directly.
/// Handles the platform difference in RwLock::write() internally.
pub fn restore_into_store(dbs: Vec<HashMap<Key, KiviDBValue>>, store: &store::Store) -> usize {
    let mut total = 0usize;
    for (db_idx, map) in dbs.into_iter().enumerate() {
        if db_idx >= store.len() {
            // A snapshot with keys past the store's databases is refused as
            // it is read (see `SNAPSHOT_DATABASES`); this says so if one
            // was not. It used to drop them without a word (#482).
            if !map.is_empty() {
                log_error!(
                    "snapshot: {} keys in database {db_idx} are dropped: the store has {} \
                     databases",
                    map.len(),
                    store.len()
                );
            }
            continue;
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
    /// The indexes' languages (see [`write_kdb_language_section`]).
    language_section: Vec<u8>,
    /// The indexes' scores (see [`write_kdb_score_section`]).
    score_section: Vec<u8>,
    /// What all of the above takes, which is no key's and which deleting
    /// keys does not give back: not counted against `maxmemory` while the
    /// capture lives (#284). Its holds on strings are counted as the table
    /// lets go of them (`store::entry`).
    _not_counted: crate::memory_use::NotCounted,
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
    /// What the capture of the database takes besides the entries it holds.
    fn own_bytes(&self) -> usize {
        let held = self.strings.iter().map(Vec::capacity).sum::<usize>()
            * std::mem::size_of::<store::EntryRef>();
        let encoded = self.encoded.iter().map(Vec::capacity).sum::<usize>();
        let timed =
            self.timed.iter().map(Vec::capacity).sum::<usize>() * std::mem::size_of::<(Key, u64)>();
        held + encoded + timed
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
    let mut language_section = Vec::new();
    write_kdb_language_section(&mut language_section, index_store)?;
    let functions = function_libraries();
    let mut score_section = Vec::new();
    write_kdb_score_section(&mut score_section, index_store)?;
    let own = dbs.iter().map(FrozenDb::own_bytes).sum::<usize>()
        + index_section.capacity()
        + alias_section.capacity()
        + language_section.capacity()
        + functions.as_ref().map_or(0, Vec::capacity)
        + score_section.capacity();

    Ok(FrozenDataset {
        dbs,
        index_section,
        alias_section,
        functions,
        language_section,
        score_section,
        _not_counted: crate::memory_use::NotCounted::new(own),
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
    #[allow(dead_code)]
    pub fn into_payload(self) -> std::io::Result<Vec<u8>> {
        self.into_image().map(|(image, _)| image)
    }

    /// Lay the captured dataset out as a KDB image, which is not counted
    /// against `maxmemory` while the second part lives: a full sync's or a
    /// backup's image, which deleting keys does not give back (#284).
    pub fn into_image(self) -> std::io::Result<(Vec<u8>, crate::memory_use::NotCounted)> {
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
        let mut not_counted = crate::memory_use::NotCounted::new(out.capacity());
        let before = out.capacity();
        self.write_to(&mut out)?;
        // Grown past what was reckoned (the sections after the keys).
        not_counted.add(out.capacity().saturating_sub(before));
        Ok((out, not_counted))
    }

    /// Write the image to `path`, made durable, under that name. The image is
    /// written as it is laid out, not gathered in memory first: for a dump of
    /// the whole dataset that would be the dataset over again.
    pub fn write_file(self, path: &str) -> std::io::Result<()> {
        let raw = PacedFile::new(File::create(path)?);
        let mut file = BufWriter::with_capacity(1 << 20, raw);
        self.write_to(&mut file)?;
        file.flush()?;
        file.get_ref().sync_all()
    }

    fn write_to(mut self, out: &mut impl Write) -> std::io::Result<()> {
        // Captured, with writes going on again: a test writes over and
        // deletes keys the capture holds here.
        crate::aof::failpoint("snapshot-writing");
        write_kdb_header(out, self.dbs.len() as u32)?;
        // The times to live of the strings, for the expiry section after
        // the keys: each string's hold goes as soon as it is written, so
        // that a key written over or deleted since the capture is freed
        // then, not when the whole image is (#284).
        let mut timed_strings: Vec<Vec<(Key, u64)>> = Vec::with_capacity(self.dbs.len());
        for db in &mut self.dbs {
            let mut timed = Vec::new();
            for part in std::mem::take(&mut db.strings) {
                for entry in part {
                    write_key(out, entry.key())?;
                    out.write_all(&[TAG_STRING])?;
                    write_bytes(out, frozen_string(&entry))?;
                    if let Some(at) = entry.expires() {
                        timed.push((Key::from(entry.key()), at));
                    }
                }
            }
            timed_strings.push(timed);
            for encoded in &db.encoded {
                out.write_all(encoded)?;
            }
            write_kdb_db_end(out)?;
        }
        // The times to live and the function libraries before the index
        // sections: a reader that cannot read one of those (they are read
        // best-effort) stops there, and what comes after it is lost. Read in
        // any order (`read_image` finds each section by its magic), as 1.2.0
        // reads them too.
        self.write_expiries(out, &timed_strings)?;
        write_kdb_functions_section(out, self.functions.as_deref())?;
        out.write_all(&self.index_section)?;
        out.write_all(&self.alias_section)?;
        out.write_all(&self.language_section)?;
        out.write_all(&self.score_section)
    }
}

impl FrozenDataset {
    /// The expiry section ([`write_kdb_expiry_maps`]'s layout), from the
    /// times the capture took with the keys: `strings`, those of each
    /// database's strings, taken as they were written.
    fn write_expiries(
        &self,
        out: &mut impl Write,
        strings: &[Vec<(Key, u64)>],
    ) -> std::io::Result<()> {
        let counts: Vec<usize> = self
            .dbs
            .iter()
            .zip(strings)
            .map(|(db, strings)| strings.len() + db.timed.iter().map(Vec::len).sum::<usize>())
            .collect();
        if counts.iter().all(|&n| n == 0) {
            return Ok(());
        }
        out.write_all(KDB_EXPIRY_MAGIC)?;
        out.write_all(&(self.dbs.len() as u32).to_le_bytes())?;
        for ((db, strings), count) in self.dbs.iter().zip(strings).zip(counts) {
            out.write_all(&(count as u32).to_le_bytes())?;
            for (key, at) in strings.iter().chain(db.timed.iter().flatten()) {
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

thread_local! {
    /// Set while [`dump_payload`] writes: a time series is written without
    /// its compaction rules and source.
    static WITHOUT_LINKS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// [`WITHOUT_LINKS`], while this lives.
struct WithoutLinks(bool);

impl WithoutLinks {
    fn begin() -> Self {
        WithoutLinks(WITHOUT_LINKS.replace(true))
    }
}

impl Drop for WithoutLinks {
    fn drop(&mut self) {
        WITHOUT_LINKS.set(self.0);
    }
}

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
    // A time series goes without its links to other series, its compaction
    // rules and the series it is a compaction of, as RedisTimeSeries leaves
    // them out of a `DUMP`: they name keys, which the payload may be
    // restored beside, under another name or on another server. Restored
    // with them, a series fed a compaction that had a source of its own
    // (#593). Its samples, settings and labels go. An atomic slot migration
    // sends the rules among the keys it moves as `TS.CREATERULE`s.
    let _without = WithoutLinks::begin();
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

/// The newest Redis RDB version whose `DUMP` payloads `RESTORE` reads: the
/// one Redis 8.10 writes. Redis refuses a payload from a newer version than
/// its own, as this does, since a newer one may hold encodings it cannot
/// read. Valkey 9's, RDB 80, are read too (see `rdb_dialect`).
const REDIS_DUMP_VERSION_MAX: u16 = rdb_dialect::REDIS_RDB_VERSION_MAX;

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
    if let Some(dialect) = rdb_dialect::of_dump_version(version)
        && sealed
    {
        return restore_redis_payload(data, dialect, version).ok_or(RestoreError::BadData);
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

/// A value as Redis's or Valkey's `DUMP` serialises it: its RDB type, then
/// the value in the RDB encoding of that type, which the `dump.rdb` loader
/// reads. Every byte must belong to the value.
///
/// Dragonfly's payloads may hold its own types (`rdb_extensions`), and
/// KeyDB's end with an `mvcc-tstamp` aux field after the value, which is
/// read past: it is KeyDB's version of the value for its active replication.
fn restore_redis_payload(
    data: &[u8],
    dialect: rdb_dialect::Dialect,
    version: u16,
) -> Option<KiviDBValue> {
    let (&type_id, mut rest) = data.split_first()?;
    let value = if rdb_extensions::is_dragonfly_type(version, type_id) {
        rdb_extensions::read_dragonfly_value(&mut rest, None, type_id).ok()??
    } else {
        rdb_dialect::read_value(&mut rest, type_id, dialect).ok()??
    };
    let rest = rdb_extensions::strip_keydb_mvcc(rest)?;
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
    if rdb_dialect::of_dump_version(version).is_none()
        || redis_crc64(versioned).to_le_bytes() != crc
    {
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
            let clock = crate::commands::helpers::FieldClock::now();
            for (field, exp) in inner.expiries.iter() {
                let abs_ms = clock.unix_ms_of(*exp);
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
                let score = zset_score(f64::from_bits(u64::from_le_bytes(
                    payload[next..next + 8].try_into().ok()?,
                )))?;
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
            let mut exps = crate::types::Expiries::new();
            let clock = crate::commands::helpers::FieldClock::now();
            for _ in 0..exp_count {
                let (field_bytes, next) = read_bytes_slice(payload, pos)?;
                let field = String::from_utf8(field_bytes.to_vec()).ok()?;
                if next + 8 > payload.len() {
                    return None;
                }
                let abs_ms = u64::from_le_bytes(payload[next..next + 8].try_into().ok()?);
                pos = next + 8;
                // A field whose time is up is kept with its deadline, which
                // has passed: what is restored is the same here and on every
                // reader of the stream, whenever each restores it, and the
                // field goes by `field_expiry`, its `HDEL` after the restore.
                // Dropped here by each one's clock, a field the master kept
                // was gone on a replica that restored it a moment later. It
                // keeps its expiry either way: kept without one, it lived on
                // for good (a field `MIGRATE` read just as it ran out).
                if map.contains_key(&field) {
                    exps.insert(field, clock.instant_or_past_of(abs_ms));
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

/// A hash field's deadline of whole Unix milliseconds is the same
/// millisecond after every way a hash is written and read back: a KDB image
/// (a save, a full sync, the AOF's preamble), a `DUMP` payload (`RESTORE`,
/// `MIGRATE`) and a Redis RDB file. The time now used to be cut to a whole
/// millisecond before each conversion, and a field often read back 1 ms
/// off what it was set to (#448).
#[cfg(test)]
mod field_deadline_tests {
    use super::*;
    use crate::commands::helpers::FieldClock;

    /// `n` deadlines of whole milliseconds an hour or so from now, each at a
    /// different point of the millisecond now is in.
    fn deadlines(n: u64) -> Vec<u64> {
        let base = unix_ms_now() + 3_600_000;
        (0..n).map(|i| base + i * 7 + i % 3).collect()
    }

    fn hash_with(deadlines: &[u64]) -> KiviDBValue {
        let clock = FieldClock::now();
        let mut inner = crate::types::HashInner::new();
        for (i, at) in deadlines.iter().enumerate() {
            let field = format!("f{i}");
            inner.fields.insert(field.clone(), Arc::from(&b"v"[..]));
            inner
                .expiries
                .insert(field, clock.instant_of(*at).expect("in the future"));
        }
        KiviDBValue::HashEx(Arc::new(parking_lot::RwLock::new(inner)))
    }

    /// The deadlines of `value`'s fields, as `HPEXPIRETIME` gives them.
    fn read_back(value: &KiviDBValue, n: usize) -> Vec<u64> {
        let KiviDBValue::HashEx(hash) = value else {
            panic!("not a hash with field TTLs");
        };
        let hash = hash.read();
        let clock = FieldClock::now();
        (0..n)
            .map(|i| clock.unix_ms_of(hash.expiries[&format!("f{i}")]))
            .collect()
    }

    #[test]
    fn the_clock_converts_a_whole_millisecond_both_ways_exactly() {
        for _ in 0..2_000 {
            let at = unix_ms_now() + 60_000;
            let instant = FieldClock::now().instant_of(at).unwrap();
            // Another reading of the clocks, as a later command makes.
            std::hint::black_box(std::time::Instant::now());
            assert_eq!(FieldClock::now().unix_ms_of(instant), at);
        }
        let clock = FieldClock::now();
        assert!(clock.instant_of(unix_ms_now().saturating_sub(1)).is_none());
        assert!(clock.instant_of(0).is_none());
    }

    /// A deadline that has come is kept as the moment it was (#415): the
    /// same `Instant` as `instant_of` gives one still to come, due at once
    /// for one gone, read back to the very millisecond while the clock goes
    /// back that far, and never later than now. One from before the clock
    /// goes back to (0, as a negative Unix time is taken) is now.
    #[test]
    fn a_deadline_that_has_come_is_kept_as_the_moment_it_was() {
        for _ in 0..2_000 {
            // Read before the clocks: the millisecond now is in has come.
            let now_ms = unix_ms_now();
            let clock = FieldClock::now();
            let ahead = now_ms + 60_000;
            assert_eq!(
                clock.instant_or_past_of(ahead),
                clock.instant_of(ahead).unwrap()
            );
            for ago in [0, 1, 2, 5_000] {
                let at = now_ms - ago;
                let instant = clock.instant_or_past_of(at);
                assert!(instant <= std::time::Instant::now(), "{ago} ms ago is due");
                if clock.instant_of(at).is_none() {
                    // Another reading of the clocks, as a later command makes.
                    assert_eq!(FieldClock::now().unix_ms_of(instant), at, "{ago} ms ago");
                }
            }
        }
        let clock = FieldClock::now();
        assert!(clock.instant_or_past_of(0) <= std::time::Instant::now());
        assert_eq!(
            clock.instant_or_past_of(u64::MAX),
            clock.instant_of(u64::MAX).unwrap()
        );
    }

    /// A file's or a payload's field deadline: one past is kept as it was,
    /// one further out than the clock can hold is still a corrupt file.
    #[test]
    fn a_file_deadline_that_has_come_is_kept_and_one_out_of_range_refused() {
        let clock = FieldClock::now();
        let at = unix_ms_now() - 5_000;
        let kept = rdb_dialect::field_deadline(&clock, at).unwrap();
        assert_eq!(clock.unix_ms_of(kept), at);
        let ahead = unix_ms_now() + 60_000;
        assert_eq!(
            rdb_dialect::field_deadline(&clock, ahead).unwrap(),
            clock.instant_of(ahead).unwrap()
        );
        if clock.checked_instant_of(u64::MAX).is_none() {
            assert!(rdb_dialect::field_deadline(&clock, u64::MAX).is_err());
        }
    }

    /// A deadline further out than 64 bits of milliseconds is written as
    /// the furthest there is, not cut to 64 bits: cut, it was a moment from
    /// now, and the field was gone once loaded.
    #[test]
    fn a_deadline_past_64_bits_of_milliseconds_saturates() {
        let clock = FieldClock::now();
        if let Some(far) = std::time::Instant::now()
            .checked_add(std::time::Duration::from_secs(20_000_000_000_000_000))
        {
            assert_eq!(clock.unix_ms_of(far), u64::MAX);
        }
        assert!(clock.instant_of(u64::MAX).is_some());
        let max = crate::commands::helpers::MAX_FIELD_DEADLINE_MS;
        use crate::commands::helpers::field_deadline_ms;
        assert_eq!(field_deadline_ms(max, false, 0), Some(max));
        assert_eq!(field_deadline_ms(max + 1, false, 0), None);
        assert_eq!(
            field_deadline_ms(max / 1000, true, 0),
            Some(max / 1000 * 1000)
        );
        assert_eq!(field_deadline_ms(max / 1000 + 1, true, 0), None);
        assert_eq!(field_deadline_ms(i64::MAX, true, 0), None);
        assert_eq!(field_deadline_ms(1000, false, max - 1000), Some(max));
        assert_eq!(field_deadline_ms(1001, false, max - 1000), None);
    }

    #[test]
    fn a_dump_payload_keeps_each_deadline_to_the_millisecond() {
        for _ in 0..200 {
            let want = deadlines(16);
            let payload = dump_payload(&hash_with(&want)).unwrap();
            let back = restore_payload(&payload).expect("payload restores");
            assert_eq!(read_back(&back, want.len()), want);
        }
    }

    #[test]
    fn a_kdb_image_keeps_each_deadline_to_the_millisecond() {
        for _ in 0..200 {
            let want = deadlines(16);
            let mut out = Vec::new();
            write_kdb_header(&mut out, 1).unwrap();
            write_kdb_entry(&mut out, "h", hash_with(&want).view()).unwrap();
            write_kdb_db_end(&mut out).unwrap();
            write_kdb_expiry_maps(&mut out, &[HashMap::<Key, u64>::new()]).unwrap();
            let loaded = load_snapshot_from(&out[..]).unwrap();
            assert_eq!(read_back(&loaded.dbs[0][&b"h"[..]], want.len()), want);
        }
    }

    /// Type 24, a hash table: the soonest deadline, then for each field its
    /// deadline less the soonest one, plus 1, its name and its value.
    #[test]
    fn a_redis_rdb_hash_keeps_each_deadline_to_the_millisecond() {
        for _ in 0..200 {
            let want = deadlines(16);
            let soonest = want[0];
            let mut rdb = Vec::new();
            rdb.extend_from_slice(&(soonest as i64).to_le_bytes());
            rdb_write_length(&mut rdb, want.len());
            for (i, at) in want.iter().enumerate() {
                rdb_write_length(&mut rdb, (at - soonest + 1) as usize);
                let field = format!("f{i}");
                rdb_write_length(&mut rdb, field.len());
                rdb.extend_from_slice(field.as_bytes());
                rdb_write_length(&mut rdb, 1);
                rdb.push(b'v');
            }
            let back = rdb_read_hash_with_ttls(&mut &rdb[..], 24).unwrap();
            assert_eq!(read_back(&back, want.len()), want);
        }
    }
}

#[cfg(test)]
mod expiry_section_tests {
    use super::*;

    fn string(v: &str) -> KiviDBValue {
        KiviDBValue::String(SharedBytes::from(v.as_bytes()))
    }

    fn image(maps: &[HashMap<Key, KiviDBValue>], expiries: &[HashMap<Key, u64>]) -> Vec<u8> {
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
        let db0 = HashMap::from([(Key::from("a"), string("1")), (Key::from("b"), string("2"))]);
        let db1 = HashMap::from([(Key::from("c"), string("3"))]);
        let exp = vec![
            HashMap::from([(Key::from("a"), far)]),
            HashMap::from([(Key::from("c"), far + 1)]),
        ];
        let loaded = load_snapshot_from(&image(&[db0, db1], &exp)[..]).unwrap();
        assert_eq!(loaded.expiries[0].get(&b"a"[..]), Some(&far));
        assert_eq!(loaded.expiries[0].get(&b"b"[..]), None);
        assert_eq!(loaded.expiries[1].get(&b"c"[..]), Some(&(far + 1)));
    }

    /// The whole point of persisting an expiry is that it is honoured: a key
    /// whose time passed while the image sat on disk must not come back.
    #[test]
    fn expired_keys_are_dropped_at_load() {
        let db0 = HashMap::from([
            (Key::from("live"), string("1")),
            (Key::from("dead"), string("2")),
        ]);
        let exp = vec![HashMap::from([
            (Key::from("live"), unix_ms_now() + 60_000),
            (Key::from("dead"), unix_ms_now().saturating_sub(1)),
        ])];
        let loaded = load_snapshot_from(&image(&[db0], &exp)[..]).unwrap();
        assert!(loaded.dbs[0].contains_key(&b"live"[..]));
        assert!(!loaded.dbs[0].contains_key(&b"dead"[..]));
        assert!(!loaded.expiries[0].contains_key(&b"dead"[..]));
    }

    /// An expiry for a key that is not in the image is dropped rather than
    /// installed, where it would attach to whatever later takes that name.
    #[test]
    fn orphan_expiries_are_not_installed() {
        let db0 = HashMap::from([(Key::from("a"), string("1"))]);
        let exp = vec![HashMap::from([(
            Key::from("ghost"),
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
            Key::from("hxgone"),
            KiviDBValue::HashEx(Arc::new(parking_lot::RwLock::new(hash))),
        )]);
        let exp = vec![HashMap::from([(
            Key::from("hxgone"),
            unix_ms_now() + 60_000,
        )])];

        let loaded = load_snapshot_from(&image(&[db0], &exp)[..]).unwrap();
        assert!(!loaded.dbs[0].contains_key(&b"hxgone"[..]));
        assert!(!loaded.expiries[0].contains_key(&b"hxgone"[..]));
    }

    /// A hash field past its deadline: dropped by a load of its own, kept
    /// with its deadline by one with fields held (under an AOF, a replica's),
    /// for the `HDEL` that follows; kept by `RESTORE` always, which every
    /// reader of the stream runs at its own moment.
    #[test]
    fn a_past_field_is_kept_where_fields_are_held_and_by_restore() {
        let hash = || {
            let mut hash = crate::types::HashInner::new();
            hash.fields.insert("gone".to_string(), Arc::from(&b"1"[..]));
            hash.fields.insert("live".to_string(), Arc::from(&b"2"[..]));
            hash.expiries.insert(
                "gone".to_string(),
                std::time::Instant::now() - std::time::Duration::from_secs(1),
            );
            KiviDBValue::HashEx(Arc::new(parking_lot::RwLock::new(hash)))
        };
        let fields_of = |value: &KiviDBValue| match value {
            KiviDBValue::HashEx(h) => {
                let h = h.read();
                let mut f: Vec<(String, bool)> = h
                    .fields
                    .keys()
                    .map(|k| (k.clone(), h.expiries.contains_key(k)))
                    .collect();
                f.sort();
                f
            }
            _ => panic!("not a hash with field TTLs"),
        };
        let image_of = || image(&[HashMap::from([(Key::from("h"), hash())])], &[]);
        let loaded = load_snapshot_from(&image_of()[..]).unwrap();
        assert_eq!(
            fields_of(&loaded.dbs[0][&b"h"[..]]),
            [("live".to_string(), false)]
        );
        {
            let _hold = crate::expiry::hold();
            let loaded = load_snapshot_from(&image_of()[..]).unwrap();
            assert_eq!(
                fields_of(&loaded.dbs[0][&b"h"[..]]),
                [("gone".to_string(), true), ("live".to_string(), false)]
            );
        }
        let restored = restore_payload(&dump_payload(&hash()).unwrap()).unwrap();
        let fields = fields_of(&restored);
        assert_eq!(
            fields,
            [("gone".to_string(), true), ("live".to_string(), false)]
        );
        if let KiviDBValue::HashEx(h) = &restored {
            assert!(h.read().has_due(std::time::Instant::now()));
        }
    }

    /// An image with no expiry section — everything written before this
    /// section existed — still loads, with every key permanent as before.
    #[test]
    fn image_without_an_expiry_section_loads() {
        let db0 = HashMap::from([(Key::from("a"), string("1"))]);
        let loaded = load_snapshot_from(&image(&[db0], &[])[..]).unwrap();
        assert_eq!(loaded.dbs[0].len(), 1);
        assert_eq!(loaded.expiries.len(), 1);
        assert!(loaded.expiries[0].is_empty());
    }

    /// A torn expiry section fails the load, as a torn section of any kind
    /// does (#474): half the TTLs silently missing is worse than a loud error.
    #[test]
    fn a_truncated_expiry_section_fails_the_load() {
        let db0 = HashMap::from([(Key::from("a"), string("1"))]);
        let exp = vec![HashMap::from([(Key::from("a"), unix_ms_now() + 60_000)])];
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
        let first = vec![HashMap::from([(Key::from("a"), string("1"))])];
        write_snapshot(path_s, &first, None).unwrap();
        let second = vec![HashMap::from([
            (Key::from("a"), string("1")),
            (Key::from("b"), string("2")),
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
    use crate::vector::{
        DistanceMetric, FieldOptions, FieldType, IndexOn, IndexSchema, SchemaField, VectorAlgo,
        VectorType,
    };

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
            language: Default::default(),
            default_score: 1.0,
            score_field: None,
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
            language: Default::default(),
            default_score: 1.0,
            score_field: None,
        }
        .into_def();
        store.write().insert("bare".into(), bare);

        let mut image = Vec::new();
        write_kdb_image(
            &mut image,
            &[HashMap::<Key, KiviDBValue>::new()],
            Some(&store),
        )
        .unwrap();
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

    /// `LANGUAGE` and `LANGUAGE_FIELD` come back with the index; an index
    /// in English takes no room.
    #[test]
    fn index_language_survives_a_snapshot() {
        use crate::vector::stem::{Language, LanguageOptions};
        let index = |name: &str, language: LanguageOptions| {
            IndexSchema {
                name: name.into(),
                db_index: 0,
                on: IndexOn::Hash,
                prefixes: vec![],
                fields: vec![SchemaField {
                    name: "t".into(),
                    field_type: FieldType::Text,
                    options: FieldOptions::default(),
                }],
                aliases: Vec::new(),
                stopwords: None,
                synonyms: Default::default(),
                language,
                default_score: 1.0,
                score_field: None,
            }
            .into_def()
        };
        let german = LanguageOptions {
            language: Language::German,
            field: Some("lang".into()),
        };
        let store = crate::vector::new_index_store();
        store
            .write()
            .insert("de".into(), index("de", german.clone()));
        store
            .write()
            .insert("en".into(), index("en", LanguageOptions::default()));
        let mut image = Vec::new();
        write_kdb_image(
            &mut image,
            &[HashMap::<Key, KiviDBValue>::new()],
            Some(&store),
        )
        .unwrap();
        let loaded = load_snapshot_from(image.as_slice()).unwrap();
        let of = |name: &str| {
            loaded
                .index_schemas
                .iter()
                .find(|s| s.name == name)
                .unwrap()
                .language
                .clone()
        };
        assert_eq!(of("de"), german);
        assert_eq!(of("en"), LanguageOptions::default());

        // Without the section (an image of an older release), the index is
        // English.
        let mut bare = Vec::new();
        let empty = crate::vector::new_index_store();
        empty
            .write()
            .insert("de".into(), index("de", LanguageOptions::default()));
        write_kdb_image(
            &mut bare,
            &[HashMap::<Key, KiviDBValue>::new()],
            Some(&empty),
        )
        .unwrap();
        assert!(!bare.windows(8).any(|w| w == KDB_LANGUAGE_MAGIC));
    }

    /// An index with a text field `t`, and `SCORE` and `SCORE_FIELD` as given.
    fn score_index(
        name: &str,
        default_score: f64,
        score_field: Option<&str>,
    ) -> crate::vector::IndexDef {
        IndexSchema {
            name: name.into(),
            db_index: 0,
            on: IndexOn::Hash,
            prefixes: vec![],
            fields: vec![SchemaField {
                name: "t".into(),
                field_type: FieldType::Text,
                options: FieldOptions::default(),
            }],
            aliases: Vec::new(),
            stopwords: None,
            synonyms: Default::default(),
            language: Default::default(),
            default_score,
            score_field: score_field.map(str::to_string),
        }
        .into_def()
    }

    /// The magic of every section an image can carry, and its name.
    const EVERY_SECTION: [(&str, &[u8; 8]); 10] = [
        ("index", KDB_INDEX_MAGIC),
        ("HNSW", KDB_HNSW_MAGIC),
        ("expiry", KDB_EXPIRY_MAGIC),
        ("alias", KDB_ALIAS_MAGIC),
        ("field options", KDB_FIELD_OPTIONS_MAGIC),
        ("synonyms", KDB_SYNONYMS_MAGIC),
        ("vector types", KDB_VECTOR_TYPES_MAGIC),
        ("functions", KDB_FUNCTIONS_MAGIC),
        ("language", KDB_LANGUAGE_MAGIC),
        ("score", KDB_SCORE_MAGIC),
    ];

    /// An image that holds every section an image can carry: one key with a
    /// time to live, indexes with an HNSW field, an alias, field options,
    /// synonyms, a language and a score, and function libraries.
    fn image_with_every_section() -> Vec<u8> {
        let index = score_index;
        let mut synonyms = crate::vector::SynonymGroups::new();
        synonyms.insert("car".into(), vec!["automobile".into()]);
        let scored = IndexSchema {
            name: "scored".into(),
            db_index: 0,
            on: IndexOn::Hash,
            prefixes: vec!["scored:".into()],
            fields: vec![
                SchemaField {
                    name: "t".into(),
                    field_type: FieldType::Text,
                    options: FieldOptions::default(),
                },
                SchemaField {
                    name: "v".into(),
                    field_type: FieldType::Vector {
                        algo: VectorAlgo::Hnsw {
                            m: 16,
                            ef_construction: 64,
                            ef_runtime: 10,
                        },
                        dim: 4,
                        metric: DistanceMetric::L2,
                        data_type: VectorType::Float64,
                    },
                    options: FieldOptions::default(),
                },
            ],
            aliases: Vec::new(),
            stopwords: Some(vec!["the".into()]),
            synonyms,
            language: crate::vector::stem::LanguageOptions {
                language: crate::vector::stem::Language::German,
                field: Some("lang".into()),
            },
            default_score: 0.25,
            score_field: Some("rank".into()),
        }
        .into_def();
        let store = crate::vector::new_index_store();
        {
            let mut store = store.write();
            store.insert("scored".into(), scored.clone());
            store.insert("scored-alias".into(), scored);
            store.insert("plain".into(), index("plain", 1.0, None));
            store.insert("empty".into(), index("empty", 1.0, Some("")));
        }

        let db = crate::store::new_store(1);
        let key = "scored:1";
        crate::store::get_shard(&db[0], key)
            .write()
            .insert_str_expiring(key, b"hello", Some(unix_ms_now() + 3_600_000));
        let mut frozen = freeze_dataset_for_disk(&db, Some(&store)).unwrap();
        // Include the function section even in a build without Lua, so the
        // ordering covers every section an image can carry.
        frozen.functions = Some(b"KVFN payload".to_vec());
        frozen.into_payload().unwrap()
    }

    /// `SCORE` and `SCORE_FIELD` come back with the index; an index that
    /// sets neither takes no room.
    #[test]
    fn index_score_survives_a_snapshot() {
        let index = score_index;
        let image = image_with_every_section();

        let score_at = image
            .windows(8)
            .position(|w| w == KDB_SCORE_MAGIC)
            .expect("score section");
        for &(name, magic) in &EVERY_SECTION[..EVERY_SECTION.len() - 1] {
            let at = image
                .windows(8)
                .position(|w| w == magic)
                .unwrap_or_else(|| panic!("{name} section"));
            assert!(
                at < score_at,
                "{name} section comes after the score section"
            );
        }

        let loaded = load_snapshot_from(image.as_slice()).unwrap();
        let of = |name: &str| {
            loaded
                .index_schemas
                .iter()
                .find(|s| s.name == name)
                .unwrap()
        };
        assert_eq!(of("scored").default_score, 0.25);
        assert_eq!(of("scored").score_field.as_deref(), Some("rank"));
        assert_eq!(of("plain").default_score, 1.0);
        assert_eq!(of("plain").score_field, None);
        assert_eq!(of("empty").default_score, 1.0);
        assert_eq!(of("empty").score_field.as_deref(), Some(""));

        // Without the section (an image of an older release), the defaults.
        let mut bare = Vec::new();
        let empty = crate::vector::new_index_store();
        empty
            .write()
            .insert("plain".into(), index("plain", 1.0, None));
        write_kdb_image(
            &mut bare,
            &[HashMap::<Key, KiviDBValue>::new()],
            Some(&empty),
        )
        .unwrap();
        assert!(!bare.windows(8).any(|w| w == KDB_SCORE_MAGIC));
    }

    /// An image cut anywhere inside a section after its databases, in its
    /// magic or in its body, is refused, loaded or verified, and says it is
    /// cut short (#474). The index, HNSW and alias sections were read
    /// best-effort: cut inside one, the image loaded without its indexes, and
    /// without the TTLs and libraries stored after it.
    #[test]
    fn an_image_cut_inside_any_section_is_refused() {
        let image = image_with_every_section();
        assert!(load_snapshot_bytes(&image).is_ok());
        let mut starts: Vec<(&str, usize)> = EVERY_SECTION
            .iter()
            .map(|(name, magic)| {
                let at = image
                    .windows(8)
                    .position(|w| w == *magic)
                    .unwrap_or_else(|| panic!("no {name} section"));
                (*name, at)
            })
            .collect();
        starts.sort_by_key(|&(_, at)| at);
        for (i, &(name, at)) in starts.iter().enumerate() {
            let end = starts.get(i + 1).map_or(image.len(), |&(_, next)| next);
            let mut cuts = vec![at + 1, at + 4, at + 7, at + 8, at + 9, at + 12];
            cuts.extend([(at + end) / 2, end - 2, end - 1]);
            cuts.retain(|&cut| cut > at && cut < end);
            cuts.dedup();
            for cut in cuts {
                let torn = &image[..cut];
                let err = load_snapshot_bytes(torn)
                    .err()
                    .unwrap_or_else(|| panic!("the {name} section cut at {cut} loaded"));
                assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "{name} at {cut}");
                assert!(
                    err.to_string().contains("cut short"),
                    "{name} at {cut}: {err}"
                );
                assert!(load_snapshot_from(torn).is_err(), "{name} at {cut}");
                assert!(verify_snapshot(torn).is_err(), "{name} at {cut}");
            }
        }
    }

    /// An index section that is all there but cannot be used, an index whose
    /// name is not UTF-8, is refused, as one the image ends inside is: it was
    /// passed over, and the scan ended there, with every TTL and function
    /// library stored after it (#474).
    #[test]
    fn a_whole_but_unusable_index_section_is_refused() {
        let mut whole = Vec::new();
        write_kdb_header(&mut whole, 1).unwrap();
        let value = KiviDBValue::String(SharedBytes::from(&b"1"[..]));
        write_kdb_entry(&mut whole, "a", value.view()).unwrap();
        write_kdb_db_end(&mut whole).unwrap();
        let mut unusable = whole.clone();
        unusable.extend_from_slice(KDB_INDEX_MAGIC);
        unusable.extend(1u32.to_le_bytes());
        unusable.extend(1u32.to_le_bytes());
        unusable.push(0xFF);
        unusable.extend(0u32.to_le_bytes());
        let err = load_snapshot_bytes(&unusable).err().expect("loaded");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("index section cannot be used"),
            "{err}"
        );
        assert!(load_snapshot_from(unusable.as_slice()).is_err());
        assert!(verify_snapshot(&unusable).is_err());

        let mut cut = whole.clone();
        cut.extend_from_slice(KDB_INDEX_MAGIC);
        cut.extend(1u32.to_le_bytes());
        cut.extend(5u32.to_le_bytes());
        cut.extend(b"ab");
        assert!(load_snapshot_bytes(&cut).is_err());
        assert!(verify_snapshot(&cut).is_err());
    }
}

#[cfg(test)]
mod verify_tests {
    use super::*;

    fn image(keys: usize) -> Vec<u8> {
        let mut map: HashMap<Key, KiviDBValue> = HashMap::new();
        for i in 0..keys {
            map.insert(
                Key::from(format!("key:{i}")),
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

        // Loading refuses a cut image as verifying does, the one cut between
        // two entries too: it used to load the keys before the cut (#444).
        for cut in [12, 13, 40, whole.len() / 2] {
            assert!(
                load_snapshot_from(&whole[..cut]).is_err(),
                "an image cut at {cut} of {} loaded",
                whole.len()
            );
        }
        // One key, in the first of two databases: cut after the key, and
        // after the first database's end.
        let one = image(1);
        let entry = 4 + "key:0".len() + 1 + 4 + "value 0".len();
        assert!(load_snapshot_from(&one[..8 + 4 + entry + 8]).is_ok());
        assert!(load_snapshot_from(&one[..8 + 4 + entry]).is_err());
        assert!(load_snapshot_from(&one[..8 + 4 + entry + 4]).is_err());
    }

    /// The table-driven CRC64 that checks RDB files is Redis's.
    #[test]
    fn the_rdb_checksum_is_redis_crc64() {
        assert_eq!(redis_crc64_update(0, b"123456789"), 0xe9c6_d914_c4b8_d9ca);
        let data: Vec<u8> = (0..5000u32).map(|i| (i * 7919 % 251) as u8).collect();
        assert_eq!(redis_crc64_update(0, &data), redis_crc64(&data));
        let (a, b) = data.split_at(1234);
        assert_eq!(
            redis_crc64_update(redis_crc64_update(0, a), b),
            redis_crc64(&data)
        );
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
        write_kdb_expiry_maps(&mut out, &[HashMap::<Key, u64>::new()]).unwrap();
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

#[cfg(test)]
mod nan_score_tests {
    use super::*;

    fn zset(scores: &[(&str, f64)]) -> KiviDBValue {
        let mut ss = SortedSetInner::new();
        for (member, score) in scores {
            ss.insert(Arc::from(member.as_bytes()), *score);
        }
        KiviDBValue::SortedSet(Arc::new(parking_lot::RwLock::new(ss)))
    }

    fn image(key: &str, value: &KiviDBValue) -> Vec<u8> {
        let mut out = Vec::new();
        write_kdb_header(&mut out, 1).unwrap();
        write_kdb_entry(&mut out, key, value.view()).unwrap();
        write_kdb_db_end(&mut out).unwrap();
        out
    }

    /// `data` sealed as Redis seals a `DUMP` payload, from RDB version 11.
    fn redis_payload(data: &[u8]) -> Vec<u8> {
        let mut out = data.to_vec();
        out.extend_from_slice(&11u16.to_le_bytes());
        let crc = redis_crc64(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    /// A snapshot holding a NaN score does not load, and says which key
    /// holds it, as Redis refuses its own (#414). Nor does it pass as a
    /// full-sync payload. Infinite scores load.
    #[test]
    fn a_snapshot_with_a_nan_score_does_not_load() {
        let bad = image("scores", &zset(&[("a", 1.0), ("b", f64::NAN)]));
        let Err(e) = load_snapshot_from(&bad[..]) else {
            panic!("a NaN score loaded");
        };
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            e.to_string(),
            "Zset with NAN score detected in key 'scores'"
        );
        assert!(verify_snapshot(&bad[..]).is_err());

        let good = image(
            "scores",
            &zset(&[("a", f64::NEG_INFINITY), ("b", f64::INFINITY), ("c", -0.0)]),
        );
        let loaded = load_snapshot_from(&good[..]).expect("infinite scores load");
        match loaded.dbs[0].get(&b"scores"[..]) {
            Some(KiviDBValue::SortedSet(ss)) => assert_eq!(ss.read().len(), 3),
            _ => panic!("the sorted set is not there"),
        }
        assert_eq!(verify_snapshot(&good[..]).unwrap(), 1);
    }

    /// `RESTORE` of a payload with a NaN score is `ERR Bad data format`, in
    /// KiviDB's form and in Redis's (`ZSET`, `ZSET_2`), as in Redis.
    #[test]
    fn a_dump_payload_with_a_nan_score_is_bad_data() {
        let nan = dump_payload(&zset(&[("a", 1.0), ("b", f64::NAN)])).unwrap();
        assert_eq!(
            restore_payload_checked(&nan).err(),
            Some(RestoreError::BadData)
        );
        let inf = dump_payload(&zset(&[("a", f64::INFINITY), ("b", f64::NEG_INFINITY)])).unwrap();
        match restore_payload_checked(&inf) {
            Ok(KiviDBValue::SortedSet(ss)) => assert_eq!(ss.read().len(), 2),
            _ => panic!("infinite scores restore"),
        }

        // RDB_TYPE_ZSET_2: one member, `m`, and its score as a binary double.
        let zset_2 = |score: f64| {
            let mut data = vec![5u8, 1, 1, b'm'];
            data.extend_from_slice(&score.to_le_bytes());
            redis_payload(&data)
        };
        assert!(restore_payload_checked(&zset_2(1.5)).is_ok());
        assert!(restore_payload_checked(&zset_2(f64::INFINITY)).is_ok());
        assert_eq!(
            restore_payload_checked(&zset_2(f64::NAN)).err(),
            Some(RestoreError::BadData)
        );
        // RDB_TYPE_ZSET: the score as text, 253 standing for NaN and 254 for
        // +inf.
        assert!(restore_payload_checked(&redis_payload(&[3, 1, 1, b'm', 254])).is_ok());
        assert!(
            restore_payload_checked(&redis_payload(&[3, 1, 1, b'm', 3, b'1', b'.', b'5'])).is_ok()
        );
        assert_eq!(
            restore_payload_checked(&redis_payload(&[3, 1, 1, b'm', 253])).err(),
            Some(RestoreError::BadData)
        );
    }
}

/// The empty key in an image (#441).
#[cfg(test)]
mod empty_key_tests {
    use super::*;

    fn string(v: &str) -> KiviDBValue {
        KiviDBValue::String(SharedBytes::from(v.as_bytes()))
    }

    fn image(maps: &[HashMap<String, KiviDBValue>]) -> Vec<u8> {
        let mut out = Vec::new();
        write_kdb_image(&mut out, maps, None).unwrap();
        out
    }

    fn get_string(map: &HashMap<Key, KiviDBValue>, key: &str) -> Vec<u8> {
        match map.get(key.as_bytes()) {
            Some(KiviDBValue::String(bytes)) => bytes.as_ref().to_vec(),
            _ => panic!("{key:?} is not a string"),
        }
    }

    /// It was written with the length that ends a database: the rest of
    /// that database, and every database after it, could not be read.
    #[test]
    fn the_empty_key_is_read_back_in_every_database() {
        let db0 = HashMap::from([(String::new(), string("x")), ("a".to_string(), string("1"))]);
        let db1 = HashMap::from([("b".to_string(), string("2")), (String::new(), string("y"))]);
        let whole = image(&[db0, db1]);
        assert_eq!(verify_snapshot(whole.as_slice()).unwrap(), 4);
        let loaded = load_snapshot_from(whole.as_slice()).unwrap();
        assert_eq!(get_string(&loaded.dbs[0], ""), b"x");
        assert_eq!(get_string(&loaded.dbs[0], "a"), b"1");
        assert_eq!(get_string(&loaded.dbs[1], ""), b"y");
        assert_eq!(get_string(&loaded.dbs[1], "b"), b"2");
    }

    /// The empty key is the stand-in length and no bytes; any other key, an
    /// empty value and empty members are written as they always were.
    #[test]
    fn only_the_empty_key_is_written_differently() {
        let mut entry = Vec::new();
        write_kdb_entry(&mut entry, "", string("x").view()).unwrap();
        let mut expected = u32::MAX.to_le_bytes().to_vec();
        expected.push(TAG_STRING);
        expected.extend(1u32.to_le_bytes());
        expected.push(b'x');
        assert_eq!(entry, expected);

        let mut entry = Vec::new();
        write_kdb_entry(&mut entry, "k", string("").view()).unwrap();
        let mut expected = 1u32.to_le_bytes().to_vec();
        expected.push(b'k');
        expected.push(TAG_STRING);
        expected.extend(0u32.to_le_bytes());
        assert_eq!(entry, expected);
    }

    /// An empty hash field, set member, sorted set member and list element
    /// have a count before them, not a marker after them: they were never
    /// read as an end. Under the empty key, too.
    #[test]
    fn empty_fields_and_members_round_trip() {
        let mut fields = crate::types::HashFields::default();
        fields.insert(String::new(), Arc::from(&b"v"[..]));
        fields.insert("f".to_string(), Arc::from(&b""[..]));
        let mut members = crate::member_set::MemberSet::new();
        members.insert(Arc::from(&b""[..]));
        members.insert(Arc::from(&b"m"[..]));
        let mut zset = SortedSetInner::new();
        zset.insert(Arc::from(&b""[..]), 1.5);
        zset.insert(Arc::from(&b"z"[..]), 2.0);
        let db0 = HashMap::from([
            (
                String::new(),
                KiviDBValue::Hash(Arc::new(parking_lot::RwLock::new(fields))),
            ),
            (
                "set".to_string(),
                KiviDBValue::Set(Arc::new(parking_lot::RwLock::new(members))),
            ),
            (
                "zset".to_string(),
                KiviDBValue::SortedSet(Arc::new(parking_lot::RwLock::new(zset))),
            ),
            (
                "list".to_string(),
                KiviDBValue::List(Arc::new(parking_lot::RwLock::new(VecDeque::from([
                    Arc::from(&b""[..]),
                    Arc::from(&b"e"[..]),
                ])))),
            ),
        ]);
        let whole = image(&[db0, HashMap::from([("after".to_string(), string("1"))])]);
        assert_eq!(verify_snapshot(whole.as_slice()).unwrap(), 5);
        let loaded = load_snapshot_from(whole.as_slice()).unwrap();
        match loaded.dbs[0].get(&b""[..]) {
            Some(KiviDBValue::Hash(hash)) => {
                let hash = hash.read();
                assert_eq!(hash.len(), 2);
                assert_eq!(hash.get("").map(|v| &v[..]), Some(&b"v"[..]));
                assert_eq!(hash.get("f").map(|v| &v[..]), Some(&b""[..]));
            }
            _ => panic!("the empty key is not a hash"),
        }
        match loaded.dbs[0].get(&b"set"[..]) {
            Some(KiviDBValue::Set(set)) => {
                let set = set.read();
                assert_eq!(set.len(), 2);
                assert!(set.iter().any(|m| m.is_empty()));
            }
            _ => panic!("set is not a set"),
        }
        match loaded.dbs[0].get(&b"zset"[..]) {
            Some(KiviDBValue::SortedSet(zset)) => {
                let zset = zset.read();
                assert_eq!(zset.members.len(), 2);
                assert!(
                    zset.by_score
                        .keys()
                        .any(|(score, m)| m.is_empty() && score.0 == 1.5)
                );
            }
            _ => panic!("zset is not a sorted set"),
        }
        match loaded.dbs[0].get(&b"list"[..]) {
            Some(KiviDBValue::List(list)) => {
                let list = list.read();
                assert_eq!(list.len(), 2);
                assert!(list[0].is_empty());
            }
            _ => panic!("list is not a list"),
        }
        assert_eq!(get_string(&loaded.dbs[1], "after"), b"1");
    }

    /// A reader before 1.2.1 reads the stand-in as the length of a key and
    /// asks for that many bytes (`read_exactly`, as 1.2.0 has it): the image
    /// runs out first, and the load fails, with nothing taken for a key.
    #[test]
    fn a_reader_before_the_marker_runs_out_of_image_at_it() {
        let whole = image(&[HashMap::from([(String::new(), string("x"))])]);
        let at = KDB_MAGIC.len() + 4;
        assert_eq!(&whole[at..at + 4], &u32::MAX.to_le_bytes());
        let mut rest = &whole[at + 4..];
        let err = read_exactly(&mut rest, u32::MAX as usize).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    /// An image as 1.2.0 wrote it: the empty key with the length that ends a
    /// database, then its value. Each database's entries in the order given.
    fn image_1_2_0(dbs: &[Vec<(&str, KiviDBValue)>], expiries: &[HashMap<String, u64>]) -> Vec<u8> {
        let mut out = Vec::new();
        write_kdb_header(&mut out, dbs.len() as u32).unwrap();
        for db in dbs {
            for (key, value) in db {
                if key.is_empty() {
                    out.extend(0u32.to_le_bytes());
                    write_kdb_value(&mut out, value.view()).unwrap();
                } else {
                    write_kdb_entry(&mut out, key, value.view()).unwrap();
                }
            }
            write_kdb_db_end(&mut out).unwrap();
        }
        write_kdb_expiry_maps(&mut out, expiries).unwrap();
        write_kdb_functions_section(&mut out, Some(b"libraries")).unwrap();
        out
    }

    fn hash(field: &str, value: &str) -> KiviDBValue {
        let mut fields = crate::types::HashFields::default();
        fields.insert(field.to_string(), Arc::from(value.as_bytes()));
        KiviDBValue::Hash(Arc::new(parking_lot::RwLock::new(fields)))
    }

    /// What the server writes (a capture of the keyspace, for `SAVE`, the
    /// AOF's checkpoint and a full sync) has the empty key under the
    /// stand-in, both a string's (written apart from the other values) and
    /// another value's. The reader also takes 1.2.0's layout, so a load alone
    /// would not tell whether the writer went back to it.
    #[test]
    fn a_capture_writes_the_empty_key_under_the_stand_in() {
        let far = unix_ms_now() + 3_600_000;
        let store = crate::store::new_store(2);
        restore_into_store(
            vec![
                HashMap::from([(Key::default(), string("x"))]),
                HashMap::from([(Key::default(), hash("f", "v"))]),
            ],
            &store,
        );
        restore_expiries_into_store(
            vec![
                HashMap::from([(Key::default(), far)]),
                HashMap::from([(Key::default(), far + 1)]),
            ],
            &store,
        );
        let image = freeze_dataset_for_disk(&store, None)
            .unwrap()
            .into_payload()
            .unwrap();
        // The header, then database 0's one entry: the stand-in and no key
        // bytes, a string of one byte, the database's end.
        let header = KDB_MAGIC.len() + 4;
        assert_eq!(&image[header..header + 4], &u32::MAX.to_le_bytes());
        assert_eq!(image[header + 4], TAG_STRING);
        let db1 = header + 4 + 1 + 4 + 1 + 4;
        assert_eq!(&image[db1 - 4..db1], &0u32.to_le_bytes());
        assert_eq!(&image[db1..db1 + 4], &u32::MAX.to_le_bytes());
        assert_eq!(image[db1 + 4], TAG_HASH);

        let loaded = load_snapshot_from(image.as_slice()).unwrap();
        assert_eq!(get_string(&loaded.dbs[0], ""), b"x");
        assert!(matches!(
            loaded.dbs[1].get(&b""[..]),
            Some(KiviDBValue::Hash(_))
        ));
        assert_eq!(loaded.expiries[0].get(&b""[..]), Some(&far));
        assert_eq!(loaded.expiries[1].get(&b""[..]), Some(&(far + 1)));
    }

    /// In the last database (with `databases 1`, the only one) a 1.2.0 empty
    /// key was read as the database's end, and its tag as the start of a
    /// section that was not one: the image loaded without the rest of the
    /// database, the times to live and the function libraries.
    #[test]
    fn a_1_2_0_image_with_the_empty_key_in_its_last_database_loads_whole() {
        let far = unix_ms_now() + 3_600_000;
        let db = vec![("a", string("1")), ("", hash("f", "v")), ("b", string("2"))];
        let exp = vec![HashMap::from([
            ("a".to_string(), far),
            (String::new(), far + 1),
            ("b".to_string(), far + 2),
        ])];
        let old = image_1_2_0(&[db], &exp);
        assert_eq!(verify_snapshot(&old).unwrap(), 3);
        // Read once, as the AOF's and a full sync's readers did: the last
        // database needs no second reading.
        for loaded in [
            load_snapshot_from(old.as_slice()).unwrap(),
            load_snapshot_bytes(&old).unwrap(),
        ] {
            assert_eq!(loaded.dbs[0].len(), 3);
            assert_eq!(get_string(&loaded.dbs[0], "a"), b"1");
            assert_eq!(get_string(&loaded.dbs[0], "b"), b"2");
            assert!(matches!(
                loaded.dbs[0].get(&b""[..]),
                Some(KiviDBValue::Hash(_))
            ));
            assert_eq!(loaded.expiries[0].get(&b"a"[..]), Some(&far));
            assert_eq!(loaded.expiries[0].get(&b""[..]), Some(&(far + 1)));
            assert_eq!(loaded.expiries[0].get(&b"b"[..]), Some(&(far + 2)));
            assert_eq!(loaded.functions.as_deref(), Some(&b"libraries"[..]));
        }
    }

    /// Before the last database, the byte after a 1.2.0 empty key's 0 could
    /// as well begin the next database: such an image is read again with
    /// the empty key there. Here in the first database (as `SET "" x` with
    /// the default 16 databases) and the last, with a database between whose
    /// first key is one byte long, so its length's first byte is a tag too.
    #[test]
    fn a_1_2_0_image_with_the_empty_key_before_its_last_database_loads_whole() {
        let far = unix_ms_now() + 3_600_000;
        let dbs = vec![
            vec![("x", string("1")), ("", string("e0"))],
            vec![("k", string("v"))],
            vec![("", string("e2")), ("z", string("9"))],
        ];
        let exp = vec![
            HashMap::from([(String::new(), far)]),
            HashMap::new(),
            HashMap::from([("z".to_string(), far + 1)]),
        ];
        let old = image_1_2_0(&dbs, &exp);
        assert!(load_snapshot_from(old.as_slice()).is_err());
        assert_eq!(verify_snapshot(&old).unwrap(), 5);
        let loaded = load_snapshot_bytes(&old).unwrap();
        assert_eq!(get_string(&loaded.dbs[0], ""), b"e0");
        assert_eq!(get_string(&loaded.dbs[0], "x"), b"1");
        assert_eq!(get_string(&loaded.dbs[1], "k"), b"v");
        assert_eq!(get_string(&loaded.dbs[2], ""), b"e2");
        assert_eq!(get_string(&loaded.dbs[2], "z"), b"9");
        assert_eq!(loaded.expiries[0].get(&b""[..]), Some(&far));
        assert_eq!(loaded.expiries[2].get(&b"z"[..]), Some(&(far + 1)));
        assert_eq!(loaded.functions.as_deref(), Some(&b"libraries"[..]));

        // From a file, as at startup.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dump.kdb");
        std::fs::write(&path, &old).unwrap();
        let loaded = load_snapshot(path.to_str().unwrap()).unwrap();
        assert_eq!(get_string(&loaded.dbs[0], ""), b"e0");
        assert_eq!(get_string(&loaded.dbs[2], ""), b"e2");
    }

    /// A 1.2.0 image, read with `databases 1`: the empty key in database 0,
    /// before the last, and database 1 empty. The first reading takes the
    /// empty key's 0 for database 0's end, and what follows realigns as a
    /// key in database 1 (`"\0"`, from the empty string's length and the
    /// next key's length and byte) before it fails. That key used to refuse
    /// the image at once, and the right reading was never tried (#482).
    #[test]
    fn a_wrong_reading_of_a_1_2_0_image_past_the_databases_is_not_the_refusal() {
        let old = image_1_2_0(&[vec![("", string("")), ("\0", string("v"))], vec![]], &[]);
        // The first reading alone: past database 0, then a failure on what
        // it misread, not the refusal.
        let first = read_image(old.as_slice(), true, &[], &mut Vec::new(), 1)
            .err()
            .expect("the first reading loaded");
        assert!(!is_past_databases(&first), "{first}");
        let (loaded, _, _) = read_retrying(|| Ok(old.as_slice()), true, 1).unwrap();
        assert_eq!(get_string(&loaded.dbs[0], ""), b"");
        assert_eq!(get_string(&loaded.dbs[0], "\0"), b"v");
        assert!(loaded.dbs[1].is_empty());
    }

    /// A 1.2.0 image whose right reading is the second, with a key in a
    /// database past the server's: refused for that, not with the first
    /// reading's error.
    #[test]
    fn a_later_reading_refused_for_its_databases_says_so() {
        let old = image_1_2_0(
            &[
                vec![("x", string("1")), ("", string("e0"))],
                vec![("k", string("v"))],
            ],
            &[],
        );
        assert!(read_retrying(|| Ok(old.as_slice()), true, 2).is_ok());
        let err = read_retrying(|| Ok(old.as_slice()), true, 1)
            .err()
            .expect("loaded into one database");
        assert!(is_past_databases(&err), "{err}");
        assert!(err.to_string().contains("keys in database 1"), "{err}");
        let err = read_retrying(|| Ok(old.as_slice()), false, 1)
            .err()
            .expect("verified for one database");
        assert!(is_past_databases(&err), "{err}");
    }

    /// An image of 32 databases with keys in database 0 alone loads into 16,
    /// as every `dump.kdb` of a server with `databases 32` does; one with a
    /// key in database 20, or an index there, is refused, naming it.
    #[test]
    fn only_keys_or_an_index_past_the_databases_refuse_an_image() {
        let mut dbs: Vec<HashMap<String, KiviDBValue>> = (0..32).map(|_| HashMap::new()).collect();
        dbs[0].insert("a".into(), string("1"));
        let mut out = Vec::new();
        write_kdb_image(&mut out, &dbs, None).unwrap();
        let (loaded, keys, _) = read_retrying(|| Ok(out.as_slice()), true, 16).unwrap();
        assert_eq!((keys, get_string(&loaded.dbs[0], "a")), (1, b"1".to_vec()));

        dbs[20].insert("b".into(), string("2"));
        let mut out = Vec::new();
        write_kdb_image(&mut out, &dbs, None).unwrap();
        let err = read_retrying(|| Ok(out.as_slice()), true, 16)
            .err()
            .unwrap();
        assert!(err.to_string().contains("keys in database 20"), "{err}");
        assert!(err.to_string().contains("databases 21"), "{err}");
        assert!(read_retrying(|| Ok(out.as_slice()), true, 21).is_ok());

        dbs[20].clear();
        let schema = crate::vector::IndexSchema {
            name: "idx20".into(),
            db_index: 20,
            on: crate::vector::IndexOn::Hash,
            prefixes: vec!["p:".into()],
            fields: Vec::new(),
            aliases: Vec::new(),
            stopwords: None,
            synonyms: Default::default(),
            language: Default::default(),
            default_score: 1.0,
            score_field: None,
        };
        let store = crate::vector::new_index_store();
        store.write().insert("idx20".into(), schema.into_def());
        let mut out = Vec::new();
        write_kdb_image(&mut out, &dbs, Some(&store)).unwrap();
        let err = read_retrying(|| Ok(out.as_slice()), true, 16)
            .err()
            .unwrap();
        assert!(is_past_databases(&err), "{err}");
        assert!(
            err.to_string().contains("the index idx20 in database 20"),
            "{err}"
        );
        let (loaded, _, _) = read_retrying(|| Ok(out.as_slice()), true, 32).unwrap();
        assert_eq!(loaded.index_schemas[0].db_index, 20);
    }

    /// `image` opened by `load_snapshot_with`, counting the openings.
    fn readings(image: &[u8]) -> (io::Result<LoadedSnapshot>, usize) {
        let mut opened = 0;
        let loaded = load_snapshot_with(|| {
            opened += 1;
            Ok(image)
        })
        .map(|(loaded, _)| loaded);
        (loaded, opened)
    }

    /// `dbs` databases each holding `key`, then a value of no known type: an
    /// image that no reading loads.
    fn damaged(dbs: u32, key: &str) -> Vec<u8> {
        let mut out = Vec::new();
        write_kdb_header(&mut out, dbs).unwrap();
        for _ in 0..dbs {
            write_kdb_entry(&mut out, key, string("v").view()).unwrap();
            write_kdb_db_end(&mut out).unwrap();
        }
        out.truncate(out.len() - 4 - 1 - 4 - 1);
        out.push(0xEE);
        out
    }

    /// An end is read again as the empty key only where the next database's
    /// first byte could be the empty key's tag.
    #[test]
    fn an_image_is_read_again_only_where_1_2_0_could_have_put_the_empty_key() {
        // Keys of 16 bytes or more: no database's first byte is a tag.
        let (loaded, opened) = readings(&damaged(16, "a-key-long-enough-for-that"));
        assert!(loaded.is_err());
        assert_eq!(opened, 1);
        // One-byte keys: every end could be one.
        let (loaded, opened) = readings(&damaged(16, "k"));
        assert!(loaded.is_err());
        assert!((2..=READINGS).contains(&opened), "{opened} readings");
        // However many databases, no more than READINGS readings, nor plans.
        let (loaded, opened) = readings(&damaged(100_000, "k"));
        assert!(loaded.is_err());
        assert!(opened <= READINGS, "{opened} readings");
    }

    /// An image cut short inside a section is a fault of its layout, so it
    /// is read again where 1.2.0 could have put the empty key, no more than
    /// [`READINGS`] times, and refused with the first reading's error, which
    /// says where it is cut (#474).
    #[test]
    fn an_image_cut_inside_a_section_is_read_again_a_bounded_number_of_times() {
        for dbs in [16, 100_000] {
            let mut image = Vec::new();
            write_kdb_header(&mut image, dbs).unwrap();
            for _ in 0..dbs {
                // One-byte keys: every end could be 1.2.0's empty key.
                write_kdb_entry(&mut image, "k", string("v").view()).unwrap();
                write_kdb_db_end(&mut image).unwrap();
            }
            let whole_len = image.len();
            for (section, magic) in [
                ("index", KDB_INDEX_MAGIC),
                ("HNSW graph", KDB_HNSW_MAGIC),
                ("index alias", KDB_ALIAS_MAGIC),
                ("expiry", KDB_EXPIRY_MAGIC),
            ] {
                image.truncate(whole_len);
                image.extend_from_slice(magic);
                image.extend(1u32.to_le_bytes());
                image.extend(5u32.to_le_bytes());
                image.extend(b"ab");
                let (loaded, opened) = readings(&image);
                let err = loaded.err().unwrap_or_else(|| panic!("{section} loaded"));
                assert!(
                    (2..=READINGS).contains(&opened),
                    "{section}: {opened} readings"
                );
                assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "{section}");
                assert!(
                    err.to_string().contains(&format!("its {section} section")),
                    "{section}: {err}"
                );
                assert!(verify_snapshot(&image).is_err(), "{section}");
            }
        }
    }

    /// A reading the disk failed is not read again; nor is an image that
    /// cannot be opened again, which fails with what the first reading
    /// found.
    #[test]
    fn a_failure_of_the_disk_is_not_read_again() {
        struct Failing<'a>(&'a [u8]);
        impl Read for Failing<'_> {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if self.0.len() < 20 {
                    return Err(io::Error::other("the disk failed"));
                }
                self.0.read(buf)
            }
        }
        let image = damaged(16, "k");
        let mut opened = 0;
        let err = load_snapshot_with(|| {
            opened += 1;
            Ok(Failing(&image))
        })
        .err()
        .unwrap();
        assert_eq!(opened, 1);
        assert_eq!(err.to_string(), "the disk failed");

        let mut opened = 0;
        let err = load_snapshot_with(|| {
            opened += 1;
            if opened > 1 {
                return Err(io::Error::other("cannot open it again"));
            }
            Ok(image.as_slice())
        })
        .err()
        .unwrap();
        assert_eq!(opened, 2);
        assert_ne!(err.to_string(), "cannot open it again");
    }

    /// A reading that takes 1.2.0's empty key does not pass over a section
    /// it does not know: 1.2.0 wrote none.
    #[test]
    fn an_image_read_with_1_2_0s_empty_key_holds_no_section_1_2_0_did_not_know() {
        let before_last = vec![vec![("", string("x"))], vec![("k", string("v"))]];
        let last = vec![vec![("k", string("v")), ("", string("x"))]];
        for dbs in [before_last, last] {
            let old = image_1_2_0(&dbs, &[]);
            assert!(load_snapshot_bytes(&old).is_ok());
            let mut newer = old.clone();
            newer.extend_from_slice(b"KIVIZZ9\0and what it holds");
            assert!(load_snapshot_bytes(&newer).is_err());
            assert!(verify_snapshot(&newer).is_err());
            let mut damaged = old.clone();
            damaged.extend_from_slice(KDB_INDEX_MAGIC);
            damaged.extend_from_slice(&[0xFF; 16]);
            assert!(load_snapshot_bytes(&damaged).is_err());
        }
    }

    /// What follows the last database's end is the end of the image or a
    /// section. Anything else is refused, not passed over with the image
    /// taken for whole.
    #[test]
    fn bytes_after_the_last_database_that_are_not_a_section_are_refused() {
        let mut whole = Vec::new();
        write_kdb_header(&mut whole, 1).unwrap();
        write_kdb_entry(&mut whole, "a", string("1").view()).unwrap();
        write_kdb_db_end(&mut whole).unwrap();
        let tails: [&[u8]; 6] = [
            b"garbage!",
            b"garbage, more of it",
            b"gar",
            b"KIVI",
            &[1, 0xFF, 0xFF, 0xFF, 0x7F],
            &[3, 9],
        ];
        for tail in tails {
            let mut bad = whole.clone();
            bad.extend_from_slice(tail);
            assert!(load_snapshot_bytes(&bad).is_err(), "{tail:?} loaded");
            assert!(
                load_snapshot_from(bad.as_slice()).is_err(),
                "{tail:?} loaded"
            );
            assert!(verify_snapshot(&bad).is_err(), "{tail:?} verified");
        }
        // A section this build does not know is still passed over.
        let mut newer = whole.clone();
        newer.extend_from_slice(b"KIVIZZ9\0and what it holds");
        assert_eq!(load_snapshot_bytes(&newer).unwrap().dbs[0].len(), 1);
    }

    /// Lengths that are not the stand-in are read as lengths still: one that
    /// says more than the image holds, or the stand-in with the image cut
    /// after it or a value of no type after it, is refused.
    #[test]
    fn a_garbage_or_truncated_key_length_is_still_refused() {
        let mut header = Vec::new();
        write_kdb_header(&mut header, 1).unwrap();

        let mut long = header.clone();
        long.extend((u32::MAX - 1).to_le_bytes());
        long.extend(b"short");
        assert!(load_snapshot_from(long.as_slice()).is_err());
        assert!(verify_snapshot(long.as_slice()).is_err());

        let mut cut = header.clone();
        cut.extend(u32::MAX.to_le_bytes());
        assert!(load_snapshot_from(cut.as_slice()).is_err());
        assert!(verify_snapshot(cut.as_slice()).is_err());

        let mut untyped = cut.clone();
        untyped.push(0xEE);
        untyped.extend(0u32.to_le_bytes());
        assert!(load_snapshot_from(untyped.as_slice()).is_err());
        assert!(verify_snapshot(untyped.as_slice()).is_err());

        // Cut anywhere between the marker and the database's end.
        let whole = image(&[HashMap::from([(String::new(), string("x"))])]);
        let db_end = header.len() + 4 + 1 + 4 + 1 + 4;
        assert_eq!(&whole[db_end - 4..db_end], &0u32.to_le_bytes());
        for end in header.len() + 1..db_end {
            assert!(
                verify_snapshot(&whole[..end]).is_err(),
                "an image cut at {end} of {} verified",
                whole.len()
            );
        }
    }
}
