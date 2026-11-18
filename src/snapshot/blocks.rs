//! Compressed snapshots (#879): a KDB image, unchanged byte for byte, cut
//! into blocks that are compressed one by one with zstd.
//!
//! What a block holds is the image as `KIVIDB01` lays it out, so what is
//! compressed is exactly what a plain snapshot holds, and nothing that hashes
//! or ships an image's entries (`DUMP`, the dataset digest) changes.
//!
//! The layout, every integer little-endian:
//!
//! ```text
//! header  "KIVIDBZ1"  8 bytes: KiviDB 1.2 and earlier refuse it ("bad magic")
//!         u8          format version (1)
//!         u8          codec (1: zstd)
//!         u16         0
//!         u32         block size: no block holds more of the image than this
//!         u32         the low 32 bits of the xxh3-64 of the 16 bytes before it
//! block   u8          kind: 1 stored as it is, 2 zstd
//!         u32         bytes of the image it holds (1 to the block size)
//!         u32         bytes stored
//!         u64         xxh3-64 of the bytes of the image it holds
//!         the bytes stored
//! end     u8          'E'
//!         u64         blocks
//!         u64         bytes of the image in all
//!         u64         xxh3-64 of the 16 bytes before it
//! ```
//!
//! and nothing after the end. A container cut short anywhere, without its
//! end, with a block whose checksum or length does not match, or with bytes
//! after its end, is refused as a whole, naming the block: never loaded in
//! part. A block is never decompressed to more than it says it holds, and it
//! cannot say it holds more than the block size, which is at most
//! [`MAX_BLOCK_SIZE`].
//!
//! `rdbcompression yes` (the default) writes snapshots this way, `no` writes
//! the plain image. Both are read, told apart by the magic.

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};

use xxhash_rust::xxh3::xxh3_64;

/// What a compressed snapshot opens with.
pub const MAGIC: &[u8; 8] = b"KIVIDBZ1";
/// The one format version there is.
const VERSION: u8 = 1;
/// The one codec there is.
const CODEC_ZSTD: u8 = 1;
/// The zstd level: the fastest, which the study for #879 measured as
/// compressing about as well as level 3 on snapshots, at less CPU.
const ZSTD_LEVEL: i32 = 1;
/// The bytes of the image in each block the writer writes.
pub const BLOCK_SIZE: usize = 256 << 10;
/// The largest block size a reader accepts: a header cannot make it take
/// more memory than this for a block.
pub const MAX_BLOCK_SIZE: usize = 16 << 20;
const HEADER_LEN: usize = 20;
const KIND_STORED: u8 = 1;
const KIND_ZSTD: u8 = 2;
const KIND_END: u8 = b'E';
const BLOCK_HEADER_LEN: usize = 1 + 4 + 4 + 8;
const END_LEN: usize = 1 + 8 + 8 + 8;
/// The most threads that compress a snapshot's blocks beside the thread
/// that lays the image out.
const MAX_HELPERS: usize = 2;

/// `rdbcompression`: whether snapshots are written compressed.
static COMPRESS: AtomicBool = AtomicBool::new(true);

/// Whether snapshots are written compressed (`rdbcompression`).
pub fn enabled() -> bool {
    COMPRESS.load(Ordering::Relaxed)
}

/// Set `rdbcompression`.
pub fn set_enabled(on: bool) {
    COMPRESS.store(on, Ordering::Relaxed);
}

/// The last snapshot written to disk: the bytes of its image, and the
/// bytes written (the same for a plain one). `INFO persistence`.
static LAST_RAW: AtomicU64 = AtomicU64::new(0);
static LAST_STORED: AtomicU64 = AtomicU64::new(0);

/// Note the sizes of a snapshot just written to disk.
pub fn record_saved(raw: u64, stored: u64) {
    LAST_RAW.store(raw, Ordering::Relaxed);
    LAST_STORED.store(stored, Ordering::Relaxed);
}

/// The sizes [`record_saved`] last noted: (image, written). 0 and 0 before
/// the first save.
pub fn last_saved() -> (u64, u64) {
    (
        LAST_RAW.load(Ordering::Relaxed),
        LAST_STORED.load(Ordering::Relaxed),
    )
}

/// Whether `bytes` opens as a KDB image this build reads: plain or
/// compressed.
pub fn is_kdb(bytes: &[u8]) -> bool {
    bytes.starts_with(super::KDB_MAGIC) || bytes.starts_with(MAGIC)
}

/// Whether `bytes` opens as a compressed snapshot.
pub fn is_compressed(bytes: &[u8]) -> bool {
    bytes.starts_with(MAGIC)
}

/// The refusal of a compressed snapshot, or of the image in it. It is never
/// read again as a 1.2.0 image could be (see `read_retrying`): no 1.2.0
/// wrote one.
#[derive(Debug)]
struct Refused(String);

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Refused {}

/// `e`, from reading a compressed snapshot, as its refusal (see
/// [`is_refusal`]), its kind kept. The refusal of a key in a database past
/// the server's stays as it is: what refuses it reads it as that (#482).
pub(super) fn refusal(e: io::Error) -> io::Error {
    if super::is_past_databases(&e) || is_refusal(&e) {
        return e;
    }
    io::Error::new(e.kind(), Refused(e.to_string()))
}

/// Whether `e` is the refusal of a compressed snapshot.
pub fn is_refusal(e: &io::Error) -> bool {
    e.get_ref().is_some_and(|inner| inner.is::<Refused>())
}

fn invalid(what: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, Refused(what))
}

fn header_bytes(block_size: usize) -> [u8; HEADER_LEN] {
    let mut h = [0u8; HEADER_LEN];
    h[..8].copy_from_slice(MAGIC);
    h[8] = VERSION;
    h[9] = CODEC_ZSTD;
    h[12..16].copy_from_slice(&(block_size as u32).to_le_bytes());
    let sum = xxh3_64(&h[..16]) as u32;
    h[16..20].copy_from_slice(&sum.to_le_bytes());
    h
}

fn end_bytes(blocks: u64, raw: u64) -> [u8; END_LEN] {
    let mut e = [0u8; END_LEN];
    e[0] = KIND_END;
    e[1..9].copy_from_slice(&blocks.to_le_bytes());
    e[9..17].copy_from_slice(&raw.to_le_bytes());
    let sum = xxh3_64(&e[1..17]);
    e[17..25].copy_from_slice(&sum.to_le_bytes());
    e
}

/// The threads that compress a snapshot's blocks: up to [`MAX_HELPERS`],
/// and none on a machine of one CPU, where the thread that lays the image
/// out compresses them itself.
fn helper_count() -> usize {
    std::thread::available_parallelism()
        .map_or(1, |n| n.get())
        .saturating_sub(1)
        .min(MAX_HELPERS)
}

// ── Writing ─────────────────────────────────────────────────────────────────

/// A block, compressed or found not to shrink.
struct Packed {
    raw: Vec<u8>,
    /// The compressed bytes; unused when `compressed` is not set.
    data: Vec<u8>,
    compressed: bool,
    sum: u64,
}

/// Compress `raw` into `data`. A block that does not shrink, or that
/// cannot be compressed (no compressor), is stored as it is.
fn pack(
    cctx: Option<&mut zstd::bulk::Compressor<'static>>,
    raw: Vec<u8>,
    mut data: Vec<u8>,
) -> Packed {
    data.clear();
    let compressed = match cctx {
        Some(cctx) => {
            data.reserve(zstd::zstd_safe::compress_bound(raw.len()));
            matches!(cctx.compress_to_buffer(&raw, &mut data), Ok(n) if n < raw.len())
        }
        None => false,
    };
    let sum = xxh3_64(&raw);
    Packed {
        raw,
        data,
        compressed,
        sum,
    }
}

fn compressor() -> Option<zstd::bulk::Compressor<'static>> {
    zstd::bulk::Compressor::new(ZSTD_LEVEL).ok()
}

struct Job {
    raw: Vec<u8>,
    data: Vec<u8>,
    done: mpsc::SyncSender<Packed>,
}

/// The threads compressing for one writer. They end when it is dropped.
struct Helpers {
    jobs: Option<mpsc::SyncSender<Job>>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl Helpers {
    fn start(count: usize) -> Option<Helpers> {
        if count == 0 {
            return None;
        }
        let (tx, rx) = mpsc::sync_channel::<Job>(count);
        let rx = Arc::new(Mutex::new(rx));
        let mut threads = Vec::with_capacity(count);
        for _ in 0..count {
            let rx = Arc::clone(&rx);
            let spawned = std::thread::Builder::new()
                .name("kdb-compress".into())
                .spawn(move || {
                    let mut cctx = compressor();
                    loop {
                        let job = match rx.lock() {
                            Ok(rx) => rx.recv(),
                            Err(_) => return,
                        };
                        let Ok(job) = job else { return };
                        let packed = pack(cctx.as_mut(), job.raw, job.data);
                        let _ = job.done.send(packed);
                    }
                });
            match spawned {
                Ok(thread) => threads.push(thread),
                // As many as could be started; none, and the writer
                // compresses on its own thread.
                Err(_) => break,
            }
        }
        if threads.is_empty() {
            return None;
        }
        Some(Helpers {
            jobs: Some(tx),
            threads,
        })
    }
}

impl Drop for Helpers {
    fn drop(&mut self) {
        // Closed, the queue ends each thread once it is through its job.
        self.jobs.take();
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

/// Writes what is written to it to `out` as a compressed snapshot. What is
/// written to it is the plain image; [`BlockWriter::finish`] ends it.
///
/// Blocks are compressed on up to [`MAX_HELPERS`] threads of its own, a few
/// blocks at a time: it holds no more than a few blocks' worth of memory,
/// whatever the size of the image.
pub struct BlockWriter<W: Write> {
    out: W,
    block_size: usize,
    buf: Vec<u8>,
    helpers: Option<Helpers>,
    /// For the blocks compressed on this thread, when there are no helpers.
    cctx: Option<zstd::bulk::Compressor<'static>>,
    pending: VecDeque<mpsc::Receiver<Packed>>,
    /// The most blocks with the helpers at once.
    most_pending: usize,
    spare_raw: Vec<Vec<u8>>,
    spare_data: Vec<Vec<u8>>,
    blocks: u64,
    raw: u64,
    stored: u64,
}

impl<W: Write> BlockWriter<W> {
    /// Start a compressed snapshot on `out`: its header is written now.
    pub fn new(out: W) -> io::Result<Self> {
        Self::with(out, BLOCK_SIZE, helper_count())
    }

    fn with(mut out: W, block_size: usize, helpers: usize) -> io::Result<Self> {
        out.write_all(&header_bytes(block_size))?;
        let helpers = Helpers::start(helpers);
        let most_pending = helpers.as_ref().map_or(0, |h| 2 * h.threads.len());
        let cctx = if helpers.is_none() {
            compressor()
        } else {
            None
        };
        Ok(BlockWriter {
            out,
            block_size,
            buf: Vec::with_capacity(block_size),
            helpers,
            cctx,
            pending: VecDeque::new(),
            most_pending,
            spare_raw: Vec::new(),
            spare_data: Vec::new(),
            blocks: 0,
            raw: 0,
            stored: HEADER_LEN as u64,
        })
    }

    fn write_packed(&mut self, packed: Packed) -> io::Result<()> {
        let Packed {
            mut raw,
            mut data,
            compressed,
            sum,
        } = packed;
        let (kind, bytes) = if compressed {
            (KIND_ZSTD, &data[..])
        } else {
            (KIND_STORED, &raw[..])
        };
        let mut h = [0u8; BLOCK_HEADER_LEN];
        h[0] = kind;
        h[1..5].copy_from_slice(&(raw.len() as u32).to_le_bytes());
        h[5..9].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
        h[9..17].copy_from_slice(&sum.to_le_bytes());
        self.out.write_all(&h)?;
        self.out.write_all(bytes)?;
        self.blocks += 1;
        self.raw += raw.len() as u64;
        self.stored += (BLOCK_HEADER_LEN + bytes.len()) as u64;
        raw.clear();
        data.clear();
        self.spare_raw.push(raw);
        self.spare_data.push(data);
        Ok(())
    }

    fn wait_one(&mut self) -> io::Result<()> {
        let Some(done) = self.pending.pop_front() else {
            return Ok(());
        };
        let packed = done
            .recv()
            .map_err(|_| io::Error::other("a snapshot compression thread failed"))?;
        self.write_packed(packed)
    }

    /// Send the block being filled to be compressed and written.
    fn submit(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let fresh = self
            .spare_raw
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(self.block_size));
        let raw = std::mem::replace(&mut self.buf, fresh);
        let data = self.spare_data.pop().unwrap_or_default();
        let jobs = self.helpers.as_ref().and_then(|h| h.jobs.clone());
        let Some(jobs) = jobs else {
            let packed = pack(self.cctx.as_mut(), raw, data);
            return self.write_packed(packed);
        };
        while self.pending.len() >= self.most_pending {
            self.wait_one()?;
        }
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        jobs.send(Job {
            raw,
            data,
            done: done_tx,
        })
        .map_err(|_| io::Error::other("the snapshot compression threads are gone"))?;
        self.pending.push_back(done_rx);
        Ok(())
    }

    /// Write the last block and the end. Returns the bytes of the image and
    /// the bytes written, and `out`.
    pub fn finish(mut self) -> io::Result<(u64, u64, W)> {
        self.submit()?;
        while !self.pending.is_empty() {
            self.wait_one()?;
        }
        self.out.write_all(&end_bytes(self.blocks, self.raw))?;
        self.stored += END_LEN as u64;
        self.helpers.take();
        let BlockWriter {
            out, raw, stored, ..
        } = self;
        Ok((raw, stored, out))
    }
}

impl<W: Write> Write for BlockWriter<W> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.write_all(data)?;
        Ok(data.len())
    }

    fn write_all(&mut self, mut data: &[u8]) -> io::Result<()> {
        while !data.is_empty() {
            let room = self.block_size - self.buf.len();
            let take = room.min(data.len());
            self.buf.extend_from_slice(&data[..take]);
            data = &data[take..];
            if self.buf.len() == self.block_size {
                self.submit()?;
            }
        }
        Ok(())
    }

    /// Blocks are written whole: nothing is written until one is full.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// ── Reading ─────────────────────────────────────────────────────────────────

/// Reads the image out of a compressed snapshot, a block at a time,
/// checking each block as it comes. It gives the end of the image only once
/// the container's end has been read and checked; anything wrong before
/// that is an error, never an early end.
pub struct BlockReader<R: Read> {
    inner: R,
    block_size: usize,
    buf: Vec<u8>,
    pos: usize,
    stored: Vec<u8>,
    dctx: Option<zstd::bulk::Decompressor<'static>>,
    blocks: u64,
    raw: u64,
    /// Bytes of the container read so far.
    offset: u64,
    ended: bool,
}

/// Read exactly `buf.len()` bytes; `Ok(false)` if `r` ends before them.
fn read_full(r: &mut impl Read, buf: &mut [u8]) -> io::Result<bool> {
    let mut at = 0;
    while at < buf.len() {
        match r.read(&mut buf[at..]) {
            Ok(0) => return Ok(false),
            Ok(n) => at += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

impl<R: Read> BlockReader<R> {
    /// The reader of the container `inner` is at the header of: after its
    /// magic, which has been read (and is [`MAGIC`]).
    pub fn after_magic(mut inner: R) -> io::Result<Self> {
        let mut h = [0u8; HEADER_LEN];
        h[..8].copy_from_slice(MAGIC);
        if !read_full(&mut inner, &mut h[8..])? {
            return Err(cut_short("its header"));
        }
        let sum = u32::from_le_bytes(h[16..20].try_into().unwrap());
        if xxh3_64(&h[..16]) as u32 != sum {
            return Err(invalid(
                "compressed snapshot: its header is damaged (checksum mismatch)".into(),
            ));
        }
        if h[8] != VERSION {
            return Err(invalid(format!(
                "compressed snapshot: format version {} is not one this build reads (it reads \
                 {VERSION}): written by a later KiviDB",
                h[8]
            )));
        }
        if h[9] != CODEC_ZSTD {
            return Err(invalid(format!(
                "compressed snapshot: codec {} is not one this build reads",
                h[9]
            )));
        }
        if h[10..12] != [0, 0] {
            return Err(invalid(
                "compressed snapshot: its header has bits set that this build does not know".into(),
            ));
        }
        let block_size = u32::from_le_bytes(h[12..16].try_into().unwrap()) as usize;
        if block_size == 0 || block_size > MAX_BLOCK_SIZE {
            return Err(invalid(format!(
                "compressed snapshot: a block size of {block_size} bytes is out of range (1 to \
                 {MAX_BLOCK_SIZE})"
            )));
        }
        Ok(BlockReader {
            inner,
            block_size,
            buf: Vec::new(),
            pos: 0,
            stored: Vec::new(),
            dctx: None,
            blocks: 0,
            raw: 0,
            offset: HEADER_LEN as u64,
            ended: false,
        })
    }

    fn damaged(&self, what: &str) -> io::Error {
        invalid(format!(
            "compressed snapshot: block {} (at byte {}) {what}",
            self.blocks + 1,
            self.offset
        ))
    }

    /// Read the next block into `buf`, or the end.
    fn next_block(&mut self) -> io::Result<()> {
        self.buf.clear();
        self.pos = 0;
        let mut kind = [0u8];
        if !read_full(&mut self.inner, &mut kind)? {
            return Err(invalid(format!(
                "compressed snapshot: it is cut short after block {} (at byte {}), before its end \
                 marker",
                self.blocks, self.offset
            )));
        }
        if kind[0] == KIND_END {
            let mut e = [0u8; END_LEN];
            e[0] = KIND_END;
            if !read_full(&mut self.inner, &mut e[1..])? {
                return Err(invalid(format!(
                    "compressed snapshot: it is cut short inside its end marker (at byte {})",
                    self.offset
                )));
            }
            let blocks = u64::from_le_bytes(e[1..9].try_into().unwrap());
            let raw = u64::from_le_bytes(e[9..17].try_into().unwrap());
            let sum = u64::from_le_bytes(e[17..25].try_into().unwrap());
            if xxh3_64(&e[1..17]) != sum || blocks != self.blocks || raw != self.raw {
                return Err(invalid(format!(
                    "compressed snapshot: its end marker (at byte {}) is damaged or does not \
                     match: it says {blocks} blocks and {raw} bytes, {} blocks and {} bytes were \
                     read",
                    self.offset, self.blocks, self.raw
                )));
            }
            self.offset += END_LEN as u64;
            self.ended = true;
            return Ok(());
        }
        if kind[0] != KIND_STORED && kind[0] != KIND_ZSTD {
            return Err(self.damaged(&format!("has an unknown kind {}: it is damaged", kind[0])));
        }
        let mut h = [0u8; BLOCK_HEADER_LEN];
        h[0] = kind[0];
        if !read_full(&mut self.inner, &mut h[1..])? {
            return Err(self.damaged("is cut short in its header"));
        }
        let raw_len = u32::from_le_bytes(h[1..5].try_into().unwrap()) as usize;
        let stored_len = u32::from_le_bytes(h[5..9].try_into().unwrap()) as usize;
        let sum = u64::from_le_bytes(h[9..17].try_into().unwrap());
        if raw_len == 0 || raw_len > self.block_size {
            return Err(self.damaged(&format!(
                "says it holds {raw_len} bytes, out of range for blocks of {} bytes: it is \
                 damaged",
                self.block_size
            )));
        }
        let most_stored = if kind[0] == KIND_STORED {
            raw_len
        } else {
            zstd::zstd_safe::compress_bound(self.block_size)
        };
        if (kind[0] == KIND_STORED && stored_len != raw_len) || stored_len > most_stored {
            return Err(self.damaged(&format!(
                "says {stored_len} bytes are stored for {raw_len}: it is damaged"
            )));
        }
        self.stored.clear();
        self.stored.resize(stored_len, 0);
        if !read_full(&mut self.inner, &mut self.stored)? {
            return Err(self.damaged("is cut short"));
        }
        if kind[0] == KIND_STORED {
            std::mem::swap(&mut self.buf, &mut self.stored);
        } else {
            if self.dctx.is_none() {
                self.dctx = Some(zstd::bulk::Decompressor::new()?);
            }
            // Never more than the block says it holds: a crafted block
            // cannot make it take more memory than that.
            self.buf.reserve_exact(raw_len);
            let dctx = self.dctx.as_mut().unwrap();
            let n = match dctx.decompress_to_buffer(&self.stored[..], &mut self.buf) {
                Ok(n) => n,
                Err(_) => return Err(self.damaged("does not decompress: it is damaged")),
            };
            if n != raw_len {
                return Err(self.damaged(&format!(
                    "decompresses to {n} bytes, not the {raw_len} it says: it is damaged"
                )));
            }
        }
        if xxh3_64(&self.buf) != sum {
            self.buf.clear();
            return Err(self.damaged("does not match its checksum: it is damaged"));
        }
        self.blocks += 1;
        self.raw += raw_len as u64;
        self.offset += (BLOCK_HEADER_LEN + stored_len) as u64;
        Ok(())
    }

    /// The image has been read as far as its reader reads it: read and
    /// check the blocks left, and the end, and that nothing follows it.
    /// Returns the blocks and the bytes of the image.
    pub fn finish(mut self) -> io::Result<(u64, u64)> {
        while !self.ended {
            self.next_block()?;
        }
        let mut more = [0u8];
        if read_full(&mut self.inner, &mut more)? {
            return Err(invalid(format!(
                "compressed snapshot: bytes follow its end marker (at byte {}): it is damaged",
                self.offset
            )));
        }
        Ok((self.blocks, self.raw))
    }
}

fn cut_short(what: &str) -> io::Error {
    invalid(format!("compressed snapshot: it is cut short in {what}"))
}

impl<R: Read> Read for BlockReader<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        while self.pos == self.buf.len() {
            if self.ended {
                return Ok(0);
            }
            self.next_block()?;
        }
        let n = out.len().min(self.buf.len() - self.pos);
        out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

// ── kividb kdb-decompress / kdb-compress ────────────────────────────────────

/// `kividb kdb-decompress <in> <out>` and `kividb kdb-compress <in> <out>`:
/// the exit code, if the command line is one of them.
pub fn subcommand(args: &[std::ffi::OsString]) -> Option<i32> {
    let name = args.get(1)?.to_str()?;
    let compress = match name {
        "kdb-decompress" => false,
        "kdb-compress" => true,
        _ => return None,
    };
    if args.len() != 4 {
        eprintln!(
            "usage: kividb {name} <in> <out>\n  {}",
            if compress {
                "writes the plain snapshot <in> to <out> compressed, as rdbcompression yes writes it"
            } else {
                "writes the snapshot <in> to <out> as a plain snapshot (KIVIDB01), which KiviDB \
                 1.2 and earlier read"
            }
        );
        return Some(1);
    }
    let input = std::path::Path::new(&args[2]);
    let output = std::path::Path::new(&args[3]);
    match convert_file(input, output, compress) {
        Ok((raw, stored)) => {
            println!(
                "{}: {raw} bytes of image, {stored} bytes written to {}",
                input.display(),
                output.display()
            );
            Some(0)
        }
        Err(e) => {
            eprintln!("kividb {name}: {e}");
            Some(1)
        }
    }
}

/// Write the snapshot `input` to `output`, compressed or plain: through a
/// temporary file beside it, made durable, then renamed into place. Returns
/// the bytes of the image and the bytes written.
pub fn convert_file(
    input: &std::path::Path,
    output: &std::path::Path,
    compress: bool,
) -> io::Result<(u64, u64)> {
    if let (Ok(a), Ok(b)) = (input.canonicalize(), output.canonicalize())
        && a == b
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "<in> and <out> are the same file",
        ));
    }
    let mut source = io::BufReader::with_capacity(1 << 20, std::fs::File::open(input)?);
    let mut magic = [0u8; 8];
    if !read_full(&mut source, &mut magic)? || (&magic != MAGIC && &magic != super::KDB_MAGIC) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} is not a KiviDB snapshot", input.display()),
        ));
    }
    let tmp = {
        let mut name = output.as_os_str().to_owned();
        name.push(format!(".{}.tmp", std::process::id()));
        std::path::PathBuf::from(name)
    };
    let result = (|| -> io::Result<(u64, u64)> {
        let file = std::fs::File::create(&tmp)?;
        let mut out = io::BufWriter::with_capacity(1 << 20, file);
        let sizes = if &magic == MAGIC {
            let mut blocks = BlockReader::after_magic(&mut source)?;
            let mut inner = [0u8; 8];
            if !read_full(&mut blocks, &mut inner)? || &inner != super::KDB_MAGIC {
                return Err(invalid(
                    "compressed snapshot: what it holds is not a KDB image".into(),
                ));
            }
            if compress {
                let mut w = BlockWriter::new(&mut out)?;
                w.write_all(&inner)?;
                io::copy(&mut blocks, &mut w)?;
                blocks.finish()?;
                let (raw, stored, _) = w.finish()?;
                (raw, stored)
            } else {
                out.write_all(&inner)?;
                io::copy(&mut blocks, &mut out)?;
                let (_, raw) = blocks.finish()?;
                (raw, raw)
            }
        } else if compress {
            let mut w = BlockWriter::new(&mut out)?;
            w.write_all(&magic)?;
            io::copy(&mut source, &mut w)?;
            let (raw, stored, _) = w.finish()?;
            (raw, stored)
        } else {
            out.write_all(&magic)?;
            let n = 8 + io::copy(&mut source, &mut out)?;
            (n, n)
        };
        out.flush()?;
        out.get_ref().sync_all()?;
        drop(out);
        std::fs::rename(&tmp, output)?;
        Ok(sizes)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compress_with(image: &[u8], block_size: usize, helpers: usize) -> Vec<u8> {
        let mut w = BlockWriter::with(Vec::new(), block_size, helpers).unwrap();
        // In pieces of every size, as the image's writer writes.
        let mut at = 0;
        let mut step = 1;
        while at < image.len() {
            let end = (at + step).min(image.len());
            w.write_all(&image[at..end]).unwrap();
            at = end;
            step = step * 7 % 9973 + 1;
        }
        let (raw, stored, out) = w.finish().unwrap();
        assert_eq!(raw, image.len() as u64);
        assert_eq!(stored, out.len() as u64);
        out
    }

    fn decompress(container: &[u8]) -> io::Result<Vec<u8>> {
        assert!(container.starts_with(MAGIC));
        let mut r = BlockReader::after_magic(&container[8..])?;
        let mut out = Vec::new();
        r.read_to_end(&mut out)?;
        r.finish()?;
        Ok(out)
    }

    /// Bytes that compress some, as an image does.
    fn sample(len: usize, seed: u64) -> Vec<u8> {
        let mut x = seed | 1;
        (0..len)
            .map(|i| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                if i % 3 == 0 {
                    (x % 251) as u8
                } else {
                    b"kividb"[i % 6]
                }
            })
            .collect()
    }

    #[test]
    fn round_trips_any_length_with_and_without_helpers() {
        for helpers in [0, 1, 2] {
            for len in [0, 1, 4095, 4096, 4097, 3 * 4096, 100_000] {
                let image = sample(len, len as u64 + 7);
                let c = compress_with(&image, 4096, helpers);
                assert_eq!(
                    decompress(&c).unwrap(),
                    image,
                    "len {len} helpers {helpers}"
                );
            }
        }
    }

    #[test]
    fn incompressible_blocks_are_stored_as_they_are() {
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let image: Vec<u8> = (0..(4 << 20))
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        let c = compress_with(&image, BLOCK_SIZE, 2);
        assert!(
            (c.len() as f64) <= image.len() as f64 * 1.001,
            "{} for {}",
            c.len(),
            image.len()
        );
        assert_eq!(decompress(&c).unwrap(), image);
    }

    fn refused(container: &[u8]) -> String {
        let e = decompress(container).expect_err("damage was not found");
        assert!(is_refusal(&e), "{e}");
        assert_eq!(e.kind(), io::ErrorKind::InvalidData, "{e}");
        e.to_string()
    }

    /// Where each block starts, and the end.
    fn boundaries(c: &[u8]) -> Vec<usize> {
        let mut at = HEADER_LEN;
        let mut all = vec![at];
        while c[at] != KIND_END {
            let stored = u32::from_le_bytes(c[at + 5..at + 9].try_into().unwrap()) as usize;
            at += BLOCK_HEADER_LEN + stored;
            all.push(at);
        }
        all
    }

    #[test]
    fn cut_short_anywhere_is_refused() {
        let image = sample(40_000, 3);
        let c = compress_with(&image, 4096, 0);
        let marks = boundaries(&c);
        assert!(marks.len() > 5);
        // At every block's start, inside its header, inside its bytes, and
        // at every byte of the end.
        let mut cuts: Vec<usize> = Vec::new();
        for &m in &marks {
            cuts.extend([
                m,
                m + 1,
                m + 5,
                m + BLOCK_HEADER_LEN,
                m + BLOCK_HEADER_LEN + 3,
            ]);
        }
        cuts.extend(c.len() - END_LEN..c.len());
        cuts.extend(9..HEADER_LEN);
        for cut in cuts {
            if cut >= c.len() {
                continue;
            }
            let why = refused(&c[..cut]);
            assert!(why.contains("cut short"), "cut at {cut}: {why}");
        }
    }

    #[test]
    fn damage_is_refused_naming_the_block() {
        let image = sample(40_000, 5);
        let c = compress_with(&image, 4096, 0);
        let marks = boundaries(&c);
        // A bit flipped in the bytes of the third block.
        let mut d = c.clone();
        d[marks[2] + BLOCK_HEADER_LEN + 10] ^= 0x10;
        let why = refused(&d);
        assert!(why.contains("block 3"), "{why}");
        // In its checksum, its lengths, its kind.
        for at in [9, 1, 5, 0] {
            let mut d = c.clone();
            d[marks[2] + at] ^= 0x01;
            let why = refused(&d);
            assert!(why.contains("block 3"), "byte {at}: {why}");
        }
        // In the header: every byte after the magic.
        for at in 8..HEADER_LEN {
            let mut d = c.clone();
            d[at] ^= 0x04;
            refused(&d);
        }
        // In the end marker: every byte.
        for at in c.len() - END_LEN..c.len() {
            let mut d = c.clone();
            d[at] ^= 0x02;
            refused(&d);
        }
    }

    #[test]
    fn lengths_out_of_range_are_refused_without_taking_memory_for_them() {
        let image = sample(20_000, 9);
        let c = compress_with(&image, 4096, 0);
        let marks = boundaries(&c);
        let set = |c: &[u8], at: usize, v: u32| {
            let mut d = c.to_vec();
            d[at..at + 4].copy_from_slice(&v.to_le_bytes());
            d
        };
        // A raw length of 2^32-1, of more than the block size, of 0.
        for v in [u32::MAX, 4097, 0] {
            let why = refused(&set(&c, marks[1] + 1, v));
            assert!(why.contains("block 2"), "{v}: {why}");
        }
        // A stored length of 2^32-1, and of more than the file holds.
        for v in [u32::MAX, (c.len() * 2) as u32] {
            refused(&set(&c, marks[1] + 5, v));
        }
        // A header that declares blocks larger than the most there may be,
        // with its checksum right: refused before any block is read.
        let mut h = header_bytes(MAX_BLOCK_SIZE + 1).to_vec();
        h.extend_from_slice(&c[HEADER_LEN..]);
        assert!(refused(&h).contains("out of range"));
        let mut h = header_bytes(0).to_vec();
        h.extend_from_slice(&c[HEADER_LEN..]);
        refused(&h);
    }

    #[test]
    fn a_block_that_decompresses_to_more_than_it_says_is_refused() {
        // 1 MiB of zeros, compressed, in a block that says it holds 4 KiB:
        // a decompression bomb. The decoder stops at 4 KiB.
        let bomb = zstd::bulk::compress(&vec![0u8; 1 << 20], 1).unwrap();
        let mut c = header_bytes(4096).to_vec();
        c.push(KIND_ZSTD);
        c.extend_from_slice(&4096u32.to_le_bytes());
        c.extend_from_slice(&(bomb.len() as u32).to_le_bytes());
        c.extend_from_slice(&xxh3_64(&[0u8; 4096]).to_le_bytes());
        c.extend_from_slice(&bomb);
        c.extend_from_slice(&end_bytes(1, 4096));
        let why = refused(&c);
        assert!(why.contains("block 1"), "{why}");
        // Saying it holds the most a block may, 16 MiB: refused all the
        // same, with no more than that taken.
        let bomb = zstd::bulk::compress(&vec![0u8; 64 << 20], 1).unwrap();
        let mut c = header_bytes(MAX_BLOCK_SIZE).to_vec();
        c.push(KIND_ZSTD);
        c.extend_from_slice(&(MAX_BLOCK_SIZE as u32).to_le_bytes());
        c.extend_from_slice(&(bomb.len() as u32).to_le_bytes());
        c.extend_from_slice(&0u64.to_le_bytes());
        c.extend_from_slice(&bomb);
        c.extend_from_slice(&end_bytes(1, MAX_BLOCK_SIZE as u64));
        refused(&c);
    }

    #[test]
    fn no_blocks_and_trailing_bytes() {
        // No blocks: an empty image, which is whole as a container (what
        // reads the image refuses it for having no magic).
        let c = compress_with(b"", 4096, 0);
        assert_eq!(c.len(), HEADER_LEN + END_LEN);
        assert_eq!(decompress(&c).unwrap(), b"");
        // An end that says there were blocks when there were none.
        let mut d = header_bytes(4096).to_vec();
        d.extend_from_slice(&end_bytes(1, 10));
        refused(&d);
        // Bytes after the end.
        let image = sample(10_000, 11);
        let mut d = compress_with(&image, 4096, 0);
        d.push(0);
        assert!(refused(&d).contains("follow its end"));
        // Another container after the end.
        let mut d = compress_with(&image, 4096, 0);
        d.extend_from_slice(&compress_with(&image, 4096, 0));
        refused(&d);
    }

    /// A few thousand random corruptions: each is refused or, if it changes
    /// nothing that matters, gives the image back whole. None panics, and
    /// none gives back anything else.
    #[test]
    fn random_damage_never_loads_anything_but_the_image() {
        let image = sample(60_000, 13);
        let c = compress_with(&image, 4096, 0);
        let mut x = 0x2545_f491_4f6c_dd1du64;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..4000 {
            let mut d = c.clone();
            match next() % 5 {
                // Bits flipped.
                0 | 1 => {
                    for _ in 0..=(next() % 3) {
                        let at = (next() as usize) % d.len();
                        d[at] ^= 1 << (next() % 8);
                    }
                }
                // Cut short.
                2 => d.truncate((next() as usize) % d.len()),
                // Bytes put in.
                3 => {
                    let at = (next() as usize) % d.len();
                    let n = 1 + (next() % 16) as usize;
                    let junk: Vec<u8> = (0..n).map(|_| next() as u8).collect();
                    d.splice(at..at, junk);
                }
                // Bytes taken out.
                _ => {
                    let at = HEADER_LEN + (next() as usize) % (d.len() - HEADER_LEN);
                    let n = (1 + (next() % 16) as usize).min(d.len() - at);
                    d.drain(at..at + n);
                }
            }
            if !d.starts_with(MAGIC) {
                continue;
            }
            match decompress(&d) {
                Ok(out) => assert_eq!(out, image, "a damaged container gave back another image"),
                Err(e) => assert!(is_refusal(&e), "{e}"),
            }
        }
    }
}
