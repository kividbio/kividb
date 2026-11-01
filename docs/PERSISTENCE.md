# Persistence: snapshots, the AOF, and what recovery guarantees

KiviDB keeps its data on disk in two files in `dir`:

* **`dump.kdb`**, a snapshot of the whole dataset in KiviDB's own format.
  A Redis `dump.rdb` in the same directory is read at startup if there is no
  `dump.kdb`.
* **`appendonly.aof`**, the append-only file (AOF), with `appendonly yes`
  (or `--aof`). It holds the writes made since the last snapshot, as
  commands.

The two work together: the snapshot is the base, and the AOF is the delta on
top of it.

## Settings

| Directive | What KiviDB does with it |
|---|---|
| `appendonly yes` / `--aof` | Turns the AOF on. `CONFIG SET appendonly yes` works at runtime (see below). |
| `appendfsync` | Only `everysec`: the AOF is synced to disk once a second. `always` and `no` are accepted with a warning that they have no effect. |
| `save <points>` | Accepted with a warning that it has no effect: KiviDB does not snapshot on a schedule. Use `appendonly yes`, or `BGSAVE`. |
| `dbfilename` | Always `dump.kdb` (`dump.rdb` is accepted). Any other name is refused. |
| `appendfilename` | Always `appendonly.aof`. Any other name is refused. |
| `auto-aof-rewrite-percentage`, `auto-aof-rewrite-min-size` | When the AOF has grown by this percentage (default 100) over its size after the last checkpoint, and is at least this size (default 64 MB), a checkpoint runs. Checked every 10 seconds. `0` turns it off. Both can be changed with `CONFIG SET`. |
| `hll-write-format` | `legacy` (the default) or `redis`. New HyperLogLog values use that format; existing values keep theirs. Per server, set in the config file or with `CONFIG SET`, and written by `CONFIG REWRITE`. See below before switching. |

## Checkpoints

A **checkpoint** writes the whole dataset to `dump.kdb` and starts the AOF
again as an empty delta (it begins with `KIVIDLT1` and the number of the
save it continues from). After a checkpoint the AOF holds only the writes
since then. Checkpoints happen on:

* `SAVE`, `BGSAVE` and `BGREWRITEAOF` (which answers `ERR AOF is not
  enabled` when the AOF is off);
* a clean shutdown;
* automatic growth, as set by `auto-aof-rewrite-percentage`;
* a replica's full sync, and `CONFIG SET appendonly yes` (both below).

Writes are held only while the dataset is captured and the AOF is turned
over: with about 9 million keys, under 100 ms. The snapshot is written
after, while writes continue. Because the cut between the two files is made
with writes held, a write is in exactly one of them.

If a checkpoint is stopped part way (a crash, a kill), the previous log is
kept as `appendonly.aof.prev`. At the next start it is replayed, and then
the new log, on top of the snapshot the checkpoint started from.

## How recovery works

On startup, `dump.kdb` (or `dump.rdb`) is loaded first if it is present.
Then, with the AOF on and `appendonly.aof` not empty, the AOF is replayed
**on top of that snapshot**. This applies to replicas too: a replica starts
from its own files, then syncs with its master.

* A delta whose save number is older than the snapshot's (`dump.kdb.gen`) is
  skipped: it was already folded into the snapshot, and replaying it would
  apply its writes twice.
* A delta whose save number is newer than the snapshot's means the two files
  are from different moments, for example copied separately with a save in
  between. The writes between those saves are in neither file, and the
  server refuses to start.
* A partial record at the end of the file, the normal result of dying
  mid-write, is cut off with a `[WARN]` naming it. It was never acknowledged
  to a client.
* A transaction (or a script that wrote more than once) is written between
  a `MULTI` and an `EXEC`. One whose `EXEC` is not in the file is dropped
  whole and cut off, as Redis does: `Revert incomplete MULTI/EXEC
  transaction in AOF file`. Half of it is a dataset no client ever saw.
* A malformed record *before* the end makes the whole replay fail before
  anything is applied, and the server refuses to start rather than start
  with part of its data.
* A legacy AOF (one that starts with `*` rather than `KIVIDLT1`) is still
  replayed on top of the snapshot, with a warning: it is a full history, so
  `INCR` or `RPUSH` can be applied twice. Run `SAVE` once the server is up
  to rebase it.

A data file that is present but cannot be read stops the server; it does
not start empty. [ERRORS.md](ERRORS.md) lists each of these messages and
what to do.

Replay runs each command as the replica applier does, so a replayed write is
neither sent to replicas nor appended again to the file it came from.

### HyperLogLogs and `hll-write-format`

`hll-write-format` is read from the configuration before the AOF is replayed,
so a replayed `PFADD` or `PFMERGE` that creates a key writes it in the
configured format. Values already in the snapshot or the log keep the format
they were written in, whether that is KiviDB's legacy dense form or Redis's
dense or sparse form; the setting only chooses the format of new values.

A legacy HLL and a Redis one index different hash spaces. Changing the setting
does not convert existing values, and there is no conversion that preserves
their elements. `PFMERGE` and multi-key `PFCOUNT` therefore refuse a mixture.
Set `redis` on every node of a deployment before expecting new keys to be
interchangeable, and keep every node on `legacy` until all of them run a
release that knows this setting.

`PFADD` and `PFMERGE` are replayed and replicated as commands, not as values,
so a replica applies them with its own `hll-write-format`. A master and its
replicas must agree on it: a replica with a different setting creates a key in
a different format, and a later operation across those keys is refused. The
same applies to restoring an AOF written under another setting — the commands
are valid, but keys created during the replay follow the setting in effect.

Two things keep replay fast:

* **Index maintenance is deferred.** Search indexes are not updated per
  replayed write. They are rebuilt once, in the background, after the
  server starts listening (it answers `LOADING` meanwhile). Indexes and HNSW
  graphs stored in `dump.kdb` are loaded directly.
* **Replay starts at the last flush.** A pre-scan finds the last `FLUSHALL`
  (or the last `FLUSHDB` for each database) and begins replay there.

### The log is replayable

Commands whose result is chosen when they run are written to the AOF, and
sent to replicas, as their effect:

* `SPOP` picks random members, so it is recorded as the removal of the
  members it picked.
* `XADD … *` takes an id from the clock, so it is recorded with the id it
  was given.

Replay over a snapshot also needs each record to be idempotent. That is why
`INCRBYFLOAT`, deterministic as it is, is recorded as an absolute `SET`.

### TTLs are recorded as moments, not durations

A command that gives a TTL as "this long from now" means something else to
whoever runs it later. Logged as typed, `SET k v EX 100` replayed after a
restart would give the key a fresh hundred seconds, and a replica, which
runs the command a moment after its master, would expire the key that much
later.

Such commands are written to the AOF, and sent to replicas, with the moment
of expiry:

| Command | Recorded as |
|---|---|
| `SET k v EX n` / `PX n` | `SET k v PXAT <unix-ms>` |
| `SETEX`, `PSETEX` | `SET k v PXAT <unix-ms>` |
| `EXPIRE`, `PEXPIRE` | `PEXPIREAT k <unix-ms>` |
| `GETEX k EX/PX/EXAT/PXAT n` | `PEXPIREAT k <unix-ms>`; `GETEX k PERSIST` as `PERSIST k`; a plain `GETEX`, or one on a missing key, is not recorded |
| `MSETEX … EX/PX n` | the same with `PXAT` |
| `HEXPIRE`, `HPEXPIRE` | `HPEXPIREAT k <unix-ms> …` |
| `HSETEX`, `HGETEX … EX/PX n` | the same with `PXAT` |
| `INCREX` | `SET k <new value> KEEPTTL`, then `PEXPIREAT` or `PERSIST` if it changed the TTL; nothing if a bound made it skip |
| `RESTORE k ttl payload` | `RESTORE k <unix-ms> payload … ABSTTL` |

`EXPIRE` / `PEXPIRE` with a time that is not in the future delete the key,
and are recorded as `DEL k`.

One form is left as typed: the older KiviDB `HSETEX key seconds … KEEPTTL`,
whose "keep a field's TTL, or else use this one" has no absolute spelling.

## Replicas keep their own AOF

A replica with the AOF on appends every write it applies from its master to
its own log, as Redis does. So a promoted replica is durable from the moment
it is promoted, and losing every node at once is survivable from any node's
disk, not only the old master's. Each replica pays its own AOF write and
fsync cost; that is what `appendonly yes` means on a replica in Redis too.

### A full sync rebases the log

A full resync replaces the whole dataset; the AOF has to follow, or it would
replay to a dataset the node never had. So after loading the master's data,
and before applying the command stream, a replica with the AOF on runs a
checkpoint: `dump.kdb` holds exactly what it synced, and the AOF starts
empty.

## Enabling the AOF at runtime

`CONFIG SET appendonly yes` runs a checkpoint, so the files describe
everything that already exists. Without it, a restart would see a log of
only what happened next. Redis rewrites at this point for the same reason.

## Observing durability

`INFO persistence` carries:

| Field | Meaning |
| --- | --- |
| `aof_current_size` | Bytes on disk (AOF on only). |
| `aof_base_size` | Size after the last checkpoint, or at startup: the base for the growth ratio. |
| `aof_rewrite_in_progress` | A checkpoint is running. |
| `aof_last_rewrite_time_sec` | Redis-compatible whole seconds, `-1` if none has run. |
| `aof_last_rewrite_time_ms` | The same in milliseconds, because the seconds field shows a short checkpoint as `0`. |
| `aof_last_bgrewrite_status` | `err` if the last checkpoint failed. Durability is intact, but the log keeps growing. |
| `aof_lost_records` | Non-zero means the log has a hole and can no longer reconstruct the dataset. |

`rdb_last_bgsave_status` and `LASTSAVE` report snapshots, as in Redis.

## Online backup, and starting from one

`BACKUP START` … `BACKUP SEAL` (Redis 8.10) produce three files in
`--backupdirname` (default `backupdir`):

* **BASE**: `appendonly.aof.N.base.kdb`, the dataset at the instant
  `BACKUP START` took its cut, in the snapshot format.
* **INCR**: `appendonly.aof.N.incr.aof`, every write from that instant up to
  `BACKUP SEAL`, as commands.
* **Manifest**: `appendonly.aof.manifest`, naming the two.

The cut is the one a full resync uses: with writes held, the dataset is
frozen and a tap is attached to the write stream, so no write is in both
files and none is in neither. The tap is fed by the writers themselves, so a
backup does not depend on the AOF being on, and works on a replica.
Restoring loads BASE and replays INCR, which gives the dataset as of the
**seal**; the backups of a cluster's nodes can therefore be started one
after another and sealed together.

`BACKUP LIST` names only files that are complete (BASE once it is written,
all three after the seal) by absolute path, so they can be copied while the
server runs. `BACKUP ABORT` cancels an unsealed backup; `BACKUP CLEANUP`
removes a sealed or failed one; `--backup-sealed-ttl` removes a sealed one
after that many seconds.

To start a server from a backup, or from a single snapshot:

```bash
kividb --preload-file aof:/backups/n1/appendonly.aof.manifest
kividb --preload-file rdb:/backups/n1/dump.kdb
```

The target is loaded *instead of* the working directory's `dump.kdb` and
`appendonly.aof`. A target that is missing or cannot be read is fatal: a
server asked to start from a backup does not start empty.

Not in BASE: HNSW graphs (rebuilt in the background after loading). Search
synonym groups and dictionaries are in it, as they are in every snapshot,
and so are the function libraries.

## Snapshot format additions

The snapshot reader accepts the older records; the writer emits these where
there is something for them to carry (a stream with no idempotent producers
is still written as before):

| Record | Carries |
|---|---|
| stream, tag 13 | the stream as before, plus its idempotent-production state (`XADD … IDMP`): per producer, the remembered ids and the entries they map to, and the stream's `IDMP-DURATION` / `IDMP-MAXSIZE` |
| time series, tag 14 | the series as before, plus its duplicate policy and its compaction rules |
| alias section (`KIVIAL1\0`) | `FT.ALIASADD` aliases, so that they survive a restart |
| field options section (`KIVIFO1\0`) | each index's `STOPWORDS` and each field's options (`AS`, `SORTABLE`, `WEIGHT`, `SEPARATOR`, `CASESENSITIVE`, …), as the words `FT.CREATE` takes |
| synonyms and dictionaries section (`KIVISD1\0`) | each index's synonym groups (`FT.SYNUPDATE`) and every dictionary (`FT.DICTADD`) |
| function libraries section (`KIVIFN1\0`), written last | every library loaded with `FUNCTION LOAD`, as a `FUNCTION DUMP` payload, so that libraries survive a restart and reach a replica in a full sync. A build without Lua loads the rest of the snapshot and leaves them out |

An older release stops reading at the first section it does not know, so
these come after everything it does. A snapshot written by this version
cannot be read by an older one once it contains any of these.

## When a write is acknowledged

With the AOF on, the replies to a batch of commands that wrote are sent once
their records have been written to the file (the operating system's page
cache), as in Redis. A process killed outright (`kill -9`, the OOM killer)
loses no acknowledged write; a power failure can lose up to a second, as with
`appendfsync everysec` in Redis, since the file is synced once a second.
Connections waiting at the same time are answered by one write (a group
commit), and reads wait for nothing. Before 1.2 a write was acknowledged
once its record was queued, and a killed process could lose the last few.

The wait costs writes: on a 16-core server, 20-28% of `SET`s a second with
50 clients, with or without pipelining, and under 10% with one client.
`aof-ack-after-write no` (in the config file, or `CONFIG SET` at run time)
acknowledges a write once its record is queued, as before 1.2, at full
speed, and a killed process can lose the last few acknowledged writes. The
default is `yes`.

## Verifying

```bash
# every acknowledged write reaches the file
python smoke/aof_no_loss.py --port 6382 --dir "$aofdir" -n 200000

# a replica converges on non-deterministic writes
python smoke/replication_determinism.py --master-port 6380 --replica-port 6381

# a replica keeps its own AOF, survives promotion, and rebases on resync
# (spawns its own master/replica pairs)
python smoke/replica_aof_durability.py --binary ./target/release/kividb -n 5000
```

All three run in CI (`linux-verification.yml`) against both I/O models.
