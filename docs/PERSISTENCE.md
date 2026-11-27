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
| `appendfsync` / `--appendfsync` | `everysec` (the default): the AOF is synced to disk once a second. `always`: each write is synced before it is acknowledged, as in Redis (see below). `no` is taken as `everysec`, with a warning. `CONFIG SET appendfsync` works at runtime. |
| `save <points>` | Accepted with a warning that it has no effect: KiviDB does not snapshot on a schedule. Use `appendonly yes`, or `BGSAVE`. |
| `dbfilename` | Always `dump.kdb` (`dump.rdb` is accepted). Any other name is refused. `CONFIG GET dbfilename` answers `dump.kdb`, and `CONFIG GET dir` the directory it is in, so a tool that runs `SAVE` finds the file. |
| `appendfilename` | Always `appendonly.aof`, which `CONFIG GET appendfilename` answers. Any other name is refused. |
| `auto-aof-rewrite-percentage`, `auto-aof-rewrite-min-size` | When the AOF has grown by this percentage (default 100) over its size after the last checkpoint, and is at least this size (default 64 MB), a checkpoint runs. Checked every 10 seconds. `0` turns it off. Both can be changed with `CONFIG SET`. |
| `rdbcompression` | `no` (the default in 1.3): the plain image, which KiviDB 1.2 and earlier read. `yes`: snapshots are written compressed (see [Compressed snapshots](#compressed-snapshots)), which 1.2 cannot read. The default is planned to become `yes` in a later release. `CONFIG SET rdbcompression` works at runtime, from the next snapshot on. |
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

What a snapshot adds to memory while it is written: strings are only held
(a string written over or deleted meanwhile is freed once it has been
written), but the other values (lists, sets, sorted sets, hashes, streams)
change in place, so they are laid out when the dataset is captured, with
writes held. Each capture thread lays them out in 1 MiB buffers and hands
each one, once full, to a writer thread that writes it to a spill file in
the data directory; the snapshot copies them into `dump.kdb` as it is
written. A capture thread does not wait for that writer: when the disk is
behind, a full buffer stays in memory until the writer has room, so the
disk's speed adds nothing to the time writes are held, and a save holds
about 8 MiB a capture thread while the disk keeps up (#825). For 20,000
sorted sets of 800 members (a 480 MB dump), a save under a writer adds
12 MiB to the server's `Pss` (1.2.2: 461 MiB; regression suite 47). There
is one spill file a capture
thread, with no name (`O_TMPFILE` where
the filesystem has it, otherwise a temporary file removed as soon as it is
open), so nothing is left in the directory however the save ends (the
fallback's file, should the process die before it is removed, is removed at
the next start). Each part is checked against the xxh3 its writer took as it
is read back, and its blocks are given back once copied
(`FALLOC_FL_PUNCH_HOLE`, on Linux), so the spill files take what is still to
be copied: a save needs room for about one dump beside the previous
`dump.kdb` (on a filesystem without hole punching, the collections' part of
the dump more, until the snapshot is written). A save whose spill fails (a full disk, bytes read back otherwise
than they were written) fails, writes go on, and the previous `dump.kdb`
stays. A replica's full sync and an online backup make their image in
memory.

If a checkpoint is stopped part way (a crash, a kill), the previous log is
kept as `appendonly.aof.prev`. At the next start it is replayed, and then
the new log, on top of the snapshot the checkpoint started from.

### The old log is complete before the new one is replayed

The old log's last writes and the new log's first ones reach the disk
through separate syncs. The server never syncs the new log before the old
one, but the operating system writes pages back to the disk on its own, in
any order, so a power failure before the old log's sync can leave the new
log's first writes on disk and lose the old log's last ones.

So the new log opens, after its header, with a record of where the old log
ended when the log was turned over: its length, and a hash of its last
4 KiB (`KIVIDB.AOF-OLD-LOG-END`, 83 bytes, written with the log locked, at
the cost of one small write). A restart replays the new log only when the
old log matches that record (its length, and its last 4 KiB). If it does
not (the power failed before the old log was synced), the restart:

* replays the old log up to its last whole record, as it would a log cut
  short, and not the new log: the dataset is what the writes up to there
  made, never later writes without the earlier ones;
* logs `[WARN] AOF: a checkpoint did not finish, and appendonly.aof.prev on
  disk does not reach where appendonly.aof goes on from: ...`, and why;
* keeps the new log as `superseded.<ms>.<n>.appendonly.aof` (see [Logs kept
  aside](#logs-kept-aside)), with a warning that says how to apply its
  writes by hand, knowing that the ones between the two logs are lost;
* starts `appendonly.aof` again after the old log, which it cuts after its
  last whole record, so the next start loads the same dataset and the writes
  made from then on follow it.

None of the writes in such a new log was reported synced: under
`appendfsync always` a reply, and `WAITAOF` under any setting, waits for a
sync of the new log, and that sync first syncs the old log and the
directory. What is lost is writes that `everysec` (or `no`) allows a power
failure to lose, and with `always`, writes not yet acknowledged.

The same holds when the new log's start is not a whole record and more
follows it, as when its first page never reached the disk while later
pages did (zeros after its header, records after them): its records are
not replayed, and it is kept aside. A new log whose name never reached the
disk (`appendonly.aof.rewrite.tmp`, holding records, beside an
`appendonly.aof.prev` and no log) is put in place and replayed when the old
log matches its record, and kept aside otherwise.

An old log that ends in a record cut short has that record left out of
the replay, whatever the new log says, and cut off the file after its last
whole record before anything else is written: a later checkpoint may add
the log's records to the old log, and they must not follow half a record.
When the new log records where the old log ended, that record is first
rewritten in place to the new end, so a stop between the two still finds
the old log matching it. A power failure that tears that rewrite (the
record then holds digits of both ends) reads as a new log that does not
match the old one: the new log is kept aside, on disk, and not loaded.

To make that window short, the checkpoint syncs the log just
before it turns it over, with nothing held: the old log's last sync then
has only the writes made in between to write. Under `appendfsync always`
it does not, since every reply's sync has already synced the log.

As for any log cut short, `aof-load-truncated no` changes nothing (the
server warns that it has no effect): the restart loads what is there, logs
what it leaves out, and keeps it.

A log written by 1.2.2 or earlier has no such record, and loads as it did
there: the old log, then the new one. Going back to 1.2.x after this
version has turned a log over: 1.2.x replays the record as an unknown
command and passes over it (its start logs `[WARN] Replication: Replicated
command error (non-fatal): -ERR unknown command 'KIVIDB.AOF-OLD-LOG-END',
with args beginning with: ...`, with the record's two values), and loads every
write. It does not count a log that holds nothing but the record as empty,
so a node stopped cleanly does not go on from its saved replication point:
a replica asks its master for a full sync, and a master's replicas are
sent everything again. The log on disk is not a Redis AOF (it starts with
`KIVIDLT1`), and nothing about Redis tools changes.

### A write to the log that the disk takes only part of

A write to `appendonly.aof` that fails part-way (a full disk) is taken
back: the file is cut to where it was before the write, and synced, so it
never holds half a record with more written after it. Its records, and all
that is queued after them, are kept in memory and written first, in order,
by the once-a-second sync, as soon as the disk takes them. Until then:

* commands that would be logged are refused, as Redis refuses them:
  `-MISCONF Errors writing to the AOF file: No space left on device` (the
  system's message). That covers writes, `EVAL`, `EVALSHA` and `FCALL`, and
  an `EXEC` whose transaction holds a write (`-EXECABORT Transaction
  discarded because of: MISCONF ...`). Reads, `PING` and read-only scripts
  are served. A script or transaction already running is not cut short.
  `MIGRATE` and `FT.DROPINDEX ... DD`, which take their own locks, are not
  refused: their deletes are held with the rest;
* the replies to the writes already held wait, as for a disk that is behind
  (at most 5 seconds, then sent without them and counted in
  `aof_acks_before_write`), and `WAITAOF` counts no sync;
* a checkpoint fails rather than turn the log over; so do `CONFIG SET
  appendonly yes` and a save with the AOF off, which record or replace the
  log, each with an error that says why;
* `INFO persistence` shows `aof_last_write_status:err`.

Once the disk takes them, writes are accepted again.

A replica keeps applying its master's stream, and holds those records
with the rest. A full sync drops what is held, as it is of the dataset the
sync replaces.

What is held is bounded: at most 256 MiB (the batch that failed, the writes
in flight when the refusal began, a replica's stream, and the deletes of
keys that expire or are evicted). Past that, the server stops rather than
hold more, or write the log with a hole in it. A write that cannot be taken
back stops the server too, as does any failed write under `appendfsync
always`.

At a shutdown, records still held are in memory only. The final save of a
`SHUTDOWN` fails (its checkpoint cannot turn the log over), the error says
how many bytes of writes are in memory only, and the server keeps running,
as for any final save that fails. `SHUTDOWN NOSAVE`, and a shutdown on a
signal, log that error and exit with status 1.

The snapshot such a checkpoint, or a save with the AOF off, was writing is
left in `dir` as `dump.kdb.<pid>.<n>.tmp`, as big as `dump.kdb`. Nothing
reads it. The next start removes those files, once it holds the data
directory's lock (`kividb.lock`) and before it saves anything, and a
`[WARN]` names each one. A start that could not take the lock leaves them.

## How recovery works

On startup, `dump.kdb` (or `dump.rdb`) is loaded first if it is present.
Then, with the AOF on and `appendonly.aof` not empty, the AOF is replayed
**on top of that snapshot**. This applies to replicas too: a replica starts
from its own files, then syncs with its master.

* A log is replayed only over the snapshot it goes on from. `dump.kdb.gen`
  holds the snapshot's save number and a fingerprint of the `dump.kdb` it
  was written for (its length and a hash of its first and last 64 KiB). A
  delta whose save number is older than the snapshot's is **left out**: its
  writes are already in the snapshot, and replaying it would apply them
  twice. So is a delta whose save number is the gen file's own while the
  fingerprint is not `dump.kdb`'s: that snapshot was replaced since, for
  example by a save with the AOF off in an earlier release. A log that is
  left out is not deleted: see [Logs kept aside](#logs-kept-aside).
* A save with the AOF off (`SAVE`, `BGSAVE`, the save at `SHUTDOWN`) gives
  its snapshot a save number past every log's, marks it as one no log goes
  with, and records how long `appendonly.aof` and `appendonly.aof.prev` were
  then, with a hash of their first and last bytes. It leaves the AOF's files
  alone. A legacy log (no `KIVIDLT1` header) beside such a snapshot is left
  out only if it is still as it was at that save. One that has grown since
  holds writes made after the save, on top of it: it is replayed, as before.
  A delta with an older save number that has grown since that save is left
  out up to its length then, and what was added after is replayed: the log
  starts again on the snapshot with those records. Releases before 1.2.1
  add to such a log when they are started with the AOF on. The save also
  records the save number in each log's header, so that:
  * an `appendonly.aof.prev` that was there at the save and has grown
    since, because a checkpoint of an earlier release added the log's
    records to it, is replayed only from where the log's records of the
    save end: those are checked against what was recorded, and the snapshot
    holds them. If they do not match, `appendonly.aof.prev` is left out
    whole, and kept aside;
  * an `appendonly.aof` that has grown since, beside an
    `appendonly.aof.prev` that has not, has what was added replayed.
    Beside such an `appendonly.aof.prev`, the log is judged on its own: a
    legacy log that has changed since the save, and a log made after the
    save when there was none (an earlier release makes one on the next
    save number for an `appendonly.aof.prev` with no log beside it), are
    replayed whole, and `appendonly.aof.prev` is kept aside.

  The two kinds of log are treated differently on purpose. A legacy log
  that has grown is replayed whole, as the earlier release replayed it at
  its start, so its records from before the save can be applied twice; it
  has no header to say where the save was, and leaving it out would lose
  the writes added to it. An `appendonly.aof.prev` the earlier release
  replayed at its start is not replayed again: it is unchanged since the
  save, its records are older than the snapshot, and nothing added to it
  is lost by leaving it out.

  One case is left as it was: if an earlier release has since written
  `dump.kdb.gen` itself (a checkpoint of its own that stopped before its
  snapshot was in place), nothing recorded at the save is left, and
  `appendonly.aof.prev` and `appendonly.aof` are replayed whole on the
  snapshot, as that release replays them. Records the snapshot already
  holds can then be applied twice. Do not go back to an earlier release
  with the AOF on after a save with the AOF off.
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
* A legacy AOF (one that starts with `*` rather than `KIVIDLT1`) comes only
  from an earlier release: every log KiviDB starts now has a header. It is
  still replayed on top of the snapshot, with a warning: it is a full
  history, so `INCR` or `RPUSH` can be applied twice. Run `SAVE` once the
  server is up to rebase it. Beside a snapshot saved with the AOF off, it is
  left out instead if it has not changed since that save (above).

A data file that is present but cannot be read stops the server; it does
not start empty. [ERRORS.md](ERRORS.md) lists each of these messages and
what to do.

Replay runs each command as the replica applier does, so a replayed write is
neither sent to replicas nor appended again to the file it came from.

### Logs kept aside

A log that recovery leaves out, or that `CONFIG SET appendonly yes` starts
again from, may hold acknowledged writes that are in no snapshot. It is
never deleted. Before anything replaces it, it is kept in `dir` under a
name of its own, `superseded.<unix-ms>.<n>.appendonly.aof` (or
`….appendonly.aof.prev`): a hard link where the filesystem allows one, or
a synced copy. The directory is synced before the log is replaced. A copy
that fails part way is removed, so nothing partial is left under that name.
A log that is already kept under such a name, byte for byte (by an attempt
that could not remove it), is not kept a second time.

A `[WARN]` names each file when it is kept, and says whether its writes
are in the dataset. They are when `CONFIG SET appendonly yes` keeps the log
this server had open (it made or replayed them), and for the part of a log
that was added to after a save with the AOF off, which was just replayed:
the warning then says not to send them to the server, which would apply
them twice. Otherwise it says how to apply them. Every start lists the
kept files still in `dir`. The server never removes them, and never
replays them.

To check for them, look at `INFO persistence`:

* `aof_superseded_files`: how many are in `dir`;
* `aof_superseded_bytes`: their size. A hard link shares its blocks with
  the log it was made from until that log is replaced, so this can count
  bytes that are also in `appendonly.aof`.

Or list them: `ls -l <dir>/superseded.*`. The Docker image has no shell.
On Kubernetes, use a debug container that shares the server's processes:

```sh
kubectl debug -it <pod> --image=busybox --target=<container> \
  --profile=general --container=kividb-debug -- sh
# in it (the server is process 1 of its container):
ls -l /proc/1/root/<dir>/superseded.*
```

The server runs as uid 1000. With no `runAsUser` on the pod the debug
container runs as root, and reading another user's `/proc/1/root` then
needs `CAP_SYS_PTRACE`, which `--profile=general` adds. Where the cluster's
policy refuses that, run the debug container as uid 1000 instead (with
`--custom` and a `securityContext` of `runAsUser: 1000`). To copy a kept
file off the pod, while that debug container runs:

```sh
kubectl exec <pod> -c kividb-debug -- cat /proc/1/root/<dir>/<file> > <file>
```

The files are on the pod's volume and count against its size.

Never send a file whose warning says its writes are in the dataset. To
apply another kept file's writes, send them to the server: `tail -c +17
<file> | redis-cli --pipe` for one that starts with `KIVIDLT1` (the first 16
bytes are its header), `redis-cli --pipe < <file>` for one that starts with
`*`.
Its records may include writes the snapshot already holds, and an `INCR` or
`RPUSH` among those is applied again: read the file first (it is plain RESP
after the header) and send only what is missing. Then remove the file. A
file you do not need can be removed at any time, with the server running:
`rm <dir>/superseded.*` (in a debug container as above,
`rm /proc/1/root/<dir>/superseded.*`).

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

The moment comes from the command's one reading of the clock, as Redis's
`commandTimeSnapshot`: a command reads the clock the first time it needs
it and goes by that time to the end (a transaction by one time for its
whole `EXEC`, a script by one for its whole run). Every key it looks at
is live, or past its time, for the whole command. A write on a master
deletes the keys it names that are past their time first, and records
their `UNLINK` ahead of itself; what it then reads is what that look
found, so the record replays to what the master stored.

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
| `aof_current_rewrite_time_sec` | Whole seconds the checkpoint running now has taken, `-1` when none is. |
| `rdb_current_bgsave_time_sec` | Whole seconds the `BGSAVE` running now has taken, `-1` when none is. |
| `latest_fork_usec` | Always `0` (in `INFO stats`): a save and a checkpoint run in this process, and no fork is made. |
| `aof_last_bgrewrite_status` | `err` if the last checkpoint failed. Durability is intact, but the log keeps growing. |
| `aof_lost_records` | Non-zero means the log has a hole and can no longer reconstruct the dataset. |
| `aof_acks_before_write` | Replies to writes sent after waiting 5 s for their records to reach the file, without them: the disk fell more than 5 s behind. A process killed then can lose those writes. 0 on a disk that keeps up (see [When a write is acknowledged](#when-a-write-is-acknowledged)). |
| `aof_superseded_files` | Logs kept aside in `dir` (`superseded.*`), which the server never removes (see [Logs kept aside](#logs-kept-aside)). |
| `aof_superseded_bytes` | Their size in bytes. |
| `rdb_last_save_raw_size` | The last snapshot written to disk (`dump.kdb`, by a save, a checkpoint or the shutdown): the bytes of its image. 0 before the first. |
| `rdb_last_save_stored_size` | The bytes it took on disk: fewer than the image with `rdbcompression yes`, the same with `no`. |

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
| time series, tag 17 | tag 14's series, then whether it is a compaction of another series (and which), then its `IGNORE` settings: the time and the value difference. Written only for a series with `IGNORE`; 1.2 refuses it |
| probabilistic value, tag 40 | a kind (1: Bloom filter, 2: Count-min sketch, 3: Top-K, 4: Cuckoo filter, 5: t-digest), the version of that kind's encoding, then the value: a Bloom filter's parameters and each layer's bytes; a Count-min sketch's width, depth and count, then its counters; a Top-K sketch's k, width, depth, decay and generator, its buckets, then each kept item and its count; a Cuckoo filter's `CF.SCANDUMP` header, then each sub-filter's bytes; a t-digest's compression, counts and extremes, then its merged and unmerged centroids |
| alias section (`KIVIAL1\0`) | `FT.ALIASADD` aliases, so that they survive a restart |
| field options section (`KIVIFO1\0`) | each index's `STOPWORDS` and each field's options (`AS`, `SORTABLE`, `WEIGHT`, `SEPARATOR`, `CASESENSITIVE`, …), as the words `FT.CREATE` takes |
| synonyms and dictionaries section (`KIVISD1\0`) | each index's synonym groups (`FT.SYNUPDATE`) and every dictionary (`FT.DICTADD`) |
| function libraries section (`KIVIFN1\0`), written last | every library loaded with `FUNCTION LOAD`, as a `FUNCTION DUMP` payload, so that libraries survive a restart and reach a replica in a full sync. A build without Lua loads the rest of the snapshot and leaves them out |
| document order section (`KIVIDO1\0`), after every other section | for each index with TAG, NUMERIC or TEXT fields, its documents' keys in the order they were written: the order `SORTBY` gives documents with equal values, and a query without `SORTBY` matches that score the same. A snapshot without it (1.2's) gives the documents the order of their keys; documents written after the snapshot, replayed from the AOF, come after those it lists, in the order of their writes |

An older release stops reading at the first section it does not know, so
these come after everything it does. A snapshot written by this version
cannot be read by an older one once it contains any of these, but for the
document order section: an older release reads everything before it, and
gives the documents its own order.

## Compressed snapshots

With `rdbcompression yes` (off by default in 1.3, so that a rollback to
1.2 works), every snapshot is written
compressed: `dump.kdb` (by `SAVE`, `BGSAVE`, a checkpoint of the AOF and the
shutdown), the base of an online backup, and a full sync to a replica that
reads it. The image is the plain one, byte for byte, cut into blocks of
256 KiB, each compressed with zstd (level 1) or, if it does not shrink,
stored as it is. Each block carries its length and a checksum (xxh3) of what
it holds, and the file ends with a marker that gives the number of blocks
and the length of the image. The file starts with `KIVIDBZ1`, the plain one
with `KIVIDB01`.

The blocks are compressed on up to two compression threads (the CPUs less
one, started at the first compressed save and shared by every save), never
on the threads that serve clients, and no more than a few blocks are in
memory at once. The log line of each save gives the image's size, the
size written and the ratio.

Loading reads both forms, told apart by the first bytes, and a compressed
one a block at a time. A compressed snapshot that is cut short anywhere,
has no end marker, has a block whose checksum or length does not match, or
has bytes after its end is refused as a whole, with the block named
(`compressed snapshot: block 3 (at byte 524321) does not match its checksum:
it is damaged`): it is never loaded in part. At startup that stops the
server, and leaves its directory as it is, as for any snapshot that cannot be
loaded.

A full sync is compressed only for a replica that says it reads it (`REPLCONF
capa kdb-blocks`, which this version's replicas send). A replica of 1.2 or
earlier, or of Redis, is sent the plain image. A master with `rdbcompression
no` sends the plain image to every replica.

### Going back to 1.2

With the default, `rdbcompression no`, there is nothing to do: 1.3 writes
the plain snapshots 1.2 reads. After `rdbcompression yes`, KiviDB 1.2 and
earlier refuse a compressed snapshot (`bad magic`) and do not start on it;
nothing is changed on disk. To hand a snapshot to one:

* `CONFIG SET rdbcompression no`, then `SAVE` (with the AOF on, this writes
  the AOF's base plain too); or set `rdbcompression no` in the config file
  before the last shutdown;
* or convert the file, server running or not:

```bash
kividb kdb-decompress dump.kdb dump-plain.kdb   # what 1.2 reads
kividb kdb-compress dump-plain.kdb dump.kdb     # and back
```

An online backup's BASE (`appendonly.aof.N.base.kdb`) is a snapshot like
any other: taken by 1.3 with `rdbcompression yes`, 1.2 cannot
restore it (`--preload-file aof:...` refuses it with `bad magic`). Convert
the BASE with `kividb kdb-decompress` (and name the plain file in the
manifest's place of the old one), or take the backup with `rdbcompression
no` set.

What the two tools check, before they write anything:

* the input is read through as the server loads a snapshot: every key and
  value parsed, every section after the databases, and for a compressed
  one every block's length and checksum, its end marker and that nothing
  follows it. An input the server would refuse is refused (exit 1, a
  message saying why, no output file). The keys are not kept: memory stays
  small whatever the file's size;
* `kdb-compress` takes a plain snapshot only, `kdb-decompress` a compressed
  one only; the other form is refused (a plain snapshot needs nothing to be
  read by 1.2);
* `kdb-compress` refuses a snapshot written by KiviDB 1.2.0 with the empty
  key in a database before its last (#441): the server reads such a file
  a second way to load it, which it does not do for a compressed snapshot.
  Start a server on it and `SAVE`, then compress what it writes;
* what is written is read through again the same way before it is put in
  place.

The output goes through a temporary file beside it, synced, renamed into
place, and its directory synced. An output that exists is not replaced
(exit 1) unless `--force` comes before the files (`kividb kdb-decompress
--force <in> <out>`). The tools start no server and need no shell
(`["kividb", "kdb-decompress", ...]`).

## The cluster's nodes.conf

In cluster mode a node keeps what it knows of the cluster in
`cluster-config-file` (`nodes.conf` in `dir`): its own id, every member's id,
address, hostname, role and shard, who owns which slots, and the epochs. The
format is Redis's, one line a node and a last `vars` line.

**When it is saved.** On every change of topology: a member learnt or
forgotten, a role, an address, slots, an epoch, a vote. Changes a command
makes (`ADDSLOTS`, `REPLICATE`, `FORGET`, `SAVECONFIG`, ...) are saved
before the command is answered, and a vote before it is sent. Changes
learnt from the bus are saved on a thread of their own as they are learnt
(those of 100 ms in one save, so that a cluster being formed does not keep
its disk busy), except a node met by `CLUSTER MEET`: both ends of the handshake save the
other before they show it as a member, and the receiving end before it
answers. So once the node you sent `CLUSTER MEET` to lists the new node in
`CLUSTER NODES` without the `handshake` flag, both have it on disk. Nodes
met at the same time are saved together, in one write. The other members
learn it by gossip, within a few heartbeats, and save it then. A node also
saves the file at the start of a shutdown.

If the file cannot be saved (a full or read-only disk), the node logs it
once, as an error, until a save works again. Meanwhile it makes no node
met a member and does not answer its MEET (the end that sent it tries again,
for up to two minutes, and again whenever the other end calls on it within
the hour after), and it casts no vote. A replica that wins an election saves
its promotion before it announces it, waiting for the save up to a second.
One not saved by then (a slow disk, a failed save) takes over all the same
(the cluster has agreed to it), and the promotion is saved in the
background, made again every second until the save works.

A node stopped before that save was done starts again from the file of
before: a replica of the old master. It completes the promotion from what
the masters report: a master that holds it as the owner of slots says so in
its answers to its heartbeats, with the config epoch and the number of
masters that own slots in its view. Once a majority of the masters that own
slots (of the greater of that number and its own count) say it owns every
slot of its old master at a config epoch newer than any it knows for those
slots, it completes the promotion at that epoch, within a heartbeat or two
of starting: it takes exactly those slots, saves the promotion as above, and
serves them with the dataset it has. With the AOF on, that is everything it
had acknowledged. Without it (and no snapshot from a clean shutdown), the
node serves none of them until a replica that holds them takes over or its
replicas answer that they hold nothing either, as a master back without its
dataset does; with no replica, it serves what it has. A node that starts as
a replica begins its first sync once enough of the masters have answered
that it is not to complete a promotion (with three masters, two of them), or
a node timeout after it started, and not at all when it is to complete one.
It does not take the slots if another node owns them at a newer epoch (a
replica elected since, a `CLUSTER FAILOVER TAKEOVER`): it follows that one.
A master that voted for another replica to replace the node says nothing of
it while that election may still be won. Nodes of 1.2 among the masters say
nothing of it, so the majority has to be made of masters of a later release.

A filesystem that cannot sync a directory at all (some FUSE, CIFS and 9p
mounts answer `EINVAL` or `ENOTSUP`) does not fail the save: the node warns
once, and the rename is as durable as that filesystem makes it.

**A cluster stopped whole.** A member that at least one node has in its
`nodes.conf` is a member again after the cluster restarts: that node tells
the others of it by gossip, and the member, from the addresses in its own
file, finds the rest. One that no node has (a node killed before it ever
showed it) is not: a node that is not a member cannot join by sending
heartbeats. It has to be met again with `CLUSTER MEET`, as in Redis. That
rule keeps a process that can reach the bus port from making itself a
member. A tool that adds nodes and then may restart the whole cluster
(an operator, a script) should wait until the node it sent `CLUSTER MEET` to
lists the new node without `handshake`; to have every member save it, wait
until each one lists it, and send each `CLUSTER SAVECONFIG`.

**How it is written.** To a temporary file beside it
(`nodes.conf.tmp-<n>`), synced, put in place of it, and the directory
synced: a crash at any moment leaves the old file or the new one, never
part of one. nodes.conf itself is never written over in place. On Linux the
file a save puts out of place is kept as a spare (`nodes.conf.spare-<n>`,
two at most) once the directory is synced, when it is no longer nodes.conf
on disk either, and a later save writes over it: renamed to that save's
temporary file, written, its data synced (`fdatasync`), exchanged with
nodes.conf (`renameat2` with `RENAME_EXCHANGE`, as atomic as a rename), and
the directory synced. A spare's blocks are allocated already, so syncing
its data needs no commit of the filesystem's journal: a save costs one
commit (the directory's) where a new file costs two. A symbolic or hard
link to nodes.conf is never written through: put out of place, it is
removed. On a filesystem that cannot exchange two files or sync a
directory, and on other systems, every save writes a new file. On Linux it
is written past the page cache, padded with blank lines to a multiple of
4 KiB. Temporary files and spares are removed at startup. The one
exception is the takeover at the end of a slot
migration's handover, which does not wait for the sync while client writes
are paused: a power failure in the moment after it can leave the file
incomplete.

**Failover needs a disk that syncs it quickly.** A master saves its vote,
synced, before it sends it, and a replica its promotion, so a failover
waits on several saves, each two syncs of the disk (the file's data and
the directory). A candidate counts a vote for as long as its election lives,
10 seconds, and then stands again at a new epoch: a vote whose save takes
longer than that is never counted, and the failover does not complete.
Measured on an Azure D4s_v5, ext4 on a volume that delays every write:
with writes taking 200 ms, a save took 2.1-2.3 s and a failover of a master
4.5-6.0 s (the promotion announced after its 1 s wait, its save still under
way); with writes taking 1 s, a save took 8-13 s and no failover completed
in 330 s. On the VM's own disk a save takes about 20 ms and a failover
3.5-4.1 s.
A volume on which a save of nodes.conf takes more than a few seconds is too
slow for a cluster to fail over: give the cluster file a disk that syncs a
small file in well under a second.

**A file cut short is refused.** A `nodes.conf` that does not end with its
`vars` line stops the node at startup with `cluster config file
'nodes.conf' is unreadable: nodes.conf is incomplete ...`, and the file is
left as it is: such a file may lack members, slots and the epochs. Restore
the file from a backup; if there is none, remove it and add the node to the
cluster again as a new one.

An empty `nodes.conf` (zero bytes) is refused too (`nodes.conf is empty`).
Redis takes an empty file for a new node. To start a node
afresh, with a new id, delete the file rather than emptying it.

## When a write is acknowledged

With the AOF on, the replies to a batch of commands that wrote are sent once
their records have been written to the file (the operating system's page
cache), as in Redis. A process killed outright (`kill -9`, the OOM killer)
loses no acknowledged write while the disk keeps up (see below); a power
failure can lose up to a second, as with `appendfsync everysec` in Redis,
since the file is synced once a second. Connections waiting at the same
time are answered by one write (a group commit), and reads wait for
nothing. Before 1.2 a write was acknowledged once its record was queued,
and a killed process could lose the last few.

With `appendfsync always`, a reply waits until its records are synced to
disk, not only written to the file, so a power failure loses no
acknowledged write either. Connections waiting at the same time share one
sync. A reply waits for its sync however long the disk takes (the 5-second
limit below does not apply), `aof-ack-after-write no` has no effect, and a
write or sync that fails stops the server, as in Redis. Before 1.2.1
`always` was accepted and had no effect. A replica takes no writes of its
own: it syncs its master's stream once a second, whatever `appendfsync`
says.

A reply waits at most 5 seconds for its records. Only if the disk falls
more than 5 seconds behind (a disk slower than the writes it is given, or
one busy with something else) is the reply sent without them: the server
logs `AOF: a reply waited 5 s for its write to reach appendonly.aof and was
sent without it`, and `aof_acks_before_write` in `INFO persistence` counts
each one. A process killed right then can lose those writes. On a disk that
keeps up the counter stays at 0. If it grows, give the AOF a faster disk
(more IOPS and throughput: on a cloud, a premium or provisioned volume
rather than a small standard one), or write less to it.

A checkpoint does not hold writes back while it waits for the disk: its
snapshot goes to the disk a few megabytes at a time rather than all at once
when it is synced, and the old log is synced after it is turned over, not
with the log locked. Before 1.2.0 the checkpoint after a bulk load stalled
writes for seconds at a time on a slow cloud disk (#243).

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
