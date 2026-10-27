# Errors and warnings

What KiviDB's log lines and error replies mean, and what to do about them.
Search this page for the fixed words in a message. The parts in braces,
such as `{e}`, vary.

- [Server log](#server-log): `[FATAL]`, `[ERROR]` and `[WARN]` lines.
- [Error replies](#error-replies): what a client is told, for example
  `OOM`, `BUSY`, `NOAUTH`.

[LOGGING.md](LOGGING.md) covers log levels, formats and where the log goes.

**Urgency** in the tables below:

- **Now:** the server is down, or data is not being saved or replicated.
- **Soon:** the server works, but something will go wrong if it is left alone.
- **Note:** expected after the event it describes; act if it keeps happening.

---

## Server log

### Startup: `[FATAL]`

The server exits with status 1 after each of these. Nothing has been changed
on disk.

| Message | Meaning | What to do |
|---|---|---|
| `Bad directive or wrong number of arguments — {e}` | A line in the config file is not understood. `{e}` names the file, line and directive. KiviDB reads its config file strictly: a typo stops startup, rather than being ignored. | Fix or remove the line. Redis directives KiviDB has no use for are accepted and ignored; a misspelt one is not. |
| `Can't chdir to '{dir}': {e}` | `dir` does not exist or is not accessible. | Create the directory, or fix `dir` and its permissions. |
| `invalid tls-auth-clients '{value}' (expected yes, no or optional)`, `--update-check-url must be an https:// URL…`, `--tls-port … requires both --tls-cert-file and --tls-key-file`, `--tls-port … was given, but this binary was built without TLS support…` | The options contradict each other, or ask for something this build does not have. | Do as the message says. The `-tls` and `-full` builds have TLS. |
| `Can't open the log file '{path}': {e}…` | `logfile` cannot be opened for appending. | Create the directory, and give the server's user write permission, or unset `logfile`. |
| `Can't listen on {addr}: {e}. …` | The client port cannot be opened. The message ends with the likely cause: the port is already in use (often another KiviDB or Redis), it is below 1024 without privileges, or the bind address is not on this machine. | Stop the other process or change `port`; use a port at or above 1024, or grant `CAP_NET_BIND_SERVICE`; fix `bind`. |
| `Can't load the ACL file '{path}': {e}. …` | `aclfile` is missing or has a line KiviDB does not understand. `{e}` gives the line number. | Fix the line, or start without `aclfile`. |
| `TLS: …` | The certificate or key cannot be read, or they do not match; or `tls-ca-cert-file` is missing, unreadable or holds no certificate. | Check `tls-cert-file`, `tls-key-file` and `tls-ca-cert-file`: paths, permissions, PEM format, and that the key belongs to the certificate. |
| `dump.kdb cannot be loaded: {e}`, `dump.rdb cannot be loaded: {e}`, `appendonly.aof cannot be read: {e}`, `appendonly.aof cannot be loaded: {e}`, `appendonly.aof exists but cannot be examined: {e}`, `appendonly.aof cannot be opened for appending: {e}`, then `Refusing to start with an empty dataset in place of one that could not be loaded…` | A data file is there but cannot be read. KiviDB refuses to start rather than start empty: an empty node accepts writes, its replicas copy the empty dataset, and the next save overwrites the only copy. Redis behaves the same. | **Now.** Check the file's permissions and the disk. If the file is damaged, restore it from a backup. If you really mean to start empty, move the file out of `dir`; do not delete it. |
| `appendonly.aof continues from save number {n}, but dump.kdb is save {m}…` | The AOF and the snapshot belong to different points in time, so the writes in between are in neither. | **Now.** Restore a matching pair from a backup, or decide which file to keep and move the other away. |
| `appendonly.aof ends in a partial record that could not be cut off: {e}` | The AOF ends mid-record (normal after a crash), but the file cannot be truncated to remove it. | Check the file's permissions and the disk. |
| `preload-file '{path}' could not be loaded: {e}` | `preload-file` names a file that is missing or not a dataset KiviDB reads. | Fix the path, or remove `preload-file`. |
| `… the snapshot's function libraries cannot be loaded: {e}` (after `dump.kdb cannot be loaded`, `dump.rdb cannot be loaded` or `preload-file … could not be loaded`) | The file holds function libraries, and one does not load in KiviDB: `{e}` says why, such as code that does not compile, a first line other than `#!lua name=<library>`, or two libraries registering one function. KiviDB refuses the file rather than start without them. | Fix or delete that library where the file came from (`FUNCTION DELETE` in Redis, then save again), or start from a file without it. |

### Persistence: `[ERROR]` and `[WARN]`

| Message | Urgency | Meaning | What to do |
|---|---|---|---|
| `AOF: write to appendonly.aof failed: {e} — …` | Now | Writes are acknowledged to clients but not written to the AOF. Logged at most every 10 s. | Free disk space, or fix the data directory's permissions. When the disk is fixed, run `BGREWRITEAOF` so the AOF matches the dataset again. |
| `AOF: fsync of appendonly.aof failed: {e} — …` | Now | Writes reached the OS, but are not guaranteed to be on disk. A power failure can lose them. | Check the disk (`dmesg`, SMART). Some network filesystems refuse fsync; keep the data directory on local storage. |
| `AOF writer unavailable — write records are being lost…` | Now | The AOF writer has stopped. The AOF no longer describes the dataset. | Restart the server, after taking a snapshot (`SAVE`), and check the log before this line for the cause. |
| `Background save failed: {why}. …` | Now | `BGSAVE` (or an automatic save) failed. The previous `dump.kdb` is kept, `LASTSAVE` does not move, and `INFO persistence` shows `rdb_last_bgsave_status:err`. | Usually disk space or permissions on `dir`. Run `BGSAVE` again after fixing it. |
| `Final snapshot failed: {e}. …` | Now | Shutdown could not write `dump.kdb`, and the process exits with status 1. Writes since the last good save are only in the AOF, if `appendonly` is on. | Fix the disk before starting again. With the AOF on, nothing is lost: it is replayed at startup. |
| `Snapshot task panicked: {e}` | Now | A bug: the shutdown save crashed. | Report it with the log. The previous `dump.kdb` and the AOF are untouched. |
| `AOF ended with a partial record (likely an unclean shutdown); the incomplete tail ({n} bytes) is cut off` | Note | The server or the machine stopped mid-write. The last, incomplete command is dropped: it was never acknowledged to a client. | Nothing, unless it happens without a crash. Then check the disk. |
| `{n} AOF command(s) failed to apply and were skipped` | Soon | Commands in the AOF failed on replay, for example a command a newer version wrote that this version does not know. | Check that you did not downgrade KiviDB. Compare key counts with what you expect. |
| `appendonly.aof is a full-history log, not a delta. Replaying it on top of dump.kdb can double-apply INCR/RPUSH/APPEND…` | Soon | An AOF from before KiviDB's snapshot-plus-delta scheme, found next to a snapshot. | Run `SAVE` once the server is up: it rebases the AOF. |
| `AOF: the snapshot of the checkpoint could not be written: {e} — …`, `AOF: auto-checkpoint failed: {e}` | Soon | The AOF could not be rebased onto a new snapshot, so it keeps growing. The previous log is kept and replayed on the next start. | Disk space and permissions on `dir`. |
| `snapshot index section is unreadable ({e})…`, `snapshot HNSW section is unreadable ({e})…`, `snapshot index alias section is unreadable ({e})` | Soon | Part of `dump.kdb` that holds search indexes is damaged. The keys are loaded; indexes are rebuilt from them. As the message says, key expiries and function libraries stored after that section are lost. | Run `SAVE` once the server is up to write a whole snapshot. Check TTLs on keys that need them, and `FUNCTION LIST`. |
| `the snapshot's function libraries {names} are not loaded: this build was made without Lua…` | Soon | `dump.kdb`, a backup, a Redis `dump.rdb`, a `--preload-file` or a master's full sync holds function libraries, and this build has no Lua. The rest is loaded; the libraries are not, and the next snapshot leaves them out. | Run a build with Lua (`lua` or `full`) on this data, or load the libraries again where Lua is. |
| `RDB: unknown type byte {b} — stopping load` | Soon | A Redis `dump.rdb` holds a type KiviDB does not read, such as a Redis module's type. Keys after it are not loaded. | Load the data with a migration tool that skips module types, or remove those keys in Redis first. |
| `RDB: the search module's index definitions and dictionaries are not loaded…` | Note | A Redis `dump.rdb` (or a Redis primary's full sync) holds index definitions or `FT.DICTADD` dictionaries from the search module. KiviDB keeps indexes of its own, so it loads the keys and leaves those out. | Run `FT.CREATE` and `FT.DICTADD` again. `FT.CREATE` indexes the keys already there. |
| `RDB: … (opcode 0x…) is not supported…`, `RDB: data of module type {name} (opcode 0xF7) is not supported`, `RDB: unknown opcode {b}` (in `dump.rdb cannot be loaded: {e}` or a full sync's error) | Now | A Redis `dump.rdb` holds something KiviDB does not read yet: hash templates, module key metadata, or another module's own data. It refuses the file rather than load part of it. | Hash templates and module key metadata can't be moved yet. For another module's data, move the keys with `MIGRATE` instead. |

### Memory, clients and scripts: `[WARN]`

| Message | Urgency | Meaning | What to do |
|---|---|---|---|
| `maxmemory ({n} bytes) reached and nothing more can be evicted under maxmemory-policy {policy}: …` | Now | Writes are refused with `OOM`. Reads still work. Logged at most once a minute. | Raise `maxmemory` (`CONFIG SET maxmemory 4gb`). For a cache, use an evicting policy (`allkeys-lru`). For a queue or store (BullMQ, Sidekiq), keep `noeviction` and add memory, or find what is growing (`MEMORY USAGE`, `--bigkeys`). |
| `A script has run for more than lua-time-limit ({ms} ms): other clients now get BUSY…` | Now | A Lua script or function is still running. Scripts run alone, so every other client waits, and is now told `BUSY`. | `SCRIPT KILL` (or `FUNCTION KILL`) stops it if it has not written anything. Otherwise only `SHUTDOWN NOSAVE` does, losing writes since the last save. Then fix the script: look for unbounded loops and `KEYS`-like scans. |
| `Client id={id} closed: it is subscribed and {n} bytes behind, over the pub/sub output buffer limit` | Soon | A subscriber read too slowly, and its unread messages passed `client-output-buffer-limit pubsub`. It was disconnected so it could not exhaust memory. | Make the subscriber faster: the limit is Redis's default (32 MB, or 8 MB for 60 s) and cannot be changed. The subscriber has missed messages. Pub/sub does not redeliver; use streams if it must not. |
| `Accept error: {e} — …` | Now | New connections are failing. Most often `Too many open files`. Logged at most every 10 s. | Raise the open-file limit (`ulimit -n`, or `LimitNOFILE=` in systemd) well above the number of clients you expect, or close idle connections: an oversized connection pool is the usual cause. |
| `An io_uring worker failed to start: …` | Soon | The kernel, or a seccomp profile such as Docker's default, does not allow io_uring. | Start with `--io-model tokio`, or allow io_uring (for Docker: `--security-opt seccomp=unconfined`, or a profile that allows the io_uring calls). |
| `TLS: accept error: {e}`, `TLS: bind failed on io_uring binary path: {e}` | Soon | The TLS listener cannot accept connections, or cannot listen on `tls-port`. | As for `Accept error` and `Can't listen` above, on `tls-port`. |
| ``Index `{name}` missing after registration — skipping backfill``, `HNSW compaction skipped for '{index}' field '{field}': {e}` | Soon | A search index could not be rebuilt or compacted. Queries on it may miss documents or run slower. | `FT.DROPINDEX` and `FT.CREATE` it again. Report it if it repeats. |

### Replication: `[ERROR]` and `[WARN]`

| Message | Urgency | Meaning | What to do |
|---|---|---|---|
| `Replication: FULL RESYNC REFUSED from {master}: {reason}. …` | Now | This replica was about to replace its dataset with a much smaller one from the master (for example an empty master that restarted without its data). It kept its data and is **not** replicating. | If the master is wrong, promote this node: `REPLICAOF NO ONE`. If the master's data really is the one to keep: `CONFIG SET replica-full-sync-guard off` on this replica; it retries on its own. |
| `Replication: Replication link to {master} failed: {e} — retrying every {n}s` | Soon | The replica cannot reach its master, or lost it. It keeps retrying, and serves possibly stale data meanwhile. | Check that the master is up, the address and port, firewalls, and `masterauth`. |
| `Replication: Master closed the connection — reconnecting` | Note | The master restarted or dropped this replica, for example over its backlog limit. | If it repeats, look at the master's log for `exceeded backlog limit`. |
| `Replication: Channel replica {addr} exceeded backlog limit ({n} > {limit} bytes) — evicting` (on the master) | Soon | A replica fell too far behind, and was dropped. It reconnects and needs a full sync. | Give the replica more network or CPU, or raise `repl-replica-max-backlog-bytes` (default 256 MB) on the master. |
| `Replication: Socket replica {addr} dropped: {e}`, `Replication: Channel replica {addr} disconnected (hub send failed)` (on the master) | Note | A replica's connection broke. It reconnects on its own. | Act only if it repeats: look at the network and the replica's log. |
| `Replication: Dropped the {n} local keys before loading {m} from the master` | Note | A full sync replaced this replica's dataset, as replication does. | Nothing, unless you did not expect this node to be a replica. |
| `Replication: Replicated command error (non-fatal): {e}` | Soon | A command from the master failed here. The replica may now differ from the master. | Compare `DBSIZE` and spot-check keys. If they differ, force a full sync: `REPLICAOF NO ONE`, then `REPLICAOF` the master again. |
| `Replication: post-sync AOF rebase failed (…); this replica's AOF does not match its dataset — …` | Now | After a full sync, the replica's AOF still describes the old dataset. | Run `BGREWRITEAOF` on the replica before relying on the AOF. |
| `Replication: index rebuild after full sync failed: {e}` | Soon | Search indexes on the replica may be incomplete. | `FT.DROPINDEX` and `FT.CREATE` them on the replica, or resync. |
| `Replication: Hub sender closed — replication cannot proceed`, `Replication: could not start replication runtime / thread: {e}` | Now | Replication has stopped inside this process. | Restart the server, and report it with the log. |

### Cluster: `[ERROR]` and `[WARN]`

| Message | Urgency | Meaning | What to do |
|---|---|---|---|
| `Cluster: Node {id} marked FAIL by quorum`, `Cluster: Node {id} marked FAIL (reported by {id})` | Now | A node is unreachable from most masters. If it was a master, one of its replicas takes over. | Find out why the node is down. When it comes back, `Node {id} is reachable again — clearing FAIL` is logged at `notice`. |
| `Cluster: Starting election for epoch {n} (failed master: {id})` | Note | This replica is trying to replace a failed master. `Won election…` (at `notice`) says it did. | Nothing. |
| `Cluster: Election at epoch {n} timed out with {v} of {q} votes — abandoning` | Soon | Not enough masters voted. The slots of the failed master stay unserved, so clients get `CLUSTERDOWN`. | Check that a majority of masters is up and can reach each other on the bus port (client port + 10000 by default). |
| `Cluster: Election: failed to ask {id} ({addr}) for vote: {e}`, `Cluster: Bus accept error: {e}` | Soon | Cluster bus traffic is failing. | Firewalls and the bus port between nodes. |
| `Cluster: Migration of slots {slots} did not complete: {error}` | Soon | A resharding stopped halfway. Some keys are on each side. | Run the migration again; `CLUSTER SHARDS` shows who owns each slot. |
| `Cluster: MEET {ip}:{port} failed: {e}`, `Cluster: Manual failover aborted: {e}` | Note | The command did not complete. | Check the address and that the other node runs with `cluster-enabled yes`. |
| `Cluster: could not start the bus runtime / bus thread: {e}`, `Cluster: Bus listener exited: {e}` | Now | This node cannot talk to the rest of the cluster. | Usually the bus port is in use. Restart, and check the port. |
| `Cluster: could not start the lost-slot cleanup thread: {e} — …` | Soon | Keys in slots that moved to another node stay here, unreachable, until the next restart. They use memory. | Restart the node when convenient. |

---

## Error replies

What clients are told. These are Redis's error codes, with Redis's meaning.
Client libraries raise them as exceptions; the first word is the code.

| Reply | Meaning | What to do |
|---|---|---|
| `ERR this build of KiviDB has no Lua…` | `EVAL`, `EVALSHA`, `SCRIPT`, `FUNCTION` or `FCALL` on the default or `tls` build, which have no Lua. Job queues such as BullMQ and Sidekiq run scripts, so they hit this first. | Run the `lua` or `full` variant: Docker image `kividb/kividb:<version>-lua` or `-full`, or the `kividb-lua-…` / `kividb-full-…` release file. |
| `ERR unknown command '{name}'…` | The command does not exist, or is not supported. [COMMANDS.md](../COMMANDS.md) lists what is. | Check the spelling. Of Redis's module commands, JSON, search (`FT.*`), time series (`TS.*`) and vector sets are available. |
| `ERR wrong number of arguments for '{cmd}' command` | As it says. | Check the command's syntax. |
| `ERR syntax error`, `ERR value is not an integer or out of range`, `ERR value is not a valid float` | An argument is malformed. | Check the arguments. |
| `ERR Protocol error: …` | The client sent bytes that are not RESP, and the connection is closed. | Usually the wrong port, TLS sent to a plaintext port (or the reverse), or HTTP. Logged at `verbose`. |
| `NOAUTH Authentication required.` | A password is set, and this connection has not authenticated. | Give the client the password (`AUTH`, or `HELLO 3 AUTH`). |
| `WRONGPASS invalid username-password pair or user is disabled.` | Authentication failed. | Check the user and password. `ACL LOG` shows recent failures with client addresses. |
| `DENIED KiviDB is running in protected mode ...` (then the connection is closed) | Protected mode: no `bind` address was given and the `default` user has no password, so only clients on the same machine may connect. | Set a password (`requirepass`, or `ACL SETUSER default`), give a `bind` address, or turn it off with `protected-mode no` where the network is trusted. The Docker image turns it off unless you pass arguments of your own. |
| `NOPERM User {u} has no permissions to run the '{cmd}' command` (or access the `{key}` key, or the `{channel}` channel) | The ACL user may not do this. | Grant it (`ACL SETUSER`), or use another user. `ACL LOG` records each refusal. |
| `WRONGTYPE Operation against a key holding the wrong kind of value` | For example `LPUSH` on a string key. | A key-naming clash between two parts of an application, usually. `TYPE key` shows what is there. |
| `OOM command not allowed when used memory > 'maxmemory'.` | Memory is full and nothing more can be evicted. Reads still work. | See the `maxmemory … reached` warning above. |
| `OOM allow-oom flag is not set on the script…` | A script that writes was refused because memory is full. | As above. |
| `BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.` (or `FUNCTION KILL`) | A script has run longer than `lua-time-limit`, and holds the server. | See the `lua-time-limit` warning above. |
| `NOSCRIPT No matching script. Please use EVAL.` | `EVALSHA` with a script the server does not have, for example after a restart or failover. | Normal: client libraries (BullMQ's ioredis, Sidekiq's redis-client) load the script and retry. Handle it if you call `EVALSHA` yourself. |
| `LOADING KiviDB is loading the dataset into memory` | The server is starting, or rebuilding search indexes after starting. | Retry with backoff. Wait for `Ready to accept connections`, and for the index rebuild to finish. |
| `READONLY You can't write against a read only replica.` | A write went to a replica. | Send writes to the master. After a failover, refresh the client's view of which node that is. |
| `MOVED {slot} {host}:{port}`, `ASK {slot} {host}:{port}` | Cluster: the key lives on another node. | Cluster-aware clients follow these on their own. A client that shows them to you is not in cluster mode. |
| `CROSSSLOT Keys in request don't hash to the same slot` | Cluster: one command, script or transaction named keys in different slots. | Put related keys in one slot with a hash tag: `{user:42}:profile`, `{user:42}:cart`. BullMQ does this with its `{queue}` prefix. |
| `CLUSTERDOWN The cluster is down`, `CLUSTERDOWN Hash slot {n} is not served` | Cluster: no node serves the slot. | See the cluster `FAIL` and election messages in the log. |
| `TRYAGAIN Multiple keys request during rehashing of slot` | Cluster: the slot is being migrated. | Retry after a short wait. |
| `EXECABORT Transaction discarded because of previous errors.`, `EXECABORT Transaction discarded because of: {why}` | A command inside `MULTI` was rejected when it was queued, so `EXEC` ran nothing. | Fix the command named. |
| `NOREPLICAS …` | `min-replicas-to-write` is set, and not enough replicas are connected. | Check the replicas, or lower `min-replicas-to-write`. |
| `BUSYKEY Target key name already exists.` | `RESTORE` without `REPLACE`, onto a key that exists. (`COPY` without `REPLACE` answers `0` instead.) | Add `REPLACE`, or delete the key first. |
| `BUSYGROUP Consumer Group name already exists`, `NOGROUP …` | `XGROUP CREATE` of a group that exists; a stream command naming a group that does not. | Normal for idempotent setup code (BullMQ and others ignore `BUSYGROUP`). Create the group with `MKSTREAM` if needed. |
| `UNBLOCKED …` | `CLIENT UNBLOCK … ERROR` ended a blocking command. | Expected, if something called it. |
| `IOERR …` | `MIGRATE` could not reach, write to or read from the target. | Check the target's address and that it is up. |
| `ERR DUMP payload version or checksum are wrong` | `RESTORE` of a payload that is damaged, or from a version whose format this one does not read. | Re-export the key. |
| `INVALIDOBJ Corrupted HLL object detected` | A HyperLogLog key holds bytes that are not a valid HLL. | Delete it and rebuild it. |
| `Timeout limit was reached` | `FT.SEARCH` or `FT.AGGREGATE` ran longer than `TIMEOUT` (the command's, or `FT.CONFIG TIMEOUT`, 500 ms) and `ON_TIMEOUT` is `fail` or `return_strict`. With `return` (the default), the reply is what was found instead — RESP3 puts this text in `warning`; over RESP2 the client gets no signal. | Raise `TIMEOUT`, pass `TIMEOUT 0` for no limit, or set `ON_TIMEOUT return`. |
