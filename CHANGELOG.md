# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security

- **The Docker image is distroless.** Its runtime base is
  `gcr.io/distroless/cc-debian12` (glibc, libgcc, CA certificates, time
  zones) instead of `debian:bookworm-slim`. Scanners reported 4 critical and
  62 high CVEs on the 1.1.0 image, all in Debian packages kividb does not use
  (perl, util-linux, zlib's minizip); the new image has none critical and
  one high, with no fix released. The server still runs as UID and GID 1000
  in `/home/kividb`, so existing volumes keep working. **The image no longer
  has a shell or `redis-cli`**: connect from another container
  (`docker run --rm -it --network container:<name> redis redis-cli -p 6380`)
  or with `kubectl debug`.
- rustls 0.23.45 (GHSA-2mjx-qc3c-rqvc: TLS 1.3 handshake messages accepted
  across encryption levels) and rand 0.8.6 (GHSA-cq8v-f236-94qc).
- Every release image is scanned with Trivy before it is published: a
  critical or high CVE that has a fix stops the release. The published images
  and `Cargo.lock` are scanned again every week, and the results go to the
  repository's code scanning alerts. Dependabot proposes new base images.

## [1.1.0]

- **KiviDB is open source, under the GNU Affero General Public License,
  version 3 (AGPL-3.0-only).** The KiviDB End User License Agreement no
  longer applies to this or later releases, and the startup banner names the
  AGPLv3. Using KiviDB, unmodified, as the database of an application does
  not put the application under the AGPL. KiviDB is also available under a
  commercial license (see the README). The
  entitlement check that builds for cloud marketplaces made at startup is
  removed, with the marketplace machine images.

1.1.0 supersedes 1.0.6, which was published only as release candidates
(1.0.6-rc1 to 1.0.6-rc4) and never as a final release. Everything in those
candidates is in 1.1.0 and is listed below. The release notes, written for
people upgrading, are in
[`docs/releases/v1.1.0.md`](docs/releases/v1.1.0.md).

### Security

- **Lua scripts stay in the sandbox.** `package`, `require`, `loadfile`,
  `dofile`, `debug` and `string.dump` are gone, and `load` takes text only, not
  bytecode. Before this, a script could load native code and run commands on
  the host.
- **A command a script runs is checked against the user's ACL.** Before, a
  user allowed `EVAL` could run any command on any key through
  `redis.call`.
- **Protected mode** (`protected-mode yes`, the default, as in Redis): with no
  `bind` address given and no password on `default`, only clients on the same
  machine may connect; others get `-DENIED` and are disconnected. It used to
  be ignored. The Docker image turns it off (`CMD ["--protected-mode", "no"]`)
  unless you pass arguments of your own. **Upgrading:** a server reached over
  the network with a passwordless `default` needs a password, a `bind`
  address, or `protected-mode no`. Replicas and cluster nodes connect as
  clients too.
- **What a client sends cannot crash or stall the server.** Found by
  reading the code and by a test that sends each of these (`tests/hostile_input.rs`):
  - Counts that do not fit (`HSETEX … FIELDS`, `XACKDEL … IDS`, `MSETEX`,
    `TS.NRANGE`, `FT.SEARCH … PARAMS`, `FCALL` keys, `VADD … VALUES`) were
    added up past the largest number and then used: a crash, or in
    `FT.AGGREGATE`'s `LOAD`/`SORTBY` a loop without end.
  - `RESTORE` and `FUNCTION RESTORE` made room for as many elements as the
    payload said, and its checksum is no guard against a payload made to lie:
    one could make the server ask for terabytes and stop, or loop four
    thousand million times.
  - `BITFIELD` took any bit offset (SETBIT stops at 2^32), and grew the string
    to it. `DEBUG SLEEP` with a negative or infinite time crashed.
  - Without a limit on nesting, `cmsgpack.unpack`, an `FT.SEARCH` query, an
    `APPLY` expression or a JSONPath filter could run the thread out of stack,
    which takes the server down. Each now has one.
  - A client that has not authenticated is held to Redis's limits (a request
    of 10 arguments, each of 16 KB), and a request may come to 1 GB
    (`client-query-buffer-limit`). A request arriving in many reads was
    read again from its beginning after each one.
  - A replica's acknowledgements, and another cluster node's replies, are
    read with limits.
  - A panic in a command a script calls left the connection with a command
    pointing at freed memory; and a panic in a search coordinated across the
    cluster left the thread answering every later search as a shard.
- **A script that turns slow no longer stops a server on Tokio** (the I/O
  model on macOS and Windows, and on Linux where io_uring is blocked, as
  under Docker's default seccomp profile). A script that had run quickly
  before runs on the worker that read it; if it then ran long, and that
  worker had been the one listening on the sockets, no connection was
  answered or accepted until it ended — `SCRIPT KILL` and `FUNCTION KILL`
  could not reach it. Another worker now takes over while it runs. Found by
  Redis's own `scripting` and `functions` tests, which hung on Tokio.
- `BACKUP CLEANUP` removes only the backup's own files from `backupdirname`,
  not everything in it.

### Added

- **Logging.** Every line has a UTC timestamp, the pid and a level.
  `loglevel` takes Redis's levels (`debug`, `verbose`, `notice`, `warning`,
  `nothing`) and now actually filters; it used to be ignored. `logfile` (used
  to be ignored) and `log-format json` are new. New lines: `Ready to accept
  connections`, startup details, `CONFIG SET` changes, memory full, slow
  scripts, and, at `verbose`, client connections, failed authentication and
  protocol errors. Warnings and errors say what to do; repeating ones are
  throttled. `docs/LOGGING.md` and `docs/ERRORS.md` document the log and every
  error.
- **Startup update check.** At startup the server asks GitHub
  (`https://github.com/kividbio/kividb/releases/latest`) whether a newer
  release is out and, if one is, says so above the banner (waiting at most
  1.5 seconds). Nothing is sent but the request; only a version-shaped tag
  is read from the reply.
  Turn it off with `update-check no`, `--update-check no` or
  `KIVIDB_NO_UPDATE_CHECK=1`. See `docs/UPDATE_CHECK.md`.
- **Cluster: `min-replicas-to-write` and `min-replicas-max-lag`**, as in
  Redis (`min-slaves-*` too). A master with fewer replicas than that, online
  and heard from within the lag, refuses every write with `NOREPLICAS`, and
  `EXEC` refuses a transaction that writes. It used to apply only to writes
  made by scripts: a master cut off from its replicas went on acknowledging
  writes for the node timeout, and they were lost when it was failed over.
  `INFO replication` shows `min_slaves_good_slaves` while it is on.
- **Cluster: `INFO cluster`** (`cluster_enabled`), also in `INFO`, as in
  Redis; `redis-cli --cluster` refuses a node without it. A node's own line
  of `CLUSTER NODES` shows the slots it is migrating or importing
  (`[slot->-id]`, `[slot-<-id]`), which `redis-cli --cluster check` and
  `fix` read.
- **Cluster: `cluster-announce-hostname` and
  `cluster-preferred-endpoint-type`** (`ip`, `hostname`, `unknown-endpoint`),
  as in Redis 7. Every node learns every node's hostname; `CLUSTER NODES`,
  `SLOTS` and `SHARDS` show it, and with `hostname` preferred, `MOVED`, `ASK`
  and the endpoints in `CLUSTER SLOTS` and `SHARDS` give it instead of the
  address. Both can be changed with `CONFIG SET`.
- **Cluster: `cluster-bus-secret`.** With the same secret on every node, a
  node must prove it knows it (an HMAC-SHA256 answer to a random challenge;
  the secret is not sent) before anything it sends on the cluster bus is
  read. Anything that could reach the bus port could join the cluster
  before. `cluster-bus-port-protected-mode yes` now starts a node that has a
  secret and refuses one that has not.
- **Cluster: search index definitions reach every master.** `FT.CREATE`,
  `FT.ALTER`, `FT.DROPINDEX`, the `FT.ALIAS*` and `FT.DICT*` commands and
  `FT.SYNUPDATE`, once they succeed on a master, are run on every other
  master (as the `masteruser` / `masterauth` user), so each indexes the keys
  it holds. Running one again fills in a node that missed it; a master that
  did not take it is named in the error. A master added to the cluster
  later is given the definitions: by the master it imports slots from,
  before their keys, or by the first search that reaches it.
- **Cluster: searches answer from every master.** `FT.SEARCH`,
  `FT.AGGREGATE`, `FT.INFO` (`num_docs`) and `FT.TAGVALS` sent to any node
  are answered from the whole cluster: the node sends the query to every
  other master and merges what comes back. `FT.SEARCH` pages, sorts
  (`SORTBY`, relevance, or distance for KNN and vector ranges) and counts
  over all of them, and KNN gives the cluster's k nearest; `FT.AGGREGATE`
  runs its steps over every master's matching documents, so `GROUPBY` and
  every reducer are exact, and `WITHCURSOR` works. A node answers only for
  the slots it serves. If a master cannot answer, the search fails rather
  than answer from part of the data. Relevance scores are each master's own,
  as with RediSearch's coordinator.
- **Cluster: TLS between nodes, `tls-cluster` and `tls-replication`**, as in
  Redis. With `tls-cluster yes` the cluster bus is TLS and a node announces
  its TLS port, so that replication, slot migration, `MIGRATE` and index
  definitions between nodes are TLS too; `tls-replication yes` does the
  same for a replica of a standalone master. Nodes present `tls-cert-file`
  (or `tls-client-cert-file`) and check each other's against
  `tls-ca-cert-file`; as in Redis the chain is verified, not the name. A
  node without TLS cannot join. Builds with TLS (`-tls`, `-full`).
  Certificates must be X.509 version 3, as any certificate authority issues
  them; a version 1 certificate (`openssl x509 -req` with no extensions) is
  refused at startup.
- **A killed master continues a replica that holds the same data.** A
  master started from its AOF after `kill -9` or a crash fingerprints its
  dataset as it starts; a replica whose dataset has the same fingerprint is
  continued from there instead of being sent everything. A replica that
  holds anything different is sent everything, as before.
- **A master stopped cleanly carries on its replication stream.** With the
  AOF on, `SHUTDOWN` (and `SAVE`) records where the master's stream stood;
  started again on that data, it keeps its replication id and offset, and a
  replica stopped cleanly with it continues instead of receiving the whole
  dataset again. A planned restart of a whole cluster no longer full-syncs
  every replica.

### Changed

- **Vector ingest is about six times faster.** Each HNSW node has its own
  lock, so inserts no longer take the whole graph's: 1.18 million vectors
  (glove-25-angular, `M 16`, `EF_CONSTRUCTION 256`, 100 clients) took 197 s
  and take 31 s on a 64-core server (Redis 8.10: 38 s). A search no longer
  holds back the inserts beside it either.
- **Images are on Docker Hub as `kividb/kividb`**, as well as on GHCR as
  `ghcr.io/kividbio/kividb`, with the same tags. They are no longer
  published to Quay, and the release binaries are downloaded from GitHub
  Releases rather than `releases.kividb.io`.
- **Memory per key is lower.** A key of up to 24 bytes is kept inside its
  table entry, and a string value is one pointer with eight bytes in front
  of it: ten million keys of 16 bytes with 100-byte values take 177 bytes a
  key where they took 217, and a key with a time to live 57 fewer still.
- **Connections are spread evenly over the io_uring workers.** A worker
  that accepts a connection it has too many for passes it to the worker
  with the fewest. With few connections on many cores some workers used to
  sit idle while others were the limit.
- **The arm64 Linux binary, packages and image need an ARMv8.1 CPU**
  (Graviton2 and later, Ampere, and every other arm64 server): atomic
  operations are compiled as single instructions. A Raspberry Pi 4 and a
  first-generation Graviton can no longer run them.

- **Scripts on different hash tags run at the same time** (builds with Lua:
  `lua`, `full`). A script whose `KEYS` all carry one hash tag
  (`{orders}:waiting`) runs beside scripts on other tags, on other cores,
  where every script used to run alone. Such a script may no longer touch a
  key outside its tag: a `redis.call` on one fails with `ERR Script
  attempted to access the key ..., which is outside the hash tag ... of its
  KEYS (lua-parallel-scripts)`. Scripts with no keys, untagged keys or keys
  of two tags run alone as before, and to everybody else a script is still
  one step. `lua-parallel-scripts no` restores the old behaviour for every
  script. See `docs/SCRIPTS.md`.

- **`CONFIG REWRITE` writes the settings in effect**, `CONFIG SET` changes
  included. It wrote the configuration the server had started with.

- Log lines tagged `[AOF]`, `[REPL]` and `[CLUSTER]` are now levelled lines
  that begin `AOF:`, `Replication:` and `Cluster:`.
- `CONFIG GET loglevel` answers with Redis's level names (`notice` rather
  than `info`).
- The default and `-lua` builds now include a TLS client (rustls, with
  compiled-in root certificates), for the update check.
- **Scripts are several times faster.** A script is compiled once and kept
  (it was compiled on every call); a script that has shown itself quick runs
  without being handed to another thread; and quick scripts from several
  clients are run back to back by one thread rather than by several taking
  turns at a lock. `EVAL` of a short script with 50 clients: about 10,500 a
  second before, 36,000 to 42,000 after (Redis 8 on the same machine:
  141,000).

### Fixed

- **`REPLICAOF NO ONE` during a full sync lost the promoted node's data.**
  A replica promoted while a full-sync payload was still arriving read the
  rest of it, dropped its own dataset and loaded the old master's, and only
  then stopped replicating. The sync now stops at once and the node keeps
  its dataset; a promotion that arrives while the dataset is being replaced
  waits for that to finish, as on Redis. Found in a failover test of
  1.1.0-rc1.
- **A transaction or a script could wait for ever.** A thread that ran its
  first command while a transaction or script was starting did not queue
  behind it. A few such threads running command after command could keep it
  waiting indefinitely, and every transaction and script after it queued
  behind it. It now makes those threads wait too once it has waited 2 ms.
- **A manual failover could fail with every vote given.** A master saves
  its vote before it answers, and on a busy disk that took longer than the
  two seconds the replica waited: "timed out with 0 of 2 votes", about one
  time in ten in testing. The replica waits the node timeout.
- **A restored key was in no search index.** `RESTORE` (and so `MIGRATE`
  and slot migration) put a hash or JSON document under a key without
  indexing it: a key moved to another node of a cluster could not be found
  there, and a key a user restored was invisible to `FT.SEARCH`.
- **An AOF rewrite stopped every read and write for as long as it took.**
  An automatic rewrite, `BGREWRITEAOF`, and `SAVE`/`BGSAVE` with the AOF on
  held every shard while the whole dataset was copied, written to
  `dump.kdb` and the log swapped. Writes are now held only while the
  dataset is captured and a mark placed in the log's queue; the log is
  turned over and the snapshot written after, and the capture is spread
  over several threads without an allocation per key. With about 9
  million keys, writes are held for under 100 ms (1.5 seconds before the
  last change; 15 to 46 seconds on a cluster under load before any). A rewrite stopped part way is finished on the next start from both
  logs.
- **Slot migration held writes longer than it said.** Capturing the slots
  serialised every value but strings while writes waited (a list of a
  million and a half elements in full), and `write_pause_ms` counted only
  the final handover. Values are now copied at the capture and serialised
  after, and `write_pause_ms` counts both pauses (`capture_pause_ms` the
  first), on failure too. While writes are paused for the handover, the
  only thing waited for is the destination recording that it owns the
  slots (`nodes.conf`, written past the page cache): milliseconds
  normally, and on a disk whose journal is stalled, what that write takes,
  as in Redis. The log says where each pause went.
- **A node of a busy cluster could be failed over because its disk was
  slow.** The cluster bus wrote and synced `nodes.conf` itself whenever a
  heartbeat changed anything, and answered nobody until the disk had: with
  the disk busy with logs and snapshots, peers took the silence for a
  failure. It is written by a thread of its own now.
- **`redis-cli --cluster reshard` onto a new node left the cluster
  disagreeing**, and `rebalance` and `del-node` then refused to run. A node
  took new epochs by counting up from its own, so the slots it took over
  from a master on the same epoch stayed with that master in everyone
  else's view. New epochs start above every epoch the node has seen, and
  two masters on one epoch are separated, as in Redis.
- **A slot migration the destination retried stayed `in_progress` on the
  source for ever**, and `redis-cli --cluster reshard` waited on it until
  its one-hour timeout.
- **`auto-aof-rewrite-percentage` and `auto-aof-rewrite-min-size` took
  effect only at startup**; `CONFIG SET` answered OK and changed nothing.
- A hostname learnt from a peer was saved to `nodes.conf` only with some
  later change, and could be forgotten over a restart.
- **A cluster restarted as a whole failed nodes that were only starting.**
  A node judged its peers by times saved in `nodes.conf` before it stopped,
  found them all silent for longer than the node timeout, and suspected
  every one; started together, the nodes agreed, and a master's replica took
  over, dropped its data and resynced in full. Every peer now counts as
  heard from when the bus starts, as in Redis.

- **`SPOP` and `SRANDMEMBER` no longer walk the whole set.** Each call
  copied every member to pick one: on a set of a hundred thousand members
  `SPOP` answered a few thousand times a second. A random member is now
  found directly.
- **`ZRANGE` by index no longer copies the whole sorted set**, and
  `ZRANGEBYSCORE` / `ZREVRANGEBYSCORE` begin at the first score asked for
  instead of looking at every member.

- **A time to live was told a millisecond short and could end a millisecond
  early.** `PTTL` straight after `SET k v PX 250` said 249, and the key was
  gone up to a millisecond before its time. Expiry is set, told and ended
  by one clock in whole milliseconds, as in Redis. BullMQ's rate limiter,
  which waits for as long as it is told, came back early each time.
  `HTTL`/`HPTTL` straight after `HEXPIRE 10` said 9 for the same reason;
  what is left of a field's time is rounded up.
- **`MSET`, `MSETNX` and `MGET` are each one step.** They took their keys
  one at a time: a reader could see half an `MSET`, `MGET` read as many
  moments as it had keys, and two clients could both be told that their
  `MSETNX` had set a key. `MSET` and `MSETNX` also skipped a key that was
  not UTF-8 and wrote the rest; they refuse the command.
- `APPEND` could grow a string past 512 MB. It answers as Redis does.

- `CONFIG SET loglevel verbose` and `notice` were refused, though the error
  listed them as valid.
- A port in use on the io_uring path crashed the server with a panic.
- **`--tls-ca-cert-file` now verifies client certificates.** It was parsed
  and then ignored: the TLS listener never asked for a client certificate,
  so any client that trusted the server could connect. With a CA file set,
  clients must now present a certificate that CA signed, as on Redis.
  New `tls-auth-clients` (`yes` | `optional` | `no`, default `yes`, as on
  Redis) picks whether one is required, checked only when presented, or not
  asked for. **Upgrading:** a server with `tls-ca-cert-file` set whose
  clients send no certificate will refuse them; give the clients
  certificates, or set `tls-auth-clients no` (or `optional`) to keep the old
  behaviour. Without a CA file nothing changes.
- **Lua: every status reply is an `{ok = …}` table.** `redis.call` turned
  only `+OK` into `{ok = "OK"}`; other status replies (`TYPE`'s `+hash`,
  `PING`'s `+PONG`, …) came back as plain strings, so scripts reading
  `.ok` (BullMQ's among them) got `nil`.
- A failed background save was logged as a Rust debug dump.
- **A slot migration or a manual failover could lose an acknowledged
  write.** Both pause writes, wait out the ones in flight and hand over. A
  write that had passed the pause check, but not yet started, could run
  after the handover: applied on the old owner, acknowledged, and never sent
  to the new one. Such a write now waits for the handover and is answered by
  whoever owns the key then (`-MOVED`, or `READONLY`). The cluster test for
  it lost one `INCR` in about 1 run in 25, on 1.0.5 too; 0 in 100 now.
- **A second server started on a port in use took a share of the first
  one's clients** (Linux, the io_uring path: its workers share the port with
  `SO_REUSEPORT`). It refuses to start, with the same `[FATAL]` line as
  elsewhere.
- **A server killed mid-script could come back with half of the script.**
  Each write a script made was a record of its own in the AOF, and a kill
  could fall between two of them. A script's records are now written
  together. (2 of 40 kills left a script half applied before; 0 of 40 now.)
- **A hash gives its fields in the order they were set** (`HGETALL`,
  `HKEYS`, `HVALS`, `HSCAN`), as a Redis hash small enough for a listpack
  does. They came in hash-table order.
- **`SSCAN`, `HSCAN` and `ZSCAN` of a small collection return all of it at
  once**, with cursor 0, whatever `COUNT` says — as Redis does for a set,
  hash or sorted set small enough for its compact encodings (`OBJECT
  ENCODING` says `listpack` or `intset`: up to 128 members of up to 64
  bytes, 512 for a hash or a set of integers). They used to page it by
  `COUNT`. BullMQ's dependency pagination expects one page.
- **`XADD`/`XTRIM … MAXLEN ~` trimmed too little with large entries.** A
  node of a stream ends at `stream-node-max-bytes` (4096) as well as at
  `stream-node-max-entries`, as in Redis, so `~` trims about as close to the
  target as Redis does. With 200-byte entries, `MAXLEN ~ 20` kept 100
  entries; it now keeps 22 (Redis: 26). `stream-node-max-bytes` is a real setting
  now (it was accepted and ignored).

### Known limitations

- **A hard kill can lose the last acknowledged writes, with the AOF on.** A
  write is answered when its record is queued for the append-only file, not
  when it has been written to it. If the process is killed outright
  (`kill -9`, the OOM killer) the writes of the last fraction of a
  millisecond may not be in the file; in testing that was up to four
  writes, and one job in one of six kill-and-restart runs of a Sidekiq
  soak. A normal shutdown loses nothing, and a power failure can lose up to
  a second either way, as with `appendfsync everysec` in Redis. Redis
  itself loses nothing to a process kill. Closing this means every write
  waits for the file; it is left for a release after 1.1.0.
- **Many clients writing to one key are slower while anything follows the
  write stream** (a replica; the replication backlog, kept for an hour after
  a restart or after the last replica left; a backup in progress). Writes to
  a key are then put on the stream in the order they were made, one at a
  time, and each hands its record to the replication thread. Measured with
  50 clients, pipelining, one key, 8 CPUs: about 210,000 writes a second
  against 1.1 million with nothing following the stream. Writes spread over
  many keys are not affected. A fix that keeps the order without the
  hand-off is planned for a later release.
- **Cluster: `FT.SPELLCHECK`, `FT.PROFILE`, `FT.HYBRID` and `FT.SYNDUMP`
  answer from the node they are sent to.**
- **Cluster: without `tls-cluster`, what nodes send each other is in the
  clear**, as in Redis; `cluster-bus-secret` only keeps strangers off the
  bus.

## [1.0.5]

The release notes, written for people upgrading, are in
[`docs/releases/v1.0.5.md`](docs/releases/v1.0.5.md). 1.0.5-rc1 was the
release candidate; nothing in the engine changed after it.

Redis 8.4 – 8.10 parity, replication fixes found in failover testing, and a
pass over clustering. The full account, including what is *not* implemented,
is in [`docs/REDIS_8_4_TO_8_10_PARITY.md`](docs/REDIS_8_4_TO_8_10_PARITY.md).

Also eleven engine issues found by testing KiviDB under a Kubernetes
operator, and the eight found by running LiteLLM's semantic cache — both on
v1.0.4.

And a pass over Redis's own test suite, unit by unit: what it found, what
was fixed and what still fails is in
[`docs/REDIS_TEST_SUITE.md`](docs/REDIS_TEST_SUITE.md).

**Read before upgrading:** the ACL file and the config file are now read
strictly, a persistence file that cannot be read stops the server, key and
channel patterns are enforced, and `maxmemory` is measured differently. Each
is under *Changed* below.

**Geo data has to be re-added.** Positions are now stored in Redis's
encoding. A geo key written by 1.0.4 or earlier is read back at the wrong
coordinates: delete it and `GEOADD` its members again after upgrading. See
*Changed*.

### Added

- **Redis 8.4:** `DIGEST` / `DELEX` / `SET … IFEQ|IFNE|IFDEQ|IFDNE` with the
  XXH3 digest Redis uses; `XREADGROUP … CLAIM`; atomic slot migration
  (`CLUSTER MIGRATION IMPORT | STATUS | CANCEL`).
- **Redis 8.6:** idempotent `XADD` (`IDMP`, `IDMPAUTO`, `XCFGSET
  IDMP-DURATION / IDMP-MAXSIZE`); `HOTKEYS`; time-series `NaN` values with
  `COUNTNAN` and `COUNTALL`.
- **Redis 8.8:** `AGGREGATE COUNT` for sorted-set unions and intersections;
  `JSON.SET … FPHA`; several aggregators in one `TS.RANGE` / `TS.MRANGE`;
  `slowlog-entry-max-argc` and `slowlog-entry-max-string-len`; slowlog fields
  in `INFO commandstats`; hash subkey notifications.
- **Redis 8.10:** `LMOVEM` / `BLMOVEM`; `SUNIONCARD` / `SDIFFCARD`; `HIMPORT`;
  `BACKUP` with `backupdirname`, `backup-sealed-ttl` and `preload-file`;
  `MAXCOUNT` / `MAXSIZE` for `XREAD` and `XREADGROUP`; `FT.ALIASLIST`;
  `TS.NRANGE` / `TS.NREVRANGE`, `TS.READ`, `TS.QUERYLABELS`, `TS.MRANGE …
  EXCLUDEEMPTY`; the JSONPath extensions; the argument count in `SLOWLOG GET`.
- **A JSONPath engine.** Bracket notation, negative indexes, unions, slices,
  filters, the `~` operator, the 8.10 operators and functions, and projection
  expressions. Every JSON command answers for every value a `$` path matches.
- **Replication options:** `masterauth`, `masteruser`, `repl-timeout`,
  `repl-ping-replica-period`, `replica-full-sync-guard`,
  `replica-full-sync-guard-ratio`.
- **Cluster options:** `cluster-announce-ip` / `-port` / `-bus-port`,
  `cluster-config-file`, `cluster-require-full-coverage`,
  `cluster-bus-port-protected-mode`, and the two
  `cluster-slot-migration-*` settings.
- **ACL:** the categories `admin`, `dangerous`, `blocking`, `fast`, `slow`,
  `json`, `search`, `timeseries`, `vector` and `transaction`; rules for a
  single subcommand (`+config|get`, `-client|kill`).
- **Eviction.** `maxmemory-policy` with every Redis policy — `allkeys-lru`,
  `allkeys-lfu`, `allkeys-lrm`, `allkeys-random`, `volatile-lru`,
  `volatile-lfu`, `volatile-lrm`, `volatile-random`, `volatile-ttl` — and
  `maxmemory-samples`, `lfu-log-factor`, `lfu-decay-time`, `evicted_keys`,
  the `evicted` keyspace event, `OBJECT IDLETIME` and `OBJECT FREQ`.
- **Partial resynchronisation.** A replication backlog (`repl-backlog-size`,
  `repl-backlog-ttl`) and `PSYNC`: a replica that reconnects, or is restarted
  cleanly, continues from where it was instead of taking the whole dataset.
  `repl-diskless-load` chooses whether a full sync needs memory for one
  dataset (the default) or two.
- **Search.** Indexes `ON JSON`; `FT.AGGREGATE … WITHCURSOR` with `FT.CURSOR
  READ | DEL`; `REDUCE COLLECT` (Redis 8.8), `STDDEV`, `QUANTILE`,
  `RANDOM_SAMPLE`, `FIRST_VALUE … BY`, `SORTBY … MAX`, `ADDSCORES`; real
  `FT.EXPLAIN`, `FT.EXPLAINCLI`, `FT.PROFILE` and `FT.CONFIG`; per-field
  schema options, `STOPWORDS`, `VECTOR_RANGE`, `RETURN … AS`, `HIGHLIGHT`,
  `INKEYS`, `WITHSORTKEYS`; RESP3 map replies. Synonym groups and
  dictionaries are saved in snapshots.
- **`RESET`**; `calls`, `usec`, `rejected_calls` and `failed_calls` in `INFO
  commandstats`; active expiry of hash fields, with their notifications;
  `expired` keyspace events from the background expiry task; `sub=`, `psub=`
  and `ssub=` in `CLIENT LIST`.
- Options: `dir`, `maxmemory-policy`, `maxmemory-samples`, `lfu-log-factor`,
  `lfu-decay-time`, `repl-backlog-size`, `repl-backlog-ttl`,
  `repl-diskless-load`. The Redis spellings `appendonly`, `masterauth` and
  the rest are understood in the config file.

- **Client-side caching, whole.** `CLIENT TRACKING` with `BCAST` and
  `PREFIX`, `REDIRECT` (with `tracking-redir-broken` when the target goes),
  `OPTIN` / `OPTOUT` / `NOLOOP`; every key a command reads is tracked, not
  the first; keys written by a script, expired, evicted or flushed are
  invalidated; `tracking-table-max-keys`; the tracking fields of `INFO`.
- **`CLIENT`:** `LIST` and `KILL` with every filter (`ID`, `ADDR`, `LADDR`,
  `USER`, `TYPE`, `MAXAGE`, `SKIPME`), `LIST` in Redis's layout, `REPLY ON |
  OFF | SKIP`, `NO-EVICT`, `NO-TOUCH`, `HELP`.
- **`MONITOR` on an ordinary connection:** it goes on taking commands, and
  shows what scripts call (as `lua`) and transactions as they ran.
- `COMMAND` answers from Redis's own tables: arity, key specifications and
  flags (`COMMAND INFO`, `GETKEYS`, `GETKEYSANDFLAGS`, `LIST FILTERBY`,
  `COUNT`, `DOCS`), for subcommands too.
- `XINFO STREAM … FULL`, `XSETID` with `ENTRIESADDED` and `MAXDELETEDID`,
  consumer-group lag, `XAUTOCLAIM`'s deleted-entries reply, approximate
  trimming (`~`) by node with `stream-node-max-entries`.
- `SCAN … TYPE`, `HSCAN … NOVALUES`, `ZSCAN … NOSCORES`; `LATENCY HISTOGRAM`;
  `OBJECT ENCODING` and `OBJECT HELP`; `FUNCTION STATS` with the running
  function; `CONFIG GET` with several patterns and `CONFIG SET` with several
  pairs; `INFO` with several sections; the `new` keyspace event (`n`).
- `scripts/redis_compat.sh`, which runs Redis's test suite against a KiviDB
  binary one unit at a time. CI runs it and puts the table on the run's
  summary page.

### Changed

These follow Redis and may be visible to existing clients.

- **Geo positions are stored as Redis stores them** (52-bit interleaved
  geohash, latitude in the even bits), so scores, `GEOHASH` strings,
  distances and search results are Redis's to the last digit. **A geo key
  written by an earlier version decodes to the wrong position** — re-add its
  members.
- **A RESP2 connection that enables `CLIENT TRACKING` without `REDIRECT` is
  sent no invalidations.** It used to be written a pub/sub frame it had not
  subscribed to. Use `HELLO 3`, or `REDIRECT` to a connection subscribed to
  `__redis__:invalidate`.
- **A request that is not a request closes the connection** with `-ERR
  Protocol error: …` (a count or length that is not a number, an argument
  over 512 MB, a line over 64 KB, unbalanced quotes). Such a connection used
  to wait for ever.
- **`CLIENT PAUSE … ALL` pauses reads too** (it paused writes only), and
  `WRITE` holds everything that may write: `PFCOUNT`, `PUBLISH`, scripts not
  declared `no-writes`, and an `EXEC` containing any of them.
- **The wrong number of arguments is refused before anything else**, in
  Redis's words — `ERR wrong number of arguments for 'get' command` — and an
  unknown command is `ERR unknown command 'x', with args beginning with: …`.
  Inside `MULTI` either one aborts the transaction (`EXECABORT`).
- **`CONFIG SET` is strict.** An unknown parameter, a value out of range or
  a parameter that cannot change at run time is an error naming it, and none
  of the pairs given with it is applied.
- `EXPIRE … GT` on a key with no TTL does nothing: no TTL is for ever.
- Over `maxmemory`, room is made before any command runs, not only before a
  write; `CONFIG SET maxmemory` evicts at once.
- `SCAN`, `SSCAN`, `HSCAN` and `ZSCAN` cursors are positions in a stable
  order of hashes, not offsets: a cursor from an earlier version is not
  valid, and an element present throughout a scan is returned whatever is
  added or removed meanwhile.
- `OBJECT ENCODING` names the encoding Redis would use for the value
  (`listpack`, `intset`, `quicklist`, …), by its size and the
  `*-max-listpack-*` settings.
- `CLIENT KILL` with filters replies with the number killed; `CLIENT LIST`
  and `CLIENT INFO` use Redis's field names (`lib-name`, `lib-ver`);
  `CLIENT CACHING` is refused in the mode it does nothing in; `LATENCY
  RESET` replies with a count; `COMMAND GETKEYS` on a command without keys
  is an error; key flags are Redis's (`RW`, `access`, `update`, …).
- `XADD` with an ID not above the last is refused in Redis's words; `XSETID`
  does not create a stream; trimming does not move `max-deleted-entry-id`;
  `XGROUP` / `XINFO` on a missing key or group reply as Redis does.
- `ZADD` compares scores exactly (it used a tolerance); `RANDOMKEY` is
  random; `SORT … BY nosort` on a sorted set follows its order.
- Glob patterns (`KEYS`, `SCAN MATCH`, `PSUBSCRIBE`, ACL patterns, `CONFIG
  GET`) are matched by a port of Redis's matcher: character classes,
  ranges, negation and escapes behave as in Redis.
- **Library names are case-sensitive**, as in Redis: `mylib` and `MyLib`
  are two libraries. (Function names are not, as before.)
- **Functions are held to their flags.** `FCALL_RO` refuses a function not
  registered with `no-writes`; a function registered with it cannot write;
  at the memory limit a function runs only if it has `allow-oom` or
  `no-writes`. A library is loaded with `redis.register_function`,
  `redis.log` and the version constants in reach and nothing else — no
  `redis.call`, no `math` — and can neither read nor set globals;
  `redis.register_function` cannot be called from a script or a running
  function. `FUNCTION LOAD` is refused at the memory limit. `FUNCTION STATS`
  returns `engines` as a map; `FUNCTION LIST` shows each function's
  description and flags and refuses arguments it does not know.

- **JSON replies.** With a `$` path every command returns one entry per match
  (`JSON.ARRAPPEND`, `ARRINSERT`, `ARRTRIM`, `ARRINDEX`, `ARRPOP`, `STRAPPEND`,
  `DEBUG MEMORY`, `RESP` used to return a bare value); `JSON.GET` returns `[]`
  rather than nil for no match; `JSON.MGET` returns arrays; `JSON.NUMINCRBY` /
  `NUMMULTBY` return one JSON array in a bulk string; a command given no path
  uses the legacy root. Objects keep their members in insertion order.
  `JSON.SET` on a key of another type is `WRONGTYPE`.
- **A key that loses its last element is deleted** — lists, sets, sorted sets,
  hashes and vector sets. It used to stay behind as an empty key.
- **`-@dangerous` and `-@admin` now deny commands.** They were accepted and
  had no effect. Review users whose rules name them.
- `DIGEST` returns XXH3-64 instead of SHA-256.
- `HSETEX` takes its TTL as an option (`EX`, `PX`, `EXAT`, `PXAT`, `KEEPTTL`);
  the previous form is still accepted.
- Set algebra (`SUNION`, `SINTER`, `SDIFF`, `SINTERCARD` and the `STORE`
  forms) replies `WRONGTYPE` for a key that is not a set.
- Time-series commands reply with integer timestamps; the range filters are
  `FILTER_BY_TS` and `FILTER_BY_VALUE`.
- `XREADGROUP` on a missing group is a `NOGROUP` error.
- An index alias is no longer listed by `FT._LIST`.
- `SLOWLOG GET` entries have seven elements.
- `XDELEX` and `XACKDEL` take Redis's syntax and reply with Redis's codes:
  `XACKDEL key group [KEEPREF|DELREF|ACKED] IDS n id…`, and `2` (not `0`) for
  an entry kept because a group still references it. `ACKED` now also waits
  for groups that have not *read* the entry, so on a stream with no groups it
  deletes nothing. An acknowledged entry that is already gone is `1`, not `0`.
  The older spellings are still accepted.
- `CONFIG SET slowlog-log-slower-than` with a negative value turns the slow
  log off, and `CONFIG GET` reports `-1` for it.
- A replica loads its local dataset at startup, reports
  `master_link_status:down` unless it is synced and receiving, and by default
  refuses a full resync from an empty master while it holds data.
- `REPLICAOF` is refused in cluster mode. A build without the `tls` feature
  refuses to start with `--tls-port`.
- In cluster mode a `MULTI` is routed as its commands are queued and must
  address one slot; `SELECT` of a database other than 0 is refused.
- ACL changes apply to connections that are already open.
- `serde_json` is built with `preserve_order`; `xxhash-rust` is a new
  dependency.
- **The ACL file is read strictly, and replaces the users.** A rule that is
  not understood fails `ACL LOAD` (and startup) instead of being skipped;
  `ACL LOAD` leaves exactly the users in the file. A user keeps every key
  pattern it is given (only the last used to be kept), and key patterns —
  including `%R~` / `%W~` — and channel patterns (`&`) are enforced. A user
  with no channel pattern cannot publish or subscribe. `ACL SETUSER` and
  `ACL DELUSER` no longer write the ACL file; `ACL SAVE` does.
- **A password on `default` is enforced** however it is set — `requirepass`,
  the ACL file or `ACL SETUSER`. An unauthenticated `HELLO` is `NOAUTH`.
- **The config file is read strictly.** An unknown directive or a bad value
  stops the server with the line number, rather than being ignored. Command
  line flags take precedence over the file.
- **A persistence file that cannot be read stops the server** instead of
  being skipped and the server starting empty. An AOF cut short by a crash is
  trimmed to its last whole command and loaded.
- **`maxmemory` is measured against allocated memory** (`used_memory`), not
  the resident set size. Commands that free memory are no longer refused at
  the limit.
- **Search replies and errors follow Redis.** `FT.SEARCH` and `FT.AGGREGATE`
  return what was asked for (`RETURN`, `LOAD`) rather than every field;
  `NOCONTENT` and `RETURN 0` are honoured; unknown arguments and malformed
  queries are errors; a TAG field is queried with braces; `FT.TAGVALS` splits
  on the field's separator; `FT.INFO` has Redis's shape. `TS.MADD` on a
  missing key is an error for that sample.
- **`MULTI`/`EXEC` and scripts run alone.** Other clients' commands wait for
  a running script and are told `BUSY` only after `lua-time-limit`.
- **Pub/Sub.** A subscriber that falls too far behind is disconnected rather
  than silently skipping messages. A RESP2 connection that unsubscribes from
  everything stays open as an ordinary connection. The error for a command a
  RESP2 subscriber may not run is Redis's. `PUBSUB NUMPAT` counts distinct
  patterns; unsubscribing from nothing replies with a null name.
- `INFO` reports `redis_version:8.10.0`. `BGSAVE` replies `Background saving
  started`, runs one at a time, and `LASTSAVE` moves only when a save
  succeeds. A failed final snapshot exits with status 1.
- A replica drops its dataset before loading a full sync (it used to hold
  both); `repl-diskless-load swapdb` restores the old behaviour.
- Pipelined throughput is lower than v1.0.4's: 14–24% on CI's benchmark
  (memtier_benchmark, c7gn.12xlarge, `-c 5 --pipeline=10`) — the cost of
  isolating transactions and scripts, ordering writes for replication,
  counting allocations, checking arity before every command and keeping
  per-command statistics. Without pipelining it is not measurable.

### Fixed

- **`COPY` of a list, set, hash, sorted set or stream shared the value with
  its source**: a write to either changed both. It copies.
- **A request beginning `*3000000000` aborted the server** on the allocation.
  Counts and lengths are bounded as Redis bounds them.
- **`UNLINK` was not dispatched** and replied `unknown command`.
- **An error reply could contain a line break** — a Lua error from `FUNCTION
  LOAD`, with its traceback. The reply ended at the break and the client
  read the rest as the reply to its next command. Breaks in an error's text
  are now spaces, as in Redis.
- **A `FUNCTION LOAD … REPLACE` that failed half-way** left the functions
  the new library had registered so far in place of the old ones. A load is
  now all of a library or none of it. Two libraries could also register the
  same function name, the second silently taking it from the first; and a
  library whose code never returned held the server for ever (it is stopped
  after 500 ms).
- **A client blocked on several keys could take another client's element.**
  Blocked on `k2 k3` and woken because `k3` was pushed to, it looked at `k2`
  first; if `k2` held an element pushed for a client that had been waiting
  there longer, it took that — and the other client stayed blocked on an
  empty list while the element in `k3` went unclaimed (`BLPOP`, `BRPOP`,
  `BLMPOP`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`). About one time in eight on
  the io_uring loop, where the two clients run on different threads.
- **A command that arrived as a script started waited for the script to
  end** — it had passed the check that answers `BUSY` — and a script may
  never end. If the command came from the client that was going to `SCRIPT
  KILL` it, nothing ever did. Such a command is now told `BUSY` once the
  script is past `lua-time-limit`, like any other.
- `FUNCTION LOAD` took time in proportion to the number of libraries
  already loaded.
- `aof_rewrite_in_progress` read 0 between `BGREWRITEAOF`'s reply and the
  moment the background task began, so a client waiting on it could go on
  before the rewrite had started.
- A blocked `XREADGROUP` was not woken when its stream or group was deleted,
  and readers were not served in the order they arrived.
- `rdb_changes_since_last_save` was kept in per-thread batches of two: a
  change was not counted until the same thread made another.
  `tracking_clients` did not go down when a tracking client's connection
  ended on an error or a `CLIENT KILL`. On the io_uring loop, `CLIENT KILL …
  SKIPME no` closed the caller's connection before its reply was written.
- `WATCH` did not see a key expire, a key removed by `FLUSHALL`, or a
  `SWAPDB`; and saw changes where a command changed nothing.
- A connection's input and output buffers, once grown for one large request
  or reply, were kept at that size for the life of the connection.
- `EXPIRE` and expiry did not tell clients tracking the key; evictions were
  announced after the reply of the command that caused them.
- A client killed or gone while blocked kept its place in line for an
  element.

- **A full resync that was cut off left the replica with no keys**, and a
  master restarting in a loop did that to every replica at once. A replica
  now gives up its dataset only for a payload it has received whole and read
  through to the end.
- **The final snapshot on `SIGTERM` was sometimes not written** on Linux: the
  process could exit while it was being saved, if connections were arriving.
- **Concurrent writes to one key could reach replicas and the AOF in a
  different order than the master applied them**, leaving them with different
  data. Writes to a key are now queued for replication in the order the
  store took them.
- **A replica reported `master_link_status:up` during its first full sync.**
- **Pub/Sub lost messages** for a subscriber more than 1024 messages behind
  on a channel, without a word.
- **Keys were expired late**: the background task visited one database in
  sixteen per cycle, so a key could outlive its TTL by 1.6 s.
- **An index `ON JSON` indexed nothing**, and a hash dropped out of its index
  when one of its fields was given a TTL.
- **Search**: `VECTOR_RANGE` matched nothing; negation in a pre-filter was
  ignored; `OR` across clauses matched nothing; a non-vector `FT.SEARCH`
  returned documents without fields; `FT.CREATE` refused field options.
- **`DEL`, `EXPIRE` and the other commands that free memory were refused at
  `maxmemory`**, leaving a full server no way to make room.
- **A command could be logged as having taken an impossibly long time**, or
  panic a debug build: the slow log timed commands with the wall clock, and
  went wrong whenever that was set back.
- **A server over `maxmemory` started empty with AOF on**: every write in its
  own append-only file was refused as it was replayed. What is on disk is
  loaded whatever the limit.
- **A RESP2 subscriber's connection was closed** when it unsubscribed from
  its last channel.
- **A replica reported its link up while it could not reach its master**, for
  as long as the kernel took to give up (about fifteen minutes). It now
  notices within `repl-timeout`.
- **A full resync from an empty master discarded the replica's dataset.**
- **Replication offsets read 0** on masters and replicas.
- **Blocked list and sorted-set pops could return the wrong element, and left
  replicas with different data.** A push handed its first element to a blocked
  client instead of adding it; `BRPOP` against `RPUSH k a b` got `a`, and the
  replica then popped a different element than the master had given away.
  Blocked clients are now woken in the order they blocked and pop for
  themselves.
- **Two `WATCH` transactions could both succeed on the same key.** `EXEC`
  checked the watched keys and then ran, and another `EXEC` could do both in
  between: ten buyers and a stock of five sold six, about once in twelve runs.
  Transactions that watch keys now run one at a time from the check to the
  last command. (`docs/EXEC_ATOMICITY.md` says what is still not covered.)
- **Writes could reach the wrong database on a replica, in the AOF, or in a
  backup** when clients wrote to several databases at once: a write could be
  queued behind another client's `SELECT`. Once any client has selected a
  database other than 0, every write travels with its own `SELECT`.
- **`CONFIG SET slowlog-log-slower-than` did not reach open connections** —
  not even the one that issued it — when the server had started with the slow
  log off.
- **Clients blocked on a key were not woken** when the list arrived by
  `RENAME`, `COPY`, `RESTORE`, `SORT … STORE` or a script.
- **Relative TTLs started over on AOF replay** and expired late on replicas;
  an `EXPIRE` that deleted the key was not propagated. TTLs are now recorded
  as the moment of expiry.
- **A transaction naming another node's slot ran in part** in cluster mode,
  and an all-`SET` transaction could write keys into slots the node did not
  own.
- **A cluster of six or more nodes did not finish forming** where the clock is
  coarse (Windows): gossip chose which nodes to mention from the clock's
  nanoseconds, and kept mentioning the same three. Election delays and
  `ZRANDMEMBER` drew on the same source; node and replication IDs were derived
  from the clock and the PID. All use a random number generator now.
- **`COMMAND` and `COMMAND INFO` returned nothing**, so a client that learns
  key positions from the server — redis-py's cluster client — could not route
  a single command. They now describe every command.
- **Cluster:** commands were routed by their first argument rather than their
  keys; `MIGRATE` moved only strings and dropped TTLs; a failed-over master
  that returned served its stale dataset beside its replacement; a replica
  could not restart; `CLUSTER FORGET` was undone by gossip; a failed `CLUSTER
  MEET` left a handshake entry; `PUBLISH` did not reach subscribers on other
  nodes; an unreadable `nodes.conf` was silently replaced; keys in a slot lost
  to another master were kept.
- **ACL changes did not reach connected clients**, and commands queued in a
  `MULTI` ran after the permission for them was revoked.
- `ZUNIONSTORE` / `ZINTERSTORE … WEIGHTS` applied weights to the wrong sets.
- `WATCH` was dirtied only by writes to a command's first key.
- `XREADGROUP`, `XCLAIM` and `XAUTOCLAIM` were replicated as commands, so a
  replica's pending lists (owner, delivery count, delivery time) differed from
  its master's.
- `JSON.DEBUG MEMORY` was routed and ACL-checked by the word `MEMORY`.

## [1.0.4-rc3]

### Added

- **Startup loads `dump.kdb` first, then the AOF as the delta.** A cloud volume
  snapshot captures the dump (most of the data) plus whatever was appended to
  the AOF while the volume was being copied. Recovery is no longer either/or:
  both files apply. `SAVE` / `BGSAVE` / `BGREWRITEAOF` / clean shutdown write
  the dump and reset the AOF to a `KIVIDLT1` delta so the log is only the
  writes since that checkpoint. Automatic rewrite (default 100% growth, 64 MB
  minimum) does the same fold, so a benchmark cannot leave an unbounded log
  for the next start. `SHUTDOWN NOW` skips the reset, as it already skips
  waiting for the AOF to flush.

### Fixed

- **AOF replay no longer rebuilds indexes one record at a time.** Every `HSET`
  in the log ran a full HNSW graph insertion, serially, before the listener was
  bound — 1.18M of them for a glove-25 dataset, which is a ~3 minute start
  against 2–3 seconds from a snapshot. Replay now suppresses per-write index
  maintenance and rebuilds in a single background pass with the port already
  open, the same way snapshot startup does. Applies to legacy logs too.

- **AOF replay skips records a later flush has already discarded.** A log ending
  in `FLUSHDB` rebuilt the entire dataset in order to throw it away. Everything
  before the last `FLUSHALL` — and before the last `FLUSHDB` for the database it
  targeted — is now recognised as dead and never applied, and a `FLUSHALL` in
  the log discards the preamble rather than loading it first.

- **A rewrite could restore keys into the wrong database.** `emit_dirty` emitted
  `SELECT` only when the target database was non-zero, so a key dirtied in db 0
  after one in db 3 was written with no `SELECT` in front of it and replayed
  into db 3. Reachable before, and reachable far more often with a preamble,
  where the tail is nothing but dirty keys.

Measured on 1.71M keys: a log with 4× write amplification recovering the same
dataset took 21.7s and peaked at 1.63 GB before, against 5.8s and 372 MB for the
equivalent snapshot. See [`docs/PERSISTENCE.md`](docs/PERSISTENCE.md).

- **`FLUSHDB` / `FLUSHALL` now actually release memory.** Both emptied each
  shard with `HashMap::clear()`, which drops the entries but keeps the bucket
  array at its high-water capacity. Flushing a 1.7M-key database left ~99 MB of
  empty hash tables resident that no `MEMORY PURGE` could reclaim, because the
  process still owned them. On top of that, the freed keys and values are small
  allocations that glibc parks in its arena free lists, and nothing asked for
  them back. Since `used_memory` is process RSS, the net effect was a flush that
  reported *zero* memory released. Flushes now drop the tables outright and trim
  the allocator; on the 1.7M-key case RSS falls from 269 MB to 8.8 MB against an
  8.1 MB empty-server baseline, where before it stayed pinned at 271 MB.

  That path still missed search indexes. `FT.CREATE` HNSW/FLAT graphs, inverted
  indexes, and FT dictionaries live in `index_store`, not the key HashMaps, so a
  vector benchmark followed by `FLUSHALL` showed `DBSIZE 0` with ~1.36 GB still
  resident. Flushes now drop those indexes (Redis Stack does the same) and trim
  again.

  Small tables keep the previous keep-capacity behaviour: freeing them returns
  nothing to the OS and would make the next fill pay to grow them back, which
  matters because the test suite flushes before every test.

  The same release path is now used by `CLUSTER RESET HARD`, and a replica trims
  after a full resync swaps the previous dataset out.

  Trade-off: a flush that reclaims memory costs more than one that does not.
  `FLUSHDB` over 1.26M keys goes from ~190 ms to ~370 ms, the extra time being
  the `malloc_trim` that hands ~180 MB back. Command throughput is unmoved —
  `SET`/`GET`/`INCR` measured within run-to-run noise of the previous release.

- **Idle `--aof` instances no longer `fsync` when the log is already durable.**
  The writer timer still drains, but if nothing is queued *and* `AOF_WRITTEN`
  already equals `AOF_FSYNCED`, it skips `sync_data` instead of hopping to
  tokio's blocking pool every second. Writes that landed on the recv arm are
  still fsynced on `WAITAOF` / the 1s timer, because those counters will
  differ. `INFO connected_clients` also stopped cloning every client just to
  count them.

- **`CONFIG SET appendonly yes` on io_uring now logs writes that follow it.**
  The uring connection loop froze `aof_enabled` at accept time, so a server
  started without `--aof` kept treating every command as unlogged after the
  toggle. Combined with an asynchronous checkpoint that returned `OK` before
  the dump+delta cut finished, the next write (`post:toggle` in the replica
  durability check) missed both files. Tokio already re-read the flag per
  command, which is why that leg passed. The checkpoint now finishes before
  `OK`, and uring re-reads `AOF_ENABLED` on every command. Waiting for the AOF
  writer uses a std channel: `oneshot::blocking_recv` panics inside a Tokio
  runtime and was closing the connection on both I/O models.

- **Stream consumer groups survive `SAVE` / `BGREWRITEAOF`.** The dump stored
  stream entries and `last_id` only. Groups, the pending-entry list, consumers,
  `entries_added`, `max_deleted_id` and `max_entries` lived solely as AOF
  commands. After checkpoints reset the log to a `KIVIDLT1` delta, a restart
  recovered the entries and dropped every group — `XINFO GROUPS` came back
  empty, including the Linux rewrite-durability check (`g1` / pending `2`).
  Dumps now use a `TAG_STREAM_V2` payload; older `TAG_STREAM` files still load.
- **`used_memory_peak` reports peak resident memory, not peak virtual address
  space.** It was sourced from Linux's `VmPeak`, which counts address-space
  reservations — tokio's thread stacks and io_uring's rings alone put an idle,
  empty server at `used_memory_peak:1.4G`, and made `used_memory_peak_perc`
  meaningless. It is now a high-water mark of observed RSS.

## [1.0.4]

### Added

- **Redis 8.8 array type** — all 18 `AR*` commands (`ARSET`, `ARMSET`, `ARGET`,
  `ARMGET`, `ARGETRANGE`, `ARDEL`, `ARDELRANGE`, `ARLEN`, `ARCOUNT`, `ARINSERT`,
  `ARRING`, `ARNEXT`, `ARSEEK`, `ARSCAN`, `ARGREP`, `AROP`, `ARLASTITEMS`,
  `ARINFO`). Sparse index-addressed values with an insert cursor and ring-buffer
  mode. Fully integrated with snapshots, AOF rewrite, DUMP/RESTORE, replication,
  `TYPE`, `MEMORY USAGE`, and a new `@array` ACL category.
- **`INCREX`** (Redis 8.8) — atomic increment with `LBOUND`/`UBOUND`/`SATURATE`
  bounds and `EX`/`PX`/`EXAT`/`PXAT`/`PERSIST`/`ENX` expiry control.
- **`XNACK`** (Redis 8.8) — release pending stream entries back to the group PEL
  for immediate re-delivery, with `SILENT`/`FAIL`/`FATAL` modes.

See [`docs/REDIS_8_8_COMMANDS.md`](docs/REDIS_8_8_COMMANDS.md) for the storage
model, two resolved spec ambiguities, and known deviations.

### Fixed

- **Replicas can now be re-pointed between masters.** `REPLICAOF` spawned a new
  replication task without cancelling the previous one, and the old task only
  exited when the role changed *away from* `"slave"` — which never happens when a
  replica is merely moved between masters. A replica whose master died could
  therefore never be re-pointed at the new one, breaking automatic failover.
  `ReplState` now carries a replication epoch that supersedes stale tasks,
  checked once per streaming-loop iteration so a task blocked on a dead master
  exits promptly.
- **`INFO replication` / `ROLE` report the live master.** Both read the
  boot-time `--replicaof` value, which `REPLICAOF` never updated, so a
  re-pointed replica reported the old `master_host` indefinitely. `ROLE` also
  hardcoded its link status to `connected`, hiding an unreachable master from
  anything driving failover off `ROLE`.
- **Repeated `REPLICAOF` to the current master is now a no-op**, matching Redis.
  Operators re-issue it every reconcile pass; without this it would force a full
  resync each time.
- **`AUTH` replies with the simple string `+OK`**, not a bulk string. Clients
  text-matching a leading `+OK` on control connections read the old reply as a
  failed AUTH.
- **`QUIT` is implemented** — replies `+OK` and closes the connection. It
  previously fell through to the generic fallback, returned
  `-ERR unknown command`, and left the socket open.

## [Unreleased]

### Added

- Project documentation: README with benchmark context, BUSL-1.1 license, Contributor
  Covenant code of conduct, CI (fmt, clippy, test, build), and tag-based releases with
  Linux/macOS artifacts (tarballs, `.deb`, `.rpm`, and `SHA256SUMS.txt`).

---

For release-by-release highlights, see [GitHub Releases](https://github.com/kividbio/kividb/releases).
Each release includes automatically generated notes (merged pull requests and new
contributors) plus a short summary in the release description.
