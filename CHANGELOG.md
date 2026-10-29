# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0]

See [docs/releases/v1.2.0.md](docs/releases/v1.2.0.md) for what to check before upgrading.

**Read before upgrading:** in scripts, `pcall` and `assert` now behave as in
Redis. A script that reads `e.err` from an error `pcall` caught no longer
finds it there: `e` is now the `err` itself. One that gives `assert` a table
for its message now fails with `bad argument #2 to 'assert'`. See *Changed*.

### Added

- **Signed build provenance for every release file and image.** The
  release workflow signs a GitHub artifact attestation (Sigstore, with the
  workflow's own identity) for every tarball, `.deb` and `.rpm` in a
  release, and for the multi-arch image of each variant on GHCR (the same
  images Docker Hub carries). `SHA256SUMS.txt` shows a download arrived
  whole; the attestation shows it was built by this repository's release
  workflow from the tagged commit. Check one with
  `gh attestation verify <file> --repo kividbio/kividb`, or
  `gh attestation verify oci://ghcr.io/kividbio/kividb:<tag> --repo kividbio/kividb`.

- **`FT.SEARCH` and `FT.AGGREGATE` honour `TIMEOUT`.** A query's
  `TIMEOUT` (milliseconds) cuts it short; one that names none runs to the
  end, as before, unless `search-timeout` (`FT.CONFIG TIMEOUT`) is set:
  it is `0`, no limit, by default, because a query here can look at every
  document (`FT.SEARCH idx * LIMIT 0 0` counts them one by one) and a limit
  nobody asked for would make a large index's counts silently short.
  `search-on-timeout` (`FT.CONFIG ON_TIMEOUT`) says what running out
  means: `return` (the default) replies with what was found, and
  `total_results` is that partial count; RESP3 puts `Timeout limit was
  reached` in `warning`, while over RESP2 the client gets no signal.
  `fail` and `return_strict` reply with that as an error. Both settings
  are taken by `CONFIG SET` and the config file, and are one value with
  their `FT.CONFIG` names. What is bounded: the scan, the sort of the
  matches and `SORTBY`, the nearest-neighbour step of a filtered `KNN`,
  and FT.AGGREGATE's `GROUPBY`, `APPLY`, `FILTER` and `SORTBY`; a query
  that finishes, however late, is not reported as timed out, and every
  `FT.CURSOR READ` page of a timed-out aggregation carries the warning.
  In a cluster each master reports whether its part timed out and the
  coordinator applies `ON_TIMEOUT`; that report is sent only to a
  coordinator that asks for it, so masters of the previous release and of
  this one can be mixed during a rolling upgrade. `FT.HYBRID` does not
  take `TIMEOUT`. `search-global-timeout` is still not implemented.

- **`ts-duplicate-policy` (#127):** the duplicate policy of a time series
  made without one, by `TS.CREATE` or `TS.ADD`. It stays `last`, so that an
  upgraded server, and an AOF it replays, keep doing what they did; set it to
  `block` for RedisTimeSeries's default, where a second sample at the same
  timestamp is an error. A series keeps the policy it was made with. Config
  file and `CONFIG SET`; set it alike on a master and its replicas.

- **Client certificates pinned by name: `tls-auth-clients-allowed-names`.**
  A list of names, separated by spaces or commas; when it is set, a client
  that presents a certificate is accepted only if the certificate's subject
  CN or one of its DNS names matches one of them, and otherwise fails the
  handshake, as a certificate from another CA does. A name is exact, or
  starts with `*.`, where `*` stands for exactly one label, as in a
  certificate: `*.db1.nodes.example` takes `node-0.db1.nodes.example`, not
  `a.b.db1.nodes.example`, `db1.nodes.example` or `x.db2.nodes.example`.
  Case does not matter. It is checked wherever a certificate is asked for:
  clients and replicas on `tls-port` (with `tls-ca-cert-file` and
  `tls-auth-clients yes` or `optional`), and nodes on a `tls-cluster` bus.
  Empty, the default, is off. Config file, command line and `CONFIG SET`,
  for connections from then on; `CONFIG GET` gives it back lower case, one
  space apart. A refused certificate is logged (at most every 10 seconds)
  with the names it carried. Redis has no such setting: with one CA for
  many databases, it keeps the certificates of one from being taken by
  another.

- **TLS certificates are loaded again with `CONFIG SET`, without a restart
  (#156).** As in Redis, `CONFIG SET` of `tls-cert-file`, `tls-key-file`,
  `tls-ca-cert-file`, `tls-auth-clients`, `tls-client-cert-file` or
  `tls-client-key-file` reads the files again, also when the path is the
  same (a certificate renewed in place by cert-manager or Let's Encrypt), and
  every handshake from then on uses them: clients', and between the nodes of
  a cluster or a replica and its master. Connections already open keep
  theirs. Several in one `CONFIG SET` are loaded together, so that a
  certificate and its key change as a pair. Files that cannot be loaded
  change nothing: `Unable to update TLS configuration. Check server logs.`
  These were answered `OK` and changed nothing.

- **TLS is turned on, moved and off without a restart: `tls-port`,
  `tls-replication`.** As in Redis, `CONFIG SET tls-port <port>` opens the
  TLS port, also on a server that started without TLS, moves it, or closes
  it with `0`; `CONFIG GET tls-port` and `INFO tls` show the port open now.
  Opening or moving it loads `tls-cert-file` and `tls-key-file` first, so a
  server is given TLS with `CONFIG SET tls-cert-file … tls-key-file …
  tls-port 6380` in one command, or one setting after another. Closing the
  port closes only the port: TLS connections already open stay open. A port
  that cannot be had (`Unable to listen on this port`), files that cannot be
  loaded (`Unable to update TLS configuration. Check server logs.`) or a
  port with no certificate and key is refused, and nothing changes. While
  the port is closed, a certificate and its key are loaded once both are
  set. `CONFIG SET tls-replication yes|no` decides whether a replica's next
  connection to its master is TLS (the link that is up stays as it is); it
  needs `tls-cert-file`, `tls-key-file` and `tls-ca-cert-file`. Until now
  `tls-port` and `tls-replication` were fixed at startup, and the TLS files
  could not be set at all on a server that started without TLS.
  `tls-cluster` still is, and so is `tls-port` on a node with `tls-cluster
  yes`, which announces that port to the rest of the cluster.

- **TLS only: `port 0` turns the plaintext port off, with a `tls-port`.**
  As in Redis, `port 0` (`--port 0`, the config file, or `CONFIG SET port
  0`) means no plaintext port. There is no Unix socket here, so it needs
  the TLS port: a server started with `port 0` and no `tls-port` stops with
  an error that says so, and `CONFIG SET port 0` with no TLS port open, or
  `tls-port 0` with no plaintext port, is refused. `CONFIG SET port 0`
  closes only the port: plaintext connections already open stay open, as in
  Redis (`CLIENT KILL` them if they must go); `CONFIG SET port <n>` opens it
  again. With no plaintext port, `CONFIG GET port` and INFO's `tcp_port`
  say `0`. A replica tells its master its TLS port to be reached on
  (`REPLCONF listening-port`) when it has no plaintext port, and now also
  with `tls-replication yes`, as Redis does, unless `replica-announce-port`
  says otherwise. A cluster node with `port 0` needs `tls-cluster yes`; it
  is known to the cluster and its clients by its TLS port, and its bus is
  10000 above that port, as in Redis. A replica of a TLS-only master
  connects with `tls-replication yes`.

- **Active defrag (#113): memory freed by deletes goes back to the system.**
  After a reshard or a big delete at random, nearly every page still held
  a key that stayed, and the process kept its memory: 10 million keys of
  100 bytes with a random half deleted used 845 MB and held 1569 MB, until
  a restart. String values of up to 504 bytes now live in slabs of
  KiviDB's own, and a background thread moves the values of half-empty
  slabs into fuller ones, so that the half-empty ones empty and go back to
  the kernel: in the same test, 1426 MB came down to 986 MB within 70
  seconds. The values move a batch at a time, on a thread of their own,
  held to a share of one core, and nothing waits for it: under 9 million
  requests a second, throughput and p99 / p99.9 latency were the same with
  it running as with it off. A value something besides its key holds (a
  snapshot being written, a command running) is left where it is. On by
  default; Redis's settings, in the config file, on the command line and
  with `CONFIG SET`: `activedefrag` (`yes`), `active-defrag-ignore-bytes`
  (`64mb`), `active-defrag-threshold-lower` (`20`, per cent) and
  `active-defrag-cycle-max` (`10`, per cent of one core). `INFO memory`
  reports `active_defrag_running`, `active_defrag_hits`,
  `active_defrag_misses`, `value_slab_bytes` and `value_slab_live_bytes`.

- **`cluster-announce-client-port` (#110):** the port clients are told to
  reach a node on, in redirects (`MOVED`, `ASK`) and in `CLUSTER SLOTS`,
  `SHARDS`, `NODES` and `REPLICAS`, where it is not the port the nodes use
  between themselves. A node reachable from outside on another port (a
  Kubernetes NodePort, a load balancer that maps ports) can now say so;
  `cluster-announce-port` could not be used for it, as peers replicate and
  migrate through that port. It travels to the other nodes in the flags
  string, as the hostname does (`cport=<port>`): nodes of an older version
  skip it. Config file, command line, and `CONFIG SET` at runtime.

### Changed

- **Vector search: distances in SIMD, a filtered KNN without copies.**
  Every vector distance (HNSW search and insert, FLAT, the KNN step of a
  filtered query) was a scalar sum the compiler could not vectorise. The
  dot product, squared L2 and cosine are now AVX-512, AVX2 with FMA or NEON
  kernels, picked at run time, with a portable eight-way sum elsewhere; no
  new dependency. A cosine query is scaled to unit length once per search.
  A filtered `KNN` measures each document as the filter accepts it and
  keeps the best `k`, instead of copying every accepted key into a set and
  looking each up again. Results are the same up to float rounding. On an
  Azure D16s_v5, server on 8 CPUs, M 16, EF_CONSTRUCTION 128
  (redis/vector-db-benchmark; Redis 8.10.3 in brackets):
  dbpedia-openai-100K-1536 at ef 128 goes from 330 to 811 queries a second
  with one client (978) and from 1883 to 3894 with 16 (5005), recall 0.985
  (0.980); its upload from 126 s to 29 s (14 s). h-and-m-2048 with filters
  uploads in 41 s instead of 156 s (20 s); its filtered queries gain only
  a quarter, 9.0 to 11.3 a second with one client (167) and 52 to 69 with
  16 (964), recall 0.998 for both, because a TAG or NUMERIC filter still
  reads every document.

- **Release candidates are no longer published as GitHub Releases.** A tag
  such as `v1.2.0-rc1` is still built, its images are still pushed to GHCR
  (`ghcr.io/kividbio/kividb:v1.2.0-rc1`) and smoke-tested, and its Linux
  binaries, packages and checksums are pushed to GHCR beside them, signed:
  `oras pull ghcr.io/kividbio/kividb-binaries:v1.2.0-rc1`. The Releases page
  lists only releases.

- **Key specs come from Valkey's command files, and from KiviDB's own.**
  The key specs that `COMMAND INFO` and `COMMAND DOCS` report, and that a
  cluster routes by, were generated from Redis 8.10's command files, which
  are not under a licence KiviDB can ship in every build. They are now
  generated from Valkey 9.1.3's (BSD 3-Clause, see `NOTICE`). The commands
  Valkey does not have (the `AR*` array commands, `DELEX`, `DIGEST`,
  `INCREX`, `LMOVEM`, `BLMOVEM`, `SDIFFCARD`, `SUNIONCARD`, `XACKDEL`,
  `XDELEX`, `XNACK`, `XCFGSET`, `XIDMPRECORD`, `HIMPORT SET`) and `HSETEX`,
  which KiviDB implements as Redis 8 does, have specs written for KiviDB
  (`scripts/key_specs_kividb.json`). Which arguments are keys is unchanged.
  Some flags differ from Redis's:
  - `ARCOUNT`, `ARLEN`, `ARINFO` and `ARNEXT` are `RO` without `access`;
  - `ARINSERT` and `ARRING` are `insert`, not `update`;
  - `DELEX` is `RM delete`, not `RW delete variable_flags`;
  - the `STORE` and `STOREDIST` keys of `GEORADIUS` and `GEORADIUSBYMEMBER`
    carry `variable_flags`.

- **`AUTH <password>` is refused when no password is set, as in Redis
  (#135).** With the `default` user open (no `requirepass`), `AUTH` with a
  password replied `OK`; it now replies `ERR AUTH <password> called without
  any password configured for the default user. Are you sure your
  configuration is correct?`, which is how a client learns that the
  password it was given is not in use. `AUTH default <password>` still logs
  in. A client configured with a password for a server that has none now
  gets this error when it connects.

- **`CONFIG SET` refuses what it cannot set, as Redis does (#143).** A name
  that is neither KiviDB's nor Redis's (`bogus-name`, a misspelling) gets
  `ERR Unknown option or number of arguments for CONFIG SET`; it was taken,
  kept, and did nothing. A setting read only at startup (`threads`,
  `io-model`, `aof`, `repl-replica-max-backlog-bytes`, `backupdirname`,
  `bind`, `dir`, ...) gets `can't set immutable config`, unless set to what
  it already is; it was answered `OK` and left as it was. Redis's settings
  that mean nothing here (`maxclients`, `hz`, `timeout`, `tcp-keepalive`,
  the encodings, ...) are still taken, and `CONFIG GET` now gives back what
  was set for `hz`, `timeout`, `tcp-keepalive`, `activerehashing` and
  `lazyfree-lazy-eviction` too, and for `busy-reply-threshold` in a build
  without Lua. `CONFIG GET *` lists `latency-monitor-threshold`.

- **The image has no OpenSSL.** Its base is now distroless's `base-nossl`
  with `libgcc_s` copied in, rather than `cc`, which ships `libssl3`. KiviDB
  never loaded it (TLS is rustls, built into the binary), but image scanners
  reported its CVEs all the same: 12 of the 15 Trivy reported in October
  2026, one of them high. The 3 left are Debian's `tzdata` and
  `ca-certificates` updates, neither of which the binary reads, and go with
  the next distroless build.

- **String values take less memory (#113).** A value's block no longer
  carries the system allocator's header and rounding: 10 million keys of
  100 bytes take 1753 MB instead of 1898 MB (184 bytes a key instead of
  199). `used_memory` now counts each value's block at the size of its
  slab's class, a multiple of 16 bytes, which is what it takes: about 4
  bytes a key more than before for values of 100 bytes, so a `maxmemory`
  is reached that much sooner.

- **The cluster bus keeps one connection per peer for heartbeats (#112).**
  Every node pinged every other one each second over a new TCP connection:
  N x (N - 1) connections a second across the cluster, 552 at 24 nodes.
  Each peer is now pinged over a connection kept open between heartbeats,
  replaced when it breaks (a peer that restarts is reached again on the
  next heartbeat), and dropped on a late reply so that no PONG is read as
  the answer to the next PING. `CLUSTER INFO` reports
  `cluster_stats_bus_connects`, the outbound bus connections opened.

- **`pcall` hands back the error's message, and `assert` takes only a
  string, as in Redis (#90).** A table raised with an `err` that is a string
  or a number (`error({err = ...})`, `error(redis.error_reply(...))`) comes
  back from `pcall` as that `err`. A failed `redis.call`
  (`ERR value is not an integer or out of range`), and any other error from
  the scripting API (`cjson.decode`, `redis.sha1hex`, ...), comes back as its
  message, where it was an object whose text began `runtime error:` and
  ended in a traceback.
  `xpcall`, and a table without such an `err`, are left as they are.
  `assert`'s message is a string or a number: `assert(false, nil)` says
  `assertion failed!`, and a table is refused with
  `bad argument #2 to 'assert' (string expected, got table)`.

### Fixed

- **On io_uring, `CONFIG SET port` opens the plaintext port, and `port 0`
  closes it.** On the io_uring I/O model, the default on Linux, `CONFIG SET
  port` was refused with `Unable to listen on this port: Address already in
  use` for any port, also one nothing had used, and on a server started with
  `--port 0`: the check that the port is free was made again before each
  worker's socket, and the first of them made the port look taken. And
  `CONFIG SET port 0` answered OK but left the sockets listening: each went
  only when a connection woke its accept, and that connection was accepted
  and never answered. Now the port is opened with a socket for each worker,
  set as at startup, before the reply; `port 0`, or a move, shuts the old
  sockets down before the reply, so a connection that comes after it is
  refused, and one accepted just before is served. Connections already open
  stay up. The tokio I/O model was not affected, nor was `tls-port`. Moving
  the port on io_uring has failed this way since it came in 1.1.1; `port 0`
  is new in 1.2.0. Reported by KiviDB Cloud.
- **A replica no longer stops receiving writes after a partial resync to a
  promoted master, while both sides report it healthy.** A replica that is
  promoted (`CLUSTER FAILOVER`, `REPLICAOF NO ONE`) while clients write to
  it had its old replication session notice the promotion up to 200 ms
  later, and that session then wrote its last position over the new
  master's offset. The writes of that moment were in the backlog, and
  reached the replicas continued from it, but the offset no longer counted
  them. The next full sync restarted the backlog at the shorter offset
  (`Backlog restarted at offset X (it ended at X+N)`), and a replica that
  had been continued before it and was later restarted was sent the stream
  from N bytes too far on, in the middle of a command. It applied nothing
  from then on, and kept acknowledging the offset it was stuck at:
  `master_link_status:up`, `state=online`, `lag=0`, its offset and DBSIZE
  frozen. Now a session that has been superseded neither applies what it
  read after the promotion nor touches the offset. As safeguards, a master
  refuses to continue a replica from a position where no command begins,
  and if its offset and the stream its replicas were sent ever come apart,
  it disconnects them and takes a new replication id, so that each resyncs
  in full; and a replica whose stream does not read as commands resyncs in
  full instead of waiting. The same overwrite let a replica re-pointed with
  `REPLICAOF` apply a few writes of its old master. Found by a Kubernetes
  test of 1.2.0-rc2: 4 of 7 partial resyncs to masters promoted by a
  `CLUSTER FAILOVER` froze, about 150,000 keys behind.

- **A master drops a replica that has gone silent, after `repl-timeout`,
  as Redis does.** A replica whose host went away without closing the
  connection (a terminated VM, a lost network path, a paused container)
  stayed in `INFO replication` as `state=online`, its lag growing, and was
  counted in `connected_slaves` until the master restarted. Now a replica
  that is online and sends no `REPLCONF ACK` for `repl-timeout` seconds (60
  by default) is disconnected, with `Replication: disconnecting timedout
  replica <ip>:<port>` in the log, over plaintext and TLS alike.
  `min-replicas-to-write` and `WAIT` were not misled: they already went by
  each replica's last acknowledgement. Not new in 1.2.0. Reported by KiviDB
  Cloud.

- **A graceful shutdown no longer loses writes it acknowledged.** On SIGTERM
  or `SHUTDOWN`, the server stopped accepting connections but let the ones
  already open go on running commands while it wrote its final snapshot.
  The snapshot is cut at one moment, so a write made after that moment was
  acknowledged, then lost when the process exited. A test that wrote from
  six clients through a `SHUTDOWN` lost 2,000 to 3,700 acknowledged INCRs of
  about 23,000. Found by a Kubernetes test: deleting a master pod lost 92
  acknowledged writes. No command runs now from the start of the final
  snapshot to the exit, as in Redis; clients waiting on a reply see their
  connection close.
- **With the AOF on, a write over TLS is answered at once again, not at
  the next once-a-second sync.** In 1.2.0-rc1 a reply to a write waits
  until its record is in the AOF, and the connection writes the log itself
  to get there. TLS connections queued their records on a channel of
  their own, which a task copied onto the writer's. When the connection
  wrote the log, its record had often not been copied yet, and nothing
  else wrote it until the sync. A client doing SETs one after another over
  TLS got about one reply a second. Plaintext connections, reads and
  `aof-ack-after-write no` were not affected. TLS connections now queue on
  the writer's channel, as plaintext ones do. Reported by KiviDB Cloud.

- **A cluster node no longer stops answering for good after a `CONFIG
  GET`.** A node could stop answering everything, `PING` included, with
  its CPU idle, and never recover. It was seen on nodes of a sharded
  cluster soon after a master restarted, when an operator polls every node
  more often. `CONFIG GET` took the read lock on the node table twice in a
  row, and still held the first when it took the second. If the cluster bus
  asked for the write lock in between, as it does on every heartbeat, the
  bus waited for `CONFIG GET` and `CONFIG GET` waited behind the bus. Every
  other command that reads the cluster's topology then queued behind them,
  until no worker thread was left to answer anything. Now `CONFIG GET`
  takes that lock once. While looking we found and fixed a second deadlock
  of the same kind: a bus that took in a peer's slot claims at the moment
  an atomic slot migration finished could deadlock with it, because the two
  took the slot table and the migration table in opposite orders. A node
  whose bus has not run for 10 s now logs `The cluster bus has not run
  for N s`, and logs again when the bus runs again.

- **A manual failover no longer loses writes when its election is slow
  (#185).** `CLUSTER FAILOVER` has the master hold writes while its replica
  catches up and wins an election. The master held them for 5 s, and the
  replica took over whenever it won, however late that was. Each voter syncs
  its vote to disk before it answers, and on a busy disk that can take
  seconds. If the election took longer than 5 s, the master started taking
  writes again, and the ones it acknowledged after the replica took over
  and before it heard about it were lost. That is the likely cause of the
  63 writes one CI run lost. Now
  the master holds writes for 10 s, and the replica has 5 s from asking to
  catch up and win, as in Redis. If it hasn't won by then it gives the
  failover up (`Manual failover abandoned` in its log), and the master goes
  on as before once the pause ends. `CLUSTER FAILOVER FORCE` and `TAKEOVER`
  are unchanged.

- **A transaction, and a script, reach replicas and the AOF as one step
  (#172).** On the master a transaction ran as one step, but its writes went
  to replicas one by one, and a replica applied them one by one: a client
  reading from a replica could see half of a transfer, the money gone from
  one account and not yet in the other. The same was true of a script that
  writes more than once. In the AOF, a transaction's records were written
  separately, so a process killed between two of them left half the
  transaction in the log, and the restart replayed that half. Now, as in
  Redis, the writes of a transaction or a script that writes more than once
  go to replicas and to the AOF between a `MULTI` and an `EXEC`. A replica
  applies them together, with no command of its own clients in between,
  and a restart drops a transaction whose `EXEC` never reached the file
  (`Revert incomplete MULTI/EXEC transaction in AOF file` in the log) and
  cuts it off the file. One that writes once is sent as that one write, as
  before. A KiviDB replica following a Redis master, which has always sent
  `MULTI` and `EXEC`, now applies those transactions as one step too.
  Rolling upgrades: upgrade replicas before their master, as usual. A 1.1.x
  replica of an upgraded master still applies every write, one by one as
  before, but logs `Replicated command error (non-fatal): ERR unknown
  command 'MULTI'` (and `'EXEC'`) for every transaction; a 1.1.x server
  started on an AOF this version wrote logs the same while it replays it.
- **`VSIM` and `VLINKS` `WITHSCORES` score cosine as Redis does (#171).**
  They gave the cosine itself, from -1 to 1. Redis gives `(1 + cosine) / 2`,
  from 0 to 1: identical vectors score 1 in both, but vectors at right
  angles scored 0 here and 0.5 in Redis, and opposite ones -1 and 0. The
  order of the results was always the same. A threshold an application
  sets on the score (`score > 0.8`) now means what it means on Redis.
  Sets made with KiviDB's own `METRIC IP` or `METRIC L2` keep their scores.

- **Three small ones (#173):**
  - `DEBUG POPULATE` replied `OK` and made nothing. It makes its keys now,
    as Redis: `DEBUG POPULATE count [prefix] [size]`, `key:0` onwards
    holding `value:0`…, cut or padded with zero bytes to `size`, leaving
    keys that are there alone.
  - An `LMOVE` (and `RPOPLPUSH`, `BLMOVE`, `LMOVEM`) that emptied its source
    published `del` first, and the pop before the push. It now publishes in
    Redis's order: the push, the pop, then the delete.
  - `MEMORY DOCTOR`, finding fragmentation, said to turn on `activedefrag`,
    which is on by default. With it on, it says that it is working through
    it, and that `MEMORY PURGE` gives memory back at once.

- **`XREAD BLOCK` left its registration behind when it ended without data
  (#170).** A reader that timed out, was unblocked, disconnected or was
  woken by another of its streams stayed registered on every stream it
  had named until an `XADD` to that stream. A client polling idle streams
  with a short `BLOCK` grew the server's memory without limit. A reader is
  now taken out of every stream it waited on when its read ends, in
  constant time however many others wait on the same stream. (Found and
  fixed by @Parikalp-Bhardwaj in #175.)

- **With the AOF on, a write is acknowledged once its record is in the file
  (#147).** It was acknowledged as soon as its record was queued for the
  AOF's writer, so a process killed with no warning (`kill -9`, the OOM
  killer, a container stopped without a grace period) lost writes its
  clients had been told were made: one to four in our tests, more under a
  burst. As in Redis, the replies to a batch of commands that wrote now wait
  for their records to reach the file (the page cache: `appendfsync
  everysec` still syncs it within the second, so a crash of the machine
  can lose up to a second, as in Redis). Waiting connections are answered
  together by one write, a group commit; reads, and everything with the AOF
  off, wait for nothing. A connection writes its own records before it
  replies, with no thread to wake. It costs writes: on a 16-core server
  with the AOF on, 20-28% of `SET`s a second with 50 clients and
  pipelined, under 10% with one client, nothing for reads. The new setting
  `aof-ack-after-write no` (default `yes`, also `CONFIG SET`) gives 1.1.2's
  behaviour and speed back, for those who would rather lose the last few
  writes to a kill.

- **Pub/Sub: channel names are binary-safe, and a subscriber over the soft
  limit is cut off on time (#161).** `PUBLISH` refused a channel name that
  was not UTF-8, and `SUBSCRIBE` changed one into another (`\xff` became
  `�`); names are now bytes, as in Redis, through `PUBSUB CHANNELS` and
  `NUMSUB` and across a cluster's nodes. A subscriber over the 8 MB soft
  limit for 60 seconds was disconnected only when another message was
  published to it; a check every second now does it.

- **More of `INFO` and `CLIENT LIST` is measured (#152, #158).**
  `mem_replication_backlog` is what the backlog holds, and
  `mem_clients_normal` and `mem_clients_slaves` what the clients' query
  buffers hold (they were 0); `CLIENT LIST`'s `multi` is the commands a
  client has queued in its transaction (-1 outside one), `watch` the keys it
  watches, and `tot-mem` its query buffer (they were -1, 0 and 0);
  `clients_in_timeout_table` counts the clients blocked with a timeout.
  `INFO all` and `everything` give a `keysizes` count up to 10 seconds old,
  so that an exporter scraping them pays for one pass over the keys per 10
  seconds rather than one each time (`INFO keysizes` counts now).
  `MEMORY MALLOC-STATS` says what Redis says when it is not built with
  jemalloc, as KiviDB is not: `Stats not supported for the current
  allocator`; it was a fixed piece of XML. The allocator's internals
  (`allocator_active`, its ratios) are not something KiviDB can read, and
  `fd`, `obl`, `oll`, `omem` and `argv-mem` have no counterpart in how it
  writes replies.

- **Scripts: the `struct` library (#154).** `struct.pack`, `struct.unpack`
  and `struct.size`, with Redis's formats (`>`, `<`, `!`, `x`, `b`/`B`,
  `h`/`H`, `l`/`L`, `T`, `i`/`I` with a size, `c` with a size or `c0`, `s`,
  `f`, `d`). A script that used it failed with `attempted to access
  nonexistent global variable 'struct'`.

- **Shard channels: `SPUBLISH` reaches a replica's subscribers, and
  `SSUBSCRIBE` is routed in a cluster (#161).** Standalone, `PUBLISH` went
  down the replication stream to subscribers on replicas, and `SPUBLISH` did
  not. In a cluster, `SSUBSCRIBE` succeeded on any node, which got the
  channel's messages only if it had the channel's slot; as in Redis, it is
  now answered `MOVED` by a node that does not have the slot (a replica of
  its master takes it), and `CROSSSLOT` for channels of more than one slot.

- **Search: synonym groups had no effect (#130).** `FT.SYNUPDATE` kept its
  groups, and `FT.SEARCH` for one term of a group did not find documents with
  another. As in RediSearch, a query term now matches every term of the
  groups it is in, in documents indexed before the group was made as well as
  after. A quoted phrase is matched as written.

- **Vector sets: `VSIM ... FILTER` filters; `REDUCE` is refused; `VINFO`
  says how vectors are kept (#132).** `FILTER` was taken and ignored, so an
  application that filtered by attribute got elements that did not match. It
  now takes Redis's expressions over the elements' JSON attributes
  (`.color == "red" and .size > 3`, `"a" in .tags`, `.year in [2024,
  2025]`, arithmetic, `not`, `**`, ...), and only the elements that pass are
  scored; the search being exact, `FILTER-EF` changes nothing. An element
  without the field asked for does not pass, as in Redis; an expression that
  is not one is `ERR syntax error in FILTER expression`. `VADD ... REDUCE`
  was taken and ignored, and the set kept the dimension given; it is now
  refused. `Q8` and `BIN` are still taken, with the vectors kept at full
  precision, and `VINFO` reports `quant-type` `f32`, Redis's name for that,
  where it said `NOQUANT`.

- **Keyspace notifications: Redis's event names, events from the commands
  that sent none, and `keymiss` (#160).**
  - Named as in Redis: `INCR`, `DECR` and `DECRBY` send `incrby` (they sent
    `incr`, `decr`, `decrby`); `GETDEL` `del`; `GETSET` and `SETNX` `set`;
    `GETEX` `expire` or `persist`; `LMOVE` `lpop` or `rpop` on the source and
    `lpush` or `rpush` on the destination (it sent `lmove` on both). A
    listener on `__keyevent@0__:incrby` or `:set` missed these.
  - Sent now: `setrange` (`SETRANGE`), `setbit` (`SETBIT`, a `BITFIELD` that
    writes), `set` or `del` (`BITOP`'s destination), `pfadd` (`PFADD`,
    `PFMERGE`), `zadd` (`GEOADD`), `move_from` and `move_to` (`MOVE`), and the
    JSON, time series and vector set commands' own (`json.set`, `ts.add`,
    `vadd`, ...), under a new `d` class, which `A` includes, as in Redis.
  - `m`: a read that looks for a key and does not find it sends `keymiss`.
    It never fired.
  - A `notify-keyspace-events` with a character that is not a flag is
    refused, by `CONFIG SET` and in the config file; `KEAQ!` was taken as
    `AKE`.

- **The script cache had no limit (#155).** Every distinct script sent with
  `EVAL` stayed cached until `SCRIPT FLUSH`, so a client that wrote its
  values into the script (rather than passing them as arguments) grew it for
  ever: 20,000 such `EVAL`s left 20,012 scripts and 4.6 MB. As in Redis 7.4,
  the cache keeps 500 scripts sent only with `EVAL`, letting the least
  recently used go (`EVAL` or `EVALSHA` of one counts as a use), and counts
  them in `evicted_scripts` in `INFO stats`. Scripts loaded with
  `SCRIPT LOAD` are not counted and stay.

- **Scripts: `redis.log` writes to the log; `redis.replicate_commands()` and
  `redis.set_repl()` exist (#154).** `redis.log` did nothing; it now writes
  its arguments, joined by spaces, at Redis's level for it (`LOG_WARNING`
  as a warning, `LOG_NOTICE` as info, ...). `redis.replicate_commands()`,
  how scripts asked before Redis 7 to be replicated as their effects (job
  queues, rate limiters), returns true, as in Redis 7: KiviDB always does
  so. Scripts that opened with it failed before doing anything.
  `redis.set_repl(redis.REPL_ALL)` is taken; keeping a script's writes from
  the AOF or the replicas is not supported, and is refused rather than
  ignored.

- **A compaction rule wrote its open bucket as samples arrived, and
  `LATEST` changed nothing (#128).** As in RedisTimeSeries, the destination
  now gets a bucket once it has closed (when the first sample of a later one
  arrives), and again if a sample is added to it later; the bucket still
  open is reported only with `LATEST`, on `TS.GET`, `TS.MGET`, `TS.RANGE`,
  `TS.REVRANGE`, `TS.MRANGE` and `TS.MREVRANGE`. Dashboards and downsampling
  jobs saw partial values as final. A compaction knows its source:
  `TS.INFO` reports `sourceKey`, and a destination takes one source only
  (`TSDB: the destination key already has a src rule`). A snapshot with a
  compaction in it is not readable by 1.1.2 and older.

- **Scripts that only read run on a replica (#145).** `EVAL`, `EVALSHA` and
  `FCALL` were refused with `READONLY` on a replica before the script ran,
  even when it only read, so read/write splitting and cluster clients with
  `READONLY` failed. As in Redis, a script now runs on a replica and a write
  it makes is refused there; a script whose shebang declares it may write
  (no `no-writes` flag), and a function registered without `no-writes`, are
  refused before they run.

- **`SCRIPT KILL` replied `OK` to a script that had written, which ran on
  (#129).** It looked for the script's writes on the thread of the client
  that sent it, where there were none, so it answered `OK`; the script,
  knowing it had written, carried on, and the server stayed `BUSY`. As in
  Redis, it now answers `UNKILLABLE Sorry the script already executed write
  commands against the dataset.`, and `SHUTDOWN NOSAVE` is the way out.
  `FUNCTION KILL` is the same.

- **`ACL GENPASS` with 1 to 4 bits returned an empty password (#140).** It
  gives a hex digit per 4 bits, as Redis does: one digit for 1 to 4 bits,
  three for 9 to 12. Whole bytes were taken, so under 8 bits gave none, and
  an empty password matches an empty `AUTH`.

- **`CLUSTER REPLICAS` and `CLUSTER SLAVES` reply with an array (#138).**
  They replied with one string, the replicas' lines joined by newlines; now,
  as in Redis, one bulk string per replica in the `CLUSTER NODES` line
  format, and an empty array for a master without replicas.

- **`CLUSTER SLOT-STATS ORDERBY` sorts descending by default (#139).** It
  sorted ascending, so `ORDERBY key-count LIMIT n` returned empty slots;
  as in Redis, it now returns the busiest first unless `ASC` is given.

- **`WAITAOF` said a write was on disk with the AOF off (#137).** It replied
  `[1, 0]`, one local fsync, when nothing is written to an AOF. With the AOF
  off, `WAITAOF` with `numlocal` above 0 is now refused, as in Redis
  (`ERR WAITAOF cannot be used when numlocal is set but appendonly is
  disabled.`), and `WAITAOF 0 ...` replies `[0, 0]`. With the AOF on, the
  first number is 1 once the write is fsynced, whatever `numlocal` asked
  for, as in Redis; it was 0 for `numlocal` 0.

- **`ACL HELP` answers; `COMMAND` with an unknown subcommand is an error
  (#134).** `ACL HELP` was an unknown subcommand, in an error that said to
  try `ACL HELP`; it now lists ACL's subcommands, as Redis does. `COMMAND
  NOPE` replied nil; it now replies `ERR unknown subcommand 'NOPE'. Try
  COMMAND HELP.`, as the other containers and Redis do.

- **`COMMAND` no longer lists `GEODELBYMEMBER` (#126).** It was in the
  command tables with nothing behind it, and answered `unknown command`;
  Redis has no such command either (`ZREM` removes a member of a geo set).

- **`INCR` at the largest integer wrapped to the smallest (#124).** `INCR` on
  `9223372036854775807` stored `-9223372036854775808` (and a debug build
  panicked); it now answers `ERR increment or decrement would overflow` and
  leaves the value, as `INCRBY` and Redis do.

- **Monitoring saw less than Redis shows (#158).** The latency monitor's
  `command` and `fast-command` events are recorded when a command takes at
  least `latency-monitor-threshold` (in the config file too), as in Redis;
  they were recorded by `slowlog-log-slower-than`, so with the defaults
  `CONFIG SET latency-monitor-threshold` recorded nothing, and
  `fast-command` was any command under a millisecond rather than a command
  Redis flags fast. `LATENCY LATEST` gives four fields, the maximum
  included, which redis_exporter reads; `LATENCY HELP` answers.
  `INFO latencystats` gives each command's percentiles
  (`latency-tracking-info-percentiles`), from the same histogram as
  `LATENCY HISTOGRAM`, so to its power of two. `instantaneous_input_kbps`
  and `instantaneous_output_kbps` are measured; `INFO server` gives
  `slowlog_log_slower_than:-1` when the slow log is off, the
  `multiplexing_api` in use (`io_uring`, `epoll`, ...), the `config_file`
  and the `executable`; `maxclients` is what was set.

- **`mem_fragmentation_ratio` was virtual size over RSS, so `MEMORY DOCTOR`
  always warned (#152).** It is now Redis's: RSS over `used_memory`, on
  every platform (it was a fixed `1.0` on macOS and Windows), with
  `mem_fragmentation_bytes` their difference. An empty Linux server
  reported `176.19`, and one with 100,000 keys `56.67`, where the ratio was
  about 1.8; dashboards and alerts above 1.5 always fired. `MEMORY DOCTOR`
  follows Redis's rules: nothing to say about an instance using under 5 MB,
  fragmentation above 1.4 only when it is over 10 MB, and a peak half as
  large again as now. `used_memory_peak` is the peak of `used_memory`,
  sampled every 100 ms, not the most RSS seen when `INFO` ran;
  `used_memory_startup` is measured at startup, not a fixed 1 MB. `CONFIG
  GET lazyfree-lazy-user-del` answers `no`, which is what `DEL` does.

- **`LASTSAVE` was 0 after a restart, and some persistence fields never
  changed (#149).** `LASTSAVE` and `rdb_last_save_time` now start at the
  server's start, as in Redis, and move with every save of the whole
  dataset: `SAVE`, `BGSAVE`, the save at shutdown, a replica's after a full
  sync, and a checkpoint of the AOF (`BGREWRITEAOF`, an automatic rewrite,
  `CONFIG SET appendonly yes`), which writes `dump.kdb` too; they moved only
  for `SAVE` and `BGSAVE`. `aof_last_rewrite_time_sec`,
  `aof_last_rewrite_time_ms` and `aof_last_bgrewrite_status` report the last
  rewrite (they were always `-1` and `ok`), and `rdb_last_bgsave_time_sec`
  how long the last `BGSAVE` took, not when it ended.

- **`INFO stats` reported no commands, hits or misses on a quiet server
  (#136).** `total_commands_processed`, `keyspace_hits` and
  `keyspace_misses` were counted in batches of 128 on each thread, and only
  the full batches reached `INFO`: after a few commands, all three were 0,
  and a hit-rate panel showed a server doing nothing. Each thread's count is
  now added up when `INFO` reads it, at the same cost on the hot path.
  `total_reads_processed` and `total_writes_processed`, always 0, now count
  reads from and writes to client connections.

- **A backup sealed before a restart could not be cleaned up, and stood in
  the way of the next one (#150).** A backup's state was only in memory:
  after a restart, `BACKUP CLEANUP` said there was no backup and
  `BACKUP START` that the directory was not empty, until its files were
  deleted by hand; its `backup-sealed-ttl` never ran out. At startup, a
  complete backup in `backupdirname` is now taken up again as sealed
  (`BACKUP LIST`, `BACKUP CLEANUP`, and the TTL, counted from the seal), and
  what a backup cut short by the restart left there as failed, cleared by
  `BACKUP CLEANUP` or the next `BACKUP START`.

- **Settings `CONFIG SET` takes stopped the server in the config file
  (#142).** `tracking-table-max-keys`, `array-slice-size`,
  `array-sparse-kmax`, `array-sparse-kmin`, `stream-node-max-entries`,
  `stream-node-max-bytes` and `busy-reply-threshold` (`lua-time-limit`)
  were unknown directives, and had no flag either: they could only be set
  after every start. They are now read from the file, checked as
  `CONFIG SET` checks them. Redis's settings KiviDB has no use for
  (`maxclients`, `maxmemory-clients`, `client-query-buffer-limit`,
  `latency-tracking`, the encodings, and any other of Redis 8's) are
  accepted with a warning, as `hz` and `timeout` already were, so that a
  `redis.conf` carried over starts. A name that is neither KiviDB's nor
  Redis's still stops it.

- **Search: scores drifted, `DD` kept the documents, KNN was ignored in
  `FT.AGGREGATE` (#162).**
  - BM25 counted every write of a TEXT field as a new document, and a
    delete took nothing away: after documents came and went, a word's
    rarity and the average length were wrong, and one document's score for
    a word fell a hundredfold after 200 others were added and deleted. A
    field written again also still matched the words it had held. What
    each document's TEXT fields contribute is now kept, and taken back out
    when a field is written again or the document goes; a delete no longer
    walks every posting list to find its words.
  - `FT.DROPINDEX index DD` deletes the documents the index covered, as in
    Redis; they were kept.
  - A KNN clause in `FT.AGGREGATE` (`*=>[KNN 10 @v $q AS d]`) keeps the
    nearest `k` of what the filter left, exactly, with the distance under
    its name for `LOAD`, `SORTBY` or `GROUPBY`, and in a cluster the
    nearest of every master's; it was dropped, and every document went on.

- **A JSON document from Redis 8 made a whole snapshot unreadable (#157).**
  RedisJSON 8 saves an array of numbers all of one kind as a CBOR typed
  array (RFC 8746): `[1, 2]` as 8-bit integers, `[1.5, 2.5]` as half
  floats. The reader took them for byte strings, `CBOR byte strings have
  no JSON form`, and refused the file: a `dump.rdb` copied from Redis 8.10,
  a `--preload-file`, or a replica's full sync from a Redis 8.10 master
  loaded nothing, and `MIGRATE` of such a document failed with
  `ERR Bad data format`. Typed arrays of every width and byte order are
  read.

- **`MEMORY USAGE key SAMPLES 0` panicked (#151).** On a hash, a set or a
  sorted set it divided by zero: the worker thread panicked and the
  client's connection closed. `SAMPLES 0` now looks at every element, as
  in Redis and as `MEMORY HELP` says; a count that is not a number is an
  error rather than a silent 5, a negative one or an option other than
  `SAMPLES` a syntax error.

- **An AOF turned on at runtime grew for ever (#146).** The thread that
  rewrites the append-only file once it passes `auto-aof-rewrite-min-size`
  and grows by `auto-aof-rewrite-percentage` was started only by a server
  that had the AOF on at startup; after `CONFIG SET appendonly yes`, the
  file was never rewritten until a restart or a `BGREWRITEAOF`. It is now
  started whatever the setting, and waits while the AOF is off.

- **A snapshot could hold half a transaction (#148).** With the AOF off,
  `BGSAVE` and the save at shutdown copied the dataset a shard at a time
  while writes went on, so a `MULTI`/`EXEC`, a script or an `MSET` whose
  keys were in two shards could be saved half applied; and with it on, a
  checkpoint, a backup or a full sync could cut between two commands of
  one `EXEC`. A snapshot is now the dataset at one moment with the AOF on
  or off: it is captured with writes held, strings by reference, and
  written while they go on, and `EXEC` is one write to every cut.

- **A write of several keys could be seen half done (#159).** Keys are
  spread over shards that threads work on at once, and only `MSET`,
  `MSETNX` and `MGET` locked all of theirs together: a reader could find
  a `DEL` of 200 keys with some gone and some not, a `RENAME` with neither
  key there, a `SUNIONSTORE`, `SORT ... STORE` or `LMOVE` part way. In
  Redis every command is one step. A write naming keys of more than one
  shard now runs alone, as `EXEC` does: other commands wait for it, which
  costs only those writes, not single-key ones or `MSET`.
  And three transaction fixes:
  - a command refused by ACL (`NOPERM`) inside `MULTI` aborts the
    transaction, as any other refused command does: `EXEC` ran the rest;
  - a write that changed nothing (`SET ... NX` on a key that exists, `SADD`
    of a member there already, `EXPIRE` of a missing key) no longer fails
    a `WATCH` on the key, as in Redis;
  - on a replica, the writes its master sends fail a `WATCH`, as a client's
    do; they did not.

- **A client using client-side caching could be left serving changed keys
  (#161).** Invalidations were queued for a connection with room for 512
  (256 under io_uring), and one that found the queue full was dropped
  without a trace: the client kept a value that had changed, and was never
  told. The queue now holds 65536, which costs nothing until it is used,
  and a client that falls further behind than that is closed, with a line
  in the log, so that it drops what it cached, as Redis closes a client
  past its output buffer limit. `SPUBLISH` with the wrong number of
  arguments named `publish` in its error.

- **A replica of a replica got one copy of the data and nothing after
  (#144).** A replica took `PSYNC` from a node of its own, sent it its
  dataset, and never passed on its master's writes, while the node
  following it reported its link up. KiviDB does not chain replicas: a
  replica now answers `PSYNC` and `SYNC` with `ERR this server is a replica,
  and KiviDB does not chain replicas: replicate from its master instead`,
  so the would-be replica's link stays down, and says why in its log. A
  master made a replica (`REPLICAOF host port`) lets go of the replicas it
  had, rather than keep them connected with nothing to send them.

- **A script on a cluster node could write a key another node owns (#153).**
  `redis.call` was not routed: a script given a key of its own node could
  `SET` one of another node's slots, and the key was stored here, where no
  client would look for it, and copied to this node's replicas and
  snapshot. As in Redis, a script now uses only keys this node serves
  (`ERR Script attempted to access a non local key in a cluster node`),
  and, unless it is flagged `allow-cross-slot-keys`, only keys in its own
  slot: that of its first key, or of the first key it uses
  (`ERR Script attempted to access keys that do not hash to the same
  slot`). The flag, on a script's first line or a function, was accepted
  and ignored; it now does what it does in Redis.

- **Memory of deleted keys' table slots was never given back (#113).** A
  shard's hash table, of values and of expiry times, grew as keys came and
  kept its size as they went: after a reshard moved half the keys away, or
  a big delete, every empty slot stayed in memory. Each shard's tables are
  now looked at about once a second, and one holding under 4/9 of what it
  has room for is shrunk to its keys and an eighth more, as Redis resizes
  its tables from its cron. A table just shrunk, or just grown, is never
  sparse enough to shrink, so a shard does not go back and forth.

- **After a failover, every other replica of the shard resynced in full
  (#109).** A promoted replica started a new replication stream with
  nothing of the old one, so the failed master's other replicas, asking to
  continue the old master's stream, were sent the whole dataset again,
  just as the shard had lost a node. Now, as Redis does with `replid2`:
  - a replica keeps a backlog of the stream it reads (`repl-backlog-size`),
    in its master's numbering;
  - when it is promoted (election, `CLUSTER FAILOVER`, `REPLICAOF NO ONE`),
    its new stream carries on from where it had read, and it keeps the
    old stream's id as its second, valid up to that point
    (`master_replid2`, `second_repl_offset` in `INFO replication`);
  - a `PSYNC` under that id is continued from the backlog, and `+CONTINUE`
    names the new id, which the replica follows from then on.
  A replica that had read further than the new master still resyncs in
  full: it has writes the new master never saw.

- **An import cut short by a restart of its destination left its keys behind
  (#108).** `CLUSTER MIGRATION IMPORT` copies keys to the destination before
  it takes the slots over; tasks are kept in memory, so if the destination
  went down first, nothing removed the partial copy when it came back. The
  keys stayed, unreachable (`MOVED`) but counted by `DBSIZE`, returned by
  `KEYS` and `SCAN`, saved in snapshots, and live again if the slots were
  later given to that node. The destination now keeps the ranges of the
  imports under way in `<cluster-config-file>.imports` and, at startup,
  deletes the keys it holds in those of them it does not own. Keys moved by
  `MIGRATE` are not touched (they are the only copy). Also: a takeover whose
  cluster config could not be saved is refused, and the slots stay with the
  source, where the save's failure used to be ignored.
- **`CONFIG SET` of a cluster setting said OK and changed nothing (#111).**
  `cluster-slot-migration-handoff-max-lag-bytes` and
  `cluster-slot-migration-write-pause-timeout` now take effect at runtime,
  from the next slot migration's handoff, and `CONFIG GET` shows them. A
  `cluster-*` setting KiviDB does not have (`cluster-allow-reads-when-down`,
  `cluster-migration-barrier`, ...) is refused with Redis's
  `Unknown option` error, where it was taken and ignored. Other settings of
  Redis's that mean nothing here are still taken, as before.

- **Automatic failover could elect the replica with the least data (#107).**
  When a master failed, each of its replicas waited a random half second and
  stood for election, however far behind it was: with two replicas, the one
  that had read less could win, and the writes only the other had were lost.
  Now each replica asks the others how far they have read (a new `OFFSET`
  bus message; a node of an older version does not answer, and is not
  counted) and waits an extra second for each one ahead of it, as Redis
  does. A replica whose link to its master has been down longer than
  `cluster-node-timeout × cluster-replica-validity-factor` plus
  `repl-ping-replica-period` does not stand at all.
  `cluster-replica-validity-factor` is new (default 10, `0` = always stand),
  in the config file and with `CONFIG SET`. The election's waiting is also
  out of the failure detector's loop, which it held up for the length of the
  delay.

- **Script commands in a build without Lua** said "Lua scripting is not
  supported by KiviDB", which BullMQ and Sidekiq users read as KiviDB not
  running their queue. `EVAL`, `EVALSHA`, `SCRIPT`, `FUNCTION` and `FCALL`
  now say this build has no Lua and name the `lua` or `full` variant to
  use, with the Docker tag (#105).

- **32-bit builds** (i386 and other 32-bit targets) didn't compile (#99):
  the key hash used 64-bit constants in a `usize`, and a time series'
  `CHUNK_SIZE` was read from the snapshot as a `usize` from 8 bytes. Both
  are 64-bit on every target now, so a 32-bit server loads a 64-bit one's
  snapshot. Nothing changes on 64-bit: the same shard for every key, the
  same snapshot bytes.

## [1.1.2]

A patch release of 1.1.0. It replaces 1.1.1, which was withdrawn.

### Added

- **`INFO keysizes`** (Redis 8.10): for each database, how many strings,
  lists, sets, sorted sets and hashes have a length in each power-of-two
  bin (`db0_distrib_lists_items:1=2,4=1`). Redis keeps these as keys change
  and gives them in the default `INFO`; here they are counted when asked
  for (`INFO keysizes`, `all`, `everything`), a pass over the keys, so they
  are not in the default `INFO`, which monitoring asks for every few
  seconds, and cost writes nothing.

- **Redis 8.10 additions:** the keyspace event classes `o` (a key's value
  overwritten), `c` (its type changed) and `a` (array commands, part of
  `A`); `XIDMPRECORD`; per-client `tot-net-in`, `tot-net-out`, `tot-cmds`,
  `read-events` and pipeline-length counters, and `omem-shared` /
  `omem-unshared`, in `CLIENT LIST` and `CLIENT INFO`; `expired_keys_active`
  in `INFO stats`; the `expire-cycle` latency event; `nacked-count` in
  `XINFO STREAM FULL`. `CONFIG SET stream-idmp-duration` and
  `stream-idmp-maxsize` now take effect at run time (they were accepted and
  ignored).

- **VECTOR fields take `TYPE FLOAT64`, `FLOAT16` and `BFLOAT16`.** Only
  FLOAT32 was accepted. Hash blobs and query vectors are read in the field's
  type, as in Redis, and `FT.INFO` reports it as `data_type`. An unknown
  type gets Redis 8.2's reply, ``Bad arguments for vector similarity FLAT
  index `TYPE`: Unknown argument``. The index still holds `f32`, so the
  smaller types save no memory. A snapshot records the type in a new section
  after all the others. An older release that loads such a snapshot reads the
  field as FLOAT32: it refuses the field's query vectors and does not index
  new or rebuilt documents.

- **The `.deb` and `.rpm` packages run KiviDB as a service** (#44). They
  installed the binary and nothing else: no config and no unit, so the
  server could not be run as a service, and started from `/` it would have
  written its snapshot and AOF there. They now bring a systemd unit,
  `kividb.service`, that runs the server as an unprivileged `kividb` user
  with its data in `/var/lib/kividb`, and a config, `/etc/kividb/kividb.conf`
  (owned by `kividb`, mode `0640`, so `CONFIG REWRITE` works), that listens
  on `127.0.0.1:6380` with the update check off. Removing the package stops
  and disables the service; the `kividb` user and `/var/lib/kividb` are kept.

### Changed

- **`EVAL` scripts and functions have a Lua VM each**, as in Redis. The
  shared VM that runs scripts one at a time is reported as `used_memory_lua`
  (and Redis's newer name, `used_memory_vm_eval`), which was always 0, and
  no longer counts in `used_memory_vm_functions`; `used_memory_vm_total` is
  the two together, and `used_memory_scripts_eval` what the cached scripts'
  text takes. The VMs each thread keeps for parallel scripts are not in
  these figures. `SCRIPT FLUSH` lets go of the scripts' VMs, the threads'
  as well, as Redis lets go of its own (with `ASYNC`, more than 64 scripts
  are freed in the background); `FUNCTION FLUSH` leaves them, and `SCRIPT
  FLUSH` leaves the functions'. A flushed VM is made again when next
  wanted, and shows 0 until then. The two still share one lock: a script
  and a function never run at the same time, and `SCRIPT RESET` renews both
  under it at once.

- **A script's error ends as Redis's does**, naming the script and the
  place: `... script: <sha1>, on @user_script:1.` (the function's name for
  `FCALL`). `redis.error_reply` gives a message without a code the code
  `ERR` (`redis.error_reply('X')` is `-ERR X`), as Redis does. A new
  `SUBSCRIBE`, `PSUBSCRIBE` or `SSUBSCRIBE` past `maxmemory` under
  `noeviction` is refused with `OOM`, as Redis 8.10 refuses it.

- `HEXPIRE`, `HPEXPIRE` and the `EX`/`PX` of `HGETEX` and `HSETEX` take a
  time of 0 as now, as Redis 8.10 does: the fields go at once (`HEXPIRE`
  replies 2 for each). Only a negative time is refused, `invalid expire
  time, must be >= 0`. A time of 0 used to be an error.
- `RESTORE` tells its two errors apart as Redis does: `DUMP payload version
  or checksum are wrong` for the trailer, and `Bad data format` for a
  payload sealed right that holds no value it can read. A zero checksum is
  taken as none, as Redis writes with checksums off.
- `overwritten` and `type_changed` are announced before the command's own
  event for the commands that replace a value whole (SET, the `*STORE`s,
  SORT … STORE, BITOP …), and after it for RENAME, COPY and RESTORE, in
  Redis's order.

These follow Redis 8.10, the release KiviDB reports itself as; a client
written against an earlier KiviDB may notice them.

- `MSETEX` replies `1`, or `0` when an `NX`/`XX` condition stops it, instead
  of `OK` and nil.
- `ARSCAN` and `ARGREP … WITHVALUES` reply with `[index, value]` pairs
  instead of a flat list.
- `XINFO STREAM FULL` calls a group's and a consumer's pending entries
  `pending` (it was `pel`).
- `INCREX` is propagated as one `SET` of the result (`KEEPTTL`, `PXAT`, or
  plain for `PERSIST`), or `DEL`/`UNLINK` when the new TTL has already
  passed, and its keyspace event is `incrby` / `incrbyfloat`. SET, SETEX
  and PSETEX with a TTL announce `set` then `expire` (SETEX and PSETEX used
  to announce `setex` / `psetex`); RESTORE announces `restore`.
- `XADD … IDMPAUTO` hashes the fields as Redis does, in any order, so an
  entry's idempotent ID is the one Redis gives it. IDs recorded by an
  earlier release do not match new ones for the rest of their retention.
- Arguments that used to be ignored or read loosely are refused with
  Redis's errors: unknown `XREADGROUP` options and malformed IDs, a second
  `IDMP`/`IDMPAUTO` on `XADD`, `XCFGSET` without a parameter or with a
  value that is not an integer, a `DELEX` condition on a value that is not
  a string, a `SET`/`DELEX` digest that is not 16 characters, a negative
  `LIMIT` offset on `ZRANGEBYSCORE`/`ZRANGEBYLEX` (now an empty reply), and
  the hash field-expiry commands' `FIELDS` block. Many error messages are
  now Redis 8.10's wording (`INCREX`, `LMOVEM`, `SDIFFCARD`, `ARGET`,
  `ARGREP`, `XCFGSET`, the slowlog settings).

### Fixed

- **Expired keys no longer pile up under a steady stream of writes with a
  TTL** (#87). The background expiry took them twenty at a time, each time
  searching the shard from its start, so it slowed as the keyspace grew:
  under a steady load of `SET ... EX 20` at about 90,000 a second on 8
  vCPUs, the server held 17.6 million keys where 1.8 million were live, and
  its memory grew without end (6.5 GB after three minutes, against Redis's
  271 MB). Reads never returned an expired key, but the memory was not
  given back. Now expired keys are found and deleted 1,024 at a time, each
  batch under one lock, by a quarter of the cores (at least one thread),
  each going through its own shards; a thread whose cycle runs out of time
  while more than a tenth of the keys it looked at were expired starts the
  next straight away instead of 100 ms later, as in Redis. Under the same
  load the keyspace stays at the live 1.8 million keys and memory is flat;
  on 8 vCPUs expiry keeps up with about 200,000 keys coming due a second
  under load, and deletes about a million a second when idle. Faster than
  that it still falls behind: a pipelined `SET ... EX` load of about 600,000
  a second outgrew it. This was in 1.1.0 too.

- **Packages: the service is enabled and started whenever the package
  brings the unit.** An upgrade from the 1.1.0 `.deb`/`.rpm`, which had no
  unit, installed it but left it disabled and stopped (the upgrade branch
  only restarts a running service). On Debian, an install after
  `apt remove` (without purge) did the same. The package now looks before
  unpacking: if its unit is not on disk yet it is enabled and started;
  otherwise an upgrade restarts it, and a unit the administrator disabled
  stays disabled.

- **`error()` given a table is reported by its `err`.**
  `error({err = 'ERR ...'})` and `error(redis.error_reply(...))` replied
  `ERR user_script:1: table: 0x... script:`; they reply with the message,
  line breaks in it made spaces, and `error({})` with `ERR unknown error`.
  `redis.acl_check_cmd` refuses a command with the wrong number of
  arguments (`Wrong number of args for redis.acl_check_cmd()`) instead of
  answering for it. Loading many functions no longer lets the Lua heap run
  a third over what they hold: the collector is stepped as they load, as in
  Redis. An `HSETEX` with a time already past announces `hset` and then
  `hdel`; it announced only `hdel`. **The clients a `CLIENT PAUSE` held go
  on before anything else once `CLIENT UNPAUSE` ends it**, as in Redis: a
  `CLIENT UNBLOCK` sent straight after found the `BLPOP` the pause had held
  not yet blocked, and answered 0. They also go on at once; they looked
  again only every millisecond.
  **`XTRIM` and `XADD` trimming with `ACKED` pass over an entry a group
  still needs** and go on to the next, as Redis does; they stopped at it.
  With `~`, under `DELREF` or `ACKED`, a node that cannot go whole is gone
  through entry by entry, as in Redis. **`XDELEX … ACKED` deletes an entry
  of a stream with no consumer groups**, as Redis 8.10 does: nothing refers
  to it. It answered 2, so that once the last group was destroyed nothing
  could be deleted with `ACKED`.
  `XACKDEL` on a group that does not exist answers -1 for each ID, as for
  a key that does not exist, as Redis 8.10 does; it was a `NOGROUP` error.
  `XNACK` takes its options before the `IDS` block as well as after it,
  names an option it does not know, and answers `NOGROUP` for a key that
  does not exist (it answered 0), as Redis 8.10 does. `XGROUP CREATE` and
  `SETID` cap `ENTRIESREAD` at the entries the stream has had added.
  `XREAD` refuses `CLAIM`, which is `XREADGROUP`'s; `CLAIM`'s time is read
  as Redis reads an integer (`+10` is not one) and a negative one is
  refused. An entry `XCLAIM … FORCE` creates starts as one delivery, as
  in Redis (it started at none).
  Entries `XNACK` releases are claimed in the order they were released,
  and one released again goes to the back, as in Redis's NACK zone (they
  were claimed by ID). `CLAIM -0` is not an integer, as Redis reads one.
- `MEMORY USAGE` of a string is what Redis 8 reports for one: its object's
  header, key and value, rounded up to the allocator's size class. It was
  a share of the key table's entries plus the value, which for small
  strings came out above the most Redis's suite allows (64 bytes for a
  1-byte key and value; it said 73).
- `LATENCY GRAPH` draws Redis's graph: a header with the high and the low
  (`<event> - high N ms, low N ms (all time high N ms)`), a rule, and an
  ASCII sparkline of four rows labelled with how long ago each sample was.
  It was a line of Unicode block characters. (The all-time high is the
  highest of the samples kept.)
- **A `BCAST` tracking client is told only of the keys its user may read**,
  by the user it is authenticated as now, as Redis 8.10 does. It was told
  of every key its prefixes matched, keys its user had no access to
  included.
- `CLIENT INFO`/`CLIENT LIST`'s `tot-cmds` counts a command once it has
  run, as Redis does: a `CLIENT INFO` does not count itself, and a blocked
  `BLPOP` counts once, when it is answered (it counted when it began, and
  again when it was answered). The commands a script runs (`redis.call`)
  count as its client's.
- **`COMMAND GETKEYS` and `GETKEYSANDFLAGS` know the commands Redis 8.10
  added** (`DELEX`, `DIGEST`, `INCREX`, `LMOVEM`, `SDIFFCARD`, the array
  commands and the stream ones): the key specs are now made from Redis
  8.10.2's command descriptions, not 8.0.0's, by `scripts/gen_keyspecs.py`.
  `DELEX key` is `RM delete`, and with a condition `RW delete`, as in
  Redis. `GETKEYSANDFLAGS delex k1` was `Invalid arguments specified`.
- **`ARINFO FULL` reports an array's slices as Redis keeps them**: dense or
  sparse, by Redis's rules (a slice turns dense past `array-sparse-kmax`
  elements, and sparse again when a deletion leaves `array-sparse-kmin` or
  fewer), with Redis's fields `avg-dense-size`, `avg-dense-fill` and
  `avg-sparse-size` in place of `avg-slice-size` and `avg-fill-rate`. It
  had called a slice dense when it was half full. `array-slice-size`,
  `array-sparse-kmax` and `array-sparse-kmin` can be set as in Redis.
  `ARLASTITEMS` after `ARSEEK 0` gives the array's tail, and an insert past
  the last index is `insert index overflow`, as in Redis.
- **`ARRING` follows Redis 8.10**: the ring is the array itself. A size
  that shrinks it, or grows it after it has wrapped, rebuilds it from the
  newest item back as far as the first hole, oldest first from index 0,
  so a grown ring uses its new room. `ARSEEK` only moves the cursor, and
  `ARLASTITEMS` walks back from it, wrapping, with a nil for a hole.
  `ARINFO` no longer reports `ring-size`, which Redis does not have, and
  `OBJECT ENCODING` of an array is `sliced-array`.

- **`CONFIG SET` of a setting under both its names is a duplicate**
  (`busy-reply-threshold` and `lua-time-limit`, and the other
  `slave`/`replica` and `ziplist`/`listpack` pairs), and
  **`backup-sealed-ttl` takes effect at run time** (it was accepted and not
  applied). The `overwritten`/`type_changed` bookkeeping added in #66 no
  longer costs every command: it was 16 instructions a command with the
  events off, the default (measured with callgrind); now it is one branch on
  a write.

- **Function libraries are kept in `dump.kdb`** (#70). The snapshot had no
  place for them, so whatever wrote one dropped every library loaded with
  `FUNCTION LOAD`: `SAVE` and a restart, a normal stop with the AOF on
  (which rebases the log onto the final snapshot), `BGREWRITEAOF`, and a
  replica's full sync, which left the replica without them until the next
  `FUNCTION LOAD`. After any of these, `FCALL` returned `ERR Function not
  found`. Redis keeps them in its RDB file. They are now a last section of
  the snapshot, loaded at startup, from `--preload-file` and in a full sync,
  and they are in `BACKUP`'s base. An older release reads such a snapshot
  without them.
- **Redis 8.10 compatibility** (#66 and the work after it). CI now runs
  Redis 8.10.2's test suite (#64): it went from 2079 passed, 76 failed and
  7 units that hung or stopped early, to 2400 passed and 70 failed with 4,
  most of the failures left in units that now run to the end. Among the
  fixes: `INCREX` options, errors and float overflow; `XREADGROUP` option
  checks; `HSETEX` and `HGETEX` options after the `FIELDS` block; `GETRANGE`
  with both ends negative; `LMOVEM`'s option order; the slowlog INFO times
  to two places and the slowlog settings' minimums; `COMMAND
  GETKEYSANDFLAGS` for `MSETEX` and for `SET` with a condition; `XINFO`
  counting only idempotent IDs still within their retention, and `XCFGSET`
  keeping them when a value is set again unchanged.

- **`FUNCTION RESTORE` accepts payloads produced by Redis 7.4 and 8.10**
  (#45). It previously refused them with `ERR DUMP payload version or checksum
  are wrong`; Redis function-library payloads now restore atomically while
  KiviDB's own format continues to work.
- **A HyperLogLog keeps its cached count where Redis keeps it** (#28). The
  count was never cached: every `PFCOUNT` counted all 16,384 registers again,
  and bytes 8-15 of the value did not say what Redis's say. Now `PFCOUNT`
  writes the count into bytes 8-15 (little-endian) and the next `PFCOUNT`
  reads it from there; a `PFADD` that changes a register, a `PFMERGE` and
  `PFDEBUG RESET` mark it stale with the top bit of byte 15, as Redis does.
  A HyperLogLog saved by an earlier release is counted again the first time,
  then cached. The registers themselves are still not Redis's (another hash,
  no sparse encoding), so HyperLogLogs do not move between Redis and KiviDB.
- **An `XREADGROUP` that changes nothing is not counted as a change** (#27).
  Reading a consumer's own pending entries again (an ID other than `>`),
  and a `>` read that found nothing new, added one to
  `rdb_changes_since_last_save`, which Redis does not: only new entries
  delivered and a consumer created count. The delivery count and time of a
  re-read entry are still sent to replicas and the AOF.
- **`RESTORE` applies `IDLETIME` and `FREQ`, and `key-access-tracking yes`
  keeps `OBJECT IDLETIME` and `FREQ` under every policy** (#21). `RESTORE
  ... IDLETIME n` and `FREQ n` were read and ignored; now the restored key
  has that idle time (under an LRU policy) or that frequency (under LFU), as
  in Redis, and invalid values get Redis's errors. The idle time may be
  longer than the server has been up. A key's use is still recorded only
  while an LRU or LFU policy is selected, so under the default `noeviction`
  `OBJECT IDLETIME` is 0: recording it on every command costs about 500
  instructions a command (11-13% of a GET or SET) and 53 bytes a key. The
  new setting `key-access-tracking` (default `no`; config file, command line
  and `CONFIG SET`) records it whatever the policy, at that cost. `TTL`,
  `PTTL`, `TYPE`, `EXISTS` and `OBJECT` do not count as a use either way.

- **`CONFIG SET port` moves the listener, and `CONFIG SET` with several
  settings is refused whole for a bad value** (#24). `CONFIG SET port`
  replied `OK` and changed nothing: the server kept listening where it was.
  Now it binds the new port before it replies (every io_uring worker gets a
  socket of its own), then stops taking connections on the old one;
  connections already open stay open. A port that is taken is refused with
  Redis's `Unable to listen on this port`, and with it the whole `CONFIG
  SET`: the settings before it are put back. `CONFIG GET port` and `INFO`'s
  `tcp_port` report the port in use. Port 0 (no TCP at all, in Redis) is
  refused. `maxmemory-clients` and `client-query-buffer-limit` took any
  value; now they take what Redis takes (a size, or a percentage up to 100,
  and a size of at least 1 MB), so `CONFIG SET maxmemory 10000001
  maxmemory-clients 200%` fails and leaves `maxmemory` alone.

- **`UNLINK`, `FLUSHALL ASYNC`, `FLUSHDB ASYNC` and `FUNCTION FLUSH ASYNC`
  free in the background, and `INFO` counts it** (#19). Everything was freed
  in place, under the shard lock, and `INFO` reported nothing:
  `lazyfree_pending_objects` was always `0`, and `lazyfreed_objects` and
  `used_memory_vm_functions` were missing. Now a value `UNLINK` removes that
  takes more than 64 units of work to free (Redis's measure: a collection's
  elements, a stream's nodes and pending entries) is freed on a background
  thread, as are the keys of an `ASYNC` flush and the libraries and Lua VM of
  `FUNCTION FLUSH ASYNC`. `lazyfreed_objects` counts what was freed,
  `lazyfree_pending_objects` what is still waiting, and `CONFIG RESETSTAT`
  resets the first. As in Redis, a plain `FLUSHALL` or `FLUSHDB` counts its
  keys too, except in a transaction or a script. `DEL` still frees in place.
  `used_memory_vm_functions` is the size of the Lua VM the functions are
  loaded into; EVAL scripts that run alone share that VM. `MEMORY STATS`
  reports the same counts.

- **A message a subscribed connection publishes to itself comes after the
  reply** to the command that published it, as in Redis (#23). With RESP3, a
  `PUBLISH` to one of the connection's own channels, inside a Lua script or a
  `MULTI`/`EXEC` as on its own, could have its `message` push written before
  the reply, which confuses clients that match replies to requests in order.
  Such a push now waits until the reply is written. And `SUBSCRIBE`,
  `UNSUBSCRIBE` and their pattern and shard forms sent inside `MULTI` are
  queued like any other command, with their replies as elements of `EXEC`'s
  reply, as in Redis; they used to run at once, outside the transaction.

- **Data from Redis loads: `RESTORE` of a Redis payload, `MIGRATE` from a
  Redis server, a Redis `dump.rdb`, and a full sync from a Redis primary**
  (#39). `RESTORE` refused every payload Redis produces, whatever the type,
  with `ERR DUMP payload version or checksum are wrong`: KiviDB's CRC64 is
  not Redis's (its polynomial is written the wrong way round for the
  reflected loop), so no Redis checksum ever matched. And the RDB reader,
  which loads a `dump.rdb` at startup and a Redis primary's full sync, read
  most compact encodings under the wrong RDB type numbers, read a list
  node's container after its bytes instead of before, and misread listpack
  short strings (`"a"` came back as `353`), and read a 64-bit length as a
  32-bit one, so a file from Redis 6 or later holding lists, small hashes,
  small sets, small sorted sets or any stream did not load, or loaded wrong;
  it did not read streams, hashes with per-field TTLs or RedisJSON values at
  all. Now strings, lists, sets, sorted sets and hashes load from Redis 6.2
  to 8.10 in every encoding those versions write, and so do streams (every
  RDB stream type from Redis 6.2 to 8.10, with consumer groups, pending
  entries, entries released by `XNACK` and idempotent production), hashes
  whose fields have TTLs (Redis 7.4+; fields already past their time are
  dropped) and RedisJSON documents (from their CBOR form in Redis 8, and as
  JSON text before). All of it is checked against payloads and `dump.rdb`
  files captured from real servers; `MIGRATE` from Redis 8.10 moves them,
  TTLs included. KiviDB's own `DUMP` payloads keep their checksum, so
  payloads from earlier releases still restore. Values of other modules
  (time series, Bloom filters, vector sets) and Redis 8.10's newest types
  (arrays, template hashes, GCRA) are not read yet, and are refused.
- **A Redis `dump.rdb` saved under an LRU or LFU policy, or by a cluster
  node, loads** (#71). Under an LRU policy Redis writes each key's idle time
  (`IDLE`) in front of it, and under an LFU policy its access frequency
  (`FREQ`), after the key's expiry when it has one. A cluster node writes the
  slot of the keys that follow (`SLOT_INFO`). The RDB reader took these for
  a key's type, so such a file stopped KiviDB from starting with
  `unsupported RDB type 248` (or `249`, `244`, or `failed to fill whole
  buffer`), and a replica of such a Redis primary never synced.
  `allkeys-lru` is a common setting for a cache. They are now read past, at
  startup, from `--preload-file` and in a full sync. The idle time and
  frequency are not kept yet.
- **A Redis `dump.rdb` that holds the search module's index definitions or
  dictionaries loads, as does Redis Stack 7.4's** (#71). The search module
  saves its index definitions and dictionaries as data of its own
  (`MODULE_AUX`): Redis 8 once an index or a dictionary exists, and Redis
  Stack 7.4 always, even with neither. The reader took it for a key, so
  such a file stopped KiviDB from starting (`unexpected end of file`), or
  crashed it before #69. It is now read past, with a warning to run
  `FT.CREATE` and `FT.DICTADD` again when it held any. Another module's
  data, hash templates and module key metadata are refused by name, where
  they were read as a key.
- **A Redis `dump.rdb`'s function libraries are loaded** (#71), at startup,
  from `--preload-file` and in a full sync from a Redis primary. Redis keeps
  each library's code in the file (`FUNCTION2`), and the reader took it for
  a key's type, so such a file did not load. The libraries are now loaded
  as `FUNCTION RESTORE` loads a payload from Redis's `FUNCTION DUMP`, and
  kept in `dump.kdb` from then on (#70).
- **Nothing expires while writes are paused**, as in Redis (#22). During
  `CLIENT PAUSE` (`WRITE` or `ALL`), a slot handoff or a manual failover, a
  key whose time has passed stays where it is: a read takes it as gone, but
  it is deleted, counted in `expired_keys` and announced (`expired`) only
  once the pause is over. The background sweeps wait too, so hash fields'
  `hexpired` events and `expired_subkeys` also come after the pause. Before,
  reads and the sweeps deleted keys during the pause. Expiries are not replicated (every node expires keys on its own
  clock), so replication and failover were not affected. `RANDOMKEY` during a
  pause answers with an expired key, as Redis does, instead of nil.
- **`expired_keys` in `INFO` is exact.** It was counted per thread in batches
  of up to 128, so after a few expiries it could still read 0.

- **A backslash in a TEXT field escapes the next character, as in Redis.**
  `users\.u_1` is the one word `users.u_1`, in a document and in a query, so
  `@f:users\.u_1` no longer also matches `users\.u_1\.sub`, and
  `@f:users\.u_1*` is a prefix query instead of a syntax error. LangGraph's
  `RedisStore` depends on this to keep namespaces apart: before, `get` and
  `delete` on a namespace could reach an item in a namespace under it. TEXT
  indexes are rebuilt from the data when the server starts, so no reindex is
  needed after upgrading. Fuzzy terms (`%…%`) and `FT.SPELLCHECK` read words
  the same way, and a `$param` value is one word, not split at punctuation,
  as in Redis.
- **`MODULE LIST` names the built-in module families.** It lists `search`,
  `ReJSON`, `timeseries` and `vectorset` with the versions Redis 8.10
  reports and an empty path, as Redis lists its own built-in `vectorset`.
  `HELLO` reports the same list under `modules`, and
  `COMMAND LIST FILTERBY MODULE search` (or `ReJSON`, `timeseries`,
  `vectorset`) lists that family's commands.
  It was empty, and clients that check it before using FT.* refused to
  connect: redisvl 0.4, which LlamaIndex's Redis vector store requires,
  raised `RedisModuleVersionError` on `RedisVectorStore(redis_url=...)`.

- **`CLIENT LIST` and `CLIENT INFO` report `qbuf`, `qbuf-free` and `idle`
  truthfully** (#32, #60). `idle=` was always 0 and the query-buffer fields
  were placeholders. `idle=` is now the seconds since the connection last
  ran a command, on a monotonic clock, and listing clients does not reset
  it. A client's query buffer grows for a large argument and is shrunk back
  once it is no longer needed, as in Redis: under the Tokio server on
  Redis's two-second idle timer, under io_uring at the client's next read
  (an idle client's buffer is not shrunk until it sends something). The
  io_uring server now reads straight into the buffer it parses, saving a
  copy of every read.
- **A hash field whose time had passed came back from a snapshot as a field
  that never expires** (#48): the loader dropped its time and kept the
  field. It is now dropped when the snapshot loads, and a hash left with no
  fields is not loaded at all.

### Security

- **A `RESTORE` could crash the server.** A payload whose string claimed a
  length near 2^64 made the RDB reader allocate that much at once, and the
  process aborted. The payload's checksum did not stop it: anyone can compute
  one for their own payload. A crafted RDB from a primary in a full sync
  could do the same. Strings are now read as the bytes arrive, an LZF
  string's claimed size is bounded by what LZF can produce, and such a
  payload is refused with `ERR Bad data format`. Affects 1.1.0 and earlier.

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
- **`CONFIG REWRITE` keeps the config file's mode** (#51). It wrote a new
  file with the default mode, so a config kept at `0600` or `0640` because
  it holds `requirepass` or ACL passwords became readable by every user. The
  rewritten file now has the old one's mode, and its owner when the server
  runs as root; a config written for the first time is `0600`. A file or
  symlink already at the temporary path is replaced, not written through.
  Redis resets the mode to `0644` less the umask on every rewrite, so this
  differs from it on purpose.

## [1.1.1] - 2026-10-23 [YANKED]

Withdrawn: under a high rate of writes with a TTL, background expiry fell
behind and memory grew without bound (#87). Its changes are all in 1.1.2,
with the fix; use 1.1.2.

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

## Before 1.0.4

### Added

- Project documentation: README with benchmark context, BUSL-1.1 license, Contributor
  Covenant code of conduct, CI (fmt, clippy, test, build), and tag-based releases with
  Linux/macOS artifacts (tarballs, `.deb`, `.rpm`, and `SHA256SUMS.txt`).

---

For release-by-release highlights, see [GitHub Releases](https://github.com/kividbio/kividb/releases).
Each release includes automatically generated notes (merged pull requests and new
contributors) plus a short summary in the release description.
