# Running and configuring KiviDB

- [Running KiviDB](#running-kividb)
- [Protected mode](#protected-mode)
- [Configuration](#configuration)
- [TLS](#tls)
- [Health checks](#health-checks)
- [Logs and errors](#logs-and-errors)
- [Startup update check](#startup-update-check)

## Running KiviDB

```bash
./target/release/kividb                    # listens on 0.0.0.0:6380
redis-cli -p 6380 PING
```

Any Redis client works; point it at port **6380**. KiviDB listens on **6380**, not Redis's
6379, so the two can run side by side; point your client at that port, or pass
`--port 6379`. The default listen address is `0.0.0.0:6380`, with
[protected mode](#protected-mode) on.

[INSTALL.md](INSTALL.md) covers the packages, images and binaries;
[BUILDING.md](BUILDING.md) covers building from source.

## Protected mode

Protected mode is **on by default**, as in Redis. While it is on, **no bind address has been
set** (`--bind` or `bind` in the config file) and the **`default` user has no password**, only
clients on the same machine (loopback) may connect. Any other client
receives a `-DENIED` error that explains the options, and is disconnected.

To accept clients from other machines, do one of these:

- set a password: `--requirepass <password>`, or `ACL SETUSER default ><password>`;
- set a bind address: `--bind <address>`;
- turn protected mode off: `--protected-mode no`, `protected-mode no` in the config file, or
  `CONFIG SET protected-mode no` from a local client.

If you turn it off or bind to a public address, make sure the server cannot be reached from
the internet without a password.

The Docker image runs with protected mode off by default; see
[Docker](INSTALL.md#docker).

## Configuration

KiviDB is configured with command-line flags, or with a Redis-style configuration file
(one directive per line, such as `port 6380` or `appendonly yes`). The file is read from
`/etc/kividb/kividb.conf` if it exists; `--configfile <path>` names another one. A server that
read the default file without `--configfile` naming it logs a warning and refuses
`CONFIG REWRITE`, since other servers on the host may read the same file; a future release will
read a config file only when one is named, as Redis does. The packaged systemd unit passes
`--configfile /etc/kividb/kividb.conf`. A flag given
on the command line wins over the same setting in the file. Run `kividb --help` for the full
list.

Any directive the config file or `CONFIG SET` takes is also taken on the command line in the
form `redis-server` uses, `--<directive> <value>`, so a Compose file, Helm chart or platform
template written for Redis starts KiviDB unchanged:

```bash
kividb --appendonly yes --requirepass s3cret --maxmemory 256mb --maxmemory-policy allkeys-lru
kividb /etc/redis.conf --save "" --loglevel warning     # a config file first, then overrides
```

A directive is read as a config line is, with the same checks: a value that is not valid, or a
name that is not known, stops the server at start-up with a message that names it (and the
closest known name, for a typo). A directive may have several values (`--save 900 1 --save 300
10`), an empty one (`--save ""`), or one that starts with a dash. The first argument, when it
does not start with `-`, is a config file, and `-` as the first or the last argument reads config
text from standard input (so `--dir -` as the last argument is an error, as in Redis). Names are
not case-sensitive, and `--name=value` is taken too, with spaces in the value (`--dir=/my dir`);
`redis-server` does not accept that form. `--bind a b` lists two addresses; KiviDB listens on
one, and warns, as it does for the config file. A directive that KiviDB accepts but
does not act on (`--save 900 1`, `--tcp-keepalive`, `--io-threads`, ...) logs one warning.

The order of precedence, strongest first: a flag of the table below, a directive on the command
line, the config file, the default. A flag wins over the directive for the same setting
whatever their order (`--aof --appendonly no` turns the append-only file on), and no warning
is logged for the directive it overrides. A flag given twice counts once, the last.

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `6380` | Port to listen on; `0` for none (TLS only, with `--tls-port`) |
| `--bind` | `0.0.0.0` | Address to listen on. Setting it explicitly turns off [protected mode](#protected-mode) |
| `--protected-mode <yes\|no>` | `yes` | See [Protected mode](#protected-mode) |
| `--enable-debug-command <no\|yes\|local>` | `no` | Who may run `DEBUG`: nobody, any client its ACL user allows, or only clients on loopback; startup only (see below the table) |
| `--threads` | `0` (auto) | Worker threads; auto is half the CPUs the process may use, rounded up (see [Worker threads](#worker-threads)) |
| `--worker-cpu-affinity <off\|auto>` | `off` | `auto` binds each io_uring worker to a CPU of its own when they are as many as the process's CPUs (see [Worker CPU affinity](#worker-cpu-affinity)) |
| `--search-index-threads <auto\|N>` | `auto` | Helper threads that link vectors written to HNSW indexes; startup only (see [Search index threads](#search-index-threads)) |
| `--search-resp3-reply <auto\|redis\|valkey>` | `auto` | The shape of the search commands' replies over RESP3 (see [Search reply shape over RESP3](#search-reply-shape-over-resp3)) |
| `--databases` | `16` | Number of databases |
| `--dir <path>` | start directory | Where `dump.kdb`, `appendonly.aof` and `nodes.conf` live |
| `--aof` | off | Append-only file persistence |
| `--maxmemory <bytes>` | `0` (unlimited) | Memory limit |
| `--maxmemory-policy` | `noeviction` | What happens at the limit: `noeviction`, or the `allkeys-*` / `volatile-*` eviction policies (`kividb --help` lists them) |
| `--key-access-tracking <yes\|no>` | `no` | `yes` keeps when each key was last used, and how often, under every `maxmemory-policy`, so that `OBJECT IDLETIME`, `OBJECT FREQ` and `RESTORE ... IDLETIME` work under `noeviction` as in Redis. It costs about 1.0 to 1.5% more instructions on a pipelined `GET` or `SET`, 0.5 to 1.5% unpipelined, and no memory. `no` records only under an LRU, LFU or LRM policy. Changeable with `CONFIG SET` |
| `--loglevel` | `info` | `debug`, `verbose`, `notice` (or `info`), `warning`, `nothing` |
| `--logfile <path>` | standard error | Write the log to a file |
| `--log-format <text\|json>` | `text` | Log line format |
| `--io-model` | `auto` | `auto`, `uring` or `tokio` (io_uring is Linux only) |
| `--requirepass <password>` | none | Password for the `default` user |
| `--aclfile <path>` | none | ACL file to load users from |
| `--replicaof <HOST> <PORT>` | none | Replicate from a primary |
| `--replica-announced-port <port>` | server port (the TLS port with `--tls-replication yes` or `--port 0`) | Port announced to the primary via `REPLCONF listening-port` |
| `--replica-priority <n>` | `100` | Sentinel promotes the replica of lowest priority first, and never one of `0`. New in 1.2.1: 1.2.0 has no such flag, and ignores the setting in the file |
| `--replica-announced <yes\|no>` | `yes` | Whether Sentinel lists this replica. New in 1.2.1, as above |
| `--slowlog-log-slower-than <us>` | off | Slowlog threshold in microseconds (`0` logs every command) |
| `--slowlog-max-len <n>` | `128` | Slowlog entries kept |
| `--cluster-enabled <yes\|no>` | `no` | Cluster mode: a cluster bus on `<port> + 10000`, topology in `nodes.conf`, gossip, failure detection and failover |
| `--cluster-node-timeout <ms>` | `15000` | Time without a reply before a peer is suspected to have failed |
| `--cluster-announce-client-ipv4 <addr>`, `--cluster-announce-client-ipv6 <addr>` | none | The address clients connected over IPv4 / IPv6 are told to reach this node at, in redirects and `CLUSTER SLOTS`, `SHARDS`, `NODES` and `REPLICAS`, where it is not the one nodes use between themselves (Valkey 8). Unset, a client is told the node's address. Changeable with `CONFIG SET` (empty unsets); the other nodes learn it from this node's next heartbeat |
| `--tls-port <port>` | `0` (off) | TLS listener (TLS builds only); with `--port 0`, the only one |
| `--tls-cert-file`, `--tls-key-file` | none | Server certificate and key (PEM) |
| `--tls-ca-cert-file` | none | CA that client certificates are checked against |
| `--tls-auth-clients <yes\|optional\|no>` | `yes` | Whether clients must present a certificate (only with `--tls-ca-cert-file`) |
| `--tls-auth-clients-allowed-names <names>` | none | Names a client certificate's CN or DNS name must match; `*.` is one label |
| `--update-check <yes\|no>` | `yes` | The [startup update check](#startup-update-check) |

`DEBUG` is refused by default from 1.3.0, as in Redis 7 and later: `enable-debug-command`
is `no`, and every client gets Redis's `ERR DEBUG command not allowed ...`. Start the server
with `--enable-debug-command yes` (or `enable-debug-command yes` in the config file) to allow
it to any client whose ACL user may run `DEBUG`, or `local` to allow it only to clients on
loopback (`127.0.0.0/8`, `::1`). Behind a proxy every client has the proxy's address, so
`local` means local to the proxy. The ACL is checked first. Inside `MULTI` the refusal
discards the transaction (`EXEC` answers `EXECABORT`); a script cannot run `DEBUG` whatever
the setting. The setting is read at startup only: `CONFIG GET enable-debug-command` reports
it and `CONFIG SET` refuses it, whatever the value. Up to 1.2.2 it was accepted and ignored,
and `DEBUG` was open to any user whose ACL allowed it. `redis-cli --cluster fix` compares a
key both nodes hold with `DEBUG DIGEST-VALUE`, so it needs `DEBUG` allowed, as with Redis 7.

`maxclients` (default `10000`, in the file or with `CONFIG SET`) counts plaintext and TLS
clients, TLS ones from the start of their handshake. At startup KiviDB raises the soft
open-file limit to `maxclients` plus what it keeps for itself (32 files, or on io_uring 40 and
8 a worker thread) if the hard limit allows; if not, it lowers `maxclients` to fit and logs a
warning. Cluster bus links, a replica's link to its master and files written while saving
are not counted in that; leave room for them in a large cluster.

Small collections are kept in one block of memory up to Redis's limits, which are set as in
Redis, in the file or with `CONFIG SET`, and reported by `CONFIG GET` and `OBJECT ENCODING`:
`set-max-intset-entries` (512), `set-max-listpack-entries` (128) and `set-max-listpack-value`
(64) for sets; `hash-max-listpack-entries` (512) and `hash-max-listpack-value` (64) for hashes;
`zset-max-listpack-entries` (128) and `zset-max-listpack-value` (64) for sorted sets (a sorted
set of up to that many members, none longer than that many bytes, is one block; past either it
is a table of members and an ordered index of them). The `*-max-ziplist-*` names of the hash and sorted-set
settings are the same settings. A limit goes no higher than what one block holds (128 members
of 64 bytes for sets and sorted sets, 512 fields for hashes): a larger value is taken as that
maximum, `CONFIG GET` reports what was kept, and a warning naming the maximum is logged. `0`
entries keeps new hashes and sorted sets out of blocks. A changed limit applies to a hash at
its next write that sets a field, to a set or a sorted set at its next new member (a changed
score never moves a sorted set); a collection never moves back to a block.
[COMPATIBILITY.md](COMPATIBILITY.md) has the details for each type.

A set past those limits (`OBJECT ENCODING` `hashtable`) is a table of its own: a member of up
to seven bytes (any integer of up to seven digits) lives in the table's slot, and any other is
one block of a pool of slabs kept for collections. A hash past its limits is the same kind of
table, each field and its value side by side in one block of that pool (a value too long for
the pool's largest block in a block of the system allocator). A sorted set past its limits
(`skiplist`) is the same kind of table, each member and its score in one block of that pool,
with an index of the members' order beside it. The table grows and shrinks a few members
at a time with each write, and a resize that the writes leave part-way is finished in the
background, so no write pays for moving every member at once. Active defrag does not move a
set's or a sorted set's members or a hash's fields: their slabs are not among those it empties, and each slab goes back to the
system as soon as its last member or field is removed. A list's chunks are blocks of slabs of the same kind, in size classes of
their own. `INFO memory` reports those slabs (the sets', sorted sets',
hashes' and lists') as `value_slab_pool_bytes`, apart from `value_slab_bytes` (the keyspace's slabs, which defrag
compacts).

### Active defrag

Keys' entries, and string values kept apart from them, whose block (header included) is at
most 2 KiB live in 64 KiB slabs of one block size each, and those of up to 26,208 bytes in
1 MiB slabs, whose block sizes are at most 2.5 % apart and each a slab's size over a count of
blocks, 4,096 among them (a value of 4,088 bytes takes a block of 4,096, one of 4,100 a block of 4,192); a larger
value is the system allocator's. `used_memory`, `maxmemory` and `MEMORY USAGE` count a value at its block's
size: up to 2.5 % over the value from 2 KiB to 26 KB. A slab nobody is filling is used for new blocks again once a quarter of it is free
(a 1 MiB slab, once a sixteenth is);
until then its free blocks are memory the process holds.

**Room an `APPEND` keeps.** A string of the system allocator's (a block over 26,208 bytes)
that `APPEND` grows gets room for the next appends: a quarter of its length more, at most
16 MiB, and none past 512 MiB. The appends after it are written into that room, in place,
and one that finds no room left grows the block with `realloc` by a quarter again (the
allocator extends a large block where it is, or remaps its pages, rather than copying it), so
a string built by many small appends costs about the same a call whatever its length. That
room is allocated memory: `used_memory`, `maxmemory` and `MEMORY USAGE` count it, at most a
fifth of the block (25 % of the string's length, 16 MiB at most); pages of it not yet written
are usually address space only, not RSS. Nothing else sees it: replies, `STRLEN`, `DUMP`, `COPY`,
`DEBUG DIGEST-VALUE`, snapshots, the AOF and replication all use the string's length, and a
string loaded from a snapshot, an AOF or a replica's full sync has none. A string that stops
being appended to keeps its room until it is written again: a `SET` of another length gives
it back, and active defrag does not move such blocks (they are not in the slabs). A string up to
26,208 bytes gets no room beyond its block's size: appends move it from one block size to the
next, at most 2.5 % apart, copying at most 26 KB. A string held elsewhere (shared with another
key since a `COPY`, being read by a `GETEX` or `DUMP`, or captured by a `BGSAVE`, an AOF rewrite
or a replica's full sync) is copied once by the next `APPEND`, into a block with that room, and
the holder keeps the bytes from before.

Active defrag (`activedefrag`, on by
default) moves values out of the sparsest slabs so that they empty and their pages go back to
the system: the slabs a quarter free and more, and the full ones with at least
`active-defrag-full-slab-free-percent` of their blocks free. What it moves goes into the room
of the other slabs of its size first, and only then into new slabs. It runs on a thread of its
own, at most `active-defrag-cycle-max` per cent of one core, resting after every 100 us of
work, and lets go of a shard between batches, so a command waits for at most one batch of
moves.

A pass starts when either of two triggers is met, and stops when the waste is under half of
both:

| setting | default | |
|---|---|---|
| `active-defrag-threshold-lower` | `20` | the slabs hold this many per cent more than the values in them ... |
| `active-defrag-ignore-bytes` | `64mb` | ... and that is more than this many bytes (both as in Redis) |
| `active-defrag-slab-waste-percent` | `10` | or the free room inside the slabs is more than this many per cent of them ... (`0` turns this trigger off) |
| `active-defrag-slab-waste-bytes` | `16mb` | ... and more than this many bytes |
| `active-defrag-full-slab-free-percent` | `12` | a full slab is emptied when at least this many per cent of its blocks are free (`0` or `100`: full slabs are left alone) |
| `active-defrag-cycle-max` | `10` | the most of one core it uses, a percentage |

The slab-waste trigger counts free blocks only: the end of a slab that no block fits in (1,120
bytes of a slab of 1,952-byte blocks) is no room any pass gets back.

No pass starts until the bytes of the blocks in the slabs have changed by less than a 32nd of
the slabs each second for 5 seconds in a row (a large delete, or its refill: the writes take
the free room themselves), and a round that sees such a change while it runs (it looks every
10 ms, or every 20 times as long as its last look took, if longer) ends there, with its pass; the next pass then waits for 5 calm seconds again. A round
moves values out of a set of slabs in one look through the keys; what it puts in new slabs is
held twice until the round ends, so a round takes at most a 128th of the slabs' size in new
slabs (at least 1 MiB), or what is left below the most the slabs have taken before, up to an
eighth (that mark comes down when the slabs fall under half of it, after a `FLUSHALL` for
example): in the recorded runs the peak during a churn is 1.2-2.1 % above the same churn with
active defrag off. While a snapshot (`BGSAVE`), an AOF rewrite or a backup is being written,
the slab-waste trigger waits (the values a snapshot holds cannot move, and the old values of
what is written meanwhile are freed when it ends); the ratio trigger does not, so a server
that saves all the time is still compacted. A pass that the slab-waste trigger started and
that got little back (the slabs it emptied, less the new ones it took, under one in 32 of
those it emptied), or that a delete cut short, keeps that trigger off for 10 seconds, then
twice as long each time in a row, up to a minute. All of these change at run time with
`CONFIG SET`.

`INFO memory` shows the slabs (`value_slab_bytes`), the values in them
(`value_slab_live_bytes`), the free blocks of the full slabs (`value_slab_full_free_bytes`), and
active defrag's work: `active_defrag_running`, `active_defrag_hits` (values moved),
`active_defrag_misses` (values left where they were because something else held them, such as a
snapshot being written), `active_defrag_passes`, `active_defrag_slabs_emptied` and
`active_defrag_last_pass_ms` (how long the last pass took). `CONFIG RESETSTAT` sets the counts
to 0.

Persistence (snapshots and the AOF) is described in [PERSISTENCE.md](PERSISTENCE.md),
including `rdbcompression` (default `no`; `yes` writes snapshots compressed, which 1.2 cannot read) and the
`kividb kdb-decompress <in> <out>` and `kividb kdb-compress <in> <out>` tools, which convert a
snapshot between the compressed form and the plain one KiviDB 1.2 reads (refusing an input the
server would not load, and an existing output unless `--force` is given).

### Worker threads

Commands run on the worker threads, every worker serving its own share of the connections.
`--threads N` starts exactly `N`. `--threads 0`, the default, starts half the CPUs the process
may actually run on, rounded up, and never fewer than 2 where it may use 2 or more: 1 on 1 CPU, 2 on 2, 3 or 4, 3 on 6, 4 on 8, 8 on 16. The
CPUs counted are the CPUs online, cut down to the process's affinity mask (`taskset`, a cpuset,
Docker's `--cpuset-cpus`, the Kubernetes static CPU manager) and to its cgroup CPU quota
(Docker's `--cpus`, a Kubernetes CPU limit; a fraction is rounded down, so `--cpus 1.5` is
1 CPU and one worker), and never fewer than one. A container limited to 2 CPUs on a 64-CPU host
starts 2. The startup banner says how the number was chosen:

```
  * CPUs           : 8 online, 2 available (a cgroup CPU quota of 2 CPUs)
  * Worker threads : 2 (auto: half the available CPUs, at least 2 unless 1 CPU)
```

The server's other threads (the AOF writer, the replication hub, expiry, a background save,
the accept and timer threads) run beside the workers; none is given a CPU of its own, and the
CPUs the workers leave are what they, the application and the load generator use. Measured on
a `Standard_D8s_v5` with the server on 4, 6 and 8 vCPUs, across 14 workloads, one worker per
CPU lost up to 37.9% of the throughput of the best count (8.2% on average) and raised the p99 of a
server that shared its machine by up to 1011%; half the CPUs lost 6.3% at worst and 1.3% on
average, with 13 of the 14 within 3% of the best count. On 2 dedicated CPUs (a Standard_D4s_v5 run with the server on 2 CPUs) one worker served 78-85% of the best count's throughput on `GET` (50 connections) and 84% on `SET` pipelined 16, and had a p99 of 64.5 ms against 3.2 ms with two workers on tokio (500 connections) and of 4.5 ms against 0.66 ms on a near-idle load; two workers were within a few percent of the best count except on `XADD` and pubaverage, with 13 of the 14 within 3% of the best count. [PERFORMANCE.md](PERFORMANCE.md#worker-threads)sub (91-94%), which is why auto starts at least 2. [PERFORMANCE.md](PERFORMANCE.md#worker-threads)
has the numbers.

Set `--threads` yourself to use every CPU on a machine the server has to itself, or to keep
the count of an earlier release. Up to 1.2.2 auto was the CPUs minus 2 (all of them below 4),
which is:

| CPUs | 1.2.2 | now |
|---:|---:|---:|
| 2 | 2 | 2 |
| 4 | 2 | 2 |
| 8 | 6 | 4 |
| 16 | 14 | 8 |

A Kubernetes pod with a CPU request but no CPU limit has no quota, so the server sees every CPU
of the node: give it a limit, or `--threads`.

### Search reply shape over RESP3

`--search-resp3-reply` (`search-resp3-reply` in the configuration file; `CONFIG GET` and
`CONFIG SET` take it, and it applies to connections that are already open) is the shape of
the replies of `FT.SEARCH`, `FT.AGGREGATE` (and its cursor), `FT.HYBRID`, `FT.INFO` and
`FT._LIST` to a connection that has sent `HELLO 3`:

| Value | Reply over RESP3 |
|-------|------------------|
| `auto` (default) | Redis 8's, a map, except to a client that named itself valkey-glide with `CLIENT SETINFO LIB-NAME`: valkey-search's |
| `redis` | Redis 8's for every connection |
| `valkey` | valkey-search's for every connection |

valkey-search answers a RESP3 connection with the arrays it answers RESP2 with, and
valkey-glide converts that reply itself: it reads `[total, key, [field, value, ...], ...]` and
refuses a map. In the valkey shape, then, `FT.SEARCH`, `FT.AGGREGATE`, `FT.CURSOR READ`,
`FT._LIST` and `FT.INFO` are the RESP2 layout on a RESP3 connection:

- `FT.SEARCH`: the count, then each key and its fields as a flat array (a key alone with
  `NOCONTENT`). The count is an integer; values, scores and distances are bulk strings, a vector
  blob included.
- `FT.AGGREGATE` and `FT.CURSOR READ` (`[rows, cursor]`): the count, then each row as a flat array
  whose names are simple strings.
- `FT._LIST`: an array of simple strings.
- `FT.INFO`: a flat array with a simple string for each name, in every list in it, and without the
  `fields` list of strings that the Redis shape keeps for older readers. Its fields are KiviDB's,
  not valkey-search's.

`FT.HYBRID` is not RESP2's layout (RESP2 gets Redis 8's reply, as a flat list of `total_results`,
`results`, `warnings` and `execution_time`). In the valkey shape it is valkey-search's: the count,
then each row as a flat array whose names are simple strings, a row being `__key` and `__score`
or the fields `LOAD`ed. `warnings` and `execution_time` are not in it.

Not in the valkey layout either, because valkey-search has no equivalent: the warning that a
query cut short by `ON_TIMEOUT` carries in RESP3's `warning` (the client gets the partial result
with no signal, as over RESP2). A `WITHSCORES` on a KNN query gives each document's distance as
its score, as Redis does; valkey-search gives `0`.

`FT.PROFILE` has no valkey-search counterpart, and keeps the Redis shape, as do `FT.SPELLCHECK`,
`FT.CONFIG GET`, `FT.TAGVALS`, the alias list and every other search command, in every mode.

A client is valkey-glide when its library name begins with `Glide`, in any case (`GlidePy`,
`GlidePySync`, `GlideJS`, `GlideJava`, `GlideGo`, `GlideFFI`, and any of them followed by a tag in
brackets). RESP2 replies are the same in every mode, as are a script's (`redis.call` reads RESP2
unless the script says `redis.setresp(3)`) and the parts that the masters of a cluster give one
another.

The setting belongs to the node: in a cluster, the node that is asked writes the reply, so set
every node alike.

### Search index threads

`--search-index-threads` (`search-index-threads` in the configuration file; read at startup,
`CONFIG GET search-index-threads` shows it, `CONFIG SET` refuses it) is how many helper
threads link the vectors written to HNSW indexes into their graphs, so that one client
loading documents does not build the graph on one CPU. `0` starts none: each write links
its own vector, as up to 1.2. `auto`, the default, starts none under a CPU quota (Docker's
`--cpus`, a Kubernetes CPU limit) or with fewer than 8 CPUs. Otherwise it starts one fewer
than the CPUs (seven on 8, fifteen on 16) and changes how many of them link as the load
changes:
- while the server serves clients, (CPUs - 2) / 2 of them link (three on 8, seven on 16)
  and the rest wait;
- while it does nothing but take the writes that fill the backlogs (a bulk load,
  `FT.CREATE` over existing keys, a replica's full sync, which follows the same rule),
  all of them link.

Nothing is added to the workers' loop to tell the two apart. Every 50 ms the helper that
has just finished a job reads counters the server keeps anyway: the commands served, the
process's CPU time, the helpers' own CPU time and that of links made in writes. The vector
code also counts the commands that wrote vectors (once each, however many indexes and
fields they write) and times one in 64 of them on its thread's CPU clock. The server
counts as serving when, besides the commands that wrote vectors, it served 500 commands a
second, or its threads used a CPU's worth of time besides the links and what those
commands cost (the timed ones' average times their number), so that loading connections
are not taken for clients. The count goes down at
the first look that finds clients. It goes up again only after four quiet looks in a row
(200 ms). A look more than 200 ms after the last one (the helpers had nothing to do
meanwhile) starts from the serving count. Each extra helper's thread costs its stack and
its search buffers: with seven, the peak RSS of a 200,000 × 64-d build was at most 5 MiB
more than with three. On an Azure D8s_v5, with all seven a one-client load of 50,000
384-d documents takes 0.89x Redis 8.10's time and one of 200,000 64-d documents
0.97-0.98x; clients served meanwhile see the three, as before
([PERFORMANCE.md](PERFORMANCE.md#vector-index-builds) has the builds and the latencies).

A number starts exactly that many on any machine, and all of them always link. The log
says how many started, on the first write that needs them; `FT.INFO` shows
`hnsw_index_threads` (started) and `hnsw_index_threads_active` (linking now).

Vector sets' graphs (`VADD`) use the same helpers. When `auto` starts none, a vector set's
writes are linked by threads of their own instead (`vset-link-N`, half the CPUs, one to
four, started on the first such write): the writes of one key run one after another, and
each linking its own vector would link a set one vector at a time (on a 4-vCPU D4s_v5,
2,664 `VADD`s a second against 6,608 with them). `0` starts neither: each `VADD` links its
own vector.

So on a server of fewer than 8 CPUs builds run at 1.2's speed by default, and
`search-index-threads 1` (or more) trades serving latency for build speed. The helpers take
CPU time the workers would otherwise have. With every CPU of a 4-vCPU server busy (GET/SET
from memtier, one client loading 384-d vectors in pipelines, one `HSET` every 2 ms from
another client):

| helpers | GET/SET p99 | KNN p50 | `HSET` p99 | build alone |
|---|---:|---:|---:|---:|
| master (links in the writes) | 1.61 ms | 0.87 ms | 2.25 ms | 1,550 docs/s |
| 0 (`auto` below 8 CPUs) | 1.64 ms | 0.89 ms | 2.32 ms | 1,540 docs/s |
| 1 | 2.10 ms | 1.08 ms | 4.4-34 ms | 3,190 docs/s |
| 2 | 2.62 ms | 1.13 ms | 5.5 ms | 3,620 docs/s |

The `HSET` column is one writer connection, so it shows where that connection landed:
4.4-34 ms with one helper is a connection that shared the bulk loader's worker in two runs
of three. Over 8 writer connections (one `HSET` every 2 ms in turn), the one or few on the bulk
loader's worker wait behind its pipelines in every build, helpers or not (their median
15-25 ms with none, 12-16 ms with one helper); on the others one helper raised the `HSET`
p99 from 2.3-2.7 ms to 4.5-4.7 ms. Under a CPU quota a helper spends the quota and the
whole server is throttled (GET/SET p99 8-9 ms against 0.35 ms beside a load with
`--cpus 2` and one helper), which is why `auto` starts none there: in a container or a
Kubernetes pod with a CPU limit, builds run at 1.2's speed unless the setting is given a
number. [SEARCH.md](SEARCH.md#index-builds-and-ef_runtime) and
[PERFORMANCE.md](PERFORMANCE.md#vector-index-builds) have more.

### Worker CPU affinity

`--worker-cpu-affinity auto` (or `worker-cpu-affinity auto` in the configuration file;
`CONFIG GET` reports it, it is read only at startup) binds each io_uring worker to a CPU
of its own: worker N to the N-th CPU the process may use. It does so only when the workers
are exactly as many as those CPUs: `--threads` set to the CPUs of the cpuset, or to the N CPUs of a
`taskset`. The default count is half of them (at least 2), so it binds on its own only on 1 or 2 CPUs (as many workers as CPUs); on more it takes `--threads` set to the CPUs. It never binds when what limits the process is a cgroup CPU quota
(Docker's `--cpus`, a Kubernetes CPU limit): a quota is time on any CPU, not CPUs of the
server's own. The default is `off`: the workers run wherever the scheduler puts them.

An io_uring worker serves its connections alone. When the scheduler puts two workers on one
CPU, which it does at times when a third busy thread shares their CPUs (the replication hub
while a replication backlog is kept, a background save), every connection of one waits out
the other's time slice, and the p99 goes to milliseconds
([#894](https://github.com/kividbio/kividb/issues/894)). Bound, each worker keeps its CPU.

Turn it on when the server has its CPUs to itself: a Kubernetes pod under the static CPU
manager policy (Guaranteed QoS, whole CPUs), a VM or host where the server is confined with
`taskset` or a cpuset and nothing else runs on those CPUs. Leave it off when the server
shares its CPUs with other busy processes: a bound worker cannot move to a CPU that is free
while its own is busy. [PERFORMANCE.md](PERFORMANCE.md#worker-cpu-affinity) has both
cases measured.

The server says what it did, after the I/O model line, once every worker has tried its
bind:

```
I/O model      : io_uring
Worker CPUs    : pinned 2 of 2: worker 0 on CPU 0, worker 1 on CPU 2 (worker-cpu-affinity auto)
```

or that only some binds took (`pinned 1 of 2 (worker-cpu-affinity auto); refused: ...`, naming each refused bind and
the error), or
why it did not bind (`not pinned: 2 workers on 4 CPUs: pinned only when they are as many`).
The server's other threads (the replication hub, the AOF writer, expiry, the script runner,
a full sync's or a backup's work) are not bound: they run on any of the process's CPUs,
which with `auto` in effect are the workers' CPUs, so they take turns with the workers as
before; a thread a worker starts is given all of the process's CPUs back first. The tokio
I/O model is not bound (its workers take each other's connections, so none waits behind one
that is held up). Linux only: macOS has no call that binds a thread to a CPU, and on
FreeBSD the setting is not implemented; there it is accepted and the server says it did
nothing. The binds are made once, at startup: if the cpuset changes later, the kernel
decides (verified on cgroup v2, Linux 7.0: a worker whose CPU is removed runs on the whole
new set and goes back to its CPU when it returns; cgroup v1, not verified here, resets each
thread to the new set), the startup line is not updated, and a thread started after the
change is given the CPUs the process had at startup; a Kubernetes pod's exclusive CPUs
under the static CPU manager do not change while its container runs. Redis's `server-cpulist` binds the server's threads to a list of CPUs together,
not one each; KiviDB accepts it, as it accepts Redis's other settings, and does nothing
with it.

## TLS

TLS is an optional Cargo feature (`--features tls`): it is in the `tls` and `full` builds
(see [BUILDING.md](BUILDING.md#builds)).

TLS clients connect on a separate port, `--tls-port`; plain traffic stays on `--port`. With
`--port 0` there is no plaintext port, and the server is reached over TLS only (it needs a
`--tls-port`; a cluster node needs `--tls-cluster yes` as well, and its bus is then
`<tls-port> + 10000`). A replica of such a master connects with `--tls-replication yes`.

```bash
cargo build --release --features tls
./target/release/kividb --port 6380 --tls-port 6381 --tls-cert-file cert.pem --tls-key-file key.pem
redis-cli --tls -p 6381 --cacert ca.pem   # ca.pem: the CA that signed cert.pem
```

**Client certificates (mutual TLS).** With `--tls-ca-cert-file ca.pem`, clients must present a
certificate signed by that CA; others are refused during the handshake.
`--tls-auth-clients optional` accepts clients without a certificate but still refuses an
invalid one, and `--tls-auth-clients no` does not ask for one. With `redis-cli`:
`redis-cli --tls -p 6381 --cacert ca.pem --cert client.pem --key client.key`.

**Pinning client certificates by name.** `--tls-auth-clients-allowed-names "*.db1.example
admin.example"` (spaces or commas between names) accepts a client certificate only if its subject
CN or one of its DNS names matches one of the names; others signed by the same CA fail the
handshake, and the server logs the names they carried. `*` stands for exactly one label:
`*.db1.example` takes `node-0.db1.example`, not `a.b.db1.example` or `db1.example`. Case does
not matter. It applies to clients, replicas and, with `tls-cluster`, nodes on the cluster bus;
empty (the default) is off. KiviDB's own setting: Redis has none.

**Changed without a restart.** `CONFIG SET` of `tls-cert-file`, `tls-key-file`,
`tls-ca-cert-file`, `tls-auth-clients`, `tls-client-cert-file` and `tls-client-key-file` loads the
files again for every handshake from then on. `CONFIG SET tls-port <port>` opens the TLS port
(also on a server that started without TLS: `CONFIG SET tls-cert-file cert.pem tls-key-file
key.pem tls-port 6381`), moves it, or closes it with `0`; connections already open stay open.
`CONFIG SET tls-replication yes` makes a replica's next connection to its master TLS. What cannot
be loaded or listened on is refused, and nothing changes. `tls-cluster` needs a restart, and so
does `tls-port` on a node with `tls-cluster yes`.

**Clients must use TLS on the TLS port.** A plain connection to it (for example `redis-cli -p 6381`
without `--tls`) fails the handshake; at `--loglevel verbose` the server logs
`TLS: handshake failed from <address>`. For
memtier_benchmark, add `--tls` (and `--tls-skip-verify` for a self-signed certificate).

## Health checks

`kividb --ping` checks that a server is up, with nothing but the binary: the Docker images
have no shell and no `redis-cli`, so it is the health check to use there, in exec form.

```yaml
healthcheck:
  test: ["CMD", "kividb", "--ping"]
```

It connects to `127.0.0.1` on the server's port, sends `PING`, prints `PONG` and exits **0**
if the server answers `PONG`. Anything else (no connection, a refused password, an error
reply, no reply within about a second, or an argument it cannot read) exits **1**, with the
reason on standard error. It starts no server, writes no log and does not touch the data
directory.

**It is a separate process.** It does not see the arguments the server was started with (the
container's `command:` or `args:`), only its own and the config file. A `--port`,
`--requirepass`, `--dir` or `--tls-*` given to the server must be given to the check too, or
moved into the config file, which both read:

- **Port**: `--port`, or `port` in the config file. With `--port 0` it connects to
  `--tls-port` over TLS. `--ping-port` picks another port.
- **Address**: `127.0.0.1`. If `bind` does not cover loopback (for example `--bind
  10.0.0.5`), give the check the address with `--ping-host`.
- **Password**: `KIVIDB_PASSWORD` in the environment, else `requirepass` from the config file
  or the command line, sent as `AUTH <password>` for the default user. Prefer the environment
  variable or the config file: an argument shows in `ps` and `docker inspect`. A password the
  server does not need is ignored. The password goes to whatever `--ping-host` and
  `--ping-port` point at.
- **ACL users**: with `KIVIDB_USER` set as well, it sends `AUTH <user> <password>` with
  `KIVIDB_PASSWORD` (`requirepass` is the default user's, so it is not used). Where an
  `aclfile` or `ACL SETUSER` turns the default user off, give the check a user of its own that
  may run `PING` and nothing else, for example `user health on >long-password -@all +ping`,
  and set `KIVIDB_USER=health` and `KIVIDB_PASSWORD` in its environment.
- **TLS**: on the TLS port it uses TLS (the `tls` or `full` build). With
  `--tls-ca-cert-file`, it checks the server's certificate chain against that CA (not the
  name, as between nodes) and presents `tls-client-cert-file`, or `tls-cert-file`, which
  `tls-auth-clients yes` asks for. Without a CA file it takes the server's certificate as it
  is, but only from a server on this host (a loopback address): a server elsewhere needs the
  CA file, since the password would go to whoever answers. Relative TLS file names are read
  from `dir`, as the server reads them.

**Kubernetes.** A server opens its ports only once it has loaded its snapshot and append-only
file, so the check fails while a large dataset loads. Use it as a `readinessProbe`, or as a
`livenessProbe` together with a `startupProbe` that allows for the load, so that a slow start
is not taken for a dead server:

```yaml
startupProbe:
  exec:
    command: ["kividb", "--ping"]
  periodSeconds: 5
  failureThreshold: 120      # up to 10 minutes to load
livenessProbe:
  exec:
    command: ["kividb", "--ping"]
  periodSeconds: 10
readinessProbe:
  exec:
    command: ["kividb", "--ping"]
  periodSeconds: 5
```

## Logs and errors

The log goes to standard error, or to `--logfile`. [LOGGING.md](LOGGING.md)
describes the log; [ERRORS.md](ERRORS.md) explains the warnings, errors and error
replies, and what to do about each.

## Startup update check

When it starts, a release build of KiviDB makes **one HTTPS request** to
`https://github.com/kividbio/kividb/releases/latest`, the same page you would open to find
the newest release. GitHub answers with a redirect to that release's tag; if it is newer
than the running build, KiviDB prints a short notice with the link before the startup
banner. Nothing is sent but the request itself, with a `User-Agent` of `kividb/<version>`:
no host name, configuration, keys, data or identifier. GitHub sees the request's source IP
address, as any web server would.

Startup waits at most 1.5 seconds for the reply; a failure is logged at debug level only and
never affects clients. Only a version-shaped tag is accepted from the reply, and nothing
else from it is printed.

Any one of these turns it off:

- `--update-check no` on the command line;
- `update-check no` in the configuration file;
- `KIVIDB_NO_UPDATE_CHECK=1` in the environment.

It is also never made by a debug build or when the `CI` environment variable is set.
`--update-check-url` points it at another `https://` address, such as a mirror inside a
closed network. [UPDATE_CHECK.md](UPDATE_CHECK.md) has the details.
