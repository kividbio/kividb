# Redis 8.4 – 8.10: what KiviDB implements

An account of the Redis open-source release notes from 8.4 to 8.10 against
KiviDB: what is implemented, what behaves differently from before as a
result, what was found broken and fixed on the way, and what is not
implemented. Redis 8.8's commands are covered in more detail in
`docs/REDIS_8_8_COMMANDS.md`; syntax for everything is in `COMMANDS.md`.

Items in the release notes that are internal to Redis — I/O threading,
allocator and encoding optimisations, module API additions, Sentinel — have no
counterpart here and are left out.

## Implemented

### Redis 8.4

| Feature | Notes |
|---|---|
| `DIGEST`, `DELEX`, `SET … IFEQ / IFNE / IFDEQ / IFDNE` | The digest is XXH3-64 as 16 hex digits, the value Redis returns. Compare-and-set and compare-and-delete are atomic. |
| `MSETEX` | |
| `XREADGROUP … CLAIM min-idle-time` | Idle pending entries and new entries in one read. |
| `CLUSTER MIGRATION` — atomic slot migration | `IMPORT`, `STATUS`, `CANCEL`. See "Atomic slot migration" below. |
| `FT.HYBRID` | Present before this work; not re-audited against 8.4's options. |

### Redis 8.6

| Feature | Notes |
|---|---|
| `XADD … IDMP / IDMPAUTO`, `XCFGSET IDMP-DURATION / IDMP-MAXSIZE`, `XINFO STREAM` idempotency fields | At-most-once production. The producer state is saved in snapshots and reaches replicas, so a retry after a failover is still recognised. |
| `HOTKEYS START / STOP / GET / RESET / HELP` | CPU time and network bytes per key, with sampling and slot selection. |
| Time series: `NaN` values, `COUNTNAN`, `COUNTALL` | |

### Redis 8.8

| Feature | Notes |
|---|---|
| Array type (`AR*`), `INCREX`, `XNACK` | Present before this work; see `docs/REDIS_8_8_COMMANDS.md`. |
| `ZUNION` / `ZINTER` / `ZUNIONSTORE` / `ZINTERSTORE … AGGREGATE COUNT` | |
| `JSON.SET … FPHA` | The value is validated and converted to the requested float type; KiviDB has no separate homogeneous-array encoding to select. |
| `TS.RANGE` / `TS.REVRANGE` / `TS.MRANGE` / `TS.MREVRANGE` with several aggregators | `AGGREGATION min,max,avg 1000` |
| `slowlog-entry-max-argc`, `slowlog-entry-max-string-len` | |
| `INFO commandstats` slowlog fields | `slowlog_count`, `slowlog_time_ms_sum`, `slowlog_time_ms_max`. |
| Hash subkey notifications | Flags `S`, `T`, `I`, `V`. |

### Redis 8.10

| Feature | Notes |
|---|---|
| `LMOVEM`, `BLMOVEM` | |
| `SUNIONCARD`, `SDIFFCARD` | |
| `HIMPORT PREPARE / SET / DISCARD / DISCARDALL` | The command is implemented; the hashes it creates are ordinary hashes. |
| `BACKUP START / LIST / SEAL / STATUS / ABORT / CLEANUP`, `backupdirname`, `backup-sealed-ttl`, `preload-file` | See `docs/PERSISTENCE.md`. |
| `XREAD` / `XREADGROUP … MAXCOUNT / MAXSIZE` | |
| `FT.ALIASLIST` | |
| `TS.NRANGE`, `TS.NREVRANGE`, `TS.READ`, `TS.QUERYLABELS`, `TS.MRANGE … EXCLUDEEMPTY` | |
| JSONPath extensions | The whole list: projections, literal comparison, `!`, `size` / `empty`, `in` / `nin`, arithmetic, `~`, and every function. See "JSON" below. |
| `SLOWLOG GET` total argument count | A seventh element in each entry. |
| `cluster-bus-port-protected-mode` | `yes` requires `cluster-bus-secret` (KiviDB's own: a shared secret, checked by challenge and HMAC). Since 1.1.0, `tls-cluster` encrypts the bus too. |

Fixes in the 8.10 notes that applied here and were made: `SET` rejecting
`NX`/`XX` combined with `IF*`; clients blocked on a list key being woken when
`SORT … STORE` creates it; ACL key checks covering the keys `SORT … STORE`,
`GEORADIUS … STORE` and `XREAD` / `XREADGROUP` actually use; `TS.INCRBY …
TIMESTAMP *` replicating the timestamp the master used.

## Atomic slot migration

`CLUSTER MIGRATION IMPORT start end […]` on the destination. The source takes
a consistent cut of the slots' keys together with a tap on its write stream,
sends the keys as `RESTORE`s and then the writes that followed, pauses writes
when the destination is within `cluster-slot-migration-handoff-max-lag-bytes`,
sends the remainder, and hands the slots over; the destination takes them with
one epoch bump and the source deletes its copy. The destination applies what
it receives as its own writes, so its replicas and its AOF have the data by
ordinary means.

A client sees one `-MOVED` at the end and nothing else. Tested under write
load: every acknowledged increment is present exactly once on the destination,
on its replica, and nowhere else.

Differences from Redis worth knowing:

* The handoff pauses **all** client writes on the source, not only those for
  the slots being moved.
* The keys of slots being imported are visible to `KEYS` / `SCAN` / `DBSIZE`
  on the destination before it owns them (commands for them are still
  redirected).
* A script that is running across the handoff is not waited for; keep scripts
  short on a node whose slots are moving.

## JSON

The path engine was replaced. Before, a path could be made of names, indexes,
`*` and `..`; anything else was silently mis-read. Now paths are parsed, and a
path that cannot be parsed is an error.

Supported: bracket notation with quoted names, negative indexes, unions,
slices, filters with the full operator set, the `~` operator, all 8.10
functions in prefix and method form, and projection expressions at the top
level. Every example on the Redis JSON path page is in the test suite and
produces the output shown there.

The commands were rebuilt on it, which changes replies — see the next section.

## Behaviour changes

Things an existing KiviDB client may notice. Each is a move to what Redis
does.

**JSON**

* A JSONPath (`$…`) is answered for every match by every command.
  `JSON.ARRAPPEND`, `ARRINSERT`, `ARRTRIM`, `ARRINDEX`, `ARRPOP`, `STRAPPEND`,
  `DEBUG MEMORY` and `RESP` used to answer for the first match with a bare
  value; they now return an array with one entry per match.
* `JSON.GET key $.missing` returns `[]`, not nil. With a legacy path that
  matches nothing it is an error.
* `JSON.MGET` with a JSONPath returns each key's matches as a JSON array.
* `JSON.NUMINCRBY` / `NUMMULTBY` with a JSONPath return one bulk string
  holding a JSON array (`"[15,25]"`), not an array of strings.
* `JSON.TOGGLE` with a legacy path returns `"true"` / `"false"`.
* A command given no path uses the legacy root, so `JSON.ARRLEN key`,
  `JSON.OBJLEN key`, `JSON.TYPE key` return a plain value.
* Objects keep their members in insertion order (they used to be sorted by
  name). This shows in `JSON.GET`, `JSON.OBJKEYS` and `$~`.
* `JSON.SET` on a key holding another type is a `WRONGTYPE` error; it used to
  replace the key.
* `JSON.GET` understands `INDENT`, `NEWLINE` and `SPACE`; they used to be
  taken for paths.

**Keys and blocking**

* A list, set, sorted set, hash or vector set that loses its last element is
  deleted. It used to remain as an empty key — answering `EXISTS` and `TYPE`,
  listed by `KEYS`, counted by `DBSIZE`, saved and replicated.
* `LPUSH` / `RPUSH` / `ZADD` return the length (or count) with every element
  in, also when a client was blocked on the key.
* `SUNION`, `SINTER`, `SDIFF`, `SINTERCARD` and their `STORE` forms reply
  `WRONGTYPE` for a key that is not a set; they used to treat it as empty.
* A blocked client receives the element its command asks for, and clients are
  served in the order they blocked. See "Fixed along the way".

**Strings, hashes, streams, sorted sets, time series, search**

* `DIGEST` returns XXH3-64 (it returned SHA-256), and `DELEX` on a missing key
  returns 0.
* `HSETEX` takes Redis's syntax, with the TTL as an option. The earlier form
  with the TTL as the second argument is still accepted.
* `XREADGROUP` on a group that does not exist is a `NOGROUP` error; a history
  read (an explicit ID) always names the stream in its reply.
* `XTRIM` with a threshold that is not a number is an error.
* `ZUNIONSTORE` / `ZINTERSTORE` with `WEIGHTS` apply each weight to the set it
  was given for (see "Fixed along the way"), and a store whose result is empty
  deletes the destination.
* `TS.ADD`, `TS.MADD`, `TS.INCRBY`, `TS.DECRBY`, `TS.GET`, `TS.MGET` reply
  with timestamps as integers; `TS.INCRBY` refuses a timestamp older than the
  last sample.
* The range filters are `FILTER_BY_TS` and `FILTER_BY_VALUE`.
* An index alias is not listed by `FT._LIST`, and `FT.ALIASDEL` deletes only
  aliases.

**Server, ACL, replication, cluster**

* `SLOWLOG GET` entries have seven elements.
* `CONFIG SET slowlog-log-slower-than -1` turns the slow log off (it was an
  error), and `CONFIG GET` reports `-1`.
* `XDELEX` and `XACKDEL` follow Redis: the syntax is
  `[KEEPREF | DELREF | ACKED] IDS numids id …`, an entry that `ACKED` keeps is
  reported as `2` (it was `0`), and `XACKDEL` reports `1` for a pending entry
  that had already been deleted (it was `0`). `ACKED` waits for every group to
  have read *and* acknowledged the entry — so on a stream with no groups it
  deletes nothing, where the old `REF` mode deleted. `REF` and the bare
  `XACKDEL key group id …` are still accepted.
* ACL categories `admin`, `dangerous`, `blocking`, `fast`, `slow`, `json`,
  `search`, `timeseries`, `vector` and `transaction` exist. **`-@dangerous`
  and `-@admin` now deny commands**; they were accepted and denied nothing.
  Review users whose rules name them.
* ACL changes apply to connections that are already open.
* A replica reports `master_link_status:down` unless it is synced and
  receiving, refuses a full resync from an empty master when it holds data,
  and loads its local dataset at startup.
* `REPLICAOF` is refused in cluster mode; a build without TLS refuses
  `--tls-port`.
* In cluster mode a `MULTI` is routed as its commands are queued and must
  address one slot.
* **io_uring has no idle timer to shrink an idle query buffer.** `CLIENT LIST`
  reports each client's real `idle=` on both servers (seconds since that
  connection last ran a command, on a monotonic clock), and `qbuf`/`qbuf-free`
  are reported truthfully. Redis shrinks an idle oversized query buffer from
  its client cron on a two-second idle timer, and so does KiviDB's **Tokio**
  server, whose reads are cancellable: under Tokio the `unit/querybuf` tests
  pass. Under **io_uring** there is no such timer — the path cannot rebuild a
  buffer a read owns and will not cancel the read to force one, so it reclaims
  an oversized private buffer at the client's next read, and an idle client's
  `qbuf` never falls. The first `querybuf` test waits on `[client_idle_sec …]
  >= 3` **and** a smaller `qbuf`; the idle half is met, the shrink half is not,
  and it fails on io_uring alone. The second failure, `… with fat argv`, is a
  knock-on of the first — the first test fails before closing its
  `test_client`, and `client_query_buffer` then reads that stale entry — not a
  missing feature. See `docs/REDIS_TEST_SUITE.md`.

## Fixed along the way

Defects found while doing the above. None is specific to an 8.x feature.

* **Blocked pops could take the wrong element, and replicas diverged.** A push
  handed its first element straight to a blocked client. `BRPOP` against
  `RPUSH k a b` therefore returned `a` (Redis: `b`), `BZPOPMIN` the first
  member added rather than the lowest — and the replica, which received the
  push and then the pop, removed a *different* element, leaving master and
  replica with different data. Blocked clients are now woken, in the order
  they blocked, and pop for themselves.
* **A list created by `RENAME`, `COPY`, `RESTORE` or `SORT … STORE` did not
  wake clients blocked on its key.**
* **Two `WATCH` transactions could both commit against the same key.** `EXEC`
  checked its watched keys and then ran its commands, and a second `EXEC`
  could make its own check in between. The e2e suite's order-processing test —
  ten buyers, a stock of five — sold a sixth item about once in twelve runs
  (0 in 40 after the fix). An `EXEC` with watched keys now holds a lock from
  its check to its last command. A plain write from outside a transaction can
  still land in that window; see `docs/EXEC_ATOMICITY.md`.
* **Writes could be applied to the wrong database downstream.** The write
  stream marks a change of database with a `SELECT`; with one client writing
  to db 0 and another to db 3, a write could be queued behind the other
  client's `SELECT` and a replica, an AOF replay or a restored backup put the
  key in the wrong database. From the first time a client selects a database
  other than 0, each write is sent with its own `SELECT` in one piece. Servers
  that only use db 0, and every cluster node, are unaffected and pay nothing.
* **`CONFIG SET slowlog-log-slower-than` did not reach connections that were
  already open** when the server had started with the slow log off: each
  connection decided once, when it was accepted, whether to time its commands.
* **A transaction touching another node's slot ran in part.** Commands were
  routed only when `EXEC` ran them, so the ones for this node executed and the
  others came back as errors inside the reply; an all-`SET` transaction could
  write keys into slots the node did not own.
* **A cluster of six or more nodes never finished forming on Windows.** Each
  heartbeat mentions three other nodes, chosen by the clock's nanoseconds
  modulo the number of nodes; with a clock that ticks in 100 ns steps that is
  always zero for four or five other nodes, so the same three were mentioned
  for ever and the rest were never learnt. The choice is random now, as are
  election delays, `ZRANDMEMBER`, and node and replication IDs (which were
  made from the clock and the PID — 1 for everyone in a container).
* **`COMMAND` and `COMMAND INFO` returned an empty list.** redis-py's cluster
  client takes each command's key positions from that reply and could not
  route anything. They now describe every command, from the same key table
  that routing uses; redis-py's cluster client passes the e2e cluster module
  (`tests/e2e/modules/test_cluster.py`, 52 tests) against a six-node cluster.
* **Relative TTLs restarted on AOF replay, and expired late on replicas.**
  They were logged as typed. `EXPIRE key -1` and `PEXPIRE key 0`, which
  delete the key, were not propagated at all.
* **`ZUNIONSTORE` / `ZINTERSTORE … WEIGHTS`** applied weights to the wrong
  sets once the inputs had been reordered by size.
* **`WATCH`** was dirtied only by a write to a command's first argument: a
  write to `b` by `MSET a 1 b 2`, or by `RENAME a b`, did not abort a
  transaction watching `b`.
* **`SUNION`, `SINTER`, `SDIFF`, `SINTERCARD` and the `STORE` forms** treated
  a key of another type as an empty set; it is a `WRONGTYPE` error. The
  `STORE` forms left the destination's previous TTL on the new set.
* **`-@dangerous` / `-@admin`** in an ACL rule had no effect (above).
* **`JSON.DEBUG MEMORY key`** was routed, and checked against ACL key
  patterns, by the word `MEMORY`.
* Keys left empty were never deleted (above).
## Search and query

The first report against v1.0.4 came from running LiteLLM's semantic cache,
which is RedisVL underneath. All eight of its items are fixed, each with the
reporter's commands as a test (`tests/search.rs`, `report_1_…` to
`report_8_…`); RedisVL's index, `FT.INFO` round trip, KNN, filter, range-query
and `SemanticCache` paths run against it unchanged.

| Reported | Now |
|---|---|
| `FT.CREATE` refused per-field options | `AS`, `SORTABLE [UNF]`, `NOINDEX`, `WEIGHT`, `NOSTEM`, `PHONETIC`, `SEPARATOR`, `CASESENSITIVE`, `WITHSUFFIXTRIE`, `INDEXEMPTY`, `INDEXMISSING` are read, kept, saved and reported by `FT.INFO`. |
| `VECTOR_RANGE` returned nothing | `@field:[VECTOR_RANGE r $v]=>{$YIELD_DISTANCE_AS: d}` returns what is within the radius, nearest first, with the distance. |
| A non-vector `FT.SEARCH` returned no fields | Documents come back with their fields; `RETURN n field [AS name]`, `NOCONTENT`, `SORTBY`, `LIMIT`, `WITHSCORES`, `WITHSORTKEYS`, `HIGHLIGHT` and `INKEYS` do what they say. |
| Negation in a pre-filter was ignored | `-@tag:{x}` and `-(…)` exclude. |
| `OR` across clauses returned nothing | `a \| b`, `(@x:{1}) \| (@y:{2})` and unions inside a field work. |
| A tag written without braces matched | `@tag:value` on a TAG field matches nothing, as in Redis: a TAG field is queried with braces, `@tag:{value}`. |
| RESP3 replies were arrays | `FT.SEARCH`, `FT.AGGREGATE`, `FT.INFO` and `FT.PROFILE` reply with the RESP3 maps Redis uses. |
| `FT.CREATE` errors were misleading | An error names the argument it is about, in Redis's words. |

Beyond the report:

* **`ON JSON` indexes work.** They used to be created and index nothing. A
  schema field is a JSONPath; documents are indexed as they are written by
  any `JSON.*` command, on replicas and on replay as well, and when an index
  is created over documents that are already there.
* **A hash with a field TTL is still a document.** It used to drop out of
  every query when one of its fields was given a TTL.
* **`FT.AGGREGATE`**: `REDUCE COLLECT` (8.8) with its in-group `SORTBY`,
  `LIMIT` and `DISTINCT`; `STDDEV`, `QUANTILE`, `RANDOM_SAMPLE`,
  `FIRST_VALUE … BY`; `TOLIST` returns a list; `SORTBY … MAX`; `ADDSCORES`;
  groups in the order they first appear; `WITHCURSOR` and `FT.CURSOR
  READ | DEL`.
* **`FT.EXPLAIN` / `FT.EXPLAINCLI`** return the plan; **`FT.PROFILE`** runs
  the query and returns it with its profile; **`FT.CONFIG`** reads and sets
  the engine's settings. All three were stubs.
* **Synonym groups and dictionaries are saved** in snapshots and sent in a
  full sync. They used not to survive a restart.
* The tokenizer splits where Redis's does, and a backslash keeps the next
  character in the word (`users\.u_1` is one word), in a document and in a
  query. An index can have its own `STOPWORDS`, and `FT.ALTER` covers
  documents that already exist.

## Memory, eviction, Pub/Sub, transactions

* **`maxmemory-policy`**: every Redis policy — `noeviction`,
  `allkeys-lru | lfu | lrm | random`, `volatile-lru | lfu | lrm | random | ttl`
  — with `maxmemory-samples`, `lfu-log-factor`, `lfu-decay-time`,
  `evicted_keys`, the `evicted` keyspace event, and `OBJECT IDLETIME` /
  `OBJECT FREQ`. The limit is on allocated memory (`used_memory`), which the
  server's allocator now counts; commands that free memory are not refused
  at the limit, and neither is the replay of the append-only file at startup
  (a server over its limit used to come up empty).
* **Pub/Sub does not lose messages.** Subscribers used to be fed through
  1024-message buffers per channel and silently lost whatever they fell
  further behind than that. Each has one queue now, in publish order across
  its channels; one that falls too far behind is disconnected (32 MB, or 8 MB
  for 60 s). A RESP2 connection that unsubscribes from everything is an
  ordinary connection again — it used to be closed.
* **Keys expire on time.** The background task visited one database in
  sixteen each tenth of a second; a key waited up to 1.6 s after its TTL to
  be deleted and announced. It now goes round all of them each cycle, within
  a time budget. Hash fields expire in the background too, with their
  notifications, and a hash left with no fields is deleted.
* **`MULTI`/`EXEC` and scripts run alone** — see `docs/EXEC_ATOMICITY.md`.
* **`RESET`**, and `calls` / `usec` in `INFO commandstats`.

## Not implemented

| Feature | Version | Why / what happens |
|---|---|---|
| Compact hash encoding | 8.10 | An internal memory optimisation with nothing a client can observe. `HIMPORT` works; hashes are stored as before. |
| Stemming, including the Malay and Tagalog stemmers; phonetic matching | 8.10 / — | TEXT matches whole words, prefixes, suffixes, infixes and fuzzily. `NOSTEM` and `PHONETIC` are accepted in a schema and change nothing. A query for `running` does not find `run`. |
| `GEO` and `GEOSHAPE` fields; `GEOFILTER`, `INFIELDS`, `EXPLAINSCORE` | — | Refused with an error rather than ignored. |
| `search-global-timeout` | 8.10 | Not implemented. Query `TIMEOUT` and `ON_TIMEOUT` cut a query short. |
| `FT.HYBRID` shard ratio, `EXPLAINSCORE` | 8.10 | |
| Per-iterator timings in `FT.PROFILE` | — | The times reported are the query's as a whole. |
| Parse-batch metrics in `INFO` and `CLIENT LIST` | 8.8 | Not reported. |
| `XIDMPRECORD` | 8.6 | Internal to Redis's AOF rewrite; KiviDB saves idempotency state in its snapshot instead. |
| `client-output-buffer-limit` | — | Not configurable: Pub/Sub uses Redis's default limits, replicas `repl-replica-max-backlog-bytes`. |
| A master's replication ID across a restart, without the AOF | — | A master restarted without the AOF has a new ID, and its replicas resync in full. Since 1.1.0, with the AOF on, a master stopped cleanly keeps its ID and offset. A restarted replica continues by partial resync. |

## Known gaps in what is implemented

* With clients blocked on a key in both `BLPOP` and `BRPOP`, and pushes
  arriving faster than the blocked clients are scheduled, a `BRPOP` may take a
  later element than Redis would give it. Order of service is unaffected, and
  nothing is lost or delivered twice.
* `OBJECT IDLETIME` is `0` under `noeviction` and the policies that do not go
  by use: keys are watched only when a policy needs it, so that the default
  costs a command nothing. Redis always tracks one of idle time or frequency.
* A replica expires keys by its own clock rather than waiting for its
  master's `DEL`. Both hold the same absolute expiry times, so they agree to
  within their clocks.
* A replica does not enforce `maxmemory` on what its master sends it (Redis's
  `replica-ignore-maxmemory yes`, which cannot be turned off here).
* On a JSON index, a NUMERIC path that selects several numbers indexes the
  first; `RETURN` gives the first match of a path (dialect 2), not the array
  of all matches (dialect 3).
* `HIGHLIGHT` wraps an escaped word whole: `<b>users\.u_1</b>`. Redis wraps
  `<b>users\</b>.u_1`, which cuts the word after the backslash.
* An ACL change that takes away a channel a client is already subscribed to
  does not end that subscription; it stops the client subscribing again.
* HNSW graphs are not part of a backup's BASE file; they are rebuilt on
  restore.
* Throughput. Running commands inside the gate that isolates transactions,
  ordering writes for replication, counting allocations and timing every
  command for `commandstats` are each small, and together measurable: on a
  pipelined local benchmark (12 connections, 8 threads) this build does about
  10% fewer `SET`/`GET`/`INCRBY` per second than v1.0.4. No one of them
  accounts for more than about 3%.
