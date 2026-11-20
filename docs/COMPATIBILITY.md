# Compatibility with Redis

KiviDB aims to behave like **Redis 8.10** for every command it implements.
This page sums up where it stands and where it differs. The details are in
other files:

- [COMMANDS.md](../COMMANDS.md): every command, with its syntax.
- [REDIS_8_4_TO_8_10_PARITY.md](REDIS_8_4_TO_8_10_PARITY.md): each Redis
  8.4–8.10 addition, and what isn't implemented.
- [REDIS_TEST_SUITE.md](REDIS_TEST_SUITE.md): Redis's own test suite run
  against KiviDB, unit by unit, with what still fails and why.
- [MIGRATING_FROM_REDIS.md](MIGRATING_FROM_REDIS.md): moving an application
  and its data.

## At a glance

| Area | Status |
|---|---|
| Protocol | RESP2 and RESP3 (`HELLO`). Redis clients, `redis-cli` and cluster-aware clients work unchanged. |
| Core data types | Strings, lists, sets, sorted sets, hashes (with per-field TTLs), streams and consumer groups, bitmaps, geo, Pub/Sub (including sharded). HyperLogLog, which reads Redis's encoding: see *Differences*. |
| Redis 8 data types | JSON, search and query (`FT.*`, full-text, tag, numeric and vector), time series (`TS.*`), vector sets (`V*`), Bloom filters (`BF.*`), Cuckoo filters (`CF.*`), Count-min sketches (`CMS.*`), Top-K (`TOPK.*`), t-digests (`TDIGEST.*`). All built in, nothing to load. |
| Transactions and scripts | `MULTI`/`EXEC`/`WATCH` and scripts run as one step, as in Redis ([EXEC_ATOMICITY.md](EXEC_ATOMICITY.md)). Lua (`EVAL`, `FUNCTION`, `FCALL`) needs the `lua` or `full` build. |
| Security | `AUTH`, ACLs (users, key and channel patterns, ACL file), protected mode, TLS in the `tls` and `full` builds. |
| Persistence | Snapshots (`dump.kdb`, compressed with `rdbcompression yes`, which is off by default) and an append-only file. Reads a Redis `dump.rdb`. ([PERSISTENCE.md](PERSISTENCE.md)) |
| Replication | `REPLICAOF`, partial resync, and replicating from a Redis primary. |
| Cluster | Redis's cluster protocol: hash slots, `CLUSTER` commands, gossip, failover, atomic slot migration. |
| Sentinel | Not implemented. |
| Modules | `MODULE LOAD` is not supported; the Redis 8 modules' commands are built in. |

## Tested with

- Redis's own test suite: [REDIS_TEST_SUITE.md](REDIS_TEST_SUITE.md).
- BullMQ's and Sidekiq's own test suites, and kill-and-restart soaks
  (`scripts/client_compat/`).
- LiteLLM's caches (exact and semantic), router counters and proxy rate limiter (the `lua` build for the
  proxy), with LiteLLM's own Redis tests: [benchmarks/litellm-compat/REPORT.md](../benchmarks/litellm-compat/REPORT.md)
  and [docs/integrations/litellm.md](integrations/litellm.md).
- LangChain's Redis integration (`langchain-redis`): vector store, LLM and semantic cache, chat history:
  [benchmarks/langchain-compat/REPORT.md](../benchmarks/langchain-compat/REPORT.md) and
  [docs/integrations/langchain.md](integrations/langchain.md).
- LangGraph's Redis checkpointer and store (`langgraph-checkpoint-redis`):
  [benchmarks/langgraph-compat/REPORT.md](../benchmarks/langgraph-compat/REPORT.md) and
  [docs/integrations/langgraph.md](integrations/langgraph.md).
- LlamaIndex's Redis vector store, document store, index store and key-value store:
  [benchmarks/llamaindex-compat/REPORT.md](../benchmarks/llamaindex-compat/REPORT.md) and
  [docs/integrations/llamaindex.md](integrations/llamaindex.md).
- RedisVL (`redisvl`): SearchIndex create, load and query, including filters, SemanticCache,
  SemanticRouter and `HybridQuery` (`FT.HYBRID`, from the fix for #767); the default image is enough,
  no Lua: [benchmarks/redisvl-compat/REPORT.md](../benchmarks/redisvl-compat/REPORT.md) and
  [docs/integrations/redisvl.md](integrations/redisvl.md).
- Mem0's Redis vector store and the `Memory` API on top of it, in the Python SDK and the TypeScript SDK
  (whose Valkey store works too):
  [benchmarks/mem0-compat/REPORT.md](../benchmarks/mem0-compat/REPORT.md),
  [benchmarks/mem0-ts-compat/](../benchmarks/mem0-ts-compat/) and
  [docs/integrations/mem0.md](integrations/mem0.md).
- claude-mem's server job queue (BullMQ; the `lua` build): its own Docker E2E, and enqueue, retry,
  failure, cancel and restarts with real workers: [benchmarks/claude-mem-compat/](../benchmarks/claude-mem-compat/)
  and [docs/integrations/claude-mem.md](integrations/claude-mem.md).
- OpenViking's Redis parts (the `lua` build): its RAGFS cache runtime and PathLock tests, its gateway suite
  (standalone and cluster), and its expiry scheduler:
  [benchmarks/openviking-compat/](../benchmarks/openviking-compat/) and
  [docs/integrations/openviking.md](integrations/openviking.md).
- LightRAG's Redis KV and doc-status storages, with LightRAG's own contract suite:
  [benchmarks/lightrag-compat/](../benchmarks/lightrag-compat/) and
  [docs/integrations/lightrag.md](integrations/lightrag.md).
- cognee's Redis cache (sessions, usage logs, the shared graph lock, which needs the `lua` build):
  [benchmarks/cognee-compat/](../benchmarks/cognee-compat/) and
  [docs/integrations/cognee.md](integrations/cognee.md).
- OmniRoute's rate limiter (a Lua script, so the `lua` build), auth cache, quota store, circuit
  breaker and semantic cache: [benchmarks/omniroute-compat/](../benchmarks/omniroute-compat/) and
  [docs/integrations/omniroute.md](integrations/omniroute.md).
- Dify's self-hosted stack (cache, Celery broker and result backend, pub/sub, locks; the `lua` build),
  end to end and with Dify's own Redis tests: [benchmarks/dify-compat/](../benchmarks/dify-compat/) and
  [docs/integrations/dify.md](integrations/dify.md).
- RAGFlow's self-hosted stack, end to end (the `lua` build): RAGFlow 1.0 in place of its Kvrocks
  service (secret key, agent-run lease, locks, media cache), with RAGFlow's own Go HTTP API suite, and
  RAGFlow 0.27 in place of its Valkey service (the stream task queue, cache and locks):
  [benchmarks/ragflow-compat/](../benchmarks/ragflow-compat/) and
  [docs/integrations/ragflow.md](integrations/ragflow.md).
- n8n's queue mode (Bull; the `lua` build) end to end, with worker and server restarts, and n8n's own
  Redis tests: [benchmarks/n8n-compat/](../benchmarks/n8n-compat/) and
  [docs/integrations/n8n.md](integrations/n8n.md).
- Self-hosted Firecrawl (BullMQ, crawl state, rate limits; the `lua` build) with its own test suite and
  its docker compose: [benchmarks/firecrawl-compat/](../benchmarks/firecrawl-compat/) and
  [docs/integrations/firecrawl.md](integrations/firecrawl.md).
- Celery and kombu (broker and result backend; the `lua` build), with their own Redis integration suites
  and a real worker through a SIGKILL and server restarts. Chords need the result backend on RESP2 on
  1.2.0 ([#431](https://github.com/kividbio/kividb/issues/431), fixed in 1.2.1):
  [benchmarks/celery-compat/](../benchmarks/celery-compat/) and [docs/integrations/celery.md](integrations/celery.md).
- Semantic Kernel's Redis vector store (Python; JSON and hash collections), with its own Redis tests:
  [benchmarks/semantic-kernel-compat/](../benchmarks/semantic-kernel-compat/) and
  [docs/integrations/semantic-kernel.md](integrations/semantic-kernel.md).
- Spring AI's Redis vector store and chat memory repository (the `lua` build for chat memory), with its own
  Redis integration tests: [benchmarks/spring-ai-compat/](../benchmarks/spring-ai-compat/) and
  [docs/integrations/spring-ai.md](integrations/spring-ai.md).
- GPTCache's Redis scalar store, vector store and distributed eviction, with its own Redis tests:
  [benchmarks/gptcache-compat/](../benchmarks/gptcache-compat/) and [docs/integrations/gptcache.md](integrations/gptcache.md).
- `@upstash/ratelimit` 2.0 through the Serverless Redis HTTP proxy (KiviDB has no REST API; the `lua`
  build), with its own test suite: [benchmarks/upstash-ratelimit-compat/](../benchmarks/upstash-ratelimit-compat/)
  and [docs/integrations/upstash-ratelimit.md](integrations/upstash-ratelimit.md).
- Not compatible: Haystack's Valkey document store, which needs valkey-search's reply formats (it fails on
  Redis 8 too): [docs/integrations/haystack.md](integrations/haystack.md).
- `redis-py` throughout the end-to-end tests (`tests/e2e/`).

## Differences you may notice

**Builds and defaults**
- The default port is **6380**, not 6379.
- The default build has no Lua. The scripting commands return an error
  there; use the `lua` or `full` build for job queues and anything else that
  runs scripts.
- Scripts run on **Lua 5.4**, not Redis's Lua 5.1. `string.format('%d')`
  and the other integer conversions take numbers with a fraction as 5.1
  does, and numbers are passed to commands, returned, and encoded by
  `cjson` and `cmsgpack` as Redis does. What stays 5.4: `/` gives a float
  that is written with `.0` (`tostring(10 / 2)` is `"5.0"`, so keys built
  with `..` from a division differ; use `math.floor` or
  `string.format('%d')`), integers are exact 64-bit numbers, and 5.4's
  syntax (`//`, bitwise operators, `goto`) and `table.unpack` are there.
  The full list, with workarounds: [SCRIPTS.md](SCRIPTS.md#lua-54-and-how-it-differs-from-rediss-lua-51).
- `save <points>` has no effect: KiviDB doesn't snapshot on a schedule. Use
  the AOF or `BGSAVE`.
- `appendfsync no` is taken as `everysec`; `always` and `everysec` do what
  they do in Redis.
- The config file is read strictly: an unknown directive stops the server.
- Without `--configfile`, KiviDB still reads `/etc/kividb/kividb.conf` if it
  exists, where Redis reads a config file only when one is named. Such a
  server logs a deprecation warning at startup and refuses `CONFIG REWRITE`;
  start it with `--configfile` to rewrite the file. A future minor release
  will read only a named config file.

**Sets**
- Small sets are kept as Redis keeps them, and `OBJECT ENCODING` says so:
  `intset` for up to `set-max-intset-entries` (512) integers, `listpack` for
  up to `set-max-listpack-entries` (128) members of up to
  `set-max-listpack-value` (64) bytes, `hashtable` otherwise. The two
  `listpack` limits go no higher than 128 and 64; a larger value is taken as
  those, and `CONFIG GET` says so. With `set-max-intset-entries` set under
  128, an intset that passes it becomes a `listpack` where Redis makes a
  `hashtable`; at the defaults the two agree.

**Hashes**
- Small hashes are kept as Redis keeps them, and `OBJECT ENCODING` says so:
  `listpack` for up to `hash-max-listpack-entries` (512) fields with no
  field or value longer than `hash-max-listpack-value` (64) bytes,
  `listpackex` for such a hash once a field has a TTL, `hashtable`
  otherwise; a hash never goes back, and `HSCAN` returns a `listpack` whole,
  with cursor 0. The two limits go no higher than 512 and 64 (the most one
  block holds); a larger value is taken as those, and `CONFIG GET` says so
  (Redis keeps what it is given). The `hash-max-ziplist-*` names set and
  report the same two settings.
- A hash whose fields and values pass 65,535 bytes together is a
  `hashtable` even when it is within both limits (at most 511 fields of 64
  bytes with 64-byte values fit one block); Redis's listpack holds more.
- The 64-byte and 65,535-byte limits count a field as it is stored, after
  the bytes of a field that is not UTF-8 are escaped as text: a binary
  field Redis keeps in a `listpack` can make a `hashtable` here.
- A lowered limit applies to a hash at its next write that sets a field
  (`HSET` and the like); `HDEL` and field expiry never move a hash, as in
  Redis.
- `HGETALL`, `HKEYS`, `HVALS`, `HSCAN` and `HRANDFIELD` give a hash's fields
  in the order they were first set, field TTLs or not. Redis keeps a
  `listpackex` in the order of its deadlines, the fields with one first,
  soonest first; so once a field of a small hash has a TTL the two orders
  differ. They agree for a `listpack`.

**Sorted sets**
- Small sorted sets are kept as Redis keeps them, and `OBJECT ENCODING`
  says so: `listpack` for up to `zset-max-listpack-entries` (128) members
  of up to `zset-max-listpack-value` (64) bytes, `skiplist` otherwise (a
  hash table and an ordered tree here). A set moves when a write adds a
  member past either limit, or when one `ZADD` (or `GEOADD`) gives more
  pairs than the entries limit, as in Redis; never back: a set taken back
  under the limit stays a `skiplist`, as in Redis. A set made whole (a
  load, `RESTORE`, `MIGRATE`, `ZRANGESTORE`, `ZUNIONSTORE` and the other
  `*STORE` commands) is a `listpack` if it fits, as Redis loads one.
- `ZSCAN` returns a `listpack` whole, in order, with cursor 0, whatever
  `COUNT` says, as Redis does.
- The two limits go no higher than 128 and 64 (the most one block holds);
  a larger value is taken as those, and `CONFIG GET` says so (Redis keeps
  what it is given, and its listpack holds more). The `zset-max-ziplist-*`
  names set and report the same two settings. A lowered limit applies to
  a set at its next new member; a changed score never moves a set, as in
  Redis.
- `SORT` of a `listpack` sorted set leaves it a `listpack`; Redis 8.10
  turns the set it sorts into a `skiplist`, so `OBJECT ENCODING` differs
  after a `SORT` (the replies do not).

**Lists**
- A list's elements are kept end to end in chunks of up to 64, as Redis
  keeps a quicklist of listpacks, but the size of a chunk is fixed: Redis
  closes a node at `list-max-listpack-size` (8 kB by default), KiviDB at 64
  elements. A list that has had an element longer than 1,024 bytes keeps
  every element in a block of its own from then on, as 1.2 kept every list.
- `OBJECT ENCODING` says `listpack` while a list is one chunk and
  `quicklist` otherwise, so it differs from Redis in two ways:
  - Redis `listpack`, KiviDB `quicklist`: lists of 65 to about 240 small
    elements (32 bytes each, say), and short lists that hold, or held, an
    element of 1,025 to 8,192 bytes.
  - Redis `quicklist`, KiviDB `listpack`: up to 64 elements of more than
    8 kB in all, such as a few dozen elements of 1 kB.

  Redis goes back to `listpack` when a quicklist shrinks to one node of
  half the limit; KiviDB when it is down to one chunk, and never once its
  elements are kept apart.
- `list-max-listpack-size`, `list-max-ziplist-size` and
  `list-compress-depth` are accepted and reported, and change nothing: no
  chunk is compressed.

**Durability**
- With the AOF on, a write is acknowledged once its record is in the file,
  as in Redis: a hard kill (`kill -9`, the OOM killer) loses no acknowledged
  write while the disk keeps up, and a power failure up to a second, as with
  Redis's `everysec`. A reply waits at most 5 s for its record: a disk more
  than 5 s behind gets replies sent without their writes, counted in
  `aof_acks_before_write` (Redis waits for as long as the write takes).
  Before 1.2 a write was acknowledged once its record was queued,
  and a hard kill could lose the last few; `aof-ack-after-write no` still
  does that, for more writes a second.

**Data**
- Keys, values, hash fields, set and sorted-set members, list elements,
  stream consumer group and consumer names and vector-set element names are
  any bytes, as in Redis. What is still text: ACL rules (a key or channel
  pattern in `ACL SETUSER` that is not UTF-8 is refused,
  [#437](https://github.com/kividbio/kividb/issues/437)), search index
  names, prefixes and field names, and time-series label names and values.
- A HyperLogLog copied from Redis (`DUMP`/`RESTORE`, an RDB, replication)
  loads and counts as it did on Redis. By default KiviDB writes new
  HyperLogLogs in its own format, which Redis refuses (`INVALIDOBJ`), and
  `PFMERGE` or a multi-key `PFCOUNT` refuses to combine the two formats. After
  moving from Redis, set `hll-write-format redis` so new and copied
  HyperLogLogs mix and can move back ([PERSISTENCE.md](PERSISTENCE.md)).
- Valkey 9.2's RDB format (version 81) isn't read yet: a file, full sync
  or `DUMP` payload from it is refused. Valkey 9.0's and 9.1's (version 80,
  a `VALKEY080` header) is read, hash field TTLs included.
- Redis and Valkey can't read KiviDB's `DUMP` payloads or replicate from
  KiviDB ([#450](https://github.com/kividbio/kividb/issues/450)), so moving
  data back off KiviDB means copying it key by key. Bloom and Cuckoo
  filters, Count-min sketches and Top-K sketches are the exception: their
  payloads are Redis's, and Redis restores them.
- `DUMP`/`RESTORE` and `MIGRATE` of time series and vector sets work between
  KiviDB nodes only: their payloads are KiviDB's own, and Redis's for these
  types (module values) aren't read either. A series' compaction rules and
  source are left out, as RedisTimeSeries leaves them out, so after
  `RESTORE`, `MIGRATE` or `MIGRATE`-based resharding run `TS.CREATERULE`
  again; an atomic slot migration (`CLUSTER MIGRATION`) recreates the rules
  itself. A vector set made
  `NOQUANT` arrives as a set made without an option, so it then accepts a
  `VADD` with any quantization option, the same as after a snapshot reload.
- Search: `GEO` and `GEOSHAPE` fields, `GEOFILTER` and `INFIELDS` are
  refused. Query `TIMEOUT` (or `search-timeout`) cuts a query short; `ON_TIMEOUT` or
  `search-on-timeout` chooses a partial reply with a warning, or an error. `INT8`
  and `UINT8` vectors aren't supported. Scores, `NOFREQS`, `PHONETIC`
  (`dm:en`, `dm:fr`, `dm:pt`, `dm:es`: the codes and the scores), the `~` and
  `-` of dialect 1 and the way `FT.AGGREGATE` writes numbers (12 significant
  digits) were compared with Redis 8.10.2 on the same data, and are its,
  but for these differences:
  - Scores: three of the compared queries differ from Redis's in the last
    digit, all of three words (`apple cherry banana`: `SCORER BM25` on two
    indexes, and the default scorer on one with field weights; the order
    Redis adds the terms in); `TFIDF`, `TFIDF.DOCNORM` and `BM25` of three or
    more words, one of which has a stem, can differ in the proximity factor;
    a fuzzy term scores one of the document's words it matches, which one
    can differ (see `vector/score.rs`); documents with the same score come
    in key order, Redis's in the order they were indexed.
  - Dialect 1: a field modifier applies to the bare words that follow it
    (`@title:a b` is `@title:(a b)`) in Redis, to the first only here;
    `=> {$weight: …}` on a clause is accepted but does not weigh it (and is
    refused with a space after `=>`); `$phonetic: false` on a group
    (`@p:(a b)=>{…}`) does not score as Redis does; the text of a syntax
    error and its offset differ.
  - After a document with a `PHONETIC` field is changed or deleted, Redis
    keeps counting it in the frequency of the code term until its garbage
    collection has run, so scores differ until then; here the count is
    exact.
  - `FT.SEARCH RETURN` shows a NUMERIC field as it was stored (Redis writes
    it as a double) and a `KNN` distance as the shortest text of a 32-bit
    float (Redis, 12 significant digits); `FT.AGGREGATE` with a `GROUPBY` and
    no `LIMIT` returns every group (Redis, 10); in the expressions of `APPLY`
    and `FILTER`, `-x`, `a+b` with no space and `round`/`pow` are accepted
    (Redis refuses them), `^` is not a power, a text that is not a number is
    0 in arithmetic (Redis refuses it), `SORTBY` on a TEXT field of numbers
    sorts them as numbers (Redis, as text); `COUNT_DISTINCTISH` is exact and
    `TOLIST` lists in the order the values came, where Redis's order is its
    own; `QUANTILE` is exact (the value at rank `ceil(q * n)`), which is what
    Redis gives for a small group, while for a group of thousands of values
    (4,000 were compared) Redis's answer is an estimate, close to it but not
    the same.
- A Redis RDB with search index definitions loads without them: run
  `FT.CREATE` again. Bloom filters load (RedisBloom 2.4 and later: Redis
  Stack 6.2 to 7.4, Redis 8), and so do Cuckoo filters, Count-min sketches
  and Top-K sketches. One with other module data (time series, vector sets,
  t-digests) is refused.
- Bloom filters: `COPY` copies one, which Redis refuses. A filter made before
  RedisBloom 2.0 (32-bit hashes) is not loaded. A filter has size limits
  Redis does not have: a client's commands do not make or grow one past
  `bloom-max-filter-bytes` (512 MiB unless raised) or 65,536 hashes in all
  its layers, and none of more than 2048 layers or 600,000 hashes is made,
  grown or loaded. Replication, the AOF and loads are held only to the
  last two, which no filter Redis grows reaches. `BF.MADD`, `BF.INSERT`
  and `BF.MEXISTS` have a per-command limit. What a header, a
  snapshot, an RDB file or a `DUMP` payload says about a filter is checked
  before it is used: one Redis could not have made is refused. Their `DUMP` payloads carry RDB version 9, so every Redis
  since 6.0 can `RESTORE` them.
- Count-min sketches: counts are Redis's exactly (items are placed as
  Redis places them). `COPY` copies one, which Redis refuses. A sketch has
  at most 134,217,728 counters and 65,536 rows, made or loaded; `CMS.INCRBY`,
  `CMS.QUERY` and `CMS.MERGE` have per-command limits, checked before they
  start. `CMS.MERGE` names its sources as keys (ACL patterns and cluster
  slots apply to them), and needs read permission on them and write
  permission on its destination (Redis checks only the destination,
  and takes read or write permission on it). See [COMMANDS.md](../COMMANDS.md).
- Top-K: with a decay of 1 every answer is Redis's; below 1, which bucket
  decays is left to chance, drawn from a generator each sketch keeps (so
  replicas and the AOF stay identical to the master), and answers are as
  good as Redis's, not bit for bit the same. `DUMP` (Redis's format) does
  not carry the generator: a copy made by `RESTORE`, `MIGRATE` or a slot's
  move starts it afresh, and from then on does not make the choices the
  original would have. Items are binary-safe (Redis
  cuts them at a zero byte in replies). `COPY` copies one, which Redis
  refuses. Size limits (k 65,536, 67,108,864 buckets, 65,536 rows) and a
  per-command limit on `TOPK.ADD`, `TOPK.INCRBY`, `TOPK.QUERY` and
  `TOPK.COUNT`, checked before they start. See [COMMANDS.md](../COMMANDS.md).
- Cuckoo filters are Redis's byte for byte (replies, `CF.SCANDUMP` chunks,
  `DUMP`). `COPY` copies one, which Redis refuses. At most 1 GiB and 99
  sub-filters, made, grown or loaded; `CF.INSERT`, `CF.INSERTNX` and
  `CF.MEXISTS` have a per-command limit, checked before they start, and a
  compaction stops at a limit of its own (a large filter compacts in part,
  where Redis's compaction goes on). A sub-filter over 16 MiB is dumped in
  pieces that follow on, which Redis does not do. See
  [COMMANDS.md](../COMMANDS.md).
- t-digests: answers are approximations within the same bounds as Redis's,
  not bit for bit the same (where nothing is approximated, they are Redis's).
  A query never changes a digest, so `TDIGEST.INFO`'s node counts can differ.
  Their `DUMP` payloads are KiviDB's own (Redis's are refused). A compression
  of at most 100,000; queries of many values and merges have a per-command
  limit. `TDIGEST.MERGE` needs read permission on its sources and write
  permission on its destination (Redis takes read
  permission on the destination for write). See [COMMANDS.md](../COMMANDS.md).
- Set and sorted-set members can't have their own TTLs. A Dragonfly file
  with a `SADDEX` set, or a KeyDB file or stream with `EXPIREMEMBER` on a set
  or sorted-set member, is refused, naming the key. Hash field TTLs from
  either load. Dragonfly's Bloom, Cuckoo, count-min and top-k keys and its
  compressed snapshot blocks are refused (save with `--compression_mode=0`).
- KiviDB can't be a replica of a KeyDB primary in active replication
  (`active-replica yes`): the link ends at its first `RREPLAY`, saying so.
  Move that data by RDB file or with `MIGRATE`.
- `JSON.NUMINCRBY` and `JSON.NUMMULTBY` refuse any integer result that does
  not fit an `i64` with `ERR numeric overflow`, and change nothing. Redis
  returns that error too, with three exceptions KiviDB doesn't copy: it
  stores a `u64` for some array elements (`JSON.NUMINCRBY k $[0] 1` on
  `[9223372036854775807]` succeeds), it gives a float for some integers
  above `i64::MAX`, losing precision, and it can leave a multi-match path
  half done ([#536](https://github.com/kividbio/kividb/issues/536)).

**Server and introspection**
- `HELLO` and `INFO`'s `redis_version` report the Redis release KiviDB
  follows (`server: redis`, `version: 8.10.0`), as Dragonfly does, so that
  clients use what that release has: redis-py's and Jedis's client-side
  caches need Redis 7.4 or later. KiviDB's own release is `kividb_version`,
  in `HELLO` and in `INFO`.
- `enable-debug-command` is Redis's, `no` by default from 1.3.0 (1.2.2 and
  earlier ignored it): `DEBUG` is refused with Redis's error unless the
  server started with `yes`, or with `local` and the client is on loopback
  (KiviDB has no Unix socket). Inside `MULTI` the refusal discards the
  transaction, as in Redis. `CONFIG SET` refuses it, whatever the value, as
  Redis does. One difference: a user whose ACL does not allow `DEBUG` gets
  `NOPERM` (Redis checks the setting first).
- `enable-protected-configs` and `enable-module-command` are accepted, in
  the config file and by `CONFIG SET`, and change nothing: KiviDB has no
  protected configs and no modules.
- `MEMORY USAGE` is an estimate; `MEMORY MALLOC-STATS` is a stub.
- `OBJECT IDLETIME` is `0` unless a `maxmemory-policy` that uses it is set,
  or `key-access-tracking yes` is (Redis always keeps it; the setting costs
  about 50 instructions a command, 1 to 1.5% of a pipelined `GET` or `SET`).
- `CLIENT NO-TOUCH`: the client's reads leave a key's idle time and LFU
  count alone, and `TOUCH` still touches, as in Redis. Its writes count as
  uses of their keys, and so do the keys a script declares when the client
  runs one that may write (`EVAL`, `EVALSHA`, `FCALL`); in Redis a
  `NO-TOUCH` client's write to a key that is already there, or its script,
  leaves the key's use alone, and only a key it creates is marked used.
- `client-output-buffer-limit`'s replica hard limit is
  `repl-replica-max-backlog-bytes` (0: no limit, which lifts
  `repl-replica-sync-max-backlog-bytes` too); during a full sync a replica
  is held to `repl-replica-sync-max-backlog-bytes` instead. A replica has no
  soft limit by default (`slave 268435456 0 0`; Redis: 64 MB for 60
  seconds), on purpose: a burst a replica comes through should not become a
  full resync of a large dataset. One that is set holds only once the
  replica's queue has been under it after its full sync or partial resync.
  A redis.conf line `client-output-buffer-limit replica 256mb 64mb 60` turns
  it on, and overrides a raised `repl-replica-max-backlog-bytes` earlier in
  the file (the last line wins). The limits count the bytes waiting, not
  the allocator's rounding of them.
- `MONITOR` shows every command once, as Redis does: nothing is dropped.
  A monitor that reads more slowly than commands run has what it has not
  read kept for it, held to `client-output-buffer-limit normal` (the class
  Redis counts a monitor in; no limit by default, as in Redis); past that
  limit it is disconnected, with `Client id=… closed: it is in MONITOR and
  … bytes behind` in the log and `client_output_buffer_limit_disconnections`
  counted; what it has not read shows in its `omem` in `CLIENT LIST`.
  Every monitor sees the same order, a connection's commands always in the
  order it sent them, and time stamps in the order of the lines (they are
  read from the system clock, so they go back only when the clock is set
  back, as in Redis). A command is shown when it is called, before it runs;
  Redis shows it once it has run. So a script's `EVAL` is shown before the
  commands it calls (Redis: after them), `SELECT` is shown with the
  database it was called in (Redis: the one it selected), and two
  connections' commands on one key that arrive at nearly the same moment
  on two worker threads can have run in the other order than they are
  shown; Redis, with one thread, has no such moment. `AUTH` and `HELLO …
  AUTH` are not shown at all (Redis shows them with the password as
  `(redacted)`). Up to 1.2.2 a monitor that fell 4,096
  lines behind lost lines without a word: with four connections sending at
  full speed, half or more of what they sent.
- A replica doesn't enforce `maxmemory` on what its master sends. A replica
  of a KiviDB master older than 1.1.3 expires keys by its own clock (such a
  master sends no `DEL` for the keys it expires). A replica of a 1.2.0
  master, and a 1.2.0 replica, expire hash fields by their own clock (1.2.0
  sends no `HDEL` for the fields it expires, and a 1.2.0 replica does not
  wait for one).

**Cluster**
- `FT.SPELLCHECK`, `FT.PROFILE` and `FT.SYNDUMP` answer from the node
  they're sent to.
- `FT.HYBRID` is refused in a cluster of more than one master ("FT.HYBRID is
  not supported across a cluster's masters yet"), where Redis answers it
  from every shard: one node's answer would be part of the cluster's. A
  cluster of one master answers it.
- Without `tls-cluster`, traffic between nodes is unencrypted, as in Redis.
- KiviDB nodes cluster only with KiviDB nodes.
- `cluster-node-timeout` is read only at startup: `CONFIG SET` refuses a
  new value, where Redis takes it at runtime. Set it in the config file or
  with `--cluster-node-timeout` and restart the node. The announce settings
  (`cluster-announce-ip`, `-port`, `-bus-port`, `-hostname`,
  `-client-port`, and Valkey 8's `-client-ipv4` and `-client-ipv6`) and
  `cluster-require-full-coverage` can be changed at runtime, as in Redis.
- `cluster-announce-client-ipv4` / `-ipv6` (Valkey 8): a client is told the
  address for the family of its own connection, as in Valkey; one that
  reaches a node listening on `::` over IPv4 (an IPv4-mapped address) counts
  as IPv4. They travel in a node's own heartbeats (the bus's flags
  string, not Valkey's ping extensions; never in gossip, as in Valkey), and
  are not saved in `nodes.conf`: a restarted
  node tells clients a peer's own address until that peer's first heartbeat
  (about a second).
- Keys a master holds of slots it does not serve (left, for example, on a
  replica promoted after its master crashed right after handing slots over
  in an atomic slot migration) are not deleted on their own. KiviDB logs a
  warning, reports them in `INFO cluster` (`cluster_orphan_slots`,
  `cluster_orphan_keys`) and `CLUSTER ORPHANSLOTS`, and deletes them when
  told with `CLUSTER DROPORPHANS`, which only runs on a master, for slots
  another master that is up owns, with no migration of them under way, and
  deletes only the keys that owner, asked directly, says it has. Both commands
  are KiviDB's own: Valkey's `CLUSTER FLUSHSLOT` deletes any slot's keys,
  the node's own included, and Redis's `SFLUSH` only those of slots the
  node owns. See [COMMANDS.md](../COMMANDS.md#keys-of-slots-a-master-does-not-serve).
- A replica promoted after its master died part way through an atomic slot
  import deletes what the import brought, as Redis 8 does, which tells its
  replicas of the import in the stream (`CLUSTER SYNCSLOTS CONF ASM-TASK`).
  Unlike Redis, which deletes every key of the import's slots the node does
  not own (and does so too when an import begins), KiviDB keeps the keys
  that were there before the import, and deletes only once the slot's
  owner, asked directly, says it owns the slot and is not moving it. The
  message (`KIVIDB.IMPORTS`) is KiviDB's own, sent only to replicas that ask
  for it with `REPLCONF capa slot-imports`, and is not part of the stream.

**Performance**
- Many clients writing to **one key** are slower while something follows the
  write stream (a replica, the replication backlog, a backup): about 210,000
  writes a second against 1.1 million without, on 8 CPUs. Writes spread over
  many keys aren't affected.

[COMMANDS.md](../COMMANDS.md#not-yet-supported) lists the rest, down to
single options. If something you rely on behaves differently from Redis and
isn't listed here, please
[open an issue](https://github.com/kividbio/kividb/issues/new/choose).
