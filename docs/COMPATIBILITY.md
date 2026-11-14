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
| Redis 8 data types | JSON, search and query (`FT.*`, full-text, tag, numeric and vector), time series (`TS.*`), vector sets (`V*`), Bloom filters (`BF.*`). All built in, nothing to load. |
| Transactions and scripts | `MULTI`/`EXEC`/`WATCH` and scripts run as one step, as in Redis ([EXEC_ATOMICITY.md](EXEC_ATOMICITY.md)). Lua (`EVAL`, `FUNCTION`, `FCALL`) needs the `lua` or `full` build. |
| Security | `AUTH`, ACLs (users, key and channel patterns, ACL file), protected mode, TLS in the `tls` and `full` builds. |
| Persistence | Snapshots (`dump.kdb`) and an append-only file. Reads a Redis `dump.rdb`. ([PERSISTENCE.md](PERSISTENCE.md)) |
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
- RedisVL (`redisvl`): SearchIndex create, load and query, including filters, SemanticCache and
  SemanticRouter. `HybridQuery` (`FT.HYBRID` with `SCORER`) fails on KiviDB; the default image is enough,
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
  data back off KiviDB means copying it key by key. Bloom filters are the
  exception: their payloads are Redis's, and Redis restores them.
- `DUMP`/`RESTORE` doesn't support time series and vector sets.
- Search: no stemming or phonetic matching (`NOSTEM` and `PHONETIC` are
  accepted and change nothing). `GEO` and `GEOSHAPE` fields, `GEOFILTER` and
  `INFIELDS` are refused. Query `TIMEOUT` is accepted but doesn't cut a query
  short. `INT8` and `UINT8` vectors aren't supported.
- A Redis RDB with search index definitions loads without them: run
  `FT.CREATE` again. Bloom filters load (RedisBloom 2.4 and later: Redis
  Stack 6.2 to 7.4, Redis 8). One with other module data (time series,
  vector sets, Cuckoo filters, Count-min sketches, Top-K, t-digests) is
  refused.
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
- `MEMORY USAGE` is an estimate; `MEMORY MALLOC-STATS` is a stub.
- `OBJECT IDLETIME` is `0` unless a `maxmemory-policy` that uses it is set.
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
- A replica doesn't enforce `maxmemory` on what its master sends. A replica
  of a KiviDB master older than 1.1.3 expires keys by its own clock (such a
  master sends no `DEL` for the keys it expires). A replica of a 1.2.0
  master, and a 1.2.0 replica, expire hash fields by their own clock (1.2.0
  sends no `HDEL` for the fields it expires, and a 1.2.0 replica does not
  wait for one).

**Cluster**
- `FT.SPELLCHECK`, `FT.PROFILE`, `FT.HYBRID` and `FT.SYNDUMP` answer from the
  node they're sent to.
- Without `tls-cluster`, traffic between nodes is unencrypted, as in Redis.
- KiviDB nodes cluster only with KiviDB nodes.
- `cluster-node-timeout` is read only at startup: `CONFIG SET` refuses a
  new value, where Redis takes it at runtime. Set it in the config file or
  with `--cluster-node-timeout` and restart the node. The announce settings
  (`cluster-announce-ip`, `-port`, `-bus-port`, `-hostname`,
  `-client-port`) and `cluster-require-full-coverage` can be changed at
  runtime, as in Redis.
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

**Performance**
- Many clients writing to **one key** are slower while something follows the
  write stream (a replica, the replication backlog, a backup): about 210,000
  writes a second against 1.1 million without, on 8 CPUs. Writes spread over
  many keys aren't affected.

[COMMANDS.md](../COMMANDS.md#not-yet-supported) lists the rest, down to
single options. If something you rely on behaves differently from Redis and
isn't listed here, please
[open an issue](https://github.com/kividbio/kividb/issues/new/choose).
