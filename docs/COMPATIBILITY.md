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
| Core data types | Strings, lists, sets, sorted sets, hashes (with per-field TTLs), streams and consumer groups, bitmaps, geo, Pub/Sub (including sharded). HyperLogLog works, but its encoding isn't Redis's yet: see *Differences*. |
| Redis 8 data types | JSON, search and query (`FT.*`, full-text, tag, numeric and vector), time series (`TS.*`), vector sets (`V*`). All built in, nothing to load. |
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
- LiteLLM's semantic cache: [benchmarks/litellm-compat/REPORT.md](../benchmarks/litellm-compat/REPORT.md).
- LangChain's Redis integration (`langchain-redis`): vector store, LLM and semantic cache, chat history:
  [benchmarks/langchain-compat/REPORT.md](../benchmarks/langchain-compat/REPORT.md) and
  [docs/integrations/langchain.md](integrations/langchain.md).
- LangGraph's Redis checkpointer and store (`langgraph-checkpoint-redis`):
  [benchmarks/langgraph-compat/REPORT.md](../benchmarks/langgraph-compat/REPORT.md) and
  [docs/integrations/langgraph.md](integrations/langgraph.md).
- LlamaIndex's Redis vector store, document store, index store and key-value store:
  [benchmarks/llamaindex-compat/REPORT.md](../benchmarks/llamaindex-compat/REPORT.md) and
  [docs/integrations/llamaindex.md](integrations/llamaindex.md).
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
- `redis-py` throughout the end-to-end tests (`tests/e2e/`).

## Differences you may notice

**Builds and defaults**
- The default port is **6380**, not 6379.
- The default build has no Lua. The scripting commands return an error
  there; use the `lua` or `full` build for job queues and anything else that
  runs scripts.
- `save <points>` has no effect: KiviDB doesn't snapshot on a schedule. Use
  the AOF or `BGSAVE`.
- `appendfsync` is always `everysec`.
- The config file is read strictly: an unknown directive stops the server.

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
- HyperLogLog registers aren't in Redis's layout
  ([#62](https://github.com/kividbio/kividb/issues/62)). `PFADD`, `PFCOUNT`
  and `PFMERGE` work within KiviDB, but an HLL moved between Redis and KiviDB
  (`DUMP`/`RESTORE`, an RDB, replication) gives wrong counts or is refused.
- `DUMP`/`RESTORE` doesn't support time series and vector sets.
- Search: no stemming or phonetic matching (`NOSTEM` and `PHONETIC` are
  accepted and change nothing). `GEO` and `GEOSHAPE` fields, `GEOFILTER` and
  `INFIELDS` are refused. Query `TIMEOUT` is accepted but doesn't cut a query
  short. `INT8` and `UINT8` vectors aren't supported.
- A Redis RDB with search index definitions loads without them: run
  `FT.CREATE` again. One with other module data (time series, vector sets,
  Bloom) is refused.

**Server and introspection**
- `MEMORY USAGE` is an estimate; `MEMORY MALLOC-STATS` is a stub.
- `OBJECT IDLETIME` is `0` unless a `maxmemory-policy` that uses it is set.
- `client-output-buffer-limit` isn't configurable.
- A replica doesn't enforce `maxmemory` on what its master sends. A replica
  of a KiviDB master older than 1.1.3 expires keys by its own clock (such a
  master sends no `DEL` for the keys it expires).

**Cluster**
- `FT.SPELLCHECK`, `FT.PROFILE`, `FT.HYBRID` and `FT.SYNDUMP` answer from the
  node they're sent to.
- Without `tls-cluster`, traffic between nodes is unencrypted, as in Redis.
- KiviDB nodes cluster only with KiviDB nodes.

**Performance**
- Many clients writing to **one key** are slower while something follows the
  write stream (a replica, the replication backlog, a backup): about 210,000
  writes a second against 1.1 million without, on 8 CPUs. Writes spread over
  many keys aren't affected.

[COMMANDS.md](../COMMANDS.md#not-yet-supported) lists the rest, down to
single options. If something you rely on behaves differently from Redis and
isn't listed here, please
[open an issue](https://github.com/kividbio/kividb/issues/new/choose).
