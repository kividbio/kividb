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
  write, and a power failure up to a second, as with Redis's `everysec`.
  Before 1.2 a write was acknowledged once its record was queued, and a
  hard kill could lose the last few; `aof-ack-after-write no` still does
  that, for more writes a second.

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
- A replica expires keys by its own clock, and doesn't enforce `maxmemory`
  on what its master sends.

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
