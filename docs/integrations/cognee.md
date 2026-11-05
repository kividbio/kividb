# cognee

cognee's Redis cache backend works on KiviDB. It holds the session cache (questions and answers per
session, feedback, agent traces, session context), usage logs, and the shared lock that serialises
access to a Ladybug (Kuzu) graph from several processes. Point `CACHE_HOST` and `CACHE_PORT` at KiviDB.

**Which image:** the default `kividb/kividb` image is enough for the session cache and usage logs. The
shared Ladybug lock (`SHARED_LADYBUG_LOCK=true`, or the older `SHARED_KUZU_LOCK`) needs
**`kividb/kividb:v1.2.0-lua`** (or `-full`). It is a redis-py `Lock`, and releasing one runs a Lua script
(`EVALSHA`).

**Checked**, with `cognee` `1.6.6`:
- cognee's own Redis tests: the usage-logger integration tests (5 of 5), and the session-manager tests
  pointed at a real server (15 of 15). The results are the same on KiviDB and Redis 8.
- cognee's concurrent-subprocess lock test passes on `-lua` as on Redis 8. It runs in cognee's keyless
  mode: two writers and two cognify processes share one Kuzu graph through the Redis lock.
- our harness: 21 of 21 on `-lua` and on Redis 8. On the default image, 19 of 21: the two lock-release
  checks fail, as expected without Lua.

The harness, its pins and the results are in [`benchmarks/cognee-compat/`](../../benchmarks/cognee-compat/).

## Install

```bash
docker run -d -p 6380:6380 kividb/kividb:v1.2.0-lua   # or kividb/kividb if you don't use the shared lock
pip install "cognee[redis]"
```

## Configure

In cognee's `.env`:

```bash
CACHING=true
CACHE_BACKEND=redis
CACHE_HOST=localhost
CACHE_PORT=6380              # KiviDB, not Redis's 6379
# Only with several processes on one Ladybug/Kuzu graph; needs the -lua image:
SHARED_LADYBUG_LOCK=true
# Optional:
USAGE_LOGGING=true
```

`CACHE_USERNAME`, `CACHE_PASSWORD` and `CACHE_SSL` work as with Redis. TLS needs the `-tls` or `-full`
image.

## What to know

- **Give cognee a KiviDB of its own.** `cognee.prune.prune_system()` empties the cache with `FLUSHDB`, and
  cognee has no setting for a database number or a key prefix, so it would empty anything else on the
  server too. This is the same on Redis.
- **Without Lua, the shared lock fails loudly, not silently.** On the default image the first release
  raises `ResponseError: this build of KiviDB has no Lua`. The lock key then stays until its timeout
  (`AGENTIC_LOCK_EXPIRE`, 240 s by default), and the other processes wait for it. Use the `-lua` image
  whenever `SHARED_LADYBUG_LOCK` is on.
- **Sessions expire after 7 days** (`SESSION_TTL_SECONDS`), using `EXPIRE` on each session's lists, as on
  Redis.
- **Not tested:** cognee's graph and vector stores (none of them uses Redis), Redis Cluster, and real
  models (the lock test runs cognee's local keyless models; the harness uses none).
