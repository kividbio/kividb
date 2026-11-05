# OmniRoute

OmniRoute's Redis features work on KiviDB: the per-API-key rate limiter, the API-key auth cache, the quota
store, the warmup circuit breaker and the Redis backend of the semantic cache. Set `REDIS_URL` to KiviDB.

**Which image: `kividb/kividb:v1.2.0-lua`** (or `-full`). The rate limiter is one Lua script (`EVAL`).
On the default image, which has no Lua, OmniRoute does not fail. It logs
`[RATE_LIMITER] Redis eval failed, falling back to in-memory rate limiting` and keeps a separate count in
each process. Every replica then lets through the full limit by itself. The other four features use
plain commands and work on the default image.

**Checked** with OmniRoute `3.8.52` (ioredis `5.11.1`). The harness runs OmniRoute's own modules,
including a `/v1/chat/completions` request through its chat handler to a stub upstream:
- `-lua` image: 18 of 19 checks pass, and 1 is a WARN. Redis 8: 19 of 19.
- the WARN: `INCRBYFLOAT` stores `0.1 + 0.2` as `0.30000000000000004` where Redis stores `0.3`
  ([#387](https://github.com/kividbio/kividb/issues/387)). OmniRoute reads the value with `parseFloat`, so
  its quota totals are the same.
- default image: 13 of 19 pass, 1 WARN and 5 FAIL, all five from the in-memory fallback above.

The harness, its pins and the results are in
[`benchmarks/omniroute-compat/`](../../benchmarks/omniroute-compat/).

## Run

```bash
docker run -d --name kividb -p 6380:6380 kividb/kividb:v1.2.0-lua
```

In OmniRoute's `.env`, or the container's environment:

```bash
REDIS_URL=redis://localhost:6380      # KiviDB, not Redis's 6379
# REDIS_KEY_PREFIX=omniroute:         # the default; namespaces every key
# Optional, each off by default:
QUOTA_STORE_DRIVER=redis
QUOTA_STORE_REDIS_URL=redis://localhost:6380
OMNIROUTE_SEMANTIC_CACHE_ENABLED=true
OMNIROUTE_SEMANTIC_CACHE_BACKEND=redis
```

With OmniRoute's `docker-compose.yml`, set `REDIS_URL=redis://kividb:6380` and run KiviDB beside it as a
service named `kividb`, with the `kividb/kividb:v1.2.0-lua` image.

## What to know

- **Use the `-lua` image.** On the default image, the rate limits still return 429, but per process.
  Nothing on the server shows that, except OmniRoute's own log line. With the `-lua` image, a second
  OmniRoute process sharing KiviDB is refused once the limit is reached, as on Redis.
- **No search module is needed.** The semantic cache's Redis backend stores JSON strings and sets, and
  compares embeddings in OmniRoute itself. It sends no `FT.*` command.
- **Not tested:** Redis Sentinel or Cluster (OmniRoute supports only a single node), and TLS. A
  `rediss://` URL needs the `-tls` or `-full` image.
