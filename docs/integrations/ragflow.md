# RAGFlow

RAGFlow's self-hosted stack runs on KiviDB in place of its Valkey container. That covers the task queue
(a Redis stream per priority, read by the task executors through a consumer group), the executors'
heartbeats, the cache, and RAGFlow's locks. Swap the image of RAGFlow's `redis` service for
**`kividb/kividb:v1.2.0-lua`** and keep everything else.

**Checked:** with RAGFlow `v0.27.3`'s own docker-compose, the end-to-end harness passes 20 of 20 checks on
KiviDB, the same 20 that pass on Redis 8 and on RAGFlow's default Valkey 8. The
harness, how to run it, and the raw results of each run (`results-*.json`, one line per check) are in
[benchmarks/ragflow-compat](../../benchmarks/ragflow-compat/).

## Use the `-lua` image

RAGFlow releases its distributed locks with a Lua script (`delete_if_equal`), and rate-limits agent API
calls with another (a token bucket). On the default image (`kividb/kividb:v1.2.0`, no Lua), documents are
still parsed, because RAGFlow logs the failed release and carries on: each lock is held until its timeout
(60 seconds for the progress updater) instead of being released, and the agent rate limit's script
cannot run (agents are not exercised here). In the harness, 19 of 20 checks pass there; the one that
fails counts the refused lock releases. Use `-lua` (or `-full`, Lua and TLS).

## Run it

From RAGFlow's `docker` directory, save this as `docker-compose.override.yml`, and start RAGFlow with both
files:

```yaml
services:
  redis:
    image: kividb/kividb:v1.2.0-lua
    # The settings RAGFlow gives Valkey, as KiviDB flags. --requirepass also turns off protected mode.
    command: ["--port", "6379", "--requirepass", "${REDIS_PASSWORD}", "--maxmemory", "1gb",
              "--maxmemory-policy", "volatile-lru", "--dir", "/home/kividb"]
    # KiviDB runs as uid 1000; a named volume on its home directory is writable for it.
    volumes: !override
      - kividb_data:/home/kividb
    # RAGFlow's check runs redis-cli inside the container, which KiviDB's image does not have.
    # No RAGFlow service waits for redis to be healthy.
    healthcheck:
      disable: true

volumes:
  kividb_data:
```

```bash
docker compose -f docker-compose.yml -f docker-compose.override.yml up -d
```

`!override` needs Docker Compose 2.24 or later. The service keeps the name `redis` and the port `6379`, so
`REDIS_HOST`, `REDIS_PORT` and `REDIS_PASSWORD` stay as `docker/.env` sets them.

## Persistence

KiviDB writes its snapshot on a clean shutdown (`docker compose stop` or `restart`), and does not take
snapshots on a schedule. The harness restarts the server twice with parse tasks waiting in the stream, and
none are lost, but those are clean shutdowns. If the container can be killed (an out-of-memory kill, a host
crash), add `"--aof"` to the command so writes are logged as they happen. See
[PERSISTENCE.md](../PERSISTENCE.md).

## Known limits

- **RAGFlow's own HTTP API tests are not run:** their session set-up registers ZHIPU-AI and SiliconFlow
  models with live API keys. The harness makes the same calls (register, log in, datasets, upload, parse,
  chunks, retrieval, chat assistants, sessions, completions), with RAGFlow's OpenAI-API-compatible provider
  pointed at a local stub for the chat and embedding models. It proves the wiring, not a model's results.
- **Not tested:** the Go services (`ragflow-go` profile), which use their own Kvrocks container; the
  Infinity and OceanBase document engines (the harness uses the default, Elasticsearch); GraphRAG, RAPTOR
  and agent apps.
- **Scale is not tested:** a handful of documents, one task executor, and no timing figures.
