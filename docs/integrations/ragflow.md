# RAGFlow

RAGFlow's self-hosted stack runs on KiviDB in place of its Redis-protocol container: in RAGFlow 1.0 that
is the `kvrocks` service (Apache Kvrocks), and in RAGFlow 0.27 the `redis` service (Valkey). Swap the
service's image for **`kividb/kividb:v1.2.0-lua`** and keep everything else. Both need the `-lua` image.

- [RAGFlow 1.0](#ragflow-10): checked with `v1.0.0-rc3`.
- [RAGFlow 0.27](#ragflow-027): checked with `v0.27.3`.

The harnesses, how to run them, and the raw results of each run are in
[benchmarks/ragflow-compat](../../benchmarks/ragflow-compat/).

## RAGFlow 1.0

RAGFlow 1.0 is a Go server. Its Docker deployment (`docker/docker-compose.yml`, which includes
`docker-compose-base.yml`) has one Redis-protocol service, `kvrocks` (`apache/kvrocks:2.16.0`). The
message queue is NATS JetStream, a separate `nats` service that KiviDB does not replace. There is no
Valkey `redis` service any more: the Python services that used it are gone.

**Checked:** with RAGFlow `v1.0.0-rc3`'s own docker-compose, the end-to-end harness passes 31 of 31
checks on KiviDB, the same 31 that pass on Redis 8. On Kvrocks, RAGFlow's default, 29 pass and none
fail; the other two cannot be read on Kvrocks, which does not count refused commands. RAGFlow's own Go
HTTP API suite (`test/api`) gives 916 passed, 4 failed and 6 skipped on KiviDB, on Kvrocks and on
Redis 8, test for test; the 4 that fail do so on all three and do not touch the server. RAGFlow's two Go
integration tests that need a real Kvrocks (the connection at start-up, and the media cache's whole
lifecycle) pass on KiviDB as on Kvrocks.

### What RAGFlow keeps in the server

From `internal/engine/kvrocks`, `internal/agent/canvas` and their callers, at `v1.0.0-rc3`. Database `1`
(`SELECT 1` at connect), the password only (the `username` setting is not passed to the client), RESP3
(go-redis 9.18 sends `HELLO 3`).

- **The secret key that signs login tokens** (`ragflow:system:secret_key`, no expiry), unless one is
  configured. If it is lost, every user is logged out.
- **The agent-run lease** (`agent:active-session:<session>`, 30 seconds, refreshed while the run lasts) and
  cancel markers. Taking, refreshing, cancelling and releasing it are four Lua scripts (`EVAL`); a lease that
  has expired must be gone at once, so another run can take it.
- **Locks:** `SET NX PX`, released with a compare-and-delete Lua script. Taking a lock runs that script
  first. They guard connector syncs, and schema changes on the OceanBase and Vastbase engines.
- **The media cache:** images found while a document is parsed (inline images in HTML and EPUB, for
  example) are stored with a Lua script that also enforces the per-task image quota.
- **Webhooks:** a Lua token bucket rate-limits them, and their traces are updated with
  `WATCH` / `MULTI` / `EXEC`.
- **Caches:** chunks, agent checkpoints and logs, run state, counters (`SET` with expiry, hashes, sets).
- **Not used:** streams (the queue is NATS), pub/sub, keyspace notifications, `FT.*`, `JSON.*`, Cluster
  mode, Sentinel.

### Use the `-lua` image

On the default image (`kividb/kividb:v1.2.0`, no Lua), every script answers
`this build of KiviDB has no Lua`, and RAGFlow does not carry on:

- **Agents do not run.** Every agent completion answers `Internal storage error while accessing the
  agent`, because the run cannot take its lease. RAGFlow's own suite fails its four agent-completion
  tests there (911 passed, 9 failed, 6 skipped).
- **A document with images fails to parse:** the ingestion task fails at the parser
  (`media put: ERR this build of KiviDB has no Lua`). Plain text documents still parse, and chat
  assistants still answer.
- From RAGFlow's code, not exercised here: **locks cannot be taken** (taking one runs the
  compare-and-delete script first), so connector syncs and schema changes on OceanBase and Vastbase fail;
  and **webhooks are refused**, because their rate limit fails closed.

In the harness, 20 of 31 checks pass there. `kividb/kividb:v1.2.0-full` (Lua and TLS) works too.

### Run it

From RAGFlow's `docker` directory, save this as `docker-compose.override.yml`, and start RAGFlow with both
files:

```yaml
services:
  kvrocks:
    image: kividb/kividb:v1.2.0-lua
    # Kvrocks' entrypoint script and root user are for Kvrocks: KiviDB takes its settings as flags
    # and runs as its own user. --requirepass also turns off protected mode. --aof logs writes as
    # they happen; RAGFlow keeps the secret key that signs login tokens here.
    entrypoint: !reset null
    user: !reset null
    command: ["--port", "6379", "--requirepass", "${REDIS_PASSWORD}", "--aof", "--dir", "/home/kividb"]
    # KiviDB runs as uid 1000; a named volume on its home directory is writable for it.
    volumes: !override
      - kividb_data:/home/kividb
    # RAGFlow's check runs redis-cli inside the container, which KiviDB's image does not have.
    # No RAGFlow service waits for kvrocks to be healthy.
    healthcheck:
      disable: true

volumes:
  kividb_data:
```

```bash
docker compose -f docker-compose.yml -f docker-compose.override.yml up -d
```

`!reset` and `!override` need Docker Compose 2.24 or later. The service keeps the name `kvrocks` and the
port `6379`, so `KVROCKS_HOST`, `KVROCKS_PORT` and `REDIS_PASSWORD` stay as `docker/.env` sets them, and
the port is still published on the host as Kvrocks' is.

### Persistence and memory

Kvrocks stores everything on disk (RocksDB). KiviDB holds its data in memory: with `--aof` (as above) it
logs every write and syncs the log once a second, and it writes a snapshot on a clean shutdown. Keep
`--aof`: without it, a container that is killed (an out-of-memory kill, a host crash) loses what was
written since the last clean shutdown, the secret key among it. See [PERSISTENCE.md](../PERSISTENCE.md).

The harness restarts the server twice with parse tasks waiting: none are lost, the secret key is kept,
and the login token from before the restarts still works. Those are clean restarts.

The harness ran without a memory cap. Of what RAGFlow had left in the server at the end of it, every key
had an expiry except the secret key. If you cap memory (`"--maxmemory", "1gb"`), add
`"--maxmemory-policy", "volatile-lru"` with it: that policy only evicts keys that have an expiry, so the
secret key stays.

## RAGFlow 0.27

RAGFlow 0.27's Python services use its Valkey container for the task queue (a Redis stream per
priority, read by the task executors through a consumer group), the executors' heartbeats, the cache,
and RAGFlow's locks. Swap the image of RAGFlow's `redis` service for **`kividb/kividb:v1.2.0-lua`** and
keep everything else.

**Checked:** with RAGFlow `v0.27.3`'s own docker-compose, the end-to-end harness passes 20 of 20 checks on
KiviDB, the same 20 that pass on Redis 8 and on RAGFlow's default Valkey 8. The raw results are the
`results-*.json` files without `1.0` in their name.

### Use the `-lua` image

RAGFlow releases its distributed locks with a Lua script (`delete_if_equal`), and rate-limits agent API
calls with another (a token bucket). On the default image (`kividb/kividb:v1.2.0`, no Lua), documents are
still parsed, because RAGFlow logs the failed release and carries on: each lock is held until its timeout
(60 seconds for the progress updater) instead of being released, and the agent rate limit's script
cannot run (agents are not exercised here). In the harness, 19 of 20 checks pass there; the one that
fails counts the refused lock releases. Use `-lua` (or `-full`, Lua and TLS).

### Run it

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

### Persistence

KiviDB writes its snapshot on a clean shutdown (`docker compose stop` or `restart`), and does not take
snapshots on a schedule. The harness restarts the server twice with parse tasks waiting in the stream, and
none are lost, but those are clean shutdowns. If the container can be killed (an out-of-memory kill, a host
crash), add `"--aof"` to the command so writes are logged as they happen. See
[PERSISTENCE.md](../PERSISTENCE.md).

## Known limits

- **The stub model proves the wiring, not a model's results.** No cloud model is called: RAGFlow's
  OpenAI-API-compatible provider points at a local stub for the chat and embedding models, for the
  harnesses and for RAGFlow's Go suite.
- **RAGFlow 1.0's suite runs without its CI's models.** RAGFlow's CI gives it ZHIPU-AI and SiliconFlow keys
  and a TEI embedding service; here the stub is the tenant's default model instead, so the tests that check
  a real model's output are among the 4 that fail (on every server) or the 6 that skip.
- **RAGFlow 0.27's own HTTP API tests are not run:** their session set-up registers ZHIPU-AI and
  SiliconFlow models with live API keys. The harness makes the same calls (register, log in, datasets,
  upload, parse, chunks, retrieval, chat assistants, sessions, completions).
- **Not tested:** RAGFlow 1.0's webhooks and connectors (their scripts and the `WATCH` transaction are
  checked against the server directly, not through RAGFlow); the Infinity, OceanBase and other document
  engines (the harnesses use the default, Elasticsearch); GraphRAG, RAPTOR and knowledge compilation;
  RAGFlow 0.27's agent apps.
- **Scale is not tested:** a handful of documents, one ingestor or task executor, and no timing figures.
- **KiviDB's `INFO commandstats` leaves out `HELLO`** ([#388](https://github.com/kividbio/kividb/issues/388)),
  so the list of commands RAGFlow 1.0 sends at connect is shorter on KiviDB than on Redis. go-redis also
  asks for `CLIENT MAINT_NOTIFICATIONS`, which KiviDB, like Redis 8, refuses; go-redis then carries on
  without it.
