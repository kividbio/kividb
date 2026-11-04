# claude-mem

claude-mem's server runtime (Claude-Mem Server, the Docker deployment) keeps its generation job queue in
BullMQ, on Redis or Valkey. KiviDB works as that server: jobs are enqueued, processed, retried, failed,
cancelled and kept across restarts the same way they are on Redis 8 and on the Valkey claude-mem ships with.
Use the **`-lua` image** (`kividb/kividb:v1.2.0-lua`): BullMQ runs its queue operations as Lua scripts, and
the default KiviDB image has no Lua.

**Checked:** with claude-mem `v15.9.0` (BullMQ `5.81.5`, ioredis `5.11.1`), 18 of 18 queue checks pass on
KiviDB 1.2.0, the same 18 that pass on Redis 8.10.3 and on Valkey 8, and claude-mem's own Docker E2E
(`scripts/e2e-server-docker.sh`) passes on all three. The harness, its pins and the raw results are in
[`benchmarks/claude-mem-compat/`](../../benchmarks/claude-mem-compat/) (results in
[`results-kividb.json`](../../benchmarks/claude-mem-compat/results-kividb.json),
[`results-redis8.json`](../../benchmarks/claude-mem-compat/results-redis8.json) and
[`results-valkey.json`](../../benchmarks/claude-mem-compat/results-valkey.json)).

## When claude-mem uses Redis

Only when the queue engine is BullMQ. The plugin's local worker, which is what most people run, queues in
SQLite (`CLAUDE_MEM_QUEUE_ENGINE=sqlite`, the default) and needs no Redis. The server runtime in Docker
requires `CLAUDE_MEM_QUEUE_ENGINE=bullmq` and a `CLAUDE_MEM_REDIS_URL`; its `docker-compose.yml` starts a
Valkey for it.

## Use KiviDB in claude-mem's Docker stack

Save this next to claude-mem's `docker-compose.yml` as `docker-compose.kividb.yml`. It replaces the image of
the `valkey` service, so `CLAUDE_MEM_REDIS_URL` (`redis://valkey:6379`) stays as it is:

```yaml
services:
  valkey:
    image: kividb/kividb:v1.2.0-lua
    # What claude-mem asks of Valkey: append-only file on (fsync every second) and noeviction.
    command: ["--port", "6379", "--protected-mode", "no", "--aof", "--maxmemory-policy", "noeviction"]
    volumes:
      - kividb-data:/home/kividb   # KiviDB's working directory, owned by its user (uid 1000)
    healthcheck:
      test: ["CMD", "kividb", "--version"]   # the image has no shell and no CLI to PING with
  claude-mem-server:
    environment:
      CLAUDE_MEM_QUEUE_REDIS_PREFIX: claude_mem
  claude-mem-worker:
    environment:
      CLAUDE_MEM_QUEUE_REDIS_PREFIX: claude_mem

volumes:
  kividb-data:
```

```bash
docker compose -f docker-compose.yml -f docker-compose.kividb.yml up -d --build
```

The healthcheck only proves the binary runs. The server and the worker connect with ioredis, which retries
until KiviDB accepts connections (it starts in well under a second).

### Set the queue prefix on both services

The two `CLAUDE_MEM_QUEUE_REDIS_PREFIX` lines are needed with any server, Valkey included. In claude-mem
`v15.9.0`, the prefix defaults to `claude_mem_<CLAUDE_MEM_WORKER_PORT>`. The compose file sets that port on
the server service (37877) but not on the worker service, which falls back to `claude_mem_37700`. So the
worker listens on a different queue from the one the server fills, and no job is ever generated. claude-mem's
own E2E does not notice, because it runs without a provider and only checks that jobs are queued. Setting the
same prefix on both services fixes it.

## What was checked

On each server, with a stub provider in place of the model (claude-mem's `CLAUDE_MEM_SERVER_PROVIDER=custom`),
so nothing is billed:

- an event's job is enqueued, processed and stores one observation, and 20 at once each run exactly once;
- a rate-limited job waits in BullMQ's delayed set for its backoff and completes on its second attempt;
- a job rate-limited on every attempt ends failed after 3, in Postgres and in BullMQ's failed set;
- a non-retryable error fails the job at once; an operator retry (`POST /v1/jobs/:id/retry`) re-enqueues it
  and it completes; a cancelled job (`POST /v1/jobs/:id/cancel`) leaves BullMQ and never runs;
- with the worker stopped, 10 jobs wait; they are still there after the server restarts (append-only file
  on), and run when the worker comes back. A retry's backoff (a delayed job) survives a restart too;
- claude-mem's server and worker restart without losing job state; two workers share 12 jobs, each run once;
- the session-summary queue works, and nothing is left active or stalled at the end.

The BullMQ commands this sends include `EVALSHA` (its Lua scripts), streams (`XADD`, `XREAD`) for queue
events, `BZPOPMIN`, sorted sets, lists and hashes.

## Known limits

- Use `-lua` or `-full`. The default image answers BullMQ's scripts with `ERR this build of KiviDB has no
  Lua: EVAL, SCRIPT, FUNCTION and FCALL need the lua or full variant`.
- Not tested: a model provider (the stub stands in), claude-mem's local SQLite queue (no Redis involved),
  Redis Cluster, and scale (a few dozen jobs per run, no timing).
