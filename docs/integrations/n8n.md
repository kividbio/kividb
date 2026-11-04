# n8n

n8n uses Redis only in **queue mode** (`EXECUTIONS_MODE=queue`): the main instance puts executions on a
Bull queue and workers run them. Queue mode works on KiviDB 1.2.0, with one known gap on the io_uring
I/O model (below). In n8n's default mode (`regular`) there is no Redis to replace.

Use the **`-lua` image** (`kividb/kividb:v1.2.0-lua`, or `-full` for TLS as well). Bull runs its queue
operations as Lua scripts, and n8n's own locks, leader election and instance registry use `EVAL`, so the
default image, which has no Lua, does not work.

**Checked** with n8n `2.46.6` (one main instance, two workers, Postgres): 1,000 executions through a
webhook-triggered workflow with retries and planned failures, with a worker restarted, the server
restarted while 150 jobs were waiting, and a worker killed. 35 of 35 checks pass on KiviDB and on
Redis 8.10.3; no execution was lost or ran twice. n8n's own Redis integration tests: 21 of 22 pass on
KiviDB, 22 of 22 on Redis 8. The harness and raw results are in
[`benchmarks/n8n-compat`](../../benchmarks/n8n-compat/) ([KiviDB](../../benchmarks/n8n-compat/results-kividb.json),
[Redis 8](../../benchmarks/n8n-compat/results-redis8.json)).

## Run it

KiviDB with its append-only file on, so queued executions survive a restart:

```bash
docker volume create kividb-data
docker run -d --name kividb -p 6380:6380 -v kividb-data:/home/kividb \
  kividb/kividb:v1.2.0-lua --protected-mode no --aof
```

The image's own command is `--protected-mode no`; passing flags replaces it, so keep it (or set
`--requirepass` and n8n's `QUEUE_BULL_REDIS_PASSWORD`). Use the image's working directory,
`/home/kividb`, for the volume: it belongs to the user KiviDB runs as.

Then point every n8n instance (main, workers, webhook processors) at it. These are the only Redis
settings that change:

```bash
EXECUTIONS_MODE=queue
QUEUE_BULL_REDIS_HOST=kividb      # or the host KiviDB runs on
QUEUE_BULL_REDIS_PORT=6380        # KiviDB's default port, not Redis's 6379
```

Everything else is as in n8n's queue mode guide: the same `N8N_ENCRYPTION_KEY` on every instance, a
Postgres database, and workers started with `n8n worker`. With `QUEUE_HEALTH_CHECK_ACTIVE=true`, each
worker's `/healthz/readiness` reports its connection, and it answers 200 on KiviDB as on Redis.

n8n gives up if the server is unreachable for more than 10 s (`QUEUE_BULL_REDIS_TIMEOUT_THRESHOLD`). A
KiviDB restart took about a second in the test, and n8n carried on without losing the waiting jobs.

## Known gaps

- **A worker that pauses or shuts down leaves a blocked connection behind, on io_uring**
  ([#384](https://github.com/kividbio/kividb/issues/384)). When a Bull worker stops, it sends `QUIT` on its
  blocking connection and then closes it. KiviDB on the io_uring I/O model (what it uses on Linux) does not
  notice that close while the connection waits in `BRPOPLPUSH`, so the worker's shutdown takes about 2 s
  longer, and the server keeps the wait open until it times out. If a job arrives meanwhile, that wait can
  take it; n8n's stall check (every 30 s) then puts it back, so it runs late but is not lost. n8n's test
  "should stop waiting at once when the worker pauses" fails because of this. In the end-to-end run no
  execution was lost, and a killed worker cost the same 10 executions as on Redis, but the batch took 109 s
  to drain instead of 84 s. Workaround until it is fixed: run KiviDB with `--io-model tokio`, which does
  not have the problem (all 22 of n8n's tests pass with it), at some cost in throughput.
- In RESP2, `BRPOPLPUSH` and `BLMOVE` time out with `$-1` where Redis sends `*-1`
  ([#385](https://github.com/kividbio/kividb/issues/385)). ioredis, and so n8n, reads both as `null`.

## Not tested

- Redis Cluster (`QUEUE_BULL_REDIS_CLUSTER_NODES`). n8n supports it as an option; it does not need it.
  n8n has no Sentinel support.
- TLS (`QUEUE_BULL_REDIS_TLS`, which would need the `-full` image), multi-main (a licensed feature), and
  separate webhook processors.
