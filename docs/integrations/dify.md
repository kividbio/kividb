# Dify

Dify's self-hosted stack runs on KiviDB in place of its Redis container. That covers the cache, the
Celery broker and result backend, the pub/sub that carries streamed workflow and chat events back from
the workers, Dify's locks and rate limits, and the plugin daemon. Swap the image of Dify's `redis`
service for **`kividb/kividb:v1.2.0-lua`** and keep everything else.

**Checked:** with Dify `1.19.0`'s own docker-compose, the end-to-end harness passes 33 of 33 checks on
KiviDB, the same 33 that pass on Redis 8 (23 of 33 on the default image, without Lua). Dify's own Redis
tests pass 42 of 43 on KiviDB, against 43 on Redis 8; the one that does not is the cluster-mode test, which stops on
two start-up differences filed as [#390](https://github.com/kividbio/kividb/issues/390) and
[#391](https://github.com/kividbio/kividb/issues/391). The harness, how to run it, and the raw results of
each run (`results-*.json`, one line per check) are in
[benchmarks/dify-compat](../../benchmarks/dify-compat/).

## Use the `-lua` image

The default image (`kividb/kividb:v1.2.0`) has no Lua, and Dify does not work without it:

- **The Celery worker exits from time to time.** kombu's Redis transport (`kombu` 5.6) takes a redis-py
  `Lock` when it restores unacknowledged messages (on every tenth poll), and redis-py releases a lock with a
  Lua script (`EVALSHA`). The release fails, and the worker exits with `Unrecoverable error: ResponseError`
  (in one of the runs, during start-up); Docker restarts it. Any Celery app on a Redis broker needs Lua for
  the same reason.
- **Dify's own locks fail on release.** Dify takes redis-py locks around publishing a workflow, adding a
  document to a knowledge base and building the keyword index, among about sixty places, so those requests
  answer 500 and the lock stays until it expires.
- **Dify runs Lua scripts of its own** (`EVAL`), for its rate limiters, counters, login and password-reset
  codes, and workflow collaboration.

`kividb/kividb:v1.2.0-full` (Lua and TLS) works too.

## Run it

From Dify's `docker` directory, after `cp .env.example .env` as in Dify's quick start, save this as
`docker-compose.override.yaml` (Compose reads it next to `docker-compose.yaml`):

```yaml
services:
  redis:
    image: kividb/kividb:v1.2.0-lua
    # KiviDB's image has no shell and no redis-server: pass KiviDB's flags. --requirepass keeps
    # Dify's password, and also turns off protected mode.
    command: ["--port", "6379", "--requirepass", "${REDIS_PASSWORD:-difyai123456}", "--dir", "/data"]
    volumes:
      - ./volumes/kividb/data:/data
    # Dify's check runs redis-cli inside the container, which KiviDB's image does not have.
    # Every Dify service waits only for redis to have started, not for it to be healthy.
    healthcheck:
      disable: true
```

```bash
mkdir -p volumes/kividb/data && sudo chown 1000:1000 volumes/kividb/data   # KiviDB runs as uid 1000
docker compose up -d
```

The service keeps the name `redis` and the port `6379`, so `REDIS_HOST`, `REDIS_PORT`,
`CELERY_BROKER_URL` and the password stay as Dify sets them.

## Persistence

KiviDB writes its snapshot on a clean shutdown (`docker compose stop` or `restart`), and does not take
snapshots on a schedule: it accepts `save` settings with a warning that they have no effect. The harness
restarts the server with Celery tasks still queued, and none are lost, but that is a clean shutdown. If the
container can be killed (an out-of-memory kill, a host crash), add `"--aof"` to the command so writes are
logged as they happen. See [PERSISTENCE.md](../PERSISTENCE.md).

## Known limits

- **Not tested:** Redis Sentinel (`REDIS_USE_SENTINEL`), Redis Cluster (`REDIS_USE_CLUSTERS`), TLS
  (`REDIS_USE_SSL`), and the opt-in KnowledgeFS services. The agent backend and the collaboration
  websocket server start and connect, but no agent app or live collaboration session is run.
- **Cluster mode:** Dify's own cluster-mode test passes Redis-style directives such as
  `--appendonly no`, now accepted on KiviDB's command line. It also sets `cluster-announce-ip`
  with `CONFIG SET`, which KiviDB allows only at start-up
  ([#391](https://github.com/kividbio/kividb/issues/391)).
- **Models:** the harness points Dify's OpenAI-API-compatible plugin at a local stub for the chat and
  embedding models. It proves the wiring, not a model's results.
- **Scale is not tested:** a handful of apps, runs and documents, and no timing figures.
