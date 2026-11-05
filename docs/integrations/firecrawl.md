# Firecrawl

Self-hosted Firecrawl works on KiviDB 1.2.0 as its Redis: scrape, map, batch scrape and crawl jobs, and
Firecrawl's own self-hosted test suite, with the same results as on Redis 8.

Use the **`-lua` image** (`kividb/kividb:v1.2.0-lua`, or `-full` for TLS as well). Firecrawl's BullMQ
queues, its team semaphore (`SCRIPT LOAD` and `EVALSHA`) and its Redlock locks run Lua scripts, so the
default image, which has no Lua, does not work.

**Checked** with Firecrawl `v2.11.0`, from source, as its CI's self-hosted job runs it (fetch engine,
Postgres for the NuQ queue, RabbitMQ, the local test site):

- Firecrawl's own suite (`pnpm harness pnpm test:snips`), five full runs on each server: 340 tests pass
  in every run on both servers, apart from one run on each server where the same batch scrape and crawl
  tests flake (they fail on Redis 8 the same way, and pass when run again). Tests that need an AI key, a
  search engine, Playwright, a proxy or Firecrawl's cloud services skip themselves (347).
- Scrape, map, batch scrape and crawl jobs against the test site, with the server restarted in the middle
  of a crawl: 7 of 7 checks pass on KiviDB and on Redis 8.10.3, and the crawl finishes with the same pages,
  none twice.
- The same jobs on Firecrawl's own `docker-compose.yaml` (images built from `v2.11.0`) with the override
  below, against the same stack with its own `redis` service: the same results on both. Map found no pages
  on either (an artefact of the Docker network alias the test site was served under), so the harness took
  the pages from a crawl; the other 6 checks pass on both.

Then again at `v2.11.162`, the tag Firecrawl's self-hosting guide pins, once on each server: the suite
passes 365 tests on KiviDB and 364 on Redis 8. One test fails on both because the test setup has no parse
upload secret (`PARSE_UPLOAD_REF_SECRET_MISSING`), and on Redis a map redirect test failed once. The jobs
with the restart pass 7 of 7 on both.

The harness and raw results are in [`benchmarks/firecrawl-compat`](../../benchmarks/firecrawl-compat/):
`v2.11.0` ([KiviDB](../../benchmarks/firecrawl-compat/results-kividb.json),
[Redis 8](../../benchmarks/firecrawl-compat/results-redis8.json)), `v2.11.162`
([KiviDB](../../benchmarks/firecrawl-compat/results-kividb-v2.11.162.json),
[Redis 8](../../benchmarks/firecrawl-compat/results-redis8-v2.11.162.json)).

## Run it with Firecrawl's docker compose

Firecrawl's `docker-compose.yaml` runs its own `redis` service, and every Firecrawl container reads
`REDIS_URL` (the rate limiter's `REDIS_RATE_LIMIT_URL` defaults to the same value). To use KiviDB instead,
add a compose override next to it, `docker-compose.kividb.yaml`:

```yaml
services:
  kividb:
    image: kividb/kividb:v1.2.0-lua
    command: ["--protected-mode", "no", "--aof"]
    volumes:
      - kividb-data:/home/kividb
    networks:
      - backend

volumes:
  kividb-data:
```

and point Firecrawl at it in `.env`:

```bash
REDIS_URL=redis://kividb:6380
```

```bash
docker compose -f docker-compose.yaml -f docker-compose.kividb.yaml up -d
```

The `redis` service still starts, because the `api` service depends on it, but nothing connects to it
(in the test it had no client but `redis-cli`).
`--protected-mode no` is the image's own command, which passing flags replaces; keep it or set
`--requirepass` and put the password in `REDIS_URL`. `--aof` keeps crawl state and queued jobs across a
KiviDB restart.

Running Firecrawl from source instead (`pnpm harness ...` in `apps/api`), set `REDIS_URL` and
`REDIS_RATE_LIMIT_URL` to `redis://127.0.0.1:6380`.

## What Firecrawl uses

- **BullMQ** (`5.56.7`) for its deep research, llms.txt, billing and precrawl queues, with blocking
  `BZPOPMIN`. BullMQ warns unless the eviction policy is `noeviction`; KiviDB reports `noeviction` by
  default, and no warning was logged. Scrape and crawl jobs themselves go through NuQ, Firecrawl's own
  queue on Postgres and RabbitMQ, not through Redis.
- Crawl state: sets, sorted sets and keys per crawl (`crawl:<id>:*`).
- A per-team concurrency limiter in sorted sets, and a semaphore as Lua scripts that call `TIME`.
- `rate-limiter-flexible` and Redlock.
- Not used: keyspace notifications, pub/sub, Redis Cluster or Sentinel.

## Known limits

- A full run of Firecrawl's suite sometimes fails a few batch scrape and crawl tests with "expected 0 to be
  greater than 0": once in five runs on KiviDB and once in five on Redis 8. They pass when run again, and
  the cause is in Firecrawl, not the server.
- In 1.2.0, on the io_uring I/O model, a client that closed a connection blocked in `BZPOPMIN` after
  sending more commands on it was not noticed until the wait timed out
  ([#384](https://github.com/kividbio/kividb/issues/384)); fixed in 1.2.1. It showed up with n8n's Bull
  workers; nothing in Firecrawl's tests failed because of it.
- Not tested: the Playwright engine, a proxy, AI features (they need a model key), the FoundationDB NuQ
  backend, and TLS.
