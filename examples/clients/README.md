# Client examples

KiviDB speaks the Redis protocol, so these are plain Redis clients pointed at
KiviDB. No KiviDB-specific library or driver is involved. Each example checks
its own results and exits with an error if anything is off, so they double as
a smoke test.

| Directory | Client | What it shows |
|---|---|---|
| [`python/`](python/) | [redis-py](https://github.com/redis/redis-py) | `quickstart.py`: strings, expiry, counters, hashes, a pipeline, `WATCH`/`MULTI`/`EXEC`. `vector_search.py`: built-in vector and full-text search (`FT.CREATE`, KNN filtered by tag). |
| [`node/`](node/) | [ioredis](https://github.com/redis/ioredis), [BullMQ](https://bullmq.io) | `quickstart.mjs`: the basics plus Pub/Sub. `bullmq.mjs`: a job queue and worker (**needs a build with Lua**). |
| [`go/`](go/) | [go-redis](https://github.com/redis/go-redis) v9 | Strings, expiry, hashes, a pipeline, a transaction. |
| [`java/`](java/) | [Jedis](https://github.com/redis/jedis) | Strings, expiry, hashes, a pipeline, a transaction. |

## Start KiviDB

```bash
docker run -d --name kividb -p 6380:6380 kividb/kividb:v1.1.2-lua
```

The `-lua` image is needed only for the BullMQ example. Job queues such as
BullMQ and Sidekiq run Lua scripts, and the default build has no Lua. The rest
run on any variant (`kividb/kividb:latest`).

KiviDB listens on **6380**. Every example reads `KIVIDB_URL`, which defaults
to `redis://localhost:6380/0`. With a password:
`KIVIDB_URL=redis://:secret@localhost:6380/0`.

## Run them

One at a time, from each directory:

```bash
cd python && pip install -r requirements.txt && python quickstart.py && python vector_search.py
cd node   && npm install && node quickstart.mjs && node bullmq.mjs
cd go     && go run .
cd java   && mvn -q compile exec:java
```

Or all at once: `./run-all.sh`. Leave some out with
`SKIP=java,bullmq ./run-all.sh`.

The examples write keys such as `greeting`, `user:1` and `item:*`, and the
vector example drops and recreates an index called `docs`. Point them at a
scratch server, not one with data you care about.

For moving an existing Redis application across, see
[docs/MIGRATING_FROM_REDIS.md](../../docs/MIGRATING_FROM_REDIS.md).
