# Celery

Celery works on KiviDB as its broker (kombu's Redis transport) and as its result backend. It needs the
`-lua` image, and on 1.2.0 only, the result backend on RESP2:

1. **Use the `-lua` image** (`kividb/kividb:v1.2.0-lua`). kombu's Redis transport runs Lua scripts
   (`EVALSHA`) for its queue bindings and its unacked-message lock, so a worker can't even start on the
   default image. It exits with `Unrecoverable error: ResponseError` ("this build of KiviDB has no Lua").
   Celery 5.7's chords also count their results with a Lua script.
2. **On 1.2.0 only, put the result backend on RESP2**: add `?protocol=2` to the result backend URL.
   KiviDB 1.2.0 sends a Lua script's `false` to RESP3 clients in a form redis-py 8 rejects
   ([#431](https://github.com/kividbio/kividb/issues/431)). redis-py 8 uses RESP3 by default, so without
   this, every **chord** fails (`ChordError: Join error: TypeError("int() argument must be … not
   'ResponseError'")`) and never completes. Everything else works without it. This is fixed in 1.2.1:
   from 1.2.1 on, leave the URL as it is.

**Checked**, with Celery `5.7.0` and kombu `5.7.0` (redis-py 8.1.0), against Redis 8.10.2 as the reference:

| | Redis 8.10.2 | KiviDB 1.2.0-lua, backend `?protocol=2` | KiviDB 1.2.0-lua, as is | KiviDB 1.2.0 (no Lua) |
|---|---|---|---|---|
| Celery's own integration suite (`t/integration`, tox's `integration-redis` setup without `-x`; 409 tests) | 387 passed, 7 failed | **387 passed, the same 7 failed** | stops at the first chord test (#431); stopped after 50 passes | not run: no worker starts |
| kombu's own Redis integration suite (`pytest -E redis t/integration`; 247 tests) | 129 passed | **129 passed** | 129 passed | 99 passed, 30 failed: no Lua |
| Our harness with a real worker (13 checks) | 13 of 13 | **13 of 13** | 12 of 13: the chord fails (#431) | 0 of 13: the worker exits |

The seven Celery tests that fail on Redis 8 fail the same way on KiviDB: two pub/sub churn tests in
`test_redis_backend.py`, `test_serialization`, two `test_trace_log_arguments` tests and two `test_worker`
tests. Since they fail on Redis too, we did not look into them further. The 117 skipped kombu tests are for
its other transports. Our harness runs a prefork worker through:
- a round trip, a group, a chain and a chord;
- countdown, retry, revoke, routing to a second queue, `inspect ping`, and result expiry;
- an `acks_late` task whose worker is killed with SIGKILL, which comes back to the next worker;
- 20 tasks queued across a server restart, all of which run;
- a running worker that reconnects after a server restart.

The harness, the commands, the pins and the JUnit files are in
[`benchmarks/celery-compat/`](../../benchmarks/celery-compat/).

## Run KiviDB

```bash
docker run -d -p 6380:6380 kividb/kividb:v1.2.0-lua --protected-mode no --aof
```

`--aof` keeps queued tasks across a restart. (Passing your own flags replaces the image's default
`--protected-mode no`, so add it back, or set `--requirepass`.)

## Configure

```python
from celery import Celery

app = Celery(
    "tasks",
    broker="redis://localhost:6380/0",
    backend="redis://localhost:6380/0?protocol=2",   # 1.2.0 only: RESP2 for the result backend, see #431
)
```

Or in settings: `broker_url = "redis://localhost:6380/0"` and
`result_backend = "redis://localhost:6380/0?protocol=2"`.

On 1.2.1 and later, drop `?protocol=2`: #431 is fixed there, and the backend works on RESP3, redis-py 8's
default. The broker can stay on RESP3 on 1.2.0 too: kombu's scripts don't return `false`.

## What to know

- **Visibility timeout.** As on Redis, an unacknowledged task (`acks_late`) whose worker died comes back
  after `visibility_timeout` (one hour by default), at kombu's next restore sweep. The harness lowers both
  (`visibility_timeout=8`, `unacked_restore_interval=1`, `unacked_restore_throttle=1`) to see it happen
  within seconds.
- **Not tested:** Celery beat (it keeps its schedule locally by default), Redis Sentinel and Cluster
  brokers, and scale. The worker runs the prefork pool with two processes.
