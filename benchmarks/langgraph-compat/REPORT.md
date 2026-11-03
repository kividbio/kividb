# LangGraph compatibility report

`langgraph-checkpoint-redis` run against KiviDB and against Redis 8, with the same harness
([`langgraph_kividb_compat.py`](langgraph_kividb_compat.py)) and no API keys: a stub plays the embedding
model. The checkpointer is exercised through a real graph, not only through its own methods.

**23 of 23 checks pass on KiviDB, and 23 of 23 on Redis 8.10.3.**
No check differs between the two.

| | |
|---|---|
| KiviDB | `8.10.0` as the server reports it, built from `master` at `a9895a8` |
| Reference | Redis `8.10.3` (the `redis:8` image) |
| `langgraph-checkpoint-redis` | `0.6.0` |
| `langgraph` | `1.2.19` |
| Other pins | see [`requirements.txt`](requirements.txt) |
| Python | `3.12` |
| Run | 2026-11-03; KiviDB started with only `--bind 127.0.0.1 --port 6380` |

## Results

### Connection

| Check | KiviDB | Redis 8 |
|---|---|---|
| PING | PASS | PASS |

### RedisSaver (the checkpointer)

| Check | KiviDB | Redis 8 |
|---|---|---|
| setup creates its indexes | PASS | PASS |
| state persists across invocations of a thread | PASS | PASS |
| get_state returns the latest values | PASS | PASS |
| history lists checkpoints, newest first | PASS | PASS |
| another thread starts empty | PASS | PASS |
| update_state writes a new checkpoint | PASS | PASS |
| interrupt and resume (pending writes) | PASS | PASS |
| list filters on metadata and honours limit | PASS | PASS |
| delete_thread | PASS | PASS |

### ShallowRedisSaver

| Check | KiviDB | Redis 8 |
|---|---|---|
| keeps only the latest checkpoint | PASS | PASS |

### AsyncRedisSaver

| Check | KiviDB | Redis 8 |
|---|---|---|
| ainvoke persists, aget_state, adelete_thread | PASS | PASS |

### RedisStore

| Check | KiviDB | Redis 8 |
|---|---|---|
| put and get | PASS | PASS |
| a missing key is None | PASS | PASS |
| put again replaces the value | PASS | PASS |
| search by namespace prefix | PASS | PASS |
| search with a filter | PASS | PASS |
| list_namespaces | PASS | PASS |
| delete | PASS | PASS |
| put with a ttl sets an expiry | PASS | PASS |

### RedisStore with a vector index

| Check | KiviDB | Redis 8 |
|---|---|---|
| semantic search finds the closest note | PASS | PASS |
| semantic search with a filter | PASS | PASS |
| limit is honoured | PASS | PASS |

Raw results: [`results-kividb.json`](results-kividb.json),
[`results-redis8.json`](results-redis8.json).

## Commands `langgraph-checkpoint-redis` sent

Counted from `INFO commandstats` on KiviDB after one run (the call counts are for that run):
`JSON.SET`, `JSON.GET`, `JSON.MERGE`, `FT.CREATE`, `FT.SEARCH`, `FT.INFO`, `FT.DROPINDEX`, `FT._LIST`,
`ZADD`, `ZRANGE`, `ZCARD`, `MULTI`, `EXEC`, `GET`, `SET`, `EXISTS`, `EXPIRE`, `TTL`, `SCAN` and `DEL`.
This library relies on `JSON.*` together with `FT.*` more than the LangChain one does.

## What this does and does not show

- It shows the calls above work on KiviDB as they do on Redis 8: a graph's state persisting across
  invocations of a thread, the history, `update_state`, interrupt and resume, filters and limits in
  `list()`, `delete_thread`, the shallow and the async checkpointer, and the store with a TTL and with a
  vector index.
- The embedding is a hashed bag of words, so it proves the wiring (index creation, search, filters),
  **not the quality of a real model's results**.
- **Scale is not tested.** The harness uses a few threads and a handful of checkpoints, not many
  threads, long histories or large states. It does not wait for a TTL to run out, only checks that an
  expiry is set.
- **Not covered:** `AsyncRedisStore`, `prune`, the `ttl` option of the checkpointer, `with_allowlist`,
  Redis Cluster, and LlamaIndex and Mem0.

## Left out on purpose

`RedisSaver.copy_thread` was dropped because it raises `NotImplementedError` inside the library, on
Redis 8.10.3 as well, so it says nothing about a server.

## Can the harness fail?

Yes, and it says why. [`../langchain-compat/selftest_fake_server.py`](../langchain-compat/selftest_fake_server.py)
is a server that answers `PING`, `INFO`, `SCAN` and `HELLO` and refuses every other command. The harness
against it reports 1 PASS (`PING`) and 13 FAIL, each with the error the server gave (`unknown command
'FT.INFO'`, `'FT.SEARCH'`, `'GET'`), and does not crash. A group whose setup fails is one FAIL line, and
the next group still runs.

## Reproduce

```bash
pip install -r requirements.txt

# KiviDB
python langgraph_kividb_compat.py --url redis://127.0.0.1:6380 --label kividb

# Reference
docker run -d --rm --name redis-ref -p 6399:6379 redis:8
python langgraph_kividb_compat.py --url redis://127.0.0.1:6399 --label redis8
```

Run it on a throwaway machine, not a laptop: it creates and drops indexes with random names. It removes
everything it created, including the indexes the library makes (found by comparing `FT._LIST` before and
after), and leaves nothing behind.
