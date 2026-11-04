# Mem0 compatibility report

Mem0's Redis vector store run against KiviDB and against Redis 8, with the same harness
([`mem0_kividb_compat.py`](mem0_kividb_compat.py)) and no API keys: a stub plays the embedding model and
no language model is called. The store is used directly (`RedisDB`) and through Mem0's public API
(`Memory`), not only through its own methods.

**24 of 24 checks pass on KiviDB, and 24 of 24 on Redis 8.10.3.**
No check differs between the two.

| | |
|---|---|
| KiviDB | `8.10.0` as the server reports it, built from `master` at `2ed7df2` (1.2.0 plus two unrelated fixes) |
| Reference | Redis `8.10.3` (the `redis:8` image) |
| `mem0ai` | `2.2.1` |
| `redisvl` / `redis-py` | `0.26.0` / `5.3.1`, what `pip install "mem0ai[vector-stores]"` resolves to (see below) |
| Other pins | see [`requirements.txt`](requirements.txt) |
| Python | `3.12` |
| Run | 2026-11-04; KiviDB started with only `--bind 127.0.0.1 --port 6380`; Mem0's telemetry off (`MEM0_TELEMETRY=False`) |

## Results

### Connection

| Check | KiviDB | Redis 8 |
|---|---|---|
| PING | PASS | PASS |

### RedisDB, the vector store

| Check | KiviDB | Redis 8 |
|---|---|---|
| insert creates the index and stores the 3 memories | PASS | PASS |
| search returns the closest memory first, scores ordered | PASS | PASS |
| top_k is honoured | PASS | PASS |
| filter on user_id | PASS | PASS |
| filter on user_id and agent_id together | PASS | PASS |
| filter on run_id | PASS | PASS |
| the payload comes back whole | PASS | PASS |
| get by id | PASS | PASS |
| update changes the text and the vector | PASS | PASS |
| list, with a filter and with top_k | PASS | PASS |
| keyword (BM25) search | PASS | PASS |
| delete | PASS | PASS |
| list_cols and col_info | PASS | PASS |
| reset empties the collection | PASS | PASS |

### Memory, the public API

| Check | KiviDB | Redis 8 |
|---|---|---|
| add (infer=False) stores a memory for a user | PASS | PASS |
| search is scoped to the user and finds the closest | PASS | PASS |
| search does not return another user's memory (bob's own search finds it) | PASS | PASS |
| get_all for a user | PASS | PASS |
| get by id | PASS | PASS |
| update | PASS | PASS |
| history records the ADD and the UPDATE (Mem0's local SQLite file, not the server) | PASS | PASS |
| delete | PASS | PASS |
| delete_all removes one user's memories only | PASS | PASS |

Raw results: [`results-kividb.json`](results-kividb.json),
[`results-redis8.json`](results-redis8.json).

## Commands Mem0 sent

Counted from `INFO commandstats` on KiviDB after one run: `FT.CREATE`, `FT.INFO`, `FT.SEARCH`,
`FT.DROPINDEX`, `FT._LIST`, `HSET`, `HGETALL`, `UNLINK`, `SCAN`, `INFO`, `PING` and `CLIENT SETINFO`.
A few of these (`PING`, `INFO`, `FT._LIST`, `SCAN`, `FT.DROPINDEX`) are also the harness's own set-up and
clean-up. Mem0 keeps its memories in hashes, and the history in a local SQLite file, not
on the server.

## Which `redisvl`

Mem0's `RedisDB` cannot be created with `redisvl` 0.28 or newer, because it calls
`SearchIndex.set_client()`, which those versions removed
([mem0#7819](https://github.com/mem0ai/mem0/issues/7819)). The results here use the versions that
`pip install "mem0ai[vector-stores]"` resolves to (`redisvl` 0.26.0, with `redis-py` 5.3.1), which is why
`requirements.txt` pins them. This says nothing about a server: the failure happens before any command is
sent, on Redis 8 as well.

## What this does and does not show

- It shows the calls above work on KiviDB as they do on Redis 8: inserting memories with a vector, vector
  search with filters on user, agent and run, BM25 keyword search, get, update, list, delete and reset, and,
  through `Memory`, adding a memory for a user, searching only that user's memories, updating, deleting, the
  history, and deleting one user's memories without touching another's.
- The embedding is a hashed bag of words, so it proves the wiring (index creation, search, filters),
  **not the quality of a real model's results**.
- **Mem0's own fact extraction is not exercised.** `Memory.add(..., infer=False)` stores the text as given;
  with `infer=True` Mem0 asks a language model to extract facts and decide what to add, update or delete.
  That needs a model, so it is not here. What the server sees (the same inserts, searches and updates) is
  covered.
- **Scale is not tested.** A handful of memories, and no timing or recall figures.
- **Not covered:** Mem0's other storage (graph stores) and Redis Cluster. The TypeScript SDK has its own
  harness in [`../mem0-ts-compat/`](../mem0-ts-compat/).

## Left out on purpose

These fail the same way on Redis 8.10.3, so they say nothing about a server:

- `RedisDB.get()` of an id that does not exist raises `KeyError: 'hash'`, because `redisvl`'s `fetch()`
  returns `{}` and not `None` ([mem0#7820](https://github.com/mem0ai/mem0/issues/7820)).
- `RedisDB` with `redisvl` 0.28 or newer (above).

## Can the harness fail?

Yes, and it says why. [`../langchain-compat/selftest_fake_server.py`](../langchain-compat/selftest_fake_server.py)
is a server that answers `PING`, `INFO`, `SCAN` and `HELLO` and refuses every other command. The harness
against it reports 1 PASS (`PING`) and 2 FAIL, one per group, each with the error the server gave
(`unknown command 'FT.INFO'`), and does not crash. A group whose setup fails is one FAIL line, and the next
group still runs.

The checks that scope by user and by run also fail on wrong data, not only on a missing server: with bob's
memory stored under another user, "search does not return another user's memory" (which first searches as
bob) and "delete_all removes one user's memories only" report FAIL, and with the `run_id` left off the
memory that should carry it, "filter on run_id" reports FAIL.

## Reproduce

```bash
pip install -r requirements.txt

# KiviDB
python mem0_kividb_compat.py --url redis://127.0.0.1:6380 --label kividb

# Reference
docker run -d --rm --name redis-ref -p 6390:6379 redis:8
python mem0_kividb_compat.py --url redis://127.0.0.1:6390 --label redis8
```

Run it on a throwaway machine, not a laptop: it creates and drops indexes with random names. After a run,
`FT._LIST` is empty and `DBSIZE` is 0 on both servers. It removes the indexes it made and leaves an index
that another client made meanwhile alone: a `mem0migrations` index with 8 dimensions, planted before a run on
each server, was still there, unchanged, after it.

## Telemetry

The harness sets `MEM0_TELEMETRY=False` before importing Mem0. With telemetry on (the default), Mem0 2.2.1
sends events to PostHog and also creates a `mem0migrations` index with `overwrite=True`, which would drop
and recreate an index of that name made by another Mem0 app on the same server.
