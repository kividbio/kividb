# RedisVL compatibility report

`redisvl` run against KiviDB and against Redis 8, with the same harness
([`redisvl_kividb_compat.py`](redisvl_kividb_compat.py)) and no API keys: a stub plays the embedding
model.

**67 of 67 checks pass on KiviDB with the fix for #767, and 67 of 67 on Redis 8.10.3.**
KiviDB 1.2.2 passed 59: it failed the eight `HybridQuery` checks, on a hash index and on a JSON index,
with `unknown FT.HYBRID token 'SCORER'`. No check was skipped. The default build is enough: the run sent
no `EVAL`, `EVALSHA`, `FCALL`, or `SCRIPT`.

| | |
|---|---|
| KiviDB | branch `fix/hybrid-scorer` (#767) on `master` `4a08e72b`, built from source with `cargo build --release`, default build, no Lua. Before: `1.2.2`, image `kividb/kividb:latest` (`sha256:8853c7b1e73e`) |
| Reference | Redis `8.10.3` (the `redis:8.10` image, `sha256:1f88291b4c57`) |
| `redisvl` | `0.32.0` |
| `redis-py` / `numpy` | `8.1.0` / `2.5.4` (see [`requirements.txt`](requirements.txt)) |
| Python | `3.12.3` |
| Run | 2026-11-14, on a Linux VM. KiviDB listened on `127.0.0.1:6380`; Redis in the `redis:8.10` image |

## Results

### Connection

| Check | KiviDB | Redis 8 |
|---|---|---|
| PING | PASS | PASS |

### SearchIndex, hash, FLAT

| Check | KiviDB | Redis 8 |
|---|---|---|
| create and load | PASS | PASS |
| vector query returns the closest document first | PASS | PASS |
| vector distances ascend | PASS | PASS |
| tag filter | PASS | PASS |
| tag filter excludes a value | PASS | PASS |
| numeric filter | PASS | PASS |
| numeric range filter | PASS | PASS |
| text phrase filter | PASS | PASS |
| tag and numeric filter together | PASS | PASS |
| filter query | PASS | PASS |
| text query | PASS | PASS |
| fetch by id | PASS | PASS |
| drop a document | PASS | PASS |
| delete the index | PASS | PASS |

### SearchIndex, hash, HNSW

| Check | KiviDB | Redis 8 |
|---|---|---|
| create and load | PASS | PASS |
| vector query returns the closest document first | PASS | PASS |
| vector distances ascend | PASS | PASS |
| tag filter | PASS | PASS |
| tag filter excludes a value | PASS | PASS |
| numeric filter | PASS | PASS |
| numeric range filter | PASS | PASS |
| text phrase filter | PASS | PASS |
| tag and numeric filter together | PASS | PASS |
| filter query | PASS | PASS |
| fetch by id | PASS | PASS |
| drop a document | PASS | PASS |
| delete the index | PASS | PASS |

### SearchIndex, JSON, FLAT

| Check | KiviDB | Redis 8 |
|---|---|---|
| create and load | PASS | PASS |
| vector query returns the closest document first | PASS | PASS |
| vector distances ascend | PASS | PASS |
| tag filter | PASS | PASS |
| tag filter excludes a value | PASS | PASS |
| numeric filter | PASS | PASS |
| numeric range filter | PASS | PASS |
| text phrase filter | PASS | PASS |
| tag and numeric filter together | PASS | PASS |
| filter query | PASS | PASS |
| text query | PASS | PASS |
| fetch by id | PASS | PASS |
| drop a document | PASS | PASS |
| delete the index | PASS | PASS |

`fetch` on the JSON index returns `year` as an int (`2021`) on both servers.

### HybridQuery, hash (`FT.HYBRID`)

| Check | KiviDB | Redis 8 |
|---|---|---|
| create and load | PASS | PASS |
| LINEAR fusion weighted toward the vector | PASS | PASS |
| LINEAR fusion weighted toward the text | PASS | PASS |
| RRF fusion | PASS | PASS |
| hybrid query with a tag filter | PASS | PASS |

### HybridQuery, JSON (`FT.HYBRID`)

| Check | KiviDB | Redis 8 |
|---|---|---|
| create and load | PASS | PASS |
| LINEAR fusion weighted toward the vector | PASS | PASS |
| LINEAR fusion weighted toward the text | PASS | PASS |
| RRF fusion | PASS | PASS |
| hybrid query with a tag filter | PASS | PASS |

On KiviDB 1.2.2 each of these failed with `ResponseError: unknown FT.HYBRID token 'SCORER'`. See
[What #767 changed](#what-767-changed).

### AggregateHybridQuery

This is RedisVL's other hybrid path. It fuses a text score and a vector score inside `FT.AGGREGATE`,
and it does not send `FT.HYBRID`.

| Check | KiviDB | Redis 8 |
|---|---|---|
| create and load | PASS | PASS |
| vector weight 0.1 ranks the text match first | PASS | PASS |
| vector weight 0.9 ranks the vector match first | PASS | PASS |

### SemanticCache

| Check | KiviDB | Redis 8 |
|---|---|---|
| store | PASS | PASS |
| an exact prompt hits | PASS | PASS |
| a close prompt hits | PASS | PASS |
| a far prompt misses | PASS | PASS |
| a filter for another value misses | PASS | PASS |
| a filter for the stored value hits | PASS | PASS |
| clear removes the entries | PASS | PASS |

### SemanticRouter

| Check | KiviDB | Redis 8 |
|---|---|---|
| an exact reference routes to that route | PASS | PASS |
| a near statement routes under the threshold | PASS | PASS |
| a far statement matches nothing | PASS | PASS |
| route_many returns the closest route | PASS | PASS |
| a route added later matches | PASS | PASS |

Raw results: [`results-kividb.json`](results-kividb.json),
[`results-redis8.json`](results-redis8.json).

## What #767 changed

`HybridQuery` always sends `SCORER`, because `text_scorer` defaults to `BM25STD`:

```
FT.HYBRID reprohybrid SEARCH (~@content:(rust | compiler)) SCORER BM25STD
  VSIM @embedding $vector COMBINE RRF 4 WINDOW 20 CONSTANT 60
  LOAD 1 @content LIMIT 0 1 PARAMS 2 vector <float32 blob>
```

KiviDB 1.2.2 refused the token. Past it, two more things stood in the way: the reply was not Redis's
(redis-py reads `total_results`, `results`, `warnings` and `execution_time`), and the filtered query puts
`FILTER @topic:{tech}` in the `VSIM` clause, where Redis applies it to the vector side. With the fix,
`SCORER` scores the text side as `FT.SEARCH` does, the reply is Redis's, and the vector side's `FILTER` is
applied. All eight `HybridQuery` checks pass, with the documents Redis returns.

## Commands RedisVL sent

Counted from `INFO commandstats` on KiviDB over one harness run: `FT.CREATE`, `FT.INFO`, `FT.SEARCH`,
`FT.AGGREGATE`, `FT.HYBRID`, `FT.DROPINDEX`, `FT._LIST`, `HSET`, `HGETALL`, `JSON.SET`, `JSON.GET`,
`DEL`, `SCAN`. `SemanticCache` and `SemanticRouter` are `FT.SEARCH` / `FT.AGGREGATE` on indexes the
library creates. No `EVAL`, `EVALSHA`, `FCALL`, or `SCRIPT`.

## What this does and does not show

- It shows `SearchIndex` create, load, and query on hash and JSON, FLAT and HNSW, including tag,
  numeric, and text filters, a combined filter, `FilterQuery`, `TextQuery`, fetch, drop, and delete.
- It shows `AggregateHybridQuery`, `SemanticCache` (hit, miss, filter, clear), and `SemanticRouter`
  (exact, near, far, `route_many`, `add_route`) behave as they do on Redis 8.10.3.
- It shows `HybridQuery` (LINEAR, RRF, and a filtered query) on a hash index and on a JSON index.
- The embedding is a hashed bag of words, so it proves the wiring, not the quality of a real model's
  results.
- The corpus is four documents. It does not measure speed, recall, or behaviour at size.
- Hash vectors are passed through `array_to_buffer`; JSON vectors are a list of floats. RedisVL 0.32
  requires that of each storage type, on Redis as well.

## Reproduce

```bash
pip install -r requirements.txt

# KiviDB, built from source until a release has #767. The default build has no Lua, and this
# harness does not need it.
cargo build --release && ./target/release/kividb --port 6380 &
python redisvl_kividb_compat.py --url redis://127.0.0.1:6380 --label kividb

# Reference
docker run -d --name redis-ref -p 6399:6379 redis:8.10
python redisvl_kividb_compat.py --url redis://127.0.0.1:6399 --label redis8
```

Run it on a throwaway machine, not a laptop: it creates and drops indexes with random names.
`redis-py` prints a warning that `hybrid_search` is experimental. That warning appears against both
servers.
