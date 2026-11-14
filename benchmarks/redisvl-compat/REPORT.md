# RedisVL compatibility report

`redisvl` run against KiviDB and against Redis 8, with the same harness
([`redisvl_kividb_compat.py`](redisvl_kividb_compat.py)) and no API keys: a stub plays the embedding
model.

**59 of 67 checks pass on KiviDB, and 67 of 67 on Redis 8.10.3.**
The eight failures are every `HybridQuery` check, on a hash index and on a JSON index. KiviDB answers
`unknown FT.HYBRID token 'SCORER'`. Those eight pass on Redis. Every other check passes on both, and no
check was skipped. The default image is enough: the run sent no `EVAL`, `EVALSHA`, `FCALL`, or `SCRIPT`.

| | |
|---|---|
| KiviDB | `1.2.2` as the server reports it, image `kividb/kividb:latest` (`sha256:8853c7b1e73e`), default build, no Lua |
| Reference | Redis `8.10.3` (the `redis:8.10` image, `sha256:1f88291b4c57`) |
| `redisvl` | `0.32.0` |
| `redis-py` / `numpy` | `8.1.0` / `2.5.4` (see [`requirements.txt`](requirements.txt)) |
| Python | `3.12.3` |
| Run | 2026-11-14. KiviDB listened on `0.0.0.0:6380` inside the container, published as `127.0.0.1:6380` |

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
| LINEAR fusion weighted toward the vector | FAIL | PASS |
| LINEAR fusion weighted toward the text | FAIL | PASS |
| RRF fusion | FAIL | PASS |
| hybrid query with a tag filter | FAIL | PASS |

### HybridQuery, JSON (`FT.HYBRID`)

| Check | KiviDB | Redis 8 |
|---|---|---|
| create and load | PASS | PASS |
| LINEAR fusion weighted toward the vector | FAIL | PASS |
| LINEAR fusion weighted toward the text | FAIL | PASS |
| RRF fusion | FAIL | PASS |
| hybrid query with a tag filter | FAIL | PASS |

Each KiviDB failure is `ResponseError: unknown FT.HYBRID token 'SCORER'`. The query never runs, so these
checks say nothing about fusion once the command is accepted. See [The gap](#the-gap).

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

## The gap

`HybridQuery` always sends `SCORER`, because `text_scorer` defaults to `BM25STD`. Redis 8.10.3 accepts
the command and returns the text match. KiviDB 1.2.2 rejects the token.

On current `master` the parser in `src/commands/hybrid.rs` reads `YIELD_SCORE_AS` after `SEARCH` and
then stops. `SCORER` falls through to the unknown-token error. A comment there says `SCORER` is ignored
for now; the token is not consumed, so the command errors before that.

This environment cannot open a `redis-compat` issue. The repro below is the one to file.

```python
import hashlib
import math
import re

from redisvl.index import SearchIndex
from redisvl.query import HybridQuery
from redisvl.redis.utils import array_to_buffer

DIM = 64


def embed(text):
    v = [0.0] * DIM
    for tok in re.findall(r"[a-z0-9]+", text.lower()):
        v[int(hashlib.md5(tok.encode()).hexdigest(), 16) % DIM] += 1.0
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / n for x in v]


text = "a new rust compiler release is out"
schema = {
    "index": {"name": "reprohybrid", "prefix": "reprohybrid", "storage_type": "hash"},
    "fields": [
        {"name": "content", "type": "text"},
        {"name": "embedding", "type": "vector", "attrs": {
            "dims": DIM, "algorithm": "flat", "distance_metric": "cosine", "datatype": "float32"}},
    ],
}
index = SearchIndex.from_dict(schema, redis_url="redis://127.0.0.1:6380")
index.create(overwrite=True)
index.load([{
    "id": "rust",
    "content": text,
    "embedding": array_to_buffer(embed(text), "float32"),
}], id_field="id")

index.query(HybridQuery(
    text="rust compiler",
    text_field_name="content",
    vector=embed("interest rates rose again this quarter"),
    vector_field_name="embedding",
    return_fields=["content"],
    num_results=1,
    combination_method="RRF",
    stopwords=None,
))
```

KiviDB raises `redis.exceptions.ResponseError: unknown FT.HYBRID token 'SCORER'`.
Redis 8.10.3 returns the stored document. The command RedisVL sends is:

```
FT.HYBRID reprohybrid SEARCH (~@content:(rust | compiler)) SCORER BM25STD
  VSIM @embedding $vector COMBINE RRF 4 WINDOW 20 CONSTANT 60
  LOAD 1 @content LIMIT 0 1 PARAMS 2 vector <float32 blob>
```

The same `SCORER BM25STD` clause is on the LINEAR queries and on the filtered hybrid query, and on a
JSON index. All eight fail the same way. Creating and loading the index first succeeds.

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
- It shows `HybridQuery` does not run on this KiviDB build, because of `SCORER`.
- The embedding is a hashed bag of words, so it proves the wiring, not the quality of a real model's
  results.
- The corpus is four documents. It does not measure speed, recall, or behaviour at size.
- Hash vectors are passed through `array_to_buffer`; JSON vectors are a list of floats. RedisVL 0.32
  requires that of each storage type, on Redis as well.

## Reproduce

```bash
pip install -r requirements.txt

# KiviDB. The default image has no Lua, and this harness does not need it.
docker run -d --name kividb -p 6380:6380 kividb/kividb
python redisvl_kividb_compat.py --url redis://127.0.0.1:6380 --label kividb

# Reference
docker run -d --name redis-ref -p 6399:6379 redis:8.10
python redisvl_kividb_compat.py --url redis://127.0.0.1:6399 --label redis8
```

Run it on a throwaway machine, not a laptop: it creates and drops indexes with random names.
`redis-py` prints a warning that `hybrid_search` is experimental. That warning appears against both
servers.
