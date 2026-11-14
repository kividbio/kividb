# RedisVL

RedisVL's `SearchIndex`, `SemanticCache`, and `SemanticRouter` work on KiviDB. Point `redis_url` at
KiviDB (port `6380` by default). `HybridQuery` does not: it sends `FT.HYBRID` with `SCORER`, and KiviDB
rejects that token. The default image is enough. This path runs no Lua.

**Checked:** 59 of 67 checks pass on KiviDB 1.2.2, and 67 of 67 pass on Redis 8.10.3, with `redisvl`
`0.32.0` and `redis-py` `8.1.0`. The eight failures are the `HybridQuery` checks. See the
[report](../../benchmarks/redisvl-compat/REPORT.md) for each check and the versions.

## Example

```bash
docker run -d -p 6380:6380 kividb/kividb
pip install redisvl
```

```python
import hashlib
import math
import re

from redisvl.index import SearchIndex
from redisvl.query import VectorQuery
from redisvl.query.filter import Tag
from redisvl.redis.utils import array_to_buffer

DIM = 64


def embed(text):
    """A stand-in for a real embedding model: offline, deterministic, no API key."""
    v = [0.0] * DIM
    for tok in re.findall(r"[a-z0-9]+", text.lower()):
        v[int(hashlib.md5(tok.encode()).hexdigest(), 16) % DIM] += 1.0
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / n for x in v]


schema = {
    "index": {"name": "notes", "prefix": "notes", "storage_type": "hash"},
    "fields": [
        {"name": "topic", "type": "tag"},
        {"name": "content", "type": "text"},
        {"name": "embedding", "type": "vector", "attrs": {
            "dims": DIM, "algorithm": "flat", "distance_metric": "cosine", "datatype": "float32"}},
    ],
}
index = SearchIndex.from_dict(schema, redis_url="redis://localhost:6380")  # KiviDB, not Redis's 6379
index.create(overwrite=True)
rows = [
    ("databases", "KiviDB speaks the Redis protocol"),
    ("languages", "Rust has no garbage collector"),
    ("databases", "HNSW is a graph index"),
]
# RedisVL 0.32 stores a hash vector as a float32 buffer. A JSON index takes a list of floats instead.
index.load([
    {"id": str(i), "topic": topic, "content": text,
     "embedding": array_to_buffer(embed(text), "float32")}
    for i, (topic, text) in enumerate(rows)
], id_field="id")

for hit in index.query(VectorQuery(
    vector=embed("KiviDB speaks the Redis protocol"),
    vector_field_name="embedding",
    return_fields=["topic", "content"],
    num_results=2,
    filter_expression=Tag("topic") == "databases",
)):
    print(hit["topic"], "|", hit["content"])

index.delete(drop=True)
```

It prints (the same on KiviDB and on Redis 8):

```
databases | KiviDB speaks the Redis protocol
databases | HNSW is a graph index
```

Use a real embedding model in place of `embed`, and set `dims` to its size. `SemanticCache` and
`SemanticRouter` take the same URL:

```python
from redisvl.extensions.cache.llm import SemanticCache
from redisvl.utils.vectorize import CustomVectorizer

cache = SemanticCache(
    name="llmcache",
    redis_url="redis://localhost:6380",
    distance_threshold=0.4,
    vectorizer=CustomVectorizer(embed),
)
cache.store("KiviDB speaks the Redis protocol", "port 6380")
print(cache.check("KiviDB speaks the Redis protocol")[0]["response"])
cache.delete()
```

That prints `port 6380`.

## What differs from Redis

`HybridQuery` fails on KiviDB before it searches. RedisVL's default `text_scorer` is `BM25STD`, so the
command includes `SCORER BM25STD`. KiviDB 1.2.2 answers `unknown FT.HYBRID token 'SCORER'`. Redis 8.10.3
runs the same command and returns the document. The minimal repro, already run against both:

```python
from redisvl.query import HybridQuery

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

The index above has to exist first, with a `content` text field and an `embedding` vector, and one
document loaded. The full script is in the [report](../../benchmarks/redisvl-compat/REPORT.md#the-gap).
The command on the wire is:

```
FT.HYBRID reprohybrid SEARCH (~@content:(rust | compiler)) SCORER BM25STD
  VSIM @embedding $vector COMBINE RRF 4 WINDOW 20 CONSTANT 60
  LOAD 1 @content LIMIT 0 1 PARAMS 2 vector <float32 blob>
```

On current `master`, `src/commands/hybrid.rs` reads `YIELD_SCORE_AS` after `SEARCH` and then leaves
`SCORER` for the unknown-token error. A comment there says the scorer is ignored for now; the token is
not consumed. This shows up for LINEAR and RRF, with and without a tag filter, on a hash index and on a
JSON index: eight checks, one error. Creating the index and loading it still succeed.

`AggregateHybridQuery` is a different call (`FT.AGGREGATE`). Those checks pass. The harness still runs
`HybridQuery` and records the failure.

Nothing else in this pass needs a setting Redis does not. The port is `6380`. The `-lua` image is not
required.

## Run the harness

```bash
pip install -r benchmarks/redisvl-compat/requirements.txt
python benchmarks/redisvl-compat/redisvl_kividb_compat.py --url redis://127.0.0.1:6380 --label kividb
python benchmarks/redisvl-compat/redisvl_kividb_compat.py --url redis://127.0.0.1:6399 --label redis8
```

Pins, the pass/fail table, and the server versions are in the
[report](../../benchmarks/redisvl-compat/REPORT.md).

## Known limits

- Scale is not tested: the harness uses four documents, and reports no timing or recall.
- The stub embedding proves index creation, search, filters, cache hit and miss, and routing. It does
  not prove the quality of a real model's results.
