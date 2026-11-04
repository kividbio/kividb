# LlamaIndex

LlamaIndex's Redis integrations work on KiviDB: the vector store (`RedisVectorStore`, with FLAT and HNSW
indexes), the document store, the index store and the key-value store. Point the Redis client at KiviDB
(port `6380` by default) and nothing else changes.

**Checked:** 54 of 54 checks pass on KiviDB, the same 54 that pass on Redis 8, with
`llama-index-vector-stores-redis` `0.9.0` and `llama-index-core` `0.14.25`. See the
[report](../../benchmarks/llamaindex-compat/REPORT.md) for each check, what it does not cover, and how to
run it yourself.

## Example

```bash
docker run -d -p 6380:6380 kividb/kividb
pip install llama-index-core llama-index-vector-stores-redis
```

```python
import hashlib
import math
import re

from llama_index.core import Document, StorageContext, VectorStoreIndex
from llama_index.core.embeddings import BaseEmbedding
from llama_index.vector_stores.redis import RedisVectorStore
from redis import Redis
from redisvl.schema import IndexSchema

DIM = 64


class HashEmbedding(BaseEmbedding):
    """A stand-in for a real embedding model: offline, deterministic, no API key."""

    def _vec(self, text):
        v = [0.0] * DIM
        for tok in re.findall(r"[a-z0-9]+", text.lower()):
            v[int(hashlib.md5(tok.encode()).hexdigest(), 16) % DIM] += 1.0
        n = math.sqrt(sum(x * x for x in v)) or 1.0
        return [x / n for x in v]

    def _get_query_embedding(self, query):
        return self._vec(query)

    def _get_text_embedding(self, text):
        return self._vec(text)

    async def _aget_query_embedding(self, query):
        return self._vec(query)


schema = IndexSchema.from_dict({
    "index": {"name": "notes", "prefix": "notes", "key_separator": ":"},
    "fields": [
        {"name": "id", "type": "tag"},
        {"name": "doc_id", "type": "tag"},
        {"name": "text", "type": "text"},
        {"name": "vector", "type": "vector",
         "attrs": {"dims": DIM, "algorithm": "flat", "distance_metric": "cosine"}},
    ],
})
client = Redis.from_url("redis://localhost:6380")  # KiviDB, not Redis's 6379
vector_store = RedisVectorStore(schema=schema, redis_client=client, overwrite=True)

index = VectorStoreIndex.from_documents(
    [Document(text="the cat sat on the warm mat", doc_id="cats"),
     Document(text="interest rates rose again this quarter", doc_id="rates"),
     Document(text="a new rust compiler release is out", doc_id="rust")],
    storage_context=StorageContext.from_defaults(vector_store=vector_store),
    embed_model=HashEmbedding(),
)

for hit in index.as_retriever(similarity_top_k=1).retrieve("interest rates rose"):
    print(f"{hit.node.ref_doc_id:6} score={hit.score:.2f}")

vector_store.delete_index()
```

It prints (the same on KiviDB and on Redis 8):

```
rates  score=0.82
```

Use a real embedding model in place of `HashEmbedding` (and set `dims` to its size). The document, index
and key-value stores work the same way: `RedisDocumentStore.from_redis_client(client, namespace="...")`,
`RedisIndexStore.from_redis_client(client, namespace="...")` and `RedisKVStore.from_redis_client(client)`.

## Known limits

- `RedisVectorStore.add()` returns node ids with `str.strip()` applied to the key prefix, in
  `llama-index-vector-stores-redis` itself, so an id that starts or ends with a character of the prefix
  comes back cut (`dogs` as `ogs` for a prefix that contains a `d`). Only the ids that `add()` returns
  are affected. It is the same on Redis 8, and
  [reported upstream](https://github.com/run-llama/llama_index/issues/21483).
- `RedisVectorStore.clear()` raises `NotImplementedError` in the library, on Redis 8 as well. Use
  `delete_index()`.
- Scale is not tested: the harness uses a handful of nodes, and reports no timing or recall.
- Mem0 is not covered yet. See [#33](https://github.com/kividbio/kividb/issues/33).
