# Mem0

Mem0's Redis vector store works on KiviDB, in the Python SDK and in the TypeScript SDK: the `RedisDB` store
used directly, and the `Memory` API on top of it (add, search, get, update, delete, history). Point
`redis_url` (Python) or `redisUrl` (TypeScript) at KiviDB (port `6380` by default) and nothing else changes.
The default KiviDB image is enough; Mem0 runs no Lua scripts.

**Checked:**

- Python, `mem0ai` `2.2.1`: 24 of 24 checks pass on KiviDB, the same 24 that pass on Redis 8. See the
  [report](../../benchmarks/mem0-compat/REPORT.md) for each check, what it does not cover, and how to run it
  yourself.
- TypeScript, `mem0ai` `3.3.1` (`mem0ai/oss`): 39 of 39 checks pass on KiviDB, for the Redis store, the
  Valkey store and `Memory`. See [TypeScript](#typescript) below and
  [`benchmarks/mem0-ts-compat/`](../../benchmarks/mem0-ts-compat/).

## Install

```bash
docker run -d -p 6380:6380 kividb/kividb
pip install "mem0ai[vector-stores]"
```

Use the extra. Mem0's Redis page says `pip install redis redisvl`, but that installs a `redisvl` (0.28 or
newer) that Mem0's `RedisDB` cannot use yet: it fails with `AttributeError: 'SearchIndex' object has no
attribute 'set_client'` ([mem0#7819](https://github.com/mem0ai/mem0/issues/7819)). The extra resolves to
`redisvl` 0.26.0, which works, and is what the results here were produced with.

## Example

Mem0 normally calls a language model to pull facts out of a conversation, and an embedding model to index
them. This example needs neither: `infer=False` stores the text as given, a small stand-in replaces the
embedding model, and Mem0's telemetry is turned off, so it runs offline.

```python
import hashlib
import math
import os
import re
import tempfile

os.environ["MEM0_TELEMETRY"] = "False"  # before importing mem0; see the note below

from mem0 import Memory

DIM = 64


class HashEmbedder:
    """A stand-in for a real embedding model: offline, deterministic, no API key."""

    def embed(self, text, memory_action=None):
        v = [0.0] * DIM
        for tok in re.findall(r"[a-z0-9]+", text.lower()):
            v[int(hashlib.md5(tok.encode()).hexdigest(), 16) % DIM] += 1.0
        n = math.sqrt(sum(x * x for x in v)) or 1.0
        return [x / n for x in v]


memory = Memory.from_config({
    "vector_store": {"provider": "redis", "config": {
        "redis_url": "redis://localhost:6380",  # KiviDB, not Redis's 6379
        "collection_name": "kividb_mem0_example", "embedding_model_dims": DIM}},
    "llm": {"provider": "openai", "config": {"api_key": "unused"}},
    "embedder": {"provider": "openai", "config": {"api_key": "unused", "embedding_dims": DIM}},
    "history_db_path": os.path.join(tempfile.mkdtemp(), "history.db"),
})
memory.embedding_model = HashEmbedder()

memory.add("I love hiking in the mountains", user_id="alice", infer=False)
memory.add("I prefer tea to coffee", user_id="bob", infer=False)

for hit in memory.search("hiking in the mountains", filters={"user_id": "alice"}, top_k=3)["results"]:
    print(f"{hit['memory']!r} (user {hit['user_id']})")

memory.delete_all(user_id="alice")
memory.delete_all(user_id="bob")
memory.vector_store.delete_col()  # drop the example's index
```

It prints (the same on KiviDB and on Redis 8):

```
'I love hiking in the mountains' (user alice)
```

Mem0 creates the collection's index with `overwrite=True`, so use a `collection_name` that no other index on
the server has. Telemetry is off in the example because, when it is on (the default), Mem0 also sends events
to PostHog and creates a `mem0migrations` index the same way, replacing one of that name made by another Mem0
app on the server.

With real models, drop the `HashEmbedder` line and the `infer=False` arguments, set the `llm` and `embedder`
sections to your providers, and set `embedding_model_dims` to the embedder's size. That path, with Mem0's own
fact extraction, needs a language model and is not exercised here.

## TypeScript

The TypeScript SDK (`mem0ai/oss`) has two stores that work on KiviDB: `redis` (node-redis, `redis` 4.x) and
`valkey` (iovalkey, written for valkey-search). Install the one you use with its client:

```bash
docker run -d -p 6380:6380 kividb/kividb
npm install mem0ai@3.3.1 redis@4 better-sqlite3   # better-sqlite3 for Mem0's local history file
```

```js
process.env.MEM0_TELEMETRY = 'false'; // before importing mem0ai
const { Memory } = await import('mem0ai/oss');

const memory = new Memory({
  vectorStore: {
    provider: 'redis',
    config: { redisUrl: 'redis://localhost:6380', collectionName: 'kividb_mem0_example', embeddingModelDims: 1536 },
  },
  // embedder and llm: your providers, as in Mem0's docs
});
await memory.add('I love hiking in the mountains', { userId: 'alice' });
const { results } = await memory.search('hiking', { filters: { user_id: 'alice' } });
```

For the Valkey store, use `provider: 'valkey'` with `valkeyUrl` in place of `redisUrl`.

**Checked** with [`mem0_ts_kividb_compat.mjs`](../../benchmarks/mem0-ts-compat/mem0_ts_kividb_compat.mjs)
(stub embedding, `infer: false`, no API keys): `mem0ai` `3.3.1`, `redis` `4.7.1`, `iovalkey` `0.3.3`.

| | KiviDB 1.2.0 | Redis 8.10.3 | Valkey 9.1.3 (valkey-bundle) |
|---|---|---|---|
| Connection (PING, `MODULE LIST` names `search`) | 1 of 1 | 1 of 1 | 1 of 1 |
| `RedisDB`, the Redis store | 14 of 14 | 13 of 14 | 4 of 14 |
| `ValkeyDB`, the Valkey store | 14 of 14 | 11 of 14 | 14 of 14 |
| `Memory`, on `RedisDB` | 10 of 10 | 10 of 10 | 8 of 10 |

Raw results: [KiviDB](../../benchmarks/mem0-ts-compat/results-kividb.json),
[Redis 8](../../benchmarks/mem0-ts-compat/results-redis8.json),
[Valkey](../../benchmarks/mem0-ts-compat/results-valkey-bundle.json). Each store passes everything on the
server it was written for (Redis for `RedisDB`, valkey-search for `ValkeyDB`), and on KiviDB.

The Redis 8 misses are in the SDK, not the server:

- **A search with two filters fails on Redis.** `RedisDB` (and `ValkeyDB`) put the filters before the vector
  clause without parentheses (`@user_id:{a} @agent_id:{b} =>[KNN ...]`), which Redis 8 rejects with
  `Syntax error ... near >[`; it accepts `(@user_id:{a} @agent_id:{b})=>[KNN ...]`. KiviDB accepts both
  forms, so this search works on KiviDB. A single filter works on all servers.
- `ValkeyDB`'s `list` searches with an all-zero vector, which matches nothing on Redis, and its search does
  not ask for score order, which Redis then does not give. Both work on valkey-search and on KiviDB.

## Known limits

- `Memory.get()` of an id that does not exist raises `KeyError: 'hash'` with the Python Redis store, on
  Redis 8 as well ([mem0#7820](https://github.com/mem0ai/mem0/issues/7820)). Check with `get_all` or `search`
  first. The TypeScript store returns `null`.
- Mem0's fact extraction (`infer=True`) and a real embedding model are not exercised: the harness uses a stub
  embedding and `infer=False`.
- Scale is not tested: the harness uses a handful of memories, and reports no timing or recall.
