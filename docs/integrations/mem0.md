# Mem0

Mem0's Redis vector store works on KiviDB: the `RedisDB` store used directly, and the `Memory` API on top of
it (add, search, get, update, delete, history). Point `redis_url` at KiviDB (port `6380` by default) and
nothing else changes.

**Checked:** 24 of 24 checks pass on KiviDB, the same 24 that pass on Redis 8, with `mem0ai` `2.2.1`. See the
[report](../../benchmarks/mem0-compat/REPORT.md) for each check, what it does not cover, and how to run it
yourself.

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

## Known limits

- `Memory.get()` of an id that does not exist raises `KeyError: 'hash'` with the Redis store, on Redis 8 as
  well ([mem0#7820](https://github.com/mem0ai/mem0/issues/7820)). Check with `get_all` or `search` first.
- Mem0's fact extraction (`infer=True`) and a real embedding model are not exercised: the harness uses a stub
  embedding and `infer=False`.
- Scale is not tested: the harness uses a handful of memories, and reports no timing or recall.
