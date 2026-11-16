# Semantic Kernel

Semantic Kernel's Redis vector store connector (Python, `semantic_kernel.connectors.redis`) works on KiviDB,
in both of its modes: JSON documents (`RedisJsonCollection`) and hashes (`RedisHashsetCollection`). Point
`REDIS_CONNECTION_STRING` (or `connection_string=`) at KiviDB (port `6380` by default) and nothing else
changes. The default `kividb/kividb` image is enough: the connector runs no Lua.

**Checked**, with `semantic-kernel` `1.47.0` (`redisvl` 0.30.0, `redis-py` 8.1.0), against Redis 8.10.2 as
the reference:

- Semantic Kernel's own Redis tests (the four Redis cases of
  `tests/integration/memory/test_vector_store.py`) pass 4 of 4 on KiviDB, as on Redis 8;
- our harness passes 15 checks on both, with the same one warning on both: create, upsert, get, vector
  search (nearest first, with scores), search with a tag filter, delete, a search after a server restart, and
  dropping the collection, in each mode.

The harness, its pins, the results and Semantic Kernel's JUnit files are in
[`benchmarks/semantic-kernel-compat/`](../../benchmarks/semantic-kernel-compat/).

## Install

```bash
docker run -d -p 6380:6380 kividb/kividb
pip install "semantic-kernel[redis]"
```

## Configure

```bash
export REDIS_CONNECTION_STRING=redis://localhost:6380   # KiviDB, not Redis's 6379
```

```python
from dataclasses import dataclass
from typing import Annotated

from semantic_kernel.connectors.redis import RedisJsonCollection
from semantic_kernel.data.vector import VectorStoreField, vectorstoremodel


@vectorstoremodel
@dataclass
class Note:
    id: Annotated[str, VectorStoreField("key")]
    text: Annotated[str, VectorStoreField("data", is_full_text_indexed=True)]
    tag: Annotated[str, VectorStoreField("data", type="str", is_indexed=True)]
    embedding: Annotated[list[float] | None, VectorStoreField("vector", dimensions=4, type="float")] = None


notes = RedisJsonCollection[str, Note](
    record_type=Note,
    collection_name="notes",
    prefix_collection_name_to_key_names=True,   # keys are notes:<id>, which the index covers
)
await notes.ensure_collection_exists()
await notes.upsert([Note(id="n1", text="reset my password", tag="a", embedding=[1.0, 0.0, 0.0, 0.0])])
results = await notes.search(vector=[1.0, 0.0, 0.0, 0.0], top=3, filter=lambda x: x.tag == "a")
```

KiviDB's search and JSON are built in: there is no module to load, and `FT.CREATE` takes the connector's
schema as Redis does.

## What to know

- **A Semantic Kernel bug, on Redis too:** in JSON mode with `prefix_collection_name_to_key_names=True`,
  `delete` in Semantic Kernel 1.47.0 sends `JSON.DEL` for the bare id instead of `<collection>:<id>`
  (`RedisJsonCollection._inner_delete`, `semantic_kernel/connectors/redis.py` line 755), so the record stays.
  Hash mode deletes correctly. Without the prefix option, the JSON index does not cover the records and
  searches find nothing, on Redis 8 as on KiviDB.
- **Not tested:** the .NET connector (now `CommunityToolkit.VectorData.Redis`), hybrid search, embedding
  generators (the harness passes vectors in), Redis Cluster, and scale.
