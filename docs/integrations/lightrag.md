# LightRAG

LightRAG's two Redis storages work on KiviDB: `RedisKVStorage` (documents, chunks, the LLM response
cache) and `RedisDocStatusStorage` (document status and the scheduling index). Point `REDIS_URI` at
KiviDB (port `6380` by default) and nothing else changes. The default `kividb/kividb` image is enough:
LightRAG uses no Lua.

**Checked:** LightRAG's own Redis tests (the `redis` backend of its doc-status contract suite) pass
29 of 29 on KiviDB, the same 29 as on Redis 8. Our harness passes 29 of 29 on both, with `lightrag-hku`
`1.5.7`. It runs the storages directly and an insert, query, restart and delete through LightRAG. The
harness, its pins and the results of each check are in
[`benchmarks/lightrag-compat/`](../../benchmarks/lightrag-compat/).

## Install

```bash
docker run -d -p 6380:6380 kividb/kividb
pip install lightrag-hku redis
```

## Configure

```bash
export REDIS_URI=redis://localhost:6380   # KiviDB, not Redis's 6379
```

```python
from lightrag import LightRAG

rag = LightRAG(
    working_dir="./rag_storage",
    kv_storage="RedisKVStorage",
    doc_status_storage="RedisDocStatusStorage",
    llm_model_func=...,      # your model
    embedding_func=...,      # your embedding
)
await rag.initialize_storages()
```

For the LightRAG server (`lightrag-server`), set the same in `.env`:

```bash
LIGHTRAG_KV_STORAGE=RedisKVStorage
LIGHTRAG_DOC_STATUS_STORAGE=RedisDocStatusStorage
REDIS_URI=redis://localhost:6380
```

LightRAG has no Redis graph or vector storage. Keep the graph and vectors in its default local files or
another backend.

## What to know

- **Do not let the server evict keys.** LightRAG's documents and status rows have no TTL. At start-up it
  reads `CONFIG GET maxmemory*` and refuses a server with `maxmemory` set and an `allkeys-*` policy. KiviDB's
  default is `noeviction` with no limit, which LightRAG accepts. If you set `maxmemory`, use
  `noeviction` or a `volatile-*` policy. This works on KiviDB as on Redis.
- **Workspaces** (`workspace=` or `REDIS_WORKSPACE`) prefix every key, so several LightRAG instances can
  share one KiviDB.
- **Not tested:** Redis Cluster, scale (the harness uses a handful of documents), and real models (the
  harness uses a stub LLM and stub embeddings).
