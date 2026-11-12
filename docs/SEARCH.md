# Vector search (Redis Stack–style `FT.*`)

KiviDB implements **Redis Stack–compatible** search and vector commands, so existing clients
and tutorials largely work unchanged. Use them for **full-text (BM25)**, **tags / numerics**,
and **vector ANN** (FLAT or **HNSW**) on hash fields.

## Quick start

1. **Create an index** on keys with a prefix, declaring at least one `VECTOR` field (HNSW or FLAT) and optional `TEXT` / `TAG` / `NUMERIC` fields:

```bash
redis-cli -p 6380 FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA \
  vec VECTOR HNSW 10 TYPE FLOAT32 DIM 128 DISTANCE_METRIC COSINE M 16 EF_CONSTRUCTION 128
```

2. **Store vectors** as raw **FLOAT32** little-endian bytes in a hash field (**`DIM × 4` bytes**), the same convention as Redis Stack and `redis-py` / `numpy` blobs. Use your client's binary-safe `HSET` (the integration tests under `tests/e2e/` have examples).

3. **Query with KNN** (Dialect 2), passing the query vector as a named binary parameter
   (`<blob>` stands for the query vector's raw bytes, which a shell cannot type: send this
   from a client library, as in step 2):

```bash
redis-cli -p 6380 FT.SEARCH idx '*=>[KNN 10 @vec $q]' PARAMS 2 q <blob> DIALECT 2
```

The full command surface, filters, hybrid search and edge cases are in **[COMMANDS.md](../COMMANDS.md)** (search for `FT.CREATE`, `FT.SEARCH`, `FT.HYBRID`, and so on).

## Persistence and restarts

- Native snapshots (**`dump.kdb`**) store the **keys**, the **index schemas**, and, when you **`SAVE` / `BGSAVE`** with indexes populated, the **HNSW graph** of each vector field, so search quality after a restart matches the graph you had before shutdown.
- While search indexes are being rebuilt at startup, most commands return **`LOADING`** until the rebuild finishes; **`PING`**, **`INFO`** and a few other commands still work.
- After a large ingest, run **`SAVE`** (or schedule **`BGSAVE`**) so the next start reloads the graph from disk instead of rebuilding it from the hashes.
