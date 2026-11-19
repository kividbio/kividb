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

## Index builds and `EF_RUNTIME`

- **Vectors are linked into an HNSW graph by helper threads**, as many as
  `search-index-threads` says (`--search-index-threads`, or in the configuration file;
  read at startup, `CONFIG GET` shows it): a number, 0 meaning each write links its own
  vector as 1.2 did, or `auto`, the default. `auto` starts none under a CPU quota (a
  container's `--cpus`, a Kubernetes CPU limit) or on fewer than 4 CPUs, otherwise
  (CPUs - 2) / 2: one on 4 CPUs, three on 8, seven on 16. They start on the first write
  that needs them and run as `SCHED_IDLE` on Linux, taking CPU time the workers leave; the
  log says how many there are.
  - The trade-off, measured with every server CPU busy (4 vCPUs, memtier GET/SET, one
    client loading 384-d vectors): GET/SET p99 1.6 ms with none, 2.1 ms with one, 2.6 ms
    with two; the build alone 1,540, 3,190 and 3,620 documents a second.
  - Under a CPU limit the helpers would spend the quota the workers need and the whole
    server would be throttled (GET/SET p99 8-9 ms against 0.35 ms beside a load with
    `--cpus 2` and one helper), so there builds run at 1.2's speed unless
    `search-index-threads` is given a number.
  - On a server whose CPUs the workers keep busy the backlog stays full and each write
    links its own vector too (measured with every server CPU busy: the backlog held its
    limit the whole time).
- A write puts its vector in the graph's backlog and returns; a search measures every
  vector of the backlog exactly, beside what it finds in the graph, so a document is found
  from the moment its `HSET` is answered, as before. The backlog holds at most 1,024
  vectors, and at most 128 Ki floats (85 vectors of 1,536 dimensions, 341 of 384; never
  fewer than 64): a writer that finds it full links its vector itself. The walk that indexes the
  documents already there (`FT.CREATE` on existing keys, a restart, a replica's full sync)
  hands its vectors over the same way. A graph of fewer than 1,000 nodes links each vector
  in the write.
- `FT.INFO` shows the backlog as `hnsw_backlog` (and `backlog` per field under
  `hnsw_indexes`), and as `hnsw_backlog_bytes` the memory it holds, the helpers' queued
  jobs included: at most one a waiting key, so writes over the same keys do not grow it.
  A graph dropped (`FT.DROPINDEX`, `FLUSHALL`) or replaced by a compaction is freed at
  once, whatever its jobs. `hnsw_live_count` counts the graph's nodes, so it trails the documents
  while the backlog is not empty; `num_docs` does not.
- **A KNN search looks at no fewer than 50 candidates**, whatever `EF_RUNTIME` says (the
  default is 10, as in Redis). So the default answers with the recall Redis gives at about
  `EF_RUNTIME 50`, and costs about what that costs; compare the two at the same recall, not
  the same `EF_RUNTIME`.
- Measured numbers are in [PERFORMANCE.md](PERFORMANCE.md#vector-index-builds).

## Persistence and restarts

- Native snapshots (**`dump.kdb`**) store the **keys**, the **index schemas**, and, when you **`SAVE` / `BGSAVE`** with indexes populated, the **HNSW graph** of each vector field, so search quality after a restart matches the graph you had before shutdown.
- While search indexes are being rebuilt at startup, most commands return **`LOADING`** until the rebuild finishes; **`PING`**, **`INFO`** and a few other commands still work.
- After a large ingest, run **`SAVE`** (or schedule **`BGSAVE`**) so the next start reloads the graph from disk instead of rebuilding it from the hashes.
