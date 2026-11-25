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
  container's `--cpus`, a Kubernetes CPU limit) or on fewer than 8 CPUs. Otherwise it
  starts one fewer than the CPUs (seven on 8, fifteen on 16), and how many of them link
  depends on what the server is doing:
  - while it serves clients, (CPUs - 2) / 2 of them: three on 8, seven on 16;
  - while it does nothing but take the writes that fill the backlogs (a bulk load,
    `FT.CREATE` over existing keys, a replica's full sync), all of them.

  The server counts as serving when, besides the commands that wrote vectors, it served
  500 commands a second, or its threads used a CPU's worth of time besides the links and
  the commands that wrote vectors.
  The helpers look every 50 ms. They go back to the serving count at the first look that
  finds clients. They take the others in again after 200 ms without any. A number starts
  exactly that many, all of which always link. They start on the first write that needs
  them and run as `SCHED_IDLE` on Linux, taking CPU time the workers leave; the log says
  how many there are, and `FT.INFO` shows `hnsw_index_threads` (started) and
  `hnsw_index_threads_active` (linking now).
  - **On a server of fewer than 8 CPUs, builds run at 1.2's speed by default.**
    `search-index-threads 1` (or more) trades serving latency for build speed there.
    Measured with every CPU of a 4-vCPU server busy (memtier GET/SET, one client loading
    384-d vectors in pipelines, one `HSET` every 2 ms from another client): GET/SET p99
    1.6 ms with none, 2.1 ms with one helper, 2.6 ms with two; the build alone 1,540, 3,190
    and 3,620 documents a second. A writer whose connection shares the bulk loader's
    worker waits behind the loader's pipelines with or without helpers (its median 15-25
    ms with none, 12-16 ms with one); on the other connections one helper raised the
    `HSET` p99 from 2.3-2.7 ms to 4.5-4.7 ms.
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
- Documents that a query finds level (equal `SORTBY` values, the same score, vectors as near) come in the order they were written, as in Redis: `SORTBY … ASC` first written first, `DESC` the reverse. A write that names a field of the index makes its document the last written. Snapshots keep that order (their document order section), the AOF replays it, and a replica has its master's.
- Limits of that order: `HEXPIRE` of an indexed field leaves the document where it is; with KNN and `k` below the number of vectors at the same distance, which of those are returned is not decided by the order they were written in; with `aof-preamble no`, the order after an AOF rewrite is that of the rewritten file; `FT.HYBRID` does not use it; a replica fed from its master's stream can order level documents written at the same time from different connections otherwise than its master, so a `LIMIT` page that ends among them can differ; an index of vector fields only keeps the order of the keys; the documents written after an `FT.CREATE` while its first walk is still filling the index, saved in that window, load in the order of their keys. A `RENAME` keeps the document's place, as in Redis. `FT.AGGREGATE` gives groups in the order of their first documents written.
- After a large ingest, run **`SAVE`** (or schedule **`BGSAVE`**) so the next start reloads the graph from disk instead of rebuilding it from the hashes.
