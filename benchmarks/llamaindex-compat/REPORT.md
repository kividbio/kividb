# LlamaIndex compatibility report

LlamaIndex's Redis integrations run against KiviDB and against Redis 8, with the same harness
([`llamaindex_kividb_compat.py`](llamaindex_kividb_compat.py)) and no API keys: a stub plays the embedding
model. The vector store is also driven through a `VectorStoreIndex`, and the document store through an
`IngestionPipeline`, not only through the classes' own methods.

**54 of 54 checks pass on KiviDB, and 54 of 54 on Redis 8.10.3.**
No check differs between the two. The same checks were first recorded on 2026-11-03 against `2ed7df2`.
They were run again on current `master` after the search score compatibility fixes; the result is unchanged.

| | |
|---|---|
| KiviDB | `8.10.0` as the server reports it, release build from `master` at `03b7f7c4` (`kividb --version` reports 1.2.0) |
| Reference | Redis `8.10.3` (the `redis:8` image; this run used the 8.10.3 packages with the search module loaded) |
| `llama-index-core` | `0.14.25` |
| `llama-index-vector-stores-redis` | `0.9.0` |
| Docstore, index store, kv store | `llama-index-storage-docstore-redis` `0.6.0`, `llama-index-storage-index-store-redis` `0.7.0`, `llama-index-storage-kvstore-redis` `0.4.1` |
| Other pins | see [`requirements.txt`](requirements.txt) |
| Python | `3.12` |
| Run | 2026-11-06; KiviDB started with only `--bind 127.0.0.1 --port 6380` |

These packages pin an older client stack than the LangChain and LangGraph ones: `redisvl` `0.4.1` and
`redis-py` `5.3.1`, so the connection speaks RESP2.

## Results

### Connection

| Check | KiviDB | Redis 8 |
|---|---|---|
| PING | PASS | PASS |

### RedisVectorStore, FLAT index

| Check | KiviDB | Redis 8 |
|---|---|---|
| add creates the index and stores the nodes | PASS | PASS |
| query returns the closest node first | PASS | PASS |
| similarities are ordered, best first | PASS | PASS |
| similarity_top_k is honoured | PASS | PASS |
| filter == | PASS | PASS |
| filter != | PASS | PASS |
| filter > on a number | PASS | PASS |
| filter IN | PASS | PASS |
| filters combined with AND | PASS | PASS |
| filters combined with OR | PASS | PASS |
| filter on text (TEXT_MATCH) | PASS | PASS |
| hybrid query (vector and text) | PASS | PASS |
| MMR query | PASS | PASS |
| get_nodes by id | PASS | PASS |
| delete_nodes by id | PASS | PASS |
| delete_nodes by filter | PASS | PASS |
| delete_index | PASS | PASS |

### RedisVectorStore, HNSW index

| Check | KiviDB | Redis 8 |
|---|---|---|
| add creates the index and stores the nodes | PASS | PASS |
| query returns the closest node first | PASS | PASS |
| similarities are ordered, best first | PASS | PASS |
| similarity_top_k is honoured | PASS | PASS |
| filter == | PASS | PASS |
| filter != | PASS | PASS |
| filter > on a number | PASS | PASS |
| filter IN | PASS | PASS |
| filters combined with AND | PASS | PASS |
| filters combined with OR | PASS | PASS |
| filter on text (TEXT_MATCH) | PASS | PASS |
| hybrid query (vector and text) | PASS | PASS |
| MMR query | PASS | PASS |
| get_nodes by id | PASS | PASS |
| delete_nodes by id | PASS | PASS |
| delete_nodes by filter | PASS | PASS |
| delete_index | PASS | PASS |

### VectorStoreIndex on the Redis vector store

| Check | KiviDB | Redis 8 |
|---|---|---|
| VectorStoreIndex from documents, then retrieve | PASS | PASS |
| reopen from the vector store and retrieve | PASS | PASS |
| delete_ref_doc removes the document's nodes | PASS | PASS |

### RedisDocumentStore and the ingestion pipeline

| Check | KiviDB | Redis 8 |
|---|---|---|
| add and get documents | PASS | PASS |
| exists, and a missing id | PASS | PASS |
| allow_update=False refuses an existing id | PASS | PASS |
| document hashes | PASS | PASS |
| delete a document and its hash | PASS | PASS |
| nodes of a reference document, and delete_ref_doc | PASS | PASS |
| ingestion pipeline: skips an unchanged document, redoes a changed one | PASS | PASS |

### RedisIndexStore and storage

| Check | KiviDB | Redis 8 |
|---|---|---|
| add and get an index struct | PASS | PASS |
| list index structs | PASS | PASS |
| delete an index struct | PASS | PASS |
| an index is saved, then loaded back from a new storage context | PASS | PASS |

### RedisKVStore

| Check | KiviDB | Redis 8 |
|---|---|---|
| put and get, and a missing key | PASS | PASS |
| put_all and get_all | PASS | PASS |
| delete reports whether the key existed | PASS | PASS |
| nested values and unicode | PASS | PASS |
| async put, get, get_all, delete | PASS | PASS |
Raw results: [`results-kividb.json`](results-kividb.json),
[`results-redis8.json`](results-redis8.json).

## Commands LlamaIndex sent

Counted from `INFO commandstats` on KiviDB after one run: `FT.CREATE`, `FT.SEARCH`, `FT.DROPINDEX`,
`FT._LIST`, `HSET`, `HGET`, `HDEL`, `HSCAN`, `MULTI`, `EXEC`, `DEL`, `SCAN`, `INFO`, `PING` and
`CLIENT SETINFO`. A few of these (`PING`, `INFO`, `FT._LIST`, `SCAN`, `DEL`, `FT.DROPINDEX`) are also the
harness's own set-up and clean-up. Unlike the LangGraph integration, LlamaIndex stores its data in hashes,
not in `JSON.*` documents.

## What this does and does not show

- It shows the calls above work on KiviDB as they do on Redis 8: building and querying a vector index (FLAT
  and HNSW) with every metadata filter operator the integration translates, hybrid and MMR queries,
  deleting nodes by id and by filter, a `VectorStoreIndex` built, reopened and edited, the document store
  with its hashes and reference-document bookkeeping, an ingestion pipeline that skips unchanged
  documents, an index saved and loaded back, and the key-value store, sync and async.
- The embedding is a hashed bag of words, so it proves the wiring (index creation, search, filters),
  **not the quality of a real model's results**.
- **Scale is not tested.** A handful of nodes per index, not thousands, and no timing or recall figures.
- **Not covered:** the chat store and any other LlamaIndex Redis package this harness does not import,
  the `collection_suffix` option of `RedisIndexStore`, Redis Cluster, and Mem0.

## Left out on purpose

These behave the same way on Redis 8.10.3, so they say nothing about a server:

- `RedisVectorStore.clear()` raises `NotImplementedError` in the library.
- The `text_search` query mode needs an embedding in the library.
- Node ids are chosen so that `RedisVectorStore.add()` returns them intact. It removes the key prefix with
  `str.strip()`, which cuts any id that starts or ends with a character of the prefix
  ([llama_index#21483](https://github.com/run-llama/llama_index/issues/21483)). The harness uses uppercase
  ids, and the prefix is lowercase.

## Can the harness fail?

Yes, and it says why. [`../langchain-compat/selftest_fake_server.py`](../langchain-compat/selftest_fake_server.py)
is a server that answers `PING`, `INFO`, `SCAN` and `HELLO` and refuses every other command. The harness
against it reports a FAIL for each group with the error the server gave (`unknown command 'FT._LIST'`,
`'HGET'`, `'HSET'`, `'EXEC'`), and does not crash. A group whose setup fails is one FAIL line, and the next
group still runs.

## Reproduce

```bash
pip install -r requirements.txt

# KiviDB
python llamaindex_kividb_compat.py --url redis://127.0.0.1:6380 --label kividb

# Reference
docker run -d --rm --name redis-ref -p 6390:6379 redis:8
python llamaindex_kividb_compat.py --url redis://127.0.0.1:6390 --label redis8
```

Run it on a throwaway machine, not a laptop: it creates and drops indexes with random names. It removes
everything it created, including the indexes the library makes. Every index the harness makes is named
with one of its random names, so it drops only the ones that appeared during the run and have such a
name, and leaves alone an index another client made meanwhile. After a run, `FT._LIST` is empty and
`DBSIZE` is 0 on both servers (on a server nothing else is using).
