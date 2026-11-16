# LangChain compatibility report

`langchain-redis` run against KiviDB and against Redis 8, with the same harness
([`langchain_kividb_compat.py`](langchain_kividb_compat.py)) and no API keys: a
stub plays the embedding model.

**33 of 33 checks pass on KiviDB, and 33 of 33 on Redis 8.10.3.**
No check differs between the two.

| | |
|---|---|
| KiviDB | `8.10.0` as the server reports it, built from `master` at `522577b` |
| Reference | Redis `8.10.3` (the `redis:8` image) |
| `langchain-redis` | `0.2.6` |
| `redisvl` / `langchain-core` / `redis-py` | `0.30.0` / `1.7.4` / `8.1.0` (see [`requirements.txt`](requirements.txt)) |
| Python | `3.12` |
| Run | 2026-11-03; KiviDB started with only `--bind 127.0.0.1 --port 6380` |

## Results

### connection

| Check | KiviDB | Redis 8 |
|---|---|---|
| PING | PASS | PASS |

### vectorstore (FLAT)

| Check | KiviDB | Redis 8 |
|---|---|---|
| FT.CREATE and add_texts | PASS | PASS |
| nearest neighbour is the query itself | PASS | PASS |
| scores ascend, exact match near 0 | PASS | PASS |
| metadata comes back | PASS | PASS |
| tag filter | PASS | PASS |
| numeric filter | PASS | PASS |
| tag and numeric filter together | PASS | PASS |
| as_retriever | PASS | PASS |
| max marginal relevance | PASS | PASS |
| add then delete | PASS | PASS |
| get_by_ids | PASS | PASS |
| from_existing_index | PASS | PASS |

### vectorstore (HNSW)

| Check | KiviDB | Redis 8 |
|---|---|---|
| FT.CREATE and add_texts | PASS | PASS |
| nearest neighbour is the query itself | PASS | PASS |
| scores ascend, exact match near 0 | PASS | PASS |
| metadata comes back | PASS | PASS |
| tag filter | PASS | PASS |
| numeric filter | PASS | PASS |
| tag and numeric filter together | PASS | PASS |
| as_retriever | PASS | PASS |
| max marginal relevance | PASS | PASS |
| add then delete | PASS | PASS |
| get_by_ids | PASS | PASS |
| from_existing_index | PASS | PASS |

### llm cache

| Check | KiviDB | Redis 8 |
|---|---|---|
| update then lookup | PASS | PASS |
| another model does not hit | PASS | PASS |
| clear | PASS | PASS |

### semantic cache

| Check | KiviDB | Redis 8 |
|---|---|---|
| update then a close prompt hits | PASS | PASS |
| an exact prompt hits | PASS | PASS |
| a far prompt misses | PASS | PASS |

### chat history

| Check | KiviDB | Redis 8 |
|---|---|---|
| messages read back in order | PASS | PASS |
| clear | PASS | PASS |

Raw results: [`results-kividb.json`](results-kividb.json),
[`results-redis8.json`](results-redis8.json).

## Commands `langchain-redis` sent

Counted from `INFO commandstats` on KiviDB after one run: `FT.CREATE`, `FT.SEARCH`, `FT.INFO`,
`FT.DROPINDEX`, `HSET`, `HGETALL`, `JSON.SET`, `JSON.GET`, `SCAN`, `UNLINK` and `DEL`. Chat history is the
part that uses `JSON.*`, together with `FT.*`.

## What this does and does not show

- It shows the `langchain-redis` calls above work on KiviDB as they do on Redis 8:
  `FT.CREATE` for FLAT and HNSW indexes, KNN search, tag and numeric filters, delete, fetch by id,
  max marginal relevance, the LLM cache, the semantic cache, and chat history.
- The embedding is a hashed bag of words, so it proves the wiring (index creation, search, filters,
  hit and miss), **not the quality of a real model's results**.
- The corpus is six documents. It does not measure speed, recall or behaviour at size.
- **Not covered:** LangGraph (`langgraph-checkpoint-redis`), LlamaIndex, Mem0, text-field and
  hybrid search, and any `langchain-redis` feature not listed above.

## Can the harness fail?

Yes, and it says why. [`selftest_fake_server.py`](selftest_fake_server.py) is a server that answers
`PING`, `INFO`, `SCAN` and `HELLO` and refuses every other command, like a server with no `FT.*`,
`JSON.*` or key commands. The harness against it reports 1 PASS (`PING`) and 5 FAIL, each with the
error the server gave, and does not crash (for example `unknown command 'FT.INFO'`). A group whose
setup fails is one FAIL line, and the next group still runs.

## Left out on purpose

Two checks were dropped because they fail the same way on Redis 8.10.3, so they say nothing about a server:

- `similarity_search_with_relevance_scores` raises `NotImplementedError` inside the library.
- Reading the messages of a chat session that has none raises `AttributeError` inside the library.

## Reproduce

```bash
pip install -r requirements.txt

# KiviDB
python langchain_kividb_compat.py --url redis://127.0.0.1:6380 --label kividb

# Reference
docker run -d --rm --name redis-ref -p 6399:6379 redis:8
python langchain_kividb_compat.py --url redis://127.0.0.1:6399 --label redis8
```

Run it on a throwaway machine, not a laptop: it creates and drops indexes with random names.
