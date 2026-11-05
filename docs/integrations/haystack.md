# Haystack

**Haystack has no Redis document store, and its Valkey document store does not work on KiviDB 1.2.0, nor on
Redis 8.** This page says why, so nobody has to find out the hard way.

- `deepset-ai/haystack` has no Redis integration. A Redis document store was requested in
  `haystack-core-integrations` #524 (2024), with no plans announced.
- `haystack-core-integrations` has `valkey-haystack` (`ValkeyDocumentStore`). It is built on the
  `valkey-glide` client and the **valkey-search** module, and is tested against `valkey/valkey-bundle`.
  valkey-search is a different search engine from Redis's, with its own schema rules and reply formats;
  KiviDB implements Redis's.

**What we ran:** `valkey-haystack` `1.0.0`'s integration tests (`pytest -m integration tests`, 178 tests,
`valkey-glide` 2.5.3, `haystack-ai` 3.4.1):

| Server | passed | failed | skipped |
|---|---|---|---|
| `valkey/valkey-bundle:8.1` (Valkey 8.1.11, search 1.0.3) | 153 | 0 | 25 |
| Redis 8.10.2 | 24 | 129 | 25 |
| KiviDB 1.2.0 | 35 | 118 | 25 |

On Redis 8, index creation fails: the store names its vector attribute `vector` on a JSON index, where Redis
needs a JSON path (`$.vector`), and Redis answers `Invalid JSONPath 'vector'`. KiviDB accepts the index, but
`valkey-glide` then can't read the replies. Its `FT.INFO` and `FT.SEARCH` parsers expect valkey-search's
reply shapes, and Redis and KiviDB answer in Redis's: `Response couldn't be converted for FT.INFO -
TypeError: (response was "Map")`. Searches and counts come back empty. No test passes on Redis 8 and fails on
KiviDB.

Making this store work would mean answering in valkey-search's formats, which is not Redis compatibility, so
it is not filed as a KiviDB bug. The JUnit files and the per-test comparison are in
[`benchmarks/haystack-valkey-compat/`](../../benchmarks/haystack-valkey-compat/).

## What to use instead

Haystack's other document stores, or KiviDB through a Redis-based framework that has a Redis store, such as
[LangChain](langchain.md) or [LlamaIndex](llamaindex.md).
