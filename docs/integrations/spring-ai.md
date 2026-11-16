# Spring AI

Spring AI's Redis vector store (`RedisVectorStore`, module `spring-ai-redis-store`) and its Redis chat
memory repository (`RedisChatMemoryRepository`) work on KiviDB. Both use Jedis with Redis's search and JSON
commands (`FT.CREATE … ON JSON`, `FT.SEARCH` with KNN, vector range and TEXT queries, `JSON.SET`), which
KiviDB has built in. Point `spring.data.redis.url` at KiviDB (port `6380` by default).

**Use the `-lua` image for chat memory** (`kividb/kividb:v1.2.0-lua`). The chat memory repository reserves
message timestamps with a Lua script. Without Lua it falls back to timestamps in a different unit (epoch
milliseconds times 1000), and its time-range queries (`findByTimeRange`) then find nothing. The vector
store alone works on the default `kividb/kividb` image.

**Checked**, with Spring AI `2.0.2` (Jedis 7.4.1, Java 21), by running Spring AI's own Redis integration
tests with KiviDB in place of the Redis Stack container they start, and with Redis 8.10.2 in its place as
the reference:

| Tests | Redis 8.10.2 | KiviDB 1.2.0-lua | KiviDB 1.2.0 |
|---|---|---|---|
| vector store (`RedisVectorStoreIT`, `…DistanceMetricIT`, `…ObservationIT`) | 15 of 15 | 15 of 15 | 15 of 15 |
| chat memory repository (7 IT classes, 58 tests) | 53 of 58 | 53 of 58 | 52 of 58 |
| semantic cache (`SemanticCacheAdvisorIT`, 17 tests) | skipped: needs `OPENAI_API_KEY` | skipped | skipped |

The five chat memory tests that fail on Redis 8 fail the same way on KiviDB (four with Redis's own
`SEARCH_SYNTAX Unknown field at offset 0 near metadata`, one with a count off by one); they are Spring AI
test problems. The one more on the default image is `shouldFindMessagesByTimeRange`, for the Lua reason
above. The vector store tests use a local ONNX embedding model, so they ran in full with no API key.

How the tests were run, the per-test comparison and the Maven logs are in
[`benchmarks/spring-ai-compat/`](../../benchmarks/spring-ai-compat/).

## Run KiviDB

```bash
docker run -d -p 6380:6380 kividb/kividb:v1.2.0-lua
```

## Configure

```properties
spring.data.redis.url=redis://localhost:6380
spring.ai.vectorstore.redis.initialize-schema=true
spring.ai.vectorstore.redis.index-name=spring-ai-index
spring.ai.vectorstore.redis.prefix=embedding:
```

Nothing else changes: no module to load, and the index schema Spring AI creates is accepted as Redis
accepts it.

## What to know

- **Lua for chat memory**, as above. The vector store itself runs no scripts.
- **Not tested:** the semantic cache advisor end to end (its tests need an OpenAI key), Redis Cluster, and
  scale.
