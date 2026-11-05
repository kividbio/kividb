# LangChain

`langchain-redis` works on KiviDB: the vector store, the LLM cache, the semantic cache and the chat
history. Point its Redis URL at KiviDB (port `6380` by default) and nothing else changes.

**Checked:** 33 of 33 checks pass on KiviDB, the same 33 that pass on Redis 8, with `langchain-redis`
`0.2.6`. See the [report](../../benchmarks/langchain-compat/REPORT.md) for each check, what it does not
cover, and how to run it yourself.

## Example

```bash
docker run -d -p 6380:6380 kividb/kividb
pip install langchain-redis langchain-core redisvl
```

```python
from langchain_core.embeddings import DeterministicFakeEmbedding
from langchain_redis import RedisConfig, RedisVectorStore
from redisvl.query.filter import Tag

# A stand-in so the example runs with no API key. Use your real embedding model here.
embeddings = DeterministicFakeEmbedding(size=64)

config = RedisConfig(
    index_name="docs",
    redis_url="redis://localhost:6380",          # KiviDB, not Redis's 6379
    metadata_schema=[{"name": "topic", "type": "tag"}],
)
store = RedisVectorStore(embeddings, config=config)

store.add_texts(
    ["KiviDB speaks the Redis protocol", "Rust has no garbage collector", "HNSW is a graph index"],
    [{"topic": "databases"}, {"topic": "languages"}, {"topic": "databases"}],
)

# Nearest documents, filtered by metadata.
for doc in store.similarity_search(
    "KiviDB speaks the Redis protocol", k=2, filter=Tag("topic") == "databases"
):
    print(doc.metadata["topic"], "|", doc.page_content)
```

It prints (the same on KiviDB and on Redis 8):

```
databases | KiviDB speaks the Redis protocol
databases | HNSW is a graph index
```

With a real embedding model the search returns the closest documents by meaning. The stand-in above only
matches identical text, which is enough to show the wiring.

The same works for `RedisCache`, `RedisSemanticCache` and `RedisChatMessageHistory`: pass
`redis_url="redis://localhost:6380"`.

## Known limits

- `RedisVectorStore.similarity_search_with_relevance_scores` raises `NotImplementedError` in
  `langchain-redis` itself, on Redis 8 as well.
- Reading the messages of a chat session that has none fails inside `langchain-redis` too.
- LangGraph's Redis checkpointer and LlamaIndex are not covered yet. See
  [#33](https://github.com/kividbio/kividb/issues/33).
