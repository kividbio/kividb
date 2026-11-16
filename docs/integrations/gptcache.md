# GPTCache

GPTCache's Redis backends work on KiviDB: the Redis scalar store (`CacheBase("redis")`, built on redis-om,
which uses `FT.*` and `JSON.*`), the Redis vector store (`VectorBase("redis")`, `FT.CREATE … ON HASH` with a
FLAT index) and the Redis distributed eviction. Point GPTCache's Redis URL at KiviDB (port `6380` by
default). The default `kividb/kividb` image is enough: GPTCache runs no Lua.

**Checked:** GPTCache `0.1.44`'s own Redis tests (`tests/unit_tests/manager/test_redis.py`,
`…/manager/test_redis_cache_storage.py`, `…/eviction/test_distributed_cache.py` and
`tests/integration_tests/test_redis_onnx.py`) pass 16 of 16 on KiviDB 1.2.0, and 16 of 16 on Redis 8.10.2.

GPTCache is not maintained against current libraries. Those results need redis-py 5.2.1, redis-om 0.3.3,
pydantic 1.10, transformers 4.40.2 and openai 0.28.1. With current releases of those, the same tests fail
the same way on both servers (11 of 16): redis-py 6 removed `redis.commands.search.indexDefinition`, which
GPTCache imports, and newer redis-om, pydantic and transformers break its models and its ONNX embedding.

The commands, pins and JUnit files are in [`benchmarks/gptcache-compat/`](../../benchmarks/gptcache-compat/).

## Install

```bash
docker run -d -p 6380:6380 kividb/kividb
pip install gptcache==0.1.44 "redis==5.2.1" "redis-om==0.3.3" "pydantic<2"
```

## Configure

```python
from gptcache import cache
from gptcache.manager import CacheBase, VectorBase, get_data_manager

data_manager = get_data_manager(
    CacheBase("redis", redis_host="localhost", redis_port=6380),
    VectorBase("redis", host="localhost", port="6380", dimension=768),
)
cache.init(data_manager=data_manager)
```

## What to know

- **Old dependencies**, as above; this is GPTCache's limit, not the server's.
- **Not tested:** the GPTCache server, other embedding models, scale.
