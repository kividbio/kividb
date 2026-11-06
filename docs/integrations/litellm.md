# LiteLLM

LiteLLM's Redis features work on KiviDB: the response cache (`type: redis`), the semantic cache
(`type: redis-semantic`, which uses RedisVL and KiviDB's `FT.*` vector search), the router's usage-based
routing counters, and the proxy's per-key rate limits. Point LiteLLM's Redis host and port at KiviDB
(port `6380` by default) and nothing else changes.

**Use the `-lua` image for the proxy** (`kividb/kividb:v1.2.0-lua`). The proxy's rate limiters, budget
limiters, login throttle and locks run Lua scripts, and the default `kividb/kividb` image has no Lua. The
SDK's caches and the router work on the default image.

**Checked**, with LiteLLM `1.108.0`:

- our harness: the exact cache, the semantic cache (a paraphrase hits, an unrelated prompt misses), the
  router's TPM/RPM counters and the proxy's v3 per-key limiter (`rpm_limit=2`: the third call gets HTTP
  429) pass on KiviDB as on Redis 8.10.2;
- LiteLLM's own Redis tests (the Redis files of its `sdk` integration group, 57 tests): KiviDB passes 45,
  Redis 8 passes 49. The four in between are two test-harness gaps, not LiteLLM features: their test
  server is stopped with `SHUTDOWN`, which KiviDB answers with `+OK`
  ([#429](https://github.com/kividbio/kividb/issues/429)), and their cluster fixture uses the
  `cluster-port` directive ([#430](https://github.com/kividbio/kividb/issues/430)).

The [report](../../benchmarks/litellm-compat/REPORT.md) has each check, the versions, and how to run it
yourself.

## Install

```bash
docker run -d -p 6380:6380 kividb/kividb:v1.2.0-lua
pip install "litellm[proxy,caching]"
```

## Configure the proxy

```yaml
# config.yaml
model_list:
  - model_name: gpt-4o-mini
    litellm_params:
      model: openai/gpt-4o-mini
      api_key: os.environ/OPENAI_API_KEY

router_settings:
  redis_host: localhost
  redis_port: 6380            # KiviDB, not Redis's 6379

litellm_settings:
  cache: true
  cache_params:
    type: redis
    host: localhost
    port: 6380
```

LiteLLM's Redis cache also reads `REDIS_HOST` and `REDIS_PORT` (or `REDIS_URL`) when the config gives no
host; set them to KiviDB's.

For the semantic cache, use `type: redis-semantic` with `redis_url: redis://localhost:6380` and an
embedding model (`redis_semantic_cache_embedding_model`). KiviDB's search and vector indexes are built in;
there is no module to load.

## Configure the SDK

```python
import litellm
from litellm.caching.caching import Cache

litellm.cache = Cache(type="redis", host="localhost", port="6380")
response = litellm.completion(model="gpt-4o-mini", messages=[{"role": "user", "content": "hi"}], caching=True)
```

## What to know

- **Lua.** On the default image, `EVAL` fails with `ERR this build of KiviDB has no Lua`, and the proxy
  features that run scripts don't work. The per-key rate limiter is the quiet one: it falls back to
  counting in each proxy process's memory, so limits still trigger on one worker but are not shared across
  workers or replicas. Use `kividb/kividb:v1.2.0-lua` or `-full`.
- **redis-py 8.** LiteLLM resolves redis-py 5.3.1 (RESP2). With redis-py 8, which speaks RESP3 by default,
  KiviDB 1.2.0 sends a Lua script's `false` in a form redis-py 8 rejects
  ([#431](https://github.com/kividbio/kividb/issues/431)); on 1.2.0, use `protocol=2` with redis-py 8.
  Fixed in 1.2.1. LiteLLM's own pins are not affected.
- **Not tested:** the proxy process end to end with Postgres-backed keys and teams, Redis Cluster mode, and
  real models (the harness uses a stub provider and stub embeddings).
