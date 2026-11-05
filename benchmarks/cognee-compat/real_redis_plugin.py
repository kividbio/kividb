"""
A pytest plugin that runs cognee's own session-manager tests against a real server.

cognee/tests/integration/infrastructure/session/test_session_manager_redis.py builds a real
RedisAdapter and a real SessionManager, but hands the adapter an in-memory stand-in
(_InMemoryRedisList) instead of a redis.asyncio client. This plugin replaces that stand-in with a
real redis.asyncio client on REAL_REDIS_URL, flushed before each test, so the same 16 tests run
their commands on the server. Nothing in cognee is changed.

  REAL_REDIS_URL=redis://127.0.0.1:6379 PYTHONPATH=<this folder> \
    python -m pytest -p real_redis_plugin \
    cognee/tests/integration/infrastructure/session/test_session_manager_redis.py

Use a server of its own: every test starts with FLUSHDB.
"""
import os

import redis
import redis.asyncio as aioredis

URL = os.environ.get("REAL_REDIS_URL", "redis://127.0.0.1:6379")


def _real_client():
    redis.Redis.from_url(URL).flushdb()
    return aioredis.Redis.from_url(URL, decode_responses=True)


def pytest_collection_modifyitems(session, config, items):
    for item in items:
        module = getattr(item, "module", None)
        if module is not None and hasattr(module, "_InMemoryRedisList"):
            module._InMemoryRedisList = _real_client
