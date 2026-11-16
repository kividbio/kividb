"""
pytest plugin: run Dify's own Redis tests against another server, without editing them.

Dify's container tests start Redis with testcontainers' RedisContainer (redis:6-alpine, or
redis:7-alpine for sharded pub/sub). With this plugin loaded, every RedisContainer starts
$COMPAT_REDIS_IMAGE instead. For a KiviDB image the container is given KiviDB's flags: port 6379
(KiviDB's default is 6380) and protected mode off, and a `redis-server ...` command (the cluster-mode
test passes one) has its leading `redis-server` dropped, since KiviDB takes the same flags directly.

  cd dify/api
  COMPAT_REDIS_IMAGE=kividb/kividb:v1.2.0-lua PYTHONPATH=/path/to/this/dir \
      .venv/bin/pytest -p compat_redis_image tests/e2e_tests/containers/libs
"""
import os

from testcontainers.redis import RedisContainer

IMAGE = os.environ.get("COMPAT_REDIS_IMAGE", "")
KIVIDB = "kividb" in IMAGE

_init = RedisContainer.__init__
_with_command = RedisContainer.with_command


def _patched_init(self, image="redis:latest", *args, **kwargs):
    _init(self, IMAGE or image, *args, **kwargs)
    if KIVIDB:
        _with_command(self, "--port 6379 --protected-mode no")


def _patched_with_command(self, command):
    if KIVIDB and isinstance(command, str) and command.startswith("redis-server"):
        command = command[len("redis-server"):].strip()
        # KiviDB 1.2.0 refuses `--appendonly` on its command line (its flag is `--aof`, with no
        # value). The runs in the report keep it, so the test fails as it would for a user;
        # COMPAT_TRANSLATE_FLAGS=1 translates it, to see whether the rest of the test passes.
        if os.environ.get("COMPAT_TRANSLATE_FLAGS"):
            command = command.replace("--appendonly no", "").replace("--appendonly yes", "--aof")
    return _with_command(self, command)


if IMAGE:
    RedisContainer.__init__ = _patched_init
    RedisContainer.with_command = _patched_with_command


def pytest_report_header(config):
    return f"compat_redis_image: RedisContainer runs {IMAGE or 'the image each test names'}"
