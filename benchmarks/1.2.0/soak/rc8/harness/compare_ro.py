#!/usr/bin/env python3
"""compare_ro.py A B [--out f]: benchmarks/1.2.0/soak/harness/compare.py, unchanged, with READONLY sent on every
connection, so that a cluster replica answers reads instead of MOVED (harmless on a master)."""
import os
import runpy
import sys

import redis

_Redis = redis.Redis


def _readonly(conn):
    conn.on_connect()
    conn.send_command("READONLY")
    conn.read_response()


class _ReadOnlyRedis(_Redis):
    def __init__(self, *a, **k):
        k.setdefault("redis_connect_func", _readonly)
        super().__init__(*a, **k)


redis.Redis = _ReadOnlyRedis
path = os.path.expanduser("~/h/soak/harness/compare.py")
sys.argv = [path] + sys.argv[1:]
runpy.run_path(path, run_name="__main__")
