"""
e2e persistence tests — snapshot, AOF, and index schema survival.

Each test class spins up its own isolated KiviDB process in a temp directory,
performs writes, shuts the server down cleanly, restarts it, and verifies that
all data (keys, indexes, BM25 state, vector graphs) survived across the restart.

Tests cover:
  - Snapshot (SAVE / BGSAVE) for all data types
  - AOF write-ahead log for all data types
  - Index schema persistence (FLAT / HNSW vector, TEXT, TAG, NUMERIC)
  - Native KDB snapshots embed HNSW graph topology (`KIVIHN1`); restart skips
    full graph rebuild when present while still merging TEXT/FLAT state
  - Index backfill correctness after restart
  - Incremental indexing works on the reloaded server
  - BGREWRITEAOF checkpoints dump.kdb and resets the AOF to a delta
  - Multi-database snapshot separation
  - Large-key survival
  - Snapshot + AOF combined (dump first, then AOF on top)
"""
from __future__ import annotations

import os
import struct
import tempfile
import time
import socket
import subprocess
import shutil

import pytest
import redis

# ── Infrastructure ────────────────────────────────────────────────────────────

KIVI_HOST = os.environ.get("KIVI_HOST", "127.0.0.1")

# Resolve the binary to an absolute path so that spawning with cwd=tmpdir works.
# Priority: KIVI_BINARY env var → relative to this file → relative to cwd.
_here = os.path.dirname(os.path.abspath(__file__))
KIVI_BINARY = os.environ.get("KIVI_BINARY", "../../bin/kividb")

_IS_REDIS = any(name in os.path.basename(KIVI_BINARY)
                for name in ("redis-stack-server", "redis-server"))


# Outside the ephemeral range, and never the same port twice (#262).
from utils.helpers import free_port as _free_port  # noqa: E402


def _wait_port(port: int, timeout: float = 30.0) -> bool:
    """Wait until the server accepts connections AND has finished loading its dataset."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            c = redis.Redis(host=KIVI_HOST, port=port,
                            socket_connect_timeout=0.5, socket_timeout=1)
            c.dbsize()  # raises BusyLoadingError during RDB/AOF load
            c.close()
            return True
        except redis.exceptions.BusyLoadingError:
            time.sleep(0.1)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, OSError):
            time.sleep(0.05)
        except Exception:
            time.sleep(0.05)
    return False


def _wait_gone(port: int, timeout: float = 8.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((KIVI_HOST, port), timeout=0.1):
                time.sleep(0.05)
        except OSError:
            return True
    return False


def _start(port: int, workdir: str, extra: list[str] | None = None,
           aof: bool = False) -> subprocess.Popen:
    if _IS_REDIS:
        cmd = [
            KIVI_BINARY,
            "--port", str(port),
            "--loglevel", "warning",
            "--save", "",
            "--appendonly", "yes" if aof else "no",
            "--protected-mode", "no",
            "--dir", workdir,
        ]
    else:
        cmd = [
            KIVI_BINARY,
            "--port", str(port),
            "--loglevel", "error",
            "--io-model", "tokio",
        ]
        if aof:
            cmd.append("--aof")
    cmd += (extra or [])
    proc = subprocess.Popen(cmd, cwd=workdir,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not _wait_port(port):
        proc.terminate()
        pytest.fail(f"server failed to start on port {port}")
    return proc


def _client(port: int, **kw) -> redis.Redis:
    return redis.Redis(host=KIVI_HOST, port=port,
                       decode_responses=True, socket_timeout=5, **kw)


def _bytes_client(port: int) -> redis.Redis:
    return redis.Redis(host=KIVI_HOST, port=port,
                       decode_responses=False, socket_timeout=5)


def _stop(proc: subprocess.Popen, port: int, save: bool = False) -> None:
    """Send SHUTDOWN NOSAVE (or SAVE) and wait for the process to exit."""
    try:
        c = redis.Redis(host=KIVI_HOST, port=port, socket_timeout=2)
        cmd = "SHUTDOWN SAVE" if save else "SHUTDOWN NOSAVE"
        try:
            c.execute_command(*cmd.split())
        except Exception:
            pass
    except Exception:
        pass
    _wait_gone(port)
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def _stop_aof(proc: subprocess.Popen, port: int) -> None:
    """
    For AOF tests: give the async writer time to flush all buffered commands,
    then terminate the process with SIGTERM (letting it flush on exit).
    """
    time.sleep(0.3)  # wait for AOF writer channel to drain
    proc.terminate()
    _wait_gone(port)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def _kill(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def _vec(floats: list[float]) -> bytes:
    return struct.pack(f"<{len(floats)}f", *floats)


def _knn(client: redis.Redis, index: str, vec: list[float],
         k: int, field: str = "emb") -> tuple[int, list[str]]:
    rb = redis.Redis(host=client.connection_pool.connection_kwargs["host"],
                     port=client.connection_pool.connection_kwargs["port"],
                     decode_responses=False, socket_timeout=60)
    deadline = time.monotonic() + 120.0
    raw = None
    while time.monotonic() < deadline:
        try:
            raw = rb.execute_command(
                "FT.SEARCH", index.encode(),
                f"*=>[KNN {k} @{field} $q AS score]".encode(),
                b"PARAMS", b"2", b"q", _vec(vec),
                b"RETURN", b"0",
                b"DIALECT", b"2",
            )
            break
        except redis.ResponseError as e:
            if "LOADING" not in str(e):
                rb.close()
                raise
            time.sleep(0.05)
    if raw is None:
        rb.close()
        raise AssertionError("FT.SEARCH still returned LOADING after wait")
    total = raw[0]
    keys = [raw[i].decode() for i in range(1, len(raw), 2)]
    rb.close()
    return total, keys


def _text_search(client: redis.Redis, index: str, query: str,
                 limit: int = 20) -> tuple[int, list[str]]:
    raw = client.execute_command(
        "FT.SEARCH", index, query,
        "LIMIT", "0", str(limit),
    )
    total = raw[0]
    keys = [raw[i] for i in range(1, len(raw), 2)]
    return total, keys


# ── Snapshot: all scalar types ─────────────────────────────────────────────────

class TestSnapshotScalarTypes:
    """Every native data type survives a SAVE → restart cycle."""

    def test_string_survives_snapshot(self, tmp_path):
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c = _client(port)
            c.set("s:key", "hello snapshot world")
            c.execute_command("SAVE")
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            assert c2.get("s:key") == "hello snapshot world"
        finally:
            _kill(proc2)

    def test_integer_string_survives(self, tmp_path):
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c = _client(port)
            c.set("counter", "42")
            c.execute_command("SAVE")
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            assert c2.get("counter") == "42"
        finally:
            _kill(proc2)

    def test_hash_survives_snapshot(self, tmp_path):
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c = _client(port)
            c.hset("h:user", mapping={"name": "Alice", "age": "30", "city": "Oslo"})
            c.execute_command("SAVE")
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            assert c2.hgetall("h:user") == {"name": "Alice", "age": "30", "city": "Oslo"}
        finally:
            _kill(proc2)

    def test_list_survives_snapshot(self, tmp_path):
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c = _client(port)
            c.rpush("l:items", "a", "b", "c", "d")
            c.execute_command("SAVE")
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            assert c2.lrange("l:items", 0, -1) == ["a", "b", "c", "d"]
        finally:
            _kill(proc2)

    def test_set_survives_snapshot(self, tmp_path):
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c = _client(port)
            c.sadd("set:colors", "red", "green", "blue")
            c.execute_command("SAVE")
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            assert c2.smembers("set:colors") == {"red", "green", "blue"}
        finally:
            _kill(proc2)

    def test_zset_survives_snapshot(self, tmp_path):
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c = _client(port)
            c.zadd("z:scores", {"alice": 10.0, "bob": 20.5, "carol": 5.0})
            c.execute_command("SAVE")
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            result = c2.zrange("z:scores", 0, -1, withscores=True)
            scores = {k: v for k, v in result}
            assert scores == {"alice": 10.0, "bob": 20.5, "carol": 5.0}
        finally:
            _kill(proc2)

    def test_stream_group_and_pel_survive_snapshot(self, tmp_path):
        """Consumer groups and pending entries live in dump.kdb, not only AOF."""
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c = _client(port)
            for i in range(3):
                c.xadd("st", {"f": f"v{i}"})
            c.xgroup_create("st", "g1", "0")
            c.xreadgroup("g1", "c1", {"st": ">"}, count=2)
            c.execute_command("SAVE")
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            assert c2.xlen("st") == 3
            groups = c2.xinfo_groups("st")
            assert groups, "consumer group missing after snapshot restart"
            assert groups[0]["name"] == "g1"
            assert int(groups[0]["pending"]) == 2
        finally:
            _kill(proc2)

    def test_large_value_survives_snapshot(self, tmp_path):
        """A 512 KB string must round-trip through the snapshot."""
        big = "X" * 524288
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c = _client(port)
            c.set("big:val", big)
            c.execute_command("SAVE")
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            assert c2.get("big:val") == big
        finally:
            _kill(proc2)

    def test_many_keys_survive_snapshot(self, tmp_path):
        """1 000 keys all survive a snapshot round-trip."""
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c = _client(port)
            pipe = c.pipeline(transaction=False)
            for i in range(1000):
                pipe.set(f"bulk:{i}", f"value:{i}")
            pipe.execute()
            c.execute_command("SAVE")
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            assert c2.get("bulk:0") == "value:0"
            assert c2.get("bulk:999") == "value:999"
            assert c2.dbsize() == 1000
        finally:
            _kill(proc2)


# ── Snapshot: index persistence ────────────────────────────────────────────────

class TestSnapshotIndexPersistence:
    """FT.* index schemas and live data survive SAVE → restart."""

    def test_flat_vector_index_survives(self, tmp_path):
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            rb = _bytes_client(port)
            rb.execute_command(
                b"FT.CREATE", b"idx:flat",
                b"ON", b"HASH", b"PREFIX", b"1", b"v:",
                b"SCHEMA", b"emb", b"VECTOR", b"FLAT", b"6",
                b"TYPE", b"FLOAT32", b"DIM", b"3", b"DISTANCE_METRIC", b"L2",
            )
            rb.execute_command(b"HSET", b"v:1", b"emb", _vec([1.0, 0.0, 0.0]))
            rb.execute_command(b"HSET", b"v:2", b"emb", _vec([0.0, 1.0, 0.0]))
            rb.execute_command(b"HSET", b"v:3", b"emb", _vec([0.0, 0.0, 1.0]))
            rb.execute_command(b"SAVE")
            rb.close()
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            indexes = c2.execute_command("FT._LIST")
            assert "idx:flat" in indexes, f"idx:flat missing after restart: {indexes}"

            total, keys = _knn(c2, "idx:flat", [1.0, 0.0, 0.0], k=1)
            assert total == 1
            assert keys[0] == "v:1", f"nearest to [1,0,0] should be v:1, got {keys}"
        finally:
            _kill(proc2)

    def test_hnsw_vector_index_survives(self, tmp_path):
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            rb = _bytes_client(port)
            rb.execute_command(
                b"FT.CREATE", b"idx:hnsw",
                b"ON", b"HASH", b"PREFIX", b"1", b"h:",
                b"SCHEMA", b"emb", b"VECTOR", b"HNSW", b"8",
                b"TYPE", b"FLOAT32", b"DIM", b"2",
                b"DISTANCE_METRIC", b"COSINE",
                b"M", b"16",
            )
            # Insert 20 vectors so the HNSW graph is non-trivial
            import math
            for i in range(20):
                angle = 2 * math.pi * i / 20
                rb.execute_command(b"HSET", f"h:{i}".encode(),
                                   b"emb", _vec([math.cos(angle), math.sin(angle)]))
            rb.execute_command(b"SAVE")
            rb.close()
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            # Vector closest to angle=0 → h:0 = [1.0, 0.0]
            total, keys = _knn(c2, "idx:hnsw", [1.0, 0.0], k=1)
            assert total == 1
            assert keys[0] == "h:0", f"nearest to [1,0] should be h:0, got {keys}"
        finally:
            _kill(proc2)

    def test_text_index_survives_and_backfills(self, tmp_path):
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c = _client(port)
            c.execute_command(
                "FT.CREATE", "idx:text",
                "ON", "HASH", "PREFIX", "1", "doc:",
                "SCHEMA", "body", "TEXT",
            )
            c.hset("doc:1", mapping={"body": "rust systems programming performance"})
            c.hset("doc:2", mapping={"body": "python data science machine learning"})
            c.hset("doc:3", mapping={"body": "rust memory safety ownership model"})
            c.execute_command("SAVE")
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)

            total, keys = _text_search(c2, "idx:text", "@body:rust")
            assert total == 2, f"expected 2 rust docs, got {total}: {keys}"
            assert "doc:1" in keys and "doc:3" in keys

            total2, keys2 = _text_search(c2, "idx:text", "@body:python")
            assert total2 == 1
            assert "doc:2" in keys2
        finally:
            _kill(proc2)

    def test_tag_and_numeric_index_survive(self, tmp_path):
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c = _client(port)
            c.execute_command(
                "FT.CREATE", "idx:tagnum",
                "ON", "HASH", "PREFIX", "1", "p:",
                "SCHEMA", "category", "TAG", "price", "NUMERIC",
            )
            c.hset("p:1", mapping={"category": "electronics", "price": "299"})
            c.hset("p:2", mapping={"category": "clothing",    "price": "49"})
            c.hset("p:3", mapping={"category": "electronics", "price": "89"})
            c.execute_command("SAVE")
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)

            total, keys = _text_search(c2, "idx:tagnum", "@category:{electronics}")
            assert total == 2, f"expected 2 electronics, got {total}"
            assert "p:1" in keys and "p:3" in keys

            total2, keys2 = _text_search(c2, "idx:tagnum", "@price:[50 300]")
            assert total2 >= 2, f"expected >=2 in price range, got {total2}"
        finally:
            _kill(proc2)

    def test_mixed_text_vector_index_survives(self, tmp_path):
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            rb = _bytes_client(port)
            rb.execute_command(
                b"FT.CREATE", b"idx:mixed",
                b"ON", b"HASH", b"PREFIX", b"1", b"item:",
                b"SCHEMA",
                b"title", b"TEXT",
                b"emb", b"VECTOR", b"FLAT", b"6",
                b"TYPE", b"FLOAT32", b"DIM", b"2", b"DISTANCE_METRIC", b"L2",
            )
            rb.execute_command(b"HSET", b"item:1",
                               b"title", b"rust programming book",
                               b"emb", _vec([1.0, 0.0]))
            rb.execute_command(b"HSET", b"item:2",
                               b"title", b"python cookbook",
                               b"emb", _vec([0.0, 1.0]))
            rb.execute_command(b"SAVE")
            rb.close()
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)

            # Text search — request only 'title' to avoid raw FLOAT32 vector bytes
            rb2 = _bytes_client(port2)
            raw = rb2.execute_command(
                b"FT.SEARCH", b"idx:mixed", b"@title:rust",
                b"RETURN", b"1", b"title",
                b"LIMIT", b"0", b"10",
            )
            total_t = raw[0]
            keys_t = [raw[i].decode() for i in range(1, len(raw), 2)]
            assert total_t == 1 and "item:1" in keys_t

            # KNN
            total_k, keys_k = _knn(c2, "idx:mixed", [1.0, 0.0], k=1)
            assert total_k == 1 and keys_k[0] == "item:1"
        finally:
            _kill(proc2)

    def test_multiple_indexes_survive_together(self, tmp_path):
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            rb = _bytes_client(port)
            # Index A — FLAT vector
            rb.execute_command(
                b"FT.CREATE", b"idx:A",
                b"ON", b"HASH", b"PREFIX", b"1", b"a:",
                b"SCHEMA", b"vec", b"VECTOR", b"FLAT", b"6",
                b"TYPE", b"FLOAT32", b"DIM", b"2", b"DISTANCE_METRIC", b"L2",
            )
            # Index B — TEXT
            rb.execute_command(
                b"FT.CREATE", b"idx:B",
                b"ON", b"HASH", b"PREFIX", b"1", b"b:",
                b"SCHEMA", b"text", b"TEXT",
            )
            rb.execute_command(b"HSET", b"a:1", b"vec", _vec([1.0, 0.0]))
            rb.execute_command(b"HSET", b"b:1", b"text", b"hello persistence")
            rb.execute_command(b"SAVE")
            rb.close()
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            indexes = set(c2.execute_command("FT._LIST"))
            assert "idx:A" in indexes and "idx:B" in indexes

            total_a, keys_a = _knn(c2, "idx:A", [1.0, 0.0], k=1, field="vec")
            assert total_a == 1 and keys_a[0] == "a:1"

            total_b, keys_b = _text_search(c2, "idx:B", "@text:hello")
            assert total_b == 1 and "b:1" in keys_b
        finally:
            _kill(proc2)

    def test_ft_info_correct_after_restart(self, tmp_path):
        """FT.INFO fields match the schema that was saved."""
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c = _client(port)
            c.execute_command(
                "FT.CREATE", "idx:info",
                "ON", "HASH", "PREFIX", "1", "x:",
                "SCHEMA",
                "title",    "TEXT",
                "category", "TAG",
                "price",    "NUMERIC",
            )
            c.execute_command("SAVE")
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            info = c2.execute_command("FT.INFO", "idx:info")
            # info is a flat list: [key, val, key, val, ...]
            info_dict = {}
            for i in range(0, len(info) - 1, 2):
                k = info[i].decode() if isinstance(info[i], bytes) else info[i]
                info_dict[k] = info[i + 1]

            assert info_dict.get("index_name") == "idx:info"

            # RediSearch stores key_type inside index_definition; KiviDB exposes index_on directly
            index_on = info_dict.get("index_on")
            if index_on is None:
                defn = info_dict.get("index_definition", [])
                if isinstance(defn, (list, tuple)):
                    defn_flat = [d.decode() if isinstance(d, bytes) else d for d in defn]
                    defn_dict = dict(zip(defn_flat[::2], defn_flat[1::2]))
                    index_on = defn_dict.get("key_type", "")
            assert str(index_on).upper() == "HASH", f"expected HASH, got {index_on!r}"

            # num_fields (KiviDB) vs counting attributes list (RediSearch)
            num_fields = info_dict.get("num_fields") or info_dict.get("num_attributes")
            if num_fields is None:
                attrs = info_dict.get("attributes", [])
                if isinstance(attrs, (list, tuple)) and attrs and isinstance(attrs[0], (list, tuple)):
                    num_fields = len(attrs)
            assert int(num_fields) == 3, f"expected 3 fields, got {num_fields}"
        finally:
            _kill(proc2)

    def test_prefix_filter_preserved_after_restart(self, tmp_path):
        """Non-prefix keys are still excluded from search after restart."""
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c = _client(port)
            c.execute_command(
                "FT.CREATE", "idx:prefix",
                "ON", "HASH", "PREFIX", "1", "match:",
                "SCHEMA", "title", "TEXT",
            )
            c.hset("match:1", mapping={"title": "matching document"})
            c.hset("nomatch:1", mapping={"title": "excluded document"})
            c.execute_command("SAVE")
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            total, keys = _text_search(c2, "idx:prefix", "@title:document")
            assert total == 1, f"only match:* prefix should be indexed, got {total}"
            assert "match:1" in keys
            assert "nomatch:1" not in keys
        finally:
            _kill(proc2)

    def test_incremental_indexing_works_after_restart(self, tmp_path):
        """Keys written AFTER restart are indexed by the restored index."""
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c = _client(port)
            c.execute_command(
                "FT.CREATE", "idx:incr",
                "ON", "HASH", "PREFIX", "1", "doc:",
                "SCHEMA", "body", "TEXT",
            )
            c.hset("doc:1", mapping={"body": "written before restart"})
            c.execute_command("SAVE")
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            # Write a new document after restart
            c2.hset("doc:2", mapping={"body": "written after restart"})
            time.sleep(0.05)

            total1, _ = _text_search(c2, "idx:incr", "@body:before")
            assert total1 == 1, "pre-restart doc must be findable"

            total2, _ = _text_search(c2, "idx:incr", "@body:after")
            assert total2 == 1, "post-restart doc must be indexed by restored schema"
        finally:
            _kill(proc2)

    def test_dropindex_works_after_restart(self, tmp_path):
        """FT.DROPINDEX on a restored index removes it from the store."""
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c = _client(port)
            c.execute_command(
                "FT.CREATE", "idx:drop",
                "ON", "HASH", "PREFIX", "1", "d:",
                "SCHEMA", "title", "TEXT",
            )
            c.hset("d:1", mapping={"title": "hello world"})
            c.execute_command("SAVE")
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            # Verify it exists
            assert "idx:drop" in c2.execute_command("FT._LIST")
            # Drop it
            c2.execute_command("FT.DROPINDEX", "idx:drop")
            # Must be gone immediately
            assert "idx:drop" not in c2.execute_command("FT._LIST")
            # Data key must still exist
            assert c2.hget("d:1", "title") == "hello world"
        finally:
            _kill(proc2)

    def test_dropped_index_not_restored_but_data_survives(self, tmp_path):
        """If an index is dropped and then SAVE'd, the next restart has no index."""
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c = _client(port)
            c.execute_command(
                "FT.CREATE", "idx:gone",
                "ON", "HASH", "PREFIX", "1", "g:",
                "SCHEMA", "body", "TEXT",
            )
            c.hset("g:1", mapping={"body": "data must survive"})
            # Drop the index, then save (without index)
            c.execute_command("FT.DROPINDEX", "idx:gone")
            c.execute_command("SAVE")
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            assert "idx:gone" not in c2.execute_command("FT._LIST")
            assert c2.hget("g:1", "body") == "data must survive"
        finally:
            _kill(proc2)


# ── AOF persistence ────────────────────────────────────────────────────────────

class TestAOFPersistence:
    """AOF write-ahead log replays correctly on restart."""

    def _start_aof(self, port: int, workdir: str) -> subprocess.Popen:
        return _start(port, workdir, aof=True)

    def test_string_survives_aof(self, tmp_path):
        port = _free_port()
        proc = self._start_aof(port, str(tmp_path))
        try:
            c = _client(port)
            c.set("aof:key", "aof value")
        finally:
            _stop_aof(proc, port)

        port2 = _free_port()
        proc2 = self._start_aof(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            assert c2.get("aof:key") == "aof value"
        finally:
            _kill(proc2)

    def test_hash_survives_aof(self, tmp_path):
        port = _free_port()
        proc = self._start_aof(port, str(tmp_path))
        try:
            c = _client(port)
            c.hset("aof:hash", mapping={"field1": "val1", "field2": "val2"})
        finally:
            _stop_aof(proc, port)

        port2 = _free_port()
        proc2 = self._start_aof(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            assert c2.hgetall("aof:hash") == {"field1": "val1", "field2": "val2"}
        finally:
            _kill(proc2)

    def test_list_survives_aof(self, tmp_path):
        port = _free_port()
        proc = self._start_aof(port, str(tmp_path))
        try:
            c = _client(port)
            c.rpush("aof:list", "x", "y", "z")
        finally:
            _stop_aof(proc, port)

        port2 = _free_port()
        proc2 = self._start_aof(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            assert c2.lrange("aof:list", 0, -1) == ["x", "y", "z"]
        finally:
            _kill(proc2)

    def test_del_replayed_correctly(self, tmp_path):
        """DEL command is replayed — deleted keys must not appear after restart."""
        port = _free_port()
        proc = self._start_aof(port, str(tmp_path))
        try:
            c = _client(port)
            c.set("aof:del:keep", "kept")
            c.set("aof:del:gone", "will be deleted")
            c.delete("aof:del:gone")
        finally:
            _stop_aof(proc, port)

        port2 = _free_port()
        proc2 = self._start_aof(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            assert c2.get("aof:del:keep") == "kept"
            assert c2.get("aof:del:gone") is None
        finally:
            _kill(proc2)

    def test_many_writes_survive_aof(self, tmp_path):
        """500 HSET commands are all replayed correctly."""
        port = _free_port()
        proc = self._start_aof(port, str(tmp_path))
        try:
            c = _client(port)
            for i in range(500):
                c.set(f"aof:bulk:{i}", f"v:{i}")
        finally:
            _stop_aof(proc, port)

        port2 = _free_port()
        proc2 = self._start_aof(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            assert c2.get("aof:bulk:0") == "v:0"
            assert c2.get("aof:bulk:499") == "v:499"
            assert c2.dbsize() == 500
        finally:
            _kill(proc2)

    def test_bgrewriteaof_includes_ft_create(self, tmp_path):
        """BGREWRITEAOF checkpoints to dump.kdb and resets the AOF to a delta.

        Schemas live in the dump, not as FT.CREATE records in the log. Restart
        must still recover the index from dump-then-AOF.
        """
        port = _free_port()
        proc = self._start_aof(port, str(tmp_path))
        try:
            c = _client(port)
            c.execute_command(
                "FT.CREATE", "idx:aof",
                "ON", "HASH", "PREFIX", "1", "doc:",
                "SCHEMA", "body", "TEXT",
            )
            c.hset("doc:1", mapping={"body": "hello world aof"})
            result = c.execute_command("BGREWRITEAOF")
            assert "background" in str(result).lower() or result, \
                f"BGREWRITEAOF returned unexpected: {result}"
            for _ in range(50):
                info = c.info("persistence")
                if info.get("aof_rewrite_in_progress", 1) == 0:
                    break
                time.sleep(0.1)
        finally:
            _stop(proc, port)

        dump = tmp_path / "dump.kdb"
        aof_file = tmp_path / "appendonly.aof"
        assert dump.exists(), "BGREWRITEAOF must write dump.kdb"
        assert aof_file.exists(), "appendonly.aof not created"
        header = aof_file.read_bytes()[:8]
        assert header == b"KIVIDLT1", f"AOF should be a delta after checkpoint, got {header!r}"

        port2 = _free_port()
        proc2 = self._start_aof(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            names = [n.decode() if isinstance(n, bytes) else n for n in c2.execute_command("FT._LIST")]
            assert "idx:aof" in names, f"idx:aof did not survive dump+AOF recovery: {names}"
            hits = c2.execute_command("FT.SEARCH", "idx:aof", "@body:(hello)", "NOCONTENT")
            assert int(hits[0]) == 1, f"indexed document missing after recovery: {hits}"
        finally:
            _kill(proc2)

    def test_bgrewriteaof_keeps_stream_groups(self, tmp_path):
        """BGREWRITEAOF checkpoint must retain stream groups and PEL after restart."""
        port = _free_port()
        proc = self._start_aof(port, str(tmp_path))
        try:
            c = _client(port)
            for i in range(3):
                c.xadd("st", {"f": f"v{i}"})
            c.xgroup_create("st", "g1", "0")
            c.xreadgroup("g1", "c1", {"st": ">"}, count=2)
            result = c.execute_command("BGREWRITEAOF")
            assert "background" in str(result).lower() or result, \
                f"BGREWRITEAOF returned unexpected: {result}"
            for _ in range(50):
                info = c.info("persistence")
                if info.get("aof_rewrite_in_progress", 1) == 0:
                    break
                time.sleep(0.1)
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = self._start_aof(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            assert c2.xlen("st") == 3
            groups = c2.xinfo_groups("st")
            assert groups, "consumer group missing after BGREWRITEAOF restart"
            assert groups[0]["name"] == "g1"
            assert int(groups[0]["pending"]) == 2
        finally:
            _kill(proc2)

    def test_config_set_appendonly_keeps_existing_and_later_writes(self, tmp_path):
        """CONFIG SET appendonly yes must dump existing keys and log what follows.

        A restart with SHUTDOWN NOSAVE must recover both: the pre-toggle
        dataset from dump.kdb and the post-toggle write from the AOF delta.
        """
        port = _free_port()
        proc = _start(port, str(tmp_path), aof=False)
        try:
            c = _client(port)
            c.set("pre:toggle", "before")
            reply = c.execute_command("CONFIG", "SET", "appendonly", "yes")
            assert str(reply) in ("OK", "True", "true"), f"CONFIG SET appendonly yes -> {reply!r}"
            c.set("post:toggle", "after")
            time.sleep(1.2)
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = self._start_aof(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            assert c2.get("pre:toggle") == "before", \
                "CONFIG SET appendonly yes must checkpoint keys written while AOF was off"
            assert c2.get("post:toggle") == "after", \
                "writes after CONFIG SET appendonly yes must land in the AOF delta"
        finally:
            _kill(proc2)


# ── BGSAVE ────────────────────────────────────────────────────────────────────

class TestBGSave:
    """BGSAVE writes the snapshot in background; data survives restart."""

    def test_bgsave_data_survives(self, tmp_path):
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c = _client(port)
            c.set("bg:key", "bgsave value")
            result = c.bgsave()
            # Wait for background save to finish
            for _ in range(30):
                info = c.info("persistence")
                if info.get("rdb_bgsave_in_progress", 1) == 0:
                    break
                time.sleep(0.1)
        finally:
            _stop(proc, port, save=False)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            assert c2.get("bg:key") == "bgsave value"
        finally:
            _kill(proc2)

    def test_bgsave_index_survives(self, tmp_path):
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c = _client(port)
            c.execute_command(
                "FT.CREATE", "idx:bg",
                "ON", "HASH", "PREFIX", "1", "bg:",
                "SCHEMA", "title", "TEXT",
            )
            c.hset("bg:1", mapping={"title": "background save test"})
            c.bgsave()
            for _ in range(30):
                info = c.info("persistence")
                if info.get("rdb_bgsave_in_progress", 1) == 0:
                    break
                time.sleep(0.1)
        finally:
            _stop(proc, port, save=False)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            assert "idx:bg" in c2.execute_command("FT._LIST")
            total, keys = _text_search(c2, "idx:bg", "@title:background")
            assert total == 1 and "bg:1" in keys
        finally:
            _kill(proc2)


# ── Multi-DB snapshot ─────────────────────────────────────────────────────────

class TestMultiDBSnapshot:
    """Keys in different logical databases survive the snapshot independently."""

    def test_two_databases_survive(self, tmp_path):
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c0 = _client(port)
            c0.set("db0:key", "database zero")

            c1 = redis.Redis(host=KIVI_HOST, port=port,
                             decode_responses=True, db=1)
            c1.set("db1:key", "database one")
            c1.close()

            c0.execute_command("SAVE")
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c0b = _client(port2)
            assert c0b.get("db0:key") == "database zero"

            c1b = redis.Redis(host=KIVI_HOST, port=port2,
                              decode_responses=True, db=1)
            try:
                assert c1b.get("db1:key") == "database one"
            finally:
                c1b.close()
        finally:
            _kill(proc2)

    def test_db0_keys_not_visible_in_db1(self, tmp_path):
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c0 = _client(port)
            c0.set("shared:name", "in db0 only")
            c0.execute_command("SAVE")
        finally:
            _stop(proc, port)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c1 = redis.Redis(host=KIVI_HOST, port=port2,
                             decode_responses=True, db=1)
            try:
                assert c1.get("shared:name") is None, \
                    "db0 key must not bleed into db1 after restart"
            finally:
                c1.close()
        finally:
            _kill(proc2)


# ── Shutdown modes ────────────────────────────────────────────────────────────

class TestShutdownModes:
    """SHUTDOWN SAVE persists; SHUTDOWN NOSAVE uses only previously saved data."""

    def test_shutdown_save_persists(self, tmp_path):
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c = _client(port)
            c.set("shutdown:save:key", "persisted by shutdown save")
            try:
                c.execute_command("SHUTDOWN", "SAVE")
            except Exception:
                pass
        finally:
            _wait_gone(port)
            _kill(proc)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            assert c2.get("shutdown:save:key") == "persisted by shutdown save"
        finally:
            _kill(proc2)

    def test_shutdown_nosave_uses_last_save(self, tmp_path):
        """
        Write key A, SAVE, write key B, SHUTDOWN NOSAVE.
        After restart only key A should exist (key B was not saved).
        """
        port = _free_port()
        proc = _start(port, str(tmp_path))
        try:
            c = _client(port)
            c.set("ns:A", "saved")
            c.execute_command("SAVE")
            c.set("ns:B", "not saved")
            try:
                c.execute_command("SHUTDOWN", "NOSAVE")
            except Exception:
                pass
        finally:
            _wait_gone(port)
            _kill(proc)

        port2 = _free_port()
        proc2 = _start(port2, str(tmp_path))
        try:
            c2 = _client(port2)
            assert c2.get("ns:A") == "saved"
            assert c2.get("ns:B") is None, \
                "NOSAVE must not persist writes after the last explicit SAVE"
        finally:
            _kill(proc2)
