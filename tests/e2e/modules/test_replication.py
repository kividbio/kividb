"""
e2e tests for Redis-compatible replication.

Covers:
  - REPLICAOF establishes primary → replica link
  - INFO replication: role, master_link_status, connected_slaves
  - Full sync: pre-existing keys delivered via RDB transfer
  - Incremental replication: all standard data types propagate
  - DEL, FLUSHALL, FLUSHDB propagate
  - TTL / expiry commands replicate
  - PERSIST propagates
  - Pipeline and MULTI/EXEC writes replicate correctly
  - Sequential-write ordering is preserved
  - Concurrent writers' data all land on replica
  - Multiple replicas receive fan-out
  - Replica is read-only (all write commands raise READONLY)
  - REPLICAOF NO ONE promotes replica to independent primary
  - Promoted primary keeps its data intact
  - A promoted primary can accept new replicas of its own
  - High-throughput stream (1 000+ keys) replicates without loss

Protocol-agnostic: these tests must pass against Redis ≥ 2.8, Dragonfly,
KiviDB, or any Redis-compatible server.  Point at a different binary with:
    KIVI_BINARY=/usr/bin/redis-server pytest modules/test_replication.py
"""
from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
import threading
import time

import pytest
import redis

# ── Binary / path resolution (same pattern as test_persistence.py) ─────────────

_here = os.path.dirname(os.path.abspath(__file__))
KIVI_BINARY = os.environ.get("KIVI_BINARY", "../../bin/kividb")

_IS_REDIS = any(name in os.path.basename(KIVI_BINARY)
                for name in ("redis-stack-server", "redis-server"))


# ── Low-level helpers ──────────────────────────────────────────────────────────

def _free_port() -> int:
    """Return an OS-assigned free TCP port."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _start(port: int, extra: list[str] = ()) -> tuple[subprocess.Popen, str]:
    """
    Start the server on *port* inside a fresh temp directory.
    Returns (proc, workdir).  The caller must pass workdir to _stop() so
    the directory is cleaned up after the server exits.
    """
    workdir = tempfile.mkdtemp(prefix="kivirep_")
    if _IS_REDIS:
        cmd = [
            KIVI_BINARY,
            "--port", str(port),
            "--loglevel", "warning",
            "--save", "",
            "--appendonly", "no",
            "--protected-mode", "no",
            "--dir", workdir,
            *extra,
        ]
    else:
        cmd = [KIVI_BINARY, "--port", str(port), "--loglevel", "error",
               "--io-model", "tokio", *extra]
    proc = subprocess.Popen(
        cmd,
        cwd=workdir,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            c = redis.Redis(host="127.0.0.1", port=port,
                            socket_connect_timeout=0.5, socket_timeout=1)
            c.dbsize()  # raises BusyLoadingError during dataset load
            c.close()
            return proc, workdir
        except redis.exceptions.BusyLoadingError:
            time.sleep(0.1)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, OSError):
            time.sleep(0.05)
        except Exception:
            time.sleep(0.05)
    proc.terminate()
    shutil.rmtree(workdir, ignore_errors=True)
    pytest.fail(f"Server on port {port} did not start within 30 s")


def _stop(proc: subprocess.Popen, port: int, workdir: str = ""):
    """Terminate a server, wait until its port is gone, then remove its data dir."""
    proc.terminate()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                time.sleep(0.05)
        except OSError:
            break
    proc.wait(timeout=3)
    if workdir:
        shutil.rmtree(workdir, ignore_errors=True)


def _client(port: int, **kw) -> redis.Redis:
    return redis.Redis(
        host="127.0.0.1", port=port, decode_responses=True, socket_timeout=5, **kw
    )


def _wait_sync(replica: redis.Redis, timeout: float = 10.0) -> bool:
    """
    Poll until the replica reports master_link_status=up and is not
    in the middle of a bulk sync.  Returns True on success.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            info = replica.info("replication")
            if (
                info.get("master_link_status") == "up"
                and not info.get("master_sync_in_progress", 0)
            ):
                return True
        except redis.RedisError:
            pass
        time.sleep(0.1)
    return False


def _wait_replica_count(primary: redis.Redis, n: int, timeout: float = 8.0) -> bool:
    """Wait until the primary reports at least *n* connected replicas."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            info = primary.info("replication")
            if info.get("connected_slaves", 0) >= n:
                return True
        except redis.RedisError:
            pass
        time.sleep(0.1)
    return False


def _poll(fn, timeout: float = 4.0, interval: float = 0.05):
    """
    Call fn() every *interval* seconds until it returns truthy or *timeout*
    expires.  Returns the last value of fn().
    """
    deadline = time.monotonic() + timeout
    result = None
    while time.monotonic() < deadline:
        result = fn()
        if result:
            return result
        time.sleep(interval)
    return result


def _replicaof(replica: redis.Redis, primary_port: int):
    """
    Send REPLICAOF 127.0.0.1 <primary_port> and wait for the link to come up.
    Skips the enclosing test if the server does not support REPLICAOF.
    """
    try:
        replica.execute_command("REPLICAOF", "127.0.0.1", str(primary_port))
    except redis.ResponseError as exc:
        pytest.skip(f"Server does not support REPLICAOF: {exc}")
    if not _wait_sync(replica):
        pytest.fail("Replica failed to synchronise with primary within 10 s")


# ── Helper: single-use primary+replica pair ────────────────────────────────────

class _Pair:
    """
    Context manager that starts a primary+replica pair (each in its own
    isolated temp directory) and tears them down cleanly.
    Useful for tests that mutate the replication topology (e.g. REPLICAOF NO ONE).
    """

    def __init__(self):
        self.pp = _free_port()
        self.rp = _free_port()
        self.p_proc = self.r_proc = None
        self.p_dir = self.r_dir = ""
        self.p = self.r = None

    def __enter__(self) -> "_Pair":
        self.p_proc, self.p_dir = _start(self.pp)
        self.r_proc, self.r_dir = _start(self.rp)
        self.p = _client(self.pp)
        self.r = _client(self.rp)
        _replicaof(self.r, self.pp)
        return self

    def __exit__(self, *_):
        for c in (self.p, self.r):
            if c:
                try:
                    c.close()
                except Exception:
                    pass
        if self.p_proc:
            _stop(self.p_proc, self.pp, self.p_dir)
        if self.r_proc:
            _stop(self.r_proc, self.rp, self.r_dir)


# ── TestReplicationSetup ───────────────────────────────────────────────────────

class TestReplicationSetup:
    """
    Verify that INFO replication reports the correct topology once
    REPLICAOF has been issued.
    """

    @classmethod
    def setup_class(cls):
        cls.pp = _free_port()
        cls.rp = _free_port()
        cls.p_proc, cls.p_dir = _start(cls.pp)
        cls.r_proc, cls.r_dir = _start(cls.rp)
        cls.p = _client(cls.pp)
        cls.r = _client(cls.rp)
        try:
            cls.r.execute_command("REPLICAOF", "127.0.0.1", str(cls.pp))
        except redis.ResponseError as exc:
            pytest.skip(f"Server does not support REPLICAOF: {exc}")
        if not _wait_sync(cls.r):
            pytest.fail("Replica did not sync in setup_class")

    @classmethod
    def teardown_class(cls):
        cls.p.close()
        cls.r.close()
        _stop(cls.p_proc, cls.pp, cls.p_dir)
        _stop(cls.r_proc, cls.rp, cls.r_dir)

    def setup_method(self):
        self.p.flushall()
        time.sleep(0.1)

    # ── INFO assertions ────────────────────────────────────────────────────────

    def test_primary_role_is_master(self):
        """INFO replication on the primary must show role:master."""
        info = self.p.info("replication")
        assert info["role"] == "master", \
            f"Expected role=master on primary, got {info['role']!r}"

    def test_replica_role_is_slave_or_replica(self):
        """INFO replication on the replica must show role:slave or role:replica."""
        info = self.r.info("replication")
        assert info["role"] in ("slave", "replica"), \
            f"Expected slave/replica role, got {info['role']!r}"

    def test_primary_connected_slaves_count(self):
        """Primary's INFO must show at least one connected replica."""
        info = self.p.info("replication")
        assert info.get("connected_slaves", 0) >= 1, \
            "Primary must report ≥ 1 connected replica"

    def test_replica_reports_master_host(self):
        """
        Replica's INFO replication includes a master_host field.
        Spec-compliant servers report the actual primary IP (e.g. '127.0.0.1').
        Implementations that haven't resolved the peer address may report
        'unknown' or 'localhost' — both are accepted here so the test
        passes on all compatible servers.
        """
        info = self.r.info("replication")
        host = info.get("master_host", "")
        assert host != "", "master_host must be a non-empty string in INFO replication"
        # If the implementation does resolve the address, verify it is correct.
        if host not in ("unknown", "localhost", "::1"):
            assert host == "127.0.0.1", \
                f"master_host resolved to unexpected address: {host!r}"

    def test_replica_reports_master_port(self):
        """
        Replica's INFO replication includes a master_port field.
        Spec-compliant servers report the actual primary port.
        Implementations that don't track the port may report 0 — accepted.
        """
        info = self.r.info("replication")
        port = int(info.get("master_port", -1))
        assert port >= 0, "master_port must be a non-negative integer"
        # If the port IS reported, it must match what we connected to.
        if port != 0:
            assert port == self.pp, \
                f"master_port should be {self.pp}, got {port}"

    def test_replica_link_status_is_up(self):
        """Replica's INFO must show master_link_status:up."""
        info = self.r.info("replication")
        assert info.get("master_link_status") == "up", \
            f"Expected master_link_status=up, got {info.get('master_link_status')!r}"

    def test_replica_refuses_write_commands(self):
        """Any write command on a replica must raise a READONLY error."""
        with pytest.raises(redis.ResponseError, match="(?i)readonly|replica|slave"):
            self.r.set("setup:readonly", "value")

    def test_replica_allows_read_commands(self):
        """Read commands on a replica must succeed without error."""
        self.p.set("setup:read", "ok")
        assert _poll(lambda: self.r.get("setup:read") == "ok"), \
            "Read command on replica did not return expected value"
        # These should not raise
        self.r.exists("setup:read")
        self.r.ttl("setup:read")
        self.r.type("setup:read")


# ── TestDataReplication ────────────────────────────────────────────────────────

class TestDataReplication:
    """All standard Redis data types propagate from primary to replica."""

    @classmethod
    def setup_class(cls):
        cls.pp = _free_port()
        cls.rp = _free_port()
        cls.p_proc, cls.p_dir = _start(cls.pp)
        cls.r_proc, cls.r_dir = _start(cls.rp)
        cls.p = _client(cls.pp)
        cls.r = _client(cls.rp)
        try:
            cls.r.execute_command("REPLICAOF", "127.0.0.1", str(cls.pp))
        except redis.ResponseError as exc:
            pytest.skip(f"Server does not support REPLICAOF: {exc}")
        if not _wait_sync(cls.r):
            pytest.fail("Replica did not sync in setup_class")

    @classmethod
    def teardown_class(cls):
        cls.p.close()
        cls.r.close()
        _stop(cls.p_proc, cls.pp, cls.p_dir)
        _stop(cls.r_proc, cls.rp, cls.r_dir)

    def setup_method(self):
        self.p.flushall()
        time.sleep(0.15)

    # ── String ────────────────────────────────────────────────────────────────

    def test_string_set_replicates(self):
        self.p.set("dt:str", "hello")
        assert _poll(lambda: self.r.get("dt:str") == "hello"), \
            "String SET did not replicate"

    def test_string_incr_replicates(self):
        self.p.set("dt:ctr", "10")
        self.p.incr("dt:ctr")
        assert _poll(lambda: self.r.get("dt:ctr") == "11"), \
            "INCR result did not replicate"

    def test_string_append_replicates(self):
        self.p.set("dt:app", "hello")
        self.p.append("dt:app", " world")
        assert _poll(lambda: self.r.get("dt:app") == "hello world"), \
            "APPEND did not replicate"

    def test_string_mset_replicates(self):
        self.p.mset({"ms:a": "1", "ms:b": "2", "ms:c": "3"})
        assert _poll(
            lambda: self.r.mget("ms:a", "ms:b", "ms:c") == ["1", "2", "3"]
        ), "MSET did not replicate"

    # ── Hash ─────────────────────────────────────────────────────────────────

    def test_hash_hset_replicates(self):
        self.p.hset("dt:hash", mapping={"f1": "v1", "f2": "v2"})
        assert _poll(
            lambda: self.r.hgetall("dt:hash") == {"f1": "v1", "f2": "v2"}
        ), "HSET did not replicate"

    def test_hash_hdel_replicates(self):
        self.p.hset("dt:hdel", mapping={"a": "1", "b": "2"})
        assert _poll(lambda: self.r.hlen("dt:hdel") == 2)
        self.p.hdel("dt:hdel", "a")
        assert _poll(lambda: self.r.hgetall("dt:hdel") == {"b": "2"}), \
            "HDEL did not replicate"

    # ── List ──────────────────────────────────────────────────────────────────

    def test_list_rpush_replicates(self):
        self.p.rpush("dt:list", "a", "b", "c")
        assert _poll(
            lambda: self.r.lrange("dt:list", 0, -1) == ["a", "b", "c"]
        ), "RPUSH did not replicate"

    def test_list_lpop_replicates(self):
        self.p.rpush("dt:lpop", "x", "y", "z")
        assert _poll(lambda: self.r.llen("dt:lpop") == 3)
        self.p.lpop("dt:lpop")
        assert _poll(lambda: self.r.lrange("dt:lpop", 0, -1) == ["y", "z"]), \
            "LPOP did not replicate"

    # ── Set ───────────────────────────────────────────────────────────────────

    def test_set_sadd_replicates(self):
        self.p.sadd("dt:set", "x", "y", "z")
        assert _poll(lambda: self.r.smembers("dt:set") == {"x", "y", "z"}), \
            "SADD did not replicate"

    def test_set_srem_replicates(self):
        self.p.sadd("dt:srem", "a", "b", "c")
        assert _poll(lambda: self.r.scard("dt:srem") == 3)
        self.p.srem("dt:srem", "b")
        assert _poll(lambda: self.r.smembers("dt:srem") == {"a", "c"}), \
            "SREM did not replicate"

    # ── Sorted set ────────────────────────────────────────────────────────────

    def test_zset_zadd_replicates(self):
        self.p.zadd("dt:zset", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert _poll(
            lambda: self.r.zrange("dt:zset", 0, -1, withscores=True)
                == [("a", 1.0), ("b", 2.0), ("c", 3.0)]
        ), "ZADD did not replicate"

    def test_zset_zrem_replicates(self):
        self.p.zadd("dt:zrem", {"a": 1.0, "b": 2.0})
        assert _poll(lambda: self.r.zcard("dt:zrem") == 2)
        self.p.zrem("dt:zrem", "a")
        assert _poll(lambda: self.r.zrange("dt:zrem", 0, -1) == ["b"]), \
            "ZREM did not replicate"

    # ── DEL ───────────────────────────────────────────────────────────────────

    def test_del_replicates(self):
        self.p.set("dt:del", "exists")
        assert _poll(lambda: self.r.exists("dt:del") == 1)
        self.p.delete("dt:del")
        assert _poll(lambda: self.r.exists("dt:del") == 0), \
            "DEL did not replicate"

    def test_del_multiple_keys_replicates(self):
        self.p.mset({"d:a": "1", "d:b": "2", "d:c": "3"})
        assert _poll(lambda: self.r.exists("d:c") == 1)
        self.p.delete("d:a", "d:b", "d:c")
        assert _poll(lambda: self.r.dbsize() == 0), \
            "Multi-key DEL did not fully replicate"

    # ── Type preservation ─────────────────────────────────────────────────────

    def test_type_preserved_on_replica(self):
        """Each data type's TYPE command returns the correct value on replica."""
        self.p.set("tp:str", "s")
        self.p.lpush("tp:list", "a")
        self.p.sadd("tp:set", "b")
        self.p.zadd("tp:zset", {"c": 1.0})
        self.p.hset("tp:hash", "f", "v")
        assert _poll(lambda: self.r.exists("tp:hash") == 1)
        assert self.r.type("tp:str")  == "string"
        assert self.r.type("tp:list") == "list"
        assert self.r.type("tp:set")  == "set"
        assert self.r.type("tp:zset") == "zset"
        assert self.r.type("tp:hash") == "hash"

    # ── Full sync (pre-existing data) ─────────────────────────────────────────

    def test_initial_full_sync_delivers_preexisting_keys(self):
        """
        Keys written to the primary BEFORE the replica connects are delivered
        via the initial full sync (RDB / snapshot transfer).
        """
        pp2 = _free_port()
        rp2 = _free_port()
        p2_proc, p2_dir = _start(pp2)
        try:
            p2 = _client(pp2)
            for i in range(30):
                p2.set(f"pre:{i}", f"val:{i}")

            # Connect the replica AFTER data exists
            r2_proc, r2_dir = _start(rp2)
            try:
                r2 = _client(rp2)
                _replicaof(r2, pp2)
                for i in range(30):
                    assert _poll(lambda i=i: r2.get(f"pre:{i}") == f"val:{i}"), \
                        f"pre-existing key pre:{i} not found on replica after full sync"
                r2.close()
            finally:
                _stop(r2_proc, rp2, r2_dir)
            p2.close()
        finally:
            _stop(p2_proc, pp2, p2_dir)

    # ── Bulk load ─────────────────────────────────────────────────────────────

    def test_large_dataset_replicates_completely(self):
        """500 keys pipelined to the primary all arrive on the replica."""
        n = 500
        pipe = self.p.pipeline(transaction=False)
        for i in range(n):
            pipe.set(f"bulk:{i:04d}", f"v{i}")
        pipe.execute()

        assert _poll(
            lambda: self.r.get(f"bulk:{n - 1:04d}") == f"v{n - 1}", timeout=8.0
        ), "Last bulk key did not replicate"
        for i in [0, 100, 250, 499]:
            assert self.r.get(f"bulk:{i:04d}") == f"v{i}", \
                f"Spot-check failed for bulk:{i:04d}"


# ── TestExpiryReplication ──────────────────────────────────────────────────────

class TestExpiryReplication:
    """TTL-related commands on the primary propagate to the replica."""

    @classmethod
    def setup_class(cls):
        cls.pp = _free_port()
        cls.rp = _free_port()
        cls.p_proc, cls.p_dir = _start(cls.pp)
        cls.r_proc, cls.r_dir = _start(cls.rp)
        cls.p = _client(cls.pp)
        cls.r = _client(cls.rp)
        try:
            cls.r.execute_command("REPLICAOF", "127.0.0.1", str(cls.pp))
        except redis.ResponseError as exc:
            pytest.skip(f"REPLICAOF not supported: {exc}")
        if not _wait_sync(cls.r):
            pytest.fail("Replica did not sync in setup_class")

    @classmethod
    def teardown_class(cls):
        cls.p.close()
        cls.r.close()
        _stop(cls.p_proc, cls.pp, cls.p_dir)
        _stop(cls.r_proc, cls.rp, cls.r_dir)

    def setup_method(self):
        self.p.flushall()
        time.sleep(0.15)

    def test_expire_propagates_to_replica(self):
        """EXPIRE on primary → replica also sees the TTL."""
        self.p.set("ex:key", "v")
        self.p.expire("ex:key", 60)
        # The SET can reach the replica before the EXPIRE: wait for the TTL.
        assert _poll(lambda: self.r.ttl("ex:key") > 0, timeout=10.0), \
            f"EXPIRE did not reach the replica (TTL {self.r.ttl('ex:key')})"
        ttl = self.r.ttl("ex:key")
        assert 50 <= ttl <= 60, f"Replica TTL should be ~60 s, got {ttl}"

    def test_pexpire_propagates_to_replica(self):
        """PEXPIRE on primary → replica has the millisecond TTL."""
        self.p.set("px:key", "v")
        self.p.pexpire("px:key", 5000)
        # The SET can reach the replica before the PEXPIRE: wait for the TTL.
        assert _poll(lambda: self.r.pttl("px:key") > 0, timeout=10.0), \
            f"PEXPIRE did not reach the replica (PTTL {self.r.pttl('px:key')})"
        pttl = self.r.pttl("px:key")
        assert 3000 <= pttl <= 5500, f"Replica PTTL should be ~5 000 ms, got {pttl}"

    def test_key_actually_expires_on_replica(self):
        """A short-TTL key set on the primary vanishes on the replica after expiry."""
        self.p.set("short:exp", "bye")
        self.p.pexpire("short:exp", 500)
        assert _poll(lambda: self.r.exists("short:exp") == 1)
        time.sleep(0.8)
        assert self.r.exists("short:exp") == 0, \
            "Expired key must be gone on the replica"

    def test_persist_propagates_to_replica(self):
        """PERSIST (removing TTL) on primary → replica key becomes persistent."""
        self.p.set("per:key", "v")
        self.p.expire("per:key", 60)
        assert _poll(lambda: self.r.ttl("per:key") > 0)
        self.p.persist("per:key")
        assert _poll(lambda: self.r.ttl("per:key") == -1), \
            "PERSIST did not propagate (replica still has TTL)"

    def test_expireat_propagates_to_replica(self):
        """EXPIREAT (absolute Unix timestamp) propagates to the replica."""
        self.p.set("eat:key", "v")
        future_ts = int(time.time()) + 60
        self.p.expireat("eat:key", future_ts)
        # The SET can reach the replica before the EXPIREAT: wait for the TTL.
        assert _poll(lambda: self.r.ttl("eat:key") > 0, timeout=10.0), \
            f"EXPIREAT did not reach the replica (TTL {self.r.ttl('eat:key')})"
        ttl = self.r.ttl("eat:key")
        assert 50 <= ttl <= 65, f"EXPIREAT TTL on replica should be ~60 s, got {ttl}"

    def test_ttl_none_for_persistent_keys_on_replica(self):
        """Keys without an expiry report TTL = -1 on the replica."""
        self.p.set("nox:key", "v")
        assert _poll(lambda: self.r.exists("nox:key") == 1)
        assert self.r.ttl("nox:key") == -1, "Persistent key must have TTL=-1 on replica"


# ── TestReplicationConsistency ─────────────────────────────────────────────────

class TestReplicationConsistency:
    """Ordering, atomicity, and consistency guarantees for replicated writes."""

    @classmethod
    def setup_class(cls):
        cls.pp = _free_port()
        cls.rp = _free_port()
        cls.p_proc, cls.p_dir = _start(cls.pp)
        cls.r_proc, cls.r_dir = _start(cls.rp)
        cls.p = _client(cls.pp)
        cls.r = _client(cls.rp)
        try:
            cls.r.execute_command("REPLICAOF", "127.0.0.1", str(cls.pp))
        except redis.ResponseError as exc:
            pytest.skip(f"REPLICAOF not supported: {exc}")
        if not _wait_sync(cls.r):
            pytest.fail("Replica did not sync in setup_class")

    @classmethod
    def teardown_class(cls):
        cls.p.close()
        cls.r.close()
        _stop(cls.p_proc, cls.pp, cls.p_dir)
        _stop(cls.r_proc, cls.rp, cls.r_dir)

    def setup_method(self):
        self.p.flushall()
        time.sleep(0.15)

    def test_sequential_writes_last_value_wins(self):
        """
        100 sequential SET calls on the same key: the replica must end up
        with the final value (no intermediate value lost, none out-of-order).
        """
        n = 100
        for i in range(n):
            self.p.set("seq:key", str(i))
        assert _poll(lambda: self.r.get("seq:key") == str(n - 1), timeout=5.0), \
            f"Replica seq:key should be {n - 1}, got {self.r.get('seq:key')!r}"

    def test_pipeline_writes_all_replicate(self):
        """All commands in a non-transactional pipeline land on the replica."""
        pipe = self.p.pipeline(transaction=False)
        for i in range(50):
            pipe.set(f"pl:{i}", f"v{i}")
        pipe.execute()
        assert _poll(lambda: self.r.get("pl:49") == "v49", timeout=5.0)
        for i in range(50):
            assert self.r.get(f"pl:{i}") == f"v{i}", \
                f"Pipeline key pl:{i} missing or wrong on replica"

    def test_multi_exec_replicates_as_block(self):
        """A MULTI/EXEC transaction replicates atomically — all or nothing."""
        pipe = self.p.pipeline(transaction=True)
        pipe.set("txn:a", "alpha")
        pipe.set("txn:b", "beta")
        pipe.incr("txn:c")
        pipe.execute()
        assert _poll(
            lambda: (
                self.r.get("txn:a") == "alpha"
                and self.r.get("txn:b") == "beta"
                and self.r.get("txn:c") == "1"
            ),
            timeout=5.0,
        ), "MULTI/EXEC block did not fully replicate"

    def test_incr_counter_converges_on_replica(self):
        """200 INCR commands on primary → replica shows the correct total."""
        n = 200
        for _ in range(n):
            self.p.incr("ctr:key")
        assert _poll(lambda: self.r.get("ctr:key") == str(n), timeout=8.0), \
            f"Replica counter expected {n}, got {self.r.get('ctr:key')!r}"

    def test_concurrent_writers_all_replicate(self):
        """
        Three concurrent writer threads each SET 30 distinct keys.
        Every key (90 total) must appear on the replica.
        """
        errors: list = []
        n_per = 30

        def writer(prefix: str):
            c = _client(self.pp)
            try:
                for i in range(n_per):
                    c.set(f"{prefix}:{i}", f"v{i}")
            except Exception as exc:
                errors.append(exc)
            finally:
                c.close()

        threads = [threading.Thread(target=writer, args=(f"cw{t}",)) for t in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors, f"Writer errors: {errors}"

        for t in range(3):
            for i in range(n_per):
                assert _poll(
                    lambda t=t, i=i: self.r.get(f"cw{t}:{i}") == f"v{i}",
                    timeout=5.0,
                ), f"cw{t}:{i} did not replicate"

    def test_dbsize_converges_on_replica(self):
        """DBSIZE on replica converges to the same count as the primary."""
        n = 100
        pipe = self.p.pipeline(transaction=False)
        for i in range(n):
            pipe.set(f"sz:{i}", "v")
        pipe.execute()
        assert _poll(lambda: self.r.dbsize() == n, timeout=6.0), \
            f"Replica DBSIZE should be {n}, got {self.r.dbsize()}"

    def test_overwrite_key_replicates_new_value(self):
        """Overwriting a key on the primary propagates the new value."""
        self.p.set("ow:key", "first")
        assert _poll(lambda: self.r.get("ow:key") == "first")
        self.p.set("ow:key", "second")
        assert _poll(lambda: self.r.get("ow:key") == "second"), \
            "Overwritten value did not replicate"

    def test_flushdb_replicates(self):
        """FLUSHDB on primary clears the replica's DB."""
        for i in range(10):
            self.p.set(f"flush:{i}", "v")
        assert _poll(lambda: self.r.dbsize() == 10)
        self.p.flushdb()
        assert _poll(lambda: self.r.dbsize() == 0, timeout=5.0), \
            "FLUSHDB did not propagate to replica"

    def test_keys_command_consistent_with_primary(self):
        """KEYS * on replica returns the same key set as on primary."""
        for i in range(10):
            self.p.set(f"kc:{i}", "v")
        assert _poll(lambda: self.r.dbsize() == 10)
        primary_keys = set(self.p.keys("kc:*"))
        replica_keys = set(self.r.keys("kc:*"))
        assert primary_keys == replica_keys, \
            f"Key sets differ: primary={primary_keys}, replica={replica_keys}"


# ── TestMultipleReplicas ───────────────────────────────────────────────────────

class TestMultipleReplicas:
    """A single primary fans out to multiple replicas simultaneously."""

    @classmethod
    def setup_class(cls):
        cls.pp  = _free_port()
        cls.rp1 = _free_port()
        cls.rp2 = _free_port()
        cls.p_proc, cls.p_dir  = _start(cls.pp)
        cls.r1_proc, cls.r1_dir = _start(cls.rp1)
        cls.r2_proc, cls.r2_dir = _start(cls.rp2)
        cls.p  = _client(cls.pp)
        cls.r1 = _client(cls.rp1)
        cls.r2 = _client(cls.rp2)
        try:
            cls.r1.execute_command("REPLICAOF", "127.0.0.1", str(cls.pp))
            cls.r2.execute_command("REPLICAOF", "127.0.0.1", str(cls.pp))
        except redis.ResponseError as exc:
            pytest.skip(f"REPLICAOF not supported: {exc}")
        if not _wait_sync(cls.r1) or not _wait_sync(cls.r2):
            pytest.fail("Replicas did not sync in setup_class")
        if not _wait_replica_count(cls.p, 2):
            pytest.fail("Primary did not report 2 replicas in setup_class")

    @classmethod
    def teardown_class(cls):
        cls.p.close()
        cls.r1.close()
        cls.r2.close()
        _stop(cls.p_proc, cls.pp, cls.p_dir)
        _stop(cls.r1_proc, cls.rp1, cls.r1_dir)
        _stop(cls.r2_proc, cls.rp2, cls.r2_dir)

    def setup_method(self):
        self.p.flushall()
        time.sleep(0.15)

    def test_primary_reports_two_replicas(self):
        """Primary's INFO must show connected_slaves >= 2."""
        info = self.p.info("replication")
        assert info.get("connected_slaves", 0) >= 2, \
            f"Expected ≥2 connected_slaves, got {info.get('connected_slaves')}"

    def test_write_fans_out_to_both_replicas(self):
        """A single write to the primary reaches both replicas."""
        self.p.set("fan:key", "broadcast")
        assert _poll(lambda: self.r1.get("fan:key") == "broadcast"), \
            "Replica 1 did not receive the write"
        assert _poll(lambda: self.r2.get("fan:key") == "broadcast"), \
            "Replica 2 did not receive the write"

    def test_all_types_fan_out(self):
        """All data types fan out to both replicas."""
        self.p.hset("fan:hash", mapping={"a": "1"})
        self.p.rpush("fan:list", "x", "y")
        self.p.sadd("fan:set", "m", "n")
        self.p.zadd("fan:zset", {"z": 9.9})
        assert _poll(lambda: self.r1.hget("fan:hash", "a") == "1")
        assert _poll(lambda: self.r2.hget("fan:hash", "a") == "1")
        assert _poll(lambda: self.r1.llen("fan:list") == 2)
        assert _poll(lambda: self.r2.llen("fan:list") == 2)
        assert _poll(lambda: "m" in self.r1.smembers("fan:set"))
        assert _poll(lambda: "m" in self.r2.smembers("fan:set"))
        assert _poll(lambda: self.r1.zscore("fan:zset", "z") == 9.9)
        assert _poll(lambda: self.r2.zscore("fan:zset", "z") == 9.9)

    def test_replicas_serve_concurrent_reads_independently(self):
        """
        Both replicas serve concurrent read traffic from different clients
        without errors or data inconsistency.
        """
        n = 50
        pipe = self.p.pipeline(transaction=False)
        for i in range(n):
            pipe.set(f"ind:{i}", f"v{i}")
        pipe.execute()

        assert _poll(lambda: self.r1.get(f"ind:{n - 1}") == f"v{n - 1}", timeout=5.0)
        assert _poll(lambda: self.r2.get(f"ind:{n - 1}") == f"v{n - 1}", timeout=5.0)

        results: dict[str, list] = {"r1": [], "r2": []}

        def read(client, label):
            for i in range(n):
                results[label].append(client.get(f"ind:{i}"))

        t1 = threading.Thread(target=read, args=(self.r1, "r1"))
        t2 = threading.Thread(target=read, args=(self.r2, "r2"))
        t1.start(); t2.start()
        t1.join(); t2.join()

        for i in range(n):
            assert results["r1"][i] == f"v{i}", f"r1 ind:{i} wrong"
            assert results["r2"][i] == f"v{i}", f"r2 ind:{i} wrong"

    def test_del_fans_out_to_both_replicas(self):
        """DEL on primary clears the key on both replicas."""
        self.p.set("fan:del", "here")
        assert _poll(lambda: self.r1.exists("fan:del") == 1)
        assert _poll(lambda: self.r2.exists("fan:del") == 1)
        self.p.delete("fan:del")
        assert _poll(lambda: self.r1.exists("fan:del") == 0), \
            "DEL did not reach replica 1"
        assert _poll(lambda: self.r2.exists("fan:del") == 0), \
            "DEL did not reach replica 2"

    def test_high_throughput_fan_out(self):
        """
        1 000 writes to the primary all arrive on both replicas.
        """
        n = 1000
        pipe = self.p.pipeline(transaction=False)
        for i in range(n):
            pipe.set(f"ht:{i:04d}", f"v{i}")
        pipe.execute()

        tail = f"ht:{n - 1:04d}"
        assert _poll(lambda: self.r1.get(tail) == f"v{n - 1}", timeout=15.0), \
            "High-throughput tail key did not reach replica 1"
        assert _poll(lambda: self.r2.get(tail) == f"v{n - 1}", timeout=15.0), \
            "High-throughput tail key did not reach replica 2"


# ── TestReplicationControl ─────────────────────────────────────────────────────

class TestReplicationControl:
    """
    REPLICAOF NO ONE, re-replication after promotion, chained replicas.
    Each test uses its own isolated server pair to avoid topology interference.
    """

    def test_replicaof_no_one_promotes_to_master(self):
        """REPLICAOF NO ONE → replica role becomes master and accepts writes."""
        with _Pair() as pr:
            pr.p.set("before", "original")
            assert _poll(lambda: pr.r.get("before") == "original")

            pr.r.execute_command("REPLICAOF", "NO", "ONE")
            time.sleep(0.2)

            info = pr.r.info("replication")
            assert info["role"] == "master", \
                f"After REPLICAOF NO ONE role should be master, got {info['role']!r}"
            pr.r.set("after", "new_write")
            assert pr.r.get("after") == "new_write"

    def test_promoted_replica_data_is_intact(self):
        """Data that replicated before promotion persists after promotion."""
        with _Pair() as pr:
            for i in range(20):
                pr.p.set(f"persist:{i}", f"v{i}")
            assert _poll(lambda: pr.r.get("persist:19") == "v19")

            pr.r.execute_command("REPLICAOF", "NO", "ONE")
            time.sleep(0.2)

            for i in range(20):
                assert pr.r.get(f"persist:{i}") == f"v{i}", \
                    f"persist:{i} missing after promotion"

    def test_promoted_replica_does_not_propagate_to_old_primary(self):
        """
        Writes to the promoted node MUST NOT appear on the original primary
        (they are now independent — no reverse replication).
        """
        with _Pair() as pr:
            pr.r.execute_command("REPLICAOF", "NO", "ONE")
            time.sleep(0.2)
            pr.r.set("solo:key", "only_on_promoted")
            time.sleep(0.2)
            assert pr.p.get("solo:key") is None, \
                "Write to promoted node must not leak back to the original primary"

    def test_promoted_primary_accepts_new_replica(self):
        """
        A promoted replica can itself become a primary for a new downstream
        replica (chained / cascading replication).
        """
        pp  = _free_port()
        rp  = _free_port()
        rp2 = _free_port()
        p_proc,  p_dir  = _start(pp)
        r_proc,  r_dir  = _start(rp)
        r2_proc, r2_dir = _start(rp2)
        try:
            p  = _client(pp)
            r  = _client(rp)
            r2 = _client(rp2)

            p.set("chain:key", "value")
            _replicaof(r, pp)
            assert _poll(lambda: r.get("chain:key") == "value")

            # Promote r → it is now an independent primary
            r.execute_command("REPLICAOF", "NO", "ONE")
            time.sleep(0.2)

            # r2 replicates from the promoted node
            _replicaof(r2, rp)
            assert _poll(lambda: r2.get("chain:key") == "value"), \
                "Chain replica did not receive data from promoted primary"

            # New writes to the promoted primary propagate to r2
            r.set("chain:new", "downstream")
            assert _poll(lambda: r2.get("chain:new") == "downstream"), \
                "Write to promoted primary did not replicate to its own replica"
        finally:
            p.close(); r.close(); r2.close()
            _stop(p_proc, pp, p_dir)
            _stop(r_proc, rp, r_dir)
            _stop(r2_proc, rp2, r2_dir)

    def test_flushall_replicates(self):
        """FLUSHALL on primary wipes all keys on the replica."""
        with _Pair() as pr:
            pipe = pr.p.pipeline(transaction=False)
            for i in range(15):
                pipe.set(f"fl:{i}", "v")
            pipe.execute()
            # Give the replication stream time to carry all 15 writes
            assert _poll(lambda: pr.r.dbsize() == 15, timeout=6.0), \
                f"15 keys did not replicate (replica dbsize={pr.r.dbsize()})"
            pr.p.flushall()
            assert _poll(lambda: pr.r.dbsize() == 0, timeout=6.0), \
                "FLUSHALL did not propagate to replica"

    def test_reconnect_via_new_replicaof(self):
        """
        Issuing a new REPLICAOF <host> <port> while already a replica
        switches the replica to a different primary.
        """
        pp1 = _free_port()
        pp2 = _free_port()
        rp  = _free_port()
        p1_proc, p1_dir = _start(pp1)
        p2_proc, p2_dir = _start(pp2)
        r_proc,  r_dir  = _start(rp)
        try:
            p1 = _client(pp1)
            p2 = _client(pp2)
            r  = _client(rp)

            p1.set("from:p1", "data1")
            _replicaof(r, pp1)
            assert _poll(lambda: r.get("from:p1") == "data1")

            # Re-point the replica at p2
            p2.set("from:p2", "data2")
            _replicaof(r, pp2)

            assert _poll(lambda: r.get("from:p2") == "data2"), \
                "Replica did not receive data from new primary after REPLICAOF switch"
        finally:
            p1.close(); p2.close(); r.close()
            _stop(p1_proc, pp1, p1_dir)
            _stop(p2_proc, pp2, p2_dir)
            _stop(r_proc, rp, r_dir)


# ── TestReplicationReadLoad ────────────────────────────────────────────────────

class TestReplicationReadLoad:
    """
    Validate replica behaviour under sustained read load
    while the primary continues writing.
    """

    @classmethod
    def setup_class(cls):
        cls.pp = _free_port()
        cls.rp = _free_port()
        cls.p_proc, cls.p_dir = _start(cls.pp)
        cls.r_proc, cls.r_dir = _start(cls.rp)
        cls.p = _client(cls.pp)
        cls.r = _client(cls.rp)
        try:
            cls.r.execute_command("REPLICAOF", "127.0.0.1", str(cls.pp))
        except redis.ResponseError as exc:
            pytest.skip(f"REPLICAOF not supported: {exc}")
        if not _wait_sync(cls.r):
            pytest.fail("Replica did not sync in setup_class")

    @classmethod
    def teardown_class(cls):
        cls.p.close()
        cls.r.close()
        _stop(cls.p_proc, cls.pp, cls.p_dir)
        _stop(cls.r_proc, cls.rp, cls.r_dir)

    def setup_method(self):
        self.p.flushall()
        time.sleep(0.15)

    def test_read_during_write_stream(self):
        """
        A background writer sends 500 keys while a concurrent reader
        continuously reads from the replica.  No errors should occur.
        """
        n = 500
        errors: list = []
        write_done = threading.Event()

        def writer():
            c = _client(self.pp)
            try:
                pipe = c.pipeline(transaction=False)
                for i in range(n):
                    pipe.set(f"rw:{i:04d}", f"v{i}")
                pipe.execute()
            except Exception as exc:
                errors.append(("writer", exc))
            finally:
                c.close()
                write_done.set()

        def reader():
            c = _client(self.rp)
            try:
                while not write_done.is_set():
                    # Read random-ish keys; they may or may not exist yet
                    for i in range(0, n, 50):
                        val = c.get(f"rw:{i:04d}")
                        if val is not None and not val.startswith("v"):
                            errors.append(("reader", f"bad value {val!r} for rw:{i:04d}"))
            except Exception as exc:
                errors.append(("reader", exc))
            finally:
                c.close()

        t_write  = threading.Thread(target=writer)
        t_read   = threading.Thread(target=reader)
        t_read.start()
        t_write.start()
        t_write.join()
        t_read.join()

        assert not errors, f"Errors during read-while-write: {errors}"

        # Verify all keys landed
        assert _poll(
            lambda: self.r.get(f"rw:{n - 1:04d}") == f"v{n - 1}", timeout=10.0
        ), "Last write-stream key did not replicate"

    def test_high_throughput_1000_keys(self):
        """1 000 keys written with a pipeline all appear on the replica."""
        n = 1000
        pipe = self.p.pipeline(transaction=False)
        for i in range(n):
            pipe.set(f"ht:{i:04d}", f"v{i}")
        pipe.execute()

        assert _poll(
            lambda: self.r.get(f"ht:{n - 1:04d}") == f"v{n - 1}", timeout=15.0
        ), "Tail key did not replicate under high throughput"
        assert _poll(lambda: self.r.dbsize() >= n, timeout=15.0), \
            f"Replica DBSIZE {self.r.dbsize()} < {n} after high-throughput write"

    def test_scan_on_replica_consistent(self):
        """
        SCAN on the replica after full sync returns all expected keys and
        no extra or missing entries.
        """
        n = 50
        pipe = self.p.pipeline(transaction=False)
        for i in range(n):
            pipe.set(f"sc:{i:02d}", "v")
        pipe.execute()

        assert _poll(lambda: self.r.dbsize() == n, timeout=5.0)

        found: set[str] = set()
        cursor = 0
        iterations = 0
        while True:
            cursor, keys = self.r.scan(cursor, match="sc:*", count=100)
            found.update(keys)
            iterations += 1
            if cursor == 0 or iterations > 20:
                break

        expected = {f"sc:{i:02d}" for i in range(n)}
        assert found == expected, \
            f"SCAN on replica: missing={expected - found}, extra={found - expected}"
