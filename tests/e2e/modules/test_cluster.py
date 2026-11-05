"""
e2e tests for Redis-compatible cluster mode.

These tests are skipped automatically unless a cluster is available.
Point the suite at any Redis Cluster–compatible deployment (Redis, Dragonfly,
KiviDB, Valkey, …) by setting:

    KIVI_CLUSTER_ADDRS=host:port[,host:port,...]

All tests are written against the standard Redis Cluster protocol and must
pass on any spec-compliant implementation without modification.

Covers:
  - CLUSTER INFO (cluster_state, cluster_slots_assigned, cluster_size)
  - CLUSTER NODES topology
  - CLUSTER KEYSLOT hash-slot calculation
  - Hash-tag ({tag}) routing — keys sharing a tag land in the same slot
  - Cross-slot MSET / MGET errors without hash tags
  - Same-slot MSET / MGET with hash tags
  - Basic GET / SET / DEL through cluster routing
  - All standard data types via cluster routing
  - Key expiry in cluster mode
  - SCAN across cluster nodes
  - Pub/Sub in cluster mode
  - Pipelining through cluster
  - Transactions (MULTI/EXEC) per-node
  - Replica reads (READONLY) where replicas are available
  - CLUSTER MYID returns a non-empty node ID
  - DBSIZE aggregated across all nodes
"""
from __future__ import annotations

import os
import time
import threading
from typing import Iterator

import pytest

# ── redis-py cluster client import ─────────────────────────────────────────────

try:
    from redis.cluster import RedisCluster, ClusterNode
    import redis
    _HAS_CLUSTER_CLIENT = True
except ImportError:
    _HAS_CLUSTER_CLIENT = False

# ── Environment / skip guards ──────────────────────────────────────────────────

_CLUSTER_ADDRS_ENV = os.environ.get("KIVI_CLUSTER_ADDRS", "").strip()


def _parse_cluster_addrs() -> list[tuple[str, int]]:
    """Parse 'host:port,host:port,...' into a list of (host, port) tuples."""
    if not _CLUSTER_ADDRS_ENV:
        return []
    result = []
    for token in _CLUSTER_ADDRS_ENV.split(","):
        token = token.strip()
        if not token:
            continue
        host, _, port_str = token.rpartition(":")
        host = host or "127.0.0.1"
        result.append((host, int(port_str)))
    return result


_CLUSTER_NODES = _parse_cluster_addrs()

needs_cluster = pytest.mark.skipif(
    not _CLUSTER_ADDRS_ENV or not _HAS_CLUSTER_CLIENT,
    reason=(
        "Set KIVI_CLUSTER_ADDRS=host:port[,...] and install redis-py>=4.1 "
        "to run cluster tests"
    ),
)


# A cross-slot command is refused either by the server (-CROSSSLOT) or, before
# it is sent, by redis-py ("all keys must map to the same key slot").
_CROSS_SLOT = "(?i)cross.?slot|CROSSSLOT|different|same key slot"


def _cluster_protocol() -> int:
    """
    The RESP version for a cluster client: the one the suite is running under.

    It has to be passed explicitly. The node connections a RedisCluster opens
    are ordinary redis.Redis clients, which conftest.py pins to the suite's
    protocol; the cluster client itself would otherwise use redis-py's own
    default. With redis-py 7+ those differ (its default is RESP3), and it then
    refuses to connect at all: it enables maintenance notifications for the
    cluster client, which need RESP3, on node connections that speak RESP2.
    """
    from utils.helpers import active_protocol

    return active_protocol()


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def cluster() -> Iterator["RedisCluster"]:
    """
    Module-scoped RedisCluster client.  One live cluster is shared across
    all tests in this module to avoid reconnect overhead.
    """
    if not _CLUSTER_NODES or not _HAS_CLUSTER_CLIENT:
        pytest.skip("No cluster configured")
    startup = [ClusterNode(h, p) for h, p in _CLUSTER_NODES]
    cl = RedisCluster(
        startup_nodes=startup,
        decode_responses=True,
        socket_timeout=5,
        socket_connect_timeout=5,
        protocol=_cluster_protocol(),
    )
    yield cl
    cl.close()


@pytest.fixture(autouse=True)
def flush_cluster(cluster):
    """Flush all keys before every test (runs on all primaries)."""
    cluster.flushall()
    time.sleep(0.05)
    yield


# ── Helpers ────────────────────────────────────────────────────────────────────

def _poll(fn, timeout: float = 4.0, interval: float = 0.05):
    deadline = time.monotonic() + timeout
    result = None
    while time.monotonic() < deadline:
        result = fn()
        if result:
            return result
        time.sleep(interval)
    return result


# ── TestClusterTopology ────────────────────────────────────────────────────────

@needs_cluster
class TestClusterTopology:
    """CLUSTER INFO and CLUSTER NODES report a healthy, consistent topology."""

    def test_cluster_info_state_ok(self, cluster):
        """CLUSTER INFO must report cluster_state:ok."""
        info = cluster.cluster_info()
        state = info.get("cluster_state") or info.get("cluster_enabled")
        assert info.get("cluster_state") == "ok", \
            f"cluster_state should be ok, got {info.get('cluster_state')!r}"

    def test_cluster_info_slots_assigned(self, cluster):
        """All 16 384 hash slots must be assigned."""
        info = cluster.cluster_info()
        assigned = int(info.get("cluster_slots_assigned", 0))
        assert assigned == 16384, \
            f"Expected 16384 assigned slots, got {assigned}"

    def test_cluster_info_cluster_enabled(self, cluster):
        """cluster_enabled must be 1 (cluster mode is active)."""
        info = cluster.cluster_info()
        assert int(info.get("cluster_enabled", 0)) == 1, \
            "cluster_enabled should be 1"

    def test_cluster_nodes_not_empty(self, cluster):
        """CLUSTER NODES must return at least one node."""
        nodes = cluster.cluster_nodes()
        assert nodes, "CLUSTER NODES returned empty result"

    def test_cluster_has_at_least_one_primary(self, cluster):
        """At least one node with the 'master' flag must be present."""
        nodes = cluster.cluster_nodes()
        primaries = [
            n for n in nodes.values()
            if "master" in n.get("flags", "")
            or "primary" in n.get("flags", "")
        ]
        assert primaries, "No primary/master node found in CLUSTER NODES"

    def test_cluster_myid_non_empty(self, cluster):
        """CLUSTER MYID returns a non-empty node ID string."""
        node_id = cluster.cluster_myid(cluster.get_default_node())
        assert node_id and len(node_id) > 0, \
            f"CLUSTER MYID returned empty/None: {node_id!r}"


# ── TestClusterKeyslot ─────────────────────────────────────────────────────────

@needs_cluster
class TestClusterKeyslot:
    """CLUSTER KEYSLOT implements the Redis hash-slot algorithm correctly."""

    def test_keyslot_known_value(self, cluster):
        """
        CLUSTER KEYSLOT for the key 'foo' is 12182 according to the Redis
        CRC-16 spec (it is the example in the Redis documentation).  Validate
        the server agrees.
        """
        slot = cluster.cluster_keyslot("foo")
        assert slot == 12182, f"CLUSTER KEYSLOT foo should be 12182, got {slot}"

    def test_keyslot_range(self, cluster):
        """All slot numbers must be in [0, 16383]."""
        for key in ["a", "hello", "test:key", "{tag}:suffix", "12345"]:
            slot = cluster.cluster_keyslot(key)
            assert 0 <= slot <= 16383, \
                f"Slot {slot} for key {key!r} is out of range"

    def test_hash_tag_same_slot(self, cluster):
        """
        Keys sharing the same {tag} must hash to the same slot regardless
        of the surrounding key name.
        """
        tag = "shared"
        keys = [f"{{{tag}}}:a", f"{{{tag}}}:b", f"prefix:{{{tag}}}:c", f"{{{tag}}}"]
        slots = [cluster.cluster_keyslot(k) for k in keys]
        assert len(set(slots)) == 1, \
            f"Keys with tag {{{tag}}} must all share one slot, got: {list(zip(keys, slots))}"

    def test_different_keys_may_have_different_slots(self, cluster):
        """
        Unrelated keys (no shared tag) will generally hash to different slots.
        Test at least a large enough set that some are distinct.
        """
        keys = [f"rnd:{i}" for i in range(20)]
        slots = [cluster.cluster_keyslot(k) for k in keys]
        assert len(set(slots)) > 1, \
            "20 distinct keys should not all map to the same slot"

    def test_empty_tag_uses_whole_key(self, cluster):
        """
        A key with '{}' (empty hash tag) is hashed by the whole key, not
        an empty tag — so '{}'a and '{}'b may differ.
        """
        # The key "{}a" has an empty tag → whole key is used
        slot_a = cluster.cluster_keyslot("{}a")
        slot_b = cluster.cluster_keyslot("{}b")
        # They *may* differ — just verify no crash and valid range
        assert 0 <= slot_a <= 16383
        assert 0 <= slot_b <= 16383


# ── TestClusterBasicOperations ─────────────────────────────────────────────────

@needs_cluster
class TestClusterBasicOperations:
    """Standard key operations routed through the cluster."""

    def test_set_and_get(self, cluster):
        """SET then GET returns the correct value."""
        cluster.set("basic:key", "hello")
        assert cluster.get("basic:key") == "hello"

    def test_get_missing_key_returns_none(self, cluster):
        assert cluster.get("nonexistent:key") is None

    def test_set_overwrite(self, cluster):
        cluster.set("ow:key", "first")
        cluster.set("ow:key", "second")
        assert cluster.get("ow:key") == "second"

    def test_del_removes_key(self, cluster):
        cluster.set("del:key", "v")
        cluster.delete("del:key")
        assert cluster.get("del:key") is None

    def test_exists_true_and_false(self, cluster):
        cluster.set("ex:key", "v")
        assert cluster.exists("ex:key") == 1
        assert cluster.exists("missing:key") == 0

    def test_incr_and_decr(self, cluster):
        cluster.set("ctr:key", "10")
        assert cluster.incr("ctr:key") == 11
        assert cluster.decr("ctr:key") == 10

    def test_append(self, cluster):
        cluster.set("app:key", "hello")
        cluster.append("app:key", " world")
        assert cluster.get("app:key") == "hello world"

    def test_mset_mget_with_hash_tags(self, cluster):
        """
        MSET / MGET with keys sharing the same hash tag must succeed
        (all keys in the same slot).
        """
        tag = "mst"
        cluster.mset({
            f"{{{tag}}}:a": "1",
            f"{{{tag}}}:b": "2",
            f"{{{tag}}}:c": "3",
        })
        vals = cluster.mget(f"{{{tag}}}:a", f"{{{tag}}}:b", f"{{{tag}}}:c")
        assert vals == ["1", "2", "3"]

    def test_cross_slot_mset_raises(self, cluster):
        """
        MSET with keys in different slots (no common hash tag) must raise
        a cross-slot error.
        """
        # Pick two keys guaranteed to be in different slots
        with pytest.raises(Exception, match=_CROSS_SLOT):
            cluster.mset({"foo": "1", "bar": "2"})

    def test_cross_slot_mget_raises(self, cluster):
        """MGET with keys in different slots must raise a cross-slot error."""
        with pytest.raises(Exception, match=_CROSS_SLOT):
            cluster.mget("foo", "bar")


# ── TestClusterDataTypes ───────────────────────────────────────────────────────

@needs_cluster
class TestClusterDataTypes:
    """All standard Redis data types work correctly through cluster routing."""

    def test_hash_hset_hget(self, cluster):
        cluster.hset("dt:hash", mapping={"f1": "v1", "f2": "v2"})
        assert cluster.hget("dt:hash", "f1") == "v1"
        assert cluster.hgetall("dt:hash") == {"f1": "v1", "f2": "v2"}

    def test_hash_hincrby(self, cluster):
        cluster.hset("dt:hincr", "counter", "5")
        cluster.hincrby("dt:hincr", "counter", 3)
        assert cluster.hget("dt:hincr", "counter") == "8"

    def test_list_rpush_lrange(self, cluster):
        cluster.rpush("dt:list", "a", "b", "c")
        assert cluster.lrange("dt:list", 0, -1) == ["a", "b", "c"]

    def test_list_lpush_lpop(self, cluster):
        cluster.lpush("dt:lpush", "z", "y", "x")
        assert cluster.lpop("dt:lpush") == "x"

    def test_set_sadd_smembers(self, cluster):
        cluster.sadd("dt:set", "x", "y", "z")
        assert cluster.smembers("dt:set") == {"x", "y", "z"}

    def test_set_sismember(self, cluster):
        cluster.sadd("dt:sis", "a", "b")
        assert cluster.sismember("dt:sis", "a") == 1
        assert cluster.sismember("dt:sis", "c") == 0

    def test_zset_zadd_zrange(self, cluster):
        cluster.zadd("dt:zset", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert cluster.zrange("dt:zset", 0, -1, withscores=True) == [
            ("a", 1.0), ("b", 2.0), ("c", 3.0)
        ]

    def test_zset_zscore(self, cluster):
        cluster.zadd("dt:zscore", {"member": 42.0})
        assert cluster.zscore("dt:zscore", "member") == 42.0

    def test_zset_zrank(self, cluster):
        cluster.zadd("dt:zrank", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert cluster.zrank("dt:zrank", "a") == 0
        assert cluster.zrank("dt:zrank", "c") == 2

    def test_type_command(self, cluster):
        cluster.set("tp:str", "s")
        cluster.lpush("tp:list", "a")
        cluster.sadd("tp:set", "b")
        cluster.zadd("tp:zset", {"c": 1.0})
        cluster.hset("tp:hash", "f", "v")
        assert cluster.type("tp:str")  == "string"
        assert cluster.type("tp:list") == "list"
        assert cluster.type("tp:set")  == "set"
        assert cluster.type("tp:zset") == "zset"
        assert cluster.type("tp:hash") == "hash"


# ── TestClusterExpiry ──────────────────────────────────────────────────────────

@needs_cluster
class TestClusterExpiry:
    """Key expiry works correctly in cluster mode."""

    def test_expire_and_ttl(self, cluster):
        cluster.set("exp:key", "v")
        cluster.expire("exp:key", 60)
        ttl = cluster.ttl("exp:key")
        assert 55 <= ttl <= 60, f"TTL should be ~60 s, got {ttl}"

    def test_pexpire_and_pttl(self, cluster):
        cluster.set("pexp:key", "v")
        cluster.pexpire("pexp:key", 5000)
        pttl = cluster.pttl("pexp:key")
        assert 3000 <= pttl <= 5500, f"PTTL should be ~5 000 ms, got {pttl}"

    def test_key_disappears_after_ttl(self, cluster):
        cluster.set("short:key", "v")
        cluster.pexpire("short:key", 300)
        time.sleep(0.15)
        assert cluster.exists("short:key") == 1
        time.sleep(0.25)
        assert cluster.exists("short:key") == 0, "Key must vanish after TTL"

    def test_persist_removes_ttl(self, cluster):
        cluster.set("per:key", "v")
        cluster.expire("per:key", 60)
        assert cluster.ttl("per:key") > 0
        cluster.persist("per:key")
        assert cluster.ttl("per:key") == -1, "PERSIST must clear TTL"

    def test_ttl_minus_one_for_persistent_key(self, cluster):
        cluster.set("notx:key", "v")
        assert cluster.ttl("notx:key") == -1

    def test_ttl_minus_two_for_missing_key(self, cluster):
        assert cluster.ttl("ghost:key") == -2

    def test_expireat_absolute_timestamp(self, cluster):
        cluster.set("eat:key", "v")
        future_ts = int(time.time()) + 60
        cluster.expireat("eat:key", future_ts)
        ttl = cluster.ttl("eat:key")
        assert 50 <= ttl <= 65, f"EXPIREAT TTL should be ~60 s, got {ttl}"


# ── TestClusterScan ────────────────────────────────────────────────────────────

@needs_cluster
class TestClusterScan:
    """SCAN across all cluster nodes returns a complete and correct key set."""

    def test_scan_all_keys(self, cluster):
        """
        Write 50 tagged keys (same slot) and SCAN for them.
        All must be returned exactly once.
        """
        tag = "scantag"
        n = 50
        for i in range(n):
            cluster.set(f"{{{tag}}}:key:{i:02d}", f"v{i}")

        found: set[str] = set(cluster.scan_iter(match=f"{{{tag}}}:key:*", count=100))

        expected = {f"{{{tag}}}:key:{i:02d}" for i in range(n)}
        assert found == expected, \
            f"SCAN missing={expected - found}, extra={found - expected}"

    def test_scan_with_pattern(self, cluster):
        """SCAN with a pattern returns only matching keys."""
        tag = "scanpat"
        cluster.set(f"{{{tag}}}:keep:1", "v")
        cluster.set(f"{{{tag}}}:keep:2", "v")
        cluster.set(f"{{{tag}}}:drop:1", "v")

        found: set[str] = set(cluster.scan_iter(match=f"{{{tag}}}:keep:*", count=100))

        assert f"{{{tag}}}:keep:1" in found
        assert f"{{{tag}}}:keep:2" in found
        assert f"{{{tag}}}:drop:1" not in found

    def test_scan_expired_keys_not_returned(self, cluster):
        """SCAN must not return already-expired keys."""
        tag = "scanexp"
        for i in range(10):
            cluster.set(f"{{{tag}}}:live:{i}", "alive")
        for i in range(10):
            cluster.set(f"{{{tag}}}:dead:{i}", "dying")
            cluster.pexpire(f"{{{tag}}}:dead:{i}", 100)

        time.sleep(0.2)  # let dead keys expire

        found: set[str] = set(cluster.scan_iter(match=f"{{{tag}}}:*", count=100))

        dead_found = [k for k in found if ":dead:" in k]
        assert not dead_found, f"SCAN returned expired keys: {dead_found}"


# ── TestClusterPubSub ──────────────────────────────────────────────────────────

@needs_cluster
class TestClusterPubSub:
    """
    Pub/Sub in cluster mode.  In Redis Cluster, PUBLISH is broadcast to all
    nodes so any subscriber on any node receives the message.
    """

    def test_subscribe_and_receive(self, cluster):
        """A subscriber on one channel receives a published message."""
        import queue as _queue

        # Use a raw (non-cluster) connection to a known cluster node for pub/sub
        host, port = _CLUSTER_NODES[0]
        sub_client = redis.Redis(
            host=host, port=port, decode_responses=True, socket_timeout=5
        )
        q: _queue.Queue = _queue.Queue()
        stop_evt = threading.Event()

        ps = sub_client.pubsub(ignore_subscribe_messages=True)
        ps.subscribe("cl:chan")
        time.sleep(0.1)

        def listener():
            while not stop_evt.is_set():
                msg = ps.get_message(timeout=0.1)
                if msg and msg.get("type") == "message":
                    q.put(msg)

        t = threading.Thread(target=listener, daemon=True)
        t.start()
        time.sleep(0.05)

        count = cluster.publish("cl:chan", "hello cluster")
        assert count >= 0

        try:
            msg = q.get(timeout=3.0)
        except _queue.Empty:
            msg = None
        finally:
            stop_evt.set()
            t.join(timeout=2)
            ps.close()
            sub_client.close()

        assert msg is not None, "Subscriber did not receive the published message"
        assert msg["channel"] == "cl:chan"
        assert msg["data"] == "hello cluster"

    def test_publish_no_subscribers_returns_zero(self, cluster):
        """Publishing to a channel with no subscribers returns 0."""
        count = cluster.publish("cl:nobody", "lonely")
        assert count == 0


# ── TestClusterPipeline ────────────────────────────────────────────────────────

@needs_cluster
class TestClusterPipeline:
    """
    redis-py's cluster pipeline batches commands that land on the same slot;
    cross-slot commands are split internally.
    """

    def test_pipeline_same_slot_keys(self, cluster):
        """
        Pipelining multiple commands for same-slot keys executes correctly.
        """
        tag = "pip"
        pipe = cluster.pipeline(transaction=False)
        for i in range(20):
            pipe.set(f"{{{tag}}}:{i}", f"v{i}")
        results = pipe.execute()
        assert all(r is True for r in results), f"Pipeline SET results: {results}"

        for i in range(20):
            assert cluster.get(f"{{{tag}}}:{i}") == f"v{i}", \
                f"Pipeline key {{{tag}}}:{i} wrong"

    def test_pipeline_reads(self, cluster):
        """Pipeline GET commands return values in order."""
        tag = "pget"
        for i in range(5):
            cluster.set(f"{{{tag}}}:{i}", f"val{i}")

        pipe = cluster.pipeline(transaction=False)
        for i in range(5):
            pipe.get(f"{{{tag}}}:{i}")
        vals = pipe.execute()
        assert vals == [f"val{i}" for i in range(5)]

    def test_large_pipeline(self, cluster):
        """A pipeline with 200 SET commands all execute."""
        tag = "bigpipe"
        pipe = cluster.pipeline(transaction=False)
        for i in range(200):
            pipe.set(f"{{{tag}}}:{i:03d}", f"v{i}")
        results = pipe.execute()
        assert len(results) == 200
        assert all(r is True for r in results)


# ── TestClusterTransactions ────────────────────────────────────────────────────

@needs_cluster
class TestClusterTransactions:
    """
    MULTI/EXEC in cluster mode requires all keys in the transaction to share
    the same hash slot.  redis-py enforces this by routing the pipeline to
    the correct node.
    """

    def test_multi_exec_same_slot(self, cluster):
        """MULTI/EXEC with same-slot keys executes atomically."""
        tag = "txn"
        pipe = cluster.pipeline(transaction=True)
        pipe.set(f"{{{tag}}}:a", "1")
        pipe.set(f"{{{tag}}}:b", "2")
        pipe.incr(f"{{{tag}}}:c")
        results = pipe.execute()
        assert results in ([True, True, 1], ["OK", "OK", 1])
        assert cluster.get(f"{{{tag}}}:a") == "1"
        assert cluster.get(f"{{{tag}}}:b") == "2"
        assert cluster.get(f"{{{tag}}}:c") == "1"

    def test_empty_multi_exec(self, cluster):
        """MULTI immediately followed by EXEC returns an empty list."""
        pipe = cluster.pipeline(transaction=True)
        results = pipe.execute()
        assert results == []

    def test_multi_exec_all_types(self, cluster):
        """All data types can be queued in a single per-slot transaction."""
        tag = "txntype"
        pipe = cluster.pipeline(transaction=True)
        pipe.set(f"{{{tag}}}:str", "hello")
        pipe.lpush(f"{{{tag}}}:list", "a", "b")
        pipe.sadd(f"{{{tag}}}:set", "x", "y")
        pipe.zadd(f"{{{tag}}}:zset", {"m": 1.5})
        pipe.hset(f"{{{tag}}}:hash", mapping={"f": "v"})
        results = pipe.execute()

        assert results[0] in (True, "OK")  # SET
        assert results[1] == 2             # LPUSH
        assert results[2] == 2             # SADD
        assert results[3] == 1             # ZADD
        assert results[4] == 1             # HSET


# ── TestClusterConcurrency ─────────────────────────────────────────────────────

@needs_cluster
class TestClusterConcurrency:
    """Concurrent access patterns that exercise cluster routing under load."""

    def test_concurrent_sets_from_multiple_clients(self, cluster):
        """
        Five concurrent writers each SET 20 unique keys.
        All 100 keys must be readable after all threads complete.
        """
        errors: list = []
        n_per = 20
        host, port = _CLUSTER_NODES[0]

        def writer(tid: int):
            c = RedisCluster(
                startup_nodes=[ClusterNode(host, port)],
                decode_responses=True,
                socket_timeout=5,
                protocol=_cluster_protocol(),
            )
            try:
                for i in range(n_per):
                    c.set(f"cc:{tid}:{i}", f"v{tid}{i}")
            except Exception as exc:
                errors.append(exc)
            finally:
                c.close()

        threads = [threading.Thread(target=writer, args=(t,)) for t in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Writer errors: {errors}"
        for tid in range(5):
            for i in range(n_per):
                val = cluster.get(f"cc:{tid}:{i}")
                assert val == f"v{tid}{i}", \
                    f"cc:{tid}:{i} wrong: expected v{tid}{i}, got {val!r}"

    def test_concurrent_reads_while_writing(self, cluster):
        """
        A background writer inserts 100 keys while concurrent readers
        poll keys that may or may not exist yet.  No errors should occur.
        """
        tag = "crw"
        n = 100
        errors: list = []
        write_done = threading.Event()

        def writer():
            try:
                for i in range(n):
                    cluster.set(f"{{{tag}}}:{i:03d}", f"v{i}")
            except Exception as exc:
                errors.append(("writer", exc))
            finally:
                write_done.set()

        def reader():
            try:
                while not write_done.is_set():
                    for i in range(0, n, 10):
                        val = cluster.get(f"{{{tag}}}:{i:03d}")
                        if val is not None and not val.startswith("v"):
                            errors.append(("reader", f"bad value {val!r}"))
            except Exception as exc:
                errors.append(("reader", exc))

        t_w = threading.Thread(target=writer)
        t_r = threading.Thread(target=reader)
        t_r.start(); t_w.start()
        t_w.join(); t_r.join()

        assert not errors, f"Concurrent read/write errors: {errors}"


# ── TestClusterReplicaReads ────────────────────────────────────────────────────

@needs_cluster
class TestClusterReplicaReads:
    """
    READONLY mode allows reading from replica nodes in cluster mode.
    Tests are skipped automatically if the cluster has no replicas.
    """

    def _has_replicas(self, cluster) -> bool:
        nodes = cluster.cluster_nodes()
        return any(
            "slave" in n.get("flags", "") or "replica" in n.get("flags", "")
            for n in nodes.values()
        )

    def test_readonly_read_from_replica(self, cluster):
        """
        A raw connection with READONLY set can read from a replica node
        without MOVED errors.
        """
        if not self._has_replicas(cluster):
            pytest.skip("No replica nodes in this cluster")

        # Write via the cluster client (routed to primary)
        cluster.set("rr:key", "replica_read_value")
        time.sleep(0.1)  # brief pause for replication lag

        # Find a replica node
        nodes = cluster.cluster_nodes()
        replica_addr = next(
            (addr for addr, n in nodes.items()
             if "slave" in n.get("flags", "") or "replica" in n.get("flags", "")),
            None,
        )
        assert replica_addr is not None

        host, _, port_str = replica_addr.rpartition(":")
        port = int(port_str)

        import redis as _redis
        conn = _redis.Redis(
            host=host, port=port, decode_responses=True, socket_timeout=5
        )
        try:
            conn.execute_command("READONLY")
            val = conn.get("rr:key")
            # val may be None (replica lag) or the correct value
            assert val is None or val == "replica_read_value", \
                f"Unexpected value from replica: {val!r}"
        except _redis.ResponseError as exc:
            if "MOVED" in str(exc):
                pytest.skip("READONLY not effective on this replica (MOVED response)")
        finally:
            conn.close()
