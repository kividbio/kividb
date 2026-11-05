"""
Dedicated ACID property tests for Redis-compatible servers.

These tests explicitly probe each of the four ACID properties:
  A — Atomicity:   all commands in a MULTI/EXEC block commit or none do
  C — Consistency: the database moves from one valid state to another
  I — Isolation:   concurrent transactions do not observe partial state
  D — Durability:  committed data survives reconnect (within a session)

All tests are designed to pass on Redis (the reference implementation) and
surface failures in any replacement that deviates from Redis semantics.
"""
from __future__ import annotations

import threading
import time
import pytest
import redis
from utils.helpers import r, rb, make_client  # noqa: F401


# ═══════════════════════════════════════════════════════════════════════════════
# A — ATOMICITY
# ═══════════════════════════════════════════════════════════════════════════════

class TestAtomicity:
    """
    Atomicity: a MULTI/EXEC block executes as a unit.
    Either all commands run (on success) or none run (on WATCH abort).
    No external observer can see a partial result.
    """

    def test_all_commands_run_on_exec(self, r):
        """All queued commands execute when EXEC is called."""
        pipe = r.pipeline(transaction=True)
        pipe.set("acid:a:k1", "v1")
        pipe.set("acid:a:k2", "v2")
        pipe.set("acid:a:k3", "v3")
        results = pipe.execute()
        assert all(res is True for res in results)
        assert r.get("acid:a:k1") == "v1"
        assert r.get("acid:a:k2") == "v2"
        assert r.get("acid:a:k3") == "v3"

    def test_no_commands_run_on_watch_abort(self, r):
        """None of the queued commands execute when WATCH detects a conflict."""
        r.set("acid:watch:key", "original")

        pipe = r.pipeline()
        pipe.watch("acid:watch:key")

        # External modification
        other = make_client()
        other.set("acid:watch:key", "modified_externally")
        other.close()

        pipe.multi()
        pipe.set("acid:watch:key", "txn_value")
        pipe.set("acid:watch:sentinel", "should_not_exist")

        try:
            result = pipe.execute()
            assert result is None or result == []
        except redis.WatchError:
            pass

        # Neither command must have run
        assert r.get("acid:watch:key") == "modified_externally"
        assert r.get("acid:watch:sentinel") is None

    def test_discard_prevents_all_commands(self, r):
        """DISCARD cancels all queued commands atomically."""
        with r.pipeline() as p:
            p.multi()
            p.set("discard:k1", "v1")
            p.set("discard:k2", "v2")
            p.reset()

        assert r.get("discard:k1") is None
        assert r.get("discard:k2") is None

    def test_runtime_error_does_not_abort_block(self, r):
        """
        A runtime error (WRONGTYPE) inside MULTI does NOT abort the block.
        This is a key Redis guarantee: syntax errors abort, runtime errors don't.
        Other commands before and after the error still execute.
        """
        r.set("rt:str", "i_am_a_string")
        pipe = r.pipeline(transaction=True)
        pipe.set("rt:before", "executed")
        pipe.lpush("rt:str", "type_error")   # WRONGTYPE at runtime
        pipe.set("rt:after", "also_executed")
        results = pipe.execute(raise_on_error=False)

        assert r.get("rt:before") == "executed"
        assert r.get("rt:after") == "also_executed"
        assert isinstance(results[1], Exception)

    def test_incr_inside_transaction_is_atomic(self, r):
        """
        INCR inside MULTI/EXEC is atomic: the increment and result are
        both visible together — no reader can see "in between" states.
        """
        r.set("acid:incr", "0")
        pipe = r.pipeline(transaction=True)
        for _ in range(10):
            pipe.incr("acid:incr")
        results = pipe.execute()
        assert results == list(range(1, 11))
        assert r.get("acid:incr") == "10"

    def test_multi_key_transfer_is_atomic(self, r):
        """
        Simulates a bank transfer: DEBIT from A, CREDIT to B.
        No external reader should see A debited but B not yet credited.
        """
        r.set("account:A", "1000")
        r.set("account:B", "500")

        seen_partial = []
        stop = threading.Event()

        def reader():
            c = make_client()
            while not stop.is_set():
                # MGET is a single atomic command — it cannot interleave with
                # a MULTI/EXEC block, so we always see a consistent snapshot.
                vals = c.mget("account:A", "account:B")
                a = int(vals[0] or "0")
                b = int(vals[1] or "0")
                total = a + b
                if total != 1500:
                    seen_partial.append((a, b, total))
                time.sleep(0.0005)
            c.close()

        t = threading.Thread(target=reader)
        t.start()
        time.sleep(0.01)

        # Perform 50 transfers
        for _ in range(50):
            pipe = r.pipeline(transaction=True)
            pipe.decrby("account:A", 10)
            pipe.incrby("account:B", 10)
            pipe.execute()

        stop.set()
        t.join()

        assert not seen_partial, \
            f"Partial transfer state observed: {seen_partial[:5]}"
        assert int(r.get("account:A")) == 500
        assert int(r.get("account:B")) == 1000

    def test_pipeline_100_commands_all_succeed(self, r):
        """100 SET commands in a transaction all succeed or all fail."""
        pipe = r.pipeline(transaction=True)
        for i in range(100):
            pipe.set(f"acid:bulk:{i}", f"v{i}")
        results = pipe.execute()
        assert len(results) == 100
        assert all(res is True for res in results)
        for i in range(100):
            assert r.get(f"acid:bulk:{i}") == f"v{i}"


# ═══════════════════════════════════════════════════════════════════════════════
# C — CONSISTENCY
# ═══════════════════════════════════════════════════════════════════════════════

class TestConsistency:
    """
    Consistency: every operation moves the database from one valid state
    to another. Type constraints, key-space rules, and arithmetic invariants
    are enforced at all times.
    """

    def test_wrongtype_does_not_corrupt_data(self, r):
        """A failed WRONGTYPE operation must leave the key unchanged."""
        r.set("cons:str", "original")
        with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
            r.lpush("cons:str", "v")
        assert r.get("cons:str") == "original"
        assert r.type("cons:str") == "string"

    def test_type_enforced_for_all_data_types(self, r):
        """Each data type correctly rejects operations intended for other types."""
        r.lpush("list_key", "a")
        r.hset("hash_key", "f", "v")
        r.sadd("set_key", "m")
        r.zadd("zset_key", {"m": 1.0})
        r.set("str_key", "v")

        for key in ("list_key", "hash_key", "set_key", "zset_key"):
            with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
                r.get(key)

        for key in ("hash_key", "set_key", "zset_key", "str_key"):
            with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
                r.lpush(key, "x")

    def test_incr_non_integer_rejects_cleanly(self, r):
        """INCR on a non-integer string raises an error and does not corrupt."""
        r.set("not_int", "hello")
        with pytest.raises(redis.ResponseError):
            r.incr("not_int")
        assert r.get("not_int") == "hello"

    def test_integer_overflow_rejects_cleanly(self, r):
        """INCR past MAX_INT64 raises an error; the key value is unchanged."""
        max_int = 9223372036854775807
        r.set("max", str(max_int))
        with pytest.raises(redis.ResponseError):
            r.incr("max")
        assert r.get("max") == str(max_int)

    def test_key_type_unchanged_after_failed_write(self, r):
        """A WRONGTYPE error must not corrupt the key type or its data."""
        r.rpush("cons:list", "a", "b", "c")
        with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
            r.incr("cons:list")
        assert r.type("cons:list") == "list"
        assert r.lrange("cons:list", 0, -1) == ["a", "b", "c"]

    def test_set_nx_enforces_uniqueness(self, r):
        """SET NX ensures at most one writer succeeds for a key."""
        results = []

        def try_set():
            c = make_client()
            results.append(c.setnx("unique", "my_value"))
            c.close()

        threads = [threading.Thread(target=try_set) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        winners = [r for r in results if r == 1]
        assert len(winners) == 1, f"Expected exactly 1 SETNX winner, got {len(winners)}"

    def test_hash_field_consistency_under_concurrent_hincrby(self, r):
        """50 concurrent HINEBYs each of +2 must total to exactly 100."""
        r.hset("cons:hash", "counter", "0")
        fns = [lambda: make_client().hincrby("cons:hash", "counter", 2)
               for _ in range(50)]
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            list(ex.map(lambda f: f(), fns))
        assert r.hget("cons:hash", "counter") == "100"

    def test_zadd_score_consistency(self, r):
        """After concurrent ZINCRBY operations, the final score is deterministic."""
        r.zadd("cons:zset", {"m": 0.0})

        def incr_score():
            c = make_client()
            for _ in range(10):
                c.zincrby("cons:zset", 1.0, "m")
            c.close()

        threads = [threading.Thread(target=incr_score) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        final = r.zscore("cons:zset", "m")
        assert final == pytest.approx(100.0), f"Expected 100.0, got {final}"

    def test_list_length_consistent_under_concurrent_push(self, r):
        """100 concurrent RPUSHes of 1 element each → list length == 100."""
        def push_one():
            c = make_client()
            c.rpush("cons:list", "item")
            c.close()

        threads = [threading.Thread(target=push_one) for _ in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert r.llen("cons:list") == 100

    def test_set_cardinality_consistent_under_concurrent_sadd(self, r):
        """100 concurrent SADDs of distinct members → SCARD == 100."""
        def add_member(i):
            c = make_client()
            c.sadd("cons:set", f"member_{i}")
            c.close()

        threads = [threading.Thread(target=add_member, args=(i,)) for i in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert r.scard("cons:set") == 100


# ═══════════════════════════════════════════════════════════════════════════════
# I — ISOLATION
# ═══════════════════════════════════════════════════════════════════════════════

class TestIsolation:
    """
    Isolation: concurrent transactions and operations do not observe
    each other's partial state.
    """

    def test_transaction_snapshot_isolation(self, r):
        """
        Commands inside MULTI/EXEC see the state from the moment the
        transaction started, not interleaved writes from other clients.
        """
        r.set("iso:snap", "before")
        pipe = r.pipeline(transaction=True)

        # GET is queued at this point (before EXEC runs)
        pipe.get("iso:snap")
        pipe.set("iso:snap", "inside_txn")
        pipe.get("iso:snap")

        results = pipe.execute()
        # First GET reads the pre-transaction value
        assert results[0] == "before"
        # Second GET sees the SET done inside the same block
        assert results[2] == "inside_txn"

    def test_watch_prevents_lost_update(self, r):
        """
        Two concurrent read-modify-write cycles protected by WATCH.
        The loser must retry; no update must be lost.
        """
        r.set("iso:lost", "0")
        success = [0]
        retries = [0]
        lock = threading.Lock()

        def inc_with_cas():
            c = make_client()
            for _ in range(100):
                try:
                    pipe = c.pipeline()
                    pipe.watch("iso:lost")
                    current = int(pipe.get("iso:lost"))
                    pipe.multi()
                    pipe.set("iso:lost", str(current + 1))
                    pipe.execute()
                    with lock:
                        success[0] += 1
                    c.close()
                    return
                except redis.WatchError:
                    with lock:
                        retries[0] += 1
            c.close()

        threads = [threading.Thread(target=inc_with_cas) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert success[0] == 5, f"All 5 threads must succeed, got {success[0]}"
        assert int(r.get("iso:lost")) == 5, \
            f"Final value must be 5 (no lost updates), got {r.get('iso:lost')}"

    def test_no_dirty_read(self, r):
        """
        A reader must never observe values written by a transaction that
        has not yet been committed (EXEC'd).
        """
        r.set("iso:dr", "clean")
        dirty_reads = []
        stop = threading.Event()

        def continuous_reader():
            c = make_client()
            while not stop.is_set():
                val = c.get("iso:dr")
                if val not in ("clean", "committed"):
                    dirty_reads.append(val)
                time.sleep(0.0005)
            c.close()

        reader_t = threading.Thread(target=continuous_reader)
        reader_t.start()
        time.sleep(0.01)

        pipe = r.pipeline(transaction=True)
        for _ in range(200):
            pipe.set("iso:dr", "committed")
        pipe.execute()

        stop.set()
        reader_t.join()
        assert not dirty_reads, f"Dirty reads observed: {dirty_reads[:5]}"

    def test_concurrent_watch_transactions_no_lost_writes(self, r):
        """
        10 threads each try to atomically increment a counter via WATCH+CAS.
        No thread's increment must be lost.
        """
        r.set("iso:cas", "0")
        completed = []
        errors = []

        def cas_inc():
            c = make_client()
            for attempt in range(200):
                try:
                    pipe = c.pipeline()
                    pipe.watch("iso:cas")
                    current = int(pipe.get("iso:cas"))
                    pipe.multi()
                    pipe.set("iso:cas", str(current + 1))
                    pipe.execute()
                    completed.append(1)
                    c.close()
                    return
                except redis.WatchError:
                    continue
            errors.append("max retries exceeded")
            c.close()

        threads = [threading.Thread(target=cas_inc) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"CAS errors: {errors}"
        assert len(completed) == 10
        assert int(r.get("iso:cas")) == 10

    def test_blpop_delivers_to_exactly_one_waiter(self, r):
        """
        Three clients block on BLPOP. When one item is pushed, exactly one
        client wakes up and receives it — the others stay blocked.
        """
        received = []
        lock = threading.Lock()

        def waiter():
            c = make_client()
            result = c.blpop("iso:queue", timeout=3)
            if result:
                with lock:
                    received.append(result[1])
            c.close()

        threads = [threading.Thread(target=waiter) for _ in range(3)]
        for t in threads:
            t.start()
        time.sleep(0.3)

        r.rpush("iso:queue", "only_one")
        for t in threads:
            t.join(timeout=4)

        assert len(received) == 1, \
            f"Exactly one waiter must receive the item, got {len(received)}: {received}"


# ═══════════════════════════════════════════════════════════════════════════════
# D — DURABILITY (within a session / post-commit)
# ═══════════════════════════════════════════════════════════════════════════════

class TestDurability:
    """
    Durability in Redis's in-memory model means: once EXEC returns,
    the committed data is immediately visible to all connections.
    Full disk durability (RDB/AOF) is tested in test_persistence.py.
    """

    def test_committed_data_immediately_visible_to_new_connection(self, r):
        """After EXEC, a brand-new client can immediately read the committed data."""
        pipe = r.pipeline(transaction=True)
        pipe.set("dur:k1", "written_in_txn")
        pipe.hset("dur:hash", mapping={"field": "hash_value"})
        pipe.execute()

        new_client = make_client()
        try:
            assert new_client.get("dur:k1") == "written_in_txn"
            assert new_client.hget("dur:hash", "field") == "hash_value"
        finally:
            new_client.close()

    def test_committed_data_visible_after_pipeline(self, r):
        """After a pipeline flush, all commands are durably committed."""
        pipe = r.pipeline(transaction=False)
        for i in range(50):
            pipe.set(f"dur:pipe:{i}", f"v{i}")
        pipe.execute()

        c2 = make_client()
        try:
            for i in range(50):
                assert c2.get(f"dur:pipe:{i}") == f"v{i}"
        finally:
            c2.close()

    def test_flushdb_clears_all_committed_data(self, r):
        """FLUSHDB atomically removes all keys; subsequent reads return None."""
        for i in range(100):
            r.set(f"flush:k{i}", "v")
        r.flushdb()
        assert r.dbsize() == 0
        for i in range(100):
            assert r.get(f"flush:k{i}") is None

    def test_del_is_durable(self, r):
        """DEL is immediately durable — the key is gone for all subsequent reads."""
        r.set("dur:del", "exists")
        r.delete("dur:del")

        c2 = make_client()
        try:
            assert c2.get("dur:del") is None
        finally:
            c2.close()

    def test_ttl_survives_reconnect(self, r):
        """A key with a TTL still has the correct TTL on a new connection."""
        r.set("dur:ttl", "v", ex=60)
        ttl_original = r.ttl("dur:ttl")

        c2 = make_client()
        try:
            ttl_new = c2.ttl("dur:ttl")
            assert ttl_new > 0
            assert abs(ttl_original - ttl_new) <= 2
        finally:
            c2.close()

    def test_data_visible_across_50_reconnections(self, r):
        """Simulate 50 reconnections; data is always visible."""
        r.set("dur:reconnect", "stable")
        for _ in range(50):
            c = make_client()
            assert c.get("dur:reconnect") == "stable"
            c.close()


# ═══════════════════════════════════════════════════════════════════════════════
# Full ACID scenario: e-commerce order processing
# ═══════════════════════════════════════════════════════════════════════════════

class TestFullACIDScenario:
    """
    End-to-end scenario that exercises all four ACID properties together.

    Scenario: an order processing system where:
      - Stock is decremented atomically on order placement
      - Order IDs are tracked in a set
      - A WATCH-based CAS prevents overselling when stock is low
    """

    def test_order_processing_no_oversell(self, r):
        """
        10 concurrent buyers each try to purchase 1 unit from stock of 5.
        Exactly 5 must succeed; the others must fail (no oversell).
        """
        r.set("stock:item_A", "5")
        r.delete("orders:item_A")

        successes = []
        failures = []
        lock = threading.Lock()

        def try_purchase(buyer_id):
            c = make_client()
            for _ in range(50):  # retry loop
                try:
                    pipe = c.pipeline()
                    pipe.watch("stock:item_A")
                    stock = int(pipe.get("stock:item_A") or "0")
                    if stock <= 0:
                        pipe.reset()
                        with lock:
                            failures.append(buyer_id)
                        c.close()
                        return
                    pipe.multi()
                    pipe.decrby("stock:item_A", 1)
                    pipe.sadd("orders:item_A", f"order:{buyer_id}")
                    pipe.execute()
                    with lock:
                        successes.append(buyer_id)
                    c.close()
                    return
                except redis.WatchError:
                    continue
            with lock:
                failures.append(buyer_id)
            c.close()

        threads = [threading.Thread(target=try_purchase, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        final_stock = int(r.get("stock:item_A"))
        total_orders = r.scard("orders:item_A")

        assert final_stock == 0, f"All stock must be sold, got {final_stock} remaining"
        assert total_orders == 5, f"Exactly 5 orders must be placed, got {total_orders}"
        assert len(successes) == 5, f"Exactly 5 buyers must succeed, got {successes}"
        assert len(failures) == 5, f"Exactly 5 buyers must fail, got {failures}"
        assert len(set(successes)) == 5, "No duplicate successes"

    def test_balance_transfer_acid(self, r):
        """
        Transfer funds between accounts atomically.
        No concurrent read should observe a state where money has left one
        account but not yet arrived at the other.
        """
        r.set("bal:alice", "1000")
        r.set("bal:bob", "0")
        total = 1000

        inconsistencies = []
        stop = threading.Event()

        def auditor():
            c = make_client()
            while not stop.is_set():
                # MGET is atomic — it cannot interleave with a MULTI/EXEC block
                vals = c.mget("bal:alice", "bal:bob")
                alice = int(vals[0] or "0")
                bob = int(vals[1] or "0")
                if alice + bob != total:
                    inconsistencies.append((alice, bob))
                time.sleep(0.0005)
            c.close()

        t = threading.Thread(target=auditor)
        t.start()
        time.sleep(0.01)

        # 10 transfers of $100 each
        for _ in range(10):
            pipe = r.pipeline(transaction=True)
            pipe.decrby("bal:alice", 100)
            pipe.incrby("bal:bob", 100)
            pipe.execute()

        stop.set()
        t.join()

        assert not inconsistencies, \
            f"Balance inconsistency observed during transfer: {inconsistencies[:3]}"
        assert int(r.get("bal:alice")) == 0
        assert int(r.get("bal:bob")) == 1000
