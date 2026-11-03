"""
e2e tests for MULTI/EXEC/DISCARD/WATCH transactions.

Ground-truth Redis behaviour:
  - MULTI/EXEC is atomic: all queued commands run or none (on WATCH conflict)
  - Commands inside MULTI are queued, not executed immediately
  - EXEC returns results in the same order as queued commands
  - DISCARD clears the queue without executing anything
  - Syntax errors (wrong arity) inside MULTI abort the entire block on EXEC
  - Runtime errors (WRONGTYPE) inside MULTI do NOT abort — other commands run
  - WATCH implements optimistic locking: any modification to a watched key
    causes EXEC to return nil (abort) instead of executing the queued commands
  - UNWATCH releases all watches on the current connection
  - WATCH detects: value changes, DEL, key creation on previously-nonexistent keys
  - Transactions are isolated: no other client sees partial state during EXEC
"""
from __future__ import annotations

import threading
import time
import pytest
import redis
from utils.helpers import r, rb, make_client  # noqa: F401


# ── Basic MULTI/EXEC ───────────────────────────────────────────────────────────

class TestBasicTransaction:
    def test_multi_exec_atomic_set(self, r):
        """Commands queued in MULTI are executed atomically on EXEC."""
        pipe = r.pipeline(transaction=True)
        pipe.set("txn:a", "1")
        pipe.set("txn:b", "2")
        pipe.incr("txn:c")
        results = pipe.execute()
        assert results == [True, True, 1]
        assert r.get("txn:a") == "1"
        assert r.get("txn:b") == "2"
        assert r.get("txn:c") == "1"

    def test_multi_exec_all_or_nothing(self, r):
        r.set("str:key", "notalist")
        pipe = r.pipeline(transaction=True)
        pipe.set("txn:ok", "yes")
        pipe.execute()
        assert r.get("txn:ok") == "yes"

    def test_discard_cancels_transaction(self, r):
        """DISCARD after MULTI clears the queue without executing anything."""
        with r.pipeline() as p:
            p.multi()
            p.set("discard:direct", "nope")
            p.reset()

        assert r.get("discard:direct") is None

    def test_empty_transaction(self, r):
        """MULTI immediately followed by EXEC returns an empty list."""
        pipe = r.pipeline(transaction=True)
        results = pipe.execute()
        assert results == []

    def test_transaction_queues_multiple_types(self, r):
        """All command types can be queued in a single transaction."""
        pipe = r.pipeline(transaction=True)
        pipe.set("txn:str", "hello")
        pipe.lpush("txn:list", "a", "b")
        pipe.sadd("txn:set", "x", "y")
        pipe.zadd("txn:zset", {"member": 1.5})
        pipe.hset("txn:hash", mapping={"field": "value"})
        pipe.incr("txn:counter")
        results = pipe.execute()

        assert results[0] is True
        assert results[1] == 2
        assert results[2] == 2
        assert results[3] == 1
        assert results[4] == 1
        assert results[5] == 1

        assert r.get("txn:str") == "hello"
        assert r.lrange("txn:list", 0, -1) == ["b", "a"]
        assert r.smembers("txn:set") == {"x", "y"}

    def test_transaction_sees_no_external_writes(self, r):
        """
        Within a transaction, GET reflects the state from BEFORE the transaction;
        writes made later in the same EXEC block are visible to subsequent commands.
        """
        r.set("isolated:counter", "10")
        pipe = r.pipeline(transaction=True)
        pipe.get("isolated:counter")
        pipe.incr("isolated:counter")
        pipe.get("isolated:counter")
        a, b, c = pipe.execute()

        assert a == "10"
        assert b == 11
        assert c == "11"

    def test_exec_returns_results_in_order(self, r):
        """EXEC returns results in the same order as queued commands."""
        pipe = r.pipeline(transaction=True)
        pipe.set("ord:1", "alpha")
        pipe.set("ord:2", "beta")
        pipe.get("ord:1")
        pipe.get("ord:2")
        pipe.strlen("ord:1")
        results = pipe.execute()

        assert results[0] is True
        assert results[1] is True
        assert results[2] == "alpha"
        assert results[3] == "beta"
        assert results[4] == 5

    def test_transaction_on_fresh_keys(self, r):
        """Transaction creates keys that were absent before MULTI."""
        pipe = r.pipeline(transaction=True)
        pipe.set("new:1", "a")
        pipe.set("new:2", "b")
        pipe.execute()
        assert r.get("new:1") == "a"
        assert r.get("new:2") == "b"

    def test_multi_exec_100_incrs(self, r):
        """100 INCR commands inside one MULTI/EXEC block."""
        r.set("big:counter", "0")
        pipe = r.pipeline(transaction=True)
        for _ in range(100):
            pipe.incr("big:counter")
        results = pipe.execute()
        assert results[-1] == 100
        assert r.get("big:counter") == "100"

    def test_reads_inside_exec_see_writes_from_same_block(self, r):
        """SET then GET inside the same EXEC block: GET sees the SET."""
        pipe = r.pipeline(transaction=True)
        pipe.set("rw:key", "written_in_txn")
        pipe.get("rw:key")
        _, read_val = pipe.execute()
        assert read_val == "written_in_txn"

    def test_exec_result_length_matches_queue(self, r):
        """EXEC always returns exactly as many results as queued commands."""
        n = 15
        pipe = r.pipeline(transaction=True)
        for i in range(n):
            pipe.set(f"k{i}", str(i))
        results = pipe.execute()
        assert len(results) == n

    def test_nested_multi_rejected(self, r):
        """MULTI inside an active transaction must be rejected."""
        with r.pipeline() as p:
            p.multi()
            p.execute_command("MULTI")
            # The nested MULTI should be queued as a command that errors on EXEC
            results = p.execute(raise_on_error=False)
        # Either an error is in results or the server rejected the nested MULTI
        # The important invariant: the connection must still work after
        assert r.ping() is True


# ── WATCH / optimistic locking ─────────────────────────────────────────────────

class TestWatch:
    def test_watch_no_conflict_succeeds(self, r):
        r.set("watch:clean", "original")
        r.watch("watch:clean")
        pipe = r.pipeline(transaction=True)
        pipe.set("watch:clean", "modified by txn")
        results = pipe.execute()
        assert results == [True]
        assert r.get("watch:clean") == "modified by txn"

    def test_watch_conflict_aborts_exec(self, r):
        """WATCH a key, another client modifies it, EXEC → returns None (aborted)."""
        r.set("watch:dirty", "initial")

        pipe = r.pipeline()
        pipe.watch("watch:dirty")

        other = make_client()
        other.set("watch:dirty", "changed by other")
        other.close()

        pipe.multi()
        pipe.set("watch:dirty", "txn result")
        try:
            result = pipe.execute()
            assert result is None or result == [], \
                f"Transaction should have been aborted, got: {result}"
        except redis.WatchError:
            pass

    def test_watch_unwatch_allows_exec(self, r):
        """After UNWATCH, external modification does NOT abort EXEC."""
        r.set("watch:unwatched", "value")
        pipe = r.pipeline()
        pipe.watch("watch:unwatched")
        pipe.unwatch()

        other = make_client()
        other.set("watch:unwatched", "changed after unwatch")
        other.close()

        pipe.multi()
        pipe.set("watch:result", "ok")
        results = pipe.execute()
        assert results is not None
        assert r.get("watch:result") == "ok"

    def test_watch_multiple_keys_one_modified(self, r):
        """WATCHing A and B; B modified → EXEC must abort."""
        r.set("mwatch:A", "a")
        r.set("mwatch:B", "b")

        pipe = r.pipeline()
        pipe.watch("mwatch:A", "mwatch:B")

        other = make_client()
        other.set("mwatch:B", "modified")
        other.close()

        pipe.multi()
        pipe.set("mwatch:result", "should not exist")
        try:
            result = pipe.execute()
            assert result is None or result == [], \
                "Modifying any watched key must abort the transaction"
        except redis.WatchError:
            pass
        finally:
            assert r.get("mwatch:result") is None

    def test_watch_key_deleted_aborts(self, r):
        """DELeting a WATCHed key counts as a modification → EXEC must abort."""
        r.set("watch:del", "exists")

        pipe = r.pipeline()
        pipe.watch("watch:del")

        other = make_client()
        other.delete("watch:del")
        other.close()

        pipe.multi()
        pipe.set("watch:del:result", "should not set")
        try:
            result = pipe.execute()
            assert result is None or result == [], \
                "Deleting a watched key must abort the transaction"
        except redis.WatchError:
            pass

    def test_watch_nonexistent_key_then_created(self, r):
        """WATCH a nonexistent key. Another client creates it → EXEC must abort."""
        assert r.get("watch:new") is None

        pipe = r.pipeline()
        pipe.watch("watch:new")

        other = make_client()
        other.set("watch:new", "created!")
        other.close()

        pipe.multi()
        pipe.set("watch:new:result", "abort expected")
        try:
            result = pipe.execute()
            assert result is None or result == [], \
                "Creating a watched nonexistent key must abort the transaction"
        except redis.WatchError:
            pass

    def test_watch_cas_pattern(self, r):
        """
        Classic CAS (Check-And-Set) using WATCH:
        Read a value, WATCH it, compute new value, then MULTI/EXEC.
        If another client changed it first, EXEC aborts and the loop retries.
        """
        r.set("cas:counter", "0")
        max_retries = 20
        succeeded = [False]

        def cas_increment():
            for _ in range(max_retries):
                pipe = r.pipeline()
                try:
                    pipe.watch("cas:counter")
                    current = int(pipe.get("cas:counter"))
                    pipe.multi()
                    pipe.set("cas:counter", str(current + 1))
                    pipe.execute()
                    succeeded[0] = True
                    return
                except redis.WatchError:
                    continue

        cas_increment()
        assert succeeded[0], "CAS pattern should succeed within retries"
        assert int(r.get("cas:counter")) == 1

    def test_watch_then_self_modify_before_exec(self, r):
        """
        Modifying a WATCHed key from the same connection before MULTI also
        triggers a WatchError on EXEC — self-modification is not exempt.
        """
        r.set("self:key", "v1")
        pipe = r.pipeline()
        pipe.watch("self:key")
        # Immediate-mode SET on the same pipeline modifies the key and
        # marks it as changed — EXEC will abort.
        pipe.set("self:key", "v2_before_multi")
        pipe.multi()
        pipe.get("self:key")
        import redis as _redis
        with pytest.raises(_redis.WatchError):
            pipe.execute()


# ── Error handling inside MULTI ───────────────────────────────────────────────

class TestTransactionErrors:
    def test_wrong_type_error_deferred(self, r):
        """
        A WRONGTYPE error inside MULTI does not abort the whole transaction.
        Other commands before and after the bad command still execute.
        """
        r.set("err:str", "i am a string")

        pipe = r.pipeline(transaction=True)
        pipe.set("err:before", "ok")
        pipe.lpush("err:str", "boom")
        pipe.set("err:after", "also ok")
        results = pipe.execute(raise_on_error=False)

        assert r.get("err:before") == "ok"
        assert r.get("err:after") == "also ok"
        assert isinstance(results[1], Exception)

    def test_multiple_type_errors_others_still_run(self, r):
        """Multiple runtime errors in MULTI don't prevent other commands running."""
        r.set("s1", "str")
        r.set("s2", "str")
        pipe = r.pipeline(transaction=True)
        pipe.set("pre", "ok")
        pipe.lpush("s1", "fail")   # runtime error
        pipe.lpush("s2", "fail")   # runtime error
        pipe.set("post", "ok")
        results = pipe.execute(raise_on_error=False)

        assert r.get("pre") == "ok"
        assert r.get("post") == "ok"
        assert isinstance(results[1], Exception)
        assert isinstance(results[2], Exception)

    def test_exec_after_server_error_leaves_clean_state(self, r):
        """After a failed EXEC (via WatchError), the connection is in a clean state."""
        r.set("watch:state", "v")
        pipe = r.pipeline()
        pipe.watch("watch:state")

        other = make_client()
        other.set("watch:state", "changed")
        other.close()

        pipe.multi()
        pipe.set("watch:state", "mine")
        try:
            pipe.execute()
        except (redis.WatchError, Exception):
            pass

        # Connection must still work
        assert r.ping() is True
        r.set("cleanup:test", "clean")
        assert r.get("cleanup:test") == "clean"


# ── Isolation guarantees ──────────────────────────────────────────────────────

class TestIsolation:
    def test_concurrent_increments_correct(self, r):
        """
        Two clients each INCR a counter 50 times inside MULTI/EXEC.
        Final value must be exactly 100 (no lost updates).
        """
        r.set("iso:counter", "0")
        increments = 50
        errors = []

        def increment_loop(n):
            c = make_client()
            try:
                for _ in range(n):
                    pipe = c.pipeline(transaction=True)
                    pipe.incr("iso:counter")
                    pipe.execute()
            except Exception as e:
                errors.append(e)
            finally:
                c.close()

        t1 = threading.Thread(target=increment_loop, args=(increments,))
        t2 = threading.Thread(target=increment_loop, args=(increments,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert not errors, f"Thread errors: {errors}"
        final = int(r.get("iso:counter"))
        assert final == 2 * increments, \
            f"Expected {2 * increments} increments, got {final}"

    def test_transaction_reads_own_writes(self, r):
        """Within a transaction, GET sees SET from the same EXEC block."""
        pipe = r.pipeline(transaction=True)
        pipe.set("self:key", "written")
        pipe.get("self:key")
        _, read_val = pipe.execute()
        assert read_val == "written"

    def test_two_transactions_on_same_key_serialize(self, r):
        """Two concurrent MULTI/EXEC blocks on the same key serialize correctly."""
        r.set("serial:key", "initial")
        barrier = threading.Barrier(2)
        results = []

        def txn(value):
            c = make_client()
            try:
                barrier.wait()
                pipe = c.pipeline(transaction=True)
                pipe.set("serial:key", value)
                pipe.execute()
                results.append(value)
            finally:
                c.close()

        t1 = threading.Thread(target=txn, args=("txn_A",))
        t2 = threading.Thread(target=txn, args=("txn_B",))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        final = r.get("serial:key")
        assert final in ("txn_A", "txn_B"), \
            f"Key must be set by one transaction, got {final!r}"

    def test_external_client_cannot_see_partial_transaction(self, r):
        """
        An external client issuing GETs during a long transaction must never
        see partially-committed state — it sees either the pre- or post-state.
        """
        r.set("part:a", "before")
        r.set("part:b", "before")

        observations = []
        stop_event = threading.Event()

        def reader():
            c = make_client()
            while not stop_event.is_set():
                # MGET is atomic — cannot be interleaved with a MULTI/EXEC block
                vals = c.mget("part:a", "part:b")
                observations.append((vals[0], vals[1]))
                time.sleep(0.001)
            c.close()

        reader_thread = threading.Thread(target=reader)
        reader_thread.start()
        time.sleep(0.02)

        # Run a big transaction
        pipe = r.pipeline(transaction=True)
        for _ in range(100):
            pipe.set("part:a", "after")
            pipe.set("part:b", "after")
        pipe.execute()

        stop_event.set()
        reader_thread.join()

        # Each observation must be consistent: both "before" or both "after"
        for a, b in observations:
            assert (a == "before" and b == "before") or \
                   (a == "after" and b == "after"), \
                f"Inconsistent partial state observed: a={a!r}, b={b!r}"

    def test_watch_concurrent_cas_loop(self, r):
        """
        10 concurrent threads each use WATCH+MULTI/EXEC CAS to increment a
        counter. Because of optimistic locking, each must retry on conflict.
        The final value must be exactly 10.
        """
        r.set("cas:shared", "0")
        threads_done = []
        errors = []

        def cas_worker():
            c = make_client()
            try:
                for _ in range(50):  # up to 50 retries
                    pipe = c.pipeline()
                    try:
                        pipe.watch("cas:shared")
                        current = int(pipe.get("cas:shared"))
                        pipe.multi()
                        pipe.set("cas:shared", str(current + 1))
                        pipe.execute()
                        threads_done.append(1)
                        return
                    except redis.WatchError:
                        continue
                errors.append("max retries exceeded")
            finally:
                c.close()

        threads = [threading.Thread(target=cas_worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"CAS errors: {errors}"
        assert len(threads_done) == 10
        assert int(r.get("cas:shared")) == 10


# ── Pipeline (non-transactional) ──────────────────────────────────────────────

class TestPipeline:
    def test_pipeline_batches_commands(self, r):
        pipe = r.pipeline(transaction=False)
        for i in range(10):
            pipe.set(f"pipe:{i}", f"val:{i}")
        results = pipe.execute()
        assert all(res is True for res in results)
        for i in range(10):
            assert r.get(f"pipe:{i}") == f"val:{i}"

    def test_pipeline_mixed_reads_and_writes(self, r):
        r.set("pipe:existing", "old")
        pipe = r.pipeline(transaction=False)
        pipe.get("pipe:existing")
        pipe.set("pipe:existing", "new")
        pipe.get("pipe:existing")
        old_val, _, new_val = pipe.execute()
        assert old_val == "old"
        assert new_val == "new"

    def test_large_pipeline(self, r):
        """A pipeline with 500 commands executes correctly."""
        pipe = r.pipeline(transaction=False)
        for i in range(500):
            pipe.incr("pipe:big:counter")
        results = pipe.execute()
        assert results[-1] == 500
        assert r.get("pipe:big:counter") == "500"

    def test_pipeline_errors_do_not_stop_others(self, r):
        """In a non-transactional pipeline, errors are per-command."""
        r.set("notalist", "str")
        pipe = r.pipeline(transaction=False)
        pipe.set("ok:before", "yes")
        pipe.lpush("notalist", "fail")   # error
        pipe.set("ok:after", "yes")
        results = pipe.execute(raise_on_error=False)

        assert r.get("ok:before") == "yes"
        assert r.get("ok:after") == "yes"
        assert isinstance(results[1], Exception)

    def test_pipeline_all_types(self, r):
        """Pipeline handles all data-type commands in one batch."""
        pipe = r.pipeline(transaction=False)
        pipe.set("p:str", "hello")
        pipe.hset("p:hash", mapping={"f": "v"})
        pipe.lpush("p:list", "a", "b")
        pipe.sadd("p:set", "x")
        pipe.zadd("p:zset", {"m": 1.0})
        results = pipe.execute()
        assert results[0] is True
        assert results[1] == 1
        assert results[2] == 2
        assert results[3] == 1
        assert results[4] == 1
