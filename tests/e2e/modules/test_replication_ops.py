"""
e2e tests for P2 replication and operational correctness:
  - WAIT with real ack tracking
  - maxmemory noeviction enforcement
  - CLIENT PAUSE / UNPAUSE enforcement

These tests spin up extra KiviDB processes (master + replica pairs) where
needed, isolated from the main session server so they don't interfere with
other test modules.
"""
from __future__ import annotations

import time
import socket
import subprocess
import threading
import os

import pytest
import redis

# ── Helpers ───────────────────────────────────────────────────────────────────

KIVI_HOST = os.environ.get("KIVI_HOST", "127.0.0.1")
KIVI_BINARY = os.environ.get("KIVI_BINARY", "../../bin/kividb")
_IS_REDIS = any(name in os.path.basename(KIVI_BINARY)
                for name in ("redis-stack-server", "redis-server"))


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_ready(host: str, port: int, timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            c = redis.Redis(host=host, port=port,
                            socket_connect_timeout=0.5, socket_timeout=1)
            c.dbsize()
            c.close()
            return True
        except redis.exceptions.BusyLoadingError:
            time.sleep(0.1)
        except Exception:
            time.sleep(0.05)
    return False


def start_server(port: int, extra_args: list[str] | None = None) -> subprocess.Popen:
    extra = list(extra_args or [])
    if _IS_REDIS:
        aof_enabled = "--aof" in extra
        redis_extra = [a for a in extra if a != "--aof"]
        args = [KIVI_BINARY, "--port", str(port), "--loglevel", "warning",
                "--save", "", "--appendonly", "yes" if aof_enabled else "no",
                "--protected-mode", "no"] + redis_extra
    else:
        args = [KIVI_BINARY, "--port", str(port), "--loglevel", "error",
                "--io-model", "tokio"] + extra
    proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not _wait_ready(KIVI_HOST, port, timeout=30):
        proc.terminate()
        pytest.fail(f"Server failed to start on port {port}")
    return proc


def make_client(port: int, **kwargs) -> redis.Redis:
    return redis.Redis(host=KIVI_HOST, port=port, decode_responses=True,
                       socket_timeout=5, **kwargs)


# ── WAIT tests ────────────────────────────────────────────────────────────────

class TestWait:
    def test_wait_no_replicas_returns_zero_immediately(self):
        """WAIT on a standalone master returns 0 without blocking."""
        port = free_port()
        proc = start_server(port)
        try:
            r = make_client(port)
            t0 = time.monotonic()
            result = r.execute_command("WAIT", 1, 100)
            elapsed = time.monotonic() - t0
            assert result == 0
            # Should return after the timeout (100ms), not instantly hang forever
            assert elapsed < 1.0
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_wait_zero_replicas_requested_returns_immediately(self):
        """WAIT 0 <timeout> always returns 0 immediately."""
        port = free_port()
        proc = start_server(port)
        try:
            r = make_client(port)
            t0 = time.monotonic()
            result = r.execute_command("WAIT", 0, 5000)
            elapsed = time.monotonic() - t0
            assert result == 0
            assert elapsed < 0.5, f"WAIT 0 blocked for {elapsed:.2f}s"
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_wait_with_replica_ack_tracking(self):
        """
        WAIT 1 <timeout> returns 1 once the replica has acked the write.
        Full master→replica pipeline: handshake, RDB, command stream, ACK.
        """
        master_port = free_port()
        replica_port = free_port()

        master_proc = start_server(master_port)
        replica_proc = start_server(replica_port, [
            "--replicaof", KIVI_HOST, str(master_port)
        ])

        try:
            master = make_client(master_port)
            replica = make_client(replica_port)

            # Wait for replication handshake and full sync to complete.
            # Check both: master sees connected_slaves >= 1 AND
            # replica reports master_link_status=up with no sync in progress.
            for _ in range(100):
                m_info = master.info("replication")
                r_info = replica.info("replication")
                if (m_info.get("connected_slaves", 0) >= 1
                        and r_info.get("master_link_status") == "up"
                        and r_info.get("master_sync_in_progress", 1) == 0):
                    break
                time.sleep(0.1)
            else:
                pytest.fail("Replica did not fully sync within 10s")

            # Issue a write on the master
            master.set("wait_test_key", "hello")

            # WAIT should return 1 once the replica acks (up to 10s)
            result = master.execute_command("WAIT", 1, 10000)
            assert result == 1, f"WAIT returned {result}, expected 1"

            # Confirm the key is on the replica
            val = replica.get("wait_test_key")
            assert val == "hello", f"Replica has wrong value: {val!r}"

        finally:
            replica_proc.terminate()
            replica_proc.wait(timeout=5)
            master_proc.terminate()
            master_proc.wait(timeout=5)

    def test_wait_timeout_returns_partial_count(self):
        """WAIT requesting more replicas than connected returns 0 immediately when none connected."""
        port = free_port()
        proc = start_server(port)
        try:
            r = make_client(port)
            result = r.execute_command("WAIT", 5, 200)
            assert result == 0, f"Expected 0 replicas acked, got {result}"
        finally:
            proc.terminate()
            proc.wait(timeout=5)


# ── maxmemory tests ───────────────────────────────────────────────────────────

class TestMaxmemory:
    def test_maxmemory_zero_means_unlimited(self):
        """maxmemory=0 never blocks writes regardless of RSS."""
        port = free_port()
        proc = start_server(port)
        try:
            r = make_client(port)
            r.execute_command("CONFIG", "SET", "maxmemory", "0")
            for i in range(200):
                r.set(f"unlim:{i}", "x" * 64)
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_maxmemory_noeviction_rejects_writes(self):
        port = free_port()
        proc = start_server(port)
        try:
            r = make_client(port)
            r.execute_command("CONFIG", "SET", "maxmemory", "1")
            time.sleep(1.1)

            with pytest.raises((redis.OutOfMemoryError, redis.ResponseError)):
                r.set("oom_key", "value")

            # Reads must pass through
            result = r.get("nonexistent_key")
            assert result is None

        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_maxmemory_config_set_takes_effect_at_runtime(self):
        """CONFIG SET maxmemory updates the limit without restart."""
        port = free_port()
        proc = start_server(port)
        try:
            r = make_client(port)

            # Confirm we can write freely
            r.set("before_limit", "ok")

            # Apply a real but generous limit (1 GiB) — should not block
            r.execute_command("CONFIG", "SET", "maxmemory", str(1024 ** 3))
            r.set("within_limit", "ok")

            # CONFIG GET must reflect the new value
            result = r.execute_command("CONFIG", "GET", "maxmemory")
            # Returns flat list ["maxmemory", "value"]
            assert result[1] == str(1024 ** 3), f"CONFIG GET returned {result}"

            # Clear it — writes must work again
            r.execute_command("CONFIG", "SET", "maxmemory", "0")
            r.set("after_clear", "ok")

        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_maxmemory_ping_and_auth_never_blocked(self):
        """PING is never blocked by maxmemory enforcement."""
        port = free_port()
        proc = start_server(port)
        try:
            r = make_client(port)
            r.execute_command("CONFIG", "SET", "maxmemory", "1")
            time.sleep(1.1)

            # PING must work
            assert r.ping()

            # GET (read) must work
            r.get("any_key")  # nil is fine, error is not

        finally:
            proc.terminate()
            proc.wait(timeout=5)


# ── CLIENT PAUSE tests ────────────────────────────────────────────────────────

class TestClientPause:
    def test_pause_and_unpause_returns_ok(self):
        """CLIENT PAUSE and UNPAUSE return OK."""
        port = free_port()
        proc = start_server(port)
        try:
            r = make_client(port)
            result = r.execute_command("CLIENT", "PAUSE", 100)
            assert result is True or result == "OK"
            result = r.execute_command("CLIENT", "UNPAUSE")
            assert result is True or result == "OK"
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_pause_blocks_writes_until_expiry(self):
        """
        Writes on a second connection block while CLIENT PAUSE is active,
        then complete once the pause expires naturally.
        """
        port = free_port()
        proc = start_server(port)
        try:
            ctrl = make_client(port)
            # Pause all commands for 500ms
            ctrl.execute_command("CLIENT", "PAUSE", 500)

            results = {}

            def do_write():
                w = make_client(port)
                t0 = time.monotonic()
                w.set("paused_key", "val")
                results["elapsed"] = time.monotonic() - t0

            t = threading.Thread(target=do_write)
            t.start()
            t.join(timeout=5)

            assert "elapsed" in results, "Write thread never completed"
            # Must have waited at least 400ms (500ms pause minus scheduling slack)
            assert results["elapsed"] >= 0.4, (
                f"Write completed too fast: {results['elapsed']:.3f}s"
            )

        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_unpause_unblocks_writes_early(self):
        port = free_port()
        proc = start_server(port)
        try:
            ctrl = make_client(port)
            results = {}
            barrier = threading.Barrier(2)

            def do_write():
                w = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True,
                                socket_timeout=None,
                                socket_connect_timeout=5)
                # Signal ready, wait for main thread to confirm pause is active
                barrier.wait(timeout=5)
                t0 = time.monotonic()
                try:
                    w.set("early_unblock", "val")
                    results["elapsed"] = time.monotonic() - t0
                except Exception as e:
                    results["error"] = str(e)
                    results["elapsed"] = time.monotonic() - t0

            t = threading.Thread(target=do_write)
            t.start()

            # Set pause, then release the barrier so do_write sends SET
            ctrl.execute_command("CLIENT", "PAUSE", 5000)
            barrier.wait(timeout=5)  # do_write is now about to send SET

            # Give it a moment to actually send and block
            time.sleep(0.15)
            ctrl.execute_command("CLIENT", "UNPAUSE")

            t.join(timeout=8)
            assert t.is_alive() is False, "Write thread still blocked after UNPAUSE"
            assert "error" not in results, f"Write errored: {results.get('error')}"
            assert "elapsed" in results, "Write never completed"
            if not _IS_REDIS:
                assert results["elapsed"] < 4.5, (
                    f"Write didn't unblock early: {results['elapsed']:.3f}s"
                )

        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_pause_write_mode_allows_reads(self):
        """CLIENT PAUSE <ms> WRITE blocks writes but reads pass through."""
        port = free_port()
        proc = start_server(port)
        try:
            r = make_client(port)
            r.set("readable", "yes")

            # Pause writes only for 1s
            r.execute_command("CLIENT", "PAUSE", 1000, "WRITE")

            # GET must return immediately
            t0 = time.monotonic()
            val = r.get("readable")
            elapsed = time.monotonic() - t0

            assert val == "yes"
            assert elapsed < 0.1, f"GET blocked during WRITE-only pause: {elapsed:.3f}s"

            r.execute_command("CLIENT", "UNPAUSE")

        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_pause_write_mode_blocks_sets(self):
        """CLIENT PAUSE <ms> WRITE blocks SET commands."""
        port = free_port()
        proc = start_server(port)
        try:
            ctrl = make_client(port)
            ctrl.execute_command("CLIENT", "PAUSE", 2000, "WRITE")

            results = {}

            def do_set():
                w = make_client(port)
                t0 = time.monotonic()
                w.set("write_paused", "val")
                results["elapsed"] = time.monotonic() - t0

            t = threading.Thread(target=do_set)
            t.start()

            time.sleep(0.1)
            ctrl.execute_command("CLIENT", "UNPAUSE")
            t.join(timeout=5)

            assert "elapsed" in results
            # Must have blocked for at least 80ms (the sleep above)
            assert results["elapsed"] >= 0.08, (
                f"SET returned too fast: {results['elapsed']:.3f}s"
            )

        finally:
            proc.terminate()
            proc.wait(timeout=5)

class TestWaitAof:
    def test_waitaof_aof_disabled_returns_immediately(self):
        """When AOF is disabled, WAITAOF 0 returns [0, 0] without blocking,
        and WAITAOF with numlocal > 0 is an error, as in Redis 7.4+."""
        port = free_port()
        # No --aof flag = AOF disabled
        proc = start_server(port)
        try:
            r = make_client(port)
            with pytest.raises(redis.ResponseError, match="appendonly is disabled"):
                r.execute_command("WAITAOF", 1, 0, 0)
            t0 = time.monotonic()
            result = r.execute_command("WAITAOF", 0, 0, 0)
            elapsed = time.monotonic() - t0
            assert len(result) == 2
            assert result[0] == 0  # nothing is fsynced to an AOF
            assert result[1] == 0  # replica AOF not tracked
            assert elapsed < 0.5, f"WAITAOF blocked unexpectedly: {elapsed:.3f}s"
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_waitaof_aof_enabled_fsyncs_within_timeout(self):
        port = free_port()
        proc = start_server(port, ["--aof"])
        try:
            r = make_client(port)
            r.set("waitaof_key", "value")
            result = r.execute_command("WAITAOF", 1, 0, 2000)
            assert result[0] == 1, f"local AOF not fsynced: {result}"
            assert result[1] == 0
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)

    def test_waitaof_timeout_zero_returns_current_state(self):
        """WAITAOF with a short timeout returns quickly with current fsync state."""
        port = free_port()
        proc = start_server(port, ["--aof"])
        try:
            r = make_client(port)
            r.set("instant_check", "val")
            # Use 100ms timeout, not 0 — timeout=0 means block indefinitely
            result = r.execute_command("WAITAOF", 1, 0, 100)
            assert len(result) == 2
            assert result[0] in (0, 1)
            assert result[1] == 0
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_waitaof_replica_always_zero(self):
        """Replica AOF ack tracking is not implemented — always 0."""
        port = free_port()
        proc = start_server(port, ["--aof"])
        try:
            r = make_client(port)
            r.set("replica_aof_key", "val")
            result = r.execute_command("WAITAOF", 0, 1, 500)
            assert len(result) == 2
            assert result[1] == 0, f"replica_fsynced should always be 0: {result}"
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_waitaof_multiple_writes_all_fsynced(self):
        """All writes issued before WAITAOF are covered by a single fsync."""
        port = free_port()
        proc = start_server(port, ["--aof"])
        try:
            r = make_client(port)

            for i in range(50):
                r.set(f"bulk:{i}", "x" * 64)

            result = r.execute_command("WAITAOF", 1, 0, 3000)
            assert result[0] == 1, f"50 writes not fsynced within 3s: {result}"
        finally:
            proc.terminate()
            proc.wait(timeout=5)