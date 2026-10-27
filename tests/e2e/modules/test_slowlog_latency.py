"""
e2e tests for SLOWLOG and LATENCY commands.

Place this file at: tests/e2e/modules/test_slowlog_latency.py

Every test uses the `r` fixture from conftest.py — a fresh redis.Redis client
with FLUSHDB already called. The `kivi_server` fixture handles server lifecycle.

Coverage:
  SLOWLOG GET [count]
  SLOWLOG LEN
  SLOWLOG RESET
  SLOWLOG unknown subcommand → error

  LATENCY LATEST
  LATENCY HISTORY <event>
  LATENCY RESET [event ...]
  LATENCY GRAPH <event>
  LATENCY HISTOGRAM
  LATENCY unknown subcommand → error

  Cross-cutting:
  - Fast-path commands (GET/SET) appear in latency tracker
  - Slow-path fallthrough commands appear in latency tracker
  - EXEC (MULTI block) commands are individually timed
  - Slowlog threshold is respected (threshold=0 logs everything,
    threshold=MAX logs nothing)
  - Slowlog max_len eviction (newest entry bumps oldest)
  - Concurrent writes from multiple clients don't corrupt state
  - Entry structure matches Redis wire format exactly
  - SLOWLOG RESET clears all entries and resets LEN to 0
  - LATENCY RESET clears named events only
  - LATENCY HISTORY returns entries in chronological order
  - LATENCY GRAPH returns a non-empty string for a known event
"""

from __future__ import annotations

import time
import threading
import subprocess
import os

import pytest
import redis

# ── Helpers ───────────────────────────────────────────────────────────────────

KIVI_HOST = os.environ.get("KIVI_HOST", "127.0.0.1")
KIVI_PORT = int(os.environ.get("KIVI_PORT", "6380"))
KIVI_BINARY = os.environ.get("KIVI_BINARY", "../../bin/kividb")
_IS_REDIS = any(name in os.path.basename(KIVI_BINARY)
                for name in ("redis-stack-server", "redis-server"))


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


def make_client(**kwargs) -> redis.Redis:
    return redis.Redis(
        host=KIVI_HOST,
        port=KIVI_PORT,
        decode_responses=True,
        **kwargs,
    )


def start_custom_server(port: int, extra_args: list[str]) -> subprocess.Popen:
    """
    Start a server instance on `port` with `extra_args` and block until ready.
    Returns the Popen handle — caller must terminate() it.
    """
    if _IS_REDIS:
        base_args = [KIVI_BINARY, "--port", str(port), "--loglevel", "warning",
                     "--save", "", "--appendonly", "no", "--protected-mode", "no"]
    else:
        base_args = [KIVI_BINARY, "--port", str(port), "--loglevel", "error"]
    proc = subprocess.Popen(
        base_args + extra_args,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if not _wait_ready(KIVI_HOST, port, timeout=30):
        proc.terminate()
        pytest.fail(f"Server failed to start on port {port}")
    return proc


def free_port() -> int:
    """Return an unused TCP port."""
    import socket as _socket
    with _socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def drain_slowlog(r: redis.Redis) -> None:
    """Clear the slowlog so tests start from a known state."""
    r.execute_command("SLOWLOG", "RESET")


def force_slowlog_entry(r: redis.Redis) -> None:
    """
    Issue a DEBUG SLEEP (or a large KEYS scan) to generate a guaranteed
    slowlog entry when the threshold is 0.  We use KEYS * on a populated
    DB because DEBUG SLEEP is not always available.

    With threshold=0 every command is logged, so a plain SET is enough —
    but we use a slightly heavier command to make assertions more robust.
    """
    # threshold=0 means every command is recorded; SET is sufficient.
    r.set("slowlog_probe", "1")


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="class")
def zero_threshold_server():
    """
    A KiviDB instance whose slowlog threshold is 0 (log every command).
    Scoped to the class so we don't restart for every test method.
    """
    port = free_port()
    proc = start_custom_server(
        port,
        ["--slowlog-log-slower-than", "0", "--slowlog-max-len", "128"],
    )
    client = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True)
    client.flushdb()
    yield client
    client.close()
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="class")
def maxlen_server():
    """
    A KiviDB instance with threshold=0 and max_len=5.
    Used to test eviction behaviour.
    """
    port = free_port()
    proc = start_custom_server(
        port,
        ["--slowlog-log-slower-than", "0", "--slowlog-max-len", "5"],
    )
    client = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True)
    client.flushdb()
    yield client
    client.close()
    proc.terminate()
    proc.wait(timeout=5)


# ══════════════════════════════════════════════════════════════════════════════
# SLOWLOG tests
# ══════════════════════════════════════════════════════════════════════════════

class TestSlowlogBasic:
    """SLOWLOG LEN / RESET with the default shared server."""

    def test_slowlog_len_returns_integer(self, r):
        length = r.execute_command("SLOWLOG", "LEN")
        assert isinstance(length, int)
        assert length >= 0

    def test_slowlog_reset_returns_ok(self, r):
        result = r.execute_command("SLOWLOG", "RESET")
        assert result in (True, "OK", b"OK")

    def test_slowlog_len_zero_after_reset(self, r):
        r.execute_command("SLOWLOG", "RESET")
        assert r.execute_command("SLOWLOG", "LEN") == 0

    def test_slowlog_get_returns_list(self, r):
        result = r.execute_command("SLOWLOG", "GET")
        assert isinstance(result, list)

    def test_slowlog_get_empty_after_reset(self, r):
        r.execute_command("SLOWLOG", "RESET")
        assert r.execute_command("SLOWLOG", "GET") == []

    def test_slowlog_unknown_subcommand_errors(self, r):
        with pytest.raises(redis.ResponseError):
            r.execute_command("SLOWLOG", "BADSUBCMD")

    def test_slowlog_no_subcommand_errors(self, r):
        with pytest.raises(redis.ResponseError):
            r.execute_command("SLOWLOG")


class TestSlowlogThresholdZero:
    """Tests that require threshold=0 so every command is logged."""

    def test_entry_appears_after_set(self, zero_threshold_server):
        r = zero_threshold_server
        r.execute_command("SLOWLOG", "RESET")
        r.set("k", "v")
        length = r.execute_command("SLOWLOG", "LEN")
        # RESET itself is counted, then SET — at least 1 new entry
        assert length >= 1

    def test_len_increments_with_commands(self, zero_threshold_server):
        r = zero_threshold_server
        r.execute_command("SLOWLOG", "RESET")
        for i in range(5):
            r.set(f"count_test_{i}", i)
        length = r.execute_command("SLOWLOG", "LEN")
        # 5 SETs were issued; RESET itself is also an entry, so >= 5
        assert length >= 5

    def test_get_entry_structure_seven_fields(self, zero_threshold_server):
        """
        Each entry must be a 7-element list matching Redis wire format:
          [id, unix_timestamp, duration_us, [cmd_args], client_addr, client_name,
           total_arg_count]
        The last was added in Redis 8.10: the number of arguments the command
        had, which the (possibly truncated) cmd_args no longer tells.
        """
        r = zero_threshold_server
        r.execute_command("SLOWLOG", "RESET")
        r.set("structure_test", "hello")
        entries = r.execute_command("SLOWLOG", "GET")
        assert len(entries) >= 1
        entry = entries[0]
        assert len(entry) == 7, f"Expected 7 fields, got {len(entry)}: {entry}"
        assert entry[6] == 3, f"SET key value has 3 arguments: {entry}"

    def test_entry_id_is_integer(self, zero_threshold_server):
        r = zero_threshold_server
        r.execute_command("SLOWLOG", "RESET")
        r.set("id_test", "x")
        entry = r.execute_command("SLOWLOG", "GET")[0]
        assert isinstance(entry[0], int)

    def test_entry_timestamp_is_recent_unix_seconds(self, zero_threshold_server):
        r = zero_threshold_server
        r.execute_command("SLOWLOG", "RESET")
        before = int(time.time())
        r.set("ts_test", "x")
        after = int(time.time()) + 1
        entry = r.execute_command("SLOWLOG", "GET")[0]
        ts = entry[1]
        assert isinstance(ts, int)
        assert before <= ts <= after, f"Timestamp {ts} out of range [{before}, {after}]"

    def test_entry_duration_is_non_negative_integer(self, zero_threshold_server):
        r = zero_threshold_server
        r.execute_command("SLOWLOG", "RESET")
        r.set("dur_test", "x")
        entry = r.execute_command("SLOWLOG", "GET")[0]
        duration_us = entry[2]
        assert isinstance(duration_us, int)
        assert duration_us >= 0

    def test_entry_cmd_is_list_of_strings(self, zero_threshold_server):
        r = zero_threshold_server
        r.execute_command("SLOWLOG", "RESET")
        r.set("cmd_test", "myvalue")
        entries = r.execute_command("SLOWLOG", "GET")
        # Find the SET entry
        set_entry = next(
            (e for e in entries if isinstance(e[3], list) and e[3] and e[3][0].upper() == "SET"),
            None,
        )
        assert set_entry is not None, "Could not find SET entry in slowlog"
        cmd_args = set_entry[3]
        assert isinstance(cmd_args, list)
        assert len(cmd_args) == 3  # SET key value
        assert cmd_args[0].upper() == "SET"
        assert cmd_args[1] == "cmd_test"
        assert cmd_args[2] == "myvalue"

    def test_entry_client_addr_is_string(self, zero_threshold_server):
        r = zero_threshold_server
        r.execute_command("SLOWLOG", "RESET")
        r.set("addr_test", "x")
        entry = r.execute_command("SLOWLOG", "GET")[0]
        client_addr = entry[4]
        assert isinstance(client_addr, str)
        # Should contain host:port pattern
        assert ":" in client_addr or client_addr == ""

    def test_entry_client_name_is_string(self, zero_threshold_server):
        r = zero_threshold_server
        r.execute_command("SLOWLOG", "RESET")
        r.set("name_test", "x")
        entry = r.execute_command("SLOWLOG", "GET")[0]
        client_name = entry[5]
        assert isinstance(client_name, str)  # empty string is fine (unnamed client)

    def test_entry_ids_are_monotonically_increasing(self, zero_threshold_server):
        r = zero_threshold_server
        r.execute_command("SLOWLOG", "RESET")
        for i in range(5):
            r.set(f"mono_{i}", i)
        entries = r.execute_command("SLOWLOG", "GET")
        # GET returns newest first; IDs should be strictly decreasing in the list
        ids = [e[0] for e in entries]
        for a, b in zip(ids, ids[1:]):
            assert a > b, f"IDs not monotonically decreasing: {ids}"

    def test_get_with_count_limits_results(self, zero_threshold_server):
        r = zero_threshold_server
        r.execute_command("SLOWLOG", "RESET")
        for i in range(10):
            r.set(f"limit_{i}", i)
        all_entries = r.execute_command("SLOWLOG", "GET")
        limited = r.execute_command("SLOWLOG", "GET", "3")
        assert len(limited) <= 3
        assert len(limited) <= len(all_entries)

    def test_get_with_count_zero_returns_empty(self, zero_threshold_server):
        r = zero_threshold_server
        r.execute_command("SLOWLOG", "RESET")
        r.set("zero_count", "x")
        result = r.execute_command("SLOWLOG", "GET", "0")
        assert result == []

    def test_get_with_large_count_returns_all(self, zero_threshold_server):
        r = zero_threshold_server
        r.execute_command("SLOWLOG", "RESET")
        for i in range(5):
            r.set(f"large_{i}", i)
        # Use a single GET call — don't compare two GETs since each GET
        # records itself when threshold=0.
        entries = r.execute_command("SLOWLOG", "GET", "9999")
        assert len(entries) >= 5  # at minimum: RESET + 5 SETs

    def test_reset_clears_all_entries(self, zero_threshold_server):
        r = zero_threshold_server
        for i in range(5):
            r.set(f"clear_{i}", i)
        r.execute_command("SLOWLOG", "RESET")
        # With threshold=0, SLOWLOG LEN records itself. So after RESET,
        # LEN will return 1 (the LEN call). Assert <= 1 instead of == 0.
        assert r.execute_command("SLOWLOG", "LEN") <= 1

    def test_get_fast_path_commands_are_logged(self, zero_threshold_server):
        """
        GET and SET go through the fast path. With threshold=0 they must
        appear in the slowlog — verifies fast-path timing is wired.
        """
        r = zero_threshold_server
        r.execute_command("SLOWLOG", "RESET")
        r.set("fast_path_key", "fast_path_val")
        r.get("fast_path_key")
        length = r.execute_command("SLOWLOG", "LEN")
        assert length >= 2, (
            f"Expected at least 2 entries (SET + GET) in slowlog, got {length}. "
            "Fast-path timing may not be wired."
        )

    def test_multi_exec_commands_are_individually_logged(self, zero_threshold_server):
        """
        Commands queued inside MULTI/EXEC must each produce their own
        slowlog entry — verifies EXEC-loop timing is wired.
        """
        r = zero_threshold_server
        r.execute_command("SLOWLOG", "RESET")

        pipe = r.pipeline(transaction=True)
        pipe.set("tx_a", "1")
        pipe.set("tx_b", "2")
        pipe.get("tx_a")
        pipe.execute()

        length = r.execute_command("SLOWLOG", "LEN")
        # At minimum: MULTI, SET, SET, GET, EXEC — but MULTI/EXEC may or may
        # not be logged depending on path; the inner SET+GET must be present.
        assert length >= 3, (
            f"Expected at least 3 entries for MULTI block, got {length}. "
            "EXEC-loop timing may not be wired."
        )

    def test_named_client_appears_in_entry(self, zero_threshold_server):
        """CLIENT SETNAME should appear in the entry's client_name field."""
        port = zero_threshold_server.connection_pool.connection_kwargs["port"]
        named = redis.Redis(
            host=KIVI_HOST,
            port=port,
            decode_responses=True,
            client_name="test-observer",
        )
        named.execute_command("SLOWLOG", "RESET")
        named.set("named_client_key", "v")
        entries = named.execute_command("SLOWLOG", "GET")
        set_entry = next(
            (e for e in entries if isinstance(e[3], list) and e[3] and e[3][0].upper() == "SET"),
            None,
        )
        assert set_entry is not None
        # client_name field (index 5)
        assert set_entry[5] == "test-observer", (
            f"Expected client_name='test-observer', got '{set_entry[5]}'"
        )
        named.close()


class TestSlowlogMaxLen:
    """Eviction: entries beyond max_len are dropped (oldest first)."""

    def test_entries_capped_at_max_len(self, maxlen_server):
        r = maxlen_server
        r.execute_command("SLOWLOG", "RESET")
        # Issue more commands than max_len (5)
        for i in range(20):
            r.set(f"cap_{i}", i)
        length = r.execute_command("SLOWLOG", "LEN")
        assert length <= 5, f"Expected <= 5 entries (max_len=5), got {length}"

    def test_newest_entries_are_kept(self, maxlen_server):
        """After eviction the newest entries (highest IDs) must survive."""
        r = maxlen_server
        r.execute_command("SLOWLOG", "RESET")
        for i in range(10):
            r.set(f"evict_{i}", i)
        entries = r.execute_command("SLOWLOG", "GET")
        assert len(entries) <= 5
        # The first entry (newest) must have a higher ID than the last (oldest)
        if len(entries) > 1:
            assert entries[0][0] > entries[-1][0]

    def test_len_does_not_exceed_max_len(self, maxlen_server):
        r = maxlen_server
        r.execute_command("SLOWLOG", "RESET")
        for i in range(50):
            r.set(f"overflow_{i}", i)
        assert r.execute_command("SLOWLOG", "LEN") <= 5


class TestSlowlogHighThreshold:
    """With a very high threshold, ordinary commands must NOT be logged."""

    def test_fast_commands_not_logged_above_threshold(self):
        port = free_port()
        # 10 seconds threshold — no normal command will take that long
        proc = start_custom_server(
            port,
            ["--slowlog-log-slower-than", "10000000", "--slowlog-max-len", "128"],
        )
        r = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True)
        try:
            r.flushdb()
            r.execute_command("SLOWLOG", "RESET")
            for i in range(20):
                r.set(f"nolog_{i}", i)
                r.get(f"nolog_{i}")
            length = r.execute_command("SLOWLOG", "LEN")
            assert length == 0, (
                f"Expected 0 slowlog entries with 10s threshold, got {length}"
            )
        finally:
            r.close()
            proc.terminate()
            proc.wait(timeout=5)


class TestSlowlogConcurrent:
    """Concurrent writes from multiple clients must not corrupt slowlog state."""

    def test_concurrent_writes_no_corruption(self, zero_threshold_server):
        r = zero_threshold_server
        r.execute_command("SLOWLOG", "RESET")
        port = r.connection_pool.connection_kwargs["port"]
        errors = []

        def worker(worker_id: int) -> None:
            c = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True)
            try:
                for i in range(20):
                    c.set(f"concurrent_{worker_id}_{i}", i)
            except Exception as e:
                errors.append(e)
            finally:
                c.close()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"Concurrent errors: {errors}"

        # Slowlog must still be readable and internally consistent
        entries = r.execute_command("SLOWLOG", "GET")
        length = r.execute_command("SLOWLOG", "LEN")
        assert isinstance(entries, list)
        assert isinstance(length, int)
        assert length >= 0
        # Every returned entry must have exactly 7 fields
        for entry in entries:
            assert len(entry) == 7, f"Corrupted entry: {entry}"


# ══════════════════════════════════════════════════════════════════════════════
# LATENCY tests
# ══════════════════════════════════════════════════════════════════════════════

def _slow_command(r):
    """A command the latency monitor records: one that takes at least
    `latency-monitor-threshold` (in ms), which it goes by, as in Redis, and
    not `slowlog-log-slower-than`."""
    r.config_set("latency-monitor-threshold", 1)
    r.execute_command("DEBUG", "SLEEP", "0.005")


class TestLatencyLatest:
    """LATENCY LATEST"""

    def test_latest_returns_list(self, r):
        result = r.execute_command("LATENCY", "LATEST")
        assert isinstance(result, list)

    def test_latest_populates_after_commands(self):
        if _IS_REDIS:
            pytest.skip("Redis LATENCY tracks I/O ops, not individual commands")
        port = free_port()
        proc = start_custom_server(port, ["--slowlog-log-slower-than", "0"])  # ← enable
        r = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True)
        try:
            r.flushdb()
            _slow_command(r)
            result = r.execute_command("LATENCY", "LATEST")
            assert isinstance(result, list)
            event_names = [entry[0] for entry in result]
            assert any(
                name in ("command", "fast-command") for name in event_names
            ), f"No latency events found. Events present: {event_names}"
        finally:
            r.close()
            proc.terminate()
            proc.wait(timeout=5)

    def test_latest_entry_has_four_fields(self, r):
        # Event, time, latest ms and most ms, as in Redis.
        _slow_command(r)
        result = r.execute_command("LATENCY", "LATEST")
        assert result
        for entry in result:
            assert len(entry) == 4, f"Expected 4 fields per entry, got {len(entry)}: {entry}"
            assert entry[3] >= entry[2]

    def test_latest_timestamp_is_unix_seconds(self, r):
        for i in range(3):
            r.set(f"latest_ts_{i}", i)
        result = r.execute_command("LATENCY", "LATEST")
        now = int(time.time())
        for entry in result:
            ts = entry[1]
            assert isinstance(ts, int)
            # Timestamp should be within the last minute
            assert now - 60 <= ts <= now + 5, f"Suspicious timestamp: {ts}"

    def test_latest_latency_is_non_negative(self, r):
        for i in range(3):
            r.set(f"latest_lat_{i}", i)
        result = r.execute_command("LATENCY", "LATEST")
        for entry in result:
            assert isinstance(entry[2], int)
            assert entry[2] >= 0


class TestLatencyHistory:
    """LATENCY HISTORY <event>"""

    def test_history_unknown_event_returns_empty(self, r):
        result = r.execute_command("LATENCY", "HISTORY", "nonexistent_event_xyz")
        assert result == []

    def test_history_requires_event_argument(self, r):
        with pytest.raises(redis.ResponseError):
            r.execute_command("LATENCY", "HISTORY")

    def test_history_command_event_populates(self):
        """After running commands, LATENCY HISTORY 'command' must have samples."""
        if _IS_REDIS:
            pytest.skip("Redis LATENCY tracks I/O ops, not individual commands")
        port = free_port()
        proc = start_custom_server(port, ["--slowlog-log-slower-than", "0"])  # ← enable
        r = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True)
        try:
            r.flushdb()
            _slow_command(r)
            cmd_history = r.execute_command("LATENCY", "HISTORY", "command")
            fast_history = r.execute_command("LATENCY", "HISTORY", "fast-command")
            total_samples = len(cmd_history) + len(fast_history)
            assert total_samples > 0, (
                "Expected at least one latency sample in HISTORY command or fast-command"
            )
        finally:
            r.close()
            proc.terminate()
            proc.wait(timeout=5)

    def test_history_entry_has_two_fields(self):
        port = free_port()
        proc = start_custom_server(port, [])
        r = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True)
        try:
            r.flushdb()
            for i in range(10):
                r.set(f"hist_struct_{i}", i)
            for event in ("command", "fast-command"):
                history = r.execute_command("LATENCY", "HISTORY", event)
                for entry in history:
                    assert len(entry) == 2, (
                        f"Expected 2 fields [timestamp, latency_ms], got {len(entry)}: {entry}"
                    )
        finally:
            r.close()
            proc.terminate()
            proc.wait(timeout=5)

    def test_history_entries_are_chronological(self):
        """Samples must be returned oldest-first (ascending timestamp)."""
        port = free_port()
        proc = start_custom_server(port, [])
        r = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True)
        try:
            r.flushdb()
            for i in range(20):
                r.set(f"chron_{i}", i)
                time.sleep(0.01)  # ensure distinct timestamps
            for event in ("command", "fast-command"):
                history = r.execute_command("LATENCY", "HISTORY", event)
                timestamps = [e[0] for e in history]
                assert timestamps == sorted(timestamps), (
                    f"History for '{event}' is not in chronological order: {timestamps}"
                )
        finally:
            r.close()
            proc.terminate()
            proc.wait(timeout=5)

    def test_history_capped_at_180_samples(self):
        """LATENCY HISTORY must not return more than 180 samples per event."""
        port = free_port()
        proc = start_custom_server(port, [])
        r = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True)
        try:
            r.flushdb()
            # Issue 200+ commands to exceed the 180-sample cap
            for i in range(210):
                r.set(f"cap180_{i}", i)
            for event in ("command", "fast-command"):
                history = r.execute_command("LATENCY", "HISTORY", event)
                assert len(history) <= 180, (
                    f"HISTORY for '{event}' returned {len(history)} samples; cap is 180"
                )
        finally:
            r.close()
            proc.terminate()
            proc.wait(timeout=5)

    def test_history_latency_values_non_negative(self):
        port = free_port()
        proc = start_custom_server(port, [])
        r = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True)
        try:
            r.flushdb()
            for i in range(10):
                r.set(f"nonneg_{i}", i)
            for event in ("command", "fast-command"):
                for entry in r.execute_command("LATENCY", "HISTORY", event):
                    assert isinstance(entry[1], int)
                    assert entry[1] >= 0, f"Negative latency in {event}: {entry[1]}"
        finally:
            r.close()
            proc.terminate()
            proc.wait(timeout=5)


class TestLatencyReset:
    """LATENCY RESET [event ...]"""

    def test_reset_no_args_clears_all(self):
        port = free_port()
        proc = start_custom_server(port, [])
        r = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True)
        try:
            r.flushdb()
            for i in range(10):
                r.set(f"reset_all_{i}", i)
            r.execute_command("LATENCY", "RESET")
            # The RESET call itself and the HISTORY calls after it each add a sample.
            # Allow a small number of entries — the bulk of the pre-reset data is gone.
            cmd_history = r.execute_command("LATENCY", "HISTORY", "command")
            fast_history = r.execute_command("LATENCY", "HISTORY", "fast-command")
            assert len(cmd_history) + len(fast_history) <= 3, (
                f"Expected at most 3 samples after RESET (self-recording), "
                f"got cmd={len(cmd_history)} fast={len(fast_history)}"
            )
        finally:
            r.close()
            proc.terminate()
            proc.wait(timeout=5)

    def test_reset_named_event_clears_only_that_event(self):
        port = free_port()
        proc = start_custom_server(port, [])
        r = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True)
        try:
            r.flushdb()
            for i in range(20):
                r.set(f"selective_{i}", i)
            r.execute_command("LATENCY", "RESET", "fast-command")
            fast_history = r.execute_command("LATENCY", "HISTORY", "fast-command")
            # RESET and the HISTORY call itself may add 1-2 new fast-command samples
            assert len(fast_history) <= 2, (
                f"Expected <= 2 fast-command samples after named RESET "
                f"(only self-recording), got {len(fast_history)}"
            )
            cmd_history = r.execute_command("LATENCY", "HISTORY", "command")
            assert isinstance(cmd_history, list)
        finally:
            r.close()
            proc.terminate()
            proc.wait(timeout=5)

    def test_reset_multiple_events(self):
        port = free_port()
        proc = start_custom_server(port, [])
        r = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True)
        try:
            r.flushdb()
            for i in range(10):
                r.set(f"multi_reset_{i}", i)
            r.execute_command("LATENCY", "RESET", "command", "fast-command")
            cmd_history = r.execute_command("LATENCY", "HISTORY", "command")
            fast_history = r.execute_command("LATENCY", "HISTORY", "fast-command")
            # Only the RESET call itself and these two HISTORY calls can have added samples
            assert len(cmd_history) <= 2
            assert len(fast_history) <= 2
        finally:
            r.close()
            proc.terminate()
            proc.wait(timeout=5)

    def test_reset_nonexistent_event_does_not_error(self, r):
        # Should be a no-op, not an error; Redis returns integer count, KiviDB returns OK/None
        result = r.execute_command("LATENCY", "RESET", "event_that_never_existed")
        assert result in (True, "OK", b"OK", None) or isinstance(result, int)

    def test_reset_returns_ok(self, r):
        result = r.execute_command("LATENCY", "RESET")
        assert result in (True, "OK", b"OK", None) or isinstance(result, int)


class TestLatencyGraph:
    """LATENCY GRAPH <event>"""

    def test_graph_requires_event_argument(self, r):
        with pytest.raises(redis.ResponseError):
            r.execute_command("LATENCY", "GRAPH")

    def test_graph_unknown_event_returns_error(self, r):
        with pytest.raises(redis.ResponseError):
            r.execute_command("LATENCY", "GRAPH", "event_xyz_unknown")

    def test_graph_known_event_returns_string(self):
        port = free_port()
        proc = start_custom_server(port, [])
        r = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True)
        try:
            r.flushdb()
            for i in range(20):
                r.set(f"graph_{i}", i)
            # Find a populated event
            latest = r.execute_command("LATENCY", "LATEST")
            if not latest:
                pytest.skip("No latency events recorded — skipping graph test")
            event_name = latest[0][0]
            graph = r.execute_command("LATENCY", "GRAPH", event_name)
            assert isinstance(graph, str)
            assert len(graph) > 0, "LATENCY GRAPH returned empty string"
        finally:
            r.close()
            proc.terminate()
            proc.wait(timeout=5)

    def test_graph_contains_sparkline_characters(self):
        port = free_port()
        proc = start_custom_server(port, [])
        r = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True)
        try:
            r.flushdb()
            for i in range(20):
                r.set(f"spark_{i}", i)
            latest = r.execute_command("LATENCY", "LATEST")
            if not latest:
                pytest.skip("No latency events recorded")
            event_name = latest[0][0]
            graph = r.execute_command("LATENCY", "GRAPH", event_name)
            # Redis's graph: a header with the high and the low, a rule of
            # 80 dashes, and an ASCII sparkline of _ o # and |.
            lines = graph.split("\n")
            assert lines[0].startswith(f"{event_name} - high "), repr(graph)
            assert lines[1] == "-" * 80, repr(graph)
            assert any(c in set("_o#|") for c in graph), repr(graph)
        finally:
            r.close()
            proc.terminate()
            proc.wait(timeout=5)


class TestLatencyHistogram:
    """LATENCY HISTOGRAM — per command: its calls, and how many of them took
    less than each power of two of microseconds. A map: a flat list over
    RESP2, a dict over RESP3."""

    @staticmethod
    def _by_command(result):
        if isinstance(result, dict):
            return result
        assert isinstance(result, list) and len(result) % 2 == 0
        return dict(zip(result[::2], result[1::2]))

    def test_histogram_returns_list(self, r):
        r.set("lh:k", "v")
        by_command = self._by_command(r.execute_command("LATENCY", "HISTOGRAM"))
        assert "set" in by_command

    def test_histogram_with_command_arg_returns_list(self, r):
        r.set("lh:k", "v")
        r.get("lh:k")
        by_command = self._by_command(r.execute_command("LATENCY", "HISTOGRAM", "set"))
        assert list(by_command) == ["set"]
        entry = by_command["set"]
        if isinstance(entry, list):
            entry = dict(zip(entry[::2], entry[1::2]))
        assert entry["calls"] >= 1
        assert "histogram_usec" in entry


class TestLatencyUnknownSubcommand:
    def test_unknown_subcommand_returns_error(self, r):
        with pytest.raises(redis.ResponseError):
            r.execute_command("LATENCY", "BADSUBCMD")

    def test_no_subcommand_returns_error(self, r):
        with pytest.raises(redis.ResponseError):
            r.execute_command("LATENCY")


# ══════════════════════════════════════════════════════════════════════════════
# Cross-cutting integration scenarios
# ══════════════════════════════════════════════════════════════════════════════

class TestSlowlogLatencyIntegration:
    """Verify slowlog and latency track the same workload coherently."""

    def test_slowlog_and_latency_both_see_commands(self):
        port = free_port()
        proc = start_custom_server(
            port,
            ["--slowlog-log-slower-than", "0"],
        )
        r = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True)
        try:
            r.flushdb()
            r.execute_command("SLOWLOG", "RESET")
            r.execute_command("LATENCY", "RESET")

            for i in range(10):
                r.set(f"both_{i}", i)
                r.get(f"both_{i}")
            _slow_command(r)

            slowlog_len = r.execute_command("SLOWLOG", "LEN")
            latency_latest = r.execute_command("LATENCY", "LATEST")

            assert slowlog_len > 0, "Slowlog should have entries"
            if not _IS_REDIS:
                assert len(latency_latest) > 0, "Latency should have events"
        finally:
            r.close()
            proc.terminate()
            proc.wait(timeout=5)

    def test_reset_slowlog_does_not_affect_latency(self):
        port = free_port()
        proc = start_custom_server(
            port,
            ["--slowlog-log-slower-than", "0"],
        )
        r = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True)
        try:
            r.flushdb()
            for i in range(10):
                r.set(f"indep_{i}", i)
            r.execute_command("SLOWLOG", "RESET")
            latency_latest = r.execute_command("LATENCY", "LATEST")
            assert isinstance(latency_latest, list)
            # After SLOWLOG RESET, only commands issued after it are in the log.
            # LATENCY LATEST and SLOWLOG LEN themselves are recorded, so expect <= 3.
            slowlog_len = r.execute_command("SLOWLOG", "LEN")
            assert slowlog_len <= 3, (
                f"Expected <= 3 entries after SLOWLOG RESET (self-recording with "
                f"threshold=0), got {slowlog_len}"
            )
        finally:
            r.close()
            proc.terminate()
            proc.wait(timeout=5)

    def test_reset_latency_does_not_affect_slowlog(self):
        port = free_port()
        proc = start_custom_server(
            port,
            ["--slowlog-log-slower-than", "0"],
        )
        r = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True)
        try:
            r.flushdb()
            r.execute_command("SLOWLOG", "RESET")
            for i in range(10):
                r.set(f"indep2_{i}", i)

            before_len = r.execute_command("SLOWLOG", "LEN")
            r.execute_command("LATENCY", "RESET")
            after_len = r.execute_command("SLOWLOG", "LEN")

            # LATENCY RESET itself may add one entry (it goes through execute_command)
            # so after_len >= before_len is acceptable; it should not be zero.
            assert after_len >= before_len, (
                "LATENCY RESET should not clear the slowlog"
            )
        finally:
            r.close()
            proc.terminate()
            proc.wait(timeout=5)

    def test_pipeline_commands_appear_in_both(self):
        """Pipelined commands must be timed and appear in both slowlog and latency."""
        port = free_port()
        proc = start_custom_server(
            port,
            ["--slowlog-log-slower-than", "0"],
        )
        r = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True)
        try:
            r.flushdb()
            r.execute_command("SLOWLOG", "RESET")
            r.execute_command("LATENCY", "RESET")

            pipe = r.pipeline(transaction=False)
            pipe.config_set("latency-monitor-threshold", 1)
            for i in range(20):
                pipe.set(f"pipe_{i}", i)
            pipe.execute_command("DEBUG", "SLEEP", "0.005")
            pipe.execute()

            slowlog_len = r.execute_command("SLOWLOG", "LEN")
            latency_latest = r.execute_command("LATENCY", "LATEST")

            assert slowlog_len >= 22, (
                f"Expected >= 20 slowlog entries for 20 pipelined SETs, got {slowlog_len}"
            )
            if not _IS_REDIS:
                assert len(latency_latest) > 0
        finally:
            r.close()
            proc.terminate()
            proc.wait(timeout=5)

    def test_latency_fast_vs_command_classification(self):
        """
        A command Redis flags fast (GET, SET, ...) that crosses
        `latency-monitor-threshold` is a 'fast-command' event, any other a
        'command' event; DEBUG SLEEP is the latter.
        """
        if _IS_REDIS:
            pytest.skip("Redis LATENCY does not classify individual commands")
        port = free_port()
        proc = start_custom_server(port, [])
        r = redis.Redis(host=KIVI_HOST, port=port, decode_responses=True)
        try:
            r.flushdb()
            _slow_command(r)

            latest = r.execute_command("LATENCY", "LATEST")
            assert [entry[0] for entry in latest] == ["command"], latest
            for entry in latest:
                name, ts, ms, _most = entry
                assert name in ("command", "fast-command"), (
                    f"Unexpected latency event name: {name!r}"
                )
                assert isinstance(ts, int) and ts > 0
                assert isinstance(ms, int) and ms >= 0
        finally:
            r.close()
            proc.terminate()
            proc.wait(timeout=5)