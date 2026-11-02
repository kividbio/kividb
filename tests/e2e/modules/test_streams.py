"""
Stream command tests — XADD/XLEN/XRANGE/XREVRANGE/XREAD/XDEL/XTRIM/XSETID/XACKDEL/XCFGSET
XINFO + consumer groups: XGROUP CREATE/DESTROY/SETID/CREATECONSUMER/DELCONSUMER
XREADGROUP/XACK/XPENDING/XCLAIM/XAUTOCLAIM.
"""
from __future__ import annotations
import time
import threading
import pytest
import redis
from utils.helpers import make_client, xread_streams


def add_entries(r, key, count, prefix="msg"):
    """Helper: append `count` entries to a stream, return list of IDs."""
    ids = []
    for i in range(count):
        entry_id = r.xadd(key, {f"field": f"{prefix}_{i}"})
        ids.append(entry_id)
    return ids


class TestXAdd:
    def test_xadd_auto_id(self, r):
        entry_id = r.xadd("s", {"name": "alice", "age": "30"})
        assert "-" in entry_id  # ms-seq format

    def test_xadd_explicit_id(self, r):
        entry_id = r.xadd("s", {"f": "v"}, id="1000-0")
        assert entry_id == "1000-0"

    def test_xadd_nomkstream(self, r):
        # NOMKSTREAM + non-existent key → should NOT create the key
        result = r.xadd("nokey", {"f": "v"}, nomkstream=True)
        assert result is None
        assert r.exists("nokey") == 0

    def test_xadd_maxlen(self, r):
        for i in range(20):
            r.xadd("s", {"i": str(i)}, maxlen=10, approximate=False)
        assert r.xlen("s") <= 10

    def test_xadd_wrongtype(self, r):
        r.set("k", "string")
        with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
            r.xadd("k", {"f": "v"})


class TestXLen:
    def test_xlen(self, r):
        add_entries(r, "s", 5)
        assert r.xlen("s") == 5

    def test_xlen_missing(self, r):
        assert r.xlen("missing") == 0


class TestXRange:
    def test_xrange_all(self, r):
        ids = add_entries(r, "s", 3)
        entries = r.xrange("s", "-", "+")
        assert len(entries) == 3
        assert entries[0][0] == ids[0]
        assert entries[-1][0] == ids[-1]

    def test_xrange_count(self, r):
        add_entries(r, "s", 10)
        entries = r.xrange("s", "-", "+", count=3)
        assert len(entries) == 3

    def test_xrange_bounds(self, r):
        ids = add_entries(r, "s", 5)
        entries = r.xrange("s", ids[1], ids[3])
        assert len(entries) == 3
        assert entries[0][0] == ids[1]
        assert entries[-1][0] == ids[-1] if False else entries[-1][0] == ids[3]


class TestXRevRange:
    def test_xrevrange_all(self, r):
        ids = add_entries(r, "s", 3)
        entries = r.xrevrange("s", "+", "-")
        assert len(entries) == 3
        assert entries[0][0] == ids[2]  # newest first

    def test_xrevrange_count(self, r):
        add_entries(r, "s", 5)
        entries = r.xrevrange("s", "+", "-", count=2)
        assert len(entries) == 2


class TestXRead:
    def test_xread_from_start(self, r):
        add_entries(r, "s", 3)
        result = r.xread({"s": "0-0"})
        assert result is not None
        assert len(xread_streams(result)) == 1
        key, entries = xread_streams(result)[0]
        assert key == "s"
        assert len(entries) == 3

    def test_xread_from_id(self, r):
        ids = add_entries(r, "s", 5)
        result = r.xread({"s": ids[2]})
        _, entries = xread_streams(result)[0]
        assert len(entries) == 2  # ids[3] and ids[4]

    def test_xread_count(self, r):
        add_entries(r, "s", 10)
        result = r.xread({"s": "0-0"}, count=3)
        _, entries = xread_streams(result)[0]
        assert len(entries) == 3

    def test_xread_multiple_streams(self, r):
        add_entries(r, "s1", 2)
        add_entries(r, "s2", 3)
        result = r.xread({"s1": "0-0", "s2": "0-0"})
        assert len(xread_streams(result)) == 2

    def test_xread_block_wakeup(self, r):
        """XREAD BLOCK should unblock when a new entry is added."""
        received = []

        def reader():
            c = make_client()
            result = c.xread({"s": "$"}, block=3000)
            if result:
                received.extend(result)

        t = threading.Thread(target=reader)
        t.start()
        time.sleep(0.3)
        r.xadd("s", {"msg": "hello"})
        t.join(timeout=5)
        assert len(received) == 1


class TestXDel:
    def test_xdel(self, r):
        ids = add_entries(r, "s", 3)
        assert r.xdel("s", ids[1]) == 1
        entries = r.xrange("s", "-", "+")
        returned_ids = [e[0] for e in entries]
        assert ids[1] not in returned_ids

    def test_xdel_missing(self, r):
        r.xadd("s", {"f": "v"})
        assert r.xdel("s", "9999-0") == 0


class TestXTrim:
    def test_xtrim_maxlen(self, r):
        add_entries(r, "s", 20)
        r.xtrim("s", maxlen=5, approximate=False)
        assert r.xlen("s") == 5

    def test_xadd_with_trim(self, r):
        for i in range(20):
            r.xadd("s", {"i": str(i)}, maxlen=5, approximate=False)
        assert r.xlen("s") == 5


class TestXSetId:
    def test_xsetid(self, r):
        r.xadd("s", {"f": "v"}, id="100-0")
        r.execute_command("XSETID", "s", "200-0")
        # Adding with * should generate an ID > 200-0
        new_id = r.xadd("s", {"f": "v2"})
        ms, seq = new_id.split("-")
        assert int(ms) >= 200


class TestXInfo:
    def _xinfo_stream(client, key: str) -> dict:
        """Parse XINFO STREAM into a flat dict (string keys).

        RESP2 replies with a flat [k, v, k, v] array; RESP3 replies with a real
        map, which redis-py hands back as a dict already.
        """
        raw = client.execute_command("XINFO", "STREAM", key)
        if isinstance(raw, dict):
            return dict(raw)
        it = iter(raw)
        return {k: v for k, v in zip(it, it)}

    def test_xinfo_groups_empty(self, r):
        add_entries(r, "s", 1)
        groups = r.xinfo_groups("s")
        assert groups == []


class TestConsumerGroups:
    def test_xgroup_create(self, r):
        add_entries(r, "s", 5)
        r.xgroup_create("s", "grp", "0")
        groups = r.xinfo_groups("s")
        assert any(g["name"] == "grp" for g in groups)

    def test_xgroup_create_mkstream(self, r):
        r.xgroup_create("newstream", "grp", "0", mkstream=True)
        assert r.exists("newstream") == 1

    def test_xgroup_destroy(self, r):
        add_entries(r, "s", 2)
        r.xgroup_create("s", "grp", "0")
        r.xgroup_destroy("s", "grp")
        assert r.xinfo_groups("s") == []

    def test_xreadgroup_basic(self, r):
        add_entries(r, "s", 3)
        r.xgroup_create("s", "grp", "0")
        result = r.xreadgroup("grp", "consumer1", {"s": ">"})
        assert result is not None
        _, entries = xread_streams(result)[0]
        assert len(entries) == 3

    def test_xreadgroup_count(self, r):
        add_entries(r, "s", 10)
        r.xgroup_create("s", "grp", "0")
        result = r.xreadgroup("grp", "c1", {"s": ">"}, count=3)
        _, entries = xread_streams(result)[0]
        assert len(entries) == 3

    def test_xack(self, r):
        add_entries(r, "s", 2)
        r.xgroup_create("s", "grp", "0")
        result = r.xreadgroup("grp", "c1", {"s": ">"})
        _, entries = xread_streams(result)[0]
        entry_id = entries[0][0]
        acked = r.xack("s", "grp", entry_id)
        assert acked == 1

    def test_xpending_summary(self, r):
        add_entries(r, "s", 5)
        r.xgroup_create("s", "grp", "0")
        r.xreadgroup("grp", "c1", {"s": ">"})
        pending = r.xpending("s", "grp")
        assert pending["pending"] == 5

    def test_xpending_detail(self, r):
        ids = add_entries(r, "s", 3)
        r.xgroup_create("s", "grp", "0")
        r.xreadgroup("grp", "c1", {"s": ">"})
        details = r.xpending_range("s", "grp", min="-", max="+", count=10)
        assert len(details) == 3

    def test_xgroup_setid(self, r):
        ids = add_entries(r, "s", 5)
        r.xgroup_create("s", "grp", "0")
        r.xgroup_setid("s", "grp", ids[2])
        # Entries after ids[2] are now "undelivered"
        result = r.xreadgroup("grp", "c1", {"s": ">"})
        _, entries = xread_streams(result)[0]
        assert len(entries) == 2

    def test_xgroup_createconsumer(self, r):
        add_entries(r, "s", 1)
        r.xgroup_create("s", "grp", "0")
        r.xgroup_createconsumer("s", "grp", "explicit_consumer")
        info = r.xinfo_consumers("s", "grp")
        names = [c["name"] for c in info]
        assert "explicit_consumer" in names

    def test_xgroup_delconsumer(self, r):
        add_entries(r, "s", 1)
        r.xgroup_create("s", "grp", "0")
        r.xreadgroup("grp", "c1", {"s": ">"})
        r.xgroup_delconsumer("s", "grp", "c1")
        info = r.xinfo_consumers("s", "grp")
        assert all(c["name"] != "c1" for c in info)

    def test_xclaim(self, r):
        add_entries(r, "s", 2)
        r.xgroup_create("s", "grp", "0")
        result = r.xreadgroup("grp", "c1", {"s": ">"})
        _, entries = xread_streams(result)[0]
        entry_id = entries[0][0]
        # Sleep to age the PEL entry
        time.sleep(0.05)
        claimed = r.xclaim("s", "grp", "c2", 0, [entry_id])
        assert len(claimed) == 1
        assert claimed[0][0] == entry_id

    def test_xautoclaim(self, r):
        add_entries(r, "s", 3)
        r.xgroup_create("s", "grp", "0")
        r.xreadgroup("grp", "c1", {"s": ">"})
        time.sleep(0.05)
        result = r.xautoclaim("s", "grp", "c2", 0, "0-0")
        # result: (next_id, entries, deleted_ids)
        _, entries, _ = result
        assert len(entries) >= 1


class TestStreamRealWorld:
    def test_event_log(self, r):
        """Simulate an event log with consumer group processing."""
        # Producers
        for i in range(10):
            r.xadd("events", {"type": "click", "user_id": str(i)})

        r.xgroup_create("events", "analytics", "0")

        # Consumer 1 processes first 5
        result = r.xreadgroup("analytics", "worker1", {"events": ">"}, count=5)
        _, batch1 = xread_streams(result)[0]
        assert len(batch1) == 5
        for entry_id, _ in batch1:
            r.xack("events", "analytics", entry_id)

        # Consumer 2 processes next 5
        result = r.xreadgroup("analytics", "worker2", {"events": ">"}, count=5)
        _, batch2 = xread_streams(result)[0]
        assert len(batch2) == 5

        # All acked — pending should be 5 still (batch2 not acked)
        pending = r.xpending("events", "analytics")
        assert pending["pending"] == 5

# E2E tests for XACKDEL and XCFGSET — Redis 8.2 stream commands.
#
# Scenario: a task queue where workers consume jobs, and a capacity-capped
# audit log that must never exceed a configurable size.

def _decode(v) -> str:
    return v.decode() if isinstance(v, bytes) else str(v)


def _xinfo_stream(client, key: str) -> dict:
    """Parse XINFO STREAM into a flat dict.

    RESP2 replies with a flat [k, v, k, v] array; RESP3 replies with a real map,
    which redis-py hands back as a dict already.
    """
    raw = client.execute_command("XINFO", "STREAM", key)
    if isinstance(raw, dict):
        return {_decode(k): v for k, v in raw.items()}
    it = iter(raw)
    return {_decode(k): v for k, v in zip(it, it)}


# ── XACKDEL ───────────────────────────────────────────────────────────────────

class TestXAckDel:
    """XACKDEL = atomic XACK + XDEL under a single lock."""

    @pytest.fixture(autouse=True)
    def _require_xackdel(self, r):
        try:
            r.execute_command("XACKDEL", "__probe__", "__group__", "0-0")
        except redis.ResponseError as e:
            if "unknown command" in str(e).lower() or "unknown subcommand" in str(e).lower():
                pytest.skip("XACKDEL not supported by this server")

    def test_basic_ack_and_delete(self, r):
        """Happy path: entry is pending and in the stream → returns 1."""
        r.xadd("jobs", {"task": "send_email"}, id="1-0")
        r.xgroup_create("jobs", "workers", "0")
        r.execute_command("XREADGROUP", "GROUP", "workers", "w1",
                          "STREAMS", "jobs", ">")

        result = r.execute_command("XACKDEL", "jobs", "workers", "1-0")
        assert result == [1], f"expected [1], got {result}"

        # Entry gone from stream
        assert r.xlen("jobs") == 0
        # PEL clean
        pending = r.xpending("jobs", "workers")
        assert pending["pending"] == 0

    def test_multiple_ids_per_call(self, r):
        """Batch ack+delete — all three in one round trip."""
        for i in range(1, 4):
            r.xadd("jobs", {"seq": str(i)}, id=f"{i}-0")
        r.xgroup_create("jobs", "workers", "0")
        r.execute_command("XREADGROUP", "GROUP", "workers", "w1",
                          "STREAMS", "jobs", ">")

        result = r.execute_command("XACKDEL", "jobs", "workers",
                                   "1-0", "2-0", "3-0")
        assert result == [1, 1, 1]
        assert r.xlen("jobs") == 0
        assert r.xpending("jobs", "workers")["pending"] == 0

    def test_not_in_pel_returns_minus_one(self, r):
        """ID exists in the stream but was never delivered → -1, entry untouched."""
        r.xadd("jobs", {"task": "x"}, id="1-0")
        r.xgroup_create("jobs", "workers", "0")
        # Do NOT read → 1-0 never in PEL

        result = r.execute_command("XACKDEL", "jobs", "workers", "1-0")
        assert result == [-1]
        assert r.xlen("jobs") == 1  # entry still present

    def test_entry_already_xdel_is_acknowledged(self, r):
        """
        Entry is in PEL (pending) but was deleted from the stream separately
        (e.g. by XDEL or a trim). XACKDEL acks the PEL entry and returns 1, as
        Redis does: the entry is acknowledged and gone, whoever deleted it.
        """
        r.xadd("jobs", {"task": "x"}, id="1-0")
        r.xgroup_create("jobs", "workers", "0")
        r.execute_command("XREADGROUP", "GROUP", "workers", "w1",
                          "STREAMS", "jobs", ">")

        # Manually remove the entry without acking
        r.xdel("jobs", "1-0")
        assert r.xlen("jobs") == 0
        assert r.xpending("jobs", "workers")["pending"] == 1  # still in PEL

        result = r.execute_command("XACKDEL", "jobs", "workers", "1-0")
        assert result == [1], f"acked, entry already absent → 1, got {result}"
        assert r.xpending("jobs", "workers")["pending"] == 0

    def test_mixed_results(self, r):
        """
        Three IDs in one call:
          1-0 → pending + in stream → 1
          2-0 → pending, already xdel'd → 1
          3-0 → never delivered (not in PEL) → -1
        """
        r.xadd("jobs", {"t": "a"}, id="1-0")
        r.xadd("jobs", {"t": "b"}, id="2-0")
        r.xadd("jobs", {"t": "c"}, id="3-0")
        r.xgroup_create("jobs", "workers", "0")

        # Only deliver 1-0 and 2-0 — use COUNT 2 so 3-0 is never in PEL
        r.execute_command("XREADGROUP", "GROUP", "workers", "w1",
                        "COUNT", "2", "STREAMS", "jobs", ">")

        r.xdel("jobs", "2-0")  # leave 2-0 dangling in PEL

        result = r.execute_command("XACKDEL", "jobs", "workers",
                                "1-0", "2-0", "3-0")
        assert result == [1, 1, -1], f"unexpected: {result}"

    def test_only_affects_named_group(self, r):
        """Two groups both deliver the same entry. XACKDEL on grp1 leaves grp2's PEL intact."""
        r.xadd("jobs", {"task": "x"}, id="1-0")
        r.xgroup_create("jobs", "grp1", "0")
        r.xgroup_create("jobs", "grp2", "0")
        r.execute_command("XREADGROUP", "GROUP", "grp1", "w1", "STREAMS", "jobs", ">")
        r.execute_command("XREADGROUP", "GROUP", "grp2", "w2", "STREAMS", "jobs", ">")

        result = r.execute_command("XACKDEL", "jobs", "grp1", "1-0")
        assert result == [1]

        # grp1 PEL is clear
        assert r.xpending("jobs", "grp1")["pending"] == 0
        # grp2 PEL still has the entry (stream entry is gone, dangling ref)
        assert r.xpending("jobs", "grp2")["pending"] == 1

    def test_missing_key_returns_all_minus_one(self, r):
        result = r.execute_command("XACKDEL", "nosuch", "grp", "1-0", "2-0")
        assert result == [-1, -1]

    def test_no_such_group_returns_all_minus_one(self, r):
        # As for a missing key (Redis 8.10).
        r.xadd("jobs", {"t": "x"}, id="1-0")
        result = r.execute_command("XACKDEL", "jobs", "badgroup", "1-0", "2-0")
        assert result == [-1, -1]
        assert r.xlen("jobs") == 1

    def test_wrong_type_errors(self, r):
        r.set("notastream", "value")
        with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
            r.execute_command("XACKDEL", "notastream", "grp", "1-0")

    def test_realistic_task_queue_lifecycle(self, r):
        """
        Simulate a real task queue: producer enqueues, worker processes,
        XACKDEL removes atomically — no separate XACK then XDEL needed.
        """
        # Producer: enqueue 5 jobs
        job_ids = []
        for i in range(5):
            jid = r.xadd("tasks", {"payload": f"job_{i}"})
            job_ids.append(jid)

        r.xgroup_create("tasks", "processors", "0")

        # Worker: read all 5
        result = r.execute_command("XREADGROUP", "GROUP", "processors", "worker1",
                                   "STREAMS", "tasks", ">")
        assert result is not None
        _, entries = xread_streams(result)[0]
        assert len(entries) == 5

        # Process and atomically ack+delete each job
        for entry_id, _ in entries:
            ack_result = r.execute_command("XACKDEL", "tasks", "processors",
                                           _decode(entry_id))
            assert ack_result == [1], f"expected [1] for {entry_id}, got {ack_result}"

        # Stream is now empty, PEL is clean
        assert r.xlen("tasks") == 0
        assert r.xpending("tasks", "processors")["pending"] == 0


# ── XCFGSET ───────────────────────────────────────────────────────────────────

class TestXCfgSet:
    """XCFGSET sets per-stream config: MAX-ENTRIES and MAX-DELETED-ENTRY-ID."""

    @pytest.fixture(autouse=True)
    def _require_xcfgset(self, r):
        try:
            r.execute_command("XCFGSET", "__probe__", "MAX-ENTRIES", "0")
        except redis.ResponseError as e:
            if "unknown command" in str(e).lower() or "unknown subcommand" in str(e).lower():
                pytest.skip("XCFGSET not supported by this server")

    def test_max_entries_trims_immediately(self, r):
        """Setting MAX-ENTRIES below current length trims the stream right away."""
        for i in range(1, 11):
            r.xadd("log", {"i": str(i)})

        assert r.xlen("log") == 10
        r.execute_command("XCFGSET", "log", "MAX-ENTRIES", "5")
        assert r.xlen("log") == 5

        # The 5 newest entries survive
        entries = r.xrange("log", "-", "+")
        assert len(entries) == 5

    def test_max_entries_enforced_on_xadd(self, r):
        """Every subsequent XADD respects the policy — stream never exceeds the cap."""
        r.xadd("log", {"i": "1"})
        r.execute_command("XCFGSET", "log", "MAX-ENTRIES", "3")

        for i in range(2, 8):
            r.xadd("log", {"i": str(i)})

        assert r.xlen("log") == 3, "stream must be capped at 3"

        # Newest 3 survive
        entries = r.xrange("log", "-", "+")
        values = [e[1]["i"] for e in entries]
        assert values == ["5", "6", "7"]

    def test_max_entries_zero_clears_policy(self, r):
        """MAX-ENTRIES 0 removes the cap — stream grows without limit."""
        for i in range(1, 6):
            r.xadd("log", {"i": str(i)})
        r.execute_command("XCFGSET", "log", "MAX-ENTRIES", "3")
        assert r.xlen("log") == 3

        r.execute_command("XCFGSET", "log", "MAX-ENTRIES", "0")  # clear

        for i in range(6, 16):
            r.xadd("log", {"i": str(i)})

        assert r.xlen("log") == 13, "policy cleared — no trimming should occur"

    def test_max_deleted_entry_id_advances(self, r):
        """MAX-DELETED-ENTRY-ID is visible via XINFO STREAM."""
        r.xadd("log", {"f": "v"}, id="1-0")
        r.execute_command("XCFGSET", "log", "MAX-DELETED-ENTRY-ID", "999-0")

        info = _xinfo_stream(r, "log")
        assert info["max-deleted-entry-id"] == "999-0"

    def test_max_deleted_entry_id_does_not_go_backwards(self, r):
        """Setting a lower MAX-DELETED-ENTRY-ID must be silently ignored."""
        r.xadd("log", {"f": "v"}, id="1-0")
        r.execute_command("XCFGSET", "log", "MAX-DELETED-ENTRY-ID", "500-0")
        r.execute_command("XCFGSET", "log", "MAX-DELETED-ENTRY-ID", "100-0")

        info = _xinfo_stream(r, "log")
        assert info["max-deleted-entry-id"] == "500-0"

    def test_multiple_fields_in_one_call(self, r):
        """Both fields applied atomically in a single XCFGSET."""
        # Use explicit IDs so we know exactly what gets trimmed
        for i in range(1, 11):
            r.xadd("log", {"i": str(i)}, id=f"{i}-0")

        r.execute_command("XCFGSET", "log",
                        "MAX-ENTRIES", "5",
                        "MAX-DELETED-ENTRY-ID", "999-0")

        assert r.xlen("log") == 5
        info = _xinfo_stream(r, "log")
        # Trimmed IDs were 1-0 through 5-0, all less than 999-0, so 999-0 wins
        assert info["max-deleted-entry-id"] == "999-0"

    def test_missing_key_errors(self, r):
        """XCFGSET on a non-existent key must error — it does not create streams."""
        with pytest.raises(redis.ResponseError):
            r.execute_command("XCFGSET", "nosuch", "MAX-ENTRIES", "10")

    def test_wrong_type_errors(self, r):
        r.set("notastream", "value")
        with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
            r.execute_command("XCFGSET", "notastream", "MAX-ENTRIES", "10")

    def test_unknown_field_errors(self, r):
        r.xadd("log", {"f": "v"})
        with pytest.raises(redis.ResponseError):
            r.execute_command("XCFGSET", "log", "BOGUS-FIELD", "42")

    def test_bad_value_errors_and_leaves_stream_unchanged(self, r):
        for i in range(1, 6):
            r.xadd("log", {"i": str(i)})

        with pytest.raises(redis.ResponseError):
            r.execute_command("XCFGSET", "log", "MAX-ENTRIES", "notanumber")

        assert r.xlen("log") == 5  # unchanged

    def test_realistic_audit_log_with_cap(self, r):
        """
        Simulate an audit log that must never exceed 100 entries.
        XCFGSET sets the cap once; all subsequent writes self-trim.
        """
        r.xadd("audit", {"event": "system_start"})
        r.execute_command("XCFGSET", "audit", "MAX-ENTRIES", "100")

        # Simulate 150 audit events
        for i in range(150):
            r.xadd("audit", {"event": f"action_{i}", "user": "admin"})

        final_len = r.xlen("audit")
        assert final_len == 100, \
            f"audit log must be capped at 100, got {final_len}"

        # Most recent events survive — last entry is action_149
        entries = r.xrange("audit", "-", "+")
        last_entry = entries[-1][1]
        assert last_entry["event"] == "action_149"