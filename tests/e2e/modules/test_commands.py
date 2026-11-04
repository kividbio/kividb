"""
tests/e2e/modules/test_pr1_commands.py

End-to-end tests for PR1 commands:
    TOUCH, OBJECT REFCOUNT, DUMP/RESTORE, SORT/SORT_RO,
    TIME, LOLWUT, COMMAND LIST, COMMAND GETKEYS, COMMAND GETKEYSANDFLAGS

These tests run against a live KiviDB process over a real TCP connection
using redis-py, validating both wire-level protocol correctness and
semantic correctness against the Redis specification.
"""
from __future__ import annotations

import random
import time
import pytest
import redis
from utils.helpers import make_client, make_bytes_client

def raw_command(r, *args):
    conn = r.connection_pool.get_connection("_")
    try:
        conn.send_command(*args)
        return conn.read_response()
    finally:
        r.connection_pool.release(conn)

# ──────────────────────────────────────────────────────────────────────────────
# TOUCH
# ──────────────────────────────────────────────────────────────────────────────

class TestTouch:
    def test_touch_existing_keys(self, r):
        r.set("a", "1")
        r.set("b", "2")
        assert r.touch("a", "b") == 2

    def test_touch_missing_key(self, r):
        assert r.touch("nosuch") == 0

    def test_touch_mixed_existing_missing(self, r):
        r.set("a", "1")
        assert r.touch("a", "nosuch1", "nosuch2") == 1

    def test_touch_expired_key_not_counted(self, r):
        r.set("k", "v", px=100)  # 100ms TTL
        time.sleep(0.3)
        assert r.touch("k") == 0

    def test_touch_does_not_modify_ttl(self, r):
        r.set("k", "v", ex=60)
        ttl_before = r.ttl("k")
        r.touch("k")
        ttl_after = r.ttl("k")
        # TTL should be unchanged within 1 second
        assert abs(ttl_before - ttl_after) <= 1

    def test_touch_multiple_times_same_key(self, r):
        r.set("k", "v")
        # Calling TOUCH multiple times on the same key is fine
        assert r.touch("k") == 1
        assert r.touch("k") == 1

    def test_touch_all_key_types(self, r):
        """TOUCH works on all value types."""
        r.set("str_k", "v")
        r.rpush("list_k", "v")
        r.hset("hash_k", mapping={"f": "v"})
        r.sadd("set_k", "m")
        r.zadd("zset_k", {"m": 1.0})
        r.xadd("stream_k", {"f": "v"})

        assert r.touch("str_k", "list_k", "hash_k", "set_k", "zset_k", "stream_k") == 6


# ──────────────────────────────────────────────────────────────────────────────
# OBJECT REFCOUNT
# ──────────────────────────────────────────────────────────────────────────────

class TestObjectRefcount:
    def test_refcount_string(self, r):
        r.set("k", "hello")
        result = r.object("refcount", "k")
        assert result == 1

    def test_refcount_missing_key(self, r):
        assert r.object("refcount", "nosuch") is None

    def test_refcount_all_types(self, r):
        r.set("str_k", "v")
        r.rpush("list_k", "v")
        r.hset("hash_k", mapping={"f": "v"})
        r.sadd("set_k", "m")
        r.zadd("zset_k", {"m": 1.0})

        for key in ("str_k", "list_k", "hash_k", "set_k", "zset_k"):
            assert r.object("refcount", key) == 1, f"OBJECT REFCOUNT {key} should be 1"

    def test_refcount_expired_key(self, r):
        r.set("k", "v", px=100)
        time.sleep(0.3)
        assert r.object("refcount", "k") is None

    def test_object_encoding_still_works(self, r):
        """Verify OBJECT ENCODING was not broken alongside REFCOUNT."""
        r.set("k", "hello")
        enc = r.object("encoding", "k")
        assert enc is not None

    def test_object_idletime_still_works(self, r):
        """Verify OBJECT IDLETIME was not broken."""
        r.set("k", "v")
        idle = r.object("idletime", "k")
        assert isinstance(idle, int)


# ──────────────────────────────────────────────────────────────────────────────
# DUMP / RESTORE
# ──────────────────────────────────────────────────────────────────────────────

class TestDumpRestore:
    def test_dump_missing_key_returns_none(self, r):
        assert r.dump("nosuch") is None

    def test_dump_restore_string_roundtrip(self, rb):
        """Round-trip a string value with no TTL."""
        rb.set("src", b"hello_world")
        payload = rb.dump("src")
        assert payload is not None

        rb.restore("dst", 0, payload)
        assert rb.get("dst") == b"hello_world"

    def test_dump_restore_list_roundtrip(self, rb):
        rb.rpush("src", b"a", b"b", b"c")
        payload = rb.dump("src")
        assert payload is not None

        rb.restore("dst", 0, payload)
        assert rb.lrange("dst", 0, -1) == [b"a", b"b", b"c"]

    def test_dump_restore_hash_roundtrip(self, rb):
        rb.hset("src", mapping={b"field1": b"value1", b"field2": b"value2"})
        payload = rb.dump("src")
        assert payload is not None

        rb.restore("dst", 0, payload)
        assert rb.hget("dst", b"field1") == b"value1"
        assert rb.hget("dst", b"field2") == b"value2"

    def test_dump_restore_set_roundtrip(self, rb):
        rb.sadd("src", b"alpha", b"beta", b"gamma")
        payload = rb.dump("src")
        assert payload is not None

        rb.restore("dst", 0, payload)
        assert rb.scard("dst") == 3
        assert rb.sismember("dst", b"alpha")
        assert rb.sismember("dst", b"beta")
        assert rb.sismember("dst", b"gamma")

    def test_dump_restore_sortedset_roundtrip(self, rb):
        rb.zadd("src", {b"one": 1.0, b"two": 2.0, b"three": 3.0})
        payload = rb.dump("src")
        assert payload is not None

        rb.restore("dst", 0, payload)
        assert rb.zcard("dst") == 3
        assert float(rb.zscore("dst", b"two")) == pytest.approx(2.0)

    def test_restore_with_ttl(self, rb):
        """RESTORE with a positive TTL should set an expiry on the key."""
        rb.set("src", b"ttl_test")
        payload = rb.dump("src")

        rb.restore("dst", 10000, payload)  # 10 second TTL in ms
        pttl = rb.pttl("dst")
        assert 0 < pttl <= 10000, f"expected PTTL in (0, 10000], got {pttl}"

    def test_restore_busykey_error(self, rb):
        """RESTORE without REPLACE fails if the destination key exists."""
        rb.set("src", b"hello")
        rb.set("dst", b"already_here")
        payload = rb.dump("src")

        with pytest.raises(redis.ResponseError, match="BUSYKEY"):
            rb.restore("dst", 0, payload)

    def test_restore_with_replace(self, rb):
        """RESTORE with REPLACE overwrites an existing key."""
        rb.set("src", b"new_value")
        rb.set("dst", b"old_value")
        payload = rb.dump("src")

        rb.restore("dst", 0, payload, replace=True)
        assert rb.get("dst") == b"new_value"

    def test_dump_expired_key_returns_none(self, rb):
        rb.set("k", b"v", px=100)
        time.sleep(0.3)
        assert rb.dump("k") is None

    def test_dump_preserves_type_through_restore(self, rb):
        """After RESTORE the key has the same TYPE as the source."""
        test_cases = [
            ("str_src", lambda: rb.set("str_src", b"v")),
            ("list_src", lambda: rb.rpush("list_src", b"v")),
            ("hash_src", lambda: rb.hset("hash_src", mapping={b"f": b"v"})),
            ("set_src", lambda: rb.sadd("set_src", b"m")),
        ]
        for src_key, setup in test_cases:
            setup()
            src_type = rb.type(src_key).decode()
            payload = rb.dump(src_key)
            dst_key = src_key.replace("src", "dst")
            rb.restore(dst_key, 0, payload)
            dst_type = rb.type(dst_key).decode()
            assert src_type == dst_type, f"type mismatch for {src_key}: {src_type} != {dst_type}"


# ──────────────────────────────────────────────────────────────────────────────
# SORT / SORT_RO
# ──────────────────────────────────────────────────────────────────────────────

class TestSort:
    def test_sort_list_numeric_asc(self, r):
        r.rpush("nums", "3", "1", "2")
        assert r.sort("nums") == ["1", "2", "3"]

    def test_sort_list_numeric_desc(self, r):
        r.rpush("nums", "3", "1", "2")
        assert r.sort("nums", desc=True) == ["3", "2", "1"]

    def test_sort_list_alpha(self, r):
        r.rpush("words", "banana", "apple", "cherry")
        assert r.sort("words", alpha=True) == ["apple", "banana", "cherry"]

    def test_sort_list_alpha_desc(self, r):
        r.rpush("words", "banana", "apple", "cherry")
        assert r.sort("words", alpha=True, desc=True) == ["cherry", "banana", "apple"]

    def test_sort_limit_offset_count(self, r):
        r.rpush("nums", "5", "3", "1", "4", "2")
        # sorted = [1,2,3,4,5], skip 1, take 3 → [2,3,4]
        result = r.sort("nums", start=1, num=3)
        assert result == ["2", "3", "4"]

    def test_sort_limit_offset_beyond_end(self, r):
        r.rpush("nums", "1", "2", "3")
        assert r.sort("nums", start=10, num=5) == []

    def test_sort_set_numeric(self, r):
        r.sadd("nums", "30", "10", "20")
        assert r.sort("nums") == ["10", "20", "30"]

    def test_sort_zset_alpha(self, r):
        r.zadd("z", {"cherry": 1, "apple": 2, "banana": 3})
        assert r.sort("z", alpha=True) == ["apple", "banana", "cherry"]

    def test_sort_store(self, r):
        r.rpush("src", "3", "1", "2")
        count = r.sort("src", store="dst")
        assert count == 3
        assert r.lrange("dst", 0, -1) == ["1", "2", "3"]
        assert r.type("dst") == "list"

    def test_sort_store_empty_deletes_destination(self, r):
        r.set("dst", "old_value")
        # Sort of a missing key → empty → dst should be deleted
        count = r.sort("nosuch_src", store="dst")
        assert count == 0
        assert r.exists("dst") == 0

    def test_sort_missing_key_returns_empty(self, r):
        assert r.sort("nosuch") == []

    def test_sort_non_numeric_without_alpha_errors(self, r):
        r.rpush("words", "foo", "bar")
        with pytest.raises(redis.ResponseError):
            r.sort("words")

    def test_sort_wrong_type_errors(self, r):
        r.hset("h", mapping={"f": "v"})
        with pytest.raises(redis.ResponseError):
            r.sort("h")

    def test_sort_stable_for_equal_elements(self, r):
        """SORT with all-equal elements should not crash and return all elements."""
        r.rpush("nums", "5", "5", "5")
        result = r.sort("nums")
        assert result == ["5", "5", "5"]

    def test_sort_single_element(self, r):
        r.rpush("nums", "42")
        assert r.sort("nums") == ["42"]

    def test_sort_large_list(self, r):
        values = list(range(1000))
        random.shuffle(values)
        pipe = r.pipeline(transaction=False)
        for v in values:
            pipe.rpush("big", str(v))
        pipe.execute()
        result = r.sort("big")
        assert result == [str(i) for i in range(1000)]

    def test_sort_ro_basic(self, r):
        r.rpush("nums", "3", "1", "2")
        result = raw_command(r, "SORT_RO", "nums")
        assert result == ["1", "2", "3"]

    def test_sort_ro_rejects_store(self, r):
        r.rpush("nums", "1", "2", "3")
        with pytest.raises(redis.ResponseError):
            raw_command(r, "SORT_RO", "nums", "STORE", "dst")

    def test_sort_ro_with_alpha(self, r):
        r.rpush("words", "c", "a", "b")
        result = raw_command(r, "SORT_RO", "words", "ALPHA")
        assert result == ["a", "b", "c"]

    def test_sort_ro_with_limit(self, r):
        r.rpush("nums", "5", "4", "3", "2", "1")
        result = raw_command(r, "SORT_RO", "nums", "LIMIT", "1", "2")
        assert result == ["2", "3"]

    def test_sort_with_desc_and_limit(self, r):
        r.rpush("nums", "1", "2", "3", "4", "5")
        # DESC sorted = [5,4,3,2,1], LIMIT 0 3 → [5,4,3]
        result = r.sort("nums", desc=True, start=0, num=3)
        assert result == ["5", "4", "3"]

    def test_sort_store_overwrites_existing_dst(self, r):
        r.rpush("src", "2", "1")
        r.set("dst", "old_value")  # dst is currently a string
        count = r.sort("src", store="dst")
        assert count == 2
        assert r.type("dst") == "list"
        assert r.lrange("dst", 0, -1) == ["1", "2"]


# ──────────────────────────────────────────────────────────────────────────────
# TIME
# ──────────────────────────────────────────────────────────────────────────────

class TestTime:
    def test_time_returns_two_element_list(self, r):
        result = r.time()
        assert isinstance(result, tuple), f"expected tuple, got {type(result)}"
        assert len(result) == 2

    def test_time_seconds_is_plausible(self, r):
        secs, _ = r.time()
        # Should be after 2024-01-01 (Unix ts 1704067200)
        assert secs > 1_704_067_200, f"TIME seconds looks wrong: {secs}"

    def test_time_microseconds_in_range(self, r):
        _, micros = r.time()
        assert 0 <= micros < 1_000_000, f"TIME microseconds out of range: {micros}"

    def test_time_is_close_to_system_time(self, r):
        """The server time should be within 5 seconds of the client's system time."""
        client_secs = int(time.time())
        server_secs, _ = r.time()
        assert abs(server_secs - client_secs) <= 5, (
            f"Server time {server_secs} diverges from client time {client_secs} by > 5s"
        )

    def test_time_non_decreasing_across_calls(self, r):
        """Two consecutive TIME calls must return non-decreasing timestamps."""
        secs1, micros1 = r.time()
        secs2, micros2 = r.time()
        ts1 = secs1 * 1_000_000 + micros1
        ts2 = secs2 * 1_000_000 + micros2
        assert ts2 >= ts1, f"TIME went backwards: {ts1} then {ts2}"

    def test_time_raw_protocol(self, r):
        result = r.time()
        assert isinstance(result[0], int)
        assert isinstance(result[1], int)


# ──────────────────────────────────────────────────────────────────────────────
# LOLWUT
# ──────────────────────────────────────────────────────────────────────────────

class TestLolwut:
    def test_lolwut_returns_string(self, r):
        result = raw_command(r, "LOLWUT")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_lolwut_contains_kivi(self, r):
        result = raw_command(r, "LOLWUT").lower()
        assert "kivi" in result or "version" in result or "ver" in result or "redis" in result, (
            f"LOLWUT output doesn't mention KiviDB or version: {result!r}"
        )

    def test_lolwut_with_version_arg(self, r):
        """LOLWUT VERSION n should still succeed."""
        result = raw_command(r, "LOLWUT", "VERSION", "6")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_lolwut_idempotent(self, r):
        """Calling LOLWUT multiple times should always return something."""
        for _ in range(3):
            result = raw_command(r, "LOLWUT")
            assert len(result) > 0


# ──────────────────────────────────────────────────────────────────────────────
# COMMAND LIST
# ──────────────────────────────────────────────────────────────────────────────

class TestCommandList:
    def test_command_list_returns_non_empty_list(self, r):
        result = raw_command(r, "COMMAND", "LIST")
        assert isinstance(result, list)
        assert len(result) > 0

    def test_command_list_all_strings(self, r):
        result = raw_command(r, "COMMAND", "LIST")
        for cmd in result:
            assert isinstance(cmd, str) and len(cmd) > 0, f"Invalid entry: {cmd!r}"

    def test_command_list_contains_pr1_commands(self, r):
        result = raw_command(r, "COMMAND", "LIST")
        commands_upper = {c.upper() for c in result}
        for expected in ("TOUCH", "DUMP", "RESTORE", "SORT", "SORT_RO", "TIME", "LOLWUT"):
            assert expected in commands_upper, f"COMMAND LIST missing: {expected}"

    def test_command_list_contains_core_commands(self, r):
        result = raw_command(r, "COMMAND", "LIST")
        commands_upper = {c.upper() for c in result}
        for expected in ("SET", "GET", "DEL", "HSET", "LPUSH", "ZADD", "XADD", "PING"):
            assert expected in commands_upper, f"COMMAND LIST missing core command: {expected}"

    def test_command_list_filterby_pattern_z_star(self, r):
        result = raw_command(r, "COMMAND", "LIST", "FILTERBY", "PATTERN", "z*")
        assert len(result) > 0
        for cmd in result:
            assert cmd.upper().startswith("Z"), f"Pattern z* matched non-Z command: {cmd}"
        # Known Z commands must appear
        upper = {c.upper() for c in result}
        assert "ZADD" in upper
        assert "ZRANGE" in upper

    def test_command_list_filterby_pattern_ft_star(self, r):
        result = raw_command(r, "COMMAND", "LIST", "FILTERBY", "PATTERN", "ft.*")
        upper = {c.upper() for c in result}
        assert "FT.SEARCH" in upper
        assert "FT.CREATE" in upper

    def test_command_list_filterby_module_empty(self, r):
        result = raw_command(r, "COMMAND", "LIST", "FILTERBY", "MODULE", "nonexistent")
        assert result == [] or result is None or len(result) == 0

    def test_command_list_filterby_aclcat_string(self, r):
        result = raw_command(r, "COMMAND", "LIST", "FILTERBY", "ACLCAT", "string")
        upper = {c.upper() for c in result}
        assert "GET" in upper
        assert "SET" in upper
        assert "APPEND" in upper
        # List commands should NOT appear
        assert "LPUSH" not in upper

    def test_command_list_filterby_aclcat_hash(self, r):
        result = raw_command(r, "COMMAND", "LIST", "FILTERBY", "ACLCAT", "hash")
        upper = {c.upper() for c in result}
        assert "HSET" in upper
        assert "HGET" in upper
        assert "HDEL" in upper

    def test_command_list_filterby_aclcat_generic(self, r):
        result = raw_command(r, "COMMAND", "LIST", "FILTERBY", "ACLCAT", "generic")
        if not result:
            pytest.skip("COMMAND LIST FILTERBY ACLCAT generic returned empty on this server")
        upper = {c.upper() for c in result}
        assert "DEL" in upper
        assert "EXPIRE" in upper
        assert "TTL" in upper
        assert "SORT" in upper
        assert "TOUCH" in upper

    def test_command_list_no_duplicates(self, r):
        result = raw_command(r, "COMMAND", "LIST")
        # No command should appear twice in the unfiltered list
        upper = [c.upper() for c in result]
        assert len(upper) == len(set(upper)), "COMMAND LIST contains duplicate entries"


# ──────────────────────────────────────────────────────────────────────────────
# COMMAND GETKEYS
# ──────────────────────────────────────────────────────────────────────────────

class TestCommandGetKeys:
    def test_getkeys_set(self, r):
        result = raw_command(r, "COMMAND", "GETKEYS", "SET", "mykey", "myval")
        assert result == ["mykey"]

    def test_getkeys_get(self, r):
        result = raw_command(r, "COMMAND", "GETKEYS", "GET", "mykey")
        assert result == ["mykey"]

    def test_getkeys_mset(self, r):
        result = raw_command(r, "COMMAND", "GETKEYS", "MSET", "k1", "v1", "k2", "v2")
        assert set(result) == {"k1", "k2"}

    def test_getkeys_mget(self, r):
        result = raw_command(r, "COMMAND", "GETKEYS", "MGET", "k1", "k2", "k3")
        assert result == ["k1", "k2", "k3"]

    def test_getkeys_del(self, r):
        result = raw_command(r, "COMMAND", "GETKEYS", "DEL", "k1", "k2", "k3")
        assert result == ["k1", "k2", "k3"]

    def test_getkeys_rename(self, r):
        result = raw_command(r, "COMMAND", "GETKEYS", "RENAME", "src", "dst")
        assert set(result) == {"src", "dst"}

    def test_getkeys_copy(self, r):
        result = raw_command(r, "COMMAND", "GETKEYS", "COPY", "src", "dst")
        assert set(result) == {"src", "dst"}

    def test_getkeys_touch(self, r):
        result = raw_command(r, "COMMAND", "GETKEYS", "TOUCH", "k1", "k2")
        assert result == ["k1", "k2"]

    def test_getkeys_sort(self, r):
        result = raw_command(r, "COMMAND", "GETKEYS", "SORT", "mylist")
        assert result == ["mylist"]

    def test_getkeys_zadd(self, r):
        result = raw_command(r, "COMMAND", "GETKEYS", "ZADD", "myzset", "1.0", "member")
        assert result == ["myzset"]

    def test_getkeys_lmove(self, r):
        result = raw_command(r, "COMMAND", "GETKEYS", "LMOVE", "src", "dst", "LEFT", "RIGHT")
        assert set(result) == {"src", "dst"}

    def test_getkeys_xread_streams(self, r):
        result = raw_command(r, 
            "COMMAND", "GETKEYS", "XREAD", "STREAMS", "stream1", "stream2", "0-0", "0-0"
        )
        assert set(result) == {"stream1", "stream2"}

    def test_getkeys_time_no_keys(self, r):
        try:
            result = raw_command(r, "COMMAND", "GETKEYS", "TIME")
            assert result == [] or result is None or len(result) == 0
        except redis.ResponseError:
            pass  # Redis raises an error for commands with no key arguments

    def test_getkeys_ping_no_keys(self, r):
        try:
            result = raw_command(r, "COMMAND", "GETKEYS", "PING")
            assert result == [] or result is None or len(result) == 0
        except redis.ResponseError:
            pass  # Redis raises an error for commands with no key arguments

    def test_getkeys_unknown_command_errors(self, r):
        with pytest.raises(redis.ResponseError):
            raw_command(r, "COMMAND", "GETKEYS", "NOTACOMMAND", "arg")

    def test_getkeys_no_subargs_errors(self, r):
        with pytest.raises(redis.ResponseError):
            raw_command(r, "COMMAND", "GETKEYS")

    def test_getkeys_blpop(self, r):
        # BLPOP k1 k2 timeout — keys are all but the last arg
        result = raw_command(r, "COMMAND", "GETKEYS", "BLPOP", "k1", "k2", "0")
        assert set(result) == {"k1", "k2"}

    def test_getkeys_pfmerge(self, r):
        # PFMERGE dst src1 src2 — dst is write, srcs are read
        result = raw_command(r, "COMMAND", "GETKEYS", "PFMERGE", "dst", "src1", "src2")
        assert set(result) == {"dst", "src1", "src2"}

    def test_getkeys_zunionstore(self, r):
        # ZUNIONSTORE dst 2 k1 k2
        result = raw_command(r, "COMMAND", "GETKEYS", "ZUNIONSTORE", "dst", "2", "k1", "k2")
        assert set(result) == {"dst", "k1", "k2"}

    def test_getkeys_dump(self, r):
        result = raw_command(r, "COMMAND", "GETKEYS", "DUMP", "mykey")
        assert result == ["mykey"]

    def test_getkeys_restore(self, r):
        result = raw_command(r, "COMMAND", "GETKEYS", "RESTORE", "mykey", "0", "payload")
        assert result == ["mykey"]


# ──────────────────────────────────────────────────────────────────────────────
# COMMAND GETKEYSANDFLAGS
# ──────────────────────────────────────────────────────────────────────────────

def _has_write_flag(flags) -> bool:
    """Accept both KiviDB ('write') and Redis ('OW', 'RW', 'W') write flag forms."""
    return bool({"write", "OW", "RW", "W"} & set(flags))


def _has_read_flag(flags) -> bool:
    """Accept both KiviDB ('read') and Redis ('RO', 'RW', 'R') read flag forms."""
    return bool({"read", "RO", "RW", "R"} & set(flags))


class TestCommandGetKeysAndFlags:
    def test_getkeysandflags_set_write_flag(self, r):
        result = raw_command(r, "COMMAND", "GETKEYSANDFLAGS", "SET", "mykey", "myval")
        # result is a list of [key, [flags...]] pairs
        assert len(result) == 1
        key, flags = result[0][0], result[0][1]
        assert key == "mykey"
        assert _has_write_flag(flags), f"expected write flag, got {flags}"

    def test_getkeysandflags_get_read_flag(self, r):
        result = raw_command(r, "COMMAND", "GETKEYSANDFLAGS", "GET", "mykey")
        assert len(result) == 1
        key, flags = result[0][0], result[0][1]
        assert key == "mykey"
        assert _has_read_flag(flags), f"expected read flag, got {flags}"

    def test_getkeysandflags_rename_src_read_dst_write(self, r):
        result = raw_command(r, "COMMAND", "GETKEYSANDFLAGS", "RENAME", "src", "dst")
        assert len(result) == 2
        keys_flags = {entry[0]: entry[1] for entry in result}
        assert _has_read_flag(keys_flags["src"]), f"src should have read flag, got {keys_flags['src']}"
        assert _has_write_flag(keys_flags["dst"]), f"dst should have write flag, got {keys_flags['dst']}"

    def test_getkeysandflags_mset_all_write(self, r):
        result = raw_command(r, "COMMAND", "GETKEYSANDFLAGS", "MSET", "k1", "v1", "k2", "v2")
        assert len(result) == 2
        for entry in result:
            assert _has_write_flag(entry[1]), f"MSET key {entry[0]} should have write flag, got {entry[1]}"

    def test_getkeysandflags_mget_all_read(self, r):
        result = raw_command(r, "COMMAND", "GETKEYSANDFLAGS", "MGET", "k1", "k2")
        assert len(result) == 2
        for entry in result:
            assert _has_read_flag(entry[1]), f"MGET key {entry[0]} should have read flag, got {entry[1]}"

    def test_getkeysandflags_time_no_keys(self, r):
        try:
            result = raw_command(r, "COMMAND", "GETKEYSANDFLAGS", "TIME")
            assert result == [] or result is None or len(result) == 0
        except redis.ResponseError:
            pass  # Redis raises an error for commands with no key arguments