"""
e2e tests for key expiry.

Covers EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST, EXPIRETIME,
PEXPIRETIME, and the Redis 7 conditional EXPIRE flags (NX, XX, GT, LT).
All tests define the ground-truth Redis behaviour.
"""
from __future__ import annotations

import time
import threading
import pytest
import redis
from utils.helpers import r, rb, make_client  # noqa: F401


# ── TTL / EXPIRE basics ────────────────────────────────────────────────────────

class TestExpireBasics:
    def test_expire_key_disappears(self, r):
        r.set("ex:key", "value")
        r.pexpire("ex:key", 200)
        assert r.get("ex:key") == "value"
        time.sleep(0.5)
        assert r.get("ex:key") is None, "Key must be gone after TTL"

    def test_pexpire_millisecond_precision(self, r):
        r.set("pex:key", "pvalue")
        r.pexpire("pex:key", 300)
        time.sleep(0.15)
        assert r.get("pex:key") == "pvalue"
        time.sleep(0.2)
        assert r.get("pex:key") is None, "Key must be gone after 300ms"

    def test_ttl_returns_remaining_seconds(self, r):
        r.set("ttl:key", "v")
        r.expire("ttl:key", 60)
        ttl = r.ttl("ttl:key")
        assert 55 <= ttl <= 60, f"TTL should be ~60, got {ttl}"

    def test_pttl_returns_remaining_milliseconds(self, r):
        r.set("pttl:key", "v")
        r.pexpire("pttl:key", 5000)
        pttl = r.pttl("pttl:key")
        assert 4000 <= pttl <= 5000, f"PTTL should be ~5000ms, got {pttl}"

    def test_ttl_persistent_key_returns_minus_one(self, r):
        r.set("persist:key", "v")
        assert r.ttl("persist:key") == -1

    def test_ttl_missing_key_returns_minus_two(self, r):
        assert r.ttl("nonexistent:key") == -2

    def test_pttl_persistent_key_returns_minus_one(self, r):
        r.set("k", "v")
        assert r.pttl("k") == -1

    def test_pttl_missing_key_returns_minus_two(self, r):
        assert r.pttl("missing") == -2

    def test_persist_removes_ttl(self, r):
        r.set("per:key", "v")
        r.expire("per:key", 30)
        assert r.ttl("per:key") > 0
        r.persist("per:key")
        assert r.ttl("per:key") == -1, "PERSIST must clear TTL"
        time.sleep(0.5)
        assert r.get("per:key") == "v", "Key must still exist after PERSIST"

    def test_persist_on_non_expiring_key_returns_false(self, r):
        r.set("k", "v")
        result = r.persist("k")
        assert result == 0 or result is False

    def test_persist_on_missing_key_returns_false(self, r):
        result = r.persist("missing")
        assert result == 0 or result is False

    def test_expireat_absolute_timestamp(self, r):
        r.set("eat:key", "v")
        future_ms = int(time.time() * 1000) + 300
        r.pexpireat("eat:key", future_ms)
        pttl = r.pttl("eat:key")
        assert 0 < pttl <= 400, f"PTTL should be ~300ms, got {pttl}"
        time.sleep(0.6)
        assert r.get("eat:key") is None

    def test_pexpireat_millisecond_timestamp(self, r):
        r.set("peat:key", "v")
        future_ms = int(time.time() * 1000) + 400
        r.pexpireat("peat:key", future_ms)
        pttl = r.pttl("peat:key")
        assert 0 < pttl <= 500
        time.sleep(0.5)
        assert r.get("peat:key") is None

    def test_expire_past_timestamp_deletes_immediately(self, r):
        r.set("past:key", "v")
        past_ts = int(time.time()) - 10
        r.expireat("past:key", past_ts)
        assert r.get("past:key") is None, "Past EXPIREAT must delete key immediately"

    def test_expire_nonexistent_key_returns_false(self, r):
        result = r.expire("ghost:key", 10)
        assert result == 0 or result is False, "EXPIRE on nonexistent key must return 0"

    def test_expire_returns_1_on_success(self, r):
        r.set("k", "v")
        result = r.expire("k", 10)
        assert result == 1 or result is True


# ── EXPIRETIME / PEXPIRETIME (Redis 7.0+) ─────────────────────────────────────

class TestExpireTime:
    def test_expiretime(self, r):
        """EXPIRETIME returns the Unix timestamp at which the key will expire."""
        r.set("k", "v")
        future_ts = int(time.time()) + 60
        r.expireat("k", future_ts)
        et = r.execute_command("EXPIRETIME", "k")
        assert abs(et - future_ts) <= 1, f"EXPIRETIME should be ~{future_ts}, got {et}"

    def test_expiretime_no_expiry_returns_minus_one(self, r):
        r.set("k", "v")
        et = r.execute_command("EXPIRETIME", "k")
        assert et == -1

    def test_expiretime_missing_key_returns_minus_two(self, r):
        et = r.execute_command("EXPIRETIME", "missing")
        assert et == -2

    def test_pexpiretime(self, r):
        """PEXPIRETIME returns the Unix ms timestamp at which the key will expire."""
        r.set("k", "v")
        future_ms = int(time.time() * 1000) + 60_000
        r.pexpireat("k", future_ms)
        pet = r.execute_command("PEXPIRETIME", "k")
        assert abs(pet - future_ms) <= 100, f"PEXPIRETIME should be ~{future_ms}, got {pet}"

    def test_pexpiretime_no_expiry_returns_minus_one(self, r):
        r.set("k", "v")
        assert r.execute_command("PEXPIRETIME", "k") == -1

    def test_pexpiretime_missing_key_returns_minus_two(self, r):
        assert r.execute_command("PEXPIRETIME", "missing") == -2


# ── Conditional EXPIRE flags (Redis 7.0+) ─────────────────────────────────────

class TestConditionalExpireFlags:
    """
    Redis 7.0 added NX, XX, GT, LT flags to EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT.
    These tests verify their semantics exactly.
    """

    def test_expire_nx_sets_when_no_expiry(self, r):
        """EXPIRE NX succeeds when the key has no expiry."""
        r.set("k", "v")
        result = r.execute_command("EXPIRE", "k", "30", "NX")
        assert result == 1
        assert r.ttl("k") > 0

    def test_expire_nx_fails_when_already_has_expiry(self, r):
        """EXPIRE NX does NOT update an existing expiry."""
        r.set("k", "v", ex=10)
        result = r.execute_command("EXPIRE", "k", "60", "NX")
        assert result == 0
        assert r.ttl("k") <= 10  # unchanged

    def test_expire_xx_fails_when_no_expiry(self, r):
        """EXPIRE XX does nothing when the key has no expiry."""
        r.set("k", "v")
        result = r.execute_command("EXPIRE", "k", "30", "XX")
        assert result == 0
        assert r.ttl("k") == -1  # still no expiry

    def test_expire_xx_sets_when_has_expiry(self, r):
        """EXPIRE XX succeeds when the key already has an expiry."""
        r.set("k", "v", ex=10)
        result = r.execute_command("EXPIRE", "k", "60", "XX")
        assert result == 1
        assert r.ttl("k") > 10

    def test_expire_gt_sets_when_new_greater(self, r):
        """EXPIRE GT updates when new TTL is greater than current TTL."""
        r.set("k", "v", ex=10)
        result = r.execute_command("EXPIRE", "k", "60", "GT")
        assert result == 1
        assert r.ttl("k") > 10

    def test_expire_gt_fails_when_new_smaller(self, r):
        """EXPIRE GT does NOT update when new TTL ≤ current TTL."""
        r.set("k", "v", ex=60)
        result = r.execute_command("EXPIRE", "k", "10", "GT")
        assert result == 0
        assert r.ttl("k") > 10  # unchanged

    def test_expire_lt_sets_when_new_smaller(self, r):
        """EXPIRE LT updates when new TTL is less than current TTL."""
        r.set("k", "v", ex=60)
        result = r.execute_command("EXPIRE", "k", "10", "LT")
        assert result == 1
        assert r.ttl("k") <= 10

    def test_expire_lt_fails_when_new_greater(self, r):
        """EXPIRE LT does NOT update when new TTL ≥ current TTL."""
        r.set("k", "v", ex=10)
        result = r.execute_command("EXPIRE", "k", "60", "LT")
        assert result == 0
        assert r.ttl("k") <= 10  # unchanged

    def test_pexpire_nx_flag(self, r):
        """PEXPIRE NX works the same as EXPIRE NX but in ms."""
        r.set("k", "v")
        result = r.execute_command("PEXPIRE", "k", "5000", "NX")
        assert result == 1
        assert r.pttl("k") > 0

    def test_pexpire_gt_flag(self, r):
        r.set("k", "v")
        r.execute_command("PEXPIRE", "k", "1000", "NX")
        result = r.execute_command("PEXPIRE", "k", "5000", "GT")
        assert result == 1
        assert r.pttl("k") > 1000


# ── TTL on all data types ──────────────────────────────────────────────────────

class TestExpireAllTypes:
    def test_hash_expires(self, r):
        r.hset("ex:hash", mapping={"a": "1", "b": "2"})
        r.pexpire("ex:hash", 200)
        assert r.hget("ex:hash", "a") == "1"
        time.sleep(0.5)
        assert r.hgetall("ex:hash") == {}

    def test_list_expires(self, r):
        r.rpush("ex:list", "a", "b", "c")
        r.pexpire("ex:list", 200)
        time.sleep(0.5)
        assert r.llen("ex:list") == 0

    def test_set_expires(self, r):
        r.sadd("ex:set", "x", "y", "z")
        r.pexpire("ex:set", 200)
        time.sleep(0.5)
        assert r.scard("ex:set") == 0

    def test_zset_expires(self, r):
        r.zadd("ex:zset", {"a": 1.0, "b": 2.0})
        r.pexpire("ex:zset", 200)
        time.sleep(0.5)
        assert r.zcard("ex:zset") == 0

    def test_stream_expires(self, r):
        r.xadd("ex:stream", {"f": "v"})
        r.pexpire("ex:stream", 200)
        time.sleep(0.5)
        assert r.xlen("ex:stream") == 0


# ── Overwriting TTL ────────────────────────────────────────────────────────────

class TestOverwriteTTL:
    def test_set_clears_ttl(self, r):
        r.set("ow:key", "old")
        r.expire("ow:key", 30)
        assert r.ttl("ow:key") > 0
        r.set("ow:key", "new")
        assert r.ttl("ow:key") == -1, "SET must clear existing TTL"

    def test_expire_can_be_overwritten(self, r):
        r.set("ow2:key", "v")
        r.expire("ow2:key", 5)
        r.expire("ow2:key", 60)
        ttl = r.ttl("ow2:key")
        assert 55 <= ttl <= 60, "Second EXPIRE should replace first"

    def test_persist_after_expire(self, r):
        r.set("ep:key", "v")
        r.pexpire("ep:key", 200)
        r.persist("ep:key")
        time.sleep(0.4)
        assert r.get("ep:key") == "v", "PERSIST must prevent expiry"
        assert r.ttl("ep:key") == -1

    def test_incr_preserves_ttl(self, r):
        r.set("incr:key", "10")
        r.expire("incr:key", 60)
        r.incr("incr:key")
        ttl = r.ttl("incr:key")
        assert ttl > 0, f"INCR must preserve TTL, got {ttl}"
        assert r.get("incr:key") == "11"

    def test_append_preserves_ttl(self, r):
        r.set("app:key", "hello")
        r.expire("app:key", 60)
        r.append("app:key", " world")
        ttl = r.ttl("app:key")
        assert ttl > 0, f"APPEND must preserve TTL, got {ttl}"

    def test_hset_preserves_key_ttl(self, r):
        """Adding a field to a hash must not affect the hash key's TTL."""
        r.hset("h", "field1", "v1")
        r.expire("h", 60)
        r.hset("h", "field2", "v2")
        assert r.ttl("h") > 0

    def test_lpush_preserves_key_ttl(self, r):
        r.rpush("l", "a")
        r.expire("l", 60)
        r.rpush("l", "b")
        assert r.ttl("l") > 0

    def test_set_keepttl_preserves_ttl(self, r):
        r.set("k", "v1", ex=60)
        original_ttl = r.ttl("k")
        r.set("k", "v2", keepttl=True)
        new_ttl = r.ttl("k")
        assert new_ttl > 0
        assert abs(original_ttl - new_ttl) <= 2


# ── RENAME with TTL ────────────────────────────────────────────────────────────

class TestRenameWithTTL:
    def test_rename_transfers_ttl(self, r):
        r.set("rn:src", "value")
        r.expire("rn:src", 60)
        r.rename("rn:src", "rn:dst")

        assert r.ttl("rn:src") == -2
        dst_ttl = r.ttl("rn:dst")
        assert dst_ttl > 0, f"renamed key must carry TTL, got {dst_ttl}"
        assert r.get("rn:dst") == "value"

    def test_rename_overwrites_target_ttl(self, r):
        r.set("rn2:src", "new")
        r.set("rn2:dst", "old")
        r.expire("rn2:dst", 5)

        r.rename("rn2:src", "rn2:dst")
        assert r.ttl("rn2:dst") == -1, "persistent source clears destination's TTL"
        assert r.get("rn2:dst") == "new"

    def test_rename_src_with_ttl_to_persistent_dst(self, r):
        r.set("src", "v", ex=30)
        r.set("dst", "existing")
        r.rename("src", "dst")
        assert r.ttl("dst") > 0
        assert r.get("dst") == "v"


# ── Edge cases ────────────────────────────────────────────────────────────────

class TestExpiryEdgeCases:
    def test_multiple_keys_expire_independently(self, r):
        r.set("ind:1", "a")
        r.set("ind:2", "b")
        r.set("ind:3", "c")
        r.pexpire("ind:1", 200)
        r.pexpire("ind:2", 5000)

        time.sleep(0.5)
        assert r.get("ind:1") is None, "ind:1 must have expired"
        assert r.get("ind:2") == "b",  "ind:2 must still exist"
        assert r.get("ind:3") == "c",  "ind:3 (persistent) must still exist"

    def test_zero_ttl_deletes_immediately(self, r):
        r.set("zero:key", "v")
        r.expire("zero:key", 0)
        assert r.get("zero:key") is None

    def test_negative_expire_deletes_key(self, r):
        r.set("neg:key", "v")
        try:
            r.expire("neg:key", -1)
        except redis.ResponseError:
            pass
        ttl = r.ttl("neg:key")
        assert ttl in (-2, -1), f"Negative expire must delete or be rejected, ttl={ttl}"

    def test_type_unchanged_by_expiry(self, r):
        r.lpush("type:list", "a", "b")
        r.expire("type:list", 60)
        assert r.type("type:list") == "list"

    def test_keys_with_ttl_counted_in_dbsize(self, r):
        r.set("db:1", "v")
        r.set("db:2", "v")
        r.expire("db:2", 60)
        assert r.dbsize() == 2

    def test_pexpire_then_ttl_seconds_approximate(self, r):
        """After PEXPIRE 5000ms, TTL (in seconds) should be 4 or 5."""
        r.set("k", "v")
        r.pexpire("k", 5000)
        ttl = r.ttl("k")
        assert ttl in (4, 5), f"TTL should be 4 or 5, got {ttl}"

    def test_expireat_far_future(self, r):
        """EXPIREAT with a timestamp 1 year in the future should work."""
        r.set("k", "v")
        far_future = int(time.time()) + 365 * 24 * 3600
        r.expireat("k", far_future)
        ttl = r.ttl("k")
        assert ttl > 30 * 24 * 3600  # at least 30 days

    def test_expired_key_not_returned_by_keys(self, r):
        """Expired keys must not appear in KEYS * results."""
        r.set("live", "v", ex=60)
        r.set("dead", "v", px=50)
        time.sleep(0.1)
        keys = r.keys("*")
        assert "dead" not in keys
        assert "live" in keys


# ── Expiry under load ─────────────────────────────────────────────────────────

class TestExpiryUnderLoad:
    def test_concurrent_expire_and_read(self, r):
        n = 50
        for i in range(n):
            r.set(f"load:ex:{i}", f"v{i}")
            r.pexpire(f"load:ex:{i}", 200)

        errors = []

        def reader():
            c = make_client()
            try:
                for i in range(n):
                    val = c.get(f"load:ex:{i}")
                    if val is not None and not val.startswith("v"):
                        errors.append(f"unexpected value {val!r} for load:ex:{i}")
            except Exception as e:
                errors.append(str(e))
            finally:
                c.close()

        threads = [threading.Thread(target=reader) for _ in range(5)]
        for t in threads:
            t.start()
        time.sleep(0.3)
        for t in threads:
            t.join()

        assert not errors, f"Expiry-under-load errors: {errors}"

        time.sleep(0.4)
        for i in range(n):
            assert r.get(f"load:ex:{i}") is None, \
                f"load:ex:{i} must be expired by now"

    def test_scan_with_expiring_keys(self, r):
        for i in range(20):
            r.set(f"scan:live:{i}", "alive")

        for i in range(10):
            r.set(f"scan:dead:{i}", "dying")
            r.pexpire(f"scan:dead:{i}", 100)

        time.sleep(0.2)

        cursor = 0
        found_dead = []
        iteration = 0
        while True:
            cursor, keys = r.scan(cursor, match="scan:*", count=100)
            for k in keys:
                if k.startswith("scan:dead:"):
                    found_dead.append(k)
            iteration += 1
            if cursor == 0 or iteration > 20:
                break

        assert not found_dead, \
            f"SCAN returned expired keys: {found_dead}"

    def test_many_expiring_keys_ttl_accuracy(self, r):
        """Verify that 100 keys with different TTLs expire within their window."""
        for i in range(100):
            r.set(f"ttl:acc:{i}", "v")
            r.pexpire(f"ttl:acc:{i}", 300 + i * 2)

        time.sleep(0.5)  # all should be expired

        alive = sum(1 for i in range(100) if r.get(f"ttl:acc:{i}") is not None)
        assert alive == 0, f"{alive} keys still alive after expected expiry"
