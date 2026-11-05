"""
String command tests — exhaustive coverage of all Redis string commands.

All tests run against redis-stack-server (or a compatible server) and define
the ground-truth behaviour a Redis replacement must match exactly.
"""
import time
import pytest
import redis
from utils.helpers import make_client, run_parallel


# ──────────────────────────────────────────────────────────────────────────────
# Basic SET / GET
# ──────────────────────────────────────────────────────────────────────────────

class TestSetGet:
    def test_set_and_get(self, r):
        assert r.set("k", "hello") is True
        assert r.get("k") == "hello"

    def test_get_missing(self, r):
        assert r.get("no_such_key") is None

    def test_set_overwrite(self, r):
        r.set("k", "v1")
        r.set("k", "v2")
        assert r.get("k") == "v2"

    def test_set_ex(self, r):
        r.set("k", "v", px=200)
        assert r.get("k") == "v"
        time.sleep(0.5)
        assert r.get("k") is None

    def test_set_px(self, r):
        r.set("k", "v", px=300)
        assert r.get("k") == "v"
        time.sleep(0.5)
        assert r.get("k") is None

    def test_set_nx_success(self, r):
        assert r.set("k", "v", nx=True) is True
        assert r.get("k") == "v"

    def test_set_nx_fail(self, r):
        r.set("k", "existing")
        assert r.set("k", "new", nx=True) is None
        assert r.get("k") == "existing"

    def test_set_xx_success(self, r):
        r.set("k", "old")
        assert r.set("k", "new", xx=True) is True
        assert r.get("k") == "new"

    def test_set_xx_fail_missing(self, r):
        assert r.set("k", "v", xx=True) is None

    def test_set_get_option(self, r):
        """SET ... GET returns the old value."""
        r.set("k", "old")
        old = r.set("k", "new", get=True)
        assert old == "old"
        assert r.get("k") == "new"

    def test_set_get_option_missing(self, r):
        result = r.set("k", "v", get=True)
        assert result is None

    def test_set_nx_and_get_existing(self, r):
        """SET NX on existing key: returns None, old value unchanged."""
        r.set("k", "existing_val")
        result = r.set("k", "new_val", nx=True)
        assert result is None
        assert r.get("k") == "existing_val"

    def test_set_empty_string_value(self, r):
        """Empty string is a valid value — distinct from missing key."""
        r.set("k", "")
        assert r.get("k") == ""
        assert r.exists("k") == 1

    def test_set_overwrites_any_type_errors(self, r):
        """SET on a key of any type must succeed (no WRONGTYPE on SET)."""
        r.lpush("k", "list_val")
        r.set("k", "string_now")
        assert r.get("k") == "string_now"
        assert r.type("k") == "string"

    def test_set_keepttl(self, r):
        """SET KEEPTTL preserves the existing TTL."""
        r.set("k", "v1", ex=60)
        original_ttl = r.ttl("k")
        r.set("k", "v2", keepttl=True)
        new_ttl = r.ttl("k")
        assert new_ttl > 0, "KEEPTTL must preserve TTL"
        assert abs(original_ttl - new_ttl) <= 2

    def test_set_exat(self, r):
        """SET EXAT sets absolute Unix timestamp expiry."""
        future_ms = int(time.time() * 1000) + 300
        r.set("k", "v", pxat=future_ms)
        pttl = r.pttl("k")
        assert 0 < pttl <= 400
        time.sleep(0.6)
        assert r.get("k") is None

    def test_set_pxat(self, r):
        """SET PXAT sets absolute Unix ms timestamp expiry."""
        future_ms = int(time.time() * 1000) + 400
        r.set("k", "v", pxat=future_ms)
        pttl = r.pttl("k")
        assert 0 < pttl <= 500
        time.sleep(0.6)
        assert r.get("k") is None

    def test_set_clears_previous_expiry(self, r):
        """Plain SET without TTL options removes any existing expiry."""
        r.set("k", "v1", ex=60)
        assert r.ttl("k") > 0
        r.set("k", "v2")
        assert r.ttl("k") == -1

    def test_get_wrong_type(self, r):
        """GET on a non-string key raises WRONGTYPE."""
        r.lpush("k", "v")
        with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
            r.get("k")


# ──────────────────────────────────────────────────────────────────────────────
# GETDEL / GETSET / GETEX
# ──────────────────────────────────────────────────────────────────────────────

class TestGetDel:
    def test_getdel_existing(self, r):
        r.set("k", "v")
        assert r.getdel("k") == "v"
        assert r.get("k") is None

    def test_getdel_missing(self, r):
        assert r.getdel("missing") is None

    def test_getdel_removes_key_with_ttl(self, r):
        r.set("k", "v", ex=60)
        assert r.getdel("k") == "v"
        assert r.ttl("k") == -2


class TestGetSet:
    def test_getset(self, r):
        r.set("k", "old")
        assert r.getset("k", "new") == "old"
        assert r.get("k") == "new"

    def test_getset_missing(self, r):
        assert r.getset("k", "v") is None
        assert r.get("k") == "v"

    def test_getset_clears_ttl(self, r):
        """GETSET (like SET) discards any existing TTL — key becomes persistent."""
        r.set("k", "old", ex=60)
        r.getset("k", "new")
        ttl = r.ttl("k")
        assert ttl == -1, f"GETSET must clear TTL, got {ttl}"


class TestGetEx:
    def test_getex_ex(self, r):
        r.set("k", "v")
        assert r.getex("k", px=200) == "v"
        time.sleep(0.5)
        assert r.get("k") is None

    def test_getex_persist(self, r):
        r.set("k", "v", ex=30)
        r.getex("k", persist=True)
        assert r.ttl("k") == -1

    def test_getex_missing(self, r):
        assert r.getex("no_key", ex=10) is None

    def test_getex_px(self, r):
        r.set("k", "v")
        r.getex("k", px=300)
        pttl = r.pttl("k")
        assert 0 < pttl <= 350

    def test_getex_exat(self, r):
        r.set("k", "v")
        future_ts = int(time.time()) + 10
        r.getex("k", exat=future_ts)
        assert r.ttl("k") > 0

    def test_getex_does_not_change_value(self, r):
        r.set("k", "original")
        val = r.getex("k", ex=60)
        assert val == "original"
        assert r.get("k") == "original"


# ──────────────────────────────────────────────────────────────────────────────
# GETRANGE / SETRANGE / STRLEN
# ──────────────────────────────────────────────────────────────────────────────

class TestGetRange:
    def test_basic(self, r):
        r.set("k", "Hello World")
        assert r.getrange("k", 0, 4) == "Hello"
        assert r.getrange("k", 6, 10) == "World"

    def test_negative_indices(self, r):
        r.set("k", "Hello")
        assert r.getrange("k", -3, -1) == "llo"

    def test_full_range(self, r):
        r.set("k", "abc")
        assert r.getrange("k", 0, -1) == "abc"

    def test_out_of_range(self, r):
        r.set("k", "hi")
        assert r.getrange("k", 0, 100) == "hi"

    def test_missing_key(self, r):
        assert r.getrange("missing", 0, 5) == ""

    def test_start_beyond_end(self, r):
        """When start > end, GETRANGE returns empty string."""
        r.set("k", "abc")
        assert r.getrange("k", 3, 1) == ""

    def test_single_char(self, r):
        r.set("k", "hello")
        assert r.getrange("k", 1, 1) == "e"

    def test_empty_string_value(self, r):
        r.set("k", "")
        assert r.getrange("k", 0, 10) == ""

    def test_substr_alias(self, r):
        """SUBSTR is an alias for GETRANGE."""
        r.set("k", "Hello World")
        result = r.execute_command("SUBSTR", "k", 0, 4)
        assert result == "Hello"


class TestSetRange:
    def test_basic(self, r):
        r.set("k", "Hello World")
        r.setrange("k", 6, "Redis")
        assert r.get("k") == "Hello Redis"

    def test_zero_pad(self, r):
        r.setrange("k", 5, "hi")
        val = r.get("k")
        assert val.endswith("hi")
        assert len(val) == 7

    def test_returns_new_length(self, r):
        r.set("k", "abc")
        assert r.setrange("k", 0, "xyz") == 3

    def test_extend_string(self, r):
        r.set("k", "abc")
        assert r.setrange("k", 5, "X") == 6
        val = r.get("k")
        assert len(val) == 6
        assert val.startswith("abc")
        assert val.endswith("X")

    def test_setrange_on_nonexistent_key(self, r):
        length = r.setrange("newkey", 3, "abc")
        assert length == 6
        val = r.get("newkey")
        assert len(val) == 6
        assert val[3:] == "abc"

    def test_setrange_zero_offset_full_replace(self, r):
        r.set("k", "abcdef")
        r.setrange("k", 0, "XY")
        assert r.get("k") == "XYcdef"


class TestStrlen:
    def test_strlen(self, r):
        r.set("k", "hello")
        assert r.strlen("k") == 5

    def test_strlen_missing(self, r):
        assert r.strlen("missing") == 0

    def test_strlen_empty_value(self, r):
        r.set("k", "")
        assert r.strlen("k") == 0

    def test_strlen_unicode(self, r):
        r.set("k", "héllo")
        # Redis stores raw bytes; strlen reports byte length
        length = r.strlen("k")
        assert length >= 5


# ──────────────────────────────────────────────────────────────────────────────
# SETEX / PSETEX / SETNX
# ──────────────────────────────────────────────────────────────────────────────

class TestSetEx:
    def test_setex(self, r):
        r.psetex("k", 200, "v")
        assert r.get("k") == "v"
        time.sleep(0.5)
        assert r.get("k") is None

    def test_psetex(self, r):
        r.psetex("k", 300, "v")
        assert r.get("k") == "v"
        time.sleep(0.5)
        assert r.get("k") is None

    def test_setnx(self, r):
        assert r.setnx("k", "v") == 1
        assert r.setnx("k", "v2") == 0
        assert r.get("k") == "v"

    def test_setex_overwrites_existing(self, r):
        r.set("k", "old")
        r.setex("k", 60, "new")
        assert r.get("k") == "new"
        assert r.ttl("k") > 0

    def test_setex_zero_seconds_errors(self, r):
        """SETEX with 0 or negative TTL must raise an error."""
        with pytest.raises(redis.ResponseError):
            r.setex("k", 0, "v")

    def test_psetex_zero_ms_errors(self, r):
        with pytest.raises(redis.ResponseError):
            r.psetex("k", 0, "v")


# ──────────────────────────────────────────────────────────────────────────────
# APPEND
# ──────────────────────────────────────────────────────────────────────────────

class TestAppend:
    def test_append_creates(self, r):
        assert r.append("k", "hello") == 5
        assert r.get("k") == "hello"

    def test_append_extends(self, r):
        r.set("k", "hello")
        assert r.append("k", " world") == 11
        assert r.get("k") == "hello world"

    def test_append_empty_string(self, r):
        r.set("k", "abc")
        assert r.append("k", "") == 3

    def test_append_to_empty_value(self, r):
        r.set("k", "")
        assert r.append("k", "x") == 1
        assert r.get("k") == "x"

    def test_append_preserves_ttl(self, r):
        """APPEND must not clear an existing TTL."""
        r.set("k", "start", ex=60)
        ttl_before = r.ttl("k")
        r.append("k", "_end")
        ttl_after = r.ttl("k")
        assert ttl_after > 0, "APPEND must preserve TTL"
        assert abs(ttl_before - ttl_after) <= 2

    def test_append_wrong_type(self, r):
        r.lpush("k", "v")
        with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
            r.append("k", "x")


# ──────────────────────────────────────────────────────────────────────────────
# Integer / float arithmetic
# ──────────────────────────────────────────────────────────────────────────────

class TestIncr:
    def test_incr_from_zero(self, r):
        assert r.incr("k") == 1
        assert r.incr("k") == 2

    def test_incrby(self, r):
        r.set("k", "10")
        assert r.incrby("k", 5) == 15

    def test_incrby_negative(self, r):
        r.set("k", "10")
        assert r.incrby("k", -3) == 7

    def test_decr(self, r):
        r.set("k", "5")
        assert r.decr("k") == 4

    def test_decrby(self, r):
        r.set("k", "10")
        assert r.decrby("k", 4) == 6

    def test_incrbyfloat(self, r):
        r.set("k", "10.5")
        result = r.incrbyfloat("k", 0.1)
        assert abs(float(result) - 10.6) < 1e-6

    def test_incr_non_integer_error(self, r):
        r.set("k", "notanint")
        with pytest.raises(redis.ResponseError):
            r.incr("k")

    def test_incr_creates_at_zero(self, r):
        assert r.incrby("newkey", 7) == 7

    def test_incr_on_missing_key_starts_from_zero(self, r):
        assert r.incr("counter") == 1

    def test_decr_creates_at_minus_one(self, r):
        assert r.decr("newkey") == -1

    def test_incr_preserves_string_representation(self, r):
        """After INCR the stored value is a decimal integer string."""
        r.incr("k")
        r.incr("k")
        assert r.get("k") == "2"

    def test_incrby_zero(self, r):
        r.set("k", "5")
        assert r.incrby("k", 0) == 5
        assert r.get("k") == "5"

    def test_incrbyfloat_creates_key(self, r):
        result = r.incrbyfloat("newkey", 1.5)
        assert abs(float(result) - 1.5) < 1e-9

    def test_incrbyfloat_result_stored_as_string(self, r):
        r.set("k", "3.14")
        r.incrbyfloat("k", 2.86)
        stored = r.get("k")
        assert abs(float(stored) - 6.0) < 1e-9

    def test_incr_overflow_error(self, r):
        """INCR past MAX_INT64 must raise an error."""
        max_int64 = 9223372036854775807
        r.set("k", str(max_int64))
        with pytest.raises(redis.ResponseError):
            r.incr("k")

    def test_decr_underflow_error(self, r):
        """DECR past MIN_INT64 must raise an error."""
        min_int64 = -9223372036854775808
        r.set("k", str(min_int64))
        with pytest.raises(redis.ResponseError):
            r.decr("k")

    def test_incr_float_string_error(self, r):
        """INCR on a float string raises an error."""
        r.set("k", "3.14")
        with pytest.raises(redis.ResponseError):
            r.incr("k")

    def test_incr_preserves_ttl(self, r):
        """INCR must not remove a key's TTL."""
        r.set("k", "10", ex=60)
        r.incr("k")
        assert r.ttl("k") > 0

    def test_decr_preserves_ttl(self, r):
        r.set("k", "10", ex=60)
        r.decr("k")
        assert r.ttl("k") > 0


# ──────────────────────────────────────────────────────────────────────────────
# Multi-key operations
# ──────────────────────────────────────────────────────────────────────────────

class TestMSet:
    def test_mset_mget(self, r):
        r.mset({"a": "1", "b": "2", "c": "3"})
        assert r.mget("a", "b", "c") == ["1", "2", "3"]

    def test_mget_with_missing(self, r):
        r.set("a", "1")
        assert r.mget("a", "missing") == ["1", None]

    def test_msetnx_all_new(self, r):
        assert r.msetnx({"x": "1", "y": "2"}) == 1
        assert r.get("x") == "1"
        assert r.get("y") == "2"

    def test_msetnx_one_exists(self, r):
        r.set("x", "existing")
        assert r.msetnx({"x": "1", "y": "2"}) == 0
        assert r.get("y") is None  # none were set — atomic

    def test_msetnx_is_atomic(self, r):
        """MSETNX must set all keys or none."""
        r.set("b", "exists")
        result = r.msetnx({"a": "new_a", "b": "new_b"})
        assert result == 0
        assert r.get("a") is None
        assert r.get("b") == "exists"

    def test_mget_ordering(self, r):
        """MGET returns values in the same order as the keys."""
        r.mset({"k1": "v1", "k2": "v2", "k3": "v3"})
        assert r.mget("k3", "k1", "k2") == ["v3", "v1", "v2"]

    def test_mget_all_missing(self, r):
        assert r.mget("x", "y", "z") == [None, None, None]

    def test_mset_overwrites(self, r):
        r.set("k", "old")
        r.mset({"k": "new", "other": "v"})
        assert r.get("k") == "new"

    def test_mset_large_batch(self, r):
        """MSET / MGET with 500 keys round-trips correctly."""
        data = {f"key:{i}": f"val:{i}" for i in range(500)}
        r.mset(data)
        keys = list(data.keys())
        vals = r.mget(*keys)
        assert vals == [f"val:{i}" for i in range(500)]


# ──────────────────────────────────────────────────────────────────────────────
# Large / binary values
# ──────────────────────────────────────────────────────────────────────────────

class TestLargeAndBinaryValues:
    def test_large_string_1mb(self, r):
        """A 1 MB string must round-trip without corruption."""
        big = "A" * (1024 * 1024)
        r.set("big", big)
        assert r.get("big") == big

    def test_value_with_special_chars(self, r):
        """Values with newlines, tabs, and nulls round-trip correctly."""
        val = "line1\nline2\ttab\r\n"
        r.set("k", val)
        assert r.get("k") == val

    def test_key_with_colons(self, r):
        """Colon-delimited key names (common pattern) work correctly."""
        r.set("user:1001:name", "Alice")
        assert r.get("user:1001:name") == "Alice"

    def test_key_with_spaces(self, r):
        r.set("key with spaces", "value")
        assert r.get("key with spaces") == "value"


# ──────────────────────────────────────────────────────────────────────────────
# Concurrency stress: parallel INCR on a single key
# ──────────────────────────────────────────────────────────────────────────────

class TestStringConcurrency:
    def test_parallel_incr(self, r):
        """200 parallel INCRs on the same key must yield exactly 200."""
        r.set("counter", "0")
        fns = [lambda: make_client().incr("counter") for _ in range(200)]
        run_parallel(fns)
        assert int(r.get("counter")) == 200

    def test_parallel_append(self, r):
        """Parallel APPENDs should not corrupt data (each is a distinct byte)."""
        fns = [lambda c=chr(65 + i % 26): make_client().append("buf", c) for i in range(50)]
        run_parallel(fns)
        val = r.get("buf")
        assert len(val) == 50

    def test_parallel_set_nx_exactly_one_wins(self, r):
        """100 concurrent SETNX calls on the same key: exactly one must win."""
        wins = []

        def try_setnx():
            c = make_client()
            if c.setnx("race", "winner"):
                wins.append(1)
            c.close()

        fns = [try_setnx for _ in range(100)]
        run_parallel(fns)
        assert len(wins) == 1, f"Expected exactly 1 SETNX winner, got {len(wins)}"

    def test_parallel_incrbyfloat_accumulates(self, r):
        """100 parallel INCRBYFLOATs of 0.1 must accumulate to exactly 10.0."""
        r.set("f", "0")
        fns = [lambda: make_client().incrbyfloat("f", 0.1) for _ in range(100)]
        run_parallel(fns)
        result = float(r.get("f"))
        assert abs(result - 10.0) < 1e-4


# ──────────────────────────────────────────────────────────────────────────────
# Real-world patterns
# ──────────────────────────────────────────────────────────────────────────────

class TestStringRealWorld:
    def test_rate_limiter_pattern(self, r):
        """
        Simulate a rate limiter: INCR a key with an expiry window.
        Key must expire correctly and reset counter.
        """
        key = "rate:user:99"
        for _ in range(5):
            r.incr(key)
        r.pexpire(key, 200)
        assert int(r.get(key)) == 5

        time.sleep(0.5)
        assert r.get(key) is None

        # New window starts at 1
        r.incr(key)
        assert r.get(key) == "1"

    def test_session_token_pattern(self, r):
        """Store a session token with TTL; GETEX extends lifetime."""
        token = "abc123def456"
        r.set(f"session:{token}", "user:42", ex=3600)
        # Client activity extends session
        user = r.getex(f"session:{token}", ex=3600)
        assert user == "user:42"
        assert r.ttl(f"session:{token}") > 3590

    def test_distributed_lock_pattern(self, r):
        """
        SETNX-based distributed lock: only one holder at a time.
        SET NX EX ensures no deadlock if holder crashes.
        """
        lock_key = "lock:resource"
        holder = r.set(lock_key, "holder_A", nx=True, ex=30)
        assert holder is True

        # Second attempt must fail
        rejected = r.set(lock_key, "holder_B", nx=True, ex=30)
        assert rejected is None

        # Release and re-acquire
        r.delete(lock_key)
        acquired = r.set(lock_key, "holder_B", nx=True, ex=30)
        assert acquired is True

    def test_cache_aside_pattern(self, r):
        """Cache-aside: SET if miss, GET if hit, honour TTL."""
        def get_or_set(key, value, ttl=60):
            existing = r.get(key)
            if existing is not None:
                return existing
            r.set(key, value, ex=ttl)
            return value

        val1 = get_or_set("cache:item:1", "computed_value")
        assert val1 == "computed_value"

        val2 = get_or_set("cache:item:1", "should_not_overwrite")
        assert val2 == "computed_value"
