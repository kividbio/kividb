"""
Hash command tests — exhaustive coverage of all Redis hash commands.

All tests define the ground-truth Redis behaviour that a replacement must match.
"""
import pytest
import redis
from utils.helpers import make_client, run_parallel


class TestHSet:
    def test_hset_single(self, r):
        assert r.hset("h", "f", "v") == 1
        assert r.hget("h", "f") == "v"

    def test_hset_multiple(self, r):
        assert r.hset("h", mapping={"f1": "v1", "f2": "v2"}) == 2
        assert r.hget("h", "f1") == "v1"
        assert r.hget("h", "f2") == "v2"

    def test_hset_update_existing(self, r):
        r.hset("h", "f", "old")
        assert r.hset("h", "f", "new") == 0  # updated, not added
        assert r.hget("h", "f") == "new"

    def test_hset_returns_new_fields_count_only(self, r):
        """HSET returns count of NEW fields added, not updated ones."""
        r.hset("h", mapping={"a": "1", "b": "2", "c": "3"})
        # Update two existing + add one new → returns 1
        count = r.hset("h", mapping={"a": "new_a", "b": "new_b", "d": "4"})
        assert count == 1
        assert r.hget("h", "a") == "new_a"
        assert r.hget("h", "d") == "4"

    def test_hmset(self, r):
        r.hmset("h", {"a": "1", "b": "2"})
        assert r.hget("h", "a") == "1"
        assert r.hget("h", "b") == "2"

    def test_hget_missing_field(self, r):
        r.hset("h", "f", "v")
        assert r.hget("h", "missing") is None

    def test_hget_missing_key(self, r):
        assert r.hget("missing", "f") is None

    def test_wrongtype(self, r):
        r.set("k", "string")
        with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
            r.hset("k", "f", "v")

    def test_hset_field_value_empty_string(self, r):
        """Hash fields can hold an empty string value."""
        r.hset("h", "empty", "")
        assert r.hget("h", "empty") == ""
        assert r.hexists("h", "empty") is True

    def test_hset_field_name_with_special_chars(self, r):
        r.hset("h", "field:with:colons", "value")
        assert r.hget("h", "field:with:colons") == "value"

    def test_hset_numeric_field_value(self, r):
        """Numeric values stored as strings."""
        r.hset("h", "n", "42")
        assert r.hget("h", "n") == "42"


class TestHMGet:
    def test_hmget(self, r):
        r.hset("h", mapping={"a": "1", "b": "2"})
        assert r.hmget("h", ["a", "b", "c"]) == ["1", "2", None]

    def test_hmget_all_missing(self, r):
        r.hset("h", "x", "v")
        assert r.hmget("h", ["a", "b"]) == [None, None]

    def test_hmget_on_missing_key(self, r):
        assert r.hmget("missing", ["a", "b"]) == [None, None]

    def test_hmget_preserves_order(self, r):
        r.hset("h", mapping={"z": "26", "a": "1", "m": "13"})
        result = r.hmget("h", ["m", "a", "z"])
        assert result == ["13", "1", "26"]


class TestHDel:
    def test_hdel_existing(self, r):
        r.hset("h", mapping={"a": "1", "b": "2"})
        assert r.hdel("h", "a") == 1
        assert r.hget("h", "a") is None

    def test_hdel_multiple(self, r):
        r.hset("h", mapping={"a": "1", "b": "2", "c": "3"})
        assert r.hdel("h", "a", "b") == 2
        assert r.hget("h", "c") == "3"

    def test_hdel_missing(self, r):
        assert r.hdel("h", "f") == 0

    def test_hdel_missing_field_does_not_count(self, r):
        r.hset("h", "a", "1")
        assert r.hdel("h", "a", "nosuch") == 1

    def test_hdel_all_fields_empties_hash(self, r):
        """Deleting all fields of a hash results in an empty hash (HLEN=0, HGETALL={})."""
        r.hset("h", mapping={"a": "1"})
        r.hdel("h", "a")
        assert r.hlen("h") == 0
        assert r.hgetall("h") == {}


class TestHExists:
    def test_hexists(self, r):
        r.hset("h", "f", "v")
        assert r.hexists("h", "f") is True
        assert r.hexists("h", "missing") is False

    def test_hexists_on_missing_key(self, r):
        assert r.hexists("nosuch", "f") is False

    def test_hexists_after_hdel(self, r):
        r.hset("h", "f", "v")
        r.hdel("h", "f")
        assert r.hexists("h", "f") is False


class TestHLen:
    def test_hlen(self, r):
        r.hset("h", mapping={"a": "1", "b": "2"})
        assert r.hlen("h") == 2

    def test_hlen_missing(self, r):
        assert r.hlen("missing") == 0

    def test_hlen_after_del(self, r):
        r.hset("h", mapping={"a": "1", "b": "2"})
        r.hdel("h", "a")
        assert r.hlen("h") == 1

    def test_hlen_large_hash(self, r):
        data = {f"f{i}": str(i) for i in range(1000)}
        r.hset("h", mapping=data)
        assert r.hlen("h") == 1000


class TestHKeys:
    def test_hkeys(self, r):
        r.hset("h", mapping={"a": "1", "b": "2"})
        keys = r.hkeys("h")
        assert sorted(keys) == ["a", "b"]

    def test_hvals(self, r):
        r.hset("h", mapping={"a": "1", "b": "2"})
        vals = r.hvals("h")
        assert sorted(vals) == ["1", "2"]

    def test_hgetall(self, r):
        r.hset("h", mapping={"a": "1", "b": "2"})
        assert r.hgetall("h") == {"a": "1", "b": "2"}

    def test_hgetall_missing_key_empty_dict(self, r):
        assert r.hgetall("missing") == {}

    def test_hkeys_empty_list_on_missing_key(self, r):
        assert r.hkeys("missing") == []

    def test_hvals_empty_list_on_missing_key(self, r):
        assert r.hvals("missing") == []

    def test_hgetall_all_fields_present(self, r):
        data = {f"f{i}": f"v{i}" for i in range(10)}
        r.hset("h", mapping=data)
        result = r.hgetall("h")
        assert result == data


class TestHIncr:
    def test_hincrby(self, r):
        r.hset("h", "counter", "10")
        assert r.hincrby("h", "counter", 5) == 15

    def test_hincrby_creates(self, r):
        assert r.hincrby("h", "counter", 3) == 3

    def test_hincrby_negative(self, r):
        r.hset("h", "counter", "10")
        assert r.hincrby("h", "counter", -3) == 7

    def test_hincrbyfloat(self, r):
        r.hset("h", "val", "1.5")
        result = r.hincrbyfloat("h", "val", 0.5)
        assert abs(float(result) - 2.0) < 1e-9

    def test_hincrby_non_int_error(self, r):
        r.hset("h", "f", "notanint")
        with pytest.raises(redis.ResponseError):
            r.hincrby("h", "f", 1)

    def test_hincrbyfloat_on_float_field(self, r):
        r.hset("h", "f", "3.14")
        result = r.hincrbyfloat("h", "f", 2.86)
        assert abs(float(result) - 6.0) < 1e-9

    def test_hincrby_creates_hash_if_missing(self, r):
        """HINCRBY on a completely missing key creates the hash."""
        result = r.hincrby("newh", "field", 10)
        assert result == 10
        assert r.hget("newh", "field") == "10"

    def test_hincrby_stores_as_integer_string(self, r):
        r.hincrby("h", "n", 42)
        assert r.hget("h", "n") == "42"

    def test_hincrby_zero(self, r):
        r.hset("h", "n", "5")
        assert r.hincrby("h", "n", 0) == 5

    def test_hincrby_negative_to_zero(self, r):
        r.hset("h", "n", "5")
        assert r.hincrby("h", "n", -5) == 0


class TestHSetNx:
    def test_hsetnx_new(self, r):
        assert r.hsetnx("h", "f", "v") == 1
        assert r.hget("h", "f") == "v"

    def test_hsetnx_existing(self, r):
        r.hset("h", "f", "old")
        assert r.hsetnx("h", "f", "new") == 0
        assert r.hget("h", "f") == "old"

    def test_hsetnx_creates_hash(self, r):
        assert r.hsetnx("newh", "field", "value") == 1
        assert r.hget("newh", "field") == "value"


class TestHRandField:
    def test_hrandfield_single(self, r):
        r.hset("h", mapping={"a": "1", "b": "2", "c": "3"})
        field = r.hrandfield("h")
        assert field in ["a", "b", "c"]

    def test_hrandfield_count(self, r):
        r.hset("h", mapping={"a": "1", "b": "2", "c": "3"})
        fields = r.hrandfield("h", count=2)
        assert len(fields) == 2
        assert all(f in ["a", "b", "c"] for f in fields)
        assert len(set(fields)) == 2  # no duplicates with positive count

    def test_hrandfield_negative_count_repeats(self, r):
        r.hset("h", "only", "one")
        fields = r.hrandfield("h", count=-5)
        assert len(fields) == 5
        assert all(f == "only" for f in fields)

    def test_hrandfield_withvalues(self, r):
        r.hset("h", mapping={"a": "1", "b": "2"})
        result = r.hrandfield("h", count=2, withvalues=True)
        # RESP2: a flat list of field, value, field, value. RESP3: a list of
        # [field, value] pairs, as Redis sends it.
        if result and isinstance(result[0], list):
            assert len(result) == 2
            pairs = {field: value for field, value in result}
        else:
            assert len(result) == 4
            pairs = dict(zip(result[::2], result[1::2]))
        assert set(pairs) == {"a", "b"}
        for k, v in pairs.items():
            assert k in ["a", "b"]
            assert v == ("1" if k == "a" else "2")

    def test_hrandfield_count_gt_len_returns_all(self, r):
        """Count > number of fields returns all fields (no repeats)."""
        r.hset("h", mapping={"x": "1", "y": "2"})
        fields = r.hrandfield("h", count=10)
        assert sorted(fields) == ["x", "y"]

    def test_hrandfield_on_missing_key_returns_none(self, r):
        assert r.hrandfield("missing") is None

    def test_hrandfield_count_on_missing_key_returns_empty(self, r):
        result = r.hrandfield("missing", count=3)
        assert result == [] or result is None

    def test_hrandfield_count_zero_returns_empty(self, r):
        r.hset("h", "f", "v")
        result = r.hrandfield("h", count=0)
        assert result == []


class TestHScan:
    def test_hscan_full(self, r):
        data = {f"field{i}": str(i) for i in range(20)}
        r.hset("h", mapping=data)
        seen = {}
        cursor = 0
        while True:
            cursor, partial = r.hscan("h", cursor)
            seen.update(partial)
            if cursor == 0:
                break
        assert seen == data

    def test_hscan_match(self, r):
        r.hset("h", mapping={"alpha": "1", "beta": "2", "gamma": "3"})
        _, result = r.hscan("h", 0, match="a*")
        assert "alpha" in result

    def test_hscan_match_no_results(self, r):
        r.hset("h", mapping={"alpha": "1", "beta": "2"})
        seen = {}
        cursor = 0
        while True:
            cursor, partial = r.hscan("h", cursor, match="z*")
            seen.update(partial)
            if cursor == 0:
                break
        assert seen == {}

    def test_hscan_count_hint_returns_all(self, r):
        """COUNT hint doesn't filter; all fields must be returned eventually."""
        data = {f"f{i}": str(i) for i in range(100)}
        r.hset("h", mapping=data)
        seen = {}
        cursor = 0
        while True:
            cursor, partial = r.hscan("h", cursor, count=10)
            seen.update(partial)
            if cursor == 0:
                break
        assert len(seen) == 100

    def test_hscan_missing_key_empty(self, r):
        cursor, result = r.hscan("missing", 0)
        assert cursor == 0
        assert result == {}


class TestHashRealWorld:
    def test_user_profile(self, r):
        """Simulate storing / updating a user profile as a hash."""
        user_id = "user:1001"
        r.hset(user_id, mapping={
            "name": "Alice",
            "email": "alice@example.com",
            "age": "30",
            "score": "0",
        })
        r.hset(user_id, "name", "Alice Smith")
        assert r.hget(user_id, "name") == "Alice Smith"

        r.hincrby(user_id, "score", 10)
        r.hincrby(user_id, "score", 5)
        assert r.hget(user_id, "score") == "15"

        r.hdel(user_id, "age")
        assert r.hexists(user_id, "age") is False

        profile = r.hgetall(user_id)
        assert profile["email"] == "alice@example.com"
        assert profile["score"] == "15"
        assert "age" not in profile

    def test_inventory_counter(self, r):
        """Use HINCRBY to track inventory counts per SKU."""
        r.hset("inventory", mapping={"sku:001": "100", "sku:002": "50"})
        r.hincrby("inventory", "sku:001", -10)  # sold 10
        r.hincrby("inventory", "sku:003", 200)  # new SKU restocked
        assert r.hget("inventory", "sku:001") == "90"
        assert r.hget("inventory", "sku:002") == "50"
        assert r.hget("inventory", "sku:003") == "200"

    def test_concurrent_hincrby(self, r):
        """100 parallel HINEBYs on same field must produce the correct total."""
        r.hset("h", "counter", "0")
        fns = [lambda: make_client().hincrby("h", "counter", 1) for _ in range(100)]
        run_parallel(fns)
        assert r.hget("h", "counter") == "100"

    def test_hash_as_config_store(self, r):
        """Hashes work well as a config store — field-level access."""
        r.hset("config:app", mapping={
            "max_connections": "100",
            "timeout": "30",
            "debug": "false",
        })
        assert r.hget("config:app", "timeout") == "30"
        r.hset("config:app", "debug", "true")
        assert r.hget("config:app", "debug") == "true"
        fields = r.hkeys("config:app")
        assert sorted(fields) == ["debug", "max_connections", "timeout"]
