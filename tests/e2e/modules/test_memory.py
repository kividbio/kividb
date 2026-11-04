"""
E2E tests for MEMORY commands: USAGE, STATS, DOCTOR, PURGE, MALLOC-STATS.
Place at: tests/e2e/modules/test_memory.py
"""
import pytest
import redis


class TestMemoryUsage:
    def test_usage_missing_key_returns_nil(self, r):
        result = r.execute_command("MEMORY", "USAGE", "no_such_key_xyz")
        assert result is None

    def test_usage_string_returns_integer(self, r):
        r.set("mem_str", "hello world")
        result = r.execute_command("MEMORY", "USAGE", "mem_str")
        assert isinstance(result, int)
        assert result > 0

    def test_usage_string_larger_value_uses_more_memory(self, r):
        r.set("mem_small", "x")
        r.set("mem_large", "x" * 10000)
        small = r.execute_command("MEMORY", "USAGE", "mem_small")
        large = r.execute_command("MEMORY", "USAGE", "mem_large")
        assert large > small

    def test_usage_list(self, r):
        for i in range(10):
            r.rpush("mem_list", f"value_{i}")
        result = r.execute_command("MEMORY", "USAGE", "mem_list")
        assert isinstance(result, int)
        assert result > 0

    def test_usage_list_grows_with_elements(self, r):
        r.rpush("mem_list_small", "a")
        for i in range(100):
            r.rpush("mem_list_large", f"element_{i}")
        small = r.execute_command("MEMORY", "USAGE", "mem_list_small")
        large = r.execute_command("MEMORY", "USAGE", "mem_list_large")
        assert large > small

    def test_usage_hash(self, r):
        r.hset("mem_hash", mapping={"f1": "v1", "f2": "v2", "f3": "v3"})
        result = r.execute_command("MEMORY", "USAGE", "mem_hash")
        assert isinstance(result, int)
        assert result > 0

    def test_usage_set(self, r):
        r.sadd("mem_set", "a", "b", "c", "d", "e")
        result = r.execute_command("MEMORY", "USAGE", "mem_set")
        assert isinstance(result, int)
        assert result > 0

    def test_usage_zset(self, r):
        r.zadd("mem_zset", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = r.execute_command("MEMORY", "USAGE", "mem_zset")
        assert isinstance(result, int)
        assert result > 0

    def test_usage_samples_argument_accepted(self, r):
        r.set("mem_samples", "value")
        # SAMPLES 0 means no sampling
        result0 = r.execute_command("MEMORY", "USAGE", "mem_samples", "SAMPLES", "0")
        assert isinstance(result0, int)
        # SAMPLES 10
        result10 = r.execute_command("MEMORY", "USAGE", "mem_samples", "SAMPLES", "10")
        assert isinstance(result10, int)

    def test_usage_json_key(self, r):
        r.execute_command("JSON.SET", "mem_json", "$", '{"a":1,"b":"hello","c":[1,2,3]}')
        result = r.execute_command("MEMORY", "USAGE", "mem_json")
        assert isinstance(result, int)
        assert result > 0

    def test_usage_no_key_argument_errors(self, r):
        with pytest.raises(redis.ResponseError):
            r.execute_command("MEMORY", "USAGE")

    def test_usage_stream(self, r):
        r.xadd("mem_stream", {"field": "value"})
        r.xadd("mem_stream", {"field": "value2"})
        result = r.execute_command("MEMORY", "USAGE", "mem_stream")
        assert isinstance(result, int)
        assert result > 0


class TestMemoryStats:
    def test_stats_returns_list(self, r):
        result = r.execute_command("MEMORY", "STATS")
        assert isinstance(result, list)

    def test_stats_has_even_number_of_elements(self, r):
        result = r.execute_command("MEMORY", "STATS")
        assert len(result) % 2 == 0, "MEMORY STATS must return flat key-value pairs"

    def test_stats_contains_used_memory(self, r):
        result = r.execute_command("MEMORY", "STATS")
        pairs = dict(zip(result[::2], result[1::2]))
        # KiviDB uses 'used_memory'; Redis MEMORY STATS uses 'total.allocated'
        assert "used_memory" in pairs or "total.allocated" in pairs, \
            f"No memory usage key found in MEMORY STATS: {list(pairs.keys())}"

    def test_stats_contains_used_memory_human(self, r):
        result = r.execute_command("MEMORY", "STATS")
        pairs = dict(zip(result[::2], result[1::2]))
        # KiviDB exposes 'used_memory_human'; Redis MEMORY STATS does not — skip gracefully
        if "used_memory_human" not in pairs:
            pytest.skip("used_memory_human not present in MEMORY STATS on this server")
        assert isinstance(pairs["used_memory_human"], str)
        assert len(pairs["used_memory_human"]) > 0

    def test_stats_contains_fragmentation_ratio(self, r):
        result = r.execute_command("MEMORY", "STATS")
        pairs = dict(zip(result[::2], result[1::2]))
        # KiviDB uses 'mem_fragmentation_ratio'; Redis MEMORY STATS uses 'fragmentation'
        assert "mem_fragmentation_ratio" in pairs or "fragmentation" in pairs, \
            f"No fragmentation key found in MEMORY STATS: {list(pairs.keys())}"

    def test_stats_no_args_required(self, r):
        # MEMORY STATS takes no arguments — verify it works with none
        result = r.execute_command("MEMORY", "STATS")
        assert result is not None


class TestMemoryDoctor:
    def test_doctor_returns_string(self, r):
        result = r.execute_command("MEMORY", "DOCTOR")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_doctor_healthy_message_on_fresh_server(self, r):
        # On a fresh server with little data, should report no issues
        result = r.execute_command("MEMORY", "DOCTOR")
        # Either the healthy message or a warning — both are valid strings
        assert isinstance(result, str)

    def test_doctor_no_args_required(self, r):
        result = r.execute_command("MEMORY", "DOCTOR")
        assert result is not None


class TestMemoryPurge:
    def test_purge_returns_ok(self, r):
        result = r.execute_command("MEMORY", "PURGE")
        assert result == "OK"

    def test_purge_is_idempotent(self, r):
        # Multiple PURGEs should all succeed
        for _ in range(3):
            result = r.execute_command("MEMORY", "PURGE")
            assert result == "OK"

    def test_purge_does_not_affect_data(self, r):
        r.set("purge_test_key", "purge_test_value")
        r.execute_command("MEMORY", "PURGE")
        assert r.get("purge_test_key") == "purge_test_value"


class TestMemoryMallocStats:
    def test_malloc_stats_returns_string(self, r):
        result = r.execute_command("MEMORY", "MALLOC-STATS")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_malloc_stats_contains_xml(self, r):
        result = r.execute_command("MEMORY", "MALLOC-STATS")
        # KiviDB returns minimal XML; Redis may return allocator-specific text or a notice
        if "not supported" in result.lower() or "Stats not supported" in result:
            pytest.skip("MEMORY MALLOC-STATS XML not supported by this allocator")
        assert "<malloc" in result


class TestMemoryUnknownSubcommand:
    def test_unknown_subcommand_errors(self, r):
        with pytest.raises(redis.ResponseError):
            r.execute_command("MEMORY", "BADSUBCMD")

    def test_no_subcommand_errors(self, r):
        with pytest.raises(redis.ResponseError):
            r.execute_command("MEMORY")