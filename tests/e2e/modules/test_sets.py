"""
Set command tests — exhaustive coverage of all Redis set commands.

All tests define the ground-truth Redis behaviour that a replacement must match.
"""
import pytest
import redis
from utils.helpers import make_client, run_parallel


class TestSAdd:
    def test_sadd_basic(self, r):
        assert r.sadd("s", "a", "b", "c") == 3
        assert r.scard("s") == 3

    def test_sadd_duplicates(self, r):
        r.sadd("s", "a")
        assert r.sadd("s", "a", "b") == 1

    def test_wrongtype(self, r):
        r.set("k", "v")
        with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
            r.sadd("k", "x")

    def test_sadd_creates_key(self, r):
        r.sadd("newset", "x")
        assert r.type("newset") == "set"

    def test_sadd_returns_count_of_new_members_only(self, r):
        r.sadd("s", "a", "b")
        added = r.sadd("s", "b", "c", "d")
        assert added == 2  # b already existed

    def test_sadd_single_member(self, r):
        assert r.sadd("s", "only") == 1

    def test_sadd_empty_string_member(self, r):
        r.sadd("s", "")
        assert r.sismember("s", "")


class TestSCard:
    def test_scard(self, r):
        r.sadd("s", "a", "b")
        assert r.scard("s") == 2

    def test_scard_missing(self, r):
        assert r.scard("missing") == 0

    def test_scard_after_srem(self, r):
        r.sadd("s", "a", "b", "c")
        r.srem("s", "b")
        assert r.scard("s") == 2


class TestSIsMember:
    def test_sismember_present(self, r):
        r.sadd("s", "a")
        assert r.sismember("s", "a")

    def test_sismember_absent(self, r):
        r.sadd("s", "a")
        assert not r.sismember("s", "b")

    def test_sismember_missing_key(self, r):
        assert not r.sismember("missing", "x")

    def test_smismember(self, r):
        r.sadd("s", "a", "b")
        result = r.smismember("s", "a", "b", "c")
        assert result[0]
        assert result[1]
        assert not result[2]

    def test_smismember_all_missing(self, r):
        r.sadd("s", "x")
        result = r.smismember("s", "a", "b")
        assert result == [False, False]

    def test_smismember_on_missing_key(self, r):
        result = r.smismember("missing", "a", "b")
        assert result == [False, False]


class TestSMembers:
    def test_smembers(self, r):
        r.sadd("s", "a", "b", "c")
        assert r.smembers("s") == {"a", "b", "c"}

    def test_smembers_missing_key(self, r):
        assert r.smembers("missing") == set()

    def test_smembers_single_element(self, r):
        r.sadd("s", "only")
        assert r.smembers("s") == {"only"}


class TestSRem:
    def test_srem(self, r):
        r.sadd("s", "a", "b", "c")
        assert r.srem("s", "a", "c") == 2
        assert r.smembers("s") == {"b"}

    def test_srem_missing(self, r):
        assert r.srem("s", "x") == 0

    def test_srem_partial_missing(self, r):
        r.sadd("s", "a", "b")
        assert r.srem("s", "a", "z") == 1  # z doesn't exist

    def test_srem_all_members_empties_set(self, r):
        r.sadd("s", "only")
        r.srem("s", "only")
        assert r.scard("s") == 0
        assert r.smembers("s") == set()

    def test_srem_on_missing_key(self, r):
        assert r.srem("missing", "x") == 0


class TestSPop:
    def test_spop_one(self, r):
        r.sadd("s", "a")
        val = r.spop("s")
        assert val == "a"
        assert r.scard("s") == 0

    def test_spop_count(self, r):
        r.sadd("s", "a", "b", "c", "d")
        popped = r.spop("s", 2)
        assert len(popped) == 2
        assert r.scard("s") == 2

    def test_spop_from_missing_key(self, r):
        assert r.spop("missing") is None

    def test_spop_count_beyond_size(self, r):
        """SPOP count > set size returns all members."""
        r.sadd("s", "a", "b")
        popped = r.spop("s", 10)
        assert len(popped) == 2
        assert r.scard("s") == 0

    def test_spop_removes_member(self, r):
        r.sadd("s", "x", "y", "z")
        member = r.spop("s")
        assert member in {"x", "y", "z"}
        assert not r.sismember("s", member)


class TestSRandMember:
    def test_srandmember_single(self, r):
        r.sadd("s", "a", "b", "c")
        val = r.srandmember("s")
        assert val in {"a", "b", "c"}

    def test_srandmember_count(self, r):
        r.sadd("s", "a", "b", "c")
        vals = r.srandmember("s", 2)
        assert len(vals) == 2
        assert len(set(vals)) == 2

    def test_srandmember_negative_repeats(self, r):
        r.sadd("s", "only")
        vals = r.srandmember("s", -5)
        assert len(vals) == 5
        assert all(v == "only" for v in vals)

    def test_srandmember_count_gt_size_returns_all(self, r):
        """Positive count > size returns all members without repetition."""
        r.sadd("s", "a", "b")
        vals = r.srandmember("s", 10)
        assert sorted(vals) == ["a", "b"]

    def test_srandmember_count_zero(self, r):
        r.sadd("s", "a")
        assert r.srandmember("s", 0) == []

    def test_srandmember_does_not_remove(self, r):
        r.sadd("s", "a", "b")
        r.srandmember("s", 2)
        assert r.scard("s") == 2

    def test_srandmember_missing_key(self, r):
        assert r.srandmember("missing") is None


class TestSMove:
    def test_smove(self, r):
        r.sadd("src", "a", "b")
        r.sadd("dst", "c")
        assert r.smove("src", "dst", "a") is True
        assert not r.sismember("src", "a")
        assert r.sismember("dst", "a")

    def test_smove_missing_member(self, r):
        r.sadd("src", "a")
        assert r.smove("src", "dst", "x") is False

    def test_smove_to_empty_dst(self, r):
        r.sadd("src", "x")
        assert r.smove("src", "dst", "x") is True
        assert r.scard("src") == 0
        assert r.smembers("dst") == {"x"}

    def test_smove_member_already_in_dst(self, r):
        """SMOVE removes from src even if member already in dst."""
        r.sadd("src", "x")
        r.sadd("dst", "x")
        assert r.smove("src", "dst", "x") is True
        assert r.scard("src") == 0
        assert r.scard("dst") == 1  # no duplicate

    def test_smove_missing_src(self, r):
        assert r.smove("missing", "dst", "x") is False


class TestSetAlgebra:
    def test_sdiff(self, r):
        r.sadd("s1", "a", "b", "c")
        r.sadd("s2", "b", "c", "d")
        assert r.sdiff("s1", "s2") == {"a"}

    def test_sdiffstore(self, r):
        r.sadd("s1", "a", "b")
        r.sadd("s2", "b")
        assert r.sdiffstore("dst", "s1", "s2") == 1
        assert r.smembers("dst") == {"a"}

    def test_sinter(self, r):
        r.sadd("s1", "a", "b", "c")
        r.sadd("s2", "b", "c", "d")
        assert r.sinter("s1", "s2") == {"b", "c"}

    def test_sinterstore(self, r):
        r.sadd("s1", "a", "b")
        r.sadd("s2", "b", "c")
        assert r.sinterstore("dst", "s1", "s2") == 1
        assert r.smembers("dst") == {"b"}

    def test_sintercard(self, r):
        r.sadd("s1", "a", "b", "c")
        r.sadd("s2", "b", "c", "d")
        assert r.sintercard(2, ["s1", "s2"]) == 2

    def test_sintercard_limit(self, r):
        r.sadd("s1", "a", "b", "c")
        r.sadd("s2", "a", "b", "c")
        assert r.sintercard(2, ["s1", "s2"], limit=2) == 2

    def test_sintercard_zero_limit_no_cap(self, r):
        """limit=0 means no limit — returns full intersection count."""
        r.sadd("s1", "a", "b", "c")
        r.sadd("s2", "a", "b", "c")
        assert r.sintercard(2, ["s1", "s2"], limit=0) == 3

    def test_sunion(self, r):
        r.sadd("s1", "a", "b")
        r.sadd("s2", "b", "c")
        assert r.sunion("s1", "s2") == {"a", "b", "c"}

    def test_sunionstore(self, r):
        r.sadd("s1", "a", "b")
        r.sadd("s2", "c")
        assert r.sunionstore("dst", "s1", "s2") == 3
        assert r.smembers("dst") == {"a", "b", "c"}

    def test_sdiff_empty_result(self, r):
        """SDIFF where s1 ⊂ s2 returns empty set."""
        r.sadd("s1", "a", "b")
        r.sadd("s2", "a", "b", "c")
        assert r.sdiff("s1", "s2") == set()

    def test_sdiff_with_missing_key(self, r):
        """Missing key treated as empty set."""
        r.sadd("s1", "a", "b")
        assert r.sdiff("s1", "missing") == {"a", "b"}

    def test_sinter_empty_result(self, r):
        r.sadd("s1", "a")
        r.sadd("s2", "b")
        assert r.sinter("s1", "s2") == set()

    def test_sinter_with_missing_key(self, r):
        """SINTER with a missing key always returns empty."""
        r.sadd("s1", "a")
        assert r.sinter("s1", "missing") == set()

    def test_sunion_with_missing_key(self, r):
        r.sadd("s1", "a")
        assert r.sunion("s1", "missing") == {"a"}

    def test_sdiffstore_overwrites_existing_dst(self, r):
        r.sadd("dst", "old_data")
        r.sadd("s1", "new_a")
        r.sadd("s2", "old_data")
        r.sdiffstore("dst", "s1", "s2")
        assert r.smembers("dst") == {"new_a"}

    def test_sunionstore_self_union(self, r):
        r.sadd("s", "a", "b")
        count = r.sunionstore("out", "s", "s")
        assert count == 2

    def test_algebra_three_sets(self, r):
        r.sadd("a", "1", "2", "3")
        r.sadd("b", "2", "3", "4")
        r.sadd("c", "3", "4", "5")
        # intersection of all three
        assert r.sinter("a", "b", "c") == {"3"}
        # union of all three
        assert r.sunion("a", "b", "c") == {"1", "2", "3", "4", "5"}


class TestSScan:
    def test_sscan_all(self, r):
        members = {f"m{i}" for i in range(30)}
        r.sadd("s", *members)
        seen = set()
        cursor = 0
        while True:
            cursor, partial = r.sscan("s", cursor)
            seen.update(partial)
            if cursor == 0:
                break
        assert seen == members

    def test_sscan_match(self, r):
        r.sadd("s", "apple", "banana", "avocado", "cherry")
        _, result = r.sscan("s", 0, match="a*")
        for m in result:
            assert m.startswith("a")

    def test_sscan_count_hint(self, r):
        members = {f"m{i}" for i in range(100)}
        r.sadd("s", *members)
        seen = set()
        cursor = 0
        while True:
            cursor, partial = r.sscan("s", cursor, count=20)
            seen.update(partial)
            if cursor == 0:
                break
        assert seen == members

    def test_sscan_missing_key(self, r):
        cursor, result = r.sscan("missing", 0)
        assert cursor == 0
        assert result == []


class TestSetConcurrency:
    def test_parallel_sadd_no_duplicate(self, r):
        """100 parallel SADDs of the same member should result in exactly 1 member."""
        fns = [lambda: make_client().sadd("s", "unique") for _ in range(100)]
        run_parallel(fns)
        assert r.scard("s") == 1

    def test_parallel_sadd_distinct_members(self, r):
        """100 parallel SADDs of distinct members → 100 members."""
        fns = [lambda i=i: make_client().sadd("s", f"m{i}") for i in range(100)]
        run_parallel(fns)
        assert r.scard("s") == 100


class TestSetRealWorld:
    def test_unique_visitors(self, r):
        r.sadd("visitors:2024-01-01", "user:1", "user:2", "user:3")
        r.sadd("visitors:2024-01-02", "user:2", "user:3", "user:4")

        new = r.sdiff("visitors:2024-01-02", "visitors:2024-01-01")
        assert new == {"user:4"}

        returning = r.sinter("visitors:2024-01-01", "visitors:2024-01-02")
        assert returning == {"user:2", "user:3"}

        total = r.sunion("visitors:2024-01-01", "visitors:2024-01-02")
        assert total == {"user:1", "user:2", "user:3", "user:4"}

    def test_social_following(self, r):
        """Social-graph pattern: mutual follows as set intersection."""
        r.sadd("following:alice", "bob", "carol", "dave")
        r.sadd("following:bob", "alice", "carol")

        mutual = r.sinter("following:alice", "following:bob")
        assert mutual == {"carol"}

        alice_exclusive = r.sdiff("following:alice", "following:bob")
        assert alice_exclusive == {"bob", "dave"}

    def test_tag_based_search(self, r):
        """Find articles matching multiple tags via set intersection."""
        r.sadd("tag:python", "article:1", "article:2", "article:3")
        r.sadd("tag:redis", "article:2", "article:3", "article:4")
        r.sadd("tag:database", "article:3", "article:5")

        both_py_redis = r.sinter("tag:python", "tag:redis")
        assert both_py_redis == {"article:2", "article:3"}

        all_three = r.sinter("tag:python", "tag:redis", "tag:database")
        assert all_three == {"article:3"}
