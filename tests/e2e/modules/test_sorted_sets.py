"""
Sorted set command tests — exhaustive coverage of all Redis sorted-set commands.

All tests define the ground-truth Redis behaviour that a replacement must match.
"""
import time
import threading
import math
import pytest
import redis
from utils.helpers import make_client, score_pairs, flat_triple


class TestZAdd:
    def test_zadd_basic(self, r):
        assert r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0}) == 3

    def test_zadd_update(self, r):
        r.zadd("z", {"a": 1.0})
        assert r.zadd("z", {"a": 5.0}) == 0
        assert r.zscore("z", "a") == 5.0

    def test_zadd_nx(self, r):
        r.zadd("z", {"a": 1.0})
        r.zadd("z", {"a": 99.0}, nx=True)
        assert r.zscore("z", "a") == 1.0

    def test_zadd_xx(self, r):
        r.zadd("z", {"a": 1.0})
        r.zadd("z", {"a": 99.0, "b": 2.0}, xx=True)
        assert r.zscore("z", "a") == 99.0
        assert r.zscore("z", "b") is None

    def test_zadd_gt(self, r):
        r.zadd("z", {"a": 5.0})
        r.zadd("z", {"a": 3.0}, gt=True)
        assert r.zscore("z", "a") == 5.0
        r.zadd("z", {"a": 10.0}, gt=True)
        assert r.zscore("z", "a") == 10.0

    def test_zadd_lt(self, r):
        r.zadd("z", {"a": 5.0})
        r.zadd("z", {"a": 10.0}, lt=True)
        assert r.zscore("z", "a") == 5.0
        r.zadd("z", {"a": 1.0}, lt=True)
        assert r.zscore("z", "a") == 1.0

    def test_zadd_incr(self, r):
        r.zadd("z", {"a": 1.0})
        new_score = r.zadd("z", {"a": 4.0}, incr=True)
        assert new_score == 5.0

    def test_zadd_ch(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0})
        changed = r.zadd("z", {"a": 9.0, "c": 3.0}, ch=True)
        assert changed == 2

    def test_wrongtype(self, r):
        r.set("k", "v")
        with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
            r.zadd("k", {"a": 1.0})

    def test_zadd_inf_score(self, r):
        """Members can have +inf and -inf scores."""
        r.zadd("z", {"min": float("-inf"), "max": float("+inf"), "mid": 0.0})
        assert r.zscore("z", "min") == float("-inf")
        assert r.zscore("z", "max") == float("+inf")
        # Order: min < mid < max
        assert r.zrange("z", 0, -1) == ["min", "mid", "max"]

    def test_zadd_returns_added_count_not_updated(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0})
        # Update a, add c — returns 1 (only c is new)
        count = r.zadd("z", {"a": 10.0, "c": 3.0})
        assert count == 1

    def test_zadd_nx_does_not_update(self, r):
        r.zadd("z", {"a": 5.0})
        r.zadd("z", {"a": 99.0, "b": 1.0}, nx=True)
        assert r.zscore("z", "a") == 5.0  # unchanged
        assert r.zscore("z", "b") == 1.0  # added

    def test_zadd_same_score_lex_order(self, r):
        """Members with equal scores are ordered lexicographically."""
        r.zadd("z", {"b": 0.0, "a": 0.0, "c": 0.0})
        assert r.zrange("z", 0, -1) == ["a", "b", "c"]


class TestZCardScore:
    def test_zcard(self, r):
        r.zadd("z", {"a": 1, "b": 2})
        assert r.zcard("z") == 2

    def test_zcard_missing(self, r):
        assert r.zcard("missing") == 0

    def test_zscore_existing(self, r):
        r.zadd("z", {"a": 3.14})
        assert r.zscore("z", "a") == pytest.approx(3.14)

    def test_zscore_missing(self, r):
        r.zadd("z", {"a": 1.0})
        assert r.zscore("z", "missing") is None

    def test_zmscore(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0})
        assert r.zmscore("z", ["a", "b", "c"]) == [1.0, 2.0, None]

    def test_zmscore_all_missing(self, r):
        r.zadd("z", {"x": 1.0})
        assert r.zmscore("z", ["a", "b"]) == [None, None]

    def test_zcard_after_zrem(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0})
        r.zrem("z", "a")
        assert r.zcard("z") == 1


class TestZRank:
    def test_zrank(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert r.zrank("z", "a") == 0
        assert r.zrank("z", "c") == 2

    def test_zrevrank(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert r.zrevrank("z", "c") == 0
        assert r.zrevrank("z", "a") == 2

    def test_zrank_missing(self, r):
        assert r.zrank("z", "x") is None

    def test_zrevrank_missing(self, r):
        r.zadd("z", {"a": 1.0})
        assert r.zrevrank("z", "x") is None

    def test_zrank_consistent_with_zrange(self, r):
        """ZRANK must be consistent with position in ZRANGE."""
        members = {"d": 4.0, "a": 1.0, "c": 3.0, "b": 2.0}
        r.zadd("z", members)
        ordered = r.zrange("z", 0, -1)
        for i, m in enumerate(ordered):
            assert r.zrank("z", m) == i


class TestZIncrBy:
    def test_zincrby(self, r):
        r.zadd("z", {"a": 1.0})
        assert r.zincrby("z", 4.0, "a") == pytest.approx(5.0)

    def test_zincrby_creates(self, r):
        assert r.zincrby("z", 7.0, "new") == pytest.approx(7.0)

    def test_zincrby_negative(self, r):
        r.zadd("z", {"a": 10.0})
        result = r.zincrby("z", -3.0, "a")
        assert result == pytest.approx(7.0)

    def test_zincrby_creates_key_if_missing(self, r):
        r.zincrby("newz", 5.0, "m")
        assert r.zscore("newz", "m") == 5.0

    def test_zincrby_preserves_ordering(self, r):
        r.zadd("z", {"a": 1.0, "b": 3.0})
        r.zincrby("z", 5.0, "a")
        # a now has score 6.0, b has 3.0 → order is b, a
        assert r.zrange("z", 0, -1) == ["b", "a"]


class TestZRem:
    def test_zrem(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0})
        assert r.zrem("z", "a") == 1
        assert r.zscore("z", "a") is None

    def test_zrem_multiple(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert r.zrem("z", "a", "c") == 2

    def test_zrem_missing_member(self, r):
        r.zadd("z", {"a": 1.0})
        assert r.zrem("z", "nosuch") == 0

    def test_zrem_all_empties_zset(self, r):
        r.zadd("z", {"a": 1.0})
        r.zrem("z", "a")
        assert r.zcard("z") == 0
        assert r.zrange("z", 0, -1) == []


class TestZCount:
    def test_zcount(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
        assert r.zcount("z", 2, 3) == 2
        assert r.zcount("z", "-inf", "+inf") == 4

    def test_zcount_exclusive(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert r.zcount("z", "(1", "3") == 2

    def test_zcount_exclusive_upper(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert r.zcount("z", "1", "(3") == 2

    def test_zcount_both_exclusive(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert r.zcount("z", "(1", "(3") == 1

    def test_zcount_empty_range(self, r):
        r.zadd("z", {"a": 1.0})
        assert r.zcount("z", "5", "10") == 0

    def test_zlexcount(self, r):
        r.zadd("z", {"a": 0.0, "b": 0.0, "c": 0.0})
        assert r.zlexcount("z", "-", "+") == 3
        assert r.zlexcount("z", "[a", "[b") == 2

    def test_zlexcount_exclusive(self, r):
        r.zadd("z", {"a": 0.0, "b": 0.0, "c": 0.0, "d": 0.0})
        assert r.zlexcount("z", "(a", "[c") == 2  # b, c


class TestZRange:
    def test_zrange_by_index(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert r.zrange("z", 0, -1) == ["a", "b", "c"]

    def test_zrange_withscores(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0})
        result = r.zrange("z", 0, -1, withscores=True)
        assert score_pairs(result) == [("a", 1.0), ("b", 2.0)]

    def test_zrange_rev(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert r.zrevrange("z", 0, -1) == ["c", "b", "a"]

    def test_zrangebyscore(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert r.zrangebyscore("z", 1, 2) == ["a", "b"]

    def test_zrangebyscore_exclusive(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert r.zrangebyscore("z", "(1", 3) == ["b", "c"]

    def test_zrangebyscore_limit(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
        result = r.zrangebyscore("z", 1, 4, start=1, num=2)
        assert result == ["b", "c"]

    def test_zrevrangebyscore(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert r.zrevrangebyscore("z", 3, 1) == ["c", "b", "a"]

    def test_zrevrangebyscore_exclusive(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert r.zrevrangebyscore("z", "(3", 1) == ["b", "a"]

    def test_zrevrangebyscore_limit(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
        result = r.zrevrangebyscore("z", 4, 1, start=1, num=2)
        assert result == ["c", "b"]

    def test_zrangebylex(self, r):
        r.zadd("z", {"a": 0.0, "b": 0.0, "c": 0.0, "d": 0.0})
        assert r.zrangebylex("z", "[a", "[c") == ["a", "b", "c"]

    def test_zrangebylex_exclusive(self, r):
        r.zadd("z", {"a": 0.0, "b": 0.0, "c": 0.0, "d": 0.0})
        assert r.zrangebylex("z", "(a", "(d") == ["b", "c"]

    def test_zrangebylex_all(self, r):
        r.zadd("z", {"a": 0.0, "b": 0.0, "c": 0.0})
        assert r.zrangebylex("z", "-", "+") == ["a", "b", "c"]

    def test_zrevrangebylex(self, r):
        r.zadd("z", {"a": 0.0, "b": 0.0, "c": 0.0, "d": 0.0})
        result = r.zrevrangebylex("z", "+", "-")
        assert result == ["d", "c", "b", "a"]

    def test_zrevrange(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert r.zrevrange("z", 0, -1) == ["c", "b", "a"]

    def test_zrangestore(self, r):
        r.zadd("src", {"a": 1.0, "b": 2.0, "c": 3.0})
        n = r.zrangestore("dst", "src", 0, 1)
        assert n == 2
        assert r.zrange("dst", 0, -1) == ["a", "b"]

    def test_zrange_empty_set(self, r):
        assert r.zrange("missing", 0, -1) == []

    def test_zrevrange_empty_set(self, r):
        assert r.zrevrange("missing", 0, -1) == []


class TestZRemRange:
    def test_zremrangebyrank(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
        removed = r.zremrangebyrank("z", 0, 1)
        assert removed == 2
        assert r.zrange("z", 0, -1) == ["c", "d"]

    def test_zremrangebyrank_all(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0})
        removed = r.zremrangebyrank("z", 0, -1)
        assert removed == 2
        assert r.zcard("z") == 0

    def test_zremrangebyscore(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
        removed = r.zremrangebyscore("z", 2, 3)
        assert removed == 2
        assert r.zrange("z", 0, -1) == ["a", "d"]

    def test_zremrangebyscore_exclusive(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        removed = r.zremrangebyscore("z", "(1", "(3")
        assert removed == 1
        assert r.zscore("z", "b") is None
        assert r.zscore("z", "a") == 1.0
        assert r.zscore("z", "c") == 3.0

    def test_zremrangebylex(self, r):
        r.zadd("z", {"a": 0.0, "b": 0.0, "c": 0.0, "d": 0.0})
        removed = r.zremrangebylex("z", "[b", "[c")
        assert removed == 2
        assert r.zrange("z", 0, -1) == ["a", "d"]

    def test_zremrangebyrank_negative(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        removed = r.zremrangebyrank("z", -2, -1)
        assert removed == 2
        assert r.zrange("z", 0, -1) == ["a"]


class TestZPop:
    def test_zpopmin(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = r.zpopmin("z")
        assert score_pairs(result) == [("a", 1.0)]

    def test_zpopmax(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = r.zpopmax("z")
        assert score_pairs(result) == [("c", 3.0)]

    def test_zpopmin_count(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = score_pairs(r.zpopmin("z", 2))
        assert len(result) == 2
        assert result[0] == ("a", 1.0)
        assert result[1] == ("b", 2.0)

    def test_zpopmax_count(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = score_pairs(r.zpopmax("z", 2))
        assert len(result) == 2
        assert result[0] == ("c", 3.0)
        assert result[1] == ("b", 2.0)

    def test_zpopmin_empty(self, r):
        assert r.zpopmin("missing") == []

    def test_zpopmax_empty(self, r):
        assert r.zpopmax("missing") == []

    def test_zmpop(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0})
        result = r.zmpop(1, ["z"], min=True)
        assert result is not None
        key, members = result
        assert key == "z"
        assert len(members) == 1
        assert members[0][0] == "a"
        assert float(members[0][1]) == pytest.approx(1.0)

    def test_bzpopmin_immediate(self, r):
        r.zadd("z", {"a": 1.0})
        result = r.bzpopmin("z", timeout=1)
        assert flat_triple(result) == ("z", "a", 1.0)

    def test_bzpopmax_immediate(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0})
        result = r.bzpopmax("z", timeout=1)
        assert flat_triple(result) == ("z", "b", 2.0)

    def test_bzpopmin_timeout(self, r):
        result = r.bzpopmin("empty", timeout=0.2)
        assert result is None

    def test_bzpopmax_timeout(self, r):
        result = r.bzpopmax("empty", timeout=0.2)
        assert result is None

    def test_bzpopmin_wakeup(self, r):
        """BZPOPMIN blocks then wakes when a member is added."""
        received = []

        def blocker():
            c = make_client()
            received.append(c.bzpopmin("bz_wake", timeout=3))

        t = threading.Thread(target=blocker)
        t.start()
        time.sleep(0.3)
        r.zadd("bz_wake", {"item": 1.0})
        t.join(timeout=4)
        assert [flat_triple(x) for x in received] == [("bz_wake", "item", 1.0)]


class TestZRandMember:
    def test_zrandmember(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        m = r.zrandmember("z")
        assert m in ["a", "b", "c"]

    def test_zrandmember_count(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = r.zrandmember("z", count=2)
        assert len(result) == 2
        assert len(set(result)) == 2

    def test_zrandmember_negative_count(self, r):
        r.zadd("z", {"only": 1.0})
        result = r.zrandmember("z", count=-5)
        assert len(result) == 5
        assert all(m == "only" for m in result)

    def test_zrandmember_withscores(self, r):
        r.zadd("z", {"a": 1.0})
        result = score_pairs(r.zrandmember("z", count=1, withscores=True))
        assert len(result) == 1
        assert result[0][0] == "a"
        assert result[0][1] == pytest.approx(1.0)

    def test_zrandmember_count_gt_len(self, r):
        r.zadd("z", {"a": 1.0, "b": 2.0})
        result = r.zrandmember("z", count=10)
        assert sorted(result) == ["a", "b"]

    def test_zrandmember_missing_key(self, r):
        assert r.zrandmember("missing") is None

    def test_zrandmember_count_on_missing_key(self, r):
        assert r.zrandmember("missing", count=3) == []


class TestZSetAlgebra:
    def test_zunionstore(self, r):
        r.zadd("z1", {"a": 1.0, "b": 2.0})
        r.zadd("z2", {"b": 3.0, "c": 4.0})
        assert r.zunionstore("out", ["z1", "z2"]) == 3
        assert r.zscore("out", "b") == 5.0

    def test_zinterstore(self, r):
        r.zadd("z1", {"a": 1.0, "b": 2.0})
        r.zadd("z2", {"b": 3.0, "c": 4.0})
        assert r.zinterstore("out", ["z1", "z2"]) == 1
        assert r.zscore("out", "b") == 5.0

    def test_zdiff(self, r):
        r.zadd("z1", {"a": 1.0, "b": 2.0})
        r.zadd("z2", {"b": 3.0})
        result = r.zdiff(["z1", "z2"])
        assert result == ["a"]

    def test_zdiffstore(self, r):
        r.zadd("z1", {"a": 1.0, "b": 2.0})
        r.zadd("z2", {"b": 3.0})
        assert r.zdiffstore("out", ["z1", "z2"]) == 1

    def test_zunionstore_weights(self, r):
        r.zadd("z1", {"a": 1.0})
        r.zadd("z2", {"a": 2.0})
        r.zunionstore("out", {"z1": 2, "z2": 3})
        # a: 1*2 + 2*3 = 8
        assert r.zscore("out", "a") == pytest.approx(8.0)

    def test_zinterstore_aggregate_min(self, r):
        r.zadd("z1", {"a": 5.0})
        r.zadd("z2", {"a": 2.0})
        r.zinterstore("out", ["z1", "z2"], aggregate="MIN")
        assert r.zscore("out", "a") == 2.0

    def test_zinterstore_aggregate_max(self, r):
        r.zadd("z1", {"a": 5.0})
        r.zadd("z2", {"a": 2.0})
        r.zinterstore("out", ["z1", "z2"], aggregate="MAX")
        assert r.zscore("out", "a") == 5.0

    def test_zunion_no_store(self, r):
        """ZUNION returns the union without storing (Redis 6.2+)."""
        r.zadd("z1", {"a": 1.0, "b": 2.0})
        r.zadd("z2", {"b": 3.0, "c": 4.0})
        result = r.zunion(["z1", "z2"])
        assert set(result) == {"a", "b", "c"}

    def test_zinter_no_store(self, r):
        """ZINTER returns the intersection without storing (Redis 6.2+)."""
        r.zadd("z1", {"a": 1.0, "b": 2.0})
        r.zadd("z2", {"b": 3.0, "c": 4.0})
        result = r.zinter(["z1", "z2"])
        assert result == ["b"]

    def test_zdiff_withscores(self, r):
        r.zadd("z1", {"a": 1.0, "b": 2.0})
        r.zadd("z2", {"b": 3.0})
        # redis-py hands back a flat [member, score, ...] list under RESP2 and
        # nested [member, score] pairs under RESP3; normalise and assert the data.
        result = score_pairs(r.zdiff(["z1", "z2"], withscores=True))
        assert len(result) == 1
        assert result[0][0] == "a"
        assert result[0][1] == pytest.approx(1.0)


class TestZScan:
    def test_zscan_full(self, r):
        members = {f"m{i}": float(i) for i in range(30)}
        r.zadd("z", members)
        seen = {}
        cursor = 0
        while True:
            cursor, partial = r.zscan("z", cursor)
            seen.update({m: s for m, s in partial})
            if cursor == 0:
                break
        assert len(seen) == 30

    def test_zscan_match(self, r):
        members = {f"apple_{i}": float(i) for i in range(5)}
        members.update({f"banana_{i}": float(i) for i in range(5)})
        r.zadd("z", members)
        seen = {}
        cursor = 0
        while True:
            cursor, partial = r.zscan("z", cursor, match="apple*")
            seen.update({m: s for m, s in partial})
            if cursor == 0:
                break
        assert len(seen) == 5
        for k in seen:
            assert k.startswith("apple")

    def test_zscan_missing_key(self, r):
        cursor, result = r.zscan("missing", 0)
        assert cursor == 0
        assert result == []


class TestZSetRealWorld:
    def test_leaderboard(self, r):
        players = {"alice": 1500, "bob": 2300, "carol": 1800, "dave": 2300}
        r.zadd("leaderboard", players)

        top3 = r.zrevrange("leaderboard", 0, 2, withscores=True)
        assert top3[0][1] == 2300

        alice_rank = r.zrevrank("leaderboard", "alice")
        assert alice_rank == 3

        r.zincrby("leaderboard", 1000, "alice")
        assert r.zscore("leaderboard", "alice") == 2500

        elite = r.zcount("leaderboard", 2000, "+inf")
        assert elite >= 2

    def test_rate_limiter_sliding_window(self, r):
        """Use ZADD + ZREMRANGEBYSCORE for a sliding-window rate limiter."""
        now_ms = int(time.time() * 1000)
        window_ms = 1000  # 1 second window

        # Simulate 5 events in the current window
        for i in range(5):
            r.zadd("ratelimit:user1", {f"event:{i}": now_ms + i})

        # Remove expired events
        r.zremrangebyscore("ratelimit:user1", "-inf", now_ms - window_ms)

        count = r.zcard("ratelimit:user1")
        assert count == 5  # all within window

    def test_priority_queue(self, r):
        """Use ZPOPMIN to implement a priority queue (lower score = higher priority)."""
        r.zadd("pq", {"high": 1.0, "medium": 5.0, "low": 10.0})
        first = score_pairs(r.zpopmin("pq"))
        assert first[0][0] == "high"
        second = score_pairs(r.zpopmin("pq"))
        assert second[0][0] == "medium"

    def test_time_series_range_query(self, r):
        """Store timestamps as scores for efficient range queries."""
        base_ts = 1_700_000_000
        for i in range(10):
            r.zadd("events", {f"event:{i}": base_ts + i * 60})

        # Get events between minute 2 and minute 7
        start_ts = base_ts + 2 * 60
        end_ts = base_ts + 7 * 60
        events = r.zrangebyscore("events", start_ts, end_ts)
        assert len(events) == 6  # events 2..7

    def test_lexicographic_autocomplete(self, r):
        """Use lex range on zero-score members for autocomplete."""
        words = ["apple", "application", "apply", "apt", "banana", "band"]
        r.zadd("vocab", {w: 0.0 for w in words})

        # Autocomplete prefix "app"
        results = r.zrangebylex("vocab", "[app", "[app\xff")
        assert "apple" in results
        assert "application" in results
        assert "apply" in results
        assert "banana" not in results
