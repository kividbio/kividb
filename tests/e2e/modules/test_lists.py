"""
List command tests — exhaustive coverage of all Redis list commands.

All tests define the ground-truth Redis behaviour that a replacement must match.
"""
import time
import threading
import pytest
import redis
from utils.helpers import make_client, flat_pair


class TestPushPop:
    def test_rpush_lpop(self, r):
        assert r.rpush("l", "a", "b", "c") == 3
        assert r.lpop("l") == "a"
        assert r.lpop("l") == "b"

    def test_lpush_rpop(self, r):
        assert r.lpush("l", "a", "b", "c") == 3
        # LPUSH prepends each in order: result is [c, b, a]
        assert r.rpop("l") == "a"

    def test_pop_empty(self, r):
        assert r.lpop("missing") is None
        assert r.rpop("missing") is None

    def test_lpop_count(self, r):
        r.rpush("l", "a", "b", "c", "d")
        assert r.lpop("l", 2) == ["a", "b"]

    def test_rpop_count(self, r):
        r.rpush("l", "a", "b", "c", "d")
        assert r.rpop("l", 2) == ["d", "c"]

    def test_wrongtype(self, r):
        r.set("k", "string")
        with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
            r.lpush("k", "v")

    def test_rpush_returns_total_length(self, r):
        assert r.rpush("l", "a") == 1
        assert r.rpush("l", "b", "c") == 3

    def test_lpush_prepends_in_order(self, r):
        """LPUSH 'a', 'b', 'c' → list is ['c', 'b', 'a']."""
        r.lpush("l", "a", "b", "c")
        assert r.lrange("l", 0, -1) == ["c", "b", "a"]

    def test_rpush_appends_in_order(self, r):
        r.rpush("l", "a", "b", "c")
        assert r.lrange("l", 0, -1) == ["a", "b", "c"]

    def test_lpop_count_beyond_length(self, r):
        """LPOP count > list length returns all items."""
        r.rpush("l", "a", "b")
        result = r.lpop("l", 10)
        assert result == ["a", "b"]

    def test_rpop_count_beyond_length(self, r):
        r.rpush("l", "a", "b")
        result = r.rpop("l", 10)
        assert result == ["b", "a"]

    def test_pop_deletes_key_when_empty(self, r):
        """Popping the last element leaves an empty list."""
        r.rpush("l", "only")
        r.lpop("l")
        assert r.llen("l") == 0

    def test_lpush_empty_value(self, r):
        r.lpush("l", "")
        assert r.lrange("l", 0, -1) == [""]

    def test_rpush_duplicate_values(self, r):
        r.rpush("l", "a", "a", "b", "a")
        assert r.lrange("l", 0, -1) == ["a", "a", "b", "a"]
        assert r.llen("l") == 4


class TestLLen:
    def test_llen(self, r):
        r.rpush("l", "a", "b", "c")
        assert r.llen("l") == 3

    def test_llen_empty(self, r):
        assert r.llen("missing") == 0

    def test_llen_decrements_on_pop(self, r):
        r.rpush("l", "a", "b", "c")
        r.lpop("l")
        assert r.llen("l") == 2

    def test_llen_wrong_type(self, r):
        r.set("k", "v")
        with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
            r.llen("k")


class TestLRange:
    def test_full_range(self, r):
        r.rpush("l", "a", "b", "c")
        assert r.lrange("l", 0, -1) == ["a", "b", "c"]

    def test_partial_range(self, r):
        r.rpush("l", "a", "b", "c", "d")
        assert r.lrange("l", 1, 2) == ["b", "c"]

    def test_negative_indices(self, r):
        r.rpush("l", "a", "b", "c")
        assert r.lrange("l", -2, -1) == ["b", "c"]

    def test_out_of_bounds(self, r):
        r.rpush("l", "a", "b")
        assert r.lrange("l", 0, 100) == ["a", "b"]

    def test_empty_list(self, r):
        assert r.lrange("missing", 0, -1) == []

    def test_start_gt_end_returns_empty(self, r):
        r.rpush("l", "a", "b", "c")
        assert r.lrange("l", 3, 1) == []

    def test_single_element(self, r):
        r.rpush("l", "only")
        assert r.lrange("l", 0, 0) == ["only"]
        assert r.lrange("l", -1, -1) == ["only"]


class TestLIndex:
    def test_lindex(self, r):
        r.rpush("l", "a", "b", "c")
        assert r.lindex("l", 0) == "a"
        assert r.lindex("l", -1) == "c"

    def test_lindex_out_of_bounds(self, r):
        r.rpush("l", "a")
        assert r.lindex("l", 5) is None

    def test_lindex_negative(self, r):
        r.rpush("l", "a", "b", "c")
        assert r.lindex("l", -1) == "c"
        assert r.lindex("l", -2) == "b"
        assert r.lindex("l", -3) == "a"

    def test_lindex_missing_key(self, r):
        assert r.lindex("missing", 0) is None


class TestLSet:
    def test_lset(self, r):
        r.rpush("l", "a", "b", "c")
        r.lset("l", 1, "X")
        assert r.lindex("l", 1) == "X"

    def test_lset_out_of_bounds(self, r):
        r.rpush("l", "a")
        with pytest.raises(redis.ResponseError):
            r.lset("l", 5, "X")

    def test_lset_negative_index(self, r):
        r.rpush("l", "a", "b", "c")
        r.lset("l", -1, "Z")
        assert r.lindex("l", -1) == "Z"

    def test_lset_missing_key_error(self, r):
        with pytest.raises(redis.ResponseError):
            r.lset("missing", 0, "X")


class TestLInsert:
    def test_insert_before(self, r):
        r.rpush("l", "a", "c")
        r.linsert("l", "BEFORE", "c", "b")
        assert r.lrange("l", 0, -1) == ["a", "b", "c"]

    def test_insert_after(self, r):
        r.rpush("l", "a", "b")
        r.linsert("l", "AFTER", "a", "x")
        assert r.lrange("l", 0, -1) == ["a", "x", "b"]

    def test_pivot_missing(self, r):
        r.rpush("l", "a", "b")
        assert r.linsert("l", "BEFORE", "z", "x") == -1

    def test_insert_missing_list_returns_zero(self, r):
        assert r.linsert("missing", "BEFORE", "x", "y") == 0

    def test_insert_updates_length(self, r):
        r.rpush("l", "a", "b")
        new_len = r.linsert("l", "AFTER", "a", "x")
        assert new_len == 3


class TestLPos:
    def test_lpos_found(self, r):
        r.rpush("l", "a", "b", "a", "c")
        assert r.lpos("l", "a") == 0

    def test_lpos_missing(self, r):
        r.rpush("l", "x")
        assert r.lpos("l", "y") is None

    def test_lpos_rank(self, r):
        """RANK 2 returns the second occurrence."""
        r.rpush("l", "a", "b", "a", "c", "a")
        assert r.lpos("l", "a", rank=2) == 2

    def test_lpos_rank_negative(self, r):
        """Negative RANK searches from the tail."""
        r.rpush("l", "a", "b", "a", "c")
        assert r.lpos("l", "a", rank=-1) == 2

    def test_lpos_count(self, r):
        """COUNT returns all matching positions."""
        r.rpush("l", "a", "b", "a", "c", "a")
        positions = r.lpos("l", "a", count=0)
        assert positions == [0, 2, 4]

    def test_lpos_count_limited(self, r):
        r.rpush("l", "a", "b", "a", "c", "a")
        positions = r.lpos("l", "a", count=2)
        assert positions == [0, 2]

    def test_lpos_maxlen(self, r):
        """MAXLEN limits how far into the list to scan."""
        r.rpush("l", "x", "x", "a", "x")
        result = r.lpos("l", "a", maxlen=2)
        assert result is None

    def test_lpos_missing_key(self, r):
        assert r.lpos("missing", "x") is None


class TestLRem:
    def test_lrem_from_head(self, r):
        r.rpush("l", "a", "b", "a", "a")
        assert r.lrem("l", 2, "a") == 2
        assert r.lrange("l", 0, -1) == ["b", "a"]

    def test_lrem_all(self, r):
        r.rpush("l", "x", "x", "x")
        assert r.lrem("l", 0, "x") == 3
        assert r.lrange("l", 0, -1) == []

    def test_lrem_from_tail(self, r):
        r.rpush("l", "a", "b", "a", "a")
        assert r.lrem("l", -1, "a") == 1
        assert r.lrange("l", 0, -1) == ["a", "b", "a"]

    def test_lrem_not_found(self, r):
        r.rpush("l", "a", "b")
        assert r.lrem("l", 1, "z") == 0

    def test_lrem_empty_after_removal(self, r):
        r.rpush("l", "x")
        r.lrem("l", 0, "x")
        assert r.llen("l") == 0
        assert r.lrange("l", 0, -1) == []

    def test_lrem_missing_list(self, r):
        assert r.lrem("missing", 1, "x") == 0


class TestLTrim:
    def test_ltrim(self, r):
        r.rpush("l", "a", "b", "c", "d")
        r.ltrim("l", 1, 2)
        assert r.lrange("l", 0, -1) == ["b", "c"]

    def test_ltrim_beyond_end(self, r):
        r.rpush("l", "a", "b")
        r.ltrim("l", 0, 100)
        assert r.lrange("l", 0, -1) == ["a", "b"]

    def test_ltrim_to_single_element(self, r):
        r.rpush("l", "a", "b", "c")
        r.ltrim("l", 0, 0)
        assert r.lrange("l", 0, -1) == ["a"]

    def test_ltrim_empty_range_empties_list(self, r):
        r.rpush("l", "a", "b", "c")
        r.ltrim("l", 5, 10)
        assert r.llen("l") == 0
        assert r.lrange("l", 0, -1) == []

    def test_ltrim_negative_indices(self, r):
        r.rpush("l", "a", "b", "c", "d")
        r.ltrim("l", -2, -1)
        assert r.lrange("l", 0, -1) == ["c", "d"]


class TestLMove:
    def test_left_right(self, r):
        r.rpush("src", "a", "b", "c")
        val = r.lmove("src", "dst", "LEFT", "RIGHT")
        assert val == "a"
        assert r.lrange("src", 0, -1) == ["b", "c"]
        assert r.lrange("dst", 0, -1) == ["a"]

    def test_right_left(self, r):
        r.rpush("src", "a", "b", "c")
        val = r.lmove("src", "dst", "RIGHT", "LEFT")
        assert val == "c"
        assert r.lrange("dst", 0, -1) == ["c"]

    def test_rotate(self, r):
        """LMOVE src src LEFT RIGHT rotates the list."""
        r.rpush("l", "a", "b", "c")
        r.lmove("l", "l", "LEFT", "RIGHT")
        assert r.lrange("l", 0, -1) == ["b", "c", "a"]

    def test_lmove_missing_src_returns_none(self, r):
        result = r.lmove("missing_src", "dst", "LEFT", "RIGHT")
        assert result is None

    def test_lmove_creates_dst_if_missing(self, r):
        r.rpush("src", "a", "b")
        r.lmove("src", "newdst", "LEFT", "RIGHT")
        assert r.exists("newdst") == 1

    def test_lmove_right_right(self, r):
        r.rpush("src", "a", "b", "c")
        val = r.lmove("src", "dst", "RIGHT", "RIGHT")
        assert val == "c"
        assert r.lrange("dst", 0, -1) == ["c"]

    def test_lmove_left_left(self, r):
        r.rpush("src", "a", "b", "c")
        val = r.lmove("src", "dst", "LEFT", "LEFT")
        assert val == "a"
        assert r.lrange("dst", 0, -1) == ["a"]


class TestLMPop:
    def test_lmpop_left(self, r):
        r.rpush("l", "a", "b", "c")
        result = r.lmpop(1, "l", direction="LEFT")
        assert result is not None
        key, items = result
        assert key == "l"
        assert items == ["a"]

    def test_lmpop_count(self, r):
        r.rpush("l", "a", "b", "c")
        result = r.lmpop(1, "l", direction="LEFT", count=2)
        _, items = result
        assert items == ["a", "b"]

    def test_lmpop_first_nonempty(self, r):
        r.rpush("b", "x")
        result = r.lmpop(2, "empty", "b", direction="LEFT")
        assert result[0] == "b"

    def test_lmpop_all_empty_returns_none(self, r):
        result = r.lmpop(2, "empty1", "empty2", direction="LEFT")
        assert result is None

    def test_lmpop_right(self, r):
        r.rpush("l", "a", "b", "c")
        result = r.lmpop(1, "l", direction="RIGHT")
        _, items = result
        assert items == ["c"]

    def test_lmpop_drains_list(self, r):
        r.rpush("l", "a", "b")
        r.lmpop(1, "l", direction="LEFT", count=2)
        assert r.llen("l") == 0
        assert r.lrange("l", 0, -1) == []

    def test_lmpop_count_beyond_length(self, r):
        r.rpush("l", "a", "b")
        result = r.lmpop(1, "l", direction="LEFT", count=10)
        _, items = result
        assert items == ["a", "b"]


class TestBlocking:
    def test_blpop_immediate(self, r):
        r.rpush("l", "val")
        result = r.blpop("l", timeout=1)
        assert flat_pair(result) == ("l", "val")

    def test_blpop_timeout(self, r):
        result = r.blpop("missing", timeout=0.2)
        assert result is None

    def test_blpop_wakeup(self, r):
        """A blocked BLPOP should wake when another client pushes."""
        results = []

        def blocker():
            c = make_client()
            results.append(c.blpop("wakeup_list", timeout=3))

        t = threading.Thread(target=blocker)
        t.start()
        time.sleep(0.2)
        r.rpush("wakeup_list", "hello")
        t.join(timeout=4)
        assert [flat_pair(x) for x in results] == [("wakeup_list", "hello")]

    def test_brpop_wakeup(self, r):
        results = []

        def blocker():
            c = make_client()
            results.append(c.brpop("wake_r", timeout=3))

        t = threading.Thread(target=blocker)
        t.start()
        time.sleep(0.2)
        r.rpush("wake_r", "world")
        t.join(timeout=4)
        assert [flat_pair(x) for x in results] == [("wake_r", "world")]

    def test_blpop_multiple_keys_first_non_empty(self, r):
        """BLPOP on multiple keys returns from the first non-empty one."""
        r.rpush("b", "found")
        result = r.blpop(["a", "b", "c"], timeout=1)
        assert flat_pair(result) == ("b", "found")

    def test_blpop_priority_left_to_right(self, r):
        """When multiple keys have data, BLPOP picks the leftmost."""
        r.rpush("key1", "first")
        r.rpush("key2", "second")
        result = r.blpop(["key1", "key2"], timeout=1)
        assert flat_pair(result) == ("key1", "first")

    def test_brpop_immediate(self, r):
        r.rpush("l", "val")
        result = r.brpop("l", timeout=1)
        assert flat_pair(result) == ("l", "val")


class TestRPopLPush:
    def test_rpoplpush(self, r):
        r.rpush("src", "a", "b")
        val = r.rpoplpush("src", "dst")
        assert val == "b"
        assert r.lrange("dst", 0, -1) == ["b"]

    def test_rpoplpush_self_rotate(self, r):
        """RPOPLPUSH src src rotates the list."""
        r.rpush("l", "a", "b", "c")
        r.rpoplpush("l", "l")
        assert r.lrange("l", 0, -1) == ["c", "a", "b"]

    def test_rpoplpush_missing_src_returns_none(self, r):
        assert r.rpoplpush("missing", "dst") is None

    def test_rpoplpush_creates_dst(self, r):
        r.rpush("src", "a")
        r.rpoplpush("src", "newdst")
        assert r.type("newdst") == "list"


class TestListRealWorld:
    def test_queue_fifo(self, r):
        """RPUSH + LPOP implements a FIFO queue."""
        for task in ["task1", "task2", "task3"]:
            r.rpush("queue", task)
        assert r.lpop("queue") == "task1"
        assert r.lpop("queue") == "task2"
        assert r.lpop("queue") == "task3"
        assert r.lpop("queue") is None

    def test_stack_lifo(self, r):
        """RPUSH + RPOP implements a LIFO stack."""
        for item in ["a", "b", "c"]:
            r.rpush("stack", item)
        assert r.rpop("stack") == "c"
        assert r.rpop("stack") == "b"

    def test_bounded_log(self, r):
        """Use RPUSH + LTRIM to keep only the last N log entries."""
        max_entries = 3
        for i in range(10):
            r.rpush("log", f"entry:{i}")
            r.ltrim("log", -max_entries, -1)
        entries = r.lrange("log", 0, -1)
        assert len(entries) == max_entries
        assert entries == ["entry:7", "entry:8", "entry:9"]

    def test_deque_operations(self, r):
        """A deque supports push/pop from both ends."""
        r.rpush("deque", "middle")
        r.lpush("deque", "left")
        r.rpush("deque", "right")
        assert r.lrange("deque", 0, -1) == ["left", "middle", "right"]
        assert r.lpop("deque") == "left"
        assert r.rpop("deque") == "right"
