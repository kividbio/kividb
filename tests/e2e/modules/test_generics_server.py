"""
Generic key commands — TTL/EXPIRE/PERSIST/RENAME/TYPE/EXISTS/DEL/SCAN/OBJECT/KEYS/SORT
Server commands — PING/ECHO/SELECT/DBSIZE/FLUSHDB/INFO/CONFIG GET
Pub/Sub — SUBSCRIBE/PUBLISH/PSUBSCRIBE
"""
from __future__ import annotations
import time
import threading
import pytest
import redis
from utils.helpers import make_client


# ──────────────────────────────────────────────────────────────────────────────
# Generic key commands
# ──────────────────────────────────────────────────────────────────────────────

class TestExpiry:
    def test_expire_and_ttl(self, r):
        r.set("k", "v")
        r.expire("k", 10)
        ttl = r.ttl("k")
        assert 8 <= ttl <= 10

    def test_pexpire_and_pttl(self, r):
        r.set("k", "v")
        r.pexpire("k", 5000)
        pttl = r.pttl("k")
        assert 3000 <= pttl <= 5000

    def test_expireat(self, r):
        r.set("k", "v")
        future = int(time.time()) + 60
        r.expireat("k", future)
        assert r.ttl("k") > 50

    def test_persist(self, r):
        r.set("k", "v", ex=30)
        r.persist("k")
        assert r.ttl("k") == -1

    def test_ttl_no_expiry(self, r):
        r.set("k", "v")
        assert r.ttl("k") == -1

    def test_ttl_missing(self, r):
        assert r.ttl("missing") == -2

    def test_expiry_removes_key(self, r):
        r.set("k", "v", px=200)
        time.sleep(0.4)
        assert r.get("k") is None

    @pytest.mark.skip(reason="not yet implemented")
    def test_expiretime(self, r):
        r.set("k", "v")
        future = int(time.time()) + 60
        r.expireat("k", future)
        et = r.expiretime("k")
        assert abs(et - future) <= 1


class TestExists:
    def test_exists(self, r):
        r.set("a", "1")
        assert r.exists("a") == 1
        assert r.exists("missing") == 0

    def test_exists_multiple(self, r):
        r.set("a", "1")
        r.set("b", "2")
        assert r.exists("a", "b", "missing") == 2


class TestDel:
    def test_del_single(self, r):
        r.set("k", "v")
        assert r.delete("k") == 1
        assert r.get("k") is None

    def test_del_multiple(self, r):
        r.set("a", "1")
        r.set("b", "2")
        assert r.delete("a", "b", "c") == 2  # c doesn't exist

    def test_del_missing(self, r):
        assert r.delete("missing") == 0


class TestType:
    def test_string_type(self, r):
        r.set("k", "v")
        assert r.type("k") == "string"

    def test_list_type(self, r):
        r.rpush("l", "v")
        assert r.type("l") == "list"

    def test_hash_type(self, r):
        r.hset("h", "f", "v")
        assert r.type("h") == "hash"

    def test_set_type(self, r):
        r.sadd("s", "v")
        assert r.type("s") == "set"

    def test_zset_type(self, r):
        r.zadd("z", {"m": 1.0})
        assert r.type("z") == "zset"

    def test_missing_type(self, r):
        assert r.type("missing") == "none"


class TestRename:
    @pytest.mark.skip(reason="not yet implemented")
    def test_rename(self, r):
        r.set("src", "value")
        r.rename("src", "dst")
        assert r.get("dst") == "value"
        assert r.get("src") is None

    @pytest.mark.skip(reason="not yet implemented")
    def test_rename_overwrites(self, r):
        r.set("a", "1")
        r.set("b", "2")
        r.rename("a", "b")
        assert r.get("b") == "1"

    @pytest.mark.skip(reason="not yet implemented")
    def test_renamenx_success(self, r):
        r.set("src", "v")
        assert r.renamenx("src", "dst") == 1

    @pytest.mark.skip(reason="not yet implemented")
    def test_renamenx_fail(self, r):
        r.set("src", "v")
        r.set("dst", "existing")
        assert r.renamenx("src", "dst") == 0
        assert r.get("dst") == "existing"


class TestKeys:
    def test_keys_all(self, r):
        r.set("a", "1")
        r.set("b", "2")
        keys = r.keys("*")
        assert set(keys) == {"a", "b"}

    def test_keys_pattern(self, r):
        r.set("hello", "1")
        r.set("hallo", "1")
        r.set("world", "1")
        result = r.keys("h?llo")
        assert set(result) == {"hello", "hallo"}

    def test_keys_prefix(self, r):
        r.mset({"user:1": "a", "user:2": "b", "other": "c"})
        result = r.keys("user:*")
        assert set(result) == {"user:1", "user:2"}


class TestScan:
    def test_scan_all(self, r):
        for i in range(50):
            r.set(f"key{i}", i)
        seen = set()
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor)
            seen.update(keys)
            if cursor == 0:
                break
        assert len(seen) == 50

    def test_scan_match(self, r):
        r.set("foo:1", "a")
        r.set("foo:2", "b")
        r.set("bar:1", "c")
        seen = set()
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match="foo:*")
            seen.update(keys)
            if cursor == 0:
                break
        assert seen == {"foo:1", "foo:2"}

    def test_scan_count_hint(self, r):
        for i in range(100):
            r.set(f"k{i}", i)
        seen = set()
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, count=20)
            seen.update(keys)
            if cursor == 0:
                break
        assert len(seen) == 100

    @pytest.mark.skip(reason="not yet implemented")
    def test_scan_type_filter(self, r):
        r.set("s1", "v")
        r.set("s2", "v")
        r.rpush("l1", "v")
        seen = set()
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, _type="string")
            seen.update(keys)
            if cursor == 0:
                break
        assert "l1" not in seen
        assert "s1" in seen


class TestUnlink:
    @pytest.mark.skip(reason="not yet implemented")
    def test_unlink(self, r):
        r.set("k", "v")
        assert r.unlink("k") == 1
        assert r.get("k") is None


class TestObject:
    def test_object_encoding_string(self, r):
        r.set("k", "hello")
        enc = r.object("encoding", "k")
        assert enc is not None  # just check it doesn't error

    def test_object_idletime(self, r):
        r.set("k", "v")
        idle = r.object("idletime", "k")
        assert isinstance(idle, int)


# ──────────────────────────────────────────────────────────────────────────────
# Server commands
# ──────────────────────────────────────────────────────────────────────────────

class TestServer:
    def test_ping(self, r):
        assert r.ping() is True

    def test_echo(self, r):
        assert r.echo("hello") == "hello"

    def test_dbsize(self, r):
        r.set("a", "1")
        r.set("b", "2")
        assert r.dbsize() >= 2

    def test_flushdb(self, r):
        r.set("a", "1")
        r.flushdb()
        assert r.dbsize() == 0

    def test_select(self, r):
        # Write to DB 0
        r.set("k", "db0")
        # Switch to DB 1
        r.execute_command("SELECT", "1")
        assert r.get("k") is None  # not visible in DB 1
        r.execute_command("SELECT", "0")

    def test_info(self, r):
        info = r.info()
        assert isinstance(info, dict)

    def test_config_get(self, r):
        result = r.config_get("maxmemory")
        assert "maxmemory" in result

    def test_config_get_star(self, r):
        result = r.config_get("*")
        assert len(result) > 0

    def test_dbsize_after_flushdb(self, r):
        for i in range(10):
            r.set(f"k{i}", i)
        r.flushdb()
        assert r.dbsize() == 0

    @pytest.mark.skip(reason="not yet implemented")
    def test_client_id(self, r):
        cid = r.client_id()
        assert isinstance(cid, int)

    @pytest.mark.skip(reason="not yet implemented")
    def test_client_setname_getname(self, r):
        r.client_setname("test-conn")
        assert r.client_getname() == "test-conn"
    
    @pytest.mark.skip(reason="not yet implemented")
    def test_command_count(self, r):
        n = r.execute_command("COMMAND", "COUNT")
        assert n > 0


# ──────────────────────────────────────────────────────────────────────────────
# Pub/Sub
# ──────────────────────────────────────────────────────────────────────────────

class TestPubSub:
    def test_subscribe_publish(self, r):
        """A subscriber on a channel should receive a published message."""
        received = []
        ready = threading.Event()

        def subscriber():
            c = make_client()
            ps = c.pubsub()
            ps.subscribe("mychan")
            ready.set()
            for msg in ps.listen():
                if msg["type"] == "message":
                    received.append(msg["data"])
                    break
            ps.unsubscribe()

        t = threading.Thread(target=subscriber)
        t.start()
        ready.wait(timeout=3)
        time.sleep(0.1)  # give listen() time to block
        r.publish("mychan", "hello")
        t.join(timeout=5)
        assert received == ["hello"]

    def test_psubscribe(self, r):
        """Pattern subscribe should match wildcard channels."""
        received = []
        ready = threading.Event()

        def subscriber():
            c = make_client()
            ps = c.pubsub()
            ps.psubscribe("news.*")
            ready.set()
            for msg in ps.listen():
                if msg["type"] == "pmessage":
                    received.append(msg["data"])
                    break
            ps.punsubscribe()

        t = threading.Thread(target=subscriber)
        t.start()
        ready.wait(timeout=3)
        time.sleep(0.1)
        r.publish("news.sports", "goal!")
        t.join(timeout=5)
        assert received == ["goal!"]

    def test_publish_returns_subscriber_count(self, r):
        """PUBLISH returns the number of clients that received the message."""
        received_count = [0]
        ready = threading.Event()

        def subscriber():
            c = make_client()
            ps = c.pubsub()
            ps.subscribe("chan")
            ready.set()
            for msg in ps.listen():
                if msg["type"] == "message":
                    break

        t = threading.Thread(target=subscriber)
        t.start()
        ready.wait(timeout=3)
        time.sleep(0.1)
        n = r.publish("chan", "msg")
        received_count[0] = n
        t.join(timeout=3)
        assert received_count[0] == 1

    def test_no_subscribers(self, r):
        """Publishing to a channel with no subscribers returns 0."""
        assert r.publish("nobody_listening", "msg") == 0


# ──────────────────────────────────────────────────────────────────────────────
# HyperLogLog
# ──────────────────────────────────────────────────────────────────────────────

class TestHyperLogLog:
    def test_pfadd_pfcount(self, r):
        r.pfadd("hll", *[f"item{i}" for i in range(1000)])
        count = r.pfcount("hll")
        assert 900 <= count <= 1100  # ±10% tolerance

    def test_pfadd_returns_changed(self, r):
        assert r.pfadd("hll", "a") == 1
        assert r.pfadd("hll", "a") == 0  # no change

    def test_pfmerge(self, r):
        r.pfadd("h1", "a", "b", "c")
        r.pfadd("h2", "c", "d", "e")
        r.pfmerge("merged", "h1", "h2")
        count = r.pfcount("merged")
        assert count == 5  # union of {a,b,c,d,e}

    def test_pfcount_union(self, r):
        r.pfadd("h1", "a", "b")
        r.pfadd("h2", "b", "c")
        count = r.pfcount("h1", "h2")
        assert count == 3

# E2E tests for SORT BY <external-pattern> and GET <external-pattern>.
#
# Scenario: a leaderboard system where player IDs are stored in a set,
# scores in external hash keys, and SORT BY assembles the ranked view.

class TestSortByExternalStringKey:
    """SORT BY weight_* — sort by value of external string keys."""

    def test_basic_sort_by_string_key(self, r):
        r.rpush("items", "item3", "item1", "item2")
        r.set("weight_item1", "10")
        r.set("weight_item2", "5")
        r.set("weight_item3", "20")

        result = r.sort("items", by="weight_*")
        assert result == ["item2", "item1", "item3"]

    def test_sort_by_string_key_desc(self, r):
        r.rpush("ids", "a", "b", "c")
        r.set("score_a", "1")
        r.set("score_b", "3")
        r.set("score_c", "2")

        result = r.sort("ids", by="score_*", desc=True)
        assert result == ["b", "c", "a"]

    def test_missing_external_key_sorts_as_zero(self, r):
        """Missing weight keys default to 0.0 — same as Redis behaviour."""
        r.rpush("ids", "x", "y", "z")
        r.set("score_z", "5")
        # score_x and score_y don't exist → treated as 0.0

        result = r.sort("ids", by="score_*")
        # z(5) must be last; x and y (both 0) first two in any order
        assert result[2] == "z"
        assert set(result[:2]) == {"x", "y"}

    def test_sort_by_on_set(self, r):
        """BY external key works on sets too — not just lists."""
        r.sadd("members", "p1", "p2", "p3")
        r.set("rank_p1", "3")
        r.set("rank_p2", "1")
        r.set("rank_p3", "2")

        result = r.sort("members", by="rank_*")
        assert result == ["p2", "p3", "p1"]


class TestSortByHashField:
    """SORT BY hash_*->field — sort by a field inside external hash keys."""

    def test_sort_by_hash_field_asc(self, r):
        r.rpush("users", "u1", "u2", "u3")
        r.hset("user_u1", mapping={"age": "30"})
        r.hset("user_u2", mapping={"age": "25"})
        r.hset("user_u3", mapping={"age": "35"})

        result = r.sort("users", by="user_*->age")
        assert result == ["u2", "u1", "u3"]

    def test_sort_by_hash_field_desc(self, r):
        r.rpush("users", "u1", "u2", "u3")
        r.hset("user_u1", mapping={"score": "100"})
        r.hset("user_u2", mapping={"score": "200"})
        r.hset("user_u3", mapping={"score": "150"})

        result = r.sort("users", by="user_*->score", desc=True)
        assert result == ["u2", "u3", "u1"]

    def test_sort_by_hash_missing_field_sorts_as_zero(self, r):
        """Hash exists but field is absent → 0.0 sort key."""
        r.rpush("ids", "a", "b", "c")
        r.hset("obj_a", mapping={"weight": "10"})
        r.hset("obj_b", mapping={"weight": "5"})
        r.hset("obj_c", mapping={"other": "99"})  # no "weight" field

        result = r.sort("ids", by="obj_*->weight")
        # c(0) < b(5) < a(10)
        assert result[0] == "c"
        assert result[1] == "b"
        assert result[2] == "a"


class TestSortGetExternalPattern:
    """GET <pattern> — return external values instead of elements."""

    def test_get_external_string(self, r):
        r.rpush("ids", "3", "1", "2")
        r.set("name_1", "alice")
        r.set("name_2", "bob")
        r.set("name_3", "carol")

        # Sort numerically (1, 2, 3) then GET name for each
        result = r.sort("ids", get="name_*")
        assert result == ["alice", "bob", "carol"]

    def test_get_hash_field(self, r):
        r.rpush("ids", "3", "1", "2")
        r.hset("obj_1", mapping={"name": "alice", "city": "NYC"})
        r.hset("obj_2", mapping={"name": "bob",   "city": "LA"})
        r.hset("obj_3", mapping={"name": "carol", "city": "SF"})

        result = r.sort("ids", get="obj_*->name")
        assert result == ["alice", "bob", "carol"]

    def test_get_missing_key_returns_none(self, r):
        """GET pattern for a missing key returns None (nil in RESP)."""
        r.rpush("ids", "1", "2")
        r.set("name_1", "alice")
        # name_2 doesn't exist

        result = r.sort("ids", get="name_*")
        assert result[0] == "alice"
        assert result[1] is None

    def test_get_hash_missing_field_returns_none(self, r):
        r.rpush("ids", "1", "2")
        r.hset("obj_1", mapping={"label": "A"})
        r.hset("obj_2", mapping={"other": "X"})  # no "label" field

        result = r.sort("ids", get="obj_*->label")
        assert result[0] == "A"
        assert result[1] is None


class TestSortByAndGetCombined:
    """BY + GET together — sort by one external key, return another."""

    def test_sort_by_hash_get_hash_field(self, r):
        r.rpush("users", "u1", "u2", "u3")
        r.hset("user_u1", mapping={"age": "30", "name": "alice"})
        r.hset("user_u2", mapping={"age": "25", "name": "bob"})
        r.hset("user_u3", mapping={"age": "35", "name": "carol"})

        # Sort by age, return name
        result = r.sort("users", by="user_*->age", get="user_*->name")
        # u2(25)→bob, u1(30)→alice, u3(35)→carol
        assert result == ["bob", "alice", "carol"]

    def test_sort_by_weight_get_label_with_limit(self, r):
        r.rpush("ids", "a", "b", "c", "d", "e")
        weights = {"a": 5, "b": 2, "c": 8, "d": 1, "e": 4}
        labels  = {"a": "A", "b": "B", "c": "C", "d": "D", "e": "E"}
        for k, v in weights.items():
            r.set(f"weight_{k}", str(v))
        for k, v in labels.items():
            r.set(f"label_{k}", v)

        # Sorted by weight: d(1), b(2), e(4), a(5), c(8)
        # LIMIT 1 3 → skip d, take b, e, a → labels B, E, A
        result = r.sort("ids", by="weight_*", get="label_*",
                        start=1, num=3)
        assert result == ["B", "E", "A"]

    def test_sort_by_and_get_with_store(self, r):
        """BY + GET + STORE: sorted names written to a list key."""
        r.rpush("users", "u1", "u2", "u3")
        r.hset("user_u1", mapping={"age": "30", "name": "alice"})
        r.hset("user_u2", mapping={"age": "25", "name": "bob"})
        r.hset("user_u3", mapping={"age": "35", "name": "carol"})

        count = r.sort("users", by="user_*->age",
                       get="user_*->name", store="sorted_names")
        assert count == 3

        result = r.lrange("sorted_names", 0, -1)
        assert result == ["bob", "alice", "carol"]


class TestSortByRegressions:
    """Ensure existing nosort and GET # behaviour is unchanged."""

    def test_nosort_preserves_insertion_order(self, r):
        r.rpush("items", "c", "a", "b")
        result = r.sort("items", by="nosort", alpha=True)
        assert result == ["c", "a", "b"]

    def test_get_hash_self_unchanged(self, r):
        r.rpush("nums", "3", "1", "2")
        result = r.sort("nums", get="#")
        assert result == ["1", "2", "3"]

    def test_plain_numeric_sort_unchanged(self, r):
        r.rpush("nums", "5", "2", "8", "1")
        assert r.sort("nums") == ["1", "2", "5", "8"]

    def test_plain_alpha_sort_unchanged(self, r):
        r.rpush("words", "banana", "apple", "cherry")
        assert r.sort("words", alpha=True) == ["apple", "banana", "cherry"]


class TestSortByRealisticLeaderboard:
    """
    Realistic scenario: a leaderboard where player IDs live in a set,
    scores in hash keys, and SORT BY assembles the ranked view.
    """

    def test_leaderboard_top_three(self, r):
        players = {
            "alice": 1450,
            "bob":   980,
            "carol": 2100,
            "dave":  750,
            "eve":   1800,
        }
        r.sadd("leaderboard", *players.keys())
        for name, score in players.items():
            r.hset(f"player_{name}", mapping={"score": str(score), "name": name})

        # Top 3 by score descending, return names
        top3 = r.sort("leaderboard",
                      by="player_*->score",
                      get="player_*->name",
                      desc=True,
                      start=0, num=3)

        assert top3 == ["carol", "eve", "alice"], \
            f"expected carol(2100), eve(1800), alice(1450), got {top3}"

    def test_leaderboard_with_store_for_caching(self, r):
        """
        Common pattern: compute the sorted leaderboard once and STORE it
        as a list for cheap repeated reads.
        """
        players = {"p1": 300, "p2": 100, "p3": 200}
        r.sadd("lb", *players.keys())
        for pid, score in players.items():
            r.hset(f"player_{pid}", mapping={"score": str(score)})

        r.sort("lb", by="player_*->score",
               get="player_*->score",
               desc=True,
               store="lb:cached")

        cached = r.lrange("lb:cached", 0, -1)
        assert cached == ["300", "200", "100"]

        # Cached result is a plain list — reads are O(n) range scans
        assert r.type("lb:cached") == "list"