"""
JSON command tests — JSON.SET/GET/DEL/TYPE/STRLEN/STRAPPEND/TOGGLE/NUMINCRBY/
NUMMULTBY/OBJKEYS/OBJLEN/ARRLEN/ARRAPPEND/ARRPOP/ARRINSERT/ARRTRIM/ARRINDEX/
JSON.CLEAR/JSON.MERGE.

TimeSeries tests — TS.CREATE/ADD/GET/MADD/DEL/RANGE/REVRANGE/MRANGE/INFO/
QUERYINDEX/CREATERULE/DELETERULE/ALTER/MGET.

Geo tests — GEOADD/GEODIST/GEOPOS/GEOHASH/GEOSEARCH/GEOSEARCHSTORE.
"""
import time
import pytest
import redis


# ──────────────────────────────────────────────────────────────────────────────
# JSON
# ──────────────────────────────────────────────────────────────────────────────

class TestJsonSetGet:
    def test_set_and_get_root(self, r):
        r.json().set("j", "$", {"name": "alice", "age": 30})
        result = r.json().get("j", "$")
        assert result == [{"name": "alice", "age": 30}]

    def test_get_field(self, r):
        r.json().set("j", "$", {"name": "bob", "score": 99})
        result = r.json().get("j", "$.name")
        assert result == ["bob"]

    def test_set_nested(self, r):
        r.json().set("j", "$", {"a": {"b": {"c": 42}}})
        result = r.json().get("j", "$.a.b.c")
        assert result == [42]

    def test_set_path_on_existing(self, r):
        r.json().set("j", "$", {"x": 1})
        r.json().set("j", "$.x", 99)
        assert r.json().get("j", "$.x") == [99]

    def test_del_field(self, r):
        r.json().set("j", "$", {"a": 1, "b": 2})
        r.json().delete("j", "$.a")
        result = r.json().get("j", "$")
        assert "a" not in result[0]

    def test_type(self, r):
        r.json().set("j", "$", {"arr": [1, 2], "num": 3, "str": "hi"})
        assert r.json().type("j", "$.arr") == ["array"]
        assert r.json().type("j", "$.num") == ["integer"]
        assert r.json().type("j", "$.str") == ["string"]

    def test_wrongtype(self, r):
        r.set("k", "string")
        with pytest.raises(redis.ResponseError):
            r.json().get("k")


class TestJsonString:
    def test_strlen(self, r):
        r.json().set("j", "$", {"s": "hello"})
        assert r.json().strlen("j", "$.s") == [5]

    def test_strappend(self, r):
        r.json().set("j", "$", {"s": "hello"})
        # redis-py json().strappend(name, value, path)
        # Internally calls json.JSONEncoder().encode(value), so pass a plain
        # Python str — redis-py wraps it in JSON quotes automatically.
        r.json().strappend("j", " world", "$.s")
        assert r.json().get("j", "$.s") == ["hello world"]


class TestJsonToggle:
    def test_toggle(self, r):
        r.json().set("j", "$", {"flag": True})
        r.json().toggle("j", "$.flag")
        assert r.json().get("j", "$.flag") == [False]
        r.json().toggle("j", "$.flag")
        assert r.json().get("j", "$.flag") == [True]


class TestJsonNumeric:
    def test_numincrby(self, r):
        r.json().set("j", "$", {"n": 10})
        result = r.json().numincrby("j", "$.n", 5)
        # Redis returns the new value; may be string or int depending on encoding
        assert int(result[0]) == 15

    def test_nummultby(self, r):
        r.json().set("j", "$", {"n": 3})
        result = r.json().nummultby("j", "$.n", 4)
        assert int(result[0]) == 12


class TestJsonObject:
    def test_objkeys(self, r):
        r.json().set("j", "$", {"a": 1, "b": 2, "c": 3})
        keys = r.json().objkeys("j", "$")
        assert sorted(keys[0]) == ["a", "b", "c"]

    def test_objlen(self, r):
        r.json().set("j", "$", {"a": 1, "b": 2})
        assert r.json().objlen("j", "$") == [2]


class TestJsonArray:
    def test_arrlen(self, r):
        r.json().set("j", "$", {"arr": [1, 2, 3]})
        assert r.json().arrlen("j", "$.arr") == [3]

    def test_arrappend(self, r):
        r.json().set("j", "$", {"arr": [1, 2]})
        r.json().arrappend("j", "$.arr", 3, 4)
        assert r.json().get("j", "$.arr") == [[1, 2, 3, 4]]

    def test_arrpop(self, r):
        r.json().set("j", "$", {"arr": [1, 2, 3]})
        val = r.json().arrpop("j", "$.arr")
        popped = val if isinstance(val, list) else [val]
        # Redis returns JSON-encoded strings; decode before comparing
        import json
        popped_decoded = [json.loads(v) if isinstance(v, str) else v for v in popped]
        assert popped_decoded == [3]
        assert r.json().get("j", "$.arr") == [[1, 2]]

    def test_arrinsert(self, r):
        r.json().set("j", "$", {"arr": [1, 3]})
        r.json().arrinsert("j", "$.arr", 1, 2)
        assert r.json().get("j", "$.arr") == [[1, 2, 3]]

    def test_arrtrim(self, r):
        r.json().set("j", "$", {"arr": [0, 1, 2, 3, 4]})
        r.json().arrtrim("j", "$.arr", 1, 3)
        assert r.json().get("j", "$.arr") == [[1, 2, 3]]

    def test_arrindex(self, r):
        r.json().set("j", "$", {"arr": [10, 20, 30, 20]})
        result = r.json().arrindex("j", "$.arr", 20)
        # With $ path: returns list of per-path results; redis-py may unwrap.
        idx = result if isinstance(result, list) else [result]
        assert idx == [1]


class TestJsonClear:
    def test_clear_array(self, r):
        r.json().set("j", "$", {"arr": [1, 2, 3]})
        r.json().clear("j", "$.arr")
        assert r.json().get("j", "$.arr") == [[]]

    def test_clear_number(self, r):
        r.json().set("j", "$", {"n": 42})
        r.json().clear("j", "$.n")
        assert r.json().get("j", "$.n") == [0]


class TestJsonMerge:
    def test_merge_adds_key(self, r):
        r.json().set("j", "$", {"a": 1})
        r.json().merge("j", "$", {"b": 2})
        result = r.json().get("j", "$")[0]
        assert result == {"a": 1, "b": 2}

    def test_merge_deletes_null_key(self, r):
        r.json().set("j", "$", {"a": 1, "b": 2})
        r.json().merge("j", "$", {"b": None})
        result = r.json().get("j", "$")[0]
        assert "b" not in result


# ──────────────────────────────────────────────────────────────────────────────
# TimeSeries helpers
# ──────────────────────────────────────────────────────────────────────────────

def _ts_info(r, key):
    """Return TS.INFO as a dict with numeric fields coerced to int."""
    raw = r.execute_command("TS.INFO", key)
    info = {raw[i]: raw[i + 1] for i in range(0, len(raw), 2)}
    for k in ("totalSamples", "memoryUsage", "firstTimestamp", "lastTimestamp",
              "retentionTime", "chunkSize"):
        if k in info:
            try:
                info[k] = int(info[k])
            except (ValueError, TypeError):
                pass
    return info


# ──────────────────────────────────────────────────────────────────────────────
# TimeSeries
# ──────────────────────────────────────────────────────────────────────────────

class TestTimeSeries:
    def test_create_and_add(self, r):
        r.execute_command("TS.CREATE", "ts1", "LABELS", "sensor", "temp")
        # Explicit timestamps 1ms apart to prevent duplicate-policy collapsing
        base = int(time.time() * 1000)
        r.execute_command("TS.ADD", "ts1", str(base),     "25.5")
        r.execute_command("TS.ADD", "ts1", str(base + 1), "26.0")
        info = _ts_info(r, "ts1")
        assert info["totalSamples"] == 2

    def test_ts_get(self, r):
        r.execute_command("TS.CREATE", "ts2")
        ts = int(time.time() * 1000)
        r.execute_command("TS.ADD", "ts2", str(ts), "42.0")
        result = r.execute_command("TS.GET", "ts2")
        assert len(result) == 2
        assert float(result[1]) == pytest.approx(42.0)

    def test_ts_range(self, r):
        r.execute_command("TS.CREATE", "ts3")
        base = int(time.time() * 1000)
        for i in range(5):
            r.execute_command("TS.ADD", "ts3", str(base + i * 1000), str(float(i)))
        result = r.execute_command("TS.RANGE", "ts3", str(base), str(base + 10000))
        assert len(result) == 5

    def test_ts_revrange(self, r):
        r.execute_command("TS.CREATE", "ts_rev")
        base = int(time.time() * 1000)
        for i in range(3):
            r.execute_command("TS.ADD", "ts_rev", str(base + i * 1000), str(float(i)))
        result = r.execute_command("TS.REVRANGE", "ts_rev", str(base), str(base + 10000))
        timestamps = [int(row[0]) for row in result]
        assert timestamps == sorted(timestamps, reverse=True)

    def test_ts_madd(self, r):
        r.execute_command("TS.CREATE", "ts4")
        base = int(time.time() * 1000)
        r.execute_command(
            "TS.MADD",
            "ts4", str(base),        "1.0",
            "ts4", str(base + 1000), "2.0",
        )
        result = r.execute_command("TS.RANGE", "ts4", "-", "+")
        assert len(result) == 2

    def test_ts_del(self, r):
        r.execute_command("TS.CREATE", "ts5")
        base = int(time.time() * 1000)
        for i in range(5):
            r.execute_command("TS.ADD", "ts5", str(base + i * 1000), str(float(i)))
        deleted = r.execute_command("TS.DEL", "ts5", str(base), str(base + 2000))
        assert deleted == 3

    def test_ts_queryindex(self, r):
        r.execute_command("TS.CREATE", "sensor:1", "LABELS", "type", "temp",     "room", "A")
        r.execute_command("TS.CREATE", "sensor:2", "LABELS", "type", "temp",     "room", "B")
        r.execute_command("TS.CREATE", "sensor:3", "LABELS", "type", "humidity", "room", "A")
        result = r.execute_command("TS.QUERYINDEX", "type=temp")
        assert set(result) == {"sensor:1", "sensor:2"}

    def test_ts_alter(self, r):
        r.execute_command("TS.CREATE", "ts_alt")
        r.execute_command("TS.ALTER", "ts_alt", "RETENTION", "5000")
        info = _ts_info(r, "ts_alt")
        assert info["retentionTime"] == 5000

    def test_ts_mget(self, r):
        r.execute_command("TS.CREATE", "mget:1", "LABELS", "region", "eu")
        r.execute_command("TS.CREATE", "mget:2", "LABELS", "region", "eu")
        r.execute_command("TS.ADD", "mget:1", "*", "10.0")
        r.execute_command("TS.ADD", "mget:2", "*", "20.0")
        result = r.execute_command("TS.MGET", "FILTER", "region=eu")
        assert len(result) == 2

    def test_ts_aggregation(self, r):
        r.execute_command("TS.CREATE", "ts_agg")
        base = int(time.time() * 1000) // 1000 * 1000  # align to second boundary
        for i in range(10):
            r.execute_command("TS.ADD", "ts_agg", str(base + i * 100), str(float(i)))
        result = r.execute_command(
            "TS.RANGE", "ts_agg", str(base), str(base + 2000),
            "AGGREGATION", "AVG", "1000"
        )
        assert len(result) >= 1

    def test_ts_compaction_rule(self, r):
        r.execute_command("TS.CREATE", "src_ts")
        r.execute_command("TS.CREATE", "dst_ts")
        r.execute_command("TS.CREATERULE", "src_ts", "dst_ts", "AGGREGATION", "AVG", "1000")
        info = _ts_info(r, "src_ts")
        assert "rules" in info

        r.execute_command("TS.DELETERULE", "src_ts", "dst_ts")
        info2 = _ts_info(r, "src_ts")
        assert len(info2.get("rules", [])) == 0


# ──────────────────────────────────────────────────────────────────────────────
# Geo
# ──────────────────────────────────────────────────────────────────────────────

class TestGeo:
    def _add_cities(self, r):
        # redis-py geoadd expects a flat list: [lon, lat, name, ...]
        r.geoadd("cities", [
            13.361389,  52.524722, "berlin",
             2.349014,  48.864716, "paris",
            -0.127758,  51.507351, "london",
            12.496366,  41.902783, "rome",
        ])

    def test_geoadd_geopos(self, r):
        self._add_cities(r)
        pos = r.geopos("cities", "berlin")
        assert len(pos) == 1
        lon, lat = pos[0]
        assert abs(lon - 13.361389) < 0.001
        assert abs(lat - 52.524722) < 0.001

    def test_geodist(self, r):
        self._add_cities(r)
        dist = r.geodist("cities", "berlin", "paris", "km")
        assert 870 <= dist <= 890

    def test_geohash(self, r):
        self._add_cities(r)
        hashes = r.geohash("cities", "berlin")
        assert len(hashes[0]) == 11

    def test_geosearch_radius(self, r):
        self._add_cities(r)
        results = r.geosearch(
            "cities", longitude=2.35, latitude=48.86,
            radius=500, unit="km", sort="ASC"
        )
        assert "paris" in results

    def test_geosearch_bybox(self, r):
        self._add_cities(r)
        results = r.geosearch(
            "cities", longitude=7.0, latitude=50.0,
            width=2000, height=1000, unit="km", sort="ASC"
        )
        assert isinstance(results, list)

    def test_geosearch_withdist(self, r):
        self._add_cities(r)
        results = r.geosearch(
            "cities", longitude=2.35, latitude=48.86,
            radius=1000, unit="km", sort="ASC", withdist=True
        )
        for item in results:
            assert len(item) == 2

    def test_geosearchstore(self, r):
        self._add_cities(r)
        n = r.geosearchstore(
            "nearby", "cities",
            longitude=2.35, latitude=48.86,
            radius=1000, unit="km", sort="ASC"
        )
        assert n >= 1
        assert r.type("nearby") == "zset"

    def test_geopos_missing(self, r):
        r.geoadd("g", [0.0, 0.0, "origin"])
        pos = r.geopos("g", "origin", "missing")
        assert pos[1] is None

    def test_geo_type(self, r):
        r.geoadd("g", [0.0, 0.0, "pt"])
        assert r.type("g") == "zset"