#!/usr/bin/env python3
"""
RedisVL <-> Redis-protocol server compatibility harness.

Runs RedisVL against any server (KiviDB, Redis 8) and records PASS / FAIL per check. A
deterministic stub plays the embedding model, so no real API keys, model downloads, or spend
are involved.

  pip install -r requirements.txt
  python redisvl_kividb_compat.py --url redis://127.0.0.1:6380 --label kividb
  python redisvl_kividb_compat.py --url redis://127.0.0.1:6399 --label redis8

Writes results-<label>.json next to the script. Run the same command against stock Redis
(for example the `redis:8.10` image) with a different --label to get a comparison.

What it covers, from redisvl itself:
  * SearchIndex: create, load, and query a hash index (FLAT and HNSW) and a JSON index.
    VectorQuery, tag / numeric / text filters and a filter that combines them, FilterQuery,
    TextQuery, fetch, drop one document, and delete the index.
  * HybridQuery: Redis 8.4+ FT.HYBRID, LINEAR fusion weighted toward the vector and toward
    the text, RRF fusion, and a hybrid query with a tag filter. On a hash index and on a
    JSON index.
  * AggregateHybridQuery: the older hybrid path, which fuses a text score and a vector score
    inside FT.AGGREGATE.
  * SemanticCache: store, an exact hit, a close hit, a far miss, a tag filter on the cache,
    and clear.
  * SemanticRouter: an exact reference, a near statement, a far statement that matches
    nothing, route_many, and add_route.

Hash documents store the vector with array_to_buffer(); JSON documents store it as a list of
floats. That is what RedisVL 0.32 requires of each storage type. The same values are sent to
every server.

NOTE: the stub embedding is a hashed bag of words. It proves the wiring (index creation,
search, filters, fusion, cache hit and miss, routing), not the quality of a real model's
results.

Everything it creates has a random prefix and is removed at the end, including the indexes
the library creates. One that another client made on the same server meanwhile is left alone.
"""
import argparse
import hashlib
import json
import math
import os
import re
import sys
import time
import uuid
from importlib.metadata import version

import redis
from redisvl.extensions.cache.llm import SemanticCache
from redisvl.extensions.router import Route, SemanticRouter
from redisvl.index import SearchIndex
from redisvl.query import (
    AggregateHybridQuery,
    FilterQuery,
    HybridQuery,
    TextQuery,
    VectorQuery,
)
from redisvl.query.filter import Num, Tag, Text
from redisvl.redis.utils import array_to_buffer
from redisvl.utils.vectorize import CustomVectorizer

DIM = 64
results = []
PREFIXES = []  # every random prefix used, so its keys can be removed

RATES = "interest rates rose again this quarter"
CATS = "the cat sat on the warm mat"
RUST = "a new rust compiler release is out"
# No token in common with the corpus, so the cosine distance to every document is 1.
FAR = "vector databases index embeddings"

# (id, topic, year, content). Ids are not prefix characters, so a key prefix cannot cut them.
CORPUS = [
    ("cats", "animals", 2020, CATS),
    ("dogs", "animals", 2023, "the dog chased the ball in the park"),
    ("rates", "finance", 2021, RATES),
    ("rust", "tech", 2022, RUST),
]

LUA_COMMANDS = {
    "eval",
    "evalsha",
    "eval_ro",
    "evalsha_ro",
    "script",
    "fcall",
    "fcall_ro",
    "function",
}


# ---------------------------------------------------------------- stub embedding
def embed(text, **_kwargs):
    """Hashed bag of words, unit length: deterministic and offline."""
    vector = [0.0] * DIM
    for tok in re.findall(r"[a-z0-9]+", str(text).lower()):
        vector[int(hashlib.md5(tok.encode()).hexdigest(), 16) % DIM] += 1.0
    norm = math.sqrt(sum(x * x for x in vector)) or 1.0
    return [x / norm for x in vector]


VECTORIZER = CustomVectorizer(embed)


# ---------------------------------------------------------------- check runner
def check(name, fn):
    t0 = time.time()
    try:
        detail = fn()
        status = "PASS"
    except Exception as e:  # noqa
        detail, status = f"{type(e).__name__}: {str(e)[:400]}", "FAIL"
    results.append({
        "check": name,
        "status": status,
        "detail": detail or "",
        "ms": round((time.time() - t0) * 1000),
    })
    print(f"  [{status:4}] {name}" + (f"  ({detail})" if detail else ""))
    return status == "PASS"


def run_group(title, fn, *args):
    """Run a group of checks. If its setup raises (a server without the feature it needs),
    that is one FAIL line, and the next group still runs."""
    print(f"\n== {title}")
    try:
        return fn(*args)
    except Exception as e:  # noqa
        detail = f"{type(e).__name__}: {str(e)[:400]}"
        results.append({"check": f"{title}: setup", "status": "FAIL", "detail": detail, "ms": 0})
        print(f"  [FAIL] {title}: setup  ({detail})")
        return None


def unique(prefix):
    name = f"{prefix}_{uuid.uuid4().hex[:8]}"
    PREFIXES.append(name)
    return name


def indexes(client):
    """The names of the search indexes, or none if the server cannot list them."""
    try:
        return {i.decode() if isinstance(i, bytes) else i for i in client.execute_command("FT._LIST")}
    except Exception:  # noqa
        return set()


def cleanup(client, before):
    """Drop this run's indexes, with their documents, then any key that has one of this run's
    random names in it."""
    for idx in indexes(client) - before:
        if not any(prefix in idx for prefix in PREFIXES):
            continue
        try:
            client.execute_command("FT.DROPINDEX", idx, "DD")
        except Exception:  # noqa
            pass
    for prefix in PREFIXES:
        for key in client.scan_iter(match=f"*{prefix}*", count=500):
            client.delete(key)


def command_counts(client):
    """INFO commandstats as {command: calls}. Names are whatever the server reports."""
    stats = client.info("commandstats") or {}
    counts = {}
    for key, val in stats.items():
        name = key.decode() if isinstance(key, bytes) else str(key)
        name = name.removeprefix("cmdstat_")
        calls = val.get("calls", val.get(b"calls", 0)) if isinstance(val, dict) else 0
        counts[name] = int(calls or 0)
    return counts


def doc_ids(rows):
    return [row.get("doc_id") for row in rows]


def distance(row):
    return float(row["vector_distance"])


# ---------------------------------------------------------------- index schema
def make_schema(name, storage, algorithm):
    return {
        "index": {"name": name, "prefix": name, "storage_type": storage},
        "fields": [
            {"name": "doc_id", "type": "tag"},
            {"name": "topic", "type": "tag"},
            {"name": "year", "type": "numeric"},
            {"name": "content", "type": "text"},
            {"name": "embedding", "type": "vector", "attrs": {
                "dims": DIM,
                "algorithm": algorithm,
                "distance_metric": "cosine",
                "datatype": "float32",
            }},
        ],
    }


def documents(storage):
    """One row per corpus entry. Hash vectors are a float32 buffer; JSON vectors are a list."""
    rows = []
    for doc_id, topic, year, content in CORPUS:
        vector = embed(content)
        if storage == "hash":
            vector = array_to_buffer(vector, "float32")
        rows.append({
            "id": doc_id,
            "doc_id": doc_id,
            "topic": topic,
            "year": year,
            "content": content,
            "embedding": vector,
        })
    return rows


def open_index(url, storage, algorithm):
    name = unique(f"rvl{storage[0]}{algorithm[0]}")
    return SearchIndex.from_dict(make_schema(name, storage, algorithm), redis_url=url)


def drop_quiet(index):
    try:
        if index.exists():
            index.delete(drop=True)
    except Exception:  # noqa
        pass


def vector_query(index, text, k=4, **kwargs):
    return index.query(VectorQuery(
        vector=embed(text),
        vector_field_name="embedding",
        return_fields=["doc_id", "topic", "year", "content"],
        num_results=k,
        **kwargs,
    ))


def load_checks(index, storage, label):
    """Create and load. Returns False when that failed, so the queries are not asked of an
    index that is not there."""
    def created():
        index.create(overwrite=True)
        keys = index.load(documents(storage), id_field="id")
        assert index.exists(), "index does not exist after create"
        got = sorted(key.rsplit(":", 1)[-1] for key in keys)
        assert got == ["cats", "dogs", "rates", "rust"], keys

    return check(f"{label}: create and load", created)


def query_checks(index, label, include_text):
    def nearest():
        rows = vector_query(index, RATES)
        assert doc_ids(rows)[0] == "rates", doc_ids(rows)
        assert distance(rows[0]) < 1e-4, rows[0]["vector_distance"]

    def ordered():
        dists = [distance(row) for row in vector_query(index, RATES)]
        assert dists == sorted(dists), dists

    def tag_eq():
        got = sorted(doc_ids(vector_query(
            index, "x", 10, filter_expression=Tag("topic") == "animals")))
        assert got == ["cats", "dogs"], got

    def tag_ne():
        got = sorted(doc_ids(vector_query(
            index, "x", 10, filter_expression=Tag("topic") != "animals")))
        assert got == ["rates", "rust"], got

    def numeric():
        got = sorted(doc_ids(vector_query(
            index, "x", 10, filter_expression=Num("year") > 2021)))
        assert got == ["dogs", "rust"], got

    def numeric_range():
        filt = (Num("year") >= 2021) & (Num("year") <= 2021)
        got = sorted(doc_ids(vector_query(index, "x", 10, filter_expression=filt)))
        assert got == ["rates"], got

    def text_filter():
        got = doc_ids(vector_query(
            index, "x", 10, filter_expression=Text("content") == "rust compiler"))
        assert got == ["rust"], got

    def combined():
        filt = (Tag("topic") == "animals") & (Num("year") >= 2023)
        got = doc_ids(vector_query(index, "x", 10, filter_expression=filt))
        assert got == ["dogs"], got

    def filter_query():
        rows = index.query(FilterQuery(
            filter_expression=Tag("topic") == "finance",
            return_fields=["doc_id"],
        ))
        assert doc_ids(rows) == ["rates"], doc_ids(rows)

    check(f"{label}: vector query returns the closest document first", nearest)
    check(f"{label}: vector distances ascend", ordered)
    check(f"{label}: tag filter", tag_eq)
    check(f"{label}: tag filter excludes a value", tag_ne)
    check(f"{label}: numeric filter", numeric)
    check(f"{label}: numeric range filter", numeric_range)
    check(f"{label}: text phrase filter", text_filter)
    check(f"{label}: tag and numeric filter together", combined)
    check(f"{label}: filter query", filter_query)
    if include_text:
        def text_query():
            rows = index.query(TextQuery(
                text="rust compiler",
                text_field_name="content",
                return_fields=["doc_id"],
                num_results=4,
                stopwords=None,
            ))
            assert rows and rows[0]["doc_id"] == "rust", doc_ids(rows)
            assert float(rows[0]["score"]) > 0, rows[0].get("score")

        check(f"{label}: text query", text_query)


def search_index(url, storage, algorithm, include_text, fetch_year_type):
    index = open_index(url, storage, algorithm)
    label = f"SearchIndex {storage} {algorithm}"
    try:
        if not load_checks(index, storage, label):
            return
        query_checks(index, label, include_text)

        def fetched():
            got = index.fetch("rates")
            assert got and got.get("content") == RATES, got
            assert got.get("doc_id") == "rates", got
            if fetch_year_type:
                assert got.get("year") == 2021 and isinstance(got.get("year"), int), got.get("year")

        def dropped():
            assert index.drop_documents("cats") == 1
            assert not index.fetch("cats"), index.fetch("cats")
            got = sorted(doc_ids(vector_query(
                index, "x", 10, filter_expression=Tag("topic") == "animals")))
            assert got == ["dogs"], got

        def deleted():
            index.delete(drop=True)
            assert index.exists() is False

        check(f"{label}: fetch by id", fetched)
        check(f"{label}: drop a document", dropped)
        check(f"{label}: delete the index", deleted)
    finally:
        drop_quiet(index)


def hybrid_query(index, method, alpha=None, text="rust compiler", filt=None):
    kwargs = {
        "text": text,
        "text_field_name": "content",
        "vector": embed(RATES),
        "vector_field_name": "embedding",
        "return_fields": ["doc_id", "topic"],
        "num_results": 4,
        "combination_method": method,
        "stopwords": None,
    }
    if method == "LINEAR":
        kwargs["linear_alpha"] = alpha
    if filt is not None:
        kwargs["filter_expression"] = filt
    return index.query(HybridQuery(**kwargs))


def hybrid(url, storage):
    """FT.HYBRID. The text is 'rust compiler' and the vector is the rates document, so a
    text-heavy fusion ranks rust first and a vector-heavy fusion ranks rates first."""
    index = open_index(url, storage, "flat")
    label = f"HybridQuery {storage}"
    try:
        if not load_checks(index, storage, label):
            return

        def vector_heavy():
            rows = hybrid_query(index, "LINEAR", alpha=0.05)
            assert doc_ids(rows)[0] == "rates", doc_ids(rows)
            assert "rust" in doc_ids(rows), doc_ids(rows)

        def text_heavy():
            rows = hybrid_query(index, "LINEAR", alpha=0.95)
            assert doc_ids(rows)[0] == "rust", doc_ids(rows)
            assert "rates" in doc_ids(rows), doc_ids(rows)

        def rrf():
            got = doc_ids(hybrid_query(index, "RRF"))
            assert got[:2] == ["rust", "rates"], got

        def filtered():
            rows = hybrid_query(
                index, "RRF", text="compiler", filt=Tag("topic") == "tech")
            assert doc_ids(rows) == ["rust"], doc_ids(rows)

        check(f"{label}: LINEAR fusion weighted toward the vector", vector_heavy)
        check(f"{label}: LINEAR fusion weighted toward the text", text_heavy)
        check(f"{label}: RRF fusion", rrf)
        check(f"{label}: hybrid query with a tag filter", filtered)
    finally:
        drop_quiet(index)


def aggregate_hybrid(url):
    """FT.AGGREGATE hybrid. alpha is the vector weight, so 0.9 ranks rates first and 0.1 ranks
    rust (the text match) first."""
    index = open_index(url, "hash", "flat")
    label = "AggregateHybridQuery"
    try:
        if not load_checks(index, "hash", label):
            return

        def run(alpha):
            return index.query(AggregateHybridQuery(
                text="rust compiler",
                text_field_name="content",
                vector=embed(RATES),
                vector_field_name="embedding",
                return_fields=["doc_id"],
                num_results=4,
                alpha=alpha,
                stopwords=None,
            ))

        def text_heavy():
            assert doc_ids(run(0.1))[0] == "rust", doc_ids(run(0.1))

        def vector_heavy():
            assert doc_ids(run(0.9))[0] == "rates", doc_ids(run(0.9))

        check(f"{label}: vector weight 0.1 ranks the text match first", text_heavy)
        check(f"{label}: vector weight 0.9 ranks the vector match first", vector_heavy)
    finally:
        drop_quiet(index)


def semantic_cache(url):
    name = unique("rvlc")
    cache = SemanticCache(
        name=name,
        redis_url=url,
        distance_threshold=0.4,
        vectorizer=VECTORIZER,
        filterable_fields=[{"name": "user", "type": "tag"}],
        overwrite=True,
    )
    try:
        def stored():
            rates_key = cache.store(RATES, "rates answer", filters={"user": "alice"})
            cats_key = cache.store(CATS, "cat answer", filters={"user": "alice"})
            assert rates_key and cats_key and rates_key != cats_key, (rates_key, cats_key)

        def exact():
            hits = cache.check(RATES)
            assert hits and hits[0]["response"] == "rates answer", hits
            assert hits[0]["vector_distance"] < 1e-4, hits[0]["vector_distance"]

        def close():
            hits = cache.check("interest rates rose")
            assert hits and hits[0]["response"] == "rates answer", hits
            assert hits[0]["vector_distance"] < 0.4, hits[0]["vector_distance"]

        def far():
            assert cache.check(FAR) == [], cache.check(FAR)

        def other_user():
            hits = cache.check(RATES, filter_expression=Tag("user") == "bob")
            assert hits == [], hits

        def matching_user():
            hits = cache.check(RATES, filter_expression=Tag("user") == "alice")
            assert hits and hits[0]["response"] == "rates answer", hits

        def cleared():
            cache.clear()
            assert cache.check(RATES) == []
            assert cache.check(CATS) == []

        if not check("SemanticCache: store", stored):
            return
        check("SemanticCache: an exact prompt hits", exact)
        check("SemanticCache: a close prompt hits", close)
        check("SemanticCache: a far prompt misses", far)
        check("SemanticCache: a filter for another value misses", other_user)
        check("SemanticCache: a filter for the stored value hits", matching_user)
        check("SemanticCache: clear removes the entries", cleared)
    finally:
        try:
            cache.delete()
        except Exception:  # noqa
            pass


def semantic_router(url):
    name = unique("rvlr")
    router = SemanticRouter(
        name=name,
        routes=[
            Route(name="greeting", references=["hello there friend"], distance_threshold=0.5),
            Route(name="finance", references=["interest rates rose"], distance_threshold=0.5),
        ],
        vectorizer=VECTORIZER,
        redis_url=url,
        overwrite=True,
    )
    try:
        def exact():
            match = router("hello there friend")
            assert match.name == "greeting", (match.name, match.distance)
            assert match.distance < 1e-4, match.distance

        def near():
            match = router("interest rates rose again")
            assert match.name == "finance", (match.name, match.distance)
            assert match.distance < 0.5, match.distance

        def far():
            match = router(FAR)
            assert match.name is None, (match.name, match.distance)

        def many():
            matches = router.route_many("interest rates rose", max_k=2)
            assert matches and matches[0].name == "finance", [(m.name, m.distance) for m in matches]

        def added():
            router.add_route(Route(
                name="animals", references=[CATS], distance_threshold=0.5))
            match = router(CATS)
            assert match.name == "animals", (match.name, match.distance)
            assert match.distance < 1e-4, match.distance

        check("SemanticRouter: an exact reference routes to that route", exact)
        check("SemanticRouter: a near statement routes under the threshold", near)
        check("SemanticRouter: a far statement matches nothing", far)
        check("SemanticRouter: route_many returns the closest route", many)
        check("SemanticRouter: a route added later matches", added)
    finally:
        try:
            router.delete()
        except Exception:  # noqa
            pass


# ---------------------------------------------------------------- main
def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="redis://127.0.0.1:6380")
    parser.add_argument("--label", default="kividb")
    args = parser.parse_args()

    client = redis.Redis.from_url(args.url)
    server_version = client.info("server").get("redis_version", "?")
    print(f"server {args.label} at {args.url} (redis_version {server_version})")
    check("connection: PING", lambda: client.ping() and "")
    before_indexes = indexes(client)
    before_commands = command_counts(client)

    run_group("SearchIndex hash FLAT", search_index, args.url, "hash", "flat", True, False)
    run_group("SearchIndex hash HNSW", search_index, args.url, "hash", "hnsw", False, False)
    run_group("SearchIndex JSON FLAT", search_index, args.url, "json", "flat", True, True)
    run_group("HybridQuery hash", hybrid, args.url, "hash")
    run_group("HybridQuery JSON", hybrid, args.url, "json")
    run_group("AggregateHybridQuery", aggregate_hybrid, args.url)
    run_group("SemanticCache", semantic_cache, args.url)
    run_group("SemanticRouter", semantic_router, args.url)
    cleanup(client, before_indexes)

    after_commands = command_counts(client)
    delta = {}
    for name in set(before_commands) | set(after_commands):
        calls = after_commands.get(name, 0) - before_commands.get(name, 0)
        if calls:
            delta[name] = calls
    lua_used = sorted(name for name in delta if name.lower() in LUA_COMMANDS)

    counts = {s: sum(1 for row in results if row["status"] == s) for s in ("PASS", "FAIL")}
    print(f"\n{args.label}: {counts['PASS']} PASS, {counts['FAIL']} FAIL of {len(results)} checks")
    if lua_used:
        print(f"lua commands seen: {', '.join(lua_used)}")
    else:
        print("lua commands seen: none")

    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"results-{args.label}.json")
    with open(out, "w") as fh:
        json.dump({
            "label": args.label,
            "url": args.url,
            "redis_version": server_version,
            "redisvl": version("redisvl"),
            "redis_py": version("redis"),
            "numpy": version("numpy"),
            "python": sys.version.split()[0],
            "lua_commands": lua_used,
            "commands": dict(sorted(delta.items())),
            "counts": counts,
            "checks": results,
        }, fh, indent=2)
        fh.write("\n")
    print(f"wrote {out}")
    return 1 if counts["FAIL"] else 0


if __name__ == "__main__":
    sys.exit(main())
