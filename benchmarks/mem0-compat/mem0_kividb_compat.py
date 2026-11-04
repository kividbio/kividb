#!/usr/bin/env python3
"""
Mem0 <-> Redis-protocol server compatibility harness.

Runs Mem0's Redis vector store against any server (KiviDB, Redis 8, Valkey) and records PASS / FAIL /
WARN per check. A deterministic stub plays the embedding model and no language model is called, so no
real API keys or spend are involved.

  pip install -r requirements.txt
  python mem0_kividb_compat.py --url redis://HOST:6380 --label kividb
  python mem0_kividb_compat.py --url redis://HOST:6379 --label redis8

Writes results-<label>.json next to the script. Run the same command against stock Redis (for
example the `redis:8` image) with a different --label to get a comparison.

What it covers, from `mem0ai`:
  * RedisDB, the vector store, used directly: insert, vector search, filters on user / agent / run,
    get, update, list, BM25 keyword search, delete, the collection helpers, reset
  * Memory, the public API, on top of that store: add (with infer=False, which stores the text as
    given and so needs no language model), search scoped to a user, get_all, get, update, delete and
    the change history

Left out on purpose, because it fails the same way on Redis 8.10.3, so it says nothing about a server:
  * RedisDB.get() of an id that does not exist raises KeyError, because redisvl's fetch() returns {}
    and not None (https://github.com/mem0ai/mem0/issues/7820)
  * with redisvl 0.28 or newer RedisDB cannot be created at all, which is why requirements.txt pins
    the versions that "pip install mem0ai[vector-stores]" resolves to
    (https://github.com/mem0ai/mem0/issues/7819)

NOTE: the stub embedding is a hashed bag of words. It proves the wiring (index creation, search,
filters), not semantic quality. Mem0's own fact extraction (infer=True) needs a language model and is
not exercised.

Everything it creates has a random name and is removed at the end, including the indexes. Every index
the harness makes carries one of those names, except `mem0migrations`, which Mem0 itself creates with a
fixed name whenever a Memory is built. Only an index that appeared during the run and has one of these
names is dropped (with its documents): one that another client made on the same server meanwhile is
left alone.
"""
import argparse
import hashlib
import json
import logging
import math
import os
import re
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from importlib.metadata import version

import redis

logging.disable(logging.CRITICAL)  # mem0 and its telemetry are noisy

from mem0 import Memory  # noqa: E402
from mem0.vector_stores.redis import RedisDB  # noqa: E402

DIM = 64
results = []
PREFIXES = []  # every random name used, so its keys can be removed
# Mem0 makes this index itself, with a fixed name, every time a Memory is created (it keeps a user id
# for its telemetry there). It is dropped too, but only if it was not there before the run.
LIBRARY_INDEXES = {"mem0migrations"}


# ---------------------------------------------------------------- stub embedding model
def embed(text):
    """Hashed bag of words, unit length: deterministic and offline."""
    v = [0.0] * DIM
    for tok in re.findall(r"[a-z0-9]+", text.lower()):
        v[int(hashlib.md5(tok.encode()).hexdigest(), 16) % DIM] += 1.0
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / n for x in v]


class StubEmbedder:
    def embed(self, text, memory_action=None):
        return embed(text)


# ---------------------------------------------------------------- check runner
def check(name, fn, level="FAIL"):
    t0 = time.time()
    try:
        detail = fn()
        status = "PASS"
    except Exception as e:  # noqa
        detail, status = f"{type(e).__name__}: {str(e)[:220]}", level
    results.append({"check": name, "status": status, "detail": detail or "",
                    "ms": round((time.time() - t0) * 1000)})
    print(f"  [{status:4}] {name}" + (f"  ({detail})" if detail else ""))


def run_group(title, fn, *args):
    """Run a group of checks. If its setup raises (a server without the feature it needs),
    that is one FAIL line, and the next group still runs."""
    print(f"\n== {title}")
    try:
        return fn(*args)
    except Exception as e:  # noqa
        detail = f"{type(e).__name__}: {str(e)[:220]}"
        results.append({"check": f"{title}: setup", "status": "FAIL", "detail": detail, "ms": 0})
        print(f"  [FAIL] {title}: setup  ({detail})")
        return None


def unique(prefix):
    name = f"{prefix}_{uuid.uuid4().hex[:8]}"
    PREFIXES.append(name)
    return name


def indexes(r):
    """The names of the search indexes, or none if the server cannot list them (the library's own
    errors are then what gets reported)."""
    try:
        return {i.decode() if isinstance(i, bytes) else i for i in r.execute_command("FT._LIST")}
    except Exception:  # noqa
        return set()


def cleanup(r, before):
    """Drop this run's indexes, with their documents, then any key that has one of this run's
    random names in it. Every index the harness makes is named with one of those names (or is
    Mem0's own, see LIBRARY_INDEXES), so an index another client made during the run is left alone."""
    for idx in indexes(r) - before:
        if idx not in LIBRARY_INDEXES and not any(prefix in idx for prefix in PREFIXES):
            continue
        try:
            r.execute_command("FT.DROPINDEX", idx, "DD")
        except Exception:  # noqa
            pass
    for prefix in PREFIXES:
        for key in r.scan_iter(match=f"*{prefix}*", count=500):
            r.delete(key)


# ---------------------------------------------------------------- the vector store, used directly
NOW = datetime.now(timezone.utc).isoformat()
ITEMS = [
    ("m-cats", "I like cats and warm mats", "alice", "agent-1"),
    ("m-rates", "interest rates rose again this quarter", "alice", "agent-2"),
    ("m-rust", "a new rust compiler release is out", "bob", "agent-1"),
]


def vector_store(url, r):
    name = unique("m0v")
    db = RedisDB(redis_url=url, collection_name=name, embedding_model_dims=DIM)

    def payload(text, user, agent):
        return {"data": text, "user_id": user, "agent_id": agent, "hash": hashlib.md5(text.encode()).hexdigest(),
                "created_at": NOW, "topic": "demo"}

    def insert():
        db.insert([embed(t) for _, t, _, _ in ITEMS], [payload(t, u, a) for _, t, u, a in ITEMS],
                  [i for i, _, _, _ in ITEMS])
        assert name in indexes(r), f"index {name} not listed"

    def nearest():
        top = db.search("rates", embed("interest rates rose"), top_k=3)
        assert top and top[0].id == "m-rates", [x.id for x in top]
        assert top[0].score > 0.5, top[0].score
        assert [x.score for x in top] == sorted((x.score for x in top), reverse=True), "scores not ordered"

    def top_k():
        assert len(db.search("x", embed("the"), top_k=1)) == 1
        assert len(db.search("x", embed("the"), top_k=3)) == 3

    def by_user():
        got = sorted(x.id for x in db.search("x", embed("anything"), top_k=10, filters={"user_id": "alice"}))
        assert got == ["m-cats", "m-rates"], got

    def by_two():
        got = [x.id for x in db.search("x", embed("anything"), top_k=10,
                                       filters={"user_id": "alice", "agent_id": "agent-2"})]
        assert got == ["m-rates"], got

    def payload_back():
        hit = db.search("x", embed("rust compiler"), top_k=1)[0]
        assert hit.id == "m-rust" and hit.payload["data"] == "a new rust compiler release is out", hit.payload
        assert hit.payload["user_id"] == "bob" and hit.payload["topic"] == "demo", hit.payload
        assert hit.payload["created_at"].startswith(NOW[:10]), hit.payload["created_at"]

    def get():
        got = db.get("m-rust")
        assert got.id == "m-rust" and got.payload["data"] == "a new rust compiler release is out", got

    def update():
        db.update("m-cats", embed("I like dogs"), payload("I like dogs", "alice", "agent-1"))
        assert db.get("m-cats").payload["data"] == "I like dogs"
        top = db.search("x", embed("I like dogs"), top_k=1)
        assert top[0].id == "m-cats", [x.id for x in top]

    def listing():
        got = sorted(x.id for page in db.list(filters={"user_id": "alice"}) for x in page)
        assert got == ["m-cats", "m-rates"], got
        assert sum(len(page) for page in db.list(top_k=2)) == 2

    def keyword():
        got = [x.id for x in db.keyword_search("rust", top_k=3)]
        assert got == ["m-rust"], got

    def delete():
        db.delete("m-rates")
        got = sorted(x.id for page in db.list() for x in page)
        assert got == ["m-cats", "m-rust"], got

    def collections():
        assert name in db.list_cols(), db.list_cols()
        assert db.col_info(name), "no collection info"

    def reset():
        db.reset()
        assert sum(len(page) for page in db.list()) == 0, "memories left after reset"

    check("vector store: insert creates the index and stores the memories", insert)
    check("vector store: search returns the closest memory first, scores ordered", nearest)
    check("vector store: top_k is honoured", top_k)
    check("vector store: filter on user_id", by_user)
    check("vector store: filter on user_id and agent_id together", by_two)
    check("vector store: the payload comes back whole", payload_back)
    check("vector store: get by id", get)
    check("vector store: update changes the text and the vector", update)
    check("vector store: list, with a filter and with top_k", listing)
    check("vector store: keyword (BM25) search", keyword)
    check("vector store: delete", delete)
    check("vector store: list_cols and col_info", collections)
    check("vector store: reset empties the collection", reset)
    db.delete_col()


# ---------------------------------------------------------------- Memory, the public API
def memory_api(url, r):
    name = unique("m0m")
    config = {
        "vector_store": {"provider": "redis", "config": {
            "redis_url": url, "collection_name": name, "embedding_model_dims": DIM}},
        # Never called: infer=False skips the language model, and the embedder is replaced below.
        "llm": {"provider": "openai", "config": {"api_key": "unused"}},
        "embedder": {"provider": "openai", "config": {"api_key": "unused", "embedding_dims": DIM}},
        "history_db_path": os.path.join(tempfile.mkdtemp(), "history.db"),
    }
    memory = Memory.from_config(config)
    memory.embedding_model = StubEmbedder()
    made = {}

    def add():
        for who, text in [("alice", "I love hiking in the mountains"), ("alice", "My cat is called Tom"),
                          ("bob", "I prefer tea to coffee")]:
            out = memory.add(text, user_id=who, infer=False)["results"]
            assert len(out) == 1 and out[0]["event"] == "ADD", out
            made[text] = out[0]["id"]

    def search():
        top = memory.search("hiking in the mountains", filters={"user_id": "alice"}, top_k=3)["results"]
        assert top and top[0]["memory"] == "I love hiking in the mountains", top
        assert all(x["user_id"] == "alice" for x in top), top

    def scoped():
        got = memory.search("tea coffee", filters={"user_id": "alice"}, top_k=5)["results"]
        assert all("tea" not in x["memory"] for x in got), "another user's memory came back"

    def get_all():
        got = sorted(x["memory"] for x in memory.get_all(filters={"user_id": "alice"})["results"])
        assert got == ["I love hiking in the mountains", "My cat is called Tom"], got

    def get_one():
        got = memory.get(made["My cat is called Tom"])
        assert got["memory"] == "My cat is called Tom" and got["user_id"] == "alice", got

    def update():
        mid = made["My cat is called Tom"]
        memory.update(mid, "My cat is called Felix")
        assert memory.get(mid)["memory"] == "My cat is called Felix"

    def history():
        rows = memory.history(made["My cat is called Tom"])  # newest first
        assert sorted(h["event"] for h in rows) == ["ADD", "UPDATE"], [h["event"] for h in rows]
        update = next(h for h in rows if h["event"] == "UPDATE")
        assert update["old_memory"] == "My cat is called Tom" and update["new_memory"] == "My cat is called Felix", update

    def delete():
        memory.delete(made["My cat is called Tom"])
        got = [x["memory"] for x in memory.get_all(filters={"user_id": "alice"})["results"]]
        assert got == ["I love hiking in the mountains"], got

    def delete_all():
        memory.delete_all(user_id="alice")
        assert memory.get_all(filters={"user_id": "alice"})["results"] == []
        assert len(memory.get_all(filters={"user_id": "bob"})["results"]) == 1, "another user's memory was deleted"

    check("Memory: add (infer=False) stores a memory for a user", add)
    check("Memory: search is scoped to the user and finds the closest", search)
    check("Memory: search does not return another user's memory", scoped)
    check("Memory: get_all for a user", get_all)
    check("Memory: get by id", get_one)
    check("Memory: update", update)
    check("Memory: history records the ADD and the UPDATE", history)
    check("Memory: delete", delete)
    check("Memory: delete_all removes one user's memories only", delete_all)


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="redis://localhost:6380")
    ap.add_argument("--label", default="kividb")
    args = ap.parse_args()

    r = redis.Redis.from_url(args.url)
    server_version = r.info("server").get("redis_version", "?")
    print(f"server {args.label} at {args.url} (redis_version {server_version})")
    check("connection: PING", lambda: r.ping() and "")
    start = indexes(r)

    run_group("RedisDB, the vector store", vector_store, args.url, r)
    run_group("Memory, the public API", memory_api, args.url, r)
    cleanup(r, start)

    counts = {s: sum(1 for x in results if x["status"] == s) for s in ("PASS", "WARN", "FAIL")}
    print(f"\n{args.label}: {counts['PASS']} PASS, {counts['WARN']} WARN, {counts['FAIL']} FAIL "
          f"of {len(results)} checks")
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"results-{args.label}.json")
    with open(out, "w") as f:
        json.dump({"label": args.label, "url": args.url, "redis_version": server_version,
                   "mem0ai": version("mem0ai"), "redisvl": version("redisvl"), "redis_py": version("redis"),
                   "counts": counts, "checks": results}, f, indent=2)
    print(f"wrote {out}")
    return 1 if counts["FAIL"] else 0


if __name__ == "__main__":
    sys.exit(main())
