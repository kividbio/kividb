#!/usr/bin/env python3
"""
LangGraph <-> Redis-protocol server compatibility harness.

Runs `langgraph-checkpoint-redis` against any server (KiviDB, Redis 8, Valkey) and records
PASS / FAIL / WARN per check. The checkpointer is exercised through a real graph, not only
through its own methods. A deterministic stub plays the embedding model, so no real API keys
or spend are involved.

  pip install -r requirements.txt
  python langgraph_kividb_compat.py --url redis://HOST:6380 --label kividb
  python langgraph_kividb_compat.py --url redis://HOST:6379 --label redis8

Writes results-<label>.json next to the script. Run the same command against stock Redis (for
example the `redis:8` image) with a different --label to get a comparison.

What it covers, from `langgraph-checkpoint-redis`:
  * RedisSaver, the checkpointer: state that persists across invocations of a thread, state and
    history, thread isolation, update_state, interrupt and resume (pending writes), metadata
    filters and limits in list(), delete_thread
  * ShallowRedisSaver (keeps only the latest checkpoint) and AsyncRedisSaver
  * RedisStore: put, get, overwrite, delete, search by namespace prefix and by filter,
    list_namespaces, an expiry from a TTL, and semantic search on a vector index

The checkpointer and the store rely most on `JSON.*` together with `FT.*`, so this is where a
gap is most likely.

Left out on purpose, because it fails the same way on Redis 8.10.3 (langgraph-checkpoint-redis
0.6.0), so it says nothing about a server:
  * RedisSaver.copy_thread (raises NotImplementedError in the library)

NOTE: the stub embedding is a hashed bag of words. It proves the wiring (index creation, search,
filters), not semantic quality.

Everything it creates has a random prefix and is removed at the end, including the indexes the
library creates. The library names each index after the prefix it is given (checkpoint_prefix,
checkpoint_write_prefix, store_prefix, vector_prefix), and the harness always gives it one of its
random prefixes, so only an index that appeared during the run and has such a name is dropped (with
its documents): one that another client made on the same server meanwhile is left alone.
"""
import argparse
import asyncio
import hashlib
import json
import math
import operator
import os
import re
import sys
import time
import uuid
from importlib.metadata import version
from typing import Annotated, TypedDict

import redis
from langchain_core.embeddings import Embeddings
from langgraph.checkpoint.redis import AsyncRedisSaver, RedisSaver, ShallowRedisSaver
from langgraph.graph import END, START, StateGraph
from langgraph.store.redis import RedisStore

DIM = 64
results = []
PREFIXES = []  # every random prefix used, so its keys can be removed


# ---------------------------------------------------------------- stub embedding model
class HashEmbeddings(Embeddings):
    """Hashed bag of words, unit length: deterministic and offline."""

    def _embed(self, text):
        v = [0.0] * DIM
        for tok in re.findall(r"[a-z0-9]+", text.lower()):
            v[int(hashlib.md5(tok.encode()).hexdigest(), 16) % DIM] += 1.0
        n = math.sqrt(sum(x * x for x in v)) or 1.0
        return [x / n for x in v]

    def embed_documents(self, texts):
        return [self._embed(t) for t in texts]

    def embed_query(self, text):
        return self._embed(text)


# ---------------------------------------------------------------- a small graph
class State(TypedDict):
    n: int
    log: Annotated[list, operator.add]


def inc(s):
    return {"n": s["n"] + 1, "log": [f"inc->{s['n'] + 1}"]}


def dbl(s):
    return {"n": s["n"] * 2, "log": [f"dbl->{s['n'] * 2}"]}


def graph():
    g = StateGraph(State)
    g.add_node("inc", inc)
    g.add_node("dbl", dbl)
    g.add_edge(START, "inc")
    g.add_edge("inc", "dbl")
    g.add_edge("dbl", END)
    return g


def cfg(thread):
    return {"configurable": {"thread_id": thread}}


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
    random names in it. The library names every index after a prefix the harness gave it, so an
    index another client made during the run is left alone."""
    for idx in indexes(r) - before:
        if not any(prefix in idx for prefix in PREFIXES):
            continue
        try:
            r.execute_command("FT.DROPINDEX", idx, "DD")
        except Exception:  # noqa
            pass
    # The name is random and unique to this run, so it can be matched anywhere in a key: the
    # library keeps some bookkeeping keys (write_keys_zset:, write_registry:) that carry a thread
    # id but not the configured prefix.
    for prefix in PREFIXES:
        for key in r.scan_iter(match=f"*{prefix}*", count=500):
            r.delete(key)


# ---------------------------------------------------------------- the checkpointer
def checkpointer(url, r):
    pre, wr = unique("lgcp"), unique("lgcw")
    before = indexes(r)
    with RedisSaver.from_conn_string(url, checkpoint_prefix=pre, checkpoint_write_prefix=wr) as saver:
        def setup():
            saver.setup()
            made = indexes(r) - before
            assert made, "setup created no index"
            return ""

        check("checkpointer: setup creates its indexes", setup)
        app = graph().compile(checkpointer=saver)
        t = unique("thread")

        def persists():
            first = app.invoke({"n": 1, "log": []}, cfg(t))
            assert first["n"] == 4 and first["log"] == ["inc->2", "dbl->4"], first
            second = app.invoke({"n": 5, "log": []}, cfg(t))
            assert second["n"] == 12, second
            assert second["log"] == ["inc->2", "dbl->4", "inc->6", "dbl->12"], second["log"]

        def latest():
            st = app.get_state(cfg(t))
            assert st.values["n"] == 12 and st.next == (), (st.values, st.next)
            assert st.metadata is not None and "step" in st.metadata, st.metadata

        def history():
            hist = list(app.get_state_history(cfg(t)))
            assert len(hist) >= 4, f"only {len(hist)} checkpoints"
            steps = [h.metadata["step"] for h in hist]
            assert steps == sorted(steps, reverse=True), f"not newest first: {steps}"
            assert hist[0].values["n"] == 12, hist[0].values

        def isolated():
            assert app.get_state(cfg(unique("other"))).values == {}, "another thread saw state"

        def update():
            before_n = len(list(app.get_state_history(cfg(t))))
            app.update_state(cfg(t), {"n": 100, "log": ["edited"]})
            st = app.get_state(cfg(t))
            assert st.values["log"][-1] == "edited", st.values
            assert len(list(app.get_state_history(cfg(t)))) == before_n + 1, "no new checkpoint"

        def interrupt_resume():
            paused = graph().compile(checkpointer=saver, interrupt_before=["dbl"])
            th = unique("pause")
            out = paused.invoke({"n": 1, "log": []}, cfg(th))
            st = paused.get_state(cfg(th))
            assert st.next == ("dbl",), f"expected to wait before dbl, next={st.next}"
            assert st.values["n"] == 2, st.values
            done = paused.invoke(None, cfg(th))
            assert done["n"] == 4, f"resume gave {done}"
            assert paused.get_state(cfg(th)).next == (), "still waiting after resume"

        def listing():
            got = list(saver.list(cfg(t), filter={"source": "loop"}, limit=2))
            assert 0 < len(got) <= 2, f"limit 2 gave {len(got)}"
            assert all(c.metadata["source"] == "loop" for c in got), [c.metadata for c in got]

        def delete():
            saver.delete_thread(t)
            assert app.get_state(cfg(t)).values == {}, "state survives delete_thread"

        check("checkpointer: state persists across invocations of a thread", persists)
        check("checkpointer: get_state returns the latest values", latest)
        check("checkpointer: history lists checkpoints, newest first", history)
        check("checkpointer: another thread starts empty", isolated)
        check("checkpointer: update_state writes a new checkpoint", update)
        check("checkpointer: interrupt and resume (pending writes)", interrupt_resume)
        check("checkpointer: list filters on metadata and honours limit", listing)
        check("checkpointer: delete_thread", delete)
    return before


def shallow(url, r):
    pre, wr = unique("lgsh"), unique("lgshw")
    before = indexes(r)
    with ShallowRedisSaver.from_conn_string(url, checkpoint_prefix=pre, checkpoint_write_prefix=wr) as saver:
        saver.setup()
        app = graph().compile(checkpointer=saver)
        t = unique("shallow")

        def only_latest():
            app.invoke({"n": 1, "log": []}, cfg(t))
            app.invoke({"n": 5, "log": []}, cfg(t))
            assert app.get_state(cfg(t)).values["n"] == 12, app.get_state(cfg(t)).values
            hist = list(app.get_state_history(cfg(t)))
            assert len(hist) == 1, f"a shallow saver kept {len(hist)} checkpoints"

        check("shallow checkpointer: keeps only the latest checkpoint", only_latest)
    return before


def asynchronous(url, r):
    before = indexes(r)

    async def run():
        pre, wr = unique("lgas"), unique("lgasw")
        async with AsyncRedisSaver.from_conn_string(url, checkpoint_prefix=pre,
                                                    checkpoint_write_prefix=wr) as saver:
            await saver.asetup()
            app = graph().compile(checkpointer=saver)
            t = unique("async")
            first = await app.ainvoke({"n": 1, "log": []}, cfg(t))
            assert first["n"] == 4, first
            second = await app.ainvoke({"n": 5, "log": []}, cfg(t))
            assert second["n"] == 12 and len(second["log"]) == 4, second
            st = await app.aget_state(cfg(t))
            assert st.values["n"] == 12, st.values
            await saver.adelete_thread(t)
            assert (await app.aget_state(cfg(t))).values == {}, "state survives adelete_thread"

    check("async checkpointer: ainvoke persists, aget_state, adelete_thread", lambda: asyncio.run(run()))
    return before


# ---------------------------------------------------------------- the store
def store(url, r):
    sp, vp = unique("lgst"), unique("lgstv")
    before = indexes(r)
    with RedisStore.from_conn_string(url, store_prefix=sp, vector_prefix=vp) as st:
        st.setup()
        ns = ("users", unique("u"))

        def roundtrip():
            st.put(ns, "alice", {"kind": "person", "age": 30, "text": "likes rust"})
            item = st.get(ns, "alice")
            assert item is not None and item.value["age"] == 30, item
            assert item.key == "alice" and tuple(item.namespace) == ns, (item.key, item.namespace)

        def missing():
            assert st.get(ns, "nobody") is None, "a missing key was found"

        def overwrite():
            st.put(ns, "alice", {"kind": "person", "age": 31, "text": "likes go"})
            assert st.get(ns, "alice").value["age"] == 31, st.get(ns, "alice").value

        def search_prefix():
            st.put(ns, "bob", {"kind": "robot", "age": 3, "text": "beeps"})
            found = {i.key for i in st.search(("users",), limit=50) if tuple(i.namespace) == ns}
            assert found == {"alice", "bob"}, found

        def search_filter():
            found = [i.key for i in st.search(ns, filter={"kind": "robot"})]
            assert found == ["bob"], found

        def namespaces():
            listed = {tuple(n) for n in st.list_namespaces(prefix=("users",))}
            assert ns in listed, listed

        def delete():
            st.delete(ns, "bob")
            assert st.get(ns, "bob") is None, "still there after delete"

        def ttl():
            st.put(ns, "short-lived", {"kind": "temp"}, ttl=5)
            expiring = [k for k in r.scan_iter(match=f"{sp}*", count=500) if r.ttl(k) > 0]
            assert expiring, "no key under the store prefix has an expiry"

        check("store: put and get", roundtrip)
        check("store: a missing key is None", missing)
        check("store: put again replaces the value", overwrite)
        check("store: search by namespace prefix", search_prefix)
        check("store: search with a filter", search_filter)
        check("store: list_namespaces", namespaces)
        check("store: delete", delete)
        check("store: put with a ttl sets an expiry", ttl)
    return before


def store_vector(url, r):
    sp, vp = unique("lgsv"), unique("lgsvv")
    before = indexes(r)
    index = {"dims": DIM, "embed": HashEmbeddings(), "fields": ["text"]}
    with RedisStore.from_conn_string(url, index=index, store_prefix=sp, vector_prefix=vp) as st:
        st.setup()
        ns = ("notes", unique("n"))
        docs = {
            "cats": {"text": "the cat sat on the warm mat", "topic": "animals"},
            "rates": {"text": "interest rates rose again this quarter", "topic": "finance"},
            "rust": {"text": "a new rust compiler release is out", "topic": "tech"},
        }
        for key, value in docs.items():
            st.put(ns, key, value)

        def nearest():
            top = st.search(ns, query="interest rates rose again", limit=3)
            assert top and top[0].key == "rates", [i.key for i in top]
            assert top[0].score is not None, "no score on a semantic hit"

        def filtered():
            top = st.search(ns, query="anything at all", filter={"topic": "tech"}, limit=3)
            assert [i.key for i in top] == ["rust"], [i.key for i in top]

        def limited():
            top = st.search(ns, query="the cat sat on the warm mat", limit=2)
            assert len(top) == 2, f"limit 2 gave {len(top)}"

        check("store (vector): semantic search finds the closest note", nearest)
        check("store (vector): semantic search with a filter", filtered)
        check("store (vector): limit is honoured", limited)
    return before


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="redis://localhost:6380")
    ap.add_argument("--label", default="kividb")
    args = ap.parse_args()

    r = redis.Redis.from_url(args.url)
    server_version = r.info("server").get("redis_version", "?")
    print(f"server {args.label} at {args.url} (redis_version {server_version})")
    check("connection: PING", lambda: r.ping() or "")
    start = indexes(r)

    run_group("RedisSaver", checkpointer, args.url, r)
    run_group("ShallowRedisSaver", shallow, args.url, r)
    run_group("AsyncRedisSaver", asynchronous, args.url, r)
    run_group("RedisStore", store, args.url, r)
    run_group("RedisStore with a vector index", store_vector, args.url, r)
    cleanup(r, start)

    counts = {s: sum(1 for x in results if x["status"] == s) for s in ("PASS", "WARN", "FAIL")}
    print(f"\n{args.label}: {counts['PASS']} PASS, {counts['WARN']} WARN, {counts['FAIL']} FAIL "
          f"of {len(results)} checks")
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"results-{args.label}.json")
    with open(out, "w") as f:
        json.dump({"label": args.label, "url": args.url, "redis_version": server_version,
                   "langgraph_checkpoint_redis": version("langgraph-checkpoint-redis"),
                   "langgraph": version("langgraph"),
                   "counts": counts, "checks": results}, f, indent=2)
    print(f"wrote {out}")
    return 1 if counts["FAIL"] else 0


if __name__ == "__main__":
    sys.exit(main())
