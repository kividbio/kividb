#!/usr/bin/env python3
"""
LightRAG <-> Redis-protocol server compatibility harness.

Runs LightRAG's two Redis storages against any server (KiviDB, Redis 8, Valkey) and records PASS / FAIL /
WARN per check. A deterministic stub plays the language model and another the embedding model, so no
real API keys or spend are involved.

  pip install -r requirements.txt
  python lightrag_kividb_compat.py --url redis://HOST:6380 --label kividb
  python lightrag_kividb_compat.py --url redis://HOST:6379 --label redis8

Writes results-<label>.json next to the script. Run the same command against stock Redis (for
example the `redis:8` image) with a different --label to get a comparison.

What it covers, from `lightrag-hku` (lightrag/kg/redis_impl.py has exactly two storage classes):
  * RedisKVStorage, used directly: upsert, get_by_id, get_by_ids, filter_keys, is_empty, delete, drop,
    the text_chunks defaults, and workspace isolation
  * RedisDocStatusStorage, used directly: upsert, get_by_id, status counts, by status, by track id,
    paging and sorting, lookup by file path, targeted field update, delete and drop
  * the eviction guard: LightRAG refuses a server set to evict any key (CONFIG GET maxmemory*)
  * LightRAG itself with kv_storage=RedisKVStorage and doc_status_storage=RedisDocStatusStorage:
    insert two documents, query (naive and hybrid), the LLM response cache, a restart that reads
    the same workspace back, and delete by document id

LightRAG has no Redis graph or vector storage, so the graph and the vectors stay in LightRAG's
default local files (NetworkXStorage, NanoVectorDBStorage) in a temporary folder.

LightRAG's own Redis tests (the `redis` backend of tests/kg/test_doc_status_scheduling_contract.py)
are run separately: see REPORT.md.

NOTE: the stub embedding is a hashed bag of words and the stub LLM returns fixed-format extractions
(every capitalised word becomes an entity). This proves the wiring, not the quality of a real model.

Everything it creates lives in a workspace with a random name and is removed at the end.
"""
import argparse
import asyncio
import hashlib
import json
import logging
import math
import os
import re
import shutil
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from importlib.metadata import version

import numpy as np
import redis

logging.disable(logging.CRITICAL)  # LightRAG is noisy

DIM = 64
results = []
PREFIXES = []  # every random name used, so its keys can be removed


# ---------------------------------------------------------------- stub models
def embed(text):
    """Hashed bag of words, unit length: deterministic and offline."""
    v = [0.0] * DIM
    for tok in re.findall(r"[a-z0-9]+", text.lower()):
        v[int(hashlib.md5(tok.encode()).hexdigest(), 16) % DIM] += 1.0
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / n for x in v]


async def stub_embed(texts, **_kwargs):
    return np.array([embed(t) for t in texts], dtype=np.float32)


STOP = {"The", "A", "An", "In", "On", "And", "It", "Its", "This", "Who", "What", "Where", "Which"}
CALLS = {"extract": 0, "keywords": 0, "answer": 0}
ANSWER_CONTEXTS = []  # the system prompt of every answer call: it carries the retrieved context


def names_in(text):
    seen = []
    for w in re.findall(r"\b[A-Z][a-z]+\b", text):
        if w not in STOP and w not in seen:
            seen.append(w)
    return seen


async def stub_llm(prompt, system_prompt=None, history_messages=None, keyword_extraction=False, **_kwargs):
    system_prompt = system_prompt or ""
    if "---Input Text---" in prompt:
        CALLS["extract"] += 1
        m = re.search(r"---Input Text---\s*```\s*(.*?)```", prompt, re.S)
        names = names_in(m.group(1) if m else "")[:6]
        rows = [f"entity<|#|>{n}<|#|>Other<|#|>{n} is named in the text." for n in names]
        for a, b in zip(names, names[1:]):
            rows.append(f"relation<|#|>{a}<|#|>{b}<|#|>named together<|#|>{a} and {b} are named together.")
        rows.append("<|COMPLETE|>")
        return "\n".join(rows)
    if keyword_extraction or "high_level_keywords" in prompt:
        CALLS["keywords"] += 1
        m = re.search(r"User Query:\s*(.*)", prompt)
        query = m.group(1) if m else prompt
        return json.dumps({"high_level_keywords": ["people and places"], "low_level_keywords": names_in(query)})
    CALLS["answer"] += 1
    ANSWER_CONTEXTS.append(system_prompt + "\n" + prompt)
    return f"stub answer {CALLS['answer']}"


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


async def acheck(name, fn, level="FAIL"):
    """check() for the async groups: fn is a coroutine function."""
    t0 = time.time()
    try:
        detail = await fn()
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
        out = fn(*args)
        if asyncio.iscoroutine(out):
            out = LOOP.run_until_complete(out)
        return out
    except Exception as e:  # noqa
        detail = f"{type(e).__name__}: {str(e)[:220]}"
        results.append({"check": f"{title}: setup", "status": "FAIL", "detail": detail, "ms": 0})
        print(f"  [FAIL] {title}: setup  ({detail})")
        return None


def unique(prefix):
    name = f"{prefix}{uuid.uuid4().hex[:8]}"
    PREFIXES.append(name)
    return name


def keys_like(r, pattern):
    return sorted(k.decode() for k in r.scan_iter(match=pattern, count=500))


def cleanup(r):
    for prefix in PREFIXES:
        for key in r.scan_iter(match=f"*{prefix}*", count=500):
            r.delete(key)


LOOP = asyncio.new_event_loop()
asyncio.set_event_loop(LOOP)


# ---------------------------------------------------------------- RedisKVStorage, used directly
async def kv_store(r):
    from lightrag.kg.redis_impl import RedisKVStorage

    ws = unique("lrkv")
    other_ws = unique("lrkvo")

    def make(namespace, workspace=ws):
        return RedisKVStorage(namespace=namespace, workspace=workspace, global_config={},
                              embedding_func=None)

    docs = make("full_docs")
    chunks = make("text_chunks")
    foreign = make("full_docs", other_ws)
    for s in (docs, chunks, foreign):
        await s.initialize()

    async def upsert_get():
        await docs.upsert({"d1": {"content": "first document"}, "d2": {"content": "second document"}})
        got = await docs.get_by_id("d1")
        assert got["content"] == "first document", got
        assert got["create_time"] > 0 and got["update_time"] >= got["create_time"], got
        assert keys_like(r, f"{ws}_full_docs:*") == [f"{ws}_full_docs:d1", f"{ws}_full_docs:d2"], \
            keys_like(r, f"{ws}_full_docs:*")

    async def get_many():
        got = await docs.get_by_ids(["d2", "missing", "d1"])
        assert [g and g["content"] for g in got] == ["second document", None, "first document"], got

    async def filter_keys():
        new = await docs.filter_keys({"d1", "d2", "d3", "d4"})
        assert new == {"d3", "d4"}, new

    async def update():
        time.sleep(1.1)  # update_time has a one-second resolution
        before = (await docs.get_by_id("d1"))["update_time"]
        await docs.upsert({"d1": {"content": "first document, edited"}})
        got = await docs.get_by_id("d1")
        assert got["content"] == "first document, edited" and got["update_time"] > before, got

    async def chunk_defaults():
        await chunks.upsert({"c1": {"content": "a chunk", "full_doc_id": "d1", "tokens": 2}})
        got = await chunks.get_by_id("c1")
        assert got["llm_cache_list"] == [] and got["full_doc_id"] == "d1", got

    async def empty():
        assert not await docs.is_empty()
        fresh = make("llm_response_cache")
        await fresh.initialize()
        assert await fresh.is_empty()
        await fresh.finalize()

    async def isolation():
        await foreign.upsert({"d1": {"content": "another workspace"}})
        assert (await docs.get_by_id("d1"))["content"] == "first document, edited"
        assert (await foreign.get_by_id("d1"))["content"] == "another workspace"

    async def delete():
        await docs.delete(["d2"])
        assert await docs.get_by_id("d2") is None
        assert (await docs.get_by_id("d1")) is not None

    async def drop():
        out = await docs.drop()
        assert out["status"] == "success", out
        assert await docs.is_empty()
        assert (await chunks.get_by_id("c1")) is not None, "drop removed another namespace"
        assert (await foreign.get_by_id("d1")) is not None, "drop removed another workspace"

    await acheck("KV: upsert and get_by_id, with create_time and update_time", upsert_get)
    await acheck("KV: get_by_ids keeps the order and returns None for a missing id", get_many)
    await acheck("KV: filter_keys returns the ids not stored yet", filter_keys)
    await acheck("KV: upsert over an id updates it and its update_time", update)
    await acheck("KV: text_chunks rows get an llm_cache_list", chunk_defaults)
    await acheck("KV: is_empty", empty)
    await acheck("KV: two workspaces with the same namespace do not see each other", isolation)
    await acheck("KV: delete", delete)
    await acheck("KV: drop removes this namespace only", drop)
    for s in (docs, chunks, foreign):
        await s.finalize()


# ---------------------------------------------------------------- RedisDocStatusStorage, used directly
async def doc_status(r):
    from lightrag.base import DocStatus
    from lightrag.kg.redis_impl import RedisDocStatusStorage

    ws = unique("lrds")
    st = RedisDocStatusStorage(namespace="doc_status", workspace=ws, global_config={}, embedding_func=None)
    await st.initialize()

    def row(status, n, track, path):
        stamp = datetime(2026, 1, 1, 0, 0, n, tzinfo=timezone.utc).isoformat()
        return {"status": status, "content_summary": f"summary {n}", "content_length": 100 + n,
                "file_path": path, "created_at": stamp, "updated_at": stamp, "track_id": track,
                "chunks_count": n, "chunks_list": [f"chunk-{n}"]}

    rows = {
        "doc-1": row(DocStatus.PROCESSED.value, 1, "track-a", "a/one.txt"),
        "doc-2": row(DocStatus.PROCESSED.value, 2, "track-a", "a/two.txt"),
        "doc-3": row(DocStatus.PENDING.value, 3, "track-b", "b/three.txt"),
        "doc-4": row(DocStatus.FAILED.value, 4, "track-b", "b/four.txt"),
    }

    async def upsert_get():
        await st.upsert({k: dict(v) for k, v in rows.items()})
        got = await st.get_by_id("doc-3")
        assert got["status"] == "pending" and got["file_path"] == "b/three.txt", got
        assert await st.get_by_id("doc-missing") is None

    async def counts():
        got = await st.get_status_counts()
        assert (got["processed"], got["pending"], got["failed"]) == (2, 1, 1), got
        n = await st.count_docs_by_statuses([DocStatus.PROCESSED, DocStatus.FAILED])
        assert n == 3, n

    async def by_status():
        got = await st.get_docs_by_statuses([DocStatus.PROCESSED])
        assert sorted(got) == ["doc-1", "doc-2"], sorted(got)
        assert got["doc-2"].chunks_count == 2, got["doc-2"]

    async def by_track():
        got = await st.get_docs_by_track_id("track-b")
        assert sorted(got) == ["doc-3", "doc-4"], sorted(got)

    async def paged():
        page, total = await st.get_docs_paginated(page=1, page_size=10, sort_field="created_at",
                                                  sort_direction="asc")
        assert total == 4 and [d for d, _ in page] == ["doc-1", "doc-2", "doc-3", "doc-4"], (total, page)
        page, total = await st.get_docs_paginated(status_filter=DocStatus.PROCESSED, page=1, page_size=10,
                                                  sort_field="created_at", sort_direction="desc")
        assert total == 2 and [d for d, _ in page] == ["doc-2", "doc-1"], (total, page)

    async def by_path():
        got = await st.get_doc_by_file_path("b/four.txt")
        assert got and got["status"] == "failed", got
        assert await st.get_doc_by_file_path("nowhere.txt") is None

    async def update_fields():
        await st.update_doc_status_fields("doc-3", {"status": DocStatus.PROCESSED.value,
                                                    "updated_at": datetime.now(timezone.utc).isoformat()})
        got = await st.get_status_counts()
        assert (got["processed"], got["pending"]) == (3, 0), got
        assert await st.count_docs_by_statuses([DocStatus.PENDING]) == 0

    async def delete():
        await st.delete(["doc-4"])
        assert await st.get_by_id("doc-4") is None
        assert await st.count_docs_by_statuses([DocStatus.FAILED]) == 0
        assert (await st.get_status_counts())["failed"] == 0

    async def drop():
        out = await st.drop()
        assert out["status"] == "success", out
        assert await st.is_empty()
        left = keys_like(r, f"*{ws}*")
        assert left == [], left

    await acheck("doc status: upsert and get_by_id", upsert_get)
    await acheck("doc status: status counts, scanned and from the per-status sorted sets", counts)
    await acheck("doc status: get_docs_by_statuses", by_status)
    await acheck("doc status: get_docs_by_track_id", by_track)
    await acheck("doc status: paging, with a status filter and both sort directions", paged)
    await acheck("doc status: lookup by file path", by_path)
    await acheck("doc status: a targeted field update moves the doc to its new status", update_fields)
    await acheck("doc status: delete removes the row and its index entries", delete)
    await acheck("doc status: drop leaves no key behind", drop)
    await st.finalize()


# ---------------------------------------------------------------- eviction guard
async def eviction_guard(r, url):
    from lightrag.exceptions import StorageControlPlaneError  # noqa: F401  (the error it should raise)
    from lightrag.kg import redis_impl
    from redis.asyncio import Redis

    client = Redis.from_url(url)
    saved = r.config_get("maxmemory*")

    async def allowed():
        await redis_impl._ensure_no_eviction_policy(client, f"{url}#guard-ok-{uuid.uuid4().hex}", "guard")

    async def refused():
        r.config_set("maxmemory", "1gb")
        r.config_set("maxmemory-policy", "allkeys-lru")
        try:
            try:
                await redis_impl._ensure_no_eviction_policy(client, f"{url}#guard-{uuid.uuid4().hex}", "guard")
            except Exception as e:  # noqa
                assert type(e).__name__ == "StorageControlPlaneError", repr(e)
                assert "allkeys-lru" in str(e), str(e)
                return
            raise AssertionError("an allkeys-lru server with maxmemory set was accepted")
        finally:
            r.config_set("maxmemory", saved.get("maxmemory", "0"))
            r.config_set("maxmemory-policy", saved.get("maxmemory-policy", "noeviction"))

    await acheck("eviction guard: the default server config is accepted", allowed)
    await acheck("eviction guard: maxmemory with allkeys-lru is refused (CONFIG GET maxmemory*)", refused)
    await client.aclose()


# ---------------------------------------------------------------- LightRAG, end to end
TEXTS = {
    "doc-alice": "Alice works at Acme in Berlin. Alice builds rockets for Acme.",
    "doc-bob": "Bob lives in Lisbon and repairs bicycles. Bob rides with Carol on Sundays.",
}


async def end_to_end(r):
    from lightrag import LightRAG, QueryParam
    from lightrag.base import DocStatus
    from lightrag.utils import EmbeddingFunc

    ws = unique("lre2e")
    workdir = tempfile.mkdtemp(prefix="lightrag-compat-")

    def new_rag():
        return LightRAG(working_dir=workdir, workspace=ws,
                        kv_storage="RedisKVStorage", doc_status_storage="RedisDocStatusStorage",
                        llm_model_func=stub_llm, entity_extract_max_gleaning=0,
                        embedding_func=EmbeddingFunc(embedding_dim=DIM, max_token_size=8192, func=stub_embed))

    rag = new_rag()
    await rag.initialize_storages()
    state = {"rag": rag}

    async def insert():
        await rag.ainsert(list(TEXTS.values()), ids=list(TEXTS), file_paths=["alice.txt", "bob.txt"])
        for doc_id in TEXTS:
            got = await rag.doc_status.get_by_id(doc_id)
            assert got and got["status"] == DocStatus.PROCESSED.value, got
        assert CALLS["extract"] >= 2, CALLS

    async def stored_in_server():
        full = keys_like(r, f"{ws}_full_docs:*")
        assert full == [f"{ws}_full_docs:doc-alice", f"{ws}_full_docs:doc-bob"], full
        assert keys_like(r, f"{ws}_text_chunks:*"), "no text_chunks keys on the server"
        assert keys_like(r, f"{ws}_doc_status:*"), "no doc_status keys on the server"
        assert keys_like(r, f"{ws}_llm_response_cache:*"), "no extraction cache on the server"
        local = [f for f in os.listdir(workdir) if f.startswith(("kv_store", "doc_status"))]
        assert not local, f"KV or doc status written to local files instead: {local}"
        return f"{len(keys_like(r, f'*{ws}*'))} keys"

    async def graph_built():
        labels = await rag.chunk_entity_relation_graph.get_all_labels()
        for name in ("Alice", "Acme", "Bob", "Lisbon"):
            assert name in labels, labels

    async def naive():
        before = CALLS["answer"]
        out = await rag.aquery("Where does Bob live?", param=QueryParam(mode="naive", enable_rerank=False))
        assert out.startswith("stub answer"), out
        assert CALLS["answer"] == before + 1
        assert "repairs bicycles" in ANSWER_CONTEXTS[-1], "Bob's chunk is not in the context"

    async def hybrid():
        out = await rag.aquery("What does Alice build at Acme?",
                               param=QueryParam(mode="hybrid", enable_rerank=False))
        assert out.startswith("stub answer"), out
        ctx = ANSWER_CONTEXTS[-1]
        assert "Alice" in ctx and "builds rockets" in ctx, "Alice's entity or chunk is not in the context"

    async def cached():
        before = dict(CALLS)
        out = await rag.aquery("Where does Bob live?", param=QueryParam(mode="naive", enable_rerank=False))
        assert out.startswith("stub answer"), out
        assert CALLS["answer"] == before["answer"], "the answer was not served from the LLM cache"

    async def restart():
        await rag.finalize_storages()
        again = new_rag()
        await again.initialize_storages()
        state["rag"] = again
        counts = await again.doc_status.get_status_counts()
        assert counts["processed"] == 2, counts
        got = await again.full_docs.get_by_id("doc-bob")
        assert got and "bicycles" in got["content"], got
        out = await again.aquery("Who rides with Carol?", param=QueryParam(mode="naive", enable_rerank=False))
        assert out.startswith("stub answer") and "Sundays" in ANSWER_CONTEXTS[-1]

    async def delete():
        again = state["rag"]
        res = await again.adelete_by_doc_id("doc-bob")
        assert getattr(res, "status", "success") == "success", res
        assert await again.doc_status.get_by_id("doc-bob") is None
        assert await again.full_docs.get_by_id("doc-bob") is None
        assert keys_like(r, f"{ws}_full_docs:*") == [f"{ws}_full_docs:doc-alice"], keys_like(r, f"{ws}_full_docs:*")
        assert (await again.doc_status.get_status_counts())["processed"] == 1

    await acheck("LightRAG: insert two documents; both reach PROCESSED", insert)
    await acheck("LightRAG: documents, chunks, doc status and LLM cache are on the server, not in local files",
          stored_in_server)
    await acheck("LightRAG: the graph holds the extracted entities", graph_built)
    await acheck("LightRAG: naive query answers from the stored chunks", naive)
    await acheck("LightRAG: hybrid query (keywords, entities, chunks)", hybrid)
    await acheck("LightRAG: the same query again is served from the LLM cache", cached)
    await acheck("LightRAG: a new instance on the same workspace reads it all back", restart)
    await acheck("LightRAG: delete by document id", delete)

    await state["rag"].finalize_storages()
    shutil.rmtree(workdir, ignore_errors=True)


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="redis://localhost:6380")
    ap.add_argument("--label", default="kividb")
    args = ap.parse_args()
    os.environ["REDIS_URI"] = args.url
    os.environ.pop("REDIS_WORKSPACE", None)

    r = redis.Redis.from_url(args.url)
    server_version = r.info("server").get("redis_version", "?")
    print(f"server {args.label} at {args.url} (redis_version {server_version})")
    check("connection: PING", lambda: r.ping() and "")
    from lightrag.kg.shared_storage import initialize_share_data
    initialize_share_data()  # LightRAG's in-process locks, which the storages use

    run_group("RedisKVStorage, used directly", kv_store, r)
    run_group("RedisDocStatusStorage, used directly", doc_status, r)
    run_group("The eviction guard", eviction_guard, r, args.url)
    run_group("LightRAG, end to end", end_to_end, r)

    from lightrag.kg.redis_impl import RedisConnectionManager
    LOOP.run_until_complete(RedisConnectionManager.close_all_pools())
    cleanup(r)

    counts = {s: sum(1 for x in results if x["status"] == s) for s in ("PASS", "WARN", "FAIL")}
    print(f"\n{args.label}: {counts['PASS']} PASS, {counts['WARN']} WARN, {counts['FAIL']} FAIL "
          f"of {len(results)} checks")
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"results-{args.label}.json")
    with open(out, "w") as f:
        json.dump({"label": args.label, "url": args.url, "redis_version": server_version,
                   "lightrag_hku": version("lightrag-hku"), "redis_py": version("redis"),
                   "counts": counts, "checks": results}, f, indent=2)
    print(f"wrote {out}")
    return 1 if counts["FAIL"] else 0


if __name__ == "__main__":
    sys.exit(main())
