#!/usr/bin/env python3
"""
LangChain <-> Redis-protocol server compatibility harness.

Runs the `langchain-redis` integration against any server (KiviDB, Redis 8, Valkey)
and records PASS / FAIL / WARN per check. A deterministic stub plays the embedding
model, so no real API keys or spend are involved.

  pip install langchain-redis redisvl langchain-core numpy
  python langchain_kividb_compat.py --url redis://HOST:6380 --label kividb
  python langchain_kividb_compat.py --url redis://HOST:6379 --label redis8

Writes results-<label>.json next to the script. Run the same command against stock
Redis (for example the `redis:8` image) with a different --label to get a comparison.

What it covers, from `langchain-redis`:
  * RedisVectorStore: add, similarity search, scores, metadata filters (tag, numeric),
    delete, get by id, MMR, retriever, from_existing_index, FLAT and HNSW
  * RedisCache: the LLM cache (update, lookup, clear)
  * RedisSemanticCache: the semantic LLM cache (hit on a close prompt, miss on a far one)
  * RedisChatMessageHistory: add, read back in order, clear

Left out on purpose, because they fail the same way on Redis 8.10.3 (langchain-redis
0.2.6, redis-py 8.1.0), so they say nothing about a server:
  * similarity_search_with_relevance_scores (raises NotImplementedError in the library)
  * reading the messages of a session that has none (raises AttributeError in the library)

NOTE: the stub embedding is a hashed bag of words. It proves the wiring (FT.CREATE,
KNN search, filters, hit and miss logic), not semantic quality.
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

import redis
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.outputs import Generation
from langchain_redis import (
    RedisCache,
    RedisChatMessageHistory,
    RedisConfig,
    RedisSemanticCache,
    RedisVectorStore,
)
from redisvl.query.filter import Num, Tag

DIM = 64
results = []

CORPUS = [
    ("the cat sat on the warm mat", {"category": "animals", "year": 2020}),
    ("dogs love to chase a red ball", {"category": "animals", "year": 2021}),
    ("the stock market fell sharply today", {"category": "finance", "year": 2022}),
    ("interest rates rose again this quarter", {"category": "finance", "year": 2023}),
    ("a new rust compiler release is out", {"category": "tech", "year": 2024}),
    ("vector databases index embeddings for search", {"category": "tech", "year": 2025}),
]
METADATA_SCHEMA = [
    {"name": "category", "type": "tag"},
    {"name": "year", "type": "numeric"},
]


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


EMB = HashEmbeddings()


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
    """Run a group of checks. If its setup raises (a server without the feature it
    needs), that is one FAIL line, and the next group still runs."""
    print(f"\n== {title}")
    try:
        return fn(*args)
    except Exception as e:  # noqa
        detail = f"{type(e).__name__}: {str(e)[:220]}"
        results.append({"check": f"{title}: setup", "status": "FAIL", "detail": detail, "ms": 0})
        print(f"  [FAIL] {title}: setup  ({detail})")
        return None


def unique(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def drop(r, index, prefix):
    """Remove an index with its documents, then any key left under the prefix."""
    try:
        r.execute_command("FT.DROPINDEX", index, "DD")
    except Exception:  # noqa
        pass
    for key in r.scan_iter(match=f"{prefix}*", count=500):
        r.delete(key)


def store(url, index, prefix, algorithm="FLAT", metadata=True):
    cfg = RedisConfig(
        index_name=index,
        key_prefix=prefix,
        redis_url=url,
        indexing_algorithm=algorithm,
        distance_metric="COSINE",
        metadata_schema=METADATA_SCHEMA if metadata else [],
    )
    vs = RedisVectorStore(EMB, config=cfg)
    vs.add_texts([t for t, _ in CORPUS], [m for _, m in CORPUS])
    return vs


# ---------------------------------------------------------------- checks
def vector_store(url, r, algorithm):
    index, prefix = unique(f"lc_{algorithm.lower()}"), unique("lcdoc")
    tag = f"[{algorithm}]"
    vs = None

    def build():
        nonlocal vs
        vs = store(url, index, prefix, algorithm)
        assert r.execute_command("FT.INFO", index), "FT.INFO returned nothing"

    def nearest():
        docs = vs.similarity_search("the cat sat on the warm mat", k=3)
        assert len(docs) == 3, f"asked for 3, got {len(docs)}"
        assert docs[0].page_content == "the cat sat on the warm mat", docs[0].page_content

    def scores():
        pairs = vs.similarity_search_with_score("dogs love to chase a red ball", k=4)
        assert pairs[0][0].page_content == "dogs love to chase a red ball"
        distances = [s for _, s in pairs]
        assert distances == sorted(distances), f"not ascending: {distances}"
        assert distances[0] < 0.01, f"exact match distance {distances[0]}"

    def metadata_roundtrip():
        docs = vs.similarity_search("vector databases index embeddings", k=1)
        meta = docs[0].metadata
        assert meta.get("category") == "tech", meta
        assert float(meta.get("year")) == 2025, meta

    def tag_filter():
        docs = vs.similarity_search("anything at all", k=6, filter=Tag("category") == "finance")
        assert len(docs) == 2, f"finance has 2 documents, got {len(docs)}"
        assert all(d.metadata["category"] == "finance" for d in docs), docs

    def numeric_filter():
        docs = vs.similarity_search("anything at all", k=6, filter=Num("year") >= 2024)
        years = sorted(int(float(d.metadata["year"])) for d in docs)
        assert years == [2024, 2025], years

    def combined_filter():
        flt = (Tag("category") == "tech") & (Num("year") < 2025)
        docs = vs.similarity_search("anything at all", k=6, filter=flt)
        assert [d.page_content for d in docs] == ["a new rust compiler release is out"], docs

    def retriever():
        got = vs.as_retriever(search_kwargs={"k": 2}).invoke("rust compiler")
        assert len(got) == 2 and all(isinstance(d, Document) for d in got), got

    def mmr():
        docs = vs.max_marginal_relevance_search("the cat sat on the warm mat", k=2, fetch_k=5)
        assert len(docs) == 2, f"asked for 2, got {len(docs)}"

    def add_and_delete():
        ids = vs.add_texts(["a temporary note about gardening"], [{"category": "temp", "year": 1999}])
        assert ids, "add_texts returned no ids"
        hit = vs.similarity_search("a temporary note about gardening", k=1)
        assert hit[0].page_content == "a temporary note about gardening"
        vs.delete(ids)
        again = vs.similarity_search("a temporary note about gardening", k=7)
        assert all(d.page_content != "a temporary note about gardening" for d in again), "still found"

    def by_ids():
        ids = vs.add_texts(["findable by its id"], [{"category": "temp", "year": 1}])
        got = vs.get_by_ids(ids)
        assert len(got) == 1 and got[0].page_content == "findable by its id", got

    def existing_index():
        reopened = RedisVectorStore.from_existing_index(index, EMB, redis_url=url, key_prefix=prefix)
        docs = reopened.similarity_search("the stock market fell", k=1)
        assert docs and docs[0].page_content == "the stock market fell sharply today", docs

    check(f"vectorstore {tag}: FT.CREATE and add_texts", build)
    if vs is None:
        return index, prefix
    check(f"vectorstore {tag}: nearest neighbour is the query itself", nearest)
    check(f"vectorstore {tag}: scores ascend, exact match near 0", scores)
    check(f"vectorstore {tag}: metadata comes back", metadata_roundtrip)
    check(f"vectorstore {tag}: tag filter", tag_filter)
    check(f"vectorstore {tag}: numeric filter", numeric_filter)
    check(f"vectorstore {tag}: tag and numeric filter together", combined_filter)
    check(f"vectorstore {tag}: as_retriever", retriever)
    check(f"vectorstore {tag}: max marginal relevance", mmr)
    check(f"vectorstore {tag}: add then delete", add_and_delete)
    check(f"vectorstore {tag}: get_by_ids", by_ids)
    check(f"vectorstore {tag}: from_existing_index", existing_index)
    return index, prefix


def llm_cache(url, r):
    prefix = unique("lccache")
    cache = RedisCache(redis_url=url, prefix=prefix)
    llm = "fake-llm-v1"

    def roundtrip():
        assert cache.lookup("what is 2+2", llm) is None, "hit on an empty cache"
        cache.update("what is 2+2", llm, [Generation(text="4")])
        got = cache.lookup("what is 2+2", llm)
        assert got and got[0].text == "4", got

    def other_model_misses():
        cache.update("shared prompt", "model-a", [Generation(text="from a")])
        assert cache.lookup("shared prompt", "model-b") is None, "model-b hit model-a's entry"

    def clear():
        cache.update("to be cleared", llm, [Generation(text="x")])
        cache.clear()
        assert cache.lookup("to be cleared", llm) is None, "still cached after clear"

    check("llm cache: update then lookup", roundtrip)
    check("llm cache: another model does not hit", other_model_misses)
    check("llm cache: clear", clear)
    for key in r.scan_iter(match=f"{prefix}*", count=500):
        r.delete(key)


def semantic_cache(url, r):
    name, prefix = unique("lcsem"), unique("lcsemk")
    llm = "fake-llm-v1"
    cache = RedisSemanticCache(EMB, redis_url=url, distance_threshold=0.3, name=name, prefix=prefix)

    def hit():
        cache.update("what is the capital of france", llm, [Generation(text="Paris")])
        got = cache.lookup("the capital of france", llm)
        assert got and got[0].text == "Paris", got

    def miss():
        got = cache.lookup("how do i bake sourdough bread", llm)
        assert got is None, got

    def exact():
        got = cache.lookup("what is the capital of france", llm)
        assert got and got[0].text == "Paris", got

    check("semantic cache: update then a close prompt hits", hit)
    check("semantic cache: an exact prompt hits", exact)
    check("semantic cache: a far prompt misses", miss)
    drop(r, name, prefix)


def chat_history(url, r):
    session, prefix, index = unique("lcsess"), unique("lcchat:"), unique("lcchatidx")
    hist = RedisChatMessageHistory(session, redis_url=url, key_prefix=prefix, index_name=index)

    def order():
        hist.add_message(HumanMessage(content="hello"))
        hist.add_message(AIMessage(content="hi, how can I help?"))
        hist.add_message(HumanMessage(content="what is KiviDB?"))
        msgs = hist.messages
        assert [m.content for m in msgs] == ["hello", "hi, how can I help?", "what is KiviDB?"], \
            [m.content for m in msgs]
        assert [m.type for m in msgs] == ["human", "ai", "human"], [m.type for m in msgs]

    def clear():
        hist.clear()
        assert hist.messages == [], hist.messages

    check("chat history: messages read back in order", order)
    check("chat history: clear", clear)
    drop(r, index, prefix)


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="redis://localhost:6380")
    ap.add_argument("--label", default="kividb")
    args = ap.parse_args()

    r = redis.Redis.from_url(args.url)
    info = r.info("server")
    version = info.get("redis_version", "?")
    print(f"server {args.label} at {args.url} (redis_version {version})")
    check("connection: PING", lambda: r.ping() or "")

    cleanups = []
    for algorithm in ("FLAT", "HNSW"):
        cleanups.append(run_group(f"RedisVectorStore ({algorithm})", vector_store, args.url, r, algorithm))
    run_group("RedisCache", llm_cache, args.url, r)
    run_group("RedisSemanticCache", semantic_cache, args.url, r)
    run_group("RedisChatMessageHistory", chat_history, args.url, r)
    for done in cleanups:
        if done:
            drop(r, *done)

    counts = {s: sum(1 for x in results if x["status"] == s) for s in ("PASS", "WARN", "FAIL")}
    print(f"\n{args.label}: {counts['PASS']} PASS, {counts['WARN']} WARN, {counts['FAIL']} FAIL "
          f"of {len(results)} checks")
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"results-{args.label}.json")
    with open(out, "w") as f:
        json.dump({"label": args.label, "url": args.url, "redis_version": version,
                   "langchain_redis": __import__("importlib.metadata", fromlist=["version"]).version("langchain-redis"),
                   "counts": counts, "checks": results}, f, indent=2)
    print(f"wrote {out}")
    return 1 if counts["FAIL"] else 0


if __name__ == "__main__":
    sys.exit(main())
