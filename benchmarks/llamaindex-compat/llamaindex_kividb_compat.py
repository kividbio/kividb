#!/usr/bin/env python3
"""
LlamaIndex <-> Redis-protocol server compatibility harness.

Runs LlamaIndex's Redis integrations against any server (KiviDB, Redis 8, Valkey) and records
PASS / FAIL / WARN per check. A deterministic stub plays the embedding model, so no real API keys
or spend are involved.

  pip install -r requirements.txt
  python llamaindex_kividb_compat.py --url redis://HOST:6380 --label kividb
  python llamaindex_kividb_compat.py --url redis://HOST:6379 --label redis8

Writes results-<label>.json next to the script. Run the same command against stock Redis (for
example the `redis:8` image) with a different --label to get a comparison.

What it covers:
  * llama-index-vector-stores-redis: RedisVectorStore on a FLAT and on an HNSW index. add, query,
    scores, top_k, metadata filters (==, !=, >, IN, AND, OR, text match), hybrid and MMR queries,
    get_nodes, delete_nodes by id and by filter, and deleting the index. Through a
    VectorStoreIndex: build from documents, retrieve, reopen from the vector store, delete a
    document.
  * llama-index-storage-docstore-redis: RedisDocumentStore (add, get, exists, delete, hashes,
    nodes, the reference-document bookkeeping), and an IngestionPipeline that uses it to skip
    unchanged documents.
  * llama-index-storage-index-store-redis: RedisIndexStore (add, get, list, delete), and an index
    persisted and loaded back from storage.
  * llama-index-storage-kvstore-redis: RedisKVStore, sync and async.

Left out on purpose, because it behaves the same way on Redis 8.10.3 (so it says nothing about a
server):
  * RedisVectorStore.clear() raises NotImplementedError in the library
  * the "text_search" query mode needs an embedding, in the library
  * node ids are chosen so that RedisVectorStore.add() returns them intact: it removes the key
    prefix with str.strip(), which cuts any id that starts or ends with a character of the prefix
    (https://github.com/run-llama/llama_index/issues/21483)

NOTE: the stub embedding is a hashed bag of words. It proves the wiring (index creation, search,
filters), not semantic quality.

Everything it creates has a random prefix and is removed at the end, including the indexes the
library creates, which are found by comparing FT._LIST before and after.
"""
import argparse
import asyncio
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
from llama_index.core import (
    Document,
    StorageContext,
    SummaryIndex,
    VectorStoreIndex,
    load_index_from_storage,
)
from llama_index.core.data_structs.data_structs import IndexGraph
from llama_index.core.embeddings import BaseEmbedding
from llama_index.core.ingestion import DocstoreStrategy, IngestionPipeline
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.schema import NodeRelationship, RelatedNodeInfo, TextNode
from llama_index.core.vector_stores.types import (
    FilterCondition,
    FilterOperator,
    MetadataFilter,
    MetadataFilters,
    VectorStoreQuery,
    VectorStoreQueryMode,
)
from llama_index.storage.docstore.redis import RedisDocumentStore
from llama_index.storage.index_store.redis import RedisIndexStore
from llama_index.storage.kvstore.redis import RedisKVStore
from llama_index.vector_stores.redis import RedisVectorStore
from redis.asyncio import Redis as AsyncRedis
from redisvl.schema import IndexSchema

DIM = 64
results = []
PREFIXES = []  # every random prefix used, so its keys can be removed


# ---------------------------------------------------------------- stub embedding model
class HashEmbedding(BaseEmbedding):
    """Hashed bag of words, unit length: deterministic and offline."""

    def _vec(self, text):
        v = [0.0] * DIM
        for tok in re.findall(r"[a-z0-9]+", text.lower()):
            v[int(hashlib.md5(tok.encode()).hexdigest(), 16) % DIM] += 1.0
        n = math.sqrt(sum(x * x for x in v)) or 1.0
        return [x / n for x in v]

    def _get_query_embedding(self, query):
        return self._vec(query)

    def _get_text_embedding(self, text):
        return self._vec(text)

    async def _aget_query_embedding(self, query):
        return self._vec(query)


EMB = HashEmbedding()


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
    """Drop the indexes created since `before`, with their documents, then any key that has
    one of this run's random names in it."""
    for idx in indexes(r) - before:
        try:
            r.execute_command("FT.DROPINDEX", idx, "DD")
        except Exception:  # noqa
            pass
    for prefix in PREFIXES:
        for key in r.scan_iter(match=f"*{prefix}*", count=500):
            r.delete(key)


# ---------------------------------------------------------------- the vector store
# Uppercase ids: the key prefix is lowercase, so add() cannot cut them (see the note above).
CORPUS = {
    "NODE-CATS": ("the cat sat on the warm mat", "animals", 2020),
    "NODE-RATES": ("interest rates rose again this quarter", "finance", 2021),
    "NODE-RUST": ("a new rust compiler release is out", "tech", 2022),
    "NODE-DOGS": ("the dog chased the ball in the park", "animals", 2023),
}


def make_schema(name, algorithm):
    return IndexSchema.from_dict({
        "index": {"name": name, "prefix": name, "key_separator": ":"},
        "fields": [
            {"name": "id", "type": "tag"},
            {"name": "doc_id", "type": "tag"},
            {"name": "text", "type": "text"},
            {"name": "topic", "type": "tag"},
            {"name": "year", "type": "numeric"},
            {"name": "vector", "type": "vector",
             "attrs": {"dims": DIM, "algorithm": algorithm, "distance_metric": "cosine"}},
        ],
    })


def nodes():
    return [TextNode(text=txt, id_=nid, metadata={"topic": topic, "year": year},
                     embedding=EMB.get_text_embedding(txt))
            for nid, (txt, topic, year) in CORPUS.items()]


def vq(text, k=3, **kw):
    return VectorStoreQuery(query_embedding=EMB.get_query_embedding(text), similarity_top_k=k, **kw)


def flt(*filters, condition=FilterCondition.AND):
    return MetadataFilters(filters=list(filters), condition=condition)


def ids(result):
    """Result ids, without the index prefix."""
    return sorted(i.split(":", 1)[-1] for i in result.ids)


def vector_store(r, algorithm):
    name = unique(f"lix{algorithm[0]}")
    vs = RedisVectorStore(schema=make_schema(name, algorithm), redis_client=r)
    label = f"vector store ({algorithm})"

    def add():
        vs.add(nodes())
        assert vs.index_exists(), "index does not exist after add"

    check(f"{label}: add creates the index and stores the nodes", add)

    def nearest():
        top = vs.query(vq("interest rates rose")).ids
        assert top and top[0].endswith("NODE-RATES"), top

    def scores():
        res = vs.query(vq("interest rates rose"))
        assert res.similarities and res.similarities[0] > 0.5, res.similarities
        assert res.similarities == sorted(res.similarities, reverse=True), res.similarities

    def top_k():
        assert len(vs.query(vq("the", k=1)).ids) == 1
        assert len(vs.query(vq("the", k=4)).ids) == 4

    check(f"{label}: query returns the closest node first", nearest)
    check(f"{label}: similarities are ordered, best first", scores)
    check(f"{label}: similarity_top_k is honoured", top_k)

    def f_eq():
        got = ids(vs.query(vq("x", 10, filters=flt(MetadataFilter(key="topic", value="animals")))))
        assert got == ["NODE-CATS", "NODE-DOGS"], got

    def f_ne():
        got = ids(vs.query(vq("x", 10, filters=flt(
            MetadataFilter(key="topic", value="animals", operator=FilterOperator.NE)))))
        assert got == ["NODE-RATES", "NODE-RUST"], got

    def f_gt():
        got = ids(vs.query(vq("x", 10, filters=flt(
            MetadataFilter(key="year", value=2021, operator=FilterOperator.GT)))))
        assert got == ["NODE-DOGS", "NODE-RUST"], got

    def f_in():
        got = ids(vs.query(vq("x", 10, filters=flt(
            MetadataFilter(key="topic", value=["tech", "finance"], operator=FilterOperator.IN)))))
        assert got == ["NODE-RATES", "NODE-RUST"], got

    def f_and():
        got = ids(vs.query(vq("x", 10, filters=flt(
            MetadataFilter(key="topic", value="animals"),
            MetadataFilter(key="year", value=2022, operator=FilterOperator.GT)))))
        assert got == ["NODE-DOGS"], got

    def f_or():
        got = ids(vs.query(vq("x", 10, filters=flt(
            MetadataFilter(key="topic", value="tech"), MetadataFilter(key="topic", value="finance"),
            condition=FilterCondition.OR))))
        assert got == ["NODE-RATES", "NODE-RUST"], got

    def f_text():
        got = ids(vs.query(vq("x", 10, filters=flt(
            MetadataFilter(key="text", value="rust", operator=FilterOperator.TEXT_MATCH)))))
        assert got == ["NODE-RUST"], got

    check(f"{label}: filter ==", f_eq)
    check(f"{label}: filter !=", f_ne)
    check(f"{label}: filter > on a number", f_gt)
    check(f"{label}: filter IN", f_in)
    check(f"{label}: filters combined with AND", f_and)
    check(f"{label}: filters combined with OR", f_or)
    check(f"{label}: filter on text (TEXT_MATCH)", f_text)

    def hybrid():
        res = vs.query(vq("cat", 3, query_str="cat", mode=VectorStoreQueryMode.HYBRID, alpha=0.5))
        assert res.ids and res.ids[0].endswith("NODE-CATS"), res.ids

    def mmr():
        res = vs.query(vq("cat", 2, mode=VectorStoreQueryMode.MMR, mmr_threshold=0.5))
        assert len(res.ids) == 2, res.ids

    check(f"{label}: hybrid query (vector and text)", hybrid)
    check(f"{label}: MMR query", mmr)

    def get_nodes():
        got = sorted(n.node_id for n in vs.get_nodes(node_ids=["NODE-CATS", "NODE-RUST"]))
        assert got == ["NODE-CATS", "NODE-RUST"], got

    def delete_ids():
        vs.delete_nodes(node_ids=["NODE-RUST"])
        got = ids(vs.query(vq("x", 10)))
        assert "NODE-RUST" not in got and len(got) == 3, got

    def delete_filter():
        vs.delete_nodes(filters=flt(MetadataFilter(key="topic", value="finance")))
        got = ids(vs.query(vq("x", 10)))
        assert got == ["NODE-CATS", "NODE-DOGS"], got

    def drop():
        vs.delete_index()
        assert not vs.index_exists(), "index still exists"

    check(f"{label}: get_nodes by id", get_nodes)
    check(f"{label}: delete_nodes by id", delete_ids)
    check(f"{label}: delete_nodes by filter", delete_filter)
    check(f"{label}: delete_index", drop)


def vector_index(r):
    name = unique("lixe")
    vs = RedisVectorStore(schema=make_schema(name, "flat"), redis_client=r)
    docs = [Document(text=txt, doc_id=f"DOC-{nid[5:]}") for nid, (txt, _, _) in CORPUS.items()]

    def build():
        index = VectorStoreIndex.from_documents(
            docs, storage_context=StorageContext.from_defaults(vector_store=vs), embed_model=EMB)
        top = index.as_retriever(similarity_top_k=2).retrieve("interest rates rose")
        assert top and top[0].node.ref_doc_id == "DOC-RATES", [n.node.ref_doc_id for n in top]

    def reopen():
        index = VectorStoreIndex.from_vector_store(vs, embed_model=EMB)
        top = index.as_retriever(similarity_top_k=2).retrieve("a new rust compiler")
        assert top and top[0].node.ref_doc_id == "DOC-RUST", [n.node.ref_doc_id for n in top]

    def delete_doc():
        index = VectorStoreIndex.from_vector_store(vs, embed_model=EMB)
        index.delete_ref_doc("DOC-RATES", delete_from_docstore=True)
        got = [n.node.ref_doc_id for n in index.as_retriever(similarity_top_k=4).retrieve("rates")]
        assert "DOC-RATES" not in got and len(got) == 3, got

    check("index: VectorStoreIndex from documents, then retrieve", build)
    check("index: reopen from the vector store and retrieve", reopen)
    check("index: delete_ref_doc removes the document's nodes", delete_doc)
    vs.delete_index()


# ---------------------------------------------------------------- docstore, index store, kv store
def docstore(r):
    ns = unique("lixd")
    ds = RedisDocumentStore.from_redis_client(r, namespace=ns)
    docs = [Document(text="alpha text", doc_id="d1", metadata={"k": "v"}),
            Document(text="beta text", doc_id="d2")]

    def add_get():
        ds.add_documents(docs)
        assert ds.get_document("d1").text == "alpha text"
        assert ds.get_document("d1").metadata == {"k": "v"}
        assert sorted(ds.docs) == ["d1", "d2"], sorted(ds.docs)

    def exists():
        assert ds.document_exists("d1") and not ds.document_exists("nope")
        assert ds.get_document("nope", raise_error=False) is None

    def no_overwrite():
        try:
            ds.add_documents(docs, allow_update=False)
        except ValueError:
            return
        raise AssertionError("an existing id was overwritten with allow_update=False")

    def hashes():
        ds.set_document_hash("d1", "h1")
        assert ds.get_document_hash("d1") == "h1"
        assert ds.get_all_document_hashes().get("h1") == "d1", ds.get_all_document_hashes()

    def delete():
        ds.delete_document("d1")
        assert not ds.document_exists("d1")
        assert ds.get_document_hash("d1") is None
        ds.delete_document("nope", raise_error=False)

    def ref_docs():
        src = {NodeRelationship.SOURCE: RelatedNodeInfo(node_id="REF")}
        ds.add_documents([TextNode(text="n1", id_="n1", relationships=src),
                          TextNode(text="n2", id_="n2", relationships=src)])
        info = ds.get_ref_doc_info("REF")
        assert info is not None and sorted(info.node_ids) == ["n1", "n2"], info
        assert "REF" in ds.get_all_ref_doc_info()
        assert [n.node_id for n in ds.get_nodes(["n1", "n2"])] == ["n1", "n2"]
        ds.delete_ref_doc("REF")
        assert not ds.document_exists("n1") and ds.get_ref_doc_info("REF") is None

    check("docstore: add and get documents", add_get)
    check("docstore: exists, and a missing id", exists)
    check("docstore: allow_update=False refuses an existing id", no_overwrite)
    check("docstore: document hashes", hashes)
    check("docstore: delete a document and its hash", delete)
    check("docstore: nodes of a reference document, and delete_ref_doc", ref_docs)

    def pipeline():
        ds2 = RedisDocumentStore.from_redis_client(r, namespace=unique("lixp"))
        pl = IngestionPipeline(transformations=[SentenceSplitter(chunk_size=64, chunk_overlap=0)],
                               docstore=ds2, docstore_strategy=DocstoreStrategy.UPSERTS)
        doc = [Document(text="Cats sleep a lot. Dogs run in the park.", doc_id="p1")]
        assert len(pl.run(documents=doc)) == 1, "first run produced no nodes"
        assert len(pl.run(documents=doc)) == 0, "an unchanged document was processed again"
        changed = [Document(text="Totally new text here.", doc_id="p1")]
        assert len(pl.run(documents=changed)) == 1, "a changed document was skipped"

    check("ingestion pipeline: skips an unchanged document, redoes a changed one", pipeline)


def index_store(r):
    ns = unique("lixi")
    ix = RedisIndexStore.from_redis_client(r, namespace=ns)
    g = IndexGraph()

    def add_get():
        ix.add_index_struct(g)
        assert ix.get_index_struct(g.index_id).index_id == g.index_id

    def listing():
        assert [s.index_id for s in ix.index_structs()] == [g.index_id]

    def delete():
        ix.delete_index_struct(g.index_id)
        assert ix.get_index_struct(g.index_id) is None

    check("index store: add and get an index struct", add_get)
    check("index store: list index structs", listing)
    check("index store: delete an index struct", delete)

    def persisted():
        sp = unique("lixs")

        def ctx():
            return StorageContext.from_defaults(
                docstore=RedisDocumentStore.from_redis_client(r, namespace=sp),
                index_store=RedisIndexStore.from_redis_client(r, namespace=sp))

        index = SummaryIndex.from_documents([Document(text="persist me", doc_id="s1")],
                                            storage_context=ctx())
        loaded = load_index_from_storage(ctx(), index_id=index.index_id)
        got = [n.node.get_content() for n in loaded.as_retriever().retrieve("anything")]
        assert got == ["persist me"], got

    check("storage: an index is saved, then loaded back from a new storage context", persisted)


def kv_store(r, url):
    kv = RedisKVStore.from_redis_client(r)
    col = unique("lixk")

    def put_get():
        kv.put("a", {"x": 1}, collection=col)
        assert kv.get("a", collection=col) == {"x": 1}
        assert kv.get("nope", collection=col) is None

    def put_all():
        kv.put_all([("b", {"x": 2}), ("c", {"x": 3})], collection=col)
        assert sorted(kv.get_all(collection=col)) == ["a", "b", "c"]

    def delete():
        assert kv.delete("a", collection=col) is True
        assert kv.get("a", collection=col) is None
        assert kv.delete("a", collection=col) is False

    def nested():
        value = {"a": [1, 2, {"b": None}], "s": "é😀", "f": 1.5}
        kv.put("n", value, collection=col)
        assert kv.get("n", collection=col) == value

    def asynchronous():
        async def go():
            akv = RedisKVStore(redis_uri=url, async_redis_client=AsyncRedis.from_url(url))
            acol = col + "a"
            await akv.aput("q", {"y": 1}, collection=acol)
            assert await akv.aget("q", collection=acol) == {"y": 1}
            await akv.aput_all([("r", {"y": 2})], collection=acol)
            assert sorted(await akv.aget_all(collection=acol)) == ["q", "r"]
            await akv.adelete("q", collection=acol)
            assert await akv.aget("q", collection=acol) is None

        asyncio.run(go())

    check("kv store: put and get, and a missing key", put_get)
    check("kv store: put_all and get_all", put_all)
    check("kv store: delete reports whether the key existed", delete)
    check("kv store: nested values and unicode", nested)
    check("kv store: async put, get, get_all, delete", asynchronous)


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

    run_group("RedisVectorStore, FLAT index", vector_store, r, "flat")
    run_group("RedisVectorStore, HNSW index", vector_store, r, "hnsw")
    run_group("VectorStoreIndex on the Redis vector store", vector_index, r)
    run_group("RedisDocumentStore", docstore, r)
    run_group("RedisIndexStore", index_store, r)
    run_group("RedisKVStore", kv_store, r, args.url)
    cleanup(r, start)

    counts = {s: sum(1 for x in results if x["status"] == s) for s in ("PASS", "WARN", "FAIL")}
    print(f"\n{args.label}: {counts['PASS']} PASS, {counts['WARN']} WARN, {counts['FAIL']} FAIL "
          f"of {len(results)} checks")
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"results-{args.label}.json")
    with open(out, "w") as f:
        json.dump({"label": args.label, "url": args.url, "redis_version": server_version,
                   "llama_index_core": version("llama-index-core"),
                   "llama_index_vector_stores_redis": version("llama-index-vector-stores-redis"),
                   "llama_index_storage_docstore_redis": version("llama-index-storage-docstore-redis"),
                   "llama_index_storage_index_store_redis": version("llama-index-storage-index-store-redis"),
                   "llama_index_storage_kvstore_redis": version("llama-index-storage-kvstore-redis"),
                   "redisvl": version("redisvl"), "redis_py": version("redis"),
                   "counts": counts, "checks": results}, f, indent=2)
    print(f"wrote {out}")
    return 1 if counts["FAIL"] else 0


if __name__ == "__main__":
    sys.exit(main())
