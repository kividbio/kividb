#!/usr/bin/env python3
"""JSON documents that hold an embedding: memory a document (#1001).

The shape of #1001, made small: DOCS (5,000) JSON documents, each with a
1,536-element `embedding` array of float32 values and a few short fields, as
langgraph-redis's `RedisStore` and spring-ai's `RedisVectorStore` write them:

  {"prefix":"memories","key":"doc:<i>","namespace":"user:42",
   "created_at":<n>,"content":"note <i> about topic <i % 50>",
   "embedding":[<1536 numbers>]}

each number written as Python's `json.dumps` writes `float(np.float32(x))`:
the float32 widened to a double and printed shortest-round-trip as a double
(about 20 characters a number, 30 KB a document). Written with `JSON.SET
doc:<i> $ <text>`, 50 documents a pipeline.

Configurations (each on a fresh server):

  plain  no index: the document's own storage
  flat   `FT.CREATE idx ON JSON PREFIX 1 doc:` with `$.prefix` TAG, `$.content`
         TEXT, `$.created_at` NUMERIC and `$.embedding` VECTOR FLAT FLOAT32
         1536 COSINE, created before the load (langgraph-redis's shape)
  hnsw   the same with VECTOR HNSW M 16 EF_CONSTRUCTION 200 (spring-ai's),
         read once every vector is in the graph

What `flat` and `hnsw` hold beyond `plain` is the index's share.

Per document: the growth of INFO used_memory over the load (settled, as
`31-zset-memory` reads it), of VmRSS, and of used_memory_peak (what the
load needed at its worst); MEMORY USAGE of 50 documents (`SAMPLES 0`, every
element counted) and JSON.DEBUG MEMORY of their `$.embedding`. `get_same`
is 1 when JSON.GET of a document returns the text it was written with.
`knn_hits` (index configurations) is the number of results of one KNN 10
query, which must be 10. `plain` also keeps, in the raw data, what JSON.GET
returns of a list of numbers written in different textual forms (`1` and
`1.0`, exponents, digits past a double's precision), for comparing how each
engine prints numbers back.

Engines: v1.2.2 and the candidate; Redis 8.10.x when REGRESS_REDISJSON_SO
and REGRESS_REDISEARCH_SO name `rejson.so` and `redisearch.so` of the
redis:8.10 image (the VM's source-built Redis has neither): it is started
with `--loadmodule` for both, so the configurations are self-hosted.

Gates: none that hold the line yet (this suite records where the bytes go
before the representation changes); `bytes_per_doc` within 5 % of recorded.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, MEMORY, EXACT, info, scaled  # noqa: E402
from nodes import Nodes  # noqa: E402
from rss import vmrss_mib, MIB  # noqa: E402

JSON_SO = os.environ.get("REGRESS_REDISJSON_SO", "")
SEARCH_SO = os.environ.get("REGRESS_REDISEARCH_SO", "")
ENGINES = ("v1_2_2", "candidate") + (("redis",) if JSON_SO and SEARCH_SO else ())
S = Suite("45-json-embedding-memory", [Config("plain", threads=4, engines=ENGINES, self_hosted=True),
                                       Config("flat", threads=4, engines=ENGINES, self_hosted=True),
                                       Config("hnsw", threads=4, engines=ENGINES, self_hosted=True)])
DOCS = 5000
DIM = 1536
BATCH = 50
SAMPLE = 50
# Textual forms a number can be written in, for JSON.GET to print back.
NUMBER_FORMS = ("[1,1.0,1.50,-0.0,0,1e5,1E5,1.5e-7,0.1,100000000000000000000,12345678901234567890,"
                "-0.12345678901234567890,3.4028234663852886e38,1.0000000000000002,5e-324]")


def field(info_reply, name):
    if isinstance(info_reply, dict):
        d = info_reply
    else:
        d = dict(zip(info_reply[::2], info_reply[1::2]))
    v = d.get(name.encode(), d.get(name))
    return v.decode() if isinstance(v, bytes) else v


def doc_text(i, rng):
    import numpy as np
    emb = rng.random(DIM, dtype=np.float32) * 2 - 1
    doc = {"prefix": "memories", "key": "doc:%d" % i, "namespace": "user:42",
           "created_at": 1732000000 + i, "content": "note %d about topic %d" % (i, i % 50),
           "embedding": emb.tolist()}
    return json.dumps(doc, separators=(",", ":"))


def create(c, kind):
    algo = ["FLAT", "6"] if kind == "flat" else ["HNSW", "10"]
    extra = [] if kind == "flat" else ["M", "16", "EF_CONSTRUCTION", "200"]
    c.execute_command("FT.CREATE", "idx", "ON", "JSON", "PREFIX", "1", "doc:", "SCHEMA",
                      "$.prefix", "AS", "prefix", "TAG",
                      "$.content", "AS", "content", "TEXT",
                      "$.created_at", "AS", "created_at", "NUMERIC",
                      "$.embedding", "AS", "embedding", "VECTOR", *algo,
                      "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "COSINE", *extra)


def redis_hnsw_waiting(c):
    """Vectors Redis has not linked into its graph yet (as 28-ft-hnsw reads it)."""
    raw = c.execute_command("_FT.DEBUG", "VECSIM_INFO", "idx", "embedding")
    flat = []

    def walk(x):
        for y in x if isinstance(x, list) else [x]:
            if isinstance(y, list):
                walk(y)
            else:
                flat.append(y.decode() if isinstance(y, bytes) else y)
    walk(raw)
    n = 0
    for i, v in enumerate(flat):
        if v == "BACKGROUND_INDEXING" and str(flat[i + 1]) != "0":
            n += 1
        if v == "FRONTEND_INDEX":
            j = flat.index("INDEX_SIZE", i)
            n += int(flat[j + 1])
    return n


def indexed(c, engine_kind, config, docs, limit=1800.0):
    """Wait until the index holds every document (and every vector is linked)."""
    end = time.time() + limit
    while time.time() < end:
        fi = c.execute_command("FT.INFO", "idx")
        n = int(float(field(fi, "num_docs") or 0))
        pct = field(fi, "percent_indexed")
        busy = pct is not None and float(pct) < 1.0
        if config == "hnsw":
            if engine_kind == "redis":
                busy = busy or redis_hnsw_waiting(c) > 0
            else:
                busy = busy or int(field(fi, "hnsw_backlog") or 0) > 0
        if n >= docs and not busy:
            return
        time.sleep(0.5)
    raise RuntimeError("index did not catch up with %d documents in %d s" % (docs, limit))


def settled(c, limit=60.0):
    """used_memory once it has stopped falling for 2 s."""
    low = c.info("memory")["used_memory"]
    end = time.perf_counter() + limit
    since = time.perf_counter()
    while time.perf_counter() < end and time.perf_counter() - since < 2.0:
        time.sleep(0.1)
        now = c.info("memory")["used_memory"]
        if now < low - 4096:
            since = time.perf_counter()
        low = min(low, now)
    return low


def debug_memory(c, key):
    r = c.execute_command("JSON.DEBUG", "MEMORY", key, "$.embedding")
    return int(r[0] if isinstance(r, list) else r)


@S.run
def run(ctx):
    import numpy as np
    cfg = ctx.config.name
    docs = scaled(DOCS, step=500)
    args = ("--loadmodule", JSON_SO, "--loadmodule", SEARCH_SO) if ctx.kind == "redis" else ()
    with Nodes(ctx) as ns:
        node = ns.start(args=args)
        c = node.client()
        if cfg != "plain":
            create(c, cfg)
        time.sleep(0.5)
        u0 = settled(c)
        r0 = vmrss_mib(node.pid)
        p0 = c.info("memory").get("used_memory_peak", 0)

        rng = np.random.default_rng(1001)
        first_text = None
        t = time.perf_counter()
        p = c.pipeline(transaction=False)
        for i in range(docs):
            text = doc_text(i, rng)
            if i == 0:
                first_text = text
            p.execute_command("JSON.SET", "doc:%d" % i, "$", text)
            if (i + 1) % BATCH == 0:
                p.execute()
        p.execute()
        if cfg != "plain":
            indexed(c, ctx.kind, cfg, docs)
        secs = time.perf_counter() - t
        assert c.dbsize() == docs, "DBSIZE %d != %d" % (c.dbsize(), docs)

        u1 = settled(c)
        r1 = vmrss_mib(node.pid)
        mem = c.info("memory")
        peak = mem.get("used_memory_peak", 0)
        ctx.emit("bytes_per_doc", (u1 - u0) / docs, "B", "lower", gate=[MEMORY])
        ctx.emit("rss_per_doc", (r1 - r0) * MIB / docs, "B", "lower", gate=info())
        ctx.emit("peak_per_doc", (max(peak, u1) - u0) / docs, "B", "lower", gate=info())

        keys = ["doc:%d" % (i * docs // SAMPLE) for i in range(SAMPLE)]
        usage = [int(c.execute_command("MEMORY", "USAGE", k, "SAMPLES", "0")) for k in keys]
        ctx.emit("memory_usage_per_doc", sum(usage) / len(usage), "B", "lower", gate=info())
        dbg = [debug_memory(c, k) for k in keys]
        ctx.emit("json_debug_memory_embedding", sum(dbg) / len(dbg), "B", "lower", gate=info())

        got = c.execute_command("JSON.GET", "doc:0")
        got = got.decode() if isinstance(got, bytes) else got
        ctx.emit("get_same", 1 if got == first_text else 0, "bool", "higher", gate=info())
        if got != first_text:
            at = next((k for k, (a, b) in enumerate(zip(got, first_text)) if a != b), min(len(got), len(first_text)))
            ctx.raw["get_first_difference"] = {"at": at, "sent": first_text[max(0, at - 40):at + 40],
                                               "got": got[max(0, at - 40):at + 40]}

        if cfg != "plain":
            q = (rng.random(DIM, dtype=np.float32) * 2 - 1).tobytes()
            r = c.execute_command("FT.SEARCH", "idx", "*=>[KNN 10 @embedding $v]", "PARAMS", "2", "v", q,
                                  "NOCONTENT", "DIALECT", "2")
            ctx.emit("knn_hits", (len(r) - 1) if isinstance(r, list) else 0, "docs", "higher", gate=EXACT)
        else:
            c.execute_command("JSON.SET", "numbers", "$", NUMBER_FORMS)
            back = c.execute_command("JSON.GET", "numbers")
            ctx.raw["number_forms"] = {"sent": NUMBER_FORMS,
                                       "got": back.decode() if isinstance(back, bytes) else back}

        ctx.raw["memory"] = {"used_memory_before": u0, "used_memory_after": u1, "rss_mib_before": r0,
                             "rss_mib_after": r1, "used_memory_peak": peak, "peak_before": p0,
                             "docs": docs, "doc_text_bytes": len(first_text), "load_s": round(secs, 2),
                             "memory_usage_samples": usage, "json_debug_samples": dbg}
        ctx.emit("load_s", secs, "s", "lower", gate=info())
        c.flushall()


if __name__ == "__main__":
    sys.exit(S.main())
