#!/usr/bin/env python3
"""#760: an FT.* HNSW index built from one pipelined client, and KNN at recall.

The shape of #760: `FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA score NUMERIC v
VECTOR HNSW 10 TYPE FLOAT32 DIM 384 DISTANCE_METRIC COSINE M 16
EF_CONSTRUCTION 200`, then 50,000 documents of random vectors (numpy seed 760,
uniform in [0, 1)) written by ONE client in HSET pipelines of 1,000. The clock
runs until every document is in the index and nothing waits to be linked
(KiviDB's FT.INFO `hnsw_backlog`; Redis's tiered buffer, `_FT.DEBUG
VECSIM_INFO`). Up to 1.2.2 KiviDB linked each vector inside its write, on the
writing connection's thread: one core for one client. Since #760 helper
threads link them (`src/vector/backlog.rs`).

Then 300 queries (seed 761): recall@10 against exact cosine, and 5 s of one
client at EF_RUNTIME 100 (KNN 10, NOCONTENT): q/s, p50, p99. Random vectors
make for low recall at any EF (about 0.55 here); what matters is that it does
not move. A real dataset's recall is measured by vector-benchmark.yml.

Engines: v1.2.2 and the candidate. Redis 8.10.2 runs too when
REGRESS_REDISEARCH_SO names the query engine module (`redisearch.so` of the
redis:8.10.2 image: the VM's source-built Redis has no FT.*); it is started
with `--loadmodule` and the configuration is self-hosted for that reason.

Gates: build docs/s within 10 % and keeping 60 % of the gain over v1.2.2, and
not more than 1.5x slower than Redis when it ran (1.28x on a D8s_v5: the
helpers are half the CPUs, which keeps GET/SET beside a build at its 1.2
rate); KNN q/s within 10 %;
recall@10 not more than 2 % under recorded (it moves with the graph, which
parallel linking builds in another order each run); peak RSS growth a document
within 5 %.
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, KEEP_IMPROVEMENT, rel, vs_redis, info, scaled, median  # noqa: E402
from nodes import Nodes  # noqa: E402
from rss import vmrss_mib  # noqa: E402

MODULE = os.environ.get("REGRESS_REDISEARCH_SO", "")
ENGINES = ("v1_2_2", "candidate") + (("redis",) if MODULE else ())
S = Suite("28-ft-hnsw", [Config("pipeline", engines=ENGINES, self_hosted=True)])
DIM, M, EFC, EF, BATCH = 384, 16, 200, 100, 1000


def field(info_reply, name):
    if isinstance(info_reply, dict):
        d = info_reply
    else:
        d = dict(zip(info_reply[::2], info_reply[1::2]))
    v = d.get(name.encode(), d.get(name))
    return v.decode() if isinstance(v, bytes) else v


def waiting(c, kind):
    """Vectors written and not linked into the graph yet."""
    if kind == "redis":
        raw = c.execute_command("_FT.DEBUG", "VECSIM_INFO", "idx", "v")
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
    return int(field(c.execute_command("FT.INFO", "idx"), "hnsw_backlog") or 0)


@S.run
def run(ctx):
    import numpy as np
    args = ("--loadmodule", MODULE) if ctx.kind == "redis" else ()
    with Nodes(ctx) as ns:
        node = ns.start(args=args)
        c = node.client(protocol=2)
        n = scaled(50_000)
        rng = np.random.default_rng(760)
        vecs = rng.random((n, DIM), dtype=np.float32)
        queries = np.random.default_rng(761).random((300, DIM), dtype=np.float32)
        c.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA",
                          "score", "NUMERIC", "v", "VECTOR", "HNSW", "10", "TYPE", "FLOAT32",
                          "DIM", DIM, "DISTANCE_METRIC", "COSINE", "M", M, "EF_CONSTRUCTION", EFC)
        rss0 = vmrss_mib(node.pid)
        t0 = time.perf_counter()
        for s in range(0, n, BATCH):
            p = c.pipeline(transaction=False)
            for i in range(s, min(n, s + BATCH)):
                p.hset("doc:%d" % i, mapping={"v": vecs[i].tobytes(), "score": i % 101})
            p.execute()
        acked = time.perf_counter() - t0
        deadline = time.time() + 900
        while True:
            docs = int(field(c.execute_command("FT.INFO", "idx"), "num_docs"))
            if docs >= n and waiting(c, ctx.kind) == 0:
                break
            if time.time() > deadline:
                raise RuntimeError("the index was not built in 900 s")
            time.sleep(0.02)
        built = time.perf_counter() - t0
        ctx.emit("build_docs_per_s", n / built, "docs/s", "higher",
                 gate=[rel(10), KEEP_IMPROVEMENT, vs_redis(1.5)])
        ctx.emit("acked_docs_per_s", n / acked, "docs/s", "higher", gate=info())
        ctx.emit("rss_per_doc", (vmrss_mib(node.pid) - rss0) * 1048576 / n, "B", "lower", gate=rel(5))

        # Recall@10 against exact cosine.
        vn = vecs / np.linalg.norm(vecs, axis=1, keepdims=True)
        qn = queries / np.linalg.norm(queries, axis=1, keepdims=True)
        truth = np.argsort(-(qn @ vn.T), axis=1)[:, :10]
        q = "*=>[KNN 10 @v $vec EF_RUNTIME %d]" % EF
        blobs = [x.tobytes() for x in queries]

        def search(blob):
            return c.execute_command("FT.SEARCH", "idx", q, "PARAMS", "2", "vec", blob,
                                     "NOCONTENT", "LIMIT", "0", "10", "DIALECT", "2")
        hits = 0
        for i, blob in enumerate(blobs):
            got = {int(k.decode().split(":")[1]) for k in search(blob)[1:]}
            hits += len(got & set(truth[i].tolist()))
        ctx.emit("knn_recall_at_10", hits / (10.0 * len(blobs)), "ratio", "higher", gate=rel(2))

        # One client, 5 s, three times.
        rates, p50s, p99s = [], [], []
        for _ in range(3):
            lat = []
            end = time.perf_counter() + 5
            i = 0
            while time.perf_counter() < end:
                t = time.perf_counter()
                search(blobs[i % len(blobs)])
                lat.append((time.perf_counter() - t) * 1000)
                i += 1
            lat.sort()
            rates.append(len(lat) / 5.0)
            p50s.append(lat[len(lat) // 2])
            p99s.append(lat[int(len(lat) * 0.99)])
        ctx.emit("knn_qps_ef100", median(rates), "q/s", "higher", gate=rel(10), samples=rates)
        ctx.emit("knn_p99_ms_ef100", median(p99s), "ms", "lower", gate=info(), samples=p99s)
        ctx.raw["p50_ms"] = p50s


if __name__ == "__main__":
    sys.exit(S.main())
