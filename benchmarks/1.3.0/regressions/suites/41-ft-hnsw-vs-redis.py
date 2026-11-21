#!/usr/bin/env python3
"""#760's own cases against Redis's query engine, in the same session.

#760 compared one HNSW index on KiviDB and on Redis 8.10.3: `FT.CREATE idx ON
HASH PREFIX 1 doc: SCHEMA score NUMERIC v VECTOR HNSW 10 TYPE FLOAT32 DIM <d>
DISTANCE_METRIC COSINE M 16 EF_CONSTRUCTION 200`, documents of random vectors
(numpy seed 760, uniform in [0, 1)) with `score` = i % 101, written by ONE
client in HSET pipelines of 1,000, the clock running until every document is
in the index and nothing waits to be linked (KiviDB's FT.INFO `hnsw_backlog`;
Redis's tiered buffer, `_FT.DEBUG VECSIM_INFO`). Then `FT.SEARCH idx
"(<filter>)=>[KNN 10 @v $vec]" PARAMS 2 vec <blob> NOCONTENT LIMIT 0 10
DIALECT 2`, one client, 5 s a case, 300 queries (seed 761).

Configurations, the issue's two datasets: `d384` (50,000 x 384) and `d64`
(200,000 x 64). The candidate runs as it starts (`--threads 4`, the default
on the suite's 8-vCPU machine, and `search-index-threads auto`, which starts
seven helpers there and lets all of them link while the load is all the
server does, three while it serves clients): the issue compared default
settings.

Cases: no filter (`*`), a wide filter `@score:[50 100]` (half the documents)
and a narrow one `@score:[0 2]` (3 %), at the default EF_RUNTIME; no filter
at `EF_RUNTIME` 50, 100 and 200 and the wide filter at 50 and 100, where the
two engines do the same work. Each case: q/s, and recall@10 against the exact
nearest of the documents the filter admits (numpy, cosine). A KiviDB KNN looks
at no fewer than 50 candidates (docs/SEARCH.md): at the default it does the
work of `EF_RUNTIME 50` where Redis does that of 10, so its default rows are
slower than Redis's and their recall is three times Redis's; the rows at the
same EF compare the same work. `d64` also: the latency of unfiltered KNN at
the default EF while a second process writes over random documents, 500
`HSET`s a second (RediSearch#5867's shape). Peak RSS (`VmHWM`).

Engines: v1.2.2, the candidate and Redis 8.10.2 with its query engine module
(`REGRESS_REDISEARCH_SO`: `redisearch.so` of the redis:8.10.2 image, as for
suite 28; the VM's source-built Redis has no FT.*). Without it the suite runs
v1.2.2 and the candidate only, and the gates against Redis are skipped.

Gates: every rate and time within 10 % of the recorded value, recall within
2 %, peak RSS within 5 %; and against Redis in the same session: the build at
most the recorded gap (`BUILD_X`), the rows at the same EF at most 1.1x
slower than Redis without a filter and 1.2x with the wide one (`SAME_EF_X`)
and their recall at least Redis's, the narrow filter
faster than Redis, the default rows at most the recorded gap, peak RSS at most
Redis's. Recorded on a Standard_D8s_v5 (`min_cpus=8`).
"""
import multiprocessing
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, rel, vs_redis, info, jitter, scaled, median  # noqa: E402
from nodes import Nodes  # noqa: E402

MODULE = os.environ.get("REGRESS_REDISEARCH_SO", "")
ENGINES = ("v1_2_2", "candidate") + (("redis",) if MODULE else ())
DATASETS = {"d384": (50_000, 384), "d64": (200_000, 64)}
S = Suite("41-ft-hnsw-vs-redis",
          [Config(name, threads=4, engines=ENGINES, self_hosted=True) for name in DATASETS],
          min_cpus=8)
M, EFC, BATCH, SECS, NQ = 16, 200, 1000, 5.0, 300
WIDE, NARROW = (50, 100), (0, 2)
WRITES_PER_S = 500
# The candidate at most this many times slower than Redis this session.
# Build: what this suite recorded, with room for a run's noise and for the
# two CPUs a D8s_v5 comes with (Xeon 8370C or 8573C). With
# `search-index-threads auto` linking on all seven helpers during the load,
# d384 1.01-1.05x and d64 1.26-1.34x in three runs on an 8370C (they were
# 1.37x and 1.73x with three helpers; see the PRs of #760).
BUILD_X = {"d384": 1.25, "d64": 1.6}
# The same EF_RUNTIME on both: the same work. Without a filter KiviDB was
# from 0.96x to 1.22x Redis's q/s; with the wide one from 0.93x (d384, EF 100)
# to 1.35x, its recall higher in every case.
SAME_EF_X = {"none": 1.1, "wide": 1.2}
# At the default EF KiviDB does 5x Redis's work (see the docstring).
DEFAULT_X = {"none": 2.2, "wide": 1.8}


def field(info_reply, name):
    d = info_reply if isinstance(info_reply, dict) else dict(zip(info_reply[::2], info_reply[1::2]))
    v = d.get(name.encode(), d.get(name))
    return v.decode() if isinstance(v, bytes) else v


def waiting(c, kind):
    """Vectors written and not linked into the graph yet."""
    if kind == "redis":
        flat = []

        def walk(x):
            for y in x if isinstance(x, list) else [x]:
                if isinstance(y, list):
                    walk(y)
                else:
                    flat.append(y.decode() if isinstance(y, bytes) else y)
        walk(c.execute_command("_FT.DEBUG", "VECSIM_INFO", "idx", "v"))
        n = 0
        for i, v in enumerate(flat):
            if v == "BACKGROUND_INDEXING" and str(flat[i + 1]) != "0":
                n += 1
            if v == "FRONTEND_INDEX":
                n += int(flat[flat.index("INDEX_SIZE", i) + 1])
        return n
    return int(field(c.execute_command("FT.INFO", "idx"), "hnsw_backlog") or 0)


def vmhwm_mib(pid):
    with open("/proc/%d/status" % pid) as f:
        for line in f:
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) / 1024.0
    return None


def writer(port, n, dim, seconds, out):
    """WRITES_PER_S HSETs a second over random documents, for `seconds`."""
    import numpy as np
    import redis
    c = redis.Redis(host="127.0.0.1", port=port, socket_timeout=600, protocol=2)
    rng = np.random.default_rng(999)
    vecs = rng.random((4096, dim), dtype=np.float32)
    t0 = time.perf_counter()
    done = 0
    while time.perf_counter() - t0 < seconds:
        due = int((time.perf_counter() - t0) * WRITES_PER_S)
        while done < due:
            i = int(rng.integers(0, n))
            c.hset("doc:%d" % i, mapping={"v": vecs[done % 4096].tobytes(), "score": i % 101})
            done += 1
        time.sleep(0.001)
    out.put(done)


@S.run
def run(ctx):
    import numpy as np
    name = ctx.config.name
    n, dim = DATASETS[name]
    n = scaled(n)
    args = ("--loadmodule", MODULE) if ctx.kind == "redis" else ()
    with Nodes(ctx) as ns:
        node = ns.start(args=args)
        c = node.client(protocol=2)
        vecs = np.random.default_rng(760).random((n, dim), dtype=np.float32)
        scores = np.arange(n) % 101
        queries = np.random.default_rng(761).random((NQ, dim), dtype=np.float32)
        c.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA",
                          "score", "NUMERIC", "v", "VECTOR", "HNSW", "10", "TYPE", "FLOAT32",
                          "DIM", dim, "DISTANCE_METRIC", "COSINE", "M", M, "EF_CONSTRUCTION", EFC)
        t0 = time.perf_counter()
        for s in range(0, n, BATCH):
            p = c.pipeline(transaction=False)
            for i in range(s, min(n, s + BATCH)):
                p.hset("doc:%d" % i, mapping={"v": vecs[i].tobytes(), "score": int(scores[i])})
            p.execute()
        deadline = time.time() + 900
        while int(field(c.execute_command("FT.INFO", "idx"), "num_docs")) < n or waiting(c, ctx.kind):
            if time.time() > deadline:
                raise RuntimeError("the index was not built in 900 s")
            time.sleep(0.01)
        built = time.perf_counter() - t0
        ctx.emit("build_docs_per_s", n / built, "docs/s", "higher",
                 gate=[rel(10), vs_redis(BUILD_X[name])])

        vn = vecs / np.linalg.norm(vecs, axis=1, keepdims=True)
        qn = queries / np.linalg.norm(queries, axis=1, keepdims=True)
        sims = qn @ vn.T
        blobs = [q.tobytes() for q in queries]

        def search(q, blob):
            return c.execute_command("FT.SEARCH", "idx", q, "PARAMS", "2", "vec", blob,
                                     "NOCONTENT", "LIMIT", "0", "10", "DIALECT", "2")

        def case(label, where, ef, qps_gate, recall_gate):
            clause = "KNN 10 @v $vec" + ("" if ef is None else " EF_RUNTIME %d" % ef)
            q = "*=>[%s]" % clause if where is None else "(@score:[%d %d])=>[%s]" % (where + (clause,))
            s = sims if where is None else np.where(
                ((scores >= where[0]) & (scores <= where[1]))[None, :], sims, -np.inf)
            truth = np.argsort(-s, axis=1)[:, :10]
            hits = 0
            for i, blob in enumerate(blobs):
                got = {int(k.decode().split(":")[1]) for k in search(q, blob)[1:]}
                hits += len(got & set(truth[i].tolist()))
            ctx.emit("%s_recall_at_10" % label, hits / (10.0 * NQ), "ratio", "higher", gate=recall_gate)
            done = 0
            end = time.perf_counter() + SECS
            while time.perf_counter() < end:
                search(q, blobs[done % NQ])
                done += 1
            ctx.emit("%s_qps" % label, done / SECS, "q/s", "higher", gate=qps_gate)

        same = lambda where: [rel(10), vs_redis(SAME_EF_X[where])]  # noqa: E731
        recall_same = [rel(2), vs_redis(1.0)]
        case("none_default", None, None, [rel(10), vs_redis(DEFAULT_X["none"])], recall_same)
        case("wide_default", WIDE, None, [rel(10), vs_redis(DEFAULT_X["wide"])], recall_same)
        case("narrow_default", NARROW, None, [rel(10), vs_redis(1.0)], [rel(2)])
        for ef in (50, 100, 200):
            case("none_ef%d" % ef, None, ef, same("none"), recall_same)
        for ef in (50, 100):
            case("wide_ef%d" % ef, WIDE, ef, same("wide"), recall_same)

        if name == "d64":
            mp = multiprocessing.get_context("spawn")
            out = mp.Queue()
            w = mp.Process(target=writer, args=(node.port, n, dim, SECS + 1, out))
            w.start()
            time.sleep(0.5)
            lat = []
            end = time.perf_counter() + SECS
            i = 0
            while time.perf_counter() < end:
                t = time.perf_counter()
                search("*=>[KNN 10 @v $vec]", blobs[i % NQ])
                lat.append((time.perf_counter() - t) * 1000)
                i += 1
            writes = out.get(timeout=120)
            w.join()
            lat.sort()
            ctx.emit("knn_p99_ms_beside_writes", lat[int(len(lat) * 0.99)], "ms", "lower",
                     gate=jitter([rel(50, abs=0.2)]))
            ctx.emit("knn_max_ms_beside_writes", lat[-1], "ms", "lower", gate=info())
            ctx.raw["writes"] = writes
            ctx.raw["p50_ms_beside_writes"] = median(lat)
        ctx.emit("peak_rss_mib", vmhwm_mib(node.pid), "MiB", "lower", gate=[rel(5), vs_redis(1.0)])


if __name__ == "__main__":
    sys.exit(S.main())
