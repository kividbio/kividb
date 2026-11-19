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

Configurations: `pipeline`, the candidate as it starts (`search-index-threads
auto`, which on the suite's D4s_v5 starts one helper); `pipeline-n3`, the
candidate with `--search-index-threads 3`, what `auto` gives on 8 CPUs, so
that the build's gain stays gated whatever the default (v1.2.2 and Redis run
as in `pipeline`).

Gates: build docs/s within 10 %; for `pipeline-n3` also keeping 60 % of the
gain over v1.2.2 and not more than 1.5x slower than Redis when it ran; KNN
q/s within 10 %;
recall@10 not more than 2 % under recorded (it moves with the graph, which
parallel linking builds in another order each run); peak RSS growth a document
within 5 %.

`beside`: what the helpers cost the rest of a busy server. Of the first four
CPUs this process may use, the server on the first and third (separate cores
on a D4s_v5), `--threads 2`; memtier_benchmark (GET/SET 1:1, 1 thread, 20
connections, 100,000 keys of 64 bytes) on the second; the loading client
and a KNN probe (`EF_RUNTIME 100`, one query at a time over 8 connections
in turn), separate processes, on the fourth. 20,000 documents first, then
for 20 s, five times (medians of the five): the load writes on, memtier
runs, the probe searches. The probe's KNN p50 is that of its fastest
connection: one on the loading client's worker waits behind its pipelines
(1.2's way too), which says where the connection landed, not what the
helpers cost. Gated: GET/SET p99 beside the
build and the probe's KNN p50 beside the ingest, each within its recorded
value and at most 2x v1.2.2's in the same session: on two CPUs
`search-index-threads auto` starts no helper, so the candidate links in the
writes as 1.2.2 does, and serving beside a build must not get worse by
default.
Documents ingested a second and GET/SET a second are recorded.
Needs memtier_benchmark (`$MEMTIER`, else on PATH).
"""
import multiprocessing
import os
import json
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (  # noqa: E402
    Suite, Config, KEEP_IMPROVEMENT, rel, vs_redis, vs_v1_2_2, info, jitter, scaled, median)
from nodes import Nodes  # noqa: E402
from rss import vmrss_mib  # noqa: E402

MODULE = os.environ.get("REGRESS_REDISEARCH_SO", "")
ENGINES = ("v1_2_2", "candidate") + (("redis",) if MODULE else ())
S = Suite("28-ft-hnsw", [Config("pipeline", threads=4, engines=ENGINES, self_hosted=True),
                         Config("pipeline-n3", threads=4, engines=ENGINES, self_hosted=True),
                         Config("beside", threads=2, engines=ENGINES, self_hosted=True)])
# The helper count of `pipeline-n3`'s candidate: `auto` on 8 CPUs.
EXPLICIT_HELPERS = 3
DIM, M, EFC, EF, BATCH = 384, 16, 200, 100, 1000
BESIDE_S, ROUNDS, PROBE_CONNS = 20, 5, 8
# The candidate at most this many times v1.2.2's value in the same session.
# With the server on two CPUs `search-index-threads auto` starts no helper,
# so the default must serve as 1.2.2 does there.
BESIDE_P99_X, BESIDE_KNN_X = 2.0, 2.0


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
    if ctx.config.name == "beside":
        return beside(ctx)
    return pipeline(ctx)


def create(c):
    c.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA",
                      "score", "NUMERIC", "v", "VECTOR", "HNSW", "10", "TYPE", "FLOAT32",
                      "DIM", DIM, "DISTANCE_METRIC", "COSINE", "M", M, "EF_CONSTRUCTION", EFC)


def cpus():
    """Server, memtier and clients CPUs, of the first four this process may use:
    the server on the first and third (separate cores on a D4s_v5, whose CPUs
    0-1 and 2-3 are the two hyper-threads of a core)."""
    have = sorted(os.sched_getaffinity(0))
    if len(have) < 4:
        raise RuntimeError("28-ft-hnsw/beside needs 4 CPUs, this process may use %d" % len(have))
    return [have[0], have[2]], [have[1]], [have[3]]


def pin(pid, which):
    """Every thread of `pid` onto `which`; threads it starts later inherit it."""
    for tid in os.listdir("/proc/%d/task" % pid):
        os.sched_setaffinity(int(tid), which)


def memtier_bin():
    for p in (os.environ.get("MEMTIER", ""), shutil.which("memtier_benchmark") or ""):
        if p and os.access(p, os.X_OK):
            return p
    raise RuntimeError("memtier_benchmark not found ($MEMTIER or PATH)")


def ingest_proc(port, cpu, first, seconds, ready, go, out):
    """The loading client: HSET pipelines of BATCH new documents for `seconds`,
    their vectors made before the clock starts."""
    import numpy as np
    import redis
    os.sched_setaffinity(0, cpu)
    c = redis.Redis(host="127.0.0.1", port=port, socket_timeout=600)
    c.ping()
    blobs = [x.tobytes() for x in np.random.default_rng(first).random((40 * BATCH, DIM), dtype=np.float32)]
    ready.set()
    go.wait()
    n = 0
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < seconds:
        p = c.pipeline(transaction=False)
        for i in range(BATCH):
            p.hset("doc:%d" % (first + n + i), mapping={"v": blobs[(n + i) % len(blobs)], "score": i % 101})
        p.execute()
        n += BATCH
    out.put({"docs": n, "s": time.perf_counter() - t0})


def probe_proc(port, cpu, seconds, ready, go, out):
    """One query at a time, KNN 10 at EF_RUNTIME EF, over PROBE_CONNS connections
    in turn: the latencies of each connection's queries. A connection that shares
    a worker with the loading client waits behind its pipelines, which is not
    what this measures: the suite takes the connection with the lowest median."""
    import numpy as np
    import redis
    os.sched_setaffinity(0, cpu)
    cs = [redis.Redis(host="127.0.0.1", port=port, socket_timeout=600) for _ in range(PROBE_CONNS)]
    for c in cs:
        c.ping()
    blobs = [x.tobytes() for x in np.random.default_rng(761).random((300, DIM), dtype=np.float32)]
    q = "*=>[KNN 10 @v $vec EF_RUNTIME %d]" % EF
    lat = [[] for _ in cs]
    ready.set()
    go.wait()
    t0 = time.perf_counter()
    i = 0
    while time.perf_counter() - t0 < seconds:
        t = time.perf_counter()
        cs[i % len(cs)].execute_command("FT.SEARCH", "idx", q, "PARAMS", "2", "vec", blobs[i % 300],
                                        "NOCONTENT", "LIMIT", "0", "10", "DIALECT", "2")
        lat[i % len(cs)].append((time.perf_counter() - t) * 1000)
        i += 1
    out.put(lat)


def beside(ctx):
    import numpy as np
    args = ("--loadmodule", MODULE) if ctx.kind == "redis" else ()
    srv, mt_cpu, cl_cpu = cpus()
    mt = memtier_bin()
    with Nodes(ctx) as ns:
        node = ns.start(args=args)
        # Before the first vector: the helpers start then, on these CPUs.
        pin(node.pid, srv)
        ctx.raw["cpus"] = {"server": srv, "memtier": mt_cpu, "clients": cl_cpu}
        c = node.client(protocol=2)
        create(c)
        pre = scaled(20_000)
        vecs = np.random.default_rng(760).random((pre, DIM), dtype=np.float32)
        for s in range(0, pre, BATCH):
            p = c.pipeline(transaction=False)
            for i in range(s, min(pre, s + BATCH)):
                p.hset("doc:%d" % i, mapping={"v": vecs[i].tobytes(), "score": i % 101})
            p.execute()
        deadline = time.time() + 900
        while waiting(c, ctx.kind) != 0 or int(field(c.execute_command("FT.INFO", "idx"), "num_docs")) < pre:
            if time.time() > deadline:
                raise RuntimeError("the first documents were not indexed in 900 s")
            time.sleep(0.02)
        c.execute_command("DEBUG", "POPULATE", "100000", "key", "64")
        seconds = max(4, int(BESIDE_S * min(1.0, float(os.environ.get("SCALE", "1") or 1)) or 4))
        p99s, ops, p50s, docs, conn_p50s = [], [], [], [], []
        first = pre
        mp = multiprocessing.get_context("spawn")
        for _ in range(ROUNDS):
            q_in, q_kn = mp.Queue(), mp.Queue()
            r_in, r_kn, go = mp.Event(), mp.Event(), mp.Event()
            ing = mp.Process(target=ingest_proc, args=(node.port, cl_cpu, first, seconds, r_in, go, q_in))
            prb = mp.Process(target=probe_proc, args=(node.port, cl_cpu, seconds, r_kn, go, q_kn))
            ing.start()
            prb.start()
            if not (r_in.wait(120) and r_kn.wait(120)):
                raise RuntimeError("the clients did not start")
            with tempfile.TemporaryDirectory() as tmp:
                js = os.path.join(tmp, "memtier.json")
                m = subprocess.Popen(["taskset", "-c", ",".join(map(str, mt_cpu)), mt, "-s", "127.0.0.1",
                                      "-p", str(node.port), "--protocol=redis", "-t", "1", "-c", "20",
                                      "--ratio", "1:1", "--key-pattern", "R:R", "--key-prefix", "key:",
                                      "--key-maximum", "100000", "-d", "64", "--test-time", str(seconds),
                                      "--hide-histogram", "--json-out-file", js],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
                go.set()
                got_in, lat = q_in.get(timeout=seconds + 600), q_kn.get(timeout=seconds + 600)
                ing.join()
                prb.join()
                _, err = m.communicate(timeout=seconds + 120)
                if m.returncode != 0:
                    raise RuntimeError("memtier_benchmark exited %d: %s" % (m.returncode, err[-500:]))
                tot = json.load(open(js))["ALL STATS"]["Totals"]
            first += got_in["docs"]
            docs.append(got_in["docs"] / got_in["s"])
            ops.append(tot["Ops/sec"])
            p99s.append(tot["Percentile Latencies"]["p99.00"])
            meds = sorted(sorted(x)[len(x) // 2] for x in lat if x)
            p50s.append(meds[0])
            conn_p50s.append(meds)
        ctx.emit("getset_p99_ms_beside_build", median(p99s), "ms", "lower",
                 gate=jitter([rel(50, abs=0.5), vs_v1_2_2(BESIDE_P99_X)]), samples=p99s)
        ctx.emit("knn_p50_ms_beside_ingest", median(p50s), "ms", "lower",
                 gate=jitter([rel(30, abs=0.2), vs_v1_2_2(BESIDE_KNN_X)]), samples=p50s)
        ctx.raw["knn_p50_ms_by_connection"] = conn_p50s
        ctx.emit("getset_ops_beside_build", median(ops), "ops/s", "higher", gate=info(), samples=ops)
        ctx.emit("ingest_docs_per_s_beside", median(docs), "docs/s", "higher", gate=info(), samples=docs)


def pipeline(ctx):
    import numpy as np
    args = ("--loadmodule", MODULE) if ctx.kind == "redis" else ()
    explicit = ctx.config.name == "pipeline-n3"
    if explicit and ctx.engine == "candidate":
        args = ("--search-index-threads", str(EXPLICIT_HELPERS))
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
                 gate=[rel(10), KEEP_IMPROVEMENT, vs_redis(1.5)] if explicit else rel(10))
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
