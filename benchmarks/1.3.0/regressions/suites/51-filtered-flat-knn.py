#!/usr/bin/env python3
"""Filtered KNN on a FLAT index of 1536-dimension vectors (#1020).

#1020's shape: 100,000 HASH documents `doc:<i>`, a FLAT FLOAT32 COSINE
vector of 1536 dimensions (unit vectors, numpy seed 1020) and a TAG field,
`FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA user_id TAG user_small TAG
embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 1536 DISTANCE_METRIC COSINE`,
written by one client in `HSET` pipelines of 500. Two layouts in the same
documents: `user_id` is `user-<i // 90>` for the first 90,000 documents and
`__shared__` for the last 10,000, so the issue's query
`(@user_id:{user\\-<u>|__shared__})=>[KNN 5 @embedding $vector AS
vector_distance]` matches about 10,090 documents (`big`); `user_small` is
`user-<i // 100>`, and `(@user_small:{user\\-<u>})=>[KNN 5 ...]` matches
100 (`small`). Then `RETURN 1 vector_distance DIALECT 2`, a random user of
1,000 and a random query vector of 256 (seed 1021) each time; and the same
KNN with no filter (`none`).

A closed loop of 8 and of 64 connections (threads of CLIENT_PROCS processes
on the client's CPUs), WINDOW_S each, median of ROUNDS rounds: lookups a
second and p99. One server an engine, `--threads 4` (Redis `--io-threads 4
--search-workers 4`), pinned to the first half of the CPUs, the client to
the other half (`REGRESS_SERVER_CPUS` / `REGRESS_CLIENT_CPUS`). Before the
rounds, the top 5 of 10 queries of each layout are checked against numpy
(`wrong_top5`, exactly 0).

What #1020 changed: a FLAT index keeps its vectors in fixed blocks by slot,
and a filtered query goes from each matching document id to its slot's
vector; each vector was a separate allocation found by its key, and the
large filter waits on memory (the profile of the issue). On a
Standard_D8s_v5 the big layout went from 533 lookups a second (v1.2.2) to
about 800-950, against Redis's 545.

Gates: `big` at 8 and 64 connections at least 1.25x v1.2.2's and 1.15x
Redis's in the same session, and within 10 % of the recorded value;
`wrong_top5` 0; `used_memory` a document within 5 %. Shown, not gated:
`small` (this Python client is the limit there: tens of thousands of
lookups a second), `none` (each lookup reads all 100,000 vectors: a few a
second), p99s, the load rate. Redis runs when `REGRESS_REDISEARCH_SO`
names the query engine module (as suites 28 and 41); without it its gates
are skipped. `min_cpus=8`, recorded on a Standard_D8s_v5. Linux only.
"""
import multiprocessing
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, THROUGHPUT, MEMORY, vs_redis, vs_v1_2_2, bound, info, median, scaled  # noqa: E402
from nodes import Nodes  # noqa: E402

MODULE = os.environ.get("REGRESS_REDISEARCH_SO", "")
ENGINES = ("v1_2_2", "candidate") + (("redis",) if MODULE else ())
S = Suite("51-filtered-flat-knn", [
    Config("flat1536", threads=4, engines=ENGINES, self_hosted=True),
], min_cpus=8)
N, DIM, USERS, K = scaled(100_000), 1536, 1000, 5
BATCH = 500
WINDOW_S = 10.0
WARM_S = 2.0
ROUNDS = 3
CLIENT_PROCS = 4
POINTS = (8, 64)
QUERIES = 256
# The candidate measured 1.5-1.8x v1.2.2 and 1.5-1.7x Redis 8.12 on the big
# layout (see the PR of #1020): these keep most of that, with room for a
# run's noise.
BIG_GATES = [vs_v1_2_2(0.8), vs_redis(0.87), THROUGHPUT]


def parse_cpus(text):
    out = set()
    for part in text.split(","):
        part = part.strip()
        if "-" in part:
            a, b = part.split("-")
            out.update(range(int(a), int(b) + 1))
        elif part:
            out.add(int(part))
    return out


def cpu_split():
    server, client = os.environ.get("REGRESS_SERVER_CPUS"), os.environ.get("REGRESS_CLIENT_CPUS")
    cpus = sorted(os.sched_getaffinity(0))
    half = len(cpus) // 2
    return (parse_cpus(server) if server else set(cpus[:half]),
            parse_cpus(client) if client else set(cpus[half:]))


def pin(pid, cpus):
    for tid in os.listdir("/proc/%d/task" % pid):
        try:
            os.sched_setaffinity(int(tid), cpus)
        except OSError:
            pass  # a thread that ended meanwhile


def big_tag(i):
    return "user-%d" % (i // 90) if i < 90_000 else "__shared__"


def query_text(layout, u):
    knn = "=>[KNN %d @embedding $vector AS vector_distance]" % K
    if layout == "big":
        return "(@user_id:{user\\-%d|__shared__})%s" % (u, knn)
    if layout == "small":
        return "(@user_small:{user\\-%d})%s" % (u, knn)
    return "*" + knn


def vectors(n, seed):
    import numpy as np
    v = np.random.default_rng(seed).standard_normal((n, DIM), dtype=np.float32)
    v /= np.linalg.norm(v, axis=1, keepdims=True)
    return v


def search(c, layout, u, blob):
    return c.execute_command("FT.SEARCH", "idx", query_text(layout, u), "PARAMS", "2", "vector", blob,
                             "RETURN", "1", "vector_distance", "DIALECT", "2")


def client_proc(port, layout, threads, cpu, start_at, seconds, out):
    """`threads` connections in a closed loop from `start_at` for WARM_S +
    `seconds`; the lookups and latencies of the last `seconds`."""
    import redis
    os.sched_setaffinity(0, {cpu})
    blobs = [q.tobytes() for q in vectors(QUERIES, 1021)]
    done, lat, errors = [0], [], [0]
    lock = threading.Lock()

    def loop(t):
        import random
        rnd = random.Random(1022 + 1000 * cpu + t)
        c = redis.Redis(host="127.0.0.1", port=port, protocol=2, socket_timeout=120)
        mine = []
        n = 0
        while time.time() < start_at:
            time.sleep(0.001)
        begin = start_at + WARM_S
        end = begin + seconds
        while True:
            t0 = time.perf_counter()
            now = time.time()
            if now >= end:
                break
            try:
                search(c, layout, rnd.randrange(USERS), blobs[rnd.randrange(QUERIES)])
            except redis.ResponseError:
                errors[0] += 1
            if now >= begin:
                n += 1
                mine.append(time.perf_counter() - t0)
        with lock:
            done[0] += n
            lat.extend(mine)

    ts = [threading.Thread(target=loop, args=(t,)) for t in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    out.put((done[0], lat, errors[0]))


def one_point(port, layout, conns, client_cpus):
    cpus = sorted(client_cpus)[:CLIENT_PROCS]
    per = [conns // len(cpus) + (1 if i < conns % len(cpus) else 0) for i in range(len(cpus))]
    mp = multiprocessing.get_context("spawn")
    q = mp.Queue()
    start_at = time.time() + 3.0
    ps = [mp.Process(target=client_proc, args=(port, layout, per[i], cpus[i], start_at, WINDOW_S, q))
          for i in range(len(cpus)) if per[i]]
    for p in ps:
        p.start()
    results = [q.get(timeout=WINDOW_S + 300) for _ in ps]
    for p in ps:
        p.join()
    lat = sorted(x for r in results for x in r[1])
    if sum(r[2] for r in results):
        raise RuntimeError("%d error replies" % sum(r[2] for r in results))
    return {"qps": sum(r[0] for r in results) / WINDOW_S,
            "p99_ms": lat[int(len(lat) * 0.99)] * 1000 if lat else None}


@S.run
def run(ctx):
    import numpy as np
    if not hasattr(os, "sched_getaffinity"):
        raise RuntimeError("Linux only: the suite pins CPUs")
    server_cpus, client_cpus = cpu_split()
    ctx.raw["server_cpus"], ctx.raw["client_cpus"] = sorted(server_cpus), sorted(client_cpus)
    args = []
    if ctx.kind == "redis":
        args = ["--loadmodule", MODULE, "--io-threads", "4", "--search-workers", "4"]
    with Nodes(ctx) as ns:
        node = ns.start(args=args)
        pin(node.pid, server_cpus)
        c = node.client(protocol=2)
        c.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA",
                          "user_id", "TAG", "user_small", "TAG", "embedding", "VECTOR", "FLAT", "6",
                          "TYPE", "FLOAT32", "DIM", DIM, "DISTANCE_METRIC", "COSINE")
        vecs = vectors(N, 1020)
        before = int(c.info("memory")["used_memory"])
        t0 = time.perf_counter()
        for s in range(0, N, BATCH):
            p = c.pipeline(transaction=False)
            for i in range(s, min(N, s + BATCH)):
                p.hset("doc:%d" % i, mapping={"user_id": big_tag(i), "user_small": "user-%d" % (i // 100),
                                               "embedding": vecs[i].tobytes()})
            p.execute()
        deadline = time.time() + 600
        while True:
            d = c.execute_command("FT.INFO", "idx")
            d = d if isinstance(d, dict) else dict(zip(d[::2], d[1::2]))
            docs = d.get(b"num_docs", d.get("num_docs"))
            ix = d.get(b"indexing", d.get("indexing", 0))
            ix = ix.decode() if isinstance(ix, bytes) else str(ix)
            if int(docs) >= N and ix in ("0", "0.0"):
                break
            if time.time() > deadline:
                raise RuntimeError("the index was not built in 600 s")
            time.sleep(0.05)
        ctx.emit("load_docs_per_s", N / (time.perf_counter() - t0), "docs/s", "higher", gate=info())
        time.sleep(1.0)
        used = int(c.info("memory")["used_memory"])
        ctx.emit("used_memory_per_doc", (used - before) / N, "B", "lower", gate=MEMORY)

        # The top 5 of 10 queries of each layout against numpy.
        big = np.array([big_tag(i) for i in range(N)])
        small = np.array(["user-%d" % (i // 100) for i in range(N)])
        qs = vectors(QUERIES, 1021)
        wrong = 0
        for layout in ("big", "small"):
            for j in range(10):
                u = (j * 97) % USERS
                q = qs[j]
                mask = (big == "user-%d" % u) | (big == "__shared__") if layout == "big" else small == "user-%d" % u
                ids = np.nonzero(mask)[0]
                want = [int(x) for x in ids[np.argsort(1 - vecs[ids] @ q)[:K]]]
                res = search(c, layout, u, q.tobytes())
                got = [int(res[i].decode().split(":")[1]) for i in range(1, len(res), 2)]
                wrong += got != want
        ctx.emit("wrong_top5", wrong, "queries", "lower", gate=bound(max=0))
        del vecs

        rows = {}
        for _ in range(ROUNDS):
            for layout in ("big", "small", "none"):
                for conns in POINTS:
                    if layout == "none" and conns != 8:
                        continue
                    pin(node.pid, server_cpus)  # threads started since
                    rows.setdefault((layout, conns), []).append(
                        one_point(node.port, layout, conns, client_cpus))
        ctx.raw["rows"] = {"%s_c%d" % k: v for k, v in rows.items()}
        for (layout, conns), r in sorted(rows.items()):
            qps = [x["qps"] for x in r]
            ctx.emit("%s_c%d_lookups_per_s" % (layout, conns), median(qps), "q/s", "higher",
                     gate=BIG_GATES if layout == "big" else info(), samples=qps)
            ctx.emit("%s_c%d_p99_ms" % (layout, conns), median([x["p99_ms"] for x in r]), "ms", "lower",
                     gate=info())


if __name__ == "__main__":
    sys.exit(S.main())
