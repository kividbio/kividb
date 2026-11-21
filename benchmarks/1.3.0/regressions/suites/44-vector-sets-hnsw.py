#!/usr/bin/env python3
"""#710: VSIM through a vector set's HNSW graph, against Redis 8.10.2's vector
sets at their defaults, in the same session.

A vector set of 4,096 elements or more is searched in its own HNSW graph (the
graph FT.SEARCH's vector fields use), which explores `EF` candidates (100
unless given, Redis's default) that are then scored from the set's vectors; a
smaller set, or `TRUTH`, is scanned. Suite 26 measures VADD and VSIM at
matched recall (each server's EF raised until recall@10 is 0.95); this one
measures them at the defaults every client gets, with no `EF` on `VSIM` and
none on `VADD` (Redis's `M` 16 and build `EF` 200), and the recall those
defaults give: the numbers a user of either server sees without tuning.

Data (downloaded once into $VSET_DATASETS, default $REGRESS_CACHE/datasets,
SHA-256 checked; suite 26's two files, and SIFT for 128 dimensions):

  glove100  glove-100-angular (ann-benchmarks), 100 dimensions
  laion768  laion-img-emb-768-1M-cosine (redis/vector-db-benchmark), 768
  sift128   sift-128-euclidean (ann-benchmarks), 128 dimensions, searched by
            cosine here as every set is (Redis's vector sets are cosine only)

Configurations, `Q8` on every server: `glove100_100k` and `laion768_100k`
(#627's datasets), `sift128_10k` and `sift128_100k`, and `sift128_1m` with
FULL=1 (Redis loads it at about 2,000 VADD/s: allow SUITE_TIMEOUT=3600).
`NOQUANT` on every server (f32, like for like): `sift128_100k_noquant` and
`laion768_100k_noquant`. No quantization option (what a client gets without
asking: Redis's default is `Q8`, KiviDB's f32): `sift128_100k_default`.
Queries: the first 500 test vectors of the dataset. Ground truth: the exact
top 10 by cosine over the f32 vectors loaded (numpy, cached beside the file).

Every server gets the same commands:

  VADD vs FP32 <blob> <i> [Q8 | NOQUANT]
                                  (the configuration's option, none in the
                                   `_default` one; pipelined 500 at a time
                                   from 4 client processes)
  VSIM vs FP32 <blob> COUNT 10    (asyncio, 4 client processes)

Rows, per configuration:

  vadd_per_s               vectors loaded a second
  graph_ready_s            from the last VADD's reply until VSIM searches the
                           graph (KiviDB's VINFO index-type HNSW: the builder
                           has handed every element over); 0 for Redis and
                           v1.2.2, which have nothing to wait for
  bytes_per_vector         INFO used_memory once it holds still after the
                           load, less before, a vector: the vector, the name
                           and the graph
  recall_at_10             at the default EF, over the 500 queries, one client
  vsim_qps_c1/_c8/_c50     VSIM a second at 1, 8 and 50 connections (2 s
                           warm-up, 10 s)
  vsim_p50_ms_*, _p99_ms_* their latencies

v1.2.2 scans every set: its rows are what this change is measured against
(recall is what Q8 leaves, about 0.98).

Gates (candidate): recall_at_10 within 2 % of the recorded value and at
least Redis's this session less 0.01 (`vs_redis(RECALL_X)`: Redis's recall
divided by 1.0101 is at most 0.01 below it); VSIM q/s within 15 % of the
recorded value (the asyncio clients share the VM) and at most `QPS_X` times
slower than Redis this session; VADD/s within 10 % and bytes per vector
within 5 % of the recorded values. p50, p99 and graph_ready_s are shown.

KiviDB runs with `--threads 4` on the suite's 8 vCPUs (`search-index-threads
auto`: seven helpers link while the load is all the server does); Redis with
its own threads (its VSIM runs on a thread per query). `min_cpus=8`, recorded
on a Standard_D8s_v5.

Needs numpy and h5py (requirements.txt).
"""
import asyncio
import hashlib
import multiprocessing as mp
import os
import shutil
import sys
import time
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, THROUGHPUT, MEMORY, info, jitter, rel, vs_redis, scaled  # noqa: E402

DATASETS = {
    "glove100": {
        "file": "glove-100-angular.hdf5",
        "url": "https://ann-benchmarks.com/glove-100-angular.hdf5",
        "sha256": "544af1d5e84e112cd4749571dcfd8ca109818a572f850af75a3a09e093a953c4",
    },
    "laion768": {
        "file": "laion-img-emb-768-1M-cosine.hdf5",
        "url": "https://s3.amazonaws.com/benchmarks.redislabs/vecsim/laion400m/laion-img-emb-768-1M-cosine.hdf5",
        "sha256": "86376b43c51bbdd484b6ab58365e34d9ee7568d0b94af98ee37535e1585ba753",
    },
    "sift128": {
        "file": "sift-128-euclidean.hdf5",
        "url": "https://ann-benchmarks.com/sift-128-euclidean.hdf5",
        "sha256": "dd6f0a6ed6b7ebb8934680f861a33ed01ff33991eaee4fd60914d854a0ca5984",
    },
}
# name: (dataset, vectors, FULL=1 only, the VADD's quantization option or
# None for the server's own default)
CONFIGS = {
    "glove100_100k": ("glove100", 100_000, False, "Q8"),
    "laion768_100k": ("laion768", 100_000, False, "Q8"),
    "sift128_10k": ("sift128", 10_000, False, "Q8"),
    "sift128_100k": ("sift128", 100_000, False, "Q8"),
    "sift128_1m": ("sift128", 1_000_000, True, "Q8"),
    "sift128_100k_noquant": ("sift128", 100_000, False, "NOQUANT"),
    "laion768_100k_noquant": ("laion768", 100_000, False, "NOQUANT"),
    "sift128_100k_default": ("sift128", 100_000, False, None),
}
K = 10
NQ = 500
QUANT = "Q8"  # set per configuration before the loaders fork
KEY = "vs"
LOAD_CLIENTS = 4
BATCH = 500
CLIENTS = (1, 8, 50)
PROCS = 4
SECONDS = 10.0
WARMUP = 2.0
# Redis's recall / candidate's at most this: the candidate at most 0.01 below.
RECALL_X = 1.0101
# VSIM q/s: the candidate at most this many times slower than Redis. Measured
# (D8s_v5, 2026-11-21): 1.07-1.41x Redis's q/s at 1 connection on SIFT and
# glove, 0.94x on LAION's 768 dimensions; 1.0-1.85x at 8 and 50. The room is
# for LAION's single connection and a run's noise.
QPS_X = {"c1": 1.2, "c8": 1.15, "c50": 1.15}

S = Suite("44-vector-sets-hnsw",
          [Config(n, threads=4, full_only=full) for n, (_, _, full, _) in CONFIGS.items()],
          min_cpus=8)

# Set before the client processes fork, so they share them.
QUANT_ARGS = ()
DATA = QUERIES = None
TARGET_PORT = None


# ── data ─────────────────────────────────────────────────────────────────────

def dataset_dir():
    d = os.environ.get("VSET_DATASETS") or os.path.join(
        os.environ.get("REGRESS_CACHE") or os.path.expanduser("~/.cache/kividb-regress"), "datasets")
    os.makedirs(d, exist_ok=True)
    return d


def fetch(name):
    """The dataset's file, downloaded once and checked against its pinned SHA-256."""
    ds = DATASETS[name]
    path = os.path.join(dataset_dir(), ds["file"])
    if not os.path.exists(path):
        part = path + ".part"
        req = urllib.request.Request(ds["url"], headers={"User-Agent": "kividb-regress"})
        with urllib.request.urlopen(req, timeout=60) as r, open(part, "wb") as f:
            shutil.copyfileobj(r, f, 1 << 20)
        os.replace(part, path)
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    if h.hexdigest() != ds["sha256"]:
        raise RuntimeError("%s: SHA-256 %s, want %s (delete it to fetch it again)"
                           % (path, h.hexdigest(), ds["sha256"]))
    return path


def load_data(name, n):
    """(vectors, queries, exact top-K ids of each query among the vectors)."""
    import h5py
    import numpy as np
    path = fetch(name)
    with h5py.File(path, "r") as f:
        if f["train"].shape[0] < n:
            raise RuntimeError("%s holds %d vectors, not %d" % (path, f["train"].shape[0], n))
        data = np.ascontiguousarray(f["train"][:n], dtype="<f4")
        queries = np.ascontiguousarray(f["test"][:NQ], dtype="<f4")
    cache = "%s.n%d.q%d.top%d.npy" % (path, n, NQ, K)
    if os.path.exists(cache):
        return data, queries, np.load(cache)
    unit = data / np.maximum(np.linalg.norm(data, axis=1, keepdims=True), 1e-12)
    qunit = queries / np.maximum(np.linalg.norm(queries, axis=1, keepdims=True), 1e-12)
    truth = np.empty((len(queries), K), dtype=np.int64)
    for s in range(0, len(queries), 50):
        sims = qunit[s:s + 50] @ unit.T
        part = np.argpartition(-sims, K, axis=1)[:, :K]
        for j in range(len(part)):
            truth[s + j] = part[j][np.argsort(-sims[j, part[j]])]
    np.save(cache, truth)
    return data, queries, truth


# ── one server ───────────────────────────────────────────────────────────────

def load_worker(rank):
    import redis
    c = redis.Redis(port=TARGET_PORT, socket_timeout=3600)
    n = len(DATA)
    lo, hi = n * rank // LOAD_CLIENTS, n * (rank + 1) // LOAD_CLIENTS
    for s in range(lo, hi, BATCH):
        p = c.pipeline(transaction=False)
        for i in range(s, min(hi, s + BATCH)):
            p.execute_command("VADD", KEY, "FP32", DATA[i].tobytes(), str(i), *QUANT_ARGS)
        for res in p.execute(raise_on_error=False):
            if isinstance(res, Exception):
                return "VADD: %s" % res
    c.close()
    return None


def vsim(qi):
    return ["VSIM", KEY, "FP32", QUERIES[qi].tobytes(), "COUNT", str(K)]


async def query_conn(conn, nconns, deadline, lat):
    import redis.asyncio as aredis
    r = aredis.Redis(port=TARGET_PORT, socket_timeout=600)
    qi = conn % len(QUERIES)
    try:
        while time.perf_counter() < deadline:
            t = time.perf_counter()
            await r.execute_command(*vsim(qi))
            lat.append(time.perf_counter() - t)
            qi = (qi + nconns) % len(QUERIES)
    finally:
        await r.aclose()


def query_worker(job):
    rank, procs, nconns, deadline = job
    lat = []

    async def go():
        await asyncio.gather(*(query_conn(c, nconns, deadline, lat)
                               for c in range(rank, nconns, procs)))

    t0 = time.perf_counter()
    asyncio.run(go())
    return lat, t0, time.perf_counter()


def query_cell(nconns):
    """(VSIM a second, p50 ms, p99 ms) at nconns connections."""
    import numpy as np
    procs = min(nconns, PROCS)
    with mp.get_context("fork").Pool(procs) as pool:
        deadline = time.perf_counter() + WARMUP
        pool.map(query_worker, [(p, procs, nconns, deadline) for p in range(procs)])
        deadline = time.perf_counter() + SECONDS
        parts = pool.map(query_worker, [(p, procs, nconns, deadline) for p in range(procs)])
    secs = max(p[2] for p in parts) - min(p[1] for p in parts)
    lat = np.array([x for p in parts for x in p[0]]) * 1000.0
    if not len(lat):
        raise RuntimeError("no VSIM answered at %d connections" % nconns)
    return len(lat) / secs, float(np.percentile(lat, 50)), float(np.percentile(lat, 99))


def settled_memory(c):
    """used_memory once it holds still: KiviDB's set takes in, after the
    load, the vectors its graph linked last (a second's samples within
    0.2 %, 60 s at most)."""
    time.sleep(2)
    last = int(c.info("memory")["used_memory"])
    for _ in range(60):
        time.sleep(1)
        now = int(c.info("memory")["used_memory"])
        if abs(now - last) <= last * 0.002:
            return now
        last = now
    return last


def vinfo(c):
    v = c.execute_command("VINFO", KEY)
    if isinstance(v, list):
        v = dict(zip(v[0::2], v[1::2]))
    dec = lambda x: x.decode() if isinstance(x, bytes) else x  # noqa: E731
    return {dec(k): dec(x) for k, x in v.items()}


@S.run
def run(ctx):
    global DATA, QUERIES, TARGET_PORT, QUANT, QUANT_ARGS
    name, n, _, QUANT = CONFIGS[ctx.config.name]
    QUANT_ARGS = (QUANT,) if QUANT else ()
    n = scaled(n, step=2000)
    DATA, QUERIES, truth = load_data(name, n)
    TARGET_PORT = ctx.port
    ctx.raw["data"] = {"dataset": name, "vectors": n, "dim": int(DATA.shape[1]), "queries": NQ,
                       "quant": QUANT, "procs": PROCS, "clients_cpus": os.cpu_count()}
    c = ctx.client()
    c.delete(KEY)
    time.sleep(2)
    mem0 = int(c.info("memory")["used_memory"])
    t0 = time.perf_counter()
    with mp.get_context("fork").Pool(LOAD_CLIENTS) as pool:
        errors = [e for e in pool.map(load_worker, range(LOAD_CLIENTS)) if e]
    load_s = time.perf_counter() - t0
    if errors:
        raise RuntimeError(errors[0])
    card = int(c.execute_command("VCARD", KEY))
    if card != n:
        raise RuntimeError("VCARD %d after loading %d" % (card, n))
    t1 = time.perf_counter()
    deadline = t1 + 1800
    # A KiviDB set searched by scan (index-type FLAT) until its graph has
    # every element. v1.2.2 always scans (FLAT, and no hnsw-m) and Redis has
    # no index-type: nothing to wait for.
    while "hnsw-m" in (vi := vinfo(c)) and vi.get("index-type", "HNSW") != "HNSW" and n >= 4096:
        if time.perf_counter() > deadline:
            raise RuntimeError("the graph was not made in 1800 s")
        time.sleep(0.05)
    ready_s = time.perf_counter() - t1
    mem1 = settled_memory(c)
    ctx.raw["vinfo"] = vinfo(c)
    ctx.raw["load_s"] = round(load_s, 2)

    hits = 0
    for qi in range(len(QUERIES)):
        got = {int(x) for x in c.execute_command(*vsim(qi))}
        hits += len(got & set(truth[qi].tolist()))
    recall = hits / (K * len(QUERIES))

    ctx.emit("vadd_per_s", n / load_s, "VADD/s", "higher", gate=THROUGHPUT)
    ctx.emit("graph_ready_s", ready_s, "s", "lower", gate=info())
    ctx.emit("bytes_per_vector", (mem1 - mem0) / n, "B", "lower", gate=MEMORY)
    ctx.emit("recall_at_10", recall, "ratio", "higher", gate=[rel(2), vs_redis(RECALL_X)])
    for nconns in CLIENTS:
        qps, p50, p99 = query_cell(nconns)
        cell = "c%d" % nconns
        ctx.emit("vsim_qps_" + cell, qps, "q/s", "higher", gate=[rel(15), vs_redis(QPS_X[cell])])
        ctx.emit("vsim_p50_ms_" + cell, p50, "ms", "lower", gate=jitter(info()))
        ctx.emit("vsim_p99_ms_" + cell, p99, "ms", "lower", gate=jitter(info()))
    c.delete(KEY)


if __name__ == "__main__":
    sys.exit(S.main())
