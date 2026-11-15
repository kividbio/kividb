#!/usr/bin/env python3
"""Vector-set benchmark: VADD load, VSIM latency/throughput and recall@10.

Runs against any server that speaks Redis vector sets (KiviDB, Redis 8, ...):

    python3 vset_bench.py --host 127.0.0.1 --port 6379 --label kividb \
        --n 100000 --dim 128 --clients 1,16,100 --ef default,200 --out out.json

The dataset is random unit vectors from a fixed seed, so every server gets
the same data and queries. The queries are separate random unit vectors, and
the ground truth is the exact top 10 by cosine similarity, computed with
numpy. Needs redis-py and numpy.
"""

import argparse
import asyncio
import json
import multiprocessing as mp
import os
import platform
import sys
import time

import numpy as np
import redis
import redis.asyncio as aredis

K = 10
TRUTH_SETS = None

# Set in main() before the worker processes fork, so they share them.
DATA = None
QUERIES = None
TRUTH = None
ARGS = None


def unit_vectors(rng, n, dim, chunk=100_000):
    out = np.empty((n, dim), dtype=np.float32)
    for s in range(0, n, chunk):
        e = min(n, s + chunk)
        v = rng.standard_normal((e - s, dim), dtype=np.float32)
        v /= np.linalg.norm(v, axis=1, keepdims=True)
        out[s:e] = v
    return out


def exact_top_k(data, queries, k=K, qchunk=100):
    """Exact top-k by cosine similarity (all vectors are unit length)."""
    truth = np.empty((len(queries), k), dtype=np.int64)
    for s in range(0, len(queries), qchunk):
        sims = data @ queries[s:s + qchunk].T          # (n, qchunk)
        part = np.argpartition(-sims, k, axis=0)[:k]   # (k, qchunk), unordered
        for j in range(part.shape[1]):
            idx = part[:, j]
            truth[s + j] = idx[np.argsort(-sims[idx, j])]
    return truth


def vector_args(vec, fmt):
    if fmt == "fp32":
        return ["FP32", vec.astype("<f4").tobytes()]
    return ["VALUES", str(len(vec))] + [repr(float(x)) for x in vec]


def connect(sync=True):
    cls = redis.Redis if sync else aredis.Redis
    return cls(host=ARGS.host, port=ARGS.port, password=ARGS.password or None,
               socket_timeout=600, socket_connect_timeout=30)


# ── load ──────────────────────────────────────────────────────────────────

def load_worker(rank):
    r = connect()
    n = len(DATA)
    lo = n * rank // ARGS.load_clients
    hi = n * (rank + 1) // ARGS.load_clients
    extra = list(ARGS.vadd_opts.split()) if ARGS.vadd_opts else []
    errors = 0
    for s in range(lo, hi, ARGS.batch):
        p = r.pipeline(transaction=False)
        for i in range(s, min(hi, s + ARGS.batch)):
            p.execute_command("VADD", ARGS.key, *vector_args(DATA[i], ARGS.format), str(i), *extra)
        for res in p.execute(raise_on_error=False):
            if isinstance(res, Exception):
                errors += 1
    r.close()
    return errors


def used_memory(r):
    return int(r.info("memory")["used_memory"])


def load(r):
    r.delete(ARGS.key)
    mem0 = used_memory(r)
    t0 = time.perf_counter()
    with mp.get_context("fork").Pool(ARGS.load_clients) as pool:
        errors = sum(pool.map(load_worker, range(ARGS.load_clients)))
    secs = time.perf_counter() - t0
    card = int(r.execute_command("VCARD", ARGS.key))
    mem1 = used_memory(r)
    return {
        "seconds": secs,
        "vectors_per_sec": len(DATA) / secs,
        "errors": errors,
        "vcard": card,
        "used_memory_delta": mem1 - mem0,
        "bytes_per_vector": (mem1 - mem0) / max(card, 1),
    }


# ── query ─────────────────────────────────────────────────────────────────

def vsim_args(qi, ef):
    args = ["VSIM", ARGS.key, *vector_args(QUERIES[qi], ARGS.format), "COUNT", str(K)]
    if ef is not None:
        args += ["EF", str(ef)]
    return args


async def query_conn(conn_id, nconns_total, ef, deadline, lat, hits):
    r = connect(sync=False)
    nq = len(QUERIES)
    qi = conn_id % nq
    try:
        while time.perf_counter() < deadline:
            args = vsim_args(qi, ef)
            t = time.perf_counter()
            res = await r.execute_command(*args)
            lat.append(time.perf_counter() - t)
            got = {int(x) for x in res}
            hits.append((qi, len(got & TRUTH_SETS[qi])))
            qi = (qi + nconns_total) % nq
    finally:
        await r.aclose()


def query_worker(job):
    proc_rank, nprocs, nconns, ef, deadline = job
    # this process owns connections proc_rank, proc_rank+nprocs, ...
    mine = list(range(proc_rank, nconns, nprocs))
    lat, hits = [], []

    async def run():
        await asyncio.gather(*(query_conn(c, nconns, ef, deadline, lat, hits) for c in mine))

    t0 = time.perf_counter()
    asyncio.run(run())
    return lat, hits, t0, time.perf_counter()


def query_cell(nconns, ef):
    nprocs = min(nconns, ARGS.procs)
    # warm-up: one short pass so connections and caches are hot
    deadline = time.perf_counter() + ARGS.warmup
    with mp.get_context("fork").Pool(nprocs) as pool:
        pool.map(query_worker, [(p, nprocs, nconns, ef, deadline) for p in range(nprocs)])
    deadline = time.perf_counter() + ARGS.duration
    with mp.get_context("fork").Pool(nprocs) as pool:
        parts = pool.map(query_worker, [(p, nprocs, nconns, ef, deadline) for p in range(nprocs)])
    # wall time from the first worker starting to the last in-flight reply
    secs = max(p[3] for p in parts) - min(p[2] for p in parts)
    lat = np.array([x for p in parts for x in p[0]]) * 1000.0
    hits = [h for p in parts for h in p[1]]
    # recall over distinct queries, so a query answered more often does not count twice
    per_q = {}
    for qi, h in hits:
        per_q[qi] = h
    recall = sum(per_q.values()) / (K * len(per_q)) if per_q else 0.0
    return {
        "clients": nconns,
        "ef": ef,
        "queries": int(len(lat)),
        "seconds": secs,
        "ops_per_sec": len(lat) / secs,
        "p50_ms": float(np.percentile(lat, 50)),
        "p99_ms": float(np.percentile(lat, 99)),
        "mean_ms": float(lat.mean()),
        f"recall_at_{K}": recall,
        "distinct_queries": len(per_q),
    }


def main():
    global DATA, QUERIES, TRUTH, TRUTH_SETS, ARGS
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--password", default=os.environ.get("VSET_BENCH_PASSWORD", ""))
    ap.add_argument("--label", default="server", help="name of the server in the output")
    ap.add_argument("--key", default="vsbench")
    ap.add_argument("--n", type=int, default=10_000, help="vectors to load")
    ap.add_argument("--dim", type=int, default=128)
    ap.add_argument("--nq", type=int, default=1000, help="distinct query vectors")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--format", choices=["fp32", "values"], default="fp32",
                    help="vector encoding for VADD and VSIM (FP32 blob or VALUES n ...)")
    ap.add_argument("--vadd-opts", default="", help='extra VADD options, e.g. "NOQUANT" or "Q8 M 16"')
    ap.add_argument("--load-clients", type=int, default=4)
    ap.add_argument("--batch", type=int, default=500, help="VADDs per pipeline")
    ap.add_argument("--clients", default="1,16,100", help="comma list of concurrent VSIM connections")
    ap.add_argument("--ef", default="default",
                    help='comma list of VSIM EF values; "default" sends no EF')
    ap.add_argument("--procs", type=int, default=4, help="client processes (connections are spread over them)")
    ap.add_argument("--duration", type=float, default=10.0, help="seconds per query cell")
    ap.add_argument("--warmup", type=float, default=2.0, help="seconds of warm-up per query cell")
    ap.add_argument("--skip-load", action="store_true", help="query the set already loaded")
    ap.add_argument("--keep", action="store_true", help="do not delete the set at the end")
    ap.add_argument("--out", help="write the result JSON here")
    ARGS = ap.parse_args()

    rng = np.random.default_rng(ARGS.seed)
    t = time.perf_counter()
    DATA = unit_vectors(rng, ARGS.n, ARGS.dim)
    QUERIES = unit_vectors(rng, ARGS.nq, ARGS.dim)
    TRUTH = exact_top_k(DATA, QUERIES)
    TRUTH_SETS = [set(map(int, row)) for row in TRUTH]
    print(f"dataset n={ARGS.n} dim={ARGS.dim} nq={ARGS.nq} seed={ARGS.seed} "
          f"(generated + ground truth in {time.perf_counter() - t:.1f}s)", flush=True)

    r = connect()
    info = r.info("server")
    server = {k: info.get(k) for k in ("redis_version", "kividb_version", "kividb_variant",
                                        "kividb_git_sha", "os", "arch_bits")}
    result = {
        "label": ARGS.label,
        "server": {k: v for k, v in server.items() if v is not None},
        "config": {k: v for k, v in vars(ARGS).items() if k not in ("password", "out")},
        "client_host": {"machine": platform.machine(), "cpus": os.cpu_count(),
                        "python": platform.python_version(), "redis_py": redis.__version__,
                        "numpy": np.__version__},
        "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    if not ARGS.skip_load:
        result["load"] = load(r)
        print(f"load: {result['load']}", flush=True)
    try:
        vinfo = r.execute_command("VINFO", ARGS.key)
        if not isinstance(vinfo, dict):
            vinfo = dict(zip(vinfo[0::2], vinfo[1::2]))
        dec = lambda v: v.decode("utf8", "replace") if isinstance(v, bytes) else v
        result["vinfo"] = {dec(k): dec(v) for k, v in vinfo.items()}
    except redis.ResponseError as e:
        result["vinfo"] = {"error": str(e)}

    efs = [None if e.strip().lower() == "default" else int(e) for e in ARGS.ef.split(",")]
    cells = []
    for ef in efs:
        for c in [int(x) for x in ARGS.clients.split(",")]:
            cell = query_cell(c, ef)
            cells.append(cell)
            print(f"vsim clients={c:>3} ef={str(ef):>7}: {cell['ops_per_sec']:>9.1f} ops/s  "
                  f"p50 {cell['p50_ms']:.3f} ms  p99 {cell['p99_ms']:.3f} ms  "
                  f"recall@10 {cell['recall_at_10']:.4f}", flush=True)
    result["vsim"] = cells

    if not ARGS.keep:
        r.delete(ARGS.key)
    if ARGS.out:
        with open(ARGS.out, "w") as f:
            json.dump(result, f, indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
