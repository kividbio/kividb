#!/usr/bin/env python3
"""Vector-set ingest, search latency, recall@10 and memory for one server."""

import argparse
import json
import multiprocessing as mp
import time

import numpy as np
import redis


def dataset(dim, n, queries, seed=697):
    rng = np.random.default_rng(seed)
    centres = rng.normal(0, 1, (64, dim)).astype(np.float32)
    which = rng.integers(0, 64, n)
    data = centres[which] + rng.normal(0, 0.6, (n, dim)).astype(np.float32)
    qwhich = rng.integers(0, 64, queries)
    qs = centres[qwhich] + rng.normal(0, 0.6, (queries, dim)).astype(np.float32)
    return data.astype(np.float32), qs.astype(np.float32)


def truth(data, qs, k=10):
    dn = data / np.linalg.norm(data, axis=1, keepdims=True)
    qn = qs / np.linalg.norm(qs, axis=1, keepdims=True)
    sims = qn @ dn.T
    return np.argsort(-sims, axis=1)[:, :k]


def info_mem(r):
    m = r.info("memory")
    return int(m["used_memory"]), int(m.get("used_memory_rss", 0))


def pct(xs, p):
    return float(np.percentile(np.array(xs) * 1000.0, p))


def vsim_args(key, q, extra):
    return ["VSIM", key, "FP32", q.tobytes(), "COUNT", 10] + extra


def worker(port, key, qs, extra, seconds, out):
    r = redis.Redis(port=port)
    done, i = 0, 0
    end = time.perf_counter() + seconds
    while time.perf_counter() < end:
        r.execute_command(*vsim_args(key, qs[i % len(qs)], extra))
        i += 1
        done += 1
    out.put(done)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--quant", default="", help="NOQUANT, Q8, BIN or empty for default")
    ap.add_argument("--dim", type=int, required=True)
    ap.add_argument("--n", type=int, required=True)
    ap.add_argument("--queries", type=int, default=300)
    ap.add_argument("--clients", type=int, default=8)
    ap.add_argument("--seconds", type=float, default=8.0)
    ap.add_argument("--truth", action="store_true", help="add TRUTH to VSIM (Redis exact scan)")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    r = redis.Redis(port=a.port)
    r.flushall()
    time.sleep(0.5)
    mem0, rss0 = info_mem(r)

    data, qs = dataset(a.dim, a.n, a.queries)
    gt = truth(data, qs)
    key = "vs"
    opts = [a.quant] if a.quant else []

    # Ingest: pipelined batches from one client (the usual bulk-load shape).
    t0 = time.perf_counter()
    batch = 200
    for s in range(0, a.n, batch):
        p = r.pipeline(transaction=False)
        for i in range(s, min(s + batch, a.n)):
            p.execute_command("VADD", key, "FP32", data[i].tobytes(), f"e{i}", *opts)
        p.execute_command("PING")
        p.execute()
    ingest = time.perf_counter() - t0

    # Per-call VADD latency: replace 500 existing elements, unpipelined.
    lat_add = []
    for i in range(500):
        v = data[i].tobytes()
        t = time.perf_counter()
        r.execute_command("VADD", key, "FP32", v, f"e{i}", *opts)
        lat_add.append(time.perf_counter() - t)

    time.sleep(1.0)
    mem1, rss1 = info_mem(r)
    usage = int(r.execute_command("MEMORY", "USAGE", key, "SAMPLES", "0"))
    vinfo = r.execute_command("VINFO", key)
    vinfo = dict(zip(vinfo[0::2], vinfo[1::2]))
    qtype = vinfo.get(b"quant-type", b"?").decode()

    extra = ["TRUTH"] if a.truth else []
    # Recall and single-client latency.
    hits, lat = 0, []
    for qi, q in enumerate(qs):
        t = time.perf_counter()
        res = r.execute_command(*vsim_args(key, q, extra))
        lat.append(time.perf_counter() - t)
        got = {int(x.decode()[1:]) for x in res}
        hits += len(got & set(gt[qi].tolist()))
    recall = hits / (len(qs) * 10)

    # Throughput: N client processes for a fixed time.
    out = mp.Queue()
    procs = [
        mp.Process(target=worker, args=(a.port, key, qs, extra, a.seconds, out))
        for _ in range(a.clients)
    ]
    for p in procs:
        p.start()
    total = sum(out.get() for _ in procs)
    for p in procs:
        p.join()
    qps = total / a.seconds

    row = dict(
        label=a.label,
        quant=qtype,
        dim=a.dim,
        n=a.n,
        truth=a.truth,
        ingest_s=round(ingest, 3),
        vadd_per_s=round(a.n / ingest, 1),
        vadd_p50_ms=round(pct(lat_add, 50), 3),
        vadd_p99_ms=round(pct(lat_add, 99), 3),
        used_memory_delta=mem1 - mem0,
        bytes_per_vector=round((mem1 - mem0) / a.n, 1),
        rss_delta=rss1 - rss0,
        memory_usage_key=usage,
        recall_at_10=round(recall, 4),
        vsim_p50_ms=round(pct(lat, 50), 3),
        vsim_p95_ms=round(pct(lat, 95), 3),
        vsim_p99_ms=round(pct(lat, 99), 3),
        vsim_qps=round(qps, 1),
        clients=a.clients,
    )
    print(json.dumps(row))
    with open(a.out, "a") as f:
        f.write(json.dumps(row) + "\n")


if __name__ == "__main__":
    main()
