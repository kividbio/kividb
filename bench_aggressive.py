#!/usr/bin/env python3
"""
Aggressive vector DB benchmark for c7gn.12xlarge (or comparable).

Tests:
  - Ingest throughput across many checkpoints with multiple parallel clients
  - Search latency distribution (p50/p90/p95/p99/p999)
  - Concurrent search QPS at various client counts
  - Recall@10 against ground truth
  - Memory footprint after ingest
  - Mixed read+write workload (90% search, 10% insert)

Usage:
  python3 bench_aggressive.py --port 6379 --dataset glove-25-angular --size 100000
  python3 bench_aggressive.py --port 6379 --dataset glove-100-angular --size 1000000
"""

import argparse
import json
import math
import os
import statistics
import struct
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import h5py
import numpy as np
import redis


def make_pool(host, port, max_connections):
    return redis.ConnectionPool(
        host=host,
        port=port,
        max_connections=max_connections,
        decode_responses=False,
        socket_keepalive=True,
        socket_timeout=60,
    )


def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def percentiles(latencies_ms, points=(0.50, 0.90, 0.95, 0.99, 0.999)):
    s = sorted(latencies_ms)
    return {f"p{int(p*1000)/10}": percentile(s, p) for p in points}


# ── Phase 1: ingest ────────────────────────────────────────────────────────────

def ingest_worker(pool, vectors, start_idx, end_idx, dim, pipeline_size=128):
    """Insert vectors[start_idx:end_idx] using pipelined HSET calls."""
    r = redis.Redis(connection_pool=pool)
    pipe = r.pipeline(transaction=False)
    pack_fmt = f"{dim}f"
    for i in range(start_idx, end_idx):
        vec_bytes = struct.pack(pack_fmt, *vectors[i])
        pipe.hset(f"vec:{i}", mapping={"embedding": vec_bytes})
        if (i - start_idx + 1) % pipeline_size == 0:
            pipe.execute()
    pipe.execute()


def run_ingest(pool, vectors, dim, num_workers, checkpoints):
    """Run ingest with `num_workers` threads, each pulling a slice.
    Reports throughput at each checkpoint."""
    total = len(vectors)
    print(f"\n=== Ingest: {total} vectors, dim={dim}, workers={num_workers} ===")

    # We want checkpoints in absolute counts, e.g. [10000, 50000, 100000].
    # Each worker takes a shard of (prev_ckpt..ckpt).
    prev = 0
    full_start = time.time()
    for ckpt in checkpoints:
        if ckpt > total:
            ckpt = total
        if ckpt <= prev:
            continue

        # Split (prev..ckpt) across num_workers
        slice_size = ckpt - prev
        per_worker = max(1, slice_size // num_workers)
        ranges = []
        s = prev
        for w in range(num_workers):
            e = s + per_worker if w < num_workers - 1 else ckpt
            if s < e:
                ranges.append((s, e))
            s = e

        t0 = time.time()
        with ThreadPoolExecutor(max_workers=num_workers) as ex:
            futures = [ex.submit(ingest_worker, pool, vectors, s, e, dim) for (s, e) in ranges]
            for f in as_completed(futures):
                f.result()
        dt = time.time() - t0
        print(f"  {prev:>8} → {ckpt:>8}: {slice_size/dt:>9.0f} vec/sec  ({dt:.3f}s for {slice_size})")
        prev = ckpt
        if prev >= total:
            break

    full_dt = time.time() - full_start
    print(f"  TOTAL: {total/full_dt:>9.0f} vec/sec  ({full_dt:.3f}s)")
    return full_dt


# ── Phase 2: search latency (single client) ───────────────────────────────────

def search_one(r, query_vec, k, dim):
    blob = struct.pack(f"{dim}f", *query_vec)
    res = r.execute_command(
        "FT.SEARCH", "idx",
        f"*=>[KNN {k} @embedding $vec AS score]",
        "PARAMS", "2", "vec", blob,
        "RETURN", "1", "score",
        "DIALECT", "2",
        "LIMIT", "0", str(k),
    )
    ids = []
    for j in range(1, len(res), 2):
        key = res[j].decode() if isinstance(res[j], bytes) else res[j]
        try:
            ids.append(int(key.split(":")[1]))
        except (IndexError, ValueError):
            pass
    return ids


def run_single_client_latency(pool, queries, k, dim, n=1000):
    print(f"\n=== Single-client latency: {n} queries ===")
    r = redis.Redis(connection_pool=pool)
    latencies = []
    for i in range(n):
        q = queries[i % len(queries)]
        t = time.perf_counter()
        search_one(r, q, k, dim)
        latencies.append((time.perf_counter() - t) * 1000.0)
    p = percentiles(latencies)
    print(f"  mean: {statistics.mean(latencies):.3f}ms")
    for name, val in p.items():
        print(f"  {name}: {val:.3f}ms")
    return p


# ── Phase 3: concurrent throughput ─────────────────────────────────────────────

def search_burst_worker(pool, queries, k, dim, num_queries):
    r = redis.Redis(connection_pool=pool)
    latencies = []
    for i in range(num_queries):
        q = queries[i % len(queries)]
        t = time.perf_counter()
        search_one(r, q, k, dim)
        latencies.append((time.perf_counter() - t) * 1000.0)
    return latencies


def run_concurrent_throughput(pool, queries, k, dim, client_counts, queries_per_client=200):
    print(f"\n=== Concurrent throughput ===")
    results = {}
    for clients in client_counts:
        total_queries = clients * queries_per_client
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=clients) as ex:
            futures = [
                ex.submit(search_burst_worker, pool, queries, k, dim, queries_per_client)
                for _ in range(clients)
            ]
            all_latencies = []
            for f in as_completed(futures):
                all_latencies.extend(f.result())
        dt = time.time() - t0
        qps = total_queries / dt
        p = percentiles(all_latencies)
        print(f"  {clients:>3} clients: QPS={qps:>8.0f}  mean={statistics.mean(all_latencies):>6.3f}ms  "
              f"p50={p['p50.0']:.3f}ms  p99={p['p99.0']:.3f}ms  p99.9={p['p99.9']:.3f}ms")
        results[clients] = {"qps": qps, "latencies_ms": p}
    return results


# ── Phase 4: recall ────────────────────────────────────────────────────────────

def run_recall(pool, queries, train_vectors, k, dim, n_queries=1000):
    print(f"\n=== Recall@{k} ({n_queries} queries, ground truth over uploaded subset) ===")
    r = redis.Redis(connection_pool=pool)

    # Pre-normalize for cosine similarity comparison
    train_norm = train_vectors / (np.linalg.norm(train_vectors, axis=1, keepdims=True) + 1e-9)

    hits = 0
    for i in range(n_queries):
        q = queries[i % len(queries)]
        got = set(search_one(r, q, k, dim))
        q_norm = q / (np.linalg.norm(q) + 1e-9)
        sims = train_norm @ q_norm
        true_ids = set(np.argsort(-sims)[:k].tolist())
        hits += len(got & true_ids)
    recall = hits / (n_queries * k)
    print(f"  Recall@{k}: {recall:.4f}")
    return recall


# ── Phase 5: mixed workload ────────────────────────────────────────────────────

def mixed_worker(pool, queries, train_vectors, k, dim, duration_sec, write_ratio, start_idx):
    r = redis.Redis(connection_pool=pool)
    n_writes = 0
    n_reads = 0
    read_lats = []
    write_lats = []
    t_end = time.time() + duration_sec
    next_id = start_idx
    pack_fmt = f"{dim}f"
    while time.time() < t_end:
        if np.random.random() < write_ratio:
            v = train_vectors[next_id % len(train_vectors)]
            t = time.perf_counter()
            r.hset(f"mixed:{next_id}", mapping={"embedding": struct.pack(pack_fmt, *v)})
            write_lats.append((time.perf_counter() - t) * 1000.0)
            n_writes += 1
            next_id += 1
        else:
            q = queries[n_reads % len(queries)]
            t = time.perf_counter()
            search_one(r, q, k, dim)
            read_lats.append((time.perf_counter() - t) * 1000.0)
            n_reads += 1
    return n_reads, n_writes, read_lats, write_lats


def run_mixed_workload(pool, queries, train_vectors, k, dim, clients, duration_sec, write_ratio, train_size):
    print(f"\n=== Mixed workload: {clients} clients, {duration_sec}s, {int(write_ratio*100)}% writes ===")
    with ThreadPoolExecutor(max_workers=clients) as ex:
        futures = [
            ex.submit(mixed_worker, pool, queries, train_vectors, k, dim, duration_sec, write_ratio, train_size + c * 100000)
            for c in range(clients)
        ]
        total_reads = 0
        total_writes = 0
        all_read_lats = []
        all_write_lats = []
        for f in as_completed(futures):
            nr, nw, rl, wl = f.result()
            total_reads += nr
            total_writes += nw
            all_read_lats.extend(rl)
            all_write_lats.extend(wl)
    print(f"  Reads:  {total_reads:>8} ops, {total_reads/duration_sec:>8.0f} qps")
    if all_read_lats:
        p = percentiles(all_read_lats)
        print(f"          p50={p['p50.0']:.3f}ms p99={p['p99.0']:.3f}ms p99.9={p['p99.9']:.3f}ms")
    print(f"  Writes: {total_writes:>8} ops, {total_writes/duration_sec:>8.0f} qps")
    if all_write_lats:
        p = percentiles(all_write_lats)
        print(f"          p50={p['p50.0']:.3f}ms p99={p['p99.0']:.3f}ms p99.9={p['p99.9']:.3f}ms")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", type=str, default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--dataset", default="datasets/glove-25-angular/glove-25-angular.hdf5")
    ap.add_argument("--size", type=int, default=100000, help="Number of vectors to ingest")
    ap.add_argument("--ingest-workers", type=int, default=16)
    ap.add_argument("--query-count", type=int, default=1000)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--client-counts", default="1,8,32,64,128",
                    help="Comma-separated client counts for throughput phase")
    ap.add_argument("--mixed-duration", type=int, default=30)
    ap.add_argument("--mixed-clients", type=int, default=64)
    ap.add_argument("--mixed-write-ratio", type=float, default=0.1)
    ap.add_argument("--skip-mixed", action="store_true")
    ap.add_argument("--m", type=int, default=16)
    ap.add_argument("--ef-construction", type=int, default=200)
    ap.add_argument("--out", default=None, help="Optional JSON output path")
    args = ap.parse_args()

    client_counts = [int(x) for x in args.client_counts.split(",")]
    pool = make_pool(args.host, args.port, max_connections=max(client_counts) * 2 + args.ingest_workers + 4)

    r_admin = redis.Redis(connection_pool=pool)
    r_admin.flushall()

    # Load dataset
    print(f"Loading {args.dataset} (size={args.size})...")
    f = h5py.File(args.dataset, "r")
    train = np.array(f["train"][: args.size], dtype=np.float32)
    test = np.array(f["test"][: args.query_count], dtype=np.float32)
    dim = train.shape[1]
    print(f"  train shape={train.shape}  test shape={test.shape}  dim={dim}")

    # Create index
    print(f"\nCreating index (M={args.m}, EF_CONSTRUCTION={args.ef_construction})...")
    try:
        r_admin.execute_command("FT.DROPINDEX", "idx")
    except redis.ResponseError:
        pass
    r_admin.execute_command(
        "FT.CREATE", "idx", "ON", "HASH",
        "PREFIX", "1", "vec:",
        "SCHEMA", "embedding", "VECTOR", "HNSW", "10",
        "TYPE", "FLOAT32",
        "DIM", str(dim),
        "DISTANCE_METRIC", "COSINE",
        "M", str(args.m),
        "EF_CONSTRUCTION", str(args.ef_construction),
    )
    # NOTE: count is "6" — the minimum required. M and EF_CONSTRUCTION are
    # appended AFTER but not counted. This matches Redis semantics and is
    # what redis-performance/vector-db-benchmark sends.

    # Ingest checkpoints scale with size
    if args.size <= 10_000:
        ckpts = [100, 500, 1000, 2000, 5000, args.size]
    elif args.size <= 100_000:
        ckpts = [1000, 5000, 10_000, 25_000, 50_000, args.size]
    else:
        ckpts = [10_000, 50_000, 100_000, 250_000, 500_000, args.size]
    ckpts = sorted(set(c for c in ckpts if c <= args.size))

    # Run benchmarks
    results = {"config": vars(args), "dim": dim}
    results["ingest_total_sec"] = run_ingest(pool, train, dim, args.ingest_workers, ckpts)
    results["single_client_latency"] = run_single_client_latency(pool, test, args.k, dim, n=min(1000, len(test)))
    results["concurrent_throughput"] = run_concurrent_throughput(pool, test, args.k, dim, client_counts)
    results["recall"] = run_recall(pool, test, train, args.k, dim, n_queries=min(1000, len(test)))

    # Memory footprint
    info = r_admin.info("memory")
    used_bytes = info.get("used_memory", 0)
    print(f"\n=== Memory ===")
    print(f"  used_memory: {used_bytes/1024/1024:.1f} MiB")
    results["used_memory_mib"] = used_bytes / 1024 / 1024

    # Optional mixed workload
    if not args.skip_mixed:
        run_mixed_workload(
            pool, test, train, args.k, dim,
            args.mixed_clients, args.mixed_duration, args.mixed_write_ratio,
            args.size,
        )

    # Sanity check
    print(f"\n=== Sanity check ===")
    r = redis.Redis(connection_pool=pool)
    q = test[0]
    got = search_one(r, q, args.k, dim)
    train_norm = train / (np.linalg.norm(train, axis=1, keepdims=True) + 1e-9)
    q_norm = q / (np.linalg.norm(q) + 1e-9)
    true_ids = np.argsort(-(train_norm @ q_norm))[: args.k].tolist()
    print(f"  Got:  {got[:5]}")
    print(f"  True: {true_ids[:5]}")
    print(f"  Top-5 overlap: {len(set(got[:5]) & set(true_ids[:5]))}/5")

    if args.out:
        with open(args.out, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nResults written to {args.out}")


if __name__ == "__main__":
    main()
