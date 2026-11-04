"""
Benchmarks for KiviDB — wraps memtier_benchmark for standard Redis commands
and provides a custom Python micro-benchmark for vector search (FT.SEARCH KNN
and VSIM) since memtier doesn't natively support vector ops.

Usage:
    pytest benchmarks/bench_memtier.py -s -v
    BENCH_MEMTIER=1 pytest benchmarks/bench_memtier.py -s -v  # runs memtier tests
    BENCH_VECTOR=1  pytest benchmarks/bench_memtier.py -s -v  # runs vector benchmarks

Results are written to benchmarks/results/ as JSON + CSV for CI artifact upload.
"""
from __future__ import annotations

import os
import json
import csv
import math
import time
import struct
import subprocess
import threading
import statistics
import pathlib
import pytest
import redis
from utils.helpers import (
    KIVI_HOST, KIVI_PORT, make_client, make_bytes_client, vec_to_bytes,
)

RESULTS_DIR = pathlib.Path(__file__).parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)

RUN_MEMTIER = os.environ.get("BENCH_MEMTIER", "0") == "1"
RUN_VECTOR = os.environ.get("BENCH_VECTOR", "0") == "1"

MEMTIER_BIN = os.environ.get("MEMTIER_BIN", "memtier_benchmark")
THREADS = int(os.environ.get("BENCH_THREADS", "4"))
CLIENTS = int(os.environ.get("BENCH_CLIENTS", "20"))
REQUESTS = int(os.environ.get("BENCH_REQUESTS", "100000"))
PIPELINE = int(os.environ.get("BENCH_PIPELINE", "1"))
DATA_SIZE = int(os.environ.get("BENCH_DATA_SIZE", "64"))


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _run_memtier(test_name: str, extra_args: list[str]) -> dict:
    """
    Run memtier_benchmark and parse the summary line.
    Returns a dict with ops/sec, latency_avg_ms, latency_p99_ms.
    """
    json_out = RESULTS_DIR / f"{test_name}.json"
    cmd = [
        MEMTIER_BIN,
        "--server", KIVI_HOST,
        "--port", str(KIVI_PORT),
        "--protocol", "redis",
        "--threads", str(THREADS),
        "--clients", str(CLIENTS),
        "--requests", str(REQUESTS),
        "--pipeline", str(PIPELINE),
        "--data-size", str(DATA_SIZE),
        "--json-out-file", str(json_out),
        "--hide-histogram",
    ] + extra_args

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        pytest.fail(f"memtier_benchmark failed:\n{result.stderr}")

    with open(json_out) as f:
        data = json.load(f)

    # Extract totals from the JSON report
    totals = data.get("ALL STATS", {}).get("Totals", {})
    return {
        "test": test_name,
        "ops_per_sec": totals.get("Ops/sec", 0),
        "latency_avg_ms": totals.get("Latency", {}).get("Average", 0),
        "latency_p99_ms": totals.get("Latency", {}).get("p99.00", 0),
        "throughput_mb_s": totals.get("KB/sec", 0) / 1024,
    }


VECTOR_RESULTS_PATH = RESULTS_DIR / "vector_bench.json"


def _save_vector_result(key: str, data: dict) -> None:
    """
    Merge one named result into results/vector_bench.json, so all three
    vector benchmarks (which run as separate test functions) end up in one
    file a CI workflow can diff against a baseline, rather than each test
    clobbering the last one's output.
    """
    existing: dict = {}
    if VECTOR_RESULTS_PATH.exists():
        try:
            existing = json.loads(VECTOR_RESULTS_PATH.read_text())
        except json.JSONDecodeError:
            existing = {}
    existing[key] = data
    VECTOR_RESULTS_PATH.write_text(json.dumps(existing, indent=2))


def _save_results(results: list[dict]) -> None:
    csv_path = RESULTS_DIR / "benchmark_summary.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)
    json_path = RESULTS_DIR / "benchmark_summary.json"
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nBenchmark results saved to {RESULTS_DIR}")


# ──────────────────────────────────────────────────────────────────────────────
# memtier benchmark tests
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def bench_client(kivi_server):
    c = make_client()
    c.flushdb()
    yield c
    c.close()


@pytest.mark.skipif(not RUN_MEMTIER, reason="Set BENCH_MEMTIER=1 to run")
class TestMemtierBenchmarks:
    results: list[dict] = []

    def test_get_throughput(self, bench_client):
        bench_client.flushdb()
        # Pre-populate keys
        pipe = bench_client.pipeline(transaction=False)
        for i in range(10000):
            pipe.set(f"bench:key{i}", "x" * DATA_SIZE)
        pipe.execute()

        res = _run_memtier("get_throughput", [
            "--ratio", "0:1",            # GET only
            "--key-prefix", "bench:key",
            "--key-maximum", "10000",
        ])
        self.__class__.results.append(res)
        print(f"\n[GET] {res['ops_per_sec']:,.0f} ops/sec, "
              f"avg {res['latency_avg_ms']:.3f}ms, p99 {res['latency_p99_ms']:.3f}ms")
        assert res["ops_per_sec"] > 0

    def test_set_throughput(self, bench_client):
        bench_client.flushdb()
        res = _run_memtier("set_throughput", [
            "--ratio", "1:0",            # SET only
        ])
        self.__class__.results.append(res)
        print(f"\n[SET] {res['ops_per_sec']:,.0f} ops/sec, "
              f"avg {res['latency_avg_ms']:.3f}ms, p99 {res['latency_p99_ms']:.3f}ms")
        assert res["ops_per_sec"] > 0

    def test_mixed_rw(self, bench_client):
        bench_client.flushdb()
        res = _run_memtier("mixed_rw", [
            "--ratio", "1:4",            # 20% SET, 80% GET
        ])
        self.__class__.results.append(res)
        print(f"\n[MIXED 1:4] {res['ops_per_sec']:,.0f} ops/sec")
        assert res["ops_per_sec"] > 0

    def test_pipelined_get(self, bench_client):
        bench_client.flushdb()
        pipe = bench_client.pipeline(transaction=False)
        for i in range(10000):
            pipe.set(f"pk{i}", "x" * DATA_SIZE)
        pipe.execute()

        res = _run_memtier("pipelined_get", [
            "--ratio", "0:1",
            "--pipeline", "10",
            "--key-prefix", "pk",
            "--key-maximum", "10000",
        ])
        self.__class__.results.append(res)
        print(f"\n[PIPELINE=10 GET] {res['ops_per_sec']:,.0f} ops/sec")
        assert res["ops_per_sec"] > 0

    def test_lpush_lpop(self, bench_client):
        bench_client.flushdb()
        res = _run_memtier("list_push_pop", [
            "--command", "LPUSH list_bench __data__",
            "--command", "LPOP list_bench",
            "--command-ratio", "1",
        ])
        self.__class__.results.append(res)
        print(f"\n[LPUSH/LPOP] {res['ops_per_sec']:,.0f} ops/sec")
        assert res["ops_per_sec"] > 0

    def test_incr(self, bench_client):
        bench_client.flushdb()
        bench_client.set("counter", "0")
        res = _run_memtier("incr", [
            "--command", "INCR counter",
            "--command-ratio", "1",
        ])
        self.__class__.results.append(res)
        print(f"\n[INCR] {res['ops_per_sec']:,.0f} ops/sec")
        assert res["ops_per_sec"] > 0

    def test_hset_hget(self, bench_client):
        bench_client.flushdb()
        res = _run_memtier("hash_ops", [
            "--command", "HSET hash_bench field __data__",
            "--command", "HGET hash_bench field",
            "--command-ratio", "1",
        ])
        self.__class__.results.append(res)
        print(f"\n[HSET/HGET] {res['ops_per_sec']:,.0f} ops/sec")
        assert res["ops_per_sec"] > 0

    def test_zadd_zrange(self, bench_client):
        bench_client.flushdb()
        res = _run_memtier("zset_ops", [
            "--command", "ZADD zbench 1 __data__",
            "--command", "ZRANGE zbench 0 10",
            "--command-ratio", "1",
        ])
        self.__class__.results.append(res)
        print(f"\n[ZADD/ZRANGE] {res['ops_per_sec']:,.0f} ops/sec")
        assert res["ops_per_sec"] > 0

    def test_sadd_smembers(self, bench_client):
        bench_client.flushdb()
        res = _run_memtier("set_ops", [
            "--command", "SADD sbench __data__",
            "--command", "SMEMBERS sbench",
            "--command-ratio", "1",
        ])
        self.__class__.results.append(res)
        print(f"\n[SADD/SMEMBERS] {res['ops_per_sec']:,.0f} ops/sec")
        assert res["ops_per_sec"] > 0

    @classmethod
    def teardown_class(cls):
        if cls.results:
            _save_results(cls.results)


# ──────────────────────────────────────────────────────────────────────────────
# Vector search benchmark (Python micro-bench — memtier can't express KNN)
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.skipif(not RUN_VECTOR, reason="Set BENCH_VECTOR=1 to run")
class TestVectorBenchmarks:
    DIM = 128
    N_VECTORS = 10_000
    K = 10
    N_QUERIES = 200
    CONCURRENT_CLIENTS = 8

    @pytest.fixture(scope="class", autouse=True)
    def setup_index(self, kivi_server):
        """Build a FLAT COSINE index with N_VECTORS random vectors."""
        import random
        random.seed(99)
        r = make_bytes_client()
        r.flushdb()

        # Create index
        r.execute_command(
            "FT.CREATE", "bench_idx",
            "ON", "HASH",
            "PREFIX", "1", "bv:",
            "SCHEMA", "vec", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32",
            "DIM", str(self.DIM),
            "DISTANCE_METRIC", "COSINE",
        )

        # Bulk insert
        pipe = r.pipeline(transaction=False)
        for i in range(self.N_VECTORS):
            vec = [random.gauss(0, 1) for _ in range(self.DIM)]
            pipe.hset(f"bv:{i}", mapping={"vec": vec_to_bytes(vec)})
            if i % 500 == 0:
                pipe.execute()
                pipe = r.pipeline(transaction=False)
        pipe.execute()
        time.sleep(0.2)  # let index settle
        yield r
        r.flushdb()
        r.close()

    def _knn_query(self, r, query_vec):
        raw = r.execute_command(
            "FT.SEARCH", "bench_idx",
            f"*=>[KNN {self.K} @vec $vec AS score]",
            "PARAMS", "2", "vec", vec_to_bytes(query_vec),
            "RETURN", "1", "score",
            "DIALECT", "2",
        )
        return raw[0]

    def test_knn_latency_serial(self, setup_index, kivi_server):
        import random
        random.seed(42)
        r = setup_index
        latencies = []
        for _ in range(self.N_QUERIES):
            q = [random.gauss(0, 1) for _ in range(self.DIM)]
            t0 = time.perf_counter()
            self._knn_query(r, q)
            latencies.append((time.perf_counter() - t0) * 1000)

        p50 = statistics.median(latencies)
        p99 = sorted(latencies)[int(len(latencies) * 0.99)]
        avg = statistics.mean(latencies)
        print(f"\n[FLAT KNN dim={self.DIM} n={self.N_VECTORS} k={self.K}]")
        print(f"  Serial  avg={avg:.2f}ms  p50={p50:.2f}ms  p99={p99:.2f}ms  "
              f"({self.N_QUERIES} queries)")

        _save_vector_result("knn_flat_serial", {
            "dim": self.DIM, "n_vectors": self.N_VECTORS, "k": self.K,
            "queries": self.N_QUERIES,
            "avg_ms": round(avg, 3), "p50_ms": round(p50, 3), "p99_ms": round(p99, 3),
            "qps": round(1000 / avg, 1),
        })

        assert p99 < 500  # sanity: p99 < 500ms for 10k FLAT

    def test_knn_throughput_concurrent(self, setup_index, kivi_server):
        import random
        random.seed(1)
        queries_per_client = self.N_QUERIES // self.CONCURRENT_CLIENTS
        all_latencies = []
        lock = threading.Lock()

        def worker():
            c = make_bytes_client()
            lats = []
            for _ in range(queries_per_client):
                q = [random.gauss(0, 1) for _ in range(self.DIM)]
                t0 = time.perf_counter()
                self._knn_query(c, q)
                lats.append((time.perf_counter() - t0) * 1000)
            c.close()
            with lock:
                all_latencies.extend(lats)

        t_start = time.perf_counter()
        threads = [threading.Thread(target=worker) for _ in range(self.CONCURRENT_CLIENTS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.perf_counter() - t_start

        total_queries = queries_per_client * self.CONCURRENT_CLIENTS
        qps = total_queries / elapsed
        p50 = statistics.median(all_latencies)
        p99 = sorted(all_latencies)[int(len(all_latencies) * 0.99)]

        print(f"\n[FLAT KNN concurrent clients={self.CONCURRENT_CLIENTS}]")
        print(f"  QPS={qps:.0f}  p50={p50:.2f}ms  p99={p99:.2f}ms  "
              f"elapsed={elapsed:.2f}s")

        _save_vector_result("knn_flat_concurrent", {
            "dim": self.DIM, "n_vectors": self.N_VECTORS, "k": self.K,
            "clients": self.CONCURRENT_CLIENTS, "queries": total_queries,
            "elapsed_s": round(elapsed, 3),
            "qps": round(qps, 1), "p50_ms": round(p50, 3), "p99_ms": round(p99, 3),
        })

        assert qps > 0

    def test_vsim_throughput(self, kivi_server):
        """Benchmark VSIM (Vector Set) operations."""
        import random
        random.seed(7)
        DIM = 32
        N = 1000
        r = make_bytes_client()
        r.flushdb()

        for i in range(N):
            vec = [random.gauss(0, 1) for _ in range(DIM)]
            args = ["VADD", "vs_bench", "VALUES", str(DIM)]
            args += [str(x) for x in vec]
            args.append(f"item{i}")
            r.execute_command(*args)

        latencies = []
        for _ in range(200):
            q = [random.gauss(0, 1) for _ in range(DIM)]
            args = ["VSIM", "vs_bench", "VALUES", str(DIM)] + [str(x) for x in q] + ["COUNT", "10"]
            t0 = time.perf_counter()
            r.execute_command(*args)
            latencies.append((time.perf_counter() - t0) * 1000)

        avg = statistics.mean(latencies)
        p99 = sorted(latencies)[int(len(latencies) * 0.99)]
        print(f"\n[VSIM dim={DIM} n={N} k=10]  avg={avg:.2f}ms  p99={p99:.2f}ms")

        _save_vector_result("vsim", {
            "dim": DIM, "n": N, "k": 10, "queries": len(latencies),
            "avg_ms": round(avg, 3), "p99_ms": round(p99, 3),
            "qps": round(1000 / avg, 1),
        })

        r.flushdb()
        r.close()
        assert p99 < 500