#!/usr/bin/env python3
"""How long a vector set takes to come back after a restart (#710).

Starts the server in an empty directory, loads N vectors of a dataset with
VADD (Q8 unless --quant), saves a snapshot, stops the server, starts it again
on the same files and reports:

  load_s    from the start until the server answers PING (the snapshot read)
  graph_s   from then until VSIM searches the set's graph: KiviDB's VINFO
            index-type HNSW (the graph is not saved; it is made again from
            the vectors, in the background, and VSIM scans meanwhile); 0 for
            a server that saves its graph (Redis)
  vsim_ms_during / vsim_ms_after
            one client's mean VSIM ... COUNT 10 latency while the graph is
            made and after

  load_time.py --kind kividb --bin target/release/kividb --ds sift128 --n 1000000
  load_time.py --kind redis --bin /opt/redis-8.10.2/src/redis-server --ds laion768 --n 100000

The datasets are those of regression suite 44 (same files, same cache
directory, $VSET_DATASETS or ~/.cache/kividb-regress/datasets). Prints one
JSON line. Needs redis, numpy, h5py.
"""
import argparse
import json
import multiprocessing as mp
import os
import shutil
import subprocess
import sys
import tempfile
import time

import h5py
import numpy as np
import redis

FILES = {
    "glove100": "glove-100-angular.hdf5",
    "sift128": "sift-128-euclidean.hdf5",
    "laion768": "laion-img-emb-768-1M-cosine.hdf5",
}
KEY = "vs"
DATA = None
PORT = None
QUANT = "Q8"


def loader(rank, procs=4, batch=500):
    c = redis.Redis(port=PORT, socket_timeout=3600)
    lo, hi = len(DATA) * rank // procs, len(DATA) * (rank + 1) // procs
    for s in range(lo, hi, batch):
        p = c.pipeline(transaction=False)
        for i in range(s, min(hi, s + batch)):
            p.execute_command("VADD", KEY, "FP32", DATA[i].tobytes(), str(i), QUANT)
        p.execute()


def start(kind, binary, port, d):
    if kind == "redis":
        args = [binary, "--port", str(port), "--bind", "127.0.0.1", "--appendonly", "no", "--dir", d]
    else:
        args = [binary, "--port", str(port), "--bind", "127.0.0.1", "--threads", "4"]
    return subprocess.Popen(args, cwd=d, stdout=open(os.path.join(d, "server.log"), "ab"),
                            stderr=subprocess.STDOUT)


def wait_ping(port, proc, secs=3600):
    t = time.perf_counter()
    while time.perf_counter() - t < secs:
        if proc.poll() is not None:
            raise SystemExit("the server exited")
        try:
            if redis.Redis(port=port, socket_timeout=1).ping():
                return time.perf_counter() - t
        except (redis.ConnectionError, redis.TimeoutError, redis.ResponseError):
            pass
        time.sleep(0.02)
    raise SystemExit("no PING in %d s" % secs)


def index_type(c):
    v = c.execute_command("VINFO", KEY)
    d = dict(zip(v[0::2], v[1::2])) if isinstance(v, list) else v
    d = {(k.decode() if isinstance(k, bytes) else k): x for k, x in d.items()}
    if "hnsw-m" not in d:
        return "HNSW"  # a server with no graph to make (Redis saves its own; 1.2 scans)
    t = d.get("index-type", b"HNSW")
    return t.decode() if isinstance(t, bytes) else t


def mean_vsim_ms(c, queries, until=None, secs=3.0):
    t, n = time.perf_counter(), 0
    while time.perf_counter() - t < secs and (until is None or not until()):
        c.execute_command("VSIM", KEY, "FP32", queries[n % len(queries)].tobytes(), "COUNT", "10")
        n += 1
    return (time.perf_counter() - t) / max(n, 1) * 1000


def main():
    global DATA, PORT, QUANT
    ap = argparse.ArgumentParser()
    ap.add_argument("--kind", choices=("kividb", "redis"), required=True)
    ap.add_argument("--bin", required=True)
    ap.add_argument("--ds", choices=sorted(FILES), default="sift128")
    ap.add_argument("--n", type=int, default=1_000_000)
    ap.add_argument("--quant", default="Q8")
    ap.add_argument("--port", type=int, default=17720)
    a = ap.parse_args()
    PORT, QUANT = a.port, a.quant
    d = os.environ.get("VSET_DATASETS") or os.path.expanduser("~/.cache/kividb-regress/datasets")
    with h5py.File(os.path.join(d, FILES[a.ds]), "r") as f:
        DATA = np.ascontiguousarray(f["train"][:a.n], dtype="<f4")
        queries = np.ascontiguousarray(f["test"][:200], dtype="<f4")
    work = tempfile.mkdtemp(prefix="vset-load.")
    out = {"kind": a.kind, "ds": a.ds, "n": a.n, "quant": QUANT}
    proc = start(a.kind, a.bin, PORT, work)
    try:
        wait_ping(PORT, proc)
        t = time.perf_counter()
        with mp.get_context("fork").Pool(4) as pool:
            pool.map(loader, range(4))
        out["vadd_s"] = round(time.perf_counter() - t, 2)
        c = redis.Redis(port=PORT, socket_timeout=3600)
        t = time.perf_counter()
        c.execute_command("SAVE")
        out["save_s"] = round(time.perf_counter() - t, 2)
        try:
            c.execute_command("SHUTDOWN", "NOSAVE")
        except redis.ConnectionError:
            pass
        proc.wait(120)
        sizes = [os.path.getsize(os.path.join(work, f)) for f in os.listdir(work)
                 if f.startswith("dump.")]
        out["snapshot_bytes"] = max(sizes) if sizes else None
        proc = start(a.kind, a.bin, PORT, work)
        out["load_s"] = round(wait_ping(PORT, proc), 2)
        c = redis.Redis(port=PORT, socket_timeout=3600)
        if int(c.execute_command("VCARD", KEY)) != a.n:
            raise SystemExit("VCARD after the restart is not %d" % a.n)
        t = time.perf_counter()
        building = lambda: index_type(c) == "HNSW"  # noqa: E731
        out["vsim_ms_during"] = None if building() else round(mean_vsim_ms(c, queries, building, 3.0), 3)
        while not building():
            time.sleep(0.05)
        out["graph_s"] = round(time.perf_counter() - t, 2)
        out["vsim_ms_after"] = round(mean_vsim_ms(c, queries), 3)
    finally:
        try:
            redis.Redis(port=PORT, socket_timeout=5).execute_command("SHUTDOWN", "NOSAVE")
        except Exception:  # noqa: BLE001 - it may be gone already
            pass
        proc.kill()
        shutil.rmtree(work, ignore_errors=True)
    print(json.dumps(out))


if __name__ == "__main__":
    sys.exit(main())
