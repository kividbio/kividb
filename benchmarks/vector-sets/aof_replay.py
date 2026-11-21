#!/usr/bin/env python3
"""How long a KiviDB server takes to replay an AOF of vector-set writes (#710).

Starts the server with the AOF on in an empty directory, loads N vectors of a
dataset with VADD (Q8 unless --quant), waits for the AOF to be on disk, kills
the server (no snapshot, and automatic rewrites off: the AOF is every VADD), starts it again
on the same files and reports:

  replay_s   from the start until the server answers PING: the replay
  graph_s    from then until VSIM searches the set's graph (VINFO index-type
             HNSW); a replay writes no graph, which is made after it in the
             background, as after a snapshot load; 0 for a server with no
             graph (1.2)

  aof_replay.py --bin target/release/kividb --ds sift128 --n 1000000

Datasets as in `load_time.py` (regression suite 44's files). Prints one JSON
line. Needs redis, numpy, h5py.
"""
import argparse
import json
import multiprocessing as mp
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time

import h5py
import numpy as np
import redis

import load_time

KEY = "vs"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--ds", choices=sorted(load_time.FILES), default="sift128")
    ap.add_argument("--n", type=int, default=1_000_000)
    ap.add_argument("--quant", default="Q8")
    ap.add_argument("--port", type=int, default=17730)
    a = ap.parse_args()
    load_time.PORT, load_time.QUANT = a.port, a.quant
    d = os.environ.get("VSET_DATASETS") or os.path.expanduser("~/.cache/kividb-regress/datasets")
    with h5py.File(os.path.join(d, load_time.FILES[a.ds]), "r") as f:
        load_time.DATA = np.ascontiguousarray(f["train"][:a.n], dtype="<f4")
    work = tempfile.mkdtemp(prefix="vset-aof.")
    # No rewrite: the AOF is every VADD, not a snapshot of the set.
    args = [a.bin, "--port", str(a.port), "--bind", "127.0.0.1", "--threads", "4", "--aof",
            "--auto-aof-rewrite-percentage", "0"]
    out = {"bin": a.bin, "ds": a.ds, "n": a.n, "quant": a.quant}

    def start():
        return subprocess.Popen(args, cwd=work, stdout=open(os.path.join(work, "server.log"), "ab"),
                                stderr=subprocess.STDOUT)

    proc = start()
    try:
        load_time.wait_ping(a.port, proc)
        t = time.perf_counter()
        with mp.get_context("fork").Pool(4) as pool:
            pool.map(load_time.loader, range(4))
        out["vadd_s"] = round(time.perf_counter() - t, 2)
        time.sleep(3)  # the AOF's fsync (everysec)
        proc.send_signal(signal.SIGKILL)
        proc.wait(60)
        out["aof_bytes"] = sum(os.path.getsize(os.path.join(dp, f))
                               for dp, _, fs in os.walk(work) for f in fs if "aof" in f or "appendonly" in f)
        proc = start()
        out["replay_s"] = round(load_time.wait_ping(a.port, proc), 2)
        c = redis.Redis(port=a.port, socket_timeout=3600)
        out["vcard"] = int(c.execute_command("VCARD", KEY))
        t = time.perf_counter()
        while load_time.index_type(c) != "HNSW":
            time.sleep(0.05)
        out["graph_s"] = round(time.perf_counter() - t, 2)
    finally:
        try:
            redis.Redis(port=a.port, socket_timeout=5).execute_command("SHUTDOWN", "NOSAVE")
        except Exception:  # noqa: BLE001 - it may be gone already
            pass
        proc.kill()
        shutil.rmtree(work, ignore_errors=True)
    print(json.dumps(out))


if __name__ == "__main__":
    sys.exit(main())
