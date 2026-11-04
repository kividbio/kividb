#!/usr/bin/env python3
"""Does AOF load time track the number of keys, or the number of writes?

Takes the compacted AOF produced by bench_aof_vs_kdb_load.py and concatenates it
N times. Every pass rewrites the same keys, so the recovered dataset is
identical while the log holds N times the records — the shape an AOF takes
during a benchmark that overwrites its keyspace and is never rewritten.

Usage: bench_aof_amplified.py [multiplier] [binary]
"""

import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bench_aof_vs_kdb_load import Client, WORK, mb, start, stop, vm  # noqa: E402

MULT = int(sys.argv[1]) if len(sys.argv) > 1 else 4
AMP = os.path.join(os.path.expanduser("~"), "kividb-aof-amp")
SRC = os.path.join(WORK, "appendonly.aof")


def main():
    if not os.path.exists(SRC):
        raise SystemExit(f"run bench_aof_vs_kdb_load.py first — {SRC} missing")

    shutil.rmtree(AMP, ignore_errors=True)
    os.makedirs(AMP)
    with open(SRC, "rb") as src, open(os.path.join(AMP, "appendonly.aof"), "wb") as dst:
        for _ in range(MULT):
            src.seek(0)
            shutil.copyfileobj(src, dst, 8 << 20)

    size = os.path.getsize(os.path.join(AMP, "appendonly.aof"))
    print(f"appendonly.aof: {mb(size)}  ({MULT}x the compacted log, same dataset)")

    import bench_aof_vs_kdb_load as bench
    bench.WORK = AMP
    # The imported module derives BIN from sys.argv, which means something else here.
    bench.BIN = sys.argv[2] if len(sys.argv) > 2 else os.path.join(
        bench.REPO, "target", "release",
        "kividb.exe" if sys.platform == "win32" else "kividb")

    proc, secs = start("--aof")
    c = Client(bench.PORT)
    keys = c.cmd("DBSIZE")
    rss, peak = vm(proc.pid, "VmRSS"), vm(proc.pid, "VmHWM")
    c.sock.close()
    print(f"\n{'load from appendonly.aof':<26} {secs:7.2f}s  keys={keys:<10} "
          f"rss={mb(rss):<10} peak={mb(peak)}")
    stop(proc)

    with open(os.path.join(AMP, "server.log"), errors="replace") as f:
        for line in f:
            if "AOF replay" in line:
                print(line.rstrip())

    print("=== AMPLIFIED AOF BENCH DONE ===")


main()
