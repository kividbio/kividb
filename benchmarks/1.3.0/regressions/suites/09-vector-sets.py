#!/usr/bin/env python3
"""#697: quantized vector sets, Q8 and BIN (#730).

Runs benchmarks/1.3.0/vector-sets/bench.py (the harness #730 recorded with:
deterministic data, numpy seed 697, exact f32 ground truth; VADD pipelined
200 at a time, VSIM ... COUNT 10 from 8 client processes for 8 s) on the
server of each configuration and gates its numbers; it is not duplicated here.

  q8_768_50k   768 dimensions, 50,000 vectors, Q8: bytes a vector, recall@10
  bin_768_50k  the same with BIN
  q8_768_10k   768 dimensions, 10,000 vectors, Q8: VSIM q/s and p50
  f32_768_10k  the same stored as f32 (NOQUANT): the unquantized path of 1.3.0
               against 1.2.2's, which must not get slower

v1.2.2 has no quantization: it runs every configuration with the default
(stored f32), which is #730's "1.2.2" column. Redis 8.10.2 runs the same
quantization with its HNSW search (the PR's "with its HNSW graph" memory and
"HNSW" q/s; its exact scan, TRUTH, is not run here).

Gates: bytes a vector within 5 % and keeping 60 % of the gain; recall@10 not
more than 2 % under recorded (deterministic: any drop is a change in the
quantizer); VSIM q/s and VADD/s at 10K within 25 % (#730 saw repeats move by
up to 15 %). The Q8 q/s gain #730 measured on an Apple M3 Pro (7.7x at
768-d/10K) does not show on this x86-64 VM (see README.md), so there is no
gain to keep and no ratio gate on it.
"""
import json
import os
import subprocess
import sys

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(here, "..", "lib"))
from suite import Suite, Config, MEMORY, KEEP_IMPROVEMENT, rel, info  # noqa: E402

BENCH = os.path.normpath(os.path.join(here, "..", "..", "vector-sets", "bench.py"))
# name: (quantization, vectors, what is gated)
CONFIGS = {
    "q8_768_50k": ("Q8", 50000, "memory"),
    "bin_768_50k": ("BIN", 50000, "memory"),
    "q8_768_10k": ("Q8", 10000, "speed"),
    "f32_768_10k": ("NOQUANT", 10000, "speed"),
}
S = Suite("09-vector-sets", [Config(n) for n in CONFIGS])


@S.run
def run(ctx):
    quant, n, what = CONFIGS[ctx.config.name]
    if os.environ.get("SCALE", "1") not in ("", "1"):
        n = 2000
    if ctx.engine == "v1_2_2":
        quant = ""  # stored f32: v1.2.2 has no quantization
    out = os.path.join(ctx.out, "raw", S.name, "%s-%s.jsonl" % (ctx.config.name, ctx.engine))
    os.makedirs(os.path.dirname(out), exist_ok=True)
    if os.path.exists(out):
        os.remove(out)
    cmd = [sys.executable, BENCH, "--port", str(ctx.port), "--label", ctx.engine, "--quant", quant,
           "--dim", "768", "--n", str(n), "--out", out]
    subprocess.run(cmd, check=True, timeout=1500)
    with open(out) as f:
        row = json.loads(f.read().strip().splitlines()[-1])
    ctx.raw["bench_row"] = row
    note = "stored as " + row["quant"]
    ctx.emit("recall_at_10", row["recall_at_10"], "ratio", "higher", gate=rel(2), note=note)
    if what == "memory":
        ctx.emit("bytes_per_vector", row["bytes_per_vector"], "B", "lower", gate=[MEMORY, KEEP_IMPROVEMENT])
        ctx.emit("vsim_qps", row["vsim_qps"], "q/s", "higher", gate=info())
        ctx.emit("vadd_per_s", row["vadd_per_s"], "VADD/s", "higher", gate=info())
    else:
        ctx.emit("vsim_qps", row["vsim_qps"], "q/s", "higher", gate=rel(25))
        ctx.emit("vsim_p50_ms", row["vsim_p50_ms"], "ms", "lower", gate=info())
        ctx.emit("vadd_per_s", row["vadd_per_s"], "VADD/s", "higher", gate=rel(25))


if __name__ == "__main__":
    sys.exit(S.main())
