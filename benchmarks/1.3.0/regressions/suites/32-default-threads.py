#!/usr/bin/env python3
"""The default worker count, watched on purpose and in one place.

Every other configuration states its `--threads` (the count its values were
recorded with), so none of them measures the default policy along with the
code. The default changed in 1.3.0 (#903: one worker per available CPU, then half the available CPUs, rounded up;
1.2 started the CPUs minus two, all of them below four) and what is best
depends on where the clients run: with the load generator on the same 4
vCPUs, suite 05's `XADD` row served 594k/s with 2 workers and 426k with 4.
This suite measures the default against the best explicit count of the same
build, in the same session, on the machine it is recorded on.

One self-hosted configuration (KiviDB only: Redis has no worker count). For
each of 3 rounds, the suite starts the server once with no `--threads` (the
default; its banner's count is kept as `default_threads`) and once with each
explicit count from 1 to the machine's CPUs (1-4 on the Standard_D4s_v5),
unpinned, the load generators on the same CPUs (the "one box" case), and
measures four rows on it:

  get_c50_rps        unpipelined GET, redis-benchmark -c 50, 100,000 keys of
                     100 B (set first), 5 s
  set_p16_rps        SET, -c 50 -P 16, the same keys, 5 s
  xadd5_rps          suite 05's XADD of five fields, -c 4 -P 16, 5 s
  pubsub_per_s       suite 23's pattern fan-out (48 publishers, 16 pattern
                     subscribers), messages delivered a second, one round of
                     300,000 publishes

For each row, medians of the 3 rounds: `<row>_default` (the default's rate),
`<row>_best` (the best explicit count's), `<row>_best_threads` (which count
that was) and `<row>_default_vs_best` (default / best).

Gates: `pubsub_lost` 0, exactly. `<row>_default_vs_best` is recorded and
shown but INFORMATIONAL: with the default of one per CPU XADD's
ratio was about 0.71 on the Standard_D4s_v5 (half the CPUs, rounded up, replaced it), so a gate (at least
0.90: the default at most 10 % under the best explicit count of the same
build) would have failed on master the day the suite landed. It can become that gate: the default
worker count is settled now, and the recorded rows say where it stands. The rates are
shown, not gated (the other suites gate the code at fixed counts). v1.2.2 runs too, as a reference of the old rule
(its ratio is shown, not gated).
"""
import importlib.util
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, EXACT, bound, info, median, scaled  # noqa: E402
from nodes import Nodes, redis_tool, redis_benchmark_for  # noqa: E402
import subprocess  # noqa: E402

S = Suite("32-default-threads", [Config("shared", threads=0, engines=("v1_2_2", "candidate"), self_hosted=True)])
ROUNDS = 3
KEYS = 100_000
# Informational: with the default of #903 (one worker per CPU) XADD's
# default/best is about 0.71 on the Standard_D4s_v5, so a 0.90 gate would fail the
# release gate on the day the suite lands. The rows are recorded and shown; they
# become gates (bound(min=0.9)) once the default worker count is settled, a
# decision that is with the owner. pubsub_lost stays an exact gate.
DEFAULT_VS_BEST = info()

_spec = importlib.util.spec_from_file_location(
    "suite23", os.path.join(os.path.dirname(os.path.abspath(__file__)), "23-pubsub-pattern-fanout.py"))
s23 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(s23)


def counts():
    n = os.cpu_count() or 1
    return sorted({c for c in (1, 2, 3, 4, (n + 1) // 2, n - 1, n) if 1 <= c <= n})


class _Port:
    """What suite 23's one_round needs of a ctx."""

    def __init__(self, port):
        self.port = port


def measure(node, secs):
    port = node.port
    subprocess.run([redis_tool("redis-benchmark"), "-p", str(port), "-q", "-c", "50", "-t", "set",
                    "-n", str(scaled(200_000)), "-r", str(KEYS), "-d", "100"],
                   capture_output=True, text=True, timeout=600, check=True)
    out = {
        "get_c50_rps": redis_benchmark_for(port, secs, ["-c", "50", "-t", "get", "-r", str(KEYS), "-d", "100"]),
        "set_p16_rps": redis_benchmark_for(port, secs, ["-c", "50", "-P", "16", "-t", "set", "-r", str(KEYS),
                                                        "-d", "100"]),
        "xadd5_rps": redis_benchmark_for(port, secs, ["-c", "4", "-P", "16", "XADD", "w5", "*", "f1", "v1", "f2",
                                                      "v2", "f3", "v3", "f4", "v4", "f5", "v5"]),
    }
    p = _Port(port)
    s23.one_round(p, "pattern", scaled(30_000, step=1000), probe_runs=False)  # warm-up
    rate, _, _, lost = s23.one_round(p, "pattern", scaled(300_000, step=1000), probe_runs=False)
    out["pubsub_per_s"] = rate
    return out, lost


@S.run
def run(ctx):
    secs = 5 if float(os.environ.get("SCALE", "1") or 1) == 1 else 1
    ns_counts = counts()
    got = {}       # threads (0 = default) -> row -> [values]
    lost = 0
    default_threads = []
    logs = {}
    for rnd in range(ROUNDS):
        for t in [0] + ns_counts:
            with Nodes(ctx) as ns:
                node = ns.start(threads=t)
                if t == 0:
                    m = re.search(r"Worker threads\s*:\s*(\d+)", node.log_tail())
                    default_threads.append(int(m.group(1)) if m else None)
                vals, l = measure(node, secs)
            logs["r%d-t%d" % (rnd, t)] = ctx.raw.pop("node_logs", {})
            lost += l
            for k, v in vals.items():
                got.setdefault(t, {}).setdefault(k, []).append(v)
            ctx.log("round %d, threads %s: %s" % (rnd, t or "default", ", ".join(
                "%s %.0f" % kv for kv in sorted(vals.items()))))
    ctx.raw["node_logs"] = logs
    ctx.raw["rates"] = {str(t): rows for t, rows in got.items()}
    ctx.emit("default_threads", median(default_threads), "threads", "higher", gate=info(),
             samples=default_threads)
    for row in ("get_c50_rps", "set_p16_rps", "xadd5_rps", "pubsub_per_s"):
        unit = "msg/s" if row == "pubsub_per_s" else "req/s"
        d = median(got[0][row])
        best_t = max(ns_counts, key=lambda t: median(got[t][row]))
        best = median(got[best_t][row])
        ctx.emit(row + "_default", d, unit, "higher", gate=info(), samples=got[0][row])
        ctx.emit(row + "_best", best, unit, "higher", gate=info(), samples=got[best_t][row])
        ctx.emit(row + "_best_threads", best_t, "threads", "higher", gate=info())
        ctx.emit(row + "_default_vs_best", d / best if best else None, "ratio", "higher", gate=DEFAULT_VS_BEST)
    ctx.emit("pubsub_lost", lost, "messages", "lower", gate=EXACT)


if __name__ == "__main__":
    sys.exit(S.main())
