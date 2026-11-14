#!/usr/bin/env python3
"""#605: SAVE runs off the Tokio runtime, so it blocks only its own client (#696).

`--io-model tokio --threads 1` (the setup of #696's test), 1,500,000 keys from
DEBUG POPULATE. Then three SAVEs, each with two PING probes (lib/probe.py,
one PING a millisecond each) running: the longest PING in flight during the
SAVE, and the SAVE's own time. Medians of the three.

On v1.2.2 a PING waits for the whole SAVE (532 ms of a 582 ms SAVE in #696).
Redis's SAVE blocks every client by design: its row is shown, not compared.

Gates: the worst PING during a SAVE is an absolute 20 ms (as for the long
commands of suite 04: a ratio against a sub-millisecond value is noise); the
SAVE time is shown, not gated: the same build took 339 and 503 ms in two
runs (the dump goes to the VM's disk).
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, rel, bound, info, scaled, median  # noqa: E402
from probe import Probe  # noqa: E402

S = Suite("07-save-ping", [Config("tokio1", io="tokio", threads=1)])


@S.run
def run(ctx):
    c = ctx.client()
    c.flushall()
    n = scaled(1_500_000)
    c.execute_command("DEBUG", "POPULATE", n)
    assert c.dbsize() == n, c.dbsize()
    c.save()  # warm up: the first dump creates the file
    worst, save_ms, each = [], [], []
    for _ in range(3):
        with Probe(ctx.port, n=2) as pr:
            time.sleep(0.2)
            t0 = time.perf_counter()
            c.save()
            t1 = time.perf_counter()
            time.sleep(0.1)
        worst.append(pr.worst(t0, t1))
        each.append(pr.worst_each(t0, t1))
        save_ms.append((t1 - t0) * 1000)
    ctx.raw["probe_worst_each"] = each
    gate = info() if ctx.kind == "redis" else [bound(max=20)]
    ctx.emit("ping_during_save_ms", median(worst), "ms", "lower", gate=gate, samples=worst)
    ctx.emit("ping_during_save_max_ms", max(worst), "ms", "lower", gate=gate)
    ctx.emit("save_ms", median(save_ms), "ms", "lower", gate=info(), samples=save_ms)


if __name__ == "__main__":
    sys.exit(S.main())
