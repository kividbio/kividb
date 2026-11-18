#!/usr/bin/env python3
"""#894: a rate limiter's checks beside other replicas' load, on a server
restarted from its snapshot.

envoyproxy/ratelimit sends, for each check, `INCRBY` + `EXPIRE` on each of
two keys, the four in one write, and waits for the replies. In #894 one real
replica ran 8 % fewer checks a second, at 1.7x Redis's p99, as other
replicas' load on the same KiviDB (2 cores, `--threads 2`) rose, though the
server was not saturated. The cause: a master that starts with data (a
snapshot of its own, or any dataset) keeps the replication backlog, for a
replica that may come back, for `repl-backlog-ttl` (an hour), and every write
was handed to the replication thread one by one, waking it each time. On two
CPUs that thread took them from the connections.

So each engine is started, loaded with 100,000 keys, saved and stopped, and
started again from that snapshot (KiviDB: `repl_backlog_active:1`, recorded).
Its threads are pinned to two CPUs of separate cores; the checks and the
background load run on CPUs of their own:

  checks      65 connections, each a closed loop of checks with 2 ms of the
              limiter's own time after each (about 31,000 checks a second
              when the server is idle)
  background  100 connections sending `INCRBY` + `EXPIRE` pairs, paced to
              BG_RATES commands a second in all

Needs 8 CPUs (server on CPUs 0 and 2, checks on 4-5, background on 6-7; two
background threads reach 300,000 commands a second) and a C compiler: the
load is `lib/ratelimit_load.c`, built once into $REGRESS_CACHE.

Metrics, for each background rate (median of 3 rounds of 10 s):

  checks_per_s_bg<k>  checks a second
  p99_ms_bg<k>        p99 of a check, request to last reply
  backlog_active      KiviDB's repl_backlog_active after the restart (1: the
                      case of #894; info)

Gates: checks a second 10 % and at most 1.05x worse than Redis this session;
the p99 is scheduling on a shared VM (jitter): 100 % or 0.5 ms, at most 2x
Redis's this session, and with background load, 60 % of the gain over v1.2.2
kept.
"""
import os
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (Suite, Config, THROUGHPUT, KEEP_IMPROVEMENT, rel, vs_redis, jitter, info, median)  # noqa: E402
from nodes import Nodes  # noqa: E402

S = Suite("25-ratelimit-restarted", [Config("t2", threads=2, self_hosted=True)])
KEYS = 100_000
BG_RATES = (0, 100_000, 200_000, 300_000)
ROUNDS = 3
SECONDS = 10
THINK_US = 2000
LIB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib")


def load_binary():
    """lib/ratelimit_load.c, built once into $REGRESS_CACHE."""
    cache = os.environ.get("REGRESS_CACHE", os.path.expanduser("~/.cache/kividb-regress"))
    os.makedirs(cache, exist_ok=True)
    src = os.path.join(LIB, "ratelimit_load.c")
    out = os.path.join(cache, "ratelimit_load")
    if not os.path.exists(out) or os.path.getmtime(out) < os.path.getmtime(src):
        subprocess.run([os.environ.get("CC", "cc"), "-O2", "-pthread", "-o", out, src], check=True)
    return out


def cpus():
    """Server, checks and background CPUs: two separate cores for the server."""
    have = sorted(os.sched_getaffinity(0))
    if len(have) < 8:
        raise RuntimeError("25-ratelimit-restarted needs 8 CPUs, this process may use %d" % len(have))
    return have[0:3:2], have[4:6], have[6:8]


def pin(pid, which):
    """Every thread of `pid` onto `which`; threads it starts later inherit it."""
    for tid in os.listdir("/proc/%d/task" % pid):
        os.sched_setaffinity(int(tid), which)


def parse(line):
    m = re.search(r"ops/s (\d+) cmds/s (\d+) p50 (\d+) p99 (\d+) p999 (\d+) max (\d+)", line)
    if not m:
        raise RuntimeError("ratelimit_load printed %r" % line)
    return {"ops": int(m.group(1)), "p99_us": int(m.group(4)), "p999_us": int(m.group(5))}


def one_round(load, port, rate, fg_cpus, bg_cpus):
    bg = None
    if rate:
        bg = subprocess.Popen(["taskset", "-c", ",".join(map(str, bg_cpus)), load, str(port), "bg", "100",
                               str(len(bg_cpus)), str(SECONDS + 4), str(rate), "1", "1", str(KEYS)],
                              stdout=subprocess.PIPE, text=True)
        time.sleep(1)
    fg = subprocess.run(["taskset", "-c", ",".join(map(str, fg_cpus)), load, str(port), "fg", "65",
                         str(len(fg_cpus)), str(SECONDS), str(THINK_US), "4", "1", str(KEYS)],
                        capture_output=True, text=True, timeout=SECONDS + 60)
    bg_out = bg.communicate(timeout=60)[0] if bg else ""
    if fg.returncode != 0:
        raise RuntimeError("checks failed: %s" % fg.stderr[-2000:])
    return parse(fg.stdout), (parse(bg_out) if bg else None)


def restarted(ns, ctx):
    """A server loaded, saved, stopped and started again from its snapshot."""
    first = ns.start()
    c = first.client()
    p = c.pipeline(transaction=False)
    for i in range(KEYS):
        p.set("rl:%d" % i, 1)
        if i % 5000 == 4999:
            p.execute()
    p.execute()
    c.save()
    first.stop()
    node = ns.start(dir=first.dir)
    dbsize = node.client().dbsize()
    if dbsize != KEYS:
        raise RuntimeError("restarted with %d keys, not %d" % (dbsize, KEYS))
    return node


@S.run
def run(ctx):
    load = load_binary()
    srv_cpus, fg_cpus, bg_cpus = cpus()
    with Nodes(ctx) as ns:
        node = restarted(ns, ctx)
        pin(node.pid, srv_cpus)
        if ctx.kind == "kividb":
            active = node.client().info("replication").get("repl_backlog_active", 0)
            ctx.emit("backlog_active", int(active), "flag", "lower", gate=info())
        ctx.raw["cpus"] = {"server": srv_cpus, "checks": fg_cpus, "background": bg_cpus}
        rounds = {}
        for rate in BG_RATES:
            one_round(load, node.port, rate, fg_cpus, bg_cpus)  # warm-up
            fgs, bgs = [], []
            for _ in range(ROUNDS):
                fg, bg = one_round(load, node.port, rate, fg_cpus, bg_cpus)
                fgs.append(fg)
                bgs.append(bg)
            rounds[rate] = {"checks": fgs, "background": bgs}
            k = "bg%dk" % (rate // 1000)
            ctx.emit("checks_per_s_" + k, median([f["ops"] for f in fgs]), "checks/s", "higher",
                     gate=[THROUGHPUT, vs_redis(1.05)], samples=[f["ops"] for f in fgs])
            p99 = [f["p99_us"] / 1e3 for f in fgs]
            gates = [rel(100, abs=0.5), vs_redis(2)] + ([KEEP_IMPROVEMENT] if rate else [])
            ctx.emit("p99_ms_" + k, median(p99), "ms", "lower", gate=jitter(gates), samples=p99)
        ctx.raw["rounds"] = rounds


if __name__ == "__main__":
    sys.exit(S.main())
