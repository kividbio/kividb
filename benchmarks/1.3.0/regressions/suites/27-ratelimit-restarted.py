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
started again from that snapshot (KiviDB: `repl_backlog_active:1`, kept in
the raw data). On the standard 4-vCPU VM (two cores, two hyper-threads each,
the first four CPUs this process may use if it has more): the server's
threads on CPUs 0 and 2 (one hyper-thread of each core), the checks on 1, the
background load on 3:

  checks      65 connections, each a closed loop of checks with 2 ms of the
              limiter's own time after each (about 31,000 checks a second
              when the server keeps up)
  background  100 connections sending `INCRBY` + `EXPIRE` pairs, paced to
              BG_RATES commands a second in all

Needs a C compiler: the load is `lib/ratelimit_load.c`, built once into
$REGRESS_CACHE. The #894 issue's own shape (checks and load on CPUs of their
own, up to 300,000 a second) needs 8 CPUs; the PR measured it by hand.

Metrics, for each background rate (median of 3 rounds of 10 s):

  checks_per_s_bg<k>  checks a second
  p99_ms_bg<k>        p99 of a check, request to last reply

and then, alone, what it costs a write to be fed to the kept backlog (#894's
second step: each write appended to the hub's run of writes, not sent as a
message of its own): `redis-benchmark -t set -d 100 -r 100000 -c 50
--threads 2` on the checks' and the load's CPUs, median of 3 runs of 8 s:

  set_p16_per_s, set_p100_per_s   SETs a second, pipeline 16 and 100

Gates: checks a second 10 % and at most 1.05x worse than Redis this session;
the p99 is scheduling on a shared VM (jitter): 100 % or 0.5 ms, and with
background load, 60 % of the gain over v1.2.2 kept. Recorded on a D4s_v5
(rustc 1.100.0 builds), checks a second and p99, v1.2.2 / the change /
Redis: 31.0k 0.30 ms / 31.2k 0.22 ms / 31.2k 0.52 ms at 0; 30.6k 1.56 ms /
31.6k 0.15 ms / 31.9k 0.08 ms at 50,000; 27.8k 6.1 ms / 31.4k 0.19 ms /
31.8k 0.12 ms at 100,000. The SET rows: 10 % and 60 % of the gain over
v1.2.2 kept (Redis, restarted, keeps no backlog: shown, not gated),
recorded on a D4s_v5 (`results/pr-894-set-d4s_v5`), v1.2.2 / the change /
Redis: 762k / 1.06M / 785k at pipeline 16, 927k / 1.37M / 1.10M at 100.
"""
import os
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (Suite, Config, THROUGHPUT, KEEP_IMPROVEMENT, rel, vs_redis, jitter, median)  # noqa: E402
from nodes import Nodes, redis_tool  # noqa: E402

S = Suite("27-ratelimit-restarted", [Config("t2", threads=2, self_hosted=True)])
KEYS = 100_000
BG_RATES = (0, 50_000, 100_000)
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
    """Server, checks and background CPUs, of the first four this process may
    use: the server on the first and third (separate cores on a D4s_v5,
    whose CPUs 0-1 and 2-3 are the two hyper-threads of a core)."""
    have = sorted(os.sched_getaffinity(0))
    if len(have) < 4:
        raise RuntimeError("27-ratelimit-restarted needs 4 CPUs, this process may use %d" % len(have))
    return [have[0], have[2]], [have[1]], [have[3]]


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
            ctx.raw["backlog_active"] = node.client().info("replication").get("repl_backlog_active")
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
            gates = [rel(100, abs=0.5)] + ([KEEP_IMPROVEMENT] if rate else [])
            ctx.emit("p99_ms_" + k, median(p99), "ms", "lower", gate=jitter(gates), samples=p99)
        ctx.raw["rounds"] = rounds
        client_cpus = ",".join(map(str, fg_cpus + bg_cpus))
        for pipeline in SET_PIPELINES:
            args = ["-t", "set", "-d", "100", "-r", str(KEYS), "-c", "50", "--threads", "2",
                    "-P", str(pipeline)]
            set_rate(node.port, client_cpus, args)  # warm-up
            rates = [set_rate(node.port, client_cpus, args) for _ in range(ROUNDS)]
            ctx.emit("set_p%d_per_s" % pipeline, median(rates), "SETs/s", "higher",
                     gate=[THROUGHPUT, KEEP_IMPROVEMENT], samples=rates)
        if ctx.kind == "kividb":
            ctx.raw["backlog_active_after"] = node.client().info("replication").get("repl_backlog_active")


SET_PIPELINES = (16, 100)
SET_SECONDS = 8


def set_rate(port, cpus, args):
    """SETs a second, `redis-benchmark <args>` on `cpus` for SET_SECONDS (as
    `nodes.redis_benchmark_for` takes it, pinned)."""
    cmd = ["taskset", "-c", cpus, "timeout", "-s", "INT", str(SET_SECONDS), redis_tool("redis-benchmark"),
           "-p", str(port), "-q", "-n", "1000000000"] + list(args)
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=SET_SECONDS + 60).stdout
    m = re.findall(r"rps=[\d.]+ \(overall: ([\d.]+)\)", out.replace("\r", "\n"))
    if not m:
        raise RuntimeError("redis-benchmark printed no rate: %r" % out[-500:])
    return float(m[-1])


if __name__ == "__main__":
    sys.exit(S.main())
