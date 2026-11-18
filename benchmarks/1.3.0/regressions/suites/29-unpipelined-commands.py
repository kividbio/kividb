#!/usr/bin/env python3
"""Plain, unpipelined commands on many connections, against v1.2.2.

An API gateway (Tyk) sends its few commands a request one at a time, from
many connections: GET, PTTL, INCR, SET of a 385-byte session, PUBLISH of a
177-byte message to a channel nobody subscribes to. During 1.3.0 master
served about 10 % fewer of them a second than v1.2.2 with both at 100 % of
two cores, although it ran fewer instructions a command: the io_uring loop
submitted the next read with every write (#865), before the client could
have answered, so the kernel parked that read on every turn and woke the
thread for it later; a read submitted a turn later, as v1.2.2's was, finds
the request there already. The loop now sends without waiting first, and
reads on while it writes only once the socket is full.

One server per configuration, `--threads 2`, its threads pinned to the
first two CPUs this process may use; `redis-benchmark --threads 2` on the
next two, 64 connections (`-c 64`, no `-P`), keys `key:<0..4999>`
(`-r 5000`). Rows, in this order on one server: `SET` (385 bytes; it also
loads the keys the others read), `GET`, `INCR`, `PTTL`, `PUBLISH` (177
bytes, no subscriber). Each runs 2 s of warm-up, then the server's own
`total_commands_processed` is read over a 6 s window (redis-benchmark's
own rate jumps in 250 ms steps with `--threads`); median of 3 rounds.

Gates: each rate at most 1.05x worse than v1.2.2 measured in the same
session (`vs_v1_2_2`). The client must not be the limit: if redis-benchmark
used 1.9 of its 2 CPUs while the server used less than 1.9 of its 2, the
row says nothing about the server, and the rate is emitted ungated
(`client_limited` 1) rather than failed. With fewer than 4 CPUs to use,
the suite measures nothing and emits `skipped_cpus` (info) only.
"""
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, vs_v1_2_2, info, median  # noqa: E402
from nodes import redis_tool  # noqa: E402

ENGINES = ("v1_2_2", "candidate")
S = Suite("29-unpipelined-commands", [
    Config("uring", io="uring", threads=2, engines=ENGINES),
    Config("tokio", io="tokio", threads=2, engines=ENGINES),
])
CONNS = 64
KEYS = 5000
ROUNDS = 3
WARM_S = 2.0
WINDOW_S = 6.0
SESSION = "s" * 385
MESSAGE = "m" * 177
ROWS = [
    ("set", ["SET", "key:__rand_int__", SESSION]),
    ("get", ["GET", "key:__rand_int__"]),
    ("incr", ["INCR", "counter:__rand_int__"]),
    ("pttl", ["PTTL", "key:__rand_int__"]),
    ("publish", ["PUBLISH", "notifications", MESSAGE]),
]
GATE = [vs_v1_2_2(1.05)]


def cpu_seconds(pid):
    """utime + stime of every thread of pid, in seconds."""
    with open("/proc/%d/stat" % pid) as f:
        fields = f.read().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def pin(pid, cpus):
    """Every thread of pid onto cpus."""
    for tid in os.listdir("/proc/%d/task" % pid):
        os.sched_setaffinity(int(tid), cpus)


def processed(c):
    return int(c.info("stats")["total_commands_processed"])


def one_row(ctx, c, args, client_cpus):
    """Commands a second over the window, the server's and redis-benchmark's
    CPUs used over it."""
    cmd = [redis_tool("redis-benchmark"), "-p", str(ctx.port), "-c", str(CONNS), "-n", "2000000000",
           "-r", str(KEYS), "--threads", "2", "-q"] + args
    bench = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                             preexec_fn=lambda: os.sched_setaffinity(0, client_cpus))
    try:
        time.sleep(WARM_S)
        n0, t0, s0, b0 = processed(c), time.perf_counter(), cpu_seconds(ctx.pid), cpu_seconds(bench.pid)
        time.sleep(WINDOW_S)
        n1, t1, s1, b1 = processed(c), time.perf_counter(), cpu_seconds(ctx.pid), cpu_seconds(bench.pid)
    finally:
        bench.terminate()
        bench.wait(timeout=30)
    dt = t1 - t0
    return (n1 - n0) / dt, (s1 - s0) / dt, (b1 - b0) / dt


@S.run
def run(ctx):
    cpus = sorted(os.sched_getaffinity(0))
    if len(cpus) < 4:
        ctx.log("only %d CPUs: nothing measured" % len(cpus))
        ctx.emit("skipped_cpus", len(cpus), "cpus", "higher", gate=info())
        return
    server_cpus, client_cpus = set(cpus[:2]), set(cpus[2:4])
    pin(ctx.pid, server_cpus)
    ctx.raw["server_cpus"], ctx.raw["client_cpus"] = sorted(server_cpus), sorted(client_cpus)
    c = ctx.client()
    c.flushall()
    rows = {name: {"rate": [], "server": [], "client": []} for name, _ in ROWS}
    for _ in range(ROUNDS):
        for name, args in ROWS:
            rate, srv, cli = one_row(ctx, c, args, client_cpus)
            rows[name]["rate"].append(rate)
            rows[name]["server"].append(srv)
            rows[name]["client"].append(cli)
    for name, _ in ROWS:
        r = rows[name]
        server, client = median(r["server"]), median(r["client"])
        limited = client >= 1.9 and server < 1.9
        ctx.emit("%s_per_s" % name, median(r["rate"]), "req/s", "higher",
                 gate=info() if limited else GATE, samples=r["rate"],
                 note="client-limited: not gated" if limited else None)
        ctx.emit("%s_server_cpus" % name, server, "cpus", "higher", gate=info(), samples=r["server"])
        ctx.emit("%s_client_cpus" % name, client, "cpus", "higher", gate=info(), samples=r["client"])
        ctx.emit("%s_client_limited" % name, int(limited), "flag", "lower", gate=info())


if __name__ == "__main__":
    sys.exit(S.main())
