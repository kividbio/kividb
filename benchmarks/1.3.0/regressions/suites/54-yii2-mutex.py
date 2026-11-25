#!/usr/bin/env python3
"""#1049: yii2-redis's `Mutex`, a lock taken and given up by the script it
sends with every release. Each cycle is `acquire()` and `release()` of
`yii\\redis\\Mutex` (yii2-redis 2.1.3), on a lock of the connection's own:

  SET <lock> <token> NX PX 30000
  EVAL "if redis.call(\\"GET\\",KEYS[1])==ARGV[1] then return redis.call(\\"DEL\\",KEYS[1]) else return 0 end" 1 <lock> <token>

the release by `EVAL` with the script's text every time, no `EVALSHA`; the
lock's name is the library's (`keyPrefix` and the md5 of the name: 37
characters, no hash tag), the token 20 characters, new for every cycle.

The script names no key but `KEYS[1]`, with commands whose keys are known:
KiviDB runs it beside other commands under its key's lock (#1049), where a
script with a key without a tag otherwise runs alone. 32 connections, one
command at a time each (PHP workers), for SECONDS after a WARMUP: the load is
`lib/mutex_load.c`, one thread a connection, built once into
$REGRESS_CACHE (needs a C compiler). Before anything is measured, 4
connections for a second under MONITOR: every release must be the script
verbatim with one key in the KEYS position, no key may carry a `{`, and the
4 connections must use 4 different locks; where the server shows the
script's own calls, each `GET` and `DEL` is on a lock a release named. A
reply other than `+OK` / `:1` fails the suite.

One server an engine, `--threads 4` (Redis `--io-threads 4`, as the issue
ran it), its threads pinned to the first half of the CPUs this process may
use, the load to the other half (`REGRESS_SERVER_CPUS` /
`REGRESS_CLIENT_CPUS` choose others). ROUNDS rounds, the median: commands a
second (two a cycle), the p50 and p99 of `EVAL` and of `SET`, the server's
CPUs over the window. The candidate must run at least 90 % of the releases
beside others (`INFO stats`, kept in the raw data), or the suite fails.

Gates in the same session: commands a second at least 1.2x Redis's and at
least 1.6x v1.2.2's, and within 10 % of recorded; the `EVAL` p50 at most
0.9x Redis's and its p99 at most 1.1x Redis's. Recorded on a
`Standard_D8s_v5` (`results/pr-1049-54-d8s_v5`): 248,780 commands a second
against Redis's 183,141 (1.36x), an `EVAL` p50/p99 of 130/185 us against
Redis's 172/295; all of the candidate's releases ran beside others. Needs
the `lua,tls` builds (`--candidate-full`); `min_cpus=8`.
"""
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, THROUGHPUT, vs_redis, vs_v1_2_2, info, median  # noqa: E402
from nodes import Nodes  # noqa: E402

S = Suite("54-yii2-mutex", [
    Config("uring4", io="uring", threads=4, self_hosted=True, lua=True),
], min_cpus=8)

LIB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib")
CONNS = 32
WARMUP = 1.0
SECONDS = 10.0
ROUNDS = 3
SCRIPT = ('if redis.call("GET",KEYS[1])==ARGV[1] then return redis.call("DEL",KEYS[1]) '
          'else return 0 end')
LOCK = re.compile(r"^a1b2c[0-9a-f]{32}$")
GATES = {
    "ops_per_s": [vs_redis(1 / 1.2), vs_v1_2_2(1 / 1.6), THROUGHPUT],
    "eval_p50_us": [vs_redis(0.9)],
    "eval_p99_us": [vs_redis(1.1)],
}


def parse_cpus(text):
    out = set()
    for part in text.split(","):
        part = part.strip()
        if "-" in part:
            a, b = part.split("-")
            out.update(range(int(a), int(b) + 1))
        elif part:
            out.add(int(part))
    return out


def cpu_split():
    server, client = os.environ.get("REGRESS_SERVER_CPUS"), os.environ.get("REGRESS_CLIENT_CPUS")
    cpus = sorted(os.sched_getaffinity(0))
    half = len(cpus) // 2
    return (parse_cpus(server) if server else set(cpus[:half]),
            parse_cpus(client) if client else set(cpus[half:]))


def pin(pid, cpus):
    for tid in os.listdir("/proc/%d/task" % pid):
        try:
            os.sched_setaffinity(int(tid), cpus)
        except OSError:
            pass  # a thread that ended meanwhile


def cpu_seconds(pid):
    with open("/proc/%d/stat" % pid) as f:
        fields = f.read().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def load_bin():
    """lib/mutex_load.c, built once into $REGRESS_CACHE."""
    cache = os.environ.get("REGRESS_CACHE", os.path.expanduser("~/.cache/kividb-regress"))
    os.makedirs(cache, exist_ok=True)
    src = os.path.join(LIB, "mutex_load.c")
    out = os.path.join(cache, "mutex_load")
    if not os.path.exists(out) or os.path.getmtime(out) < os.path.getmtime(src):
        subprocess.run([os.environ.get("CC", "cc"), "-O2", "-pthread", "-o", out, src], check=True)
    return out


def run_load(load, port, conns, warmup, seconds, cpus):
    res = subprocess.run([load, str(port), str(conns), str(warmup), str(seconds)], capture_output=True,
                         text=True, timeout=warmup + seconds + 120,
                         preexec_fn=lambda: os.sched_setaffinity(0, cpus))
    if res.returncode != 0:
        raise RuntimeError("mutex_load exited %d: %s" % (res.returncode, res.stderr[-2000:]))
    out = json.loads(res.stdout.strip().splitlines()[-1])
    if out["bad"]:
        raise RuntimeError("%d replies were not +OK / :1" % out["bad"])
    if not out["cycles"]:
        raise RuntimeError("mutex_load did no work")
    return out


ARG = re.compile(r'"((?:[^"\\]|\\.)*)"')
MONITOR_LINE = re.compile(r'^[\d.]+ \[\d+ ([^\]]+)\] (.*)$')


def unescape(text):
    def one(m):
        c = m.group(1)
        if c[0] == "x" and len(c) == 3:
            return chr(int(c[1:], 16))
        return {"n": "\n", "r": "\r", "t": "\t"}.get(c, c)
    return re.sub(r"\\(x[0-9a-fA-F]{2}|.)", one, text)


def check_calls(load, port, cpus):
    """4 connections for a second under MONITOR -> what was seen. Raises unless
    the calls are the cycle as described in the docstring."""
    sock = socket.create_connection(("127.0.0.1", port))
    sock.sendall(b"MONITOR\r\n")
    chunks, stop = [], threading.Event()

    def read():
        sock.settimeout(0.2)
        while not stop.is_set():
            try:
                data = sock.recv(1 << 16)
            except socket.timeout:
                continue
            if not data:
                return
            chunks.append(data)

    t = threading.Thread(target=read)
    t.start()
    time.sleep(0.3)
    run_load(load, port, 4, 0.2, 1.0, cpus)
    time.sleep(0.3)
    stop.set()
    t.join()
    sock.close()
    sets, evals, inner, locks = 0, 0, 0, set()
    inner_keys = []
    for raw in b"".join(chunks).decode("utf-8", "replace").split("\r\n"):
        m = MONITOR_LINE.match(raw.strip().lstrip("+"))
        if not m:
            continue
        args = [unescape(a) for a in ARG.findall(m.group(2))]
        if not args:
            continue
        name = args[0].upper()
        if name == "SET":
            if len(args) != 6 or [a.upper() for a in args[3:5]] != ["NX", "PX"] or args[5] != "30000" \
                    or not LOCK.match(args[1]) or len(args[2]) != 20:
                raise RuntimeError("an acquire not as yii2 sends it: %r" % args)
            sets += 1
            locks.add(args[1])
        elif name == "EVAL":
            if len(args) != 5 or args[1] != SCRIPT or args[2] != "1" or not LOCK.match(args[3]):
                raise RuntimeError("a release not as yii2 sends it: %r" % args[:4])
            evals += 1
            locks.add(args[3])
        elif name in ("GET", "DEL"):
            inner += 1
            inner_keys.append(args[1])
    if sets < 100 or evals < 100:
        raise RuntimeError("MONITOR saw %d acquires and %d releases" % (sets, evals))
    if len(locks) != 4:
        raise RuntimeError("4 connections used %d locks" % len(locks))
    if any("{" in k for k in locks):
        raise RuntimeError("a lock carries a hash tag")
    stray = [k for k in inner_keys if k not in locks]
    if stray:
        raise RuntimeError("a script call on a key no release named: %r" % stray[:3])
    return {"acquires": sets, "releases": evals, "script_calls": inner, "locks": len(locks)}


def script_counts(c):
    stats = c.info("stats")
    return {k: v for k, v in stats.items() if k.startswith("scripts_")}


@S.run
def run(ctx):
    if not hasattr(os, "sched_getaffinity"):
        raise RuntimeError("Linux only: the suite pins CPUs")
    server_cpus, client_cpus = cpu_split()
    ctx.raw["server_cpus"], ctx.raw["client_cpus"] = sorted(server_cpus), sorted(client_cpus)
    load = load_bin()
    args = ["--io-threads", "4"] if ctx.kind == "redis" else []
    with Nodes(ctx) as ns:
        node = ns.start(args=args)
        pin(node.pid, server_cpus)
        c = node.client()
        ctx.raw["monitor"] = check_calls(load, node.port, client_cpus)
        rows = []
        for _ in range(ROUNDS):
            c.flushall()
            pin(node.pid, server_cpus)  # threads started since
            before = script_counts(c) if ctx.kind == "kividb" else {}
            s0, t0 = cpu_seconds(node.pid), time.perf_counter()
            r = run_load(load, node.port, CONNS, WARMUP, SECONDS, client_cpus)
            r["server_cpus"] = (cpu_seconds(node.pid) - s0) / (time.perf_counter() - t0)
            if ctx.kind == "kividb":
                after = script_counts(c)
                moved = {k: after.get(k, 0) - before.get(k, 0) for k in after}
                ran = sum(v for k, v in moved.items() if k.startswith("scripts_run_"))
                r["beside_pct"] = 100.0 * moved.get("scripts_run_beside_others", 0) / max(1, ran)
                r["script_counts"] = moved
            rows.append(r)
        ctx.raw["rows"] = rows
    for metric, key, unit, better in (("ops_per_s", "ops", "ops/s", "higher"),
                                      ("eval_p50_us", "eval_p50", "us", "lower"),
                                      ("eval_p99_us", "eval_p99", "us", "lower"),
                                      ("set_p50_us", "set_p50", "us", "lower"),
                                      ("set_p99_us", "set_p99", "us", "lower"),
                                      ("server_cpus", "server_cpus", "cpus", "lower")):
        values = [r[key] for r in rows]
        ctx.emit(metric, median(values), unit, better, gate=GATES.get(metric, info()), samples=values)
    if ctx.engine == "candidate":
        beside = median([r["beside_pct"] for r in rows])
        ctx.raw["releases_beside_others_pct"] = beside
        if beside < 90:
            raise RuntimeError("only %.1f %% of the releases ran beside others" % beside)


if __name__ == "__main__":
    sys.exit(S.main())
