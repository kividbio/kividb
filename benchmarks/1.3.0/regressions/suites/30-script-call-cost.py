#!/usr/bin/env python3
"""#901: what one small script call costs the server, against v1.2.2 and Redis.

The scripts that client libraries run by the thousand are short: a rate
limiter's sorted-set window, a lock's compare-and-delete, a queue's job
move. Each runs a few commands, and what the server spends around them (the
arguments set up, each redis.call, the reply converted back to RESP, the
bookkeeping of the call) was about three times what Redis spends: Tyk's
sliding log, four commands, cost 32.7 microseconds of server CPU a call on
master against 13.8 on Redis 8.10.2 (#901). These rows keep what the #901
work gained.

Workloads, each by redis-benchmark (--threads 2, 32 connections, one
command at a time), EVALSHA of a script loaded first:

  tyk        Tyk 5.16's sliding-log limiter (ZREMRANGEBYSCORE, ZCARD, ZADD,
             EXPIRE on one key, a 3-element reply) over 5,000 keys of 144
             members each
  lock       the compare-and-delete lock release of #880, on 5,000 keys held
             by another owner (GET, compare, return 0)
  return1    `return 1`, no keys
  *_tag      the same with every key under a hash tag, which runs beside
             other scripts (lua-parallel-scripts)

For each, over an 8 s window after 2 s of warm-up, the median of 3 rounds:
  <w>_cpu_us_per_call   the server's CPU (user + system, all its threads)
                        divided by the calls it counted (INFO commandstats)
  <w>_calls_per_s       those calls a second

Centrifuge's history scripts are suite 25's. BullMQ's moveToFinished and
addStandardJob need Node and the bullmq package; they were measured for #901
by scratch harnesses and are in docs/PERFORMANCE.md, not here.

Gates: CPU per call within 10 % of the recorded value, and at most the
given fraction of v1.2.2's measured in the same session; calls a second
10 % (no v1.2.2 gate: with --threads 2 the untagged rows are bound by the
one script that runs at a time, not by its cost alone). Redis is shown.
"""
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (Suite, Config, THROUGHPUT, rel, vs_v1_2_2, median)  # noqa: E402
from nodes import redis_tool  # noqa: E402

S = Suite("30-script-call-cost", [
    Config("scripts", threads=2, lua=True),
])
ROUNDS = 3
WARMUP_S = 2.0
WINDOW_S = 8.0
KEYS = 5000
MEMBERS = 144

TYK = """local key = KEYS[1]

local now_str = ARGV[1]
local window_start_str = ARGV[2]
local max_allowed = tonumber(ARGV[3])
local per_seconds = tonumber(ARGV[4])

redis.call("ZREMRANGEBYSCORE", key, "-inf", window_start_str)

local count = redis.call("ZCARD", key)

redis.call("ZADD", key, now_str, now_str)
redis.call("EXPIRE", key, per_seconds)

local remaining = max_allowed - count - 1
if remaining < 0 then
	remaining = 0
end

local earliest_next = now_str
if count >= max_allowed then
	local oldest = redis.call("ZRANGE", key, -max_allowed, -max_allowed)
	if #oldest > 0 then
		earliest_next = oldest[1]
	end
end

return { count, remaining, earliest_next }
"""

LOCK = 'if redis.call("get",KEYS[1]) == ARGV[1] then return redis.call("del",KEYS[1]) else return 0 end'

RETURN1 = "return 1"

# The window: members at 1..MEMBERS, a window that starts below all of them,
# and the call's own member above them, so every call trims nothing, counts
# MEMBERS (+1 once it has added its own) and adds one that is already there.
NOW, WINDOW_START = "1000000", "0"


def key(name, i, tag):
    return ("%s:{%012d}" if tag else "%s:%012d") % (name, i)


def cpu_ticks(pid):
    with open("/proc/%d/stat" % pid) as f:
        fields = f.read().rsplit(")", 1)[1].split()
    return int(fields[11]) + int(fields[12])


def calls(c):
    return c.info("commandstats").get("cmdstat_evalsha", {}).get("calls", 0)


def measure(ctx, bench_args):
    """(CPU µs per call, calls a second) over the window, while redis-benchmark runs."""
    c = ctx.client()
    cmd = [redis_tool("redis-benchmark"), "-p", str(ctx.port), "-q", "-n", "1000000000",
           "--threads", "2", "-c", "32", "-P", "1"] + bench_args
    rb = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    hz = os.sysconf("SC_CLK_TCK")
    try:
        time.sleep(WARMUP_S)
        a, ta, t0 = calls(c), cpu_ticks(ctx.pid), time.perf_counter()
        time.sleep(WINDOW_S)
        b, tb, t1 = calls(c), cpu_ticks(ctx.pid), time.perf_counter()
    finally:
        rb.terminate()
        try:
            _, err = rb.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            rb.kill()
            _, err = rb.communicate()
    if "ERR" in (err or "") or b <= a:
        raise RuntimeError("redis-benchmark %s: %s calls; %s" % (" ".join(bench_args[:4]), b - a, (err or "")[-300:]))
    return (tb - ta) / hz * 1e6 / (b - a), (b - a) / (t1 - t0)


def load(ctx, tag):
    c = ctx.client()
    c.flushall()
    p = c.pipeline(transaction=False)
    for i in range(KEYS):
        p.zadd(key("rl", i, tag), {str(m): m for m in range(1, MEMBERS + 1)})
        p.set(key("lock", i, tag), "someone-else")
        if i % 500 == 499:
            p.execute()
    p.execute()


# (what is measured, keys, the script, its arguments after the key)
WORKLOADS = [
    ("tyk", "rl", TYK, [NOW, WINDOW_START, "1000", "60"]),
    ("lock", "lock", LOCK, ["mine"]),
    ("return1", None, RETURN1, []),
]

# At most this many times v1.2.2's CPU per call (measured in the same session).
VS_V1_2_2 = {"tyk": 0.85, "lock": 0.85, "return1": 0.85}


@S.run
def run(ctx):
    c = ctx.client()
    shas = {name: c.script_load(src) for name, _, src, _ in WORKLOADS}
    rows = {}
    for tag in (False, True):
        load(ctx, tag)
        for name, kname, _, args in WORKLOADS:
            w = name + ("_tag" if tag else "")
            if kname is None:
                # `return 1` names a key only to have a tag to run under.
                bench = (["-r", str(KEYS), "EVALSHA", shas[name], "1", "{__rand_int__}"] if tag
                         else ["EVALSHA", shas[name], "0"])
            else:
                k = ("%s:{__rand_int__}" if tag else "%s:__rand_int__") % kname
                bench = ["-r", str(KEYS), "EVALSHA", shas[name], "1", k] + args
            cpu, rate = [], []
            for _ in range(ROUNDS):
                us, per_s = measure(ctx, bench)
                cpu.append(us)
                rate.append(per_s)
            rows[w] = {"cpu_us_per_call": cpu, "calls_per_s": rate}
            ctx.emit(w + "_cpu_us_per_call", median(cpu), "us", "lower",
                     gate=[THROUGHPUT, vs_v1_2_2(VS_V1_2_2[name])], samples=cpu)
            ctx.emit(w + "_calls_per_s", median(rate), "calls/s", "higher", gate=[THROUGHPUT], samples=rate)
    ctx.raw["samples"] = rows
    ctx.raw["stats"] = {k: v for k, v in c.info("stats").items() if str(k).startswith("scripts_")}


if __name__ == "__main__":
    sys.exit(S.main())
