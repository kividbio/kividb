#!/usr/bin/env python3
"""#849: Kong's rate-limiting plugin with `policy: redis`, the per-request
script published in Kong/kong discussion #15068: 1.4x Redis at 200
connections, 1.5x at 500 (KiviDB 1.2.2, 8 vCPUs).

With the plugin's default `sync_rate` (-1, every request counted in Redis),
each proxied request runs one script, `usage()` in
`kong/plugins/rate-limiting/policies/init.lua` (Kong 3.9.4 and master),
verbatim:

  local cache_key, expiration = KEYS[1], ARGV[1]
  local result_incr = redis.call("incr", cache_key)
  if result_incr == 1 then
    redis.call("expire", cache_key, expiration)
  end

  return result_incr - 1

on one key, `ratelimit:<route>:<service>:<identifier>:<period_date>:<period>`
(`get_local_key`), with the period's expiry (60 for `minute`). Kong sends it as `EVAL <script> 1
<key> 60` (lua-resty-redis `red:eval`, every request carries the script text);
the suite sends it both ways, as Kong does (`eval`) and by its SHA-1 after one
`SCRIPT LOAD` (`evalsha`, what a client that caches the script would send).

Load: memtier_benchmark, 4 threads, 200 and 500 connections, one call at a
time per connection, the identifier drawn uniformly from 100,000
(`c1`..`c100000`), each connection its own sequence (`--distinct-client-seed`).

Configurations, each with the key as Kong builds it (`plain`) and with the
identifier under a hash tag (`tag`, `ratelimit:<route>:<service>:{c<n>}:...`):
they take different paths in KiviDB. Its scripts with a tagged key run
beside other work under the tag's lock; with no tag, one at a time, handed
to the script-runner (#880).

  default   the server as it starts (`lua-parallel-scripts yes`, the default
            in this build and in v1.2.2)
  serial    KiviDB with `lua-parallel-scripts no`: every script one at a
            time, tagged or not. Redis has no such setting, and runs as in
            `default`.

Before anything is measured, for each way of calling and key form, a run of 4
connections x 500 calls under MONITOR: every call must name 1 key in the KEYS
position, of the form asked for, with `60` as its only ARGV (and, for `eval`,
Kong's script text as its script); the keys must differ from call to call (at
least 90 % distinct: memtier without `--distinct-client-seed` gives every
connection the same sequence, and four connections then send each key four
times); and where the server shows the script's own calls, each `incr` is on a
key a call named. The suite fails otherwise.

Each connection count: one warm-up run, then RUNS runs of SECONDS; the metric
is the median. Rows per configuration, way of calling (`eval`, `evalsha`) and
key form, named `<mode>_<form>_...`: ops/s and p99 at each
connection count, and `keys_without_ttl` (must be 0: the script sets the
expiry when it makes the key). The suite fails if the scan sees under 90 % of
the keys, or a run does no work.

CPU: as suite 20, the server's threads pinned to the first half of the CPUs
this process may use and memtier to the other half, KiviDB with as many
threads as the server has CPUs (`REGRESS_SERVER_CPUS` / `REGRESS_CLIENT_CPUS`
choose others). Needs 8 vCPUs (`min_cpus=8`) and the `lua,tls` builds
(`lua=True`: `--candidate-full`).

Gates: ops/s at least 1.2x Redis at 200 connections and 1.3x at 500, in the
same session (~15 % under what was published), in every configuration and key
form, and THROUGHPUT against the recorded candidate. A row under its floor
stays a failing row: the floor is not lowered to fit (#849). p99 is shown,
not gated (`jitter`).

Needs memtier_benchmark 2.6.1 or newer: `$MEMTIER`, else on PATH.
"""
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (Suite, Config, THROUGHPUT, bound, info, jitter, vs_redis, median, scaled)  # noqa: E402

KEYS = 100_000
EXPIRATION = 60
THREADS = 4
SECONDS = 20
WARMUP_SECONDS = 10
RUNS = 3
# Connections -> the floor over Redis for ops/s (published 1.4x / 1.5x).
CONNS = {200: 1.2, 500: 1.3}

# Kong's `usage()` script, as it is (Kong 3.9.4, policies/init.lua).
SCRIPT = """local cache_key, expiration = KEYS[1], ARGV[1]
local result_incr = redis.call("incr", cache_key)
if result_incr == 1 then
  redis.call("expire", cache_key, expiration)
end

return result_incr - 1"""

# `get_local_key`: route and service ids (a route on a service), the
# identifier (a consumer), the period's start in ms, the period.
ROUTE = "6f9c5f5c-3b4e-4b8e-9d55-2a1c8f0e4b21"
SERVICE = "0d3c6e1a-7b2f-4c6d-8e9a-5f4b3a2c1d0e"
PERIOD_DATE = "1763571600000"
FORMS = {
    "plain": "ratelimit:%s:%s:__key__:%s:minute" % (ROUTE, SERVICE, PERIOD_DATE),
    "tag": "ratelimit:%s:%s:{__key__}:%s:minute" % (ROUTE, SERVICE, PERIOD_DATE),
}
# What memtier makes of each: the identifier `c<n>`.
FORM_KEY = {
    "plain": re.compile(r"^ratelimit:%s:%s:c\d+:%s:minute$" % (ROUTE, SERVICE, PERIOD_DATE)),
    "tag": re.compile(r"^ratelimit:%s:%s:\{c\d+\}:%s:minute$" % (ROUTE, SERVICE, PERIOD_DATE)),
}
# How the script is sent: by its SHA-1 (`EVALSHA`), and as Kong sends it
# (`EVAL` with the script text).
MODES = ("evalsha", "eval")
# KiviDB's setting, per configuration.
PARALLEL = {"default": None, "serial": "no"}


def parse_cpus(text):
    """`0-3,6` -> [0, 1, 2, 3, 6]."""
    cpus = []
    for part in text.split(","):
        if "-" in part:
            lo, hi = part.split("-")
            cpus.extend(range(int(lo), int(hi) + 1))
        elif part.strip():
            cpus.append(int(part))
    return cpus


def cpu_split():
    """(server CPUs, client CPUs), or (None, None) where nothing is pinned."""
    if not hasattr(os, "sched_getaffinity"):
        return None, None
    server, client = os.environ.get("REGRESS_SERVER_CPUS"), os.environ.get("REGRESS_CLIENT_CPUS")
    if server and client:
        return parse_cpus(server), parse_cpus(client)
    cpus = sorted(os.sched_getaffinity(0))
    if len(cpus) < 2:
        return None, None
    half = len(cpus) // 2
    return cpus[:half], cpus[half:]


SERVER_CPUS, CLIENT_CPUS = cpu_split()
_THREADS = len(SERVER_CPUS) if SERVER_CPUS else 0
S = Suite("34-kong-ratelimit", [Config(name, threads=_THREADS, lua=True) for name in PARALLEL],
          min_cpus=8)


def pin_server(pid):
    """Every thread of the server onto SERVER_CPUS; threads it starts later
    inherit it from the one that starts them."""
    for tid in os.listdir("/proc/%d/task" % pid):
        os.sched_setaffinity(int(tid), SERVER_CPUS)


def memtier_bin():
    for p in (os.environ.get("MEMTIER", ""), shutil.which("memtier_benchmark") or ""):
        if p and os.access(p, os.X_OK):
            return p
    raise RuntimeError("memtier_benchmark not found ($MEMTIER or PATH)")


def call_command(mode, sha, form):
    """memtier's --command for one call. Kong sends the script itself (`EVAL`,
    lua-resty-redis `red:eval`), not its SHA-1; memtier takes a quoted argument
    (single quotes: the script has none) as one."""
    if mode == "eval":
        return "EVAL '%s' 1 %s %d" % (SCRIPT, FORMS[form], EXPIRATION)
    return "EVALSHA %s 1 %s %d" % (sha, FORMS[form], EXPIRATION)


def memtier(memtier_path, port, sha, form, conns, keys, seconds=None, requests=None,
            threads=THREADS, mode="evalsha"):
    """One run, for `seconds` or `requests` calls a connection -> (ops/s, p99 ms)."""
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "out.json")
        length = (["--requests", str(requests)] if requests else ["--test-time", str(seconds)])
        cmd = [memtier_path, "-s", "127.0.0.1", "-p", str(port), "--protocol=redis",
               "-t", str(threads), "-c", str(max(1, conns // threads))] + length + [
               "--key-prefix=c", "--key-minimum=1", "--key-maximum=%d" % keys,
               "--distinct-client-seed", "--hide-histogram", "--json-out-file", out,
               # --command-key-pattern must follow its --command.
               "--command=" + call_command(mode, sha, form),
               "--command-key-pattern=R"]
        pin = (lambda: os.sched_setaffinity(0, CLIENT_CPUS)) if CLIENT_CPUS else None
        res = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
                             timeout=(seconds or 60) + 120, preexec_fn=pin)
        tail = "\n".join(res.stderr.strip().splitlines()[-10:])
        if res.returncode != 0:
            raise RuntimeError("memtier_benchmark exited %d:\n%s" % (res.returncode, tail))
        # memtier answers error replies (a NOSCRIPT among them) with a line on
        # stderr, and exits 0.
        errors = [line for line in res.stderr.splitlines() if "error response" in line]
        if errors:
            raise RuntimeError("memtier_benchmark got %d error replies, e.g.:\n%s"
                               % (len(errors), "\n".join(errors[:5])))
        with open(out) as f:
            total = json.load(f)["ALL STATS"]["Totals"]
    if not total["Ops/sec"] > 0:
        raise RuntimeError("memtier_benchmark did no work: %r" % total)
    return total["Ops/sec"], total["Percentile Latencies"]["p99.00"]


ARG = re.compile(r'"((?:[^"\\]|\\.)*)"')
MONITOR_LINE = re.compile(r'^[\d.]+ \[\d+ ([^\]]+)\] (.*)$')
CHECK_CONNS, CHECK_CALLS = 4, 500


def unescape(text):
    """One argument as MONITOR quotes it (backslash n, backslash quote, two backslashes)."""
    return re.sub(r"\\(.)", lambda m: {"n": "\n", "r": "\r", "t": "\t"}.get(m.group(1), m.group(1)),
                  text)


def check_keys(memtier_path, port, sha, form, keys, mode="evalsha"):
    """4 connections x 500 calls under MONITOR -> what was seen. Raises unless
    every EVALSHA (EVAL, by `mode`, with Kong's script text) names one key of
    the form in the KEYS position with `60` its only ARGV, the keys differ from
    call to call, and each `incr` the server shows the script making is on a
    key a call named."""
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

    reader = threading.Thread(target=read)
    reader.start()
    try:
        time.sleep(0.2)
        memtier(memtier_path, port, sha, form, CHECK_CONNS, keys, requests=CHECK_CALLS, threads=2,
               mode=mode)
        time.sleep(0.5)
    finally:
        stop.set()
        reader.join()
        sock.close()
    calls, bad, named, incrs, stray = 0, [], [], 0, []
    for line in b"".join(chunks).decode("utf-8", "replace").splitlines():
        # Each line is a RESP simple string: `+<time> [<db> <client>] <args>`.
        m = MONITOR_LINE.match(line.strip().lstrip("+"))
        if not m:
            continue
        who, rest = m.groups()
        args = ARG.findall(rest)
        if not args:
            continue
        name = args[0].lower()
        if name == mode and who != "lua":
            calls += 1
            first = sha if mode == "evalsha" else None
            if (len(args) != 5 or args[2] != "1"
                    or (first is not None and args[1] != first)
                    or (first is None and unescape(args[1]) != SCRIPT)
                    or not FORM_KEY[form].match(args[3]) or args[4] != str(EXPIRATION)):
                bad.append(line)
            else:
                named.append(args[3])
        elif name == "incr" and who == "lua":
            incrs += 1
            if len(args) != 2 or not FORM_KEY[form].match(args[1]):
                stray.append(line)
    seen = {"calls": calls, "distinct_keys": len(set(named)), "script_incrs": incrs}
    # KiviDB's MONITOR feed can drop some lines under load (see suite 20):
    # half of what was sent is plenty to judge by.
    if calls < CHECK_CONNS * CHECK_CALLS // 2:
        raise RuntimeError("MONITOR saw %d %s of %d sent" % (calls, mode, CHECK_CONNS * CHECK_CALLS))
    if bad:
        raise RuntimeError("MONITOR: %d %s without one %s key in KEYS and ARGV 60, e.g. %s"
                           % (len(bad), mode, form, bad[0]))
    if len(set(named)) < 0.9 * len(named):
        raise RuntimeError("MONITOR: %d distinct keys in %d calls: the connections share a key "
                           "sequence" % (len(set(named)), len(named)))
    if stray or (incrs and incrs < calls // 2):
        raise RuntimeError("MONITOR: the script's incr is not on the key in KEYS (%d of %d), e.g. %s"
                           % (len(stray), incrs, stray[:1]))
    return seen


def keys_without_ttl(c):
    """Keys with no time to live -> (keys without one, keys seen)."""
    missing, seen = 0, 0
    cursor = 0
    while True:
        cursor, keys = c.scan(cursor, match="ratelimit:*", count=1000)
        if keys:
            p = c.pipeline(transaction=False)
            for k in keys:
                p.ttl(k)
            ttls = p.execute()
            seen += len(keys)
            missing += sum(1 for t in ttls if t == -1)
        if cursor == 0:
            return missing, seen


@S.run
def run(ctx):
    memtier_path = memtier_bin()  # before anything else
    c = ctx.client()
    keys = scaled(KEYS, step=1000)
    setting = PARALLEL[ctx.config.name]
    if setting is not None and ctx.kind == "kividb":
        c.config_set("lua-parallel-scripts", setting)
    ctx.raw["lua_parallel_scripts"] = (
        c.config_get("lua-parallel-scripts").get("lua-parallel-scripts")
        if ctx.kind == "kividb" else "n/a (Redis)")
    if setting is not None and ctx.kind == "kividb" and ctx.raw["lua_parallel_scripts"] != setting:
        raise RuntimeError("lua-parallel-scripts is %r, not %r" % (ctx.raw["lua_parallel_scripts"], setting))
    pinned = bool(SERVER_CPUS and ctx.pid)
    if pinned:
        pin_server(ctx.pid)
    ctx.raw["cpus"] = {"pinned": pinned, "server": SERVER_CPUS, "client": CLIENT_CPUS,
                       "os_cpus": os.cpu_count()}
    ctx.raw["monitor"], ctx.raw["runs"], ctx.raw["keys_seen"] = {}, {}, {}
    for form in FORMS:
        for mode in MODES:
            c.flushall()
            sha = c.script_load(SCRIPT)
            tag = "%s_%s" % (mode, form)
            ctx.raw["monitor"][tag] = check_keys(memtier_path, ctx.port, sha, form, keys, mode)
            ctx.raw["runs"][tag] = {}
            for conns, floor in CONNS.items():
                memtier(memtier_path, ctx.port, sha, form, conns, keys, seconds=WARMUP_SECONDS,
                        mode=mode)
                runs = [memtier(memtier_path, ctx.port, sha, form, conns, keys, seconds=SECONDS,
                                mode=mode) for _ in range(RUNS)]
                ctx.raw["runs"][tag][conns] = runs
                ops = [r[0] for r in runs]
                ctx.emit("%s_ops_c%d" % (tag, conns), median(ops), "ops/s", "higher",
                         gate=[THROUGHPUT, vs_redis(round(1 / floor, 3))], samples=ops)
                p99 = [r[1] for r in runs]
                ctx.emit("%s_p99_ms_c%d" % (tag, conns), median(p99), "ms", "lower",
                         gate=jitter(info()), samples=p99)
            missing, seen = keys_without_ttl(c)
            ctx.raw["keys_seen"][tag] = seen
            # Counted over the keys the runs made, or it says nothing.
            if seen < 0.9 * keys:
                raise RuntimeError("the scan saw %d ratelimit:* keys of %d" % (seen, keys))
            ctx.emit("%s_keys_without_ttl" % tag, missing, "keys", "lower", gate=bound(max=0))

if __name__ == "__main__":
    sys.exit(S.main())
