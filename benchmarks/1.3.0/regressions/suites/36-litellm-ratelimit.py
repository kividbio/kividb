#!/usr/bin/env python3
"""#871: LiteLLM's proxy rate limiter, its five Lua scripts as LiteLLM sends
them, against Redis.

LiteLLM's proxy runs these on every request (BerriAI/litellm,
`litellm/rust/crates/redis-store/src/rate_limits/lua.rs`, main @ 4b12a405,
2026-11-16, byte for byte; their SHA-1s are checked against `SCRIPT LOAD`'s):

  BATCH_RATE_LIMITER              requests a minute: a window key and a counter
                                  per descriptor, reset when the window ends
                                  (builds `<prefix>:requests`/`:tokens` from
                                  the window key at run time, keeping its tag)
  CHECK_AND_INCREMENT_BY_N        tokens a minute, all or nothing across the
                                  descriptors, on the server's `TIME`
  WINDOW_GUARDED_TOKEN_INCREMENT  the reconcile of a token reservation
  PARALLEL_ACQUIRE                the max-parallel-requests gauge (a sorted
                                  set of slots), on the server's `TIME`
  PARALLEL_RELEASE                its release

A descriptor's keys share its hash tag: `{api_key:<n>}:window`,
`{api_key:<n>}:requests`; a team's are `{team:<n>}:...`. One call for a key
and its team carries both descriptors, so two tags: KiviDB runs such a script
beside other work holding both tags' locks (#878); before that it ran alone.

Workloads (memtier_benchmark, `--transaction`: every `__key__` of one
rotation of the commands is the same drawn key, so a call's key and team
are `{api_key:<n>}` and `{team:<n>}` with one `<n>`, as one request has):

  rpm_key            BATCH_RATE_LIMITER, one descriptor (one tag)
  rpm_key_team       BATCH_RATE_LIMITER, key + team (two tags)
  tpm_key_team       CHECK_AND_INCREMENT_BY_N, key + team
  parallel_key_team  PARALLEL_ACQUIRE, key + team
  request_mix        one proxied request, 13 commands a rotation: MGET of
                     three spend counters, the four scripts above for key +
                     team, PARALLEL_RELEASE, an INCRBYFLOAT of the
                     deployment's tokens, and three INCRBYFLOAT + EXPIRE of
                     the spend counters

`<n>` is drawn from 50,000 (`--key-maximum`), uniformly, each connection its
own sequence (`--distinct-client-seed`). The window's `now` (BATCH's ARGV) is
the time the run starts, so the first call of a key opens its window and the
rest count in it.

Before anything is measured: (1) a transcript of 22 calls whose replies are
checked exactly (window reset, all-or-nothing over the limit for key + team,
the reconcile, acquire past the limit and release, TTLs): the suite fails on
any difference; (2) the mix under MONITOR, 4 connections x 300 rotations:
every script call must name its keys in the KEYS positions, with one `<n>`
across its `api_key` and `team` keys, the keys differing from call to call;
(3) for the candidate only, its `INFO stats` script counters over the mix's
measured runs: at least 90 % of the scripts must have run beside others
(`scripts_run_beside_others`), or the suite fails. That is the property #871
asked for, and a regression of it is what this suite is for.

Each workload and connection count (200, 500): one warm-up run of
WARMUP_SECONDS, then RUNS runs of SECONDS; the metric is the median. Rows
`<workload>_ops_c<conns>` (gated) and `<workload>_p99_ms_c<conns>` (shown);
`keys_without_ttl` over every `{api_key:*}` and `{team:*}` key after the
runs, which must be 0 (every script sets the expiry of what it makes).

CPU: as suites 20 and 34, the server's threads pinned to the first half of the
CPUs this process may use and memtier to the other half, KiviDB with as many
threads as the server has CPUs (`--threads 4` on a Standard_D8s_v5). Needs 8
vCPUs (`min_cpus=8`) and the `lua,tls` builds (`lua=True`: `--candidate-full`).

Gates: ops/s at least the floor times Redis's in the same session (see
FLOORS: about 70 % of the ratio measured before the recording, rounded
down; recorded 2.3-2.8x), and THROUGHPUT against the recorded candidate. p99
is shown, not gated (`jitter`). Recorded on a Standard_D8s_v5: see the
README, "Recorded run, suite 36".

Needs memtier_benchmark 2.6.1 or newer (`--transaction`): `$MEMTIER`, else
on PATH.
"""
import hashlib
import json
import math
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

KEYS = 50_000
THREADS = 4
SECONDS = 15
WARMUP_SECONDS = 5
RUNS = 3
CONNS = (200, 500)

# LiteLLM's scripts, byte for byte (each a Rust raw string: it starts and
# ends with a newline).
BATCH_RATE_LIMITER = """
local results = {}
local now = tonumber(ARGV[1])
local window_size = tonumber(ARGV[2])

-- Process each window/counter pair
for i = 1, #KEYS, 2 do
    local window_key = KEYS[i]
    local counter_key = KEYS[i + 1]
    local increment_value = 1

    -- Check if window exists and is valid
    local window_start = redis.call('GET', window_key)
    if not window_start or (now - tonumber(window_start)) >= window_size then
        -- Reset window and counter
        local prefix = string.sub(window_key, 1, -(#':window') - 1)
        redis.call('DEL', prefix .. ':requests', prefix .. ':tokens')
        redis.call('SET', window_key, tostring(now))
        redis.call('SET', counter_key, increment_value)
        redis.call('EXPIRE', window_key, window_size)
        redis.call('EXPIRE', counter_key, window_size)
        table.insert(results, tostring(now)) -- window_start
        table.insert(results, increment_value) -- counter
    else
        local counter = redis.call('INCR', counter_key)
        -- This happens when window_key exists but counter_key doesn't (e.g., tokens key
        -- created after requests key when both share the same window_key)
        local current_ttl = redis.call('TTL', counter_key)
        if current_ttl == -1 then
            redis.call('EXPIRE', counter_key, window_size)
        end
        table.insert(results, window_start) -- window_start
        table.insert(results, counter) -- counter
    end
end

return results
"""

CHECK_AND_INCREMENT_BY_N = """
-- Atomic check-and-increment-by-N across one or more descriptors.
-- All-or-nothing: if any descriptor would exceed its limit, no counter is
-- modified.
--
-- Uses Redis server time (`redis.call('TIME')`) instead of a client-supplied
-- timestamp so that window resets are deterministic across replicas with
-- skewed wall-clocks. This prevents a clock-skew-induced reopening of the
-- TOCTOU window across multi-replica deployments.
--
-- KEYS layout: pairs of (window_key, counter_key), one pair per descriptor.
-- ARGV layout: per-descriptor 4-tuple, starting at ARGV[1]:
--     ARGV[(i-1)*4 + 1] = limit
--     ARGV[(i-1)*4 + 2] = increment
--     ARGV[(i-1)*4 + 3] = ttl_seconds (counter TTL when window resets)
--     ARGV[(i-1)*4 + 4] = window_size_seconds (sliding-window length)
--
-- Return on success:
--     { 0, new_counter_1, window_start_1, new_counter_2, window_start_2, ... }
-- Return on over-limit: { 1, descriptor_index, current_counter, limit }
local time_reply = redis.call('TIME')
local now = tonumber(time_reply[1])
local descriptor_count = #KEYS / 2
local reset_windows = {}

-- Pass 1: read state, validate. Abort without writing if any over limit.
local descriptor_state = {}
for i = 1, descriptor_count do
    local window_key = KEYS[(i - 1) * 2 + 1]
    local counter_key = KEYS[(i - 1) * 2 + 2]
    local arg_base = (i - 1) * 4 + 1
    local limit = tonumber(ARGV[arg_base])
    local increment = tonumber(ARGV[arg_base + 1])
    local window_size = tonumber(ARGV[arg_base + 3])

    local window_start = redis.call('GET', window_key)
    local window_expired = (not window_start) or
        ((now - tonumber(window_start)) >= window_size)

    local current_counter
    if window_expired then
        current_counter = 0
    else
        current_counter = tonumber(redis.call('GET', counter_key) or 0)
    end

    local blocked
    if increment > 0 then
        blocked = current_counter + increment > limit
    else
        blocked = current_counter >= limit
    end
    if blocked then
        return { 1, i, current_counter, limit }
    end

    descriptor_state[i] = { window_expired, current_counter, window_start }
end

-- Pass 2: all checks passed. Apply increments.
local results = { 0 }
for i = 1, descriptor_count do
    local window_key = KEYS[(i - 1) * 2 + 1]
    local counter_key = KEYS[(i - 1) * 2 + 2]
    local arg_base = (i - 1) * 4 + 1
    local increment = tonumber(ARGV[arg_base + 1])
    local ttl = tonumber(ARGV[arg_base + 2])
    local window_size = tonumber(ARGV[arg_base + 3])

    local window_expired = descriptor_state[i][1]
    local active_window_start

    if window_expired then
        active_window_start = now
        if not reset_windows[window_key] then
            local prefix = string.sub(window_key, 1, -(#':window') - 1)
            redis.call('DEL', prefix .. ':requests', prefix .. ':tokens')
            reset_windows[window_key] = true
        end
        redis.call('SET', window_key, tostring(now))
        redis.call('SET', counter_key, increment)
        redis.call('EXPIRE', window_key, window_size)
        if ttl > 0 then
            redis.call('EXPIRE', counter_key, ttl)
        end
        table.insert(results, increment)
    else
        active_window_start = tonumber(descriptor_state[i][3])
        local new_counter = redis.call('INCRBY', counter_key, increment)
        local current_ttl = redis.call('TTL', counter_key)
        if current_ttl == -1 and ttl > 0 then
            redis.call('EXPIRE', counter_key, ttl)
        end
        table.insert(results, new_counter)
    end
    table.insert(results, active_window_start)
end

return results
"""

WINDOW_GUARDED_TOKEN_INCREMENT = """
local results = {}
for i = 1, #KEYS, 2 do
    local window_key = KEYS[i]
    local counter_key = KEYS[i + 1]
    local arg_base = ((i - 1) / 2) * 3 + 1
    local expected_window_start = ARGV[arg_base]
    local increment = tonumber(ARGV[arg_base + 1])
    local ttl = tonumber(ARGV[arg_base + 2])
    local active_window_start = redis.call('GET', window_key)

    if active_window_start and active_window_start == expected_window_start then
        local new_counter = redis.call('INCRBY', counter_key, increment)
        local current_ttl = redis.call('TTL', counter_key)
        if current_ttl == -1 and ttl > 0 then
            redis.call('EXPIRE', counter_key, ttl)
        end
        table.insert(results, 1)
        table.insert(results, new_counter)
    else
        table.insert(results, 0)
        table.insert(results, tonumber(redis.call('GET', counter_key) or 0))
    end
end
return results
"""

PARALLEL_ACQUIRE = """
-- Atomic check-and-acquire for the max_parallel_requests concurrency gauge.
-- Each gauge key is a sorted set of per-request slot ids scored by acquire
-- time (Redis server clock). In-flight requests are counted by ZCARD after
-- pruning slots older than the slot TTL, so unlike the windowed RPM/TPM
-- counters the gauge is never reset while requests are in flight, a
-- rejected request never occupies a slot, and a slot leaked by a crashed
-- worker self-heals after the slot TTL even under continuous traffic.
--
-- KEYS: one gauge zset key per descriptor.
-- ARGV: per-key triples (limit, slot_ttl_seconds, slot_id).
-- Success: { 0, in_flight_1, ... }. Over-limit: { 1, key_index, in_flight, limit }.
local time_reply = redis.call('TIME')
local now = tonumber(time_reply[1])
for i = 1, #KEYS do
    local limit = tonumber(ARGV[(i - 1) * 3 + 1])
    local slot_ttl = tonumber(ARGV[(i - 1) * 3 + 2])
    redis.call('ZREMRANGEBYSCORE', KEYS[i], '-inf', now - slot_ttl)
    local in_flight = redis.call('ZCARD', KEYS[i])
    if in_flight + 1 > limit then
        return { 1, i, in_flight, limit }
    end
end
local results = { 0 }
for i = 1, #KEYS do
    local slot_ttl = tonumber(ARGV[(i - 1) * 3 + 2])
    local slot_id = ARGV[(i - 1) * 3 + 3]
    redis.call('ZADD', KEYS[i], now, slot_id)
    redis.call('EXPIRE', KEYS[i], slot_ttl)
    table.insert(results, redis.call('ZCARD', KEYS[i]))
end
return results
"""

PARALLEL_RELEASE = """
-- Release one slot per gauge key by removing this request's slot id.
-- ZREM of an absent member (or key) is a no-op, so a release without a
-- matching acquire (proxy-side rejection, double-fired callback, slot
-- already expired) can never free a slot owned by another request.
-- KEYS: gauge zset keys. ARGV: per-key slot_id.
-- Returns the remaining in-flight count per key.
local results = {}
for i = 1, #KEYS do
    redis.call('ZREM', KEYS[i], ARGV[i])
    table.insert(results, redis.call('ZCARD', KEYS[i]))
end
return results
"""

SCRIPTS = {
    "brl": BATCH_RATE_LIMITER,
    "ci": CHECK_AND_INCREMENT_BY_N,
    "wg": WINDOW_GUARDED_TOKEN_INCREMENT,
    "pa": PARALLEL_ACQUIRE,
    "pr": PARALLEL_RELEASE,
}
# The number of keys each script is called with for key + team, and with
# one descriptor (BATCH only).
KEY = "{api_key:__key__}"
TEAM = "{team:__key__}"


def workloads(sha, now):
    """name -> [(memtier --command, ratio)]."""
    brl = lambda keys: "EVALSHA %s %d %s %d 60" % (sha["brl"], len(keys), " ".join(keys), now)  # noqa: E731
    brl_key = brl(["%s:window" % KEY, "%s:requests" % KEY])
    brl_kt = brl(["%s:window" % KEY, "%s:requests" % KEY, "%s:window" % TEAM, "%s:requests" % TEAM])
    ci_kt = "EVALSHA %s 4 %s:window %s:tokens %s:window %s:tokens %s" % (
        sha["ci"], KEY, KEY, TEAM, TEAM, " ".join(["1000000000 600 60 60"] * 2))
    wg_kt = "EVALSHA %s 4 %s:window %s:tokens %s:window %s:tokens 0 -50 60 0 -50 60" % (
        sha["wg"], KEY, KEY, TEAM, TEAM)
    pa_kt = "EVALSHA %s 2 %s:max_parallel_requests %s:max_parallel_requests 100000 600 slot 100000 600 slot" % (
        sha["pa"], KEY, TEAM)
    pr_kt = "EVALSHA %s 2 %s:max_parallel_requests %s:max_parallel_requests slot slot" % (
        sha["pr"], KEY, TEAM)
    return {
        "rpm_key": [(brl_key, 1)],
        "rpm_key_team": [(brl_kt, 1)],
        "tpm_key_team": [(ci_kt, 1)],
        "parallel_key_team": [(pa_kt, 1)],
        "request_mix": [
            ("MGET spend:key:__key__ spend:team:__key__ spend:user:__key__", 1),
            (brl_kt, 1), (pa_kt, 1), (ci_kt, 1), (wg_kt, 1), (pr_kt, 1),
            ("INCRBYFLOAT dep1:gpt-4o:tpm:__key__ 550", 1),
            ("INCRBYFLOAT spend:key:__key__ 0.0021", 3),
            ("EXPIRE spend:key:__key__ 86400", 3),
        ],
    }


# Ratio to Redis each row must reach in the same session, per workload: about
# 70 % of the lower of the ratios measured at 200 and 500 connections before
# the recording (pr962-d8s_v5-r0: 2.68, 2.50, 2.56, 2.30, 2.54), rounded down.
FLOORS = {
    "rpm_key": 1.8,
    "rpm_key_team": 1.7,
    "tpm_key_team": 1.7,
    "parallel_key_team": 1.6,
    "request_mix": 1.7,
}


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
S = Suite("36-litellm-ratelimit", [Config("default", threads=_THREADS, lua=True)], min_cpus=8)


def pin_server(pid):
    """Every thread of the server onto SERVER_CPUS."""
    for tid in os.listdir("/proc/%d/task" % pid):
        os.sched_setaffinity(int(tid), SERVER_CPUS)


def memtier_bin():
    for p in (os.environ.get("MEMTIER", ""), shutil.which("memtier_benchmark") or ""):
        if p and os.access(p, os.X_OK):
            return p
    raise RuntimeError("memtier_benchmark not found ($MEMTIER or PATH)")


def memtier(memtier_path, port, commands, conns, keys, seconds=None, requests=None,
            threads=THREADS):
    """One run of `commands` ([(command, ratio)]) -> (ops/s, p99 ms)."""
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "out.json")
        length = (["--requests", str(requests)] if requests else ["--test-time", str(seconds)])
        cmd = [memtier_path, "-s", "127.0.0.1", "-p", str(port), "--protocol=redis",
               "-t", str(threads), "-c", str(max(1, conns // threads))] + length + [
               "--key-prefix=", "--key-minimum=1", "--key-maximum=%d" % keys,
               "--distinct-client-seed", "--transaction", "--hide-histogram",
               "--json-out-file", out]
        for command, ratio in commands:
            # Each --command's options follow it.
            cmd += ["--command=" + command, "--command-ratio=%d" % ratio,
                    "--command-key-pattern=R"]
        pin = (lambda: os.sched_setaffinity(0, CLIENT_CPUS)) if CLIENT_CPUS else None
        res = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
                             timeout=(seconds or 60) + 120, preexec_fn=pin)
        tail = "\n".join(res.stderr.strip().splitlines()[-10:])
        if res.returncode != 0:
            raise RuntimeError("memtier_benchmark exited %d:\n%s" % (res.returncode, tail))
        errors = [line for line in res.stderr.splitlines() if "error response" in line]
        if errors:
            raise RuntimeError("memtier_benchmark got %d error replies, e.g.:\n%s"
                               % (len(errors), "\n".join(errors[:5])))
        with open(out) as f:
            total = json.load(f)["ALL STATS"]["Totals"]
    if not total["Ops/sec"] > 0:
        raise RuntimeError("memtier_benchmark did no work: %r" % total)
    return total["Ops/sec"], total["Percentile Latencies"]["p99.00"]


def check_transcript(c):
    """LiteLLM's semantics, checked exactly on this engine -> the replies."""
    def run(name, keys, *args):
        return c.evalsha(sha_of(SCRIPTS[name]), len(keys), *keys, *args)

    def norm(reply):
        return [x.decode() if isinstance(x, bytes) else x for x in reply]

    K, T = "{api_key:t1}", "{team:t1}"
    seen = {}
    got = [norm(run("brl", [K + ":window", K + ":requests"], 1000, 60)) for _ in range(3)]
    want = [["1000", 1], ["1000", 2], ["1000", 3]]
    seen["rpm_three_calls"] = got
    if got != want:
        raise RuntimeError("BATCH_RATE_LIMITER: %r, not %r" % (got, want))
    got = norm(run("brl", [K + ":window", K + ":requests", T + ":window", T + ":requests"], 1000, 60))
    if got != ["1000", 4, "1000", 1]:
        raise RuntimeError("BATCH_RATE_LIMITER key + team: %r" % got)
    got = norm(run("brl", [K + ":window", K + ":requests"], 1070, 60))
    if got != ["1070", 1]:
        raise RuntimeError("BATCH_RATE_LIMITER after the window: %r" % got)
    # Tokens, key + team, limit 1500, 600 at a time: the third is refused
    # whole, and neither counter moves.
    K2, T2 = "{api_key:t2}", "{team:t2}"
    tk = [K2 + ":window", K2 + ":tokens", T2 + ":window", T2 + ":tokens"]
    args = [1500, 600, 60, 60] * 2
    got = [norm(run("ci", tk, *args)) for _ in range(3)]
    seen["tpm_three_calls"] = got
    if [g[:2] for g in got[:2]] != [[0, 600], [0, 1200]] or got[0][3] != 600 or got[1][3] != 1200 \
            or got[2] != [1, 1, 1200, 1500]:
        raise RuntimeError("CHECK_AND_INCREMENT_BY_N: %r" % got)
    counters = [c.get(K2 + ":tokens"), c.get(T2 + ":tokens")]
    if counters != [b"1200", b"1200"]:
        raise RuntimeError("CHECK_AND_INCREMENT_BY_N over the limit moved a counter: %r" % counters)
    start = c.get(K2 + ":window").decode()
    got = norm(run("wg", tk, start, -100, 60, start, -100, 60))
    if got != [1, 1100, 1, 1100]:
        raise RuntimeError("WINDOW_GUARDED_TOKEN_INCREMENT: %r" % got)
    got = norm(run("wg", tk, "0", -100, 60, "0", -100, 60))
    if got != [0, 1100, 0, 1100]:
        raise RuntimeError("WINDOW_GUARDED_TOKEN_INCREMENT, another window: %r" % got)
    gauges = [K2 + ":max_parallel_requests", T2 + ":max_parallel_requests"]
    got = [norm(run("pa", gauges, 2, 600, "s%d" % i, 2, 600, "s%d" % i)) for i in range(3)]
    if got != [[0, 1, 1], [0, 2, 2], [1, 1, 2, 2]]:
        raise RuntimeError("PARALLEL_ACQUIRE: %r" % got)
    got = norm(run("pr", gauges, "s0", "s0"))
    if got != [1, 1]:
        raise RuntimeError("PARALLEL_RELEASE: %r" % got)
    ttls = {k: c.ttl(k) for k in [K + ":requests", T + ":requests", K2 + ":tokens", gauges[0]]}
    seen["ttls"] = ttls
    for k, t in ttls.items():
        if not 0 < t <= 600:
            raise RuntimeError("TTL of %s is %r" % (k, t))
    c.flushall()
    return seen


def sha_of(script):
    return hashlib.sha1(script.encode()).hexdigest()


ARG = re.compile(r'"((?:[^"\\]|\\.)*)"')
MONITOR_LINE = re.compile(r'^[\d.]+ \[\d+ ([^\]]+)\] (.*)$')
TAGGED = re.compile(r"^\{(api_key|team):(\d+)\}:")
CHECK_CONNS, CHECK_ROTATIONS = 4, 300


def check_keys(memtier_path, port, commands, sha, keys):
    """The mix under MONITOR -> what was seen. Raises unless every script call
    names the keys it should, with one <n> across its api_key and team keys,
    and the keys differ from call to call."""
    names = {v: k for k, v in sha.items()}
    want_keys = {"brl": 4, "ci": 4, "wg": 4, "pa": 2, "pr": 2}
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
        rotation = sum(r for _, r in commands)
        memtier(memtier_path, port, commands, CHECK_CONNS, keys,
                requests=CHECK_ROTATIONS * rotation, threads=2)
        time.sleep(0.5)
    finally:
        stop.set()
        reader.join()
        sock.close()
    calls, bad, ns = {}, [], []
    for line in b"".join(chunks).decode("utf-8", "replace").splitlines():
        m = MONITOR_LINE.match(line.strip().lstrip("+"))
        if not m:
            continue
        who, rest = m.groups()
        args = ARG.findall(rest)
        if len(args) < 3 or who == "lua" or args[0].lower() != "evalsha":
            continue
        name = names.get(args[1])
        if name is None:
            bad.append(line)
            continue
        calls[name] = calls.get(name, 0) + 1
        nkeys = int(args[2])
        found = [TAGGED.match(k) for k in args[3:3 + nkeys]]
        numbers = {f.group(2) for f in found if f}
        if nkeys != want_keys[name] or not all(found) or len(numbers) != 1 \
                or {f.group(1) for f in found} != {"api_key", "team"}:
            bad.append(line)
        else:
            ns.append(numbers.pop())
    seen = {"calls": calls, "distinct_n": len(set(ns))}
    total = sum(calls.values())
    if set(calls) != set(want_keys) or total < CHECK_CONNS * CHECK_ROTATIONS * 5 // 2:
        raise RuntimeError("MONITOR saw %r script calls of %d sent"
                           % (calls, CHECK_CONNS * CHECK_ROTATIONS * 5))
    if bad:
        raise RuntimeError("MONITOR: %d script calls whose keys are not one api_key and its team, "
                           "e.g. %s" % (len(bad), bad[0]))
    # Five scripts a rotation share one <n>; the rotations draw a new one,
    # uniformly from `keys`: at least 80 % of the distinct values that many
    # draws give on average (a shared sequence gives a quarter of them).
    rotations = len(ns) / 5
    expected = keys * (1 - math.exp(-rotations / keys))
    if len(set(ns)) < 0.8 * expected:
        raise RuntimeError("MONITOR: %d distinct keys in %d calls" % (len(set(ns)), len(ns)))
    return seen


def script_counts(c):
    stats = c.info("stats")
    return {k: int(v) for k, v in stats.items() if k.startswith("scripts_run_")}


def keys_without_ttl(c):
    """{api_key:*} and {team:*} keys with no time to live -> (without, seen)."""
    missing, seen = 0, 0
    for match in ("{api_key:*", "{team:*"):
        cursor = 0
        while True:
            cursor, keys = c.scan(cursor, match=match, count=1000)
            if keys:
                p = c.pipeline(transaction=False)
                for k in keys:
                    p.ttl(k)
                ttls = p.execute()
                seen += len(keys)
                missing += sum(1 for t in ttls if t == -1)
            if cursor == 0:
                break
    return missing, seen


@S.run
def run(ctx):
    memtier_path = memtier_bin()  # before anything else
    c = ctx.client()
    keys = scaled(KEYS, step=1000)
    pinned = bool(SERVER_CPUS and ctx.pid)
    if pinned:
        pin_server(ctx.pid)
    ctx.raw["cpus"] = {"pinned": pinned, "server": SERVER_CPUS, "client": CLIENT_CPUS,
                       "os_cpus": os.cpu_count()}
    if ctx.kind == "kividb":
        ctx.raw["lua_parallel_scripts"] = c.config_get("lua-parallel-scripts").get("lua-parallel-scripts")
    c.flushall()
    sha = {}
    for name, script in SCRIPTS.items():
        loaded = c.script_load(script)
        if loaded != sha_of(script):
            raise RuntimeError("SCRIPT LOAD of %s gave %s, not %s" % (name, loaded, sha_of(script)))
        sha[name] = loaded
    ctx.raw["sha1"] = sha
    ctx.raw["transcript"] = check_transcript(c)
    now = int(time.time())
    ctx.raw["now"] = now
    loads = workloads(sha, now)
    ctx.raw["monitor"] = check_keys(memtier_path, ctx.port, loads["request_mix"], sha, keys)
    c.flushall()
    ctx.raw["runs"], ctx.raw["script_counts"] = {}, {}
    for name, commands in loads.items():
        ctx.raw["runs"][name] = {}
        for conns in CONNS:
            memtier(memtier_path, ctx.port, commands, conns, keys, seconds=WARMUP_SECONDS)
            before = script_counts(c) if ctx.kind == "kividb" else {}
            runs = [memtier(memtier_path, ctx.port, commands, conns, keys, seconds=SECONDS)
                    for _ in range(RUNS)]
            if ctx.kind == "kividb":
                after = script_counts(c)
                counts = {k: after[k] - before.get(k, 0) for k in after}
                ctx.raw["script_counts"]["%s_c%d" % (name, conns)] = counts
                ran = sum(counts.values())
                beside = counts.get("scripts_run_beside_others", 0)
                if ctx.engine == "candidate" and ran and beside < 0.9 * ran:
                    raise RuntimeError("%s at %d connections: %d of %d scripts ran beside others: %r"
                                       % (name, conns, beside, ran, counts))
            ctx.raw["runs"][name][conns] = runs
            ops = [r[0] for r in runs]
            ctx.emit("%s_ops_c%d" % (name, conns), median(ops), "ops/s", "higher",
                     gate=[THROUGHPUT, vs_redis(round(1 / FLOORS[name], 3))], samples=ops)
            p99 = [r[1] for r in runs]
            ctx.emit("%s_p99_ms_c%d" % (name, conns), median(p99), "ms", "lower",
                     gate=jitter(info()), samples=p99)
    missing, seen = keys_without_ttl(c)
    ctx.raw["keys_seen"] = seen
    if seen < 0.9 * keys:
        raise RuntimeError("the scan saw %d tagged keys, for %d drawn" % (seen, keys))
    ctx.emit("keys_without_ttl", missing, "keys", "lower", gate=bound(max=0))


if __name__ == "__main__":
    sys.exit(S.main())
