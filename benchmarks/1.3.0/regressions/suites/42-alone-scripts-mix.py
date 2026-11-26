#!/usr/bin/env python3
"""#880: plain commands beside scripts that run alone. A script whose key has
no hash tag runs alone: no other command runs while it does. Nextcloud's
lock release and Appwrite's abuse limiter are such scripts, sent beside
plain commands. The build before #880 (master 6bcf8afa, Standard_D8s_v5,
interleaved with the build with it) ran Nextcloud's mix with a p99 of 8.45 ms
at 500 connections and 4.38 ms at 200, against 2.53 and 0.96 ms for the same
mix with no script; with #880, 2.45 and 0.90 ms.

Since #1049 a script whose keys have no hash tag runs beside others when
its source names no key but `KEYS[n]` (docs/SCRIPTS.md, "Scripts that keep
to their keys"). Both scripts here are such scripts: their rows now measure
them running beside others, not alone (the values were recorded before);
its gates are all in the worse direction.

Scripts, verbatim:

  cad       Nextcloud's lock release (`lib/private/Lock/MemcacheLockingProvider`
            through `OC\\Memcache\\Redis::compareAndDelete`), sent by its SHA-1:
              if redis.call("get", KEYS[1]) == ARGV[1] then return
              redis.call("del", KEYS[1]) else return 0 end
  appwrite  Appwrite's abuse limiter (utopia-php/abuse, `Adapters/TimeLimit/Redis`),
            sent as `EVAL` with the script text, as the library sends it:
              local c = tonumber(redis.call('GET', KEYS[1]) or '0') or 0
              if c >= tonumber(ARGV[1]) then return c end
              redis.call('INCR', KEYS[1])
              redis.call('EXPIRE', KEYS[1], ARGV[2])
              return c

Workloads (memtier_benchmark, 4 threads, one call at a time per connection,
keys drawn uniformly from 100,000, each connection its own sequence:
`--randomize --distinct-client-seed`; every `__key__` of a command is its
own draw):

  nextcloud           GET 40, TTL 15, INCRBY (`inc:<key>`) 15, EXPIRE 15,
                      EVALSHA cad 12, SETEX 3 (`k:<n>`, value `v`)
  nextcloud_noscript  the same with GET 52 in place of the script: what the
                      plain commands of the mix cost on their own
  cad                 EVALSHA cad only
  appwrite            EVAL appwrite only, on `abuse:k:<n>`, limit 100,000,000
  cad_tag             cad on `{k:<n>}`: a tagged key runs beside others
  appwrite_tag        appwrite on `{abuse:k:<n>}`
  superset            Superset's cache and lock: GET 85 of 10,000 values of
                      50 KB, SETEX 10 (50 KB), SET `lock:<key>` NX EX 3, and
                      its unlock 2 (the same compare-and-delete, EVALSHA)

The 100,000 keys `k:<n>` and `{k:<n>}` hold `v` before every run (written
again before each one), so compare-and-delete finds what it asks for and
deletes it; the share that did is `cad_deleted_pct` (shown). For superset,
`k:1` .. `k:10000` hold 50 KB each instead.

Before anything is measured, each script workload runs under MONITOR (4
connections x 500 calls): every call must name 1 key, of the workload's form,
in the KEYS position, with its ARGV, and the keys must differ from call to
call (at least 90 % distinct). Every run must end with no error reply
(memtier's error lines, `INFO errorstats`); the suite fails otherwise.

Each workload and connection count (200, 500): one warm-up run of
WARMUP_SECONDS (the scripts are then known to be quick), then RUNS runs of
SECONDS; the metric is the median. Rows `<workload>_ops_c<conns>` and
`<workload>_p50_ms_`, `_p99_ms_`, `_p999_ms_c<conns>`; and for nextcloud
`nextcloud_p99_over_noscript_c<conns>`, its p99 over that of
nextcloud_noscript in the same session, the number #880 is about.

CPU: as suites 20, 34 and 36, the server's threads pinned to the first half
of the CPUs this process may use and memtier to the other half, KiviDB with
as many threads as the server has CPUs (`--threads 4` on a Standard_D8s_v5).
Needs 8 vCPUs (`min_cpus=8`) and the `lua,tls` builds (`lua=True`).

Gates (the candidate):
  * nextcloud's p99 at most 2x nextcloud_noscript's in the same session
    (`nextcloud_p99_over_noscript`, a ratio: never scaled).
  * nextcloud's and superset's p99 at most 0.75x Redis's in the same session
    (Redis runs every command on one thread; the build before #880 had a
    nextcloud p99 of 1.4x Redis's at 500 connections and 1.8x at 200), and
    within 50 % (or 1 ms) of recorded (`jitter`: not scaled by host speed).
  * ops/s at least FLOORS times Redis's in the same session, and THROUGHPUT
    against the recorded candidate, for every workload.
  * Other latencies are shown (`jitter(info())`).
Valkey is not one of the suite's engines; the numbers against Valkey 8.1
(`--io-threads 4`) are in the README, "Recorded run, suite 42".

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
from suite import (Suite, Config, THROUGHPUT, bound, info, jitter, rel, vs_redis, median,  # noqa: E402
                   scaled)

KEYS = 100_000
THREADS = 4
SUPERSET_KEYS, SUPERSET_VALUE = 10_000, 51_200
SECONDS = 15
WARMUP_SECONDS = 5
RUNS = 3
CONNS = (200, 500)

CAD = ('if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) '
       'else return 0 end')
APPWRITE = ("local c = tonumber(redis.call('GET', KEYS[1]) or '0') or 0\n"
            "if c >= tonumber(ARGV[1]) then return c end\n"
            "redis.call('INCR', KEYS[1])\n"
            "redis.call('EXPIRE', KEYS[1], ARGV[2])\n"
            "return c")
LIMIT, WINDOW = "100000000", "3600"

# ops/s at least this many times Redis's: recorded (4 workers on a
# Standard_D8s_v5, Redis 8.10.2 from the VM image) nextcloud 2.0x, its no-script
# mix 1.8-1.9x, cad 1.9x, appwrite 1.3-1.4x, the tagged ones 2.4-2.6x,
# superset 1.8-2.0x; the floors are about 70 % of that, rounded down.
FLOORS = {"nextcloud": 1.4, "nextcloud_noscript": 1.4, "cad": 1.4, "appwrite": 1.0,
          "cad_tag": 1.8, "appwrite_tag": 1.8, "superset": 1.2}
# nextcloud's p99: at most this many times Redis's, and this many times the
# same mix's with no script, in the same session.
P99_VS_REDIS, P99_OVER_NOSCRIPT = 0.75, 2.0


def workloads(sha):
    """name -> ([(memtier command, ratio)], {MONITOR check} or None)."""
    app = APPWRITE.replace("\n", " ")
    return {
        "nextcloud": ([("GET __key__", 40), ("TTL __key__", 15), ("INCRBY inc:__key__ 1", 15),
                       ("EXPIRE __key__ 1000", 15), ("EVALSHA %s 1 __key__ v" % sha, 12),
                       ("SETEX __key__ 1000 v", 3)], None),
        "nextcloud_noscript": ([("GET __key__", 52), ("TTL __key__", 15),
                                ("INCRBY inc:__key__ 1", 15), ("EXPIRE __key__ 1000", 15),
                                ("SETEX __key__ 1000 v", 3)], None),
        "cad": ([("EVALSHA %s 1 __key__ v" % sha, 1)],
                {"name": "evalsha", "first": sha, "key": r"^k:\d+$", "argv": ["v"]}),
        "appwrite": ([('EVAL "%s" 1 abuse:__key__ %s %s' % (app, LIMIT, WINDOW), 1)],
                     {"name": "eval", "first": None, "key": r"^abuse:k:\d+$",
                      "argv": [LIMIT, WINDOW]}),
        "cad_tag": ([("EVALSHA %s 1 {__key__} v" % sha, 1)],
                    {"name": "evalsha", "first": sha, "key": r"^\{k:\d+\}$", "argv": ["v"]}),
        "appwrite_tag": ([('EVAL "%s" 1 {abuse:__key__} %s %s' % (app, LIMIT, WINDOW), 1)],
                         {"name": "eval", "first": None, "key": r"^\{abuse:k:\d+\}$",
                          "argv": [LIMIT, WINDOW]}),
        "superset": ([("GET __key__", 85), ("SETEX __key__ 1000 __data__", 10),
                      ("SET lock:__key__ t NX EX 30", 3),
                      ("EVALSHA %s 1 lock:__key__ t" % sha, 2)], None),
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
S = Suite("42-alone-scripts-mix", [Config("default", threads=_THREADS, lua=True)], min_cpus=8)


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


def memtier(memtier_path, port, commands, conns, keys, seconds=None, requests=None,
            threads=THREADS, data_size=None):
    """One run -> {ops, p50, p99, p999} (ms). Raises on any error reply."""
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "out.json")
        length = (["--requests", str(requests)] if requests else ["--test-time", str(seconds)])
        cmd = [memtier_path, "-s", "127.0.0.1", "-p", str(port), "--protocol=redis",
               "-t", str(threads), "-c", str(max(1, conns // threads))] + length + [
               "--key-prefix=k:", "--key-minimum=1", "--key-maximum=%d" % keys,
               "--randomize", "--distinct-client-seed", "--hide-histogram",
               "--print-percentiles=50,99,99.9", "--json-out-file", out]
        if data_size:
            cmd += ["--data-size=%d" % data_size]
        for command, ratio in commands:
            # --command-key-pattern (and the ratio) must follow its --command.
            cmd += ["--command=" + command, "--command-key-pattern=R"]
            if len(commands) > 1:
                cmd += ["--command-ratio=%d" % ratio]
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
    p = total["Percentile Latencies"]
    return {"ops": total["Ops/sec"], "p50": p["p50.00"], "p99": p["p99.00"], "p999": p["p99.90"]}


ARG = re.compile(r'"((?:[^"\\]|\\.)*)"')
MONITOR_LINE = re.compile(r'^[\d.]+ \[\d+ ([^\]]+)\] (.*)$')
CHECK_CONNS, CHECK_CALLS = 4, 500


def unescape(text):
    """One argument as MONITOR quotes it -> the argument."""
    def one(m):
        c = m.group(1)
        if c[0] == "x" and len(c) == 3:
            return chr(int(c[1:], 16))
        return {"n": "\n", "r": "\r", "t": "\t"}.get(c, c)
    return re.sub(r"\\(x[0-9a-fA-F]{2}|.)", one, text)


def check_keys(memtier_path, port, commands, want, keys):
    """CHECK_CONNS x CHECK_CALLS calls under MONITOR -> what was seen. Raises
    unless every script call names one key of the form in KEYS with its ARGV
    (and, by EVAL, the script's text), and the keys differ from call to call."""
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
        memtier(memtier_path, port, commands, CHECK_CONNS, keys, requests=CHECK_CALLS, threads=2)
        time.sleep(0.5)
    finally:
        stop.set()
        reader.join()
        sock.close()
    key_form = re.compile(want["key"])
    calls, bad, named = 0, [], []
    for line in b"".join(chunks).decode("utf-8", "replace").splitlines():
        m = MONITOR_LINE.match(line.strip().lstrip("+"))
        if not m:
            continue
        who, rest = m.groups()
        args = ARG.findall(rest)
        if not args or who == "lua" or args[0].lower() != want["name"]:
            continue
        calls += 1
        ok = (len(args) == 4 + len(want["argv"]) and args[2] == "1"
              and key_form.match(args[3]) and args[4:] == want["argv"]
              and (args[1] == want["first"] if want["first"]
                   else unescape(args[1]).replace("\n", " ") == APPWRITE.replace("\n", " ")))
        if ok:
            named.append(args[3])
        else:
            bad.append(line)
    seen = {"calls": calls, "distinct_keys": len(set(named))}
    # KiviDB's MONITOR feed can drop some lines under load (see suite 20).
    if calls < CHECK_CONNS * CHECK_CALLS // 2:
        raise RuntimeError("MONITOR saw %d %s of %d sent" % (calls, want["name"],
                                                             CHECK_CONNS * CHECK_CALLS))
    if bad:
        raise RuntimeError("MONITOR: %d %s without one key of the form in KEYS and its ARGV, "
                           "e.g. %s" % (len(bad), want["name"], bad[0]))
    if len(set(named)) < 0.9 * len(named):
        raise RuntimeError("MONITOR: %d distinct keys in %d calls: the connections share a key "
                           "sequence" % (len(set(named)), len(named)))
    return seen


def preload(c, keys, value=None):
    """`k:<n>` and `{k:<n>}` = `v` for every n, and nothing else; with
    `value`, `k:<n>` = value only."""
    c.flushall()
    step = 100 if value else 5000
    for start in range(1, keys + 1, step):
        p = c.pipeline(transaction=False)
        for n in range(start, min(start + step, keys + 1)):
            p.set("k:%d" % n, value or "v")
            if not value:
                p.set("{k:%d}" % n, "v")
        p.execute()


def errorstats(c):
    return {k: v for k, v in c.info("errorstats").items()}


def calls(c, name):
    """`INFO commandstats` calls of `name` (0 if none)."""
    row = c.info("commandstats").get("cmdstat_%s" % name)
    return int(row["calls"]) if row else 0


def script_counts(c):
    return {k: int(v) for k, v in c.info("stats").items() if k.startswith("scripts_")}


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
        ctx.raw["lua_parallel_scripts"] = c.config_get("lua-parallel-scripts").get(
            "lua-parallel-scripts")
    sha = c.script_load(CAD)
    loads = workloads(sha)
    ctx.raw["monitor"], ctx.raw["runs"], ctx.raw["script_counts"] = {}, {}, {}
    ctx.raw["cad_deleted"] = {}
    for name, (commands, want) in loads.items():
        big = name == "superset"
        n_keys = scaled(SUPERSET_KEYS, step=1000) if big else keys
        value, size = ("x" * SUPERSET_VALUE, SUPERSET_VALUE) if big else (None, None)
        if want:
            preload(c, keys)
            ctx.raw["monitor"][name] = check_keys(memtier_path, ctx.port, commands, want, keys)
        ctx.raw["runs"][name] = {}
        for conns in CONNS:
            preload(c, n_keys, value)
            memtier(memtier_path, ctx.port, commands, conns, n_keys, seconds=WARMUP_SECONDS,
                    data_size=size)
            runs, before = [], script_counts(c) if ctx.kind == "kividb" else {}
            evalsha0, del0 = calls(c, "evalsha"), calls(c, "del")
            for _ in range(RUNS):
                preload(c, n_keys, value)
                runs.append(memtier(memtier_path, ctx.port, commands, conns, n_keys,
                                    seconds=SECONDS, data_size=size))
            errs = errorstats(c)
            if errs:
                raise RuntimeError("%s at %d connections: error replies %r" % (name, conns, errs))
            if ctx.kind == "kividb":
                after = script_counts(c)
                ctx.raw["script_counts"]["%s_c%d" % (name, conns)] = {
                    k: after[k] - before.get(k, 0) for k in after}
            if name in ("nextcloud", "cad", "cad_tag", "superset"):
                evalshas = calls(c, "evalsha") - evalsha0
                # Redis counts a script's own calls in commandstats too; KiviDB
                # as well (`cmdstat_del` of the script's `del`).
                deleted = calls(c, "del") - del0
                pct = 100.0 * deleted / evalshas if evalshas else 0.0
                ctx.raw["cad_deleted"]["%s_c%d" % (name, conns)] = [deleted, evalshas]
                ctx.emit("%s_cad_deleted_pct_c%d" % (name, conns), pct, "%", "higher", gate=info())
            ctx.raw["runs"][name][conns] = runs
            ops = [r["ops"] for r in runs]
            ctx.emit("%s_ops_c%d" % (name, conns), median(ops), "ops/s", "higher",
                     gate=[THROUGHPUT, vs_redis(round(1 / FLOORS[name], 3))], samples=ops)
            for q in ("p50", "p99", "p999"):
                vals = [r[q] for r in runs]
                gate = jitter(info())
                if name in ("nextcloud", "superset") and q == "p99":
                    gate = jitter([rel(50, abs=1.0), vs_redis(P99_VS_REDIS)])
                ctx.emit("%s_%s_ms_c%d" % (name, q, conns), median(vals), "ms", "lower",
                         gate=gate, samples=vals)
    for conns in CONNS:
        mix = median([r["p99"] for r in ctx.raw["runs"]["nextcloud"][conns]])
        alone = median([r["p99"] for r in ctx.raw["runs"]["nextcloud_noscript"][conns]])
        ctx.emit("nextcloud_p99_over_noscript_c%d" % conns, mix / alone, "x", "lower",
                 gate=bound(max=P99_OVER_NOSCRIPT))


if __name__ == "__main__":
    sys.exit(S.main())
