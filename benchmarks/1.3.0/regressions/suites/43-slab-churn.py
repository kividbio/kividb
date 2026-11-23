#!/usr/bin/env python3
"""#240: the memory string values hold after light delete/refill churn, after a
snapshot and an AOF rewrite under writes, and GET/SET latency while active
defrag runs a pass.

Active defrag empties the sparsest slabs: since #240 also the full ones with an
eighth of their blocks free (`active-defrag-full-slab-free-percent`, 12), from
a list of full slabs kept by each size class, and it starts on the free room
inside the slabs as well (`active-defrag-slab-waste-percent` 10 and
`active-defrag-slab-waste-bytes` 16 MiB), not only on the ratio trigger
(`active-defrag-threshold-lower` 20 % and `active-defrag-ignore-bytes`
64 MiB). A slab is not used for blocks again until a quarter of it is free, so
after a light churn most of the free room is in full slabs, which only active
defrag gets back.

Churn configurations (the issue's shapes), each a fresh server:

  b100-c10, b100-c20     1,000,000 keys of 100-byte values
  kb1-c10, kb1-c20       1,000,000 keys of 1,024-byte values
  mixed-c10, mixed-c20   120,000 keys of 100 to 16,384 bytes (uniform, seed 240)

Load the keys (16-byte names), then three cycles, each deleting a random 10 %
(`-c10`) or 20 % (`-c20`) of the live keys and writing as many new ones (the
same size distribution), 8 processes sending pipelined RESP (as the issue's
driver did). VmRSS is sampled every 50 ms throughout.

  load_rss_mib      VmRSS 2 s after the load
  load_used_mib     INFO used_memory then
  peak_rss_mib      the highest VmRSS from the first delete to the end
  settled_rss_mib   VmRSS 60 s after the last cycle (nothing sent meanwhile but
                    the INFO reads at the end)
  settled_used_mib  INFO used_memory then
  settled_x_load    settled_rss_mib / load_rss_mib
  peak_x_load       peak_rss_mib / load_rss_mib

`snapshot`: 2,000,000 keys of 1,024 bytes, two writer processes overwriting
random keys (pipelines of 20, paced to about 40,000 SETs a second between them)
for the whole run: `BGSAVE`, 20 s of writes after it ends, then the AOF turned
on and `BGREWRITEAOF`, 20 s of writes after the rewrite ends, then the writers
stop and 30 s pass. The comment on #240 that made this a case: 99.7 % of the
free room a `BGSAVE` left was in full slabs.

  bgsave_rss_mib    VmRSS 20 s after BGSAVE ended (writers still writing)
  rewrite_rss_mib   VmRSS 20 s after the rewrite ended
  final_rss_mib     VmRSS 30 s after the writers stopped
  final_used_mib    INFO used_memory then
  final_x_used      final_rss_mib / final_used_mib

`pass-latency`: 1,000,000 keys of 1,000 bytes with a random 30 % deleted while
active defrag is off, then memtier_benchmark (4 threads x 25 connections,
GET:SET 1:1 over the keys that were loaded, 1,000-byte values, `--randomize
--distinct-client-seed`, 40 s) with active defrag turned on 5 s in: the pass
runs while the clients read and overwrite. Three trials on the same server,
each from an empty one; every metric is the median of the three (kept as its
samples). From memtier's per-second percentiles:

  get_p99_ms / set_p99_ms, get_p999_ms / set_p999_ms
                    the median over the seconds the pass ran of each second's
                    p99 and p99.9 (KiviDB only: Redis runs no pass)
  get_p99_idle_ms / set_p99_idle_ms
                    the same over the seconds before it started (info)
  pass_s            how long the pass ran (KiviDB)

On the 100-byte and 1 KB shapes the same churn then runs again on the same
server, emptied (`FLUSHALL`), with active defrag off (Redis: off both times):

  peak_x_load_off   that run's peak over its load: the 99th percentile of its
                    50 ms samples, not their highest, which moved by 5 % from
                    run to run of one build
  peak_vs_off       peak_x_load / peak_x_load_off: what active defrag adds to
                    the peak

Gates (the candidate): `settled_x_load` at most 1.07 (1 KB) and 1.08 (100 B)
(the issue asked for "within about 5 % of the post-load RSS"; 1.03 and 1.05
measured, against 1.10 to 1.16 before #240) and the recorded value + 5 % on
all; `peak_vs_off` at most 1.07 (the issue: a peak within 2 % of defrag off
counts as equal; three runs each of b100-c10 and b100-c20 of one build on
one D8s_v5 gave 1.012 to 1.058 by the highest samples); `settled_rss_mib` the
recorded value + 5 %; `peak_x_load` +5 % on the 100-byte and 1 KB shapes
(b100-c20 is 1.17 or 1.21 from run to run of one build, the rest within
1 %), +10 % on the mixed one; `final_rss_mib` +5 % of recorded; the pass's
p99 and p99.9 within 25 % (and 0.2 ms) of recorded, and the pass's GET and SET
p99 at most 1.5x the same run's idle seconds plus 0.2 ms (the pass must not
cost latency). v1.2.2 and Redis are shown, not gated.

CPU: as suite 39, the server is pinned to the first half of the CPUs
(`--threads` as many) and the clients to the other half. Needs 8 vCPUs
(`min_cpus=8`): record and run it with `run-all.sh --min-cpus-only`.
"""
import json
import multiprocessing
import os
import random
import socket
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, rel, bound, info, jitter, scaled, median  # noqa: E402
from rss import vmrss_mib  # noqa: E402

MIB = 1048576.0
PROCS = 8
PIPE = 500
SAMPLE_S = 0.05
SETTLE_S = 60
SHAPES = {
    "b100": (1_000_000, 100, 100),
    "kb1": (1_000_000, 1024, 1024),
    "mixed": (120_000, 100, 16384),
}
SNAP_KEYS, SNAP_SIZE = 2_000_000, 1024
LAT_KEYS, LAT_SIZE, LAT_DELETE = 1_000_000, 1000, 0.30
LAT_TIME, LAT_ON_AT = 40, 5


def parse_cpus(text):
    cpus = []
    for part in text.split(","):
        if "-" in part:
            lo, hi = part.split("-")
            cpus.extend(range(int(lo), int(hi) + 1))
        elif part.strip():
            cpus.append(int(part))
    return cpus


def cpu_split():
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
THREADS = len(SERVER_CPUS) if SERVER_CPUS else 0
CONFIGS = [Config("%s-c%d" % (shape, pct), threads=THREADS) for shape in SHAPES for pct in (10, 20)]
CONFIGS += [Config("snapshot", threads=THREADS),
            # Redis runs no pass here (activedefrag is off by default, and it has no slabs to empty).
            Config("pass-latency", threads=THREADS, engines=("v1_2_2", "candidate"))]
S = Suite("43-slab-churn", CONFIGS, min_cpus=8)


def pin_server(pid):
    for tid in os.listdir("/proc/%d/task" % pid):
        try:
            os.sched_setaffinity(int(tid), SERVER_CPUS)
        except OSError:
            pass


def blob():
    return random.Random(2400).randbytes(1 << 15)


DATA = blob()


def key(i):
    return b"k:%014d" % i  # 16 bytes


def value(i, size):
    o = (i * 7919) % (len(DATA) - size)
    return DATA[o:o + size]


def resp(*args):
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)


def read_replies(sock, n, buf=b""):
    """Read n simple replies (+OK, :1, ...); return what is left over."""
    lines = buf.count(b"\r\n")
    while lines < n:
        chunk = sock.recv(1 << 20)
        if not chunk:
            raise RuntimeError("connection closed")
        buf += chunk
        lines += chunk.count(b"\r\n")
    # every reply here is one line; keep what follows the n-th
    at = 0
    for _ in range(n):
        at = buf.index(b"\r\n", at) + 2
    if buf[:1] == b"-":
        raise RuntimeError(buf[:200])
    return buf[at:]


def send_cmds(port, cmds):
    """Pipelined: send cmds (bytes each) in chunks of PIPE, read the replies."""
    s = socket.create_connection(("127.0.0.1", port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    left = b""
    for i in range(0, len(cmds), PIPE):
        chunk = cmds[i:i + PIPE]
        s.sendall(b"".join(chunk))
        left = read_replies(s, len(chunk), left)
    s.close()


def _worker(args):
    port, kind, items = args
    if kind == "set":
        cmds = [resp(b"SET", key(i), value(i, n)) for i, n in items]
    else:
        cmds = [resp(b"DEL", key(i)) for i in items]
    send_cmds(port, cmds)
    return len(cmds)


def parallel(pool, port, kind, items):
    parts = [items[p::PROCS] for p in range(PROCS)]
    return sum(pool.map(_worker, [(port, kind, part) for part in parts]))


class Sampler(threading.Thread):
    def __init__(self, pid):
        super().__init__(daemon=True)
        self.pid, self.stop_ev = pid, threading.Event()
        self.peak, self.timeline, self.t0 = 0.0, [], time.perf_counter()

    def run(self):
        while not self.stop_ev.is_set():
            try:
                r = vmrss_mib(self.pid)
            except Exception:
                break
            self.peak = max(self.peak, r)
            self.timeline.append((round(time.perf_counter() - self.t0, 2), round(r, 1)))
            self.stop_ev.wait(SAMPLE_S)

    def finish(self):
        self.stop_ev.set()
        self.join()

    def p99(self):
        """The 99th percentile of the samples (the highest of 100 and fewer)."""
        rss = sorted(r for _, r in self.timeline)
        return rss[min(len(rss) - 1, len(rss) * 99 // 100)] if rss else 0.0


def memory(c, pid):
    m = c.info("memory")
    out = {"rss_mib": vmrss_mib(pid), "used_mib": m.get("used_memory", 0) / MIB}
    for k in ("value_slab_bytes", "value_slab_live_bytes", "value_slab_full_free_bytes"):
        if k in m:
            out[k.replace("_bytes", "_mib")] = m[k] / MIB
    for k in ("active_defrag_hits", "active_defrag_passes", "active_defrag_slabs_emptied",
              "active_defrag_last_pass_ms", "active_defrag_running"):
        if k in m:
            out[k] = m[k]
    return {k: (round(v, 2) if isinstance(v, float) else v) for k, v in out.items()}


def setup(ctx):
    c = ctx.client()
    c.flushall()
    pinned = bool(SERVER_CPUS and ctx.pid)
    if pinned:
        pin_server(ctx.pid)
        os.sched_setaffinity(0, CLIENT_CPUS)
    ctx.raw["cpus"] = {"pinned": pinned, "server": SERVER_CPUS, "client": CLIENT_CPUS}
    return c


def sizes_for(shape, rnd, n):
    _, lo, hi = SHAPES[shape]
    return [lo if lo == hi else rnd.randint(lo, hi) for _ in range(n)]


def churn_once(ctx, c, shape, pct, settle):
    """Load, three cycles of churn, and (if `settle`) the wait after: (load,
    cycles, settled, sampler)."""
    keys = scaled(SHAPES[shape][0], step=1000)
    rnd = random.Random(240)
    settled = None
    with multiprocessing.Pool(PROCS) as pool:
        live = list(range(keys))
        parallel(pool, ctx.port, "set", list(zip(live, sizes_for(shape, rnd, keys))))
        assert c.dbsize() == keys, (c.dbsize(), keys)
        time.sleep(2)
        load = memory(c, ctx.pid)
        sampler = Sampler(ctx.pid)
        sampler.start()
        cycles, nxt = [], keys
        try:
            for cycle in range(3):
                n = len(live) * pct // 100
                rnd.shuffle(live)
                gone, live = live[:n], live[n:]
                parallel(pool, ctx.port, "del", gone)
                new = list(range(nxt, nxt + n))
                nxt += n
                parallel(pool, ctx.port, "set", list(zip(new, sizes_for(shape, rnd, n))))
                live += new
                time.sleep(2)
                cycles.append(memory(c, ctx.pid))
            assert c.dbsize() == len(live), (c.dbsize(), len(live))
            if settle:
                time.sleep(SETTLE_S)
                settled = memory(c, ctx.pid)
        finally:
            sampler.finish()
    return load, cycles, settled, sampler


def run_churn(ctx, c, shape, pct):
    load, cycles, settled, sampler = churn_once(ctx, c, shape, pct, settle=True)
    ctx.raw["load"], ctx.raw["cycles"], ctx.raw["settled"] = load, cycles, settled
    ctx.raw["rss_timeline"] = sampler.timeline[::4]
    tight = shape in ("b100", "kb1")
    kividb = ctx.kind != "redis"
    if tight:
        # The same churn again on the same server, emptied, with active defrag
        # off: the peak a pass must not raise (Redis: off in both).
        c.flushall()
        time.sleep(15)
        c.config_set("activedefrag", "no")
        off_load, off_cycles, _, off_sampler = churn_once(ctx, c, shape, pct, settle=False)
        ctx.raw["off"] = {"load": off_load, "cycles": off_cycles, "peak_rss_mib": off_sampler.peak,
                          "p99_rss_mib": off_sampler.p99()}
        off_x = off_sampler.p99() / off_load["rss_mib"]
        ctx.emit("peak_x_load_off", off_x, "x", "lower", gate=info())
        ctx.emit("peak_vs_off", (sampler.peak / load["rss_mib"]) / off_x, "x", "lower",
                 gate=[bound(max=1.07)] if kividb else info())
    ctx.emit("load_rss_mib", load["rss_mib"], "MiB", "lower", gate=info())
    ctx.emit("load_used_mib", load["used_mib"], "MiB", "lower", gate=info())
    ctx.emit("peak_rss_mib", sampler.peak, "MiB", "lower", gate=info())
    ctx.emit("settled_rss_mib", settled["rss_mib"], "MiB", "lower", gate=[rel(5)] if kividb else info())
    ctx.emit("settled_used_mib", settled["used_mib"], "MiB", "lower", gate=info())
    # 1 KB 1.03, 100 B 1.05 measured (#240, 2026-11-21; master 1.10-1.16): the issue's "about 5 %",
    # with room.
    gx = [rel(5)] + ([bound(max=1.07 if shape == "kb1" else 1.08)] if tight else [])
    ctx.emit("settled_x_load", settled["rss_mib"] / load["rss_mib"], "x", "lower", gate=gx if kividb else info())
    ctx.emit("peak_x_load", sampler.peak / load["rss_mib"], "x", "lower",
             gate=[rel(5 if tight else 10)] if kividb else info())


def _writer(port, keys, seed, stop, counter, rate):
    rnd = random.Random(seed)
    s = socket.create_connection(("127.0.0.1", port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    left, done, t0 = b"", 0, time.perf_counter()
    while not stop.is_set():
        batch = [resp(b"SET", key(rnd.randrange(keys)), value(rnd.randrange(keys), SNAP_SIZE)) for _ in range(20)]
        s.sendall(b"".join(batch))
        left = read_replies(s, len(batch), left)
        done += len(batch)
        counter.value = done
        ahead = done / rate - (time.perf_counter() - t0)
        if ahead > 0:
            time.sleep(ahead)
    s.close()


def wait_field(c, section, field, want, limit=900):
    t0 = time.time()
    seen_other = False
    while time.time() - t0 < limit:
        v = int(c.info(section).get(field, 0))
        if v != want:
            seen_other = True
        elif seen_other or time.time() - t0 > 2:
            return
        time.sleep(0.1)
    raise RuntimeError("%s stayed off %s for %d s" % (field, want, limit))


def run_snapshot(ctx, c):
    keys = scaled(SNAP_KEYS, step=1000)
    with multiprocessing.Pool(PROCS) as pool:
        parallel(pool, ctx.port, "set", [(i, SNAP_SIZE) for i in range(keys)])
    time.sleep(2)
    load = memory(c, ctx.pid)
    stop = multiprocessing.Event()
    counters = [multiprocessing.Value("q", 0) for _ in range(2)]
    procs = [multiprocessing.Process(target=_writer, args=(ctx.port, keys, 7 + i, stop, counters[i], 20_000))
             for i in range(2)]
    sampler = Sampler(ctx.pid)
    marks = {}
    try:
        for p in procs:
            p.start()
        sampler.start()
        time.sleep(5)
        c.bgsave()
        wait_field(c, "persistence", "rdb_bgsave_in_progress", 0)
        time.sleep(20)
        marks["bgsave"] = memory(c, ctx.pid)
        c.config_set("appendonly", "yes")
        time.sleep(1)
        wait_field(c, "persistence", "aof_rewrite_in_progress", 0)
        c.bgrewriteaof()
        wait_field(c, "persistence", "aof_rewrite_in_progress", 0)
        time.sleep(20)
        marks["rewrite"] = memory(c, ctx.pid)
    finally:
        stop.set()
        for p in procs:
            p.join(30)
            if p.is_alive():
                p.terminate()
    time.sleep(30)
    marks["final"] = memory(c, ctx.pid)
    sampler.finish()
    ctx.raw["load"], ctx.raw["marks"] = load, marks
    ctx.raw["writes"] = sum(x.value for x in counters)
    ctx.raw["rss_timeline"] = sampler.timeline[::10]
    kividb = ctx.kind != "redis"
    ctx.emit("load_rss_mib", load["rss_mib"], "MiB", "lower", gate=info())
    ctx.emit("bgsave_rss_mib", marks["bgsave"]["rss_mib"], "MiB", "lower", gate=info())
    ctx.emit("rewrite_rss_mib", marks["rewrite"]["rss_mib"], "MiB", "lower", gate=info())
    ctx.emit("peak_rss_mib", sampler.peak, "MiB", "lower", gate=info())
    ctx.emit("final_rss_mib", marks["final"]["rss_mib"], "MiB", "lower", gate=[rel(5)] if kividb else info())
    ctx.emit("final_used_mib", marks["final"]["used_mib"], "MiB", "lower", gate=info())
    ctx.emit("final_x_used", marks["final"]["rss_mib"] / marks["final"]["used_mib"], "x", "lower",
             gate=[rel(5)] if kividb else info())


def series(d, op):
    ts = d["ALL STATS"][op]["Time-Serie"]
    return {int(k): v for k, v in ts.items()}


LAT_TRIALS = 3


def latency_trial(ctx, c, trial):
    """One load, delete and memtier run with a pass in it: ({metric: value}, raw data)."""
    keys = scaled(LAT_KEYS, step=1000)
    kividb = ctx.kind != "redis"
    c.flushall()
    time.sleep(5)
    if kividb:
        c.config_set("activedefrag", "no")
    rnd = random.Random(2401)
    with multiprocessing.Pool(PROCS) as pool:
        parallel(pool, ctx.port, "set", [(i, LAT_SIZE) for i in range(keys)])
        gone = rnd.sample(range(keys), int(keys * LAT_DELETE))
        parallel(pool, ctx.port, "del", gone)
    time.sleep(2)
    out = os.path.join(ctx.out, "raw", S.name, "memtier-%s-%s-%d.json" % (ctx.config.name, ctx.engine, trial))
    os.makedirs(os.path.dirname(out), exist_ok=True)
    memtier = os.environ.get("MEMTIER", "memtier_benchmark")
    cmd = [memtier, "-s", "127.0.0.1", "-p", str(ctx.port), "--protocol=redis", "-t", "4", "-c", "25",
           "--test-time", str(LAT_TIME), "--ratio", "1:1", "-d", str(LAT_SIZE), "--key-prefix", "k:",
           "--key-minimum", "0", "--key-maximum", str(keys - 1), "--key-pattern", "R:R",
           "--randomize", "--distinct-client-seed", "--hide-histogram", "--json-out-file", out]
    t0 = time.perf_counter()
    mt = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    running = []  # (t, running)
    turned_on = None
    while mt.poll() is None:
        now = time.perf_counter() - t0
        if kividb and turned_on is None and now >= LAT_ON_AT:
            c.config_set("activedefrag", "yes")
            turned_on = now
        if kividb:
            running.append((now, int(c.info("memory").get("active_defrag_running", 0))))
        time.sleep(0.1)
    if mt.returncode != 0:
        raise RuntimeError("memtier failed: %d" % mt.returncode)
    with open(out) as f:
        d = json.load(f)
    on = [t for t, r in running if r]
    if kividb:
        if not on:
            raise RuntimeError("no pass ran")
        start, end = on[0], on[-1]
        pass_s = end - start
        busy = [s for s in range(LAT_TIME) if s >= start and s + 1 <= end]
        if not busy:  # a pass under a second: the second it was in
            busy = [int(start)]
        idle = [s for s in range(1, int(LAT_ON_AT))]
    else:
        busy, idle, pass_s = list(range(1, LAT_TIME - 1)), list(range(1, int(LAT_ON_AT))), None
    v = {}
    for op, name in (("Gets", "get"), ("Sets", "set")):
        ts = series(d, op)
        for pct, label in (("p99.00", "p99"), ("p99.90", "p999")):
            v["%s_%s_ms" % (name, label)] = median([ts[s][pct] for s in busy if s in ts])
            if label == "p99":
                v["%s_p99_idle_ms" % name] = median([ts[s][pct] for s in idle if s in ts])
        v["%s_ops_per_s_pass" % name] = median([ts[s]["Count"] for s in busy if s in ts])
    v["pass_s"] = pass_s
    raw = {"memory_after": memory(c, ctx.pid), "defrag_on_at": turned_on, "running": running[::5],
           "busy_seconds": busy, "idle_seconds": idle}
    return v, raw


def run_latency(ctx, c):
    """LAT_TRIALS trials on the same server, each from an empty one; every
    metric is the median of the trials, which are kept as its samples."""
    kividb = ctx.kind != "redis"
    trials = []
    for trial in range(LAT_TRIALS):
        v, raw = latency_trial(ctx, c, trial)
        trials.append(v)
        ctx.raw["trial%d" % trial] = raw
        ctx.log("trial %d: %s" % (trial, v))

    def emit(metric, unit, better, gate):
        xs = [t[metric] for t in trials if t.get(metric) is not None]
        ctx.emit(metric, median(xs), unit, better, gate=gate, samples=xs)

    for name in ("get", "set"):
        for label in ("p99", "p999"):
            emit("%s_%s_ms" % (name, label), "ms", "lower", jitter([rel(25, abs=0.2)]) if kividb else info())
        emit("%s_p99_idle_ms" % name, "ms", "lower", info())
        if kividb:
            for t in trials:
                t["%s_p99_excess_ms" % name] = t["%s_p99_ms" % name] - 1.5 * t["%s_p99_idle_ms" % name]
                t["%s_p99_pass_over_idle" % name] = t["%s_p99_ms" % name] / max(t["%s_p99_idle_ms" % name], 0.001)
            emit("%s_p99_pass_over_idle" % name, "x", "lower", info())
            # The pass must not cost latency: its seconds against the same trial's idle ones.
            emit("%s_p99_excess_ms" % name, "ms", "lower", jitter(bound(max=0.2)))
        emit("%s_ops_per_s_pass" % name, "ops/s", "higher", info())
    if kividb:
        emit("pass_s", "s", "lower", jitter(info()))


@S.run
def run(ctx):
    c = setup(ctx)
    name = ctx.config.name
    if name == "snapshot":
        return run_snapshot(ctx, c)
    if name == "pass-latency":
        return run_latency(ctx, c)
    shape, pct = name.split("-c")
    run_churn(ctx, c, shape, int(pct))


if __name__ == "__main__":
    sys.exit(S.main())
