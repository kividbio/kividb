#!/usr/bin/env python3
"""#825: `BGSAVE` of large collections under writes. A save laid out every
value that is not a string in memory, with writes held, and kept it until
the dump was written: the size of the collections' part of the dump over
again (+0.45 GiB of RSS for #825's 20,000 Mastodon home feeds, whose dump is
482 MB), whatever the write rate. Each capture thread now keeps at most
8 MiB of it and spills the rest to a file of its own, so the growth is at
most 64 MiB a save, whatever the dataset.

Two datasets, about #825's size:

  zset   20,000 sorted sets of 800 members (Mastodon's home feeds,
         `FeedManager::MAX_ITEMS`; members and scores are 18-digit status
         ids), loaded with one `ZADD` of 800 a key, in pipelines of 50.
         The writer adds a status to a random feed and trims it back to 800
         (`ZADD`, `ZREMRANGEBYRANK 0 -801`), as Mastodon's fan-out does.
  hash   20,000 hashes of 800 fields (`f<15 digits>` -> an 18-digit
         number), loaded likewise with `HSET`; the writer sets a field of a
         random hash anew.

Load: one writer process, pipelines of 40, as fast as the server answers,
from 2 s before `BGSAVE` until the save has finished and 1 s longer. The
growth does not move with the write rate (#825: +0.450 GiB at half the
issue's rate, +0.452 at 1.7x), so one writer is enough to make it a save
under writes; `writes` says how many got through.

A probe connection sends `SET probe <n>` one at a time, every 1 ms, from
the `BGSAVE` command until its end: the slowest reply is how long writes were
held (the capture happens with writes held, and now writes the spill files
then).

What is measured, as suite 39: the memory of the server and of every
process it started (Redis's fork), once every 50 ms from the `BGSAVE` command
until the save finished, each process's `Pss` summed from
`/proc/<pid>/smaps_rollup`.

  idle_mib        the memory before the writer starts
  peak_mib        the highest sum from `BGSAVE` to its end
  growth_mib      peak_mib - idle_mib
  peak_x_idle     peak_mib / idle_mib
  worst_write_ms  the probe's slowest `SET` during the save
  dump_mib        the size of the dump written
  bgsave_s        the time from `BGSAVE` to `rdb_bgsave_in_progress:0`
  writes          the writer's commands meanwhile
  bgsave_ok       1 if `rdb_last_bgsave_status` is `ok` and a dump was written

Gates (the candidate): `growth_mib` at most 128 MiB (the 64 MiB of spill
budgets, and as much again for the writes and the allocator) and within
25 % or 16 MiB of recorded; `peak_x_idle` within 5 %; `worst_write_ms`
within 50 % or 10 ms of recorded (scheduling jitter); the save must succeed.
v1.2.2 and Redis are shown, not gated: v1.2.2 grows by about the dump's size,
Redis's fork by the pages the writer changes.

Distinct from `39-bgsave-strings` (string values, which a capture only
holds) and `24-snapshot-size` (the dump's size and time).

CPU: as suite 39, on Linux the server is pinned to the first half of the
CPUs (`--threads` as many) and the clients to the other half;
`REGRESS_SERVER_CPUS` / `REGRESS_CLIENT_CPUS` choose others. Needs 8 vCPUs
(`min_cpus=8`): record and run it with `run-all.sh --min-cpus-only`.
"""
import glob
import multiprocessing
import os
import random
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, rel, bound, info, jitter, scaled  # noqa: E402

FEEDS = 20_000
MEMBERS = 800
LOAD_BATCH = 50
WRITE_BATCH = 40
LEAD_S = 2.0
TAIL_S = 1.0
SAMPLE_S = 0.05
PROBE_EVERY_S = 0.001
BASE_ID = 110_000_000_000_000_000
GROWTH_MAX_MIB = 128


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
THREADS = len(SERVER_CPUS) if SERVER_CPUS else 0
S = Suite("47-bgsave-collections", [Config("zset", threads=THREADS), Config("hash", threads=THREADS)],
          min_cpus=8)


def pin_server(pid):
    for tid in os.listdir("/proc/%d/task" % pid):
        os.sched_setaffinity(int(tid), SERVER_CPUS)


def load(c, kind, feeds):
    p = c.pipeline(transaction=False)
    for k in range(feeds):
        key = "feed:%d" % k
        if kind == "zset":
            ids = range(BASE_ID + k * MEMBERS, BASE_ID + (k + 1) * MEMBERS)
            p.zadd(key, {str(i): i for i in ids})
        else:
            p.hset(key, mapping={"f%015d" % m: BASE_ID + m for m in range(MEMBERS)})
        if k % LOAD_BATCH == LOAD_BATCH - 1:
            p.execute()
    p.execute()


def writer(port, kind, feeds, stop, counter):
    """Writes to random existing keys, as fast as the server answers."""
    import redis
    rnd = random.Random(825)
    c = redis.Redis(host="127.0.0.1", port=port, socket_timeout=60)
    p = c.pipeline(transaction=False)
    done, next_id = 0, BASE_ID + feeds * MEMBERS
    while not stop.is_set():
        for _ in range(WRITE_BATCH):
            key = "feed:%d" % rnd.randrange(feeds)
            if kind == "zset":
                p.zadd(key, {str(next_id): next_id})
                p.zremrangebyrank(key, 0, -(MEMBERS + 1))
                next_id += 1
            else:
                p.hset(key, "f%015d" % rnd.randrange(MEMBERS), next_id)
                next_id += 1
        p.execute()
        done += WRITE_BATCH * (2 if kind == "zset" else 1)
        counter.value = done


def family(pid):
    """pid and every process below it."""
    seen, todo = [], [pid]
    while todo:
        p = todo.pop()
        seen.append(p)
        try:
            for tid in os.listdir("/proc/%d/task" % p):
                with open("/proc/%d/task/%s/children" % (p, tid)) as f:
                    todo.extend(int(x) for x in f.read().split())
        except OSError:
            pass
    return seen


def pss_kib(pid):
    """Pss in KiB summed over the server and its children."""
    pss = 0
    for p in family(pid):
        try:
            with open("/proc/%d/smaps_rollup" % p) as f:
                for line in f:
                    if line.startswith("Pss:"):
                        pss += int(line.split()[1])
        except OSError:
            pass
    return pss


class Sampler(threading.Thread):
    def __init__(self, pid):
        super().__init__(daemon=True)
        self.pid, self.stop_ev, self.peak = pid, threading.Event(), 0

    def run(self):
        while not self.stop_ev.is_set():
            self.peak = max(self.peak, pss_kib(self.pid))
            self.stop_ev.wait(SAMPLE_S)

    def finish(self):
        self.stop_ev.set()
        self.join()


class Probe(threading.Thread):
    """`SET probe <n>` one at a time; the slowest reply."""

    def __init__(self, port):
        super().__init__(daemon=True)
        import redis
        self.c = redis.Redis(host="127.0.0.1", port=port, socket_timeout=60)
        self.stop_ev, self.worst, self.sent = threading.Event(), 0.0, 0

    def run(self):
        while not self.stop_ev.is_set():
            t = time.perf_counter()
            self.c.set("probe", self.sent)
            self.worst = max(self.worst, time.perf_counter() - t)
            self.sent += 1
            self.stop_ev.wait(PROBE_EVERY_S)

    def finish(self):
        self.stop_ev.set()
        self.join()


@S.run
def run(ctx):
    c = ctx.client()
    c.flushall()
    pinned = bool(SERVER_CPUS and ctx.pid)
    if pinned:
        pin_server(ctx.pid)
        os.sched_setaffinity(0, CLIENT_CPUS)  # this process and the writer it forks
    ctx.raw["cpus"] = {"pinned": pinned, "server": SERVER_CPUS, "client": CLIENT_CPUS,
                       "os_cpus": os.cpu_count()}
    kind = ctx.config.name
    feeds = scaled(FEEDS, step=100)
    load(c, kind, feeds)
    assert c.dbsize() == feeds, (c.dbsize(), feeds)
    ctx.raw["keys"], ctx.raw["members"] = feeds, feeds * MEMBERS
    time.sleep(2)
    idle = pss_kib(ctx.pid) / 1024.0

    stop = multiprocessing.Event()
    counter = multiprocessing.Value("q", 0)
    proc = multiprocessing.Process(target=writer, args=(ctx.port, kind, feeds, stop, counter))
    sampler, probe = Sampler(ctx.pid), Probe(ctx.port)
    status = ""
    try:
        proc.start()
        time.sleep(LEAD_S)
        written_before = counter.value
        sampler.start()
        probe.start()
        t0 = time.perf_counter()
        c.bgsave()
        seen_running, ended = False, None
        while True:
            persistence = c.info("persistence")
            running = int(persistence.get("rdb_bgsave_in_progress", 0))
            seen_running = seen_running or running == 1
            now = time.perf_counter()
            if running == 0 and (seen_running or now - t0 > 2.0):
                ended = now
                status = persistence.get("rdb_last_bgsave_status", "")
                break
            if now - t0 > 600:
                raise RuntimeError("BGSAVE did not finish in 600 s")
            time.sleep(0.05)
        probe.finish()
        time.sleep(TAIL_S)
    finally:
        sampler.finish()
        stop.set()
        proc.join(30)
        if proc.is_alive():
            proc.terminate()
    writes = counter.value - written_before
    d = c.config_get("dir")["dir"]
    dumps = [f for f in glob.glob(os.path.join(d, "dump.*")) if os.path.getsize(f) > 0]
    ok = 1 if status == "ok" and dumps else 0
    peak = sampler.peak / 1024.0
    ctx.raw["bgsave"] = {"status": status, "dumps": dumps, "probes": probe.sent,
                         "dump_bytes": [os.path.getsize(f) for f in dumps]}
    if sampler.peak == 0 or writes == 0:
        raise RuntimeError("nothing measured: peak %d KiB, %d writes" % (sampler.peak, writes))

    ctx.emit("idle_mib", idle, "MiB", "lower", gate=[rel(5)])
    ctx.emit("peak_mib", peak, "MiB", "lower", gate=info())
    ctx.emit("growth_mib", peak - idle, "MiB", "lower",
             gate=[bound(max=GROWTH_MAX_MIB), rel(25, abs=16)])
    ctx.emit("peak_x_idle", peak / idle, "x", "lower", gate=[rel(5)])
    ctx.emit("worst_write_ms", probe.worst * 1000.0, "ms", "lower", gate=jitter(rel(50, abs=10)))
    ctx.emit("dump_mib", max([os.path.getsize(f) for f in dumps] or [0]) / 1048576.0, "MiB", "lower",
             gate=info())
    ctx.emit("bgsave_s", ended - t0, "s", "lower", gate=jitter(info()))
    ctx.emit("writes", writes, "commands", "higher", gate=info())
    ctx.emit("bgsave_ok", ok, "bool", "higher", gate=bound(min=1))


if __name__ == "__main__":
    sys.exit(S.main())
