#!/usr/bin/env python3
"""#825: `BGSAVE` of large collections under writes. A save lays out the
values that are not strings when it captures the dataset, with writes held;
for `dump.kdb` it lays them out in 1 MiB buffers, each handed once full to a
writer thread that writes it to a spill file in the data directory, copied
into the dump as the snapshot is written. A capture thread never waits for
that writer: when it is behind, a buffer is held in memory until there is
room. So the save holds about 8 MiB a capture thread while the disk keeps
up; v1.2.2 holds the collections' part of the dump whole (+0.45 GiB of RSS
for #825's 20,000 Mastodon home feeds, whose dump is 482 MB), whatever the
write rate.

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

A probe process of its own (`lib/probe.py`, `kind="set"`) sends a `SET` one
at a time, every 1 ms: its slowest reply in flight during the save is how
long writes were held for the capture.

What is measured, as suite 39: the memory of the server and of every
process it started (Redis's fork), once every 50 ms from the `BGSAVE` command
until the save finished, each process's `Pss` summed from
`/proc/<pid>/smaps_rollup`.

  idle_mib          the memory before the writer starts
  peak_mib          the highest sum from `BGSAVE` to its end
  growth_mib        peak_mib - idle_mib
  peak_x_idle       peak_mib / idle_mib
  worst_write_ms    the probe's slowest `SET` during the save
  dump_mib          the size of the dump written
  bgsave_s          the time from `BGSAVE` to `rdb_bgsave_in_progress:0`
  writes            the writer's commands meanwhile
  bgsave_ok         1 if `rdb_last_bgsave_status` is `ok` and a dump was written
  cgroup_growth_mib the growth of the server's cgroup's `memory.current`
                    meanwhile (it counts the page cache the spill files and
                    the dump fill, which `Pss` does not), where it can be read
  dirty_peak_mib    the highest `Dirty:` of `/proc/meminfo` meanwhile

Gates (the candidate): `growth_mib` at most 128 MiB (8 MiB of buffers a
capture thread, and as much again for the writes and the allocator) and
within 25 % or 16 MiB of recorded; `peak_x_idle` within 5 %;
`worst_write_ms` within 50 % or 10 ms of recorded (scheduling jitter), and
at most 1.3x v1.2.2's this session (the pause is what a spill could
lengthen, and for `hash` also the walk of each table-backed hash, whose
fields are each a block of their own: the capture asks for a few groups'
fields ahead, and writes an ASCII field name as it is); the save must
succeed. v1.2.2 and Redis are shown: v1.2.2 grows
by about the dump's size, Redis's fork by the pages the writer changes.

Distinct from `39-bgsave-strings` (string values, which a capture only
holds) and `24-snapshot-size` (the dump's size and time).

CPU: as suite 39, on Linux the server is pinned to the first half of the
CPUs (`--threads` as many) and the clients to the other half;
`REGRESS_SERVER_CPUS` / `REGRESS_CLIENT_CPUS` choose others. Needs 4 vCPUs
(`min_cpus=4`): a writer, a probe and the sampler on two CPUs measure the
server, not themselves; recorded on a `Standard_D4s_v5`.
"""
import glob
import multiprocessing
import os
import random
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, rel, bound, info, jitter, scaled, vs_v1_2_2  # noqa: E402
from probe import Probe  # noqa: E402

FEEDS = 20_000
MEMBERS = 800
LOAD_BATCH = 50
WRITE_BATCH = 40
LEAD_S = 2.0
TAIL_S = 1.0
SAMPLE_S = 0.05
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
          min_cpus=4)


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


def cgroup_bytes(pid):
    """The `memory.current` of `pid`'s cgroup (v2), or None."""
    try:
        with open("/proc/%d/cgroup" % pid) as f:
            path = next(line.split(":", 2)[2].strip() for line in f if line.startswith("0::"))
        with open("/sys/fs/cgroup%s/memory.current" % path) as f:
            return int(f.read())
    except (OSError, StopIteration, ValueError):
        return None


def dirty_kib():
    try:
        with open("/proc/meminfo") as f:
            return next(int(line.split()[1]) for line in f if line.startswith("Dirty:"))
    except (OSError, StopIteration, ValueError):
        return 0


class Sampler(threading.Thread):
    def __init__(self, pid):
        super().__init__(daemon=True)
        self.pid, self.stop_ev, self.peak = pid, threading.Event(), 0
        self.cgroup_before, self.cgroup_peak, self.dirty_peak = cgroup_bytes(pid), 0, 0

    def run(self):
        while not self.stop_ev.is_set():
            self.peak = max(self.peak, pss_kib(self.pid))
            self.cgroup_peak = max(self.cgroup_peak, cgroup_bytes(self.pid) or 0)
            self.dirty_peak = max(self.dirty_peak, dirty_kib())
            self.stop_ev.wait(SAMPLE_S)

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
    sampler, probe = Sampler(ctx.pid), Probe(ctx.port, n=1, kind="set", prefix="probe:", keys=1)
    status = ""
    try:
        proc.start()
        probe.start()
        time.sleep(LEAD_S)
        written_before = counter.value
        sampler.start()
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
        time.sleep(TAIL_S)
    finally:
        probe.stop()
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
    worst_ms = probe.worst(t0, ended)
    ctx.raw["bgsave"] = {"status": status, "dumps": dumps, "probes": probe.count(),
                         "probe_top": probe.top(10), "dump_bytes": [os.path.getsize(f) for f in dumps],
                         "cgroup_before": sampler.cgroup_before, "cgroup_peak": sampler.cgroup_peak}
    if sampler.peak == 0 or writes == 0:
        raise RuntimeError("nothing measured: peak %d KiB, %d writes" % (sampler.peak, writes))

    ctx.emit("idle_mib", idle, "MiB", "lower", gate=[rel(5)])
    ctx.emit("peak_mib", peak, "MiB", "lower", gate=info())
    ctx.emit("growth_mib", peak - idle, "MiB", "lower",
             gate=[bound(max=GROWTH_MAX_MIB), rel(25, abs=16)])
    ctx.emit("peak_x_idle", peak / idle, "x", "lower", gate=[rel(5)])
    if ctx.kind == "redis":
        pause_gate = info()
    else:
        pause_gate = [rel(50, abs=10), vs_v1_2_2(1.3)]
    ctx.emit("worst_write_ms", worst_ms, "ms", "lower", gate=jitter(pause_gate))
    ctx.emit("dump_mib", max([os.path.getsize(f) for f in dumps] or [0]) / 1048576.0, "MiB", "lower",
             gate=info())
    ctx.emit("bgsave_s", ended - t0, "s", "lower", gate=jitter(info()))
    ctx.emit("writes", writes, "commands", "higher", gate=info())
    ctx.emit("bgsave_ok", ok, "bool", "higher", gate=bound(min=1))
    if sampler.cgroup_before is not None:
        ctx.emit("cgroup_growth_mib", (sampler.cgroup_peak - sampler.cgroup_before) / 1048576.0, "MiB",
                 "lower", gate=info())
    ctx.emit("dirty_peak_mib", sampler.dirty_peak / 1024.0, "MiB", "lower", gate=info())


if __name__ == "__main__":
    sys.exit(S.main())
