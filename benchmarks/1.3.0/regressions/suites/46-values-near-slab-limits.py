#!/usr/bin/env python3
"""#240 (values near 4 KiB): memory per key and SET rate of string values on
both sides of each size where their memory changes kind.

A key's entry (header, 16-byte name, the value inline) of up to 4,096 bytes
holds the value; a longer value has a block of its own (8-byte header). Blocks
of up to 2 KiB are in 64 KiB slabs, 16 and 32 bytes apart; since #240 the ones
above, up to 26,208 bytes (`value_slab::MAX_BLOCK`), are in 1 MiB slabs, in
classes at most 2.5 % apart whose sizes are a slab's over a count of blocks,
and a longer one is the system allocator's (in v1.2.2: 64 KiB slabs up to
4,096 bytes, the system allocator above).

Configurations, each a fresh server: a group of keys (16-byte names) per size,
loaded one group after the other by 8 processes in pipelines of 100:

  near-2k   1,996 and 2,012 bytes: the last entry in a 64 KiB slab and the
            first in a 512 KiB one (20,000 keys each)
  near-4k   3,500 to 4,500 bytes, 100 apart, 4,048 / 4,056 (the last value
            inline and the first in a block of its own) and 4,088 / 4,100
            (v1.2.2's last block in a slab and first of the system
            allocator's) (20,000 each)
  near-max  24,000 to 28,000 bytes, 1,000 apart, and 26,192 / 26,208: the
            last block in a slab and the first of the system allocator's
            (4,000 each)

  rss_b_<size>         VmRSS growth over the group's load, per key (info)
  used_b_<size>        used_memory growth, per key (info)
  worst_overhead_pct   the most a size's RSS per key is above the value's size
                       plus the configuration's smallest overhead (RSS per key
                       less the value's size), as a per cent of the value's
                       size: at most 3 (the class a block is rounded up to)
  set_kops_<size>      thousands of SETs a second, 8 processes overwriting
                       random keys of the group in pipelines of 100 for 3 s;
                       the median of 3 trials, the sizes taken in turn (info)
  cpu_us_<size>        the server's CPU time a SET in those trials (info)
  step_<a>_<b>         set_kops of b over a, across the boundary: at least 0.97
                       (each trial's pair is taken back to back; the median)

Before #240, `step_4048_4056` was about 1 (both in 64 KiB slabs) but 4,100 B
cost 15 % more CPU a SET than 4,088 B, and near-4k's worst overhead was
200 bytes on 3,832 (the end of a 64 KiB slab of 4,096-byte blocks): those are
the shapes the gates hold. v1.2.2 and Redis are shown, not gated.

CPU: as suite 43, the server is pinned to the first half of the CPUs
(`--threads` as many) and the clients to the other half. Needs 8 vCPUs
(`min_cpus=8`): record and run it with `run-all.sh --min-cpus-only`.
"""
import multiprocessing
import os
import random
import socket
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, bound, info, scaled, median  # noqa: E402
from rss import vmrss_mib  # noqa: E402

PROCS = 8
PIPE = 100
TRIAL_S = 3
TRIALS = 3
MIB = 1048576.0
SHAPES = {
    "near-2k": (20_000, [1996, 2012], [(1996, 2012)]),
    "near-4k": (20_000, sorted(set(range(3500, 4501, 100)) | {4048, 4056, 4088}), [(4048, 4056), (4088, 4100)]),
    "near-max": (4_000, sorted(set(range(24000, 28001, 1000)) | {26192, 26208}), [(26192, 26208)]),
}


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
S = Suite("46-values-near-slab-limits", [Config(name, threads=THREADS) for name in SHAPES], min_cpus=8)
DATA = random.Random(2460).randbytes(1 << 15)


def key(size, i):
    return b"%05d:%010d" % (size, i)  # 16 bytes


def value(size, i):
    o = (i * 7919) % (len(DATA) - size)
    return DATA[o:o + size]


def resp(*args):
    return b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(a), a) for a in args)


def read_ok(sock, n, left):
    """Read n `+OK` replies; return what is left over."""
    want = 5 * n
    while len(left) < want:
        chunk = sock.recv(1 << 20)
        if not chunk:
            raise RuntimeError("connection closed")
        left += chunk
    if left[:1] != b"+":
        raise RuntimeError(left[:200])
    return left[want:]


def connect(port):
    s = socket.create_connection(("127.0.0.1", port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return s


def _load(args):
    port, size, ids = args
    s, left = connect(port), b""
    for at in range(0, len(ids), PIPE):
        chunk = ids[at:at + PIPE]
        s.sendall(b"".join(resp(b"SET", key(size, i), value(size, i)) for i in chunk))
        left = read_ok(s, len(chunk), left)
    s.close()


def _overwrite(args):
    """SETs of random keys of the group until `until`: how many."""
    port, size, keys, seed, start, until = args
    rnd = random.Random(seed)
    bufs = [b"".join(resp(b"SET", key(size, rnd.randrange(keys)), value(size, rnd.randrange(keys)))
                     for _ in range(PIPE)) for _ in range(4)]
    s, left, done = connect(port), b"", 0
    while time.time() < start:
        time.sleep(0.001)
    while time.time() < until:
        s.sendall(bufs[done // PIPE % 4])
        left = read_ok(s, PIPE, left)
        done += PIPE
    s.close()
    return done


def cpu_s(pid):
    f = open("/proc/%d/stat" % pid).read().rsplit(")", 1)[1].split()
    return (int(f[11]) + int(f[12])) / os.sysconf("SC_CLK_TCK")


def pin_server(pid):
    for tid in os.listdir("/proc/%d/task" % pid):
        try:
            os.sched_setaffinity(int(tid), SERVER_CPUS)
        except OSError:
            pass


@S.run
def run(ctx):
    c = ctx.client()
    c.flushall()
    pinned = bool(SERVER_CPUS and ctx.pid)
    if pinned:
        pin_server(ctx.pid)
        os.sched_setaffinity(0, CLIENT_CPUS)
    ctx.raw["cpus"] = {"pinned": pinned, "server": SERVER_CPUS, "client": CLIENT_CPUS}
    keys, sizes, pairs = SHAPES[ctx.config.name]
    keys = scaled(keys, step=1000)
    groups = {}
    with multiprocessing.Pool(PROCS) as pool:
        for size in sizes:
            time.sleep(0.5)
            rss0, used0 = vmrss_mib(ctx.pid), c.info("memory")["used_memory"]
            ids = list(range(keys))
            pool.map(_load, [(ctx.port, size, ids[p::PROCS]) for p in range(PROCS)])
            time.sleep(1)
            rss1, used1 = vmrss_mib(ctx.pid), c.info("memory")["used_memory"]
            groups[size] = {"rss_b": (rss1 - rss0) * MIB / keys, "used_b": (used1 - used0) / keys}
        assert c.dbsize() == keys * len(sizes), (c.dbsize(), keys * len(sizes))
        rates = {size: [] for size in sizes}
        cpus = {size: [] for size in sizes}
        for trial in range(TRIALS):
            for size in sizes:
                start = time.time() + 0.2
                until = start + TRIAL_S
                cpu0 = cpu_s(ctx.pid)
                done = sum(pool.map(_overwrite, [(ctx.port, size, keys, trial * 100 + p, start, until)
                                                 for p in range(PROCS)]))
                took = time.time() - start
                cpus[size].append((cpu_s(ctx.pid) - cpu0) * 1e6 / done)
                rates[size].append(done / took / 1000)
    overhead = {size: g["rss_b"] - size for size, g in groups.items()}
    least = min(overhead.values())
    for size in sizes:
        ctx.emit("rss_b_%d" % size, groups[size]["rss_b"], "B", "lower", gate=info())
        ctx.emit("used_b_%d" % size, groups[size]["used_b"], "B", "lower", gate=info())
        ctx.emit("set_kops_%d" % size, median(rates[size]), "kops", "higher", gate=info(), samples=rates[size])
        ctx.emit("cpu_us_%d" % size, median(cpus[size]), "us", "lower", gate=info(), samples=cpus[size])
    worst = max((overhead[s] - least) * 100.0 / s for s in sizes)
    ctx.emit("worst_overhead_pct", worst, "%", "lower", gate=bound(max=3))
    for a, b in pairs:
        steps = [rb / ra for ra, rb in zip(rates[a], rates[b])]
        ctx.emit("step_%d_%d" % (a, b), median(steps), "x", "higher", gate=bound(min=0.97), samples=steps)
    ctx.raw["groups"] = groups
    ctx.raw["rates"] = rates
    ctx.raw["cpu_us"] = cpus


if __name__ == "__main__":
    sys.exit(S.main())
