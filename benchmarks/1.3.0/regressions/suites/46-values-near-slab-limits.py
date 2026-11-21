#!/usr/bin/env python3
"""#240 (values near 4 KiB): memory per key and SET rate of string values on
both sides of each size where their memory changes kind.

A key's entry (a 12-byte header, the 16-byte name and the value inline) of up
to 4,096 bytes holds the value: up to 4,068 bytes; a longer value has a block
of its own (8-byte header). Blocks of up to 2 KiB are in 64 KiB slabs, 16 and
32 bytes apart; since #240 the ones above, up to 26,208 bytes
(`value_slab::MAX_BLOCK`), are in 1 MiB slabs, in classes at most 2.5 % apart
whose sizes are a slab's over a count of blocks, and a longer one is the
system allocator's (in v1.2.2: 64 KiB slabs up to 4,096 bytes, the system
allocator above).

Configurations: a group of keys (16-byte names) per size, each group in a
server of its own (so that a table's growth is every group's alike), loaded by
8 processes in pipelines of 100, one server after the other; the servers stay
up for the SET trials:

  near-2k   1,900 to 2,200 bytes, with 2,016 / 2,028: the last entry in a
            64 KiB slab and the first in a 1 MiB one (40,000 keys each)
  near-4k   3,500 to 4,500 bytes, 100 apart, with 4,064 / 4,072 (the last
            value inline and the first in a block of its own) and 4,088 /
            4,100 (v1.2.2's last block in a slab and its first of the system
            allocator's) (50,000 each)
  near-max  24,000 to 28,000 bytes, 1,000 apart, with 26,192 / 26,208: the
            last block in a slab and the first of the system allocator's
            (16,000 each)

The pairs are chosen with both requests on one side of a multiple of 4 KiB
(and 2 KiB): a SET's request with a 16-byte key is the value and 41 bytes, and
one just over 4,096 bytes costs every server more than one just under, KiviDB
as Redis (`step_4048_4056`, shown, not gated).

  rss_b_<size>         VmRSS growth over the group's load, per key (info; one
                       size a configuration within 3 % of its recorded value:
                       2,100, 4,300 and 25,000 bytes)
  used_b_<size>        used_memory growth, per key (info)
  worst_overhead_pct   the most a size's RSS per key is above the value's size
                       plus the configuration's smallest overhead (RSS per key
                       less the value's size), as a per cent of the value's
                       size: at most 3 (the class a block is rounded up to)
  set_kops_<size>      thousands of SETs a second, 8 processes overwriting
                       random keys of the group in pipelines of 100 for 3 s;
                       the median of 7 trials, the sizes taken in turn (info)
  cpu_us_<size>        the server's CPU time a SET in those trials (info)
  step_<a>_<b>         set_kops of b over a, across the boundary: at least 0.97
                       (each trial's pair is taken in the same round; the
                       median of the 7 rounds' ratios); `step_4064_4072`, no
                       more than 3 % below its recorded value: a string of its
                       own is written in place as an inline one is, but its
                       block is one more line to reach, not in the cache, and
                       costs a SET about 5 % more of the server's CPU
  cpu_step_<a>_<b>     cpu_us of b over a, the same way (info)

v1.2.2 and Redis are shown, not gated.

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
from suite import Suite, Config, bound, info, rel, scaled, median  # noqa: E402
from rss import vmrss_mib  # noqa: E402
from nodes import Nodes  # noqa: E402

PROCS = 8
PIPE = 100
TRIAL_S = 3
TRIALS = 7
MIB = 1048576.0
# A step's gate: within 3 %, no more than 3 % below the recorded one, or shown.
WITHIN, KEPT, SHOWN = "within", "kept", "shown"
# keys a group, sizes, the pairs and their gates
SHAPES = {
    "near-2k": (40_000, sorted(set(range(1900, 2201, 100)) | {2016, 2028}), [(2016, 2028, WITHIN)]),
    "near-4k": (50_000, sorted(set(range(3500, 4501, 100)) | {4048, 4056, 4064, 4072, 4088}),
                [(4064, 4072, KEPT), (4088, 4100, WITHIN), (4048, 4056, SHOWN)]),
    "near-max": (16_000, sorted(set(range(24000, 28001, 1000)) | {26192, 26208}), [(26192, 26208, WITHIN)]),
}
GATES = {WITHIN: bound(min=0.97), KEPT: rel(3), SHOWN: info()}
# One size a configuration whose RSS a key is held to its recorded value
# (3 %): `worst_overhead_pct` is within its group, and a rise of every size
# alike would pass it.
HELD_RSS = {"near-2k": 2100, "near-4k": 4300, "near-max": 25000}


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
S = Suite("46-values-near-slab-limits", [Config(name, threads=THREADS, self_hosted=True) for name in SHAPES],
          min_cpus=8)
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
    pinned = bool(SERVER_CPUS)
    if pinned:
        os.sched_setaffinity(0, CLIENT_CPUS)
    ctx.raw["cpus"] = {"pinned": pinned, "server": SERVER_CPUS, "client": CLIENT_CPUS}
    keys, sizes, pairs = SHAPES[ctx.config.name]
    keys = scaled(keys, step=1000)
    groups, nodes = {}, {}
    with Nodes(ctx) as ns, multiprocessing.Pool(PROCS) as pool:
        for size in sizes:
            node = ns.start()
            if pinned:
                pin_server(node.pid)
            c = node.client()
            time.sleep(0.5)
            rss0, used0 = vmrss_mib(node.pid), c.info("memory")["used_memory"]
            ids = list(range(keys))
            pool.map(_load, [(node.port, size, ids[p::PROCS]) for p in range(PROCS)])
            time.sleep(1)
            rss1, used1 = vmrss_mib(node.pid), c.info("memory")["used_memory"]
            assert c.dbsize() == keys, (c.dbsize(), keys)
            groups[size] = {"rss_b": (rss1 - rss0) * MIB / keys, "used_b": (used1 - used0) / keys}
            nodes[size] = node
        rates = {size: [] for size in sizes}
        cpus = {size: [] for size in sizes}
        for trial in range(TRIALS):
            for size in sizes:
                node = nodes[size]
                start = time.time() + 0.2
                until = start + TRIAL_S
                cpu0 = cpu_s(node.pid)
                done = sum(pool.map(_overwrite, [(node.port, size, keys, trial * 100 + p, start, until)
                                                 for p in range(PROCS)]))
                took = time.time() - start
                cpus[size].append((cpu_s(node.pid) - cpu0) * 1e6 / done)
                rates[size].append(done / took / 1000)
    overhead = {size: g["rss_b"] - size for size, g in groups.items()}
    least = min(overhead.values())
    for size in sizes:
        rss_gate = rel(3) if size == HELD_RSS[ctx.config.name] else info()
        ctx.emit("rss_b_%d" % size, groups[size]["rss_b"], "B", "lower", gate=rss_gate)
        ctx.emit("used_b_%d" % size, groups[size]["used_b"], "B", "lower", gate=info())
        ctx.emit("set_kops_%d" % size, median(rates[size]), "kops", "higher", gate=info(), samples=rates[size])
        ctx.emit("cpu_us_%d" % size, median(cpus[size]), "us", "lower", gate=info(), samples=cpus[size])
    worst = max((overhead[s] - least) * 100.0 / s for s in sizes)
    ctx.emit("worst_overhead_pct", worst, "%", "lower", gate=bound(max=3))
    for a, b, kind in pairs:
        steps = [rb / ra for ra, rb in zip(rates[a], rates[b])]
        ctx.emit("step_%d_%d" % (a, b), median(steps), "x", "higher", gate=GATES[kind], samples=steps)
        costs = [cb / ca for ca, cb in zip(cpus[a], cpus[b])]
        ctx.emit("cpu_step_%d_%d" % (a, b), median(costs), "x", "lower", gate=info(), samples=costs)
    ctx.raw["groups"] = groups
    ctx.raw["rates"] = rates
    ctx.raw["cpu_us"] = cpus


if __name__ == "__main__":
    sys.exit(S.main())
