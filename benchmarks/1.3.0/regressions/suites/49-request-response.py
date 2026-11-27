#!/usr/bin/env python3
"""Request-response from many connections, each waiting for every reply (#1015).

@keyv/redis's own benchmark (jaredwray/keyv `storage/redis/benchmark`):
200,000 keys of 1 KB with a TTL; each connection owns a share of them and
does one `SET ... PX` of a key of its share, then nine `GET`s (the first of
the key it just wrote, then random keys of its share), and waits for every
reply before it sends the next command; each reply is checked. Here the
client is this file's own, in Python: one process a client CPU (at most 4),
each an asyncio loop over its share of the connections, as the benchmark
runs its connections on 4 Node worker threads. Like Node's, the client is
the limit: it uses all of its CPUs, so a server that costs the client's
CPUs more a request (an interrupt it sends them, a wake-up) is answered
fewer times a second, although the server itself is half idle.

On Linux an io_uring ring by default finishes a socket read that waited by
interrupting the worker thread at once if it is running; the CPU that took
the request in, the client's, sends that interrupt, about once a request
with one request in flight a connection. KiviDB's workers ask the kernel
not to (`IORING_SETUP_COOP_TASKRUN`, Linux 5.19): the read then finishes at
the worker's next system call, which it makes on every turn of its loop.

One server an engine, `--threads 4` (Redis `--io-threads 4`), its threads
pinned to the first half of the CPUs this process may use, the client's
processes to the other half (`REGRESS_SERVER_CPUS` / `REGRESS_CLIENT_CPUS`,
`0-3`, `4-7`, choose others). Points: 1, 4, 16, 48 and 500 connections
(the issue's two are 48 and 500). Each point: the keys written anew (500
`SET`s a pipeline), then WINDOW_S of the workload; operations a second
counted by the client, its p50 and p99 (microseconds, every operation),
the server's CPUs used over the window, and the client's; median of ROUNDS
rounds, points in turn.

Gates, at 48 connections: at most 1.03x worse than Redis in the same
session, at least 1.01x better than v1.2.2 in the same session, and within
10 % of the recorded value; at 500 connections, at most 1.08x worse than
Redis. Every other row is shown, not gated. Two runs on a Standard_D8s_v5
measured the candidate at 1.014x and 1.023x worse than Redis at 48
connections and 1.030x and 1.034x better than v1.2.2; at 500, 1.004x and
1.060x worse than Redis, whose own samples there spread by 6 % (122-130k).
A reply that is not what was written fails the suite.
`min_cpus=8`: needs a Standard_D8s_v5 (`run-all.sh --min-cpus-only`).
Linux only (it pins CPUs and reads `/proc`).
"""
import array
import asyncio
import multiprocessing
import os
import random
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, THROUGHPUT, vs_redis, vs_v1_2_2, info, median, scaled  # noqa: E402
from nodes import Nodes  # noqa: E402

S = Suite("49-request-response", [
    Config("uring4", io="uring", threads=4, self_hosted=True),
], min_cpus=8)
KEYS = scaled(200_000)
VALUE = 1024
TTL_MS = 10 * 60_000
POINTS = (1, 4, 16, 48, 500)
WINDOW_S = 8.0
ROUNDS = 3
MAX_CLIENT_PROCS = 4
PREFIX = b"keyv:"
BUCKETS = 200_000  # 1 us buckets up to 200 ms
GATES = {
    48: [vs_redis(1.03), vs_v1_2_2(0.99), THROUGHPUT],
    500: [vs_redis(1.08), THROUGHPUT],
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


def value_for(i, version):
    return ("%d/%d" % (i, version)).encode().ljust(VALUE, b".")


def key_for(i):
    return PREFIX + str(i).encode()


def bulk(*args):
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)


def preload(c):
    """Every key at version 0, with a TTL, 500 SETs a pipeline."""
    for start in range(0, KEYS, 500):
        p = c.pipeline(transaction=False)
        for i in range(start, min(start + 500, KEYS)):
            p.set(key_for(i), value_for(i, 0), px=TTL_MS)
        if not all(p.execute()):
            raise RuntimeError("the server rejected the preload")


async def one_connection(port, first, count, deadline, hist, done):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    versions = [0] * count
    perf = time.perf_counter
    ttl = str(TTL_MS).encode()
    rounds = 0
    try:
        while perf() < deadline:
            offset = random.randrange(count)
            versions[offset] += 1
            value = value_for(first + offset, versions[offset])
            t0 = perf()
            writer.write(bulk(b"SET", key_for(first + offset), value, b"PX", ttl))
            if await reader.readline() != b"+OK\r\n":
                raise RuntimeError("the server rejected a write")
            us = int((perf() - t0) * 1e6)
            hist[us if us < BUCKETS else BUCKETS - 1] += 1
            for read in range(9):
                target = offset if read == 0 else random.randrange(count)
                t0 = perf()
                writer.write(bulk(b"GET", key_for(first + target)))
                head = await reader.readline()
                if head[:1] != b"$":
                    raise RuntimeError("unexpected reply %r" % head[:40])
                got = await reader.readexactly(int(head[1:-2]) + 2)
                us = int((perf() - t0) * 1e6)
                hist[us if us < BUCKETS else BUCKETS - 1] += 1
                if got[:-2] != value_for(first + target, versions[target]):
                    raise RuntimeError("the server returned an unexpected value")
            rounds += 1
    finally:
        writer.close()
    done.append(rounds)


def client_proc(port, conns, proc, procs, total, cpus, start_at, seconds, out):
    """One client CPU's share of the connections; puts (rounds, histogram, cpu s)."""
    try:
        os.sched_setaffinity(0, cpus)
        per = conns // procs

        async def main():
            hist = array.array("I", bytes(4 * BUCKETS))
            done = []
            await asyncio.sleep(max(0.0, start_at - time.time()))
            deadline = time.perf_counter() + seconds
            tasks = []
            for i in range(per):
                client = proc * per + i
                first = client * KEYS // total
                count = (client + 1) * KEYS // total - first
                tasks.append(one_connection(port, first, count, deadline, hist, done))
            await asyncio.gather(*tasks)
            return sum(done), hist

        t0 = time.process_time()
        rounds, hist = asyncio.run(main())
        out.put((rounds, hist.tobytes(), time.process_time() - t0, None))
    except Exception as e:  # reported by the parent
        out.put((0, b"", 0.0, repr(e)))


def percentile(hist, total, p):
    want, acc = total * p, 0
    for us, n in enumerate(hist):
        acc += n
        if acc >= want:
            return us
    return len(hist)


def one_point(port, server_pid, conns, client_cpus):
    procs = min(MAX_CLIENT_PROCS, conns, len(client_cpus))
    cpus = sorted(client_cpus)
    mpc = multiprocessing.get_context("fork")
    out = mpc.Queue()
    start_at = time.time() + 1.5
    ps = [mpc.Process(target=client_proc,
                                  args=(port, conns, i, procs, conns, {cpus[i % len(cpus)]}, start_at,
                                        WINDOW_S, out))
          for i in range(procs)]
    for p in ps:
        p.start()
    time.sleep(max(0.0, start_at - time.time()))
    s0, t0 = cpu_seconds(server_pid), time.perf_counter()
    results = [out.get(timeout=WINDOW_S + 120) for _ in ps]
    s1, t1 = cpu_seconds(server_pid), time.perf_counter()
    for p in ps:
        p.join(30)
    errors = [r[3] for r in results if r[3]]
    if errors:
        raise RuntimeError("client: %s" % errors[0])
    hist = [0] * BUCKETS
    for r in results:
        for i, n in enumerate(array.array("I", r[1])):
            if n:
                hist[i] += n
    ops = sum(hist)
    if ops == 0 or any(r[0] == 0 for r in results):
        raise RuntimeError("a client process did no work at %d connections" % conns)
    dt = max(WINDOW_S, t1 - t0)
    return {"ops": ops / WINDOW_S, "p50": percentile(hist, ops, 0.5), "p99": percentile(hist, ops, 0.99),
            "server_cpus": (s1 - s0) / dt, "client_cpus": sum(r[2] for r in results) / WINDOW_S}


@S.run
def run(ctx):
    if not hasattr(os, "sched_getaffinity"):
        raise RuntimeError("Linux only: the suite pins CPUs")
    server_cpus, client_cpus = cpu_split()
    ctx.raw["server_cpus"], ctx.raw["client_cpus"] = sorted(server_cpus), sorted(client_cpus)
    args = ["--io-threads", "4"] if ctx.kind == "redis" else []
    with Nodes(ctx) as ns:
        node = ns.start(args=args)
        pin(node.pid, server_cpus)
        c = node.client()
        rows = {n: [] for n in POINTS}
        for _ in range(ROUNDS):
            for conns in POINTS:
                c.flushall()
                preload(c)
                pin(node.pid, server_cpus)  # threads started since
                rows[conns].append(one_point(node.port, node.pid, conns, client_cpus))
        ctx.raw["rows"] = {str(n): r for n, r in rows.items()}
        for conns in POINTS:
            r = rows[conns]
            ops = [x["ops"] for x in r]
            ctx.emit("c%d_ops_per_s" % conns, median(ops), "ops/s", "higher",
                     gate=GATES.get(conns, info()), samples=ops)
            for k, unit in (("p50", "us"), ("p99", "us")):
                ctx.emit("c%d_%s_us" % (conns, k), median([x[k] for x in r]), unit, "lower", gate=info())
            ctx.emit("c%d_server_cpus" % conns, median([x["server_cpus"] for x in r]), "cpus", "lower",
                     gate=info())
            ctx.emit("c%d_client_cpus" % conns, median([x["client_cpus"] for x in r]), "cpus", "higher",
                     gate=info())


if __name__ == "__main__":
    sys.exit(S.main())
