#!/usr/bin/env python3
"""#759: set algebra of a big set with a small one costs the small set's size.

Dataset (the issue's): `big` holds the integers 0..N-1 and `small` 4,096
integers sampled from 0..2N-1 with random.Random(7), so about half of them
are in `big`. N is 1M and 5M in the default configuration (`big` grows from
1M to 5M between the two) and 20M in `n20m` (FULL=1).

Commands, each run once to warm up and then 5 times (median):

  sinter_big_small     SINTER big small
  sinter_small_big     SINTER small big
  sintercard           SINTERCARD 2 big small
  sintercard_limit     SINTERCARD 2 big small LIMIT 100
  sinterstore          SINTERSTORE dst big small
  sdiff_small_big      SDIFF small big
  sdiffstore_big_small SDIFFSTORE dst big small   (walks all of big: a cost
                       guard, rel 25 %, not an improvement)

The `sunion` configuration (Tokio, N = 1M) runs `SUNION big small` 3 times
while two writer processes each send `SADD small <a member it has>` back to
back, and records the slowest SADD that overlapped a SUNION. A union reads
one set at a time, and past 65,536 members its walk runs off the Tokio
worker (`long_work`), so a writer of the small set waits neither for the walk
of the big one nor behind it on the worker: the candidate's worst SADD must
stay under 20 ms. Redis runs everything on one thread, so its worst SADD is
the whole SUNION; it and v1.2.2 are shown, not gated.

v1.2.2 copied every set out of the store before intersecting, so its time
grew with N (1.9 s at 20M against Redis's 3.7 ms in the issue); the
candidate walks the smallest set and looks its members up in the others. The
replies (counts) must equal the recorded ones exactly, on every engine.

Gates: the times are a few milliseconds or less, most of it the round trip
and the client parsing the reply, so the `rel` gate is 50 % or 2 ms,
whichever is larger (v1.2.2 took 80 ms and more at 1M, so the old cost cannot
come back unnoticed). The commands that reply with the members (SINTER,
SDIFF: about 2 ms on every engine) must also keep 60 % of the gain over
v1.2.2 and stay within 2x of Redis, both measured in the same session. The
count-only ones (SINTERCARD, SINTERSTORE) take 0.05-0.3 ms, where a ratio is
mostly the noise of the round trip, so they have only the `rel` gate.
"""
import multiprocessing
import os
import random
import socket
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (Suite, Config, KEEP_IMPROVEMENT, EXACT, rel, bound, vs_redis, median, scaled)  # noqa: E402

RUNS = 5
S = Suite("14-sinter-big-small", [
    Config("default"),
    Config("n20m", full_only=True),
    Config("sunion", io="tokio"),
])
SIZES = {"default": (1_000_000, 5_000_000), "n20m": (20_000_000,)}
GATES = [rel(50, abs=2), KEEP_IMPROVEMENT, vs_redis(2)]  # replies of ~2,000 members
SMALL_GATES = [rel(50, abs=2)]                         # count-only replies, sub-ms


def grow(c, frm, to):
    """SADD big frm..to-1, 1,000 members per command."""
    p = c.pipeline(transaction=False)
    for i in range(frm, to, 1000):
        p.sadd("big", *range(i, min(i + 1000, to)))
        if i % 200_000 == 0:
            p.execute()
    p.execute()


def timed(c, args):
    """(median ms, size of the reply) over RUNS runs after one warm-up."""
    ms, n = [], None
    for i in range(RUNS + 1):
        t = time.perf_counter()
        r = c.execute_command(*args)
        took = (time.perf_counter() - t) * 1000
        n = r if isinstance(r, int) else len(r)
        if i:
            ms.append(took)
    return median(ms), ms, n


CASES = [
    ("sinter_big_small", ("SINTER", "big", "{small}"), GATES),
    ("sinter_small_big", ("SINTER", "{small}", "big"), GATES),
    ("sintercard", ("SINTERCARD", "2", "big", "{small}"), SMALL_GATES),
    ("sintercard_limit", ("SINTERCARD", "2", "big", "{small}", "LIMIT", "100"), SMALL_GATES),
    ("sinterstore", ("SINTERSTORE", "dst", "big", "{small}"), SMALL_GATES),
    ("sdiff_small_big", ("SDIFF", "{small}", "big"), GATES),
    ("sdiffstore_big_small", ("SDIFFSTORE", "dst", "big", "{small}"), [rel(25)]),
]


def _writer(port, key, member, ready, stop, q):
    """SADD key member back to back on a raw socket; (t_sent, seconds) each."""
    s = socket.create_connection(("127.0.0.1", port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    cmd = b"*3\r\n$4\r\nSADD\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n" % (len(key), key, len(member), member)
    out = []

    def sadd():
        t0 = time.perf_counter()
        s.sendall(cmd)
        buf = b""
        while not buf.endswith(b"\r\n"):
            d = s.recv(64)
            if not d:
                raise EOFError("server closed the writer connection")
            buf += d
        return t0, time.perf_counter() - t0

    sadd()
    ready.release()
    while not stop.is_set():
        out.append(sadd())
        time.sleep(0.0005)
    s.close()
    q.put(out)


def run_sunion(ctx, c):
    """SUNION big small beside two SADD-to-small writers (the `sunion` config)."""
    n = scaled(1_000_000)
    grow(c, 0, n)
    members = random.Random(7).sample(range(2 * n), 4096)
    c.sadd("small", *members)
    mp_ctx = multiprocessing.get_context("fork")
    ready, stop, q = mp_ctx.Semaphore(0), mp_ctx.Event(), mp_ctx.Queue()
    procs = [mp_ctx.Process(target=_writer, daemon=True,
                            args=(ctx.port, b"small", str(members[0]).encode(), ready, stop, q))
             for _ in range(2)]
    for p in procs:
        p.start()
    for _ in procs:
        if not ready.acquire(timeout=30):
            raise RuntimeError("a writer did not start")
    windows, ms, size = [], [], None
    try:
        time.sleep(0.2)
        for _ in range(3):
            t0 = time.perf_counter()
            size = len(c.execute_command("SUNION", "big", "small"))
            t1 = time.perf_counter()
            windows.append((t0, t1))
            ms.append((t1 - t0) * 1000)
            time.sleep(0.2)
    finally:
        stop.set()
        samples = [q.get(timeout=60) for _ in procs]
        for p in procs:
            p.join(timeout=10)
    worst = []
    for t0, t1 in windows:
        worst.append(max((rtt for one in samples for (ts, rtt) in one if ts < t1 and ts + rtt > t0),
                         default=0.0) * 1000)
    ctx.raw["sunion"] = {"ms": ms, "worst_sadd_ms": worst, "reply": size,
                         "sadds": sum(len(one) for one in samples)}
    ctx.emit("sunion_ms", median(ms), "ms", "lower", gate=[rel(25)], samples=ms)
    ctx.emit("sunion_reply", size, "members", "higher", gate=EXACT)
    ctx.emit("sunion_worst_sadd_ms", max(worst), "ms", "lower", gate=[bound(max=20)], samples=worst)


@S.run
def run(ctx):
    c = ctx.client()
    c.flushall()
    if ctx.config.name == "sunion":
        return run_sunion(ctx, c)
    have = 0
    ctx.raw["cases"] = {}
    for n in SIZES[ctx.config.name]:
        n = scaled(n)
        t0 = time.perf_counter()
        grow(c, have, n)
        have = n
        small = "small%d" % n
        c.sadd(small, *random.Random(7).sample(range(2 * n), 4096))
        ctx.raw["load_s_%d" % n] = round(time.perf_counter() - t0, 1)
        tag = "%dm" % (n // 1_000_000) if n >= 1_000_000 else str(n)
        ctx.emit("%s_big_card" % tag, c.scard("big"), "members", "higher", gate=EXACT)
        for name, args, gates in CASES:
            args = [a.format(small=small) for a in args]
            med, ms, size = timed(c, args)
            ctx.raw["cases"]["%s_%s" % (tag, name)] = {"ms": ms, "reply": size}
            ctx.emit("%s_%s_ms" % (tag, name), med, "ms", "lower", gate=gates, samples=ms)
            ctx.emit("%s_%s_reply" % (tag, name), size, "members", "higher", gate=EXACT)


if __name__ == "__main__":
    sys.exit(S.main())
