#!/usr/bin/env python3
"""#758: BRPOP across many keys with many blocked clients.

The job-queue shape (Sidekiq, Resque, Celery): every worker blocks in
`BRPOP <all 270 queues, in a random order> 2`, and a producer pushes jobs to
random queues. Configurations:

  c1000   1,000 workers x 270 queues
  c4000   4,000 workers x 270 queues
  c1000x1 1,000 workers x 1 queue (a guard: one key per BRPOP was never slow)

The workers are raw sockets in WORKER_PROCS processes (selectors, not
redis-py: 4,000 redis-py connections in one interpreter measure the client).
Each sends its BRPOP again as soon as it is answered, a job or a timeout.
After WARMUP seconds the producer (redis-py, this process) sends 200 `LPUSH
queue:<random> job` in one pipeline every 0.1 s for MEASURE seconds, as in
the issue: at most 2,000 jobs/s are offered, fewer when the server is slow
to answer the pipeline. A PING probe (lib/probe.py) runs throughout.

Metrics:

  jobs_per_s       jobs the workers received during the MEASURE window / MEASURE
  ping_p99_ms      the probe's p99 PING during the window (shown, not gated)
  ping_max_ms      its worst PING: max(1.5x recorded, Redis + 10 ms), the
                   suites' WORST_PING (`jitter()`: scheduling, not CPU speed,
                   so never host-scaled); v1.2.2's was 1-7 s
  lost             jobs pushed - jobs received - jobs still in the queues, once
                   the producer has stopped and the queues have drained (up
                   to DRAIN s): must be 0 on every engine (a job popped for a
                   client that timed out at the same moment and never sent is
                   lost; a job sent twice makes it negative)
  blocked_after    `INFO clients` blocked_clients 2 s after every worker has
                   disconnected (1.2.2 left hundreds: #758)

Gates: jobs_per_s stays within 10 % of the recorded value and at most 1.5x
worse than Redis 8.10 measured in the same session; c1000 also keeps 60 % of
its gain over v1.2.2. c4000 has no ratio gate: v1.2.2 delivers 8-20 jobs/s
there, too noisy a denominator; c1000x1 has only the 10 % (a guard, not an
improvement). None is scaled by the host's speed (`jitter()`): the rate is
capped by what the producer offers. lost and blocked_after are exact (0).

The offered rate is the cap: an engine that keeps up delivers about 1,900
jobs/s whatever the client count, so the gain over v1.2.2 is bounded by it.
"""
import os
import random
import selectors
import socket
import sys
import time
import multiprocessing as mp

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (Suite, Config, KEEP_IMPROVEMENT, THROUGHPUT, WORST_PING, bound, info, jitter, vs_redis, scaled)  # noqa: E402
from probe import Probe  # noqa: E402

WARMUP = 8.0
MEASURE = 20.0
DRAIN = 10.0
WORKER_PROCS = 2
BATCH = 200
EVERY = 0.1

S = Suite("15-brpop-many-keys", [
    Config("c1000"),
    Config("c4000"),
    Config("c1000x1"),
])
SHAPES = {"c1000": (1000, 270), "c4000": (4000, 270), "c1000x1": (1000, 1)}


def _bulk(b):
    return b"$%d\r\n%s\r\n" % (len(b), b)


def _brpop(keys):
    return b"*%d\r\n" % (len(keys) + 2) + _bulk(b"BRPOP") + b"".join(_bulk(k) for k in keys) + _bulk(b"2")


def _reply(buf, at):
    """End of one BRPOP reply starting at `at` and whether it was a job, or None."""
    if buf.startswith(b"*-1\r\n", at):
        return at + 5, False
    if not buf.startswith(b"*2\r\n", at):
        if len(buf) - at >= 4:
            raise RuntimeError("unexpected reply %r" % buf[at:at + 64])
        return None
    pos = at + 4
    for _ in range(2):
        nl = buf.find(b"\r\n", pos)
        if nl < 0:
            return None
        n = int(buf[pos + 1:nl])
        pos = nl + 2 + n + 2
        if pos > len(buf):
            return None
    return pos, True


def _workers(port, n, nkeys, seed, ready, stop, q):
    rnd = random.Random(seed)
    keys = [b"queue:%d" % i for i in range(nkeys)]
    cmds = []
    for _ in range(16):
        rnd.shuffle(keys)
        cmds.append(_brpop(keys))
    sel = selectors.DefaultSelector()
    bufs = {}
    for i in range(n):
        s = socket.create_connection(("127.0.0.1", port))
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.sendall(cmds[i % len(cmds)])
        s.setblocking(False)
        sel.register(s, selectors.EVENT_READ)
        bufs[s] = b""
    ready.release()
    got, timeouts, k = [], 0, 0
    while not stop.is_set():
        for key, _ in sel.select(0.05):
            s = key.fileobj
            try:
                d = s.recv(65536)
            except BlockingIOError:
                continue
            if not d:
                raise EOFError("server closed a worker connection")
            buf = bufs[s] + d
            at = 0
            while True:
                r = _reply(buf, at)
                if r is None:
                    break
                at, job = r
                if job:
                    got.append(time.perf_counter())
                else:
                    timeouts += 1
                k += 1
                s.setblocking(True)
                s.sendall(cmds[k % len(cmds)])
                s.setblocking(False)
            bufs[s] = buf[at:]
    for s in bufs:
        s.close()
    q.put((got, timeouts))


@S.run
def run(ctx):
    clients, nkeys = SHAPES[ctx.config.name]
    clients = scaled(clients, step=10)
    queues = ["queue:%d" % i for i in range(nkeys)]
    c = ctx.client()
    c.delete(*queues)
    ctx_mp = mp.get_context("fork")
    ready, stop, q = ctx_mp.Semaphore(0), ctx_mp.Event(), ctx_mp.Queue()
    per = clients // WORKER_PROCS
    procs = [ctx_mp.Process(target=_workers, daemon=True,
                            args=(ctx.port, per, nkeys, 7 + i, ready, stop, q))
             for i in range(WORKER_PROCS)]
    rnd = random.Random(7)
    pushed = 0
    with Probe(ctx.port, n=1) as pr:
        for p in procs:
            p.start()
        for _ in procs:
            if not ready.acquire(timeout=120):
                raise RuntimeError("the workers did not connect")
        time.sleep(WARMUP)
        p = c.pipeline(transaction=False)
        t0 = time.perf_counter()
        while time.perf_counter() - t0 < MEASURE:
            for _ in range(BATCH):
                p.lpush(rnd.choice(queues), "job")
            p.execute()
            pushed += BATCH
            time.sleep(EVERY)
        t1 = time.perf_counter()
        drained_at = None
        while time.perf_counter() - t1 < DRAIN:
            if sum(_llens(c, queues)) == 0:
                drained_at = round(time.perf_counter() - t1, 2)
                break
            time.sleep(0.2)
        time.sleep(2.0)  # replies already sent reach the workers
        stop.set()
        results = [q.get(timeout=120) for _ in procs]
        for p_ in procs:
            p_.join(timeout=30)
    got = sorted(t for r, _ in results for t in r)
    timeouts = sum(t for _, t in results)
    in_window = sum(1 for t in got if t0 <= t <= t1)
    left = sum(_llens(c, queues))
    time.sleep(2.0)
    blocked = int(c.info("clients").get("blocked_clients", 0))
    lost = pushed - len(got) - left
    pings = sorted(r for out in pr.samples for ts, r in out if t0 <= ts <= t1)
    p99 = pings[min(len(pings) - 1, int(len(pings) * 0.99))] * 1000 if pings else None
    ctx.raw.update({"clients": clients, "keys": nkeys, "pushed": pushed, "received": len(got),
                    "left": left, "timeouts": timeouts, "window_s": round(t1 - t0, 3),
                    "drained_after_s": drained_at, "pings": len(pings), "ping_top": pr.top(10)})
    ctx.log("pushed %d, received %d (%d in window), left %d, timeouts %d, drained after %s s"
            % (pushed, len(got), in_window, left, timeouts, drained_at))
    # Not scaled by the host's speed (`jitter()`): the rate is capped by
    # what the producer offers, not by the CPU, and v1.2.2's c4000 rate
    # (8-20 jobs/s) is too noisy to be a speed factor or a ratio's
    # denominator.
    gate = {"c1000": [THROUGHPUT, KEEP_IMPROVEMENT, vs_redis(1.5)],
            "c4000": [THROUGHPUT, vs_redis(1.5)],
            "c1000x1": [THROUGHPUT]}[ctx.config.name]
    gate = jitter(gate)
    ctx.emit("jobs_per_s", in_window / (t1 - t0), "jobs/s", "higher", gate=gate)
    ctx.emit("ping_p99_ms", p99, "ms", "lower", gate=jitter(info()))
    ctx.emit("ping_max_ms", pr.worst(t0, t1), "ms", "lower", gate=jitter(WORST_PING))
    ctx.emit("lost", lost, "jobs", "lower", gate=bound(eq=0))
    ctx.emit("blocked_after", blocked, "clients", "lower", gate=bound(eq=0))


def _llens(c, queues):
    p = c.pipeline(transaction=False)
    for k in queues:
        p.llen(k)
    return p.execute()


if __name__ == "__main__":
    sys.exit(S.main())
