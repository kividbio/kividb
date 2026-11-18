#!/usr/bin/env python3
"""#851: PUBLISH fanned out to pattern subscribers (socket.io's Redis adapter).

socket.io's Redis adapter broadcasts with `PUBLISH socket.io#/#<room>#` and
every Node server `PSUBSCRIBE`s `socket.io#/#*`. 1.2.2 delivered about 10 %
fewer of those messages a second than Redis 8.10: each subscriber framed
every message again, and its forwarder reallocated its batch and its frame
buffer for each batch, which on glibc took the arena lock the publishing
thread was allocating under.

One server per configuration: KiviDB with `--threads 2`, Redis with its
defaults. The clients (redis-benchmark, 4 reader processes and the probe)
share the VM's 4 vCPUs with the server; with its default of one worker a
CPU, KiviDB's 4 workers and the clients take turns on them and the rows
measure the scheduler (master: 1.15M delivered a second and a 4.3 ms p99
with 4 workers, against Redis's 1.35M and 0.9 ms in the same session).
16 subscriber connections, each `PSUBSCRIBE socket.io#/#*` (`pattern`) or
`SUBSCRIBE` one channel (`channel`, the plain fan-out guard), read by 4
processes that count bytes (every frame has the same length, so bytes are
messages). `redis-benchmark -c 48` publishes `PUBLISH:` messages of 200 bytes
to `socket.io#/#room<12 digits>#` (`-r 10000`) for the pattern rows, to the
one channel for the channel rows.

Metrics (median of 3 rounds of `PUBLISHES` publishes each):

  delivered_per_s   messages delivered to the 16 subscribers a second, from
                    the first publish until the last subscriber has every
                    message (Redis's PUBLISH returns before the message is
                    written, so its publish rate alone says nothing)
  published_per_s   redis-benchmark's rate (shown)
  delivery_p99_ms   p99 of a probe's own messages, published every 2 ms on
                    a channel of the same length that the probe also
                    `SUBSCRIBE`s on another connection, publish to receipt
  lost              messages a subscriber missed: 0, exactly

Gates: delivered_per_s 10 % and at most 1.1x worse than Redis this session
(the issue's gap); delivery_p99_ms is scheduling on a 4-vCPU VM shared with
the clients: jitter, 50 % or 1 ms.
"""
import multiprocessing as mp
import os
import re
import selectors
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (Suite, Config, THROUGHPUT, EXACT, rel, vs_redis, jitter, info, median, scaled)  # noqa: E402
from nodes import redis_tool  # noqa: E402
from probe import percentile  # noqa: E402

S = Suite("23-pubsub-pattern-fanout", [Config("pattern", threads=2), Config("channel", threads=2)])
SUBSCRIBERS = 16
READERS = 4
PUBLISHERS = 48
PUBLISHES = 600_000
ROUNDS = 3
PAYLOAD = 200
PATTERN = b"socket.io#/#*"
CHANNEL = b"socket.io#/#room000000000000#"
PROBE_CHANNEL = b"socket.io#/#probe00000000000#"   # as long as a room channel


def command(*args):
    return b"*%d\r\n" % len(args) + b"".join(bulk(a) for a in args)


def bulk(b):
    return b"$%d\r\n%s\r\n" % (len(b), b)


def frame_len(kind):
    """The length of one delivered frame: every channel has the same length."""
    if kind == "pattern":
        frame = b"*4\r\n" + bulk(b"pmessage") + bulk(PATTERN)
    else:
        frame = b"*3\r\n" + bulk(b"message")
    return len(frame + bulk(CHANNEL) + bulk(b"x" * PAYLOAD))


def subscribe(port, kind, n):
    socks = []
    for _ in range(n):
        s = socket.create_connection(("127.0.0.1", port))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 << 20)
        s.sendall(command(b"PSUBSCRIBE", PATTERN) if kind == "pattern" else command(b"SUBSCRIBE", CHANNEL))
        ack = b""
        while not ack.endswith(b":1\r\n"):
            ack += s.recv(4096)
        socks.append(s)
    return socks


def _reader(port, kind, n, load_bytes, ready, done_q):
    """Count bytes on n subscriber connections: when each has had load_bytes
    (the load's messages), and then all that comes until they are quiet for
    half a second (the probe's messages, for a pattern subscriber)."""
    socks = subscribe(port, kind, n)
    sel = selectors.DefaultSelector()
    got = {}
    for s in socks:
        sel.register(s, selectors.EVENT_READ)
        got[s] = 0
    ready.set()
    buf = bytearray(1 << 20)
    reached = 0.0
    deadline = time.perf_counter() + 600
    while time.perf_counter() < deadline:
        short = len([s for s in socks if got[s] < load_bytes])
        events = sel.select(timeout=1.0 if short else 0.5)
        if not events and not short:
            break
        for key, _ in events:
            s = key.fileobj
            n_read = s.recv_into(buf)
            if not n_read:
                sel.unregister(s)
                continue
            before = got[s]
            got[s] += n_read
            if before < load_bytes <= got[s]:
                reached = max(reached, time.perf_counter())
    done_q.put((reached, sum(got.values())))
    for s in socks:
        s.close()


def _probe(port, stop, out_q):
    """Publish a timestamped message every 2 ms; time its arrival on a
    connection subscribed to the probe channel."""
    sub = socket.create_connection(("127.0.0.1", port))
    sub.sendall(command(b"SUBSCRIBE", PROBE_CHANNEL))
    ack = b""
    while not ack.endswith(b":1\r\n"):
        ack += sub.recv(4096)
    pub = socket.create_connection(("127.0.0.1", port))
    pub.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sub.settimeout(5)
    pub.settimeout(5)
    rtts, pending = [], b""
    while not stop.is_set():
        sent = time.perf_counter()
        payload = (b"%020d" % time.perf_counter_ns()).ljust(PAYLOAD, b"p")
        pub.sendall(command(b"PUBLISH", PROBE_CHANNEL, payload))
        reply = b""
        while not reply.endswith(b"\r\n"):
            reply += pub.recv(64)
        while payload not in pending:
            pending += sub.recv(1 << 16)
        rtts.append((sent, time.perf_counter() - sent))
        pending = pending[pending.index(payload) + PAYLOAD:]
        time.sleep(max(0.0, 0.002 - (time.perf_counter() - sent)))
    out_q.put(rtts)


def one_round(ctx, kind, publishes, probe_runs):
    mpc = mp.get_context("fork")
    # What each subscriber receives: the load, and for the pattern
    # subscribers the probe's messages too (the probe channel matches the
    # pattern), whose count is only known at the end.
    flen = frame_len(kind)
    expected = publishes * flen
    ready = [mpc.Event() for _ in range(READERS)]
    done_q = mpc.Queue()
    readers = [mpc.Process(target=_reader, daemon=True,
                           args=(ctx.port, kind, SUBSCRIBERS // READERS, expected, ready[i], done_q))
               for i in range(READERS)]
    for r in readers:
        r.start()
    for e in ready:
        e.wait(60)
    stop, probe_q = mpc.Event(), mpc.Queue()
    probe = mpc.Process(target=_probe, args=(ctx.port, stop, probe_q), daemon=True) if probe_runs else None
    channel = (b"socket.io#/#room__rand_int__#" if kind == "pattern" else CHANNEL).decode()
    cmd = [redis_tool("redis-benchmark"), "-p", str(ctx.port), "-c", str(PUBLISHERS), "-n", str(publishes),
           "-r", "10000", "--csv", "PUBLISH", channel, "x" * PAYLOAD]
    t0 = time.perf_counter()
    if probe:
        probe.start()
    bench = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    stop.set()
    rtts = probe_q.get(timeout=60) if probe else []
    if probe:
        probe.join(timeout=30)
    reached, received = [], 0
    for _ in readers:
        when, got = done_q.get(timeout=660)
        reached.append(when)
        received += got
    for r in readers:
        r.join(timeout=30)
    m = re.search(r'"PUBLISH[^"]*","([\d.]+)"', bench.stdout)
    published = float(m.group(1)) if m else 0.0
    # The probe's messages were delivered to the pattern subscribers too.
    extra = len(rtts) if kind == "pattern" else 0
    sent = SUBSCRIBERS * (publishes + extra)
    lost = max(0, sent * flen - received) // flen
    if min(reached) == 0.0:   # a subscriber never had the whole load
        return 0.0, published, rtts, lost
    return (sent - lost) / (max(reached) - t0), published, rtts, lost


@S.run
def run(ctx):
    kind = ctx.config.name
    publishes = scaled(PUBLISHES, step=1000)
    rates, pubs, rtts_all, lost = [], [], [], 0
    one_round(ctx, kind, scaled(50_000, step=1000), probe_runs=False)  # warm-up
    for _ in range(ROUNDS):
        rate, published, rtts, l = one_round(ctx, kind, publishes, probe_runs=True)
        rates.append(rate)
        pubs.append(published)
        rtts_all.extend(r for _, r in rtts)
        lost += l
    ctx.raw["probe_messages"] = len(rtts_all)
    ctx.emit("delivered_per_s", median(rates), "msg/s", "higher",
             gate=[THROUGHPUT, vs_redis(1.1)], samples=rates)
    ctx.emit("published_per_s", median(pubs), "req/s", "higher", gate=info(), samples=pubs)
    ctx.emit("delivery_p99_ms", percentile(rtts_all, 0.99) * 1e3, "ms", "lower", gate=jitter(rel(50, abs=1)))
    ctx.emit("lost", lost, "messages", "lower", gate=EXACT)


if __name__ == "__main__":
    sys.exit(S.main())
