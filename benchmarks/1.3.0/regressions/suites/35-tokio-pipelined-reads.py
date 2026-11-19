#!/usr/bin/env python3
"""Pipelined writes of kilobyte values on the Tokio loop, against v1.2.2.

The Tokio loop (macOS, Windows, and Linux with `--io-model tokio` or
without io_uring) read a client 4 KiB at a time, through a stack array,
into its query buffer: a pipeline of 1 KiB `SET`s took a read a few
commands. It now reads into the query buffer itself, 4 KiB at first and
up to 32 KiB once a read fills its room, and gives the room back when the
client's requests are small or it goes quiet (#432, #891). The reads it
makes while its replies wait, and the turns that run what they read, are
32 KiB too: at 4 KiB a client with a small receive buffer that sent a long
pipeline of small commands before reading a reply was answered a few
hundred bytes at a time, and it and the server waited for each other for
good.

One configuration, `tokio`: `--io-model tokio --threads 1`, the server
pinned to the first CPU this process may use, `redis-benchmark` on the next
three (`--threads 3`), 50 connections, keys `key:<0..99999>` (`-r 100000`).
Rows:

- `set_1k_p16_per_s`: `SET` of 1,024-byte values, pipeline 16;
- `set_4k_p32_per_s`: `SET` of 4,096-byte values, pipeline 32.

Each runs 2 s of warm-up, then the server's own `total_commands_processed`
is read over a 6 s window; median of 3 rounds. Gates: 10 % of the recorded
value (`THROUGHPUT`, scaled by the host's speed) and 60 % of the gain over
v1.2.2 measured in the same session (`KEEP_IMPROVEMENT`; recorded about
1.8x and 2.2x).

- `small_rcvbuf_pipeline_ok`: one connection with a 64 KiB receive buffer
  (`SO_RCVBUF`, set after connecting, as a client library would) sends
  400,000 `SET`s of 20-byte values (19 MB) in one write and reads the
  replies only once it has written it all. 1 if the write finishes within
  20 s and all 400,000 `+OK` replies arrive, 0 otherwise. Gate: at least 1
  (`bound`); v1.2.2 stalls (0). `small_rcvbuf_pipeline_s`, the seconds the
  write took (until it was given up, on a stall), is shown beside it.
"""
import os
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, THROUGHPUT, KEEP_IMPROVEMENT, bound, info, median, scaled  # noqa: E402
from nodes import redis_tool  # noqa: E402

ENGINES = ("v1_2_2", "candidate")
S = Suite("35-tokio-pipelined-reads", [
    Config("tokio", io="tokio", threads=1, engines=ENGINES),
], min_cpus=4)
CONNS = 50
KEYS = 100000
ROUNDS = 3
WARM_S = 2.0
WINDOW_S = 6.0
CLIENT_CPUS = 3
ROWS = [
    ("set_1k_p16", 1024, 16),
    ("set_4k_p32", 4096, 32),
]
GATE = [THROUGHPUT, KEEP_IMPROVEMENT]
STALL_SETS = 400000
STALL_RCVBUF = 64 * 1024
STALL_TIMEOUT_S = 20.0


def pin(pid, cpus):
    """Every thread of pid onto cpus."""
    for tid in os.listdir("/proc/%d/task" % pid):
        os.sched_setaffinity(int(tid), cpus)


def processed(c):
    return int(c.info("stats")["total_commands_processed"])


def one_row(ctx, c, size, pipeline, client_cpus):
    """`SET`s a second over the window, counted by the server."""
    cmd = [redis_tool("redis-benchmark"), "-p", str(ctx.port), "-c", str(CONNS), "-n", "2000000000",
           "-r", str(KEYS), "-P", str(pipeline), "-d", str(size), "-t", "set",
           "--threads", str(len(client_cpus)), "-q"]
    bench = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                             preexec_fn=lambda: os.sched_setaffinity(0, client_cpus))
    try:
        time.sleep(WARM_S)
        n0, t0 = processed(c), time.perf_counter()
        time.sleep(WINDOW_S)
        n1, t1 = processed(c), time.perf_counter()
    finally:
        bench.terminate()
        bench.wait(timeout=30)
    return (n1 - n0) / (t1 - t0)


def small_rcvbuf_pipeline(ctx, sets):
    """(ok, seconds the write took): see the docstring."""
    pipeline = b"".join(
        b"*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$20\r\n%s\r\n" % (len(k), k, b"v" * 20)
        for k in (b"pk:%d" % (i % 1000) for i in range(sets)))
    s = socket.create_connection(("127.0.0.1", ctx.port))
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, STALL_RCVBUF)
        s.settimeout(STALL_TIMEOUT_S)
        t0 = time.perf_counter()
        try:
            s.sendall(pipeline)
        except socket.timeout:
            ctx.log("the pipeline stalled: not written within %.0f s" % STALL_TIMEOUT_S)
            return 0, time.perf_counter() - t0
        took = time.perf_counter() - t0
        want, got = 5 * sets, 0
        try:
            while got < want:
                b = s.recv(1 << 20)
                if not b:
                    break
                got += len(b)
        except socket.timeout:
            pass
        if got != want:
            ctx.log("replies: %d bytes of %d" % (got, want))
            return 0, took
        return 1, took
    finally:
        s.close()


@S.run
def run(ctx):
    cpus = sorted(os.sched_getaffinity(0))
    if len(cpus) < 1 + CLIENT_CPUS:
        ctx.log("only %d CPUs: nothing measured" % len(cpus))
        ctx.emit("skipped_cpus", len(cpus), "cpus", "higher", gate=info())
        return
    server_cpus = {cpus[0]}
    client_cpus = set(cpus[1:1 + CLIENT_CPUS])
    pin(ctx.pid, server_cpus)
    ctx.raw["server_cpus"], ctx.raw["client_cpus"] = sorted(server_cpus), sorted(client_cpus)
    c = ctx.client()
    c.flushall()
    rates = {name: [] for name, _, _ in ROWS}
    for _ in range(ROUNDS):
        for name, size, pipeline in ROWS:
            rates[name].append(one_row(ctx, c, size, pipeline, client_cpus))
    for name, _, _ in ROWS:
        ctx.emit("%s_per_s" % name, median(rates[name]), "req/s", "higher", gate=GATE,
                 samples=rates[name])
    c.flushall()
    ok, took = small_rcvbuf_pipeline(ctx, scaled(STALL_SETS))
    ctx.emit("small_rcvbuf_pipeline_ok", ok, "flag", "higher", gate=bound(min=1))
    ctx.emit("small_rcvbuf_pipeline_s", took, "s", "lower", gate=info())


if __name__ == "__main__":
    sys.exit(S.main())
