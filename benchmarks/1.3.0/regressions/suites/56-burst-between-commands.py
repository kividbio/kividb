#!/usr/bin/env python3
"""Bursts between single commands: a connection keeps the read buffer it uses.

A client that sends a pipelined burst, then a few single commands, then a
burst again (a worker that flushes a batch of writes and then reads a few
keys one at a time), and one whose requests each arrive in several small
reads (a slow sender of 8 KiB values). A read buffer grown for a burst is
given back only after 16 reads in a row have not needed it, so both keep the
buffer from one burst or request to the next: the next burst is read in as
many reads as the first, rather than one more each time, and nothing is
grown and given back between them.

Configurations (each a fresh server, `--threads 2`; one connection):

  uring  `--io-model uring`
  tokio  `--io-model tokio`

Each runs, after 5 cycles of warm-up:

  burst  CYCLES cycles of one pipelined burst of 400 `SET k<i> <100 B>`
         (about 50 KiB, sent in one write), its replies read, then 4 `GET`s,
         each read before the next
  slow   SLOW requests, each `SET` of an 8 KiB value sent in 2 KiB pieces
         2 ms apart, its reply read before the next

  burst_reads      the server's reads a burst cycle (INFO
                   total_reads_processed), less the 4 `GET`s'
  burst_cpu_us     the server's CPU time a burst cycle (every thread's
                   schedstat), shown
  slow_cpu_us      the server's CPU time a slow request, shown

Gates (the candidate): `burst_reads` within 0.25 of recorded and at most
1.1x v1.2.2's (whose read buffer is 32 KiB from the start) in the same
session. v1.2.2 and Redis are shown.
"""
import os
import socket
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, rel, info, vs_v1_2_2, scaled  # noqa: E402

S = Suite("56-burst-between-commands", [
    Config("uring", io="uring", threads=2),
    Config("tokio", io="tokio", threads=2),
])
CYCLES = 2000
SLOW = 200
BURST = 400
SINGLES = 4


def enc(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        if isinstance(a, str):
            a = a.encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


class Conn:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=60)
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""

    def _fill(self):
        d = self.s.recv(1 << 20)
        if not d:
            raise RuntimeError("connection closed")
        self.buf += d

    def line(self):
        while b"\r\n" not in self.buf:
            self._fill()
        i = self.buf.index(b"\r\n")
        ln, self.buf = self.buf[:i], self.buf[i + 2:]
        return ln

    def reply(self):
        ln = self.line()
        if ln[:1] == b"$":
            n = int(ln[1:])
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                self._fill()
            v, self.buf = self.buf[:n], self.buf[n + 2:]
            return v
        if ln[:1] == b"-":
            raise RuntimeError(ln.decode())
        return ln

    def cmd(self, *args):
        self.s.sendall(enc(*args))
        return self.reply()


def server_cpu(pid):
    """Seconds on a CPU, every thread of the server (schedstat, ns)."""
    t = 0
    for tid in os.listdir("/proc/%d/task" % pid):
        try:
            with open("/proc/%d/task/%s/schedstat" % (pid, tid)) as f:
                t += int(f.read().split()[0])
        except OSError:
            pass
    return t / 1e9


@S.run
def run(ctx):
    cycles = scaled(CYCLES, step=100)
    slow = scaled(SLOW, step=20)
    ctl = ctx.client()
    c = Conn(ctx.port)
    value = b"b" * 100
    burst = b"".join(enc("SET", "k%d" % i, value) for i in range(BURST))

    def burst_cycle():
        c.s.sendall(burst)
        for _ in range(BURST):
            if c.reply() != b"+OK":
                raise RuntimeError("SET in a burst not OK")
        for i in range(SINGLES):
            if c.cmd("GET", "k%d" % i) != value:
                raise RuntimeError("GET k%d" % i)

    slow_request = enc("SET", "slow", b"s" * 8192)

    def slow_cycle():
        for i in range(0, len(slow_request), 2048):
            c.s.sendall(slow_request[i:i + 2048])
            time.sleep(0.002)
        if c.reply() != b"+OK":
            raise RuntimeError("slow SET not OK")

    def reads():
        return int(ctl.info("stats")["total_reads_processed"])

    for _ in range(5):
        burst_cycle()
    r0, t0 = reads(), server_cpu(ctx.pid)
    for _ in range(cycles):
        burst_cycle()
    # The INFO that reads the count is one read of its own connection.
    r1, t1 = reads() - 1, server_cpu(ctx.pid)
    burst_reads = (r1 - r0) / float(cycles) - SINGLES
    burst_cpu = (t1 - t0) * 1e6 / cycles

    for _ in range(5):
        slow_cycle()
    t2 = server_cpu(ctx.pid)
    for _ in range(slow):
        slow_cycle()
    slow_cpu = (server_cpu(ctx.pid) - t2) * 1e6 / slow

    ctx.raw["shape"] = {"cycles": cycles, "slow": slow, "burst_bytes": len(burst),
                        "reads_per_cycle": (r1 - r0) / float(cycles)}
    ctx.emit("burst_reads", burst_reads, "reads", "lower",
             gate=[rel(0, abs=0.25), vs_v1_2_2(1.1)])
    ctx.emit("burst_cpu_us", burst_cpu, "us", "lower", gate=info())
    ctx.emit("slow_cpu_us", slow_cpu, "us", "lower", gate=info())


if __name__ == "__main__":
    sys.exit(S.main())
