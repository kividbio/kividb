"""PING probes: N connections, each sending PING every 1 ms.

Each probe is its own process with a raw socket, so the probes neither share
Python's GIL with the workload nor with each other. Timestamps are
time.perf_counter() (CLOCK_MONOTONIC on Linux), so they can be lined up with
the workload's own timestamps in another process.

    with Probe(port, n=4) as pr:          # connected and pinging on return
        t0 = time.perf_counter(); run(); t1 = time.perf_counter()
    pr.worst(t0, t1)       # longest PING that overlapped [t0, t1], over all probes
    pr.worst_each(t0, t1)  # the same per probe
    pr.p99()               # p99 of every PING of every probe
    pr.max()               # longest PING of the whole run

A PING that was sent late (the probe process was not scheduled) still counts
from when it was sent, so the numbers are what a client would see.
"""
import multiprocessing as mp
import socket
import time


def _probe(port, interval, ready, stop, q, host):
    s = socket.create_connection((host, port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    out = []

    def ping():
        t0 = time.perf_counter()
        s.sendall(b"PING\r\n")
        buf = b""
        while not buf.endswith(b"\r\n"):
            d = s.recv(64)
            if not d:
                raise EOFError("server closed the probe connection")
            buf += d
        return t0, time.perf_counter() - t0

    ping()  # warm up: the connection is accepted and served before we report ready
    ready.release()
    nxt = time.perf_counter()
    while not stop.is_set():
        now = time.perf_counter()
        if now < nxt:
            time.sleep(nxt - now)
        t0, rtt = ping()
        out.append((t0, rtt))
        nxt = t0 + interval
    s.close()
    q.put(out)


def percentile(xs, p):
    """p in [0, 1]; nearest-rank on the sorted values; nan when empty."""
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))]


class Probe:
    def __init__(self, port, n=1, interval=0.001, host="127.0.0.1", timeout=30.0):
        self.port, self.n, self.interval, self.host, self.timeout = port, n, interval, host, timeout
        self.samples = []  # one list of (t_sent, rtt_seconds) per probe
        self._procs = []

    def start(self):
        ctx = mp.get_context("fork")
        self._ready = ctx.Semaphore(0)
        self._stop = ctx.Event()
        self._q = ctx.Queue()
        self._procs = [
            ctx.Process(target=_probe, args=(self.port, self.interval, self._ready, self._stop, self._q, self.host),
                        daemon=True)
            for _ in range(self.n)
        ]
        for p in self._procs:
            p.start()
        for _ in self._procs:
            if not self._ready.acquire(timeout=self.timeout):
                self.stop()
                raise RuntimeError("a probe did not connect in time")
        return self

    def stop(self):
        if not self._procs:
            return self.samples
        self._stop.set()
        got = []
        deadline = time.time() + self.timeout
        while len(got) < len(self._procs) and time.time() < deadline:
            try:
                got.append(self._q.get(timeout=max(0.1, deadline - time.time())))
            except Exception:
                break
        for p in self._procs:
            p.join(timeout=5)
            if p.is_alive():
                p.kill()
        self._procs = []
        if len(got) < self.n:
            raise RuntimeError("only %d of %d probes reported" % (len(got), self.n))
        self.samples = got
        return got

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()
        return False

    # ---- statistics, in milliseconds ----
    def worst_each(self, t0=None, t1=None, slack=0.002):
        """Per probe: the longest PING in flight at any time in [t0, t1]."""
        res = []
        for out in self.samples:
            if t0 is None:
                win = [r for _, r in out]
            else:
                win = [r for ts, r in out if ts <= t1 and ts + r >= t0 - slack]
            res.append(max(win) * 1000 if win else 0.0)
        return res

    def worst(self, t0=None, t1=None):
        each = self.worst_each(t0, t1)
        return max(each) if each else float("nan")

    def all_ms(self):
        return [r * 1000 for out in self.samples for _, r in out]

    def p99(self):
        return percentile(self.all_ms(), 0.99)

    def max(self):
        xs = self.all_ms()
        return max(xs) if xs else float("nan")

    def count(self):
        return sum(len(o) for o in self.samples)

    def over(self, ms):
        return sum(1 for x in self.all_ms() if x > ms)

    def top(self, k=20):
        """The k longest PINGs as (probe, t_sent, ms), for the raw log."""
        xs = [(i, ts, r * 1000) for i, out in enumerate(self.samples) for ts, r in out]
        xs.sort(key=lambda x: -x[2])
        return [(i, round(ts, 4), round(ms, 2)) for i, ts, ms in xs[:k]]
