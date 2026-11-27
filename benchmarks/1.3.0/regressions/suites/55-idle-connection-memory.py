#!/usr/bin/env python3
"""Idle connections: memory a connection holds while it waits for its client.

Connection pools keep many connections open that send nothing for long
spells. A connection's read buffer starts at a small read's room and its
reply buffers start empty; a read buffer grown for a burst goes back to that
size once the connection's reads are small again (and, on the Tokio loop,
once it has been quiet for `query-buffer-idle-resize-after`).

Configurations (each a fresh server; CONNS connections, 10,000, opened from
one client process; `CONFIG SET maxclients 20000` first). The count is the
same at any SCALE: `clients` is gated equal to the recorded count.

  tokio-idle  `--io-model tokio`: connections that send nothing
  tokio-ping  `--io-model tokio`: each sends one `PING`, reads its reply,
              and sends nothing more
  uring-idle  `--io-model uring`: as tokio-idle
  uring-ping  `--io-model uring`: as tokio-ping

Redis runs every configuration (it has one I/O model): its figure is beside
KiviDB's in each.

  used_per_conn   growth of INFO used_memory from before the connections are
                  opened to 5 s after the last one went idle, / CONNS
  rss_per_conn    growth of VmRSS over the same span, / CONNS
  clients         connected_clients less the measuring one: every connection
                  was taken in (exact)

Gates (the candidate): `used_per_conn` within 5 % (or 256 bytes) of recorded,
and at least 60 % of the recorded gain over v1.2.2; `rss_per_conn` within
10 % (or 1 KiB) of recorded (it moves with where the allocator places the
buffers: a page partly touched counts whole); `clients` as recorded. v1.2.2
and Redis are shown.
"""
import os
import resource
import socket
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, rel, bound, info, KEEP_IMPROVEMENT  # noqa: E402
from rss import vmrss_mib, MIB  # noqa: E402

S = Suite("55-idle-connection-memory", [
    Config("tokio-idle", io="tokio", threads=2),
    Config("tokio-ping", io="tokio", threads=2),
    Config("uring-idle", io="uring", threads=2),
    Config("uring-ping", io="uring", threads=2),
])
CONNS = 10_000
PING = b"*1\r\n$4\r\nPING\r\n"


def recv_exactly(s, n):
    got = 0
    while got < n:
        d = s.recv(n - got)
        if not d:
            raise RuntimeError("connection closed")
        got += len(d)


@S.run
def run(ctx):
    conns = CONNS
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft < conns + 100:
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(hard, conns + 1000), hard))
    c = ctx.client()
    c.config_set("maxclients", 20000)
    c.ping()
    time.sleep(1)
    u0 = c.info("memory")["used_memory"]
    r0 = vmrss_mib(ctx.pid) * MIB
    socks = []
    t0 = time.time()
    for _ in range(conns):
        s = socket.create_connection(("127.0.0.1", ctx.port), timeout=60)
        socks.append(s)
    if ctx.config.name.endswith("-ping"):
        for s in socks:
            s.sendall(PING)
        for s in socks:
            recv_exactly(s, 7)
    open_s = time.time() - t0
    time.sleep(5)
    u1 = c.info("memory")["used_memory"]
    r1 = vmrss_mib(ctx.pid) * MIB
    clients = int(c.info("clients")["connected_clients"]) - 1
    ctx.raw["shape"] = {"conns": conns, "open_s": round(open_s, 2)}
    ctx.emit("used_per_conn", (u1 - u0) / float(conns), "B", "lower",
             gate=[rel(5, abs=256), KEEP_IMPROVEMENT])
    ctx.emit("rss_per_conn", (r1 - r0) / float(conns), "B", "lower", gate=rel(10, abs=1024))
    ctx.emit("clients", clients, "connections", "higher", gate=bound(eq="recorded"))
    ctx.emit("open_s", open_s, "s", "lower", gate=info())
    for s in socks:
        s.close()


if __name__ == "__main__":
    sys.exit(S.main())
