#!/usr/bin/env python3
"""Small lists: memory a list element, as #1022 loads them and as a timeline
keeps them.

#1022 is Misskey's home timelines (`FanoutTimelineService.ts`): one list a
user, capped at 300 note ids of 10 bytes with `LPUSH` + `LTRIM key 0 299`,
read with `LRANGE key 0 -1`. A list is kept in packed chunks (two bytes an
element besides the element itself; v1.2.2 kept every element in an
allocation of its own, about 48 bytes a 10-byte element). Memory a list element
is the growth of INFO used_memory, and of VmRSS, over the load, divided by the
elements; keys are `tl:<12 digits>`, elements are decimal ids padded with `x` to
the size.

Configurations (each a fresh server; LISTS lists, 10,000):

  load        #1022's load: LISTS lists of 300 elements of 10 bytes, `RPUSH`
              one element a command (as a pipe file written by `redis-cli
              --pipe` sends them), pipelined
  load-batch  the same, one `RPUSH` of 300 elements a list
  timeline    the load-batch lists, then every list taken through 3,000
              `LPUSH id` + `LTRIM key 0 299` pairs (one push a list a round,
              the lists in key order): the chunks are filled at the front,
              trimmed at the back and given up, as a timeline's are; what
              the server holds after that is the number that matters for a
              timeline
  size32      the load-batch lists of 32-byte elements
  size64      the load-batch lists of 64-byte elements
  tiny        10,000 lists of 30 elements of 10 bytes, `RPUSH` one element a
              command

  used_per_element   growth of used_memory over the load / elements
  rss_per_element    growth of VmRSS (settled 5 s) / elements
  usage_one          `MEMORY USAGE tl:000000000000`, bytes
  list_len           `LLEN` of the first list (300, 30): the lists are whole
  push_s             seconds the load, or the timeline's pairs, took (info)

Gates (the candidate): `used_per_element` within 5 % (or 1 byte) of recorded,
at most 1.3x Redis's in the same session and at most 0.8x v1.2.2's;
`rss_per_element` within 10 % (or 1 byte) of recorded, at most 1.5x Redis's
(2.2x for `timeline` on a glibc build, the default one: 1.9x measured, 1.3x
with mimalloc) and at most 0.8x v1.2.2's (0.5x for `timeline`); `list_len` as
recorded. The RSS of `tiny` (4.5 MB in all) is shown, not gated. v1.2.2 and
Redis are shown.
"""
import os
import socket
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, rel, bound, info, vs_redis, vs_v1_2_2, scaled  # noqa: E402
from rss import vmrss_mib, MIB  # noqa: E402

S = Suite("52-small-lists-memory", [Config("load", threads=4), Config("load-batch", threads=4),
                                    Config("timeline", threads=4), Config("size32", threads=4),
                                    Config("size64", threads=4), Config("tiny", threads=4)])
LISTS = 10_000
PUSHES = 3_000
KEEP = 300
# name: (elements a list, element bytes, one command an element, pushes)
SHAPES = {
    "load": (300, 10, True, 0),
    "load-batch": (300, 10, False, 0),
    "timeline": (300, 10, False, PUSHES),
    "size32": (300, 32, False, 0),
    "size64": (300, 64, False, 0),
    "tiny": (30, 10, True, 0),
}


def enc(*args):
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        if isinstance(a, str):
            a = a.encode()
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)


def key(i):
    return b"tl:%012d" % i


def elem(size, n):
    s = b"%d" % n
    return (s + b"x" * size)[:size] if len(s) < size else s[-size:]


def blast(port, chunks, replies):
    """Sends every chunk, as fast as the server takes them, and waits for `replies`
    replies (each of the commands here answers with one line)."""
    s = socket.create_connection(("127.0.0.1", port), timeout=600)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    seen = [0]

    def drain():
        while seen[0] < replies:
            data = s.recv(1 << 20)
            if not data:
                break
            seen[0] += data.count(b"\n")

    reader = threading.Thread(target=drain, daemon=True)
    reader.start()
    for chunk in chunks:
        s.sendall(chunk)
    reader.join(900)
    s.close()
    if seen[0] != replies:
        raise RuntimeError("%d replies of %d" % (seen[0], replies))


def load_chunks(lists, n, size, one_each, per_chunk=500):
    batch = []
    for k in range(lists):
        kb = key(k)
        if one_each:
            batch.append(b"".join(enc("RPUSH", kb, elem(size, k * 1000 + j)) for j in range(n)))
        else:
            batch.append(enc("RPUSH", kb, *[elem(size, k * 1000 + j) for j in range(n)]))
        if len(batch) >= (per_chunk // n if one_each else per_chunk):
            yield b"".join(batch)
            batch = []
    if batch:
        yield b"".join(batch)


def timeline_chunks(lists, size, pushes):
    keys = [key(k) for k in range(lists)]
    pre = [b"*3\r\n$5\r\nLPUSH\r\n$%d\r\n%s\r\n$%d\r\n" % (len(k), k, size) for k in keys]
    trim = [b"*4\r\n$5\r\nLTRIM\r\n$%d\r\n%s\r\n$1\r\n0\r\n$3\r\n%d\r\n" % (len(k), k, KEEP - 1) for k in keys]
    pairs = list(zip(pre, trim))
    for r in range(pushes):
        e = elem(size, r) + b"\r\n"
        yield b"".join(a + e + b for a, b in pairs)


@S.run
def run(ctx):
    name = ctx.config.name
    n, size, one_each, pushes = SHAPES[name]
    lists = scaled(LISTS, step=100)
    pushes = scaled(pushes, step=100) if pushes else 0
    c = ctx.client()
    c.flushall()
    time.sleep(1)
    u0 = c.info("memory")["used_memory"]
    r0 = vmrss_mib(ctx.pid) * MIB
    t0 = time.time()
    blast(ctx.port, load_chunks(lists, n, size, one_each), lists * n if one_each else lists)
    if pushes:
        blast(ctx.port, timeline_chunks(lists, size, pushes), 2 * lists * pushes)
    secs = time.time() - t0
    time.sleep(5)
    u1 = c.info("memory")["used_memory"]
    r1 = vmrss_mib(ctx.pid) * MIB
    elements = lists * (KEEP if pushes else n)
    ctx.raw["shape"] = {"lists": lists, "elements_a_list": n, "element_bytes": size, "pushes": pushes,
                        "dbsize": c.dbsize()}
    ctx.emit("used_per_element", (u1 - u0) / float(elements), "B", "lower",
             gate=[rel(5, abs=1), vs_redis(1.3), vs_v1_2_2(0.8)])
    # `tiny` holds 4.5 MB in all: what the allocator keeps over it moves the
    # ratio by a third from run to run, so its RSS is shown, not gated. The
    # timeline's RSS on a glibc build (the default one) is what glibc keeps
    # of chunks that come and go: 1.9x Redis's against 1.3x with mimalloc.
    if name == "tiny":
        rss_gate = info()
    elif name == "timeline":
        rss_gate = [rel(10, abs=1), vs_redis(2.2), vs_v1_2_2(0.5)]
    else:
        rss_gate = [rel(10, abs=1), vs_redis(1.5), vs_v1_2_2(0.8)]
    ctx.emit("rss_per_element", (r1 - r0) / float(elements), "B", "lower", gate=rss_gate)
    ctx.emit("usage_one", int(c.execute_command("MEMORY", "USAGE", key(0))), "B", "lower", gate=info())
    ctx.emit("list_len", c.llen(key(0)), "elements", "higher", gate=bound(eq="recorded"))
    ctx.emit("push_s", secs, "s", "lower", gate=info())


if __name__ == "__main__":
    sys.exit(S.main())
