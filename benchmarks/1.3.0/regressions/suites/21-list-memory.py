#!/usr/bin/env python3
"""Collection storage step 3: lists in chunks of packed elements.

Bytes per element is the growth of INFO used_memory over the load divided by
the elements, as steps 1 and 2 (`17-set-memory`, `19-hash-memory`) take it:
TOTAL elements (1,000,000) in lists of N elements each (TOTAL / N keys), N =
10, 100, 1,000 and 100,000, loaded with RPUSH of up to 500 elements a command
in pipelines of about 5,000 elements. Keys are `k:lst:<i>` (16 bytes);
elements `valuevaluevaluevaluevalue<i>` (32 bytes, i zero-padded to 7
digits), the size the redesign was measured with.

  packed  the lists as loaded: every element 32 bytes, so every list is kept
          in chunks (`listpack` while it is one chunk of up to 64 elements,
          `quicklist` beyond)
  plain   the same, with a first element of 1,025 bytes pushed and popped:
          the list keeps its elements apart from then on, as 1.2 kept every
          list (a guard: the change must not cost these)
  short   the same sizes, elements of 6 bytes: job ids (`100000` up, as a
          queue's are, decimal), where the elements' lengths are most of
          what an element costs
  queue   one list of short elements as a queue leaves it, not as a bulk
          load lays it down:
            bullmq_<n>: BullMQ's wait list, ids `1` to `n` (n = 200,000)
              pushed one `LPUSH` at a time
            rotated_<n>: n 6-byte ids pushed one `LPUSH` at a time, then
              10 n times one `LPUSH` and one `RPOP` (ten full rotations)
            sawtooth_low_<n>: grown to n (one `LPUSH` at a time) and
              drained to n / 10 by `RPOP`, three times (n = 1,000,000):
              what is held per element left
          `memory_usage_<shape>` is `MEMORY USAGE key SAMPLES 0` over the
          growth of used_memory, in % (info: exact accounting is 100)

`encoding_<n>` records what OBJECT ENCODING says of the first key (2
listpack, 5 quicklist) and must not change.

Gates: memory per element within 5 % (or 1 B) of recorded at every size; for
`packed`, `short` and `queue` it must also keep 60 % of the gain over v1.2.2
measured in the same session.
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, KEEP_IMPROVEMENT, EXACT, rel, info, scaled  # noqa: E402

S = Suite("21-list-memory", [Config("packed", threads=4), Config("plain", threads=4), Config("short", threads=4), Config("queue", threads=4)])
SIZES = [10, 100, 1000, 100_000]
TOTAL = 1_000_000
BATCH = 500
ENCODINGS = {"listpack": 2, "quicklist": 5}
LONG = "L" * 1025
QUEUE = 200_000
SAWTOOTH = 1_000_000
ROTATIONS = 10


def elements(start, n):
    return ["valuevaluevaluevaluevalue%07d" % i for i in range(start, start + n)]


def short_elements(start, n):
    return ["%d" % (100_000 + i % 900_000) for i in range(start, start + n)]


def load(c, n, total, plain, make=elements):
    keys = max(1, total // n)
    p = c.pipeline(transaction=False)
    pending = 0
    for k in range(keys):
        key = "k:lst:%010d" % k
        if plain:
            p.execute_command("RPUSH", key, LONG)
        done = 0
        while done < n:
            m = min(BATCH, n - done)
            p.execute_command("RPUSH", key, *make(done, m))
            done += m
            pending += m
            if pending >= 5000:
                p.execute()
                pending = 0
        if plain:
            p.execute_command("LPOP", key)
    p.execute()
    return keys


def pipelined(c, commands, every=5000):
    """Send each command of the iterable `commands`, pipelined."""
    p = c.pipeline(transaction=False)
    pending = 0
    for args in commands:
        p.execute_command(*args)
        pending += 1
        if pending >= every:
            p.execute()
            pending = 0
    p.execute()


def by_size(ctx, c, make, gate_gain):
    plain = ctx.config.name == "plain"
    total = scaled(TOTAL)
    for n in SIZES:
        n_here = min(n, total)
        c.flushall()
        time.sleep(0.5)
        u0 = c.info("memory")["used_memory"]
        t = time.perf_counter()
        keys = load(c, n_here, total, plain, make)
        secs = time.perf_counter() - t
        u1 = c.info("memory")["used_memory"]
        first = "k:lst:%010d" % 0
        assert c.llen(first) == n_here, "LLEN %d != %d" % (c.llen(first), n_here)
        per = (u1 - u0) / float(keys * n_here)
        gate = [rel(5, abs=1)]
        if gate_gain:
            gate.append(KEEP_IMPROVEMENT)
        ctx.emit("bytes_per_element_%d" % n, per, "B", "lower", gate=gate)
        enc = c.object("encoding", first)
        enc = enc.decode() if isinstance(enc, bytes) else enc
        ctx.emit("encoding_%d" % n, ENCODINGS.get(enc, 0), "code", "lower", gate=EXACT)
        ctx.emit("load_%d_s" % n, secs, "s", "lower", gate=info())


def queue_shapes(ctx, c):
    n_queue = scaled(QUEUE)
    n_saw = scaled(SAWTOOTH)
    key = "k:queue"

    def measured(shape, u0, held):
        u1 = c.info("memory")["used_memory"]
        assert c.llen(key) == held, "%s: LLEN %d != %d" % (shape, c.llen(key), held)
        ctx.emit("%s_bytes_per_element" % shape, (u1 - u0) / float(held), "B", "lower",
                 gate=[rel(5, abs=1), KEEP_IMPROVEMENT])
        usage = c.execute_command("MEMORY", "USAGE", key, "SAMPLES", "0")
        ctx.emit("memory_usage_%s" % shape, 100.0 * int(usage) / float(u1 - u0), "%", "lower", gate=info())

    def fresh():
        c.flushall()
        time.sleep(0.5)
        return c.info("memory")["used_memory"]

    # BullMQ's wait list.
    u0 = fresh()
    pipelined(c, (("LPUSH", key, "%d" % (i + 1)) for i in range(n_queue)))
    measured("bullmq_%d" % QUEUE, u0, n_queue)

    # Ten full rotations of a queue of 6-byte ids.
    u0 = fresh()
    pipelined(c, (("LPUSH", key, v) for v in short_elements(0, n_queue)))
    ids = short_elements(n_queue, 1000)

    def rotations():
        for r in range(ROTATIONS * n_queue):
            yield ("LPUSH", key, ids[r % 1000])
            yield ("RPOP", key)

    pipelined(c, rotations())
    measured("rotated_%d" % QUEUE, u0, n_queue)

    # Grown and drained, three times.
    u0 = fresh()
    low = n_saw // 10
    for _ in range(3):
        held = c.llen(key)
        pipelined(c, (("LPUSH", key, v) for v in short_elements(held, n_saw - held)))
        pipelined(c, (("RPOP", key) for _ in range(n_saw - low)))
    measured("sawtooth_low_%d" % SAWTOOTH, u0, low)


@S.run
def run(ctx):
    c = ctx.client()
    name = ctx.config.name
    if name == "queue":
        queue_shapes(ctx, c)
    elif name == "short":
        by_size(ctx, c, short_elements, True)
    else:
        by_size(ctx, c, elements, name == "packed")
    c.flushall()


if __name__ == "__main__":
    sys.exit(S.main())
