#!/usr/bin/env python3
"""#1016: writes to a large string cost the same whatever its length.

Three changes of 1.3.0 make a command on a long string cost what the bytes
it touches cost, not what the string's length does: #1027 reads a bit or a
field where the string is kept (a `SETBIT` that leaves the bit as it is, a
`BITFIELD` with only `GET`s), #1031 writes a `SETBIT` that changes a bit in
place when nothing but its key holds the string, and #1016's `APPEND`
writes in place too, a long string keeping room for the next appends
(a quarter of its length, at most 16 MiB) and growing by `realloc` when it
has none left. In v1.2.2 each of these copied the whole string, twice for
a `SETBIT` that changed a bit.

Dataset, one server, one key per case: a string of 64 MiB (`SETRANGE k
67108863 a`: zero bytes and an `a`), made afresh for each case.

Cases, each timed over one connection, one call at a time, after a
warm-up of a few calls; 3 rounds, the median of them (candidate and Redis:
2,000 calls a round; v1.2.2, whose every call copies 64 MiB: 10):

  setbit_noop     SETBIT k <bit in the middle> 0 (the bit is 0)
  bitfield_get    BITFIELD k GET u8 <offset in the middle>
  setbit_change   SETBIT k <bit> 1, then SETBIT k <bit> 0, by turns
  append_3b       APPEND k abc
  append_1k       APPEND k <1,024 bytes>

Each emits `<case>_srv_us`, the server's own microseconds a call (`INFO
commandstats`, reset before each round), and `<case>_us`, the client's wall
time a call, round trip included. `bad_replies` counts replies that are not
what the case makes them (0 for the no-op `SETBIT`, `[0]` for the field,
the old bit, the new length); `append_3b_strlen` and `append_1k_strlen` are
the lengths the appends end at.

Gates, all in the same session, with large fixed margins rather than
recorded values: every `<case>_srv_us` at most 0.01x v1.2.2's (at least
100 times faster: v1.2.2 takes 30-130 ms a call here, the candidate a
microsecond or two, so the margin is a thousandfold); every `<case>_us`
at most 0.05x v1.2.2's (the round trip, about 50 us from Python, bounds
what can be seen) and at most 2x Redis's. The server times are not gated
against Redis's: both are around a microsecond, and the server clock counts
whole microseconds. `bad_replies` exactly 0, and the two lengths as
recorded (they follow from the call counts).
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (Suite, Config, EXACT, median, vs_redis, vs_v1_2_2)  # noqa: E402

S = Suite("53-large-string-writes", [
    Config("s64m", threads=2),
])
LEN = 64 << 20
ROUNDS = 3
WARM = 3
CASES = ["setbit_noop", "bitfield_get", "setbit_change", "append_3b", "append_1k"]
KILO = b"x" * 1024


def calls_for(ctx):
    if os.environ.get("SCALE", "1") not in ("", "1"):
        return 20
    return 10 if ctx.engine == "v1_2_2" else 2000


def server_usec(c, name):
    s = c.info("commandstats").get("cmdstat_" + name, {})
    return s.get("calls", 0), s.get("usec", 0)


def make(c, key):
    c.delete(key)
    c.setrange(key, LEN - 1, "a")


def plan(case, key, length):
    """The calls of `case` and the reply each should get, from a string of
    `length` bytes; the command name INFO commandstats counts."""
    bit = LEN * 4
    if case == "setbit_noop":
        return (lambda i: (["SETBIT", key, bit, 0], 0)), "setbit"
    if case == "bitfield_get":
        return (lambda i: (["BITFIELD", key, "GET", "u8", LEN // 2], [0])), "bitfield"
    if case == "setbit_change":
        return (lambda i: (["SETBIT", key, bit, 1 - i % 2], i % 2)), "setbit"
    step = 3 if case == "append_3b" else 1024
    extra = b"abc" if case == "append_3b" else KILO
    return (lambda i: (["APPEND", key, extra], length + step * (i + 1))), "append"


@S.run
def run(ctx):
    c = ctx.client()
    c.flushall()
    calls = calls_for(ctx)
    bad = 0
    ctx.raw["cases"] = {}
    gate_wall = [vs_v1_2_2(0.05), vs_redis(2.0)]
    for case in CASES:
        key = "k:" + case
        make(c, key)
        done = 0
        wall, server = [], []
        for r in range(ROUNDS + 1):
            n = WARM if r == 0 else calls
            length = c.strlen(key)
            call, name = plan(case, key, length)
            if r:
                c.config_resetstat()
            t = time.perf_counter()
            for i in range(n):
                args, want = call(done + i if case == "setbit_change" else i)
                if c.execute_command(*args) != want:
                    bad += 1
            elapsed = time.perf_counter() - t
            done += n
            if r:
                wall.append(elapsed / n * 1e6)
                counted, usec = server_usec(c, name)
                server.append(usec / counted if counted else 0.0)
        ctx.raw["cases"][case] = {"wall_us": wall, "server_us": server, "strlen": c.strlen(key)}
        ctx.emit(case + "_srv_us", median(server), "us", "lower", gate=vs_v1_2_2(0.01), samples=server)
        ctx.emit(case + "_us", median(wall), "us", "lower", gate=gate_wall, samples=wall)
        if case.startswith("append"):
            ctx.emit(case + "_strlen", c.strlen(key), "bytes", "higher", gate=EXACT)
        c.delete(key)
    ctx.emit("bad_replies", bad, "replies", "lower", gate=EXACT)


if __name__ == "__main__":
    sys.exit(S.main())
