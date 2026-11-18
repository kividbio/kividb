#!/usr/bin/env python3
"""#879: snapshots are written compressed (`rdbcompression yes`, the default).

Two datasets, each loaded into a fresh server of the engine (lib/nodes.py):

- `issue`: #879's dataset, `DEBUG POPULATE 6000000 k 150` (6M strings of 150
  bytes, `value:<n>` padded with zero bytes, compressible as memtier's
  default payload of the issue is);
- `coll`: a mix of collections, with fixed seeds: 200,000 hashes of 10
  fields (text and numbers), and 50,000 each of sets of 20 integers, sorted
  sets of 20 members and lists of 20 text elements.

For each: `SAVE` (median of 3, the first one a warm-up), the snapshot's size
on disk, then the server stopped and a new one started on the same directory:
the time until it answers `PING` with every key loaded (best of 2).

On v1.2.2 the issue's dataset writes 1,007 MB (7x Redis's 143 MB, #879);
with compression it is about 33 MB.

Gates: the snapshot size keeps 60 % of its gain over v1.2.2, and the issue's
dataset is within 1.5x of Redis's `dump.rdb` (the issue's ask). The save
does more work (it compresses) and writes less: at most 2.5x v1.2.2's time
this session (the study measured up to 2.2x on tmpfs for the collections);
the load decompresses: at most 1.5x v1.2.2's.
"""
import os
import random
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, ratio, vs_redis, vs_v1_2_2, info, scaled, median  # noqa: E402
from nodes import Nodes  # noqa: E402

S = Suite("24-snapshot-size", [Config("issue", self_hosted=True), Config("coll", self_hosted=True)])


def dump_path(node):
    name = "dump.rdb" if node.kind == "redis" else "dump.kdb"
    return os.path.join(node.dir, name)


def load_issue(c):
    n = scaled(6_000_000)
    c.execute_command("DEBUG", "POPULATE", n, "k", 150)
    return n


WORDS = ["ka", "lo", "mi", "ne", "ra", "to", "su", "vi", "an", "el", "or", "us", "ber", "dan",
         "fel", "gor", "hin", "jat", "kel", "mor", "pex", "quo"]


def text(rnd, lo, hi):
    s = " ".join(rnd.choice(WORDS) + rnd.choice(WORDS) for _ in range(8))
    return (s * 4)[: rnd.randint(lo, hi)]


def load_coll(c):
    rnd = random.Random(879)
    hashes, others = scaled(200_000), scaled(50_000)
    fields = ["name", "email", "city", "age", "score", "plan", "created", "flags", "last_ip", "bio"]
    p = c.pipeline(transaction=False)
    for i in range(hashes):
        m = {}
        for f, name in enumerate(fields):
            m[name] = str(rnd.randint(0, 2_000_000_000)) if f in (3, 4, 6) else text(rnd, 6, 24)
        p.hset("user:%d" % i, mapping=m)
        if i % 1000 == 999:
            p.execute()
    for i in range(others):
        p.sadd("tags:%d" % i, *[rnd.randint(0, 100_000) for _ in range(20)])
        p.zadd("board:%d" % i, {"%s%d" % (rnd.choice(WORDS), j): round(rnd.random() * 1000, 2)
                                for j in range(20)})
        p.rpush("q:%d" % i, *[text(rnd, 8, 32) for _ in range(20)])
        if i % 300 == 299:
            p.execute()
    p.execute()
    return hashes + 3 * others


@S.run
def run(ctx):
    load = load_issue if ctx.config.name == "issue" else load_coll
    with Nodes(ctx) as ns:
        first = ns.start()
        c = first.client()
        n = load(c)
        assert c.dbsize() == n, (c.dbsize(), n)
        saves = []
        for _ in range(4):
            t = time.perf_counter()
            c.save()
            saves.append((time.perf_counter() - t) * 1000)
        saves = saves[1:]
        size = os.path.getsize(dump_path(first))
        with open(dump_path(first), "rb") as f:
            ctx.raw["magic"] = f.read(8).decode("latin-1")
        if ctx.kind != "redis":
            p = c.info("persistence")
            ctx.raw["rdb_last_save_raw_size"] = p.get("rdb_last_save_raw_size")
            ctx.raw["rdb_last_save_stored_size"] = p.get("rdb_last_save_stored_size")
        first.stop()
        loads = []
        for _ in range(2):
            t = time.perf_counter()
            again = ns.start(dir=first.dir)
            ready = (time.perf_counter() - t) * 1000
            got = again.client().dbsize()
            assert got == n, (got, n)
            loads.append(ready)
            again.stop()
        size_gates = [ratio()]
        if ctx.config.name == "issue":
            size_gates.append(vs_redis(1.5))
        ctx.emit("snapshot_mb", size / 1e6, "MB", "lower", gate=size_gates)
        ctx.emit("save_ms", median(saves), "ms", "lower", gate=[vs_v1_2_2(2.5)], samples=saves)
        ctx.emit("load_ms", min(loads), "ms", "lower", gate=[vs_v1_2_2(1.5)], samples=loads)
        ctx.emit("keys", n, "keys", "higher", gate=info())


if __name__ == "__main__":
    sys.exit(S.main())
