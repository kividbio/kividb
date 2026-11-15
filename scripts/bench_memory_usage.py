#!/usr/bin/env python3
"""MEMORY USAGE of sets and hashes against what their keys take.

For each shape and size, a fresh server process is started (no persistence),
a fixed number of identical keys is loaded, every key's size is checked, and
the growth of `used_memory` and of `used_memory_rss` per key is compared with
the keys' average MEMORY USAGE (default SAMPLES and SAMPLES 0).

    scripts/bench_memory_usage.py --server target/release/kividb
    scripts/bench_memory_usage.py --server redis-server --kind redis

`--repeat N` runs each measurement in N separate processes. `--json out.json`
writes the rows.

What the numbers are:

- KiviDB's `used_memory` is what the allocator was asked for (the counting
  allocator, `memory_use`); it is what `maxmemory` is measured against, and
  what MEMORY USAGE estimates.
- Redis's `used_memory` is what jemalloc (or libc) reports as allocated, its
  size classes included. Compare a server's estimate with its own
  `used_memory`; across servers, compare like with like and say which.
- RSS is what the operating system holds for the process: allocator size
  classes, free lists and fragmentation included. It is reported, not
  compared with the estimate.
"""

import argparse
import json
import socket
import statistics
import subprocess
import sys
import tempfile
import time

import redis

SHAPES = ["set-int", "set-str16", "hash", "hash-ttl"]
OPTIONAL_SHAPES = ["zset"]
SIZES = [1, 2, 8, 10, 32, 64, 65, 128, 512, 5000]


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start(server, kind):
    port = free_port()
    workdir = tempfile.mkdtemp(prefix="memusage-")
    args = [server, "--port", str(port), "--bind", "127.0.0.1", "--dir", workdir]
    if kind == "redis":
        args += ["--save", "", "--appendonly", "no"]
    proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    r = redis.Redis(port=port)
    for _ in range(400):
        try:
            r.ping()
            break
        except redis.ConnectionError:
            time.sleep(0.05)
    else:
        proc.kill()
        sys.exit(f"{server} did not start")
    r.config_set("save", "")
    return proc, r


def memory(r):
    info = r.info("memory")
    return int(info["used_memory"]), int(info["used_memory_rss"])


def settled(r):
    """used_memory and RSS once two readings 100 ms apart agree (2 s at most)."""
    last = memory(r)
    for _ in range(20):
        time.sleep(0.1)
        now = memory(r)
        if now[0] == last[0]:
            return now
        last = now
    return last


def commands(shape, key, size):
    if shape == "set-int":
        yield ("SADD", key, *range(size))
    elif shape == "set-str16":
        yield ("SADD", key, *(f"member-{i:09d}" for i in range(size)))
    elif shape in ("hash", "hash-ttl"):
        pairs = []
        for i in range(size):
            pairs += [f"field:{i}", f"value-{i:010d}"]
        yield ("HSET", key, *pairs)
        if shape == "hash-ttl":
            yield ("HPEXPIRE", key, 3_600_000, "FIELDS", size,
                   *(f"field:{i}" for i in range(size)))
    elif shape == "zset":
        args = []
        for i in range(size):
            args += [i, f"member-{i:09d}"]
        yield ("ZADD", key, *args)


def run_pipeline(r, cmds):
    pipe = r.pipeline(transaction=False)
    out = []
    for n, cmd in enumerate(cmds, 1):
        pipe.execute_command(*cmd)
        if n % 500 == 0:
            out += pipe.execute()
    out += pipe.execute()
    return out


def measure(server, kind, shape, size, keys):
    proc, r = start(server, kind)
    try:
        names = [f"{shape}:{k}" for k in range(keys)]
        # Warm-up in another database: whatever the commands allocate once.
        r.execute_command("SELECT", 1)
        run_pipeline(r, [c for k in range(50) for c in commands(shape, f"w{k}", size)])
        r.execute_command("SELECT", 0)
        before = settled(r)
        run_pipeline(r, [c for k in names for c in commands(shape, k, size)])
        after = settled(r)
        card = {"set-int": "SCARD", "set-str16": "SCARD", "zset": "ZCARD"}.get(shape, "HLEN")
        lens = run_pipeline(r, [(card, k) for k in names])
        assert all(n == size for n in lens), f"{shape} {size}: sizes {set(lens)}"
        usage = run_pipeline(r, [("MEMORY", "USAGE", k) for k in names])
        usage0 = run_pipeline(r, [("MEMORY", "USAGE", k, "SAMPLES", 0) for k in names])
        encoding = r.object("encoding", names[0])
        encoding = encoding.decode() if isinstance(encoding, bytes) else str(encoding)
    finally:
        proc.terminate()
        proc.wait()
    measured = (after[0] - before[0]) / keys
    return {
        "shape": shape, "size": size, "keys": keys, "encoding": encoding,
        "measured": measured,
        "rss": (after[1] - before[1]) / keys,
        "usage_default": statistics.mean(usage),
        "usage_samples0": statistics.mean(usage0),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", required=True)
    ap.add_argument("--kind", choices=["kividb", "redis"], default="kividb")
    ap.add_argument("--shapes", default=",".join(SHAPES),
                    help=f"of {SHAPES + OPTIONAL_SHAPES}")
    ap.add_argument("--sizes", default=",".join(map(str, SIZES)))
    ap.add_argument("--members", type=int, default=400_000,
                    help="members loaded per measurement (keys = members / size)")
    ap.add_argument("--repeat", type=int, default=1)
    ap.add_argument("--json")
    args = ap.parse_args()

    rows = []
    print(f"{'shape':10} {'size':>5} {'keys':>6} {'encoding':>10} {'used/key':>10} "
          f"{'+-':>5} {'rss/key':>9} {'usage':>9} {'err%':>7} {'usage(0)':>9} {'err%':>7}")
    for shape in args.shapes.split(","):
        for size in map(int, args.sizes.split(",")):
            keys = max(100, min(20_000, args.members // size))
            runs = [measure(args.server, args.kind, shape, size, keys)
                    for _ in range(args.repeat)]
            m = [x["measured"] for x in runs]
            row = dict(runs[0])
            row["measured"] = statistics.mean(m)
            row["measured_spread"] = (max(m) - min(m)) / 2
            row["rss"] = statistics.mean(x["rss"] for x in runs)
            row["err_default_pct"] = (row["usage_default"] - row["measured"]) / row["measured"] * 100
            row["err_samples0_pct"] = (row["usage_samples0"] - row["measured"]) / row["measured"] * 100
            rows.append(row)
            print(f"{shape:10} {size:>5} {keys:>6} {row['encoding']:>10} {row['measured']:>10.1f} "
                  f"{row['measured_spread']:>5.1f} {row['rss']:>9.1f} {row['usage_default']:>9.1f} "
                  f"{row['err_default_pct']:>+7.2f} {row['usage_samples0']:>9.1f} "
                  f"{row['err_samples0_pct']:>+7.2f}", flush=True)
    if args.json:
        with open(args.json, "w") as f:
            json.dump({"server": args.server, "kind": args.kind, "rows": rows}, f, indent=1)


if __name__ == "__main__":
    main()
