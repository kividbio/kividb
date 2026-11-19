#!/usr/bin/env python3
"""#761: FT.AGGREGATE ... GROUPBY over 2,000,000 hash documents, and the index's memory.

KiviDB only: the Redis 8.10.2 on the VM image is built without the Query
Engine module (no FT.*). The issue's comparison with Redis 8.10.3 (the
`redis:8` image) is in the PR and in this README's recorded run.

The issue's workload, on one server:

  1. FT.CREATE userIdx ON HASH PREFIX 1 user: SCHEMA name TEXT email TAG
     communityNumber NUMERIC age NUMERIC, then 2,000,000 documents
     HSET user:<i> name "user <i>" email u<i>@example.com
     communityNumber <i % 1000> age <18 + i % 60>, in pipelines of 2,000,
     until FT.INFO says every document is indexed.
  2. used_memory and VmRSS once the load has settled (3 s), and the parts of
     FT.INFO's memory breakdown (info rows).
  3. Each query once to warm up, then 5 times: the median. `count` is the
     issue's query, GROUPBY 1 @communityNumber REDUCE COUNT 0 AS n LIMIT 0
     10; `avg` adds REDUCE AVG 1 @age and SORTBY 2 @n DESC; `filtered` is
     `count` over @age:[20 40] (666,667 documents, from the NUMERIC
     postings). Each reply's total must be 1000 groups
     (the candidate's: v1.2.2 reported the page's 10 rows).
  4. VmHWM (peak RSS) after the queries: 1.2.2 made a row (a map of every
     field, as strings) of each of the 2,000,000 documents before grouping
     them, some 1.5 GB at its peak.

Gates: query times within 10 % of recorded (host-scaled), keeping 60 % of
the gain over v1.2.2; used_memory and RSS after the load within 5 %, and the
peak within 10 %, keeping 60 % of their gains; the group counts exactly 1000.
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, THROUGHPUT, MEMORY, KEEP_IMPROVEMENT, EXACT, rel, info, scaled, median  # noqa: E402
from rss import vmrss_mib, used_memory_mib  # noqa: E402

S = Suite("33-ft-aggregate-groupby", [Config("docs2m", engines=("v1_2_2", "candidate"))])
DOCS = 2_000_000
REPS = 5
GROUP = ["GROUPBY", "1", "@communityNumber", "REDUCE", "COUNT", "0", "AS", "n"]
QUERIES = {
    "count": ["*"] + GROUP + ["LIMIT", "0", "10"],
    "avg": ["*"] + GROUP + ["REDUCE", "AVG", "1", "@age", "AS", "a", "SORTBY", "2", "@n", "DESC",
                            "LIMIT", "0", "10"],
    "filtered": ["@age:[20 40]"] + GROUP + ["LIMIT", "0", "10"],
}
# FT.INFO's memory parts, where the engine reports them.
PARTS = ["text_terms_bytes", "text_postings_bytes", "text_docs_bytes", "postings_ids_bytes",
         "postings_tag_bytes", "postings_numeric_bytes"]


def vmhwm_mib(pid):
    with open("/proc/%d/status" % pid) as f:
        for line in f:
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) / 1024.0
    raise RuntimeError("no VmHWM for pid %d" % pid)


def ft_info(c):
    r = c.execute_command("FT.INFO", "userIdx")
    out = {}
    for k, v in zip(r[::2], r[1::2]):
        k = k.decode() if isinstance(k, bytes) else str(k)
        if isinstance(v, bytes):
            v = v.decode()
        out[k] = v
    return out


@S.run
def run(ctx):
    c = ctx.client()
    c.flushall()
    n = scaled(DOCS)
    groups = min(1000, n)
    c.execute_command("FT.CREATE", "userIdx", "ON", "HASH", "PREFIX", "1", "user:", "SCHEMA",
                      "name", "TEXT", "email", "TAG", "communityNumber", "NUMERIC", "age", "NUMERIC")
    time.sleep(0.5)
    u0 = c.info("memory")["used_memory"]
    t = time.perf_counter()
    p = c.pipeline(transaction=False)
    for i in range(n):
        p.hset("user:%d" % i, mapping={"name": "user %d" % i, "email": "u%d@example.com" % i,
                                       "communityNumber": i % 1000, "age": 18 + i % 60})
        if i % 2000 == 1999:
            p.execute()
    p.execute()
    while True:
        d = ft_info(c)
        if int(d.get("num_docs", 0)) >= n and str(d.get("indexing", "0")) in ("0", "0.0"):
            break
        time.sleep(0.2)
    ctx.emit("load_s", time.perf_counter() - t, "s", "lower", gate=info())
    time.sleep(3)
    d = ft_info(c)
    ctx.raw["ft_info"] = {k: v for k, v in d.items() if isinstance(v, (str, int, float))}
    ctx.emit("used_memory_mib", used_memory_mib(c), "MiB", "lower", gate=[MEMORY, KEEP_IMPROVEMENT])
    ctx.emit("bytes_per_doc", (c.info("memory")["used_memory"] - u0) / float(n), "B", "lower",
             gate=info())
    ctx.emit("rss_loaded_mib", vmrss_mib(ctx.pid), "MiB", "lower", gate=[MEMORY, KEEP_IMPROVEMENT])
    ctx.emit("rss_peak_load_mib", vmhwm_mib(ctx.pid), "MiB", "lower", gate=info())
    for part in PARTS:
        if part in d:
            ctx.emit(part.replace("_bytes", "_mib"), int(d[part]) / 1048576.0, "MiB", "lower",
                     gate=info())
    for name, args in QUERIES.items():
        cmd = ["FT.AGGREGATE", "userIdx"] + args
        first = c.execute_command(*cmd)
        xs = []
        for _ in range(REPS):
            t = time.perf_counter()
            c.execute_command(*cmd)
            xs.append((time.perf_counter() - t) * 1000)
        ctx.emit("%s_ms" % name, median(xs), "ms", "lower", gate=[THROUGHPUT, KEEP_IMPROVEMENT],
                 samples=xs)
        # v1.2.2 reported the page's rows (10), not the groups (#795).
        if ctx.engine == "candidate":
            assert int(first[0]) == groups, (name, first[0])
        ctx.emit("%s_groups" % name, int(first[0]), "groups", "higher", gate=EXACT)
    ctx.emit("rss_peak_mib", vmhwm_mib(ctx.pid), "MiB", "lower", gate=[rel(10), KEEP_IMPROVEMENT])
    c.flushall()


if __name__ == "__main__":
    sys.exit(S.main())
