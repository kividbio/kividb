# Large sets on CollectionTable (#764 step 2, #770)

`run.sh` starts each server in turn, fresh for each test, and runs one of
`bench.py`'s tests against it; results go to `results/<label>/results.jsonl`.

    cargo build --release        # in each tree
    benchmarks/1.3.0/large-sets/run.sh <label> master=/path/master/kividb \
        branch=target/release/kividb redis=/opt/redis-8.10.2/src/redis-server

Needs `redis-cli`, `redis-benchmark` (`REDIS_BENCHMARK`) and Python with
redis-py (`PYTHON`). The tests are described at the top of `bench.py`.
Memory per member is in regression suite 17 (`17-set-memory`), instructions
per command in `scripts/perf/run.sh` with `WORKLOADS=sets`.

## Results (2026-11-20, Azure D4s_v5, rustc 1.100.0, release builds)

Master is `0caf6055`; the branch is the PR's (`results/d4s_v5`: `99467f18`;
the later runs: with the background's step at 1,024 units); Redis 8.10.2.
Each server has 2 worker threads (the default on 4 CPUs).

**Filling one set to 3M members, one `SADD` a command, one connection**
(`fill`; `PING` from a second connection meanwhile):

| | worst `SADD` | `SADD` p99 | `PING` p99.9 | worst `PING` |
|---|---|---|---|---|
| master (2 runs) | 119.2 / 141.7 ms | 0.055 ms | 0.064 / 0.062 ms | 3.0 / 0.83 ms |
| branch (4 runs) | 1.35 / 9.07 / 1.14 / 9.59 ms | 0.055-0.063 ms | 0.062-0.101 ms | 1.08 / 9.25 / 1.28 / 9.68 ms |
| Redis 8.10.2 | 5.06 ms | 0.055 ms | 0.071 ms | 5.07 ms |

Master's worst is the doubling of the hash table, all members moved by one
write. The branch's growth is a few members a write and then the
background's; two of its four runs had one pause of about 9 ms that both
connections saw at once (a pause of the process, not a wait for the set's
lock, which the background holds for at most 0.27 ms a step: see
`member_set::BACKGROUND_STEP`); its cause is not found yet.

**After the review (`results/review-fill`, commit `72ccc7d1`, 4 runs, `A`):**
worst `SADD` 1.94 / 1.77 / 1.53 / 1.67 ms, worst `PING` 1.84 / 1.48 / 1.30 /
1.96 ms, `PING` p99.9 0.069-0.089 ms; no ~9 ms pause in any run. The same
tree with retired arrays given back a megabyte at a time (`madvise`) before
they are freed (`B`, runs alternated with `A`): 2.86 / 1.05 / 1.84 / 2.33 ms
worst `SADD`, so that attempt was not kept.

**`SSCAN` of a 1M-member set, the whole of it** (`sscan`; one client,
redis-py; every member returned, checked):

| COUNT | master: calls, wall, server CPU | branch | Redis 8.10.2 |
|---|---|---|---|
| 10 | 1,025, 33.7 s, 32.0 s | 83,968, 8.0 s, 1.78 s | 95,412, 8.6 s, 1.79 s |
| 100 | 1,025, 33.1 s, 31.7 s | 9,816, 2.2 s, 0.41 s | 9,954, 2.2 s, 0.37 s |
| 1000 | 1,000, 32.6 s, 30.9 s | 999, 1.4 s, 0.17 s | 1,000, 1.5 s, 0.23 s |

Master read the whole set on every call (and returned at least a 1,024th of
it); the table reads only the page's groups.

**Random members of a 1M-member set** (`rand`; median of 5, ms):

| | `SRANDMEMBER` 10 / 1,000 / 500,000 | `SPOP` 10 / 1,000 / 500,000 |
|---|---|---|
| master | 0.13 / 1.48 / 651 | 0.39 / 1.98 / 693 |
| branch | 0.16 / 1.39 / 663 | 0.40 / 2.08 / 682 |
| Redis | 0.13 / 1.82 / 1,086 | 0.42 / 2.88 / 842 |

**Copies of a 1M-member set** (`copy`): `COPY` 41.8 ms on master (the members
were shared, only the table copied), 169 ms on the branch (every member
copied), 181 ms on Redis; each copy takes 35.7, 33.5 and 30.1 bytes a member.
`SUNIONSTORE` of it with a set of 10: 107, 136 and 235 ms.

**Removing a 3M-member set** (`free`; the command's own time, and the worst
`PING` from a second connection while it and the second after it ran):

| | master | branch | Redis |
|---|---|---|---|
| `DEL` | 317 ms, worst `PING` 0.44 ms | 228 ms, 0.32 ms | 253 ms, 252 ms |
| `UNLINK` | 1.2 ms, 0.37 ms | 1.2 ms, 0.79 ms | 1.0 ms, 1.1 ms |
| expiry | 367 ms, 1.0 ms | 222 ms, 0.38 ms | 258 ms, 256 ms |
| eviction | 2.3 ms, 248 ms | 2.5 ms, 155 ms | 254 ms, 252 ms |
| `FLUSHALL` | 252 ms, 251 ms | 152 ms, 151 ms | 300 ms, 0.37 ms |

**A set left just past a doubling** (`background`: 3,211,265 members loaded,
then nothing): `MEMORY USAGE` 161.4 MB while both arrays are held, 119.4 MB
(37.2 bytes a member) 0.5-0.6 s later, finished by the background with no
further command. Master: 174.1 MB.

**Digests** (`digest`): `DEBUG DIGEST-VALUE` of the same 1M strings and 200k
integers is the same on master and on the branch.
