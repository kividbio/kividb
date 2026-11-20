# Large hashes on CollectionTable (#764 step 3, #770)

`run.sh` starts each server in turn, fresh for each test, and runs one of
`bench.py`'s tests against it; results go to `results/<label>/results.jsonl`.

    cargo build --release        # in each tree
    benchmarks/1.3.0/large-hashes/run.sh <label> master=/path/master/kividb \
        branch=target/release/kividb redis=/opt/redis-8.10.2/src/redis-server

Needs `redis-cli`, `redis-benchmark` (`REDIS_BENCHMARK`) and Python with
redis-py (`PYTHON`). The tests are described at the top of `bench.py`.
Memory per field at the suite's sizes is in regression suite 19
(`19-hash-memory`), instructions per command in `scripts/perf/run.sh` with
`WORKLOADS=hashes`.

## Results (2026-11-20, Azure Standard_D8s_v5, rustc 1.100.0, release builds)

`results/d8s_v5`. Master is `7c03b24e`, built in the same session on the
same VM; the branch is the PR's; Redis 8.10.2. Each KiviDB server runs its
default worker count (4 on 8 CPUs). Nothing else ran on the machine.
The branch binary is the PR's tree before its last change, a loop of its
own for each form in `HGETALL` (`HashFields::for_each`), which only changes
how `HGETALL` walks a hash (measured in instructions in the PR).

**#764's load** (one hash, fields `f0`..`f<n-1>`, each `v`; `used_memory` it
added once settled):

| fields | master | **branch** | Redis 8.10.2 |
|---:|---:|---:|---:|
| 3,000,000 | 102.93 B (294.5 MiB) | **29.63 B (84.8 MiB)** | 37.91 B (108.5 MiB) |
| 3,211,264 (fullest table) | 98.27 B | **29.32 B** | 36.75 B |
| 3,211,265 (just doubled) | 98.27 B | **37.16 B** | 36.75 B |

**Field TTLs** (one hash of 1,000,000 fields `field:<i>` with 32-byte
values; then every field given a TTL): master 168.92 / 252.16 B a field,
branch **65.47 / 148.69**, Redis 70.12 / 95.60. The deadlines are still kept
beside the table (`Expiries`, a second copy of each name): 83 B a field with
a TTL on master and on the branch, against 25 in Redis.

**Filling one hash to 3M fields, one `HSET` a command, one connection**
(`fill`; `PING` from a second connection meanwhile):

| | worst `HSET` | `HSET` p99 | `PING` p99.9 | worst `PING` |
|---|---|---|---|---|
| master | 64.7 ms | 0.055 ms | 0.186 ms | 6.9 ms |
| branch | **6.9 ms** | 0.055 ms | 0.142 ms | 7.9 ms |
| Redis 8.10.2 | 6.7 ms | 0.055 ms | 0.161 ms | 7.5 ms |

**HSCAN, full iteration of a 1M-field hash** (calls / wall / server CPU; every
field returned once, checked):

| COUNT | master | **branch** | Redis 8.10.2 |
|---|---|---|---|
| 10 | 1,025 / 13.6 s / 10.95 s | **84,041 / 10.2 s / 2.36 s** | 95,442 / 10.5 s / 1.93 s |
| 100 | 1,025 / 13.6 s / 10.97 s | **9,813 / 3.5 s / 0.51 s** | 9,953 / 3.5 s / 0.50 s |
| 1000 | 1,000 / 13.3 s / 10.66 s | **999 / 2.8 s / 0.36 s** | 1,000 / 2.8 s / 0.34 s |

**HRANDFIELD on 1M fields** (ms, median of 5; count 10 / 1,000 / 500,000):
master 0.11 / 1.81 / 717, branch 0.14 / 1.65 / 718, Redis 0.13 / 2.18 / 680;
negative counts master 0.10 / 1.48 / 711, branch 0.10 / 1.59 / 707, Redis
0.11 / 1.77 / 811.

**COPY of a 1M-field hash:** master 75.8 ms (121 B a field added: names
copied, values shared), branch 199.7 ms (65.5 B), Redis 286.8 ms (70.1 B).

**Freeing a 3M-field hash** (command's time, worst `PING` meanwhile): `DEL`
master 147 ms / 3.1 ms, branch 196 ms / 1.1 ms, Redis 373 ms / 372 ms;
`UNLINK` 0.9 ms on each.

**The background finishing a resize** (3,211,265 fields, pipelined, then left
alone): `MEMORY USAGE` 264.1 MB at once, 222.2 MB 0.6 s later with no command
(the growth finished and its old array freed by the background).

**`DEBUG DIGEST-VALUE`** of the same 1M-field hash and of a small one: equal
on master and the branch.

**Search ingest** (`FT.CREATE ... ON HASH` with TEXT, TAG and NUMERIC fields,
then 200,000 documents by pipelined `HSET`, until `FT.INFO` says indexing is
done; one run each): small documents (`listpack`) master 78,788 / branch
78,902 a second; documents with a 100-byte body (`hashtable`) master 61,325
/ branch 60,398 a second (-1.5 %). Redis 8.10.2 here has no query engine.
