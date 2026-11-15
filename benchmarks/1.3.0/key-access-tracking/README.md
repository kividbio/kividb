# `key-access-tracking yes`: what it costs (#21)

What it costs a command, and a key, to keep when each key was last used (or
how often) under every `maxmemory-policy`, so that `OBJECT IDLETIME`,
`OBJECT FREQ` and `RESTORE ... IDLETIME` work under `noeviction` as in Redis.

## Instructions per command

Whole server, one thread, `--io-model uring`, `maxmemory-policy noeviction`,
callgrind, 40,000 commands per workload, built with `LTO=fat` (the release
profile as shipped). Pipelined is `redis-benchmark -P 16`, not pipelined `-P 1`;
the keys are loaded first. `MGET16` is an `MGET` of 16 keys.

| command | master, tracking off (today) | branch, tracking off | branch, tracking on | tracking on vs today |
|---|---:|---:|---:|---:|
| GET 100 B, pipelined | 3258 | 3276 (+0.55%) | 3291 | +33 (+1.01%) |
| SET 100 B, pipelined | 3984 | 4011 (+0.68%) | 4013 | +29 (+0.73%) |
| GET 1 KB, pipelined | 3826 | 3813 (-0.34%) | 3884 | +58 (+1.52%) |
| SET 1 KB, pipelined | 4107 | 4082 (-0.61%) | 4149 | +42 (+1.02%) |
| GET 100 B, not pipelined | 9792 | 9902 (+1.12%) | 9940 | +148 (+1.51%) |
| SET 100 B, not pipelined | 10583 | 10480 (-0.97%) | 10636 | +53 (+0.50%) |
| GET 1 KB, not pipelined | 10455 | 10399 (-0.54%) | 10555 | +100 (+0.96%) |
| SET 1 KB, not pipelined | 10663 | 10637 (-0.24%) | 11045 | +382 (+3.58%) |
| INCR | 4101 | 4076 (-0.61%) | 4143 | +42 (+1.02%) |
| HGET | 3976 | 3949 (-0.68%) | 4027 | +51 (+1.28%) |
| LPUSH | 5289 | 5332 (+0.81%) | 5390 | +101 (+1.91%) |
| LPOP | 4530 | 4509 (-0.46%) | 4593 | +63 (+1.39%) |
| MGET of 16 keys | 20493 | 20500 (+0.03%) | 20548 | +55 (+0.27%) |

This is the run on the final commit (`results/callgrind-*.txt`). An earlier
run of the same code (before the default was put back to `no`) gave, for
pipelined 100 B `GET` and `SET`: master 3234 and 3960, branch off 3230 and
3945, branch on 3284 and 4001 (+1.55% and +1.04% over master). Master itself
differs by 0.7% between the two runs (3234 and 3258; the keys redis-benchmark
picks and the hash seeds differ), so a single run cannot show less than about
+-0.7% on one command, and the not-pipelined `SET 1 KB` outlier (+3.58%) is
of that kind. The mean of the two runs, pipelined `GET`/`SET` 100 B: tracking
off +0.2% / +0.15% over master, tracking on +1.3% / +0.9%.

Turning tracking on in master, before this change, cost +9.3% to +9.9% on a
pipelined `GET` of 100 bytes (+7.7 to +8.0% on `SET`, +7 to +8% on `INCR`,
`HGET`, `LPUSH` and `LPOP`): about 320 instructions a command. What brought it to about 50:

- the clock is kept for the second it was read in, on each thread, instead of
  subtracting two instants for every command (a third of the cost);
- the scope a command's lookups write under is a few bytes, and open and
  closed with no allocation or copying; what an LFU scope puts back is kept
  on a stack beside it;
- a command is told from the few that never count as a use (`OBJECT`, `TTL`,
  `EXISTS`, ...) and from scripts by its first letter before any name is
  compared (`GET` was compared to `TTL`, byte by byte);
- whether the keys have their use written after the command (a script, the LRM
  policies) is decided with the stamp, not asked again after the command;
- the common stamp is one comparison, the other kinds are out of line.

The raw `raw.txt` files are in `results/` (`callgrind-master.txt`,
`callgrind-branch.txt`).

## Bytes per key

`memory.py`: 10,000,000 keys `k<i>` with an 8-byte value, a fresh server for
each setting (tokio, nothing persisted), growth of `INFO` `used_memory` and
`used_memory_rss` divided by `DBSIZE`:

| `key-access-tracking` | `used_memory` per key | RSS per key |
|---|---:|---:|
| `no` | 47.10 | 49.13 |
| `yes` | 47.10 | 49.47 |

The access word is in every key's entry whatever the setting, so nothing is
added per key; the RSS difference is 0.3 B a key, 3 MB over 10 million, and is
the allocator's.

## Redis 8.10.2

`results/redis-8.10.2-replies.txt` has what Redis 8.10.2 answers to the
sequence in `compat.py` (`RESTORE ... IDLETIME` and `FREQ` and `OBJECT
IDLETIME` and `FREQ` under `noeviction`, `allkeys-lfu` and `allkeys-lru`, and
after `TTL`, `TYPE`, `EXISTS`, `OBJECT`, `TOUCH`, `GET`); KiviDB's answers
are the same line for line, error texts included.

## Running it

```
# instructions per command: on a Linux machine with Docker and the
# kividb-ci-valgrind image (see scripts/perf/run.sh)
WORKLOADS=access LTO=fat N=40000 scripts/perf/run.sh branch
REV=origin/master WORKLOADS=access LTO=fat N=40000 scripts/perf/run.sh master
python3 benchmarks/1.3.0/key-access-tracking/table.py target/perf/out-branch/raw.txt

# bytes per key
python3 benchmarks/1.3.0/key-access-tracking/memory.py --server target/release/kividb

# Redis's replies and KiviDB's
python3 benchmarks/1.3.0/key-access-tracking/compat.py <port>   # against each server
```

`WORKLOADS=access` runs each command with `--key-access-tracking no` and
`yes`, both explicit, so a change of the default does not change what is
measured.

## What counts as a regression

Instruction counts are the same from run to run for a given binary; two
builds differ by up to 1 to 2% in a command nothing changed in (code layout),
which is why the table is built with `LTO=fat`.

- Tracking off: more than 0.3% above master's on a pipelined `GET` or `SET`
  averaged over two runs, or more than 1% on any command here in one.
- Tracking on: more than 1.6% above master's tracking-off count on a pipelined
  `GET`, or 2% on any other command here, over two runs.
- Bytes per key: any difference between the two settings beyond 1 B.
