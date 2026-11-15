# `key-access-tracking yes`: what it costs (#21)

What it costs a command, and a key, to keep when each key was last used (or
how often) under every `maxmemory-policy`, so that `OBJECT IDLETIME`,
`OBJECT FREQ` and `RESTORE ... IDLETIME` work under `noeviction` as in Redis.

## Instructions per command

Whole server, one thread, `--io-model uring`, `maxmemory-policy noeviction`,
callgrind, 40,000 commands per workload, built with `LTO=fat` (the release
profile as shipped). Pipelined is `redis-benchmark -P 16`, not pipelined `-P 1`;
the keys are loaded first. `MGET16` is an `MGET` of 16 keys.

| command | master, tracking off (today's default) | branch, tracking off | branch, tracking on | tracking on vs today |
|---|---:|---:|---:|---:|
| GET 100 B, pipelined | 3234 | 3230 (-0.12%) | 3284 | +50 (+1.55%) |
| SET 100 B, pipelined | 3960 | 3945 (-0.38%) | 4001 | +41 (+1.04%) |
| GET 1 KB, pipelined | 3826 | 3798 (-0.73%) | 3863 | +37 (+0.97%) |
| SET 1 KB, pipelined | 4095 | 4082 (-0.32%) | 4147 | +52 (+1.27%) |
| GET 100 B, not pipelined | 9725 | 9721 (-0.04%) | 9778 | +53 (+0.54%) |
| SET 100 B, not pipelined | 10447 | 10434 (-0.12%) | 10498 | +51 (+0.49%) |
| GET 1 KB, not pipelined | 10311 | 10294 (-0.16%) | 10320 | +9 (+0.09%) |
| SET 1 KB, not pipelined | 10582 | 10573 (-0.09%) | 10631 | +49 (+0.46%) |
| INCR | 4096 | 4072 (-0.59%) | 4131 | +35 (+0.85%) |
| HGET | 3954 | 3938 (-0.40%) | 3994 | +40 (+1.01%) |
| LPUSH | 5299 | 5314 (+0.28%) | 5368 | +69 (+1.30%) |
| LPOP | 4519 | 4503 (-0.35%) | 4554 | +35 (+0.77%) |
| MGET of 16 keys | 20536 | 20506 (-0.15%) | 20559 | +23 (+0.11%) |

Turning tracking on in master, before this change, cost +9.9% on a pipelined
`GET` of 100 bytes (+8.0% on `SET`, +7 to +8% on `INCR`, `HGET`, `LPUSH` and
`LPOP`): about 320 instructions a command. What brought it to about 50:

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

- Tracking off: more than 1% above master's on any command here.
- Tracking on: more than 2% above master's tracking-off count on a pipelined
  `GET` or `SET`, or more than 1% on one that is not pipelined.
- Bytes per key: any difference between the two settings beyond 1 B.
