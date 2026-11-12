# KiviDB 1.2.1 benchmarks

KiviDB 1.2.1 (commit `878746e4`) measured against the 1.2.0 release binary on Azure VMs, 2026-11-09. The point of these runs is to show whether the patch release costs anything, so they compare the two builds only. The competitor numbers are in [../1.2.0](../1.2.0/README.md). Each suite used the 1.2.0 suite's method, harness, VM sizes and load generators. The two builds alternated on the same VMs in one session.

**Result: no regression in the string, list, set, sorted-set, counter or memory numbers.** Most commands are a few percent cheaper per request. Two hash rows got dearer, and both are filed:
- `HSET` at pipeline 16 (#544);
- `HSETEX`'s instruction count (#543).

Each folder holds its README, harness and raw results. The README gives the hardware, the exact builds and their SHA-256s, the method, the spread across runs and the caveats.

| Folder | What it measures | Rounds |
|---|---|---|
| [core](core/README.md) | `GET`/`SET` at 100 B, 1 KB and 2 KB, pipeline 1 and 16, `MGET`/`MSET` of 10 keys; p50/p99/p99.9, two load generators | 4 alternating rounds per build |
| [datatypes](datatypes/README.md) | lists, hashes, sets, sorted sets and counters at 100 B and 2 KB, pipeline 1 and 16 (42 rows), plus 24 new rows on hashes whose fields carry TTLs | 2 alternating rounds (6 runs) per build; 4 more for `HSET` p16 |
| [memory](memory/README.md) | bytes per key as the dataset grows to 50M keys (100 B and 1 KB), the warefeats cell, hashes with field TTLs, idle memory | 2-3 per build |
| [profiling](profiling/README.md) | instructions per command with callgrind, fat LTO: the 18 `WORKLOADS=full` workloads plus 5 hash workloads | 4 runs per build |
| [final](final/README.md) | the final commit `74071531` against 1.2.0: `GET`/`SET`, a mixed workload, `TS.ADD`, vector KNN, and the hash and field-TTL rows | 4 alternating rounds (2 for the hash rows) |
| [vector](vector/README.md) | commit `ccba20e2` against the 1.2.0 release binary, Redis 8.10.2 (with and without io-threads) and Dragonfly 2.0.2: the 1.2.0 vector suite on all four datasets (q/s by ef and clients, recall, latency, ingest, memory per vector and after a delete), and whether the published 1.2.0 numbers reproduce | 3 alternating passes per engine |
| [memory/full](memory/full/README.md) | commit `ccba20e2` against the 1.2.0 release binary, Redis 8.10.3, Valkey 9.1.3 and Dragonfly 2.0.2: bytes per key (warefeats), growth to 50M keys, `BGSAVE` and `BGREWRITEAOF` under 100k writes/s, `maxmemory` during a snapshot (#284), active defrag | 1 round per build (active defrag: 4) |
| [release-checks](release-checks/README.md) | the code 1.2.1 ships (commit `76f06dee`) against 1.2.0: the functional checks and the Redis Tcl suite, the vector and memory/snapshot suites (with 30M-key snapshot load times), the AWS bare-metal headline, and the operator end to end on AKS | per suite: 3 passes (vector), 3 rounds (memory), 4 rounds (bare metal) |

**On the final commit (`74071531`, [final](final/README.md)):** no row is more than 3% slower than 1.2.0. `HSET` at pipeline 16 is now 7.8% faster (#544 fixed by #569), and writes of hash fields with a TTL are 5-44% faster.

## Builds

| build | what | SHA-256 |
|---|---|---|
| 1.2.0 | the published `kividb-linux-x86_64` / `kividb-linux-aarch64` binaries of release v1.2.0, checked against `SHA256SUMS.txt` | x86_64 `a211d544…48edd72f`, aarch64 `e936ce57…77673c` |
| 1.2.1 | `878746e4`, built the way `release.yml` builds a release: `amazonlinux:2023`, rustc 1.99.1, `cargo build --release` (fat LTO), default features, `+lse` on arm64 | x86_64 `b07d5234…bcf63a05`, aarch64 `3355e26e…c35b4a7` |
| 1.2.0 from its tag | core only: v1.2.0 built the same way | byte-identical to the published aarch64 binary |

Profiling needs symbols, so it compares v1.2.0 and `878746e4` built the same way with `debug=1`, not the stripped release binaries. Both 1.2.1 binaries print `kividb 1.2.0`, because the version is bumped at release.

## Headlines: 1.2.1 against 1.2.0

Each figure is the median across rounds; the suite READMEs give the ranges. **C** means the load generators were the limit and **N** the server's NIC.

| | 1.2.1 | 1.2.0 | Δ | Verdict |
|---|---:|---:|---:|---|
| `GET` 100 B, pipeline 16 ([core](core/README.md)) | 10.53M ops/s | 10.50M | +0.3% | same |
| `SET` 100 B, pipeline 16 ([core](core/README.md)) | 8.42M ops/s | 8.38M | +0.5% | same |
| `GET` / `SET` 100 B, pipeline 1 (C) | 1.75M / 1.71M | 1.75M / 1.68M | -0.1% / +1.6% | same |
| `MGET` / `MSET` of 10 keys, pipeline 1 | 1.18M / 575K | 1.15M / 569K | +2.6% / +1.1% | same |
| `GET` / `SET` 1 KB, pipeline 16 (N) | 1.87M / 2.80M | 1.87M / 2.80M | 0.0% / 0.0% | same |
| `GET` / `SET` 2 KB, pipeline 16 (N) | 938K / 1.43M | 938K / 1.43M | +0.1% / 0.0% | same |
| p99 / p99.9, 100 B core rows | | | within 0.05 / 0.07 ms | same |
| Server CPU per request, `GET` / `SET` p16 | 2.85 / 3.35 µs | 2.87 / 3.37 µs | -0.7% / -0.6% | same |
| Lists, sets, sorted sets, counters, `HGET`, pipeline 16 ([datatypes](datatypes/README.md)) | | | -2.4% to +7.5% | same, all rows |
| **`HSET` 100 B, pipeline 16** ([datatypes](datatypes/README.md)) | **4.03M** | **4.44M** | **-9.2%** | **slower** (#544) |
| `HSETEX PX` / `HPEXPIRE`, 100 B, pipeline 16 | 1.57M / 1.67M | 921K / 823K | +71% / +104% | faster |
| `HSETEX PX 50`, fields expiring during the run, 100 B p16 | 1.05M | 583K | +80% | faster |
| `HGET` of fields with TTLs, 100 B p16 | 5.39M | 4.85M | +11% | faster |
| Bytes per key, 100 B, 10M keys, RSS / `used_memory` ([memory](memory/README.md)) | 145.0 / 143.1 | 144.7 / 143.1 | +0.2% / 0 | same |
| Bytes per key, 100 B, 50M keys | 140.7 / 140.1 | 140.6 / 140.1 | 0 / 0 | same |
| Bytes per key, warefeats 1M × 100 B | 149.0 / 146.9 | 149.0 / 146.9 | 0 / 0 | same |
| Bytes per hash field with a TTL (`used_memory`) | 223.4 | 223.4 | 0 | same |
| Instructions per command, `GET` / `SET` 100 B ([profiling](profiling/README.md)) | 3,261 / 3,980 | 3,420 / 4,375 | -4.7% / -9.0% | cheaper |
| Instructions per command, `HSET` / `HGET` / `INCR` | 5,355 / 4,072 / 4,032 | 5,687 / 4,095 / 4,142 | -5.9% / -0.6% / -2.7% | cheaper |
| **Instructions per command, `HSETEX` on fields with a TTL** | **18,171** | **14,081** | **+29.1%** | **dearer** (#543) |

### Why most of it got cheaper

- Keys are handled as bytes, so a command no longer pays for a UTF-8 check of each key (-142 instructions on a `GET`, -207 on a `SET`).
- The command entry path is leaner.
- Hash reads on hashes with field TTLs take the read lock and check the earliest deadline first. 1.2.0 took the write lock and swept on every read, so field-TTL rows gain most under many connections.

## Where 1.2.1 loses

- **`HSET` at 100 B, pipeline 16, is 9% slower** than 1.2.0 (4.03M against 4.44M ops/s over 12 alternating runs, with no overlap between the ranges, and p50 0.49 against 0.44 ms). Each `HSET` takes about 7% more server CPU.
  - `HSET` at pipeline 1, at 2 KB, `HGET` and `HGETALL` are unchanged.
  - Callgrind with one worker thread counts **fewer** instructions per `HSET` in 1.2.1. So the cost probably comes from threads contending on the hash write path changed by #531, not from more work per command.
  - Filed as [#544](https://github.com/kividbio/kividb/issues/544).
- **`HSETEX` costs 29% more instructions per command** with one worker thread. Since #531 it builds its replicated form, about ten allocations, even when no replica, AOF or tap reads it.
  - Under many connections `HSETEX` is still 1.5-2x faster than 1.2.0, because 1.2.0's field-TTL writes contended more.
  - The single-thread cost is real, though, and a lightly loaded server pays it on every call.
  - Filed as [#543](https://github.com/kividbio/kividb/issues/543).
- **Field-TTL writes** still cost about 2.5x a plain `HSET` in both builds.
- **Latency:** p99.9 is 0.2-0.45 ms higher on three pipeline-1 rows whose fields expire during the run.
- **The `LPOP` stall** at 100 B, pipeline 16 is unchanged: p99.9 is 44.5 ms against 46.2 ms ([datatypes](datatypes/README.md)).
- **Idle memory:** the empty engine holds 2 KB more `used_memory` (+0.07%). It is a fixed amount, not per key.
- **Against other engines**, nothing here changes what [1.2.0](../1.2.0/README.md#headlines) found: Garnet is faster on `SET` at pipeline 16 and on `MSET`; Redis Cluster is ahead on `HSET` and `INCR`; Dragonfly uses fewer bytes per key for ASCII values (137 against 149 in warefeats) and less memory during a snapshot. Those were not re-measured.

## Caveats

- **Some rows don't measure the server.**
  - The 1 KB and 2 KB core rows stop at the 16 Gbps NIC, or at the load generators at pipeline 1.
  - The 100 B pipeline-1 rows are client-bound (93-95% load-generator CPU).
  - Those rows can show a regression in CPU per request, not in ops/s.
- **The VM layouts are the 1.2.0 suite's, with these differences:**
  - **datatypes:** ran in eastasia, because koreacentral and japaneast had no regional vCPU quota left beside the other suites.
  - **memory:** its client was a D16ls_v7, not a D32ls_v7, for the same reason. The client doesn't change bytes per key.
  - **memory:** only the growth, warefeats and field-TTL scenarios ran, not the mixed, snapshot, defrag or AOF ones.
- **Absolute numbers differ from the 1.2.0 suite's** (for example 10.5M against 10.8M `GET` p16), because these are different machines on a different day. Only the comparison within this session counts.
- **Profiling noise:** the active-expire thread wakes every 100 ms of wall-clock time and takes a larger share of the count in this session. With it left out, today's 1.2.0 build matches the 1.2.0 suite's published counts to 0.1%.

## Release checks run with these benchmarks

The same commit passed the functional release checks on Azure VMs:
- the Linux gate (`scripts/ci-local.sh full`) on x86_64 and arm64;
- Redis 8.10.2's Tcl suite per unit, with both I/O models;
- replication and Sentinel failover, including mixed 1.2.0 and 1.2.1;
- persistence, including `appendfsync always` with kill -9 and power loss;
- a 3+3 cluster, with `CLUSTER FAILOVER` during `CLUSTER MIGRATION IMPORT` over 44 runs;
- a 30-minute soak with a replica, the AOF, eviction, binary keys and field TTLs.

None of them lost an acknowledged write outside asynchronous replication's documented window, and nothing crashed.
