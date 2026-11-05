# KiviDB 1.2.0: the complete benchmark report

Every number from the 1.2.0 benchmarks and release tests, against every engine they were compared with, in one file. It was measured on Azure VMs and AKS from 2026-11-01 to 2026-11-03.

The tables are generated from each suite's result files (`results/*.csv`, `results/tables.md`). Each suite's README has the harness, the exact flags, the raw data and how to run it again; this report links to them.

**What was measured.**
- **Full runs.** The competitor runs used commit `75482abb`, the code of 1.2.0-rc7.
- **Rechecks.** Every suite was rerun on rc8 (`082dab3d`) and again on rc10 (`39bfacd2`), the code that ships as 1.2.0. Each recheck alternated two builds on the same VMs (section 7).
- **What changed after rc7.** rc8 made `MSET` 3.3x faster; everything else measured level. rc9 and rc10 changed only replication, cluster and shutdown code, plus `MIGRATE`, and measured level on every suite.

So the rc7 numbers below stand for 1.2.0, except `MSET`: section 1 gives the rc8 figure.

**Names used in the tables.**
- *KiviDB 1.2.0*: the release code. Row `kividb-cand` / `kividb-75482ab` in the CSVs, "Candidate" in the memory tables.
- *KiviDB 1.1.3*: the previous release.
- *Redis*: 8.10.3 (8.10.2 in vector). *Valkey*: 9.1.3. *Dragonfly*: 2.0.2. *Garnet*: 2.2.5. *kvrocks*: 2.17.0.
- 1.2.0-rc4 rows in the CSVs were the baseline of an earlier round. They are left out of the competitor tables; the memory and vector sections keep their tables against rc4.

## Contents

1. [Core strings](#1-core-strings-get-set-mget-mset): `GET`, `SET`, `MGET`, `MSET`, pipeline depth, CPU per request
2. [Data types](#2-data-types-lists-hashes-sets-sorted-sets-counters): lists, hashes, sets, sorted sets, counters; memory by type
3. [Profiling](#3-profiling-where-the-cpu-goes): instructions per command, CPU split
4. [Memory](#4-memory): bytes per key, growth, mixed load, snapshots, eviction, active defrag, AOF
5. [Vector search](#5-vector-search)
6. [KiviDB as a cache (#202)](#6-kividb-as-a-cache-issue-202)
7. [The rc8 and rc10 rechecks](#7-the-rc8-and-rc10-rechecks)
8. [Release tests](#8-release-tests-smoke-soak-failover-operator): smoke, soak, failover, operator
9. [Where KiviDB loses](#9-where-kividb-loses)
10. [What is not covered](#10-what-is-not-covered)

## Summary

| | KiviDB 1.2.0 | Best other | Redis |
|---|---|---|---|
| `GET` 100 B, pipeline 16 | 10.45M ops/s | Garnet 10.69M | 1.41M |
| `SET` 100 B, pipeline 16 | 7.94M ops/s | Garnet 9.83M | 1.25M |
| `MSET` of 10 keys (rc8) | 569K commands/s | Garnet 800K | 144K |
| Lists, hashes, sets, sorted sets, counters, pipeline 16 | 4.2-6.6M ops/s | Redis Cluster x16: level, ahead on `HSET` and `INCR` (client-bound) | 3.7-6.0x less |
| Bytes per key, 100 B values, 50M keys | 140.7 | Dragonfly 141.5 | 174.4 |
| Bytes per key, warefeats harness | 149.2 | Dragonfly 137.1 | 166.3 |
| Memory added by `BGSAVE`, 30M keys of 1 KB under writes | +10% | Dragonfly +0.4% | +89% |
| Filtered vector search, 100 clients | 4,478 q/s | Dragonfly 811 | 1,802 |
| Cache hit rate, #202 | 60.96% | | 60.78% |
| Acknowledged writes lost to kills, failovers, power loss and forced deletes, outside a promotion past unreplicated writes | 0 | | 0 |

## Hardware

Each suite had its own proximity placement group, with the server and the load generators on separate VMs. These are cloud VMs, not the bare-metal machines behind the project README's numbers, so absolute numbers are lower. Engines are compared on the same machines in the same hour.

| Suite | Server | Load generator | Region |
|---|---|---|---|
| core | `Standard_D32ps_v5`, 32 vCPU Ampere Altra (arm64), 128 GB | 2 x `Standard_D16ps_v5`, memtier_benchmark 2.5.2 | eastasia (rc8 recheck: centralindia) |
| datatypes | `Standard_D16ds_v5`, 16 vCPU Xeon Platinum 8370C | `Standard_D48s_v5` | koreacentral |
| profiling | `Standard_D16ds_v5` (callgrind on a `Standard_D8s_v5`) | `Standard_D48s_v5` | koreacentral, japaneast |
| memory | pair A: `Standard_E32ds_v7`, Xeon 6973P-C, 251 GiB, local NVMe; pair B: `Standard_E32ds_v6`, Xeon Platinum 8573C | pair A: `Standard_D32ls_v7`; pair B: `Standard_D16ls_v6` | germanywestcentral, polandcentral |
| vector | `Standard_D32ps_v5`, 32 vCPU Ampere Altra (arm64), 128 GB | `Standard_D16ps_v5` | swedencentral |
| cache-pi | `Standard_D4pls_v5` x 6 pairs (the reporter used a Raspberry Pi 4B) | `Standard_D4pls_v5` | see README |


## 1. Core strings: GET, SET, MGET, MSET

From [core](core/README.md). Median of at least three rounds from two load generators at once. Latency is p50 / p99 / p99.9 in ms from the two generators' histograms merged. *Server CPU* is the share of the server's cores busy. *Limit* marks rows where the load generators (`client`) were the bottleneck, so the number is a floor. At 1 KB and 2 KB with pipelining, every fast engine stops at the server's NIC.


### 100 B values

| Engine | Workload | ops/s | min-max | p50 / p99 / p99.9 ms | Server CPU | Limit |
|---|---|---:|---:|---:|---:|---|
| KiviDB 1.2.0 | GET, pipeline 1 | 1.73M | 1.72M-1.76M | 0.68 / 1.03 / 1.89 | 95% | client |
| KiviDB 1.1.3 | GET, pipeline 1 | 1.75M | 1.73M-1.77M | 0.68 / 1.04 / 1.97 | 95% | client |
| Garnet 2.2.5 | GET, pipeline 1 | 1.05M | 1.05M-1.07M | 0.61 / 6.43 / 10.11 | 96% |  |
| Dragonfly 2.0.2 | GET, pipeline 1 | 1.65M | 1.64M-1.65M | 0.72 / 1.11 / 2.17 | 100% | client |
| Redis Cluster 8.10.3 (32 shards) | GET, pipeline 1 | 820K | 802K-831K | 20.73 / 28.03 / 36.61 | 78% | client |
| Redis 8.10.3, io-threads 24 | GET, pipeline 1 | 764K | 724K-767K | 1.57 / 2.11 / 3.62 | 48% |  |
| Valkey 9.1.3, io-threads 24 | GET, pipeline 1 | 601K | 582K-607K | 1.98 / 2.75 / 3.92 | 35% |  |
| KiviDB 1.2.0 | GET, pipeline 16 | 10.45M | 10.41M-10.59M | 0.44 / 0.78 / 1.23 | 94% |  |
| KiviDB 1.1.3 | GET, pipeline 16 | 10.24M | 10.19M-10.45M | 0.46 / 0.80 / 1.25 | 94% |  |
| Garnet 2.2.5 | GET, pipeline 16 | 10.69M | 10.67M-10.96M | 0.38 / 1.72 / 3.20 | 99% | client |
| Dragonfly 2.0.2 | GET, pipeline 16 | 5.66M | 5.64M-5.71M | 0.83 / 1.33 / 1.76 | 99% |  |
| Redis Cluster 8.10.3 (32 shards) | GET, pipeline 16 | 5.70M | 5.56M-5.76M | 20.73 / 24.96 / 26.75 | 77% | client |
| Redis 8.10.3, io-threads 24 | GET, pipeline 16 | 1.41M | 1.33M-1.45M | 3.41 / 4.00 / 6.91 | 14% |  |
| Valkey 9.1.3, io-threads 24 | GET, pipeline 16 | 1.20M | 1.20M-1.26M | 4.01 / 4.70 / 4.83 | 28% |  |
| KiviDB 1.2.0 | SET, pipeline 1 | 1.70M | 1.68M-1.72M | 0.69 / 1.06 / 2.00 | 94% | client |
| KiviDB 1.1.3 | SET, pipeline 1 | 1.70M | 1.69M-1.72M | 0.69 / 1.05 / 1.94 | 95% | client |
| Garnet 2.2.5 | SET, pipeline 1 | 1.04M | 1.03M-1.05M | 0.61 / 6.11 / 9.92 | 94% |  |
| Dragonfly 2.0.2 | SET, pipeline 1 | 1.62M | 1.61M-1.63M | 0.73 / 1.13 / 2.27 | 100% | client |
| Redis Cluster 8.10.3 (32 shards) | SET, pipeline 1 | 945K | 893K-973K | 17.02 / 26.88 / 30.08 | 79% | client |
| Redis 8.10.3, io-threads 24 | SET, pipeline 1 | 692K | 667K-701K | 1.73 / 2.30 / 3.79 | 42% |  |
| Valkey 9.1.3, io-threads 24 | SET, pipeline 1 | 424K | 418K-436K | 2.83 / 3.66 / 4.45 | 32% |  |
| KiviDB 1.2.0 | SET, pipeline 16 | 7.94M | 7.88M-8.02M | 0.59 / 0.99 / 1.27 | 88% |  |
| KiviDB 1.1.3 | SET, pipeline 16 | 8.19M | 8.19M-8.20M | 0.57 / 0.97 / 1.26 | 89% |  |
| Garnet 2.2.5 | SET, pipeline 16 | 9.83M | 9.81M-9.83M | 0.38 / 2.40 / 4.06 | 99% |  |
| Dragonfly 2.0.2 | SET, pipeline 16 | 4.87M | 4.80M-4.88M | 0.97 / 1.69 / 4.99 | 99% |  |
| Redis Cluster 8.10.3 (32 shards) | SET, pipeline 16 | 5.79M | 5.76M-5.89M | 20.09 / 25.73 / 32.64 | 84% | client |
| Redis 8.10.3, io-threads 24 | SET, pipeline 16 | 1.25M | 1.23M-1.31M | 3.85 / 4.42 / 7.71 | 12% |  |
| Valkey 9.1.3, io-threads 24 | SET, pipeline 16 | 723K | 714K-734K | 6.66 / 7.55 / 7.81 | 26% |  |

### 1 KB values

| Engine | Workload | ops/s | min-max | p50 / p99 / p99.9 ms | Server CPU | Limit |
|---|---|---:|---:|---:|---:|---|
| KiviDB 1.2.0 | GET, pipeline 1 | 1.63M | 1.59M-1.65M | 0.73 / 1.08 / 2.10 | 95% | client+nic |
| KiviDB 1.1.3 | GET, pipeline 1 | 1.64M | 1.58M-1.66M | 0.72 / 1.07 / 2.02 | 95% | client+nic |
| Garnet 2.2.5 | GET, pipeline 1 | 1.00M | 985K-1.01M | 0.63 / 6.17 / 9.73 | 94% |  |
| Dragonfly 2.0.2 | GET, pipeline 1 | 1.50M | 1.50M-1.50M | 0.79 / 1.21 / 2.54 | 100% | client |
| Redis Cluster 8.10.3 (32 shards) | GET, pipeline 1 | 777K | 768K-781K | 21.38 / 28.80 / 36.86 | 78% | client |
| Redis 8.10.3, io-threads 24 | GET, pipeline 1 | 689K | 679K-694K | 1.74 / 2.32 / 3.81 | 46% |  |
| Valkey 9.1.3, io-threads 24 | GET, pipeline 1 | 573K | 570K-592K | 2.10 / 2.90 / 3.97 | 39% |  |
| KiviDB 1.2.0 | GET, pipeline 16 | 1.87M | 1.87M-3.36M | 1.30 / 7.09 / 7.78 | 49% | nic |
| KiviDB 1.1.3 | GET, pipeline 16 | 1.87M | 1.87M-1.88M | 1.29 / 7.13 / 7.81 | 49% | nic |
| Garnet 2.2.5 | GET, pipeline 16 | 1.86M | 1.86M-1.86M | 1.23 / 8.32 / 9.79 | 48% | nic |
| Dragonfly 2.0.2 | GET, pipeline 16 | 1.86M | 1.86M-1.86M | 1.73 / 6.46 / 9.34 | 69% | nic |
| Redis Cluster 8.10.3 (32 shards) | GET, pipeline 16 | 1.82M | 1.82M-1.82M | 16.77 / 258.05 / 520.19 | 82% | client+nic |
| Redis 8.10.3, io-threads 24 | GET, pipeline 16 | 1.16M | 1.15M-1.19M | 3.21 / 6.40 / 6.75 | 76% |  |
| Valkey 9.1.3, io-threads 24 | GET, pipeline 16 | 1.14M | 1.08M-1.16M | 3.76 / 11.13 / 11.90 | 63% |  |
| KiviDB 1.2.0 | SET, pipeline 1 | 1.61M | 1.55M-1.61M | 0.73 / 1.12 / 2.31 | 95% | client+nic |
| KiviDB 1.1.3 | SET, pipeline 1 | 1.62M | 1.61M-1.62M | 0.73 / 1.09 / 2.16 | 95% | client+nic |
| Garnet 2.2.5 | SET, pipeline 1 | 978K | 972K-986K | 0.66 / 6.53 / 10.43 | 95% |  |
| Dragonfly 2.0.2 | SET, pipeline 1 | 1.44M | 1.43M-1.45M | 0.82 / 1.26 / 2.61 | 100% | client |
| Redis Cluster 8.10.3 (32 shards) | SET, pipeline 1 | 954K | 890K-1.01M | 15.81 / 28.03 / 31.49 | 79% | client |
| Redis 8.10.3, io-threads 24 | SET, pipeline 1 | 643K | 611K-645K | 1.86 / 2.43 / 3.97 | 42% |  |
| Valkey 9.1.3, io-threads 24 | SET, pipeline 1 | 623K | 611K-637K | 1.91 / 2.70 / 3.82 | 43% |  |
| KiviDB 1.2.0 | SET, pipeline 16 | 2.80M | 2.79M-2.80M | 0.71 / 7.28 / 7.71 | 59% | nic |
| KiviDB 1.1.3 | SET, pipeline 16 | 2.80M | 2.80M-2.80M | 0.69 / 7.33 / 7.71 | 61% | nic |
| Garnet 2.2.5 | SET, pipeline 16 | 2.80M | 2.80M-2.80M | 0.66 / 7.20 / 9.21 | 50% | nic |
| Dragonfly 2.0.2 | SET, pipeline 16 | 2.79M | 2.79M-2.80M | 1.25 / 5.44 / 7.29 | 88% | nic |
| Redis Cluster 8.10.3 (32 shards) | SET, pipeline 16 | 2.73M | 2.73M-2.73M | 33.02 / 88.58 / 247.81 | 96% | client+nic |
| Redis 8.10.3, io-threads 24 | SET, pipeline 16 | 1.09M | 1.07M-1.09M | 4.08 / 8.32 / 8.70 | 76% |  |
| Valkey 9.1.3, io-threads 24 | SET, pipeline 16 | 1000K | 980K-1.01M | 4.42 / 12.35 / 13.25 | 76% |  |

### 2 KB values

| Engine | Workload | ops/s | min-max | p50 / p99 / p99.9 ms | Server CPU | Limit |
|---|---|---:|---:|---:|---:|---|
| KiviDB 1.2.0 | GET, pipeline 1 | 920K | 920K-920K | 1.15 / 3.04 / 4.25 | 79% | nic |
| KiviDB 1.1.3 | GET, pipeline 1 | 919K | 919K-920K | 1.17 / 3.01 / 4.35 | 79% | nic |
| Garnet 2.2.5 | GET, pipeline 1 | 890K | 889K-897K | 0.83 / 6.21 / 9.73 | 94% | nic |
| Dragonfly 2.0.2 | GET, pipeline 1 | 921K | 920K-921K | 1.12 / 3.06 / 4.35 | 86% | nic |
| Redis Cluster 8.10.3 (32 shards) | GET, pipeline 1 | 700K | 698K-701K | 24.06 / 32.77 / 37.38 | 75% | client |
| Redis 8.10.3, io-threads 24 | GET, pipeline 1 | 651K | 642K-670K | 1.82 / 2.64 / 4.83 | 49% |  |
| Valkey 9.1.3, io-threads 24 | GET, pipeline 1 | 574K | 534K-574K | 2.08 / 2.94 / 4.38 | 41% |  |
| KiviDB 1.2.0 | GET, pipeline 16 | 938K | 937K-939K | 7.07 / 9.21 / 9.86 | 43% | nic |
| KiviDB 1.1.3 | GET, pipeline 16 | 938K | 937K-938K | 7.07 / 9.21 / 9.92 | 43% | nic |
| Garnet 2.2.5 | GET, pipeline 16 | 936K | 935K-936K | 7.07 / 9.54 / 10.30 | 43% | nic |
| Dragonfly 2.0.2 | GET, pipeline 16 | 934K | 934K-935K | 6.82 / 9.73 / 11.20 | 55% | nic |
| Redis Cluster 8.10.3 (32 shards) | GET, pipeline 16 | 914K | 913K-914K | 11.13 / 532.48 / 1073.15 | 70% | nic |
| Redis 8.10.3, io-threads 24 | GET, pipeline 16 | 938K | 937K-938K | 5.12 / 9.41 / 10.05 | 60% | nic |
| Valkey 9.1.3, io-threads 24 | GET, pipeline 16 | 936K | 936K-936K | 4.77 / 10.18 / 11.97 | 69% | nic |
| KiviDB 1.2.0 | SET, pipeline 1 | 1.24M | 1.23M-1.26M | 0.95 / 1.43 / 2.97 | 95% | client+nic |
| KiviDB 1.1.3 | SET, pipeline 1 | 1.14M | 1.13M-1.14M | 1.03 / 1.71 / 3.20 | 91% | nic |
| Garnet 2.2.5 | SET, pipeline 1 | 819K | 791K-821K | 1.08 / 7.52 / 11.97 | 99% | nic |
| Dragonfly 2.0.2 | SET, pipeline 1 | 1.08M | 1.08M-1.08M | 1.09 / 1.76 / 3.17 | 100% | nic |
| Redis Cluster 8.10.3 (32 shards) | SET, pipeline 1 | 872K | 841K-953K | 16.32 / 30.21 / 35.58 | 86% | client |
| Redis 8.10.3, io-threads 24 | SET, pipeline 1 | 560K | 560K-574K | 2.14 / 2.75 / 4.45 | 50% |  |
| Valkey 9.1.3, io-threads 24 | SET, pipeline 1 | 541K | 538K-556K | 2.30 / 3.46 / 4.38 | 57% |  |
| KiviDB 1.2.0 | SET, pipeline 16 | 1.43M | 1.43M-1.43M | 0.94 / 9.18 / 10.14 | 43% | nic |
| KiviDB 1.1.3 | SET, pipeline 16 | 1.43M | 1.43M-1.43M | 0.97 / 9.09 / 9.98 | 46% | nic |
| Garnet 2.2.5 | SET, pipeline 16 | 1.43M | 1.43M-1.43M | 1.29 / 9.66 / 10.43 | 35% | nic |
| Dragonfly 2.0.2 | SET, pipeline 16 | 1.43M | 1.43M-1.43M | 1.70 / 8.64 / 9.28 | 61% | nic |
| Redis Cluster 8.10.3 (32 shards) | SET, pipeline 16 | 1.40M | 1.39M-1.40M | 22.27 / 221.18 / 466.94 | 90% | client+nic |
| Redis 8.10.3, io-threads 24 | SET, pipeline 16 | 967K | 926K-1.10M | 5.38 / 5.98 / 6.24 | 76% | nic |
| Valkey 9.1.3, io-threads 24 | SET, pipeline 16 | 806K | 800K-836K | 6.69 / 7.62 / 7.81 | 76% |  |

### MGET and MSET of 10 keys, 100 B, pipeline 1 (commands/s; keys/s is ten times that)

| Engine | MGET10 | p50 / p99 / p99.9 ms | MSET10 | p50 / p99 / p99.9 ms |
|---|---:|---:|---:|---:|
| KiviDB 1.2.0 | 1.16M | 1.02 / 1.46 / 3.76 | 166K | 7.26 / 9.95 / 10.97 |
| KiviDB 1.1.3 | 1.12M | 1.05 / 1.53 / 3.90 | 153K | 7.78 / 12.22 / 14.02 |
| Garnet 2.2.5 | 788K | 0.86 / 7.13 / 11.46 | 761K | 0.91 / 7.42 / 11.71 |
| Dragonfly 2.0.2 | 640K | 1.85 / 2.85 / 5.02 | 557K | 2.08 / 4.48 / 6.82 |
| Redis 8.10.3, io-threads 24 | 136K | 8.89 / 9.79 / 17.54 | 150K | 8.03 / 8.77 / 15.81 |
| Valkey 9.1.3, io-threads 24 | 145K | 8.26 / 9.28 / 9.66 | 94K | 12.80 / 14.27 / 14.78 |

The MSET row above is the code before the MSET fix. In 1.2.0 as shipped (rc8 onwards), [core/rc8](core/rc8/README.md) measured on the same VM shapes, with the competitors in the same session:

| Engine | MSET10 commands/s | MSET10 p99 ms | MGET10 commands/s |
|---|---:|---:|---:|
| KiviDB 1.2.0 (rc8) | 569K | 2.89 | 1.14M (client-bound) |
| KiviDB before the fix (rc7) | 172K | 9.63 | 1.14M (client-bound) |
| Garnet 2.2.5 | 800K | 7.90 | 832K |
| Dragonfly 2.0.2 | 561K | 4.38 | 618K |
| Redis 8.10.3, io-threads 24 | 144K | 9.54 | 135K |

In that session KiviDB's MSET p99.9 was 5.31 ms, against Garnet's 12.54 ms. Garnet's p50 was lower (0.88 against 2.11 ms). The competitors had one round each, against three in the full run.


### GET throughput by pipeline depth, 100 B, 300 connections

| Engine | depth 1 | depth 4 | depth 16 | depth 64 |
|---|---:|---:|---:|---:|
| KiviDB 1.2.0 | 1.21M | 4.23M | 10.45M | 16.72M C |
| KiviDB 1.1.3 | 1.22M | 4.19M | 10.24M | 16.28M C |
| Garnet 2.2.5 | 1.06M | 3.86M | 10.69M C | 17.23M C |
| Dragonfly 2.0.2 | 1.09M | 2.99M | 5.66M | 8.07M |
| Redis Cluster 8.10.3 (32 shards) | 1.22M C | 2.41M C | 5.70M C | 8.62M C |
| Redis 8.10.3, io-threads 24 | 563K | 1.03M | 1.41M | 1.27M |
| Valkey 9.1.3, io-threads 24 | 578K | 900K | 1.20M | 1.58M |

*C*: client-bound.


### Server CPU per request, 100 B (µs, user / kernel)

| Engine | GET p1 | GET p16 | SET p1 | SET p16 |
|---|---:|---:|---:|---:|
| KiviDB 1.2.0 | 17.52 (3.27 / 14.23) | 2.88 (1.34 / 1.52) | 17.86 (3.51 / 14.34) | 3.56 (1.71 / 1.84) |
| KiviDB 1.1.3 | 17.38 (3.24 / 14.24) | 2.94 (1.42 / 1.52) | 17.84 (3.52 / 14.29) | 3.46 (1.63 / 1.83) |
| Garnet 2.2.5 | 29.17 (6.72 / 22.51) | 2.95 (1.14 / 1.81) | 29.00 (7.03 / 22.20) | 3.24 (1.18 / 2.05) |
| Dragonfly 2.0.2 | 19.41 (6.15 / 13.26) | 5.62 (3.57 / 2.05) | 19.70 (6.51 / 13.27) | 6.52 (4.14 / 2.44) |
| Redis Cluster 8.10.3 (32 shards) | 30.24 (5.97 / 24.27) | 4.31 (1.47 / 2.83) | 26.75 (5.52 / 21.30) | 4.65 (1.67 / 2.99) |
| Redis 8.10.3, io-threads 24 | 20.24 (3.63 / 16.57) | 3.17 (1.40 / 1.75) | 19.54 (3.42 / 16.07) | 3.07 (1.51 / 1.54) |
| Valkey 9.1.3, io-threads 24 | 19.42 (6.82 / 12.60) | 7.58 (6.44 / 1.23) | 24.08 (10.79 / 13.21) | 11.11 (9.63 / 1.46) |

## 2. Data types: lists, hashes, sets, sorted sets, counters

From [datatypes](datatypes/README.md). Median of three runs, ops/s, with p99 in ms below it. Redis runs with io-threads 8 and Valkey with io-threads 6 (each chosen by a sweep); Redis Cluster has 16 shards, one per vCPU. kvrocks keeps its data in RocksDB on disk, a different class of store.


### 100 B values, pipeline 1

| Command | KiviDB 1.2.0 | KiviDB 1.1.3 | Redis Cluster x16 | Dragonfly 2.0.2 | Redis 8.10.3 | Valkey 9.1.3 | kvrocks 2.17.0 |
|---|---:|---:|---:|---:|---:|---:|---:|
| LPUSH | 1.22M<br>0.68 | 1.23M<br>0.75 | 1.05M<br>1.97 | 1.05M<br>0.79 | 498K<br>1.51 | 512K<br>1.47 | 213K<br>6.94 |
| LPOP | 1.22M<br>0.69 | 1.23M<br>0.69 | 978K<br>2.00 | 990K<br>0.78 | 445K<br>1.66 | 463K<br>1.58 | 162K<br>6.94 |
| LRANGE_100 | 159K<br>9.28 | 155K<br>9.28 | 179K<br>30.34 | 148K<br>8.77 | 151K<br>7.29 | 148K<br>7.74 | 130K<br>7.07 |
| HSET | 1.18M<br>0.69 | 1.21M<br>0.68 | 1.15M<br>1.76 | 1.03M<br>0.76 | 615K<br>1.50 | 580K<br>1.29 | 282K<br>4.86 |
| HGET | 1.23M<br>0.69 | 1.25M<br>0.69 | 1.04M<br>6.56 | 1.09M<br>0.81 | 645K<br>1.42 | 629K<br>1.14 | 642K<br>1.71 |
| HGETALL | 1.05M<br>0.77 | 1.06M<br>0.73 | 731K<br>6.94 | 825K<br>0.92 | 410K<br>1.69 | 222K<br>2.90 | 436K<br>2.40 |
| SADD | 1.22M<br>0.68 | 1.22M<br>0.67 | 1.20M<br>3.81 | 1.07M<br>0.74 | 559K<br>1.51 | 600K<br>1.22 | 506K<br>2.11 |
| SISMEMBER | 1.23M<br>0.68 | 1.26M<br>0.65 | 1.10M<br>6.50 | 1.13M<br>0.73 | 691K<br>1.35 | 625K<br>1.27 | 573K<br>1.90 |
| ZADD | 1.16M<br>0.69 | 1.16M<br>0.73 | 1.15M<br>2.33 | 946K<br>0.81 | 455K<br>1.60 | 536K<br>1.67 | 494K<br>2.22 |
| ZRANGE | 1.04M<br>0.81 | 1.05M<br>0.78 | 834K<br>8.00 | 773K<br>0.97 | 355K<br>1.86 | 379K<br>1.70 | 310K<br>2.99 |
| INCR | 1.26M<br>0.72 | 1.25M<br>0.70 | 1.22M<br>1.71 | 1.11M<br>0.71 | 877K<br>1.10 | 648K<br>1.11 | 334K<br>3.57 |

### 100 B values, pipeline 16

| Command | KiviDB 1.2.0 | KiviDB 1.1.3 | Redis Cluster x16 | Dragonfly 2.0.2 | Redis 8.10.3 | Valkey 9.1.3 | kvrocks 2.17.0 |
|---|---:|---:|---:|---:|---:|---:|---:|
| LPUSH | 4.77M<br>0.71 | 5.13M<br>0.69 | 4.68M<br>10.05 | 3.26M<br>1.22 | 817K<br>4.96 | 712K<br>3.21 | 382K<br>12.73 |
| LPOP | 4.15M<br>0.62 | 5.10M<br>0.56 | 5.33M<br>10.05 | 3.06M<br>1.04 | 689K<br>4.03 | 638K<br>4.16 | 275K<br>17.54 |
| LRANGE_100 | 151K<br>39.94 | 154K<br>40.19 | 144K<br>421.89 | 149K<br>39.42 | 149K<br>29.57 | 137K<br>21.38 | 127K<br>26.50 |
| HSET | 4.39M<br>0.69 | 4.68M<br>0.69 | 5.80M<br>8.03 | 3.32M<br>1.00 | 974K<br>4.16 | 832K<br>2.64 | 459K<br>10.37 |
| HGET | 5.62M<br>0.57 | 5.72M<br>0.55 | 5.35M<br>7.74 | 3.53M<br>0.85 | 1.00M<br>2.83 | 1.03M<br>2.29 | 1.48M<br>2.33 |
| HGETALL | 1.37M<br>6.46 | 1.37M<br>6.53 | 1.34M<br>40.19 | 1.40M<br>3.15 | 553K<br>5.47 | 338K<br>6.56 | 623K<br>6.30 |
| SADD | 5.31M<br>0.66 | 5.61M<br>0.60 | 3.84M<br>8.77 | 3.29M<br>1.02 | 1.00M<br>4.13 | 892K<br>3.39 | 747K<br>4.61 |
| SISMEMBER | 5.56M<br>0.60 | 5.66M<br>0.59 | 5.29M<br>7.87 | 3.95M<br>0.85 | 1.13M<br>3.63 | 1.07M<br>2.13 | 1.10M<br>2.91 |
| ZADD | 4.37M<br>0.69 | 4.36M<br>0.77 | 1.97M<br>23.55 | 2.23M<br>1.65 | 802K<br>5.05 | 770K<br>3.55 | 856K<br>6.05 |
| ZRANGE | 1.36M<br>6.37 | 1.36M<br>6.43 | 1.36M<br>40.19 | 1.35M<br>2.91 | 461K<br>5.60 | 435K<br>5.21 | 451K<br>8.38 |
| INCR | 6.56M<br>0.52 | 6.08M<br>0.60 | 7.51M<br>5.98 | 4.56M<br>0.69 | 1.79M<br>1.52 | 1.71M<br>1.42 | 623K<br>7.93 |

### 2 KB values, pipeline 1

| Command | KiviDB 1.2.0 | KiviDB 1.1.3 | Redis Cluster x16 | Dragonfly 2.0.2 | Redis 8.10.3 | Valkey 9.1.3 | kvrocks 2.17.0 |
|---|---:|---:|---:|---:|---:|---:|---:|
| LPUSH | 821K<br>1.02 | 845K<br>1.00 | 439K<br>3.18 | 763K<br>1.37 | 263K<br>2.42 | 271K<br>2.25 | 85K<br>10.81 |
| LPOP | 808K<br>1.95 | 813K<br>2.19 | 231K<br>2.21 | 710K<br>1.11 | 346K<br>2.11 | 355K<br>2.00 | 220K<br>5.63 |
| LRANGE_100 | 8K<br>181.25 | 8K<br>181.25 | 8K<br>933.89 | 9K<br>189.44 | 8K<br>181.25 | 8K<br>200.70 | 8K<br>181.25 |
| HSET | 824K<br>1.01 | 830K<br>1.07 | 586K<br>2.32 | 767K<br>1.05 | 392K<br>1.70 | 406K<br>1.66 | 87K<br>11.58 |
| HGET | 759K<br>2.99 | 750K<br>2.94 | 752K<br>7.90 | 753K<br>2.25 | 461K<br>1.88 | 491K<br>1.43 | 485K<br>2.32 |
| HGETALL | 79K<br>19.33 | 79K<br>19.33 | 85K<br>220.16 | 80K<br>19.20 | 79K<br>18.43 | 71K<br>9.21 | 99K<br>17.28 |
| SADD | 904K<br>0.96 | 907K<br>0.89 | 322K<br>3.42 | 792K<br>1.04 | 321K<br>2.16 | 345K<br>2.21 | 320K<br>4.86 |
| SISMEMBER | 920K<br>0.90 | 924K<br>0.94 | 674K<br>9.66 | 817K<br>1.01 | 373K<br>1.76 | 389K<br>1.70 | 427K<br>2.43 |
| ZADD | 846K<br>1.11 | 865K<br>0.96 | 299K<br>3.58 | 723K<br>1.14 | 282K<br>2.40 | 314K<br>2.16 | 332K<br>2.99 |
| ZRANGE | 78K<br>19.33 | 79K<br>19.33 | 92K<br>221.18 | 77K<br>19.20 | 79K<br>18.05 | 79K<br>18.05 | 98K<br>11.20 |

### 2 KB values, pipeline 16

| Command | KiviDB 1.2.0 | KiviDB 1.1.3 | Redis Cluster x16 | Dragonfly 2.0.2 | Redis 8.10.3 | Valkey 9.1.3 | kvrocks 2.17.0 |
|---|---:|---:|---:|---:|---:|---:|---:|
| LPUSH | 1.51M<br>3.46 | 1.19M<br>3.60 | 1.51M<br>18.43 | 1.74M<br>3.15 | 375K<br>6.17 | 379K<br>6.50 | 94K<br>55.30 |
| LPOP | 813K<br>7.97 | 818K<br>8.06 | 981K<br>49.66 | 833K<br>7.20 | 445K<br>7.74 | 430K<br>7.33 | 301K<br>16.89 |
| LRANGE_100 | 8K<br>481.28 | 8K<br>481.28 | 8K<br>1777.66 | 8K<br>481.28 | 8K<br>481.28 | 8K<br>481.28 | 9K<br>430.08 |
| HSET | 1.47M<br>5.21 | 1.50M<br>5.44 | 1.99M<br>20.61 | 1.51M<br>4.54 | 554K<br>4.19 | 568K<br>5.05 | 145K<br>39.42 |
| HGET | 764K<br>8.83 | 760K<br>8.77 | 747K<br>80.38 | 792K<br>8.26 | 600K<br>4.58 | 613K<br>3.82 | 891K<br>4.45 |
| HGETALL | 78K<br>79.87 | 77K<br>78.85 | 75K<br>790.53 | 79K<br>78.85 | 78K<br>79.87 | 79K<br>69.63 | 96K<br>70.14 |
| SADD | 1.65M<br>6.40 | 1.69M<br>6.53 | 1.16M<br>20.61 | 1.73M<br>4.58 | 331K<br>7.93 | 464K<br>7.07 | 374K<br>12.93 |
| SISMEMBER | 1.52M<br>6.43 | 1.50M<br>6.56 | 1.54M<br>40.19 | 1.68M<br>5.28 | 514K<br>4.51 | 529K<br>4.61 | 573K<br>5.73 |
| ZADD | 1.65M<br>5.98 | 1.60M<br>6.01 | 979K<br>28.03 | 1.71M<br>2.38 | 299K<br>8.89 | 427K<br>7.17 | 307K<br>17.28 |
| ZRANGE | 78K<br>79.36 | 81K<br>79.87 | 74K<br>761.86 | 81K<br>79.87 | 78K<br>79.87 | 79K<br>79.36 | 91K<br>69.63 |

### Memory by type (RSS above the empty server, MB)

The same keys loaded into each engine. kvrocks also shows its disk use.

| Engine | hash-100 | hash-2048 | set-100 | set-2048 | zset-100 | zset-2048 |
|---|---:|---:|---:|---:|---:|---:|
| KiviDB 1.2.0 | 253 | 2115 | 175 | 2037 | 218 | 2071 |
| KiviDB 1.1.3 | 253 | 2114 | 175 | 2036 | 218 | 2070 |
| Redis Cluster x16 | 143 | 2518 | 146 | 2505 | 250 | 2560 |
| Dragonfly 2.0.2 | 142 | 2577 | 129 | 2562 | 159 | 2599 |
| Redis 8.10.3 | 141 | 2517 | 146 | 2504 | 250 | 2560 |
| Valkey 9.1.3 | 140 | 2504 | 139 | 2493 | 245 | 2550 |
| kvrocks 2.17.0 | 84 (disk 281) | 68 (disk 2871) | 95 (disk 259) | 456 (disk 2675) | 128 (disk 464) | 308 (disk 5844) |

## 3. Profiling: where the CPU goes

From [profiling](profiling/README.md).


### Instructions per command (callgrind, one thread, whole server path)

| Workload | KiviDB 1.2.0 | KiviDB 1.1.3 | Redis 8.10.3 | KiviDB vs Redis |
|---|---:|---:|---:|---:|
| GET-100 | 3,339 | 3,250 | 4,139 | -19% |
| SET-100 | 4,276 | 3,947 | 5,359 | -20% |
| GET-1K | 3,833 | 3,837 | 4,250 | -10% |
| SET-1K | 4,380 | 4,041 | 5,498 | -20% |
| MIX-100 | 7,625 | 7,218 | 9,559 | -20% |
| INCR | 4,086 | 4,319 | 4,489 | -9% |
| DEL | 4,556 | 4,038 | 4,483 | +2% |
| GET-100-TTL | 3,402 | 3,560 | 4,165 | -18% |
| SET-100-TTL | 5,657 | 5,996 |  |  |
| INCR-TTL | 4,154 | 4,560 |  |  |
| DEL-TTL | 5,165 | 5,002 |  |  |
| GET-100-LRU | 3,687 | 3,705 |  |  |
| SET-100-LRU | 4,627 | 4,436 |  |  |
| INCR-LRU | 4,431 | 4,746 |  |  |
| DEL-LRU | 4,826 | 4,630 |  |  |
| GET-100-LFU | 3,895 | 3,709 |  |  |
| SET-100-LFU | 4,963 | 4,438 |  |  |
| INCR-LFU | 4,721 | 4,743 |  |  |

### Server CPU per request, split by where it goes (µs per request, all threads)


**KiviDB 1.2.0**

| Where | get-p1 | get-p16 | set-p1 | set-p16 |
|---|---:|---:|---:|---:|
| kernel: thread sleep/wake (futex, scheduler) | 0.69 | 0.18 | 0.78 | 0.23 |
| kernel: network I/O (syscalls, io_uring, TCP, softirq) | 8.77 | 0.84 | 8.50 | 0.89 |
| memory allocation | 0.03 | 0.03 | 0.04 | 0.03 |
| locks and atomics | 0.08 | 0.07 | 0.02 | 0.02 |
| parsing requests | 0.08 | 0.07 | 0.09 | 0.10 |
| building replies | 0.02 | 0.02 | 0.01 | 0.01 |
| command execution (lookup, read/write the value) | 0.74 | 0.59 | 1.05 | 0.96 |
| event loop and connection handling (user space) | 0.87 | 0.17 | 0.87 | 0.18 |
| other user space | 0.00 | 0.00 | 0.00 | 0.00 |
| **total** | 11.28 | 1.97 | 11.38 | 2.41 |

**Redis 8.10.3**

| Where | get-p1 | get-p16 | set-p1 | set-p16 |
|---|---:|---:|---:|---:|
| kernel: thread sleep/wake (futex, scheduler) | 0.56 | 0.14 | 0.66 | 0.59 |
| kernel: network I/O (syscalls, io_uring, TCP, softirq) | 6.08 | 0.71 | 6.04 | 1.03 |
| memory allocation | 0.07 | 0.06 | 0.10 | 0.10 |
| locks and atomics | 0.04 | 0.02 | 0.04 | 0.10 |
| parsing requests | 0.22 | 0.11 | 0.23 | 0.17 |
| building replies | 0.09 | 0.04 | 0.03 | 0.01 |
| command execution (lookup, read/write the value) | 0.50 | 0.39 | 0.80 | 0.64 |
| event loop and connection handling (user space) | 1.62 | 0.42 | 1.40 | 0.74 |
| other user space | 0.00 | 0.00 | 0.00 | 0.00 |
| **total** | 9.19 | 1.88 | 9.29 | 3.38 |

## 4. Memory

From [memory](memory/README.md): every table of `memory/results/tables.md`. *Candidate* is KiviDB 1.2.0 (75482abb).


### warefeats/kv-stores harness (a324d29)

Candidate and rc4: median (min-max) over their rounds; one run for the others.

#### memory-load: 1M keys x 100 B, then a second million on the same engine

| engine | runs | 1st M RSS B/key | 1st M used B/key | 2nd M RSS B/key | 2nd M used B/key |
|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | 3 | 149.2 (149.2-149.2) | 146.9 (146.9-146.9) | 146.8 (146.7-154.9) | 146.9 (146.9-146.9) |
| KiviDB 1.2.0-rc4 | 3 | 148.9 (148.9-149.2) | 146.9 (146.9-146.9) | 147.1 (146.8-147.6) | 146.9 (146.9-146.9) |
| KiviDB 1.1.3 | 1 | 266.9 | 246.4 | 352.5 | 246.4 |
| Dragonfly 2.0.2 | 1 | 137.1 | 138.7 | 147.4 | 147.1 |
| Redis 8.10.3 | 1 | 166.3 | 165.3 | 166.3 | 166.1 |
| Valkey 9.1.3 | 1 | 170.5 | 167.1 | 172.8 | 169.5 |

#### memory-load-random: 1M keys x 100 B, then a second million on the same engine

| engine | runs | 1st M RSS B/key | 1st M used B/key | 2nd M RSS B/key | 2nd M used B/key |
|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | 3 | 149.2 (149.2-149.2) | 146.9 (146.9-146.9) | 146.7 (146.7-146.7) | 146.9 (146.9-146.9) |
| KiviDB 1.2.0-rc4 | 3 | 149.2 (149.0-149.3) | 146.9 (146.9-146.9) | 146.7 (146.7-146.9) | 146.9 (146.9-146.9) |
| KiviDB 1.1.3 | 1 | 267.0 | 246.4 | 352.5 | 246.4 |
| Dragonfly 2.0.2 | 1 | 153.1 | 154.7 | 163.3 | 163.1 |
| Redis 8.10.3 | 1 | 166.0 | 165.3 | 167.2 | 166.1 |
| Valkey 9.1.3 | 1 | 170.6 | 167.1 | 172.7 | 169.5 |

#### set-get-small: SET:GET 1:10, 100 B, pipeline 1

| engine | runs | ops/s (median of passes) | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | 3 | 1,004,305 (966,437-1,015,373) | 0.191 (0.191-0.191) | 0.375 (0.367-0.375) | 0.475 (0.459-0.503) | 41.5 (40.0-41.8) |
| KiviDB 1.2.0-rc4 | 3 | 968,399 (954,011-1,026,910) | 0.191 (0.191-0.191) | 0.375 (0.371-0.383) | 0.467 (0.447-0.555) | 41.0 (39.3-42.4) |
| KiviDB 1.1.3 | 1 | 989,747 | 0.191 | 0.375 | 0.455 | 42.1 |
| Dragonfly 2.0.2 | 1 | 906,033 | 0.199 | 0.391 | 0.575 | 38.0 |
| Redis 8.10.3 | 1 | 176,722 | 1.115 | 1.623 | 1.827 | 10.4 |
| Valkey 9.1.3 | 1 | 177,037 | 1.107 | 1.631 | 1.835 | 10.0 |

#### set-get-large: SET:GET 1:10, 10 KB, pipeline 1

| engine | runs | ops/s (median of passes) | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | 3 | 320,324 (320,194-320,618) | 0.623 (0.623-0.623) | 1.103 (1.027-1.139) | 1.271 (1.227-1.275) | 30.4 (23.4-32.0) |
| KiviDB 1.2.0-rc4 | 3 | 320,424 (320,234-320,462) | 0.623 (0.623-0.623) | 1.067 (1.031-1.103) | 1.259 (1.227-1.271) | 29.1 (22.5-32.3) |
| KiviDB 1.1.3 | 1 | 320,639 | 0.623 | 1.063 | 1.251 | 22.2 |
| Dragonfly 2.0.2 | 1 | 321,783 | 0.627 | 1.011 | 1.219 | 24.8 |
| Redis 8.10.3 | 1 | 118,856 | 1.679 | 2.543 | 2.767 | 14.0 |
| Valkey 9.1.3 | 1 | 117,031 | 1.703 | 2.663 | 2.983 | 9.8 |

#### pipeline-30: SET:GET 1:10, 100 B, pipeline 30

| engine | runs | ops/s (median of passes) | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | 3 | 15,630,271 (15,041,498-15,684,903) | 0.343 (0.343-0.351) | 0.735 (0.731-0.759) | 0.943 (0.939-0.987) | 61.9 (60.4-62.7) |
| KiviDB 1.2.0-rc4 | 3 | 15,149,332 (15,115,581-15,588,430) | 0.351 (0.351-0.351) | 0.759 (0.735-0.759) | 0.983 (0.951-0.983) | 59.5 (58.8-61.0) |
| KiviDB 1.1.3 | 1 | 11,549,894 | 0.495 | 1.087 | 1.339 | 46.1 |
| Dragonfly 2.0.2 | 1 | 8,009,195 | 0.699 | 1.395 | 3.247 | 34.4 |
| Redis 8.10.3 | 1 | 1,604,362 | 3.695 | 7.999 | 8.959 | 6.1 |
| Valkey 9.1.3 | 1 | 1,583,534 | 3.775 | 8.175 | 8.991 | 6.0 |

### Memory as the dataset grows (random values)

RSS bytes per key (used_memory bytes per key). Candidate and rc4: median over rounds.

#### 100 B values (random)

| engine | runs | 1M keys | 5M keys | 10M keys | 25M keys | 50M keys | RSS at largest | load s (all stages) |
|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | 3 | 153.8 (146.9) | 146.4 (143.1) | 145.1 (143.1) | 141.1 (140.1) | 140.7 (140.1) | 6.57 GiB | 13.0 (12.8-13.0) |
| KiviDB 1.1.3 | 1 | 219.6 (194.1) | 197.6 (176.8) | 197.1 (176.8) | 183.2 (163.0) | 183.1 (163.0) | 8.54 GiB | 12.9 |
| Redis 8.10.3 | 1 | 170.5 (165.4) | 179.3 (177.1) | 179.0 (177.3) | 174.5 (173.1) | 174.4 (173.1) | 8.15 GiB | 55.1 |
| Valkey 9.1.3 | 1 | 172.3 (167.0) | 169.2 (166.4) | 169.3 (166.7) | 166.6 (165.4) | 166.6 (165.4) | 7.77 GiB | 45.0 |
| Dragonfly 2.0.2 | 1 | 164.0 (154.8) | 150.3 (148.0) | 155.6 (148.9) | 143.4 (139.4) | 141.5 (139.6) | 6.65 GiB | 16.0 |
| KiviDB 1.2.0-rc4 | 3 | 153.6 (146.9) | 146.3 (143.1) | 144.9 (143.1) | 141.0 (140.1) | 140.7 (140.1) | 6.57 GiB | 12.9 (12.9-13.1) |

#### 1024 B values (random)

| engine | runs | 1M keys | 5M keys | 10M keys | 25M keys | 50M keys | RSS at largest | load s (all stages) |
|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | 3 | 1085.7 (1074.9) | 1077.6 (1071.1) | 1076.2 (1071.1) | 1072.1 (1068.1) | 1071.7 (1068.1) | 49.92 GiB | 22.9 (22.9-23.0) |
| KiviDB 1.1.3 | 1 | 1132.5 (1118.1) | 1109.8 (1100.8) | 1109.3 (1100.8) | 1095.2 (1087.0) | 1095.1 (1087.0) | 51.01 GiB | 28.9 |
| Redis 8.10.3 | 1 | 1347.6 (1333.4) | 1356.9 (1345.1) | 1356.7 (1345.3) | 1352.2 (1341.1) | 1352.2 (1341.1) | 62.99 GiB | 63.9 |
| Valkey 9.1.3 | 1 | 1352.8 (1336.6) | 1347.4 (1334.8) | 1347.2 (1334.8) | 1344.4 (1333.4) | 1344.4 (1333.4) | 62.62 GiB | 50.9 |
| Dragonfly 2.0.2 | 1 | 1148.7 (1082.8) | 1092.3 (1076.0) | 1085.5 (1076.9) | 1073.5 (1067.5) | 1071.2 (1067.6) | 49.94 GiB | 22.8 |
| KiviDB 1.2.0-rc4 | 3 | 1086.1 (1074.9) | 1077.6 (1071.1) | 1076.2 (1071.1) | 1072.1 (1068.1) | 1071.7 (1068.1) | 49.92 GiB | 22.9 (22.9-23.0) |

### Mixed workload (pair B): 10M keys, 150k ops/s, 80% GET / 20% SET (64-1024 B, TTL 60-600 s), 20 min, distinct client seeds

| engine | round | RSS at start GiB | RSS peak GiB | RSS at end GiB | used at end GiB | RSS/used at end | keys at end (M) | ops/s | GET p50/p99/p99.9 ms | SET p50/p99/p99.9 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | r1 | 1.38 | 4.43 | 3.93 | 3.43 | 1.15 | 6.13 | 149,707 | 0.26/0.46/0.64 | 0.27/0.49/0.78 |
| KiviDB 1.2.0 (75482abb) | r2 | 1.37 | 4.44 | 3.94 | 3.43 | 1.15 | 6.13 | 149,676 | 0.30/0.47/0.79 | 0.31/0.57/1.01 |
| KiviDB 1.2.0 (75482abb) | r3 | 1.37 | 4.44 | 3.92 | 3.43 | 1.14 | 6.13 | 149,716 | 0.29/0.46/0.69 | 0.30/0.51/0.92 |
| KiviDB 1.1.3 | r1 | 1.86 | 4.70 | 4.70 | 4.02 | 1.17 | 6.14 | 149,870 | 0.29/0.46/0.66 | 0.29/0.49/0.84 |
| Redis 8.10.3 | r1 | 1.70 | 5.84 | 5.84 | 4.32 | 1.35 | 6.96 | 150,000 | 0.25/0.42/0.60 | 0.26/0.47/1.00 |
| Valkey 9.1.3 | r1 | 1.60 | 6.21 | 6.20 | 4.26 | 1.46 | 6.96 | 149,998 | 0.30/0.56/0.78 | 0.31/0.56/0.78 |
| Dragonfly 2.0.2 | r1 | 1.47 | 5.04 | 5.03 | 4.01 | 1.25 | 6.34 | 150,000 | 0.28/0.44/0.54 | 0.29/0.47/0.85 |
| KiviDB 1.2.0-rc4 | r1 | 1.37 | 4.43 | 3.94 | 3.43 | 1.15 | 6.13 | 149,704 | 0.27/0.45/0.63 | 0.28/0.48/0.78 |
| KiviDB 1.2.0-rc4 | r2 | 1.37 | 4.44 | 3.94 | 3.43 | 1.15 | 6.13 | 149,697 | 0.28/0.47/0.65 | 0.29/0.49/0.81 |
| KiviDB 1.2.0-rc4 | r3 | 1.37 | 4.44 | 3.92 | 3.43 | 1.14 | 6.13 | 149,676 | 0.29/0.48/0.72 | 0.29/0.52/0.86 |

### Snapshot under a steady 100k SET/s (uniform random keys): 30M keys x 1 KB, random values

| engine | round | event | memory before GiB | peak during GiB | added GiB | added % | duration s | worst write ms | worst 1-s p99 ms | write p99.9 over window ms | p99 before ms | memory 60 s after GiB | used after GiB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | r1 | BGSAVE | 30.29 | 33.40 | 3.11 | 10% | 70.6 | 210 | 1.70 | 1.85 | 1.42 | 33.37 | 30.09 |
| KiviDB 1.2.0 (75482abb) | r1 | BGREWRITEAOF | 33.37 | 35.45 | 2.07 | 6% | 72.5 | 220 | 1.65 | 1.90 | 1.46 | 34.98 | 30.09 |
| KiviDB 1.2.0 (75482abb) | r2 | BGSAVE | 30.30 | 33.42 | 3.12 | 10% | 70.4 | 209 | 1.44 | 1.96 | 1.39 | 33.39 | 30.09 |
| KiviDB 1.2.0 (75482abb) | r2 | BGREWRITEAOF | 33.39 | 35.47 | 2.08 | 6% | 72.0 | 221 | 1.67 | 1.85 | 1.40 | 34.96 | 30.09 |
| KiviDB 1.2.0 (75482abb) | r3 | BGSAVE | 30.31 | 33.43 | 3.12 | 10% | 70.4 | 206 | 1.56 | 2.06 | 1.38 | 33.40 | 30.09 |
| KiviDB 1.2.0 (75482abb) | r3 | BGREWRITEAOF | 33.40 | 35.49 | 2.09 | 6% | 71.9 | 226 | 1.46 | 1.87 | 1.23 | 34.96 | 30.09 |
| KiviDB 1.1.3 | r1 | BGSAVE | 63.58 | 64.51 | 0.93 | 1% | 97.9 | 297 | 0.69 | 0.84 | 0.60 | 63.61 | 31.41 |
| KiviDB 1.1.3 | r1 | BGREWRITEAOF | 63.61 | 64.51 | 0.89 | 1% | 81.0 | 289 | 1.01 | 1.13 | 0.82 | 63.61 | 31.41 |
| Redis 8.10.3 | r1 | BGSAVE | 37.60 | 70.90 | 33.29 | 89% | 175.0 | 391 | 2.59 | 7.07 | 0.73 | 37.61 | 37.28 |
| Redis 8.10.3 | r1 | BGREWRITEAOF | 37.61 | 70.80 | 33.20 | 88% | 177.6 | 1006 | 1.74 | 4.96 | 0.67 | 37.63 | 37.30 |
| Valkey 9.1.3 | r1 | BGSAVE | 37.87 | 67.35 | 29.48 | 78% | 165.2 | 408 | 1.75 | 7.07 | 0.93 | 38.24 | 37.40 |
| Valkey 9.1.3 | r1 | BGREWRITEAOF | 38.38 | 68.33 | 29.95 | 78% | 172.3 | 1024 | 4.89 | 6.75 | 0.61 | 38.64 | 37.40 |
| Dragonfly 2.0.2 | r1 | BGSAVE | 30.73 | 30.86 | 0.12 | 0% | 55.1 | 291 | 19.71 | 10.11 | 0.57 | 30.73 | 30.61 |
| KiviDB 1.2.0-rc4 | r1 | BGSAVE | 30.29 | 33.44 | 3.15 | 10% | 71.5 | 209 | 1.54 | 1.87 | 1.41 | 33.41 | 30.09 |
| KiviDB 1.2.0-rc4 | r1 | BGREWRITEAOF | 33.41 | 35.51 | 2.10 | 6% | 73.1 | 222 | 1.53 | 1.78 | 1.38 | 34.98 | 30.09 |
| KiviDB 1.2.0-rc4 | r2 | BGSAVE | 30.30 | 33.43 | 3.13 | 10% | 70.9 | 203 | 1.53 | 2.02 | 1.38 | 33.40 | 30.09 |
| KiviDB 1.2.0-rc4 | r2 | BGREWRITEAOF | 33.40 | 35.49 | 2.09 | 6% | 72.6 | 225 | 1.53 | 2.06 | 1.33 | 34.97 | 30.09 |
| KiviDB 1.2.0-rc4 | r3 | BGSAVE | 30.31 | 33.39 | 3.09 | 10% | 70.5 | 226 | 1.74 | 2.02 | 1.42 | 33.36 | 30.09 |
| KiviDB 1.2.0-rc4 | r3 | BGREWRITEAOF | 33.36 | 35.42 | 2.06 | 6% | 72.5 | 222 | 1.60 | 1.79 | 1.28 | 34.97 | 30.09 |

#### Snapshot, per engine: median over rounds

| engine | runs | event | added GiB | added % | duration s | worst write ms | write p99.9 ms | p99 before ms | memory 60 s after GiB |
|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | 3 | BGSAVE | 3.12 (3.11-3.12) | 10 (10-10) | 70.4 (70.4-70.6) | 209 (206-210) | 1.96 (1.85-2.06) | 1.39 (1.38-1.42) | 33.39 (33.37-33.40) |
| KiviDB 1.2.0 (75482abb) | 3 | BGREWRITEAOF | 2.08 (2.07-2.09) | 6 (6-6) | 72.0 (71.9-72.5) | 221 (220-226) | 1.87 (1.85-1.90) | 1.40 (1.23-1.46) | 34.96 (34.96-34.98) |
| KiviDB 1.1.3 | 1 | BGSAVE | 0.93 | 1 | 97.9 | 297 | 0.84 | 0.60 | 63.61 |
| KiviDB 1.1.3 | 1 | BGREWRITEAOF | 0.89 | 1 | 81.0 | 289 | 1.13 | 0.82 | 63.61 |
| Redis 8.10.3 | 1 | BGSAVE | 33.29 | 89 | 175.0 | 391 | 7.07 | 0.73 | 37.61 |
| Redis 8.10.3 | 1 | BGREWRITEAOF | 33.20 | 88 | 177.6 | 1006 | 4.96 | 0.67 | 37.63 |
| Valkey 9.1.3 | 1 | BGSAVE | 29.48 | 78 | 165.2 | 408 | 7.07 | 0.93 | 38.24 |
| Valkey 9.1.3 | 1 | BGREWRITEAOF | 29.95 | 78 | 172.3 | 1024 | 6.75 | 0.61 | 38.64 |
| Dragonfly 2.0.2 | 1 | BGSAVE | 0.12 | 0 | 55.1 | 291 | 10.11 | 0.57 | 30.73 |
| KiviDB 1.2.0-rc4 | 3 | BGSAVE | 3.13 (3.09-3.15) | 10 (10-10) | 70.9 (70.5-71.5) | 209 (203-226) | 2.02 (1.87-2.02) | 1.41 (1.38-1.42) | 33.40 (33.36-33.41) |
| KiviDB 1.2.0-rc4 | 3 | BGREWRITEAOF | 2.09 (2.06-2.10) | 6 (6-6) | 72.6 (72.5-73.1) | 222 (222-225) | 1.79 (1.78-2.06) | 1.33 (1.28-1.38) | 34.97 (34.97-34.98) |

### maxmemory + BGSAVE (issue #284): 10M keys x 1 KB, maxmemory = used + 10%, allkeys-lru, 100k same-size overwrites/s (uniform)

| engine | round | keys before | keys after | evicted keys | maxmemory | lowest key count during run | SET p99 / p99.9 ms |
|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | r1 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.73 / 1.11 |
| KiviDB 1.2.0 (75482abb) | r2 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.70 / 1.05 |
| KiviDB 1.2.0 (75482abb) | r3 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.78 / 1.14 |
| KiviDB 1.1.3 | r1 | 10,000,000 | 8,789,704 | 11,055,716 | 11.28 GiB | 0.09M | 1.21 / 1.85 |
| Redis 8.10.3 | r1 | 10,000,000 | 10,000,000 | 0 | 13.78 GiB | 10.00M | 0.62 / 1.08 |
| KiviDB 1.2.0-rc4 | r1 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.82 / 1.18 |
| KiviDB 1.2.0-rc4 | r2 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.73 / 1.09 |
| KiviDB 1.2.0-rc4 | r3 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.59 / 0.87 |

### Active defrag after deleting a random half: 5M keys x 100 B, probe load 80k GET/s + 20k SET/s (pair B)

Slab = INFO value_slab_bytes (live = value_slab_live_bytes). *Give-back time*: from the end of the delete until RSS has made 90% of its final drop. *Compaction*: seconds with active_defrag_running = 1. Latency: per-second p99 / p99.9 from memtier, median over the seconds of the window (worst second in brackets); *before* = the 50 s before the delete, *during* = while compaction runs.

| build | round | RSS after load MB | slab / live after delete MB | RSS after delete MB | RSS final MB | slab / live final MB | RSS returned MB | give-back s | compaction s | hits | GET p99 before / during ms | GET p99.9 before / during ms | SET p99 before / during ms | SET p99.9 before / during ms | max ms during |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | r1 | 722 | 624 / 319 | 722 | 387 | 321 / 319 | 335 | 12.8 | 13 | 1,399,554 | 0.52 / 0.51 (0.54) | 0.80 / 0.78 (1.06) | 0.26 / 0.25 (0.29) | 0.54 / 0.49 (0.77) | 2.7 |
| KiviDB 1.2.0 (75482abb) | r2 | 723 | 624 / 319 | 724 | 389 | 321 / 319 | 334 | 12.1 | 13 | 1,397,852 | 0.51 / 0.51 (0.66) | 0.80 / 0.79 (0.94) | 0.21 / 0.22 (0.23) | 0.28 / 0.27 (0.38) | 2.4 |
| KiviDB 1.2.0 (75482abb) | r3 | 724 | 624 / 319 | 724 | 390 | 321 / 319 | 334 | 13.1 | 13 | 1,398,652 | 0.51 / 0.47 (0.52) | 0.83 / 0.80 (1.19) | 0.24 / 0.24 (0.26) | 0.41 / 0.36 (0.57) | 2.3 |
| KiviDB 1.2.0 (75482abb) | r4 | 722 | 624 / 319 | 723 | 389 | 321 / 319 | 334 | 11.9 | 12 | 1,388,405 | 0.57 / 0.58 (0.62) | 0.83 / 0.89 (1.06) | 0.26 / 0.24 (0.29) | 0.37 / 0.39 (0.53) | 2.4 |
| KiviDB 1.2.0 (75482abb) | r5 | 722 | 624 / 319 | 722 | 385 | 321 / 319 | 337 | 11.7 | 13 | 1,384,211 | 0.39 / 0.39 (0.41) | 0.44 / 0.44 (0.51) | 0.20 / 0.20 (0.21) | 0.30 / 0.29 (0.35) | 2.0 |
| KiviDB 1.2.0 (75482abb) (activedefrag no) | r1 | 722 | 624 / 319 | 723 | 688 | 624 / 319 | 35 | 1.0 | 0 | 0 | 0.47 / - | 0.61 / - | 0.13 / - | 0.14 / - | - |
| KiviDB 1.2.0 (75482abb) (activedefrag no) | r2 | 721 | 624 / 319 | 722 | 687 | 624 / 319 | 35 | 1.1 | 0 | 0 | 0.34 / - | 0.48 / - | 0.16 / - | 0.18 / - | - |
| KiviDB 1.2.0 (75482abb) (activedefrag no) | r3 | 723 | 624 / 319 | 723 | 689 | 624 / 319 | 34 | 0.9 | 0 | 0 | 0.36 / - | 0.78 / - | 0.17 / - | 0.31 / - | - |
| KiviDB 1.2.0-rc4 | r1 | 723 | 624 / 319 | 724 | 389 | 321 / 319 | 335 | 12.8 | 13 | 1,379,059 | 0.28 / 0.28 (0.35) | 0.47 / 0.40 (1.06) | 0.19 / 0.17 (0.29) | 0.36 / 0.25 (0.36) | 1.8 |
| KiviDB 1.2.0-rc4 | r2 | 722 | 624 / 319 | 723 | 388 | 321 / 319 | 335 | 12.1 | 13 | 1,401,933 | 0.56 / 0.59 (0.66) | 0.81 / 0.81 (0.97) | 0.23 / 0.23 (0.25) | 0.37 / 0.29 (0.49) | 2.2 |
| KiviDB 1.2.0-rc4 | r3 | 722 | 624 / 319 | 722 | 386 | 321 / 319 | 336 | 12.1 | 13 | 1,395,036 | 0.34 / 0.38 (0.38) | 0.66 / 0.73 (1.00) | 0.27 / 0.26 (0.28) | 0.29 / 0.29 (0.34) | 2.0 |
| KiviDB 1.2.0-rc4 | r4 | 722 | 624 / 319 | 722 | 386 | 321 / 319 | 336 | 11.9 | 12 | 1,369,731 | 0.54 / 0.47 (0.56) | 0.72 / 0.73 (0.94) | 0.26 / 0.24 (0.28) | 0.39 / 0.39 (0.54) | 1.6 |
| KiviDB 1.2.0-rc4 (activedefrag no) | r2 | 723 | 624 / 319 | 723 | 689 | 624 / 319 | 34 | 0.9 | 0 | 0 | 0.35 / - | 0.63 / - | 0.44 / - | 0.66 / - | - |

### Write latency with the AOF on, no snapshot (pair B): 2M x 1 KB, 100k uniformly random SET/s, 180 s

The check of the snapshot runs' steady-state write p99 (`harness/aoflat.sh`). Rounds r1-r4 alternate the builds; p1 is the round with `perf record` running for 30 s.

| build | round | ops/s | SET p50 ms | p99 ms | p99.9 ms | worst write ms | median of 1-s p99 ms | engine CPU s (load + run) |
|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | r1 | 99,474 | 0.351 | 0.887 | 1.255 | 164 | 0.911 | 550 |
| KiviDB 1.2.0 (75482abb) | r2 | 99,532 | 0.367 | 0.919 | 1.311 | 163 | 0.927 | 547 |
| KiviDB 1.2.0 (75482abb) | r3 | 99,465 | 0.391 | 0.967 | 1.391 | 175 | 1.031 | 562 |
| KiviDB 1.2.0 (75482abb) | r4 | 99,273 | 0.359 | 0.903 | 1.255 | 168 | 0.919 | 557 |
| KiviDB 1.2.0 (75482abb) | p1 | 99,581 | 0.399 | 0.991 | 1.439 | 133 | 1.031 | 570 |
| KiviDB 1.2.0-rc4 | r1 | 99,590 | 0.359 | 0.895 | 1.255 | 125 | 0.903 | 552 |
| KiviDB 1.2.0-rc4 | r2 | 99,655 | 0.407 | 1.007 | 1.423 | 159 | 1.039 | 564 |
| KiviDB 1.2.0-rc4 | r3 | 99,387 | 0.407 | 0.999 | 1.407 | 172 | 1.031 | 562 |
| KiviDB 1.2.0-rc4 | r4 | 99,375 | 0.415 | 1.007 | 1.423 | 167 | 1.023 | 558 |
| KiviDB 1.2.0-rc4 | p1 | 99,652 | 0.399 | 0.975 | 1.399 | 155 | 1.015 | 571 |

### Against 1.2.0-rc4

Median (min-max) over rounds. *Change* is candidate median vs rc4 median; *spread* is the larger of the two builds' (max-min)/median. A row is flagged when the candidate is worse by more than the spread and by at least 1% (memory counts repeat to a few tenths of a percent, so a spread near zero would otherwise flag a 0.2% difference).

| section | metric | candidate | rc4 | change | spread | verdict |
|---|---|---|---|---|---|---|
| warefeats ASCII | 1st M RSS B/key | 149.2 (149.2-149.2) | 148.9 (148.9-149.2) | 0.2% | 0.2% | level |
| warefeats ASCII | 1st M used B/key | 146.9 (146.9-146.9) | 146.9 (146.9-146.9) | 0.0% | 0.0% | level |
| warefeats ASCII | 2nd M RSS B/key | 146.8 (146.7-154.9) | 147.1 (146.8-147.6) | -0.2% | 5.6% | level |
| warefeats random | 1st M RSS B/key | 149.2 (149.2-149.2) | 149.2 (149.0-149.3) | 0.0% | 0.2% | level |
| warefeats random | 1st M used B/key | 146.9 (146.9-146.9) | 146.9 (146.9-146.9) | 0.0% | 0.0% | level |
| warefeats random | 2nd M RSS B/key | 146.7 (146.7-146.7) | 146.7 (146.7-146.9) | 0.0% | 0.1% | level |
| warefeats set-get-small | ops/s | 1,004,305 (966,437-1,015,373) | 968,399 (954,011-1,026,910) | 3.7% | 7.5% | level |
| warefeats set-get-small | p99 ms | 0.375 (0.367-0.375) | 0.375 (0.371-0.383) | 0.0% | 3.2% | level |
| warefeats set-get-small | p99.9 ms | 0.475 (0.459-0.503) | 0.467 (0.447-0.555) | 1.7% | 23.1% | level |
| warefeats set-get-large | ops/s | 320,324 (320,194-320,618) | 320,424 (320,234-320,462) | -0.0% | 0.1% | level |
| warefeats set-get-large | p99 ms | 1.103 (1.027-1.139) | 1.067 (1.031-1.103) | 3.4% | 10.2% | level |
| warefeats set-get-large | p99.9 ms | 1.271 (1.227-1.275) | 1.259 (1.227-1.271) | 1.0% | 3.8% | level |
| warefeats pipeline-30 | ops/s | 15,630,271 (15,041,498-15,684,903) | 15,149,332 (15,115,581-15,588,430) | 3.2% | 4.1% | level |
| warefeats pipeline-30 | p99 ms | 0.735 (0.731-0.759) | 0.759 (0.735-0.759) | -3.2% | 3.8% | level |
| warefeats pipeline-30 | p99.9 ms | 0.943 (0.939-0.987) | 0.983 (0.951-0.983) | -4.1% | 5.1% | level |
| growth 100 B | RSS B/key at 1M | 153.8 (153.3-154.1) | 153.6 (153.5-154.4) | 0.1% | 0.5% | level |
| growth 100 B | RSS B/key at 5M | 146.4 (146.4-146.7) | 146.3 (146.2-146.5) | 0.1% | 0.2% | level |
| growth 100 B | RSS B/key at 10M | 145.1 (145.0-145.2) | 144.9 (144.8-145.2) | 0.2% | 0.2% | level |
| growth 100 B | RSS B/key at 25M | 141.1 (141.0-141.1) | 141.0 (141.0-141.1) | 0.1% | 0.1% | level |
| growth 100 B | RSS B/key at 50M | 140.7 (140.7-140.7) | 140.7 (140.7-140.7) | 0.0% | 0.0% | level |
| growth 100 B | load seconds, 50M keys | 13.0 (12.8-13.0) | 12.9 (12.9-13.1) | 0.7% | 1.6% | level |
| growth 1024 B | RSS B/key at 1M | 1085.7 (1085.6-1086.1) | 1086.1 (1085.9-1086.1) | -0.0% | 0.0% | level |
| growth 1024 B | RSS B/key at 5M | 1077.6 (1077.5-1078.0) | 1077.6 (1077.4-1078.0) | 0.0% | 0.0% | level |
| growth 1024 B | RSS B/key at 10M | 1076.2 (1076.0-1076.2) | 1076.2 (1076.0-1076.4) | -0.0% | 0.0% | level |
| growth 1024 B | RSS B/key at 25M | 1072.1 (1072.0-1072.1) | 1072.1 (1072.0-1072.1) | -0.0% | 0.0% | level |
| growth 1024 B | RSS B/key at 50M | 1071.7 (1071.6-1071.7) | 1071.7 (1071.6-1071.7) | -0.0% | 0.0% | level |
| growth 1024 B | load seconds, 50M keys | 22.9 (22.9-23.0) | 22.9 (22.9-23.0) | 0.1% | 0.8% | level |
| mixed 20 min | RSS at end GiB | 3.93 (3.92-3.94) | 3.94 (3.92-3.94) | -0.3% | 0.5% | level |
| mixed 20 min | RSS peak GiB | 4.44 (4.43-4.44) | 4.44 (4.43-4.44) | -0.1% | 0.3% | level |
| mixed 20 min | RSS/used at end | 1.15 (1.14-1.15) | 1.15 (1.14-1.15) | -0.3% | 0.5% | level |
| mixed 20 min | GET p99 ms | 0.46 (0.46-0.47) | 0.47 (0.45-0.48) | -1.7% | 6.8% | level |
| mixed 20 min | GET p99.9 ms | 0.69 (0.64-0.79) | 0.65 (0.63-0.72) | 7.4% | 21.9% | level |
| mixed 20 min | SET p99 ms | 0.51 (0.49-0.57) | 0.49 (0.48-0.52) | 3.2% | 15.7% | level |
| mixed 20 min | SET p99.9 ms | 0.92 (0.78-1.01) | 0.81 (0.78-0.86) | 13.9% | 24.4% | level |
| snapshot BGSAVE | memory added GiB | 3.12 (3.11-3.12) | 3.13 (3.09-3.15) | -0.3% | 1.9% | level |
| snapshot BGSAVE | duration s | 70.4 (70.4-70.6) | 70.9 (70.5-71.5) | -0.7% | 1.4% | level |
| snapshot BGSAVE | worst write ms | 209 (206-210) | 209 (203-226) | -0.4% | 10.8% | level |
| snapshot BGSAVE | write p99.9 ms | 1.96 (1.85-2.06) | 2.02 (1.87-2.02) | -2.8% | 11.0% | level |
| snapshot BGSAVE | write p99 before ms | 1.39 (1.38-1.42) | 1.41 (1.38-1.42) | -1.1% | 3.4% | level |
| snapshot BGSAVE | memory 60 s after GiB | 33.39 (33.37-33.40) | 33.40 (33.36-33.41) | -0.0% | 0.1% | level |
| snapshot BGREWRITEAOF | memory added GiB | 2.08 (2.07-2.09) | 2.09 (2.06-2.10) | -0.4% | 1.8% | level |
| snapshot BGREWRITEAOF | duration s | 72.0 (71.9-72.5) | 72.6 (72.5-73.1) | -0.8% | 0.9% | level |
| snapshot BGREWRITEAOF | worst write ms | 221 (220-226) | 222 (222-225) | -0.3% | 2.4% | level |
| snapshot BGREWRITEAOF | write p99.9 ms | 1.87 (1.85-1.90) | 1.79 (1.78-2.06) | 4.5% | 15.6% | level |
| snapshot BGREWRITEAOF | write p99 before ms | 1.40 (1.23-1.46) | 1.33 (1.28-1.38) | 5.4% | 16.6% | level |
| snapshot BGREWRITEAOF | memory 60 s after GiB | 34.96 (34.96-34.98) | 34.97 (34.97-34.98) | -0.0% | 0.1% | level |
| #284 eviction | keys evicted | 0 (0-0) | 0 (0-0) | -% | 0.0% | - |
| active defrag | RSS final MB | 389 (385-390) | 387 (386-389) | 0.5% | 1.1% | level |
| active defrag | RSS returned MB | 334 (334-337) | 335 (335-336) | -0.3% | 0.8% | level |
| active defrag | give-back s | 12.1 (11.7-13.1) | 12.1 (11.9-12.8) | -0.3% | 11.1% | level |
| active defrag | compaction s | 13 (12-13) | 13 (12-13) | 0.0% | 7.7% | level |
| active defrag | slab final MB | 321 (321-321) | 321 (321-321) | 0.0% | 0.2% | level |
| active defrag | GET p99 during compaction ms | 0.51 (0.39-0.58) | 0.42 (0.28-0.59) | 21.4% | 74.1% | level |
| active defrag | GET p99.9 during compaction ms | 0.79 (0.44-0.89) | 0.73 (0.40-0.81) | 8.8% | 57.2% | level |
| active defrag | SET p99 during compaction ms | 0.24 (0.20-0.25) | 0.24 (0.17-0.26) | 0.8% | 40.5% | level |
| active defrag | SET p99.9 during compaction ms | 0.36 (0.27-0.49) | 0.29 (0.25-0.39) | 23.4% | 62.4% | level |
| active defrag | GET p99 during / before (same run) | 1.00 (0.93-1.02) | 1.02 (0.87-1.11) | -1.7% | 22.9% | level |
| active defrag | GET p99.9 during / before (same run) | 0.99 (0.97-1.07) | 1.01 (0.85-1.11) | -1.5% | 26.1% | level |
| active defrag | SET p99 during / before (same run) | 1.00 (0.92-1.04) | 0.95 (0.87-1.00) | 4.8% | 13.2% | level |
| active defrag | SET p99.9 during / before (same run) | 0.95 (0.88-1.05) | 0.88 (0.69-0.99) | 7.1% | 34.1% | level |
| AOF-on writes | SET p50 ms | 0.363 (0.351-0.391) | 0.407 (0.359-0.415) | -10.8% | 13.8% | level |
| AOF-on writes | SET p99 ms | 0.911 (0.887-0.967) | 1.003 (0.895-1.007) | -9.2% | 11.2% | level |
| AOF-on writes | SET p99.9 ms | 1.283 (1.255-1.391) | 1.415 (1.255-1.423) | -9.3% | 11.9% | level |
| AOF-on writes | worst write ms | 166 (163-175) | 163 (125-172) | 1.9% | 29.1% | level |
| AOF-on writes | engine CPU s | 554 (547-562) | 560 (552-564) | -1.2% | 2.7% | level |


## 5. Vector search

From [vector](vector/README.md), with [redis/vector-db-benchmark](https://github.com/redis/vector-db-benchmark). Server D32ps_v5 (32 vCPU arm64), client D16ps_v5. q/s is the median of 3 passes.

### h-and-m 2048-d, filtered (105K)

| engine | ef | clients | q/s median (3 passes) | spread between passes | largest spread of 3 reps | recall@10 | p50 ms | p95 ms | p99 ms | p99.9 ms | client-bound? |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| KiviDB 1.2.0 | 64 | 1 | 293 | 2.0% | 4.7% | 0.9983 | 2.08 | 9.77 | 26.58 | 45.09 | no |
| KiviDB 1.2.0 | 128 | 1 | 252 | 0.9% | 4.5% | 0.9984 | 2.35 | 14.72 | 16.53 | 45.38 | no |
| KiviDB 1.2.0 | 256 | 1 | 214 | 3.3% | 5.9% | 0.9985 | 2.51 | 16.52 | 20.37 | 46.72 | no |
| KiviDB 1.2.0 | 128 | 16 | 3,092 | 0.6% | 0.9% | 0.9984 | 3.08 | 19.60 | 22.16 | 60.22 | no |
| KiviDB 1.2.0 | 128 | 100 | 4,478 | 1.7% | 1.2% | 0.9984 | 17.70 | 52.75 | 86.99 | 122.69 | no |
| Redis 8.10.2 (workflow flags) | 64 | 1 | 131 | 2.4% | 2.0% | 0.9986 | 2.87 | 39.72 | 47.67 | 53.38 | no |
| Redis 8.10.2 (workflow flags) | 128 | 1 | 131 | 1.9% | 1.0% | 0.9986 | 2.85 | 39.74 | 47.54 | 53.82 | no |
| Redis 8.10.2 (workflow flags) | 256 | 1 | 130 | 1.4% | 0.9% | 0.9986 | 2.87 | 39.86 | 47.64 | 53.57 | no |
| Redis 8.10.2 (workflow flags) | 128 | 16 | 1,765 | 4.9% | 0.9% | 0.9986 | 3.34 | 47.62 | 56.71 | 64.61 | no |
| Redis 8.10.2 (workflow flags) | 128 | 100 | 1,802 | 2.3% | 1.3% | 0.9986 | 51.33 | 92.15 | 109.30 | 121.22 | no |
| Redis 8.10.2 + io-threads 8 | 64 | 1 | 130 | 2.5% | 0.8% | 0.9986 | 2.91 | 40.02 | 47.86 | 53.47 | no |
| Redis 8.10.2 + io-threads 8 | 128 | 1 | 131 | 2.1% | 0.9% | 0.9986 | 2.88 | 40.16 | 47.75 | 53.89 | no |
| Redis 8.10.2 + io-threads 8 | 256 | 1 | 131 | 2.7% | 1.1% | 0.9986 | 2.86 | 39.63 | 47.36 | 53.22 | no |
| Redis 8.10.2 + io-threads 8 | 128 | 16 | 1,795 | 3.0% | 1.1% | 0.9986 | 3.31 | 47.05 | 55.98 | 63.01 | no |
| Redis 8.10.2 + io-threads 8 | 128 | 100 | 1,842 | 4.2% | 2.3% | 0.9986 | 50.28 | 89.93 | 107.64 | 118.85 | no |
| Dragonfly 2.0.2 | 64 | 1 | 204 | 2.5% | 1.7% | 0.9975 | 3.35 | 11.65 | 29.47 | 95.17 | no |
| Dragonfly 2.0.2 | 128 | 1 | 181 | 2.4% | 2.6% | 0.9981 | 3.35 | 13.33 | 36.95 | 108.35 | no |
| Dragonfly 2.0.2 | 256 | 1 | 154 | 1.7% | 1.9% | 0.9983 | 3.37 | 19.30 | 51.12 | 122.69 | no |
| Dragonfly 2.0.2 | 128 | 16 | 394 | 3.2% | 3.9% | 0.9981 | 29.56 | 103.36 | 133.48 | 194.30 | no |
| Dragonfly 2.0.2 | 128 | 100 | 811 | 2.5% | 3.1% | 0.9981 | 113.72 | 218.13 | 257.26 | 296.45 | no |

| engine | loads | upload s (median, min-max) | index wait s | upload+index s | peak RSS GiB | RSS after load GiB | used_memory after load GiB | RSS B/vector | used_memory B/vector |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| KiviDB 1.2.0 | 3 | 14.8 (14.8-14.8) | 0.01 | 14.9 | 2.19 | 2.16 | 2.04 | 21,936 | 20,859 |
| Redis 8.10.2 (workflow flags) | 3 | 15.1 (14.9-15.2) | 0.00 | 15.1 | 2.28 | 2.25 | 2.37 | 22,785 | 24,220 |
| Redis 8.10.2 + io-threads 8 | 3 | 13.3 (13.1-13.5) | 0.00 | 13.3 | 2.29 | 2.25 | 2.37 | 22,769 | 24,237 |
| Dragonfly 2.0.2 | 3 | 9.5 (9.5-9.8) | 0.00 | 9.5 | 1.46 | 1.45 | 1.27 | 14,233 | 12,919 |
| KiviDB 1.2.0-rc4 | 3 | 15.1 (14.5-15.1) | 0.01 | 15.1 | 2.19 | 2.16 | 2.04 | 21,920 | 20,859 |

### dbpedia 1536-d (100K)

| engine | ef | clients | q/s median (3 passes) | spread between passes | largest spread of 3 reps | recall@10 | p50 ms | p95 ms | p99 ms | p99.9 ms | client-bound? |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| KiviDB 1.2.0 | 64 | 1 | 932 | 1.0% | 3.3% | 0.9685 | 1.07 | 1.33 | 1.50 | 1.79 | no |
| KiviDB 1.2.0 | 128 | 1 | 609 | 3.3% | 4.5% | 0.9864 | 1.64 | 2.08 | 2.24 | 2.54 | no |
| KiviDB 1.2.0 | 256 | 1 | 371 | 1.0% | 4.4% | 0.9939 | 2.71 | 3.49 | 3.82 | 4.06 | no |
| KiviDB 1.2.0 | 128 | 16 | 7,163 | 3.0% | 2.0% | 0.9864 | 2.22 | 2.84 | 3.07 | 3.43 | no |
| KiviDB 1.2.0 | 128 | 100 | 10,255 | 3.6% | 3.4% | 0.9864 | 9.27 | 13.03 | 13.88 | 14.90 | no |
| Redis 8.10.2 (workflow flags) | 64 | 1 | 902 | 0.7% | 0.9% | 0.9570 | 1.11 | 1.31 | 1.48 | 1.69 | no |
| Redis 8.10.2 (workflow flags) | 128 | 1 | 589 | 1.6% | 1.1% | 0.9813 | 1.71 | 2.04 | 2.19 | 2.47 | no |
| Redis 8.10.2 (workflow flags) | 256 | 1 | 360 | 1.4% | 0.9% | 0.9921 | 2.83 | 3.39 | 3.58 | 3.83 | no |
| Redis 8.10.2 (workflow flags) | 128 | 16 | 7,470 | 2.5% | 4.5% | 0.9813 | 2.12 | 2.54 | 2.75 | 3.21 | no |
| Redis 8.10.2 (workflow flags) | 128 | 100 | 8,198 | 0.9% | 1.6% | 0.9813 | 12.10 | 12.79 | 13.09 | 13.53 | no |
| Redis 8.10.2 + io-threads 8 | 64 | 1 | 881 | 1.0% | 0.5% | 0.9560 | 1.13 | 1.34 | 1.54 | 1.73 | no |
| Redis 8.10.2 + io-threads 8 | 128 | 1 | 580 | 1.4% | 1.1% | 0.9810 | 1.74 | 2.07 | 2.21 | 2.62 | no |
| Redis 8.10.2 + io-threads 8 | 256 | 1 | 357 | 1.6% | 0.3% | 0.9919 | 2.84 | 3.41 | 3.62 | 3.89 | no |
| Redis 8.10.2 + io-threads 8 | 128 | 16 | 7,400 | 1.0% | 3.0% | 0.9810 | 2.17 | 2.58 | 2.81 | 3.25 | no |
| Redis 8.10.2 + io-threads 8 | 128 | 100 | 8,069 | 1.8% | 1.5% | 0.9810 | 12.28 | 13.08 | 13.44 | 13.76 | no |
| Dragonfly 2.0.2 | 64 | 1 | 712 | 5.0% | 2.3% | 0.9563 | 1.39 | 1.69 | 2.17 | 2.68 | no |
| Dragonfly 2.0.2 | 128 | 1 | 462 | 5.6% | 2.2% | 0.9801 | 2.16 | 2.63 | 2.92 | 3.62 | no |
| Dragonfly 2.0.2 | 256 | 1 | 290 | 5.2% | 2.1% | 0.9912 | 3.48 | 4.27 | 4.65 | 5.36 | no |
| Dragonfly 2.0.2 | 128 | 16 | 4,724 | 3.9% | 3.2% | 0.9801 | 3.26 | 3.64 | 4.23 | 4.88 | no |
| Dragonfly 2.0.2 | 128 | 100 | 9,399 | 4.6% | 1.9% | 0.9801 | 10.09 | 14.76 | 17.43 | 20.24 | no |

| engine | loads | upload s (median, min-max) | index wait s | upload+index s | peak RSS GiB | RSS after load GiB | used_memory after load GiB | RSS B/vector | used_memory B/vector |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| KiviDB 1.2.0 | 3 | 11.7 (11.6-11.8) | 0.01 | 11.7 | 1.25 | 1.25 | 1.22 | 13,265 | 13,066 |
| Redis 8.10.2 (workflow flags) | 3 | 10.1 (10.1-10.3) | 0.00 | 10.1 | 1.42 | 1.37 | 1.40 | 14,451 | 15,073 |
| Redis 8.10.2 + io-threads 8 | 3 | 10.2 (10.1-10.3) | 0.00 | 10.2 | 1.42 | 1.38 | 1.41 | 14,565 | 15,074 |
| Dragonfly 2.0.2 | 3 | 9.2 (9.1-9.4) | 0.00 | 9.2 | 0.84 | 0.83 | 0.72 | 8,281 | 7,691 |
| KiviDB 1.2.0-rc4 | 3 | 11.9 (11.5-11.9) | 0.01 | 11.9 | 1.25 | 1.25 | 1.22 | 13,266 | 13,066 |

### glove 25-d (1.18M)

| engine | ef | clients | q/s median (3 passes) | spread between passes | largest spread of 3 reps | recall@10 | p50 ms | p95 ms | p99 ms | p99.9 ms | client-bound? |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| KiviDB 1.2.0 | 64 | 100 | 38,217 | 1.8% | 1.8% | 0.9478 | 2.41 | 3.32 | 3.54 | 4.13 | no |
| KiviDB 1.2.0 | 128 | 100 | 33,891 | 1.1% | 1.4% | 0.9647 | 2.71 | 3.72 | 3.94 | 4.24 | no |
| KiviDB 1.2.0 | 256 | 100 | 23,451 | 1.0% | 0.9% | 0.9908 | 4.01 | 5.44 | 5.78 | 6.05 | no |
| KiviDB 1.2.0 | 512 | 100 | 14,992 | 1.0% | 0.5% | 0.9983 | 6.31 | 8.66 | 9.26 | 9.83 | no |
| Redis 8.10.2 (workflow flags) | 64 | 100 | 20,071 | 5.9% | 6.0% | 0.9117 | 4.77 | 7.32 | 8.86 | 10.06 | no |
| Redis 8.10.2 (workflow flags) | 128 | 100 | 19,863 | 1.3% | 0.7% | 0.9367 | 4.92 | 5.75 | 6.98 | 7.86 | no |
| Redis 8.10.2 (workflow flags) | 256 | 100 | 14,019 | 1.3% | 0.7% | 0.9791 | 7.02 | 7.40 | 8.14 | 9.19 | no |
| Redis 8.10.2 (workflow flags) | 512 | 100 | 8,973 | 2.0% | 1.3% | 0.9951 | 11.03 | 11.60 | 12.00 | 13.05 | no |
| Redis 8.10.2 + io-threads 8 | 64 | 100 | 21,198 | 0.6% | 1.9% | 0.9116 | 4.56 | 5.98 | 7.14 | 7.92 | no |
| Redis 8.10.2 + io-threads 8 | 128 | 100 | 19,310 | 0.7% | 0.8% | 0.9366 | 5.05 | 6.03 | 7.04 | 7.85 | no |
| Redis 8.10.2 + io-threads 8 | 256 | 100 | 13,677 | 2.1% | 1.2% | 0.9791 | 7.24 | 7.68 | 8.49 | 9.38 | no |
| Redis 8.10.2 + io-threads 8 | 512 | 100 | 8,794 | 2.0% | 0.6% | 0.9951 | 11.29 | 11.82 | 12.33 | 13.16 | no |
| Dragonfly 2.0.2 | 64 | 100 | 25,582 | 4.0% | 5.3% | 0.9116 | 3.72 | 5.51 | 7.04 | 8.42 | no |
| Dragonfly 2.0.2 | 128 | 100 | 25,089 | 0.7% | 3.2% | 0.9366 | 3.83 | 5.52 | 6.51 | 7.59 | no |
| Dragonfly 2.0.2 | 256 | 100 | 21,412 | 3.5% | 3.6% | 0.9791 | 4.44 | 6.47 | 8.09 | 10.09 | no |
| Dragonfly 2.0.2 | 512 | 100 | 15,452 | 0.5% | 2.1% | 0.9951 | 6.14 | 8.79 | 10.69 | 12.49 | no |

| engine | loads | upload s (median, min-max) | index wait s | upload+index s | peak RSS GiB | RSS after load GiB | used_memory after load GiB | RSS B/vector | used_memory B/vector |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| KiviDB 1.2.0 | 3 | 30.2 (29.8-32.3) | 0.09 | 30.2 | 1.26 | 1.25 | 1.17 | 1,126 | 1,055 |
| Redis 8.10.2 (workflow flags) | 3 | 60.3 (59.8-60.4) | 0.00 | 60.3 | 1.22 | 1.10 | 1.02 | 981 | 925 |
| Redis 8.10.2 + io-threads 8 | 3 | 59.8 (59.7-61.3) | 0.00 | 59.8 | 1.25 | 1.10 | 1.02 | 981 | 925 |
| Dragonfly 2.0.2 | 3 | 27.8 (27.7-27.9) | 0.00 | 27.8 | 1.06 | 1.05 | 1.07 | 905 | 965 |
| KiviDB 1.2.0-rc4 | 3 | 29.6 (29.4-29.8) | 0.08 | 29.7 | 1.26 | 1.25 | 1.17 | 1,126 | 1,055 |

### dbpedia 1536-d (1M, text-embedding-3-large)

| engine | ef | clients | q/s median (3 passes) | spread between passes | largest spread of 3 reps | recall@10 | p50 ms | p95 ms | p99 ms | p99.9 ms | client-bound? |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| KiviDB 1.2.0 | 64 | 1 | 396 | 1.7% | 5.9% | 0.9457 | 2.53 | 3.02 | 3.23 | 3.66 | no |
| KiviDB 1.2.0 | 128 | 1 | 341 | 1.8% | 4.5% | 0.9604 | 2.86 | 3.53 | 3.68 | 4.04 | no |
| KiviDB 1.2.0 | 256 | 1 | 218 | 0.8% | 3.9% | 0.9828 | 4.63 | 5.90 | 6.21 | 6.59 | no |
| KiviDB 1.2.0 | 128 | 16 | 4,509 | 3.0% | 0.7% | 0.9604 | 3.57 | 4.40 | 4.73 | 5.09 | no |
| KiviDB 1.2.0 | 128 | 100 | 6,255 | 2.4% | 6.7% | 0.9604 | 14.60 | 19.94 | 21.66 | 24.13 | no |
| Redis 8.10.2 (workflow flags) | 64 | 1 | 400 | 2.8% | 1.0% | 0.9180 | 2.53 | 2.93 | 3.03 | 3.46 | no |
| Redis 8.10.2 (workflow flags) | 128 | 1 | 352 | 1.6% | 1.0% | 0.9388 | 2.83 | 3.36 | 3.55 | 4.09 | no |
| Redis 8.10.2 (workflow flags) | 256 | 1 | 235 | 1.3% | 0.1% | 0.9728 | 4.29 | 5.26 | 5.48 | 5.84 | no |
| Redis 8.10.2 (workflow flags) | 128 | 16 | 4,832 | 1.8% | 1.6% | 0.9388 | 3.28 | 3.87 | 4.16 | 4.76 | no |
| Redis 8.10.2 (workflow flags) | 128 | 100 | 5,386 | 2.4% | 1.0% | 0.9388 | 18.15 | 19.30 | 19.79 | 20.50 | no |
| Redis 8.10.2 + io-threads 8 | 64 | 1 | 397 | 0.7% | 1.3% | 0.9169 | 2.53 | 2.93 | 3.10 | 3.33 | no |
| Redis 8.10.2 + io-threads 8 | 128 | 1 | 351 | 1.4% | 2.2% | 0.9385 | 2.87 | 3.35 | 3.55 | 4.04 | no |
| Redis 8.10.2 + io-threads 8 | 256 | 1 | 232 | 1.1% | 0.3% | 0.9728 | 4.34 | 5.31 | 5.54 | 5.79 | no |
| Redis 8.10.2 + io-threads 8 | 128 | 16 | 4,734 | 2.3% | 3.0% | 0.9385 | 3.35 | 3.96 | 4.16 | 4.78 | no |
| Redis 8.10.2 + io-threads 8 | 128 | 100 | 5,320 | 2.3% | 0.7% | 0.9385 | 18.33 | 19.43 | 19.92 | 20.32 | no |
| Dragonfly 2.0.2 | 64 | 1 | 406 | 1.3% | 2.3% | 0.9163 | 2.46 | 2.96 | 3.22 | 3.70 | no |
| Dragonfly 2.0.2 | 128 | 1 | 350 | 2.9% | 2.3% | 0.9379 | 2.85 | 3.53 | 4.04 | 5.08 | no |
| Dragonfly 2.0.2 | 256 | 1 | 224 | 4.0% | 6.2% | 0.9726 | 4.43 | 5.61 | 5.98 | 6.24 | no |
| Dragonfly 2.0.2 | 128 | 16 | 3,517 | 4.4% | 1.7% | 0.9379 | 4.40 | 4.91 | 5.42 | 5.57 | no |
| Dragonfly 2.0.2 | 128 | 100 | 6,768 | 4.6% | 5.1% | 0.9379 | 13.91 | 20.32 | 24.69 | 28.24 | no |

| engine | loads | upload s (median, min-max) | index wait s | upload+index s | peak RSS GiB | RSS after load GiB | used_memory after load GiB | RSS B/vector | used_memory B/vector |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| KiviDB 1.2.0 | 3 | 220.9 (218.5-223.6) | 0.07 | 221.0 | 12.31 | 12.30 | 12.23 | 13,202 | 13,140 |
| Redis 8.10.2 (workflow flags) | 3 | 180.8 (180.3-184.1) | 0.00 | 180.8 | 13.44 | 13.19 | 14.01 | 14,151 | 15,057 |
| Redis 8.10.2 + io-threads 8 | 3 | 183.9 (180.2-184.0) | 0.00 | 183.9 | 13.45 | 13.20 | 14.01 | 14,170 | 15,057 |
| Dragonfly 2.0.2 | 3 | 149.7 (145.6-149.9) | 0.00 | 149.7 | 7.95 | 7.93 | 7.17 | 8,467 | 7,697 |
| KiviDB 1.2.0-rc4 | 3 | 220.9 (220.0-223.3) | 0.07 | 220.9 | 12.31 | 12.30 | 12.23 | 13,203 | 13,140 |

### KiviDB 1.2.0 (75482abb) against 1.2.0-rc4 (same VMs, same session, 3 passes each)

| dataset | ef | clients | rc4 q/s | 1.2.0 q/s | 1.2.0/rc4 | run-to-run spread | verdict | rc4 recall | 1.2.0 recall | rc4 p50 / p99 / p99.9 ms | 1.2.0 p50 / p99 / p99.9 ms |
|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|
| h-and-m | 64 | 1 | 289 | 293 | 1.015 | 2.0% | same | 0.9983 | 0.9983 | 2.06 / 26.86 / 45.38 | 2.08 / 26.58 / 45.09 |
| h-and-m | 128 | 1 | 247 | 252 | 1.018 | 1.7% | faster | 0.9984 | 0.9984 | 2.42 / 16.91 / 47.20 | 2.35 / 16.53 / 45.38 |
| h-and-m | 256 | 1 | 214 | 214 | 1.004 | 3.4% | same | 0.9985 | 0.9985 | 2.54 / 20.58 / 47.20 | 2.51 / 20.37 / 46.72 |
| h-and-m | 128 | 16 | 3,131 | 3,092 | 0.988 | 1.7% | same | 0.9984 | 0.9984 | 3.05 / 21.84 / 59.97 | 3.08 / 22.16 / 60.22 |
| h-and-m | 128 | 100 | 4,527 | 4,478 | 0.989 | 3.9% | same | 0.9984 | 0.9984 | 18.16 / 84.86 / 113.15 | 17.70 / 86.99 / 122.69 |
| dbpedia-100K | 64 | 1 | 932 | 932 | 1.000 | 1.7% | same | 0.9688 | 0.9685 | 1.06 / 1.47 / 1.85 | 1.07 / 1.50 / 1.79 |
| dbpedia-100K | 128 | 1 | 602 | 609 | 1.011 | 3.3% | same | 0.9856 | 0.9864 | 1.66 / 2.29 / 2.54 | 1.64 / 2.24 / 2.54 |
| dbpedia-100K | 256 | 1 | 371 | 371 | 1.001 | 4.3% | same | 0.9936 | 0.9939 | 2.70 / 3.76 / 4.07 | 2.71 / 3.82 / 4.06 |
| dbpedia-100K | 128 | 16 | 7,328 | 7,163 | 0.978 | 3.0% | same | 0.9856 | 0.9864 | 2.17 / 2.99 / 3.40 | 2.22 / 3.07 / 3.43 |
| dbpedia-100K | 128 | 100 | 10,085 | 10,255 | 1.017 | 3.6% | same | 0.9856 | 0.9864 | 9.47 / 14.16 / 15.62 | 9.27 / 13.88 / 14.90 |
| glove-25 | 64 | 100 | 38,124 | 38,217 | 1.002 | 1.8% | same | 0.9479 | 0.9478 | 2.42 / 3.63 / 3.92 | 2.41 / 3.54 / 4.13 |
| glove-25 | 128 | 100 | 34,108 | 33,891 | 0.994 | 1.1% | same | 0.9648 | 0.9647 | 2.72 / 3.92 / 4.21 | 2.71 / 3.94 / 4.24 |
| glove-25 | 256 | 100 | 23,381 | 23,451 | 1.003 | 1.0% | same | 0.9908 | 0.9908 | 3.99 / 5.81 / 6.06 | 4.01 / 5.78 / 6.05 |
| glove-25 | 512 | 100 | 14,932 | 14,992 | 1.004 | 1.2% | same | 0.9983 | 0.9983 | 6.40 / 9.29 / 9.75 | 6.31 / 9.26 / 9.83 |
| dbpedia-1M | 64 | 1 | 394 | 396 | 1.006 | 2.3% | same | 0.9460 | 0.9457 | 2.52 / 3.22 / 3.48 | 2.53 / 3.23 / 3.66 |
| dbpedia-1M | 128 | 1 | 346 | 341 | 0.985 | 2.8% | same | 0.9609 | 0.9604 | 2.94 / 3.79 / 3.98 | 2.86 / 3.68 / 4.04 |
| dbpedia-1M | 256 | 1 | 218 | 218 | 1.002 | 4.1% | same | 0.9828 | 0.9828 | 4.55 / 6.18 / 6.51 | 4.63 / 6.21 / 6.59 |
| dbpedia-1M | 128 | 16 | 4,400 | 4,509 | 1.025 | 3.0% | same | 0.9609 | 0.9604 | 3.60 / 4.69 / 5.08 | 3.57 / 4.73 / 5.09 |
| dbpedia-1M | 128 | 100 | 6,342 | 6,255 | 0.986 | 3.7% | same | 0.9609 | 0.9604 | 14.58 / 22.17 / 27.36 | 14.60 / 21.66 / 24.13 |

| dataset | rc4 upload+index s (median, min-max) | 1.2.0 | 1.2.0/rc4 | rc4 RSS B/vector | 1.2.0 RSS B/vector | 1.2.0/rc4 |
|---|---:|---:|---:|---:|---:|---:|
| h-and-m | 15.1 (14.5-15.1) | 14.9 (14.8-14.9) | 0.985 | 21,920 | 21,936 | 1.001 |
| dbpedia-100K | 11.9 (11.5-11.9) | 11.7 (11.6-11.8) | 0.986 | 13,266 | 13,265 | 1.000 |
| glove-25 | 29.7 (29.5-29.9) | 30.2 (29.9-32.4) | 1.019 | 1,126 | 1,126 | 1.000 |
| dbpedia-1M | 220.9 (220.1-223.3) | 221.0 (218.5-223.7) | 1.000 | 13,203 | 13,202 | 1.000 |


## 6. KiviDB as a cache (issue #202)

From [cache-pi](cache-pi/README.md): the reporter's harness, unchanged. One server core, `maxmemory` 1 GiB, 2,097,152 keys up to 1 KiB with Zipf popularity, GET then SET on a miss. *F* is the reporter's length (33,554,432 operations), *S* a quarter of it, run for 1.1.3 like for like. Median of the runs.

| Length | Policy | Engine | Hit rate | Peak memory (VmHWM) | GETs/s | Hits/s | P50 / P99 / P99.9 µs | Server CPU |
|---|---|---|---:|---:|---:|---:|---:|---:|
| F | allkeys-lfu | KiviDB 1.2.0 | 60.96% | 1,066 MiB | 49,089 | 29,927 | 428 / 1,083 / 1,869 | 98% |
| F | allkeys-lfu | Redis 8.10.3 | 60.78% | 1,057 MiB | 53,312 | 32,401 | 389 / 1,286 / 2,809 | 97% |
| F | allkeys-lfu | Redis unstable 6bf6224 | 60.77% | 1,057 MiB | 53,021 | 32,221 | 390 / 1,286 / 2,832 | 96% |
| F | allkeys-lru | KiviDB 1.2.0 | 60.38% | 1,066 MiB | 49,462 | 29,865 | 427 / 1,144 / 1,989 | 98% |
| F | allkeys-lru | Redis 8.10.3 | 59.96% | 1,057 MiB | 52,714 | 31,606 | 394 / 1,315 / 2,866 | 96% |
| F | allkeys-lru | Redis unstable 6bf6224 | 59.96% | 1,057 MiB | 54,424 | 32,631 | 391 / 1,306 / 2,843 | 94% |
| S | allkeys-lfu | KiviDB 1.2.0 | 60.96% | 1,046 MiB | 50,952 | 31,059 | 414 / 1,097 / 2,025 | 97% |
| S | allkeys-lfu | KiviDB 1.1.3 | 57.19% | 1,224 MiB | 6,648 | 3,802 | 3,323 / 8,286 / 9,962 | 99% |
| S | allkeys-lfu | Redis 8.10.3 | 60.72% | 1,057 MiB | 53,738 | 32,628 | 395 / 1,238 / 2,700 | 95% |
| S | allkeys-lru | KiviDB 1.2.0 | 60.40% | 1,046 MiB | 50,744 | 30,646 | 418 / 1,122 / 1,949 | 97% |
| S | allkeys-lru | KiviDB 1.1.3 | 59.05% | 1,224 MiB | 6,974 | 4,118 | 3,151 / 8,099 / 9,803 | 99% |
| S | allkeys-lru | Redis 8.10.3 | 59.97% | 1,057 MiB | 53,024 | 31,802 | 397 / 1,232 / 2,744 | 95% |

## 7. The rc8 and rc10 rechecks

Each recheck alternated the new build with the previous one on the same VMs. A row is *slower* only if every round of the new build is below every round of the old one and the medians differ by more than 1%.

| Suite | rc8 against rc7 | rc10 (1.2.0) against rc8 |
|---|---|---|
| [core](core/README.md) | 13 of 14 rows level; **MSET-10 172K -> 569K (+232%)**, p99 9.63 -> 2.89 ms ([core/rc8](core/rc8/README.md)) | level on every row ([core/rc10](core/rc10/README.md)) |
| [datatypes](datatypes/README.md) | 42 of 42 rows level ([datatypes/rc8](datatypes/rc8/README.md)) | 42 of 42 level ([datatypes/rc10](datatypes/rc10/README.md)) |
| [profiling](profiling/README.md) | instructions per command level ([profiling/rc8](profiling/rc8/README.md)) | KiviDB's own code within 0.4% ([profiling/rc10](profiling/rc10/README.md)) |
| [memory](memory/README.md) | no regression ([memory/rc8](memory/rc8/README.md)) | all pass; bytes per key identical, because the 64 MB `repl-backlog-size` is never allocated without a replica ([memory/rc10](memory/rc10/README.md)) |
| [vector](vector/README.md) | level ([vector/rc8](vector/rc8/README.md)) | 10 of 10 rows level ([vector/rc10](vector/rc10/README.md)) |
| [cache-pi](cache-pi/README.md) | level ([cache-pi/rc8](cache-pi/rc8/README.md)) | rc10 / rc8 ratio: LFU 1.007, LRU 0.983 and 0.995, inside the 0.98-1.02 that pairs of identical code measure ([cache-pi/rc10](cache-pi/rc10/README.md)) |

rc9 was checked in [failover/rc9](failover/rc9/README.md): core benchmark rc9 against rc8 was GET p1 -0.7% (client-bound), GET p16 +0.4%, SET p16 +1.6%, MSET-10 -0.3%.

## 8. Release tests: smoke, soak, failover, operator

These tests count acknowledged writes rather than throughput. Every write a server acknowledged was read back afterwards.

### Smoke: every published artifact

| Build | Result |
|---|---|
| [rc7](smoke/README.md) | 36 of 36 artifact runs pass (2,238 checks), plus 144 new checks per architecture. One failure: the `.rpm` did not install without `shadow-utils` (fixed in rc8) |
| [rc10](smoke/rc10/README.md) | every artifact passes every check: binaries (4 variants), `.deb`, `.rpm`, GHCR and Docker Hub images, amd64 and arm64 |
| 1.2.0 | the release's smoke-test workflow passed (run 41696230733) |

### Soak: hours of load with AOF, replicas, failovers and reshards

| Build | Run | Acknowledged writes | Lost | Notes |
|---|---|---:|---:|---|
| [rc7](soak/README.md) | standalone, 2 h, master + replica, AOF on, 100k ops/s, master SIGTERM and replica `kill -9` | 5,623,474 | 0 | a replica that full-syncs under heavy writes could loop (fixed in rc9, #305) |
| [rc7](soak/README.md) | cluster, 2 h, 3 shards x (master + replica), 5 failovers, 4 reshards | 11,056,954 | 0 | |
| [rc8](soak/rc8/README.md) | cluster, 2 h, the same events | 10,872,312 | 0 | `redis-cli --cluster fix` could not close a reshard left open under load (#295, fixed in rc10) |
| [rc8](soak/rc8/README.md) | standalone, 20 min | 950,019 | 0 | master vs replica: 0 differences in 1,628,568 keys |
| [rc10](soak/rc10/README.md) | cluster, 2 h, 2 x `CLUSTER FAILOVER`, 3 x `kill -9`, 2 x `CLUSTER MIGRATION IMPORT` of 500 slots, 3 x `redis-cli --cluster reshard` of 128 slots, 4 x replica SIGTERM | 6,577,270 | 0 | 6,172,947 overwrites, 0 stale; master vs replica 0 differences in 7,856,315 keys; reshards 3 of 3 exit 0 but slow (749-1,629 s, #310) |

rc10 failover times: `CLUSTER FAILOVER` 0.02 / 0.04 s; `kill -9` promotion 5.20 / 5.55 / 5.90 s with `cluster-node-timeout 5000`.

### Failover and durability

| Build | Checks | Result |
|---|---|---|
| [rc7](failover/README.md) | 17 failover scenarios x 3 runs, rc7 and Redis 8.10.3 (102 runs, 33.3M acknowledged writes) | 0 lost wherever nothing is promoted past unreplicated writes. One write was lost to the asynchronous-replication window (a replica promoted past a write it never got) |
| [rc7](failover/README.md) | 5 power-offs under writes, AOF on | started by itself 5 of 5 (rc4: 0 of 2), including an AOF ending in 550 zero bytes. Lost 3,051 / 1,842 / 2,260 / 1,093 / 0 writes, all acknowledged in the last 0.256 s, the documented `everysec` window |
| [rc7](failover/README.md) | expired keys after AOF replay; slab race (20 of 20); `MIGRATE` on tokio with a slow destination; replica with its own AOF | all pass; replica memory flat at 0.91 GB (rc4: 7.06 GB and rising) |
| [rc8](failover/rc8/README.md) | 16 runs of kill, stop, partition and failover (11.24M acknowledged); 2 power-offs; two engines in one data directory (#303), 8 of 8; `MIGRATE` stopped by `TRYAGAIN`, 12 of 12 | 0 lost, 0 wrong values; power-off rep 1 lost 1,329 of 3,402,046, all in the last 0.121 s |
| [rc9](failover/rc9/README.md) | clean master restart 10x; forced full sync of 3.4M keys at 100k ops/s; 10 x `CLUSTER FAILOVER`; `kill -9` during a full sync; SIGTERM mid full sync; 30-min soak (1,415,323 acknowledged) | all pass: 10 of 10 partial resyncs, full sync converged first time, the syncing replica did not stand for election, 0 lost. Found a replica shutdown deadlock (#308, fixed in rc10) |

### Kubernetes operator on AKS (operator 0.7.0)

| Build | Acknowledged writes read back | Lost | Notes |
|---|---:|---:|---|
| [rc7](operator/README.md) | ~21.9M across 9 loaders | 121,939 in one scenario | a forced pod delete started a second engine on the same volume (#303, fixed in rc8). Everything else 0 lost: upgrade 1.1.3 -> rc7, rolling restarts, failovers, drains, reshard 3 -> 4 -> 3, backup and restore |
| [rc8](operator/rc8/README.md) | 8,524,842 across 6 loaders | 0 | the #303 repro: 0 lost in 6 same-node force deletes |
| [rc9](operator/rc9/README.md) | 3,317,045 across 3 loaders | 0 | every demoted master continued with a partial resync; 0 full syncs in rolling restarts, failovers and deletes |

## 9. Where KiviDB loses

Each suite's README says why.
- **Garnet** is faster on `SET` at pipeline 16 (9.83M against 7.94M, +24%) and on `MSET` of 10 keys (800K against 569K). KiviDB's `MSET` has the lower tail: p99 2.89 against 7.90 ms.
- **Redis Cluster** (16 shards, one per vCPU) is ahead on `HSET` and `INCR` at pipeline 16, where the load generator is the limit.
- **Dragonfly** uses less memory per vector and during a snapshot (+0.4% against +10%), and fewer bytes per key for ASCII values (warefeats: 137.1 against 149.2).
- **Redis** ingests vectors faster, and serves 3-9% more cache GETs a second in the #202 test.

## 10. What is not covered

- **Garnet** ran in core only: not in data types, memory, vector or the cache test.
- **Dragonfly** did not run the #202 cache test.
- **kvrocks** ran in data types only. It keeps its data in RocksDB on disk, a different class of store.
- **Valkey** is not in vector or the cache test.
- **The rechecks (section 7)** compared KiviDB builds with each other. Only core/rc8 re-ran competitors, one round each, and they landed within -4% / +6% of their published medians.
- **Known issues in 1.2.0** ([release notes](../../docs/releases/v1.2.0.md)): #277, #300, `WAIT` on io_uring, and #310. Under #310, a `redis-cli` reshard under heavy writes is slow (12-27 min), and on io_uring the source's write p99 is about 1 s, against about 100 ms on tokio.
