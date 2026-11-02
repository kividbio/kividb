# Core string benchmark: KiviDB 1.2.0 against Redis, Redis Cluster, Valkey, Dragonfly and Garnet

GET and SET at 100 B, 1 KB and 2 KB, with and without pipelining, plus MGET and MSET
of 10 keys. Seven engines ran one at a time on the same 32-vCPU arm64 server, measured
from two load generators at once. Every row has at least three rounds and p50 / p99 / p99.9.
Run on 2026-11-02 for the 1.2.0 release, on the code that ships: `release/1.2.0` at
commit `75482ab` (it reports `1.2.0-rc6` until rc7 is tagged from it). The same session
re-ran 1.2.0-rc4 alongside it as a regression check.

The run answers these points from the [Show HN thread](https://news.ycombinator.com/item?id=50152541):

| Asked on HN | Where it is answered |
|---|---|
| "Past ~5M ops/sec we needed 3 separate memtier hosts ... Any p99 numbers alongside it?" ([falconBrisk47](https://news.ycombinator.com/item?id=50153410)) | Two load generators run together, client CPU is recorded for every row, and rows where the clients (or the NIC) were the limit are marked. p50/p99/p99.9 come from the two generators' histograms merged. |
| "Could you add Valkey ... big values (2KB) and small values (<256B)" ([edoceo](https://news.ycombinator.com/item?id=50153931)), "And MS Garnet" ([esafak](https://news.ycombinator.com/item?id=50154673)) | Valkey 9.1.3 and Garnet 2.2.5 are both included, at 100 B, 1 KB and 2 KB. |
| "running 46 redis instances under the pretence that Redis is mono-threaded ... Redis uses multi-threading on io" ([Shamanoid](https://news.ycombinator.com/item?id=50156303)) | Redis and Valkey run with `io-threads`, the count picked by a sweep (4/8/16/24). Redis Cluster is a separate row, one shard per core. |
| "whether the advantage ... stems primarily from more efficient I/O batching or from faster command execution" ([AtalantaDB](https://news.ycombinator.com/item?id=50161446)), "why its so much faster than redis" ([shepardrtc](https://news.ycombinator.com/item?id=50154950)) | [I/O batching or command execution?](#io-batching-or-command-execution) splits server CPU per request into user and kernel time at pipeline depths 1, 4, 16 and 64. |

## Summary

- **Pipelined 100 B: KiviDB and Garnet lead and are close on GET; Garnet leads on SET.**
  - GET at pipeline 16: KiviDB 1.2.0 10.45M/s, Garnet 10.69M/s (+2%).
  - SET at pipeline 16: KiviDB 7.94M/s, Garnet 9.83M/s (**+24% for Garnet**).
  - Both are well ahead of Dragonfly (5.66M / 4.87M) and Redis Cluster (5.70M / 5.79M, client-bound). KiviDB is 6-7× Redis with io-threads (1.41M / 1.25M) and 9-11× Valkey (1.20M / 0.72M).
  - KiviDB has the lowest tail latency of the 100 B pipelined rows: GET p99 0.78 ms, against Garnet 1.72 ms, Dragonfly 1.33 ms and Redis 4.00 ms.
- **Without pipelining**, KiviDB (1.73M GET) and Dragonfly (1.65M) both use up our two load generators (95% CPU), so those rows are a floor, not a ceiling. Garnet does 1.05M with a high tail: p99 6.4 ms against about 1 ms for KiviDB and Dragonfly.
- **1 KB and 2 KB values**: with pipelining, every fast engine stops at the same rate, the server's 16 Gbps NIC: ~1.87M GET/s at 1 KB and ~0.94M at 2 KB. These rows compare latency and CPU, not throughput. (In one 1 KB round the NIC briefly carried twice that, and KiviDB served 3.36M GET/s; see [Caveats](#caveats).)
- **MGET of 10 keys**: KiviDB leads at 1.16M commands/s (11.6M keys/s), against Garnet 0.79M and Dragonfly 0.64M; KiviDB's row is client-bound.
- **MSET of 10 keys is KiviDB's clear loss**: 166K commands/s against Garnet 761K (4.6×) and Dragonfly 557K (3.4×). It is about level with Redis (150K). See [MSET](#mset-of-10-keys-a-clear-loss).
- **1.2.0 against 1.2.0-rc4** (six alternating rounds each, same machines, same session): **no regression.**
  - All 14 rows are inside the run-to-run spread. Medians are −0.5% to +0.7%, and p99 moved by at most 0.15 ms.
  - Server CPU profiles of the two builds under load are the same, function by function. See [Against 1.2.0-rc4](#against-120-rc4).
- **1.2.0 against 1.1.3**:
  - 100 B SET at pipeline 16 is still **3.1% slower** (8.19M → 7.94M). This is the cost rc4 already showed (−2.8%), from the write-order lock every write takes since #271.
  - 2 KB SET p1 is 9% faster, MSET 8% and MGET 3%. Every other row is level.

![100 B throughput](charts/throughput-100.svg)

## Hardware and setup

| | |
|---|---|
| Cloud | Azure, eastasia. All three VMs in one proximity placement group (one datacenter), accelerated networking. |
| Server | `Standard_D32ps_v5`: 32 vCPU Ampere Altra (Neoverse-N1), 128 GB, 16 Gbps network |
| Load generators | 2 × `Standard_D16ps_v5`: 16 vCPU, 64 GB, 12.5 Gbps each |
| OS | Ubuntu 24.04.5 LTS arm64, kernel 7.0.0-1017-azure |
| Load tool | memtier_benchmark 2.5.2 (built from its tag) |

These are the same VM sizes, region, CPU and kernel as the 1.2.0-rc4 run of 2026-11-01. The machines are new.

**How this compares with what we published before**
- The README's bare-metal figures (21.2M GET at pipeline 16) came from an AWS `c7gn.metal` server confined to 32 cores and a 64-vCPU load generator, with a 200 Gbps network.
- This run is on a smaller, older core (Neoverse-N1 vs Graviton3), a 16 Gbps NIC, and 32 vCPUs of load generation in total. So every engine's absolute numbers here are lower, and the 1 KB and 2 KB rows hit the NIC.
- The VM sizes match [benchmarks/LATEST.md](../../LATEST.md) (D32ps_v5 server). LATEST.md used one D32ps_v5 client at pipeline 10; this run uses two clients.
- Why two 16-vCPU load generators and not three: the region's quota is 65 vCPUs, so 32 server + 2×16 client is the most we could run.

## Engines and flags

Every engine runs alone on the server, on all 32 vCPUs, with persistence off and no memory limit. The exact command lines and versions are in [results/launch.log](results/launch.log).

| Engine | Version | Command (beyond `--port 6379 --bind 0.0.0.0`) | Threads |
|---|---|---|---|
| KiviDB 1.2.0 | `release/1.2.0` at `75482ab` (reports `1.2.0-rc6`), default variant | none | 30 workers (its default, vCPUs − 2), io_uring |
| KiviDB 1.2.0-rc4 | tag `v1.2.0-rc4` = `a6b1bf3`, default variant; regression check only | none | same |
| KiviDB 1.1.3 | release binary `kividb-linux-aarch64` | none | same |
| Redis | 8.10.3, from its release tag, plain `make` | `--save "" --appendonly no --protected-mode no --io-threads 24` | 1 command thread + I/O threads |
| Redis Cluster | 8.10.3 | 32 primaries on ports 7000-7031, no replicas, `--cluster-enabled yes`, same persistence flags | 32 single-threaded shards (one per vCPU) |
| Valkey | 9.1.3 (project's noble arm64 package) | `--save "" --appendonly no --protected-mode no --io-threads 24` | 1 command thread + I/O threads |
| Dragonfly | 2.0.2 release binary | `--dbfilename "" --logtostderr` | defaults: one proactor thread per vCPU |
| Garnet | 2.2.5 release package on .NET 10.0.14 | `--index 1g --memory 64g` | defaults (.NET thread pool) |

Notes on the flags:
- **KiviDB** has no scheduled snapshots and the AOF is off by default. All three builds are the default variant (no Lua, no TLS), built for arm64 as `release.yml` builds them (`cargo build --release`, `RUSTFLAGS="-C target-feature=+lse"`).
  - 1.2.0 and rc4 have no GitHub release assets, so both were built on the server, from the commit and the tag, with rustc 1.99.1 (the toolchain of the rc4 run): [results/kividb-candidate-build.txt](results/kividb-candidate-build.txt), [results/kividb-rc4-build.txt](results/kividb-rc4-build.txt).
- **Garnet**'s default log memory (16 GB) cannot hold 10M × 2 KB without its disk tier, so it was raised to 64 GB. The hash index is sized for 10M keys.
- **Redis and Valkey io-threads**: we use 24, chosen by the sweep in the 1.2.0-rc4 run ([results/io-threads-sweep.csv](results/io-threads-sweep.csv); 100 B, 10M keys, one round each). That sweep ran on the same VM sizes, CPU and kernel, so it was not repeated. 24 had the best geometric mean of GET p1, GET p16 and SET p16.

  | io-threads | Redis GET p1 / GET p16 / SET p16 | Valkey GET p1 / GET p16 / SET p16 |
  |---|---|---|
  | 4 | 311K / 1.35M / 1.32M | 299K / 1.22M / 0.75M |
  | 8 | 559K / **1.65M** / **1.45M** | 607K / 1.19M / 0.69M |
  | 16 | 762K / 1.42M / 1.26M | 588K / 1.22M / 0.70M |
  | 24 | **785K** / 1.44M / 1.29M | **613K** / **1.27M** / 0.69M |

  Redis at **io-threads 8 was 13-14% faster at pipeline 16** in that sweep (1.65M GET, 1.45M SET), and 29% slower without pipelining. Neither setting changes how the engines rank.

## Method

**Dataset and workloads**
- 10,000,000 keys. Before each value size, the key space is emptied and every key written once: each load generator writes half, at pipeline 16.
- Then each scenario runs for 20 s after a 5 s warm-up, with uniform random keys over all 10M. The same dataset, key space and value sizes are used for every engine.
- GET and SET at pipeline 1 and pipeline 16, at 100 B, 1 KB and 2 KB.
  - Pipeline 1: 2 × 15 threads × 40 connections = 1,200 connections.
  - Pipeline 16: 2 × 15 × 10 = 300 connections.
  - These are the connection counts of the README's run.
- At 100 B only:
  - GET at depths 1, 4 and 64 with 300 connections, for the pipeline-depth chart (depth 16 is the main pipeline-16 row).
  - `MGET` and `MSET` of 10 random keys at pipeline 1 with 1,200 connections. They are not run on Redis Cluster: ten random keys are in different hash slots and the commands are refused.

**Rounds** (all times UTC, 2026-11-02)
- KiviDB first: 1.2.0, rc4 and 1.1.3 in turn, three rounds (1.2.0, rc4, 1.1.3, 1.2.0, ...; 13:57-15:01). The regression comparison shares one time window.
- Then the competitors, three rounds, round-robin: each round runs every engine once (15:01-16:52).
- Then a fourth 1.2.0 / rc4 round (16:52-17:06), and a fourth Redis round to check drift (17:06-17:14).
- Then two more 1.2.0 / rc4 rounds (17:14-17:42), as the regression check asks for whenever a row looks slower. Here that was one side row: GET at depth 1, 300 connections, −0.4% after four rounds. 1.2.0 and rc4 therefore have six rounds each, 1.1.3 and the competitors three.
- Drift:
  - **KiviDB**: rounds 4-6 were 0-2% below rounds 1-3, for both 1.2.0 and rc4 alike.
  - **Redis**: the fourth round ([results/drift-check-redis.csv](results/drift-check-redis.csv)) landed within 2% of the first three on five of the six main 100 B rows. GET p16 was 4.8% lower, inside the first three rounds' range. GET at depths 1 and 4 with 300 connections was 6-7% higher.
- Each table and chart shows the median of the rounds. "±" is half the min-max spread, and the whiskers on the charts are min and max.

**Measurements**
- Both load generators run the same memtier command at the same moment. Throughput is their sum.
- Latency percentiles come from their two HdrHistograms merged (exact), not from averaging percentiles.
- During the middle 60% of every scenario, `sample.sh` records machine-wide CPU on all three machines (user, system, softirq) and the server's NIC bytes.

**Limit flags**
- **C**: the load generators were the limit (either above 90% CPU).
- **N**: the server's NIC was the limit (above 85% of 2 GB/s).
- A flagged row says how fast the engine was *at least*, not how fast it can go.

## Results

Each cell gives requests/s, then **p50 / p99 / p99.9 in ms**. A flag after the throughput means the row was limited by **C** (the load generators) or **N** (the server NIC). Raw per-round rows are in [results/runs.csv](results/runs.csv), and the medians in [results/summary.csv](results/summary.csv).

#### 100 B values

| Engine | GET p1 | GET p16 | SET p1 | SET p16 |
|---|---:|---:|---:|---:|
| KiviDB 1.2.0 | 1.73M ±1.2% C<br>0.68 / 1.03 / 1.89 | 10.45M ±0.8%<br>0.44 / 0.78 / 1.23 | 1.70M ±1.2% C<br>0.69 / 1.06 / 2.00 | 7.94M ±0.9%<br>0.59 / 0.99 / 1.27 |
| KiviDB 1.1.3 | 1.75M ±1.1% C<br>0.68 / 1.04 / 1.97 | 10.24M ±1.2%<br>0.46 / 0.80 / 1.25 | 1.70M ±0.8% C<br>0.69 / 1.05 / 1.94 | 8.19M ±0.1%<br>0.57 / 0.97 / 1.26 |
| Redis 8.10.3, io-threads 24 | 764K ±2.8%<br>1.57 / 2.11 / 3.62 | 1.41M ±4.0%<br>3.41 / 4.00 / 6.91 | 692K ±2.5%<br>1.73 / 2.30 / 3.79 | 1.25M ±3.2%<br>3.85 / 4.42 / 7.71 |
| Redis 8.10.3 Cluster, 32 shards | 820K ±1.8% C<br>20.73 / 28.03 / 36.61 | 5.70M ±1.8% C<br>20.73 / 24.96 / 26.75 | 945K ±4.2% C<br>17.02 / 26.88 / 30.08 | 5.79M ±1.1% C<br>20.09 / 25.73 / 32.64 |
| Valkey 9.1.3, io-threads 24 | 601K ±2.1%<br>1.98 / 2.75 / 3.92 | 1.20M ±2.6%<br>4.01 / 4.70 / 4.83 | 424K ±2.0%<br>2.83 / 3.66 / 4.45 | 723K ±1.4%<br>6.66 / 7.55 / 7.81 |
| Dragonfly 2.0.2 | 1.65M ±0.2% C<br>0.72 / 1.11 / 2.17 | 5.66M ±0.7%<br>0.83 / 1.33 / 1.76 | 1.62M ±0.4% C<br>0.73 / 1.13 / 2.27 | 4.87M ±0.8%<br>0.97 / 1.69 / 4.99 |
| Garnet 2.2.5 | 1.05M ±1.0%<br>0.61 / 6.43 / 10.11 | 10.69M ±1.4% C<br>0.38 / 1.72 / 3.20 | 1.04M ±0.6%<br>0.61 / 6.11 / 9.92 | 9.83M ±0.1%<br>0.38 / 2.40 / 4.06 |

![100 B latency](charts/latency-100.svg)

#### 1 KB values

| Engine | GET p1 | GET p16 | SET p1 | SET p16 |
|---|---:|---:|---:|---:|
| KiviDB 1.2.0 | 1.63M ±1.9% C N<br>0.73 / 1.08 / 2.10 | 1.87M ±0.0% N\*<br>1.30 / 7.09 / 7.78 | 1.61M ±2.0% C N<br>0.73 / 1.12 / 2.31 | 2.80M ±0.1% N<br>0.71 / 7.28 / 7.71 |
| KiviDB 1.1.3 | 1.64M ±2.2% C N<br>0.72 / 1.07 / 2.02 | 1.87M ±0.1% N<br>1.29 / 7.13 / 7.81 | 1.62M ±0.5% C N<br>0.73 / 1.09 / 2.16 | 2.80M ±0.1% N<br>0.69 / 7.33 / 7.71 |
| Redis 8.10.3, io-threads 24 | 689K ±1.1%<br>1.74 / 2.32 / 3.81 | 1.16M ±1.6%<br>3.21 / 6.40 / 6.75 | 643K ±2.7%<br>1.86 / 2.43 / 3.97 | 1.09M ±1.1%<br>4.08 / 8.32 / 8.70 |
| Redis 8.10.3 Cluster, 32 shards | 777K ±0.8% C<br>21.38 / 28.80 / 36.86 | 1.82M ±0.1% C N<br>16.77 / 258.05 / 520.19 | 954K ±6.3% C<br>15.81 / 28.03 / 31.49 | 2.73M ±0.1% C N<br>33.02 / 88.58 / 247.81 |
| Valkey 9.1.3, io-threads 24 | 573K ±1.9%<br>2.10 / 2.90 / 3.97 | 1.14M ±3.2%<br>3.76 / 11.13 / 11.90 | 623K ±2.1%<br>1.91 / 2.70 / 3.82 | 1.00M ±1.4%<br>4.42 / 12.35 / 13.25 |
| Dragonfly 2.0.2 | 1.50M ±0.2% C<br>0.79 / 1.21 / 2.54 | 1.86M ±0.0% N<br>1.73 / 6.46 / 9.34 | 1.44M ±0.6% C<br>0.82 / 1.26 / 2.61 | 2.79M ±0.2% N<br>1.25 / 5.44 / 7.29 |
| Garnet 2.2.5 | 1.00M ±1.1%<br>0.63 / 6.17 / 9.73 | 1.86M ±0.1% N<br>1.23 / 8.32 / 9.79 | 978K ±0.8%<br>0.66 / 6.53 / 10.43 | 2.80M ±0.1% N<br>0.66 / 7.20 / 9.21 |

\* Five of KiviDB 1.2.0's six rounds measured 1.872-1.873M. Round 6 measured 3.36M while the server's NIC briefly carried 3.8 GB/s, twice its usual cap. The spread shown leaves that round out. [Caveats](#caveats) has the details, and [results/runs.csv](results/runs.csv) has the round.

![1 KB throughput](charts/throughput-1024.svg)

#### 2 KB values

| Engine | GET p1 | GET p16 | SET p1 | SET p16 |
|---|---:|---:|---:|---:|
| KiviDB 1.2.0 | 920K ±0.0% N<br>1.15 / 3.04 / 4.25 | 938K ±0.1% N<br>7.07 / 9.21 / 9.86 | 1.24M ±1.1% C N<br>0.95 / 1.43 / 2.97 | 1.43M ±0.1% N<br>0.94 / 9.18 / 10.14 |
| KiviDB 1.1.3 | 919K ±0.1% N<br>1.17 / 3.01 / 4.35 | 938K ±0.1% N<br>7.07 / 9.21 / 9.92 | 1.14M ±0.2% N<br>1.03 / 1.71 / 3.20 | 1.43M ±0.1% N<br>0.97 / 9.09 / 9.98 |
| Redis 8.10.3, io-threads 24 | 651K ±2.1%<br>1.82 / 2.64 / 4.83 | 938K ±0.0% N<br>5.12 / 9.41 / 10.05 | 560K ±1.2%<br>2.14 / 2.75 / 4.45 | 967K ±9.2% N<br>5.38 / 5.98 / 6.24 |
| Redis 8.10.3 Cluster, 32 shards | 700K ±0.2% C<br>24.06 / 32.77 / 37.38 | 914K ±0.0% N<br>11.13 / 532.48 / 1073.15 | 872K ±6.5% C<br>16.32 / 30.21 / 35.58 | 1.40M ±0.1% C N<br>22.27 / 221.18 / 466.94 |
| Valkey 9.1.3, io-threads 24 | 574K ±3.5%<br>2.08 / 2.94 / 4.38 | 936K ±0.0% N<br>4.77 / 10.18 / 11.97 | 541K ±1.6%<br>2.30 / 3.46 / 4.38 | 806K ±2.2%<br>6.69 / 7.62 / 7.81 |
| Dragonfly 2.0.2 | 921K ±0.1% N<br>1.12 / 3.06 / 4.35 | 934K ±0.1% N<br>6.82 / 9.73 / 11.20 | 1.08M ±0.1% N<br>1.09 / 1.76 / 3.17 | 1.43M ±0.1% N<br>1.70 / 8.64 / 9.28 |
| Garnet 2.2.5 | 890K ±0.5% N<br>0.83 / 6.21 / 9.73 | 936K ±0.1% N<br>7.07 / 9.54 / 10.30 | 819K ±1.8% N<br>1.08 / 7.52 / 11.97 | 1.43M ±0.0% N<br>1.29 / 9.66 / 10.43 |

![2 KB throughput](charts/throughput-2048.svg)

Latency charts for 1 KB and 2 KB: [latency-1024.svg](charts/latency-1024.svg), [latency-2048.svg](charts/latency-2048.svg).

#### MGET / MSET of 10 keys, 100 B, pipeline 1 (commands/s; keys/s is ten times that)

| Engine | MGET 10 keys | MSET 10 keys |
|---|---:|---:|
| KiviDB 1.2.0 | 1.16M ±1.9% C<br>1.02 / 1.46 / 3.76 | 166K ±0.8%<br>7.26 / 9.95 / 10.97 |
| KiviDB 1.1.3 | 1.12M ±0.7% C<br>1.05 / 1.53 / 3.90 | 153K ±1.4%<br>7.78 / 12.22 / 14.02 |
| Redis 8.10.3, io-threads 24 | 136K ±1.0%<br>8.89 / 9.79 / 17.54 | 150K ±2.5%<br>8.03 / 8.77 / 15.81 |
| Valkey 9.1.3, io-threads 24 | 145K ±2.1%<br>8.26 / 9.28 / 9.66 | 94K ±2.2%<br>12.80 / 14.27 / 14.78 |
| Dragonfly 2.0.2 | 640K ±0.1%<br>1.85 / 2.85 / 5.02 | 557K ±0.8%<br>2.08 / 4.48 / 6.82 |
| Garnet 2.2.5 | 788K ±0.8%<br>0.86 / 7.13 / 11.46 | 761K ±0.2%<br>0.91 / 7.42 / 11.71 |

![MGET and MSET](charts/multikey.svg)

#### Throughput by pipeline depth (GET, 100 B, 300 connections)

| Engine | depth 1 | depth 4 | depth 16 | depth 64 |
|---|---:|---:|---:|---:|
| KiviDB 1.2.0 | 1.21M | 4.23M | 10.45M | 16.72M C N |
| KiviDB 1.1.3 | 1.22M | 4.19M | 10.24M | 16.28M C N |
| Redis 8.10.3, io-threads 24 | 563K | 1.03M | 1.41M | 1.27M |
| Redis 8.10.3 Cluster, 32 shards | 1.22M C | 2.41M C | 5.70M C | 8.62M C |
| Valkey 9.1.3, io-threads 24 | 578K | 900K | 1.20M | 1.58M |
| Dragonfly 2.0.2 | 1.09M | 2.99M | 5.66M | 8.07M |
| Garnet 2.2.5 | 1.06M | 3.86M | 10.69M C | 17.23M C N |

![Throughput by pipeline depth](charts/pipeline-depth.svg)

#### KiviDB 1.2.0 against 1.1.3

Median, with [min - max] and the number of rounds. 1.1.3 ran three rounds, interleaved with the first three of 1.2.0 in the same hour. "Slower" means 1.2.0's best round is below 1.1.3's worst.

| Scenario | 1.1.3 | 1.2.0 | Change | Beyond spread? |
|---|---:|---:|---:|---|
| 100 B GET p1 | 1.75M [1.73M - 1.77M] (3) | 1.73M [1.72M - 1.76M] (6) | −0.9% | no |
| 100 B GET p16 | 10.24M [10.19M - 10.45M] (3) | 10.45M [10.41M - 10.59M] (6) | +2.0% | no |
| 100 B SET p1 | 1.70M [1.69M - 1.72M] (3) | 1.70M [1.68M - 1.72M] (6) | −0.2% | no |
| 100 B SET p16 | 8.19M [8.19M - 8.20M] (3) | 7.94M [7.88M - 8.02M] (6) | **−3.1%** | **slower** |
| 1 KB GET p1 | 1.64M [1.58M - 1.66M] (3) | 1.63M [1.59M - 1.65M] (6) | −1.1% | no (NIC) |
| 1 KB GET p16 | 1.87M [1.87M - 1.88M] (3) | 1.87M [1.87M - 3.36M\*] (6) | 0.0% | no (NIC) |
| 1 KB SET p1 | 1.62M [1.61M - 1.62M] (3) | 1.61M [1.55M - 1.61M] (6) | −0.5% | no (NIC) |
| 1 KB SET p16 | 2.80M [2.80M - 2.80M] (3) | 2.80M [2.79M - 2.80M] (6) | 0.0% | no (NIC) |
| 2 KB GET p1 | 919K [919K - 920K] (3) | 920K [920K - 920K] (6) | +0.1% | no (NIC) |
| 2 KB GET p16 | 938K [937K - 938K] (3) | 938K [937K - 939K] (6) | 0.0% | no (NIC) |
| 2 KB SET p1 | 1.14M [1.13M - 1.14M] (3) | 1.24M [1.23M - 1.26M] (6) | +9.1% | faster |
| 2 KB SET p16 | 1.43M [1.43M - 1.43M] (3) | 1.43M [1.43M - 1.43M] (6) | 0.0% | no (NIC) |
| 100 B MGET10 p1 | 1.12M [1.12M - 1.13M] (3) | 1.16M [1.14M - 1.18M] (6) | +3.1% | faster |
| 100 B MSET10 p1 | 153K [152K - 157K] (3) | 166K [165K - 167K] (6) | +8.0% | faster |

\* The NIC burst of round 6; see the 1 KB table.

- The SET p16 loss is the one rc4 already had against 1.1.3 (−2.8% in the rc4 run). It comes from the key's write-order lock that every write takes since 1.2.0 (#271), so that read-modify-write commands cannot lose updates. CPU per SET rose by about the same share (3.46 → 3.56 µs).
- 2 KB GET p1 is formally "faster" (+0.1%), but it is a NIC-bound row and the difference is noise; it is marked "no" here.

## Against 1.2.0-rc4

The regression check for the code that ships. 1.2.0 (`75482ab`) and rc4 alternated for six rounds on the same machines: rounds 1-3 between 13:57 and 15:01 UTC, rounds 4-6 between 16:52 and 17:42. The first three rounds also included 1.1.3. "Slower" would mean 1.2.0's best round is below rc4's worst.

| Scenario | 1.2.0-rc4 | 1.2.0 | Change | Beyond spread? | p99 ms, rc4 → 1.2.0 | server µs/req, rc4 → 1.2.0 |
|---|---:|---:|---:|---|---:|---:|
| 100 B GET p1 | 1.74M [1.72M - 1.76M] | 1.73M [1.72M - 1.76M] | −0.4% | no | 1.03 → 1.03 | 17.45 → 17.52 |
| 100 B GET p16 | 10.47M [10.37M - 10.55M] | 10.45M [10.41M - 10.59M] | −0.2% | no | 0.78 → 0.78 | 2.88 → 2.88 |
| 100 B SET p1 | 1.70M [1.65M - 1.71M] | 1.70M [1.68M - 1.72M] | 0.0% | no | 1.05 → 1.06 | 17.82 → 17.86 |
| 100 B SET p16 | 7.97M [7.92M - 8.01M] | 7.94M [7.88M - 8.02M] | −0.4% | no | 0.99 → 0.99 | 3.54 → 3.56 |
| 1 KB GET p1 | 1.63M [1.61M - 1.64M] | 1.63M [1.59M - 1.65M] | −0.3% | no (NIC) | 1.08 → 1.08 | 18.63 → 18.71 |
| 1 KB GET p16 | 1.87M [1.87M - 1.88M] | 1.87M [1.87M - 3.36M\*] | 0.0% | no (NIC) | 7.10 → 7.09 | 8.34 → 8.30 |
| 1 KB SET p1 | 1.61M [1.58M - 1.61M] | 1.61M [1.55M - 1.61M] | +0.1% | no (NIC) | 1.11 → 1.12 | 18.85 → 18.82 |
| 1 KB SET p16 | 2.80M [2.80M - 2.80M] | 2.80M [2.79M - 2.80M] | 0.0% | no (NIC) | 7.13 → 7.28 | 6.26 → 6.76 |
| 2 KB GET p1 | 920K [919K - 920K] | 920K [920K - 920K] | 0.0% | no (NIC) | 2.97 → 3.04 | 27.62 → 27.52 |
| 2 KB GET p16 | 937K [937K - 938K] | 938K [937K - 939K] | 0.0% | no (NIC) | 9.18 → 9.21 | 14.56 → 14.70 |
| 2 KB SET p1 | 1.23M [1.23M - 1.25M] | 1.24M [1.23M - 1.26M] | +0.7% | no (NIC) | 1.44 → 1.43 | 24.55 → 24.39 |
| 2 KB SET p16 | 1.43M [1.43M - 1.43M] | 1.43M [1.43M - 1.43M] | 0.0% | no (NIC) | 9.15 → 9.18 | 9.45 → 9.60 |
| 100 B MGET10 p1 | 1.15M [1.14M - 1.17M] | 1.16M [1.14M - 1.18M] | +0.2% | no | 1.46 → 1.46 | 26.21 → 26.16 |
| 100 B MSET10 p1 | 166K [164K - 168K] | 166K [165K - 167K] | −0.5% | no | 9.89 → 9.95 | 57.69 → 57.88 |

\* The NIC burst of round 6; see the 1 KB table.

![1.2.0 against 1.2.0-rc4](charts/regression-rc4-vs-1.2.0.svg)

**Nothing is slower beyond the run-to-run spread.**

**One side row was checked further.** GET at depth 1 with 300 connections (100 B) was below rc4 in every one of the first four rounds: 1.214M against 1.219M, −0.4%. The spread there was only ±0.3%, so it counted as a possible regression.
- **Two more rounds of both builds.** With six rounds it is no longer beyond the spread: 1.213M [1.203M - 1.218M] against 1.219M [1.211M - 1.225M], −0.5%.
- **A `perf record -g` profile of the server process for each build**, under the same load from both load generators, two runs each, alternating ([profile.sh](harness/profile.sh)).
  - Throughput while profiled: 1.208M and 1.209M for 1.2.0, against 1.210M and 1.214M for rc4.
  - The flat profiles are the same, function by function, to within 0.1-0.2 points ([results/profile-get-p1-candidate.txt](results/profile-get-p1-candidate.txt), [results/profile-get-p1-rc4.txt](results/profile-get-p1-rc4.txt)):

    | | 1.2.0 | rc4 |
    |---|---:|---:|
    | Kernel (TCP, softirq, netfilter conntrack, mlx5 driver) | 77.1% | 77.3% |
    | `kividb` binary | 20.8% | 20.6% |
    | `server_uring::handle_client` | 3.32% | 3.31% |
    | shard map lookup (`ShardMap::find`) | 3.26% | 3.30% |
    | `commands::handler::run_command` | 1.31% | 1.29% |
- **Cause: none in the code.** At this depth the server spends three quarters of its CPU in the kernel's network stack, and every KiviDB function costs the same in both builds. The −0.5% is within what a round varies here (the other rows' spreads are ±0.5-1%, and rounds 4-6 of both builds were about 1% below rounds 1-3).

**What changed since rc4, and why the rows did not move.** These are the 1.2.0 changes the check looked for, against the code paths this benchmark runs:
- `server.rs`, `server_uring.rs` and `commands/handler.rs` (the per-request loop and dispatch) are unchanged since rc4.
- The SLOWLOG/MONITOR redaction runs only when a command is logged (over the 10 ms threshold) or a MONITOR client is attached. In this benchmark neither happens.
- The SORT/SORT_RO ACL key checks run only on SORT.
- The AOF writer and replay changes are inactive with the AOF off.
- The compaction slab-generation check runs only in compaction.
- Cluster gossip, MIGRATE, `INFO stats rejected_connections` and the FreeBSD code are not on these paths.

## Findings

### Where KiviDB wins and why

- **Pipelined GET, and tail latency everywhere it is not network-bound.**
  - KiviDB runs 30 worker threads. Each owns a set of connections and drives them with io_uring.
  - A worker reads a batch of pipelined requests, executes them itself against a keyspace split into 8 shards per core (a hash map behind a reader-writer lock each), and writes the replies back in one go. No request is handed to another thread.
  - That is shared-everything with fine-grained locks: GETs on different keys almost never meet on the same lock. At pipeline 16 its p99 is 0.78 ms, less than half of Garnet's (1.72 ms) and well under Dragonfly's (1.33 ms).
- **Against Dragonfly (shared-nothing)**: a request for a key owned by another thread has to hop to that thread and back. At pipeline 16, Dragonfly spends 2.7× KiviDB's user CPU per GET (3.57 µs vs 1.34 µs; see the next section), and serves 5.66M against 10.45M.
- **Against Redis and Valkey with io-threads**: commands still execute on one thread. Throughput stops at 1.2-1.5M/s whatever the I/O thread count (sweep above). Past depth 16, Redis gets slower (1.41M → 1.27M at depth 64).
- **Against Redis Cluster (32 shards)**: a cluster-aware client splits each 16-deep pipeline across 32 shards. Each socket then carries about one request at a time, the batching is lost, and our load generators ran out of CPU (95%) at 5.7-5.8M/s. The numbers are what the cluster did for a fully cluster-aware memtier on 32 vCPUs of clients, not its ceiling. Its latency (p99 ~25 ms) comes from those saturated clients as much as from the cluster.
- **MGET of 10 keys**: KiviDB locks the shards for reading only, so 1.16M commands/s (11.6M keys/s), client-bound. That is 1.5× Garnet and 1.8× Dragonfly.

### Where KiviDB loses and why

- **SET at pipeline 16, 100 B: Garnet is 24% faster** (9.83M vs 7.94M). The cause is waiting, not CPU:
  - KiviDB's server was 88% busy and its load generators 72%, so neither was the limit.
  - Garnet's server was at 99%.
  - Every KiviDB SET takes its shard's write lock. Since 1.2.0 (#271) it also takes the key's write-order lock, so that read-modify-write commands cannot lose updates; that is why 1.2.0 is 3% slower than 1.1.3 on this row.
  - Garnet's hash index (Tsavorite) updates values in place, as far as we understand without a lock over a whole shard.
  - Per request, KiviDB's SET costs 3.56 µs of server CPU (1.71 user, 1.85 kernel) against Garnet's 3.24 µs (1.18 user, 2.05 kernel).
  - The flamegraph suite has the profile; this suite only shows that the gap is lock waits and per-request work, not the network.
- **GET at pipeline 16**: Garnet is 2% ahead (10.69M vs 10.45M). The difference is near the edge of what our load generators can drive: Garnet's row is client-bound at 90%, and KiviDB's clients were at 88-89%.
- **Without pipelining**: Dragonfly and KiviDB are level, and both rows are limited by our load generators. The README's bare-metal run, with a 64-vCPU load generator, measured 3.07M for KiviDB and 3.35M for Dragonfly at this depth.

### MSET of 10 keys: a clear loss

| | MSET 10 keys, commands/s | server CPU busy | p99 |
|---|---:|---:|---:|
| Garnet 2.2.5 | 761K | 95% | 7.42 ms |
| Dragonfly 2.0.2 | 557K | 99% | 4.48 ms |
| **KiviDB 1.2.0** | **166K** | **30%** | 9.95 ms |
| KiviDB 1.1.3 | 153K | 26% | 12.22 ms |
| Redis 8.10.3 | 150K | 14% | 8.77 ms |

**What MSET does in KiviDB**
- MSET is atomic: no client may see some of its keys set and others not.
- KiviDB keeps that promise by write-locking every shard the keys fall in, together, in a fixed order (`WriteShards::lock`), and then writing.
- Ten random keys land in ten of the 240 shards. With 1,200 connections doing the same, almost every MSET waits for a shard another MSET holds.

**Evidence that it is waiting, not work**
- The server sits 70% idle while throughput is 4.6× below Garnet's.
- Latency is high (p50 7.3 ms).

**What it is not**
- It is not the #271 write-order lock: 1.1.3, which has the same shard locking but not #271, does 153K. 1.2.0 is 8% faster.
- MGET takes the same shards for reading, and reads do not block each other: it runs at 1.16M.

**How the others do it**
- Dragonfly and Garnet make multi-key writes atomic without locking whole shards: Dragonfly schedules transactions on per-key intent locks, and Garnet locks keys, not shards, in Tsavorite.

**In practice**
- KiviDB's MSET is about as fast as single-threaded Redis's (150K), and well behind the two other multi-threaded engines.
- Workloads heavy on multi-key writes (MSET, or MSETNX, which locks the same way) should expect this.
- Locking per key instead of per shard is the fix.

### I/O batching or command execution?

The question from HN, asked directly: is KiviDB's advantage I/O batching or faster command execution? It is **neither on its own**. Batching cuts the I/O cost for every engine, and KiviDB's per-command execution is about as expensive as Redis's. The difference is that KiviDB runs that execution on all its cores at once without moving requests between threads.

Server CPU per request, split into user time (parsing, executing, writing replies) and kernel time (system calls, TCP/IP, interrupts). GET, 100 B, 300 connections. Values are µs of server CPU per request, user + kernel:

| Engine | depth 1 | depth 4 | depth 16 | depth 64 |
|---|---:|---:|---:|---:|
| KiviDB 1.2.0 | 4.61 + 18.88 | 2.02 + 4.91 | 1.34 + 1.52 | 1.16 + 0.65 |
| Redis 8.10.3, io-threads 24 | 4.49 + 20.79 | 2.03 + 5.77 | 1.40 + 1.75 | 3.43 + 5.27 |
| Redis 8.10.3 Cluster, 32 shards | 4.78 + 18.09 | 2.78 + 8.38 | 1.47 + 2.83 | 1.23 + 1.45 |
| Dragonfly 2.0.2 | 8.57 + 19.27 | 5.42 + 5.16 | 3.57 + 2.05 | 2.78 + 1.13 |
| Garnet 2.2.5 | 7.27 + 22.81 | 2.47 + 5.78 | 1.14 + 1.81 | 0.85 + 0.92 |

1. **Without pipelining, every engine is a network stack with a database attached.** Each request is a system call in and a system call out, and the kernel takes 18-23 µs per request against 4-9 µs of user time. KiviDB's kernel share is the same as Redis's.
2. **Batching is where the per-request cost goes away.** From depth 1 to 64, KiviDB's kernel time per request falls 29× (18.9 → 0.65 µs), but its user time only 4× (4.6 → 1.2 µs). io_uring adds a little on top: one `io_uring_enter` submits the reads and writes of many connections. But the bulk of the saving is the pipelining itself, and Garnet's kernel time falls just as far over the same range (22.8 → 0.92).
3. **Command execution costs about what it costs in Redis.** At depth 16, KiviDB spends 1.34 µs of user CPU per GET, Redis 1.40 and a Redis Cluster shard 1.47. Garnet is cheaper (1.14) and Dragonfly dearer (3.57).
4. **What does differ is how many cores execute.** Redis executes on one thread, so its 1.4 µs per GET caps it near 1.4M/s, and extra I/O threads only move the reading and writing. KiviDB executes on 30 threads at once and never hands a request to another thread. Its fine-grained shard locks are almost never contended on GET, so the same per-request cost multiplies by the core count: 10.45M/s at depth 16, ~7× Redis.

Caveat on the table: an engine whose threads poll while idle shows CPU it does not need.
- Dragonfly's server read 94-100% busy in every 100 B row, at any throughput.
- Valkey's server spent 6.4 µs of user CPU per GET at depth 16, 4.6 times Redis's 1.4 µs, at a lower throughput (1.20M against 1.41M).
- Redis's I/O threads busy-wait, which is likely why its depth-64 row costs more than its depth-16 row.

Those engines' per-request CPU is an upper bound. Valkey is left out of this table for that reason; its numbers are in [results/summary.csv](results/summary.csv). KiviDB, Garnet, and Redis at depths 1-16 were close to their real load.

## Caveats

- **NIC cap at 1 KB and 2 KB.** With pipelining, every engine that is not bound elsewhere stops at the same rate: ~1.86-1.87M GET/s at 1 KB, ~0.94M at 2 KB, 2.8M / 1.43M SET/s. That is the server's 16 Gbps network (~1.93 GB/s of replies). It is not a property of any engine. Those rows rank latency and CPU only.
- **One NIC burst.** In KiviDB 1.2.0's sixth round, 1 KB GET at pipeline 16 (17:32-17:33 UTC), the server sent 3.84 GB/s for the whole scenario, twice the 1.93 GB/s of every other round, and KiviDB served 3.36M GET/s.
  - The server was then 89% busy and the load generators 91%, against 48% for both in the NIC-bound rounds.
  - The warm-up just before it also ran at 3.9M/s. The 2 KB block that followed, seconds later, was back at the usual cap.
  - We read it as Azure briefly lifting the VM's bandwidth limit, not as anything KiviDB did.
  - The round stays in [results/runs.csv](results/runs.csv). It does not move any median; the tables mark it, and the regression chart draws it at the axis edge.
  - It does show what KiviDB serves at 1 KB when the network allows: at least 3.36M GET/s, client-bound.
- **Pipeline-1 rows are client-bound for the fast engines.** KiviDB and Dragonfly drive both 16-vCPU load generators to ~95% CPU without pipelining. Their p1 numbers are floors; the bare-metal README run (64-vCPU generator) measured more than our 32 vCPUs of clients can send.
  - KiviDB's server was also 95% busy in those rows, almost all of it kernel network time (see above). A bigger load generator alone would not double them.
- **Redis Cluster is client-bound in every row.** memtier in cluster mode splits each pipeline across 32 shards, which costs the client far more CPU per request. It saturated both load generators at 95%. The Redis Cluster numbers are the most two 16-vCPU cluster-aware clients could push, not the cluster's limit.
  - With a 64-vCPU load generator on bare metal, the README measured 4.7M GET/s at pipeline 16 for 32 shards.
- **Redis/Valkey io-threads.** The matrix uses 24, the best overall in the rc4 run's sweep. Redis with io-threads 8 was 13-14% faster at pipeline 16 and 29% slower without pipelining. Valkey is within a few percent across 8-24.
- **No bytes-per-key figures.** The servers' `used_memory` after each load is recorded in [results/loads.txt](results/loads.txt), but it is not comparable across engines: Dragonfly's value at 2 KB is below the raw data size. Memory per key is measured properly in the memory suite.
- **One machine type.** In our earlier runs, how KiviDB and Garnet ranked against each other changed between Ampere Altra (Azure) and Graviton3 (AWS) machines. Whole-machine numbers on other hardware will differ.
- **20 s scenarios after a full load of 10M keys and a 5 s warm-up.** Round-to-round spread is under 3% on almost every row. Exceptions:
  - Redis: 2 KB SET p16 (±9%), and ±3-4% on several 100 B rows.
  - Redis Cluster SET p1 (±4-7%).
  - Valkey 1 KB GET p16 and 2 KB GET p1 (±3-4%).
  - The KiviDB NIC burst above.

## Reproduce

Scripts are in [harness/](harness/), with step-by-step use in [harness/README.md](harness/README.md):

1. **Create the machines.** `create-vms.sh` makes three arm64 VMs in one proximity placement group: one D32ps_v5 and two D16ps_v5. It needs 64 vCPUs of quota.
2. **Install the engines.** `setup-server.sh` on the server (pinned versions), `fetch-kividb.sh 75482abbb258bafe84c0e9d05fc1c904abb19cfd cand build` and `fetch-kividb.sh v1.2.0-rc4 rc4 build` for the KiviDB builds. `setup-client.sh` on both load generators.
3. **Run it.** From load generator 1: `PHASES="K A X drift" REDIS_IO=24 VALKEY_IO=24 bash run-all.sh`. That is three rounds of 1.2.0 / rc4 / 1.1.3 interleaved, three rounds of the competitors, a fourth 1.2.0 / rc4 round and a Redis drift round. It takes about 3.5 hours. `PHASES=X XROUNDS="5 6"` adds the two extra 1.2.0 / rc4 rounds.
4. **Summarise and draw.**
   - `python3 summarize.py results` writes `runs.csv` and `summary.csv`, merging the latency histograms of both load generators.
   - `MAIN_ENGINES="kividb-cand kividb-113 redis@24 redis-cluster valkey@24 dragonfly garnet" python3 charts.py results charts` draws the charts.
   - `python3 tables.py results` writes the tables above.
5. **Profile (optional).** `profile.sh kividb-cand 1 10` and `profile.sh kividb-rc4 1 10` record the server under GET at depth 1, 300 connections.
6. **Delete the resource group.**

## Files

- `results/summary.csv`: one row per engine × size × scenario. It has the median, min and max requests/s; p50/p99/p99.9; server CPU and µs per request split into user and kernel; NIC rates; client CPU; and the limit flag.
- `results/runs.csv`: the same, per round.
- `results/io-threads-sweep.csv`, `results/io-threads-sweep-log.txt`: the Redis/Valkey io-threads sweep, from the 1.2.0-rc4 run on the same VM sizes (2026-11-01).
- `results/drift-check-redis.csv`, `results/drift-check-redis-runs.csv`, `results/drift-suite-log.txt`: the fourth Redis round, run after KiviDB's fourth round.
- `results/launch.log`: every engine start, with its version and full command line.
- `results/suite-log.txt`: the driver's log, including each load generator's throughput and the CPU samples per scenario.
- `results/loads.txt`: load time and `used_memory` after each load (see the caveat above).
- `results/kividb-candidate-build.txt`, `results/kividb-rc4-build.txt`: the commit and build command of the two KiviDB builds.
- `results/profile-get-p1-candidate.txt`, `results/profile-get-p1-rc4.txt`, `results/profile-runs.txt`: the flat `perf` profiles of the regression check and the throughput of the profiled runs.
- `results/tables.md`: the tables as `tables.py` printed them.
- `charts/`: the SVG charts.
