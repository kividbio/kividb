# KiviDB 1.2.0 benchmarks and tests

Everything measured for the 1.2.0 release, on **1.2.0-rc4** (tag `v1.2.0-rc4`, commit `a6b1bf38`), on Azure VMs and AKS, between 2026-11-01 22:00 and 2026-11-02 05:00 UTC. Each folder holds the harness that produced its numbers, the raw results, the charts and a README with the hardware, the exact versions and flags of every engine, how to run it again, and what the numbers mean. Where KiviDB loses, the README says so and why.

Many of these were asked for in the [Hacker News thread](https://news.ycombinator.com/item?id=50152541) on the launch. The table below links each question to its answer.

The fixes made after rc4 for problems these runs found are verified in [fixes/](#fixes-made-after-rc4).

## Suites

| Folder | What it measures | Engines |
|---|---|---|
| [core](core/README.md) | `GET`/`SET` at 100 B, 1 KB and 2 KB, pipeline 1 and 16, a pipeline-depth sweep, `MGET`/`MSET`; p50/p99/p99.9 with two load generators | KiviDB rc4 and 1.1.3, Redis 8.10.3, Redis Cluster (32 shards), Valkey 9.1.3, Dragonfly 2.0.2, Garnet 2.2.5 |
| [datatypes](datatypes/README.md) | lists, hashes, sets, sorted sets, counters at 100 B and 2 KB; memory by type | KiviDB, Redis, Redis Cluster, Valkey, Dragonfly, kvrocks 2.17.0 |
| [profiling](profiling/README.md) | CPU per request (user vs kernel), threads, instructions per command, flamegraphs | KiviDB rc4 and 1.1.3, Redis |
| [memory](memory/README.md) | bytes per key with the warefeats harness, memory as a dataset grows to 50M keys, memory and write latency across `BGSAVE` and an AOF rewrite, eviction during a snapshot (#284); a first pass at mixed workloads | KiviDB rc4 and 1.1.3, Redis 8.10.3, Valkey 9.1.3, Dragonfly 2.0.2 |
| [vector](vector/README.md) | [vector-db-benchmark](https://github.com/redis/vector-db-benchmark): filtered and unfiltered search, recall, ingest, memory during ingest | KiviDB rc4 and rc3, Redis 8.10.2, Dragonfly 2.0.2 |
| [cache-pi](cache-pi/README.md) | the cache benchmark from [#202](https://github.com/kividbio/kividb/issues/202): hit rate, peak memory, latency under `allkeys-lfu`/`allkeys-lru` on a small arm64 box | KiviDB rc4, 1.1.3 and rc2, Redis 8.10.3 and unstable |
| [smoke](smoke/README.md) | every release artifact (binaries, packages, images, amd64 and arm64) starts and serves | KiviDB rc4 |
| [soak](soak/README.md) | hours of mixed load with AOF and a replica; memory and latency over time; checkpoints | KiviDB rc4, 1.1.3, rc3, Redis |
| [failover](failover/README.md) | kills, hangs, network cuts, power loss, failovers under load; every acknowledged write checked afterwards | KiviDB rc4 and 1.1.3, Redis |
| [operator](operator/README.md) | the Kubernetes operator on AKS: upgrades, failures, drains, resharding, backup and restore under load | KiviDB rc4 and 1.1.3, operator 0.6.1 and master |

## Headlines

All on 1.2.0-rc4. Each README has the latency, the spread across runs and the caveats.

| | KiviDB | Best other | Redis |
|---|---|---|---|
| `GET` 100 B, pipeline 16 ([core](core/README.md)) | 10.39M ops/s | Garnet 10.71M | 1.45M |
| `SET` 100 B, pipeline 16 ([core](core/README.md)) | 7.91M ops/s | Garnet 9.93M | 1.27M |
| `MSET` of 10 keys ([core](core/README.md)) | 168K commands/s | Garnet 787K | 153K |
| Lists, hashes, sets, sorted sets, pipeline 16 ([datatypes](datatypes/README.md)) | 4.0-6.8M ops/s | Redis Cluster: within 15%, ahead on `HSET` and `INCR` (client-bound) | 3.6-5.7x less |
| Bytes per key, 100 B values, 50M keys ([memory](memory/README.md)) | 140.7 | Dragonfly 141.5 | 174.4 |
| Bytes per key, warefeats harness ([memory](memory/README.md)) | 149.0 | Dragonfly 137.2 | 165.7 |
| Filtered vector search, 100 clients ([vector](vector/README.md)) | 4,548 q/s | Dragonfly 762 | 1,847 |
| Cache hit rate, #202 ([cache-pi](cache-pi/README.md)) | 60.96% | | 60.76% |
| Acknowledged writes lost, failover and operator runs without a promotion past unreplicated writes ([failover](failover/README.md), [operator](operator/README.md)) | 0 | | 0 to 2 |

KiviDB loses to Garnet on `SET` and `MSET`, to Redis Cluster on `HSET` and `INCR`, to Dragonfly on snapshot pauses, memory per vector and ASCII-heavy bytes per key, and to Redis on vector ingest speed and cache throughput (6-10%). The READMEs say why.

## Questions from the thread

| Question | Answer |
|---|---|
| p99, and proof that the load generator is not what is measured ([falconBrisk47](https://news.ycombinator.com/item?id=50153410)) | Every throughput number comes with p50/p99/p99.9 from both load generators' histograms merged exactly, and with the load generators' CPU. Rows where the load generator was the limit are marked as such: [core](core/README.md), [datatypes](datatypes/README.md). |
| Valkey, Garnet and kvrocks ([edoceo](https://news.ycombinator.com/item?id=50153931), [esafak](https://news.ycombinator.com/item?id=50154673), [syepes](https://news.ycombinator.com/item?id=50153768)) | Valkey and Garnet in [core](core/README.md); Valkey and kvrocks in [datatypes](datatypes/README.md). Garnet is faster than KiviDB on `SET` at pipeline 16 and on `MSET`; the README explains why. |
| Lists and hashes, and 2 KB and small values ([edoceo](https://news.ycombinator.com/item?id=50153931)) | [datatypes](datatypes/README.md), at 100 B and 2 KB; [core](core/README.md) at 100 B, 1 KB and 2 KB. |
| Memory during ingestion against Redis as the dataset grows, with graphs, and mixed workloads ([kingcauchy](https://news.ycombinator.com/item?id=50154366)) | [memory](memory/README.md): bytes per key while loading to 50M keys, against Redis, Valkey and Dragonfly, with RSS over time; [vector](vector/README.md): memory over time while loading up to a million 1536-dimension vectors. Mixed workloads ran only in part (the README says which). |
| Redis with `io-threads`, for a fair comparison ([Shamanoid](https://news.ycombinator.com/item?id=50156303)) | Redis and Valkey run with `io-threads` chosen by a sweep on each machine; the sweeps are in [core](core/README.md) and [datatypes](datatypes/README.md). Redis Cluster with one shard per core is there as well. |
| I/O batching or faster command execution? Pipeline 1 against 16, flamegraphs ([AtalantaDB](https://news.ycombinator.com/item?id=50161446)) | [profiling](profiling/README.md): CPU per request is about the same as Redis's; the difference is that KiviDB runs commands on every core. Flamegraphs, instruction counts and the user/kernel split at pipeline 1 and 16 are there. |
| Memory spike and write pause during a snapshot ([porker](https://news.ycombinator.com/item?id=50155311), [romange](https://news.ycombinator.com/item?id=50155630)) | [memory](memory/README.md): 30M keys of 1 KB under 100k writes a second. During `BGSAVE` Redis grows 91% (fork copy-on-write), KiviDB 10%; the worst write waits 412 ms on Redis, 256-293 ms on KiviDB, 8 ms on Dragonfly. [soak](soak/README.md): checkpoints under two hours of load. |
| Durability: are acknowledged writes kept? ([ignoramous](https://news.ycombinator.com/item?id=50155776)) | [failover](failover/README.md) and [operator](operator/README.md) check every acknowledged write after each kill, failover and power loss. |
| KiviDB as a cache, 7 times slower than Redis ([imchuncai](https://news.ycombinator.com/item?id=50300421), [#202](https://github.com/kividbio/kividb/issues/202)) | [cache-pi](cache-pi/README.md): 1.2.0 serves 7–8 times as much as 1.1.3 did, matches Redis on hit rate, memory and tail latency, and serves 6–10% fewer requests a second than Redis. |

## Fixes made after rc4

The runs, and CI on the release, found four problems in rc4. They are fixed in 1.2.0, and each fix is verified against rc4 here:

- [fixes/replica-aof](fixes/replica-aof/README.md): a replica with the AOF on ran out of memory under sustained writes (since 1.2.0-rc1).
- [fixes/cluster-moved-address](fixes/cluster-moved-address/README.md): cluster nodes that moved together kept each other's old addresses (in 1.1.x too).
- An AOF ending in zeros after a power loss stopped the server from starting (in 1.1.x too): see [failover](failover/README.md).
- [fixes/migrate-tokio-stall](fixes/migrate-tokio-stall/README.md): with `--io-model tokio`, a `MIGRATE` waiting on its destination could stop the server reading any other connection (in 1.1.x too).

## Hardware

Azure, one proximity placement group per suite, server and load generators on separate VMs. The sizes, regions and kernels are in each README. These are cloud VMs, not the bare-metal machines behind the numbers in the project README, so absolute numbers are lower; the comparisons between engines are on the same machines in the same hour.
