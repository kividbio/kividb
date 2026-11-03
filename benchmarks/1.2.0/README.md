# KiviDB 1.2.0 benchmarks and tests

Everything measured for the 1.2.0 release, on Azure VMs and AKS, from 2026-11-01 to 2026-11-03. **[REPORT.md](REPORT.md) has every number and every comparison in one file.**

- **The full benchmark runs** used commit `75482abb`, the code of 1.2.0-rc7, against Redis, Redis Cluster, Valkey, Dragonfly, Garnet and kvrocks.
- **Every suite was then rerun** on 1.2.0-rc8 (`082dab3d`), alternating with rc7 on the same VMs, in each suite's `rc8/` folder.
- **Every suite ran once more** on 1.2.0-rc10 (`39bfacd2`), the code that ships as 1.2.0, alternating with rc8 on the same VMs, in each suite's `rc10/` folder.

rc9 and rc10 changed only replication, cluster and shutdown code, plus `MIGRATE`. In the rc10 round:
- every suite is level with rc8, and bytes per key are identical: the 64 MB `repl-backlog-size` is never allocated without a replica;
- instructions per command in KiviDB's own code are within 0.4%;
- the #202 cache test under `allkeys-lru` leans about 1% lower on two VM pairs, inside the 0.98-1.02 that pairs of identical code measure in that suite.

Each folder holds the harness that produced its numbers, the raw results, the charts, and a README. The README gives the hardware, the exact versions and flags of every engine, how to run it again, and what the numbers mean. Where KiviDB loses, the README says so and why.

The raw results, logs and charts are in the repository, but not in source archives of later releases (the tarballs and zips GitHub makes of a tag): `.gitattributes` leaves them out there, and keeps the READMEs, the harnesses and the summary tables. Browse them here on GitHub.

Many of these were asked for in the [Hacker News thread](https://news.ycombinator.com/item?id=50152541) on the launch. [The table below](#questions-from-the-thread) links each question to its answer. The problems these runs found, and how each was fixed, are [at the end](#what-the-runs-found).

## Suites

| Folder | What it measures | Engines |
|---|---|---|
| [core](core/README.md) | `GET`/`SET` at 100 B, 1 KB and 2 KB, pipeline 1 and 16, a pipeline-depth sweep, `MGET`/`MSET`; p50/p99/p99.9 with two load generators | KiviDB 1.2.0 and 1.1.3, Redis 8.10.3, Redis Cluster (32 shards), Valkey 9.1.3, Dragonfly 2.0.2, Garnet 2.2.5 |
| [datatypes](datatypes/README.md) | lists, hashes, sets, sorted sets, counters at 100 B and 2 KB; memory by type | KiviDB, Redis, Redis Cluster, Valkey, Dragonfly, kvrocks 2.17.0 |
| [profiling](profiling/README.md) | CPU per request (user vs kernel), threads, instructions per command, flamegraphs | KiviDB 1.2.0 and 1.1.3, Redis |
| [memory](memory/README.md) | bytes per key with the warefeats harness, memory as a dataset grows to 50M keys, mixed load, memory and write latency across `BGSAVE` and an AOF rewrite, eviction during a snapshot (#284), active defrag | KiviDB 1.2.0 and 1.1.3, Redis 8.10.3, Valkey 9.1.3, Dragonfly 2.0.2 |
| [vector](vector/README.md) | [vector-db-benchmark](https://github.com/redis/vector-db-benchmark): filtered and unfiltered search, recall, ingest, memory during ingest | KiviDB 1.2.0, Redis 8.10.2, Dragonfly 2.0.2 |
| [cache-pi](cache-pi/README.md) | the cache benchmark from [#202](https://github.com/kividbio/kividb/issues/202): hit rate, peak memory, latency under `allkeys-lfu`/`allkeys-lru` on a small arm64 box | KiviDB 1.2.0 and 1.1.3, Redis 8.10.3 and unstable |
| [smoke](smoke/README.md) | every release artifact (binaries, packages, images, amd64 and arm64) starts and serves | KiviDB 1.2.0-rc7, [rc10](smoke/rc10/README.md) |
| [soak](soak/README.md) | hours of mixed load with AOF and a replica, and a sharded cluster with failovers and reshards; memory and latency over time | KiviDB 1.2.0-rc7, [rc8](soak/rc8/README.md), [rc10](soak/rc10/README.md) |
| [failover](failover/README.md) | kills, hangs, network cuts, power loss, failovers under load; every acknowledged write checked afterwards | KiviDB 1.2.0-rc7, [rc8](failover/rc8/README.md), [rc9](failover/rc9/README.md); Redis 8.10.3 |
| [operator](operator/README.md) | the Kubernetes operator on AKS: upgrades, failures, forced deletes, drains, resharding, backup and restore under load | KiviDB 1.2.0-rc7, [rc8](operator/rc8/README.md), [rc9](operator/rc9/README.md) and 1.1.3; operator 0.7.0 |

## Headlines

Each README has the latency, the spread across runs and the caveats.

| | KiviDB 1.2.0 | Best other | Redis |
|---|---|---|---|
| `GET` 100 B, pipeline 16 ([core](core/README.md)) | 10.45M ops/s | Garnet 10.69M | 1.41M |
| `SET` 100 B, pipeline 16 ([core](core/README.md)) | 7.94M ops/s | Garnet 9.83M | 1.25M |
| `MSET` of 10 keys ([core/rc8](core/rc8/README.md)) | 569K commands/s | Garnet 800K | 144K |
| Lists, hashes, sets, sorted sets, counters, pipeline 16 ([datatypes](datatypes/README.md)) | 4.2-6.6M ops/s | Redis Cluster x16: level, ahead on `HSET` and `INCR` (client-bound) | 3.7-6.0x less |
| Bytes per key, 100 B values, 50M keys ([memory](memory/README.md)) | 140.7 | Dragonfly 141.5 | 174.4 |
| Bytes per key, warefeats harness ([memory](memory/README.md)) | 149.2 | Dragonfly 137.1 | 166.3 |
| Memory added by `BGSAVE`, 30M keys of 1 KB under writes ([memory](memory/README.md)) | +10% | Dragonfly +0.4% | +89% |
| Filtered vector search, 100 clients ([vector](vector/README.md)) | 4,478 q/s | Dragonfly 811 | 1,802 |
| Cache hit rate, #202 ([cache-pi](cache-pi/README.md)) | 60.96% | | 60.78% |
| Acknowledged writes lost to kills, failovers, power loss and forced deletes, outside a promotion past unreplicated writes ([failover](failover/README.md), [operator](operator/README.md)) | 0 | | 0 |

**Where KiviDB loses,** with each README saying why:
- **Garnet:** faster on `SET` at pipeline 16 (+24%) and on `MSET` (KiviDB is at 0.71 of it).
- **Redis Cluster:** ahead on `HSET` and `INCR`.
- **Dragonfly:** less memory per vector and during a snapshot, and fewer bytes per key for ASCII values.
- **Redis:** faster vector ingest, and 3-9% more cache GETs a second.

## Questions from the thread

| Question | Answer |
|---|---|
| p99, and proof that the load generator is not what is measured ([falconBrisk47](https://news.ycombinator.com/item?id=50153410)) | Every throughput number comes with p50/p99/p99.9 from both load generators' histograms merged exactly, and with the load generators' CPU. Rows where the load generator was the limit are marked as such: [core](core/README.md), [datatypes](datatypes/README.md). |
| Valkey, Garnet and kvrocks ([edoceo](https://news.ycombinator.com/item?id=50153931), [esafak](https://news.ycombinator.com/item?id=50154673), [syepes](https://news.ycombinator.com/item?id=50153768)) | Valkey and Garnet in [core](core/README.md); Valkey and kvrocks in [datatypes](datatypes/README.md). Garnet is faster than KiviDB on `SET` at pipeline 16 and on `MSET`; the README explains why. |
| Lists and hashes, and 2 KB and small values ([edoceo](https://news.ycombinator.com/item?id=50153931)) | [datatypes](datatypes/README.md), at 100 B and 2 KB; [core](core/README.md) at 100 B, 1 KB and 2 KB. |
| Memory during ingestion against Redis as the dataset grows, with graphs, and mixed workloads ([kingcauchy](https://news.ycombinator.com/item?id=50154366)) | [memory](memory/README.md): bytes per key while loading to 50M keys against Redis, Valkey and Dragonfly, RSS over time, and a 20-minute mixed workload on every engine. [vector](vector/README.md): memory over time while loading up to a million 1536-dimension vectors. |
| Redis with `io-threads`, for a fair comparison ([Shamanoid](https://news.ycombinator.com/item?id=50156303)) | Redis and Valkey run with `io-threads` chosen by a sweep on each machine; the sweeps are in [core](core/README.md) and [datatypes](datatypes/README.md). Redis Cluster with one shard per core is there as well. |
| I/O batching or faster command execution? Pipeline 1 against 16, flamegraphs ([AtalantaDB](https://news.ycombinator.com/item?id=50161446)) | [profiling](profiling/README.md): CPU per request is about the same as Redis's; the difference is that KiviDB runs commands on every core. Flamegraphs, instruction counts and the user/kernel split at pipeline 1 and 16 are there. |
| Memory spike and write pause during a snapshot ([porker](https://news.ycombinator.com/item?id=50155311), [romange](https://news.ycombinator.com/item?id=50155630)) | [memory](memory/README.md): 30M keys of 1 KB under 100k uniformly spread writes a second. During `BGSAVE` Redis grows 89% (fork copy-on-write), KiviDB 10% and Dragonfly 0.4%. The worst write waits 391 ms on Redis, 209 ms on KiviDB and 291 ms on Dragonfly. [soak](soak/README.md): checkpoints under two hours of load. |
| Durability: are acknowledged writes kept? ([ignoramous](https://news.ycombinator.com/item?id=50155776)) | [failover](failover/README.md) and [operator](operator/README.md) check every acknowledged write after each kill, failover, power loss and forced delete; [soak](soak/README.md) checks every one after hours of load with failovers and reshards. |
| KiviDB as a cache, 7 times slower than Redis ([imchuncai](https://news.ycombinator.com/item?id=50300421), [#202](https://github.com/kividbio/kividb/issues/202)) | [cache-pi](cache-pi/README.md): 1.2.0 serves 7–8 times as much as 1.1.3 did, matches Redis on hit rate and memory, has lower tail latency, and serves 3–9% fewer requests a second than Redis. |

## What the runs found

These runs, and CI on the release, found the problems below. All are fixed in 1.2.0, and each fix is checked in the folder named.

**From the rc4 runs (fixed in rc5):**
- [fixes/replica-aof](fixes/replica-aof/README.md): a replica with the AOF on ran out of memory under sustained writes (since 1.2.0-rc1).
- [fixes/cluster-moved-address](fixes/cluster-moved-address/README.md): cluster nodes that moved together kept each other's old addresses (in 1.1.x too).
- An AOF ending in zeros after a power loss stopped the server from starting (in 1.1.x too): see [failover](failover/README.md).
- [fixes/migrate-tokio-stall](fixes/migrate-tokio-stall/README.md): with `--io-model tokio`, a `MIGRATE` waiting on its destination could stop the server reading any other connection (in 1.1.x too).

**From the rc6 runs (fixed in rc7):**
- Keys and hash fields that expired while the server was down came back after the AOF was replayed: see [failover](failover/README.md).
- Active defrag could hand out a block of memory twice, a race new in the 1.2.0 release candidates: see [failover](failover/README.md).

**From the rc7 runs (fixed in rc8):**
- `MSET` and `MSETNX` of keys in many shards queued behind each other: [core/rc8](core/rc8/README.md).
- A second server started in a data directory another was still using, as a pod replaced after a forced delete can be, could load an old dump and carry on the first one's stream (#303): [operator/rc8](operator/rc8/README.md), [failover/rc8](failover/rc8/README.md).
- Hashes loaded from `dump.kdb` stayed, empty, after their fields expired (#302): [soak/rc8](soak/rc8/README.md).
- `redis-cli --cluster fix` couldn't close a reshard that stopped halfway (#295). Fully fixed in rc10, below.
- The `.rpm` didn't declare `shadow-utils`: [smoke](smoke/README.md).

**From the rc8 runs (fixed in rc9):** [failover/rc9](failover/rc9/README.md), [operator/rc9](operator/rc9/README.md)
- After a clean restart, or a handover, of a master, its replicas were sent the whole dataset again (#306).
- A replica's full sync of a large dataset under heavy writes never finished (#305).
- A replica in the middle of a full sync could win an election, or save its partial dataset over its last good dump.

**From the rc8 and rc9 runs (fixed in rc10):** [soak/rc10](soak/rc10/README.md)
- `redis-cli --cluster reshard` stopped when a key was written while it was moving it, and `--cluster fix` then left acknowledged keys unreadable. Writes to the keys a `MIGRATE` is moving now wait for it, as in Redis.
- A replica stopped with `SIGTERM` under writes could deadlock (#308).
- `repl-backlog-size` is 64 MB by default, so that replicas carry on after their master restarts under load.

Still open, and listed as known issues in the [release notes](../../docs/releases/v1.2.0.md): a `redis-cli` reshard under heavy writes slows the source node's writes (#310), and the others listed there.

## Hardware

Azure, one proximity placement group per suite, server and load generators on separate VMs. The sizes, regions and kernels are in each README. These are cloud VMs, not the bare-metal machines behind the numbers in the project README, so absolute numbers are lower. The comparisons between engines are on the same machines in the same hour.
