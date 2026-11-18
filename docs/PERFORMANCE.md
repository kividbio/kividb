# Performance

<a href="../assets/benchmarks/1.2.0/kividb-1.2.0-benchmarks.mp4">
  <img src="../assets/benchmarks/1.2.0/kividb-1.2.0-benchmarks.webp" alt="KiviDB 1.2.0 against Garnet, Dragonfly, Redis, Redis Cluster and Valkey: throughput, latency, MSET, memory, snapshots and vector search" width="720">
</a>

*KiviDB 1.2.0 against Garnet, Dragonfly, Redis, Redis Cluster and Valkey. Click for the full-resolution video.*

- [Bare metal](#bare-metal)
- [The full suite, on Azure VMs](#the-full-suite-on-azure-vms)
- [Memory of small sets](#memory-of-small-sets)
- [Memory of small hashes](#memory-of-small-hashes)
- [Worker threads](#worker-threads)
- [Earlier runs](#earlier-runs)
- [Independent reviews](#independent-reviews)

## Bare metal

AWS Graviton3, November 2026: the server on a `c7gn.metal` confined to 32 cores, the load
generator on a second `c7gn.metal` in the same placement group, one engine at a time,
persistence off. memtier_benchmark, 10 million keys of 100 bytes, 30 s a run, the mean of
two rounds; 300 connections pipelined, 1,200 without.

| | KiviDB 1.2.0 | Garnet 2.2.5 | Dragonfly 2.0.2 | Redis 8.10.3 |
|---|---:|---:|---:|---:|
| `GET`, pipeline 16 | **21.24M** | 20.82M | 9.31M | 2.36M |
| `SET`, pipeline 16 | 17.79M | **20.27M** | 8.70M | 1.94M |
| `GET`, no pipelining | 2.99M | 2.88M | **3.30M** | 1.47M |
| `SET`, no pipelining | 3.22M | 3.07M | **3.37M** | 1.26M |
| p99, `GET` / `SET` at pipeline 16 | **0.36 / 0.45 ms** | 0.67 / 0.80 ms | 0.88 / 1.02 ms | 2.29 / 2.72 ms |
| Bytes a key, 10M keys of 100 B | 143 | | **134** | 178 |

Pipelined, KiviDB served 9.0× Redis's GETs and 9.2× its SETs, and more than twice
Dragonfly's. Garnet was a close second on `GET` and ahead on pipelined `SET`; without
pipelining Dragonfly was 5-10% ahead. Scripts and raw results:
[kivi-benchmark](https://github.com/kividbio/kivi-benchmark).

## The full suite, on Azure VMs

KiviDB 1.2.0 was measured in November 2026 on Azure VMs against Redis 8.10, Redis Cluster,
Valkey 9.1, Dragonfly 2.0, Garnet 2.2 and kvrocks 2.17, each on the same machines in the same
hour, with p50/p99/p99.9 latency from two load generators.
[benchmarks/1.2.0/REPORT.md](../benchmarks/1.2.0/REPORT.md) has every number and comparison;
[benchmarks/1.2.0](../benchmarks/1.2.0/README.md) has the harnesses, raw results and charts.

| | KiviDB 1.2.0 | Best other | Redis 8.10 |
|---|---|---|---|
| `GET` 100 B, pipeline 16 | 10.45M ops/s | Garnet 10.69M | 1.41M |
| `SET` 100 B, pipeline 16 | 7.94M ops/s | Garnet 9.83M | 1.25M |
| `MSET` of 10 keys | 569K commands/s | Garnet 800K | 144K |
| Lists, hashes, sets, sorted sets, counters, pipeline 16 | 4.2-6.6M ops/s | Redis Cluster (16 shards): level, ahead on `HSET` and `INCR` | 3.7-6.0x less |
| Bytes per key, 100 B values, 50M keys | 140.7 | Dragonfly 141.5 | 174.4 |
| Memory added by `BGSAVE`, 30M keys of 1 KB under writes | +10% | Dragonfly +0.4% | +89% |
| Filtered vector search, 100 clients | 4,478 q/s | Dragonfly 811 | 1,802 |
| Cache hit rate ([#202](https://github.com/kividbio/kividb/issues/202)) | 60.96% | | 60.78% |

Where KiviDB loses: Garnet is faster on `SET` at pipeline 16 and on `MSET`; Redis Cluster on
`HSET` and `INCR`; Dragonfly uses less memory per vector, during a snapshot, and for ASCII
values; Redis ingests vectors faster and serves 3-9% more cache reads a second. These are
cloud VMs, so absolute numbers are lower than on the [bare metal](#bare-metal) above.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../assets/benchmarks/1.2.0/throughput-pipelined-dark.png">
  <img src="../assets/benchmarks/1.2.0/throughput-pipelined-light.png" alt="Throughput at pipeline 16, GET and SET, 100-byte values" width="720">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../assets/benchmarks/1.2.0/throughput-unpipelined-dark.png">
  <img src="../assets/benchmarks/1.2.0/throughput-unpipelined-light.png" alt="Throughput without pipelining, GET and SET" width="720">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../assets/benchmarks/1.2.0/latency-p99-pipelined-dark.png">
  <img src="../assets/benchmarks/1.2.0/latency-p99-pipelined-light.png" alt="p99 latency at pipeline 16" width="720">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../assets/benchmarks/1.2.0/mset-dark.png">
  <img src="../assets/benchmarks/1.2.0/mset-light.png" alt="MSET of 10 keys" width="720">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../assets/benchmarks/1.2.0/memory-per-key-dark.png">
  <img src="../assets/benchmarks/1.2.0/memory-per-key-light.png" alt="Memory per key, 50M keys of 100 bytes" width="720">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../assets/benchmarks/1.2.0/snapshot-memory-dark.png">
  <img src="../assets/benchmarks/1.2.0/snapshot-memory-light.png" alt="Memory added by BGSAVE under writes" width="720">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../assets/benchmarks/1.2.0/vector-qps-recall-dark.png">
  <img src="../assets/benchmarks/1.2.0/vector-qps-recall-light.png" alt="Vector search: queries per second against recall" width="720">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../assets/benchmarks/1.2.0/vector-filtered-dark.png">
  <img src="../assets/benchmarks/1.2.0/vector-filtered-light.png" alt="Filtered vector search, 100 clients" width="720">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../assets/benchmarks/1.2.0/vector-ingest-dark.png">
  <img src="../assets/benchmarks/1.2.0/vector-ingest-light.png" alt="Vector ingest and index build" width="720">
</picture>

## Memory of small sets

Since 1.3.0 a small set is kept in one block (`listpack` in `OBJECT ENCODING`:
up to 128 members of up to 64 bytes) or, when its members are integers, as a
sorted array (`intset`: up to 512), where 1.2 kept every set as a hash table
with an allocation per member (#297). Bytes of `used_memory` a member, 3M
members in sets of each size, 14-byte members (`membr:00000001`) or the
integers 0 to n-1, on an Azure `Standard_D4s_v5`:

| members a set | 1.2.2 strings | 1.3.0 strings | Redis 8.10.2 strings | 1.2.2 integers | 1.3.0 integers | Redis 8.10.2 integers |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 76.0 | 34.6 | 25.8 | 68.0 | 17.6 | 9.8 |
| 100 | 55.5 | 20.4 | 18.5 | 47.5 | 3.8 | 2.8 |
| 1,000 | 67.0 | 67.0 | 30.0 | 59.0 | 59.0 | 22.3 |
| 100,000 | 54.3 | 54.3 | 36.8 | 46.3 | 46.3 | 28.8 |

What is left over Redis at 10 members is mostly the key's own: an entry and
the set's header, about 150 bytes a key. Sets of more
than 128 members (512 integers) are tables as before; making those smaller is
the next step of #297. Suite `17-set-memory` of
[the regression suite](../benchmarks/1.3.0/regressions/README.md) measures
this table again and fails a build that loses it.

## Memory of small hashes

Since 1.3.0 a small hash is kept in one block, its fields and values end to
end (`listpack` in `OBJECT ENCODING`, `listpackex` once a field has a TTL: up
to 512 fields, no field or value longer than 64 bytes), where 1.2 kept every
hash as a table with two allocations per field. Bytes of `used_memory` a
field, 3M fields in hashes of each size, 14-byte fields (`field:00000001`)
with 32-byte values, on an Azure `Standard_D4s_v5`:

| fields a hash | 1.2.2 | 1.3.0 | Redis 8.10.2 |
|---:|---:|---:|---:|
| 10 | 162.8 | 71.8 | 57.8 |
| 100 | 129.2 | 55.8 | 51.7 |
| 1,000 | 166.6 | 166.6 | 70.1 |
| 100,000 | 128.8 | 128.8 | 76.8 |
| 3,000,000 | 133.3 | 133.3 | 77.9 |

As for sets, what is left over Redis at 10 fields is mostly the key's own
entry and the hash's header. Hashes past the block's limits are tables as
before. Suite `19-hash-memory` of
[the regression suite](../benchmarks/1.3.0/regressions/README.md) measures
this again (with and without field TTLs) and fails a build that loses it.

## Memory of lists

Since 1.3.0 a list's elements lie end to end in chunks of up to 64, each
element between two copies of its length, where 1.2 kept a slot, a shared
header and an allocation of its own for every element. Bytes of
`used_memory` an element, 3M elements of 32 bytes in lists of each size, on
an Azure `Standard_D4s_v5`:

| elements a list | 1.2.2 | 1.3.0 | Redis 8.10.2 |
|---:|---:|---:|---:|
| 10 | 76.0 | 48.8 | 45.0 |
| 100 | 65.2 | 38.4 | 36.4 |
| 1,000 | 64.1 | 36.7 | 34.6 |
| 100,000 | 68.5 | 36.7 | 34.3 |
| 3,000,000 | 69.8 | 36.7 | 34.3 |

At 10 elements most of what is left over Redis is the key's own: its entry
and the list's header. A list that has had an element longer than 1,024
bytes keeps its elements apart, as 1.2 did, and takes what 1.2 took. Suite
`21-list-memory` of
[the regression suite](../benchmarks/1.3.0/regressions/README.md) measures
this table again (1M elements, 10 to 100,000 a list) and fails a build that
loses it.

## Snapshot size

Since 1.3.0 a snapshot is written compressed (`rdbcompression yes`, the
default; see [PERSISTENCE.md](PERSISTENCE.md#compressed-snapshots)): the
same image, in blocks of 256 KiB compressed with zstd level 1 off the
serving threads. On an Azure `Standard_D4s_v5`, the server on 2 CPUs, the file
on tmpfs (the save on the VM's disk in its own row):

| dataset | 1.2.2 | 1.3.0 | Redis 8.10.2 |
|---|---:|---:|---:|
| 6M strings of 150 B ([#879](https://github.com/kividbio/kividb/issues/879)), file | 1,007 MB | 32.7 MB | 142.9 MB |
| the same, `SAVE` to the VM's disk | 6.2 s | 2.0 s | 6.4 s |
| the same, master memory during a full sync | +1,055 MB | +99 MB | |
| 6M text strings of 16-64 B, file | 359 MB | 131 MB | 308 MB |
| 6M random strings of 16-64 B, file | 359 MB | 305 MB | 323 MB |
| 1M hashes and 600k sets, sorted sets and lists, file | 490 MB | 206 MB | 392 MB |

Compressing costs CPU on the saving threads: a save to tmpfs of the text
strings takes 2.0 s instead of 1.2 s, and loading takes up to 1.1 s more.
`rdbcompression no` writes the plain image, as 1.2 did. Suite
`24-snapshot-size` of
[the regression suite](../benchmarks/1.3.0/regressions/README.md) measures
the first and last datasets again (snapshot size, save and load time against
1.2.2 and Redis) and fails a build that loses it.

## Worker threads

How many workers to start on a CPU allowance ([#873](https://github.com/kividbio/kividb/issues/873)),
measured on master `b2203cce` in November 2026 on one Azure `Standard_D8s_v5` (8 vCPUs, 4
cores with two hyper-threads each): the server confined with `taskset`, memtier_benchmark
2.6.1 with 4 threads on the other CPUs, 1M keys of 100 B and 500k of 2 KB, `GET`:`SET`
9:1, three runs of 8 s, the median. "4 vCPUs" is the server on the two hyper-threads of 2
cores, "4 cores" on one hyper-thread of each of the 4 cores with memtier on the siblings.
Up to 1.2, auto started 2 workers on 4 vCPUs and 2 on 2.

Unpipelined, 500 connections, ops/s and p99:

| Server on | I/O model | 1 worker | 2 workers | 3 workers | 4 workers | 5 workers |
|---|---|---:|---:|---:|---:|---:|
| 2 vCPUs | io_uring | 131k, 4.32 ms | **222k, 2.43 ms** | 215k, 5.95 ms | | |
| 4 vCPUs | io_uring | | 302k, 1.81 ms | 344k, 2.06 ms | 326k, 2.29 ms | 345k, 2.50 ms |
| 4 cores | io_uring | | 239k, 2.51 ms | 336k, 1.83 ms | 326k, 2.30 ms | 360k, 2.58 ms |
| 4 cores | tokio | | 210k, 12.93 ms | 344k, 3.71 ms | **376k, 1.96 ms** | 353k, 3.92 ms |

- On 2 vCPUs the server is the limit: one worker served 24-41% less than two in every
  workload (100 B and 2 KB, 48 and 500 connections, pipeline 1 and 16, a Rails cache mix of
  `GET`, `MGET` of 10 and `SET PX`, with AOF, with a replica, during `BGSAVE`). A third
  worker served 0-10% less (22% more during `BGSAVE`) and multiplied the p99 by 2 to 12
  (by 1.1 during `BGSAVE`).
- On 4 vCPUs the load generator, at about 340k unpipelined and 2.4M pipelined, is the
  limit from 3 workers up, so 3, 4 and 5 workers are within about 10% of each other. 2 workers
  served 2-22% less than 4 on io_uring, and 20-33% less on 4 cores; on tokio 23% less on 4 vCPUs and 37-46% less on 4 cores,
  at 3.5 to 6.6 times the p99.
- During back-to-back `BGSAVE`s of 4 GB, on 4 vCPUs, 3 workers had a lower p99 than 4
  (3.0 against 5.5 ms on io_uring, at the same throughput), and with a replica 2.3 against
  3.0 ms; on tokio with AOF it was the other way round (4.0 against 2.1 ms on 4 cores).
  Neither is consistent enough to start a worker fewer when persistence is on.
- With the server and memtier sharing all 8 vCPUs, 4 to 9 workers were within 9% of each
  other.
- A fractional quota: in a container with `--cpus 1.5`, two workers served 202k with a
  p99 of 27.5 ms, one 119k at 4.7 ms; with `--cpus 2.5`, three workers 282k at 18.2 ms,
  two 273k at 2.1 ms; and with `--cpus 2`, three workers 223k at 35.6 ms, two 270k at
  2.0 ms. Auto rounds the quota down.

## Earlier runs

KiviDB 1.1 was also measured on AWS Graviton3 bare metal in October 2026, against
Dragonfly, Redis and Redis Cluster: [benchmarks/1.1-bare-metal.md](../benchmarks/1.1-bare-metal.md).

## Independent reviews

Written by others, not by the KiviDB project:

- [Redis vs Valkey vs Dragonfly vs KiviDB](https://warefeats.com/posts/2026-11-05-kv-stores-redis-vs-valkey-vs-dragonfly-vs-kividb/)
  (warefeats, 5 November 2026): KiviDB 1.2.0 measured on AWS Graviton3 against Redis 8.10,
  Valkey 9.1 and Dragonfly 2.1: throughput, pipelining, latency and memory per key.
- [KiviDB Review: Rust Redis Alternative With Vector Search](https://andrew.ooo/posts/kividb-review-rust-redis-alternative-vector-search/)
  (andrew.ooo, 10 November 2026): a review of KiviDB's design, published benchmarks,
  durability record and where it fits.
