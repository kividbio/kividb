# Performance

<a href="../assets/benchmarks/1.2.0/kividb-1.2.0-benchmarks.mp4">
  <img src="../assets/benchmarks/1.2.0/kividb-1.2.0-benchmarks.webp" alt="KiviDB 1.2.0 against Garnet, Dragonfly, Redis, Redis Cluster and Valkey: throughput, latency, MSET, memory, snapshots and vector search" width="720">
</a>

*KiviDB 1.2.0 against Garnet, Dragonfly, Redis, Redis Cluster and Valkey. Click for the full-resolution video.*

- [Bare metal](#bare-metal)
- [The full suite, on Azure VMs](#the-full-suite-on-azure-vms)
- [What 1.3.0 changed](#what-130-changed)
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

## What 1.3.0 changed

Every gain in 1.3.0 that was measured, with the numbers its pull request recorded. Each row
was measured on the machine named, against the master build of the day without the change
(the "1.2.2" column, which behaves as 1.2.2 on that path), and against Redis 8.10 where the
PR ran it beside KiviDB. "Not measured" means no number exists yet; nothing here is
estimated.

These are the PRs' own measurements, on different machines. Before 1.3.0 is released,
the release regression suite (`benchmarks/1.3.0/regressions/`, coming in a separate pull
request) re-records every row on one VM, 1.2.2 against 1.3.0, and adds the command that
reproduces each one. Until then, only the vector-set rows have a script in the repository:
[benchmarks/1.3.0/vector-sets/](../benchmarks/1.3.0/vector-sets/).

| What was measured | 1.2.2 | 1.3.0 | Redis 8.10 | Machine | PR |
|---|---:|---:|---:|---|---|
| `redis-cli` 7.0.15 `--cluster reshard` of 1,000 slots (about 176,000 keys; 3 masters of about 1M keys) | 272.2 s | 23.6 s | not measured | Azure D4s_v5 | [#724](https://github.com/kividbio/kividb/pull/724) (#294) |
| Connections each other master received for 2,000 `FT.SEARCH` calls (3-master cluster) | 2,001 | at most 20 (1-16 kept) | n/a | Azure D4s_v5 | [#725](https://github.com/kividbio/kividb/pull/725) (#723) |
| Connections the target received for 2,000 `MIGRATE`s | 2,000 | 1 | 1 (8.10.2) | Azure D4s_v5 | [#733](https://github.com/kividbio/kividb/pull/733) (#726) |
| Stream memory, one field `f`=`v`, 1M entries (`used_memory` a entry) | 158.4 B | 6.1 B | not measured | Azure D32s_v5 | [#683](https://github.com/kividbio/kividb/pull/683) (via #713) |
| Stream memory, five fields with shared names, 200,000 entries | 478.5 B | 51.2 B | not measured | Azure D32s_v5 | #683 |
| Stream memory, five fields with unique names | 498.6 B | 101.1 B | not measured | Azure D32s_v5 | #683 |
| Stream memory, one field with a 200-byte value | 350.9 B | 215.8 B | not measured | Azure D32s_v5 | #683 |
| Process RSS, 1M one-field stream entries | 207 MB | 22 MB | not measured | Azure D32s_v5 | #683 |
| `XINFO CONSUMERS`, 200,000 pending over 50 consumers (average) | 82.5 ms | 0.059 ms | not measured | Azure D4s_v5 | [#736](https://github.com/kividbio/kividb/pull/736) (#637) |
| `XADD` to a stream whose `XINFO CONSUMERS` is polled every 50 ms | 38,820/s | 104,384/s | not measured | Azure D4s_v5 | #736 |
| Full `SCAN 0 MATCH schedule:* COUNT 1000`, 2,002,000 keys | 1,328 ms | 479 ms | 531 ms (8.10.2) | Azure D4s_v5 | [#769](https://github.com/kividbio/kividb/pull/769) (#751) |
| Full `SCAN 0 COUNT 1000`, same keys | 3,087 ms | 1,935 ms | 2,041 ms (8.10.2) | Azure D4s_v5 | #769 |
| Longest `PING` on another connection while 8M keys are loaded (Tokio) | 141.0 ms | 21.1 ms | 17.1 ms (8.10.2) | Azure D4s_v5 | [#780](https://github.com/kividbio/kividb/pull/780) (#752) |
| Longest `PING` while 17.5M keys with `EX` are loaded (Tokio) | 160.2 ms | 20.5-24.4 ms | 17.8 ms (8.10.2) | Azure D4s_v5 | #780 |
| Longest `PING` on another connection during `KEYS user:99*` over 2M keys (Tokio) | 134 ms | 0.1 ms | 202-213 ms (8.10.2) | Azure D4s_v5 | [#757](https://github.com/kividbio/kividb/pull/757) (#754) |
| Longest `PING` during `SCAN 0 MATCH user:99* COUNT 2000000` (Tokio) | 247-256 ms | 0.1 ms | 467-471 ms (8.10.2) | Azure D4s_v5 | #757 |
| Longest `PING` during `DEL` of a 3M-field hash (Tokio) | 129-137 ms | 0.1 ms | 337-349 ms (8.10.2) | Azure D4s_v5 | #757 |
| Another client's `PING` during a `SAVE` of 1.5M keys (Tokio, `--threads 1`) | 532 ms | answered at once (not timed) | not measured | Azure D16s_v5 | [#696](https://github.com/kividbio/kividb/pull/696) (#605) |
| RSS once idle after `DEL` of a 3M-field hash | 199.6 MiB, stays | 16.9 MiB after 1.5 s | 62.7 MiB after about 10 s (8.10.2) | Azure D4s_v5 | [#763](https://github.com/kividbio/kividb/pull/763) (#753) |
| RSS once idle after `HDEL` of 1M fields | 77.9 MiB, stays | 16.8 MiB after 1.5 s | 13.5 MiB after about 7 s (8.10.2) | Azure D4s_v5 | #763 |
| `FT.INFO` of a 1,000-document index beside 5.4M other keys (p50) | 53 ms | 0.06 ms | not measured | Apple M2 laptop | [#727](https://github.com/kividbio/kividb/pull/727) (#590) |
| `SET`s a second beside 4 clients polling `FT.INFO` | 15,000-20,000 | 50,000-65,000 | not measured | Apple M2 laptop | #727 |
| Vector set memory, 768 dimensions, 50,000 vectors, `Q8` (bytes a vector) | 3,168 (stored f32) | 864 | 1,526 (with its HNSW graph) | Apple M3 Pro | [#730](https://github.com/kividbio/kividb/pull/730) (#697) |
| The same, `BIN` | 3,168 (stored f32) | 192 | 854 (with its HNSW graph) | Apple M3 Pro | #730 |
| `VSIM` queries a second, 768 dimensions, 10,000 vectors, `Q8`, 8 clients | 1,637 (f32) | 12,635 | 5,822 exact scan, 23,625 HNSW (8.10.3) | Apple M3 Pro | #730 |
| RedisVL 0.32.0 checks passed | 59 of 67 | 67 of 67 | 67 of 67 (8.10.3) | Linux VM | [#779](https://github.com/kividbio/kividb/pull/779) (#767) |

What these gains cost, as measured in the same PRs:

| Cost | 1.2.2 | 1.3.0 | Machine | PR |
|---|---:|---:|---|---|
| Stream reads, instructions a command (callgrind): `XDEL`, `XRANGE … COUNT 1`, `XRANGE … COUNT 10` with one and five fields, `XREAD COUNT 10` | | +3.7%, +3.4%, +5.7%, +7.4%, +5.8% | Linux aarch64 (contributor's machine) | #683 |
| Stream `XADD` of five fields, instructions a command | | about -21% | Linux aarch64 | #683 |
| Load of 3.2 million out-of-order stream entries (a crafted payload) | 0.82 s | 2.0 s | Apple Silicon laptop | #683 |
| Memory per pending stream entry (`used_memory` / RSS) | 122 / 144 B | 129 / 154 B | Azure D4s_v5 | #736 |
| Instructions per `string.format` call in a script | | +201 to +368 | Azure D4s_v5 | [#768](https://github.com/kividbio/kividb/pull/768) (#747) |
| `EVALSHA` of a script of 20 `string.format` calls (`redis-benchmark -c 50`) | 60.0k/s | 57.4k/s (-4.4%) | Azure D4s_v5 | #768 |
| Memory per document of an index of vectors alone | | +31 B | Apple M2 laptop | #727 |
| `HSET` of a new document into an index of vectors alone (DIM 2) | | about 5% slower | Apple M2 laptop | #727 |
| `GET`/`SET` instructions a command on a standalone server (callgrind) | | within 0.1% | Azure D4s_v5 | #724 |

"n/a": Redis has no cluster-wide search to compare with.

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
