# Memory and snapshots: 1.2.1 (35a413c0) against 1.2.0

The memory suite of `benchmarks/1.2.1/memory/full/`, rerun on **1.2.1 (commit `35a413c0`)** and the **1.2.0 release
binary**, three rounds on three identical pair-A VM sets in parallel, plus pair B for active defrag, on 2026-11-12
(UTC). New in this run: **snapshot load time and memory at load** for a 30M x 1 KB dump written by each build and
loaded by each build. The competitors were not rerun. This run compares 1.2.1 with 1.2.0, and the competitor numbers in
`benchmarks/1.2.1/memory/full/` stand.

**Verdicts**
- **No regression in memory.** Bytes per key (warefeats, and growth to 50M keys), the memory added by `BGSAVE` and
  `BGREWRITEAOF`, the memory held after them, and what active defrag gives back are all level with 1.2.0.
  `used_memory` is identical.
- **No latency or throughput gap beyond the run-to-run spread.**
- **#284 holds:** in all 3 rounds of each build, no key was evicted during a `BGSAVE` at `maxmemory`.
- **Snapshots load faster in 1.2.1, with less memory.** A 30M x 1 KB dump loads in **83-84 s on 1.2.1 against
  99-101 s on 1.2.0 (-17%)**. Peak RSS during the load is **37.06 against 37.97 GiB (-2.4%)**, and RSS after the
  load is 34.29 against 35.20 GiB. This holds whichever build wrote the dump. Both builds load each other's dumps,
  with identical key counts and `used_memory`.

## Headline

| | KiviDB 1.2.1 | KiviDB 1.2.0 |
|---|---:|---:|
| warefeats 1M x 100 B, RSS B/key, ASCII / random values | 148.9 / 149.1 | 148.9 / 149.0 |
| growth to 50M keys, RSS B/key, 100 B / 1 KB (random) | 140.6 / 1,071.6 | 140.7 / 1,071.6 |
| `BGSAVE` of 30M x 1 KB at 100k SET/s: memory added / worst write | 3.16 GiB / 202 ms | 3.15 GiB / 204 ms |
| `BGREWRITEAOF`: memory added / worst write | 2.11 GiB / 221 ms | 2.11 GiB / 218 ms |
| #284: keys evicted during a `BGSAVE` at `maxmemory` (3 rounds) | 0 | 0 |
| active defrag: RSS given back after deleting half of 5M x 100 B (4 rounds) | 335 MB in 12.8 s | 336 MB in 12.7 s |
| load of a 30M x 1 KB snapshot (its own dump): time / peak RSS | **84.0 s / 37.05 GiB** | 100.5 s / 37.97 GiB |
| warefeats SET:GET 1:10, 100 B, pipeline 30 (ops/s) | 14.65M | 14.77M |

## 1.2.1 against 1.2.0

Median of 3 rounds (defrag: median of 4). *Spread* is the larger (max-min)/median of the two builds. The rc2 column
gives the published one-round numbers of the previous 1.2.1 build.

| section | metric | 1.2.1 | 1.2.0 | change | spread | rc2 run (1.2.1 / 1.2.0) |
|---|---|---:|---:|---:|---:|---|
| warefeats ASCII | 1st M RSS / used B/key | 148.9 / 146.9 | 148.9 / 146.9 | 0.0% | 0.2% | 149.0 / 146.9; 149.2 / 146.9 |
| warefeats random | 1st M RSS / used B/key | 149.1 / 146.9 | 149.0 / 146.9 | +0.1% | 0.1% | 149.0; 149.1 |
| warefeats 100 B p1 | ops/s | 944,642 | 1,050,970 | -10.1% | 23.6% | 918,129; 918,224 |
| warefeats 10 KB p1 | ops/s / p99.9 ms | 321,222 / 1.243 | 321,380 / 1.251 | 0.0% / -0.6% | 3.0% / 2.2% | 319,513 / 1.283 |
| warefeats pipeline 30 | ops/s | 14.65M | 14.77M | -0.8% | 30.1% | 13.39M; 12.67M |
| growth 100 B | RSS B/key at 50M / load s | 140.6 / 13.0 | 140.7 / 13.1 | 0.0% / -0.7% | 0.1% / 2.3% | 140.6 / 15.6 |
| growth 1 KB | RSS B/key at 50M / load s | 1,071.6 / 23.3 | 1,071.6 / 23.3 | 0.0% / +0.1% | 0.0% / 4.9% | 1,071.7 / 23.8 |
| `BGSAVE` 30M x 1 KB, 100k SET/s | memory added / held after (GiB) | 3.16 / 33.41 | 3.15 / 33.41 | +0.2% / 0.0% | 15.8% / 1.4% | 3.15 / 33.43 |
| `BGSAVE` | duration s / worst write ms | 71.5 / 202 | 71.6 / 204 | -0.1% / -1.2% | 15.6% / 40% | 71.3 / 229 |
| `BGREWRITEAOF` | memory added (GiB) / worst write ms | 2.11 / 221 | 2.11 / 218 | +0.1% / +1.4% | 12.3% / 11.1% | 2.10 / 252 |
| #284 | keys evicted (3 rounds) | 0 | 0 | | | 0 / 0 |
| active defrag | RSS returned MB / give-back s | 335 / 12.8 | 336 / 12.7 | -0.3% / +1.2% | 6.3% / 27.9% | 335 / 12.2 |
| snapshot load, own dump | load s / peak RSS GiB | 84.0 / 37.05 | 100.5 / 37.97 | **-16.4% / -2.4%** | 2.0% / 0.1% | (new) |

The warefeats 100 B pipeline-1 row (-10.1%) is inside its 23.6% round-to-round spread. The three regions' VM sets
differ by up to 24% on this network-sensitive row, and the difference moves both builds alike. A few
defrag-window SET p99/p99.9 rows are also above +3%, inside spreads of 85-132%. Every table with per-round values
is in `results/tables.md`.

## Snapshot load time and memory at load

30M keys x 1 KB of random bytes (memtier, as in the snapshot test), saved with `BGSAVE` by each build. The dump is
29.3 GiB, and the two builds' dumps differ by 0 bytes in size. Each build then cold-starts on each dump three
times, with readers and writers in alternating order. Before each start the page cache is dropped, so the dump is
read from the local NVMe disk. *Load s* runs from the engine's "Found dump.kdb" log line to its "Loaded N keys"
line. *Start to first PONG* runs from `docker run` until the engine answers, since KiviDB loads before it listens.
*Peak RSS* is the process's VmHWM over the whole load. Medians of 3, with the range of the load time.

| dump written by | loaded by | load s (engine log) | start to first PONG s | peak RSS during load GiB | RSS after load GiB | used_memory GiB | keys |
|---|---|---:|---:|---:|---:|---:|---:|
| 1.2.0 | 1.2.0 | 100.5 (100.2-101.2) | 106.0 | 37.97 | 35.20 | 30.07 | 30,000,000 |
| 1.2.0 | 1.2.1 | 83.4 (83.3-84.6) | 88.9 | 37.06 | 34.29 | 30.07 | 30,000,000 |
| 1.2.1 | 1.2.0 | 99.4 (99.2-101.1) | 105.2 | 37.97 | 35.20 | 30.07 | 30,000,000 |
| 1.2.1 | 1.2.1 | 84.0 (82.6-84.3) | 89.5 | 37.05 | 34.29 | 30.07 | 30,000,000 |

- What matters is which build reads the dump, not which wrote it: 1.2.1 loads either dump in 83-84 s, and
  1.2.0 in 99-101 s.
- Peak RSS during the load is 2.7 GiB above RSS after the load for both builds. 1.2.1 sits 0.9 GiB lower on both
  measures, at the same `used_memory`.
- Downgrade works: 1.2.0 loads a dump written by 1.2.1, with the same key count and memory.

## What ran

| | VM | regions | used for |
|---|---|---|---|
| pair A x3 | Standard_E32ds_v7 engine (local NVMe data dir) + Standard_D32ls_v7 client, Xeon 6973P-C, one proximity placement group each | koreacentral (r1), germanywestcentral (r2), eastus (r3) | warefeats, growth, snapshot, eviction |
| pair A, 4th set | same sizes | germanywestcentral | snapshot load time; the eviction runs of r1 (both builds), r2 (1.2.1) and r3 (1.2.0) |
| pair B | Standard_E32ds_v6 + Standard_D16ls_v6, Xeon Platinum 8573C | polandcentral | active defrag |

Ubuntu 24.04, engines in Docker with host networking and seccomp unconfined (io_uring allowed), one at a time.

| build | what | binary SHA-256 |
|---|---|---|
| 1.2.1 | commit `35a413c0`, built as `release.yml` builds it (amazonlinux:2023, rustc 1.99.1, `cargo build --release`, default features); tarball `5e98af075db8d44553fe5057a090aa1ae5ac838aae180c9976b2c2bf61cefde7` | `b7ebacb76961b4b8b52981728b4cddae9c65f17a71cbb2d0c022736afed86b0f` |
| 1.2.0 | v1.2.0 release tarball, `SHA256SUMS.txt` check OK | `a211d544be79bb78cfc5bc16b1e1315d20913f154cd8179f80668ab048edd72f` |

**Method.** The drivers of `benchmarks/1.2.1/memory/full/harness/` were used unchanged. The queues are
`harness/queue-a-r{1,2,3}.txt` (the build that goes first alternates by round), `harness/queue-a-r4.txt` (the 4th
set) and `harness/queue-b.txt`. New drivers:
- `harness/loadtime.sh` (client side) and `harness/loadone.sh` (server side): the load-time test;
- `harness/build-release.sh`: builds the tree as `release.yml` does;
- `harness/boot.sh` and `harness/collect.sh`: set the VMs up from the operator's machine and copy the results back.

Tables: `python3 -I harness/analyze.py raw/pair-a raw/pair-b warefeats charts > results/tables.md` with
`CAND_LABEL="KiviDB 1.2.1 (35a413c0)"`.

### Files

- `results/tables.md`: every table that `analyze.py` generates.
- `results/raw.tar.gz`: `tar xzf results/raw.tar.gz -C results` unpacks to:
  - `raw/pair-a`: the client data of all four pair-A sets, merged, including `loadtime.csv`;
  - `raw/pair-b`;
  - `raw/engine-r*`: versions, SHAs, images and lscpu for each engine VM;
  - `raw/build-info.txt`;
  - `warefeats/`.
- `charts/`: as in the earlier suites.
