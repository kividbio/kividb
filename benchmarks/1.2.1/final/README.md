# 1.2.1 on its final commit against 1.2.0

The suites in this folder's parent measured `878746e4`. Fixes went in after that, so the final 1.2.1 commit, `74071531`, was measured again against the 1.2.0 release binary on 2026-11-10.

**Result: no row is slower by more than 3%.** Every change beyond 3% is a gain, and all of them are hash rows:
- `HSET` at pipeline 16 is now 7.8% faster than 1.2.0. At `878746e4` it was 9.2% slower (#544); #569 fixed it.
- Writes of hash fields with a TTL are 5-44% faster.
- `GET`, `SET`, the mixed workload, `TS.ADD` and vector KNN are within 3% of 1.2.0.

## What ran

| | |
|---|---|
| Cloud | Azure centralindia. Three VMs in one proximity placement group, with accelerated networking. |
| Server | `Standard_D16ps_v5`: 16 vCPU Ampere Altra, 64 GB, Ubuntu 24.04 arm64 |
| Load generators | 2 × `Standard_D16ps_v5` for the core and extra rows, 1 for the datatypes rows. memtier_benchmark 2.5.2, built from its tag. |
| Date | 2026-11-10, 13:58-15:29 UTC |

The parent suites used a 32-vCPU server for the core rows. Here the server has 16 vCPUs, so the core rows are limited by the server (84-90% busy), not the load generators. Absolute numbers are therefore lower than the parent suites'. Only the comparison within this session counts.

## Builds

Both are the default variant. Both print `kividb 1.2.0`, because the version is bumped at release.

| build | source | SHA-256 |
|---|---|---|
| **1.2.1** | `740715316820364ca26ce5f09722858c44e21373`, built the way `release.yml`'s linux-arm64 job builds it: `amazonlinux:2023`, rustup stable (rustc 1.99.1), `RUSTFLAGS=-C target-feature=+lse`, `cargo build --release` (fat LTO) | `497ddd4d8d5491432c9f…` |
| **1.2.0** | release asset `kividb-linux-aarch64.tar.gz`, checked against `SHA256SUMS.txt` | `e936ce57888114e8dc24…` |

## Method

- **Core rows** (`GET`, `SET`): the parent core suite's `run-suite.sh`, unchanged. 100 B values, 10M keys loaded, a 5 s warm-up, then 20 s per row from both load generators at once. Pipeline 1 uses 1,200 connections and pipeline 16 uses 300. There were 4 rounds, with the build order alternating.
- **Extra rows** (`harness/bench_extra.sh`): same timing, same alternation and same 4 rounds.
  - mixed: `SET:GET` 1:4 over 1M keys of 100 B;
  - `TS.ADD ts:<k> * 1` over 100k series;
  - `FT.SEARCH` KNN 10 over 50k FLOAT32 vectors of dimension 32 (HNSW, L2), pipeline 1, 64 connections.
- **Datatypes rows**: the parent datatypes suite's `datatypes.sh`, unchanged, run by `harness/session121.sh`.
  - The `hash` group and the field-TTL (`hfe`) group, 100 B, pipeline 1 and 16.
  - 2 alternating rounds of 3 runs, so 6 runs per build per row.
- `harness/benchall.sh` runs all three. `cmp_extra.py` and `cmp_dt.py` make the tables.

## Core rows (4 rounds per build)

| Row | 1.2.1 | 1.2.0 | Δ | p50 ms | p99 ms | p99.9 ms | server µs/req |
|---|---:|---:|---:|---:|---:|---:|---:|
| `GET` p1 | 1.12M [1.11-1.13M] | 1.13M [1.13-1.14M] | -1.4% | 1.06 / 1.04 | 1.59 / 1.58 | 2.95 / 3.04 | 12.87 / 12.68 |
| `GET` p16 | 6.73M [6.63-6.86M] | 6.81M [6.69-6.90M] | -1.2% | 0.69 / 0.69 | 1.01 / 1.02 | 1.21 / 1.22 | 2.12 / 2.10 |
| `SET` p1 | 1.06M [1.06-1.06M] | 1.07M [1.06-1.07M] | -0.6% | 1.12 / 1.11 | 1.64 / 1.64 | 3.03 / 3.04 | 13.46 / 13.38 |
| `SET` p16 | 4.77M [4.76-4.80M] | 4.79M [4.72-4.80M] | -0.4% | 1.00 / 1.00 | 1.36 / 1.35 | 1.63 / 1.66 | 2.81 / 2.81 |

On `GET` at pipeline 1, every 1.2.1 round was below every 1.2.0 round, and the server spent 1.5% more CPU per request. The gap is 1.4%. Full tables are in `results/core/tables.md`.

## Extra rows (4 rounds per build)

| Row | 1.2.1 ops/s | 1.2.0 ops/s | Δ | p50 ms | p99 ms | p99.9 ms | server µs/req |
|---|---:|---:|---:|---:|---:|---:|---:|
| mixed p16 | 6.33M | 6.30M | +0.4% | 0.74 / 0.74 | 1.11 / 1.12 | 1.35 / 1.35 | 2.18 / 2.20 |
| mixed p1 | 1.10M | 1.10M | -0.2% | 1.08 / 1.08 | 1.62 / 1.61 | 2.45 / 2.46 | 13.01 / 12.99 |
| `TS.ADD` p16 | 3.30M | 3.39M | -2.5% | 1.46 / 1.43 | 1.74 / 1.70 | 1.90 / 1.88 | 4.22 / 4.11 |
| `TS.ADD` p1 | 920K | 928K | -0.9% | 1.30 / 1.29 | 1.75 / 1.75 | 4.13 / 4.08 | 15.52 / 15.37 |
| KNN 10, p1 | 182K | 185K | -1.6% | 0.35 / 0.35 | 0.53 / 0.53 | 0.61 / 0.61 | 76.5 / 75.0 |

**`TS.ADD` at pipeline 16** was below 1.2.0 in every round (3.28-3.31M against 3.34-3.40M).
- `TS.ADD` took 2.91-2.94 µs per call in `INFO commandstats`, against 2.85 µs for 1.2.0.
- This was not profiled. Two changes touch that path: 1.2.1 checks a sample against its duplicate policy before it changes the series (#551), and it reads the options in RedisTimeSeries's order (#565).
- No call failed in either build.

The KNN ranges overlap (1.2.1 178-191K, 1.2.0 183-187K). Ranges are in `results/extra/table.md`.

## Datatypes rows: hashes and field TTLs (6 runs per build)

| Row | 1.2.1 | 1.2.0 | Δ |
|---|---:|---:|---:|
| `HGET` p1 / p16 | 0.77M / 4.28M | 0.79M / 4.39M | -2.6% / -2.6% |
| `HGETALL` p1 / p16 | 0.65M / 1.37M | 0.65M / 1.40M | -0.2% / -2.5% |
| `HSET` p1 / **p16** | 0.77M / **3.65M** | 0.77M / **3.39M** | +0.4% / **+7.8%** |
| `HGET` of fields with TTLs, p1 / p16 | 0.78M / 4.14M | 0.76M / 3.85M | +2.0% / +7.6% |
| `HSETEX PX`, p1 / p16 | 0.57M / 0.87M | 0.54M / 0.65M | +5.1% / +33.7% |
| `HPEXPIRE`, p1 / p16 | 0.58M / 0.78M | 0.54M / 0.61M | +7.5% / +26.7% |
| `HSET` + `HPEXPIRE`, p1 / p16 | 0.64M / 1.87M | 0.63M / 1.58M | +1.1% / +18.1% |
| `HSETEX PX 50` (fields expiring during the run), p1 / p16 | 0.50M / 0.68M | 0.42M / 0.47M | +16.9% / +44.4% |
| `HSETEX PX 50` + `HGET`, p1 / p16 | 0.60M / 1.37M | 0.58M / 0.98M | +3.9% / +40.3% |

The `HGET`, `HGETALL` and `HSET` p1 rows have run-to-run spreads of 4-21%, larger than their differences. Medians, ranges, latency and server cores are in `results/datatypes/table.md`.

## Caveats

- These figures don't re-measure memory or instruction counts. See the parent suites for those. #543 (`HSETEX` instructions) was not re-measured here.
- The machines differ from the parent suites': arm64 throughout, and a 16-vCPU server.
- Raw memtier output and HdrHistograms were left on the VMs, which were deleted. The CSVs here are what `summarize.py` built from them.
