# 1.2.1 on AWS bare metal: the README headline benchmark on the final 1.2.1 code

The README's headline numbers come from the 1.2.0 bare-metal capture (KiviDB 1.2.0 against Garnet 2.2.5, Dragonfly 2.0.2 and Redis 8.10.3). The 2026-11-10 rerun measured an earlier 1.2.1 build (`e7669a1f`) against them. This folder measures the final 1.2.1 code (`35a413c0`) the same way, side by side with the 1.2.0 release binary, on the same hardware type, with the same method, scripts and memtier build.

**Result: 1.2.1 has not regressed.** Against 1.2.0 in this session (4 rounds each, ABBA order):
- `GET` at pipeline 16: +0.1% (21.31M vs 21.30M).
- `SET` at pipeline 16: +2.0% (18.02M vs 17.67M).
- p99 at pipeline 16: `GET` equal (0.363 ms), `SET` 1.8% lower.
- Without pipelining: +6.1% on `GET` and +8.8% on `SET`. These rows vary by 5-15% from round to round within each build (see "Spread"); the gains are not a real change. Nothing dropped.

Against the earlier 1.2.1 build (`e7669a1f`, 2026-11-10) the pipelined rows agree within 1.2%.

Garnet, Dragonfly and Redis were **not** re-measured: their 2026-11-10 numbers (same instance type, region, placement, engine versions, memtier build and launch commands) are reused. The two KiviDB 1.2.0 measurements, 2026-11-10 and today, agree within 0.7% on the pipelined rows, so the two sessions are comparable.

## What ran

| | |
|---|---|
| Server | AWS c7gn.metal (64 vCPU Graviton3), us-east-1f, Ubuntu 22.04 (kernel 6.8.0-1069-aws). Each engine was confined to 32 cores with `taskset` (`CORES=32`). |
| Load generator | A second c7gn.metal in the same cluster placement group. memtier_benchmark 2.5.2 (pinned) through `harness/run-matrix.sh`, 60 threads. |
| Connections | 300 with pipeline 16 (5 a thread). 1,200 without pipelining (20 a thread). |
| Data | 10 million keys of 100 bytes, loaded before measuring and read uniformly at random. |
| Duration | 30 s a scenario (`DURATION=30 SIZES=100 PIPELINES="1 16" RATIOS="1:0 0:1"`). Each round loaded the data afresh. |
| Persistence | Off |
| Order | One engine at a time, 2026-11-12 04:08-04:28 UTC: 1.2.1 rounds 1-2, 1.2.0 rounds 1-2, 1.2.0 rounds 3-4, 1.2.1 rounds 3-4 (ABBA, so drift over the session favours neither build). |
| Command | `taskset -c 0-31 kividb --port 6379 --bind 0.0.0.0 --threads 32` (io_uring), both builds; see `launch.log` in the raw archive. |

## Builds

Both are the default variant (no Lua, no TLS).

| build | source | SHA-256 of the binary |
|---|---|---|
| **1.2.1** | `35a413c0`, built on the server the way `release.yml`'s linux-aarch64 job builds it: `amazonlinux:2023`, rustup stable (rustc 1.99.1), `RUSTFLAGS=-C target-feature=+lse`, `cargo build --release` (fat LTO, codegen-units 1). See `harness/build-c3.sh` and `server-build.log`. Prints `kividb 1.2.1-rc2`: the version string is bumped at release. | `2a66c1de503ded3dbd1981e0b235ae5a8bd8d9230225ef09bec71d9ba26bbb77` |
| **1.2.0** | release asset `kividb-linux-aarch64.tar.gz` (tarball `106af976…`, matches `SHA256SUMS.txt`) | `e936ce57888114e8dc24f5609e61454dcb77b1c609e1b7e59598d648b677673c` |

## Results: the README table

Operations per second, mean of rounds 1 and 2 (the published method). Garnet, Dragonfly and Redis: 2026-11-10, reused.

| | KiviDB 1.2.1 | KiviDB 1.2.0 | Garnet 2.2.5 | Dragonfly 2.0.2 | Redis 8.10.3 |
|---|---:|---:|---:|---:|---:|
| `GET`, pipeline 16 | **21.19M** | 21.33M | 20.51M | 9.01M | 2.39M |
| `SET`, pipeline 16 | 18.04M | 17.83M | **20.05M** | 8.28M | 1.97M |
| `GET`, no pipelining | **3.43M** | 3.11M | 2.78M | 3.16M | 1.27M |
| `SET`, no pipelining | **3.47M** | 3.26M | 2.89M | 3.20M | 1.24M |
| p99 `GET` / `SET`, pipeline 16 | **0.37 / 0.45 ms** | 0.36 / 0.45 | 0.91 / 1.02 | 0.90 / 1.10 | 2.25 / 2.69 |
| Bytes a key, 10M × 100 B | 143.6 | 143.5 | not reported | 133.8 | 177.7 |

Against the README's figures: `GET` at pipeline 16 is 21.19M (two-round mean; 21.31M over four rounds), 8.9x Redis, 2.4x Dragonfly, 3% ahead of Garnet, p99 0.37 ms. Garnet's pipelined `SET` still leads, by 11%.

## KiviDB 1.2.1 against 1.2.0 (all four rounds each, ABBA)

| | 1.2.1 | 1.2.0 | Δ |
|---|---:|---:|---:|
| `GET`, pipeline 16 | 21.31M | 21.30M | +0.1% |
| `SET`, pipeline 16 | 18.02M | 17.67M | +2.0% |
| `GET`, no pipelining | 3.26M | 3.08M | +6.1% |
| `SET`, no pipelining | 3.48M | 3.20M | +8.8% |
| p99 `GET`, pipeline 16 | 0.363 ms | 0.363 ms | 0.0% |
| p99 `SET`, pipeline 16 | 0.449 ms | 0.457 ms | -1.8% |

Against the earlier 1.2.1 build (`e7669a1f`, 2026-11-10, 4 rounds): `GET` p16 +0.5%, `SET` p16 +1.2%, `GET` p1 -1.5%, `SET` p1 +7.6%, p99 within 1.3%.

## Spread (lowest to highest round, all four rounds)

| | 1.2.1 | 1.2.0 |
|---|---|---|
| `GET` p16 | 20.96-21.49M (2.5%) | 21.22-21.44M (1.0%) |
| `SET` p16 | 17.94-18.13M (1.1%) | 17.50-17.87M (2.1%) |
| `GET` p1 | 2.97-3.50M (18%) | 2.99-3.22M (8%) |
| `SET` p1 | 3.38-3.60M (7%) | 2.97-3.35M (13%) |

The changes above 3% are all in the unpipelined rows, all in 1.2.1's favour, and inside the round-to-round range of a single build (1.2.1's own `GET` p1 ran from 2.97M to 3.50M). They are noise in the setup, not a property of the build; the 2026-11-10 session showed the same pattern.

## Files

- `tables.md`: every table, as `harness/aggregate-c3.py` printed it, including the reused engines and the 2026-11-10 KiviDB rounds (`prev121`, `prev120`).
- `all.csv`: one row per scenario per round per engine (this session and the reused rows).
- `results/raw.tar.gz`: `client-results.tgz` (memtier text and JSON output, `summary.csv` per round, from the load generator), `launch.log`, `server-build.log`, the session log `run.log`, and the reused 2026-11-10 `summary.csv` files (`reused/`). Unpack with `tar -xzf results/raw.tar.gz`.
- `harness/`: `c3-run.sh` (session driver, run from a workstation), `build-c3.sh` (release-style build from the copied tree), `user_data-server.sh`, `user_data-client.sh` (instance setup; memtier pinned to 2.5.2), `run-matrix.sh`, `start-server.sh`, `stop-server.sh`, `summarize.py` (the kivi-benchmark scripts, unchanged), `aggregate-c3.py`.
