# 1.2.1 on AWS bare metal: the README headline benchmark, rerun

The README's headline numbers come from the 1.2.0 bare-metal capture of 2026-11-04 (KiviDB 1.2.0 against Garnet 2.2.5, Dragonfly 2.0.2 and Redis 8.10.3). This folder reruns that capture with the same hardware, method, engine versions and memtier build. It adds KiviDB 1.2.1 side by side with the 1.2.0 release binary. All engines were measured again in the same session, so the table compares like with like.

**Result: 1.2.1 has not regressed.** Against 1.2.0 in this session, no figure moved by more than 3%:
- `GET` at pipeline 16: +0.2%.
- `SET` at pipeline 16: +0.8%.
- p99 at pipeline 16: 1-2% lower.
- Without pipelining: +2.8% on `GET` and -2.7% on `SET`. Those rows vary by ±5% from round to round within each build (see "Spread").

The 1.2.0 binary reproduced the published capture on the pipelined rows, within 0.5%. Its unpipelined rows came out 3-12% higher than published.

## What ran

| | |
|---|---|
| Server | AWS c7gn.metal (64 vCPU Graviton3), us-east-1f, Ubuntu 22.04 (kernel 6.8.0-1069-aws). Each engine was confined to 32 cores with `taskset` (`CORES=32`). |
| Load generator | A second c7gn.metal in the same cluster placement group. memtier_benchmark 2.5.2 through `harness/run-matrix.sh`, 60 threads. |
| Connections | 300 with pipeline 16 (5 a thread). 1,200 without pipelining (20 a thread). |
| Data | 10 million keys of 100 bytes, loaded before measuring and read uniformly at random. |
| Duration | 30 s a scenario (`DURATION=30 SIZES=100 PIPELINES="1 16" RATIOS="1:0 0:1"`). Each round loaded the data afresh. |
| Persistence | Off for every engine |
| Order | One engine at a time, 2026-11-10 16:33-17:08 UTC. The order was KiviDB 1.2.1 (rounds 1-2), 1.2.0 (1-2), Garnet, Dragonfly, Redis, then 1.2.0 (3-4) and 1.2.1 (3-4). Running the two KiviDB builds in ABBA order means drift over the session favours neither. |

The engines were started with the same commands as in the 1.2.0 capture. `launch.log` records each command and version:

| Engine | Command |
|---|---|
| KiviDB 1.2.1 / 1.2.0 | `taskset -c 0-31 kividb --port 6379 --bind 0.0.0.0 --threads 32` (io_uring on both) |
| Garnet v2.2.5 | `taskset -c 0-31 GarnetServer --port 6379 --bind 0.0.0.0 --index 1g` |
| Dragonfly v2.0.2 | `taskset -c 0-31 dragonfly --port 6379 --bind 0.0.0.0 --proactor_threads 32` |
| Redis 8.10.3 | `taskset -c 0-31 redis-server --port 6379 --bind 0.0.0.0 --protected-mode no --save "" --appendonly no --io-threads 8` |

## Builds

Both builds are the default variant (no Lua, no TLS). Both print `kividb 1.2.0`, because the version number is bumped at release.

| build | source | SHA-256 of the binary |
|---|---|---|
| **1.2.1** | master `e7669a1f70d14d6bfa4bd3e7e3879d9c7485016f`, built on the server the way `release.yml`'s linux-aarch64 job builds it: `amazonlinux:2023`, rustup stable (rustc 1.99.1), `RUSTFLAGS=-C target-feature=+lse`, `cargo build --release` (fat LTO). See `harness/build-121.sh` and `server-build.log`. | `497ddd4d8d5491432c9fd4f2d6f906c29871d05e52ca80d5701818fc1def8406` |
| **1.2.0** | release asset `kividb-linux-aarch64.tar.gz` (tarball `106af976…`, matches `SHA256SUMS.txt`) | `e936ce57888114e8dc24f5609e61454dcb77b1c609e1b7e59598d648b677673c` |

The 1.2.1 binary is byte-identical to the one [final](../final/README.md) measured for `74071531`. Between that commit and `e7669a1f`, nothing changed in `src/`, `Cargo.toml` or `Cargo.lock`.

## Results: the README table, measured again

These are operations per second, as the mean of rounds 1 and 2: two rounds, as the published method uses. Rounds 3 and 4 were run only for the two KiviDB builds. The 1.2.0 column from the 2026-11-04 capture is in brackets.

| | KiviDB 1.2.1 | KiviDB 1.2.0 | Garnet 2.2.5 | Dragonfly 2.0.2 | Redis 8.10.3 |
|---|---:|---:|---:|---:|---:|
| `GET`, pipeline 16 | **21.34M** | 21.27M [21.24M] | 20.51M [20.82M] | 9.01M [9.31M] | 2.39M [2.36M] |
| `SET`, pipeline 16 | 17.85M | 17.71M [17.79M] | **20.05M** [20.27M] | 8.28M [8.70M] | 1.97M [1.94M] |
| `GET`, no pipelining | 3.34M | **3.35M** [2.99M] | 2.78M [2.88M] | 3.16M [3.30M] | 1.27M [1.47M] |
| `SET`, no pipelining | **3.34M** | 3.33M [3.22M] | 2.89M [3.07M] | 3.20M [3.37M] | 1.24M [1.26M] |
| p99 `GET` / `SET`, pipeline 16 | **0.36 / 0.45 ms** | 0.37 / 0.46 [0.36 / 0.45] | 0.91 / 1.02 [0.67 / 0.80] | 0.90 / 1.10 [0.88 / 1.02] | 2.25 / 2.69 [2.29 / 2.72] |
| Bytes a key, 10M × 100 B | 143.5 | 143.5 [143.5] | not reported | 133.8 [133.8] | 177.7 [177.7] |

Measured against the README's figures for 1.2.0, which use the same method:
- `GET` at pipeline 16: 1.2.1 reached 21.34M, which is 8.9x Redis and 2.4x Dragonfly, and 4% ahead of Garnet.
- p99 at pipeline 16: 0.36 ms.

Garnet's pipelined `SET` again leads, by 12%. Without pipelining, KiviDB was ahead of Dragonfly in this session (3.34M against 3.16-3.20M). In the 1.2.0 capture Dragonfly was 5-10% ahead. Those rows are the noisiest in the run.

## KiviDB 1.2.1 against 1.2.0

The figures are means over all four rounds of each build, in ABBA order.

| | 1.2.1 | 1.2.0 | Δ |
|---|---:|---:|---:|
| `GET`, pipeline 16 | 21.21M | 21.16M | +0.2% |
| `SET`, pipeline 16 | 17.80M | 17.66M | +0.8% |
| `GET`, no pipelining | 3.31M | 3.22M | +2.8% |
| `SET`, no pipelining | 3.23M | 3.32M | -2.7% |
| p99 `GET`, pipeline 16 | 0.361 ms | 0.367 ms | -1.6% |
| p99 `SET`, pipeline 16 | 0.455 ms | 0.459 ms | -0.9% |

## Spread

Each range below runs from the lowest round to the highest, across all four rounds:

| | 1.2.1 | 1.2.0 |
|---|---|---|
| `GET` p16 | 20.87-21.35M (2.3%) | 20.96-21.30M (1.6%) |
| `SET` p16 | 17.66-17.95M (1.6%) | 17.50-17.72M (1.3%) |
| `GET` p1 | 3.20-3.36M (5%) | 3.04-3.47M (14%) |
| `SET` p1 | 3.00-3.36M (12%) | 3.17-3.49M (10%) |

- Garnet, Dragonfly and Redis varied by 2.5% or less between their two rounds at pipeline 16, and by 3.9% or less without pipelining.
- Without pipelining, run-to-run noise is larger than the gap between the two builds. Within one round, `GET` and `SET` move in opposite directions: in some rounds one is high and the other low. That points to noise in the setup, not to the build.
- The 1.2.0 capture showed `GET` p1 at 2.99M. This session's 3.04-3.47M shows how much that row varies between sessions.

## Files

- `all.csv`: one row for every scenario of every round of every engine.
- `tables.md`: the tables above as `aggregate.py` printed them, with the ranges.
- `results/<engine>-r<round>/`: memtier's text output per scenario, `load-100.txt`, `memory-100.txt`, `run.txt` and `summary.csv`.
- `client-results.tgz`: everything from the load generator, including memtier's JSON with the full latency percentiles.
- `launch.log`: every engine's command line and version, as `start-server.sh` recorded them.
- `server-build.log`: the 1.2.1 build, the checksum check of the 1.2.0 release, and the server's kernel.
- `harness/`:
  - `r121-run.sh`: the session driver, run from a workstation.
  - `build-121.sh`: the release-style build.
  - `main.tf`, `user_data-*.sh`: the two instances.
  - `run-matrix.sh`, `start-server.sh`, `stop-server.sh`, `summarize.py`, `install-more-engines.sh`: the kivi-benchmark scripts behind the 1.2.0 capture, unchanged.
