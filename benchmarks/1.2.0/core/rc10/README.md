# Core string benchmark, quick check: KiviDB 1.2.0-rc10 against rc8

**Result: rc10 passes on all six rows; rc9 and rc10 did not move the hot path.**

- No row has every rc10 round below every rc8 round. All medians are within -1.2% / +1.7% after 4
  rounds, and within -2.9% / +1.9% after the 2 rounds asked for.
- Server CPU per request is flat:
  - GET p16: 2.77 → 2.77 µs;
  - SET p16: 3.45 → 3.38 µs;
  - MSET 10: 51.9 → 52.2 µs.
- p99 and p99.9 are within ±0.1 ms.
- The closest row after 2 rounds was MSET 10, at -1.5%. rc10's best round (577,497 ops/s) was
  only 325 ops/s above rc8's worst (577,173). Two more rounds settle it: -0.6% with the ranges
  overlapping.
- Both builds ran on io_uring.

## What ran

| | |
|---|---|
| Cloud | Azure **centralindia**. The server and both load generators sat in one proximity placement group, with accelerated networking. |
| Server | `Standard_D32ps_v5`: 32 vCPU Ampere Altra (Neoverse-N1), 128 GB, Ubuntu 24.04 arm64, kernel 7.0.0-1017-azure |
| Load generators | 2 × `Standard_D16ps_v5`, memtier_benchmark 2.5.2 |
| Harness | The rc8 round's harness (the suite's `run-suite.sh`, `summarize.py`, `fetch-kividb.sh` and `sample.sh`). `engine.sh` also knows `kividb-rc10`. The driver is `harness/run-rc10.sh` and the tables come from `harness/analyze-rc10.py`. |
| Date | 2026-11-03, 11:24-11:45 UTC |

These are the same VM sizes, region, CPU and kernel as the rc8 round. The machines are new.

**Builds.**
- Both were built on the server, one after the other, with `fetch-kividb.sh`: `cargo build --release` with
  `RUSTFLAGS="-C target-feature=+lse"`, the default variant.
- The toolchain was rustc stable, which is **rustc 1.99.1 (58cf8f9c3 2026-10-13)**, the rc8 round's toolchain.
- rc8's binary is byte-identical to the rc8 round's binary (same SHA-256).

| build | source | `--version` | SHA-256 |
|---|---|---|---|
| rc10 | `39bfacd231bb294d390448eacfe0e243d365ebe8` | `kividb 1.2.0-rc10` | `4511b0c2ec1519de39a4857d89d1a19467cec74463b23ee6c973db5cdb510635` |
| rc8 | `082dab3d8929c180bccf993835e28992ea054fb5` | `kividb 1.2.0-rc8` | `ef2208fd547bb06348560dc1f0bce48302debc098e16de58ff37431bbbcf1260` |

**Method.** This is the rc8 round's method:
- 10M keys of 100 B loaded before each run;
- a 5 s warm-up;
- 20 s per scenario, from both load generators at once;
- latency taken from their merged histograms.

The rows are 100 B GET and SET at pipeline 1 (1,200 connections) and pipeline 16 (300 connections),
then MGET and MSET of 10 keys at pipeline 1. There was no depth sweep and no competitor round,
because neither was asked for.
- Rounds 1-2 are the asked run. Round 1 ran rc10 then rc8, and round 2 ran rc8 then rc10.
- Rounds 3-4 were added because MSET 10 was borderline. Round 3 ran rc10 first and round 4 rc8 first.
  - They were meant to cover MGET/MSET only.
  - `run-suite.sh` reads an empty `WL` as its default, so they ran the whole 100 B block.
  - So every row has 4 rounds per build.

## rc10 against rc8

Each cell is the median [min - max] of the rounds. **C** means the load generators were the limit
(>90% CPU). The spread is half each build's min-max range, as % of its median.

**Rule:** a row **fails** only if every rc10 round is below every rc8 round **and** the medians
differ by more than 1%.

### Rounds 1-2 (the asked run)

| Scenario | rc8 | rc10 | Change | Spread rc8 / rc10 | Verdict | p99 ms rc8 → rc10 | p99.9 ms rc8 → rc10 | server µs/req rc8 → rc10 |
|---|---:|---:|---:|---:|---|---:|---:|---:|
| 100 B GET p1 | 1.78M [1.75M - 1.81M] C | 1.75M [1.71M - 1.78M] C | −2.0% | ±1.7% / ±1.9% | pass (level, clients) | 1.02 → 1.04 | 1.82 → 1.92 | 17.05 → 17.35 |
| 100 B GET p16 | 10.81M [10.81M - 10.81M] | 10.79M [10.75M - 10.82M] | −0.2% | ±0.0% / ±0.3% | pass (level) | 0.75 → 0.76 | 1.25 → 1.26 | 2.77 → 2.77 |
| 100 B SET p1 | 1.72M [1.70M - 1.74M] C | 1.67M [1.64M - 1.70M] C | −2.9% | ±1.1% / ±1.9% | pass (level, clients) | 1.04 → 1.07 | 1.95 → 2.02 | 17.57 → 18.09 |
| 100 B SET p16 | 8.24M [8.20M - 8.27M] | 8.33M [8.30M - 8.35M] | +1.1% | ±0.4% / ±0.3% | pass (faster) | 0.96 → 0.98 | 1.26 → 1.30 | 3.44 → 3.38 |
| 100 B MGET 10 p1 | 1.16M [1.15M - 1.17M] C | 1.18M [1.18M - 1.19M] C | +1.9% | ±1.0% / ±0.8% | pass (faster, clients) | 1.46 → 1.43 | 3.85 → 3.81 | 26.02 → 25.52 |
| 100 B MSET 10 p1 | 582K [577K - 586K] | 573K [569K - 577K] | −1.5% | ±0.8% / ±0.8% | pass (level, 325 ops/s overlap) | 2.80 → 2.85 | 5.17 → 5.18 | 51.59 → 52.33 |

### Rounds 1-4

| Scenario | rc8 | rc10 | Change | Spread rc8 / rc10 | Verdict | p99 ms rc8 → rc10 | p99.9 ms rc8 → rc10 | server µs/req rc8 → rc10 |
|---|---:|---:|---:|---:|---|---:|---:|---:|
| 100 B GET p1 | 1.76M [1.73M - 1.81M] C | 1.75M [1.69M - 1.80M] C | −0.7% | ±2.1% / ±3.2% | pass (level, clients) | 1.01 → 1.03 | 1.86 → 1.92 | 17.25 → 17.35 |
| 100 B GET p16 | 10.80M [10.79M - 10.81M] | 10.77M [10.63M - 10.82M] | −0.3% | ±0.1% / ±0.8% | pass (level) | 0.75 → 0.76 | 1.25 → 1.26 | 2.77 → 2.77 |
| 100 B SET p1 | 1.70M [1.70M - 1.74M] C | 1.68M [1.64M - 1.73M] C | −1.2% | ±1.3% / ±2.8% | pass (level, clients) | 1.05 → 1.07 | 1.97 → 2.02 | 17.79 → 18.00 |
| 100 B SET p16 | 8.19M [8.17M - 8.27M] | 8.33M [8.23M - 8.38M] | +1.7% | ±0.6% / ±0.9% | pass (level) | 0.97 → 0.97 | 1.26 → 1.28 | 3.45 → 3.38 |
| 100 B MGET 10 p1 | 1.16M [1.15M - 1.17M] C | 1.18M [1.18M - 1.19M] C | +1.5% | ±1.0% / ±0.8% | pass (faster, clients) | 1.46 → 1.43 | 3.88 → 3.76 | 25.98 → 25.59 |
| 100 B MSET 10 p1 | 578K [574K - 586K] | 574K [569K - 577K] | −0.6% | ±1.0% / ±0.8% | pass (level) | 2.83 → 2.85 | 5.17 → 5.20 | 51.91 → 52.23 |

Round-by-round rows, with CPU split and load-generator CPU, are in `results/tables-rounds1-4.md`.

Notes on the rows:
- **GET p1 and SET p1 are client-bound.** The load generators ran at 95% CPU, and these rows swing ±2-3%
  from round to round, as in the rc8 round. SET p1 rounds: rc10 1.64M / 1.70M / 1.73M / 1.66M, rc8
  1.74M / 1.70M / 1.70M / 1.70M. rc10's best round is above rc8's worst, and its server CPU per
  request is within 1.2% (17.79 → 18.00 µs).
- **MSET 10 is level.** rc10 measured 569-577K and rc8 574-586K. Server CPU per command is
  51.9 → 52.2 µs (+0.6%), with the same user/kernel split, 94% server busy, and the load generators at
  62-64%. The rc8 round measured rc8 at 564-572K.
- **What rc9/rc10 changed on the per-command path.** Commit 14662105 (writes to keys a MIGRATE is
  moving wait instead of answering TRYAGAIN) touches `run_command`, `server.rs`, `server_uring.rs`
  and `pause.rs`. With no MIGRATE in flight, the write gate still does one atomic load
  (`pause::writes_must_check`), as rc8's handoff check did. The other rc9/rc10 commits touch
  replication, shutdown, cluster heartbeat and config only.
- **These absolute numbers are higher than the rc8 round's.** rc8 did GET p16 at 10.80M here against 10.44M there.
  That is the machines and the session, not code: rc8 is the identical binary.

## Files

- `results/summary.csv`, `results/runs.csv`: medians and per-round rows over rounds 1-4 (summarize.py,
  merged histograms).
- `results/tables-rounds1-2.md`, `results/tables-rounds1-4.md`: the tables as `analyze-rc10.py`
  printed them.
- `results/builds.txt`: toolchain, commits, versions and SHA-256s. `results/launch.log`: every engine start
  with its command line. `results/suite.log`: the driver's log.
- `results/<engine>/r<round>/d100/`: raw memtier output (`*.json` with the compressed latency
  histograms, `*.txt`), CPU and NIC samples (`*.cpu`), and `memory.txt`. Private addresses are replaced by
  `SERVER` / `CL1` / `CL2`.
- `harness/`:
  - `create.sh`: the VMs;
  - `setup-server-min.sh`: the server setup;
  - `build-both.sh`: the two builds;
  - `engine.sh`: with `kividb-rc10` added;
  - `run-rc10.sh` and `run-rc10-multi.sh`: the drivers for rounds 1-2 and 3-4;
  - `analyze-rc10.py`: the tables.

  The rest of the harness is the rc8 round's, unchanged.
- `NOTES.md`: the progress log.
