# Core string benchmark: KiviDB 1.2.1 against 1.2.0

**Result: 1.2.1 passes all 14 rows. No row is slower than the 1.2.0 release binary.**

- No row has every 1.2.1 round below every 1.2.0 round. After 4 alternating rounds each, the medians
  are within -0.1% / +2.6% of 1.2.0's.
- Server CPU per request is flat or lower:
  - GET p16: 2.87 → 2.85 µs;
  - SET p16: 3.37 → 3.35 µs;
  - MSET 10: 52.6 → 52.1 µs.
- Latency moved by less than 0.1 ms on every 100 B row:
  - p50 within 0.03 ms;
  - p99 within 0.05 ms;
  - p99.9 within 0.07 ms.
- **The release binary is reproducible.** v1.2.0, built from its tag the way `release.yml` builds it, is
  byte-identical to the published `kividb-linux-aarch64` binary (same SHA-256). Its 2 rounds are 2 more
  runs of the 1.2.0 code. They land inside 1.2.0's own spread, so the comparison is release-binary
  against release-way build, as it should be.
- All three binaries ran on io_uring.

## What ran

| | |
|---|---|
| Cloud | Azure **centralindia**. The server and both load generators sat in one proximity placement group, with accelerated networking. The region had 65 DPSv5 vCPUs free, enough for the layout. |
| Server | `Standard_D32ps_v5`: 32 vCPU Ampere Altra (Neoverse-N1), 128 GB, Ubuntu 24.04.5 LTS arm64, kernel 7.0.0-1017-azure |
| Load generators | 2 × `Standard_D16ps_v5`, memtier_benchmark 2.5.2 (built from its tag) |
| Harness | The 1.2.0 suite's `run-suite.sh`, `summarize.py`, `sample.sh` and `setup-client.sh`, unchanged. `engine.sh` is cut down to the three builds. The driver is `harness/run-121.sh` and the tables come from `harness/analyze-121.py`. |
| Date | 2026-11-09, 15:31-16:31 UTC |

These are the same VM sizes, region, CPU and kernel as the 1.2.0 suite's last round, which ran alternating builds on the same VMs.
The machines are new.

## Builds

All three are the default variant (no Lua, no TLS). `--version` prints `kividb 1.2.0` for all three: at
878746e4, Cargo.toml still says 1.2.0. That is expected.

| build | source | how | `--version` | SHA-256 |
|---|---|---|---|---|
| **1.2.1** | `878746e41e2f7a9050852e6ec5ef67394778368c` | built on the server, the release way | `kividb 1.2.0` | `3355e26e142272740614756bd4a2f1842b09b218b8dde0e5a27acee90c35b4a7` |
| **1.2.0** (release binary) | GitHub release v1.2.0, `kividb-linux-aarch64.tar.gz` | downloaded; tarball checked against `SHA256SUMS.txt` (`106af976…b78c`, OK) | `kividb 1.2.0` | `e936ce57888114e8dc24f5609e61454dcb77b1c609e1b7e59598d648b677673c` |
| 1.2.0 (built) | tag `v1.2.0` = `d241b74a2afe0faea662f2f528119c39070e431d` | built on the server, the release way | `kividb 1.2.0` | `e936ce57888114e8dc24f5609e61454dcb77b1c609e1b7e59598d648b677673c` (identical to the release binary) |

**"The release way"** is the `linux-arm64` job of `.github/workflows/release.yml`, which is the same at
v1.2.0 and 878746e4 (diffed on the server):
- the build runs in an `amazonlinux:2023` container (2023.12.20261102, gcc 11.5.0, glibc 2.34);
- `dnf install gcc gcc-c++ make cmake perl git openssl-devel pkgconfig`;
- rustup `stable`, which is **rustc 1.99.1 (58cf8f9c3 2026-10-13)**;
- `RUSTFLAGS="-C target-feature=+lse"`;
- `cargo build --release`. `[profile.release]` is the same at both commits: opt-level 3, `lto = "fat"`,
  `codegen-units = 1`.

**How this differs from the 1.2.0 suite's builds.** The 1.2.0 suite (`fetch-kividb.sh`) built natively
on the Ubuntu 24.04 server, with the same `cargo build --release`, the same `+lse` and the same rustc
1.99.1. So the Rust side is the same, and only the build host differs: Amazon Linux's gcc 11.5 and glibc
2.34 here, against Ubuntu's toolchain there. That host compiles the C dependencies and links against its
glibc. Here 1.2.1 was built the way it will ship, and the identical v1.2.0 build shows that this way
reproduces the published binary exactly.

Toolchain, container, checksums and `file` output are in `results/builds.txt`.

> The build script's first checksum check printed a mismatch. That was a harness bug, not a bad download:
> `SHA256SUMS.txt` lists files as `./kividb-linux-aarch64.tar.gz` and the grep left out the `./`. The
> check was rerun on the same file and passed (bottom of `results/builds.txt`).
> `harness/build-all.sh` now has the fixed grep.

## Method

This is the 1.2.0 suite's method, on all three value sizes:
- for each size, 10M keys are loaded (each load generator writes half, pipeline 16);
- a 5 s warm-up runs and is discarded;
- each scenario runs for 20 s, from both load generators at once;
- latency comes from their merged HdrHistograms, and CPU and NIC are sampled on all three machines over
  the middle 60% of each run.

The rows:
- GET and SET at 100 B, 1 KB and 2 KB, at pipeline 1 (2 × 15 threads × 40 = 1,200 connections) and
  pipeline 16 (300 connections);
- MGET and MSET of 10 keys at pipeline 1, after the 100 B block.

There was no depth sweep and no competitor round.

The rounds alternate builds on the same VMs:

| round | order |
|---|---|
| 1 | 1.2.1, 1.2.0 release binary, 1.2.0 built |
| 2 | 1.2.0 built, 1.2.0 release binary, 1.2.1 |
| 3 | 1.2.1, 1.2.0 release binary |
| 4 | 1.2.0 release binary, 1.2.1 |

Rounds 1-2 are the asked run. Rounds 3-4 were queued before rounds 1-2 finished, so that every row would
have 4 rounds per build whether or not it came out borderline. None did.

**Rule** (from the 1.2.0 suite's last round): a row **fails** only if every 1.2.1 round is below every
1.2.0 round **and** the medians differ by more than the spread. The spread is the larger of the two
builds' half min-max ranges, as % of the median, and never less than 1%.

## 1.2.1 against 1.2.0 (release binary), rounds 1-4

Each cell is median [min - max] over the 4 rounds.
- **C**: the load generators were the limit (≥90% CPU).
- **N**: the server NIC was the limit (≥85% of 2 GB/s on one direction).

Latency and CPU columns read 1.2.1 / 1.2.0.

| Row | 1.2.1 ops/s | 1.2.0 ops/s | Δ | spread 1.2.1 / 1.2.0 | Verdict | p50 ms | p99 ms | p99.9 ms | server µs/req | load gen. CPU % |
|---|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|
| 100 B GET p1 | 1.75M [1.74M - 1.78M] C | 1.75M [1.72M - 1.77M] C | -0.1% | ±1.0% / ±1.4% | pass (level, clients) | 0.68 / 0.68 | 1.05 / 1.05 | 1.90 / 1.88 | 17.39 / 17.38 | 95 / 95 |
| 100 B GET p16 | 10.53M [10.47M - 10.56M] | 10.50M [10.47M - 10.60M] | +0.3% | ±0.4% / ±0.6% | pass (level) | 0.44 / 0.44 | 0.79 / 0.79 | 1.25 / 1.27 | 2.85 / 2.87 | 88 / 87 |
| 100 B SET p1 | 1.71M [1.67M - 1.72M] C | 1.68M [1.68M - 1.73M] C | +1.6% | ±1.4% / ±1.5% | pass (level, clients) | 0.69 / 0.70 | 1.06 / 1.07 | 2.09 / 2.11 | 17.72 / 17.98 | 95 / 95 |
| 100 B SET p16 | 8.42M [8.41M - 8.48M] | 8.38M [8.33M - 8.42M] | +0.5% | ±0.5% / ±0.5% | pass (level) | 0.56 / 0.56 | 0.96 / 0.97 | 1.26 / 1.26 | 3.35 / 3.37 | 75 / 74 |
| 100 B MGET 10 p1 | 1.18M [1.17M - 1.19M] C | 1.15M [1.15M - 1.18M] C | +2.6% | ±0.8% / ±1.3% | pass (level, clients) | 0.99 / 1.02 | 1.45 / 1.50 | 3.79 / 3.78 | 25.64 / 26.25 | 94 / 93 |
| 100 B MSET 10 p1 | 575K [567K - 580K] | 569K [565K - 572K] | +1.1% | ±1.1% / ±0.6% | pass (level) | 2.09 / 2.11 | 2.92 / 2.90 | 5.33 / 5.26 | 52.08 / 52.63 | 63 / 62 |
| 1 KB GET p1 | 1.63M [1.62M - 1.65M] C N | 1.62M [1.57M - 1.63M] C N | +0.6% | ±0.8% / ±1.9% | pass (level, clients) | 0.72 / 0.73 | 1.08 / 1.11 | 2.10 / 2.20 | 18.59 / 18.71 | 95 / 95 |
| 1 KB GET p16 | 1.87M [1.87M - 1.87M] N | 1.87M [1.87M - 1.87M] N | -0.0% | ±0.1% / ±0.1% | pass (level) | 1.26 / 1.27 | 7.28 / 7.26 | 7.98 / 7.97 | 8.14 / 8.12 | 47 / 47 |
| 1 KB SET p1 | 1.62M [1.62M - 1.63M] C N | 1.59M [1.57M - 1.63M] C N | +2.3% | ±0.4% / ±1.9% | pass (level, clients) | 0.73 / 0.75 | 1.10 / 1.15 | 2.27 / 2.26 | 18.66 / 19.09 | 95 / 95 |
| 1 KB SET p16 | 2.80M [2.80M - 2.80M] N | 2.80M [2.80M - 2.80M] N | -0.0% | ±0.1% / ±0.1% | pass (level) | 0.71 / 0.75 | 7.25 / 7.13 | 7.79 / 7.65 | 6.45 / 6.16 | 37 / 37 |
| 2 KB GET p1 | 920K [919K - 920K] N | 920K [920K - 920K] N | +0.0% | ±0.1% / ±0.1% | pass (level) | 1.17 / 1.18 | 3.01 / 2.99 | 4.35 / 4.30 | 27.83 / 27.81 | 83 / 83 |
| 2 KB GET p16 | 938K [938K - 939K] N | 938K [937K - 938K] N | +0.1% | ±0.1% / ±0.1% | pass (level) | 7.05 / 7.12 | 9.31 / 9.25 | 9.95 / 9.98 | 14.51 / 14.39 | 43 / 42 |
| 2 KB SET p1 | 1.30M [1.29M - 1.31M] C N | 1.29M [1.28M - 1.30M] C N | +0.2% | ±0.7% / ±0.8% | pass (level, clients) | 0.92 / 0.91 | 1.42 / 1.59 | 2.91 / 3.07 | 23.39 / 23.39 | 94 / 94 |
| 2 KB SET p16 | 1.43M [1.43M - 1.43M] N | 1.43M [1.43M - 1.43M] N | +0.0% | ±0.1% / ±0.1% | pass (level) | 0.92 / 1.01 | 9.25 / 9.31 | 10.34 / 10.21 | 9.26 / 9.21 | 25 / 27 |

The two-round tables (the asked run) are in `results/tables-rounds1-2.md` and give the same verdicts:
- every row passes;
- the medians are within -0.1% / +2.5%.

`results/tables-rounds1-4.md` also has:
- 1.2.1 against the 1.2.0 release-way build;
- the 1.2.0 build against the release binary (-0.8% to +2.4%, all level);
- the round-by-round rows with the user/kernel CPU split.

## Where 1.2.1 loses, and caveats

- **No row is a loss.** The lowest medians are 100 B GET p1 (-0.1%) and the NIC-bound rows (-0.0%), all
  inside the spread. The biggest gains are MGET 10 (+2.6%), 1 KB SET p1 (+2.3%) and 100 B SET p1 (+1.6%).
  They are no more real than the losses:
  - all three rows are client-bound;
  - none has its rounds apart from 1.2.0's, except that 1.2.1's 1 KB SET p1 rounds (1.62-1.63M) sit at
    the top of 1.2.0's range (1.57-1.63M).
  - The 1.2.0 release-way build, which is the same binary, scored 1.62-1.63M there too.
  - Read these rows as level.
- **Most 1 KB and 2 KB rows do not measure the server's speed.**
  - The pipelined rows stop at the server's 16 Gbps NIC: ~1.87M GET/s at 1 KB, ~0.94M at 2 KB, and 2.80M /
    1.43M SET (inbound).
  - The unpipelined rows are limited by the load generators, the NIC, or both.
  - On these rows, compare latency and server CPU per request, not ops/s. This was already so in the
    1.2.0 suite.
- **1 KB SET p16 server CPU, 6.45 against 6.16 µs/req (+4.7%), is time, not code.** Both builds were at the
  NIC cap, with 51-61% of the server busy.
  - In round 1 they measured exactly the same: 6.96 µs (1.2.1) and 6.96 µs (1.2.0).
  - After that, 1.2.0's rounds fell (6.25, 6.07, 5.87) while 1.2.1 stayed at 6.28-6.52.
  - The 1.2.0 release-way build measured 6.80 / 6.56, and it is the same binary as the release.
  - Its p99 (7.25 against 7.13 ms) moves the same way. At the NIC cap that is queueing behind the
    link.
  - Every 100 B row, where the CPU is the limit, shows 1.2.1 level or lower in CPU per request.
- **The 100 B unpipelined rows (GET/SET/MGET p1) are client-bound.** The load generators ran at 93-95% CPU.
  These rows swing ±1-2% from round to round, as in the 1.2.0 suite.
- **Absolute numbers differ from earlier sessions.** 100 B GET p16 was 10.5M here, against 10.8M in
  the 1.2.0 suite's last round (same sizes and region) and 10.45M in its main run (eastasia). Those are
  different machines on different days. Only the alternating rounds of one session are compared here.
- **What 1.2.1 changes on the hot path.** The functional changes since 1.2.0 cover:
  - `appendfsync always`;
  - AOF turn-over sync;
  - `CLUSTER FAILOVER` during slot migration;
  - `CLIENT PAUSE WRITE` and `SCRIPT LOAD`/`FLUSH`;
  - master-driven hash-field expiry;
  - MIGRATE routing.

  None of them touches plain GET/SET/MGET/MSET with persistence off. This run confirms it.
- Not measured here: AOF on (`appendfsync everysec` / `always`), Lua/TLS variants, cluster mode. The
  persistence and soak areas of the release checks cover those.

## Files

- `results/summary.csv`, `results/runs.csv`: medians and per-round rows over rounds 1-4 (`summarize.py`,
  merged histograms).
- `results/tables-rounds1-2.md`, `results/tables-rounds1-4.md`: the tables as `analyze-121.py` printed
  them.
- `results/builds.txt`: build log summary, toolchain, container, commits, versions, SHA-256s, the
  `release.yml` diff and the tarball checksum check.
- `results/server-info.txt`: kernel, OS, CPU, the I/O model each build reported, and the memtier version.
- `results/launch.log`: every engine start, with its command line, version and SHA-256 prefix.
- `results/suite.log`, `results/driver.log`: the driver's log.
- `results/<build>/r<round>/d<size>/`:
  - raw memtier output (`*.json` with the compressed latency histograms, `*.txt`);
  - CPU and NIC samples (`*.cpu`) and `memory.txt`;
  - private addresses are replaced by `SERVER` / `CL1` / `CL2`;
  - `*.hgrm` files were dropped, since the JSON holds the same histograms.
- `harness/`:
  - `create.sh`: the VMs (one PPG, accelerated networking, SSH from one /32 only, auto-shutdown);
  - `setup-server.sh`: the server setup (the 1.2.0 suite's, plus Docker for the release-way build);
  - `setup-client.sh`: the load generators;
  - `build-all.sh`: the three builds;
  - `engine.sh`: starts and stops each build;
  - `run-121.sh`: the driver;
  - `run-suite.sh`, `sample.sh`, `summarize.py`: the 1.2.0 suite's, unchanged;
  - `analyze-121.py`: the tables.

The VMs and their resource group were deleted after the run.
