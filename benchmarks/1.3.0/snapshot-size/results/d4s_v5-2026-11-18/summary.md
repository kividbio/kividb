# #879 on one Standard_D4s_v5, 2026-11-18

Binaries: v1.2.2 release tarball; master aee34f90; the change (candidate, `yes` and `no`); the change with no compression thread (`MAX_HELPERS = 0`). Redis 8.10.2 (`redis:8.10.2` image). Server on CPUs 0-1, `--threads 2` (one compression thread: the CPUs less one, at most 2). One run each; the latency columns are single runs and noisy (master itself ranged 3.4-17 ms p99.9).

**6M x 150 B (#879, memtier payload)** (tmpfs unless noted)

| | size | save wall / CPU | save peak RSS (1st save, HWM) | save on VM disk | load | full sync / master mem | writes p99 / p99.9 during save |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1.2.2 | 1006.9 MB | 1.75 / 2.13 s | +73 MB | 6.2 s | 10.4 s | 10.7 s / +1055 MB | 1.191 / 3.263 ms |
| master | 1006.9 MB | 1.70 / 1.96 s | +73 MB | 6.4 s | 10.1 s | 10.5 s / +1055 MB | 2.111 / 3.711 ms |
| PR, `no` | 1006.9 MB | 1.84 / 2.13 s | +73 MB | 6.3 s | 10.5 s | 10.5 s / +1055 MB | 1.991 / 4.287 ms |
| PR, `yes` | 32.7 MB | 2.08 / 2.99 s | +74 MB | 2.0 s | 11.2 s | 10.9 s / +99 MB | 2.127 / 3.295 ms |
| PR, `yes`, no helper thread | 32.7 MB | 2.38 / 2.70 s | +74 MB | - | 10.6 s | 10.8 s / +98 MB | 0.863 / 2.463 ms |
| Redis 8.10.2 | 142.9 MB | 6.13 / 6.09 s | (fork) | 6.4 s | 6.0 s | 11.8 s / (fork) | 0.463 / 1.335 ms |

**6M text strings, 16-64 B** (tmpfs unless noted)

| | size | save wall / CPU | save peak RSS (1st save, HWM) | save on VM disk | load | full sync / master mem | writes p99 / p99.9 during save |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1.2.2 | 358.9 MB | 1.15 / 1.44 s | +72 MB | - | 8.8 s | 8.7 s / +406 MB | 0.775 / 1.663 ms |
| master | 358.9 MB | 1.13 / 1.45 s | +73 MB | - | 8.4 s | 8.7 s / +406 MB | 1.311 / 3.407 ms |
| PR, `no` | 358.9 MB | 1.20 / 1.53 s | +73 MB | - | 8.4 s | 8.7 s / +407 MB | 0.935 / 2.559 ms |
| PR, `yes` | 131.1 MB | 2.00 / 3.92 s | +74 MB | - | 8.7 s | 10.0 s / +192 MB | 2.127 / 3.519 ms |
| Redis 8.10.2 | 308.2 MB | 5.20 / 5.19 s | (fork) | - | 5.1 s | 9.1 s / (fork) | 0.935 / 3.231 ms |

**6M random strings, 16-64 B** (tmpfs unless noted)

| | size | save wall / CPU | save peak RSS (1st save, HWM) | save on VM disk | load | full sync / master mem | writes p99 / p99.9 during save |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1.2.2 | 358.9 MB | 1.14 / 1.43 s | +73 MB | - | 8.5 s | 8.7 s / +406 MB | 0.775 / 2.287 ms |
| master | 358.9 MB | 1.13 / 1.49 s | +73 MB | - | 8.3 s | 8.6 s / +406 MB | 1.623 / 3.391 ms |
| PR, `no` | 358.9 MB | 1.21 / 1.57 s | +73 MB | - | 8.5 s | 8.9 s / +406 MB | 0.823 / 1.767 ms |
| PR, `yes` | 304.6 MB | 2.22 / 4.23 s | +74 MB | - | 9.0 s | 10.0 s / +367 MB | 1.999 / 4.415 ms |
| Redis 8.10.2 | 323.0 MB | 4.98 / 4.93 s | (fork) | - | 4.2 s | 8.8 s / (fork) | 0.439 / 0.871 ms |

**1M hashes x10 + 200k sets/zsets/lists x20** (tmpfs unless noted)

| | size | save wall / CPU | save peak RSS (1st save, HWM) | save on VM disk | load | full sync / master mem | writes p99 / p99.9 during save |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1.2.2 | 490.4 MB | 1.40 / 2.35 s | +490 MB | - | 6.1 s | 6.9 s / +980 MB | 1.071 / 3.423 ms |
| master | 490.4 MB | 1.30 / 2.26 s | +490 MB | - | 5.8 s | 6.4 s / +981 MB | 2.543 / 17.023 ms |
| PR, `no` | 490.4 MB | 1.31 / 2.23 s | +490 MB | - | 6.0 s | 6.4 s / +981 MB | 2.607 / 6.847 ms |
| PR, `yes` | 205.9 MB | 3.04 / 4.23 s | +493 MB | - | 6.9 s | 8.8 s / +712 MB | 2.639 / 11.711 ms |
| PR, `yes`, no helper thread | 205.9 MB | 3.08 / 4.00 s | +492 MB | - | 6.8 s | 8.7 s / +711 MB | 2.495 / 3.759 ms |
| Redis 8.10.2 | 392.5 MB | 3.19 / 3.14 s | (fork) | - | 2.6 s | 5.0 s / (fork) | 0.479 / 1.703 ms |

These rows were taken with one compression thread started for each save. The
final code starts the compression threads once and shares them (the same
count); rerun with it (`final-*.log` in `raw.tar.gz`): the issue's dataset
32.7 MB, save 2.33 / 3.31 s, load 10.7 s; the collections 205.9 MB, save
3.08 / 4.25 s.

Hot path (`scripts/perf/run.sh`, LTO fat, `CMP=plain`, `EXEC=1`; instructions
a command; `perf-hot-path.log`):

| workload | master uring | branch uring | master tokio | branch tokio |
|---|---:|---:|---:|---:|
| GET-100-EXEC | 1702 | 1703 | 1703 | 1702 |
| SET-100-EXEC | 2300 | 2301 | 2301 | 2300 |
| GET-1K-EXEC | 2100 | 2081 | 2040 | 2038 |
| SET-1K-EXEC | 2424 | 2424 | 2424 | 2424 |
| MIX-100-EXEC | 4028 | 4028 | 4027 | 4028 |

`compat-optin.log`: the cross-version matrix again with `rdbcompression` opt-in (default `no`), on the tree merged with master: 26/26.
