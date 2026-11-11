# Memory: bytes per key, 1.2.1 against 1.2.0

> The rest of the memory suite (snapshots, the AOF rewrite, eviction during a snapshot, active defrag) and the
> competitors were rerun later on commit `ccba20e2`: see [full/](full/README.md).

The memory suite of `benchmarks/1.2.0/memory/` was run on **1.2.1 (878746e4)** and on the **1.2.0 release
binary**. The builds alternated on the same two VMs in one session on 2026-11-09. It used the 1.2.0 round's
harness, the same VM sizes and region as its last round ("pair A"), and the same scenarios:
- bytes per key while the dataset grows to 50M keys, at 100 B and at 1 KB;
- the warefeats/kv-stores bytes-per-key cell (1M x 100 B, then a second million);
- the empty engine's `used_memory`.

One scenario is new: **hashes whose fields carry a TTL**, because #531 (#415) changed how 1.2.1 handles field
expiry. No competitors ran.

**Verdict: no regression. 1.2.1 uses the same memory as 1.2.0.**
- **`used_memory` per key is identical to a hundredth of a byte in every cell:**
  - growth 100 B: 143.11 B/key at 10M and 140.08 at 50M, for both builds;
  - growth 1 KB: 1,068.09 B/key at 50M;
  - warefeats: 146.9 B/key, both millions, ASCII and random.
- **RSS per key is within 0.4%, and within the run-to-run spread:**
  - at 100 B, 145.0 against 144.7 B/key at 10M, and 140.7 against 140.6 at 50M;
  - at 1 KB, 1,071.6 against 1,071.7 at 50M.
- **Hash field TTLs cost the same:**
  - `used_memory` is 135.1 B per field without a TTL and 223.4 with one, for both builds, so a field's TTL adds
    88.3 B;
  - 1.2.1's RSS is 0.65% lower with TTLs, in all three rounds.
- **The empty engine** holds 2,889,415 B of `used_memory` against 2,887,322 B, 2 KB (+0.07%) more.

## Headline

| scenario | metric | 1.2.1 (878746e4) | 1.2.0 (release binary) | change |
|---|---|---|---|---|
| growth 100 B, 10M keys (3 rounds each) | RSS / used B/key | **145.0** / 143.1 | **144.7** / 143.1 | +0.2% / 0.0% |
| growth 100 B, 50M keys (3 rounds each) | RSS / used B/key | **140.7** / 140.1 | **140.6** / 140.1 | +0.03% / 0.0% |
| growth 100 B | RSS at 50M keys | 6.57 GiB | 6.57 GiB | 0.0% |
| growth 1 KB, 10M keys (2 rounds each) | RSS / used B/key | 1,076.0 / 1,071.1 | 1,076.1 / 1,071.1 | -0.01% / 0.0% |
| growth 1 KB, 50M keys (2 rounds each) | RSS / used B/key | 1,071.6 / 1,068.1 | 1,071.7 / 1,068.1 | -0.00% / 0.0% |
| warefeats ASCII, 1st M (3 runs each) | RSS / used B/key | **149.0** / 146.9 | **149.0** / 146.9 | 0.0% / 0.0% |
| warefeats ASCII, 2nd M | RSS / used B/key | 147.0 / 146.9 | 146.9 / 146.9 | +0.1% / 0.0% |
| warefeats random, 1st M (2 runs each) | RSS / used B/key | 149.0 / 146.9 | 148.9 / 146.9 | +0.1% / 0.0% |
| warefeats random, 2nd M | RSS / used B/key | 151.8 (146.9-156.7) / 146.9 | 152.3 (148.0-156.7) / 146.9 | -0.3% / 0.0% |
| hash field TTLs, 1M x 10 fields (3 rounds each) | used B/field, no TTL / with TTL | 135.1 / 223.4 | 135.1 / 223.4 | 0.0% / 0.0% |
| | RSS B/field, no TTL / with TTL | 186.0 / 319.1 | 186.0 / 321.2 | 0.0% / -0.65% |
| idle engine (8 starts each) | used_memory | 2,889,415 B | 2,887,322 B | +2,093 B (+0.07%) |
| growth load time, 50M keys | s, 100 B / 1 KB | 12.9 / 22.9 | 12.9 / 22.9 | 0.0% / -0.2% |

All cells are medians, with the range over rounds in brackets where it is wider than the rounding. Every cell,
with every checkpoint and the ranges, is in `results/tables.md`.

## Hardware

| | VM | CPU | memory | region | used for |
|---|---|---|---|---|---|
| engine | Standard_E32ds_v7 (32 vCPU) | Intel Xeon 6973P-C | 251 GiB, local NVMe 440 GB (data dir) | koreacentral | every engine run, one at a time |
| client | **Standard_D16ls_v7** (16 vCPU) | Intel Xeon 6973P-C | 32 GiB | koreacentral | memtier_benchmark 2.5.1, redis-cli, orchestration |

- The VM sizes are those of the 1.2.0 memory suite's pair A, except the client.
  - The 1.2.0 rounds used a 32-vCPU Standard_D32ls_v7 client.
  - In this session, the region's 65-vCPU quota had 16 vCPUs in use by another run, so the client is the 16-vCPU
    size of the same family.
  - The client changes how fast keys are loaded, not how many bytes each key takes. The 50M-key load times came
    out the same as in the 1.2.0 round anyway: 12.9 and 22.9 s, against 13.0 and 22.9.
- **Placement.**
  - Both VMs are in one proximity placement group, with accelerated networking, Ubuntu 24.04 and kernel
    7.0.0-1017-azure.
  - Engines ran in Docker with host networking and seccomp unconfined (io_uring allowed).
  - One engine ran at a time, each in its own wiped data directory on the NVMe disk. No persistence.
- **Source.** Details are in `results/raw/engine-vm.txt`.

## Builds

| build | what | `--version` | binary SHA-256 |
|---|---|---|---|
| 1.2.0 | `kividb-linux-x86_64.tar.gz` from the v1.2.0 GitHub release | kividb 1.2.0 | `a211d544be79bb78cfc5bc16b1e1315d20913f154cd8179f80668ab048edd72f` |
| 1.2.1 | commit `878746e41e2f7a9050852e6ec5ef67394778368c`, built as `release.yml` builds the release binary | kividb 1.2.0 | `b07d52342c774f8e4dcd6122ed85681cf96718366860a6872b45e4b8bcf63a05` |

- **1.2.0.** This is the shipped binary, not a rebuild.
  - The tarball's SHA-256 is `2d918e7117703a50bdc016f9e9f692769081917604f91fa0c6db51c202e4b78c`.
  - That matches the release's `SHA256SUMS.txt`: `sha256sum -c` printed OK.
- **1.2.1.** This is `release.yml`'s Linux x86_64 job.
  - The build ran inside an `amazonlinux:2023` container, with the job's `dnf install` list and rustup `stable`
    on the minimal profile (`rustc 1.99.1 (58cf8f9c3 2026-10-13)`).
  - The command was `cargo build --release` with default features: the default variant, no Lua or TLS. The release
    profile is `lto = "fat"`, `codegen-units = 1`.
  - Cargo.toml still says 1.2.0 at this commit, so `--version` prints 1.2.0. That is expected.
- **How they ran.** Each binary was wrapped in the same minimal image (`ubuntu:24.04` plus the binary,
  `ENTRYPOINT kividb`), so that the harness's `eng.sh` and warefeats' compose files start both in the same way.
  This is not the published Docker image (distroless); the server process is the same in both.
- **Flags.**
  - Our own tests: `--port 6379 --bind 0.0.0.0 --protected-mode no --dir /data`, with default threads (32) and
    default active defrag.
  - warefeats: their compose file unchanged (`maxmemory 16 GiB, allkeys-lru`).
  - Every start of both builds reported `repl-backlog-size` 64 MB, `repl_backlog_active:0` and
    `repl_backlog_histlen:0`: no backlog is allocated without a replica.

## Method

The 1.2.0 suite's method, unchanged.

**Rounds and order.** The builds alternated on the same VMs:
- 1.2.0 went first in rounds 1 and 3, and 1.2.1 first in round 2;
- warefeats ran in the order 1.2.0, 1.2.1, 1.2.1, 1.2.0, 1.2.0, 1.2.1.

The exact job list, in order, is `harness/queue.txt`. `used_memory` is the counting allocator's sum of what was
requested, so it repeats to the byte for the same data, as it did here in every round. RSS repeats within a few
tenths of a byte per key. Two or three rounds per build are enough to tell a change from noise.

**Growth** (`harness/growth.sh`, unchanged).
- A fresh engine with no persistence.
- Keys `memtier-1` ... `memtier-50000000` are written once each, in stages to 1M, 5M, 10M, 25M and 50M keys.
- Values are random bytes (`--random-data`) of 100 B (3 rounds per build) or 1 KB (2 rounds per build).
- The load uses memtier with 8 threads x 8 connections, pipeline 32.
- After each stage the load pauses 10 s and a checkpoint is taken.
- Bytes per key = (value at the checkpoint - the empty engine's) / keys, for both `used_memory` and RSS. RSS is the
  sum over the container's processes, read by the 1-second sampler.

**warefeats/kv-stores** (`harness/warefeats.sh`, their harness at a324d29, unchanged).
- Only their memory cells ran this time (`WF_ONLY`), not the throughput matrix:
  - `memory-load` (memtier's ASCII filler), 3 runs per build;
  - `memory-load-random` (random bytes), 2 runs per build.
- Each run is 1M keys of 100 B on a fresh engine, then a second million on the same engine. The bytes per key are
  their runner's own `bytesPerKey*` metrics.

**Idle `used_memory`.** `eng.sh` reads `INFO memory` from the empty engine 2 s after it answers PING. This is the
`start-memory` line of every `*.engine.txt`: 8 starts per build.

**Hashes with field TTLs** (`harness/hfe.sh`, new in this round).
1. A fresh engine is loaded with 1M hashes `h:<i>`, each with 10 fields `f0`..`f9` of 16 B. The load is
   `HSET`, one command per hash, through `redis-cli --pipe`. A checkpoint follows.
2. `HPEXPIRE h:<i> 36000000 FIELDS 10 f0 ... f9` gives every field a TTL of 10 hours. Another checkpoint follows.
   Nothing expires during the run, and `HPTTL` of the first and last hash's fields was checked at the end of
   each run.
3. Bytes per field = (checkpoint - empty engine) / 10M fields. The TTL's own cost is stage 2 minus stage 1.

This measures the memory of the field-expiry bookkeeping that #531 touched. It does not measure the expiry itself:
no field expired during the run, and there was no replica.

## Results

### Growth, 100 B random values (3 rounds each)

RSS bytes per key (used_memory bytes per key in brackets):

| build | 1M | 5M | 10M | 25M | 50M | RSS at 50M | load s, 50M keys |
|---|---|---|---|---|---|---|---|
| 1.2.1 (878746e4) | 154.0 (146.9) | 146.4 (143.1) | **145.0** (143.1) | 141.0 (140.1) | **140.7** (140.1) | 6.57 GiB | 12.9 |
| 1.2.0 (release binary) | 153.4 (146.9) | 146.3 (143.1) | **144.7** (143.1) | 140.9 (140.1) | **140.6** (140.1) | 6.57 GiB | 12.9 |
| *1.2.0 suite, published (other session)* | *153.8 (146.9)* | *146.4 (143.1)* | *145.1 (143.1)* | *141.1 (140.1)* | *140.7 (140.1)* | *6.57 GiB* | *13.0* |

- The largest RSS differences, 1.2.1 against 1.2.0:
  - 1M keys: +0.6 B/key (+0.4%). Ranges: 1.2.1 153.1-154.0, 1.2.0 153.3-153.4.
  - 10M keys: +0.3 B/key (+0.2%). Ranges: 1.2.1 144.8-145.0, 1.2.0 144.7-144.8.
  - The ranges touch or overlap. At 1M keys a 0.6 B/key RSS difference is 0.6 MB in total: a few pages of the
    allocator's arenas.
- `used_memory` per key is the same to 0.01 B in every cell and every round.
- Both builds repeat the published 1.2.0 numbers of the 1.2.0 suite to within 0.4 B/key.

### Growth, 1 KB random values (2 rounds each)

| build | 1M | 5M | 10M | 25M | 50M | RSS at 50M | load s, 50M keys |
|---|---|---|---|---|---|---|---|
| 1.2.1 (878746e4) | 1,085.4 (1,074.9) | 1,077.7 (1,071.1) | 1,076.0 (1,071.1) | 1,072.0 (1,068.1) | **1,071.6** (1,068.1) | 49.92 GiB | 22.9 |
| 1.2.0 (release binary) | 1,085.3 (1,074.9) | 1,077.4 (1,071.1) | 1,076.1 (1,071.1) | 1,072.0 (1,068.1) | **1,071.7** (1,068.1) | 49.92 GiB | 22.9 |

Level in every cell: RSS within 0.3 B/key, `used_memory` identical.

### warefeats/kv-stores, 1M x 100 B, then a second million

| build | runs | ASCII 1st M RSS (used) | ASCII 2nd M RSS (used) | random 1st M RSS (used) | random 2nd M RSS (used) |
|---|---|---|---|---|---|
| 1.2.1 (878746e4) | 3 / 2 | **149.0** (146.9) | 147.0 (146.9) | **149.0** (146.9) | 151.8 (146.9) |
| 1.2.0 (release binary) | 3 / 2 | **149.0** (146.9) | 146.9 (146.9) | **148.9** (146.9) | 152.3 (146.9) |

- The random second million is 146.9-148.0 or 156.7 B/key RSS in single runs of both builds, for 146.9 used. It
  is the same one-off RSS step that the 1.2.0 suite noted on this cell, and it hit one run of each build here.
- `used_memory` is 146.9 B/key in all 20 cells.

### Hashes with field TTLs: 1M hashes x 10 fields x 16 B (3 rounds each)

| build | used B/field, no TTL | used B/field, TTL on every field | used B per field TTL | RSS B/field, no TTL | RSS B/field, with TTL | RSS B per field TTL |
|---|---|---|---|---|---|---|
| 1.2.1 (878746e4) | 135.1 | 223.4 | 88.3 | 186.0 | **319.1** | 133.1 |
| 1.2.0 (release binary) | 135.1 | 223.4 | 88.3 | 186.0 | **321.2** | 135.2 |

- **`used_memory`.**
  - After `HPEXPIRE` it was 2,237,325,882 B for 1.2.1 against 2,237,210,944 B for 1.2.0 (median round).
  - That is 115 KB more over 10M field TTLs: 0.01 B per field.
  - Without TTLs the two are equal to the 0.1 MB the sampler shows.
- **RSS with TTLs.** 1.2.1's is 20 MB (0.65%) lower, in all three rounds of each build (3,061 against 3,081 MB).
  `used_memory` is the same, so this is how the allocator's pages are filled, not less data. It is in 1.2.1's
  favour.
- **Context, not a 1.2.1 change.** A field TTL costs 88 B of `used_memory` here, against 135 B per field for the
  hash without TTLs (that figure includes the field name, the 16 B value and the hash's own entry spread over its
  10 fields). Both builds pay the same.

### Idle engine

| build | starts | used_memory | used_memory_rss |
|---|---|---|---|
| 1.2.1 (878746e4) | 8 | 2,889,415 B (2,888,435-2,889,543) | 17.2 MB |
| 1.2.0 (release binary) | 8 | 2,887,322 B (2,886,522-2,887,630) | 17.1 MB |

1.2.1 holds 2 KB more at start (+0.07%). It is a constant, not per key: the growth checkpoints, which subtract
each run's own idle value, have identical `used_memory` per key. What the 2 KB is was not investigated; 1.2.1
adds commands and startup state (for example the Valkey 9 RDB support), which is the likely source.

## Where 1.2.1 loses, and caveats

- **Nothing in 1.2.1 is worse than 1.2.0 beyond noise.**
  - The largest differences against 1.2.1 are +0.6 B/key RSS at 1M x 100 B and +0.3 B/key at 10M. `used_memory` is
    identical in both cells, and the round ranges touch or overlap.
  - The idle engine holds 2 KB more.
- **Dragonfly is lower per key in places.** This is context from the 1.2.0 suite's numbers (2026-11-02, Dragonfly
  2.0.2, pair A in germanywestcentral), **not re-measured here**. 1.2.1's bytes per key equal 1.2.0's, so it stands
  where 1.2.0 stood:
  - **warefeats ASCII values, 1M x 100 B:** Dragonfly 137.1 B/key RSS against KiviDB 149.2. Dragonfly packs ASCII
    strings. With random bytes Dragonfly is 153.1 against KiviDB 149.2.
  - **Growth, 100 B random:** at 1M-10M keys the two trade places within ~10 B/key as their hash tables grow. At
    10M, KiviDB 145.1 and Dragonfly 155.6. At 50M, KiviDB 140.7 and Dragonfly 141.5, level.
  - **Growth, 1 KB at 50M:** Dragonfly 1,071.2 against KiviDB 1,071.7, level.
  - In warefeats' own published run (their rig, 1.1.2 era), Dragonfly measured 118 B/key with ASCII values.
  - Redis 8.10.3 (174.4 at 50M x 100 B) and Valkey 9.1.3 (166.6) used more than KiviDB in that run.
- **Client VM.** 16 vCPUs instead of the 1.2.0 rounds' 32 (see Hardware). This does not affect bytes per key. Load
  times are comparable within this session; they also happened to match the 1.2.0 round.
- **Region.** koreacentral, as the 1.2.0 suite's last round. The first 1.2.0 round ran in germanywestcentral. The
  memory numbers repeat across both, as the table above shows.
- **Fewer scenarios than the 1.2.0 suite.** Not run in this round: the mixed workload, snapshots, #284 eviction
  during a snapshot, active defrag, AOF-on writes, and warefeats' throughput rows. 1.2.1 changes none of the paths
  they measure for memory, apart from the AOF sync under `appendfsync always` (#514/#530), which the persistence
  checks cover.
- **The hash field TTL test** measures the memory of keeping field TTLs, not their expiry. With no replica and no
  field reaching its time, the propagation 1.2.1 added (#531: the master removes a due field and sends `HDEL`) did
  not run.
- **One aborted start.** The first attempt of round 1 (1.2.0, growth 100 B) was stopped during its last stage, to
  fix a harness line, and run again from a fresh engine. The engine VM's sampler appended the new run to the same
  CSV, so the aborted run's rows were cut from `results/raw/mem/growth-kividb-1.2.0-100-random-r1.csv` (rows
  before the new run's `start` event). The checkpoints were not affected: they are taken from the new run's
  samples.

## Files

- `harness/`: every script used (see `harness/README.md`). `queue.txt` is the exact job list.
- `results/tables.md`: every table, generated by `harness/analyze121.py`.
- `results/raw/engine-vm.txt`: builds, SHA-256 of the tarball and of both binaries, toolchain, CPU, kernel and
  memtier version.
- `results/raw/mem/`:
  - sampler CSVs (one row per second), `checkpoints.csv`, `hfe-checkpoints.csv`, `events.csv`;
  - memtier and `redis-cli --pipe` outputs;
  - each run's `*.engine.txt`: engine start line, backlog, idle memory, and for `hfe` the final `HPTTL` and
    `INFO memory`;
  - `queue.log`.

  Private VNet addresses are replaced by `engine-a` and `client-a`.
- `results/raw/wf/`: warefeats runner JSON per build and run, and the engines' startup logs.
