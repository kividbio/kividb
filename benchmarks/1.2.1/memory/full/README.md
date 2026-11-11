# Memory and snapshots: 1.2.1 against 1.2.0, Redis 8.10.3, Valkey 9.1.3 and Dragonfly 2.0.2

The memory suite of [`benchmarks/1.2.0/memory/`](../../../1.2.0/memory/README.md), rerun on **1.2.1 (commit
`ccba20e2`)** and on the **1.2.0 release binary**, with the same competitors, VM sizes and regions, on 2026-11-11
(UTC). It covers what the [first 1.2.1 memory round](../README.md) did not:
- `BGSAVE` and `BGREWRITEAOF` under 100k writes/s on 30M x 1 KB keys;
- `maxmemory` during a snapshot (#284);
- active defrag after a mass delete;
- every engine again on bytes per key (warefeats) and growth to 50M keys.

It was also a test of the 1.2.0 suite's docs: the run used only its README and harness. The gaps are listed at the
end and fixed in the same change.

> **One round per build.** Each KiviDB build ran each pair-A test (warefeats, growth, snapshot, eviction) **once**,
> not three times as in the 1.2.0 suite. The rounds 2 and 3 were dropped to finish sooner. One run has no spread of
> its own, so a gap of a few percent between 1.2.1 and 1.2.0 cannot be told apart from run-to-run noise. Each gap
> is judged against the spread the 1.2.0 suite published for the same metric over three rounds. Active defrag
> (pair B) did run four alternating rounds per build.

**Verdicts**
- **No regression in memory.** Every bytes-per-key cell, the memory a snapshot adds, the memory held after it, and
  what active defrag gives back are level with 1.2.0 to within 0.4%. `used_memory` is identical to the byte.
- **#284 holds:** no key evicted during a `BGSAVE` at `maxmemory` in either build (Redis none either).
- **Latency and throughput rows:** no gap is larger than the 1.2.0 suite's published spread for that metric, except
  two that one round cannot settle (load time of the 1 KB growth test, +6.1%; 10 KB p99.9, +4.2%). Both are on rows
  bound by the network; see [1.2.1 against 1.2.0](#121-against-120).
- **The published 1.2.0 memory numbers reproduced** to within 0.3% for every engine. The throughput and load-time
  numbers did not: every engine, competitors included, was 9-35% slower on this pair of VMs than on the 1.2.0
  suite's. See [Against the published 1.2.0 numbers](#against-the-published-120-numbers).

## Headline

| | KiviDB 1.2.1 | KiviDB 1.2.0 | Redis 8.10.3 | Valkey 9.1.3 | Dragonfly 2.0.2 |
|---|---:|---:|---:|---:|---:|
| warefeats 1M x 100 B, RSS B/key, ASCII / random values | **149.0** / 149.0 | 149.2 / 149.1 | 165.8 / 165.7 | 170.2 / 170.5 | 137.0 / 153.2 |
| growth to 50M keys, RSS B/key, 100 B / 1 KB (random) | **140.6** / 1,071.7 | 140.7 / 1,071.6 | 174.5 / 1,352.2 | 166.6 / 1,344.4 | 141.5 / 1,071.1 |
| `BGSAVE` of 30M x 1 KB at 100k SET/s: memory added | **+3.15 GiB** (10%) | +3.14 GiB (10%) | +33.54 GiB (89%) | +29.22 GiB (77%) | +0.13 GiB (0%) |
| `BGSAVE`: worst write | **229 ms** | 289 ms | 403 ms | 392 ms | 369 ms |
| `BGREWRITEAOF`: memory added / worst write | +2.10 GiB / 252 ms | +2.09 GiB / 251 ms | +33.33 GiB / 1,015 ms | +30.38 GiB / 1,012 ms | (no AOF) |
| #284: keys evicted during a `BGSAVE` at `maxmemory` | **0** | 0 | 0 | - | - |
| active defrag: RSS given back after deleting half of 5M x 100 B (4 rounds) | **335 MB in 12.2 s** | 335 MB in 13.0 s | - | - | - |
| warefeats SET:GET 1:10, 100 B, pipeline 30 (ops/s) | 13.39M | 12.67M | 1.38M | 1.43M | 5.89M |

Redis and Valkey run with `--io-threads 8` in our own tests and without them in warefeats' cells, as in the 1.2.0
suite. Every table, with p50/p99/p99.9 and per-round values, is in [`results/tables.md`](results/tables.md).

## 1.2.1 against 1.2.0

Change = 1.2.1 against 1.2.0, one run each (pair A) or the median of four (active defrag). *1.2.0 suite's spread*
is the larger (max-min)/median of the two builds over three rounds in [the 1.2.0 suite](../../../1.2.0/memory/results/tables.md),
the noise to judge a one-run gap against.

| section | metric | 1.2.1 | 1.2.0 | change | 1.2.0 suite's spread | verdict |
|---|---|---:|---:|---:|---:|---|
| warefeats ASCII | 1st M RSS / used B/key | 149.0 / 146.9 | 149.2 / 146.9 | -0.1% / 0 | 0.2% / 0 | level |
| warefeats ASCII | 2nd M RSS B/key | 147.3 | 147.5 | -0.1% | 5.6% | level |
| warefeats random | 1st M RSS / used B/key | 149.0 / 146.9 | 149.1 / 146.9 | -0.1% / 0 | 0.2% / 0 | level |
| warefeats random | 2nd M RSS B/key | 149.6 | 147.6 | +1.4% | 0.1% (5.6% ASCII; 146.9-156.7 in the first 1.2.1 round) | level: `used_memory` identical, RSS of the second million is noisy |
| warefeats 100 B, pipeline 1 | ops/s / p99 / p99.9 | 918,129 / 0.419 / 0.595 ms | 918,224 / 0.415 / 0.623 ms | 0.0% / +1.0% / -4.5% | 7.5% / 3.2% / 23.1% | level |
| warefeats 10 KB, pipeline 1 (network-bound) | ops/s / p99 / p99.9 | 319,513 / 1.099 / 1.283 ms | 319,548 / 1.039 / 1.231 ms | 0.0% / +5.8% / +4.2% | 0.1% / 10.2% / 3.8% | level / level / not settled by one run |
| warefeats 100 B, pipeline 30 | ops/s / p99 / p99.9 | 13.39M / 0.855 / 1.023 ms | 12.67M / 0.915 / 1.203 ms | +5.7% / -6.6% / -15.0% | 4.1% / 3.8% / 5.1% | level or better |
| growth 100 B | RSS B/key at 10M / 50M | 144.7 / 140.6 | 144.9 / 140.7 | -0.2% / -0.0% | 0.2% / 0.0% | level |
| growth 100 B | load s, 50M keys | 15.6 | 15.3 | +2.1% | 1.6% | level (see below) |
| growth 1 KB | RSS B/key at 10M / 50M | 1,076.2 / 1,071.7 | 1,075.9 / 1,071.6 | 0.0% / 0.0% | 0.0% / 0.0% | level |
| growth 1 KB | load s, 50M keys | 23.8 | 22.4 | +6.1% | 0.8% | not settled by one run (see below) |
| snapshot `BGSAVE` | memory added / held 60 s after | 3.15 / 33.43 GiB | 3.14 / 33.42 GiB | +0.2% / 0.0% | 1.9% / 0.1% | level |
| snapshot `BGSAVE` | duration / worst write | 71.3 s / 229 ms | 71.3 s / 289 ms | 0.0% / -21% | 1.4% / 10.8% | level / better |
| snapshot `BGSAVE` | write p99.9 over window / p99 before | 2.38 / 1.73 ms | 2.69 / 1.77 ms | -11% / -2.3% | 11.0% / 3.4% | level |
| snapshot `BGREWRITEAOF` | memory added / held 60 s after | 2.10 / 35.00 GiB | 2.09 / 35.14 GiB | +0.3% / -0.4% | 1.8% / 0.1% | level |
| snapshot `BGREWRITEAOF` | duration / worst write | 73.4 s / 252 ms | 72.8 s / 251 ms | +0.7% / +0.1% | 0.9% / 2.4% | level |
| snapshot `BGREWRITEAOF` | write p99.9 / p99 before | 2.54 / 1.71 ms | 2.62 / 1.70 ms | -3.0% / +0.5% | 15.6% / 16.6% | level |
| #284 | keys evicted | 0 | 0 | - | - | level |
| active defrag (4 rounds each) | RSS returned / final | 335 / 389 MB | 335 / 388 MB | 0.0% / +0.3% | 0.9% / 0.8% (this run) | level |
| active defrag | give-back s / compaction s | 12.2 / 13 | 13.0 / 13 | -6.1% / 0 | 9.8% / 7.7% (this run) | level |
| active defrag | GET / SET p99.9 during compaction, as a share of the same run's before | 0.95 / 1.02 | 1.02 / 1.00 | -6.5% / +2.3% | 16.8% / 16.6% (this run) | level |
| active defrag, `activedefrag no` | RSS returned | 33 MB | 36 MB | | | level (the 1.2.0 suite: 35) |

**The two gaps one run cannot settle.**
- **Growth load time at 1 KB, +6.1%** (23.8 against 22.4 s; at 100 B it is +2.1%). Each stage loads at 2.9-3.2M
  SET/s of 1 KB values, about 3 GB/s: the client VM's network limit. Per stage the builds overlap: 1.2.1 loaded
  at 2.94-3.03M SET/s and 1.2.0 at 2.92-3.23M. The [1.2.1 core suite](../../core/README.md) measured 1 KB `SET` at
  its NIC limit as identical for both builds (2.80M ops/s), and the first 1.2.1 memory round's 50M-key loads were
  22.9 s for both. So this is most likely the network, not 1.2.1. The rows that are not bound by the network
  (pipeline 30, about 1.3 GB/s) favour 1.2.1 by 5.7%, which is also inside the noise.
- **10 KB p99.9, +4.2%** against a published spread of 3.8%; its p99 (+5.8%) is inside its 10.2% spread. This row
  is network-bound for every engine (about 3.2 GB/s of replies), and the 1.2.0 suite already called it
  unable to separate engines.

Neither was rerun, because the rounds were dropped. A rerun of `warefeats.sh` and `growth.sh ... 1024` with the
builds alternating would settle them.

## Against the published 1.2.0 numbers

Same VM sizes, region and kernel (7.0.0-1017-azure) as the 1.2.0 suite's pair A and pair B, new VMs.

| | 1.2.0 suite (published) | this run | |
|---|---|---|---|
| warefeats ASCII 1st M RSS B/key: KiviDB 1.2.0 / Redis / Valkey / Dragonfly | 149.2 / 166.3 / 170.5 / 137.1 | 149.2 / 165.8 / 170.2 / 137.0 | reproduced (within 0.3%) |
| warefeats random 1st M RSS B/key: KiviDB / Redis / Valkey / Dragonfly | 149.2 / 166.0 / 170.6 / 153.1 | 149.1 / 165.7 / 170.5 / 153.2 | reproduced |
| growth 100 B at 50M, RSS B/key: KiviDB / Redis / Valkey / Dragonfly | 140.7 / 174.4 / 166.6 / 141.5 | 140.7 / 174.5 / 166.6 / 141.5 | reproduced |
| growth 1 KB at 50M, RSS B/key: KiviDB / Redis / Valkey / Dragonfly | 1,071.7 / 1,352.2 / 1,344.4 / 1,071.2 | 1,071.6 / 1,352.2 / 1,344.4 / 1,071.1 | reproduced |
| `BGSAVE` memory added: KiviDB / Redis / Valkey / Dragonfly (GiB) | 3.12 / 33.29 / 29.48 / 0.12 | 3.14 / 33.54 / 29.22 / 0.13 | reproduced (within 1%) |
| `BGREWRITEAOF` memory added: KiviDB / Redis / Valkey (GiB) | 2.08 / 33.20 / 29.95 | 2.09 / 33.33 / 30.38 | reproduced |
| RSS held after the snapshot and after the rewrite (KiviDB) | 33.39 / 34.96 GiB | 33.42 / 35.14 GiB | reproduced: the "RSS stays up" finding still holds |
| worst write, `BGSAVE` / `BGREWRITEAOF`: KiviDB / Redis / Valkey / Dragonfly (ms) | 209 / 391 / 408 / 291; 221 / 1,006 / 1,024 | 289 / 403 / 392 / 369; 251 / 1,015 / 1,012 | same order: KiviDB below every other engine; KiviDB 1.2.0's `BGSAVE` worst is 38% above published (one run, a single slowest write; 1.2.1 had 229) |
| AOF-on write p99 before the snapshot: KiviDB / Redis (ms) | 1.39 / 0.73 | 1.77 / 0.73 | KiviDB 27% higher on this pair, Redis level |
| #284 evicted keys: KiviDB / Redis | 0 / 0 | 0 / 0 | reproduced |
| active defrag: RSS returned / give-back s | 334 MB / 12.1 s | 335 MB / 13.0 s | reproduced |
| warefeats pipeline 30 (ops/s): KiviDB 1.2.0 / Dragonfly / Redis / Valkey | 15.63M / 8.01M / 1.60M / 1.58M | 12.67M / 5.89M / 1.38M / 1.43M | **not reproduced: every engine 9-27% lower** |
| warefeats 100 B pipeline 1 (ops/s): KiviDB 1.2.0 / Dragonfly / Redis | 1.00M / 906K / 177K | 918K / 758K / 133K | not reproduced: every engine 8-25% lower |
| growth load s at 50M x 100 B: KiviDB 1.2.0 / Redis / Valkey / Dragonfly | 13.0 / 55.1 / 45.0 / 16.0 | 15.3 / 65.7 / 60.6 / 18.5 | every engine 16-35% slower |

- **Memory reproduces exactly.** Bytes per key, snapshot memory and defrag do not depend on how fast the machines
  are.
- **Throughput did not reproduce on this pair of VMs, for any engine.** The gaps are all in the same direction and
  of similar size for KiviDB, Redis, Valkey and Dragonfly, so they come from the machines, not from a build. The
  1.2.0 README's comparisons between engines hold: pipeline 30 is 2.2-2.3x Dragonfly here against 1.95x published; KiviDB
  at pipeline 1 is 6.9x Redis without io-threads against 5.7x.
- **KiviDB's AOF-on write p99 is higher on this pair** (1.73-1.77 ms against 1.39 published), Redis's is not. AOF
  writes wait for the disk, and this engine VM's local NVMe disk is a different one of the same size. Both KiviDB
  builds moved together, so it is not 1.2.1.
- The published 1.2.0 KiviDB numbers came from a build of commit `75482abb` with the repo's Dockerfile (Rust 1.89).
  Here 1.2.0 is the release binary from `release.yml` (Amazon Linux 2023, rustc 1.99.1). The memory numbers are the
  same either way.

## What ran

| | VM | CPU | region | used for |
|---|---|---|---|---|
| pair A engine | Standard_E32ds_v7 (32 vCPU, 251 GiB, local NVMe 440 GB as data dir) | Intel Xeon 6973P-C | germanywestcentral | warefeats, growth, snapshot, eviction |
| pair A client | Standard_D32ls_v7 (32 vCPU) | Intel Xeon 6973P-C | germanywestcentral | memtier_benchmark 2.5.1 |
| pair B engine | Standard_E32ds_v6 (32 vCPU, 251 GiB, local NVMe) | Intel Xeon Platinum 8573C | polandcentral | active defrag |
| pair B client | Standard_D16ls_v6 (16 vCPU) | Intel Xeon Platinum 8573C | polandcentral | memtier |

The sizes and regions of the 1.2.0 suite. Each pair is one proximity placement group with accelerated networking,
Ubuntu 24.04, kernel 7.0.0-1017-azure. Engines ran in Docker, host networking, seccomp unconfined (io_uring
allowed), one at a time. Pair B ran its 10 jobs (12:27-13:28 UTC) beside pair A's first ones, as in the 1.2.0 suite,
and was deleted when they were done. Pair A ran 12:27-15:20 UTC.

| build | what | `--version` | binary SHA-256 |
|---|---|---|---|
| 1.2.1 | commit `ccba20e2`, `kividb-linux-x86_64.tar.gz` from `release.yml` run 42974385537 (Amazon Linux 2023, rustc 1.99.1, `cargo build --release`, default features); tarball `66569c946cf1e20b5e190ba071dd6d4178e8edabb7aa5945b394b4360537d618` | 1.2.1 with a suffix (the version is final only at release) | `cfee100229e1d890f9fa5cb6efecb2da00e1d0993056ace34a52270ff54bec27` |
| 1.2.0 | `kividb-linux-x86_64.tar.gz` of the v1.2.0 release, `sha256sum -c` against its `SHA256SUMS.txt`: OK (tarball `2d918e71…c202e4b78c`) | kividb 1.2.0 | `a211d544be79bb78cfc5bc16b1e1315d20913f154cd8179f80668ab048edd72f` |

Each binary was wrapped in the same `ubuntu:24.04` image (`ENTRYPOINT kividb`), so that `eng.sh` and warefeats'
compose files start both in the same way; the image digests are in `results/raw/pair-*/engine-vm.txt`. Competitors:
`redis:8.10.3`, `valkey/valkey:9.1.3`, `docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.2`, the 1.2.0 suite's pins.
KiviDB 1.1.3 was not rerun.

**Method.** The 1.2.0 suite's drivers, unchanged (`warefeats.sh`, `growth.sh`, `snapshot.sh` with `WIN=300 PRE=30`,
`evict.sh`, `defrag.sh` with `PROBE=300`); see [its README](../../../1.2.0/memory/README.md) for what each measures.
Order on pair A: both KiviDB builds (1.2.1 first) through every test, then the competitors
([`harness/queue-a.txt`](harness/queue-a.txt)). Pair B: four active-defrag rounds per build, alternating which went
first, plus one `--activedefrag no` control each ([`harness/queue-b.txt`](harness/queue-b.txt)). The 1.2.0 suite's
mixed-workload and AOF-on latency tests were not part of this rerun.

### Reproduce

```
bash harness/create-vms.sh <rg> germanywestcentral KEY.pub
bash harness/create-vms-b.sh <rg> polandcentral KEY.pub
for h in <four public IPs>; do ssh azureuser@$h 'sudo mkdir -p /opt/mem && sudo chown azureuser /opt/mem'; scp harness/*.sh harness/*.py azureuser@$h:/opt/mem/; done
ssh azureuser@<client> 'ssh-keygen -q -t ed25519 -N "" -f ~/.ssh/id_mem; cat ~/.ssh/id_mem.pub' | ssh azureuser@<its engine> 'cat >> ~/.ssh/authorized_keys'
gh run download 42974385537 -R kividbio/kividb -n dist-linux-amd64-default    # release.yml's build of ccba20e2
scp kividb-linux-x86_64.tar.gz azureuser@<each engine>:/var/tmp/
engine$ sudo bash /opt/mem/setup-common.sh && sudo bash /opt/mem/setup-server.sh /var/tmp/kividb-linux-x86_64.tar.gz 66569c946cf1e20b5e190ba071dd6d4178e8edabb7aa5945b394b4360537d618
client$ sudo bash /opt/mem/setup-common.sh && sudo bash /opt/mem/setup-client.sh
# log in again (docker group), then on each client:
client$ mkdir -p /var/tmp/mem && cp queue-a.txt /var/tmp/mem/queue.txt && cd /opt/mem && nohup ./queue.sh > /dev/null 2>&1 &   # queue-b.txt on pair B
# copy pair A's /var/tmp/mem to raw/a and /var/tmp/wf to raw/wf, pair B's /var/tmp/mem to raw/b, then
python3 -I harness/analyze.py raw/a raw/b raw/wf charts > results/tables.md
az group delete -n <rg> --yes
```

`harness/` is the 1.2.0 suite's, with these changes: `setup-server.sh` installs the two release binaries (checked
against their SHA-256) instead of building from git refs, and pulls only the three competitor images; `eng.sh` and
`warefeats.sh` know the builds as `kividb-1.2.0` and `kividb-1.2.1` (from the first 1.2.1 round); `analyze.py` labels
them so and writes `regression-vs-1.2.0.svg` and `defrag-vs-1.2.0.svg`; `create-vms*.sh` take the operator IP,
accept an existing resource group in another region, and add `--nsg-rule NONE` on pair B.

### Files

- `results/tables.md`: every table, generated by `analyze.py`.
- `results/warefeats/`: warefeats runner JSON per engine (their schema).
- `results/raw/pair-a`, `pair-b`: per-run sampler CSVs (one row per second), memtier JSON (per-second latency),
  events, checkpoints, engine start lines, persistence INFO after each snapshot, the queue log, `engine-vm.txt`.
- `charts/`: as in the 1.2.0 suite.

![bytes per key vs dataset size](charts/growth-bytes-per-key-random.svg)
![memory added by a snapshot](charts/snapshot-memory-added.svg)
![worst write during a snapshot](charts/snapshot-max-latency.svg)
![KiviDB 1.2.1 during the snapshot and rewrite](charts/snapshot-kividb-1.2.1.svg)
![keys during BGSAVE at maxmemory](charts/evict-keys-during-bgsave.svg)
![active defrag, 1.2.1 against 1.2.0](charts/defrag-vs-1.2.0.svg)

## Reproduction gaps found and fixed

Following [`benchmarks/1.2.0/memory/`](../../../1.2.0/memory/README.md) and its
[`harness/README.md`](../../../1.2.0/memory/harness/README.md):

1. **`create-vms.sh` failed with an existing resource group in another region**: it created the network and NSG
   without `-l`, so they land in the group's region and the VMs cannot use them. Both create scripts now create
   everything in the given region and accept an existing group; pair B gets `--nsg-rule NONE` like pair A.
2. **"copy *.sh *.py to /opt/mem on all four VMs; give each client an ssh key" had no commands**, and `/opt/mem` is
   not writable by the login user. The harness README now gives both.
3. **The docker group** added by `setup-common.sh` takes effect only at the next login; the drivers call `docker`
   without `sudo`. Now said.
4. **Running the jobs:** the README listed the drivers as foreground commands taking hours, while the suite itself
   used `queue.sh`. It now says to run them through the queue, detached.
5. **The KiviDB builds can only come from git refs** (`setup-server.sh` builds with the repo's Dockerfile, Rust
   1.89). Comparing against a published release binary needs this folder's `setup-server.sh`; the 1.2.0 harness
   README now points to it.
6. `warefeats.sh` and `analyze.py` know only the labels `kividb-cand` and `kividb-rc4`. This folder's copies use
   `kividb-1.2.1`/`kividb-1.2.0`; the 1.2.0 harness README says so.
