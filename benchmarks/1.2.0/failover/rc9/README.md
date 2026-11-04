# KiviDB 1.2.0-rc9: targeted retest of the replication fixes (2026-11-03)

**Build under test:** commit `4a47644e53b8cf261df10309d919f2c19ebadef0` on `release/1.2.0` ("Version 1.2.0-rc9"),
reports `kividb 1.2.0-rc9`. Its `src/` tree is byte-identical to the rc9 fixes branch the fixes report measured
(`git rev-parse 4a47644e:src` = `87b26f54…`); only the version in `Cargo.toml` and docs/benchmark files differ.

## Verdict

| # | Check | Result | Numbers |
|---|---|---|---|
| 1 | Clean master restart under 100k ops/s, `repl-backlog-size 128mb`, 10× | **PASS** | **10 / 10 partial resyncs**, 0 full; exit 1.30-2.53 s after SIGTERM; replicas had the whole stream 10-67 ms after it; link back 5.1 s every time |
| 1 | Same, default backlog (1 MB), 3× | (report) | **0 / 3 partial: a full sync every time**, link back 11.3-11.8 s. The master logs why: the replica's offset was 0.28-27 MB older than the backlog's start |
| 2 | Forced full sync, 3.4 M keys, 100k ops/s | **PASS** | **converged on the first sync** (1 full sync after the wipe, 0 evictions after it), synced 13.5 s after the restart; **peak queue 317,294,083 bytes** (logged; ~310 MiB sampled); **master RSS 2.27 → 3.26 GiB** |
| 3 | 10 × `CLUSTER FAILOVER` under writes with expiring keys | **PASS** | **10 / 10 partial** ("Partial resync: continuing from offset N" at the offset the demoted node recorded), roles swapped in 0.1-0.2 s |
| 4 | kill -9 of the master during the replica's full sync | **PASS** | 2 / 2 phases: the replica **did not take over** (log: "Not standing for election … has not finished loading its last full sync"), `cluster_state:fail` for A's 5,461 slots until the master returned; **ackcheck 0 lost / 0 wrong** of 8,904 and 9,024 acknowledged writes once the master was back, and again after the replica re-synced |
| 4 | then kill -9 of the master with a caught-up replica | **PASS** | **promoted after 6.62 s** ("Won election at epoch 4"); 6.28 s in the first run |
| 5 | SIGTERM a replica mid full sync | **PASS** | **4 / 4 rounds** (SIGTERM 0.2, 3, 5 and 8 s into the sync, 3.4 M keys, 100k ops/s): `dump.kdb` and `dump.kdb.repl` **byte-identical** to the good ones, "the final save is skipped" logged, exit 0.4-1.7 s, the next start full-synced and caught up in 19-23 s; offsets and DBSIZE equal at the end. The good dump had to be made with the clients stopped (next row) |
| 5 | (found on the way) SIGTERM of a **streaming** replica under writes | **FAIL: a deadlock, also in rc8 (not a regression)** | rc9: **4 of 13** clean stops under load **never exit** (3 / 10 and 1 / 3), plus 3 of the 7 clean stops in item 5's runs (two of them with the clients stopped); rc8: **6 of 13** (4 / 10, 2 / 3). The control stop with the load stopped exited every time (4 / 4). Stacks: the shutdown holds the command gate alone and waits for `apply_lock`; the replica's applier holds `apply_lock` and waits for the gate |
| 6 | 30-min standalone soak, master + replica, AOF on, 100k ops/s, master SIGTERM at 10 min, replica kill -9 at 20 min | **PASS** | write log **1,415,323 acknowledged, 0 bad on master and on replica**; ackcheck **1,415,191 acknowledged, 0 lost, 0 wrong** on both; compare **0 real differences** (3,159,757 keys, 170 differ on the first pass, 0 after the recheck); offsets equal |
| 7 | Core benchmark, rc9 vs rc8 (arm64, 2 rounds each, alternating) | **PASS** | GET p1 −0.7 % (client-bound), GET p16 +0.4 %, SET p16 +1.6 %, MSET-10 −0.3 %: all level or faster |
| 7 | Callgrind, rc9 vs rc8, ±0.5 % | **FAIL as measured (+0.04 … +1.22 %); PASS without glibc `memcmp`** | GET-100 +0.57 %, SET-100 +1.22 %, MIX-100 +0.92 %, INCR +0.05 %, DEL +0.56 %. Every function in KiviDB counts the same; all of the difference is in glibc's `__memcmp_avx2_movbe`, called the same number of times from the same places, 6 instructions dearer per call because the string literals it compares moved within their pages (see 7). Without `memcmp`: −0.00 … +0.03 % |

**Everything rc9 fixed held.** Two things did not pass:
1. **New finding, not from rc9: a replica's clean shutdown deadlocks while it is applying its master's stream**
   (details in "Replica shutdown deadlock" below). It is in rc8 as well, at about the same rate, so it does not block
   rc9 against rc8, but a SIGTERM'd replica (a Kubernetes pod stop, a rolling restart) hangs until it is killed, and
   then has no final save. It should be filed. Nothing was fixed here.
2. **Callgrind is +0.5-1.2 % on four workloads**, entirely inside glibc's `memcmp`, from where the linker put string
   literals; KiviDB's own code counts the same (section 7). The core benchmark on real cores shows no cost.

## Set-up

- **Azure, RG `kividb-rc9-retest`** (deleted at the end). westus2 holds 65 vCPUs per region, so:
  - **westus2**: `b1` D16s_v5 (builds, callgrind); **set A** `t1`, `t2` D8ds_v5 (master / replica, data on the local
    NVMe disk) + `tc` D8s_v5 (client, and the third cluster host), one PPG: items 1-5; **set B** `sm`, `sr` D8ds_v5 +
    `sc` D8s_v5, one PPG: item 6 and the shutdown repro. `b2` D16s_v5 rebuilt rc8 for the repro after `b1` was deleted.
  - **canadacentral** (the fallback): `srv` D32ps_v5 (Ampere Altra, Neoverse-N1, kernel 7.0.0-1017-azure) + `cl1`, `cl2`
    D16ps_v5, one PPG: the core benchmark, the shapes of the rc8 core round.
  - Ubuntu 24.04, accelerated networking, SSH from one IP only, auto-shutdown on every VM.
- **Builds** (`results/builds/`), all rustc **1.99.1 (58cf8f9c3 2026-10-13)**, rustup stable:

| Where | Build | How | SHA-256 |
|---|---|---|---|
| x86-64 (b1) | rc9 `4a47644e` | `harness/build.sh`: release.yml's linux-amd64 way (amazonlinux:2023 container, `cargo build --release`), 339 s | `3a869b46274260edff341681462c8f97cd60a090ea18b570bdb62fa6451ef824` (same on all six test VMs) |
| x86-64 (b1, b2) | rc8 `082dab3d` | the same, 335 s; rebuilt on b2 | `684e71afb1192a3c9f34ea74c603c32e994987701729d9c013835cc5dd75c80a` both times, and the same as the rc8 soak's build |
| arm64 (srv) | rc9 | `fetch-kividb.sh` (`cargo build --release`, `RUSTFLAGS=-C target-feature=+lse`, as release.yml builds arm64) | `02282dfc30d81600bd9f68e6757d2da5fb5d9ccedd35bf467d5a602d49e4fb46` |
| arm64 (srv) | rc8 | the same | `ef2208fd547bb06348560dc1f0bce48302debc098e16de58ff37431bbbcf1260` (the rc8 core round's) |

- Every server ran in a directory of its own (`/mnt/kv`, or `/data/<port>` for cluster nodes). AOF on with
  `appendfsync everysec` everywhere; `aof-ack-after-write yes` (the default).
- Load: the rc7 soak mix (`soakgen`, open loop, 128 connections) at **100,000 ops/s**: hash fields with 10-300 s TTLs,
  `EXPIRE` on collections; item 1 adds ~10k/s `SET … PX 1500`, item 3 `SET … PX 1500` throughout.

## 1. Clean master restart (#306)

`harness/r306.sh` (the rc9 fixes run's, with `OUT=`): 1 M keys, the mix at 100k ops/s plus `SET … PX 1500`, after 60 s
of load *N* SIGTERM restarts of the master, 30 s of load between.

| Backlog | Restarts | Partial resync | Exit after SIGTERM | Master: replicas had the whole stream after | Replica link back |
|---|---|---|---|---|---|
| **128 MB** | 10 | **10 / 10** ("continuing from offset N", 4.8-32.7 MB of backlog used) | 1.30-2.53 s | 10-67 ms (10 ms in 8 of 10) | 5.1 s every time |
| **default (1 MB)** | 3 | **0 / 3** | 1.20-1.84 s | 10 ms | 11.3-11.8 s (full syncs) |

**What the default gives.** The shutdown side works with the default too: the master logged "has the whole stream"
each time. The restarted master then could not continue the replica: "Partial resync not possible …: offset
1,877,152,833 is not in the backlog, which holds offsets 1,904,635,296 to 1,905,683,871 (repl-backlog-size) — sending
the dataset" (0.28 MB, 0.75 MB and 27.5 MB short in the three rounds). At 100k ops/s 1 MB of backlog lasts a fraction
of a second, less than the replica's reconnect. This is what the rc9 fixes report found, and what the rc9 release notes
tell operators to size. (In the 30-min soak, item 6, the same SIGTERM restart *with the default backlog* did resync
partially, `sync_partial_ok:1`: it depends on how quickly the replica reconnects.)

Files: `results/item1-restart/`.

## 2. Forced full sync under load (#305)

`harness/ab_fullsync2.sh` (rc7's, unchanged): 3.4 M string keys prefilled plus the mix (3.83 M keys at the end), the
mix at 100k ops/s; after 3 min the replica is killed, its data wiped and it is started again; 4 more minutes of load.

| Full syncs after the wipe | Evictions after the wipe | Sync done | Peak queue for the replica | Master RSS (wipe → peak) |
|---|---|---|---|---|
| **1** (converged on the first try) | **0** | loaded 3,851,585 keys 12 s after the restart, "Synced" at 13.5 s, "is streaming" (queue drained) at 18 s | **317,294,083 bytes** ("its queue peaked at 317294083 bytes during the full sync", sync limit 1 GiB); ~310 MiB sampled | **2.27 → 3.26 GiB** (2.3 s after the wipe), used_memory 1.93 GB |

One eviction happened **before** the wipe, during the unthrottled prefill (07:38:27, at the 256 MB streaming limit,
"Channel replica … exceeded backlog limit"), as in the rc7 and rc9-fixes runs, where it is not counted either; the
replica re-synced 14 s later. Files: `results/item2-fullsync/`.

## 3. `CLUSTER FAILOVER` handover (item 4 of the fixes)

`harness/c4.sh` (unchanged): 3 shards + 3 replicas (t1, t2, tc), shard A with 300k keys in one slot, writes and
`SET … PX 1500` throughout, 10 × `CLUSTER FAILOVER` on A's replica (roles swap each round).

**10 / 10 partial.** In every round the demoted master logged "This node's dataset is offset N" and then "Partial
resync: continuing from offset N" with the same N (40,642,300 … 115,580,785); roles swapped in 0.1 s (0.2 s once).
Files: `results/item3-handover/`.

## 4. No election during a full sync, then a normal failover

`harness/c3.sh` (the fixes run's, **plus ackcheck**): shard A (t1:7001, replica t2:7002) holds ~3.0 M keys (0.9 GB) in
one slot; writes to A throughout. A's replica is forced into a full sync (kill -9, data deleted, `nodes.conf` kept,
restart); A's master is killed (-9) while the master sends the dataset ("transfer") or while the replica is in the sync
("load"); 25 s later the replica's role is read; then the master is started again. ackcheck (cluster mode, 8 writers ×
100 SET/s over every slot) runs for 45 s from the start of each phase, past the master's return; its acknowledged writes
are read back once the master answers, and again after the replica has synced.

| Phase | At the kill | 25 s later | Cluster | ackcheck once A's master was back | again after the replica synced |
|---|---|---|---|---|---|
| transfer | master sending (`send_bulk` seen 1.1 s after the resync), replica `master_sync_in_progress:1`, 0 keys | **still a replica**, "Not standing for election … it has not finished loading its last full sync from the master and streamed since" | `cluster_state:fail`, 5,461 slots down | **8,904 acked, 0 lost, 0 wrong** (747 unacknowledged errors while A was down) | 0 lost, 0 wrong |
| load | replica in the sync, `master_sync_in_progress:1` | **still a replica**, same reason (it held 3,130,126 keys by then) | fail, 5,461 down | **9,024 acked, 0 lost, 0 wrong** (769 errors) | 0 lost, 0 wrong |
| failover | caught-up replica | **promoted after 6.62 s** ("Won election at epoch 4 (2 votes, quorum 2)") | ok | | |

After each phase A's master came back with all its keys and the replica synced to the same count (3,061,627 and
3,168,959). The first run of the same script, without ackcheck (it was left out by mistake), gave the same roles and a
promotion after 6.28 s. Files: `results/item4-election/` (`c3-ack/` with ackcheck, `c3-first/` without).

## 5. A replica stopped mid full sync keeps its last good dump

`harness/r5.sh`: master t1 + replica t2, AOF on, `repl-backlog-size` at its default (1 MB). 3.4 M string keys
prefilled (dump.kdb 1.3-1.4 GB), the mix at 100k ops/s. Each round:
1. the clients are stopped, the offsets meet, and the replica is stopped cleanly (SIGTERM): the sha256 of its
   `dump.kdb` and `dump.kdb.repl` are the good ones;
2. the load again for 5 s (the 1 MB backlog no longer holds the replica's offset), then the replica is started: it must
   full-sync;
3. *d* s after its `master_sync_in_progress` turns 1, SIGTERM, and wait for the exit;
4. both files must be byte-identical to the good ones and the log must say the final save was skipped; then the
   replica is started again and must sync.

| Run, round | *d* | Full sync? | At the SIGTERM | Exit | `dump.kdb` | `dump.kdb.repl` | Logged | Synced again |
|---|---|---|---|---|---|---|---|---|
| r5, 1 | 0.2 s | yes (+1 `sync_full`) | `master_sync_in_progress:1`, `loading:0` | 1.66 s | **identical** | **identical** | "the final save is skipped" | 19.5 s |
| r5, 2 | 3 s | yes | sync in progress, loading | 0.38 s | **identical** | **identical** | yes | 22.5 s |
| r5b, 1 | 8 s | yes | sync in progress, loading | 0.49 s | **identical** | **identical** | yes | 19.0 s |
| r5b, 2 | 5 s | yes | sync in progress, loading | 0.48 s | **identical** | **identical** | yes | 22.5 s |

At the end of each run, with the load stopped, the offsets met and DBSIZE was equal (3,529,959 and 3,534,246).
The log line: "Shutdown: this replica is in the middle of a full sync, and its dataset is only part of the master's:
the final save is skipped, so that dump.kdb (and its resync point) stays the last good one. The next start syncs
again". In round 1 it came 1.0 s after the SIGTERM (the bounded wait for the sync's lock).

**PASS.** But the *first* step, the clean stop that makes the good dump, is where the run first hung (below): it is
taken with the clients stopped, and the third round of each run still hung there, the master's expiries (key and
field TTLs) keeping the replica's applier busy. Those rounds stopped the run (`rounds.csv`: "clean stop hung").
Files: `results/item5-midsync/`.

## Replica shutdown deadlock (found during item 5)

**What happens.** A replica that is applying its master's stream is sent SIGTERM. It logs "SIGTERM received —
shutting down gracefully" and "Writing final snapshot...", then nothing: it never exits, uses no CPU,
and stops reading from its master (4 MB unread in the socket). In the first item-5 run it stayed so for 7 minutes.
Only kill -9 ended it, so there is no final save.

**How often** (`harness/rs.sh`: master + replica, AOF on, 1 M keys, the mix at 100k ops/s, SIGTERM of the streaming
replica, 30 s allowed for the exit; then one control round with the clients stopped):

| Build | Clean stops under load that hung | Control (clients stopped) |
|---|---|---|
| rc9 | **1 / 3, 3 / 10** (4 / 13) | exited 2 / 2 |
| rc8 (082dab3d) | **2 / 3, 4 / 10** (6 / 13) | exited 2 / 2 |
| rc9, item 5's clean stops | 1 / 1 under load; 2 / 6 with the clients stopped but expiries streaming | |

The stops that did finish took 0.9-1.3 s.

**Why** (gdb, `results/replica-shutdown-hang/stacks-try1.txt` and `rs*/stacks.txt`; the same on rc8):
- the shutdown thread (`run_uring` closure, `main.rs`) has taken `command_gate::enter_alone()` and is in
  `replication::save_dataset`, waiting for a mutex: after the checkpoint turn, `save_dataset` takes
  `repl_state.apply_lock`;
- the replica's applier (`kividb-replica` thread) holds `apply_lock` (`replica_client.rs`, taken for each command
  of the stream) and is in `ReplicationApplyEnv::apply_command → run_command → command_gate::enter_unless`, waiting
  for the gate, which the shutdown holds alone;
- the AOF auto-rewrite thread waits for a mutex too (by the code, the checkpoint turn the shutdown holds).

So it is lock-order inversion between the gate and `apply_lock`. The applier already takes the gate alone *before*
`apply_lock` for a transaction (the comment there: "Alone before the lock below, never after: a command can hold the
gate while it waits for that lock (`SAVE` does)"), but a single command takes `apply_lock` first and the gate inside
`run_command`; the shutdown takes the gate alone first and `apply_lock` second. The window is any moment the applier
has a command between the two locks, so it is frequent under writes. Both I/O paths of `main.rs` take the gate alone
before `save_dataset`. rc9's item-5 change (`dataset_for_final_save`) sits between the two and is not involved; rc8
has the same order.

**Impact.** A Kubernetes pod stop or a rolling restart of a replica that receives writes: the pod waits out its grace
period and is killed, with no final save; it restarts from its last checkpoint plus the AOF. Data on the master is not
affected.

Files: `results/replica-shutdown-hang/` (`r5-try1/` the first hang, `rs-*`, `rs10-*` the repro runs with stacks,
sockets and logs).

## 6. 30-min standalone soak

`harness/s1_30.sh` on sc (rc8's `s1_sanity.sh` at 30 min, plus the rc8 `s1_events.sh` at 10 and 20 min, plus ackcheck):
master sm + replica sr, `appendonly yes`, `appendfsync everysec`, everything else at its default (so
`repl-backlog-size` 1 MB). 1 M string keys prefilled; the mix at 100k ops/s; `writelog.py` (8 writers × 100 writes/s,
eight kinds incl. `MULTI/EXEC`, `HEXPIRE`/`HPEXPIRE` fields and `SET PX` keys) and `ackcheck.py` (8 writers × 100 SET/s,
standalone) alongside.

| Check | Result |
|---|---|
| Write log, acknowledged | **1,415,323**, 8 unknown (the master restart) |
| Write log on the master / on the replica | **0 bad / 0 bad** (1,239,411 keys checked OK on each) |
| ackcheck on the master / on the replica | **1,415,191 acknowledged, 0 lost, 0 wrong / the same**; 8 unacknowledged errors; writes back 7.1 s after the SIGTERM |
| Master SIGTERM restart at 10 min | exited 1.78 s after SIGTERM, serving again 4.6 s later, replica linked 0.23 s after that with a **partial resync** (`sync_full=0 sync_partial_ok=1`) |
| Replica kill -9 at 20 min | started 20 s later, serving after 6.9 s, linked after 10.3 s with a full sync (expected: 20 s of writes overrun the 1 MB backlog) |
| Offsets at the end | master 45,492,175,677 = replica |
| Master vs replica (`compare.py`) | 3,159,757 keys compared; 170 differ on the first pass (in flight), 0 after the recheck; **0 real differences**, 0 expiring, 0 field-expiring |
| `[ERROR]`/panic lines | 0 on both; 2 `[WARN]` on the replica (the link reset at the restart; "Dropped the 2,566,029 local keys before loading" at the full sync) |
| `aof_lost_records`, `aof_last_write_status` | 0 and `ok` in every sample, both nodes |
| Throughput | 179,999,500 ops in 30 min; lowest 10 s interval 99,368 ops/s; 646,900 client errors, all in the 10 s of the master restart |
| Latency, median of the 10 s intervals | p50 0.22 ms, p99 70 ms, p99.9 102 ms (the `MULTI/EXEC` mix; rc8's 20-min sanity run: p99 47 ms) |
| Memory | master RSS at the end 2.82 GB, used_memory 1.12 GB, 2.93 M keys |

**PASS.** Files: `results/item6-soak/` (per-writer logs and `acked.json` not copied).

## 7. Hot path

### Core benchmark (arm64), rc9 against rc8

The rc8 core round's harness (`harness/core/`: the suite's `run-suite.sh`, `engine.sh` knowing `kividb-rc9`,
`run-rc9.sh`, `analyze-rc9.py`), its VM shapes and method: 10 M keys loaded, 5 s warm-up, 20 s per scenario, both load
generators at once (memtier 2.5.2), two rounds, round 1 rc9 first, round 2 rc8 first. 100 B only: GET p1, GET p16,
SET p16, MSET of 10 keys (no sweep, no MGET). Verdict *slower* only if every rc9 round is below every rc8 round and the
medians differ by more than 1 %.

| Scenario | rc8 | rc9 | Change | Verdict | p99 ms rc8 → rc9 | server µs/req rc8 → rc9 |
|---|---:|---:|---:|---|---:|---:|
| 100 B GET p1 | 1.76M [1.75M - 1.77M] C | 1.75M [1.74M - 1.75M] C | −0.7 % | level (load generators the limit) | 1.04 → 1.03 | 17.29 → 17.38 |
| 100 B GET p16 | 10.53M [10.49M - 10.58M] | 10.57M [10.50M - 10.65M] | +0.4 % | level | 0.79 → 0.78 | 2.86 → 2.85 |
| 100 B SET p16 | 7.96M [7.90M - 8.01M] | 8.08M [8.05M - 8.12M] | +1.6 % | faster | 0.97 → 0.97 | 3.55 → 3.50 |
| 100 B MSET 10 p1 | 555K [553K - 558K] | 554K [550K - 558K] | −0.3 % | level | 2.97 → 3.00 | 53.91 → 54.05 |

**PASS**: nothing slower. (rc8's own round on these shapes: GET p16 10.44M, SET p16 8.02M, MSET-10 569K.) Chart:
`results/core/charts/regression-rc8-vs-rc9.svg`; tables `results/core/tables.md`.

### Callgrind, rc9 against rc8

`harness/cg-measure.sh`: `scripts/perf/measure.sh` of `release/1.2.0` run natively on b1 (as the rc9 fixes agent did:
`run.sh` needs a Docker image that exists only on a laptop), with `--separate-threads=yes` (the worker thread's count;
measure.sh reads the largest output file) and the workloads GET-100, SET-100, MIX-100, INCR, DEL. uring, N 60,000,
P 16, the shipped release binaries above (fat LTO), alternating rc8, rc9, rc8, rc9 on an otherwise idle VM.

| Workload | rc8 (2 runs) | rc9 (2 runs) | Δ | in glibc `memcmp`, rc8 → rc9 | without `memcmp`, rc8 → rc9 | Δ without `memcmp` |
|---|---|---|---|---|---|---|
| GET-100 | 3328, 3328 | 3346, 3347 | **+0.56 %** | 107.3 → 125.3 | 3219.8 → 3220.8 | +0.03 % |
| SET-100 | 4259, 4259 | 4311, 4311 | **+1.22 %** | 161.3 → 212.3 | 4098.7 → 4098.9 | +0.00 % |
| MIX-100 | 7614, 7613 | 7683, 7684 | **+0.92 %** | 268.3 → 337.3 | 7345.6 → 7346.5 | +0.01 % |
| INCR | 4066, 4067 | 4068, 4068 | +0.04 % | 38.4 → 40.4 | 4027.9 → 4028.1 | +0.00 % |
| DEL | 4464, 4464 | 4489, 4490 | **+0.57 %** | 192.3 → 216.3 | 4273.2 → 4273.9 | +0.02 % |

**As measured, four of the five are outside ±0.5 %. Why, and why I do not read it as a code cost:**
- Every KiviDB function in the SET-100 profile has the same count in both builds (`run_command` 33,900,930,
  `parse_multibulk` 23,460,722, `strings::handle` 14,100,224, … `results/callgrind/rc*-SET-100.annotate.txt`). The whole
  difference is one line: glibc's `__memcmp_avx2_movbe`, 9,676,066 → 12,736,290.
- `memcmp` is called the same number of times from the same callers (`replication::is_write_command` 240,136×,
  `cmdstats::record` 60,008×, `strings::handle`, `write_side_effects`, `atomic_under_its_shard` 60,000× each), and each
  call costs exactly 6 instructions more (e.g. `cmdstats::record`: 22.0 → 28.0 per call). Same calls, same lengths,
  different cost: the inputs' addresses.
- The literals compared moved: the same strings sit at page offsets 0x654 / 0x415 / 0x631 in rc8 and 0xbec / 0xae7 /
  0xbc9 in rc9 (`results/callgrind/literal-offsets.txt`). For inputs under 32 bytes `__memcmp_avx2_movbe` ORs the two
  pointers' page offsets and takes its longer page-cross path when the result is above 4064; with 0xb.. offsets that
  happens far more often. It is a layout effect of 11 commits' worth of code and strings in the binary, not something
  rc9 executes. The core benchmark, which runs the same commands on real cores, shows no cost (SET p16 +1.6 %).
- Without `memcmp` the five workloads are within +0.03 %. The rc9 fixes report measured the same source at −0.22 %
  (SET-100) and +0.24 % (GET-100) with a differently built binary, which fits a placement effect.

**Verdict: FAIL against the letter of ±0.5 %, PASS for KiviDB's own code.** If ±0.5 % on whole counts must hold, the
lever is the comparisons themselves (command-name `memcmp`s on every command), not rc9's changes.

## How to reproduce

```
bash harness/create.sh <ssh-public-key-file>                 # RG, VMs, auto-shutdown
# every test VM: bash base.sh srv|client; b1: bash base.sh build; then b1-build.sh (builds, pushes rc9 to /opt/kv/kividb)
# set A client (tc, hosts.env M= R= TCH=): bash setA.sh, then setA2.sh, then setAB3.sh A
# set B client (sc, hosts.env M= R=):     bash setB.sh, then setB2.sh, then setAB3.sh B
# b1: bash cg-run.sh; bash cg-prof.sh rc8 rc9; bash cg-prof2.sh rc8 rc9; bash cg-split.sh
# arm64: srv: setup-server-min.sh, srv-build.sh; cl1/cl2: setup-client.sh; cl1: bash h/run-rc9.sh
```

## Files

- `harness/`: every script (the rc8/rc7/rc9-fixes ones they come from are named in each header), soakgen source.
- `results/builds/`: commit, toolchain, build time, `--version`, sha256 of every build.
- `results/item1-restart/` … `results/item6-soak/`, `results/replica-shutdown-hang/`, `results/core/`,
  `results/callgrind/`.
- `NOTES.md`: the progress log.

## Teardown

`b1`, `b2` and the arm64 VMs were deleted as soon as their part was done; at the end `az group delete -n
kividb-rc9-retest --yes`, and `az group exists -n kividb-rc9-retest` → **false**. No other resource group was touched.

