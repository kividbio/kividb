# Kubernetes operator end to end on 1.2.0-rc8: the #303 force-delete repro, the rc7 scenarios, and the #42 AOF-master guard

The last gate for the operator side of 1.2.0. rc7's operator run lost **121,939 acknowledged writes** when a shard
master was force-deleted and its replacement started on the same node while the old engine was still stopping
on the same volume (#303). rc8 makes an engine take an exclusive lock on `kividb.lock` in its data directory and
wait while another process holds it. This run repeats that repro on rc8, gives the rc7 scenarios a short pass,
and checks the #42 guard again. Loaders inside the cluster write all the time, and every write the server
acknowledged is read back after each scenario.

## Results in one paragraph

**PASS. 8,524,842 acknowledged writes were read back across 6 loaders, and 0 were lost.** There were no wrong
values and no read errors.
- **The #303 repro: 0 lost in 6 same-node force deletes** (rc7: 121,939 in forcedel-rc7).
  - In 3 of the 6, the replacement engine started while the old one was still writing its final snapshot. This is the interleaving that lost rc7's writes.
  - Each time, the new engine logged `The data directory /data is in use by another process (it holds kividb.lock): waiting for it to be free (0s so far)`, then `The data directory is free after 0.1s: starting`.
  - It then loaded the old engine's final dump and carried on its replication stream from the right offset.
  - In the other 3, the old engine had exited before the new one opened the directory.
  - The graceful delete: 0 failed SETs, 0 lost.
- **Every rc7 scenario passed with 0 lost:**
  - the upgrades 1.1.3 -> rc8 (replicated and sharded) and rc7 -> rc8 (sharded);
  - the rolling restart;
  - CLUSTER FAILOVER of every shard;
  - kill -9 of a shard master;
  - a node drain;
  - resharding 3 -> 4 -> 3;
  - backup and restore.

  All had 0 failed SETs, except the rc7 -> rc8 upgrade: 36, in one second.
- **The #42 guard behaved as designed:** rc8 with `aof-ack-after-write yes` kept its master; rc8 with `no` and 1.1.3 failed over.
- **Two observations, neither a lost write:**
  - the 36 failed SETs in the mixed rc7/rc8 switchover;
  - a demoted master that writes an **empty final snapshot** when its SIGTERM lands during the full sync that follows its handover.

## What was run

| | |
|---|---|
| Cloud | Azure, centralindia, AKS (free tier), Kubernetes 1.36.5, Azure CNI overlay |
| Nodes | 4 x Standard_D4s_v5 (4 vCPU, 16 GiB), Ubuntu 24.04.5, kernel 6.8.0-1071-azure, containerd 2.3.7 |
| Disks | AKS default class (StandardSSD): 20 GiB per sharded pod, 5 GiB for rep and fx, 2 GiB for the guard clusters |
| Engine | **rc8 built in ACR** from kividb commit **082dab3d** (`release/1.2.0`), with the repo's Dockerfile. It reports `kividb 1.2.0-rc8`; image digest `sha256:d7fc7de7…`. Also `ghcr.io/kividbio/kividb:v1.1.3` and `ghcr.io/kividbio/kividb:v1.2.0-rc7` (upgrade starts and g-113) |
| Operator | kividb-operator master **0c0fb6e** (includes #42). Manager, agent and GUI images built with `az acr build`. CRDs applied server-side, the chart installed from the tree |
| Clusters | `rep`: 1 master + 2 replicas, from 1.1.3. `shd`: `spec.shards: 3`, 1 replica per shard, from 1.1.3. `fx`: 3 x (1+1), from rc7, for the repro. `g-yes`, `g-no`, `g-113`: 1 + 1 each. All with a password and operator defaults (`appendonly yes`, `appendfsync everysec`, `cluster-node-timeout 5000`) |
| Loader | rc4's loader (Go, go-redis v9.7.0): open loop, 800 SET/s (rep, shd, fx) or 400 SET/s (guard clusters), 5 s deadline per SET. Every acknowledged write is recorded, and `/verify` reads every one back |
| Pinning | loaders, `cli`/`curl` and the S3 stand-in run on the node labelled `e2e-infra=true`, which is never drained |

The definitions are rc7's (and rc4's):
- **Failed SETs** were not acknowledged.
- **Impact** runs from the first to the last second with a failed SET, or with less than half the normal rate.
- **Lost** means acknowledged writes missing at read-back that were not already missing at the previous check.

Each scenario ran once, so these are event timings, not throughput.

## Results (rc7's numbers in brackets)

### 1. The #303 force-delete repro (`run-fxloss.sh`, `SAMENODE=1`)

Each try ran this sequence on `fx`:
1. load for 40 s;
2. a `BGSAVE` on the target shard master, so an older dump is on disk;
3. load for 40 s more;
4. `kubectl delete pod --grace-period=0 --force` of that master, with every other node cordoned so that the replacement lands on the same node and mounts the same volume.

The targets rotated over shards 0, 1 and 2.

| Try | Master | Old engine got SIGTERM | New engine | Lock wait | Lost | Failed SETs (impact) |
|---|---|---|---|---|---:|---:|
| fx-loss1 | fx-0-1 | 05:19:35.840, writing its final snapshot | started 35.865: **"in use by another process … waiting"**; "free after 0.1s" at 35.916; loaded the final dump (170,306 keys), stream offset 6739120 | **yes** | 0 | 6,899 (27 s) |
| fx-loss2 | fx-1-1 | 05:22:13.453 | opened the directory after the old one exited; 209,056 keys | no | 0 | 6,904 (27 s) |
| fx-loss3 | fx-2-1 | 05:24:52.731 | after the old one exited; 248,142 keys | no | 0 | 6,805 (27 s) |
| fx-loss4 | fx-0-1 | 05:27:34.697 | after the old one exited; 280,515 keys | no | 0 | 1,401 (7 s) |
| fx-loss5 | fx-1-1 | 05:30:18.853 | started 18.898: **waiting**; free after 0.1s; 319,873 keys | **yes** | 0 | 6,849 (27 s) |
| fx-loss6 | fx-2-1 | 05:33:03.882 | started 03.948: **waiting**; free after 0.1s; 358,848 keys | **yes** | 0 | 6,890 (27 s) |
| fx-graceful | fx-1-1 | 05:35:10.489, after the handover to fx-1-0 | started 8 s after "Goodbye" | no | 0 | 0 (0) |
| **rc7** | | | forcedel-rc7: the new engine loaded the previous dump while the old one was still writing; fx-loss1..4 had 0 lost | no lock | **121,939** [forcedel-rc7], 0 [fx-loss1..4] | 2,832 [forcedel]; 6,819-6,862 [fx-loss] |

How to read the table:
- Every replacement came back as the shard's master, inside the node timeout, and carried on the old engine's stream at the offset where that engine stopped.
- In the lock-wait tries, the new engine did nothing until the old one had renamed its final dump and exited.
- `procs.txt` still lists two processes for the pod for 0.3-0.4 s, because the second one exists while it waits on the lock.
- The failed SETs are the master's downtime, the same as rc7's fx-loss tries.
- **1,187,652 acknowledged writes on fx were read back at the end, 0 missing.**

### 2. The rc7 scenarios on rc8

| Scenario | What | Acked writes checked (cumulative) | Lost | Failed SETs | Impact (s) | Slowest SET (ms) | Notes |
|---|---|---:|---:|---:|---:|---:|---|
| install | CRDs + `helm install` of 0c0fb6e | - | - | - | - | - | manager and GUI Running in 19 s [27 s] |
| create | `rep`, `shd` on 1.1.3; `fx` on rc7 | - | - | - | - | - | all Running in 47 s [47 s] |
| upgrade-rep | engine 1.1.3 -> rc8, replicated | 145,640 | 0 | 0 [0] | 0 | 187 [166] | 1 min 06 s [1 min 08 s] |
| upgrade-shd | engine 1.1.3 -> rc8, sharded | 286,072 | 0 | 0 [0] | 0 | 110 [1,151] | 2 min 28 s [2 min 07 s], one CLUSTER FAILOVER per shard |
| upgrade-fx | engine **rc7 -> rc8**, sharded | 422,452 | 0 | **36** | 1 | 5,150 | 2 min 05 s; the 36 are all at shard 1's switchover, see observation 1 |
| restart-rc8 | rolling restart (`KividbDbOps`), sharded | 1,534,280 | 0 | 0 [0] | 0 | 130 [149] | Completed in 2 min 16 s |
| failover-rc8 | `CLUSTER FAILOVER` on every shard's replica in turn | 1,621,960 | 0 | 0 [0] | 0 | 151 [162] | |
| sigkill-rc8 | kill -9 of a shard master (shd-0-1), restarts in place | 1,660,024 | 0 | 0 [0] | 0 | 2,637 [1,561] | Ready again in 10 s [12 s], from its AOF |
| drain-rc8 | drain of a node holding a shard master (shd-0-1), plus rep-1, g-yes-1 and g-no-0 | 1,760,008 | 0 | 0 [0] | 0 | 87 [265] | ShardSwitchover shd-0-1 -> shd-0-0 in 2 s, then the evictions; 1 min 21 s, mostly retries on the metrics-server PDB |
| drain-rc8-rep | same drain, replicated cluster | 1,861,616 | 0 | 0 [1,039] | 0 | 53 [565] | rep-1 was a replica this time; rc7's had the master on the node |
| reshard-up-rc8 | `spec.shards` 3 -> 4 | 1,868,096 | 0 | 0 [0] | 0 | 82 [69] | RebalanceCompleted in 46 s [42 s]; shd-3-0 has 4,096 slots in 3 ranges |
| reshard-down-rc8 | `spec.shards` 4 -> 3 | 1,915,224 | 0 | 0 [0] | 0 | 33 [261] | 16 s [11 s]; every node `cluster_state:ok`, the removed shard's PVCs deleted |
| backup / restore | see below | | 0 | | | | PASS |

After every sharded scenario, `views` checked each node's view of every peer: no stale address and no failed peer,
and every node was `cluster_state:ok`.

Resharding exercises rc8's slot changes: `CLUSTER DELSLOTS` as in Redis, and a slot assigned by hand is held
for 5 s against a peer's older claim. Both moves completed with no failed SET and nothing lost.

### 3. The #42 AOF-master guard (`scen-guard.sh`, operator down)

The operator keeps a master that restarted in a new stream only if `AofEnabled && aof-ack-after-write == "yes" && engine >= 1.2.0`.

How each case was set up:
1. the operator was scaled to 0;
2. the master's engine was frozen, its pod force-deleted and the engine SIGKILLed;
3. the new pod came up Ready from its own volume and AOF, in a new stream at offset 0;
4. the operator was scaled back to 1.

| Case | Engine reports | When the operator came back | Decision | Lost | Failed SETs (window) |
|---|---|---|---|---:|---|
| guard-yes | `1.2.0-rc8`, ack `yes` | new stream at offset 0; master and replica both 356,044 keys (AOF replay: 356,046 commands) | **KEPT** [KEPT]: no `MasterRestartedBehind`. The replica was repointed and continued with "Partial resync … from offset 0 (0 bytes of backlog)", with no transfer | 0 | 22,620 (57 s, mostly operator downtime) [26,248] |
| guard-no | `1.2.0-rc8`, ack `no` | new stream, 444,560 keys each | **FAILED OVER** [FAILED OVER]: `MasterRestartedBehind` ("restarted … at replication offset 0, behind the 17487009 it had reached and behind g-no-0 (offset 17561729)") + `Failover`, promoted g-no-0, 18 s after the operator returned [19 s] | 0 | 22,997 [37,080] |
| guard-113 | `1.1.3`, `CONFIG GET` empty | new stream, **equal key counts** (536,259 each) | **FAILED OVER** [FAILED OVER]: `MasterRestartedBehind` + `Failover`, promoted g-113-1, 18 s after the operator returned | 0 | 25,013 [23,403] |

All three behave as 0c0fb6e intends, the same as rc7 with afc6f49. This run did not repeat rc7's other two guard
cases (an unparsable version, and the operator left running).

### Backup and restore (rc8)

- **Replicated.**
  - A backup Job from the CronJob uploaded 17.7 MB (2,602,804 keys) in 6.7 s.
  - `rep-restored` (with `bootstrapFromSnapshot`) was Running after 44 s [43 s]. The sorted key lists were identical, and 300 sampled values matched.
  - Its replica answered `DBSIZE 0` for a moment while it synced, as in rc7 and rc4.
- **Sharded.**
  - One backup run of all three shards: 17.3 MB in 2.9 s, with a manifest.
  - A sharded restore is still not supported by the operator (documented), so shard 2's archive was started by hand in a standalone rc8 engine: 844,425 keys, the same as shard 2's master. 300 sampled values matched the live cluster, and the sampled keys are in shard 2's slots (11234-16380).
- Not repeated: rc7's finding 2 (an operator bug when a `bootstrapFromSnapshot` cluster is deleted and re-created). It was not in this run's list.
- Evidence: `results/evidence/backup/`.

### Final read-back

| Loader | Acknowledged writes read back | Missing | Wrong |
|---|---:|---:|---:|
| loader-rep | 2,646,956 | 0 | 0 |
| loader-shd | 2,604,456 | 0 | 0 |
| loader-fx (when fx was removed) | 1,187,652 | 0 | 0 |
| loader-g-yes | 693,084 | 0 | 0 |
| loader-g-no | 693,011 | 0 | 0 |
| loader-g-113 | 699,683 | 0 | 0 |
| **total** | **8,524,842** | **0** | **0** |

No engine container restarted unexpectedly, the operator log has no panic, and every engine pod at the end ran
the image it was meant to run.

## Observations

**1. rc7 -> rc8 upgrade: 36 failed SETs at one mixed-version switchover. Severity: low.**
- In upgrade-fx, shard 1's switchover from fx-1-0 (rc7) to fx-1-1 (rc8) failed 36 SETs, all 5 s timeouts, ending in the second 05:16:31. Nothing was lost.
- The rc8 replica won its election in 100 ms (05:16:29.85). The old rc7 master was then sent a full RDB, not a partial resync.
- The other two switchovers of that upgrade (shards 0 and 2), and every switchover of the 1.1.3 -> rc8 upgrades, had no failed SET.
- It happens only while rc7 and rc8 are mixed, for one switchover. Not studied further (evidence: `results/evidence/upgrade-fx/`).

**2. A demoted master that is stopped during its post-handover full sync writes an empty final snapshot. Severity: low (no write lost).**
In fx-graceful (`results/evidence/fx-graceful/`):
- The operator's preStop handed shard 1 over to fx-1-0. The demoted fx-1-1 began a **full** resync from the new master (#306's family): `Dropped the 380670 local keys before loading 380673` at 05:35:10.420.
- SIGTERM came at 10.489, before the load finished. Its final snapshot and AOF rebase took 30 ms.
- The new engine read `Loaded 0 keys from snapshot`. It was a replica, so it did a full sync from the master (382,937 keys) and nothing was lost.
- For the 8 s it was down, that replica's volume held an empty dataset instead of a slightly old one. rc7's fx-graceful shows the same order of events, but its sync had finished before SIGTERM, so its final snapshot held every key.
- A fix would be to keep the previous dump when the dataset is mid-load at shutdown, rather than snapshot it. Not studied further.

**3. Benign Warning Events.**
- `FailedPreStopHook` on the fx force deletes: no grace period, so no preStop.
- `ShardFailover` for our own CLUSTER FAILOVERs.
- `FailedAttachVolume` ("waiting on detach") during the drain and the guard force deletes.
- `UnexpectedJob` for the backup Jobs created by hand from the CronJobs.

## How to reproduce

You need `az`, `kubectl`, `helm` and Python 3, plus `git archive` trees of kividb 082dab3d and kividb-operator
0c0fb6e. Nothing is built locally. `harness/README-harness.md` lists every file and the order.
1. Create the RG, a Basic ACR, then AKS: `az aks create --tier free --node-count 4 --node-vm-size Standard_D4s_v5 --network-plugin azure --network-plugin-mode overlay --attach-acr <acr>`.
2. `bash harness/build.sh <operator tree> 0c0fb6e <engine context>`. The engine context holds Cargo.toml, Cargo.lock, src/, LICENSE, NOTICE and the Dockerfile.
3. Replace `ACR` in the yaml files. Apply `ns.yaml nd.yaml`, and label one node `e2e-infra=true`.
4. Apply the CRDs server-side, then `helm install … -f values-operator.yaml`.
5. Apply `rep.yaml shd.yaml fx.yaml` and their loaders. `source env.sh; cpcli`, then start `capture.sh`.
6. `bash harness/run-rc8.sh`. For the repro alone: `bash harness/run-fxloss.sh 6`.
7. `build_results.py`, then `summarize.py`.

## Files

- `results/summary.md`, `results/summary.csv`: the tables, generated.
- `results/<scenario>-stats.json`, `-verify.json`, `-timeline.json`: the loader output for each scenario.
- `results/final-*-verify.json`, `results/fx-final-verify.json`, `results/final-readback.txt`: the last read-back of every loader.
- The run's record:
  - `results/marks.log`: what we did and when;
  - `results/events.log`: the Kubernetes Events;
  - `results/operator-manager-stream.log`: the operator's log;
  - `results/pods-watch.log`: the pod watch.
- `results/rc8.out`, `results/fx-*.out`: the console output of the run.
- `results/builds.txt`: the ACR build results and the engine image digest.
- `results/evidence/<scenario>/`:
  - fx tries: the old and new engine logs, the 100 ms process and file samples (`procs.txt`, `overlap.txt`) and `summary.txt`;
  - guard cases: before, at-ready and after views, Events, the verdict;
  - backup: the restore logs and comparisons;
  - `final/`: the state at the end.
