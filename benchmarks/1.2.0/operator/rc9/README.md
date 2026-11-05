# Kubernetes operator on 1.2.0-rc9: shutdown, handover and failover under load

A short pass of kividb-operator 0c0fb6e on engine 1.2.0-rc9. rc9 changes five things the operator relies on:
- **shutdown:** clients are paused, and the engine waits up to `shutdown-timeout` (10 s) for its replicas;
- **elections:** a replica stands only when it is caught up, never in the middle of a full sync;
- **handover:** a demoted master is continued with a partial resync;
- **full sync:** it gets a 1 GiB queue allowance;
- **a replica stopped mid-sync:** it keeps its last good dump.

The list was fixed in advance:
- install;
- upgrade rc8 -> rc9, sharded;
- rolling restart;
- CLUSTER FAILOVER of every shard;
- 2 graceful and 3 same-node force deletes of a shard master;
- kill -9;
- reshard 3 -> 4 -> 3;
- the #42 guard (`yes` / `no`).

A loader inside the cluster wrote all the time, and every acknowledged write was read back after each scenario.

## Results in one paragraph

**PASS. 3,317,045 acknowledged writes were read back across 3 loaders, and 0 were lost.** There were no wrong values and no read errors.
- **Handover:** every demoted master was continued with a partial resync.
  - That covers the 3 of CLUSTER FAILOVER, the 3 of the rolling restart and the 2 of the graceful deletes.
  - **There were 0 full syncs** in the rolling restart, the failovers and the deletes. On the masters, `sync_full` rose by 0 and `sync_partial_ok` by 3 across the failovers.
- **Graceful deletes:** the demoted master's final dump held its keys (159,006 and 194,049, loaded back on restart), not the empty dump of rc8's fx-graceful.
- **Force deletes (#303):** all 3 replacements waited on `kividb.lock` (0.1-0.2 s), loaded the old engine's final dump, and the replica continued with 0 bytes of backlog.
- **The #42 guard:** rc9 `yes` kept its master; `no` failed over 19 s after the operator returned (rc8: 18 s).
- **Shutdown pause:**
  - A master with a replica took 100-107 ms from SIGTERM to exit, of which 10 ms was the wait for the replica's ack.
  - Replicas took 35-73 ms, against 25-42 ms for rc8's.
  - That is **tens of ms more, well under the "few hundred ms" limit**.
  - The slowest SET in the handovers was about 100-140 ms higher than rc8's (267-268 ms against 130-151 ms), with 0 failed SETs.
- **One difference from rc8, not a lost write:** kill -9 failed 1,384 SETs over 7 s (rc8: 0). See observation 1.

## What was run

| | |
|---|---|
| Cloud | Azure, centralindia, AKS (free tier), Kubernetes 1.36.5, Azure CNI overlay |
| Nodes | 4 x Standard_D4s_v5 (4 vCPU, 16 GiB), Ubuntu 24.04.5, kernel 6.8.0-1071-azure; one node labelled `e2e-infra=true` for the loaders and `cli` |
| Engine | **rc9 built in ACR** (`az acr build`, the repo's Dockerfile) from kividb **4a47644e**; it reports `kividb 1.2.0-rc9`, digest `sha256:064c6a73…`. **rc8** built the same way from **082dab3d** (digest `sha256:e3e09e5d…`), for the upgrade start |
| Operator | kividb-operator master **0c0fb6e**; manager, agent and GUI built with `az acr build`; CRDs applied server-side, chart installed from the tree |
| Clusters | `shd`: `spec.shards: 3`, 1 replica per shard, 10 GiB per pod, **created on rc8**. `g-yes`, `g-no`: 1 + 1 each on rc9, `g-no` with a KividbConfig `aof-ack-after-write no`. All with a password and operator defaults (`appendonly yes`, `appendfsync everysec`, `cluster-node-timeout 5000`) |
| Loader | rc4's loader (Go, go-redis v9.7.0): open loop, 800 SET/s (shd) or 400 SET/s (guard clusters), 5 s deadline per SET; `/verify` reads every acknowledged write back |

The definitions are rc8's:
- **Failed SETs** were not acknowledged.
- **Impact** runs from the first to the last second with a failed SET, or with less than half the normal rate.
- **Lost** means acknowledged writes missing at read-back that were not already missing at the previous check.

Each scenario ran once, so the timings are single events.

## Results (rc8's numbers in brackets)

| Scenario | What | Acked writes checked (cumulative) | Lost | Failed SETs | Impact (s) | Slowest SET (ms) | Full / partial resyncs | Notes |
|---|---|---:|---:|---:|---:|---:|---|---|
| install | CRDs + `helm install` of 0c0fb6e | - | - | - | - | - | - | manager and GUI Available in 19 s [19 s]; clusters Running 35 s after create [47 s] |
| upgrade-shd | engine **rc8 -> rc9**, sharded | 193,264 | 0 | 0 [0] | 0 | 208 [110] | 3 / 6 | 2 min 07 s from the patch to every pod on rc9 [2 min 13 s, from 1.1.3]. Each restart onto rc9 was a partial resync. The 3 full syncs went to the old masters, demoted while still on rc8: keeping the stream position on demotion is new in rc9 |
| restart-rc9 | rolling restart (`KividbDbOps`) | 342,336 | 0 | 0 [0] | 0 | 267 [130] | **0** / 9 | Completed in 2 min 17 s [2 min 16 s]. Every master was handed over and continued with a partial resync; SIGTERM to exit took 35-64 ms (each was a replica by then) |
| failover-rc9 | `CLUSTER FAILOVER` on every shard's replica in turn | 436,944 | 0 | 0 [0] | 0 | 268 [151] | **0** / 3 | **Each demoted master partial-resynced**. They logged "This node's dataset is offset N of its own stream …", "The master continues … as …" and "Partial resync: continuing from offset N — no dataset transfer". Master side: `sync_full` +0, `sync_partial_ok` +3 |
| graceful1 | graceful delete of a shard master (shd-0-1), same node | 545,314 | 0 | 0 [0] | 0 | 70 [61] | 0 / 2 | See the graceful table |
| graceful2 | same, shd-1-1 | 651,650 | 0 | 0 | 0 | 30 | 0 / 2 | |
| forcedel1 | same-node force delete of a shard master (shd-0-0), #303 | 778,118 | 0 | 4,172 [6,805-6,904; one 1,401] | 17 [27] | 5,419 | 0 / 1 | See the force-delete table |
| forcedel2 | same, shd-1-0 | 907,319 | 0 | 1,407 | 17 | 5,002 | 0 / 1 | |
| forcedel3 | same, shd-2-1 | 1,033,371 | 0 | 4,332 | 18 | 5,266 | 0 / 1 | |
| sigkill-rc9 | kill -9 of a shard master (shd-0-0), restarts in place | 1,075,875 | 0 | **1,384** [0] | 7 [0] | 5,003 [2,637] | 0 / 1 | The engine accepted connections 3.2 s after the kill, and the pod was Ready in 15 s [10 s]. The replica continued the new stream from offset 0, with no transfer. The replica did not stand for election. Failed SETs: observation 1 |
| reshard-up-rc9 | `spec.shards` 3 -> 4 | 1,139,275 | 0 | 0 [0] | 0 | 36 [82] | 1 / 0 | RebalanceCompleted 32 s after the patch [46 s]. The full sync is the new shard's replica, which started empty. shd-3-0 got 4,096 slots in 3 ranges |
| reshard-down-rc9 | `spec.shards` 4 -> 3 | 1,183,723 | 0 | 0 [0] | 0 | 45 [33] | 1 / 0 | 11 s after the patch [16 s]. Every node `cluster_state:ok`, the removed shard's PVCs deleted. The full sync: observation 2 |
| guard-yes | #42 guard, rc9, ack `yes` | 741,022 | 0 | 24,770 [22,620] | 63 [57] | 5,002 | 0 / 1 | **KEPT** [KEPT]: no `MasterRestartedBehind`. AOF replay 685,401 commands / 685,400 keys; replica equal, repointed, "Partial resync … from offset 0 (0 bytes of backlog)" |
| guard-no | #42 guard, rc9, ack `no` | 834,760 | 0 | 25,156 [22,997] | 63 [58] | 5,010 | 1 / 0 | **FAILED OVER** [FAILED OVER]: `MasterRestartedBehind` ("restarted … at replication offset 0, behind the 30927443 it had reached and behind g-no-1 (offset 30995297)"), then `Failover` to g-no-1, 19 s after the operator returned [18 s]. The old master then full-synced, as it should: it had lost writes |

**Full / partial resyncs** count the replicas' "Full resync from" and "Partial resync: continuing" lines in each scenario's
window, taken from every engine pod's followed log (`results/syncs.csv`, `harness/synccount.py`).

After every sharded scenario, `views` checked each node's view of every peer: no stale address, no failed peer, and
every node was `cluster_state:ok`.

### Graceful deletes: the demoted master's data stays on disk

The operator's preStop hands the shard over first. The old master is demoted, then gets SIGTERM.

| Try | Handover (old master's log) | SIGTERM -> exit | dump.kdb on the volume (watched every 100 ms) | New engine | Failed SETs |
|---|---|---|---|---|---:|
| graceful1 (shd-0-1) | 07:31:40.976: "Manual failover requested … writes paused". 41.054: "This node's dataset is offset 6286370 of its own stream". 41.058: "**Partial resync: continuing from offset 6286370 — no dataset transfer**" | 41.075 -> 41.146 (71 ms), final snapshot written | 2,070,612 B (07:27) -> **3,742,140 B** at 41.097 | "**Loaded 159006 keys** from snapshot"; then "Loaded dataset is offset 6287216 … a partial resync will be asked for" and "Partial resync: continuing from offset 6287216". Down 3 s | 0 |
| graceful2 (shd-1-1) | 07:34:03.987 failover requested. 04.031: "**Partial resync: continuing from offset 7687103**" | 04.160 -> 04.233 (73 ms) | 2,231,120 B -> **4,583,072 B** | "**Loaded 194049 keys**" (the shard held 190,662 at the delete); partial resync from 7689046 | 0 |
| rc8 fx-graceful | demoted master began a **full** resync ("Dropped the 380670 local keys"), SIGTERM came during it | 30 ms | final snapshot held **0 keys** | "Loaded 0 keys from snapshot", then full sync | 0 |

In rc9 the demoted master never starts a full sync, so its final save holds its whole dataset. The "replica stopped
mid-sync keeps its last good dump" path (`the final save is skipped`) was not triggered in either try: no sync was in
progress. Evidence: `results/evidence/graceful*/` (`summary.txt`, `dump-sizes.txt`, `procs.txt`, both logs).

### Force deletes on the same node (#303)

Each try ran this sequence:
1. a `BGSAVE` on the target master, then 30 s of load;
2. `kubectl delete pod --grace-period=0 --force`, with every other node cordoned, so that the replacement lands on the same node and volume.

| Try | Old engine | New engine | Lock wait | Two engines seen (100 ms samples) | Lost | Failed SETs (impact) |
|---|---|---|---|---:|---:|---:|
| forcedel1 (shd-0-0) | SIGTERM 07:37:04.156; "replica … has the whole stream, to offset 9443593 (**10 ms**)"; final snapshot; exit 04.256 | started 04.222: "in use by another process … waiting"; "free after 0.1s" 04.272; loaded 237,904 keys at offset 9443593; the replica continued with "0 bytes of backlog" | **yes** | 3 | 0 | 4,172 (17 s) |
| forcedel2 (shd-1-0) | SIGTERM 07:40:04.103; whole stream (10 ms); exit 04.204 | waiting from 04.129; free after 0.1s; 281,473 keys at 11186505; replica continued, 0 bytes | **yes** | 4 | 0 | 1,407 (17 s) |
| forcedel3 (shd-2-1) | SIGTERM 07:43:04.205; whole stream (10 ms); exit 04.312 | waiting from 04.195; free after 0.2s; 324,753 keys at 12918090; replica continued, 0 bytes | **yes** | 3 | 0 | 4,332 (18 s) |
| **rc8** | | 6 tries; the lock wait happened in 3 | 3 of 6 | | 0 | 6,805-6,904 (27 s); one 1,401 (7 s) |

All 3 tries hit the interleaving that lost writes in rc7. Each new engine waited for the old one's final save, then
carried on the same stream. The master came back in place within the node timeout, so its replica did not take over.
The failed SETs are the master's downtime: the new pod was Ready 14-15 s after the delete. That is fewer failed SETs than
in rc8's tries.

### The shutdown pause, measured

SIGTERM to "Goodbye", from every graceful shutdown in the engine logs (`results/engine-logs/`):

| | Count | SIGTERM -> exit | Replica-ack wait |
|---|---:|---|---|
| rc8 engines (upgrade, each a replica by then) | 6 | 25-42 ms | - |
| rc9 replicas (rolling restart, graceful deletes) | 8 | 35-73 ms | none (no replicas) |
| rc9 masters with a replica (force deletes) | 3 | 100-107 ms | **10 ms** each, "has the whole stream … it can be continued after the restart" |

So the pause adds tens of ms, not hundreds. In the operator's normal paths (upgrade, rolling restart, graceful delete),
the engine that gets SIGTERM is already a replica, so it has nothing to wait for.

### Final read-back

| Loader | Acknowledged writes read back | Missing | Wrong |
|---|---:|---:|---:|
| loader-shd | 1,592,203 | 0 | 0 |
| loader-g-yes | 859,794 | 0 | 0 |
| loader-g-no | 865,048 | 0 | 0 |
| **total** | **3,317,045** | **0** | **0** |

- Every engine pod at the end ran rc9 from this run's build.
- The only container restart was the kill -9 (shd-0-0, 1).
- The operator log has no panic.

## Observations

**1. kill -9: 1,384 failed SETs over 7 s (rc8: 0). Severity: low; nothing lost; probably not caused by rc9.**
- The engine was killed at 07:45:10 and accepted connections again at 13.2. It reloaded from its dump and AOF (348,878 keys) and carried on.
- Shard 0's SETs went through again from 07:45:14 to 18.
- From 07:45:19 to 25, every shard-0 SET timed out (5 s); the other shards were unaffected.
- From 07:45:14.0 to 20.7, `CLUSTER SLOTS` on another node listed shard 0's master endpoint as **`?`**. It became the hostname again when the operator re-sent `CONFIG SET cluster-announce-hostname` at 20.48.
- rc8's kill -9 shows the same `?` window (5.6 s) but no failed SET. That is likely because the client did not reload its slot map inside the window that time.
- The window and the client's reload would explain the failure. None of rc9's changes touch it: there was no shutdown, no election, and no sync beyond a partial resync from offset 0.
- Not studied further. Evidence: `results/evidence/sigkill-rc9/cluster-slots.txt`, `results/engine-logs/shd-0-0.log`, `results/sigkill-rc9-timeline.json`.

**2. Reshard 4 -> 3: the removed shard's replica began a full sync and was stopped during it. Severity: none.**
- When shard 3 was emptied, its replica shd-3-1 was moved under shard 2's master: "Full resync from 4e255bc… offset=24536568" at 07:48:23.645.
- 165 ms later it got SIGTERM for its own removal. It wrote a final snapshot (exit 488 ms after SIGTERM), and its PVC was then deleted.
- Its log does not show the "final save is skipped" warning, so the payload had either not started loading or had finished. Which one is not visible here.
- No data is involved, since the pod and its volume are gone.

**3. Benign Warning Events.**
- `FailedPreStopHook` on the 3 force deletes (no grace period, so no preStop).
- `ShardFailover` for our own CLUSTER FAILOVERs.
- `FailedAttachVolume` ("waiting on detach") on the guard force deletes.
- `FailedScheduling` for `cli`/`curl` before the infra node was labelled.

**Not exercised here:**
- the 1 GiB full-sync queue allowance: the datasets were small, and the largest full-sync queue peak logged was 410 bytes;
- a replica stopped in the middle of a sync, apart from observation 2.

## How to reproduce

You need `az`, `kubectl`, `helm` and Python 3, plus `git archive` trees of:
- kividb 4a47644e and 082dab3d, each with Cargo.toml, Cargo.lock, src/, LICENSE, NOTICE and the Dockerfile;
- kividb-operator 0c0fb6e.

Nothing is built locally.
1. Create the RG and a Basic ACR, then AKS: `az aks create --tier free --node-count 4 --node-vm-size Standard_D4s_v5 --network-plugin azure --network-plugin-mode overlay --attach-acr <acr>`.
2. `D=<work dir> bash harness/build.sh <operator tree> 0c0fb6e <rc9 tree> <rc8 tree>`.
3. Replace `ACR` in the yaml files. Apply `ns.yaml nd.yaml`, and label one node `e2e-infra=true`.
4. Apply the CRDs server-side, then `helm install … -f values-operator.yaml`.
5. Apply `shd.yaml guard.yaml loader-shd.yaml loader-guard.yaml`.
6. `source env.sh; cpcli`, then start `capture.sh` and `enginelogs.sh`.
7. `bash harness/run-rc9.sh`.
8. `build_results.py`, `summarize.py`, `synccount.py`.

`harness/README-harness.md` lists every file.

## Files

- `results/summary.md`, `summary.csv`: the loader table, generated. `results/syncs.csv`: full and partial resyncs, shutdown waits, skipped saves and empty loads per scenario.
- `results/<scenario>-stats.json`, `-verify.json`, `-timeline.json`: the loader output. `results/final-*`: the last read-back.
- The run's record:
  - `results/marks.log`: what we did and when;
  - `results/events.log`: the Kubernetes Events;
  - `results/pods-watch.log`: the pod watch;
  - `results/operator-manager-stream.log`: the operator's log;
  - `results/syncs.log`: every node's sync counters after each scenario;
  - `results/rc9.out`, `results/*.out`: the console output.
- `results/engine-logs/<pod>.log`: every engine pod's log for the whole run, with Kubernetes timestamps.
- `results/evidence/<scenario>/`:
  - failover: sync counters before and after, each demoted master's log;
  - deletes: old and new engine logs, the peer's log, 100 ms process and file samples, dump sizes, key counts;
  - guard: before, at-ready and after views, Events, the verdict;
  - `final/`: the state at the end.
- `results/builds.txt`: the ACR builds and image digests.
- In published files, the registry name, IP addresses and workstation paths are replaced with `<acr>`, `<ip>` and `<work>`.
