# Operator end-to-end on AKS: KiviDB 1.2.1 with kividb-operator master

The operator end-to-end plan of the 1.2.0 runs, on the final 1.2.1 code, with kividb-operator master `473ae58` (which includes PR #82's handover changes: the switchover pause is called off rather than skipped when it cannot take hold, and the old master is fenced). Both clusters start on the 1.2.0 release image and are upgraded to 1.2.1 under load; then every planned handover, crash, hang, forced delete, drain, password change, bad config, reshard, backup and restore is run against them with an acknowledged-write loader on each cluster.

**Result: PASS. 0 acknowledged writes lost in 27 checked scenarios** (2,762,835 acknowledged writes read back: 948,876 on the replicated cluster, 1,813,959 on the sharded one). There were 28 planned handovers: 10 replicated switchovers and 18 sharded CLUSTER FAILOVERs. The replicated switchovers held writes for 55-129 ms. No planned handover caused a single client error. That includes the two scenarios that lost writes in every earlier run, both now without loss:
- **A2b**, a forced delete of the replicated master: 0 lost. The 1.2.0 runs lost 68-188 here. The operator fenced the old master before promoting, and logged "fenced the master being failed away from", then "made the fenced master a replica of the new master and released it".
- **B2a**, SIGSTOP of a shard master: 0 lost. 1.2.0 lost 2 here, by timing.

## What ran

| | |
|---|---|
| Cluster | AKS 1.36.5, 4 x Standard_D4s_v5, Azure CNI overlay, centralindia, free tier; default StandardSSD disks. One node labelled `e2e-infra` hosts the loaders, the `cli`/`curl` pods, the in-cluster S3 (gofakes3) and the runner pod. |
| Engine | **1.2.1** (`35a413c0`), built from the repo `Dockerfile` (`rust:1.89-slim-bookworm`, default variant, release profile) with `az acr build` into a private ACR. Image digest `sha256:90206112ec7151ce411b6e10bc25d4370827c0dab40130d450f1f2dfb7064ce9`; `/usr/local/bin/kividb` SHA-256 `348cfa6f151d11534f1730ec825517d30b4be3f05be3394517ef34c6c6a9a2c7` (amd64). It prints `1.2.1-rc2`: the version string is bumped at release. Starting image: `ghcr.io/kividbio/kividb:v1.2.0`. |
| Operator | kividb-operator master `473ae58` (manager, agent, GUI built with `az acr build`), chart from the same commit. |
| Clusters | `rep`: 1 master + 2 replicas, password, 5 Gi. `shd`: 3 shards x (1+1), password, 20 Gi. AOF on (the operator default, `appendfsync everysec`, `aof-ack-after-write yes`). |
| Load | `harness/loader` (go-redis v9), 800 SET/s on each cluster, 4 workers, open loop, 5 s deadline a SET. Every ack is recorded, and `/verify` reads every acknowledged key back. |
| Driver | the 1.2.0 operator harness (`benchmarks/1.2.0/operator/harness`), run from an in-cluster runner pod (python:3.12-slim + kubectl + helm). The ServiceAccount is bound to cluster-admin inside the cluster, not through Azure RBAC. The replicated track (`run-c3-rep.sh`) and the sharded track (`run-c3-shd.sh`) ran side by side. Then came `run-c3-seq.sh`: drain, bulk load, reshards, GUI, backups. 2026-11-12 04:09-04:51 UTC. |

## Results

"Errors" are SETs that failed or timed out without an ack. These are not losses: the loader only checks keys it got an OK for. "Max SET" is the slowest acknowledged SET.

| # | Scenario | Result | Acked lost | Errors | Max SET | Handover / notes |
|---|---|---|---:|---:|---:|---|
| A1 | `rep` upgrade 1.2.0 -> 1.2.1 under load | PASS | 0 | 0 | 883 ms | rollout switchover rep-1 -> rep-0, writes held 66 ms; ~60 s |
| U2 | `shd` upgrade 1.2.0 -> 1.2.1 under load | PASS | 0 | 0 | 270 ms | replicas first, then one CLUSTER FAILOVER per shard, each "as asked" within 1 s; ~2 min |
| A2a | graceful delete of the replicated master | PASS | 0 | 0 | 826 ms | switchover in preStop, held 55 ms |
| B-del | graceful delete of a shard master | PASS | 0 | 0 | <150 ms | CLUSTER FAILOVER in preStop |
| A3 | `KividbDbOps` restart x3 (replicated) | PASS | 0 | 0 / 0 / 0 | 1.6 s / 661 / 536 ms | one step-down each, held 64 / 64 / 58 ms; ~66-71 s each; no MasterRestartedBehind |
| A4a | password change (Secret edit), loader on the new password at once | PASS | 0 | 0 | 931 ms | PasswordChanged; old password WRONGPASS on every pod afterwards |
| A4b | password removed | PASS | 0 | 0 | 897 ms | PasswordChanged |
| A4c | password set again | PASS | 0 | 0 | 822 ms | PasswordChanged; old refused, new PONG on all pods (checked at the end of the run) |
| C1 | KividbConfig with an unknown directive, then fixed | PASS | 0 | 0 | 1.0 s | only rep-2 rolled and crash-looped (`[FATAL] Bad directive ... unknown directive 'no-such-directive-xyz'`); master and other replica untouched; after the fix StuckPodReplaced, roll completed, `maxmemory-policy allkeys-lru` on all pods |
| **A2b** | **forced delete (`--grace-period=0 --force`) of the replicated master** | **PASS** | **0** (1.2.0 runs: 68-188) | 0 | 2.2 s | Warning Failover "master rep-0 is unavailable; promoted rep-2"; operator fenced the old master first |
| A8 | in-place SIGKILL of the replicated master's engine | PASS | 0 | 5,654 (8 s) | 5.1 s | container restarted in place, back as master from its AOF; replicas resynced |
| A2 | drain of the replicated master's node (rep-2 and shd-1-0 on it) | PASS | 0 / 0 | 0 / 0 | 2.1 s (rep) / 234 ms (shd) | switchover held 68 ms + CLUSTER FAILOVER; disk reattach |
| B2 | forced delete of a shard master | PASS | 0 | 1,403 (5 s) | 5.0 s | came back with its AOF data, same node ID |
| B2k | in-place SIGKILL of a shard master | PASS | 0 | 1,430 (6 s) | 5.0 s | same |
| **B2a** | **SIGSTOP of a shard master** | **PASS** | **0** (1.2.0: 2) | 3,172 (12 s) | 8.0 s | engine failover (Warning ShardFailover 04:14:31, ~12 s after STOP); liveness restarted the stopped container; it rejoined as a replica |
| B3 | rolling restart by spec change (`podAnnotations`) | PASS | 0 | 0 | 212 ms | 6/6 pods replaced, one CLUSTER FAILOVER per shard; ~2.5 min |
| B4 | `KividbDbOps` restart (sharded) | PASS | 0 | 0 | 360 ms | ~3 min |
| B-pw | sharded password change | PASS | 0 | 0 | 214 ms | PasswordChanged for `shd`; old password WRONGPASS on all 6 pods at the end |
| B5 | bulk load (2.5 M SETs of 1 KB, 399k SET/s with AOF on) then reshard 3 -> 4 -> 3 under load | PASS | 0 | 0 | 666 ms / 237 ms | 3->4: RebalanceCompleted in ~50 s (3 slot moves); 4->3: 45 s, shd-3 pods and PVCs gone, cluster_state ok, 6 known nodes. No multi-second write stalls and no "reply ... sent without it" AOF warning (1.2.0 run: SETs stalled 10-45 s) |
| B5b | second reshard cycle 3 -> 4 -> 3 (~4.3 M keys) | PASS | 0 | 0 | 525 ms / 244 ms | same |
| B7 | `spec.cluster.replicaValidityFactor` 10 -> 0 (full rolling restart) | PASS | 0 | 0 | 295 ms | ~6 min with ~3.9 M keys; `cluster-replica-validity-factor 0` on all 6 nodes; replicas tracking their masters |
| B8 | console GUI: read-only API, sign-in, promote from the GUI under load | PASS | 0 | 0 | 236 ms | 7 GET endpoints 200; promote without sign-in 403; login 303, session writable; promote shd-1-1 -> 202, shard 1 master moved; `defaultImage docker.io/kividb/kividb:v1.2.0` |
| A5 | replicated backup + restore (`bootstrapFromSnapshot`) | PASS | - | - | - | backup 10.0 MB in 4.2 s; rep-restored Running in 41 s; 1,496,996 keys on source and both restored pods, sorted key lists identical, 300 sampled values equal |
| B6 | per-shard backup + manual restore of one shard | PASS | - | - | - | one run, 3 shards, 55.5 MB in 20.4 s, manifest; shard 2's archive restored with `--aof`: 1,273,409 keys = the live master, 300 sampled values equal, sampled slots 11459-16252 (shard 2's range) |
| B9 | memory per node after bulk load and reshards | NOTE | - | - | - | see below |

Totals: `rep` loader, 948,876 acknowledged keys read back, 0 missing, 0 wrong. `shd` loader, 1,813,959 read back, 0 missing, 0 wrong. The checks are cumulative: each verify reads every key acknowledged since the start.

### B9 memory

The 1.2.0 run's finding that replicas held 1.8x their master's `used_memory` after a fast bulk load did not come back. Masters and replicas agreed within 1-2% on `used_memory` after the reshards, after B7 and after the second cycle. Straight after the bulk load, the one pair that differed (shd-1, 11%) also differed in key count: its replica was still applying the stream (1.33 M vs 1.44 M keys). RSS is above `used_memory` on some nodes after the bulk load and the slot moves, at up to 1.9x: shd-1-1 used 1033 MB with an RSS of 1944 MB after 3->4. After the restarts of B7, RSS was within 4% of `used_memory` everywhere. This is the known fragmentation class (#113), not new. Per-node figures are in `res/mem-*.txt` in the raw archive.

## Compared with the 1.2.0 runs (aks-e2e-rc3 / rc2 logs)

- Zero lost writes on every scenario. Before, A2b (forced delete of the replicated master) always lost writes, and B2a lost writes by timing.
- B5: no AOF-checkpoint write stalls this time; the max SET was 666 ms, against 10-45 s in 1.2.0. The bulk load was the same 2.5 M x 1 KB. The disk class and size are also the same.
- Planned handovers, password changes, C1, DbOps, rollouts, GUI, backups: same behaviour as before, with 0 client errors in every planned handover. In 1.2.0, A3/A4 saw 2-23 READONLY errors.

## Bugs

None found.

## Files

- `harness/`: the 1.2.0 operator harness plus this run's scripts: `run-c3-rep.sh`, `run-c3-shd.sh`, `run-c3-seq.sh` (scenario order), `scen-roll.sh` (spec/Secret/ACL change that rolls every pod), `scen-c1.sh`, `scen-gui.sh`, `scen-mem.sh`, `runner.yaml`, `runner-setup.sh` and `start.sh` (in-cluster runner), and `env.sh` (patched so it can run in-cluster).
- `results/raw.tar.gz`: the runner's work dir (`marks.log`, `events.log`, `operator.log`, `pods-watch.log`, `pingall-shd.log`, track logs, `res/<scenario>-{stats,verify,timeline}.json`, `res/mem-*.txt`, `evidence/`), engine logs of the final pods (`enginelogs/`), and the image record (`engine-image.txt`). Unpack with `tar -xzf results/raw.tar.gz`.
