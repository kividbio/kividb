# Harness: operator on AKS (1.2.0-rc9)

This is the rc8 run's harness (`rc8tests/operator/harness/`), cut to rc9's list, with the changes below.
- Everything ran from a workstation with `az`, `kubectl`, `helm` and Python 3.
- Nothing is built locally: every image is built with `az acr build`.
- Dropped from rc8: the replicated cluster `rep`, `fx`, `g-113`, the drain, the backup and the S3 stand-in.

| File | What it does |
|---|---|
| `build.sh <operator tree> <commit> <rc9 tree> <rc8 tree>` | builds the operator (manager, agent, GUI), the loader, and **both engines** (`kividb:1.2.0-rc9`, `kividb:1.2.0-rc8`) from `git archive` trees with the repo's Dockerfile |
| `env.sh` | shell helpers (`k`, `q`, `mark`, `begin`, `chk2`, `hsig`, `masters`, `views`, `rinfo`, `cpcli`, ...). Set `D` (work dir, a `C:/` path under Git Bash) and `A` (ACR login server). `ENG` defaults to rc9, `ENG8` to rc8. **New:** `syncs <label>` appends every shd node's `sync_full` / `sync_partial_ok` / `sync_partial_err` to `$D/syncs.log` |
| `ns.yaml`, `nd.yaml` | namespace, the `cli`/`curl` pods (on the `e2e-infra=true` node), the privileged hostPID DaemonSet used to signal an engine by its host PID |
| `values-operator.yaml` | Helm values: the ACR images of operator 0c0fb6e |
| `shd.yaml` | the sharded cluster, 3 x (1+1), **created on rc8** |
| `guard.yaml` | the password and ACL, `g-yes` (rc9) and `g-no` (rc9 with a KividbConfig `aof-ack-after-write no`) |
| `loader-*.yaml`, `loader/` | rc4's acked-write loader (800 SET/s for shd, 400 for the guard clusters) |
| `scen-upgrade.sh`, `scen-dbops.sh`, `scen-kill2.sh`, `scen-reshard.sh`, `scen-guard.sh` | rc8's scenarios, unchanged |
| `scen-failover.sh` | **new:** CLUSTER FAILOVER on every shard's replica in turn. Records the sync counters before and after (`syncdiff.py`), and each demoted master's log, with its partial and full resync lines |
| `scen-delete.sh` | **new** (rc8's `scen-fxrepro.sh`, on shd): a graceful or force delete of a shard master. While it runs, a watcher samples the node's kividb processes for that pod and its data-dir files (sizes, mtimes) every 100 ms for 75 s. It also keeps the old and new engine logs, the peer's log, key counts before and after, and `dump-sizes.txt`. `SAMENODE=1` cordons the other nodes |
| `run-deletes.sh` | 2 graceful deletes (shards 0, 1), then 3 same-node force deletes (shards 0, 1, 2), each after a BGSAVE |
| `enginelogs.sh` | **new:** follows every engine pod's log by name for the whole run, re-attaching when the pod is replaced |
| `synccount.py` | **new:** counts, per scenario window, the replicas' full and partial resyncs, rc9's shutdown waits, skipped final saves and empty dumps loaded, from those logs |
| `syncs.sh`, `rinfo.sh`, `monshd.sh`, `pingall.sh`, `repcheck.sh` | scripts run inside the `cli` pod |
| `capture.sh` | Events, operator log and pod watch, appended to files for the whole run |
| `run-rc9.sh` | the order of the run (see the README) |
| `build_results.py`, `summarize.py`, `titles.json`, `show.py` | turn the marks, Events and loader output into `scenarios.json`, `marks.csv` and `summary.csv`/`summary.md` |
| `scrub.py <dir> <acr>` | replaces the registry name, IPv4 addresses and workstation paths in the published files |

The order of a run:
1. Create the RG, a Basic ACR and AKS.
2. `D=... bash build.sh <operator tree> 0c0fb6e <rc9 tree> <rc8 tree>`.
3. Replace `ACR` in the yaml files.
4. `kubectl apply -f ns.yaml -f nd.yaml`, and label one node `e2e-infra=true`.
5. `kubectl apply --server-side -f <operator tree>/charts/kividb-operator/crds/`.
6. `helm install kividb-operator <operator tree>/charts/kividb-operator -n kividb-system --create-namespace -f values-operator.yaml`.
7. Apply `shd.yaml guard.yaml loader-shd.yaml loader-guard.yaml`.
8. `source env.sh; cpcli`.
9. Start `capture.sh` and `enginelogs.sh`.
10. `bash run-rc9.sh`.

Then:

```
python build_results.py results/marks.log titles.json results results/events.log
python summarize.py results titles.json
python synccount.py results/marks.log results/engine-logs results/syncs.csv
```
