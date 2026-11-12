# Harness: operator end-to-end on AKS (1.2.0-rc7)

Everything ran from a workstation with `az`, `kubectl`, `helm` and Python 3 (matplotlib). Nothing is built
locally: images are built in Azure Container Registry with `az acr build`, engine images come from GHCR.
The rc4 run's harness, extended for the operator fixes on master/#42 and for the #42 AOF-master guard.

| File | What it does |
|---|---|
| `build.sh` | builds the operator (manager, agent, GUI) from a kividb-operator checkout, the loader, the in-cluster S3, and `kividb:v1.2.0-rc7-badver` (`badver/`) in ACR |
| `badver/Dockerfile` | the published rc7 image with its binary's version string rewritten from `1.2.0-rc7` to `1.2.0_rc7` (same length), so `INFO server` reports a version the operator cannot parse |
| `env.sh` | shell helpers (`k`, `q`, `mark`, `begin`, `chk2`, `hsig`, `masters`, `views`, `rinfo`, `cond`, `cpcli`, ...). Set `D` (work dir, a `C:/` path under Git Bash), `A` (ACR login server), `ENG` (engine image) |
| `ns.yaml`, `nd.yaml` | namespace, in-cluster S3, `cli`/`curl` pods (pinned to the node labelled `e2e-infra=true`), the privileged hostPID DaemonSet used to signal an engine by its host PID |
| `values-operator.yaml` | Helm values: the ACR images of the operator under test |
| `rep.yaml`, `shd.yaml` | the replicated cluster (1 master + 2 replicas) and the sharded cluster (3 x (1+1)), both on 1.1.3 at first |
| `guard.yaml` | `g-yes` (rc7), `g-no` (rc7 with a KividbConfig `aof-ack-after-write no`), `g-113` (1.1.3), `g-bad` (the badver image), `fb` (rc7): 1 master + 1 replica each |
| `fx.yaml` | a separate 3 x (1+1) rc7 cluster for the force-delete repro |
| `loader-*.yaml` | the acked-write loader for each cluster (800 SET/s for rep/shd/fx, 400 SET/s for the small clusters) |
| `loader/`, `fakes3/` | the loader (Go, go-redis v9; open loop, 5 s deadline per SET, every ack in a bitset, `/verify` reads every acked key back) and an in-memory S3, both from the rc4 run |
| `scen-*.sh` (rc4) | `upgrade`, `dbops`, `stop` (SIGSTOP), `kill2` (in-place SIGKILL or force delete), `del`, `repdel`, `drain`, `reshard`, `split`, `backup` |
| `scen-dbq.sh` | two `KividbDbOps` restarts of one cluster at once (#40 queue); records both ops and the pods down every few seconds |
| `scen-holdback.sh` | `spec.configRef` to a missing KividbConfig, then SIGSTOP of the master (#36: failover must still run) |
| `scen-fblock.sh` | master frozen, its only replica wiped and restarted empty (#35: FailoverBlocked must hold on every pass) |
| `scen-guard.sh` | the #42 guard: operator scaled to 0, master frozen + force-deleted + SIGKILLed, new pod Ready from its AOF, operator back; verdict KEPT or FAILED_OVER (`opup`: operator left running) |
| `scen-shrink.sh` | 4 -> 3 shards with the last `Rebalancing` status kept, then written back after the finish with the operator stopped (a lost finish, #38); `a`: operator restarted; `b`: `spec.shards` raised back to 4 first |
| `scen-bootguard.sh` | bootstrapFromSnapshot is create-only (CEL), and delete-and-recreate waits for the old cluster, then restores exactly the snapshot (#37) |
| `scen-fxrepro.sh`, `run-fxloss.sh` | the force-delete repro: delete a master while listing, every 100 ms on its node, the kividb processes with that pod's HOSTNAME and the data dir's files; `SAMENODE=1` cordons the other nodes so the replacement lands on the same node |
| `run-rc7a.sh`, `run-rc7b.sh` | the order of the run: the rc4 layout on rc7, then the fixes, the guard, the shrink, backup/restore |
| `rinfo.sh`, `mon.sh`, `monshd.sh`, `pingall.sh`, `repcheck.sh` | monitors run inside the `cli` pod |
| `capture.sh` | Events, operator log and pod watch, appended to files for the whole run |
| `build_results.py`, `summarize.py`, `charts.py`, `titles.json` | turn the marks, Events and loader output into `results/` and `charts/` (`RC4_SUMMARY=<rc4 results/summary.csv>` puts the rc4 rows beside rc7's in the summary chart) |

Order: create RG, ACR and AKS (4 x Standard_D4s_v5, Azure CNI overlay, `--attach-acr`), `D=... A=... bash build.sh <operator checkout>`,
replace `ACR` in the yaml files, `kubectl apply -f ns.yaml -f nd.yaml`, label one node `e2e-infra=true`,
`kubectl apply --server-side -f <checkout>/charts/kividb-operator/crds/`, `helm install kividb-operator <checkout>/charts/kividb-operator -n kividb-system --create-namespace -f values-operator.yaml`,
apply `rep.yaml shd.yaml guard.yaml` and the loaders, `source env.sh; cpcli`, start `capture.sh`, then `run-rc7a.sh` and `run-rc7b.sh`. Then:

```
python build_results.py results/marks.log titles.json results results/events.log
python summarize.py results titles.json
RC4_SUMMARY=<rc4>/results/summary.csv python charts.py results charts
```
