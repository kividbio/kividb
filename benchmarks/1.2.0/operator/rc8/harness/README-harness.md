# Harness: operator end-to-end on AKS (1.2.0-rc8)

The rc7 run's harness (`rc7tests/operator/harness/`), reused, with the rc8 changes listed below. Everything
ran from a workstation with `az`, `kubectl`, `helm` and Python 3. Nothing is built locally: every image is
built in Azure Container Registry with `az acr build`.

| File | What it does |
|---|---|
| `build.sh <operator tree> <commit> [engine context]` | builds the operator (manager, agent, GUI), the loader and the S3 stand-in, and, given a context, the engine `kividb:1.2.0-rc8` from the repo's Dockerfile. **New for rc8:** takes a `git archive` tree and its commit (no checkout needed) and builds the engine; no badver image |
| `env.sh` | shell helpers (`k`, `q`, `mark`, `begin`, `chk2`, `hsig`, `masters`, `views`, `rinfo`, `cpcli`, ...). Set `D` (work dir, a `C:/` path under Git Bash), `A` (ACR login server). `ENG` defaults to `$A/kividb:1.2.0-rc8` |
| `ns.yaml`, `nd.yaml` | namespace, in-cluster S3, `cli`/`curl` pods (pinned to the node labelled `e2e-infra=true`), the privileged hostPID DaemonSet used to signal an engine by its host PID |
| `values-operator.yaml` | Helm values: the ACR images of operator 0c0fb6e |
| `rep.yaml`, `shd.yaml` | the replicated cluster (1 master + 2 replicas) and the sharded cluster (3 x (1+1)), both on 1.1.3 at first |
| `fx.yaml` | a separate 3 x (1+1) cluster, **created on rc7** (the rc7 -> rc8 upgrade check), then the force-delete repro |
| `guard.yaml` | `g-yes` (rc8), `g-no` (rc8 with a KividbConfig `aof-ack-after-write no`), `g-113` (1.1.3): 1 master + 1 replica each |
| `loader-*.yaml`, `loader/`, `fakes3/` | rc4's acked-write loader (800 SET/s for rep/shd/fx, 400 for the guard clusters) and the in-memory S3 |
| `scen-upgrade.sh` | `spec.image` change under load; **`LD=<loader>`** picks the loader (new, for fx) |
| `scen-fxrepro.sh` | delete a master while listing, every 100 ms on its node, the kividb processes with that pod's HOSTNAME and the data dir's files; `SAMENODE=1` cordons the other nodes. **The summary now also keeps the engine's lock lines** (`in use by another process`, `free after`) |
| `run-fxloss.sh [n]` | **rc8:** n (6) tries, none stopped early: load, a BGSAVE on the target master, load, a same-node force delete; targets rotate over shards 0/1/2; then one same-node graceful delete |
| `scen-dbops.sh`, `scen-kill2.sh`, `scen-drain.sh`, `scen-reshard.sh`, `scen-guard.sh`, `scen-backup.sh` | the rc7 scenarios, unchanged |
| `run-rc8.sh` | the order of the run (see the README) |
| `rinfo.sh`, `mon.sh`, `monshd.sh`, `pingall.sh`, `repcheck.sh` | monitors run inside the `cli` pod |
| `capture.sh` | Events, operator log and pod watch, appended to files for the whole run |
| `build_results.py`, `summarize.py`, `titles.json`, `show.py` | turn the marks, Events and loader output into `results/scenarios.json`, `marks.csv`, `summary.csv`/`summary.md` |

Order: RG, Basic ACR, AKS (`az aks create --tier free --node-count 4 --node-vm-size Standard_D4s_v5
--network-plugin azure --network-plugin-mode overlay --attach-acr <acr>`); `D=... A=... bash build.sh <operator tree> 0c0fb6e <engine context>`;
replace `ACR` in the yaml files; `kubectl apply -f ns.yaml -f nd.yaml`; label one node `e2e-infra=true`;
`kubectl apply --server-side -f <operator tree>/charts/kividb-operator/crds/`;
`helm install kividb-operator <operator tree>/charts/kividb-operator -n kividb-system --create-namespace -f values-operator.yaml`;
apply `rep.yaml shd.yaml fx.yaml` and their loaders; `source env.sh; cpcli`; start `capture.sh`; `bash run-rc8.sh`. Then:

```
python build_results.py results/marks.log titles.json results results/events.log
python summarize.py results titles.json
```
