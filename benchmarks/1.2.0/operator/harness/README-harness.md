# Harness: operator end-to-end on AKS

Everything here ran from a workstation with `az`, `kubectl`, `helm` and Python 3
(matplotlib). Nothing is built locally: images are built in Azure Container Registry
with `az acr build`, engine images come from GHCR.

| File | What it does |
|---|---|
| `provision.sh` | resource group, ACR, AKS (4 nodes); writes a kubeconfig next to it |
| `build.sh` | builds the operator (manager, agent, GUI) from a checkout, the loader and the in-cluster S3 in ACR |
| `env.sh` | shell helpers the scenario scripts use (`k`, `q`, `mark`, `begin`, `chk2`, `hsig`, `masters`, ...). Set `D` (work dir), `A` (ACR login server), `ENG` (engine image) |
| `ns.yaml`, `nd.yaml` | namespace, in-cluster S3, `cli`/`curl` pods (pinned to the node labelled `e2e-infra=true`), and the privileged hostPID DaemonSet used to signal an engine by its host PID |
| `rep.yaml`, `shd.yaml` | the replicated cluster (1 master + 2 replicas) and the sharded cluster (3 shards x (1+1)), both on 1.1.3 at first |
| `loader-rep.yaml`, `loader-shd.yaml` | the acked-write loader for each cluster |
| `loader/` | the loader (Go, go-redis v9): 800 SET/s open loop, 5 s deadline per SET, records every acknowledged write in a bitset; `/verify` reads every acknowledged key back; `/timeline` gives per-second ok/failed/slowest |
| `fakes3/` | an in-memory S3 for the backup tests (MinIO images were not pullable) |
| `scen-*.sh` | one script per scenario kind: `upgrade`, `dbops` (rolling restart), `stop` (SIGSTOP), `kill2` (in-place SIGKILL or force delete, with a `CLUSTER SLOTS` capture), `del` (graceful delete of a master), `repdel` (graceful delete of a replica), `drain`, `reshard`, `backup`, `split` (two pods of different shards deleted at once, then every node's view of every peer's address) |
| `run-baseline2.sh`, `run-rc4.sh`, `run-rc4b.sh` | the order the scenarios ran in: 1.1.3 rows, then the upgrade and 1.2.0-rc4 rows; `run-rc4b.sh` is the rest of the rc4 run after it was stopped at delete-rc4 (see the main README) |
| `pingall.sh`, `monshd.sh`, `mon.sh`, `repcheck.sh` | monitors run inside the `cli` pod (role, keys, `cluster nodes` every 2-5 s) |
| `capture.sh` | Events, operator log and pod watch, appended to files for the whole run |
| `build_results.py`, `summarize.py`, `charts.py` | turn the run's marks, Events and loader output into `results/` and `charts/` |

Order of a run: `provision.sh`, `build.sh`, `kubectl apply -f ns.yaml -f nd.yaml`, label one
node `e2e-infra=true`, `helm install` operator 0.6.1 from its release chart, apply
`rep.yaml` and `loader-rep.yaml`, upgrade the operator (CRDs with `kubectl apply
--server-side`, then `helm upgrade` with the ACR images), apply `shd.yaml` and
`loader-shd.yaml`, then `run-baseline2.sh` and `run-rc4.sh` (or `run-rc4b.sh` for the part after delete-rc4). Then:

```
python build_results.py work/marks.log titles.json results work/events.log
python summarize.py results titles.json
python charts.py results charts
```
