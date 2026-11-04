# Harness

Everything used for the core string benchmark, in the order it runs. All
scripts are plain bash and run as the VM's login user; nothing here holds a
secret or a path from the machine that started the run.

| Script | Where | What |
|---|---|---|
| `create-vms.sh` | your machine (`az` signed in) | resource group, proximity placement group, network, the server and two load generators |
| `setup-server.sh` | server | Redis, Valkey, Dragonfly, Garnet, KiviDB 1.1.3, Rust; OS limits |
| `setup-client.sh` | each load generator | memtier_benchmark 2.5.2, redis-cli, sysstat |
| `fetch-kividb.sh` | server | a KiviDB build as `~/bin/kividb-<name>`: release asset, or built from a tag or a 40-character commit like `release.yml` |
| `engine.sh` | server | start/stop one engine with its flags; appends the command line and version to `~/logs/launch.log` |
| `sample.sh` | all three | CPU (user / kernel) and NIC bytes over the middle of a scenario |
| `run-suite.sh` | load generator 1 | loads the key space, runs every scenario from both load generators at once |
| `run-all.sh` | load generator 1 | phase K (1.2.0, rc4 and 1.1.3 interleaved), phase A (competitors), phase X (more 1.2.0 / rc4 rounds), a Redis drift round; the io-threads sweep on request |
| `profile.sh` | load generator 1 | one KiviDB build under one GET scenario from both load generators, with `perf record -g` of the server process in the middle |
| `summarize.py` | anywhere with Python + `hdrhistogram` | `runs.csv` and `summary.csv` (merged latency histograms) |
| `charts.py` | anywhere with Python + matplotlib | the SVG charts (`MAIN_ENGINES` picks and orders the engines) |
| `tables.py` | anywhere with Python | the Markdown tables of the suite's README |

## Steps

```bash
# 1. machines (64 vCPU of quota)
RG=kividb-core LOCATION=eastasia SSH_PUBLIC_KEY_FILE=~/.ssh/id_ed25519.pub \
  SSH_CIDR="$(curl -s https://api.ipify.org)/32" bash create-vms.sh

# 2. copy this folder to ~/h on all three machines, then
ssh srv 'bash ~/h/setup-server.sh'
ssh srv 'bash ~/h/fetch-kividb.sh 75482abbb258bafe84c0e9d05fc1c904abb19cfd cand build'
ssh srv 'bash ~/h/fetch-kividb.sh v1.2.0-rc4 rc4 build'
ssh cl1 'bash ~/h/setup-client.sh'; ssh cl2 'bash ~/h/setup-client.sh'
# cl1 must reach srv (10.40.0.10) and cl2 (10.40.0.12) over ssh with a key of its own.

# 3. the run (about 3.5 hours), then two more 1.2.0 / rc4 rounds (30 min)
ssh cl1 'PHASES="K A X drift" REDIS_IO=24 VALKEY_IO=24 nohup bash ~/h/run-all.sh > run-all.out 2>&1 &'
ssh cl1 'PHASES=X XROUNDS="5 6" nohup bash ~/h/run-all.sh > run-x56.out 2>&1 &'

# 4. summarise and draw
ssh cl1 'cd ~ && python3 h/summarize.py results && python3 h/summarize.py results-drift'
ssh cl1 'cd ~ && MAIN_ENGINES="kividb-cand kividb-113 redis@24 redis-cluster valkey@24 dragonfly garnet" python3 h/charts.py results charts'
ssh cl1 'cd ~ && python3 h/tables.py results > tables.md'

# 5. optional: profile 1.2.0 and rc4 under GET at depth 1, 300 connections
ssh cl1 'for e in kividb-cand kividb-rc4 kividb-cand kividb-rc4; do bash ~/h/profile.sh $e 1 10; done'

# 6. tear down
az group delete -n kividb-core --yes
```

`REDIS_IO` / `VALKEY_IO` are the io-threads counts. The 1.2.0-rc4 run picked
24 for both with a sweep on the same VM sizes, CPU and kernel; run
`PHASES=sweep` first if you want to choose for your own machines.
