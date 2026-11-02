# Harness

Everything used for the core string benchmark, in the order it runs. All
scripts are plain bash and run as the VM's login user; nothing here holds a
secret or a path from the machine that started the run.

| Script | Where | What |
|---|---|---|
| `create-vms.sh` | your machine (`az` signed in) | resource group, proximity placement group, network, the server and two load generators |
| `setup-server.sh` | server | Redis, Valkey, Dragonfly, Garnet, KiviDB 1.1.3, Rust; OS limits |
| `setup-client.sh` | each load generator | memtier_benchmark 2.5.2, redis-cli, sysstat |
| `fetch-kividb.sh` | server | a KiviDB build as `~/bin/kividb-<name>`: release asset, or built from the tag like `release.yml` |
| `engine.sh` | server | start/stop one engine with its flags; appends the command line and version to `~/logs/launch.log` |
| `sample.sh` | all three | CPU (user / kernel) and NIC bytes over the middle of a scenario |
| `run-suite.sh` | load generator 1 | loads the key space, runs every scenario from both load generators at once |
| `run-all.sh` | load generator 1 | the io-threads sweep, then phase A (competitors) and phase B (KiviDB rc4 / 1.1.3 interleaved) |
| `summarize.py` | anywhere with Python + `hdrhistogram` | `runs.csv` and `summary.csv` (merged latency histograms) |
| `charts.py` | anywhere with Python + matplotlib | the SVG charts (`MAIN_ENGINES` picks and orders the engines) |
| `tables.py` | anywhere with Python | the Markdown tables of the suite's README |

## Steps

```bash
# 1. machines (64 vCPU of quota)
RG=kividb-rc4-core LOCATION=eastasia SSH_PUBLIC_KEY_FILE=~/.ssh/id_ed25519.pub \
  SSH_CIDR="$(curl -s https://api.ipify.org)/32" bash create-vms.sh

# 2. copy this folder to ~/h on all three machines, then
ssh srv 'bash ~/h/setup-server.sh && bash ~/h/fetch-kividb.sh v1.2.0-rc4 rc4'
ssh cl1 'bash ~/h/setup-client.sh'; ssh cl2 'bash ~/h/setup-client.sh'
# cl1 must reach srv (10.40.0.10) and cl2 (10.40.0.12) over ssh with a key of its own.

# 3. the run (about 3.5 hours)
ssh cl1 'PHASES="sweep A B" REDIS_IO=24 VALKEY_IO=24 nohup bash ~/h/run-all.sh > run-all.out 2>&1 &'

# 4. summarise and draw (copy ~/results from cl1 first)
python3 summarize.py results && python3 charts.py results charts

# 5. tear down
az group delete -n kividb-rc4-core --yes
```

`REDIS_IO` / `VALKEY_IO` are what the sweep in step 3 chose; run the sweep
alone first (`PHASES=sweep`) if you want to choose for your own machines.
