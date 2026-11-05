# Benchmark machines (Azure)

The server + client pair that `.github/workflows/benchmark.yml`,
`solo-benchmark.yaml` and `vector-benchmark.yml` run on: two arm64 Azure
VMs in one virtual network, created at the start of a run and deleted at the
end of it.

```
infra/benchmark-azure/
├── create.sh            # network, two VMs, auto-shutdown; prints IPs
├── destroy.sh           # deletes everything a run made, found by name
└── bootstrap/
    ├── server.sh        # OS prep + warm Dragonfly/Redis
    └── client.sh        # memtier_benchmark + Rust + optional GitHub runner
```

## What a run creates

All in the resource group `kividb-benchmark` (`centralindia`), all named
`kvb-<run id>-<attempt>-...`:

- a virtual network with one subnet, and a network security group on it
  that lets in SSH (from `SSH_CIDR`, default anywhere). Traffic between the
  two machines is open by the group's default rules.
- a server VM (default `Standard_D32ps_v5`, 32 vCPU) and a client VM
  (default `Standard_D32ps_v5`), Ubuntu 22.04 arm64, accelerated networking,
  each with a static public IP. The workflows put both in a proximity
  placement group (`use_proximity_group`, on by default; `create.sh` alone
  needs `USE_PROXIMITY_GROUP=true`): one datacenter rather than anywhere in
  the region, so the network adds as little as it can to the latency
  numbers. If Azure cannot place the pair together, the create step fails
  with an allocation error; rerun, or turn the input off.
- an auto-shutdown schedule on each VM, five hours out, in case teardown
  never runs. Shut down is not deleted: the disks and IPs still cost a
  little until `destroy.sh` runs.

The region's arm64 quota is 65 vCPU, so one pair at a time: every benchmark
workflow shares the `benchmark-global` concurrency group.

With `BOOTSTRAP=true` (the default) the server gets Dragonfly and Redis, and
the client builds memtier_benchmark and installs Rust. With
`REGISTER_RUNNER=true` the client also registers as an **ephemeral**
self-hosted runner (one job, then it deregisters) labelled `run-<run id>`, so
a run's benchmark job can only land on the machine made for that run.
`vector-benchmark.yml` uses `BOOTSTRAP=false`: it ships its own binaries over
SSH from GitHub-hosted runners.

`destroy.sh` deletes every resource whose name contains `kvb-<run id>-`, so
it works whether create finished, failed half way or was cancelled, and it
clears every attempt of a re-run. No state passes between jobs.

## How CI signs in

GitHub's OIDC token, exchanged for an Azure token by `azure/login`: no Azure
secret is stored anywhere.

- App registration `kividb-benchmark-github`, with a federated credential
  for `repo:kividbio/kividb:ref:refs/heads/master`. **Only runs dispatched
  from master can sign in**; to measure another branch, dispatch from master
  and set the workflow's `ref` input.
- Its service principal is **Contributor on the `kividb-benchmark` resource
  group only**. It cannot see or change anything else in the subscription.

| Name | Kind | What |
|------|------|------|
| `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID` | variable | identify the app registration and subscription (not secret) |
| `BENCHMARK_SSH_PRIVATE_KEY` | secret | its public half goes on both VMs each run |
| `GH_RUNNER_PAT` | secret | fine-grained PAT, **Administration: write** on this repo, used only to mint short-lived runner registration tokens |

## By hand

Signed in with `az login`, from Git Bash or Linux:

```bash
cd infra/benchmark-azure
RUN_ID=manual-1 SSH_PUBLIC_KEY="$(cat ~/.ssh/id_ed25519.pub)" \
  SSH_CIDR="$(curl -s https://api.ipify.org)/32" bash create.sh
# ... ssh ubuntu@<server_public_ip> ...
RUN_PREFIX=manual bash destroy.sh
```

From Git Bash on Windows, run `destroy.sh` with `MSYS_NO_PATHCONV=1`, or
Git Bash rewrites the `/subscriptions/...` ids into Windows paths.

Startup logs on the VMs: `/var/log/user-data-server.log` and
`/var/log/user-data-client.log`; the runner's is
`/home/ubuntu/actions-runner.log`. Without SSH:
`az vm run-command invoke -g kividb-benchmark -n <vm> --command-id RunShellScript --scripts "tail -n 80 /var/log/user-data-client.log"`.

To check nothing is left: `az resource list -g kividb-benchmark -o table`
should be empty between runs.
