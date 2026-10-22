# Benchmark Results

**Branch:** `master` | **Commit:** `a1f5c734` | **Run:** [39861995358](https://github.com/kividbio/kividb/actions/runs/39861995358) | **Generated:** 2026-10-22 22:23 UTC

> Methodology: [memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark) on an Azure Standard_D32ps_v5 (32 vCPU, arm64).
> Client on a separate Standard_D32ps_v5 in the same proximity placement group. Stores benchmarked sequentially,
> each using every core; memtier runs one thread per client core, 5 connections each.
> "KiviDB over TLS" is the `lua,tls` build measured over TLS.
> Full runbook: [kivi-benchmark](https://github.com/kividbio/kivi-benchmark)

## Results

| Test | KiviDB | Dragonfly v2.0.2 | Redis 8.10.2 | KiviDB vs Dragonfly | KiviDB vs Redis | KiviDB over TLS |
|------|-------|-----------------|----------|---------------------|-----------------|-----------------------|
| Pipelined write (`--ratio 1:0 -c 5 --pipeline=10`) | ~6.2M | ~2.5M | ~536K | 2.51x | 11.61x | ~373K |
| Pipelined read (`--ratio 0:1 -c 5 --pipeline=10`) | ~7.3M | ~4.7M | ~715K | 1.56x | 10.21x | ~770K |
| Mixed (`--ratio 1:1 -c 5 --pipeline=10`) | ~6.7M | ~4.4M | ~639K | 1.53x | 10.55x | ~437K |
