# Benchmark Results

**Branch:** `master` | **Commit:** `a1f5c734` | **Run:** [39862031846](https://github.com/kividbio/kividb/actions/runs/39862031846) | **Generated:** 2026-10-22 22:45 UTC

> Methodology: [memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark) on an Azure Standard_D32ps_v5 (32 vCPU, arm64).
> Client on a separate Standard_D32ps_v5 in the same proximity placement group. Stores benchmarked sequentially,
> each using every core; memtier runs one thread per client core, 5 connections each.
> "KiviDB over TLS" is the `lua,tls` build measured over TLS.
> Full runbook: [kivi-benchmark](https://github.com/kividbio/kivi-benchmark)

## Results

| Test | KiviDB | Dragonfly v2.0.2 | Redis 8.10.2 | KiviDB vs Dragonfly | KiviDB vs Redis | KiviDB over TLS |
|------|-------|-----------------|----------|---------------------|-----------------|-----------------------|
| Pipelined write (`--ratio 1:0 -c 5 --pipeline=10`) | ~6.1M | ~2.7M | ~512K | 2.23x | 11.87x | ~350K |
| Pipelined read (`--ratio 0:1 -c 5 --pipeline=10`) | ~7.0M | ~4.0M | ~672K | 1.76x | 10.40x | ~691K |
| Mixed (`--ratio 1:1 -c 5 --pipeline=10`) | ~6.6M | ~4.3M | ~608K | 1.52x | 10.80x | ~394K |
