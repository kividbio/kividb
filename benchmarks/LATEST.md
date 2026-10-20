# Benchmark Results

**Branch:** `master` | **Commit:** `4bcd0e50` | **Run:** [38844611830](https://github.com/kividbio/kividb/actions/runs/38844611830) | **Generated:** 2026-10-16 03:39 UTC

> Methodology: [memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark) on an Azure Standard_D32ps_v5 (32 vCPU, arm64).
> Client on a separate Standard_D32ps_v5 in the same virtual network. Stores benchmarked sequentially.
> Full runbook: [kivi-benchmark](https://github.com/kividbio/kivi-benchmark)

> **This run is out of date.** It is the first run on Azure, against Dragonfly
> 1.37 and Redis 8.6; the benchmark now compares Dragonfly 2.0.2 and Redis
> 8.10.2, and the next run replaces this file. The last column was labelled
> "KiviDB with all features": it is KiviDB (Lua and TLS built in) measured over
> **TLS**, which is why it is far lower. For the current figures, see the
> [README](../README.md#performance).

## Results

| Test | KiviDB | Dragonfly v1.37 | Redis 8.6 | KiviDB vs Dragonfly | KiviDB vs Redis | KiviDB over TLS |
|------|-------|-----------------|----------|---------------------|-----------------|-----------------------|
| Pipelined write (`--ratio 1:0 -c 5 --pipeline=10`) | ~6.5M | ~2.8M | ~505K | 2.32x | 12.78x | ~375K |
| Pipelined read (`--ratio 0:1 -c 5 --pipeline=10`) | ~8.1M | ~3.6M | ~600K | 2.24x | 13.45x | ~712K |
| Mixed (`--ratio 1:1 -c 5 --pipeline=10`) | ~6.4M | ~3.2M | ~562K | 2.03x | 11.37x | ~395K |
