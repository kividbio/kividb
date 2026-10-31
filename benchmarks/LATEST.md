# Benchmark Results

**Branch:** `master` | **Commit:** `b002a9e9` | **Run:** [41253233189](https://github.com/kividbio/kividb/actions/runs/41253233189) | **Generated:** 2026-10-31 23:57 UTC

> Methodology: [memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark) on an Azure Standard_D32ps_v5 (32 vCPU, arm64).
> Client on a separate Standard_D32ps_v5 in the same proximity placement group. Stores benchmarked sequentially,
> each using every core; memtier runs one thread per client core, 5 connections each.
> "KiviDB over TLS" is the `lua,tls` build measured over TLS.
> Full runbook: [kivi-benchmark](https://github.com/kividbio/kivi-benchmark)

## Results

| Test | KiviDB | Dragonfly v2.0.2 | Redis 8.10.2 | KiviDB vs Dragonfly | KiviDB vs Redis | KiviDB over TLS |
|------|-------|-----------------|----------|---------------------|-----------------|-----------------------|
| Pipelined write (`--ratio 1:0 -c 5 --pipeline=10`) | ~6.3M | ~4.3M | ~527K | 1.45x | 11.99x | ~361K |
| Pipelined read (`--ratio 0:1 -c 5 --pipeline=10`) | ~7.2M | ~4.7M | ~711K | 1.53x | 10.10x | ~730K |
| Mixed (`--ratio 1:1 -c 5 --pipeline=10`) | ~6.8M | ~4.4M | ~637K | 1.54x | 10.67x | ~414K |
