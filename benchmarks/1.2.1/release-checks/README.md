# KiviDB 1.2.1: release checks on the final code

Every check and benchmark rerun on the code 1.2.1 ships, 2026-11-12. The build under test was made from a tree whose code is that of commit `de43b81e` apart from the version string (the suite READMEs call it `35a413c0`, the local commit it was cut from), built the way `release.yml` builds a release (`amazonlinux:2023`, rustc 1.99.1, fat LTO, default features unless a suite says otherwise). Each suite compares it with the 1.2.0 release binaries, checked against `SHA256SUMS.txt`, and with its own earlier 1.2.1 run where there is one.

**Result: everything passes, nothing regressed, no new bugs.**

| Folder | What ran | Result |
|---|---|---|
| [functional](functional/README.md) | expiry, persistence, replication and cluster checks; the Redis Tcl suite on both I/O models; snapshots, AOF and full sync across 1.2.0 and 1.2.1 | all pass; 0 acknowledged writes lost anywhere; Tcl suite identical to the earlier pass in all 41 units |
| [vector](vector/README.md) | the vector suite on all four datasets, 3 alternating passes | 0.979-1.009x of 1.2.0's q/s at every point; recall, memory per vector and ingest level |
| [memory](memory/README.md) | bytes per key, growth to 50M keys, `BGSAVE` and `BGREWRITEAOF` under load, eviction during a snapshot (#284), active defrag, 30M-key snapshot load times | level with 1.2.0; 1.2.1 loads a 30M-key snapshot about 17% faster, with 2.4% less peak memory |
| [baremetal](baremetal/README.md) | the README headline on two c7gn.metal (AWS), 4 rounds per build | GET p16 21.19M vs 21.33M ops/s, SET p16 18.04M vs 17.83M; p99 level |
| [operator-aks](operator-aks/README.md) | kividb-operator master on AKS: upgrade from 1.2.0 under load, master deletes and kills, a hung master, rolling restarts, resharding 3 -> 4 -> 3, backup and restore | 27 scenarios, 0 acknowledged writes lost |

## Headlines

| | 1.2.1 | 1.2.0 |
|---|---:|---:|
| `GET` / `SET`, pipeline 16, 32 cores bare metal | 21.19M / 18.04M ops/s | 21.33M / 17.83M |
| p99 `GET` / `SET`, pipeline 16 | 0.37 / 0.45 ms | 0.36 / 0.45 ms |
| Bytes per key, warefeats (RSS / `used_memory`) | 148.9 / 146.9 | 148.9 / 146.9 |
| `BGSAVE` at 100k writes/s: memory added / worst write | 3.16 GiB / 202 ms | 3.15 GiB / 204 ms |
| Load a 30M x 1 KB snapshot: time / peak RSS | 83.4 s / 37.06 GiB | 100.5 s / 37.97 GiB |
| Vector, dbpedia 1M, ef 128, 100 clients | 6,317 q/s, recall 0.9605 | 6,347 q/s, recall 0.9609 |

Each folder holds its README (hardware, exact builds and SHA-256s, method, spread, caveats), its harness and its raw data in `results/raw.tar.gz` (`tar xzf results/raw.tar.gz -C results`). Machine addresses and local paths in the harness and raw data are replaced with placeholders.
