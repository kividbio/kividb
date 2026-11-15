# Eviction soak: no OOM while there are keys to evict (#520)

Under an `allkeys-*` policy Redis refuses a write only when nothing is left
to evict. KiviDB refused one now and then while eviction was working: the
guard that stops eviction when it gives no memory back (#284) judged that by
memory use alone, and other threads' writes kept memory use as high while
the keys evicted were freed. The guard now asks what the evicting thread itself gave back (see
`fruitless` in `src/eviction.rs`). And a write that found nothing left to
evict while another thread was still freeing the last key it evicted (a
large list) was refused; it is now let through (`freeing_elsewhere`).

`soak.sh` runs the shape of the report on one VM: a master with AOF
`everysec` and a replica with AOF, memtier `SET`/`GET` pipelined (1:2) and
not, `HSET` + `HEXPIRE` traffic, `redis-benchmark` lists, sets, hashes and
sorted sets, and `maxmemory` set at runtime once the dataset is past it.

```sh
cargo build --release
DURATION=600 benchmarks/1.3.0/eviction-soak/soak.sh target/release/kividb fix
```

It prints `oom=` (`errorstat_OOM` after the limit was set), `fruitless_logs=`
(the guard's "did not go down" log lines), `evicted=` and the writes. A run
with `oom` above 0 under an `allkeys-*` policy is a regression.

## Recorded

2026-11-15, one `Standard_D4s_v5` (4 vCPU), `DURATION=600`,
`MAXMEMORY=512mb`, master 200530ff against the fix; each run is ten
minutes after the limit is set, master and replica on the same VM.

| build | policy | OOM replies | guard trips logged | writes | evicted | pipelined SET/GET ops/s (p99 ms) |
|---|---|---:|---:|---:|---:|---:|
| master | `allkeys-lfu` | 13 | 6 | 55.3M | 34.6M | 161,913 (10.7) |
| fix | `allkeys-lfu` | **0** | 0 | 56.3M | 35.3M | 165,283 (10.8) |
| master | `allkeys-lru` | 2 | 4 | 52.2M | 32.1M | 155,284 (11.6) |
| fix | `allkeys-lru` | **0** | 0 | 54.6M | 32.6M | 157,047 (11.8) |

"Guard trips logged" counts the "did not go down" warning, which is logged
at most once a minute, so it is a lower bound. Some OOM replies on master
came with no guard line. A build with that path logged showed what they
were: writes that found every key evicted (the soak's `redis-benchmark`
pushes to one list, `mylist`, that grows to 100+ MB and crowds the other
keys out) while the list's own eviction was still being freed on another
thread. The list growing and being evicted makes the key
count swing (from ~500k down to a few thousand) on both builds; that is the
workload, not a regression.

Tolerance: any OOM reply under an `allkeys-*` policy is a regression. The
throughput columns are for orientation only (client and servers share the
VM; runs differ by a few percent).

