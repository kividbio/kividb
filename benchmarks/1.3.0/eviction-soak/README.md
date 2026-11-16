# Eviction soak: no OOM while there are keys to evict (#520)

Under an `allkeys-*` policy Redis refuses a write only when nothing is left
to evict. KiviDB refused one now and then while eviction was working: the
guard that stops eviction when it gives no memory back (#284) judged that by
memory use alone, and other threads' writes kept memory use as high while
the keys evicted were freed. The guard now asks what the evicting thread itself gave back (see
`fruitless` in `src/eviction.rs`). And a write that found nothing left to
evict while another thread was still freeing a large collection it had
evicted was refused; under `allkeys-*` it is now let through when what is
being freed brings memory under the limit by itself (`room_on_its_way`).

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
`MAXMEMORY=512mb`; master is 34099497, the fix is #824; each run is ten
minutes after the limit is set, master and replica on the same VM.

| build | policy | OOM replies | guard trips logged | writes | `peak_over` | `peak_over_held` | pipelined ops/s (p99 ms) |
|---|---|---:|---:|---:|---:|---:|---:|
| master | `allkeys-lfu` | 2 | 3 | 55.3M | 2.87 MB | 0.18 MB | 155,447 (11.8) |
| fix | `allkeys-lfu` | **0** | 0 | 55.2M | 2.08 MB | 0.16 MB | 159,055 (10.6) |
| master | `allkeys-lru` | 3 | 4 | 52.8M | 0.38 MB | 0.15 MB | 155,571 (11.6) |
| fix | `allkeys-lru` | **0** | 0 | 51.1M | 0.67 MB | 0.22 MB | 151,903 (12.2) |

"Guard trips logged" counts the "did not go down" warning, which is logged
at most once a minute, so it is a lower bound. The OOM count varies from
run to run on master: an earlier ten-minute `allkeys-lfu` run on the same
VM size refused 214 writes, another 13.

`peak_over_held` is the overshoot that lasted 100 ms or more. `peak_over`
counts single samples too: in other runs a sample alone was 95-173 MB over,
during an AOF checkpoint, and 100 ms later the server was back at the
limit. That is the two counts read apart, not memory let in, which
eviction takes seconds to take back out. A build that logged every write
let through with nothing left to evict logged none in ten minutes of
`allkeys-lru`. The soak's one big list never had to be freed while a write
waited.

Some OOM replies on master come with no guard line. A build with that path
logged showed what they were: writes that found every key evicted (the
soak's `redis-benchmark` pushes to one list, `mylist`, that grows to
100+ MB and crowds the other keys out) while the list's own eviction was
still being freed on another thread. The list growing and being evicted
makes the key count swing (from ~500k down to a few thousand) on both
builds. That is the workload, not a regression.

Tolerance: any OOM reply under an `allkeys-*` policy is a regression, and
so is a `peak_over_held` above a few MB (the largest command and the
collections being freed at one moment; see `room_on_its_way` in
`src/eviction.rs`). The throughput columns are for orientation only
(client and servers share the VM; runs differ by a few percent).
