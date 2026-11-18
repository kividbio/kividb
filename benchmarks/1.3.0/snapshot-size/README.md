# Snapshot size (#879)

What `rdbcompression yes` (compressed snapshots, the 1.3.0 default) changes:
the snapshot's size, the save's wall time, CPU and peak memory, the load, a
full sync's time and the master's memory during it, and the latency of
writes during a save. Against v1.2.2, master before the change, the change
with `rdbcompression no` and `yes`, and Redis 8.10.2.

The regression gate is suite `23-snapshot-size` of
[the regression suite](../regressions/README.md); this harness is the fuller
measurement behind the CHANGELOG and `docs/PERFORMANCE.md`.

## Setup

One Azure `Standard_D4s_v5` (4 vCPUs, 16 GiB), Ubuntu 24.04. The server on
CPUs 0-1 (`taskset`, `--threads 2`), clients and the replica on 2-3; one server
at a time. Datasets (`gen.py`, fixed seeds; the issue's with memtier):

| name | what |
|---|---|
| `issue` | 6M keys `k:<n>` of 150 B, memtier's default payload, as in #879 |
| `text6m` | 6M strings of 16-64 B: words, JSON-like records, ids |
| `rand6m` | 6M strings of 16-64 random bytes |
| `coll` | 1M hashes of 10 fields, 200k sets of 20 integers, 200k sorted sets and 200k lists of 20 |

Each is loaded once into v1.2.2 and saved (`bench.py seed`); every KiviDB
variant starts from that file and saves it in its own form. Per variant: a
first `BGSAVE`, then three (median): wall time, CPU of the process, `VmHWM`
and PSS growth; the load time of its own file (best of 2); a full sync to an
empty replica of the same build (time until the replica is up with every
key, and the master's `VmHWM` growth); then memtier at 50 % writes, 1,200
requests/s a connection, for 30 s with a `BGSAVE` at 8 s: the worst p99 and
p99.9 of the seconds the save overlaps. Redis runs from the `redis:8.10.2`
image with `--save ""`; its save is a fork, whose memory is not in these
counters.

```sh
# binaries: V122 (release tarball), MASTER, CANDIDATE, INLINE (MAX_HELPERS = 0)
for ds in issue text6m rand6m coll; do python3 bench.py seed $ds; done
./run.sh                       # results.jsonl
python3 compat.py              # the cross-version matrix (v1.2.2 against CANDIDATE)
```

## Results

`results/d4s_v5-2026-11-18/`: `summary.md` (the tables), and `raw.tar.gz` with
`results.jsonl` (every row), the run's log and the compatibility matrix's
output.
