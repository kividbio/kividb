# Large sorted sets on the collection table (#875, #825, #770)

The measurements of the PR that puts the large form of a sorted set (past
`zset-max-listpack-entries` 128 or `zset-max-listpack-value` 64,
`OBJECT ENCODING skiplist`) on `CollectionTable`: one pool block a member
(`[score][length][member]`), looked up by name in the table, and an index
of the members' order that holds each member as the address of its block.
The index is still the `BTreeMap` the large form had (16-byte keys now, 24
before); a counted tree (#875, #984) replaces it behind `ScoreOrder`.

```sh
cargo build --release                       # rustc 1.100.0, as release.yml
benchmarks/1.3.0/large-zsets/run.sh d8s_v5 master=~/bins/master/kividb \
    branch=target/release/kividb redis=/opt/redis-8.10.2/src/redis-server
```

`bench.py` lists the tests (`mem`, `mastodon`, `shrink`, `fill`, `zscan`,
`digest`); `run.sh` starts each server fresh for each test, one at a time,
and appends to `results/<label>/results.jsonl`. Every number below is from
`results/d8s_v5` (Azure Standard_D8s_v5, 8 vCPUs, Intel Xeon Platinum
8370C; master `41005579` and the branch, both built there by rustc
1.100.0; Redis 8.10.2), one run each.

## Memory

`used_memory` (and RSS) each member added, once `used_memory` has stopped
falling, master / branch / Redis 8.10.2. Members are `m` and a number,
padded to 8, 16 or 32 bytes. One set of 3,000,000 members; 1,000,000
members in sets of 1,000 or 10,000 (one such set is tens of KB, which a
connection's buffers move as much). *In order*: member `i` at score `i`,
1,000 pairs a `ZADD`; *at random*: the members in a random order, each at a
random score; *reloaded*: the random sets again after `DUMP`, `DEL` and
`RESTORE`, which builds a set as a snapshot load does (KiviDB has no
`DEBUG RELOAD`; Redis's `RESTORE` is its `rdbLoadObject` too).

| members | bytes | in order | at random | reloaded |
|---:|---:|---|---|---|
| 1,000 | 8 | 124.5 / **76.4** / 68.0 (RSS 133.8 / 80.2 / 69.1) | 114.7 / **69.4** / 68.0 (RSS 123.4 / 72.1 / 67.6) | 124.5 / **60.4** / 68.1 (RSS 133.7 / 72.5 / 67.9) |
| 1,000 | 16 | 132.5 / **84.4** / 77.9 (RSS 148.6 / 86.8 / 77.7) | 122.7 / **77.4** / 77.9 (RSS 138.8 / 79.3 / 77.7) | 132.5 / **68.5** / 78.0 (RSS 148.8 / 79.4 / 78.5) |
| 1,000 | 32 | 148.4 / **100.3** / 100.2 (RSS 163.7 / 102.5 / 99.3) | 138.7 / **93.4** / 100.2 (RSS 154.4 / 95.3 / 99.9) | 148.4 / **84.4** / 100.2 (RSS 164.8 / 95.5 / 100.9) |
| 10,000 | 8 | 113.9 / **73.5** / 78.3 (RSS 121.8 / 74.7 / 77.9) | 103.8 / **66.4** / 78.3 (RSS 111.4 / 67.2 / 78.1) | 114.2 / **57.9** / 70.5 (RSS 122.6 / 68.4 / 78.8) |
| 10,000 | 16 | 121.9 / **81.5** / 88.5 (RSS 137.7 / 82.6 / 88.2) | 111.8 / **74.5** / 88.5 (RSS 127.4 / 75.2 / 88.2) | 121.6 / **65.3** / 80.6 (RSS 138.3 / 76.4 / 89.3) |
| 10,000 | 32 | 137.9 / **97.7** / 110.4 (RSS 153.7 / 98.8 / 110.0) | 127.9 / **90.4** / 110.4 (RSS 143.5 / 91.2 / 110.2) | 137.9 / **81.5** / 102.6 (RSS 154.0 / 92.7 / 109.0) |
| 3,000,000 | 8 | 107.9 / **72.0** / 75.2 (RSS 116.7 / 74.0 / 75.8) | 97.8 / **64.8** / 75.2 (RSS 106.0 / 66.4 / 75.1) | 107.9 / **55.9** / 69.1 (RSS 116.4 / 67.1 / 69.3) |
| 3,000,000 | 16 | 115.9 / **80.0** / 85.2 (RSS 132.7 / 77.5 / 85.2) | 105.8 / **72.8** / 85.2 (RSS 113.3 / 74.2 / 85.1) | 115.9 / **63.8** / 79.1 (RSS 123.7 / 64.7 / 79.6) |
| 3,000,000 | 32 | 131.9 / **96.0** / 107.3 (RSS 148.7 / 93.5 / 108.6) | 121.8 / **88.8** / 107.3 (RSS 135.7 / 94.0 / 107.2) | 131.9 / **79.9** / 101.2 (RSS 146.0 / 86.0 / 102.3) |

RSS is the process's growth over the load: in the reloaded rows of the
smaller sets it is over `used_memory` because the sets dropped for the
reload left the pool and the allocator pages that the restored sets did not
all take again.

### Where the bytes go

The table's part is exact (`MEMORY USAGE` less the index's estimate, which
`memory_layout::score_index` computes from the length alone); the index's
is what `used_memory` took beyond it. A member's entry is a pool block of
`8 + 1 + length` bytes rounded up to the class (24, 32 or 48 bytes here),
plus the table's slot and its share of the groups (about 12 bytes at
3,000,000, between 9 and 18 as the table fills and doubles).

| members | bytes | table | index, in order | index, at random | index, built whole |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 8 | 42.0 | 34.4 | 27.4 | 18.4 |
| 1,000 | 16 | 49.5 | 34.9 | 27.4 | 18.4 |
| 1,000 | 32 | 65.5 | 34.8 | 27.4 | 18.4 |
| 10,000 | 8 | 38.8 | 34.7 | 27.6 | 17.4 |
| 10,000 | 16 | 46.8 | 34.7 | 27.7 | 18.5 |
| 10,000 | 32 | 62.8 | 34.9 | 27.6 | 18.7 |
| 3,000,000 | 8 | 37.7 | 34.3 | 27.1 | 18.2 |
| 3,000,000 | 16 | 45.7 | 34.3 | 27.1 | 18.2 |
| 3,000,000 | 32 | 61.7 | 34.3 | 27.1 | 18.2 |

The index is a `BTreeMap` of 16-byte keys (score, entry address): nodes
half full when members come in order one at a time (each split at the
right edge leaves the left node half empty), about 70 % at random, full
when built in one go. That is the part a counted tree with full nodes
shrinks; the table is where it will stay.

### Mastodon's home feeds (#825)

20,000 sorted sets of 800 members, a member an 18-digit status id and its
score that id, each set by one `ZADD` of its 800 members in order:
`used_memory` a member 121.1 / **87.9** / 79.5, RSS 130.5 / **92.7** / 80.5
(1,848 / **1,341** / 1,213 MiB). Suite 18, whose feeds are built by its
mix of commands over time, measured 113.2 (v1.2.2) / **80.0** / 79.5 bytes
a member in the same session.

### Shrinking 3,000,000 members to 1,000

| | master | **branch** | Redis 8.10.2 |
|---|---|---|---|
| `ZREM`, 1,000 members a command: `used_memory` kept | 100.07 MiB | **0.12 MiB** | 36.14 MiB |
| the same, RSS kept | 105.2 MiB | **58.1 MiB** | 234.5 MiB |
| one `ZREMRANGEBYRANK`: `used_memory` kept | 0.17 MiB | **0.09 MiB** | 32.13 MiB |
| the same, RSS kept | 4.0 MiB | **59.7 MiB** | 185.1 MiB |
| the `ZREMRANGEBYRANK` itself | 2.46 s | **1.49 s** | 1.56 s |

The RSS the branch keeps is the pool's slabs that still hold one of the
members left (a slab goes back to the system when its last block is freed;
1,000 survivors are spread over about as many slabs). Active defrag does
not move the pool's blocks, as for large sets and hashes; moving them
needs the index to be told the new address, which a later step can do
through the same reference.

## Latency: one set filled to 3,000,000 members

One `ZADD z <random score> m:<random>` a command from one connection
(`redis-benchmark -c 1 -P 1`), `PING` from a second connection meanwhile.

| | worst `ZADD` | `ZADD` p99 | `PING` p99.9 | worst `PING` |
|---|---:|---:|---:|---:|
| master | 126.4 ms | 0.055 ms | 0.058 ms | 1.06 ms |
| **branch** | **3.06 ms** | 0.055 ms | 0.058 ms | 3.35 ms |
| Redis 8.10.2 | 2.28 ms | 0.055 ms | 0.066 ms | 1.61 ms |

Master's pause is its member table rehashed whole at a doubling; the table
moves a few members a write instead. The branch's worst `ZADD` and worst
`PING` are one event of about 3 ms (one run, the VM running a callgrind
measurement on another CPU meanwhile).

## `ZSCAN`: a full scan of 1,000,000 members

Every member returned once, checked; `server_cpu` from `/proc/<pid>/stat`.

| `COUNT` | master: calls / wall / server CPU | **branch** | Redis 8.10.2 |
|---:|---|---|---|
| 10 | 1,025 / 22.8 s / 20.16 s | **83,965 / 9.4 s / 1.79 s** | 95,458 / 10.7 s / 2.24 s |
| 100 | 1,025 / 22.7 s / 20.08 s | **9,814 / 3.4 s / 0.44 s** | 9,953 / 4.1 s / 0.95 s |
| 1,000 | 1,000 / 22.4 s / 19.67 s | **999 / 2.7 s / 0.28 s** | 1,000 / 3.1 s / 0.68 s |

## Digest

`DEBUG DIGEST-VALUE` of a 1,000,000-member set at random scores and of a
small one: the same on master and the branch.
