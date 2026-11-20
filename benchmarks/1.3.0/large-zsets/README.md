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
8370C; master `41005579` and the branch built there with rustc 1.100.0,
Redis 8.10.2), one run each.

@@TABLES@@
