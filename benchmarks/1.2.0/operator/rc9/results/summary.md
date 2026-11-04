| Scenario | Engine | What | Acked writes checked | Lost | Failed SETs | Impact (s) | Slowest SET (ms) |
|---|---|---|---:|---:|---:|---:|---:|
| upgrade-shd | rc9 | engine upgrade 1.2.0-rc8 -> 1.2.0-rc9 (sharded) | 193,264 | 0 | 0 | 0 | 208 |
| restart-rc9 | rc9 | rolling restart (KividbDbOps), sharded | 342,336 | 0 | 0 | 0 | 267 |
| failover-rc9 | rc9 | CLUSTER FAILOVER on every shard's replica in turn | 436,944 | 0 | 0 | 0 | 268 |
| graceful1 | rc9 | graceful delete of a shard master, same node, try 1 | 545,314 | 0 | 0 | 0 | 70 |
| graceful2 | rc9 | graceful delete of a shard master, same node, try 2 | 651,650 | 0 | 0 | 0 | 30 |
| forcedel1 | rc9 | same-node force delete of a shard master (#303), try 1 | 778,118 | 0 | 4172 | 17 | 5,419 |
| forcedel2 | rc9 | same-node force delete of a shard master (#303), try 2 | 907,319 | 0 | 1407 | 17 | 5,002 |
| forcedel3 | rc9 | same-node force delete of a shard master (#303), try 3 | 1,033,371 | 0 | 4332 | 18 | 5,266 |
| sigkill-rc9 | rc9 | kill -9 of a shard master's engine, restarts in place | 1,075,875 | 0 | 1384 | 7 | 5,003 |
| reshard-up-rc9 | rc9 | spec.shards 3 -> 4 under load | 1,139,275 | 0 | 0 | 0 | 36 |
| reshard-down-rc9 | rc9 | spec.shards 4 -> 3 under load | 1,183,723 | 0 | 0 | 0 | 45 |
| guard-yes | rc9 | #42 guard: rc9, aof-ack-after-write yes | 741,022 | 0 | 24770 | 63 | 5,002 |
| guard-no | rc9 | #42 guard: rc9, aof-ack-after-write no | 834,760 | 0 | 25156 | 63 | 5,010 |
