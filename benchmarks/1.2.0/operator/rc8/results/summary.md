| Scenario | Engine | What | Acked writes checked | Lost | Failed SETs | Impact (s) | Slowest SET (ms) |
|---|---|---|---:|---:|---:|---:|---:|
| upgrade-rep | rc8 | engine upgrade 1.1.3 -> 1.2.0-rc8 (replicated) | 145,640 | 0 | 0 | 0 | 187 |
| upgrade-shd | rc8 | engine upgrade 1.1.3 -> 1.2.0-rc8 (sharded) | 286,072 | 0 | 0 | 0 | 110 |
| upgrade-fx | rc8 | engine upgrade 1.2.0-rc7 -> 1.2.0-rc8 (sharded fx) | 422,452 | 0 | 36 | 1 | 5,150 |
| fx-loss1 | rc8 | fx: same-node force delete of a shard master, try 1 | 542,913 | 0 | 6899 | 27 | 5,002 |
| fx-loss2 | rc8 | fx: same-node force delete of a shard master, try 2 | 653,197 | 0 | 6904 | 27 | 5,004 |
| fx-loss3 | rc8 | fx: same-node force delete of a shard master, try 3 | 764,008 | 0 | 6805 | 27 | 5,038 |
| fx-loss4 | rc8 | fx: same-node force delete of a shard master, try 4 | 880,247 | 0 | 1401 | 7 | 5,002 |
| fx-loss5 | rc8 | fx: same-node force delete of a shard master, try 5 | 991,150 | 0 | 6849 | 27 | 5,001 |
| fx-loss6 | rc8 | fx: same-node force delete of a shard master, try 6 | 1,101,612 | 0 | 6890 | 27 | 5,001 |
| fx-graceful | rc8 | fx: graceful delete of a shard master, same node | 1,186,212 | 0 | 0 | 0 | 61 |
| restart-rc8 | rc8 | rolling restart (KividbDbOps), sharded | 1,534,280 | 0 | 0 | 0 | 130 |
| failover-rc8 | rc8 | CLUSTER FAILOVER on every shard's replica in turn | 1,621,960 | 0 | 0 | 0 | 151 |
| sigkill-rc8 | rc8 | kill -9 of a shard master's engine, restarts in place | 1,660,024 | 0 | 0 | 0 | 2,637 |
| drain-rc8 | rc8 | drain of a node with a shard master | 1,760,008 | 0 | 0 | 0 | 87 |
| drain-rc8-rep | rc8 | same drain, replicated cluster | 1,861,616 | 0 | 0 | 0 | 53 |
| reshard-up-rc8 | rc8 | spec.shards 3 -> 4 under load | 1,868,096 | 0 | 0 | 0 | 82 |
| reshard-down-rc8 | rc8 | spec.shards 4 -> 3 under load | 1,915,224 | 0 | 0 | 0 | 33 |
| guard-yes | rc8 | #42 guard: rc8, aof-ack-after-write yes | 410,416 | 0 | 22620 | 57 | 5,001 |
| guard-no | rc8 | #42 guard: rc8, aof-ack-after-write no | 498,803 | 0 | 22997 | 58 | 5,001 |
| guard-113 | 1.1.3 | #42 guard: 1.1.3 | 590,839 | 0 | 25013 | 64 | 5,001 |
