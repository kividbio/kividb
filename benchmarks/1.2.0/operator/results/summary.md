| Scenario | Engine | What | Acked writes checked | Lost | Failed SETs | Impact (s) | Slowest SET (ms) |
|---|---|---|---:|---:|---:|---:|---:|
| upgrade-rep | rc7 | engine upgrade 1.1.3 -> 1.2.0-rc7 (replicated) | 213,464 | 0 | 0 | 0 | 166 |
| upgrade-shd | rc7 | engine upgrade 1.1.3 -> 1.2.0-rc7 (sharded) | 352,696 | 0 | 0 | 0 | 1,151 |
| restart-rc7 | rc7 | rolling restart (KividbDbOps), sharded | 532,216 | 0 | 0 | 0 | 149 |
| failover-rc7 | rc7 | CLUSTER FAILOVER on every replica in turn | 634,610 | 0 | 0 | 0 | 162 |
| sigstop-rc7 | rc7 | SIGSTOP of a shard master (host PID) | 802,420 | 0 | 2430 | 11 | 8,000 |
| sigkill-rc7 | rc7 | kill -9 of a shard master's engine, restarts in place | 846,988 | 0 | 0 | 0 | 1,561 |
| forcedel-rc7 | rc7 | force delete of a shard master | 890,028 | 121939 | 2832 | 12 | 5,010 |
| delete-rc7 | rc7 | graceful delete of a shard master | 939,772 | 0 | 0 | 0 | 563 |
| drain-rc7 | rc7 | drain of a node with a shard master | 1,294,868 | 0 | 0 | 0 | 265 |
| drain-rc7-rep | rc7 | same drain, replicated cluster | 1,394,057 | 0 | 1039 | 2 | 565 |
| fx-force1 | rc7 | fx: force delete of a master, replacement on another node | 75,136 | 0 | 262834 | 11 | 5,013 |
| fx-force2 | rc7 | fx: force delete, replacement on the same node, 1 GB/shard | 187,782 | 0 | 2250 | 10 | 5,018 |
| repdel-rc7 | rc7 | graceful delete of a shard's replica | 1,557,100 | 0 | 0 | 0 | 6 |
| reshard-up-rc7 | rc7 | spec.shards 3 -> 4 under load | 1,627,052 | 0 | 0 | 0 | 69 |
| fx-loss1 | rc7 | fx: same-node force delete, small dataset, try 1 | 135,357 | 0 | 6819 | 27 | 5,003 |
| reshard-down-rc7 | rc7 | spec.shards 4 -> 3 under load | 1,675,076 | 0 | 0 | 0 | 261 |
| rep-sigstop-rc7 | rc7 | SIGSTOP of the replicated master | 1,947,963 | 0 | 44831 | 56 | 8,007 |
| fx-loss2 | rc7 | fx: same-node force delete, try 2 (target was a replica) | 266,645 | 0 | 0 | 0 | 133 |
| fx-loss3 | rc7 | fx: same-node force delete, try 3 | 390,588 | 0 | 6857 | 27 | 5,005 |
| rep-sigkill-rc7 | rc7 | kill -9 of the replicated master, restarts in place | 1,982,943 | 0 | 7326 | 10 | 249 |
| rep-forcedel-rc7 | rc7 | force delete of the replicated master | 2,016,418 | 0 | 23303 | 29 | 5,061 |
| fx-loss4 | rc7 | fx: same-node force delete, try 4 | 514,806 | 0 | 6862 | 27 | 5,004 |
| rep-restart-rc7 | rc7 | rolling restart (KividbDbOps), replicated | 2,124,882 | 0 | 0 | 1 | 1,193 |
| fx-graceful | rc7 | fx: graceful delete of a master, replacement on the same node | 586,846 | 0 | 0 | 0 | 67 |
| dbq-rc7 | rc7 | two KividbDbOps restarts at once (#40 queue) | 2,331,506 | 0 | 0 | 0 | 91 |
| holdback-rc7 | rc7 | missing configRef, then SIGSTOP of the master (#36) | 2,479,229 | 0 | 38492 | 49 | 8,003 |
| fblock-rc7 | rc7 | master frozen, only replica emptied (#35 FailoverBlocked) | 1,313,168 | 0 | 61908 | 151 | 8,001 |
| guard-yes | rc7 | #42 guard: rc7, aof-ack-after-write yes; master killed in a new pod while the operator is down | 1,460,104 | 0 | 26248 | 66 | 5,170 |
| guard-no | rc7 | #42 guard: rc7, aof-ack-after-write no | 1,560,436 | 0 | 37080 | 65 | 5,001 |
| guard-113 | 1.1.3 | #42 guard: engine 1.1.3 | 1,681,269 | 0 | 23403 | 59 | 5,001 |
| guard-bad | rc7* | #42 guard: version 1.2.0_rc7 (unparsable), ack yes | 1,789,824 | 0 | 16272 | 40 | 5,001 |
| guard-yes-opup | rc7 | #42 guard: rc7 yes, operator left running | 1,871,862 | 0 | 4566 | 12 | 5,001 |
| shrink-a-rc7 | rc7 | 4 -> 3 shards, finish write lost, operator restarted (#38) | 4,032,684 | 0 | 0 | 0 | 225 |
| shrink-b-rc7 | rc7 | 4 -> 3 shards, finish lost, spec.shards raised back to 4 (#38) | 4,196,340 | 0 | 0 | 0 | 238 |
| reshard-down2-rc7 | rc7 | spec.shards 4 -> 3 under load | 4,249,964 | 0 | 0 | 0 | 94 |
