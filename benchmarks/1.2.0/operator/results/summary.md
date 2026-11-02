| Scenario | Engine | What | Acked writes checked | Lost | Failed SETs | Impact (s) | Slowest SET (ms) |
|---|---|---|---:|---:|---:|---:|---:|
| P1 | 1.1.3 | operator upgrade 0.6.1 -> master (replicated) | 167,394 | 0 | 57 | 2 | 400 |
| reshard-up-113 | 1.1.3 | reshard 3 -> 4 shards | 83,440 | 0 | 0 | 0 | 155 |
| reshard-down-113 | 1.1.3 | reshard 4 -> 3 shards | 261,264 | 0 | 0 | 0 | 24 |
| restart-113 | 1.1.3 | rolling restart (KividbDbOps) | 444,275 | 0 | 0 | 0 | 791 |
| failover-113 | 1.1.3 | CLUSTER FAILOVER of every master | 538,419 | 0 | 0 | 0 | 796 |
| sigstop-113 | 1.1.3 | SIGSTOP of a shard master | 704,726 | 0 | 3621 | 17 | 8,001 |
| sigkill-113 | 1.1.3 | SIGKILL of a shard master's engine | 753,669 | 0 | 1401 | 7 | 5,001 |
| forcedel-113 | 1.1.3 | force delete of a shard master pod | 797,112 | 120162 | 2685 | 15 | 9,990 |
| delete-113 | 1.1.3 | graceful delete of a shard master | 848,520 | 0 | 0 | 0 | 482 |
| rep-sigstop-113 | 1.1.3 | SIGSTOP of the replicated master | 1,091,939 | 0 | 35165 | 45 | 7,515 |
| rep-forcedel-113 | 1.1.3 | force delete of the replicated master | 1,130,982 | 0 | 30119 | 38 | 5,421 |
| drain-113 | 1.1.3 | drain of a shard master's node | 1,253,947 | 0 | 1405 | 27 | 5,752 |
| drain-113-rep | 1.1.3 | drain (replicated cluster) | 1,242,278 | 0 | 0 | 0 | 562 |
| upgrade-rep | 1.2.0-rc4 | engine upgrade 1.1.3 -> 1.2.0-rc4 (replicated) | 1,362,238 | 0 | 0 | 0 | 1,988 |
| upgrade-shd | 1.2.0-rc4 | engine upgrade 1.1.3 -> 1.2.0-rc4 (sharded) | 1,551,563 | 0 | 0 | 0 | 2,188 |
| restart-rc4 | 1.2.0-rc4 | rolling restart (KividbDbOps) | 1,709,902 | 0 | 1013 | 17 | 5,343 |
| failover-rc4 | 1.2.0-rc4 | CLUSTER FAILOVER of every master | 1,805,718 | 0 | 0 | 0 | 406 |
| sigstop-rc4 | 1.2.0-rc4 | SIGSTOP of a shard master | 1,973,987 | 0 | 3707 | 18 | 8,000 |
| sigkill-rc4 | 1.2.0-rc4 | SIGKILL of a shard master's engine | 2,013,862 | 0 | 1389 | 6 | 5,001 |
| delete-rc4 | 1.2.0-rc4 | graceful delete of a shard master | 2,405,281 | 0 | 10080 | 37 | 5,002 |
| drain-rc4 | 1.2.0-rc4 | drain of a shard master's node | 2,512,049 | 0 | 2800 | 27 | 8,000 |
| drain-rc4-rep | 1.2.0-rc4 | drain (replicated cluster) | 2,915,846 | 0 | 0 | 0 | 1,329 |
| reshard-up-rc4 | 1.2.0-rc4 | reshard 3 -> 4 shards | 2,601,641 | 0 | 0 | 0 | 549 |
| reshard-down-rc4 | 1.2.0-rc4 | reshard 4 -> 3 shards | 2,650,217 | 0 | 0 | 0 | 103 |
| forcedel-rc4 | 1.2.0-rc4 | force delete of a shard master pod | 2,713,677 | 0 | 2396 | 11 | 5,912 |
| repdel-rc4 | 1.2.0-rc4 | graceful delete of a shard replica | 2,779,413 | 0 | 0 | 0 | 17 |
| repdel2-rc4 | 1.2.0-rc4 | graceful delete of a shard replica (2) | 2,834,853 | 0 | 0 | 0 | 4 |
| rep-sigstop-rc4 | 1.2.0-rc4 | SIGSTOP of the replicated master | 3,420,313 | 0 | 39588 | 51 | 8,000 |
| rep-forcedel-rc4 | 1.2.0-rc4 | force delete of the replicated master | 3,453,784 | 0 | 8993 | 11 | 5,001 |
| rep-restart-rc4 | 1.2.0-rc4 | rolling restart (replicated) | 3,594,744 | 0 | 0 | 0 | 1,619 |
| bulk-rc4 | 1.2.0-rc4 | bulk load 2.5 M x 1 KB SETs | 3,384,693 | 0 | 0 | 0 | 524 |
| reshard-bulk-up-rc4 | 1.2.0-rc4 | reshard 3 -> 4 after bulk load | 3,729,221 | 0 | 0 | 0 | 1,222 |
| reshard-bulk-down-rc4 | 1.2.0-rc4 | reshard 4 -> 3 after bulk load | 3,796,325 | 0 | 0 | 0 | 1,427 |
| split-outage-rc4 | 1.2.0-rc4 | CLUSTER FAILOVER on both nodes of a stale-address pair | 3,945,907 | 0 | 1314 | 6 | 7,996 |
