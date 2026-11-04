| Scenario | Engine | Runs | Acked writes | Lost acked | WAIT-acked lost | Writes unavailable (s) | Recovery (s) | Replica differs after |
|---|---|---|---|---|---|---|---|---|
| kill -9 master, AOF everysec, restart | KiviDB 1.2.0-rc8 | 2 | 637,574 (636,825–638,323) | 0 | – | 5.0 (4.9–5.0) | 5.0 (4.9–5.0) | 0 |
| kill -9 master, appendfsync always, restart | KiviDB 1.2.0-rc8 | 2 | 632,326 (632,099–632,554) | 0 | – | 5.2 (5.2–5.3) | 5.2 (5.2–5.3) | 0 |
| kill -9 master as an AOF checkpoint starts, 3 times (KiviDB only) | KiviDB 1.2.0-rc8 | 2 | 860,711 (858,438–862,984) | 0 | – | 4.3 | 4.3 | 0 |
| SIGSTOP master 30 s | KiviDB 1.2.0-rc8 | 2 | 579,268 (575,963–582,573) | 0 | – | 30.0 | 30.0 | 0 |
| Partition master <-> replica 30 s (no promotion) | KiviDB 1.2.0-rc8 | 2 | 925,030 (923,565–926,496) | 0 | – | 0.0 | 0.0 | 0 |
| Cluster: CLUSTER FAILOVER | KiviDB 1.2.0-rc8 | 2 | 605,356 (604,758–605,953) | 0 | – | 0.1 | 0.1 | – |
| Cluster: kill -9 a master | KiviDB 1.2.0-rc8 | 2 | 643,736 (642,499–644,974) | 0 | – | 6.2 (6.1–6.2) | 6.3 (6.3–6.4) | – |
| Cluster: SIGSTOP a master 30 s | KiviDB 1.2.0-rc8 | 2 | 733,508 (733,306–733,710) | 0 | – | 7.3 | 7.3 | – |
