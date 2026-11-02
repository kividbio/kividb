| Scenario | Engine | Runs | Acked writes | Lost acked | WAIT-acked lost | Writes unavailable (s) | Recovery (s) | Replica differs after |
|---|---|---|---|---|---|---|---|---|
| Planned switchover (pause, wait, promote, move clients) | KiviDB 1.2.0-rc4 | 3 | 633,351 (623,392–640,739) | 0 | – | 0.1 (0.0–0.1) | 0.1 (0.0–0.1) | 0 |
| Planned switchover (pause, wait, promote, move clients) | Redis 8.10.3 | 3 | 634,845 (625,383–642,647) | 0 | – | 0.1 (0.0–0.1) | 0.1 (0.0–0.1) | 0 |
| Replica promoted while master still takes writes | KiviDB 1.2.0-rc4 | 3 | 639,760 (625,526–644,587) | 243 (209–258) | – | 0.0 | 0.0 | 0 |
| Replica promoted while master still takes writes | Redis 8.10.3 | 3 | 640,300 (622,885–644,845) | 195 (180–319) | – | 0.0 | 0.0 | 0 |
| kill -9 master, AOF everysec, restart | KiviDB 1.2.0-rc4 | 3 | 580,235 (567,188–585,386) | 0 | – | 5.2 | 5.2 | 0 |
| kill -9 master, AOF everysec, restart | Redis 8.10.3 | 3 | 565,874 (563,580–584,421) | 0 (0–2) | – | 5.2 | 5.2 | 0 |
| kill -9 master, appendfsync always, restart | KiviDB 1.2.0-rc4 | 3 | 577,176 (569,725–578,202) | 0 | – | 5.2 (5.2–5.3) | 5.2 (5.2–5.3) | 0 |
| kill -9 master, appendfsync always, restart | Redis 8.10.3 | 3 | 259,719 (259,295–304,420) | 0 | – | 5.1 | 5.1 | 0 |
| kill -9 master, aof-ack-after-write no (KiviDB only) | KiviDB 1.2.0-rc4 | 3 | 578,489 (574,722–581,821) | 0 (0–1) | – | 5.2 (5.2–5.3) | 5.2 (5.2–5.3) | 0 |
| kill -9 master as an AOF checkpoint starts, 3 times (KiviDB only) | KiviDB 1.2.0-rc4 | 3 | 757,541 (744,377–757,549) | 0 | – | 4.1 (4.0–4.3) | 4.1 (4.0–4.3) | 0 |
| Power-off of the master's VM, AOF everysec, restart | KiviDB 1.2.0-rc4 | 1 | 2,865,769 | 1,714 | – | 45.3 | 47.7 | – |
| Power-off of the master's VM, AOF everysec, restart | Redis 8.10.3 | 1 | 3,459,183 | 748 | – | 24.8 | 26.9 | – |
| Power-off of the master's VM, appendfsync always, restart | Redis 8.10.3 | 1 | 1,509,762 | 0 | – | 25.8 | 27.6 | – |
| SIGSTOP master 30 s | KiviDB 1.2.0-rc4 | 3 | 527,707 (525,114–544,539) | 0 | – | 30.0 | 30.0 | 0 |
| SIGSTOP master 30 s | Redis 8.10.3 | 3 | 537,454 (531,678–541,447) | 0 | – | 30.0 | 30.0 | 0 |
| Partition master <-> replica 30 s (no promotion) | KiviDB 1.2.0-rc4 | 3 | 856,400 (849,990–867,186) | 0 | – | 0.0 | 0.0 | 0 |
| Partition master <-> replica 30 s (no promotion) | Redis 8.10.3 | 3 | 841,643 (834,072–853,371) | 0 | – | 0.0 | 0.0 | 0 |
| Partition + promote replica (split brain, 15 s) | KiviDB 1.2.0-rc4 | 3 | 737,599 (736,597–743,358) | 49,585 (48,147–49,595) | – | 0.0 | 0.0 | 0 |
| Partition + promote replica (split brain, 15 s) | Redis 8.10.3 | 3 | 740,544 (724,046–750,458) | 49,781 (47,789–50,595) | – | 0.0 | 0.0 | 0 |
| Same, min-replicas-to-write 1, max-lag 2 | KiviDB 1.2.0-rc4 | 3 | 716,466 (710,563–726,333) | 31,336 (30,915–33,994) | – | 1.7 | 4.7 | 0 |
| Same, min-replicas-to-write 1, max-lag 2 | Redis 8.10.3 | 3 | 720,668 (702,684–731,337) | 27,462 (26,742–27,686) | – | 2.1 | 4.7 | 0 |
| WAIT 1 after each SET; kill -9 master, promote | KiviDB 1.2.0-rc4 | 3 | 217,560 (210,951–221,837) | 0 (0–2) | 0 | 0.8 | 0.7 | – |
| WAIT 1 after each SET; kill -9 master, promote | Redis 8.10.3 | 3 | 112,104 (103,220–114,114) | 0 | 0 | 38.7 | 38.7 | – |
| Cluster: CLUSTER FAILOVER | KiviDB 1.2.0-rc4 | 3 | 533,758 (515,212–559,962) | 0 | – | 0.0 | 0.0 | – |
| Cluster: CLUSTER FAILOVER | Redis 8.10.3 | 3 | 553,166 (530,052–561,058) | 0 | – | 0.1 | 0.1 | – |
| Cluster: CLUSTER FAILOVER FORCE | KiviDB 1.2.0-rc4 | 3 | 557,569 (514,453–559,864) | 5 (5–7) | – | 0.0 | 0.0 | – |
| Cluster: CLUSTER FAILOVER FORCE | Redis 8.10.3 | 3 | 527,841 (506,937–538,625) | 17 (13–27) | – | 0.1 | 0.2 | – |
| Cluster: kill -9 a master | KiviDB 1.2.0-rc4 | 3 | 573,211 (565,250–589,574) | 0 | – | 5.8 (5.7–5.9) | 5.8 (5.7–6.2) | – |
| Cluster: kill -9 a master | Redis 8.10.3 | 3 | 552,949 (541,401–560,663) | 0 | – | 7.9 (7.7–8.3) | 7.9 (7.7–8.5) | – |
| Cluster: WAIT 1 after each SET; kill -9 a master | KiviDB 1.2.0-rc4 | 3 | 29,482 (28,646–32,702) | 1 (0–7) | 0 | 22.2 (20.5–22.3) | 48.7 (48.6–48.7) | – |
| Cluster: WAIT 1 after each SET; kill -9 a master | Redis 8.10.3 | 3 | 199,256 (196,479–208,130) | 0 | 0 | 25.1 (25.1–25.2) | 25.1 (25.1–25.7) | – |
| Cluster: SIGSTOP a master 30 s | KiviDB 1.2.0-rc4 | 3 | 651,799 (649,595–667,659) | 0 | – | 7.2 (7.1–7.3) | 7.3 (7.1–7.3) | – |
| Cluster: SIGSTOP a master 30 s | Redis 8.10.3 | 3 | 644,175 (643,447–661,455) | 0 | – | 8.2 (8.2–8.3) | 8.4 (8.2–8.4) | – |
| Cluster: master's host cut off from other nodes 20 s | KiviDB 1.2.0-rc4 | 3 | 492,729 (487,406–499,088) | 14,176 (14,017–16,132) | – | 15.5 (15.5–15.6) | 20.1 (20.0–20.5) | – |
| Cluster: master's host cut off from other nodes 20 s | Redis 8.10.3 | 3 | 510,544 (489,166–512,566) | 20,046 (19,047–22,079) | – | 13.6 (13.6–14.3) | 20.2 (20.1–20.2) | – |
| Same, min-replicas-to-write 1, max-lag 2 | KiviDB 1.2.0-rc4 | 3 | 469,721 (432,801–474,976) | 9,094 (8,462–9,556) | – | 17.7 (17.6–17.7) | 20.4 (20.3–20.4) | – |
| Same, min-replicas-to-write 1, max-lag 2 | Redis 8.10.3 | 3 | 387,881 (360,510–394,380) | 7,729 (7,067–8,113) | – | 23.5 (22.3–24.0) | 25.9 (24.8–26.0) | – |
