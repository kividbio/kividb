| Scenario | Engine | Runs | Acked writes | Lost acked | WAIT-acked lost | Writes unavailable (s) | Recovery (s) | Replica differs after |
|---|---|---|---|---|---|---|---|---|
| Planned switchover (pause, wait, promote, move clients) | KiviDB 1.2.0-rc7 | 3 | 685,130 (674,722–701,220) | 0 | – | 0.1 | 0.1 | 0 |
| Planned switchover (pause, wait, promote, move clients) | Redis 8.10.3 | 3 | 678,063 (675,343–696,035) | 0 | – | 0.1 | 0.1 | 0 |
| Replica promoted while master still takes writes | KiviDB 1.2.0-rc7 | 3 | 695,192 (679,785–701,762) | 292 (238–301) | – | 0.0 | 0.0 | 0 |
| Replica promoted while master still takes writes | Redis 8.10.3 | 3 | 679,385 (674,013–693,837) | 260 (256–286) | – | 0.0 | 0.0 | 0 |
| kill -9 master, AOF everysec, restart | KiviDB 1.2.0-rc7 | 3 | 622,656 (618,453–632,696) | 0 | – | 5.3 | 5.3 | 0 |
| kill -9 master, AOF everysec, restart | Redis 8.10.3 | 3 | 627,282 (617,309–632,538) | 0 | – | 5.2 (5.2–5.3) | 5.2 (5.2–5.3) | 0 |
| kill -9 master, appendfsync always, restart | KiviDB 1.2.0-rc7 | 3 | 633,116 (622,677–639,560) | 0 | – | 5.3 (5.2–5.3) | 5.3 (5.2–5.3) | 0 |
| kill -9 master, appendfsync always, restart | Redis 8.10.3 | 3 | 269,931 (250,849–285,883) | 0 | – | 5.1 | 5.1 | 0 |
| kill -9 master, aof-ack-after-write no (KiviDB only) | KiviDB 1.2.0-rc7 | 3 | 632,434 (624,023–642,590) | 1 (0–6) | – | 5.2 (5.2–5.3) | 5.2 (5.2–5.3) | 0 |
| kill -9 master as an AOF checkpoint starts, 3 times (KiviDB only) | KiviDB 1.2.0-rc7 | 3 | 846,365 (841,592–846,870) | 0 | – | 4.3 (4.2–4.5) | 4.3 (4.2–4.5) | 0 |
| SIGSTOP master 30 s | KiviDB 1.2.0-rc7 | 3 | 572,986 (571,473–577,670) | 0 | – | 30.0 | 30.0 | 0 |
| SIGSTOP master 30 s | Redis 8.10.3 | 3 | 568,421 (568,381–576,177) | 0 | – | 30.0 | 30.0 | 0 |
| Partition master <-> replica 30 s (no promotion) | KiviDB 1.2.0-rc7 | 3 | 917,144 (907,873–920,219) | 0 | – | 0.0 | 0.0 | 0 |
| Partition master <-> replica 30 s (no promotion) | Redis 8.10.3 | 3 | 913,765 (905,886–925,315) | 0 | – | 0.0 | 0.0 | 0 |
| Partition + promote replica (split brain, 15 s) | KiviDB 1.2.0-rc7 | 3 | 810,270 (793,488–812,014) | 54,698 (52,657–54,748) | – | 0.0 | 0.0 | 0 |
| Partition + promote replica (split brain, 15 s) | Redis 8.10.3 | 3 | 814,806 (794,186–817,079) | 54,845 (53,377–54,847) | – | 0.0 | 0.0 | 0 |
| Same, min-replicas-to-write 1, max-lag 2 | KiviDB 1.2.0-rc7 | 3 | 788,961 (775,200–791,238) | 33,926 (33,740–34,170) | – | 1.7 (1.7–1.8) | 4.7 (4.7–4.8) | 0 |
| Same, min-replicas-to-write 1, max-lag 2 | Redis 8.10.3 | 3 | 781,046 (767,877–781,939) | 29,998 (29,165–30,392) | – | 2.1 (2.0–2.1) | 4.7 | 0 |
| WAIT 1 after each SET; kill -9 master, promote | KiviDB 1.2.0-rc7 | 3 | 233,708 (231,329–235,896) | 3 (0–6) | 0 | 0.8 | 0.7 | – |
| WAIT 1 after each SET; kill -9 master, promote | Redis 8.10.3 | 3 | 121,038 (114,624–124,141) | 0 | 0 | 38.7 | 38.7 | – |
| Cluster: CLUSTER FAILOVER | KiviDB 1.2.0-rc7 | 3 | 608,531 (591,508–610,099) | 0 | – | 0.1 (0.0–0.1) | 0.1 (0.0–0.1) | – |
| Cluster: CLUSTER FAILOVER | Redis 8.10.3 | 3 | 597,849 (590,403–603,362) | 0 | – | 0.1 | 0.1 | – |
| Cluster: CLUSTER FAILOVER FORCE | KiviDB 1.2.0-rc7 | 3 | 601,361 (594,889–606,559) | 6 (4–7) | – | 0.0 | 0.0 | – |
| Cluster: CLUSTER FAILOVER FORCE | Redis 8.10.3 | 3 | 601,423 (591,856–601,447) | 34 (33–336) | – | 0.0 (0.0–0.1) | 0.0 (0.0–0.1) | – |
| Cluster: kill -9 a master | KiviDB 1.2.0-rc7 | 3 | 642,335 (640,157–646,250) | 0 | – | 5.9 (5.6–6.1) | 6.3 (5.6–6.8) | – |
| Cluster: kill -9 a master | Redis 8.10.3 | 3 | 617,123 (598,580–629,282) | 0 | – | 8.4 (7.3–9.3) | 8.5 (7.3–9.3) | – |
| Cluster: WAIT 1 after each SET; kill -9 a master | KiviDB 1.2.0-rc7 | 3 | 28,425 (22,777–31,345) | 0 | 0 | 22.8 (22.6–23.0) | 48.6 (48.5–48.7) | – |
| Cluster: WAIT 1 after each SET; kill -9 a master | Redis 8.10.3 | 3 | 223,025 (217,481–227,166) | 0 | 0 | 25.1 (25.0–25.1) | 25.1 (25.0–25.1) | – |
| Cluster: SIGSTOP a master 30 s | KiviDB 1.2.0-rc7 | 3 | 727,821 (720,307–733,260) | 0 (0–1) | – | 7.3 (7.2–7.3) | 7.3 | – |
| Cluster: SIGSTOP a master 30 s | Redis 8.10.3 | 3 | 724,442 (699,336–728,316) | 0 | – | 8.3 (8.2–8.3) | 8.3 | – |
| Cluster: master's host cut off from other nodes 20 s | KiviDB 1.2.0-rc7 | 3 | 552,505 (528,916–553,495) | 17,274 (15,593–17,867) | – | 15.4 (15.3–15.9) | 20.4 (19.9–20.5) | – |
| Cluster: master's host cut off from other nodes 20 s | Redis 8.10.3 | 3 | 551,139 (547,645–576,649) | 21,939 (19,062–23,843) | – | 13.7 (13.4–14.8) | 20.2 (20.2–20.3) | – |
| Same, min-replicas-to-write 1, max-lag 2 | KiviDB 1.2.0-rc7 | 3 | 523,067 (516,957–527,002) | 9,791 (9,785–9,950) | – | 17.8 (17.8–17.9) | 20.5 (20.5–22.1) | – |
| Same, min-replicas-to-write 1, max-lag 2 | Redis 8.10.3 | 3 | 448,507 (445,269–452,250) | 9,316 (6,382–9,417) | – | 22.5 (22.5–24.1) | 25.7 (25.2–26.2) | – |
