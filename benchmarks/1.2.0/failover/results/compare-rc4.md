| Scenario | Runs rc7 / rc4 / Redis | Lost acked: rc7 | rc4 | Redis | WAIT-acked lost: rc7 / rc4 / Redis | Unavailable (s): rc7 | rc4 | Redis | Replica differs (rc7) | rc7 verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| Planned switchover (pause, wait, promote, move clients) | 3 / 3 / 3 | 0 | 0 | 0 | – / – / – | 0.1 | 0.1 (0.0–0.1) | 0.1 | 0 | pass |
| Replica promoted while master still takes writes | 3 / 3 / 3 | 292 (238–301) | 243 (209–258) | 260 (256–286) | – / – / – | 0.0 | 0.0 | 0.0 | 0 | pass (loss expected: async replication) |
| kill -9 master, AOF everysec, restart | 3 / 3 / 3 | 0 | 0 | 0 | – / – / – | 5.3 | 5.2 | 5.2 (5.2–5.3) | 0 | pass |
| kill -9 master, appendfsync always, restart | 3 / 3 / 3 | 0 | 0 | 0 | – / – / – | 5.3 (5.2–5.3) | 5.2 (5.2–5.3) | 5.1 | 0 | pass |
| kill -9 master, aof-ack-after-write no (KiviDB only) | 3 / 3 / – | 1 (0–6) | 0 (0–1) | – | – / – / – | 5.2 (5.2–5.3) | 5.2 (5.2–5.3) | – | 0 | pass (documented: may lose the last few) |
| kill -9 master as an AOF checkpoint starts, 3 times (KiviDB only) | 3 / 3 / – | 0 | 0 | – | – / – / – | 4.3 (4.2–4.5) | 4.1 (4.0–4.3) | – | 0 | pass |
| SIGSTOP master 30 s | 3 / 3 / 3 | 0 | 0 | 0 | – / – / – | 30.0 | 30.0 | 30.0 | 0 | pass |
| Partition master <-> replica 30 s (no promotion) | 3 / 3 / 3 | 0 | 0 | 0 | – / – / – | 0.0 | 0.0 | 0.0 | 0 | pass |
| Partition + promote replica (split brain, 15 s) | 3 / 3 / 3 | 54,698 (52,657–54,748) | 49,585 (48,147–49,595) | 54,845 (53,377–54,847) | – / – / – | 0.0 | 0.0 | 0.0 | 0 | pass (loss expected: async replication) |
| Same, min-replicas-to-write 1, max-lag 2 | 3 / 3 / 3 | 33,926 (33,740–34,170) | 31,336 (30,915–33,994) | 29,998 (29,165–30,392) | – / – / – | 1.7 (1.7–1.8) | 1.7 | 2.1 (2.0–2.1) | 0 | pass (loss expected: async replication) |
| WAIT 1 after each SET; kill -9 master, promote | 3 / 3 / 3 | 3 (0–6) | 0 (0–2) | 0 | 0 / 0 / 0 | 0.8 | 0.8 | 38.7 | – | pass (no WAIT-acked write lost) |
| Cluster: CLUSTER FAILOVER | 3 / 3 / 3 | 0 | 0 | 0 | – / – / – | 0.1 (0.0–0.1) | 0.0 | 0.1 | – | pass |
| Cluster: CLUSTER FAILOVER FORCE | 3 / 3 / 3 | 6 (4–7) | 5 (5–7) | 34 (33–336) | – / – / – | 0.0 | 0.0 | 0.0 (0.0–0.1) | – | pass (loss expected: async replication) |
| Cluster: kill -9 a master | 3 / 3 / 3 | 0 | 0 | 0 | – / – / – | 5.9 (5.6–6.1) | 5.8 (5.7–5.9) | 8.4 (7.3–9.3) | – | pass |
| Cluster: WAIT 1 after each SET; kill -9 a master | 3 / 3 / 3 | 0 | 1 (0–7) | 0 | 0 / 0 / 0 | 22.8 (22.6–23.0) | 22.2 (20.5–22.3) | 25.1 (25.0–25.1) | – | pass (no WAIT-acked write lost) |
| Cluster: SIGSTOP a master 30 s (writes the replica had received) | 3 / 3 / 3 | 0 | 0 | 0 | – / – / – | 7.3 (7.2–7.3) | 7.2 (7.1–7.3) | 8.3 (8.2–8.3) | – | pass |
| ↳ of which promoted past unreplicated writes (async window) | 3 / 3 / 3 | 1 (rep 2 only: acknowledged 3 ms before the SIGSTOP) | 0 | 0 | – / – / – | – | – | – | – | expected (see the note below) |
| Cluster: master's host cut off from other nodes 20 s | 3 / 3 / 3 | 17,274 (15,593–17,867) | 14,176 (14,017–16,132) | 21,939 (19,062–23,843) | – / – / – | 15.4 (15.3–15.9) | 15.5 (15.5–15.6) | 13.7 (13.4–14.8) | – | pass (loss expected: async replication) |
| Same, min-replicas-to-write 1, max-lag 2 | 3 / 3 / 3 | 9,791 (9,785–9,950) | 9,094 (8,462–9,556) | 9,316 (6,382–9,417) | – / – / – | 17.8 (17.8–17.9) | 17.7 (17.6–17.7) | 22.5 (22.5–24.1) | – | pass (loss expected: async replication) |
