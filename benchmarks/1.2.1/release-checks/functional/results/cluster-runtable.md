| sweep | # | failover sent (ms after IMPORT) | vs import end (ms) | first attempt | promoted | import (ms, retries) | acked | lost | stale | wrong | list_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|
| p6b | 1 | 0 | -1392 | the source refused to start: ERR a failover of this node is under way: it does not know yet whether a replica of it has taken its slots over | yes, 0.01 s | 1392, 1 | 77906 | 0 | 0 | 0 | 0 |
| p6b | 2 | 20 | -1527 | fenced (dest vote), retried | yes, 0.02 s | 1547, 1 | 78735 | 0 | 0 | 0 | 0 |
| p6b | 3 | 51 | -1452 | fenced (dest vote), retried | yes, 0.01 s | 1503, 1 | 76883 | 0 | 0 | 0 | 0 |
| p6b | 4 | 100 | -1332 | fenced (election), retried | yes, 0.02 s | 1432, 1 | 78135 | 0 | 0 | 0 | 0 |
| p6b | 5 | 150 | -1377 | fenced (election), retried | yes, 0.02 s | 1527, 1 | 78428 | 0 | 0 | 0 | 0 |
| p6b | 6 | 200 | -1501 | fenced (election), retried | yes, 0.02 s | 1701, 1 | 77995 | 0 | 0 | 0 | 0 |
| p6b | 7 | 250 | -1440 | fenced (dest vote), retried | yes, 0.01 s | 1690, 1 | 78974 | 0 | 0 | 0 | 0 |
| p6b | 8 | 300 | -1362 | fenced (election), retried | yes, 0.01 s | 1662, 1 | 79264 | 0 | 0 | 0 | 0 |
| p6b | 9 | 350 | 37 | completed | no (refused: 1 log line) | 313, 0 | 172505 | 0 | 0 | 0 | 0 |
| p6b | 10 | 400 | 111 | completed | no (refused: 1 log line) | 289, 0 | 172579 | 0 | 0 | 0 | 0 |
| p6b | 11 | 450 | 141 | completed | no (refused: 1 log line) | 309, 0 | 171672 | 0 | 0 | 0 | 0 |
| p6b | 12 | 500 | 196 | completed | no (refused: 1 log line) | 304, 0 | 173135 | 0 | 0 | 0 | 0 |
| p6c | 1 | 330 | 18 | completed | no (refused: 1 log line) | 312, 0 | 172610 | 0 | 0 | 0 | 0 |
| p6c | 2 | 350 | 50 | completed | no (refused: 1 log line) | 300, 0 | 171464 | 0 | 0 | 0 | 0 |
| p6c | 3 | 360 | 41 | completed | no (refused: 1 log line) | 319, 0 | 172784 | 0 | 0 | 0 | 0 |
| p6c | 4 | 370 | 60 | completed | no (refused: 1 log line) | 310, 0 | 172557 | 0 | 0 | 0 | 0 |
| p6c | 5 | 381 | 34 | completed | no (refused: 1 log line) | 347, 0 | 171201 | 0 | 0 | 0 | 0 |
| p6c | 6 | 380 | 64 | completed | no (refused: 1 log line) | 316, 0 | 172666 | 0 | 0 | 0 | 0 |
| p6c | 7 | 390 | 45 | completed | no (refused: 1 log line) | 345, 0 | 172713 | 0 | 0 | 0 | 0 |
| p6c | 8 | 390 | 39 | completed | no (refused: 1 log line) | 351, 0 | 171384 | 0 | 0 | 0 | 0 |
