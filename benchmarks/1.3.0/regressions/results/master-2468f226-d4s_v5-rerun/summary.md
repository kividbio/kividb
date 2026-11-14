# Regression suite: master-2468f226-d4s_v5-rerun

Verdict: **ok**

- Machine: Standard_D4s_v5, 4 vCPU, Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz, kernel 7.0.0-1017-azure, glibc 2.39
- Client: Python 3.12.3, redis-py 5.2.1
- v1_2_2: `kividb 1.2.2`, sha256 `38fc2cea40c131e041f4914527a98719e5d34dffdfe2abdc1f07ecb86aa6723e`
- candidate: `kividb 1.2.1 (default build: no Lua, no TLS)`, sha256 `76dcdd0414c70f4501de77d99f9dcf189c2b4d5f626ac6c0fc12843aa9d6e7af`
- redis: `Redis server v=8.10.2 sha=00000000:1 malloc=jemalloc-5.3.0 bits=64 build=75a9b36b1f0aa00`, sha256 `b0cd304980112fa6cd8dced524fdd6b136fd8efb802f5cf9e81610051a11891c`
- candidate commit: `2468f226`
- date: 2026-11-14
- run time: 01-scan-match 134s, 02-growth-ping 261s, 03-give-back 201s, 04-long-commands 200s

## 01-scan-match

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| default | dbsize | keys | 2,002,003 | 2,002,003 | 2,002,003 | 2,002,003 | bound: == 2,002,003 | pass |
| default | hscan_match_c1000_calls | calls | 201.0 | 201.0 | 201.0 | 201.0 | info | info |
| default | hscan_match_c1000_distinct | keys | 200.0 | 200.0 | 200.0 | 200.0 | bound: == 200.0 | pass |
| default | hscan_match_c1000_ms | ms | 468.3 | 412.1 | 41.59 | 428.0 | rel: <= 492.1 (15% over 428.0) | pass |
| default | keys_schedule_ms | ms | 118.4 | 59.32 | 138.0 | 60.44 | rel: <= 66.48 (10% over 60.44) | pass |
| default | keys_schedule_returned | keys | 2,000 | 2,000 | 2,000 | 2,000 | bound: == 2,000 | pass |
| default | scan_all_c1000_calls | calls | 513.0 | 512.0 | 2,002 | 512.0 | info | info |
| default | scan_all_c1000_distinct | keys | 2,002,003 | 2,002,003 | 2,002,003 | 2,002,003 | bound: == 2,002,003 | pass |
| default | scan_all_c1000_ms | ms | 3,618 | 2,144 | 2,231 | 2,180 | rel: <= 2,398 (10% over 2,180)<br>ratio: gain over v1.2.2 1.69x >= 1.00x (60% of recorded 1.66x)<br>vs_redis: 0.96x Redis <= 1.3x | pass<br>pass<br>pass |
| default | scan_infix_c1000_calls | calls | 513.0 | 512.0 | 2,002 | 512.0 | info | info |
| default | scan_infix_c1000_distinct | keys | 1,112,222 | 1,112,222 | 1,112,222 | 1,112,222 | bound: == 1,112,222 | pass |
| default | scan_infix_c1000_ms | ms | 2,729 | 1,500 | 1,750 | 1,494 | rel: <= 1,644 (10% over 1,494)<br>ratio: gain over v1.2.2 1.82x >= 1.08x (60% of recorded 1.80x)<br>vs_redis: 0.86x Redis <= 1.3x | pass<br>pass<br>pass |
| default | scan_match_c10000_calls | calls | 193.0 | 193.0 | 201.0 | 193.0 | info | info |
| default | scan_match_c10000_distinct | keys | 2,000 | 2,000 | 2,000 | 2,000 | bound: == 2,000 | pass |
| default | scan_match_c10000_ms | ms | 834.7 | 238.5 | 401.1 | 238.9 | rel: <= 262.8 (10% over 238.9)<br>ratio: gain over v1.2.2 3.50x >= 2.07x (60% of recorded 3.45x)<br>vs_redis: 0.59x Redis <= 1.3x | pass<br>pass<br>pass |
| default | scan_match_c1000_calls | calls | 513.0 | 512.0 | 2,002 | 512.0 | info | info |
| default | scan_match_c1000_distinct | keys | 2,000 | 2,000 | 2,000 | 2,000 | bound: == 2,000 | pass |
| default | scan_match_c1000_ms | ms | 1,439 | 430.9 | 501.2 | 406.2 | rel: <= 446.8 (10% over 406.2)<br>ratio: gain over v1.2.2 3.34x >= 2.47x (60% of recorded 4.12x)<br>vs_redis: 0.86x Redis <= 1.3x | pass<br>pass<br>pass |
| default | sscan_match_c1000_calls | calls | 201.0 | 201.0 | 201.0 | 201.0 | info | info |
| default | sscan_match_c1000_distinct | keys | 200.0 | 200.0 | 200.0 | 200.0 | bound: == 200.0 | pass |
| default | sscan_match_c1000_ms | ms | 999.7 | 782.9 | 33.61 | 1,093 | rel: <= 1,257 (15% over 1,093) | pass |
| default | zscan_match_c1000_calls | calls | 201.0 | 201.0 | 201.0 | 201.0 | info | info |
| default | zscan_match_c1000_distinct | keys | 200.0 | 200.0 | 200.0 | 200.0 | bound: == 200.0 | pass |
| default | zscan_match_c1000_ms | ms | 795.7 | 694.0 | 51.48 | 829.5 | rel: <= 954.0 (15% over 829.5) | pass |

## 02-growth-ping

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| tokio | dbsize | keys | 8,000,000 | 8,000,000 | 8,000,000 | 8,000,000 | bound: == 8,000,000 | pass |
| tokio | load_s | s | 50.71 | 50.21 | 50.85 | 50.84 | rel: <= 55.92 (10% over 50.84) | pass |
| tokio | ping_max_ms | ms | 140.6 | 8.47 | 8.49 | 9.66 | rel: <= 18.49 (50% over 9.66, Redis 8.49 + 10)<br>ratio: gain over v1.2.2 16.60x >= 6.96x (60% of recorded 11.61x) | pass<br>pass |
| tokio | ping_p99_ms | ms | 1.88 | 2.95 | 1.21 | 3.03 | rel: <= 11.21 (50% over 3.03, Redis 1.21 + 10) | pass |
| tokio | pings | pings | 45,468 | 44,496 | 47,804 | 44,740 | info | info |
| tokio | pings_over_10ms | pings | 27.00 | 0 | 0 | 0 | info | info |
| tokio | rss_loaded_mib | MiB | 524.5 | 524.7 | 552.0 | 525.0 | rel: <= 551.3 (5% over 525.0) | pass |
| tokio | used_memory_mib | MiB | 511.4 | 511.4 | 534.7 | 511.4 | rel: <= 537.0 (5% over 511.4) | pass |
| uring1 | dbsize | keys | 8,000,000 | 8,000,000 | - | 8,000,000 | bound: == 8,000,000 | pass |
| uring1 | load_s | s | 52.41 | 51.33 | - | 55.60 | rel: <= 61.16 (10% over 55.60) | pass |
| uring1 | ping_max_ms | ms | 69.41 | 9.38 | - | 20.83 | rel: <= 31.25 (50% over 20.83)<br>ratio: gain over v1.2.2 7.40x >= 2.09x (60% of recorded 3.49x) | pass<br>pass |
| uring1 | ping_p99_ms | ms | 0.68 | 1.13 | - | 1.54 | rel: <= 2.32 (50% over 1.54) | pass |
| uring1 | pings | pings | 48,420 | 48,433 | - | 51,940 | info | info |
| uring1 | pings_over_10ms | pings | 29.00 | 0 | - | 2 | info | info |
| uring1 | rss_loaded_mib | MiB | 524.6 | 524.4 | - | 524.5 | rel: <= 550.7 (5% over 524.5) | pass |
| uring1 | used_memory_mib | MiB | 511.5 | 511.6 | - | 511.6 | rel: <= 537.2 (5% over 511.6) | pass |

## 03-give-back

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| churn | delete_ms | ms | 1,612 | 1,585 | 1,498 | 1,538 | info | info |
| churn | rss_after_15s_mib | MiB | 487.3 | 427.1 | 617.3 | 429.4 | rel: <= 536.8 (25% over 429.4, +5) | pass |
| churn | rss_after_1_5s_mib | MiB | 487.3 | 427.1 | 738.4 | 429.4 | rel: <= 536.8 (25% over 429.4, +5) | pass |
| churn | rss_after_5s_mib | MiB | 487.3 | 427.1 | 677.1 | 429.4 | rel: <= 536.8 (25% over 429.4, +5) | pass |
| churn | rss_loaded_mib | MiB | 789.4 | 789.7 | 746.3 | 789.3 | rel: <= 828.8 (5% over 789.3) | pass |
| churn | rss_start_mib | MiB | 12.86 | 13.15 | 12.66 | 12.73 | info | info |
| churn | used_after_mib | MiB | 372.0 | 372.1 | 361.8 | 372.1 | info | info |
| churn | used_loaded_mib | MiB | 742.5 | 742.6 | 719.1 | 742.6 | rel: <= 779.7 (5% over 742.6) | pass |
| hashdel | delete_ms | ms | 132.9 | 144.1 | 302.1 | 134.4 | info | info |
| hashdel | rss_after_15s_mib | MiB | 196.5 | 13.70 | 25.20 | 13.60 | rel: <= 18.60 (25% over 13.60, +5) | pass |
| hashdel | rss_after_1_5s_mib | MiB | 196.5 | 13.70 | 76.60 | 13.60 | rel: <= 18.60 (25% over 13.60, +5)<br>ratio: gain over v1.2.2 14.34x >= 8.66x (60% of recorded 14.44x) | pass<br>pass |
| hashdel | rss_after_5s_mib | MiB | 196.5 | 13.70 | 76.60 | 13.60 | rel: <= 18.60 (25% over 13.60, +5) | pass |
| hashdel | rss_loaded_mib | MiB | 369.6 | 369.9 | 124.6 | 369.9 | rel: <= 388.4 (5% over 369.9) | pass |
| hashdel | rss_start_mib | MiB | 12.86 | 13.20 | 12.73 | 13.28 | info | info |
| hashdel | used_after_mib | MiB | 1.36 | 1.39 | 0.846 | 1.39 | info | info |
| hashdel | used_loaded_mib | MiB | 295.9 | 295.9 | 109.3 | 295.9 | rel: <= 310.7 (5% over 295.9) | pass |
| hdel | delete_ms | ms | 1,319 | 1,319 | 1,235 | 1,344 | info | info |
| hdel | rss_after_15s_mib | MiB | 74.20 | 13.90 | 13.70 | 13.60 | rel: <= 18.60 (25% over 13.60, +5) | pass |
| hdel | rss_after_1_5s_mib | MiB | 74.20 | 13.90 | 39.40 | 13.60 | rel: <= 18.60 (25% over 13.60, +5)<br>ratio: gain over v1.2.2 5.34x >= 3.27x (60% of recorded 5.45x) | pass<br>pass |
| hdel | rss_after_5s_mib | MiB | 74.20 | 13.90 | 27.80 | 13.60 | rel: <= 18.60 (25% over 13.60, +5) | pass |
| hdel | rss_loaded_mib | MiB | 137.8 | 138.5 | 47.34 | 138.2 | rel: <= 145.1 (5% over 138.2) | pass |
| hdel | rss_start_mib | MiB | 12.78 | 13.41 | 12.67 | 13.18 | info | info |
| hdel | used_after_mib | MiB | 1.36 | 1.39 | 0.846 | 1.39 | info | info |
| hdel | used_loaded_mib | MiB | 132.8 | 132.8 | 29.50 | 132.8 | rel: <= 139.5 (5% over 132.8) | pass |

## 04-long-commands

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| tokio | dbsize | keys | 2,000,000 | 2,000,000 | 2,000,000 | 2,000,000 | bound: == 2,000,000 | pass |
| tokio | del_cmd_ms | ms | 134.8 | 138.5 | 347.3 | 143.3 | rel: <= 157.7 (10% over 143.3) | pass |
| tokio | del_probes_stalled | probes | 4 | 0 | 4 | 0 | info | info |
| tokio | del_reply | items | 1 | 1 | 1 | 1 | bound: == 1 | pass |
| tokio | del_worst_ping_max_ms | ms | 134.2 | 4.81 | 351.3 | 1.22 | bound: <= 20.00 | pass |
| tokio | del_worst_ping_ms | ms | 129.6 | 1.25 | 346.9 | 0.942 | bound: <= 20.00 | pass |
| tokio | keys_cmd_ms | ms | 142.1 | 163.0 | 224.4 | 161.3 | rel: <= 177.5 (10% over 161.3) | pass |
| tokio | keys_probes_stalled | probes | 4 | 0 | 4 | 0 | info | info |
| tokio | keys_reply | items | 11,111 | 11,111 | 11,111 | 11,111 | bound: == 11,111 | pass |
| tokio | keys_worst_ping_max_ms | ms | 132.3 | 1.44 | 219.8 | 8.09 | bound: <= 20.00 | pass |
| tokio | keys_worst_ping_ms | ms | 131.9 | 0.347 | 214.1 | 5.94 | bound: <= 20.00 | pass |
| tokio | scan_cmd_ms | ms | 271.4 | 115.0 | 477.8 | 112.5 | rel: <= 123.8 (10% over 112.5) | pass |
| tokio | scan_probes_stalled | probes | 0 | 0 | 4 | 0 | info | info |
| tokio | scan_reply | items | 11,111 | 11,111 | 11,111 | 11,111 | info | info |
| tokio | scan_worst_ping_max_ms | ms | 246.6 | 1.95 | 467.5 | 5.19 | bound: <= 20.00 | pass |
| tokio | scan_worst_ping_ms | ms | 7.25 | 1.27 | 467.0 | 0.449 | bound: <= 20.00 | pass |
| uring | dbsize | keys | 2,000,000 | 2,000,000 | - | 2,000,000 | bound: == 2,000,000 | pass |
| uring | del_cmd_ms | ms | 132.9 | 134.3 | - | 134.3 | rel: <= 147.7 (10% over 134.3) | pass |
| uring | del_least_worst_ping_ms | ms | 0.09 | 0.11 | - | 0.41 | rel: <= 10.41 (50% over 0.41, +10) | pass |
| uring | del_probes_stalled | probes | 2 | 2 | - | 1 | info | info |
| uring | del_reply | items | 1 | 1 | - | 1 | bound: == 1 | pass |
| uring | del_worst_ping_ms | ms | 132.1 | 133.7 | - | 132.9 | info | info |
| uring | keys_cmd_ms | ms | 145.4 | 159.2 | - | 153.6 | rel: <= 169.0 (10% over 153.6) | pass |
| uring | keys_least_worst_ping_ms | ms | 0.25 | 0.09 | - | 0.95 | rel: <= 10.95 (50% over 0.95, +10) | pass |
| uring | keys_probes_stalled | probes | 2 | 1 | - | 2 | info | info |
| uring | keys_reply | items | 11,111 | 11,111 | - | 11,111 | bound: == 11,111 | pass |
| uring | keys_worst_ping_ms | ms | 134.5 | 148.0 | - | 142.8 | info | info |
| uring | scan_cmd_ms | ms | 266.1 | 109.6 | - | 112.6 | rel: <= 123.9 (10% over 112.6) | pass |
| uring | scan_least_worst_ping_ms | ms | 0.16 | 0.09 | - | 0.13 | rel: <= 10.13 (50% over 0.13, +10) | pass |
| uring | scan_probes_stalled | probes | 2 | 2 | - | 1 | info | info |
| uring | scan_reply | items | 11,111 | 11,111 | - | 11,111 | info | info |
| uring | scan_worst_ping_ms | ms | 255.4 | 98.14 | - | 101.6 | info | info |

## Not run this time

`02-growth-ping/ex17m/dbsize`, `02-growth-ping/ex17m/load_s`, `02-growth-ping/ex17m/ping_max_ms`, `02-growth-ping/ex17m/ping_p99_ms`, `02-growth-ping/ex17m/pings`, `02-growth-ping/ex17m/pings_over_10ms`, `02-growth-ping/ex17m/rss_loaded_mib`, `02-growth-ping/ex17m/used_memory_mib`

