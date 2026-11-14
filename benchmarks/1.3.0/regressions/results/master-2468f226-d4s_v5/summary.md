# Regression suite: master-2468f226-d4s_v5

Verdict: **ok**

- Machine: Standard_D4s_v5, 4 vCPU, Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz, kernel 7.0.0-1017-azure, glibc 2.39
- Client: Python 3.12.3, redis-py 5.2.1
- v1_2_2: `kividb 1.2.2`, sha256 `38fc2cea40c131e041f4914527a98719e5d34dffdfe2abdc1f07ecb86aa6723e`
- candidate: `kividb 1.2.1 (default build: no Lua, no TLS)`, sha256 `76dcdd0414c70f4501de77d99f9dcf189c2b4d5f626ac6c0fc12843aa9d6e7af`
- redis: `Redis server v=8.10.2 sha=00000000:1 malloc=jemalloc-5.3.0 bits=64 build=75a9b36b1f0aa00`, sha256 `b0cd304980112fa6cd8dced524fdd6b136fd8efb802f5cf9e81610051a11891c`
- candidate commit: `2468f226`
- date: 2026-11-14
- run time: 01-scan-match 136s, 02-growth-ping 711s, 03-give-back 200s, 04-long-commands 200s

## 01-scan-match

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| default | dbsize | keys | 2,002,003 | 2,002,003 | 2,002,003 | - | bound: eq: nothing recorded | new |
| default | hscan_match_c1000_calls | calls | 201.0 | 201.0 | 201.0 | - | info | info |
| default | hscan_match_c1000_distinct | keys | 200.0 | 200.0 | 200.0 | - | bound: eq: nothing recorded | new |
| default | hscan_match_c1000_ms | ms | 443.7 | 428.0 | 41.37 | - | rel: rel 15%: nothing recorded | new |
| default | keys_schedule_ms | ms | 116.9 | 60.44 | 130.6 | - | rel: rel 10%: nothing recorded | new |
| default | keys_schedule_returned | keys | 2,000 | 2,000 | 2,000 | - | bound: eq: nothing recorded | new |
| default | scan_all_c1000_calls | calls | 513.0 | 512.0 | 2,002 | - | info | info |
| default | scan_all_c1000_distinct | keys | 2,002,003 | 2,002,003 | 2,002,003 | - | bound: eq: nothing recorded | new |
| default | scan_all_c1000_ms | ms | 3,616 | 2,180 | 2,213 | - | rel: rel 10%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.99x Redis <= 1.3x | new<br>new<br>pass |
| default | scan_infix_c1000_calls | calls | 513.0 | 512.0 | 2,002 | - | info | info |
| default | scan_infix_c1000_distinct | keys | 1,112,222 | 1,112,222 | 1,112,222 | - | bound: eq: nothing recorded | new |
| default | scan_infix_c1000_ms | ms | 2,696 | 1,494 | 1,801 | - | rel: rel 10%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.83x Redis <= 1.3x | new<br>new<br>pass |
| default | scan_match_c10000_calls | calls | 193.0 | 193.0 | 201.0 | - | info | info |
| default | scan_match_c10000_distinct | keys | 2,000 | 2,000 | 2,000 | - | bound: eq: nothing recorded | new |
| default | scan_match_c10000_ms | ms | 823.3 | 238.9 | 390.7 | - | rel: rel 10%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.61x Redis <= 1.3x | new<br>new<br>pass |
| default | scan_match_c1000_calls | calls | 513.0 | 512.0 | 2,002 | - | info | info |
| default | scan_match_c1000_distinct | keys | 2,000 | 2,000 | 2,000 | - | bound: eq: nothing recorded | new |
| default | scan_match_c1000_ms | ms | 1,672 | 406.2 | 570.5 | - | rel: rel 10%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.71x Redis <= 1.3x | new<br>new<br>pass |
| default | sscan_match_c1000_calls | calls | 201.0 | 201.0 | 201.0 | - | info | info |
| default | sscan_match_c1000_distinct | keys | 200.0 | 200.0 | 200.0 | - | bound: eq: nothing recorded | new |
| default | sscan_match_c1000_ms | ms | 943.5 | 1,093 | 38.86 | - | rel: rel 15%: nothing recorded | new |
| default | zscan_match_c1000_calls | calls | 201.0 | 201.0 | 201.0 | - | info | info |
| default | zscan_match_c1000_distinct | keys | 200.0 | 200.0 | 200.0 | - | bound: eq: nothing recorded | new |
| default | zscan_match_c1000_ms | ms | 799.5 | 829.5 | 50.42 | - | rel: rel 15%: nothing recorded | new |

## 02-growth-ping

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| ex17m | dbsize | keys | 17,500,000 | 17,500,000 | 17,500,000 | - | bound: eq: nothing recorded | new |
| ex17m | load_s | s | 145.5 | 143.2 | 149.1 | - | rel: rel 10%: nothing recorded | new |
| ex17m | ping_max_ms | ms | 160.5 | 17.02 | 12.16 | - | rel: rel 50%: nothing recorded | new |
| ex17m | ping_p99_ms | ms | 3.36 | 3.64 | 1.91 | - | rel: rel 50%: nothing recorded | new |
| ex17m | pings | pings | 122,998 | 120,736 | 133,811 | - | info | info |
| ex17m | pings_over_10ms | pings | 71.00 | 14.00 | 1 | - | info | info |
| ex17m | rss_loaded_mib | MiB | 1,506 | 1,507 | 2,084 | - | rel: rel 5%: nothing recorded | new |
| ex17m | used_memory_mib | MiB | 1,613 | 1,613 | 2,027 | - | rel: rel 5%: nothing recorded | new |
| tokio | dbsize | keys | 8,000,000 | 8,000,000 | 8,000,000 | - | bound: eq: nothing recorded | new |
| tokio | load_s | s | 51.28 | 50.84 | 52.23 | - | rel: rel 10%: nothing recorded | new |
| tokio | ping_max_ms | ms | 112.1 | 9.66 | 8.19 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |
| tokio | ping_p99_ms | ms | 1.98 | 3.03 | 1.38 | - | rel: rel 50%: nothing recorded | new |
| tokio | pings | pings | 45,838 | 44,740 | 48,711 | - | info | info |
| tokio | pings_over_10ms | pings | 25.00 | 0 | 0 | - | info | info |
| tokio | rss_loaded_mib | MiB | 524.2 | 525.0 | 552.0 | - | rel: rel 5%: nothing recorded | new |
| tokio | used_memory_mib | MiB | 511.4 | 511.4 | 534.7 | - | rel: rel 5%: nothing recorded | new |
| uring1 | dbsize | keys | 8,000,000 | 8,000,000 | - | - | bound: eq: nothing recorded | new |
| uring1 | load_s | s | 53.25 | 55.60 | - | - | rel: rel 10%: nothing recorded | new |
| uring1 | ping_max_ms | ms | 72.71 | 20.83 | - | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |
| uring1 | ping_p99_ms | ms | 0.712 | 1.54 | - | - | rel: rel 50%: nothing recorded | new |
| uring1 | pings | pings | 49,298 | 51,940 | - | - | info | info |
| uring1 | pings_over_10ms | pings | 29.00 | 2 | - | - | info | info |
| uring1 | rss_loaded_mib | MiB | 524.4 | 524.5 | - | - | rel: rel 5%: nothing recorded | new |
| uring1 | used_memory_mib | MiB | 511.5 | 511.6 | - | - | rel: rel 5%: nothing recorded | new |

## 03-give-back

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| churn | delete_ms | ms | 1,534 | 1,538 | 1,523 | - | info | info |
| churn | rss_after_15s_mib | MiB | 483.5 | 429.4 | 616.5 | - | rel: rel 25%: nothing recorded | new |
| churn | rss_after_1_5s_mib | MiB | 483.5 | 429.4 | 737.1 | - | rel: rel 25%: nothing recorded | new |
| churn | rss_after_5s_mib | MiB | 483.5 | 429.4 | 670.2 | - | rel: rel 25%: nothing recorded | new |
| churn | rss_loaded_mib | MiB | 788.6 | 789.3 | 746.3 | - | rel: rel 5%: nothing recorded | new |
| churn | rss_start_mib | MiB | 12.45 | 12.73 | 12.57 | - | info | info |
| churn | used_after_mib | MiB | 372.1 | 372.1 | 361.8 | - | info | info |
| churn | used_loaded_mib | MiB | 742.5 | 742.6 | 719.1 | - | rel: rel 5%: nothing recorded | new |
| hashdel | delete_ms | ms | 131.0 | 134.4 | 310.2 | - | info | info |
| hashdel | rss_after_15s_mib | MiB | 196.4 | 13.60 | 19.50 | - | rel: rel 25%: nothing recorded | new |
| hashdel | rss_after_1_5s_mib | MiB | 196.4 | 13.60 | 75.80 | - | rel: rel 25%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |
| hashdel | rss_after_5s_mib | MiB | 196.4 | 13.60 | 56.20 | - | rel: rel 25%: nothing recorded | new |
| hashdel | rss_loaded_mib | MiB | 369.6 | 369.9 | 124.5 | - | rel: rel 5%: nothing recorded | new |
| hashdel | rss_start_mib | MiB | 12.84 | 13.28 | 12.67 | - | info | info |
| hashdel | used_after_mib | MiB | 1.36 | 1.39 | 0.846 | - | info | info |
| hashdel | used_loaded_mib | MiB | 295.9 | 295.9 | 109.2 | - | rel: rel 5%: nothing recorded | new |
| hdel | delete_ms | ms | 1,343 | 1,344 | 1,217 | - | info | info |
| hdel | rss_after_15s_mib | MiB | 74.10 | 13.60 | 13.60 | - | rel: rel 25%: nothing recorded | new |
| hdel | rss_after_1_5s_mib | MiB | 74.10 | 13.60 | 39.30 | - | rel: rel 25%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |
| hdel | rss_after_5s_mib | MiB | 74.10 | 13.60 | 24.50 | - | rel: rel 25%: nothing recorded | new |
| hdel | rss_loaded_mib | MiB | 137.7 | 138.2 | 47.28 | - | rel: rel 5%: nothing recorded | new |
| hdel | rss_start_mib | MiB | 12.70 | 13.18 | 12.55 | - | info | info |
| hdel | used_after_mib | MiB | 1.36 | 1.39 | 0.846 | - | info | info |
| hdel | used_loaded_mib | MiB | 132.8 | 132.8 | 29.50 | - | rel: rel 5%: nothing recorded | new |

## 04-long-commands

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| tokio | dbsize | keys | 2,000,000 | 2,000,000 | 2,000,000 | - | bound: eq: nothing recorded | new |
| tokio | del_cmd_ms | ms | 137.4 | 143.2 | 337.7 | - | rel: rel 10%: nothing recorded | new |
| tokio | del_probes_stalled | probes | 4 | 0 | 4 | - | info | info |
| tokio | del_reply | items | 1 | 1 | 1 | - | bound: eq: nothing recorded | new |
| tokio | del_worst_ping_max_ms | ms | 136.4 | 4.43 | 344.3 | - | bound: <= 20.00 | pass |
| tokio | del_worst_ping_ms | ms | 131.4 | 1.3 | 336.9 | - | bound: <= 20.00 | pass |
| tokio | keys_cmd_ms | ms | 141.9 | 164.3 | 217.7 | - | rel: rel 10%: nothing recorded | new |
| tokio | keys_probes_stalled | probes | 4 | 0 | 4 | - | info | info |
| tokio | keys_reply | items | 11,111 | 11,111 | 11,111 | - | bound: eq: nothing recorded | new |
| tokio | keys_worst_ping_max_ms | ms | 131.1 | 1.58 | 208.6 | - | bound: <= 20.00 | pass |
| tokio | keys_worst_ping_ms | ms | 126.7 | 1.56 | 208.3 | - | bound: <= 20.00 | pass |
| tokio | scan_cmd_ms | ms | 254.0 | 111.7 | 468.1 | - | rel: rel 10%: nothing recorded | new |
| tokio | scan_probes_stalled | probes | 4 | 0 | 4 | - | info | info |
| tokio | scan_reply | items | 11,111 | 11,111 | 11,111 | - | info | info |
| tokio | scan_worst_ping_max_ms | ms | 260.5 | 0.722 | 463.3 | - | bound: <= 20.00 | pass |
| tokio | scan_worst_ping_ms | ms | 240.9 | 0.404 | 455.5 | - | bound: <= 20.00 | pass |
| uring | dbsize | keys | 2,000,000 | 2,000,000 | - | - | bound: eq: nothing recorded | new |
| uring | del_cmd_ms | ms | 134.0 | 137.1 | - | - | rel: rel 10%: nothing recorded | new |
| uring | del_others_worst_ping_ms | ms | 2.4 | 0.365 | - | - | rel: rel 50%: nothing recorded | new |
| uring | del_probes_stalled | probes | 1 | 1 | - | - | info | info |
| uring | del_reply | items | 1 | 1 | - | - | bound: eq: nothing recorded | new |
| uring | del_worst_ping_ms | ms | 133.3 | 137.6 | - | - | info | info |
| uring | keys_cmd_ms | ms | 149.3 | 163.2 | - | - | rel: rel 10%: nothing recorded | new |
| uring | keys_others_worst_ping_ms | ms | 1.21 | 71.82 | - | - | rel: rel 50%: nothing recorded | new |
| uring | keys_probes_stalled | probes | 1 | 2 | - | - | info | info |
| uring | keys_reply | items | 11,111 | 11,111 | - | - | bound: eq: nothing recorded | new |
| uring | keys_worst_ping_ms | ms | 139.0 | 152.3 | - | - | info | info |
| uring | scan_cmd_ms | ms | 261.4 | 109.7 | - | - | rel: rel 10%: nothing recorded | new |
| uring | scan_others_worst_ping_ms | ms | 124.6 | 48.58 | - | - | rel: rel 50%: nothing recorded | new |
| uring | scan_probes_stalled | probes | 2 | 2 | - | - | info | info |
| uring | scan_reply | items | 11,111 | 11,111 | - | - | info | info |
| uring | scan_worst_ping_ms | ms | 250.1 | 98.33 | - | - | info | info |

