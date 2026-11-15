# Regression suite: rec04

Verdict: **ok**

- Machine: Standard_D4s_v5, 4 vCPU, Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz, kernel 7.0.0-1017-azure, glibc 2.39
- Client: Python 3.12.3, redis-py 5.2.1
- v1_2_2: `kividb 1.2.2`, sha256 `38fc2cea40c131e041f4914527a98719e5d34dffdfe2abdc1f07ecb86aa6723e`
- candidate: `kividb 1.2.1 (default build: no Lua, no TLS)`, sha256 `76dcdd0414c70f4501de77d99f9dcf189c2b4d5f626ac6c0fc12843aa9d6e7af`
- redis: `Redis server v=8.10.2 sha=00000000:1 malloc=jemalloc-5.3.0 bits=64 build=75a9b36b1f0aa00`, sha256 `b0cd304980112fa6cd8dced524fdd6b136fd8efb802f5cf9e81610051a11891c`
- candidate commit: `2468f226`
- date: 2026-11-14
- run time: 04-long-commands 200s

## 04-long-commands

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| tokio | dbsize | keys | 2,000,000 | 2,000,000 | 2,000,000 | 2,000,000 | bound: == 2,000,000 | pass |
| tokio | del_cmd_ms | ms | 132.2 | 143.3 | 333.5 | 143.2 | rel: <= 157.5 (10% over 143.2) | pass |
| tokio | del_probes_stalled | probes | 4 | 0 | 4 | 0 | info | info |
| tokio | del_reply | items | 1 | 1 | 1 | 1 | bound: == 1 | pass |
| tokio | del_worst_ping_max_ms | ms | 131.2 | 1.22 | 335.7 | 4.43 | bound: <= 20.00 | pass |
| tokio | del_worst_ping_ms | ms | 131.0 | 0.942 | 332.7 | 1.3 | bound: <= 20.00 | pass |
| tokio | keys_cmd_ms | ms | 141.1 | 161.3 | 216.5 | 164.3 | rel: <= 180.7 (10% over 164.3) | pass |
| tokio | keys_probes_stalled | probes | 4 | 0 | 4 | 0 | info | info |
| tokio | keys_reply | items | 11,111 | 11,111 | 11,111 | 11,111 | bound: == 11,111 | pass |
| tokio | keys_worst_ping_max_ms | ms | 132.1 | 8.09 | 205.1 | 1.58 | bound: <= 20.00 | pass |
| tokio | keys_worst_ping_ms | ms | 129.3 | 5.94 | 203.6 | 1.56 | bound: <= 20.00 | pass |
| tokio | scan_cmd_ms | ms | 254.3 | 112.5 | 471.7 | 111.7 | rel: <= 122.9 (10% over 111.7) | pass |
| tokio | scan_probes_stalled | probes | 4 | 0 | 4 | 0 | info | info |
| tokio | scan_reply | items | 11,111 | 11,111 | 11,111 | 11,111 | info | info |
| tokio | scan_worst_ping_max_ms | ms | 243.4 | 5.19 | 467.2 | 0.722 | bound: <= 20.00 | pass |
| tokio | scan_worst_ping_ms | ms | 241.0 | 0.449 | 461.2 | 0.404 | bound: <= 20.00 | pass |
| uring | dbsize | keys | 2,000,000 | 2,000,000 | - | 2,000,000 | bound: == 2,000,000 | pass |
| uring | del_cmd_ms | ms | 136.4 | 134.3 | - | 137.1 | rel: <= 150.8 (10% over 137.1) | pass |
| uring | del_least_worst_ping_ms | ms | 0.21 | 0.41 | - | - | rel: rel 50%: nothing recorded | new |
| uring | del_probes_stalled | probes | 1 | 1 | - | 1 | info | info |
| uring | del_reply | items | 1 | 1 | - | 1 | bound: == 1 | pass |
| uring | del_worst_ping_ms | ms | 135.8 | 132.9 | - | 137.6 | info | info |
| uring | keys_cmd_ms | ms | 145.6 | 153.6 | - | 163.2 | rel: <= 179.6 (10% over 163.2) | pass |
| uring | keys_least_worst_ping_ms | ms | 0.17 | 0.95 | - | - | rel: rel 50%: nothing recorded | new |
| uring | keys_probes_stalled | probes | 2 | 2 | - | 2 | info | info |
| uring | keys_reply | items | 11,111 | 11,111 | - | 11,111 | bound: == 11,111 | pass |
| uring | keys_worst_ping_ms | ms | 134.9 | 142.8 | - | 152.3 | info | info |
| uring | scan_cmd_ms | ms | 265.5 | 112.6 | - | 109.7 | rel: <= 120.7 (10% over 109.7) | pass |
| uring | scan_least_worst_ping_ms | ms | 0.57 | 0.13 | - | - | rel: rel 50%: nothing recorded | new |
| uring | scan_probes_stalled | probes | 2 | 1 | - | 2 | info | info |
| uring | scan_reply | items | 11,111 | 11,111 | - | 11,111 | info | info |
| uring | scan_worst_ping_ms | ms | 254.9 | 101.6 | - | 98.33 | info | info |

## Not run this time

`01-scan-match/default/dbsize`, `01-scan-match/default/hscan_match_c1000_calls`, `01-scan-match/default/hscan_match_c1000_distinct`, `01-scan-match/default/hscan_match_c1000_ms`, `01-scan-match/default/keys_schedule_ms`, `01-scan-match/default/keys_schedule_returned`, `01-scan-match/default/scan_all_c1000_calls`, `01-scan-match/default/scan_all_c1000_distinct`, `01-scan-match/default/scan_all_c1000_ms`, `01-scan-match/default/scan_infix_c1000_calls`, `01-scan-match/default/scan_infix_c1000_distinct`, `01-scan-match/default/scan_infix_c1000_ms`, `01-scan-match/default/scan_match_c10000_calls`, `01-scan-match/default/scan_match_c10000_distinct`, `01-scan-match/default/scan_match_c10000_ms`, `01-scan-match/default/scan_match_c1000_calls`, `01-scan-match/default/scan_match_c1000_distinct`, `01-scan-match/default/scan_match_c1000_ms`, `01-scan-match/default/sscan_match_c1000_calls`, `01-scan-match/default/sscan_match_c1000_distinct`, `01-scan-match/default/sscan_match_c1000_ms`, `01-scan-match/default/zscan_match_c1000_calls`, `01-scan-match/default/zscan_match_c1000_distinct`, `01-scan-match/default/zscan_match_c1000_ms`, `02-growth-ping/ex17m/dbsize`, `02-growth-ping/ex17m/load_s`, `02-growth-ping/ex17m/ping_max_ms`, `02-growth-ping/ex17m/ping_p99_ms`, `02-growth-ping/ex17m/pings`, `02-growth-ping/ex17m/pings_over_10ms`, `02-growth-ping/ex17m/rss_loaded_mib`, `02-growth-ping/ex17m/used_memory_mib`, `02-growth-ping/tokio/dbsize`, `02-growth-ping/tokio/load_s`, `02-growth-ping/tokio/ping_max_ms`, `02-growth-ping/tokio/ping_p99_ms`, `02-growth-ping/tokio/pings`, `02-growth-ping/tokio/pings_over_10ms`, `02-growth-ping/tokio/rss_loaded_mib`, `02-growth-ping/tokio/used_memory_mib`, `02-growth-ping/uring1/dbsize`, `02-growth-ping/uring1/load_s`, `02-growth-ping/uring1/ping_max_ms`, `02-growth-ping/uring1/ping_p99_ms`, `02-growth-ping/uring1/pings`, `02-growth-ping/uring1/pings_over_10ms`, `02-growth-ping/uring1/rss_loaded_mib`, `02-growth-ping/uring1/used_memory_mib`, `03-give-back/churn/delete_ms`, `03-give-back/churn/rss_after_15s_mib`, `03-give-back/churn/rss_after_1_5s_mib`, `03-give-back/churn/rss_after_5s_mib`, `03-give-back/churn/rss_loaded_mib`, `03-give-back/churn/rss_start_mib`, `03-give-back/churn/used_after_mib`, `03-give-back/churn/used_loaded_mib`, `03-give-back/hashdel/delete_ms`, `03-give-back/hashdel/rss_after_15s_mib`, `03-give-back/hashdel/rss_after_1_5s_mib`, `03-give-back/hashdel/rss_after_5s_mib`, `03-give-back/hashdel/rss_loaded_mib`, `03-give-back/hashdel/rss_start_mib`, `03-give-back/hashdel/used_after_mib`, `03-give-back/hashdel/used_loaded_mib`, `03-give-back/hdel/delete_ms`, `03-give-back/hdel/rss_after_15s_mib`, `03-give-back/hdel/rss_after_1_5s_mib`, `03-give-back/hdel/rss_after_5s_mib`, `03-give-back/hdel/rss_loaded_mib`, `03-give-back/hdel/rss_start_mib`, `03-give-back/hdel/used_after_mib`, `03-give-back/hdel/used_loaded_mib`

