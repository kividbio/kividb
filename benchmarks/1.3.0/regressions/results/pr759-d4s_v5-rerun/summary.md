# Regression suite: pr759-d4s_v5-rerun

Verdict: **ok**

- Machine: Standard_D4s_v5, 4 vCPU, Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz, kernel 7.0.0-1017-azure, glibc 2.39
- Client: Python 3.12.3, redis-py 5.2.1
- v1_2_2: `kividb 1.2.2`, sha256 `38fc2cea40c131e041f4914527a98719e5d34dffdfe2abdc1f07ecb86aa6723e`
- candidate: `kividb 1.2.1 (default build: no Lua, no TLS)`, sha256 `6d330548051f5ebf28f1b7bff5eeeaefdd1e85749a802c48b20203b034b36c7f`
- redis: `Redis server v=8.10.2 sha=00000000:1 malloc=jemalloc-5.3.0 bits=64 build=75a9b36b1f0aa00`, sha256 `b0cd304980112fa6cd8dced524fdd6b136fd8efb802f5cf9e81610051a11891c`
- candidate commit: `a2cc71d5`
- date: 2026-11-14; FULL=1, SCALE=1
- run time: 14-sinter-big-small 210s

## 14-sinter-big-small

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| default | 1m_big_card | members | 1,000,000 | 1,000,000 | 1,000,000 | 1,000,000 | bound: == 1,000,000 | pass |
| default | 1m_sdiff_small_big_ms | ms | 71.46 | 2.02 | 2.8 | 1.95 | rel: <= 3.95 (50% over 1.95, +2)<br>ratio: gain over v1.2.2 35.44x >= 21.43x (60% of recorded 35.72x)<br>vs_redis: 0.72x Redis <= 2x | pass<br>pass<br>pass |
| default | 1m_sdiff_small_big_reply | members | 1,999 | 1,999 | 1,999 | 1,999 | bound: == 1,999 | pass |
| default | 1m_sinter_big_small_ms | ms | 95.96 | 2.36 | 2.62 | 2.18 | rel: <= 4.18 (50% over 2.18, +2)<br>ratio: gain over v1.2.2 40.59x >= 26.04x (60% of recorded 43.39x)<br>vs_redis: 0.90x Redis <= 2x | pass<br>pass<br>pass |
| default | 1m_sinter_big_small_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| default | 1m_sinter_small_big_ms | ms | 71.57 | 2.19 | 2.39 | 2.12 | rel: <= 4.12 (50% over 2.12, +2)<br>ratio: gain over v1.2.2 32.72x >= 19.88x (60% of recorded 33.13x)<br>vs_redis: 0.92x Redis <= 2x | pass<br>pass<br>pass |
| default | 1m_sinter_small_big_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| default | 1m_sintercard_limit_ms | ms | 95.73 | 0.0755 | 0.0603 | 0.056 | rel: <= 2.06 (50% over 0.056, +2) | pass |
| default | 1m_sintercard_limit_reply | members | 100.0 | 100.0 | 100.0 | 100.0 | bound: == 100.0 | pass |
| default | 1m_sintercard_ms | ms | 94.27 | 0.25 | 0.47 | 0.19 | rel: <= 2.19 (50% over 0.19, +2) | pass |
| default | 1m_sintercard_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| default | 1m_sinterstore_ms | ms | 95.36 | 0.37 | 0.895 | 0.302 | rel: <= 2.3 (50% over 0.302, +2) | pass |
| default | 1m_sinterstore_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| default | 5m_big_card | members | 5,000,000 | 5,000,000 | 5,000,000 | 5,000,000 | bound: == 5,000,000 | pass |
| default | 5m_sdiff_small_big_ms | ms | 454.7 | 2.01 | 4.37 | 2.12 | rel: <= 4.12 (50% over 2.12, +2)<br>ratio: gain over v1.2.2 225.92x >= 127.11x (60% of recorded 211.84x)<br>vs_redis: 0.46x Redis <= 2x | pass<br>pass<br>pass |
| default | 5m_sdiff_small_big_reply | members | 1,998 | 1,998 | 1,998 | 1,998 | bound: == 1,998 | pass |
| default | 5m_sinter_big_small_ms | ms | 543.2 | 2.26 | 4.03 | 2.27 | rel: <= 4.27 (50% over 2.27, +2)<br>ratio: gain over v1.2.2 240.59x >= 141.25x (60% of recorded 235.41x)<br>vs_redis: 0.56x Redis <= 2x | pass<br>pass<br>pass |
| default | 5m_sinter_big_small_reply | members | 2,098 | 2,098 | 2,098 | 2,098 | bound: == 2,098 | pass |
| default | 5m_sinter_small_big_ms | ms | 457.5 | 2.11 | 3.86 | 2.11 | rel: <= 4.11 (50% over 2.11, +2)<br>ratio: gain over v1.2.2 217.02x >= 128.21x (60% of recorded 213.68x)<br>vs_redis: 0.55x Redis <= 2x | pass<br>pass<br>pass |
| default | 5m_sinter_small_big_reply | members | 2,098 | 2,098 | 2,098 | 2,098 | bound: == 2,098 | pass |
| default | 5m_sintercard_limit_ms | ms | 548.2 | 0.0559 | 0.116 | 0.0566 | rel: <= 2.06 (50% over 0.0566, +2) | pass |
| default | 5m_sintercard_limit_reply | members | 100.0 | 100.0 | 100.0 | 100.0 | bound: == 100.0 | pass |
| default | 5m_sintercard_ms | ms | 547.4 | 0.238 | 1.66 | 0.238 | rel: <= 2.24 (50% over 0.238, +2) | pass |
| default | 5m_sintercard_reply | members | 2,098 | 2,098 | 2,098 | 2,098 | bound: == 2,098 | pass |
| default | 5m_sinterstore_ms | ms | 540.3 | 0.349 | 2.32 | 0.349 | rel: <= 2.35 (50% over 0.349, +2) | pass |
| default | 5m_sinterstore_reply | members | 2,098 | 2,098 | 2,098 | 2,098 | bound: == 2,098 | pass |
| n20m | 20m_big_card | members | 20,000,000 | 20,000,000 | 20,000,000 | 20,000,000 | bound: == 20,000,000 | pass |
| n20m | 20m_sdiff_small_big_ms | ms | 2,502 | 2.04 | 4.69 | 1.98 | rel: <= 3.98 (50% over 1.98, +2)<br>ratio: gain over v1.2.2 1229.28x >= 740.93x (60% of recorded 1234.88x)<br>vs_redis: 0.43x Redis <= 2x | pass<br>pass<br>pass |
| n20m | 20m_sdiff_small_big_reply | members | 1,999 | 1,999 | 1,999 | 1,999 | bound: == 1,999 | pass |
| n20m | 20m_sinter_big_small_ms | ms | 2,716 | 2.27 | 4.73 | 2.28 | rel: <= 4.28 (50% over 2.28, +2)<br>ratio: gain over v1.2.2 1194.14x >= 672.15x (60% of recorded 1120.26x)<br>vs_redis: 0.48x Redis <= 2x | pass<br>pass<br>pass |
| n20m | 20m_sinter_big_small_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| n20m | 20m_sinter_small_big_ms | ms | 2,523 | 2.13 | 4.44 | 2.08 | rel: <= 4.08 (50% over 2.08, +2)<br>ratio: gain over v1.2.2 1183.17x >= 700.77x (60% of recorded 1167.94x)<br>vs_redis: 0.48x Redis <= 2x | pass<br>pass<br>pass |
| n20m | 20m_sinter_small_big_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| n20m | 20m_sintercard_limit_ms | ms | 2,701 | 0.054 | 0.129 | 0.0561 | rel: <= 2.06 (50% over 0.0561, +2) | pass |
| n20m | 20m_sintercard_limit_reply | members | 100.0 | 100.0 | 100.0 | 100.0 | bound: == 100.0 | pass |
| n20m | 20m_sintercard_ms | ms | 2,714 | 0.256 | 1.88 | 0.258 | rel: <= 2.26 (50% over 0.258, +2) | pass |
| n20m | 20m_sintercard_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| n20m | 20m_sinterstore_ms | ms | 2,675 | 0.371 | 3.13 | 0.369 | rel: <= 2.37 (50% over 0.369, +2) | pass |
| n20m | 20m_sinterstore_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |

## Not run this time

`01-scan-match/default/dbsize`, `01-scan-match/default/hscan_match_c1000_calls`, `01-scan-match/default/hscan_match_c1000_distinct`, `01-scan-match/default/hscan_match_c1000_ms`, `01-scan-match/default/keys_schedule_ms`, `01-scan-match/default/keys_schedule_returned`, `01-scan-match/default/scan_all_c1000_calls`, `01-scan-match/default/scan_all_c1000_distinct`, `01-scan-match/default/scan_all_c1000_ms`, `01-scan-match/default/scan_infix_c1000_calls`, `01-scan-match/default/scan_infix_c1000_distinct`, `01-scan-match/default/scan_infix_c1000_ms`, `01-scan-match/default/scan_match_c10000_calls`, `01-scan-match/default/scan_match_c10000_distinct`, `01-scan-match/default/scan_match_c10000_ms`, `01-scan-match/default/scan_match_c1000_calls`, `01-scan-match/default/scan_match_c1000_distinct`, `01-scan-match/default/scan_match_c1000_ms`, `01-scan-match/default/sscan_match_c1000_calls`, `01-scan-match/default/sscan_match_c1000_distinct`, `01-scan-match/default/sscan_match_c1000_ms`, `01-scan-match/default/zscan_match_c1000_calls`, `01-scan-match/default/zscan_match_c1000_distinct`, `01-scan-match/default/zscan_match_c1000_ms`, `02-growth-ping/ex17m/dbsize`, `02-growth-ping/ex17m/load_s`, `02-growth-ping/ex17m/ping_max_ms`, `02-growth-ping/ex17m/ping_p99_ms`, `02-growth-ping/ex17m/pings`, `02-growth-ping/ex17m/pings_over_10ms`, `02-growth-ping/ex17m/rss_loaded_mib`, `02-growth-ping/ex17m/used_memory_mib`, `02-growth-ping/tokio/dbsize`, `02-growth-ping/tokio/load_s`, `02-growth-ping/tokio/ping_max_ms`, `02-growth-ping/tokio/ping_p99_ms`, `02-growth-ping/tokio/pings`, `02-growth-ping/tokio/pings_over_10ms`, `02-growth-ping/tokio/rss_loaded_mib`, `02-growth-ping/tokio/used_memory_mib`, `02-growth-ping/uring1/dbsize`, `02-growth-ping/uring1/load_s`, `02-growth-ping/uring1/ping_max_ms`, `02-growth-ping/uring1/ping_p99_ms`, `02-growth-ping/uring1/pings`, `02-growth-ping/uring1/pings_over_10ms`, `02-growth-ping/uring1/rss_loaded_mib`, `02-growth-ping/uring1/used_memory_mib`, `03-give-back/churn/delete_ms`, `03-give-back/churn/rss_after_15s_mib`, `03-give-back/churn/rss_after_1_5s_mib`, `03-give-back/churn/rss_after_5s_mib`, `03-give-back/churn/rss_loaded_mib`, `03-give-back/churn/rss_start_mib`, `03-give-back/churn/used_after_mib`, `03-give-back/churn/used_loaded_mib`, `03-give-back/hashdel/delete_ms`, `03-give-back/hashdel/rss_after_15s_mib`, `03-give-back/hashdel/rss_after_1_5s_mib`, `03-give-back/hashdel/rss_after_5s_mib`, `03-give-back/hashdel/rss_loaded_mib`, `03-give-back/hashdel/rss_start_mib`, `03-give-back/hashdel/used_after_mib`, `03-give-back/hashdel/used_loaded_mib`, `03-give-back/hdel/delete_ms`, `03-give-back/hdel/rss_after_15s_mib`, `03-give-back/hdel/rss_after_1_5s_mib`, `03-give-back/hdel/rss_after_5s_mib`, `03-give-back/hdel/rss_loaded_mib`, `03-give-back/hdel/rss_start_mib`, `03-give-back/hdel/used_after_mib`, `03-give-back/hdel/used_loaded_mib`, `04-long-commands/tokio/dbsize`, `04-long-commands/tokio/del_cmd_ms`, `04-long-commands/tokio/del_probes_stalled`, `04-long-commands/tokio/del_reply`, `04-long-commands/tokio/del_worst_ping_max_ms`, `04-long-commands/tokio/del_worst_ping_ms`, `04-long-commands/tokio/keys_cmd_ms`, `04-long-commands/tokio/keys_probes_stalled`, `04-long-commands/tokio/keys_reply`, `04-long-commands/tokio/keys_worst_ping_max_ms`, `04-long-commands/tokio/keys_worst_ping_ms`, `04-long-commands/tokio/scan_cmd_ms`, `04-long-commands/tokio/scan_probes_stalled`, `04-long-commands/tokio/scan_reply`, `04-long-commands/tokio/scan_worst_ping_max_ms`, `04-long-commands/tokio/scan_worst_ping_ms`, `04-long-commands/uring/dbsize`, `04-long-commands/uring/del_cmd_ms`, `04-long-commands/uring/del_least_worst_ping_ms`, `04-long-commands/uring/del_probes_stalled`, `04-long-commands/uring/del_reply`, `04-long-commands/uring/del_worst_ping_ms`, `04-long-commands/uring/keys_cmd_ms`, `04-long-commands/uring/keys_least_worst_ping_ms`, `04-long-commands/uring/keys_probes_stalled`, `04-long-commands/uring/keys_reply`, `04-long-commands/uring/keys_worst_ping_ms`, `04-long-commands/uring/scan_cmd_ms`, `04-long-commands/uring/scan_least_worst_ping_ms`, `04-long-commands/uring/scan_probes_stalled`, `04-long-commands/uring/scan_reply`, `04-long-commands/uring/scan_worst_ping_ms`

