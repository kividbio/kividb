# Regression suite: pr759-d4s_v5

Verdict: **ok**

- Machine: Standard_D4s_v5, 4 vCPU, Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz, kernel 7.0.0-1017-azure, glibc 2.39
- Client: Python 3.12.3, redis-py 5.2.1
- v1_2_2: `kividb 1.2.2`, sha256 `38fc2cea40c131e041f4914527a98719e5d34dffdfe2abdc1f07ecb86aa6723e`
- candidate: `kividb 1.2.1 (default build: no Lua, no TLS)`, sha256 `6d330548051f5ebf28f1b7bff5eeeaefdd1e85749a802c48b20203b034b36c7f`
- redis: `Redis server v=8.10.2 sha=00000000:1 malloc=jemalloc-5.3.0 bits=64 build=75a9b36b1f0aa00`, sha256 `b0cd304980112fa6cd8dced524fdd6b136fd8efb802f5cf9e81610051a11891c`
- candidate commit: `a2cc71d5`
- date: 2026-11-14; FULL=1, SCALE=1
- run time: 14-sinter-big-small 205s

## 14-sinter-big-small

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| default | 1m_big_card | members | 1,000,000 | 1,000,000 | 1,000,000 | - | bound: eq: nothing recorded | new |
| default | 1m_sdiff_small_big_ms | ms | 69.70 | 1.95 | 2.56 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.76x Redis <= 2x | new<br>new<br>pass |
| default | 1m_sdiff_small_big_reply | members | 1,999 | 1,999 | 1,999 | - | bound: eq: nothing recorded | new |
| default | 1m_sinter_big_small_ms | ms | 94.62 | 2.18 | 2.58 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.84x Redis <= 2x | new<br>new<br>pass |
| default | 1m_sinter_big_small_reply | members | 2,097 | 2,097 | 2,097 | - | bound: eq: nothing recorded | new |
| default | 1m_sinter_small_big_ms | ms | 70.25 | 2.12 | 2.3 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.92x Redis <= 2x | new<br>new<br>pass |
| default | 1m_sinter_small_big_reply | members | 2,097 | 2,097 | 2,097 | - | bound: eq: nothing recorded | new |
| default | 1m_sintercard_limit_ms | ms | 91.57 | 0.056 | 0.0593 | - | rel: rel 50%: nothing recorded | new |
| default | 1m_sintercard_limit_reply | members | 100.0 | 100.0 | 100.0 | - | bound: eq: nothing recorded | new |
| default | 1m_sintercard_ms | ms | 93.02 | 0.19 | 0.464 | - | rel: rel 50%: nothing recorded | new |
| default | 1m_sintercard_reply | members | 2,097 | 2,097 | 2,097 | - | bound: eq: nothing recorded | new |
| default | 1m_sinterstore_ms | ms | 92.69 | 0.302 | 0.866 | - | rel: rel 50%: nothing recorded | new |
| default | 1m_sinterstore_reply | members | 2,097 | 2,097 | 2,097 | - | bound: eq: nothing recorded | new |
| default | 5m_big_card | members | 5,000,000 | 5,000,000 | 5,000,000 | - | bound: eq: nothing recorded | new |
| default | 5m_sdiff_small_big_ms | ms | 449.1 | 2.12 | 3.85 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.55x Redis <= 2x | new<br>new<br>pass |
| default | 5m_sdiff_small_big_reply | members | 1,998 | 1,998 | 1,998 | - | bound: eq: nothing recorded | new |
| default | 5m_sinter_big_small_ms | ms | 534.3 | 2.27 | 3.48 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.65x Redis <= 2x | new<br>new<br>pass |
| default | 5m_sinter_big_small_reply | members | 2,098 | 2,098 | 2,098 | - | bound: eq: nothing recorded | new |
| default | 5m_sinter_small_big_ms | ms | 450.6 | 2.11 | 3.37 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.63x Redis <= 2x | new<br>new<br>pass |
| default | 5m_sinter_small_big_reply | members | 2,098 | 2,098 | 2,098 | - | bound: eq: nothing recorded | new |
| default | 5m_sintercard_limit_ms | ms | 532.3 | 0.0566 | 0.11 | - | rel: rel 50%: nothing recorded | new |
| default | 5m_sintercard_limit_reply | members | 100.0 | 100.0 | 100.0 | - | bound: eq: nothing recorded | new |
| default | 5m_sintercard_ms | ms | 529.7 | 0.238 | 1.55 | - | rel: rel 50%: nothing recorded | new |
| default | 5m_sintercard_reply | members | 2,098 | 2,098 | 2,098 | - | bound: eq: nothing recorded | new |
| default | 5m_sinterstore_ms | ms | 532.2 | 0.349 | 2.15 | - | rel: rel 50%: nothing recorded | new |
| default | 5m_sinterstore_reply | members | 2,098 | 2,098 | 2,098 | - | bound: eq: nothing recorded | new |
| n20m | 20m_big_card | members | 20,000,000 | 20,000,000 | 20,000,000 | - | bound: eq: nothing recorded | new |
| n20m | 20m_sdiff_small_big_ms | ms | 2,448 | 1.98 | 5 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.40x Redis <= 2x | new<br>new<br>pass |
| n20m | 20m_sdiff_small_big_reply | members | 1,999 | 1,999 | 1,999 | - | bound: eq: nothing recorded | new |
| n20m | 20m_sinter_big_small_ms | ms | 2,555 | 2.28 | 4.69 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.49x Redis <= 2x | new<br>new<br>pass |
| n20m | 20m_sinter_big_small_reply | members | 2,097 | 2,097 | 2,097 | - | bound: eq: nothing recorded | new |
| n20m | 20m_sinter_small_big_ms | ms | 2,431 | 2.08 | 4.37 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.48x Redis <= 2x | new<br>new<br>pass |
| n20m | 20m_sinter_small_big_reply | members | 2,097 | 2,097 | 2,097 | - | bound: eq: nothing recorded | new |
| n20m | 20m_sintercard_limit_ms | ms | 2,531 | 0.0561 | 0.125 | - | rel: rel 50%: nothing recorded | new |
| n20m | 20m_sintercard_limit_reply | members | 100.0 | 100.0 | 100.0 | - | bound: eq: nothing recorded | new |
| n20m | 20m_sintercard_ms | ms | 2,533 | 0.258 | 1.91 | - | rel: rel 50%: nothing recorded | new |
| n20m | 20m_sintercard_reply | members | 2,097 | 2,097 | 2,097 | - | bound: eq: nothing recorded | new |
| n20m | 20m_sinterstore_ms | ms | 2,535 | 0.369 | 3.1 | - | rel: rel 50%: nothing recorded | new |
| n20m | 20m_sinterstore_reply | members | 2,097 | 2,097 | 2,097 | - | bound: eq: nothing recorded | new |

## Not run this time

`01-scan-match/default/dbsize`, `01-scan-match/default/hscan_match_c1000_calls`, `01-scan-match/default/hscan_match_c1000_distinct`, `01-scan-match/default/hscan_match_c1000_ms`, `01-scan-match/default/keys_schedule_ms`, `01-scan-match/default/keys_schedule_returned`, `01-scan-match/default/scan_all_c1000_calls`, `01-scan-match/default/scan_all_c1000_distinct`, `01-scan-match/default/scan_all_c1000_ms`, `01-scan-match/default/scan_infix_c1000_calls`, `01-scan-match/default/scan_infix_c1000_distinct`, `01-scan-match/default/scan_infix_c1000_ms`, `01-scan-match/default/scan_match_c10000_calls`, `01-scan-match/default/scan_match_c10000_distinct`, `01-scan-match/default/scan_match_c10000_ms`, `01-scan-match/default/scan_match_c1000_calls`, `01-scan-match/default/scan_match_c1000_distinct`, `01-scan-match/default/scan_match_c1000_ms`, `01-scan-match/default/sscan_match_c1000_calls`, `01-scan-match/default/sscan_match_c1000_distinct`, `01-scan-match/default/sscan_match_c1000_ms`, `01-scan-match/default/zscan_match_c1000_calls`, `01-scan-match/default/zscan_match_c1000_distinct`, `01-scan-match/default/zscan_match_c1000_ms`, `02-growth-ping/ex17m/dbsize`, `02-growth-ping/ex17m/load_s`, `02-growth-ping/ex17m/ping_max_ms`, `02-growth-ping/ex17m/ping_p99_ms`, `02-growth-ping/ex17m/pings`, `02-growth-ping/ex17m/pings_over_10ms`, `02-growth-ping/ex17m/rss_loaded_mib`, `02-growth-ping/ex17m/used_memory_mib`, `02-growth-ping/tokio/dbsize`, `02-growth-ping/tokio/load_s`, `02-growth-ping/tokio/ping_max_ms`, `02-growth-ping/tokio/ping_p99_ms`, `02-growth-ping/tokio/pings`, `02-growth-ping/tokio/pings_over_10ms`, `02-growth-ping/tokio/rss_loaded_mib`, `02-growth-ping/tokio/used_memory_mib`, `02-growth-ping/uring1/dbsize`, `02-growth-ping/uring1/load_s`, `02-growth-ping/uring1/ping_max_ms`, `02-growth-ping/uring1/ping_p99_ms`, `02-growth-ping/uring1/pings`, `02-growth-ping/uring1/pings_over_10ms`, `02-growth-ping/uring1/rss_loaded_mib`, `02-growth-ping/uring1/used_memory_mib`, `03-give-back/churn/delete_ms`, `03-give-back/churn/rss_after_15s_mib`, `03-give-back/churn/rss_after_1_5s_mib`, `03-give-back/churn/rss_after_5s_mib`, `03-give-back/churn/rss_loaded_mib`, `03-give-back/churn/rss_start_mib`, `03-give-back/churn/used_after_mib`, `03-give-back/churn/used_loaded_mib`, `03-give-back/hashdel/delete_ms`, `03-give-back/hashdel/rss_after_15s_mib`, `03-give-back/hashdel/rss_after_1_5s_mib`, `03-give-back/hashdel/rss_after_5s_mib`, `03-give-back/hashdel/rss_loaded_mib`, `03-give-back/hashdel/rss_start_mib`, `03-give-back/hashdel/used_after_mib`, `03-give-back/hashdel/used_loaded_mib`, `03-give-back/hdel/delete_ms`, `03-give-back/hdel/rss_after_15s_mib`, `03-give-back/hdel/rss_after_1_5s_mib`, `03-give-back/hdel/rss_after_5s_mib`, `03-give-back/hdel/rss_loaded_mib`, `03-give-back/hdel/rss_start_mib`, `03-give-back/hdel/used_after_mib`, `03-give-back/hdel/used_loaded_mib`, `04-long-commands/tokio/dbsize`, `04-long-commands/tokio/del_cmd_ms`, `04-long-commands/tokio/del_probes_stalled`, `04-long-commands/tokio/del_reply`, `04-long-commands/tokio/del_worst_ping_max_ms`, `04-long-commands/tokio/del_worst_ping_ms`, `04-long-commands/tokio/keys_cmd_ms`, `04-long-commands/tokio/keys_probes_stalled`, `04-long-commands/tokio/keys_reply`, `04-long-commands/tokio/keys_worst_ping_max_ms`, `04-long-commands/tokio/keys_worst_ping_ms`, `04-long-commands/tokio/scan_cmd_ms`, `04-long-commands/tokio/scan_probes_stalled`, `04-long-commands/tokio/scan_reply`, `04-long-commands/tokio/scan_worst_ping_max_ms`, `04-long-commands/tokio/scan_worst_ping_ms`, `04-long-commands/uring/dbsize`, `04-long-commands/uring/del_cmd_ms`, `04-long-commands/uring/del_least_worst_ping_ms`, `04-long-commands/uring/del_probes_stalled`, `04-long-commands/uring/del_reply`, `04-long-commands/uring/del_worst_ping_ms`, `04-long-commands/uring/keys_cmd_ms`, `04-long-commands/uring/keys_least_worst_ping_ms`, `04-long-commands/uring/keys_probes_stalled`, `04-long-commands/uring/keys_reply`, `04-long-commands/uring/keys_worst_ping_ms`, `04-long-commands/uring/scan_cmd_ms`, `04-long-commands/uring/scan_least_worst_ping_ms`, `04-long-commands/uring/scan_probes_stalled`, `04-long-commands/uring/scan_reply`, `04-long-commands/uring/scan_worst_ping_ms`

