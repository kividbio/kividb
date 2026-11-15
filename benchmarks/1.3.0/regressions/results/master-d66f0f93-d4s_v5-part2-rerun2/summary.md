# Regression suite: master-d66f0f93-d4s_v5-part2-rerun2

Verdict: **ok**

- Machine: Standard_D4s_v5, 4 vCPU, Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz, kernel 7.0.0-1017-azure, glibc 2.39
- Client: Python 3.12.3, redis-py 5.2.1
- v1_2_2: `kividb 1.2.2`, sha256 `38fc2cea40c131e041f4914527a98719e5d34dffdfe2abdc1f07ecb86aa6723e`
- candidate: `kividb 1.2.1 (default build: no Lua, no TLS)`, sha256 `9771dc1ec052679fa29498cf39b3daedb3f5718d29bc1501732aa32c2df5f556`
- redis: `Redis server v=8.10.2 sha=00000000:1 malloc=jemalloc-5.3.0 bits=64 build=75a9b36b1f0aa00`, sha256 `b0cd304980112fa6cd8dced524fdd6b136fd8efb802f5cf9e81610051a11891c`
- candidate commit: `d66f0f93`
- date: 2026-11-14; FULL=0, SCALE=1
- run time: 05-stream-memory 109s, 06-stream-consumers 26s, 07-save-ping 10s, 08-ft-info 59s, 09-vector-sets 179s, 10-redisvl 2s, 11-migrate-conns 3s, 12-search-fanout 4s, 13-cluster-reshard 382s

## 05-stream-memory

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| one1m | one_field_bytes_per_entry | B | 158.4 | 6.23 | 13.10 | 6.23 | rel: <= 7.23 (5% over 6.23, +1)<br>ratio: gain over v1.2.2 25.43x >= 15.26x (60% of recorded 25.43x) | pass<br>pass |
| one1m | one_field_load_s | s | 8.11 | 7.84 | 8.29 | 8.19 | info | info |
| one1m | one_field_rss_mib | MiB | 195.6 | 20.88 | 25.36 | 20.71 | rel: <= 25.71 (5% over 20.71, +5)<br>ratio: gain over v1.2.2 9.37x >= 5.67x (60% of recorded 9.45x) | pass<br>pass |
| one1m | one_field_used_mib | MiB | 152.4 | 7.33 | 13.29 | 7.33 | info | info |
| shapes | shared5_bytes_per_entry | B | 478.4 | 14.65 | 26.15 | 14.65 | rel: <= 15.65 (5% over 14.65, +1)<br>ratio: gain over v1.2.2 32.65x >= 19.59x (60% of recorded 32.65x) | pass<br>pass |
| shapes | shared5_load_s | s | 2.76 | 2.66 | 2.69 | 2.66 | info | info |
| shapes | unique5_bytes_per_entry | B | 478.4 | 54.01 | 75.42 | 54.01 | rel: <= 56.71 (5% over 54.01, +1)<br>ratio: gain over v1.2.2 8.86x >= 5.32x (60% of recorded 8.86x) | pass<br>pass |
| shapes | unique5_load_s | s | 2.85 | 2.76 | 2.86 | 2.76 | info | info |
| shapes | val200_bytes_per_entry | B | 350.9 | 216.5 | 216.3 | 216.5 | rel: <= 227.3 (5% over 216.5, +1)<br>ratio: gain over v1.2.2 1.62x >= 0.97x (60% of recorded 1.62x) | pass<br>pass |
| shapes | val200_load_s | s | 1.48 | 1.49 | 1.54 | 1.53 | info | info |
| shapes | xadd5_rps | req/s | 342,936 | 578,704 | 432,152 | 535,332 | rel: >= 455,032 (15% under 535,332) | pass |
| shapes | xrange5_count10_rps | req/s | 92,053 | 82,599 | 131,291 | 86,381 | rel: >= 73,424 (15% under 86,381) | pass |
| shapes | xrange_one_id_rps | req/s | 469,484 | 427,716 | 461,681 | 443,262 | rel: >= 376,773 (15% under 443,262) | pass |

## 06-stream-consumers

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| pel | pending_bytes_per_entry | B | 115.9 | 122.3 | 166.6 | 122.3 | rel: <= 128.4 (5% over 122.3, +2) | pass |
| pel | xadd_other_stream_max_ms | ms | 108.4 | 0.407 | 0.343 | 0.471 | info | info |
| pel | xadd_other_stream_rps | req/s | 102,354 | 121,654 | 145,879 | 104,384 | rel: >= 78,288 (25% under 104,384) | pass |
| pel | xadd_polled_stream_max_ms | ms | 71.23 | 0.351 | 0.519 | 0.823 | info | info |
| pel | xadd_polled_stream_rps | req/s | 44,504 | 102,512 | 143,472 | 117,371 | rel: >= 88,028 (25% under 117,371)<br>ratio: gain over v1.2.2 2.30x >= 1.65x (60% of recorded 2.74x) | pass<br>pass |
| pel | xinfo_consumers_ms | ms | 65.01 | 0.372 | 0.366 | 0.395 | rel: <= 0.895 (50% over 0.395, +0.5)<br>ratio: gain over v1.2.2 174.95x >= 98.93x (60% of recorded 164.88x) | pass<br>pass |
| pel | xinfo_polls | calls | 15.00 | 37.00 | 31.00 | 42.00 | info | info |
| pel | xpending_c50_ms | ms | 1.4 | 0.088 | 0.0857 | 0.11 | rel: <= 0.61 (50% over 0.11, +0.5)<br>ratio: gain over v1.2.2 15.93x >= 8.76x (60% of recorded 14.60x) | pass<br>pass |
| pel | xpending_summary_ms | ms | 5.93 | 0.171 | 0.166 | 0.193 | rel: <= 0.693 (50% over 0.193, +0.5)<br>ratio: gain over v1.2.2 34.74x >= 23.61x (60% of recorded 39.34x) | pass<br>pass |
| pel | xreadgroup_c50_history_ms | ms | 1.42 | 0.106 | 0.0991 | 0.133 | rel: <= 0.633 (50% over 0.133, +0.5)<br>ratio: gain over v1.2.2 13.39x >= 7.21x (60% of recorded 12.02x) | pass<br>pass |

## 07-save-ping

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| tokio1 | ping_during_save_max_ms | ms | 334.0 | 2.01 | 634.7 | 2.19 | bound: <= 20.00 | pass |
| tokio1 | ping_during_save_ms | ms | 292.1 | 2 | 551.5 | 1.48 | bound: <= 20.00 | pass |
| tokio1 | save_ms | ms | 294.9 | 341.0 | 552.2 | 339.4 | info | info |

## 08-ft-info

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| ftinfo | ft_info_polls_per_s | calls/s | 18.59 | 15,034 | - | 15,600 | info | info |
| ftinfo | ft_info_text_ms | ms | 115.1 | 0.0958 | - | 0.12 | rel: <= 0.62 (50% over 0.12, +0.5)<br>ratio: gain over v1.2.2 1201.82x >= 580.04x (60% of recorded 966.74x) | pass<br>pass |
| ftinfo | ft_info_vec_ms | ms | 115.5 | 0.0968 | - | 0.125 | rel: <= 0.625 (50% over 0.125, +0.5)<br>ratio: gain over v1.2.2 1193.60x >= 561.09x (60% of recorded 935.15x) | pass<br>pass |
| ftinfo | hset_vec_doc_rps | req/s | 108,108 | 120,192 | - | 107,797 | rel: >= 91,628 (15% under 107,797) | pass |
| ftinfo | populate_s | s | 1.93 | 1.89 | - | 2.09 | info | info |
| ftinfo | set_rps_beside_ft_info | req/s | 32.90 | 53,685 | - | 47,430 | rel: >= 37,944 (20% under 47,430) | pass |
| ftinfo | vec_doc_bytes | B | 474.5 | 494.1 | - | 494.1 | rel: <= 518.9 (5% over 494.1, +4) | pass |

## 09-vector-sets

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| bin_768_50k | bytes_per_vector | B | 3,168 | 191.7 | 853.7 | 191.7 | rel: <= 201.3 (5% over 191.7)<br>ratio: gain over v1.2.2 16.52x >= 9.91x (60% of recorded 16.52x) | pass<br>pass |
| bin_768_50k | recall_at_10 | ratio | 1 | 0.103 | 0.104 | 0.103 | rel: >= 0.101 (2% under 0.103) | pass |
| bin_768_50k | vadd_per_s | VADD/s | 76,004 | 62,940 | 6,203 | 61,976 | info | info |
| bin_768_50k | vsim_qps | q/s | 116.9 | 731.8 | 7,980 | 750.2 | info | info |
| f32_768_10k | recall_at_10 | ratio | 1 | 1 | 1 | 1 | rel: >= 0.98 (2% under 1) | pass |
| f32_768_10k | vadd_per_s | VADD/s | 78,804 | 62,601 | 2,340 | 62,038 | rel: >= 46,528 (25% under 62,038) | pass |
| f32_768_10k | vsim_p50_ms | ms | 1.83 | 1.96 | 0.356 | 1.91 | info | info |
| f32_768_10k | vsim_qps | q/s | 1,118 | 1,112 | 6,588 | 1,116 | rel: >= 836.6 (25% under 1,116) | pass |
| q8_768_10k | recall_at_10 | ratio | 1 | 0.972 | 0.972 | 0.972 | rel: >= 0.953 (2% under 0.972) | pass |
| q8_768_10k | vadd_per_s | VADD/s | 66,309 | 54,872 | 3,942 | 52,817 | rel: >= 39,613 (25% under 52,817) | pass |
| q8_768_10k | vsim_p50_ms | ms | 1.9 | 2.03 | 0.283 | 2.07 | info | info |
| q8_768_10k | vsim_qps | q/s | 1,151 | 981.8 | 7,856 | 986.1 | rel: >= 739.6 (25% under 986.1) | pass |
| q8_768_50k | bytes_per_vector | B | 3,168 | 863.7 | 1,526 | 863.7 | rel: <= 906.9 (5% over 863.7)<br>ratio: gain over v1.2.2 3.67x >= 2.20x (60% of recorded 3.67x) | pass<br>pass |
| q8_768_50k | recall_at_10 | ratio | 1 | 0.965 | 0.963 | 0.965 | rel: >= 0.946 (2% under 0.965) | pass |
| q8_768_50k | vadd_per_s | VADD/s | 96,900 | 64,882 | 3,961 | 64,379 | info | info |
| q8_768_50k | vsim_qps | q/s | 116.1 | 139.2 | 6,849 | 118.1 | info | info |

## 10-redisvl

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| redisvl | checks_failed | checks | 8 | 0 | - | 0 | bound: <= 0 | pass |
| redisvl | checks_passed | checks | 59.00 | 67.00 | - | 67.00 | bound: == 67.00 | pass |
| redisvl | lua_commands_sent | commands | 0 | 0 | - | 0 | info | info |

## 11-migrate-conns

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| migrate | keys_left_on_source | keys | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| migrate | keys_on_target | keys | 2,000 | 2,000 | 2,000 | 2,000 | bound: == 2,000 | pass |
| migrate | migrate_ms | ms | 0.158 | 0.108 | 0.092 | 0.105 | info | info |
| migrate | target_connections | connections | 2,000 | 1 | 1 | 1 | bound: <= 3 | pass |

## 12-search-fanout

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| cluster3 | peer_connections_max | connections | 2,000 | 1 | - | 1 | bound: <= 20.00 | pass |
| cluster3 | peer_kept_clients_max | clients | 0 | 2 | - | 2 | info | info |
| cluster3 | searches_per_s | searches/s | 4,554 | 5,690 | - | 5,772 | rel: >= 4,329 (25% under 5,772) | pass |
| cluster3 | searches_wrong_count | searches | 0 | 0 | - | 0 | bound: == 0 | pass |

## 13-cluster-reshard

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| reshard1000 | cluster_check_failed | - | 0 | 0 | 0 | 0 | bound: <= 0 | pass |
| reshard1000 | keys_after | keys | 3,000,000 | 3,000,000 | 3,000,000 | 3,000,000 | bound: == 3,000,000 | pass |
| reshard1000 | keys_moved | keys | 183,078 | 183,078 | 183,078 | 183,078 | bound: == 183,078 | pass |
| reshard1000 | load_s | s | 23.54 | 24.31 | 24.36 | 24.21 | info | info |
| reshard1000 | reshard_s | s | 267.5 | 27.02 | 6.82 | 25.30 | rel: <= 29.10 (15% over 25.30)<br>ratio: gain over v1.2.2 9.90x >= 9.05x (60% of recorded 15.09x) | pass<br>pass |

## Not run this time

`01-scan-match/default/dbsize`, `01-scan-match/default/hscan_match_c1000_calls`, `01-scan-match/default/hscan_match_c1000_distinct`, `01-scan-match/default/hscan_match_c1000_ms`, `01-scan-match/default/keys_schedule_ms`, `01-scan-match/default/keys_schedule_returned`, `01-scan-match/default/scan_all_c1000_calls`, `01-scan-match/default/scan_all_c1000_distinct`, `01-scan-match/default/scan_all_c1000_ms`, `01-scan-match/default/scan_infix_c1000_calls`, `01-scan-match/default/scan_infix_c1000_distinct`, `01-scan-match/default/scan_infix_c1000_ms`, `01-scan-match/default/scan_match_c10000_calls`, `01-scan-match/default/scan_match_c10000_distinct`, `01-scan-match/default/scan_match_c10000_ms`, `01-scan-match/default/scan_match_c1000_calls`, `01-scan-match/default/scan_match_c1000_distinct`, `01-scan-match/default/scan_match_c1000_ms`, `01-scan-match/default/sscan_match_c1000_calls`, `01-scan-match/default/sscan_match_c1000_distinct`, `01-scan-match/default/sscan_match_c1000_ms`, `01-scan-match/default/zscan_match_c1000_calls`, `01-scan-match/default/zscan_match_c1000_distinct`, `01-scan-match/default/zscan_match_c1000_ms`, `02-growth-ping/ex17m/dbsize`, `02-growth-ping/ex17m/load_s`, `02-growth-ping/ex17m/ping_max_ms`, `02-growth-ping/ex17m/ping_p99_ms`, `02-growth-ping/ex17m/pings`, `02-growth-ping/ex17m/pings_over_10ms`, `02-growth-ping/ex17m/rss_loaded_mib`, `02-growth-ping/ex17m/used_memory_mib`, `02-growth-ping/tokio/dbsize`, `02-growth-ping/tokio/load_s`, `02-growth-ping/tokio/ping_max_ms`, `02-growth-ping/tokio/ping_p99_ms`, `02-growth-ping/tokio/pings`, `02-growth-ping/tokio/pings_over_10ms`, `02-growth-ping/tokio/rss_loaded_mib`, `02-growth-ping/tokio/used_memory_mib`, `02-growth-ping/uring1/dbsize`, `02-growth-ping/uring1/load_s`, `02-growth-ping/uring1/ping_max_ms`, `02-growth-ping/uring1/ping_p99_ms`, `02-growth-ping/uring1/pings`, `02-growth-ping/uring1/pings_over_10ms`, `02-growth-ping/uring1/rss_loaded_mib`, `02-growth-ping/uring1/used_memory_mib`, `03-give-back/churn/delete_ms`, `03-give-back/churn/rss_after_15s_mib`, `03-give-back/churn/rss_after_1_5s_mib`, `03-give-back/churn/rss_after_5s_mib`, `03-give-back/churn/rss_loaded_mib`, `03-give-back/churn/rss_start_mib`, `03-give-back/churn/used_after_mib`, `03-give-back/churn/used_loaded_mib`, `03-give-back/hashdel/delete_ms`, `03-give-back/hashdel/rss_after_15s_mib`, `03-give-back/hashdel/rss_after_1_5s_mib`, `03-give-back/hashdel/rss_after_5s_mib`, `03-give-back/hashdel/rss_loaded_mib`, `03-give-back/hashdel/rss_start_mib`, `03-give-back/hashdel/used_after_mib`, `03-give-back/hashdel/used_loaded_mib`, `03-give-back/hdel/delete_ms`, `03-give-back/hdel/rss_after_15s_mib`, `03-give-back/hdel/rss_after_1_5s_mib`, `03-give-back/hdel/rss_after_5s_mib`, `03-give-back/hdel/rss_loaded_mib`, `03-give-back/hdel/rss_start_mib`, `03-give-back/hdel/used_after_mib`, `03-give-back/hdel/used_loaded_mib`, `04-long-commands/tokio/dbsize`, `04-long-commands/tokio/del_cmd_ms`, `04-long-commands/tokio/del_probes_stalled`, `04-long-commands/tokio/del_reply`, `04-long-commands/tokio/del_worst_ping_max_ms`, `04-long-commands/tokio/del_worst_ping_ms`, `04-long-commands/tokio/keys_cmd_ms`, `04-long-commands/tokio/keys_probes_stalled`, `04-long-commands/tokio/keys_reply`, `04-long-commands/tokio/keys_worst_ping_max_ms`, `04-long-commands/tokio/keys_worst_ping_ms`, `04-long-commands/tokio/scan_cmd_ms`, `04-long-commands/tokio/scan_probes_stalled`, `04-long-commands/tokio/scan_reply`, `04-long-commands/tokio/scan_worst_ping_max_ms`, `04-long-commands/tokio/scan_worst_ping_ms`, `04-long-commands/uring/dbsize`, `04-long-commands/uring/del_cmd_ms`, `04-long-commands/uring/del_least_worst_ping_ms`, `04-long-commands/uring/del_probes_stalled`, `04-long-commands/uring/del_reply`, `04-long-commands/uring/del_worst_ping_ms`, `04-long-commands/uring/keys_cmd_ms`, `04-long-commands/uring/keys_least_worst_ping_ms`, `04-long-commands/uring/keys_probes_stalled`, `04-long-commands/uring/keys_reply`, `04-long-commands/uring/keys_worst_ping_ms`, `04-long-commands/uring/scan_cmd_ms`, `04-long-commands/uring/scan_least_worst_ping_ms`, `04-long-commands/uring/scan_probes_stalled`, `04-long-commands/uring/scan_reply`, `04-long-commands/uring/scan_worst_ping_ms`

