# Regression suite: master-d66f0f93-d4s_v5-part2-rerun1

Verdict: **REGRESSED** (1 failed)

- Machine: Standard_D4s_v5, 4 vCPU, Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz, kernel 7.0.0-1017-azure, glibc 2.39
- Client: Python 3.12.3, redis-py 5.2.1
- v1_2_2: `kividb 1.2.2`, sha256 `38fc2cea40c131e041f4914527a98719e5d34dffdfe2abdc1f07ecb86aa6723e`
- candidate: `kividb 1.2.1 (default build: no Lua, no TLS)`, sha256 `9771dc1ec052679fa29498cf39b3daedb3f5718d29bc1501732aa32c2df5f556`
- redis: `Redis server v=8.10.2 sha=00000000:1 malloc=jemalloc-5.3.0 bits=64 build=75a9b36b1f0aa00`, sha256 `b0cd304980112fa6cd8dced524fdd6b136fd8efb802f5cf9e81610051a11891c`
- candidate commit: `d66f0f93`
- date: 2026-11-14; FULL=0, SCALE=1
- run time: 05-stream-memory 108s, 06-stream-consumers 26s, 07-save-ping 12s, 08-ft-info 59s, 09-vector-sets 146s, 10-redisvl 1s, 11-migrate-conns 3s, 12-search-fanout 4s, 13-cluster-reshard 483s

## 05-stream-memory

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| one1m | one_field_bytes_per_entry | B | 158.4 | 6.23 | 13.10 | 6.23 | rel: <= 7.23 (5% over 6.23, +1)<br>ratio: gain over v1.2.2 25.43x >= 15.26x (60% of recorded 25.43x) | pass<br>pass |
| one1m | one_field_load_s | s | 8.01 | 7.86 | 7.92 | 8.19 | info | info |
| one1m | one_field_rss_mib | MiB | 195.8 | 20.91 | 25.44 | 20.71 | rel: <= 25.71 (5% over 20.71, +5)<br>ratio: gain over v1.2.2 9.36x >= 5.67x (60% of recorded 9.45x) | pass<br>pass |
| one1m | one_field_used_mib | MiB | 152.4 | 7.33 | 13.29 | 7.33 | info | info |
| shapes | shared5_bytes_per_entry | B | 478.4 | 14.65 | 26.16 | 14.65 | rel: <= 15.65 (5% over 14.65, +1)<br>ratio: gain over v1.2.2 32.65x >= 19.59x (60% of recorded 32.65x) | pass<br>pass |
| shapes | shared5_load_s | s | 2.72 | 2.67 | 2.67 | 2.66 | info | info |
| shapes | unique5_bytes_per_entry | B | 478.4 | 54.01 | 75.36 | 54.01 | rel: <= 56.71 (5% over 54.01, +1)<br>ratio: gain over v1.2.2 8.86x >= 5.32x (60% of recorded 8.86x) | pass<br>pass |
| shapes | unique5_load_s | s | 2.8 | 2.79 | 2.76 | 2.76 | info | info |
| shapes | val200_bytes_per_entry | B | 350.9 | 216.5 | 216.3 | 216.5 | rel: <= 227.3 (5% over 216.5, +1)<br>ratio: gain over v1.2.2 1.62x >= 0.97x (60% of recorded 1.62x) | pass<br>pass |
| shapes | val200_load_s | s | 1.48 | 1.56 | 1.49 | 1.53 | info | info |
| shapes | xadd5_rps | req/s | 336,022 | 538,213 | 528,541 | 535,332 | rel: >= 455,032 (15% under 535,332) | pass |
| shapes | xrange5_count10_rps | req/s | 96,494 | 84,579 | 129,983 | 86,381 | rel: >= 73,424 (15% under 86,381) | pass |
| shapes | xrange_one_id_rps | req/s | 440,917 | 461,681 | 465,983 | 443,262 | rel: >= 376,773 (15% under 443,262) | pass |

## 06-stream-consumers

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| pel | pending_bytes_per_entry | B | 116.5 | 122.3 | 166.6 | 122.3 | rel: <= 128.4 (5% over 122.3, +2) | pass |
| pel | xadd_other_stream_max_ms | ms | 114.8 | 0.383 | 0.551 | 0.471 | info | info |
| pel | xadd_other_stream_rps | req/s | 94,697 | 118,977 | 143,062 | 104,384 | rel: >= 88,727 (15% under 104,384) | pass |
| pel | xadd_polled_stream_max_ms | ms | 78.72 | 0.335 | 0.551 | 0.823 | info | info |
| pel | xadd_polled_stream_rps | req/s | 47,517 | 102,564 | 143,988 | 117,371 | rel: >= 99,765 (15% under 117,371)<br>ratio: gain over v1.2.2 2.16x >= 1.65x (60% of recorded 2.74x) | pass<br>pass |
| pel | xinfo_consumers_ms | ms | 65.83 | 0.402 | 0.364 | 0.395 | rel: <= 0.895 (50% over 0.395, +0.5)<br>ratio: gain over v1.2.2 163.59x >= 98.93x (60% of recorded 164.88x) | pass<br>pass |
| pel | xinfo_polls | calls | 18.00 | 37.00 | 32.00 | 42.00 | info | info |
| pel | xpending_c50_ms | ms | 1.42 | 0.109 | 0.0882 | 0.11 | rel: <= 0.61 (50% over 0.11, +0.5)<br>ratio: gain over v1.2.2 13.02x >= 8.76x (60% of recorded 14.60x) | pass<br>pass |
| pel | xpending_summary_ms | ms | 6.31 | 0.197 | 0.165 | 0.193 | rel: <= 0.693 (50% over 0.193, +0.5)<br>ratio: gain over v1.2.2 32.03x >= 23.61x (60% of recorded 39.34x) | pass<br>pass |
| pel | xreadgroup_c50_history_ms | ms | 1.41 | 0.13 | 0.101 | 0.133 | rel: <= 0.633 (50% over 0.133, +0.5)<br>ratio: gain over v1.2.2 10.81x >= 7.21x (60% of recorded 12.02x) | pass<br>pass |

## 07-save-ping

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| tokio1 | ping_during_save_max_ms | ms | 346.2 | 2.81 | 590.8 | 2.19 | bound: <= 20.00 | pass |
| tokio1 | ping_during_save_ms | ms | 336.2 | 1.14 | 587.8 | 1.48 | bound: <= 20.00 | pass |
| tokio1 | save_ms | ms | 338.1 | 503.0 | 589.0 | 339.4 | rel: <= 390.3 (15% over 339.4) | **FAIL** |

## 08-ft-info

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| ftinfo | ft_info_polls_per_s | calls/s | 18.39 | 15,791 | - | 15,600 | info | info |
| ftinfo | ft_info_text_ms | ms | 116.2 | 0.0956 | - | 0.12 | rel: <= 0.62 (50% over 0.12, +0.5)<br>ratio: gain over v1.2.2 1215.15x >= 580.04x (60% of recorded 966.74x) | pass<br>pass |
| ftinfo | ft_info_vec_ms | ms | 116.4 | 0.0981 | - | 0.125 | rel: <= 0.625 (50% over 0.125, +0.5)<br>ratio: gain over v1.2.2 1186.97x >= 561.09x (60% of recorded 935.15x) | pass<br>pass |
| ftinfo | hset_vec_doc_rps | req/s | 99,536 | 116,414 | - | 107,797 | rel: >= 91,628 (15% under 107,797) | pass |
| ftinfo | populate_s | s | 2.14 | 2.05 | - | 2.09 | info | info |
| ftinfo | set_rps_beside_ft_info | req/s | 32.70 | 48,014 | - | 47,430 | rel: >= 37,944 (20% under 47,430)<br>ratio: gain over v1.2.2 1468.31x >= 251.62x (60% of recorded 419.36x) | pass<br>pass |
| ftinfo | vec_doc_bytes | B | 474.5 | 494.1 | - | 494.1 | rel: <= 518.9 (5% over 494.1, +4) | pass |

## 09-vector-sets

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| bin_768_50k | bytes_per_vector | B | 3,168 | 191.7 | 854.4 | 191.7 | rel: <= 201.3 (5% over 191.7)<br>ratio: gain over v1.2.2 16.52x >= 9.91x (60% of recorded 16.52x) | pass<br>pass |
| bin_768_50k | recall_at_10 | ratio | 1 | 0.103 | 0.106 | 0.103 | rel: >= 0.101 (2% under 0.103) | pass |
| bin_768_50k | vadd_per_s | VADD/s | 75,907 | 61,494 | 5,756 | 64,808 | info | info |
| bin_768_50k | vsim_qps | q/s | 112.9 | 747.9 | 7,646 | 728.9 | info | info |
| q8_768_10k | recall_at_10 | ratio | 1 | 0.972 | 0.972 | 0.972 | rel: >= 0.953 (2% under 0.972) | pass |
| q8_768_10k | vadd_per_s | VADD/s | 66,528 | 50,718 | 3,809 | 58,078 | info | info |
| q8_768_10k | vsim_p50_ms | ms | 2.29 | 2.14 | 0.335 | 2.06 | info | info |
| q8_768_10k | vsim_qps | q/s | 866.0 | 954.2 | 7,532 | 974.5 | rel: >= 730.9 (25% under 974.5)<br>ratio: gain over v1.2.2 1.10x >= 0.54x (60% of recorded 0.90x) | pass<br>pass |
| q8_768_50k | bytes_per_vector | B | 3,168 | 863.7 | 1,526 | 863.7 | rel: <= 906.9 (5% over 863.7)<br>ratio: gain over v1.2.2 3.67x >= 2.20x (60% of recorded 3.67x) | pass<br>pass |
| q8_768_50k | recall_at_10 | ratio | 1 | 0.965 | 0.963 | 0.965 | rel: >= 0.946 (2% under 0.965) | pass |
| q8_768_50k | vadd_per_s | VADD/s | 76,292 | 63,258 | 3,647 | 61,626 | info | info |
| q8_768_50k | vsim_qps | q/s | 112.6 | 113.0 | 6,682 | 117.0 | info | info |

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
| migrate | migrate_ms | ms | 0.165 | 0.113 | 0.0892 | 0.105 | info | info |
| migrate | target_connections | connections | 2,000 | 1 | 1 | 1 | bound: <= 3 | pass |

## 12-search-fanout

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| cluster3 | peer_connections_max | connections | 2,000 | 1 | - | 1 | bound: <= 20.00 | pass |
| cluster3 | peer_kept_clients_max | clients | 0 | 2 | - | 2 | info | info |
| cluster3 | searches_per_s | searches/s | 4,084 | 5,616 | - | 5,772 | rel: >= 4,329 (25% under 5,772) | pass |
| cluster3 | searches_wrong_count | searches | 0 | 0 | - | 0 | bound: == 0 | pass |

## 13-cluster-reshard

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| reshard1000 | cluster_check_failed | - | 0 | 0 | 0 | 0 | bound: <= 0 | pass |
| reshard1000 | keys_after | keys | 3,000,000 | 3,000,000 | 3,000,000 | 3,000,000 | bound: == 3,000,000 | pass |
| reshard1000 | keys_moved | keys | 183,078 | 183,078 | 183,078 | 183,078 | bound: == 183,078 | pass |
| reshard1000 | load_s | s | 24.25 | 24.12 | 24.32 | 24.21 | info | info |
| reshard1000 | reshard_s | s | 367.4 | 26.06 | 7.68 | 25.30 | rel: <= 29.10 (15% over 25.30)<br>ratio: gain over v1.2.2 14.10x >= 9.05x (60% of recorded 15.09x) | pass<br>pass |

## Not run this time

`01-scan-match/default/dbsize`, `01-scan-match/default/hscan_match_c1000_calls`, `01-scan-match/default/hscan_match_c1000_distinct`, `01-scan-match/default/hscan_match_c1000_ms`, `01-scan-match/default/keys_schedule_ms`, `01-scan-match/default/keys_schedule_returned`, `01-scan-match/default/scan_all_c1000_calls`, `01-scan-match/default/scan_all_c1000_distinct`, `01-scan-match/default/scan_all_c1000_ms`, `01-scan-match/default/scan_infix_c1000_calls`, `01-scan-match/default/scan_infix_c1000_distinct`, `01-scan-match/default/scan_infix_c1000_ms`, `01-scan-match/default/scan_match_c10000_calls`, `01-scan-match/default/scan_match_c10000_distinct`, `01-scan-match/default/scan_match_c10000_ms`, `01-scan-match/default/scan_match_c1000_calls`, `01-scan-match/default/scan_match_c1000_distinct`, `01-scan-match/default/scan_match_c1000_ms`, `01-scan-match/default/sscan_match_c1000_calls`, `01-scan-match/default/sscan_match_c1000_distinct`, `01-scan-match/default/sscan_match_c1000_ms`, `01-scan-match/default/zscan_match_c1000_calls`, `01-scan-match/default/zscan_match_c1000_distinct`, `01-scan-match/default/zscan_match_c1000_ms`, `02-growth-ping/ex17m/dbsize`, `02-growth-ping/ex17m/load_s`, `02-growth-ping/ex17m/ping_max_ms`, `02-growth-ping/ex17m/ping_p99_ms`, `02-growth-ping/ex17m/pings`, `02-growth-ping/ex17m/pings_over_10ms`, `02-growth-ping/ex17m/rss_loaded_mib`, `02-growth-ping/ex17m/used_memory_mib`, `02-growth-ping/tokio/dbsize`, `02-growth-ping/tokio/load_s`, `02-growth-ping/tokio/ping_max_ms`, `02-growth-ping/tokio/ping_p99_ms`, `02-growth-ping/tokio/pings`, `02-growth-ping/tokio/pings_over_10ms`, `02-growth-ping/tokio/rss_loaded_mib`, `02-growth-ping/tokio/used_memory_mib`, `02-growth-ping/uring1/dbsize`, `02-growth-ping/uring1/load_s`, `02-growth-ping/uring1/ping_max_ms`, `02-growth-ping/uring1/ping_p99_ms`, `02-growth-ping/uring1/pings`, `02-growth-ping/uring1/pings_over_10ms`, `02-growth-ping/uring1/rss_loaded_mib`, `02-growth-ping/uring1/used_memory_mib`, `03-give-back/churn/delete_ms`, `03-give-back/churn/rss_after_15s_mib`, `03-give-back/churn/rss_after_1_5s_mib`, `03-give-back/churn/rss_after_5s_mib`, `03-give-back/churn/rss_loaded_mib`, `03-give-back/churn/rss_start_mib`, `03-give-back/churn/used_after_mib`, `03-give-back/churn/used_loaded_mib`, `03-give-back/hashdel/delete_ms`, `03-give-back/hashdel/rss_after_15s_mib`, `03-give-back/hashdel/rss_after_1_5s_mib`, `03-give-back/hashdel/rss_after_5s_mib`, `03-give-back/hashdel/rss_loaded_mib`, `03-give-back/hashdel/rss_start_mib`, `03-give-back/hashdel/used_after_mib`, `03-give-back/hashdel/used_loaded_mib`, `03-give-back/hdel/delete_ms`, `03-give-back/hdel/rss_after_15s_mib`, `03-give-back/hdel/rss_after_1_5s_mib`, `03-give-back/hdel/rss_after_5s_mib`, `03-give-back/hdel/rss_loaded_mib`, `03-give-back/hdel/rss_start_mib`, `03-give-back/hdel/used_after_mib`, `03-give-back/hdel/used_loaded_mib`, `04-long-commands/tokio/dbsize`, `04-long-commands/tokio/del_cmd_ms`, `04-long-commands/tokio/del_probes_stalled`, `04-long-commands/tokio/del_reply`, `04-long-commands/tokio/del_worst_ping_max_ms`, `04-long-commands/tokio/del_worst_ping_ms`, `04-long-commands/tokio/keys_cmd_ms`, `04-long-commands/tokio/keys_probes_stalled`, `04-long-commands/tokio/keys_reply`, `04-long-commands/tokio/keys_worst_ping_max_ms`, `04-long-commands/tokio/keys_worst_ping_ms`, `04-long-commands/tokio/scan_cmd_ms`, `04-long-commands/tokio/scan_probes_stalled`, `04-long-commands/tokio/scan_reply`, `04-long-commands/tokio/scan_worst_ping_max_ms`, `04-long-commands/tokio/scan_worst_ping_ms`, `04-long-commands/uring/dbsize`, `04-long-commands/uring/del_cmd_ms`, `04-long-commands/uring/del_least_worst_ping_ms`, `04-long-commands/uring/del_probes_stalled`, `04-long-commands/uring/del_reply`, `04-long-commands/uring/del_worst_ping_ms`, `04-long-commands/uring/keys_cmd_ms`, `04-long-commands/uring/keys_least_worst_ping_ms`, `04-long-commands/uring/keys_probes_stalled`, `04-long-commands/uring/keys_reply`, `04-long-commands/uring/keys_worst_ping_ms`, `04-long-commands/uring/scan_cmd_ms`, `04-long-commands/uring/scan_least_worst_ping_ms`, `04-long-commands/uring/scan_probes_stalled`, `04-long-commands/uring/scan_reply`, `04-long-commands/uring/scan_worst_ping_ms`

