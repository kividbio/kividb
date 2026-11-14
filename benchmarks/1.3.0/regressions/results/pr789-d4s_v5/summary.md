# Regression suite: pr789-d4s_v5

Verdict: **ok**

- Machine: Standard_D4s_v5, 4 vCPU, Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz, kernel 7.0.0-1017-azure, glibc 2.39
- Client: Python 3.12.3, redis-py 5.2.1
- v1_2_2: `kividb 1.2.2`, sha256 `38fc2cea40c131e041f4914527a98719e5d34dffdfe2abdc1f07ecb86aa6723e`
- candidate: `kividb 1.2.1 (default build: no Lua, no TLS)`, sha256 `adc7070128be83d283007745b9dd3f5110eca436017a7de9234af5e1db72df3c`
- redis: `Redis server v=8.10.2 sha=00000000:1 malloc=jemalloc-5.3.0 bits=64 build=75a9b36b1f0aa00`, sha256 `b0cd304980112fa6cd8dced524fdd6b136fd8efb802f5cf9e81610051a11891c`
- candidate commit: `b41ec723`
- date: 2026-11-14; FULL=0, SCALE=1
- run time: 01-scan-match 123s, 04-long-commands 197s

## 01-scan-match

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| default | dbsize | keys | 2,002,003 | 2,002,003 | 2,002,003 | 2,002,003 | bound: == 2,002,003 | pass |
| default | hscan_match_c1000_calls | calls | 201.0 | 201.0 | 201.0 | 201.0 | info | info |
| default | hscan_match_c1000_distinct | keys | 200.0 | 200.0 | 200.0 | 200.0 | bound: == 200.0 | pass |
| default | hscan_match_c1000_ms | ms | 416.0 | 378.1 | 25.69 | 428.0 | rel: <= 577.7 (35% over 428.0) | pass |
| default | keys_schedule_ms | ms | 113.6 | 53.73 | 125.8 | 60.44 | rel: <= 66.48 (10% over 60.44) | pass |
| default | keys_schedule_returned | keys | 2,000 | 2,000 | 2,000 | 2,000 | bound: == 2,000 | pass |
| default | scan_all_c1000_calls | calls | 513.0 | 512.0 | 2,002 | 512.0 | info | info |
| default | scan_all_c1000_distinct | keys | 2,002,003 | 2,002,003 | 2,002,003 | 2,002,003 | bound: == 2,002,003 | pass |
| default | scan_all_c1000_ms | ms | 3,045 | 2,045 | 2,066 | 2,180 | rel: <= 2,398 (10% over 2,180)<br>ratio: gain over v1.2.2 1.49x >= 1.00x (60% of recorded 1.66x)<br>vs_redis: 0.99x Redis <= 1.3x | pass<br>pass<br>pass |
| default | scan_infix_c1000_calls | calls | 513.0 | 512.0 | 2,002 | 512.0 | info | info |
| default | scan_infix_c1000_distinct | keys | 1,112,222 | 1,112,222 | 1,112,222 | 1,112,222 | bound: == 1,112,222 | pass |
| default | scan_infix_c1000_ms | ms | 2,365 | 1,391 | 1,702 | 1,494 | rel: <= 1,644 (10% over 1,494)<br>ratio: gain over v1.2.2 1.70x >= 1.08x (60% of recorded 1.80x)<br>vs_redis: 0.82x Redis <= 1.3x | pass<br>pass<br>pass |
| default | scan_match_c10000_calls | calls | 193.0 | 193.0 | 201.0 | 193.0 | info | info |
| default | scan_match_c10000_distinct | keys | 2,000 | 2,000 | 2,000 | 2,000 | bound: == 2,000 | pass |
| default | scan_match_c10000_ms | ms | 705.7 | 204.2 | 385.6 | 238.9 | rel: <= 262.8 (10% over 238.9)<br>ratio: gain over v1.2.2 3.46x >= 2.07x (60% of recorded 3.45x)<br>vs_redis: 0.53x Redis <= 1.3x | pass<br>pass<br>pass |
| default | scan_match_c1000_calls | calls | 513.0 | 512.0 | 2,002 | 512.0 | info | info |
| default | scan_match_c1000_distinct | keys | 2,000 | 2,000 | 2,000 | 2,000 | bound: == 2,000 | pass |
| default | scan_match_c1000_ms | ms | 1,293 | 364.6 | 528.1 | 406.2 | rel: <= 446.8 (10% over 406.2)<br>ratio: gain over v1.2.2 3.55x >= 2.47x (60% of recorded 4.12x)<br>vs_redis: 0.69x Redis <= 1.3x | pass<br>pass<br>pass |
| default | sscan_match_c1000_calls | calls | 201.0 | 201.0 | 201.0 | 201.0 | info | info |
| default | sscan_match_c1000_distinct | keys | 200.0 | 200.0 | 200.0 | 200.0 | bound: == 200.0 | pass |
| default | sscan_match_c1000_ms | ms | 694.4 | 682.3 | 30.60 | 1,093 | rel: <= 1,476 (35% over 1,093) | pass |
| default | zscan_match_c1000_calls | calls | 201.0 | 201.0 | 201.0 | 201.0 | info | info |
| default | zscan_match_c1000_distinct | keys | 200.0 | 200.0 | 200.0 | 200.0 | bound: == 200.0 | pass |
| default | zscan_match_c1000_ms | ms | 640.8 | 625.9 | 39.86 | 829.5 | rel: <= 1,120 (35% over 829.5) | pass |

## 04-long-commands

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| tokio | dbsize | keys | 2,000,000 | 2,000,000 | 2,000,000 | 2,000,000 | bound: == 2,000,000 | pass |
| tokio | del_cmd_ms | ms | 131.0 | 136.4 | 328.1 | 143.3 | rel: <= 157.7 (10% over 143.3) | pass |
| tokio | del_probes_stalled | probes | 4 | 0 | 4 | 0 | info | info |
| tokio | del_reply | items | 1 | 1 | 1 | 1 | bound: == 1 | pass |
| tokio | del_worst_ping_max_ms | ms | 133.1 | 0.121 | 328.2 | 1.22 | bound: <= 20.00 | pass |
| tokio | del_worst_ping_ms | ms | 130.3 | 0.111 | 327.4 | 0.942 | bound: <= 20.00 | pass |
| tokio | keys_cmd_ms | ms | 136.9 | 90.16 | 211.7 | 161.3 | rel: <= 177.5 (10% over 161.3)<br>vs_v1_2_2: 0.66x v1.2.2 <= 0.85x | pass<br>pass |
| tokio | keys_probes_stalled | probes | 4 | 0 | 4 | 0 | info | info |
| tokio | keys_reply | items | 11,111 | 11,111 | 11,111 | 11,111 | bound: == 11,111 | pass |
| tokio | keys_worst_ping_max_ms | ms | 126.7 | 1.19 | 200.5 | 8.09 | bound: <= 20.00 | pass |
| tokio | keys_worst_ping_ms | ms | 124.7 | 0.426 | 199.9 | 5.94 | bound: <= 20.00 | pass |
| tokio | scan_cmd_ms | ms | 246.2 | 101.9 | 462.1 | 112.5 | rel: <= 123.8 (10% over 112.5) | pass |
| tokio | scan_probes_stalled | probes | 4 | 0 | 4 | 0 | info | info |
| tokio | scan_reply | items | 11,111 | 11,111 | 11,111 | 11,111 | info | info |
| tokio | scan_worst_ping_max_ms | ms | 237.3 | 0.603 | 456.2 | 5.19 | bound: <= 20.00 | pass |
| tokio | scan_worst_ping_ms | ms | 234.5 | 0.103 | 452.3 | 0.449 | bound: <= 20.00 | pass |
| uring | dbsize | keys | 2,000,000 | 2,000,000 | - | 2,000,000 | bound: == 2,000,000 | pass |
| uring | del_cmd_ms | ms | 130.9 | 135.8 | - | 134.3 | rel: <= 147.7 (10% over 134.3) | pass |
| uring | del_least_worst_ping_ms | ms | 0.06 | 0.07 | - | 0.41 | rel: <= 10.41 (50% over 0.41, +10) | pass |
| uring | del_probes_stalled | probes | 2 | 2 | - | 1 | info | info |
| uring | del_reply | items | 1 | 1 | - | 1 | bound: == 1 | pass |
| uring | del_worst_ping_ms | ms | 129.5 | 134.7 | - | 132.9 | info | info |
| uring | keys_cmd_ms | ms | 141.8 | 87.00 | - | 153.6 | rel: <= 169.0 (10% over 153.6)<br>vs_v1_2_2: 0.61x v1.2.2 <= 0.85x | pass<br>pass |
| uring | keys_least_worst_ping_ms | ms | 0.07 | 0.98 | - | 0.95 | rel: <= 10.95 (50% over 0.95, +10) | pass |
| uring | keys_probes_stalled | probes | 2 | 2 | - | 2 | info | info |
| uring | keys_reply | items | 11,111 | 11,111 | - | 11,111 | bound: == 11,111 | pass |
| uring | keys_worst_ping_ms | ms | 130.3 | 75.87 | - | 142.8 | info | info |
| uring | scan_cmd_ms | ms | 261.1 | 98.70 | - | 112.6 | rel: <= 123.9 (10% over 112.6) | pass |
| uring | scan_least_worst_ping_ms | ms | 0.07 | 0.07 | - | 0.13 | rel: <= 10.13 (50% over 0.13, +10) | pass |
| uring | scan_probes_stalled | probes | 1 | 2 | - | 1 | info | info |
| uring | scan_reply | items | 11,111 | 11,111 | - | 11,111 | info | info |
| uring | scan_worst_ping_ms | ms | 250.4 | 87.92 | - | 101.6 | info | info |

## Not run this time

`02-growth-ping/ex17m/dbsize`, `02-growth-ping/ex17m/load_s`, `02-growth-ping/ex17m/ping_max_ms`, `02-growth-ping/ex17m/ping_p99_ms`, `02-growth-ping/ex17m/pings`, `02-growth-ping/ex17m/pings_over_10ms`, `02-growth-ping/ex17m/rss_loaded_mib`, `02-growth-ping/ex17m/used_memory_mib`, `02-growth-ping/tokio/dbsize`, `02-growth-ping/tokio/load_s`, `02-growth-ping/tokio/ping_max_ms`, `02-growth-ping/tokio/ping_p99_ms`, `02-growth-ping/tokio/pings`, `02-growth-ping/tokio/pings_over_10ms`, `02-growth-ping/tokio/rss_loaded_mib`, `02-growth-ping/tokio/used_memory_mib`, `02-growth-ping/uring1/dbsize`, `02-growth-ping/uring1/load_s`, `02-growth-ping/uring1/ping_max_ms`, `02-growth-ping/uring1/ping_p99_ms`, `02-growth-ping/uring1/pings`, `02-growth-ping/uring1/pings_over_10ms`, `02-growth-ping/uring1/rss_loaded_mib`, `02-growth-ping/uring1/used_memory_mib`, `03-give-back/churn/delete_ms`, `03-give-back/churn/rss_after_15s_mib`, `03-give-back/churn/rss_after_1_5s_mib`, `03-give-back/churn/rss_after_5s_mib`, `03-give-back/churn/rss_loaded_mib`, `03-give-back/churn/rss_start_mib`, `03-give-back/churn/used_after_mib`, `03-give-back/churn/used_loaded_mib`, `03-give-back/hashdel/delete_ms`, `03-give-back/hashdel/rss_after_15s_mib`, `03-give-back/hashdel/rss_after_1_5s_mib`, `03-give-back/hashdel/rss_after_5s_mib`, `03-give-back/hashdel/rss_loaded_mib`, `03-give-back/hashdel/rss_start_mib`, `03-give-back/hashdel/used_after_mib`, `03-give-back/hashdel/used_loaded_mib`, `03-give-back/hdel/delete_ms`, `03-give-back/hdel/rss_after_15s_mib`, `03-give-back/hdel/rss_after_1_5s_mib`, `03-give-back/hdel/rss_after_5s_mib`, `03-give-back/hdel/rss_loaded_mib`, `03-give-back/hdel/rss_start_mib`, `03-give-back/hdel/used_after_mib`, `03-give-back/hdel/used_loaded_mib`, `05-stream-memory/one1m/one_field_bytes_per_entry`, `05-stream-memory/one1m/one_field_load_s`, `05-stream-memory/one1m/one_field_rss_mib`, `05-stream-memory/one1m/one_field_used_mib`, `05-stream-memory/shapes/shared5_bytes_per_entry`, `05-stream-memory/shapes/shared5_load_s`, `05-stream-memory/shapes/unique5_bytes_per_entry`, `05-stream-memory/shapes/unique5_load_s`, `05-stream-memory/shapes/val200_bytes_per_entry`, `05-stream-memory/shapes/val200_load_s`, `05-stream-memory/shapes/xadd5_rps`, `05-stream-memory/shapes/xrange5_count10_rps`, `05-stream-memory/shapes/xrange_one_id_rps`, `06-stream-consumers/pel/pending_bytes_per_entry`, `06-stream-consumers/pel/xadd_other_stream_max_ms`, `06-stream-consumers/pel/xadd_other_stream_rps`, `06-stream-consumers/pel/xadd_polled_stream_max_ms`, `06-stream-consumers/pel/xadd_polled_stream_rps`, `06-stream-consumers/pel/xinfo_consumers_ms`, `06-stream-consumers/pel/xinfo_polls`, `06-stream-consumers/pel/xpending_c50_ms`, `06-stream-consumers/pel/xpending_summary_ms`, `06-stream-consumers/pel/xreadgroup_c50_history_ms`, `07-save-ping/tokio1/ping_during_save_max_ms`, `07-save-ping/tokio1/ping_during_save_ms`, `07-save-ping/tokio1/save_ms`, `08-ft-info/ftinfo/ft_info_polls_per_s`, `08-ft-info/ftinfo/ft_info_text_ms`, `08-ft-info/ftinfo/ft_info_vec_ms`, `08-ft-info/ftinfo/hset_vec_doc_rps`, `08-ft-info/ftinfo/populate_s`, `08-ft-info/ftinfo/set_rps_beside_ft_info`, `08-ft-info/ftinfo/vec_doc_bytes`, `09-vector-sets/bin_768_50k/bytes_per_vector`, `09-vector-sets/bin_768_50k/recall_at_10`, `09-vector-sets/bin_768_50k/vadd_per_s`, `09-vector-sets/bin_768_50k/vsim_qps`, `09-vector-sets/f32_768_10k/recall_at_10`, `09-vector-sets/f32_768_10k/vadd_per_s`, `09-vector-sets/f32_768_10k/vsim_p50_ms`, `09-vector-sets/f32_768_10k/vsim_qps`, `09-vector-sets/q8_768_10k/recall_at_10`, `09-vector-sets/q8_768_10k/vadd_per_s`, `09-vector-sets/q8_768_10k/vsim_p50_ms`, `09-vector-sets/q8_768_10k/vsim_qps`, `09-vector-sets/q8_768_50k/bytes_per_vector`, `09-vector-sets/q8_768_50k/recall_at_10`, `09-vector-sets/q8_768_50k/vadd_per_s`, `09-vector-sets/q8_768_50k/vsim_qps`, `10-redisvl/redisvl/checks_failed`, `10-redisvl/redisvl/checks_passed`, `10-redisvl/redisvl/lua_commands_sent`, `11-migrate-conns/migrate/keys_left_on_source`, `11-migrate-conns/migrate/keys_on_target`, `11-migrate-conns/migrate/migrate_ms`, `11-migrate-conns/migrate/target_connections`, `12-search-fanout/cluster3/peer_connections_max`, `12-search-fanout/cluster3/peer_kept_clients_max`, `12-search-fanout/cluster3/searches_per_s`, `12-search-fanout/cluster3/searches_wrong_count`, `13-cluster-reshard/reshard1000/cluster_check_failed`, `13-cluster-reshard/reshard1000/keys_after`, `13-cluster-reshard/reshard1000/keys_moved`, `13-cluster-reshard/reshard1000/load_s`, `13-cluster-reshard/reshard1000/reshard_s`

