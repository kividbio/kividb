# Regression suite: pr759-d4s_v5

Verdict: **ok**

NOTE: v1.2.2 has no usable value for the speed metrics of 22 gate(s): speed gates were compared absolutely against the recorded values (14-sinter-big-small/default/1m_sdiff_small_big_ms, 14-sinter-big-small/default/1m_sdiffstore_big_small_ms, 14-sinter-big-small/default/1m_sinter_big_small_ms, 14-sinter-big-small/default/1m_sinter_small_big_ms, 14-sinter-big-small/default/1m_sintercard_limit_ms, ...).

- Machine: Standard_D4s_v5, 4 vCPU, Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz, kernel 7.0.0-1017-azure, glibc 2.39
- calibration (indicative only): 93.1 ms for a fixed sha256 loop, 4 cores
- Client: Python 3.12.3, redis-py 5.2.1
- v1_2_2: `kividb 1.2.2`, sha256 `38fc2cea40c131e041f4914527a98719e5d34dffdfe2abdc1f07ecb86aa6723e`
- candidate: `kividb 1.2.1 (default build: no Lua, no TLS)`, sha256 `a8dbc8ad25e0dbbb41ec655f9f79e2a3f5452f2aa158524b72e2036ec95ff6a4`
- redis: `Redis server v=8.10.2 sha=00000000:1 malloc=jemalloc-5.3.0 bits=64 build=75a9b36b1f0aa00`, sha256 `b0cd304980112fa6cd8dced524fdd6b136fd8efb802f5cf9e81610051a11891c`
- candidate commit: `219b9656`
- date: 2026-11-14; FULL=1, SCALE=1
- run time: 14-sinter-big-small 393s

## 14-sinter-big-small

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| default | 1m_big_card | members | 1,000,000 | 1,000,000 | 1,000,000 | - | bound: eq: nothing recorded | new |
| default | 1m_sdiff_small_big_ms | ms | 69.93 | 1.95 | 2.57 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.76x Redis <= 2x | new<br>new<br>pass |
| default | 1m_sdiff_small_big_reply | members | 1,999 | 1,999 | 1,999 | - | bound: eq: nothing recorded | new |
| default | 1m_sdiffstore_big_small_ms | ms | 288.1 | 253.6 | 314.2 | - | rel: rel 25%: nothing recorded | new |
| default | 1m_sdiffstore_big_small_reply | members | 997,903 | 997,903 | 997,903 | - | bound: eq: nothing recorded | new |
| default | 1m_sinter_big_small_ms | ms | 95.75 | 2.33 | 2.56 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.91x Redis <= 2x | new<br>new<br>pass |
| default | 1m_sinter_big_small_reply | members | 2,097 | 2,097 | 2,097 | - | bound: eq: nothing recorded | new |
| default | 1m_sinter_small_big_ms | ms | 69.48 | 2.09 | 2.3 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.91x Redis <= 2x | new<br>new<br>pass |
| default | 1m_sinter_small_big_reply | members | 2,097 | 2,097 | 2,097 | - | bound: eq: nothing recorded | new |
| default | 1m_sintercard_limit_ms | ms | 93.18 | 0.0547 | 0.0629 | - | rel: rel 50%: nothing recorded | new |
| default | 1m_sintercard_limit_reply | members | 100.0 | 100.0 | 100.0 | - | bound: eq: nothing recorded | new |
| default | 1m_sintercard_ms | ms | 92.71 | 0.184 | 0.46 | - | rel: rel 50%: nothing recorded | new |
| default | 1m_sintercard_reply | members | 2,097 | 2,097 | 2,097 | - | bound: eq: nothing recorded | new |
| default | 1m_sinterstore_ms | ms | 93.66 | 0.3 | 0.854 | - | rel: rel 50%: nothing recorded | new |
| default | 1m_sinterstore_reply | members | 2,097 | 2,097 | 2,097 | - | bound: eq: nothing recorded | new |
| default | 5m_big_card | members | 5,000,000 | 5,000,000 | 5,000,000 | - | bound: eq: nothing recorded | new |
| default | 5m_sdiff_small_big_ms | ms | 447.2 | 2.03 | 3.94 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.52x Redis <= 2x | new<br>new<br>pass |
| default | 5m_sdiff_small_big_reply | members | 1,998 | 1,998 | 1,998 | - | bound: eq: nothing recorded | new |
| default | 5m_sdiffstore_big_small_ms | ms | 1,645 | 1,424 | 1,825 | - | rel: rel 25%: nothing recorded | new |
| default | 5m_sdiffstore_big_small_reply | members | 4,997,902 | 4,997,902 | 4,997,902 | - | bound: eq: nothing recorded | new |
| default | 5m_sinter_big_small_ms | ms | 535.3 | 2.22 | 3.87 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.57x Redis <= 2x | new<br>new<br>pass |
| default | 5m_sinter_big_small_reply | members | 2,098 | 2,098 | 2,098 | - | bound: eq: nothing recorded | new |
| default | 5m_sinter_small_big_ms | ms | 446.3 | 2.06 | 3.52 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.58x Redis <= 2x | new<br>new<br>pass |
| default | 5m_sinter_small_big_reply | members | 2,098 | 2,098 | 2,098 | - | bound: eq: nothing recorded | new |
| default | 5m_sintercard_limit_ms | ms | 533.4 | 0.0539 | 0.116 | - | rel: rel 50%: nothing recorded | new |
| default | 5m_sintercard_limit_reply | members | 100.0 | 100.0 | 100.0 | - | bound: eq: nothing recorded | new |
| default | 5m_sintercard_ms | ms | 533.3 | 0.223 | 1.56 | - | rel: rel 50%: nothing recorded | new |
| default | 5m_sintercard_reply | members | 2,098 | 2,098 | 2,098 | - | bound: eq: nothing recorded | new |
| default | 5m_sinterstore_ms | ms | 533.3 | 0.476 | 2.52 | - | rel: rel 50%: nothing recorded | new |
| default | 5m_sinterstore_reply | members | 2,098 | 2,098 | 2,098 | - | bound: eq: nothing recorded | new |
| n20m | 20m_big_card | members | 20,000,000 | 20,000,000 | 20,000,000 | - | bound: eq: nothing recorded | new |
| n20m | 20m_sdiff_small_big_ms | ms | 2,457 | 2.01 | 4.7 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.43x Redis <= 2x | new<br>new<br>pass |
| n20m | 20m_sdiff_small_big_reply | members | 1,999 | 1,999 | 1,999 | - | bound: eq: nothing recorded | new |
| n20m | 20m_sdiffstore_big_small_ms | ms | 7,993 | 6,899 | 8,067 | - | rel: rel 25%: nothing recorded | new |
| n20m | 20m_sdiffstore_big_small_reply | members | 19,997,903 | 19,997,903 | 19,997,903 | - | bound: eq: nothing recorded | new |
| n20m | 20m_sinter_big_small_ms | ms | 2,573 | 2.29 | 4.76 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.48x Redis <= 2x | new<br>new<br>pass |
| n20m | 20m_sinter_big_small_reply | members | 2,097 | 2,097 | 2,097 | - | bound: eq: nothing recorded | new |
| n20m | 20m_sinter_small_big_ms | ms | 2,431 | 2.1 | 3.82 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded<br>vs_redis: 0.55x Redis <= 2x | new<br>new<br>pass |
| n20m | 20m_sinter_small_big_reply | members | 2,097 | 2,097 | 2,097 | - | bound: eq: nothing recorded | new |
| n20m | 20m_sintercard_limit_ms | ms | 2,571 | 0.0569 | 0.134 | - | rel: rel 50%: nothing recorded | new |
| n20m | 20m_sintercard_limit_reply | members | 100.0 | 100.0 | 100.0 | - | bound: eq: nothing recorded | new |
| n20m | 20m_sintercard_ms | ms | 2,568 | 0.296 | 1.89 | - | rel: rel 50%: nothing recorded | new |
| n20m | 20m_sintercard_reply | members | 2,097 | 2,097 | 2,097 | - | bound: eq: nothing recorded | new |
| n20m | 20m_sinterstore_ms | ms | 2,573 | 0.381 | 2.84 | - | rel: rel 50%: nothing recorded | new |
| n20m | 20m_sinterstore_reply | members | 2,097 | 2,097 | 2,097 | - | bound: eq: nothing recorded | new |
| sunion | sunion_ms | ms | 1,330 | 1,331 | 1,305 | - | rel: rel 25%: nothing recorded | new |
| sunion | sunion_reply | members | 1,001,999 | 1,001,999 | 1,001,999 | - | bound: eq: nothing recorded | new |
| sunion | sunion_worst_sadd_ms | ms | 362.7 | 0.482 | 321.9 | - | bound: <= 20.00 | pass |

## Not run this time

`01-scan-match/default/dbsize`, `01-scan-match/default/hscan_match_c1000_calls`, `01-scan-match/default/hscan_match_c1000_distinct`, `01-scan-match/default/hscan_match_c1000_ms`, `01-scan-match/default/keys_schedule_ms`, `01-scan-match/default/keys_schedule_returned`, `01-scan-match/default/scan_all_c1000_calls`, `01-scan-match/default/scan_all_c1000_distinct`, `01-scan-match/default/scan_all_c1000_ms`, `01-scan-match/default/scan_infix_c1000_calls`, `01-scan-match/default/scan_infix_c1000_distinct`, `01-scan-match/default/scan_infix_c1000_ms`, `01-scan-match/default/scan_match_c10000_calls`, `01-scan-match/default/scan_match_c10000_distinct`, `01-scan-match/default/scan_match_c10000_ms`, `01-scan-match/default/scan_match_c1000_calls`, `01-scan-match/default/scan_match_c1000_distinct`, `01-scan-match/default/scan_match_c1000_ms`, `01-scan-match/default/sscan_match_c1000_calls`, `01-scan-match/default/sscan_match_c1000_distinct`, `01-scan-match/default/sscan_match_c1000_ms`, `01-scan-match/default/zscan_match_c1000_calls`, `01-scan-match/default/zscan_match_c1000_distinct`, `01-scan-match/default/zscan_match_c1000_ms`, `02-growth-ping/ex17m/dbsize`, `02-growth-ping/ex17m/load_s`, `02-growth-ping/ex17m/ping_max_ms`, `02-growth-ping/ex17m/ping_p99_ms`, `02-growth-ping/ex17m/pings`, `02-growth-ping/ex17m/pings_over_10ms`, `02-growth-ping/ex17m/rss_loaded_mib`, `02-growth-ping/ex17m/used_memory_mib`, `02-growth-ping/tokio/dbsize`, `02-growth-ping/tokio/load_s`, `02-growth-ping/tokio/ping_max_ms`, `02-growth-ping/tokio/ping_p99_ms`, `02-growth-ping/tokio/pings`, `02-growth-ping/tokio/pings_over_10ms`, `02-growth-ping/tokio/rss_loaded_mib`, `02-growth-ping/tokio/used_memory_mib`, `02-growth-ping/uring1/dbsize`, `02-growth-ping/uring1/load_s`, `02-growth-ping/uring1/ping_max_ms`, `02-growth-ping/uring1/ping_p99_ms`, `02-growth-ping/uring1/pings`, `02-growth-ping/uring1/pings_over_10ms`, `02-growth-ping/uring1/rss_loaded_mib`, `02-growth-ping/uring1/used_memory_mib`, `03-give-back/churn/delete_ms`, `03-give-back/churn/rss_after_15s_mib`, `03-give-back/churn/rss_after_1_5s_mib`, `03-give-back/churn/rss_after_5s_mib`, `03-give-back/churn/rss_loaded_mib`, `03-give-back/churn/rss_start_mib`, `03-give-back/churn/used_after_mib`, `03-give-back/churn/used_loaded_mib`, `03-give-back/hashdel/delete_ms`, `03-give-back/hashdel/rss_after_15s_mib`, `03-give-back/hashdel/rss_after_1_5s_mib`, `03-give-back/hashdel/rss_after_5s_mib`, `03-give-back/hashdel/rss_loaded_mib`, `03-give-back/hashdel/rss_start_mib`, `03-give-back/hashdel/used_after_mib`, `03-give-back/hashdel/used_loaded_mib`, `03-give-back/hdel/delete_ms`, `03-give-back/hdel/rss_after_15s_mib`, `03-give-back/hdel/rss_after_1_5s_mib`, `03-give-back/hdel/rss_after_5s_mib`, `03-give-back/hdel/rss_loaded_mib`, `03-give-back/hdel/rss_start_mib`, `03-give-back/hdel/used_after_mib`, `03-give-back/hdel/used_loaded_mib`, `04-long-commands/tokio/dbsize`, `04-long-commands/tokio/del_cmd_ms`, `04-long-commands/tokio/del_probes_stalled`, `04-long-commands/tokio/del_reply`, `04-long-commands/tokio/del_worst_ping_max_ms`, `04-long-commands/tokio/del_worst_ping_ms`, `04-long-commands/tokio/keys_cmd_ms`, `04-long-commands/tokio/keys_probes_stalled`, `04-long-commands/tokio/keys_reply`, `04-long-commands/tokio/keys_worst_ping_max_ms`, `04-long-commands/tokio/keys_worst_ping_ms`, `04-long-commands/tokio/scan_cmd_ms`, `04-long-commands/tokio/scan_probes_stalled`, `04-long-commands/tokio/scan_reply`, `04-long-commands/tokio/scan_worst_ping_max_ms`, `04-long-commands/tokio/scan_worst_ping_ms`, `04-long-commands/uring/dbsize`, `04-long-commands/uring/del_cmd_ms`, `04-long-commands/uring/del_least_worst_ping_ms`, `04-long-commands/uring/del_probes_stalled`, `04-long-commands/uring/del_reply`, `04-long-commands/uring/del_worst_ping_ms`, `04-long-commands/uring/keys_cmd_ms`, `04-long-commands/uring/keys_least_worst_ping_ms`, `04-long-commands/uring/keys_probes_stalled`, `04-long-commands/uring/keys_reply`, `04-long-commands/uring/keys_worst_ping_ms`, `04-long-commands/uring/scan_cmd_ms`, `04-long-commands/uring/scan_least_worst_ping_ms`, `04-long-commands/uring/scan_probes_stalled`, `04-long-commands/uring/scan_reply`, `04-long-commands/uring/scan_worst_ping_ms`, `05-stream-memory/one1m/one_field_bytes_per_entry`, `05-stream-memory/one1m/one_field_load_s`, `05-stream-memory/one1m/one_field_rss_mib`, `05-stream-memory/one1m/one_field_used_mib`, `05-stream-memory/shapes/shared5_bytes_per_entry`, `05-stream-memory/shapes/shared5_load_s`, `05-stream-memory/shapes/unique5_bytes_per_entry`, `05-stream-memory/shapes/unique5_load_s`, `05-stream-memory/shapes/val200_bytes_per_entry`, `05-stream-memory/shapes/val200_load_s`, `05-stream-memory/shapes/xadd5_rps`, `05-stream-memory/shapes/xrange5_count10_rps`, `05-stream-memory/shapes/xrange_one_id_rps`, `06-stream-consumers/pel/pending_bytes_per_entry`, `06-stream-consumers/pel/xadd_other_stream_max_ms`, `06-stream-consumers/pel/xadd_other_stream_rps`, `06-stream-consumers/pel/xadd_polled_stream_max_ms`, `06-stream-consumers/pel/xadd_polled_stream_rps`, `06-stream-consumers/pel/xinfo_consumers_ms`, `06-stream-consumers/pel/xinfo_polls`, `06-stream-consumers/pel/xpending_c50_ms`, `06-stream-consumers/pel/xpending_summary_ms`, `06-stream-consumers/pel/xreadgroup_c50_history_ms`, `07-save-ping/tokio1/ping_during_save_max_ms`, `07-save-ping/tokio1/ping_during_save_ms`, `07-save-ping/tokio1/save_ms`, `08-ft-info/ftinfo/ft_info_polls_per_s`, `08-ft-info/ftinfo/ft_info_text_ms`, `08-ft-info/ftinfo/ft_info_vec_ms`, `08-ft-info/ftinfo/hset_vec_doc_rps`, `08-ft-info/ftinfo/populate_s`, `08-ft-info/ftinfo/set_rps_beside_ft_info`, `08-ft-info/ftinfo/vec_doc_bytes`, `09-vector-sets/bin_768_50k/bytes_per_vector`, `09-vector-sets/bin_768_50k/recall_at_10`, `09-vector-sets/bin_768_50k/vadd_per_s`, `09-vector-sets/bin_768_50k/vsim_qps`, `09-vector-sets/f32_768_10k/recall_at_10`, `09-vector-sets/f32_768_10k/vadd_per_s`, `09-vector-sets/f32_768_10k/vsim_p50_ms`, `09-vector-sets/f32_768_10k/vsim_qps`, `09-vector-sets/q8_768_10k/recall_at_10`, `09-vector-sets/q8_768_10k/vadd_per_s`, `09-vector-sets/q8_768_10k/vsim_p50_ms`, `09-vector-sets/q8_768_10k/vsim_qps`, `09-vector-sets/q8_768_50k/bytes_per_vector`, `09-vector-sets/q8_768_50k/recall_at_10`, `09-vector-sets/q8_768_50k/vadd_per_s`, `09-vector-sets/q8_768_50k/vsim_qps`, `10-redisvl/redisvl/checks_failed`, `10-redisvl/redisvl/checks_passed`, `10-redisvl/redisvl/lua_commands_sent`, `11-migrate-conns/migrate/keys_left_on_source`, `11-migrate-conns/migrate/keys_on_target`, `11-migrate-conns/migrate/migrate_ms`, `11-migrate-conns/migrate/target_connections`, `12-search-fanout/cluster3/peer_connections_max`, `12-search-fanout/cluster3/peer_kept_clients_max`, `12-search-fanout/cluster3/searches_per_s`, `12-search-fanout/cluster3/searches_wrong_count`, `13-cluster-reshard/reshard1000/cluster_check_failed`, `13-cluster-reshard/reshard1000/keys_after`, `13-cluster-reshard/reshard1000/keys_moved`, `13-cluster-reshard/reshard1000/load_s`, `13-cluster-reshard/reshard1000/reshard_s`

