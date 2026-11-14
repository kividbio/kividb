# Regression suite: master-d66f0f93-d4s_v5-part2

Verdict: **ok**

- Machine: Standard_D4s_v5, 4 vCPU, Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz, kernel 7.0.0-1017-azure, glibc 2.39
- Client: Python 3.12.3, redis-py 5.2.1
- v1_2_2: `kividb 1.2.2`, sha256 `38fc2cea40c131e041f4914527a98719e5d34dffdfe2abdc1f07ecb86aa6723e`
- candidate: `kividb 1.2.1 (default build: no Lua, no TLS)`, sha256 `9771dc1ec052679fa29498cf39b3daedb3f5718d29bc1501732aa32c2df5f556`
- redis: `Redis server v=8.10.2 sha=00000000:1 malloc=jemalloc-5.3.0 bits=64 build=75a9b36b1f0aa00`, sha256 `b0cd304980112fa6cd8dced524fdd6b136fd8efb802f5cf9e81610051a11891c`
- candidate commit: `d66f0f93`
- date: 2026-11-14; FULL=0, SCALE=1
- run time: 05-stream-memory 109s, 06-stream-consumers 27s, 07-save-ping 10s, 08-ft-info 59s, 09-vector-sets 145s, 10-redisvl 2s, 11-migrate-conns 3s, 12-search-fanout 4s, 13-cluster-reshard 495s

## 05-stream-memory

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| one1m | one_field_bytes_per_entry | B | 158.4 | 6.23 | 13.10 | - | rel: rel 5%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |
| one1m | one_field_load_s | s | 8.38 | 8.19 | 8.13 | - | info | info |
| one1m | one_field_rss_mib | MiB | 195.8 | 20.71 | 25.44 | - | rel: rel 5%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |
| one1m | one_field_used_mib | MiB | 152.4 | 7.33 | 13.29 | - | info | info |
| shapes | shared5_bytes_per_entry | B | 478.4 | 14.65 | 26.16 | - | rel: rel 5%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |
| shapes | shared5_load_s | s | 2.77 | 2.66 | 2.66 | - | info | info |
| shapes | unique5_bytes_per_entry | B | 478.4 | 54.01 | 75.36 | - | rel: rel 5%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |
| shapes | unique5_load_s | s | 2.86 | 2.76 | 2.78 | - | info | info |
| shapes | val200_bytes_per_entry | B | 350.9 | 216.5 | 216.3 | - | rel: rel 5%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |
| shapes | val200_load_s | s | 1.5 | 1.53 | 1.52 | - | info | info |
| shapes | xadd5_rps | req/s | 332,447 | 535,332 | 516,529 | - | rel: rel 15%: nothing recorded | new |
| shapes | xrange5_count10_rps | req/s | 89,955 | 86,381 | 131,810 | - | rel: rel 15%: nothing recorded | new |
| shapes | xrange_one_id_rps | req/s | 485,437 | 443,262 | 454,545 | - | rel: rel 15%: nothing recorded | new |

## 06-stream-consumers

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| pel | pending_bytes_per_entry | B | 115.9 | 122.3 | 166.7 | - | rel: rel 5%: nothing recorded | new |
| pel | xadd_other_stream_max_ms | ms | 133.6 | 0.471 | 0.423 | - | info | info |
| pel | xadd_other_stream_rps | req/s | 103,520 | 104,384 | 139,958 | - | rel: rel 15%: nothing recorded | new |
| pel | xadd_polled_stream_max_ms | ms | 76.03 | 0.823 | 0.407 | - | info | info |
| pel | xadd_polled_stream_rps | req/s | 42,799 | 117,371 | 144,092 | - | rel: rel 15%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |
| pel | xinfo_consumers_ms | ms | 65.19 | 0.395 | 0.362 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |
| pel | xinfo_polls | calls | 14.00 | 42.00 | 33.00 | - | info | info |
| pel | xpending_c50_ms | ms | 1.61 | 0.11 | 0.0859 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |
| pel | xpending_summary_ms | ms | 7.59 | 0.193 | 0.165 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |
| pel | xreadgroup_c50_history_ms | ms | 1.59 | 0.133 | 0.0992 | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |

## 07-save-ping

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| tokio1 | ping_during_save_max_ms | ms | 338.2 | 2.19 | 531.7 | - | bound: <= 20.00 | pass |
| tokio1 | ping_during_save_ms | ms | 333.3 | 1.48 | 501.4 | - | bound: <= 20.00 | pass |
| tokio1 | save_ms | ms | 333.7 | 339.4 | 501.9 | - | rel: rel 15%: nothing recorded | new |

## 08-ft-info

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| ftinfo | ft_info_polls_per_s | calls/s | 17.99 | 15,600 | - | - | info | info |
| ftinfo | ft_info_text_ms | ms | 116.3 | 0.12 | - | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |
| ftinfo | ft_info_vec_ms | ms | 116.7 | 0.125 | - | - | rel: rel 50%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |
| ftinfo | hset_vec_doc_rps | req/s | 110,865 | 107,797 | - | - | rel: rel 15%: nothing recorded | new |
| ftinfo | populate_s | s | 2.1 | 2.09 | - | - | info | info |
| ftinfo | set_rps_beside_ft_info | req/s | 113.1 | 47,430 | - | - | rel: rel 20%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |
| ftinfo | vec_doc_bytes | B | 474.5 | 494.1 | - | - | rel: rel 5%: nothing recorded | new |

## 09-vector-sets

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| bin_768_50k | bytes_per_vector | B | 3,168 | 191.7 | 853.4 | - | rel: rel 5%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |
| bin_768_50k | recall_at_10 | ratio | 1 | 0.103 | 0.107 | - | rel: rel 2%: nothing recorded | new |
| bin_768_50k | vadd_per_s | VADD/s | 78,904 | 64,808 | 5,776 | - | info | info |
| bin_768_50k | vsim_qps | q/s | 112.5 | 728.9 | 7,788 | - | info | info |
| q8_768_10k | recall_at_10 | ratio | 1 | 0.972 | 0.972 | - | rel: rel 2%: nothing recorded | new |
| q8_768_10k | vadd_per_s | VADD/s | 67,141 | 58,078 | 3,939 | - | info | info |
| q8_768_10k | vsim_p50_ms | ms | 2.11 | 2.06 | 0.325 | - | info | info |
| q8_768_10k | vsim_qps | q/s | 1,084 | 974.5 | 7,844 | - | rel: rel 25%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |
| q8_768_50k | bytes_per_vector | B | 3,168 | 863.7 | 1,526 | - | rel: rel 5%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |
| q8_768_50k | recall_at_10 | ratio | 1 | 0.965 | 0.963 | - | rel: rel 2%: nothing recorded | new |
| q8_768_50k | vadd_per_s | VADD/s | 91,502 | 61,626 | 3,697 | - | info | info |
| q8_768_50k | vsim_qps | q/s | 112.5 | 117.0 | 6,619 | - | info | info |

## 10-redisvl

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| redisvl | checks_failed | checks | 8 | 0 | - | - | bound: <= 0 | pass |
| redisvl | checks_passed | checks | 59.00 | 67.00 | - | - | bound: eq: nothing recorded | new |
| redisvl | lua_commands_sent | commands | 0 | 0 | - | - | info | info |

## 11-migrate-conns

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| migrate | keys_left_on_source | keys | 0 | 0 | 0 | - | bound: eq: nothing recorded | new |
| migrate | keys_on_target | keys | 2,000 | 2,000 | 2,000 | - | bound: eq: nothing recorded | new |
| migrate | migrate_ms | ms | 0.16 | 0.105 | 0.0955 | - | info | info |
| migrate | target_connections | connections | 2,000 | 1 | 1 | - | bound: <= 3 | pass |

## 12-search-fanout

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| cluster3 | peer_connections_max | connections | 2,000 | 1 | - | - | bound: <= 20.00 | pass |
| cluster3 | peer_kept_clients_max | clients | 0 | 2 | - | - | info | info |
| cluster3 | searches_per_s | searches/s | 4,520 | 5,772 | - | - | rel: rel 25%: nothing recorded | new |
| cluster3 | searches_wrong_count | searches | 0 | 0 | - | - | bound: eq: nothing recorded | new |

## 13-cluster-reshard

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| reshard1000 | cluster_check_failed | - | 0 | 0 | 0 | - | bound: <= 0 | pass |
| reshard1000 | keys_after | keys | 3,000,000 | 3,000,000 | 3,000,000 | - | bound: eq: nothing recorded | new |
| reshard1000 | keys_moved | keys | 183,078 | 183,078 | 183,078 | - | bound: eq: nothing recorded | new |
| reshard1000 | load_s | s | 23.91 | 24.21 | 23.94 | - | info | info |
| reshard1000 | reshard_s | s | 381.8 | 25.30 | 7.11 | - | rel: rel 15%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |

## Not run this time

`01-scan-match/default/dbsize`, `01-scan-match/default/hscan_match_c1000_calls`, `01-scan-match/default/hscan_match_c1000_distinct`, `01-scan-match/default/hscan_match_c1000_ms`, `01-scan-match/default/keys_schedule_ms`, `01-scan-match/default/keys_schedule_returned`, `01-scan-match/default/scan_all_c1000_calls`, `01-scan-match/default/scan_all_c1000_distinct`, `01-scan-match/default/scan_all_c1000_ms`, `01-scan-match/default/scan_infix_c1000_calls`, `01-scan-match/default/scan_infix_c1000_distinct`, `01-scan-match/default/scan_infix_c1000_ms`, `01-scan-match/default/scan_match_c10000_calls`, `01-scan-match/default/scan_match_c10000_distinct`, `01-scan-match/default/scan_match_c10000_ms`, `01-scan-match/default/scan_match_c1000_calls`, `01-scan-match/default/scan_match_c1000_distinct`, `01-scan-match/default/scan_match_c1000_ms`, `01-scan-match/default/sscan_match_c1000_calls`, `01-scan-match/default/sscan_match_c1000_distinct`, `01-scan-match/default/sscan_match_c1000_ms`, `01-scan-match/default/zscan_match_c1000_calls`, `01-scan-match/default/zscan_match_c1000_distinct`, `01-scan-match/default/zscan_match_c1000_ms`, `02-growth-ping/ex17m/dbsize`, `02-growth-ping/ex17m/load_s`, `02-growth-ping/ex17m/ping_max_ms`, `02-growth-ping/ex17m/ping_p99_ms`, `02-growth-ping/ex17m/pings`, `02-growth-ping/ex17m/pings_over_10ms`, `02-growth-ping/ex17m/rss_loaded_mib`, `02-growth-ping/ex17m/used_memory_mib`, `02-growth-ping/tokio/dbsize`, `02-growth-ping/tokio/load_s`, `02-growth-ping/tokio/ping_max_ms`, `02-growth-ping/tokio/ping_p99_ms`, `02-growth-ping/tokio/pings`, `02-growth-ping/tokio/pings_over_10ms`, `02-growth-ping/tokio/rss_loaded_mib`, `02-growth-ping/tokio/used_memory_mib`, `02-growth-ping/uring1/dbsize`, `02-growth-ping/uring1/load_s`, `02-growth-ping/uring1/ping_max_ms`, `02-growth-ping/uring1/ping_p99_ms`, `02-growth-ping/uring1/pings`, `02-growth-ping/uring1/pings_over_10ms`, `02-growth-ping/uring1/rss_loaded_mib`, `02-growth-ping/uring1/used_memory_mib`, `03-give-back/churn/delete_ms`, `03-give-back/churn/rss_after_15s_mib`, `03-give-back/churn/rss_after_1_5s_mib`, `03-give-back/churn/rss_after_5s_mib`, `03-give-back/churn/rss_loaded_mib`, `03-give-back/churn/rss_start_mib`, `03-give-back/churn/used_after_mib`, `03-give-back/churn/used_loaded_mib`, `03-give-back/hashdel/delete_ms`, `03-give-back/hashdel/rss_after_15s_mib`, `03-give-back/hashdel/rss_after_1_5s_mib`, `03-give-back/hashdel/rss_after_5s_mib`, `03-give-back/hashdel/rss_loaded_mib`, `03-give-back/hashdel/rss_start_mib`, `03-give-back/hashdel/used_after_mib`, `03-give-back/hashdel/used_loaded_mib`, `03-give-back/hdel/delete_ms`, `03-give-back/hdel/rss_after_15s_mib`, `03-give-back/hdel/rss_after_1_5s_mib`, `03-give-back/hdel/rss_after_5s_mib`, `03-give-back/hdel/rss_loaded_mib`, `03-give-back/hdel/rss_start_mib`, `03-give-back/hdel/used_after_mib`, `03-give-back/hdel/used_loaded_mib`, `04-long-commands/tokio/dbsize`, `04-long-commands/tokio/del_cmd_ms`, `04-long-commands/tokio/del_probes_stalled`, `04-long-commands/tokio/del_reply`, `04-long-commands/tokio/del_worst_ping_max_ms`, `04-long-commands/tokio/del_worst_ping_ms`, `04-long-commands/tokio/keys_cmd_ms`, `04-long-commands/tokio/keys_probes_stalled`, `04-long-commands/tokio/keys_reply`, `04-long-commands/tokio/keys_worst_ping_max_ms`, `04-long-commands/tokio/keys_worst_ping_ms`, `04-long-commands/tokio/scan_cmd_ms`, `04-long-commands/tokio/scan_probes_stalled`, `04-long-commands/tokio/scan_reply`, `04-long-commands/tokio/scan_worst_ping_max_ms`, `04-long-commands/tokio/scan_worst_ping_ms`, `04-long-commands/uring/dbsize`, `04-long-commands/uring/del_cmd_ms`, `04-long-commands/uring/del_least_worst_ping_ms`, `04-long-commands/uring/del_probes_stalled`, `04-long-commands/uring/del_reply`, `04-long-commands/uring/del_worst_ping_ms`, `04-long-commands/uring/keys_cmd_ms`, `04-long-commands/uring/keys_least_worst_ping_ms`, `04-long-commands/uring/keys_probes_stalled`, `04-long-commands/uring/keys_reply`, `04-long-commands/uring/keys_worst_ping_ms`, `04-long-commands/uring/scan_cmd_ms`, `04-long-commands/uring/scan_least_worst_ping_ms`, `04-long-commands/uring/scan_probes_stalled`, `04-long-commands/uring/scan_reply`, `04-long-commands/uring/scan_worst_ping_ms`

