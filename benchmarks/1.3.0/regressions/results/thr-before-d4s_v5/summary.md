# Regression suite: thr-before-d4s_v5

Verdict: **INVALID** (5 failed)

Why the run is invalid:

- the run failed: 25-centrifuge-scripts-publish scripts v1_2_2: suite exited 1; 25-centrifuge-scripts-publish scripts candidate: suite exited 1; 25-centrifuge-scripts-publish scripts redis: suite exited 1; 25-centrifuge-scripts-publish publish redis: suite exited 1
- 25-centrifuge-scripts-publish/scripts/add_calls_per_s: missing
- 25-centrifuge-scripts-publish/scripts/history_calls_per_s: missing
- 25-centrifuge-scripts-publish/publish/publish_8x16_per_s: no redis value
- 25-centrifuge-scripts-publish/publish/publish_p16_per_s: no redis value

- Machine: Standard_D4s_v5, 4 vCPU, Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz, kernel 7.0.0-1017-azure, glibc 2.39
- CPU: recorded on INTEL(R) XEON(R) PLATINUM 8573C / Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz; now Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz
- calibration (indicative only): 93.0 ms for a fixed sha256 loop, 4 cores
- Client: Python 3.12.3, redis-py 5.2.1
- v1_2_2: `kividb 1.2.2`, sha256 `38fc2cea40c131e041f4914527a98719e5d34dffdfe2abdc1f07ecb86aa6723e`, rustc 1.100.0
- candidate: `kividb 1.2.1 (default build: no Lua, no TLS)`, sha256 `ae622d5cdc0bb72eb2db8a81af5cf76001d2ad0b45f5c1878ea6355457d3cfd8`, rustc 1.100.0
- redis: `Redis server v=8.10.2 sha=00000000:1 malloc=jemalloc-5.3.0 bits=64 build=75a9b36b1f0aa00`, sha256 `b0cd304980112fa6cd8dced524fdd6b136fd8efb802f5cf9e81610051a11891c`
- v1_2_2_full: `kividb 1.2.2`, sha256 `ac5e05fe1274fd1167780e512bc03aa103340f42525a2cef4a2a762e47317ad5`, rustc 1.100.0
- candidate_full: `kividb 1.2.1 (full build: Lua, TLS)`, sha256 `1877f11929352c27c4027cd9795c57fc959d78fd8ea09d6e531547005b42f315`, rustc 1.100.0
- candidate commit: `e2974628`
- date: 2026-11-19; FULL=1, SCALE=1
- run time: 01-scan-match 126s, 02-growth-ping 693s, 03-give-back 199s, 04-long-commands 196s, 05-stream-memory 110s, 06-stream-consumers 28s, 07-save-ping 10s, 08-ft-info 60s, 09-vector-sets 180s, 10-redisvl 11s, 11-migrate-conns 3s, 12-search-fanout 4s, 13-cluster-reshard 515s, 14-sinter-big-small 372s, 15-brpop-many-keys 361s, 16-random-sample 77s, 17-set-memory 44s, 19-hash-memory 80s, 21-list-memory 48s, 22-scan-large-count 40s, 23-pubsub-pattern-fanout 142s, 24-snapshot-size 422s, 25-centrifuge-scripts-publish 122s, 27-ratelimit-restarted 568s, 31-zset-memory 93s

Host speed (this host vs the recorded one; 1.00 = same, below 1 = slower), from v1.2.2 measured in both sessions: global 1.01x over 168 metrics
- 01-scan-match/default: 1.07x (8 metrics)
- 02-growth-ping/ex17m: 1.00x (1 metrics, too few: the recording host's factor is used)
- 02-growth-ping/tokio: 1.01x (1 metrics, too few: the recording host's factor is used)
- 02-growth-ping/uring1: 1.04x (1 metrics, too few: the recording host's factor is used)
- 03-give-back/churn: 1.01x (1 metrics, too few: the recording host's factor is used)
- 03-give-back/hashdel: 1.00x (1 metrics, too few: the recording host's factor is used)
- 03-give-back/hdel: 1.05x (1 metrics, too few: the recording host's factor is used)
- 04-long-commands/tokio: 1.02x (3 metrics)
- 04-long-commands/uring: 1.03x (3 metrics)
- 05-stream-memory/one1m: 1.03x (1 metrics, too few: the recording host's factor is used)
- 05-stream-memory/shapes: 0.97x (6 metrics)
- 06-stream-consumers/pel: 0.93x (8 metrics)
- 07-save-ping/tokio1: 0.97x (1 metrics, too few: the recording host's factor is used)
- 08-ft-info/ftinfo: 1.00x (6 metrics)
- 09-vector-sets/bin_768_50k: 0.97x (2 metrics, too few: the recording host's factor is used)
- 09-vector-sets/f32_768_10k: 0.84x (3 metrics)
- 09-vector-sets/q8_768_10k: 0.99x (3 metrics)
- 09-vector-sets/q8_768_50k: 1.07x (2 metrics, too few: the recording host's factor is used)
- 11-migrate-conns/migrate: 0.92x (1 metrics, too few: the recording host's factor is used)
- 12-search-fanout/cluster3: 0.87x (1 metrics, too few: the recording host's factor is used)
- 13-cluster-reshard/reshard1000: 0.97x (2 metrics, too few: the recording host's factor is used)
- 14-sinter-big-small/default: 1.03x (14 metrics)
- 14-sinter-big-small/n20m: 1.05x (7 metrics)
- 14-sinter-big-small/sunion: 1.05x (1 metrics, too few: the recording host's factor is used)
- 16-random-sample/n100: 0.86x (12 metrics)
- 16-random-sample/n10k: 1.15x (12 metrics)
- 16-random-sample/n1m: 0.96x (12 metrics)
- 17-set-memory/ints: 1.84x (4 metrics)
- 17-set-memory/strings: 2.20x (4 metrics)
- 19-hash-memory/plain: 1.05x (4 metrics)
- 19-hash-memory/ttl: 1.04x (4 metrics)
- 21-list-memory/packed: 2.11x (4 metrics)
- 21-list-memory/plain: 2.17x (4 metrics)
- 22-scan-large-count/t4: 1.08x (3 metrics)
- 23-pubsub-pattern-fanout/channel: 0.96x (2 metrics, too few: the recording host's factor is used)
- 23-pubsub-pattern-fanout/pattern: 0.96x (2 metrics, too few: the recording host's factor is used)
- 24-snapshot-size/coll-default: 0.98x (2 metrics, too few: the recording host's factor is used)
- 24-snapshot-size/coll-yes: 1.08x (2 metrics, too few: the recording host's factor is used)
- 24-snapshot-size/issue-default: 1.00x (2 metrics, too few: the recording host's factor is used)
- 24-snapshot-size/issue-yes: 1.00x (2 metrics, too few: the recording host's factor is used)
- 25-centrifuge-scripts-publish/publish: 0.91x (2 metrics, too few: the recording host's factor is used)
- 27-ratelimit-restarted/t2: 1.01x (3 metrics)
- 31-zset-memory/float: 0.89x (5 metrics)
- 31-zset-memory/int: 0.88x (5 metrics)
- recorded on Standard_D4s_v5 / ?: 1.00x (56 metrics)
- recorded on Standard_D4s_v5 / INTEL(R) XEON(R) PLATINUM 8573C: 0.88x (12 metrics)
- recorded on Standard_D4s_v5 / Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz: 1.04x (100 metrics)

Metrics whose own v1.2.2 factor differs from their group's by more than 1.3x (rule applied in brackets):

- 09-vector-sets/q8_768_10k/vsim_qps: own 1.49x, group 0.99x (own-capped)
- 16-random-sample/n100/zrandmember_10_ws_us: own 1.15x, group 0.86x (own-capped)

## 01-scan-match

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| default | dbsize | keys | 2,002,003 | 2,002,003 | 2,002,003 | 2,002,003 | bound: == 2,002,003 | pass |
| default | hscan_match_c1000_calls | calls | 201.0 | 201.0 | 201.0 | 201.0 | info | info |
| default | hscan_match_c1000_distinct | keys | 200.0 | 200.0 | 200.0 | 200.0 | bound: == 200.0 | pass |
| default | hscan_match_c1000_ms | ms | 441.0 | 377.8 | 27.03 | 428.0 | rel: <= 542.4 (35% over 401.8, recorded 428.0 scaled by host speed 1.07x (group)) | pass |
| default | keys_schedule_ms | ms | 116.7 | 55.90 | 126.2 | 60.44 | rel: <= 62.42 (10% over 56.74, recorded 60.44 scaled by host speed 1.07x (group)) | pass |
| default | keys_schedule_returned | keys | 2,000 | 2,000 | 2,000 | 2,000 | bound: == 2,000 | pass |
| default | scan_all_c1000_calls | calls | 513.0 | 512.0 | 2,002 | 512.0 | info | info |
| default | scan_all_c1000_distinct | keys | 2,002,003 | 2,002,003 | 2,002,003 | 2,002,003 | bound: == 2,002,003 | pass |
| default | scan_all_c1000_ms | ms | 3,245 | 1,819 | 2,172 | 2,180 | rel: <= 2,252 (10% over 2,047, recorded 2,180 scaled by host speed 1.07x (group))<br>ratio: gain over v1.2.2 1.78x >= 1.00x (60% of recorded 1.66x)<br>vs_redis: 0.84x Redis <= 1.3x | pass<br>pass<br>pass |
| default | scan_infix_c1000_calls | calls | 513.0 | 512.0 | 2,002 | 512.0 | info | info |
| default | scan_infix_c1000_distinct | keys | 1,112,222 | 1,112,222 | 1,112,222 | 1,112,222 | bound: == 1,112,222 | pass |
| default | scan_infix_c1000_ms | ms | 2,694 | 1,121 | 1,640 | 1,494 | rel: <= 1,543 (10% over 1,403, recorded 1,494 scaled by host speed 1.07x (group))<br>ratio: gain over v1.2.2 2.40x >= 1.08x (60% of recorded 1.80x)<br>vs_redis: 0.68x Redis <= 1.3x | pass<br>pass<br>pass |
| default | scan_match_c10000_calls | calls | 193.0 | 193.0 | 201.0 | 193.0 | info | info |
| default | scan_match_c10000_distinct | keys | 2,000 | 2,000 | 2,000 | 2,000 | bound: == 2,000 | pass |
| default | scan_match_c10000_ms | ms | 778.4 | 110.6 | 368.3 | 238.9 | rel: <= 246.7 (10% over 224.3, recorded 238.9 scaled by host speed 1.07x (group))<br>ratio: gain over v1.2.2 7.04x >= 2.07x (60% of recorded 3.45x)<br>vs_redis: 0.30x Redis <= 1.3x | pass<br>pass<br>pass |
| default | scan_match_c1000_calls | calls | 513.0 | 512.0 | 2,002 | 512.0 | info | info |
| default | scan_match_c1000_distinct | keys | 2,000 | 2,000 | 2,000 | 2,000 | bound: == 2,000 | pass |
| default | scan_match_c1000_ms | ms | 1,316 | 183.2 | 496.5 | 406.2 | rel: <= 419.5 (10% over 381.4, recorded 406.2 scaled by host speed 1.07x (group))<br>ratio: gain over v1.2.2 7.19x >= 2.47x (60% of recorded 4.12x)<br>vs_redis: 0.37x Redis <= 1.3x | pass<br>pass<br>pass |
| default | sscan_match_c1000_calls | calls | 201.0 | 201.0 | 201.0 | 201.0 | info | info |
| default | sscan_match_c1000_distinct | keys | 200.0 | 200.0 | 200.0 | 200.0 | bound: == 200.0 | pass |
| default | sscan_match_c1000_ms | ms | 813.4 | 1,473 | 26.28 | 1,093 | rel: <= 1,386 (35% over 1,026, recorded 1,093 scaled by host speed 1.07x (group)) | **FAIL** |
| default | zscan_match_c1000_calls | calls | 201.0 | 201.0 | 201.0 | 201.0 | info | info |
| default | zscan_match_c1000_distinct | keys | 200.0 | 200.0 | 200.0 | 200.0 | bound: == 200.0 | pass |
| default | zscan_match_c1000_ms | ms | 745.5 | 577.5 | 39.43 | 829.5 | rel: <= 1,051 (35% over 778.9, recorded 829.5 scaled by host speed 1.07x (group)) | pass |

## 02-growth-ping

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| ex17m | dbsize | keys | 17,500,000 | 17,500,000 | 17,500,000 | 17,500,000 | bound: == 17,500,000 | pass |
| ex17m | load_s | s | 145.0 | 141.6 | 144.3 | 143.2 | rel: <= 157.8 (10% over 143.4, recorded 143.2 scaled by host speed 1.00x (host)) | pass |
| ex17m | ping_max_ms | ms | 157.0 | 10.29 | 10.22 | 17.02 | rel: <= 25.53 (50% over 17.02, Redis 10.22 + 10) | pass |
| ex17m | ping_p99_ms | ms | 3.41 | 3.35 | 1.76 | 3.64 | rel: <= 11.76 (50% over 3.64, Redis 1.76 + 10) | pass |
| ex17m | pings | pings | 123,250 | 121,985 | 132,998 | 120,736 | info | info |
| ex17m | pings_over_10ms | pings | 71.00 | 1 | 1 | 14.00 | info | info |
| ex17m | rss_loaded_mib | MiB | 1,506 | 1,511 | 2,084 | 1,507 | rel: <= 1,583 (5% over 1,507) | pass |
| ex17m | used_memory_mib | MiB | 1,613 | 1,613 | 2,027 | 1,613 | rel: <= 1,694 (5% over 1,613) | pass |
| tokio | dbsize | keys | 8,000,000 | 8,000,000 | 8,000,000 | 8,000,000 | bound: == 8,000,000 | pass |
| tokio | load_s | s | 50.88 | 49.19 | 50.04 | 50.84 | rel: <= 56.02 (10% over 50.93, recorded 50.84 scaled by host speed 1.00x (host)) | pass |
| tokio | ping_max_ms | ms | 143.4 | 6.8 | 5.89 | 9.66 | rel: <= 15.89 (50% over 9.66, Redis 5.89 + 10)<br>ratio: gain over v1.2.2 21.08x >= 6.96x (60% of recorded 11.61x) | pass<br>pass |
| tokio | ping_p99_ms | ms | 1.8 | 2.46 | 1.17 | 3.03 | rel: <= 11.17 (50% over 3.03, Redis 1.17 + 10) | pass |
| tokio | pings | pings | 46,156 | 45,139 | 47,446 | 44,740 | info | info |
| tokio | pings_over_10ms | pings | 25.00 | 0 | 0 | 0 | info | info |
| tokio | rss_loaded_mib | MiB | 523.8 | 526.4 | 551.9 | 525.0 | rel: <= 551.3 (5% over 525.0) | pass |
| tokio | used_memory_mib | MiB | 511.4 | 511.5 | 534.8 | 511.4 | rel: <= 537.0 (5% over 511.4) | pass |
| uring1 | dbsize | keys | 8,000,000 | 8,000,000 | - | 8,000,000 | bound: == 8,000,000 | pass |
| uring1 | load_s | s | 50.98 | 51.06 | - | 55.60 | rel: <= 61.27 (10% over 55.70, recorded 55.60 scaled by host speed 1.00x (host)) | pass |
| uring1 | ping_max_ms | ms | 70.89 | 8.26 | - | 20.83 | rel: <= 31.25 (50% over 20.83)<br>ratio: gain over v1.2.2 8.58x >= 2.09x (60% of recorded 3.49x) | pass<br>pass |
| uring1 | ping_p99_ms | ms | 0.572 | 1.72 | - | 1.54 | rel: <= 2.32 (50% over 1.54) | pass |
| uring1 | pings | pings | 47,593 | 47,803 | - | 51,940 | info | info |
| uring1 | pings_over_10ms | pings | 29.00 | 0 | - | 2 | info | info |
| uring1 | rss_loaded_mib | MiB | 524.5 | 526.0 | - | 524.5 | rel: <= 550.7 (5% over 524.5) | pass |
| uring1 | used_memory_mib | MiB | 511.5 | 511.7 | - | 511.6 | rel: <= 537.2 (5% over 511.6) | pass |

## 03-give-back

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| churn | delete_ms | ms | 1,524 | 1,488 | 1,490 | 1,538 | info | info |
| churn | rss_after_15s_mib | MiB | 461.8 | 417.9 | 616.5 | 429.4 | rel: <= 536.8 (25% over 429.4, +5) | pass |
| churn | rss_after_1_5s_mib | MiB | 461.8 | 417.9 | 740.5 | 429.4 | rel: <= 536.8 (25% over 429.4, +5) | pass |
| churn | rss_after_5s_mib | MiB | 461.8 | 417.9 | 668.7 | 429.4 | rel: <= 536.8 (25% over 429.4, +5) | pass |
| churn | rss_loaded_mib | MiB | 789.0 | 791.2 | 746.4 | 789.3 | rel: <= 828.8 (5% over 789.3) | pass |
| churn | rss_start_mib | MiB | 12.50 | 14.64 | 12.73 | 12.73 | info | info |
| churn | used_after_mib | MiB | 372.0 | 372.1 | 361.8 | 372.1 | info | info |
| churn | used_loaded_mib | MiB | 742.5 | 742.7 | 719.1 | 742.6 | rel: <= 779.7 (5% over 742.6) | pass |
| hashdel | delete_ms | ms | 131.5 | 158.0 | 283.4 | 134.4 | info | info |
| hashdel | rss_after_15s_mib | MiB | 196.0 | 61.60 | 24.80 | 13.60 | rel: <= 18.60 (25% over 13.60, +5) | **FAIL** |
| hashdel | rss_after_1_5s_mib | MiB | 196.0 | 61.60 | 76.40 | 13.60 | rel: <= 18.60 (25% over 13.60, +5)<br>ratio: gain over v1.2.2 3.18x >= 8.66x (60% of recorded 14.44x) | **FAIL** |
| hashdel | rss_after_5s_mib | MiB | 196.0 | 61.60 | 76.40 | 13.60 | rel: <= 18.60 (25% over 13.60, +5) | **FAIL** |
| hashdel | rss_loaded_mib | MiB | 369.1 | 371.2 | 124.4 | 369.9 | rel: <= 388.4 (5% over 369.9) | pass |
| hashdel | rss_start_mib | MiB | 12.47 | 14.65 | 12.57 | 13.28 | info | info |
| hashdel | used_after_mib | MiB | 1.36 | 1.49 | 0.846 | 1.39 | info | info |
| hashdel | used_loaded_mib | MiB | 295.9 | 296.0 | 109.3 | 295.9 | rel: <= 310.7 (5% over 295.9) | pass |
| hdel | delete_ms | ms | 1,281 | 1,274 | 1,207 | 1,344 | info | info |
| hdel | rss_after_15s_mib | MiB | 74.00 | 15.20 | 13.80 | 13.60 | rel: <= 18.60 (25% over 13.60, +5) | pass |
| hdel | rss_after_1_5s_mib | MiB | 74.00 | 15.20 | 35.40 | 13.60 | rel: <= 18.60 (25% over 13.60, +5)<br>ratio: gain over v1.2.2 4.87x >= 3.27x (60% of recorded 5.45x) | pass<br>pass |
| hdel | rss_after_5s_mib | MiB | 74.00 | 15.20 | 24.20 | 13.60 | rel: <= 18.60 (25% over 13.60, +5) | pass |
| hdel | rss_loaded_mib | MiB | 137.5 | 139.7 | 47.41 | 138.2 | rel: <= 145.1 (5% over 138.2) | pass |
| hdel | rss_start_mib | MiB | 12.46 | 14.66 | 12.69 | 13.18 | info | info |
| hdel | used_after_mib | MiB | 1.36 | 1.49 | 0.846 | 1.39 | info | info |
| hdel | used_loaded_mib | MiB | 132.8 | 132.9 | 29.51 | 132.8 | rel: <= 139.5 (5% over 132.8) | pass |

## 04-long-commands

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| tokio | dbsize | keys | 2,000,000 | 2,000,000 | 2,000,000 | 2,000,000 | bound: == 2,000,000 | pass |
| tokio | del_cmd_ms | ms | 134.3 | 139.2 | 318.7 | 143.3 | rel: <= 154.0 (10% over 140.0, recorded 143.3 scaled by host speed 1.02x (group)) | pass |
| tokio | del_probes_stalled | probes | 4 | 0 | 4 | 0 | info | info |
| tokio | del_reply | items | 1 | 1 | 1 | 1 | bound: == 1 | pass |
| tokio | del_worst_ping_max_ms | ms | 134.0 | 1.35 | 322.9 | 1.22 | bound: <= 20.00 | pass |
| tokio | del_worst_ping_ms | ms | 133.9 | 0.113 | 318.0 | 0.942 | bound: <= 20.00 | pass |
| tokio | keys_cmd_ms | ms | 137.8 | 92.27 | 209.6 | 161.3 | rel: <= 173.4 (10% over 157.6, recorded 161.3 scaled by host speed 1.02x (group))<br>vs_v1_2_2: 0.67x v1.2.2 <= 0.85x | pass<br>pass |
| tokio | keys_probes_stalled | probes | 4 | 0 | 4 | 0 | info | info |
| tokio | keys_reply | items | 11,111 | 11,111 | 11,111 | 11,111 | bound: == 11,111 | pass |
| tokio | keys_worst_ping_max_ms | ms | 126.7 | 1.02 | 203.6 | 8.09 | bound: <= 20.00 | pass |
| tokio | keys_worst_ping_ms | ms | 126.4 | 0.134 | 197.6 | 5.94 | bound: <= 20.00 | pass |
| tokio | scan_cmd_ms | ms | 248.4 | 62.78 | 450.3 | 112.5 | rel: <= 120.9 (10% over 109.9, recorded 112.5 scaled by host speed 1.02x (group)) | pass |
| tokio | scan_probes_stalled | probes | 4 | 0 | 4 | 0 | info | info |
| tokio | scan_reply | items | 11,111 | 11,111 | 11,111 | 11,111 | info | info |
| tokio | scan_worst_ping_max_ms | ms | 237.8 | 0.191 | 443.0 | 5.19 | bound: <= 20.00 | pass |
| tokio | scan_worst_ping_ms | ms | 237.4 | 0.11 | 438.8 | 0.449 | bound: <= 20.00 | pass |
| uring | dbsize | keys | 2,000,000 | 2,000,000 | - | 2,000,000 | bound: == 2,000,000 | pass |
| uring | del_cmd_ms | ms | 132.4 | 137.1 | - | 134.3 | rel: <= 143.5 (10% over 130.4, recorded 134.3 scaled by host speed 1.03x (group)) | pass |
| uring | del_least_worst_ping_ms | ms | 0.08 | 0.1 | - | 0.41 | rel: <= 10.41 (50% over 0.41, +10) | pass |
| uring | del_probes_stalled | probes | 1 | 2 | - | 1 | info | info |
| uring | del_reply | items | 1 | 1 | - | 1 | bound: == 1 | pass |
| uring | del_worst_ping_ms | ms | 137.8 | 136.1 | - | 132.9 | info | info |
| uring | keys_cmd_ms | ms | 145.7 | 90.26 | - | 153.6 | rel: <= 164.1 (10% over 149.1, recorded 153.6 scaled by host speed 1.03x (group))<br>vs_v1_2_2: 0.62x v1.2.2 <= 0.85x | pass<br>pass |
| uring | keys_least_worst_ping_ms | ms | 0.09 | 0.13 | - | 0.95 | rel: <= 10.95 (50% over 0.95, +10) | pass |
| uring | keys_probes_stalled | probes | 1 | 1 | - | 2 | info | info |
| uring | keys_reply | items | 11,111 | 11,111 | - | 11,111 | bound: == 11,111 | pass |
| uring | keys_worst_ping_ms | ms | 134.3 | 79.28 | - | 142.8 | info | info |
| uring | scan_cmd_ms | ms | 257.0 | 62.93 | - | 112.6 | rel: <= 120.3 (10% over 109.4, recorded 112.6 scaled by host speed 1.03x (group)) | pass |
| uring | scan_least_worst_ping_ms | ms | 0.28 | 0.11 | - | 0.13 | rel: <= 10.13 (50% over 0.13, +10) | pass |
| uring | scan_probes_stalled | probes | 1 | 2 | - | 1 | info | info |
| uring | scan_reply | items | 11,111 | 11,111 | - | 11,111 | info | info |
| uring | scan_worst_ping_ms | ms | 245.8 | 51.72 | - | 101.6 | info | info |

## 05-stream-memory

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| one1m | one_field_bytes_per_entry | B | 158.4 | 6.23 | 13.09 | 6.23 | rel: <= 7.23 (5% over 6.23, +1)<br>ratio: gain over v1.2.2 25.44x >= 15.26x (60% of recorded 25.43x) | pass<br>pass |
| one1m | one_field_load_s | s | 8.1 | 7.95 | 7.91 | 8.19 | info | info |
| one1m | one_field_rss_mib | MiB | 195.6 | 21.51 | 25.36 | 20.71 | rel: <= 25.71 (5% over 20.71, +5)<br>ratio: gain over v1.2.2 9.09x >= 5.67x (60% of recorded 9.45x) | pass<br>pass |
| one1m | one_field_used_mib | MiB | 152.4 | 7.42 | 13.28 | 7.33 | info | info |
| shapes | shared5_bytes_per_entry | B | 478.4 | 14.65 | 26.15 | 14.65 | rel: <= 15.65 (5% over 14.65, +1)<br>ratio: gain over v1.2.2 32.65x >= 19.59x (60% of recorded 32.65x) | pass<br>pass |
| shapes | shared5_load_s | s | 2.96 | 2.65 | 2.68 | 2.66 | info | info |
| shapes | unique5_bytes_per_entry | B | 478.4 | 54.01 | 75.22 | 54.01 | rel: <= 56.71 (5% over 54.01, +1)<br>ratio: gain over v1.2.2 8.86x >= 5.32x (60% of recorded 8.86x) | pass<br>pass |
| shapes | unique5_load_s | s | 2.82 | 2.78 | 2.77 | 2.76 | info | info |
| shapes | val200_bytes_per_entry | B | 350.9 | 216.5 | 216.3 | 216.5 | rel: <= 227.3 (5% over 216.5, +1)<br>ratio: gain over v1.2.2 1.62x >= 0.97x (60% of recorded 1.62x) | pass<br>pass |
| shapes | val200_load_s | s | 1.5 | 1.5 | 1.5 | 1.53 | info | info |
| shapes | xadd5_rps | req/s | 327,011 | 487,805 | 469,043 | 535,332 | rel: >= 443,012 (15% under 521,190, recorded 535,332 scaled by host speed 0.97x (group)) | pass |
| shapes | xrange5_count10_rps | req/s | 85,788 | 84,199 | 131,234 | 86,381 | rel: >= 71,484 (15% under 84,099, recorded 86,381 scaled by host speed 0.97x (group)) | pass |
| shapes | xrange_one_id_rps | req/s | 467,727 | 437,445 | 438,212 | 443,262 | rel: >= 366,820 (15% under 431,553, recorded 443,262 scaled by host speed 0.97x (group)) | pass |

## 06-stream-consumers

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| pel | pending_bytes_per_entry | B | 116.5 | 121.1 | 166.6 | 122.3 | rel: <= 128.4 (5% over 122.3, +2) | pass |
| pel | xadd_other_stream_max_ms | ms | 165.0 | 0.535 | 0.511 | 0.471 | info | info |
| pel | xadd_other_stream_rps | req/s | 97,229 | 112,360 | 141,044 | 104,384 | rel: >= 72,510 (25% under 96,680, recorded 104,384 scaled by host speed 0.93x (group)) | pass |
| pel | xadd_polled_stream_max_ms | ms | 94.66 | 0.487 | 0.791 | 0.823 | info | info |
| pel | xadd_polled_stream_rps | req/s | 39,580 | 103,466 | 129,618 | 117,371 | rel: >= 81,532 (25% under 108,709, recorded 117,371 scaled by host speed 0.93x (group))<br>ratio: gain over v1.2.2 2.61x >= 1.65x (60% of recorded 2.74x) | pass<br>pass |
| pel | xinfo_consumers_ms | ms | 70.28 | 0.369 | 0.365 | 0.395 | rel: <= 0.927 (50% over 0.427, +0.5, recorded 0.395 scaled by host speed 0.93x (group))<br>ratio: gain over v1.2.2 190.31x >= 98.93x (60% of recorded 164.88x) | pass<br>pass |
| pel | xinfo_polls | calls | 14.00 | 39.00 | 32.00 | 42.00 | bound: >= 5 | pass |
| pel | xpending_c50_ms | ms | 1.63 | 0.0912 | 0.0936 | 0.11 | rel: <= 0.619 (50% over 0.119, +0.5, recorded 0.11 scaled by host speed 0.93x (group))<br>ratio: gain over v1.2.2 17.85x >= 8.76x (60% of recorded 14.60x) | pass<br>pass |
| pel | xpending_summary_ms | ms | 9.17 | 0.173 | 0.174 | 0.193 | rel: <= 0.708 (50% over 0.208, +0.5, recorded 0.193 scaled by host speed 0.93x (group))<br>ratio: gain over v1.2.2 53.04x >= 23.61x (60% of recorded 39.34x) | pass<br>pass |
| pel | xreadgroup_c50_history_ms | ms | 1.51 | 0.11 | 0.107 | 0.133 | rel: <= 0.643 (50% over 0.143, +0.5, recorded 0.133 scaled by host speed 0.93x (group))<br>ratio: gain over v1.2.2 13.77x >= 7.21x (60% of recorded 12.02x) | pass<br>pass |

## 07-save-ping

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| tokio1 | ping_during_save_max_ms | ms | 470.9 | 2.78 | 550.6 | 2.19 | bound: <= 20.00 | pass |
| tokio1 | ping_during_save_ms | ms | 340.5 | 1.54 | 528.1 | 1.48 | bound: <= 20.00 | pass |
| tokio1 | save_ms | ms | 342.9 | 340.8 | 528.6 | 339.4 | info | info |

## 08-ft-info

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| ftinfo | ft_info_polls_per_s | calls/s | 18.29 | 14,873 | - | 15,600 | info | info |
| ftinfo | ft_info_text_ms | ms | 117.6 | 0.0987 | - | 0.12 | rel: <= 0.621 (50% over 0.121, +0.5, recorded 0.12 scaled by host speed 1.00x (group))<br>ratio: gain over v1.2.2 1191.93x >= 580.04x (60% of recorded 966.74x) | pass<br>pass |
| ftinfo | ft_info_vec_ms | ms | 117.0 | 0.101 | - | 0.125 | rel: <= 0.625 (50% over 0.125, +0.5, recorded 0.125 scaled by host speed 1.00x (group))<br>ratio: gain over v1.2.2 1161.90x >= 561.09x (60% of recorded 935.15x) | pass<br>pass |
| ftinfo | hset_vec_doc_rps | req/s | 112,952 | 95,329 | - | 107,797 | rel: >= 91,461 (15% under 107,602, recorded 107,797 scaled by host speed 1.00x (group)) | pass |
| ftinfo | populate_s | s | 2.1 | 2.05 | - | 2.09 | info | info |
| ftinfo | set_rps_beside_ft_info | req/s | 88.90 | 46,022 | - | 47,430 | rel: >= 37,875 (20% under 47,344, recorded 47,430 scaled by host speed 1.00x (group)) | pass |
| ftinfo | vec_doc_bytes | B | 474.5 | 347.1 | - | 494.1 | rel: <= 518.9 (5% over 494.1, +4) | pass |

## 09-vector-sets

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| bin_768_50k | bytes_per_vector | B | 3,168 | 191.7 | 854.0 | 191.7 | rel: <= 201.3 (5% over 191.7)<br>ratio: gain over v1.2.2 16.52x >= 9.91x (60% of recorded 16.52x) | pass<br>pass |
| bin_768_50k | recall_at_10 | ratio | 1 | 0.103 | 0.105 | 0.103 | rel: >= 0.101 (2% under 0.103) | pass |
| bin_768_50k | vadd_per_s | VADD/s | 94,120 | 62,729 | 5,872 | 61,976 | info | info |
| bin_768_50k | vsim_qps | q/s | 110.4 | 1,124 | 7,575 | 750.2 | info | info |
| f32_768_10k | recall_at_10 | ratio | 1 | 1 | 1 | 1 | rel: >= 0.98 (2% under 1) | pass |
| f32_768_10k | vadd_per_s | VADD/s | 67,201 | 62,251 | 2,101 | 62,038 | rel: >= 39,012 (25% under 52,016, recorded 62,038 scaled by host speed 0.84x (group)) | pass |
| f32_768_10k | vsim_p50_ms | ms | 2.21 | 2.76 | 0.458 | 1.91 | info | info |
| f32_768_10k | vsim_qps | q/s | 888.9 | 894.5 | 6,047 | 1,116 | rel: >= 701.5 (25% under 935.3, recorded 1,116 scaled by host speed 0.84x (group)) | pass |
| q8_768_10k | recall_at_10 | ratio | 1 | 0.972 | 0.972 | 0.972 | rel: >= 0.953 (2% under 0.972) | pass |
| q8_768_10k | vadd_per_s | VADD/s | 66,073 | 57,355 | 3,818 | 52,817 | rel: >= 39,131 (25% under 52,174, recorded 52,817 scaled by host speed 0.99x (group)) | pass |
| q8_768_10k | vsim_p50_ms | ms | 2.21 | 0.779 | 0.314 | 2.07 | info | info |
| q8_768_10k | vsim_qps | q/s | 1,009 | 2,777 | 7,484 | 2,493 | rel: >= 1,870 (25% under 2,493, host speed 1.00x (own-capped))<br>ratio: gain over v1.2.2 2.75x >= 2.20x (60% of recorded 3.67x) | pass<br>pass |
| q8_768_50k | bytes_per_vector | B | 3,168 | 863.7 | 1,525 | 863.7 | rel: <= 906.9 (5% over 863.7)<br>ratio: gain over v1.2.2 3.67x >= 2.20x (60% of recorded 3.67x) | pass<br>pass |
| q8_768_50k | recall_at_10 | ratio | 1 | 0.965 | 0.963 | 0.965 | rel: >= 0.946 (2% under 0.965) | pass |
| q8_768_50k | vadd_per_s | VADD/s | 89,179 | 76,702 | 3,678 | 64,379 | info | info |
| q8_768_50k | vsim_qps | q/s | 110.6 | 351.9 | 6,528 | 118.1 | info | info |

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
| migrate | migrate_ms | ms | 0.175 | 0.121 | 0.0973 | 0.105 | info | info |
| migrate | target_connections | connections | 2,000 | 1 | 1 | 1 | bound: <= 3 | pass |

## 12-search-fanout

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| cluster3 | peer_connections_max | connections | 2,000 | 1 | - | 1 | bound: <= 20.00 | pass |
| cluster3 | peer_kept_clients_max | clients | 0 | 2 | - | 2 | info | info |
| cluster3 | searches_per_s | searches/s | 3,949 | 5,743 | - | 5,772 | rel: >= 4,321 (25% under 5,762, recorded 5,772 scaled by host speed 1.00x (host)) | pass |
| cluster3 | searches_wrong_count | searches | 0 | 0 | - | 0 | bound: == 0 | pass |

## 13-cluster-reshard

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| reshard1000 | cluster_check_failed | - | 0 | 0 | 0 | 0 | bound: <= 0 | pass |
| reshard1000 | keys_after | keys | 3,000,000 | 3,000,000 | 3,000,000 | 3,000,000 | bound: == 3,000,000 | pass |
| reshard1000 | keys_moved | keys | 183,078 | 183,078 | 183,078 | 183,078 | bound: == 183,078 | pass |
| reshard1000 | load_s | s | 24.32 | 24.55 | 24.17 | 24.21 | info | info |
| reshard1000 | reshard_s | s | 399.9 | 27.08 | 6.59 | 25.30 | rel: <= 29.15 (15% over 25.35, recorded 25.30 scaled by host speed 1.00x (host))<br>ratio: gain over v1.2.2 14.76x >= 9.05x (60% of recorded 15.09x) | pass<br>pass |

## 14-sinter-big-small

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| default | 1m_big_card | members | 1,000,000 | 1,000,000 | 1,000,000 | 1,000,000 | bound: == 1,000,000 | pass |
| default | 1m_sdiff_small_big_ms | ms | 68.79 | 1.87 | 2.5 | 1.95 | rel: <= 3.89 (50% over 1.89, +2, recorded 1.95 scaled by host speed 1.03x (group))<br>ratio: gain over v1.2.2 36.72x >= 21.50x (60% of recorded 35.83x)<br>vs_redis: 0.75x Redis <= 2x | pass<br>pass<br>pass |
| default | 1m_sdiff_small_big_reply | members | 1,999 | 1,999 | 1,999 | 1,999 | bound: == 1,999 | pass |
| default | 1m_sdiffstore_big_small_ms | ms | 273.6 | 176.5 | 305.4 | 253.6 | rel: <= 307.7 (25% over 246.2, recorded 253.6 scaled by host speed 1.03x (group)) | pass |
| default | 1m_sdiffstore_big_small_reply | members | 997,903 | 997,903 | 997,903 | 997,903 | bound: == 997,903 | pass |
| default | 1m_sinter_big_small_ms | ms | 91.25 | 2.08 | 2.77 | 2.33 | rel: <= 4.26 (50% over 2.26, +2, recorded 2.33 scaled by host speed 1.03x (group))<br>ratio: gain over v1.2.2 43.95x >= 24.66x (60% of recorded 41.11x)<br>vs_redis: 0.75x Redis <= 2x | pass<br>pass<br>pass |
| default | 1m_sinter_big_small_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| default | 1m_sinter_small_big_ms | ms | 68.44 | 2 | 2.33 | 2.09 | rel: <= 4.03 (50% over 2.03, +2, recorded 2.09 scaled by host speed 1.03x (group))<br>ratio: gain over v1.2.2 34.19x >= 19.93x (60% of recorded 33.21x)<br>vs_redis: 0.86x Redis <= 2x | pass<br>pass<br>pass |
| default | 1m_sinter_small_big_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| default | 1m_sintercard_limit_ms | ms | 89.10 | 0.0545 | 0.0585 | 0.0547 | rel: <= 2.05 (50% over 0.0531, +2, recorded 0.0547 scaled by host speed 1.03x (group)) | pass |
| default | 1m_sintercard_limit_reply | members | 100.0 | 100.0 | 100.0 | 100.0 | bound: == 100.0 | pass |
| default | 1m_sintercard_ms | ms | 88.75 | 0.218 | 0.518 | 0.184 | rel: <= 2.18 (50% over 0.179, +2, recorded 0.184 scaled by host speed 1.03x (group)) | pass |
| default | 1m_sintercard_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| default | 1m_sinterstore_ms | ms | 90.95 | 0.312 | 0.872 | 0.3 | rel: <= 2.29 (50% over 0.291, +2, recorded 0.3 scaled by host speed 1.03x (group)) | pass |
| default | 1m_sinterstore_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| default | 5m_big_card | members | 5,000,000 | 5,000,000 | 5,000,000 | 5,000,000 | bound: == 5,000,000 | pass |
| default | 5m_sdiff_small_big_ms | ms | 438.3 | 2.02 | 3.84 | 2.03 | rel: <= 3.97 (50% over 1.97, +2, recorded 2.03 scaled by host speed 1.03x (group))<br>ratio: gain over v1.2.2 216.53x >= 132.00x (60% of recorded 220.01x)<br>vs_redis: 0.53x Redis <= 2x | pass<br>pass<br>pass |
| default | 5m_sdiff_small_big_reply | members | 1,998 | 1,998 | 1,998 | 1,998 | bound: == 1,998 | pass |
| default | 5m_sdiffstore_big_small_ms | ms | 1,601 | 1,147 | 1,831 | 1,424 | rel: <= 1,728 (25% over 1,383, recorded 1,424 scaled by host speed 1.03x (group)) | pass |
| default | 5m_sdiffstore_big_small_reply | members | 4,997,902 | 4,997,902 | 4,997,902 | 4,997,902 | bound: == 4,997,902 | pass |
| default | 5m_sinter_big_small_ms | ms | 516.7 | 2.42 | 3.71 | 2.22 | rel: <= 4.15 (50% over 2.15, +2, recorded 2.22 scaled by host speed 1.03x (group))<br>ratio: gain over v1.2.2 213.49x >= 145.00x (60% of recorded 241.66x)<br>vs_redis: 0.65x Redis <= 2x | pass<br>pass<br>pass |
| default | 5m_sinter_big_small_reply | members | 2,098 | 2,098 | 2,098 | 2,098 | bound: == 2,098 | pass |
| default | 5m_sinter_small_big_ms | ms | 437.6 | 2.12 | 3.59 | 2.06 | rel: <= 4 (50% over 2, +2, recorded 2.06 scaled by host speed 1.03x (group))<br>ratio: gain over v1.2.2 206.56x >= 130.08x (60% of recorded 216.80x)<br>vs_redis: 0.59x Redis <= 2x | pass<br>pass<br>pass |
| default | 5m_sinter_small_big_reply | members | 2,098 | 2,098 | 2,098 | 2,098 | bound: == 2,098 | pass |
| default | 5m_sintercard_limit_ms | ms | 514.4 | 0.0559 | 0.11 | 0.0539 | rel: <= 2.05 (50% over 0.0523, +2, recorded 0.0539 scaled by host speed 1.03x (group)) | pass |
| default | 5m_sintercard_limit_reply | members | 100.0 | 100.0 | 100.0 | 100.0 | bound: == 100.0 | pass |
| default | 5m_sintercard_ms | ms | 518.6 | 0.288 | 1.58 | 0.223 | rel: <= 2.22 (50% over 0.216, +2, recorded 0.223 scaled by host speed 1.03x (group)) | pass |
| default | 5m_sintercard_reply | members | 2,098 | 2,098 | 2,098 | 2,098 | bound: == 2,098 | pass |
| default | 5m_sinterstore_ms | ms | 517.4 | 0.532 | 2.43 | 0.476 | rel: <= 2.46 (50% over 0.462, +2, recorded 0.476 scaled by host speed 1.03x (group)) | pass |
| default | 5m_sinterstore_reply | members | 2,098 | 2,098 | 2,098 | 2,098 | bound: == 2,098 | pass |
| n20m | 20m_big_card | members | 20,000,000 | 20,000,000 | 20,000,000 | 20,000,000 | bound: == 20,000,000 | pass |
| n20m | 20m_sdiff_small_big_ms | ms | 2,318 | 2.03 | 4.5 | 2.01 | rel: <= 3.91 (50% over 1.91, +2, recorded 2.01 scaled by host speed 1.05x (group))<br>ratio: gain over v1.2.2 1139.42x >= 733.04x (60% of recorded 1221.73x)<br>vs_redis: 0.45x Redis <= 2x | pass<br>pass<br>pass |
| n20m | 20m_sdiff_small_big_reply | members | 1,999 | 1,999 | 1,999 | 1,999 | bound: == 1,999 | pass |
| n20m | 20m_sdiffstore_big_small_ms | ms | 7,538 | 5,491 | 7,930 | 6,899 | rel: <= 8,177 (25% over 6,542, recorded 6,899 scaled by host speed 1.05x (group)) | pass |
| n20m | 20m_sdiffstore_big_small_reply | members | 19,997,903 | 19,997,903 | 19,997,903 | 19,997,903 | bound: == 19,997,903 | pass |
| n20m | 20m_sinter_big_small_ms | ms | 2,443 | 2.34 | 3.99 | 2.29 | rel: <= 4.17 (50% over 2.17, +2, recorded 2.29 scaled by host speed 1.05x (group))<br>ratio: gain over v1.2.2 1042.82x >= 674.97x (60% of recorded 1124.96x)<br>vs_redis: 0.59x Redis <= 2x | pass<br>pass<br>pass |
| n20m | 20m_sinter_big_small_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| n20m | 20m_sinter_small_big_ms | ms | 2,318 | 2.18 | 3.93 | 2.1 | rel: <= 3.99 (50% over 1.99, +2, recorded 2.1 scaled by host speed 1.05x (group))<br>ratio: gain over v1.2.2 1062.55x >= 694.18x (60% of recorded 1156.97x)<br>vs_redis: 0.56x Redis <= 2x | pass<br>pass<br>pass |
| n20m | 20m_sinter_small_big_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| n20m | 20m_sintercard_limit_ms | ms | 2,437 | 0.0956 | 0.118 | 0.0569 | rel: <= 2.05 (50% over 0.054, +2, recorded 0.0569 scaled by host speed 1.05x (group)) | pass |
| n20m | 20m_sintercard_limit_reply | members | 100.0 | 100.0 | 100.0 | 100.0 | bound: == 100.0 | pass |
| n20m | 20m_sintercard_ms | ms | 2,440 | 0.383 | 1.89 | 0.296 | rel: <= 2.28 (50% over 0.281, +2, recorded 0.296 scaled by host speed 1.05x (group)) | pass |
| n20m | 20m_sintercard_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| n20m | 20m_sinterstore_ms | ms | 2,439 | 0.496 | 2.71 | 0.381 | rel: <= 2.36 (50% over 0.361, +2, recorded 0.381 scaled by host speed 1.05x (group)) | pass |
| n20m | 20m_sinterstore_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| sunion | sunion_ms | ms | 1,266 | 1,180 | 1,278 | 1,331 | rel: <= 1,607 (25% over 1,285, recorded 1,331 scaled by host speed 1.04x (host)) | pass |
| sunion | sunion_reply | members | 1,001,999 | 1,001,999 | 1,001,999 | 1,001,999 | bound: == 1,001,999 | pass |
| sunion | sunion_worst_sadd_ms | ms | 330.5 | 8.57 | 309.3 | 0.482 | bound: <= 20.00 | pass |

## 15-brpop-many-keys

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| c1000 | blocked_after | clients | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| c1000 | jobs_per_s | jobs/s | 375.0 | 1,915 | 1,567 | 1,934 | rel: >= 1,741 (10% under 1,934)<br>ratio: gain over v1.2.2 5.11x >= 3.06x (60% of recorded 5.10x)<br>vs_redis: 0.82x Redis <= 1.5x | pass<br>pass<br>pass |
| c1000 | lost | jobs | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| c1000 | ping_max_ms | ms | 1,261 | 24.69 | 50.32 | 37.72 | rel: <= 75.44 (100% over 37.72, Redis 50.32 + 10) | pass |
| c1000 | ping_p99_ms | ms | 492.4 | 6.66 | 27.89 | 5.21 | info | info |
| c1000x1 | blocked_after | clients | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| c1000x1 | jobs_per_s | jobs/s | 1,939 | 1,951 | 1,839 | 1,950 | rel: >= 1,755 (10% under 1,950) | pass |
| c1000x1 | lost | jobs | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| c1000x1 | ping_max_ms | ms | 4.67 | 7.05 | 10.83 | 1.91 | rel: <= 20.83 (50% over 1.91, Redis 10.83 + 10) | pass |
| c1000x1 | ping_p99_ms | ms | 0.718 | 0.7 | 6.16 | 0.407 | info | info |
| c4000 | blocked_after | clients | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| c4000 | jobs_per_s | jobs/s | 6.13 | 1,900 | 941.4 | 1,924 | rel: >= 1,732 (10% under 1,924)<br>vs_redis: 0.50x Redis <= 1.5x | pass<br>pass |
| c4000 | lost | jobs | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| c4000 | ping_max_ms | ms | 16,050 | 33.23 | 215.5 | 72.41 | rel: <= 225.5 (50% over 72.41, Redis 215.5 + 10) | pass |
| c4000 | ping_p99_ms | ms | 14,427 | 7.51 | 102.3 | 4.99 | info | info |

## 16-random-sample

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| n100 | bad_replies | replies | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| n100 | hlen | fields | 100.0 | 100.0 | 100.0 | 100.0 | bound: == 100.0 | pass |
| n100 | hrandfield_10_us | us | 75.30 | 66.02 | 77.13 | 76.54 | rel: <= 133.1 (50% over 88.74, +20, recorded 76.54 scaled by host speed 0.86x (group)) | pass |
| n100 | hrandfield_10_wv_us | us | 70.72 | 79.67 | 83.69 | 67.69 | rel: <= 117.7 (50% over 78.49, +20, recorded 67.69 scaled by host speed 0.86x (group)) | pass |
| n100 | hrandfield_1_us | us | 57.31 | 56.25 | 60.77 | 69.09 | rel: <= 120.2 (50% over 80.11, +20, recorded 69.09 scaled by host speed 0.86x (group)) | pass |
| n100 | hrandfield_m10_us | us | 73.35 | 72.62 | 69.42 | 58.77 | rel: <= 102.2 (50% over 68.14, +20, recorded 58.77 scaled by host speed 0.86x (group)) | pass |
| n100 | hrandfield_m1_us | us | 61.82 | 55.99 | 51.94 | 51.93 | rel: <= 90.32 (50% over 60.22, +20, recorded 51.93 scaled by host speed 0.86x (group)) | pass |
| n100 | hrandfield_none_us | us | 52.76 | 51.72 | 64.09 | 48.71 | rel: <= 84.72 (50% over 56.48, +20, recorded 48.71 scaled by host speed 0.86x (group)) | pass |
| n100 | zcard | members | 100.0 | 100.0 | 100.0 | 100.0 | bound: == 100.0 | pass |
| n100 | zrandmember_10_us | us | 80.87 | 73.52 | 62.59 | 59.50 | rel: <= 103.5 (50% over 68.99, +20, recorded 59.50 scaled by host speed 0.86x (group)) | pass |
| n100 | zrandmember_10_ws_us | us | 75.71 | 88.22 | 85.06 | 68.01 | rel: <= 102.0 (50% over 68.01, +20, host speed 1.00x (own-capped)) | pass |
| n100 | zrandmember_1_us | us | 63.81 | 54.45 | 66.26 | 51.35 | rel: <= 89.32 (50% over 59.55, +20, recorded 51.35 scaled by host speed 0.86x (group)) | pass |
| n100 | zrandmember_m10_us | us | 68.85 | 72.72 | 62.56 | 58.91 | rel: <= 102.5 (50% over 68.31, +20, recorded 58.91 scaled by host speed 0.86x (group)) | pass |
| n100 | zrandmember_m1_us | us | 67.51 | 65.95 | 61.28 | 51.64 | rel: <= 89.82 (50% over 59.88, +20, recorded 51.64 scaled by host speed 0.86x (group)) | pass |
| n100 | zrandmember_none_us | us | 58.86 | 56.98 | 48.91 | 48.14 | rel: <= 83.73 (50% over 55.82, +20, recorded 48.14 scaled by host speed 0.86x (group)) | pass |
| n10k | bad_replies | replies | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| n10k | hlen | fields | 10,000 | 10,000 | 10,000 | 10,000 | bound: == 10,000 | pass |
| n10k | hrandfield_10_us | us | 89.10 | 61.89 | 61.33 | 63.16 | rel: <= 82.43 (50% over 54.95, +20, recorded 63.16 scaled by host speed 1.15x (group)) | pass |
| n10k | hrandfield_10_wv_us | us | 75.70 | 72.44 | 70.57 | 75.70 | rel: <= 98.80 (50% over 65.87, +20, recorded 75.70 scaled by host speed 1.15x (group)) | pass |
| n10k | hrandfield_1_us | us | 66.31 | 67.54 | 50.15 | 69.71 | rel: <= 90.98 (50% over 60.65, +20, recorded 69.71 scaled by host speed 1.15x (group)) | pass |
| n10k | hrandfield_m10_us | us | 65.42 | 60.86 | 59.05 | 79.38 | rel: <= 103.6 (50% over 69.07, +20, recorded 79.38 scaled by host speed 1.15x (group)) | pass |
| n10k | hrandfield_m1_us | us | 55.89 | 52.18 | 53.99 | 69.82 | rel: <= 91.12 (50% over 60.75, +20, recorded 69.82 scaled by host speed 1.15x (group)) | pass |
| n10k | hrandfield_none_us | us | 52.79 | 54.30 | 59.49 | 47.95 | rel: <= 62.59 (50% over 41.73, +20, recorded 47.95 scaled by host speed 1.15x (group)) | pass |
| n10k | zcard | members | 10,000 | 10,000 | 10,000 | 10,000 | bound: == 10,000 | pass |
| n10k | zrandmember_10_us | us | 86.21 | 68.56 | 60.88 | 71.55 | rel: <= 93.39 (50% over 62.26, +20, recorded 71.55 scaled by host speed 1.15x (group)) | pass |
| n10k | zrandmember_10_ws_us | us | 106.7 | 89.93 | 80.99 | 78.67 | rel: <= 102.7 (50% over 68.45, +20, recorded 78.67 scaled by host speed 1.15x (group)) | pass |
| n10k | zrandmember_1_us | us | 78.99 | 56.08 | 49.71 | 74.70 | rel: <= 97.49 (50% over 64.99, +20, recorded 74.70 scaled by host speed 1.15x (group)) | pass |
| n10k | zrandmember_m10_us | us | 84.51 | 67.84 | 62.20 | 74.28 | rel: <= 96.94 (50% over 64.63, +20, recorded 74.28 scaled by host speed 1.15x (group)) | pass |
| n10k | zrandmember_m1_us | us | 74.52 | 54.92 | 50.30 | 64.89 | rel: <= 84.69 (50% over 56.46, +20, recorded 64.89 scaled by host speed 1.15x (group)) | pass |
| n10k | zrandmember_none_us | us | 71.07 | 58.23 | 50.01 | 60.13 | rel: <= 78.48 (50% over 52.32, +20, recorded 60.13 scaled by host speed 1.15x (group)) | pass |
| n1m | bad_replies | replies | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| n1m | hlen | fields | 1,000,000 | 1,000,000 | 1,000,000 | 1,000,000 | bound: == 1,000,000 | pass |
| n1m | hrandfield_10_us | us | 1,197 | 74.73 | 69.30 | 62.89 | rel: <= 98.10 (50% over 65.40, +20, recorded 62.89 scaled by host speed 0.96x (group))<br>ratio: gain over v1.2.2 16.01x >= 10.01x (60% of recorded 16.68x)<br>vs_redis: 1.08x Redis <= 2x | pass<br>pass<br>pass |
| n1m | hrandfield_10_wv_us | us | 1,407 | 84.91 | 99.71 | 69.82 | rel: <= 108.9 (50% over 72.61, +20, recorded 69.82 scaled by host speed 0.96x (group))<br>ratio: gain over v1.2.2 16.58x >= 9.54x (60% of recorded 15.91x)<br>vs_redis: 0.85x Redis <= 2x | pass<br>pass<br>pass |
| n1m | hrandfield_1_us | us | 1,192 | 64.70 | 51.21 | 50.94 | rel: <= 79.47 (50% over 52.98, +20, recorded 50.94 scaled by host speed 0.96x (group))<br>ratio: gain over v1.2.2 18.43x >= 12.91x (60% of recorded 21.52x)<br>vs_redis: 1.26x Redis <= 2x | pass<br>pass<br>pass |
| n1m | hrandfield_m10_us | us | 708.7 | 75.18 | 67.13 | 59.96 | rel: <= 93.53 (50% over 62.35, +20, recorded 59.96 scaled by host speed 0.96x (group))<br>ratio: gain over v1.2.2 9.43x >= 7.01x (60% of recorded 11.68x)<br>vs_redis: 1.12x Redis <= 2x | pass<br>pass<br>pass |
| n1m | hrandfield_m1_us | us | 695.4 | 64.12 | 54.05 | 51.03 | rel: <= 79.60 (50% over 53.07, +20, recorded 51.03 scaled by host speed 0.96x (group))<br>ratio: gain over v1.2.2 10.85x >= 8.06x (60% of recorded 13.43x)<br>vs_redis: 1.19x Redis <= 2x | pass<br>pass<br>pass |
| n1m | hrandfield_none_us | us | 68.79 | 69.27 | 51.29 | 48.56 | rel: <= 75.75 (50% over 50.50, +20, recorded 48.56 scaled by host speed 0.96x (group))<br>vs_redis: 1.35x Redis <= 2x | pass<br>pass |
| n1m | zcard | members | 1,000,000 | 1,000,000 | 1,000,000 | 1,000,000 | bound: == 1,000,000 | pass |
| n1m | zrandmember_10_us | us | 6,704 | 1,773 | 65.88 | 1,852 | rel: <= 2,890 (50% over 1,926, +20, recorded 1,852 scaled by host speed 0.96x (group))<br>ratio: gain over v1.2.2 3.78x >= 2.09x (60% of recorded 3.48x) | pass<br>pass |
| n1m | zrandmember_10_ws_us | us | 6,561 | 1,773 | 74.44 | 1,820 | rel: <= 2,839 (50% over 1,893, +20, recorded 1,820 scaled by host speed 0.96x (group))<br>ratio: gain over v1.2.2 3.70x >= 2.16x (60% of recorded 3.60x) | pass<br>pass |
| n1m | zrandmember_1_us | us | 6,630 | 1,053 | 52.39 | 995.2 | rel: <= 1,553 (50% over 1,035, +20, recorded 995.2 scaled by host speed 0.96x (group))<br>ratio: gain over v1.2.2 6.30x >= 3.96x (60% of recorded 6.59x) | pass<br>pass |
| n1m | zrandmember_m10_us | us | 5,892 | 1,790 | 64.04 | 1,803 | rel: <= 2,813 (50% over 1,875, +20, recorded 1,803 scaled by host speed 0.96x (group))<br>ratio: gain over v1.2.2 3.29x >= 1.82x (60% of recorded 3.03x) | pass<br>pass |
| n1m | zrandmember_m1_us | us | 5,813 | 1,045 | 52.52 | 1,040 | rel: <= 1,622 (50% over 1,081, +20, recorded 1,040 scaled by host speed 0.96x (group))<br>ratio: gain over v1.2.2 5.56x >= 3.23x (60% of recorded 5.38x) | pass<br>pass |
| n1m | zrandmember_none_us | us | 5,821 | 1,121 | 48.12 | 1,121 | rel: <= 1,749 (50% over 1,166, +20, recorded 1,121 scaled by host speed 0.96x (group))<br>ratio: gain over v1.2.2 5.19x >= 3.00x (60% of recorded 5.00x) | pass<br>pass |

## 17-set-memory

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| ints | bytes_per_member_10 | B | 67.58 | 17.18 | 8.76 | 17.18 | rel: <= 18.18 (5% over 17.18, +1)<br>ratio: gain over v1.2.2 3.93x >= 2.36x (60% of recorded 3.93x) | pass<br>pass |
| ints | bytes_per_member_100 | B | 47.43 | 3.75 | 2.81 | 3.75 | rel: <= 4.75 (5% over 3.75, +1)<br>ratio: gain over v1.2.2 12.65x >= 7.59x (60% of recorded 12.65x) | pass<br>pass |
| ints | bytes_per_member_1000 | B | 58.99 | 58.99 | 22.05 | 58.99 | rel: <= 61.93 (5% over 58.99, +1) | pass |
| ints | bytes_per_member_100000 | B | 46.28 | 46.28 | 28.80 | 46.28 | rel: <= 48.60 (5% over 46.28, +1) | pass |
| ints | encoding_10 | code | 1 | 1 | 1 | 1 | bound: == 1 | pass |
| ints | encoding_100 | code | 1 | 1 | 1 | 1 | bound: == 1 | pass |
| ints | encoding_1000 | code | 3 | 3 | 3 | 3 | bound: == 3 | pass |
| ints | encoding_100000 | code | 3 | 3 | 3 | 3 | bound: == 3 | pass |
| ints | load_100000_s | s | 1.02 | 1.07 | 1.18 | 1.66 | info | info |
| ints | load_1000_s | s | 1.01 | 1.01 | 1.03 | 1.96 | info | info |
| ints | load_100_s | s | 0.969 | 0.914 | 0.959 | 1.22 | info | info |
| ints | load_10_s | s | 1.48 | 1.41 | 1.43 | 2.8 | info | info |
| strings | bytes_per_member_10 | B | 75.58 | 34.18 | 24.76 | 34.18 | rel: <= 35.89 (5% over 34.18, +1)<br>ratio: gain over v1.2.2 2.21x >= 1.33x (60% of recorded 2.21x) | pass<br>pass |
| strings | bytes_per_member_100 | B | 55.43 | 20.33 | 18.49 | 20.33 | rel: <= 21.34 (5% over 20.33, +1)<br>ratio: gain over v1.2.2 2.73x >= 1.64x (60% of recorded 2.73x) | pass<br>pass |
| strings | bytes_per_member_1000 | B | 66.99 | 66.99 | 30.12 | 66.99 | rel: <= 70.33 (5% over 66.99, +1) | pass |
| strings | bytes_per_member_100000 | B | 54.28 | 54.28 | 36.83 | 54.28 | rel: <= 57.00 (5% over 54.28, +1) | pass |
| strings | encoding_10 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| strings | encoding_100 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| strings | encoding_1000 | code | 3 | 3 | 3 | 3 | bound: == 3 | pass |
| strings | encoding_100000 | code | 3 | 3 | 3 | 3 | bound: == 3 | pass |
| strings | load_100000_s | s | 1.15 | 1.17 | 1.29 | 2.54 | info | info |
| strings | load_1000_s | s | 1.11 | 1.13 | 1.12 | 2.41 | info | info |
| strings | load_100_s | s | 1.12 | 1.07 | 1.39 | 2.37 | info | info |
| strings | load_10_s | s | 1.53 | 1.49 | 1.55 | 3.25 | info | info |

## 19-hash-memory

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| plain | bytes_per_field_10 | B | 162.4 | 71.38 | 56.76 | 71.38 | rel: <= 74.95 (5% over 71.38, +1)<br>ratio: gain over v1.2.2 2.27x >= 1.36x (60% of recorded 2.27x) | pass<br>pass |
| plain | bytes_per_field_100 | B | 129.2 | 55.77 | 51.76 | 55.77 | rel: <= 58.56 (5% over 55.77, +1)<br>ratio: gain over v1.2.2 2.32x >= 1.39x (60% of recorded 2.32x) | pass<br>pass |
| plain | bytes_per_field_1000 | B | 166.6 | 166.6 | 69.96 | 166.6 | rel: <= 175.0 (5% over 166.6, +1) | pass |
| plain | bytes_per_field_100000 | B | 128.8 | 128.8 | 76.81 | 128.8 | rel: <= 135.3 (5% over 128.8, +1) | pass |
| plain | encoding_10 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| plain | encoding_100 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| plain | encoding_1000 | code | 3 | 3 | 3 | 3 | bound: == 3 | pass |
| plain | encoding_100000 | code | 3 | 3 | 3 | 3 | bound: == 3 | pass |
| plain | load_100000_s | s | 2.43 | 2.45 | 2.57 | 2.57 | info | info |
| plain | load_1000_s | s | 2.37 | 2.39 | 3.12 | 2.5 | info | info |
| plain | load_100_s | s | 2.36 | 2.37 | 2.32 | 2.39 | info | info |
| plain | load_10_s | s | 2.7 | 2.87 | 2.59 | 2.8 | info | info |
| ttl | bytes_per_field_10 | B | 192.2 | 101.2 | 72.13 | 101.2 | rel: <= 106.3 (5% over 101.2, +1)<br>ratio: gain over v1.2.2 1.90x >= 1.14x (60% of recorded 1.90x) | pass<br>pass |
| ttl | bytes_per_field_100 | B | 131.5 | 58.11 | 62.26 | 58.11 | rel: <= 61.01 (5% over 58.11, +1)<br>ratio: gain over v1.2.2 2.26x >= 1.36x (60% of recorded 2.26x) | pass<br>pass |
| ttl | bytes_per_field_1000 | B | 166.9 | 166.9 | 70.17 | 166.9 | rel: <= 175.2 (5% over 166.9, +1) | pass |
| ttl | bytes_per_field_100000 | B | 128.9 | 128.9 | 76.80 | 128.9 | rel: <= 135.3 (5% over 128.9, +1) | pass |
| ttl | encoding_10 | code | 4 | 4 | 4 | 4 | bound: == 4 | pass |
| ttl | encoding_100 | code | 4 | 4 | 4 | 4 | bound: == 4 | pass |
| ttl | encoding_1000 | code | 3 | 3 | 3 | 3 | bound: == 3 | pass |
| ttl | encoding_100000 | code | 3 | 3 | 3 | 3 | bound: == 3 | pass |
| ttl | load_100000_s | s | 2.46 | 2.5 | 2.62 | 2.56 | info | info |
| ttl | load_1000_s | s | 2.37 | 2.42 | 3.14 | 2.5 | info | info |
| ttl | load_100_s | s | 2.49 | 2.45 | 2.48 | 2.55 | info | info |
| ttl | load_10_s | s | 3.61 | 3.55 | 3.5 | 3.74 | info | info |

## 21-list-memory

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| packed | bytes_per_element_10 | B | 75.58 | 48.38 | 43.96 | 48.38 | rel: <= 50.80 (5% over 48.38, +1)<br>ratio: gain over v1.2.2 1.56x >= 0.94x (60% of recorded 1.56x) | pass<br>pass |
| packed | bytes_per_element_100 | B | 65.19 | 38.39 | 36.41 | 38.39 | rel: <= 40.31 (5% over 38.39, +1)<br>ratio: gain over v1.2.2 1.70x >= 1.02x (60% of recorded 1.70x) | pass<br>pass |
| packed | bytes_per_element_1000 | B | 64.12 | 36.69 | 34.61 | 36.69 | rel: <= 38.52 (5% over 36.69, +1)<br>ratio: gain over v1.2.2 1.75x >= 1.05x (60% of recorded 1.75x) | pass<br>pass |
| packed | bytes_per_element_100000 | B | 68.48 | 36.66 | 34.31 | 36.66 | rel: <= 38.49 (5% over 36.66, +1)<br>ratio: gain over v1.2.2 1.87x >= 1.12x (60% of recorded 1.87x) | pass<br>pass |
| packed | encoding_10 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| packed | encoding_100 | code | 2 | 5 | 2 | 5 | bound: == 5 | pass |
| packed | encoding_1000 | code | 5 | 5 | 5 | 5 | bound: == 5 | pass |
| packed | encoding_100000 | code | 5 | 5 | 5 | 5 | bound: == 5 | pass |
| packed | load_100000_s | s | 1.1 | 1.1 | 1.13 | 2.27 | info | info |
| packed | load_1000_s | s | 1.15 | 1.08 | 1.12 | 2.37 | info | info |
| packed | load_100_s | s | 1.11 | 1.08 | 1.15 | 2.66 | info | info |
| packed | load_10_s | s | 1.51 | 1.49 | 1.52 | 3.44 | info | info |
| plain | bytes_per_element_10 | B | 77.18 | 77.98 | 44.00 | 77.98 | rel: <= 81.88 (5% over 77.98, +1) | pass |
| plain | bytes_per_element_100 | B | 65.35 | 65.43 | 36.40 | 65.43 | rel: <= 68.70 (5% over 65.43, +1) | pass |
| plain | bytes_per_element_1000 | B | 64.15 | 64.16 | 34.60 | 64.16 | rel: <= 67.37 (5% over 64.16, +1) | pass |
| plain | bytes_per_element_100000 | B | 68.52 | 68.52 | 34.31 | 68.52 | rel: <= 71.95 (5% over 68.52, +1) | pass |
| plain | encoding_10 | code | 2 | 5 | 2 | 5 | bound: == 5 | pass |
| plain | encoding_100 | code | 2 | 5 | 2 | 5 | bound: == 5 | pass |
| plain | encoding_1000 | code | 5 | 5 | 5 | 5 | bound: == 5 | pass |
| plain | encoding_100000 | code | 5 | 5 | 5 | 5 | bound: == 5 | pass |
| plain | load_100000_s | s | 1.09 | 1.11 | 1.14 | 2.68 | info | info |
| plain | load_1000_s | s | 1.12 | 1.12 | 1.16 | 2.52 | info | info |
| plain | load_100_s | s | 1.24 | 1.23 | 1.25 | 2.65 | info | info |
| plain | load_10_s | s | 2.91 | 2.8 | 2.88 | 6.22 | info | info |

## 22-scan-large-count

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| t4 | dbsize | keys | 40,000 | 40,000 | 40,000 | 40,000 | bound: == 40,000 | pass |
| t4 | ping_max_ms | ms | 5.24 | 3.17 | 6.45 | 5.54 | info | info |
| t4 | ping_p99_ms | ms | 1.8 | 0.58 | 3.34 | 0.542 | rel: <= 1.54 (50% over 0.542, +1) | pass |
| t4 | scan_call_us | us | 664.9 | 138.7 | 734.9 | 144.1 | rel: <= 166.1 (25% over 132.9, +30, recorded 144.1 scaled by host speed 1.08x (group)) | pass |
| t4 | scan_calls | calls | 4 | 4 | 4 | 4 | info | info |
| t4 | scan_returned | keys | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| t4 | scan_server_us | us | 579.4 | 73.28 | 644.8 | 75.50 | rel: <= 99.64 (25% over 69.64, +30, recorded 75.50 scaled by host speed 1.08x (group))<br>ratio: gain over v1.2.2 7.91x >= 4.99x (60% of recorded 8.32x) | pass<br>pass |

## 23-pubsub-pattern-fanout

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| channel | delivered_per_s | msg/s | 1,443,375 | 1,458,510 | 1,419,790 | 1,468,950 | rel: >= 1,369,243 (10% under 1,521,381, recorded 1,468,950 scaled by host speed 1.04x (host))<br>vs_redis: 0.97x Redis <= 1.1x | pass<br>pass |
| channel | delivery_p99_ms | ms | 0.847 | 0.829 | 0.87 | 0.685 | rel: <= 1.68 (50% over 0.685, +1) | pass |
| channel | lost | messages | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| channel | published_per_s | req/s | 90,266 | 91,213 | 88,797 | 91,870 | info | info |
| pattern | delivered_per_s | msg/s | 1,260,010 | 1,454,239 | 1,366,830 | 1,467,643 | rel: >= 1,368,025 (10% under 1,520,028, recorded 1,467,643 scaled by host speed 1.04x (host))<br>vs_redis: 0.94x Redis <= 1.1x | pass<br>pass |
| pattern | delivery_p99_ms | ms | 0.808 | 1.01 | 0.936 | 0.765 | rel: <= 1.77 (50% over 0.765, +1) | pass |
| pattern | lost | messages | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| pattern | published_per_s | req/s | 77,821 | 89,996 | 84,483 | 90,840 | info | info |

## 24-snapshot-size

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| coll-default | keys | keys | 350,000 | 350,000 | 350,000 | 350,000 | info | info |
| coll-default | load_ms | ms | 1,104 | 1,005 | 506.4 | 1,004 | vs_v1_2_2: 0.91x v1.2.2 <= 1.5x | pass |
| coll-default | save_ms | ms | 997.2 | 752.0 | 1,056 | 803.0 | vs_v1_2_2: 0.75x v1.2.2 <= 2.5x | pass |
| coll-default | snapshot_mb | MB | 107.5 | 107.5 | 84.11 | 107.5 | vs_v1_2_2: 1.00x v1.2.2 <= 1.01x | pass |
| coll-yes | keys | keys | 350,000 | 350,000 | 350,000 | 350,000 | info | info |
| coll-yes | load_ms | ms | 1,104 | 1,104 | 504.8 | 1,205 | vs_v1_2_2: 1.00x v1.2.2 <= 1.5x | pass |
| coll-yes | save_ms | ms | 754.4 | 482.4 | 913.9 | 476.1 | vs_v1_2_2: 0.64x v1.2.2 <= 2.5x | pass |
| coll-yes | snapshot_mb | MB | 107.5 | 46.09 | 84.11 | 46.09 | ratio: gain over v1.2.2 2.33x >= 1.40x (60% of recorded 2.33x) | pass |
| issue-default | keys | keys | 6,000,000 | 6,000,000 | 6,000,000 | 6,000,000 | info | info |
| issue-default | load_ms | ms | 7,125 | 7,124 | 5,237 | 7,021 | vs_v1_2_2: 1.00x v1.2.2 <= 1.5x | pass |
| issue-default | save_ms | ms | 6,054 | 6,101 | 6,862 | 6,102 | vs_v1_2_2: 1.01x v1.2.2 <= 2.5x | pass |
| issue-default | snapshot_mb | MB | 1,007 | 1,007 | 213.8 | 1,007 | vs_v1_2_2: 1.00x v1.2.2 <= 1.01x | pass |
| issue-yes | keys | keys | 6,000,000 | 6,000,000 | 6,000,000 | 6,000,000 | info | info |
| issue-yes | load_ms | ms | 7,323 | 7,322 | 5,360 | 7,325 | vs_v1_2_2: 1.00x v1.2.2 <= 1.5x | pass |
| issue-yes | save_ms | ms | 6,079 | 1,442 | 6,952 | 1,375 | vs_v1_2_2: 0.24x v1.2.2 <= 2.5x | pass |
| issue-yes | snapshot_mb | MB | 1,007 | 49.47 | 213.8 | 49.47 | ratio: gain over v1.2.2 20.35x >= 12.21x (60% of recorded 20.35x)<br>vs_redis: 0.23x Redis <= 1.5x | pass<br>pass |

## 25-centrifuge-scripts-publish

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| publish | publish_8x16_per_s | calls/s | 1,331,210 | 1,307,144 | - | 1,554,222 | rel: >= 1,228,802 (10% under 1,365,336, recorded 1,554,222 scaled by host speed 0.88x (host))<br>vs_v1_2_2: 1.02x v1.2.2 <= 1.08x | pass<br>pass |
| publish | publish_p16_per_s | calls/s | 363,170 | 373,568 | - | 382,411 | rel: >= 302,342 (10% under 335,936, recorded 382,411 scaled by host speed 0.88x (host))<br>vs_v1_2_2: 0.97x v1.2.2 <= 1.05x | pass<br>pass |

## 27-ratelimit-restarted

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| t2 | checks_per_s_bg0k | checks/s | 31,291 | 31,454 | 31,252 | 31,168 | rel: >= 28,271 (10% under 31,412, recorded 31,168 scaled by host speed 1.01x (group))<br>vs_redis: 0.99x Redis <= 1.05x | pass<br>pass |
| t2 | checks_per_s_bg100k | checks/s | 28,624 | 31,319 | 31,797 | 31,399 | rel: >= 28,480 (10% under 31,645, recorded 31,399 scaled by host speed 1.01x (group))<br>vs_redis: 1.02x Redis <= 1.05x | pass<br>pass |
| t2 | checks_per_s_bg50k | checks/s | 30,660 | 31,606 | 31,817 | 31,553 | rel: >= 28,620 (10% under 31,800, recorded 31,553 scaled by host speed 1.01x (group))<br>vs_redis: 1.01x Redis <= 1.05x | pass<br>pass |
| t2 | p99_ms_bg0k | ms | 0.367 | 0.222 | 0.57 | 0.218 | rel: <= 0.718 (100% over 0.218, +0.5) | pass |
| t2 | p99_ms_bg100k | ms | 2.94 | 0.193 | 0.104 | 0.19 | rel: <= 0.69 (100% over 0.19, +0.5)<br>ratio: gain over v1.2.2 15.23x >= 19.32x (60% of recorded 32.20x) | **FAIL** |
| t2 | p99_ms_bg50k | ms | 1.21 | 0.138 | 0.094 | 0.145 | rel: <= 0.645 (100% over 0.145, +0.5)<br>ratio: gain over v1.2.2 8.75x >= 6.45x (60% of recorded 10.75x) | pass<br>pass |

## 31-zset-memory

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| float | bytes_per_member_10 | B | 118.8 | 37.58 | 27.96 | 37.58 | rel: <= 39.46 (5% over 37.58, +1)<br>ratio: gain over v1.2.2 3.16x >= 1.90x (60% of recorded 3.16x) | pass<br>pass |
| float | bytes_per_member_100 | B | 116.4 | 27.31 | 26.16 | 27.31 | rel: <= 28.67 (5% over 27.31, +1)<br>ratio: gain over v1.2.2 4.26x >= 2.56x (60% of recorded 4.26x) | pass<br>pass |
| float | bytes_per_member_1000 | B | 132.5 | 132.5 | 67.98 | 132.5 | rel: <= 139.1 (5% over 132.5, +1) | pass |
| float | bytes_per_member_128 | B | 131.7 | 26.96 | 24.42 | 26.96 | rel: <= 28.31 (5% over 26.96, +1)<br>ratio: gain over v1.2.2 4.89x >= 2.93x (60% of recorded 4.89x) | pass<br>pass |
| float | bytes_per_member_129 | B | 130.9 | 131.2 | 78.56 | 131.2 | rel: <= 137.7 (5% over 131.2, +1) | pass |
| float | encoding_10 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| float | encoding_100 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| float | encoding_1000 | code | 5 | 5 | 5 | 5 | bound: == 5 | pass |
| float | encoding_128 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| float | encoding_129 | code | 5 | 5 | 5 | 5 | bound: == 5 | pass |
| float | load_1000_s | s | 2.23 | 2.26 | 2.32 | 2 | info | info |
| float | load_100_s | s | 2.19 | 2.12 | 3.79 | 1.87 | info | info |
| float | load_10_s | s | 2.72 | 2.49 | 2.76 | 2.24 | info | info |
| float | load_128_s | s | 2.2 | 2.13 | 4.19 | 1.86 | info | info |
| float | load_129_s | s | 2.21 | 2.23 | 2.28 | 1.97 | info | info |
| int | bytes_per_member_10 | B | 118.8 | 29.58 | 24.76 | 29.58 | rel: <= 31.06 (5% over 29.58, +1)<br>ratio: gain over v1.2.2 4.02x >= 2.41x (60% of recorded 4.02x) | pass<br>pass |
| int | bytes_per_member_100 | B | 116.4 | 19.31 | 21.04 | 19.31 | rel: <= 20.31 (5% over 19.31, +1)<br>ratio: gain over v1.2.2 6.03x >= 3.62x (60% of recorded 6.03x) | pass<br>pass |
| int | bytes_per_member_1000 | B | 132.5 | 132.5 | 68.24 | 132.5 | rel: <= 139.1 (5% over 132.5, +1) | pass |
| int | bytes_per_member_128 | B | 131.7 | 18.96 | 20.42 | 18.96 | rel: <= 19.96 (5% over 18.96, +1)<br>ratio: gain over v1.2.2 6.95x >= 4.17x (60% of recorded 6.95x) | pass<br>pass |
| int | bytes_per_member_129 | B | 130.9 | 131.2 | 78.68 | 131.2 | rel: <= 137.7 (5% over 131.2, +1) | pass |
| int | encoding_10 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| int | encoding_100 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| int | encoding_1000 | code | 5 | 5 | 5 | 5 | bound: == 5 | pass |
| int | encoding_128 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| int | encoding_129 | code | 5 | 5 | 5 | 5 | bound: == 5 | pass |
| int | load_1000_s | s | 2.27 | 2.23 | 2.3 | 1.97 | info | info |
| int | load_100_s | s | 2.17 | 2.14 | 3.21 | 1.84 | info | info |
| int | load_10_s | s | 2.58 | 2.44 | 2.6 | 2.16 | info | info |
| int | load_128_s | s | 2.18 | 2.13 | 3.57 | 1.82 | info | info |
| int | load_129_s | s | 2.18 | 2.2 | 2.25 | 1.96 | info | info |

## Not run this time

`25-centrifuge-scripts-publish/scripts/add_calls_per_s`, `25-centrifuge-scripts-publish/scripts/history_calls_per_s`

