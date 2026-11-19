# Regression suite: thr-verify-d4s_v5

Verdict: **REGRESSED** (5 failed)

- Machine: Standard_D4s_v5, 4 vCPU, Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz, kernel 7.0.0-1017-azure, glibc 2.39
- CPU: recorded on INTEL(R) XEON(R) PLATINUM 8573C / Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz; now Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz
- calibration (indicative only): 93.0 ms for a fixed sha256 loop, 4 cores
- Client: Python 3.12.3, redis-py 5.2.1
- v1_2_2: `kividb 1.2.2`, sha256 `38fc2cea40c131e041f4914527a98719e5d34dffdfe2abdc1f07ecb86aa6723e`, rustc 1.100.0
- candidate: `kividb 1.2.1 (default build: no Lua, no TLS)`, sha256 `d25ed788ccd6ddd9e8bd6e3d2f750ba1f196dd2160582c013ffbc14c7aefb32b`, rustc 1.100.0
- redis: `Redis server v=8.10.2 sha=00000000:1 malloc=jemalloc-5.3.0 bits=64 build=75a9b36b1f0aa00`, sha256 `b0cd304980112fa6cd8dced524fdd6b136fd8efb802f5cf9e81610051a11891c`
- v1_2_2_full: `kividb 1.2.2`, sha256 `ac5e05fe1274fd1167780e512bc03aa103340f42525a2cef4a2a762e47317ad5`, rustc 1.100.0
- candidate_full: `kividb 1.2.1 (full build: Lua, TLS)`, sha256 `7731cf98f532d24c206b3e5bda8af97550f40324515108c4625327983bff160b`, rustc 1.100.0
- not run, need more CPUs than this machine's 4 (run them with --min-cpus-only on one that big): 18-mastodon-feed-fanout (8), 20-envoy-ratelimit (8)
- candidate commit: `23351323`
- date: 2026-11-19; FULL=1, SCALE=1
- run time: 01-scan-match 131s, 02-growth-ping 695s, 03-give-back 200s, 04-long-commands 197s, 05-stream-memory 107s, 06-stream-consumers 27s, 07-save-ping 10s, 08-ft-info 60s, 09-vector-sets 180s, 10-redisvl 2s, 11-migrate-conns 3s, 12-search-fanout 4s, 13-cluster-reshard 470s, 14-sinter-big-small 372s, 15-brpop-many-keys 335s, 16-random-sample 77s, 17-set-memory 44s, 19-hash-memory 80s, 21-list-memory 243s, 22-scan-large-count 40s, 23-pubsub-pattern-fanout 141s, 24-snapshot-size 450s, 25-centrifuge-scripts-publish 362s, 27-ratelimit-restarted 568s, 30-script-call-cost 550s, 31-zset-memory 93s, 32-default-threads 682s, 33-ft-aggregate-groupby 146s

Host speed (this host vs the recorded one; 1.00 = same, below 1 = slower), from v1.2.2 measured in both sessions: global 1.00x over 198 metrics
- 01-scan-match/default: 1.00x (8 metrics)
- 02-growth-ping/ex17m: 1.01x (1 metrics, too few: the recording host's factor is used)
- 02-growth-ping/tokio: 1.01x (1 metrics, too few: the recording host's factor is used)
- 02-growth-ping/uring1: 1.02x (1 metrics, too few: the recording host's factor is used)
- 03-give-back/churn: 1.00x (1 metrics, too few: the recording host's factor is used)
- 03-give-back/hashdel: 0.99x (1 metrics, too few: the recording host's factor is used)
- 03-give-back/hdel: 1.05x (1 metrics, too few: the recording host's factor is used)
- 04-long-commands/tokio: 1.01x (3 metrics)
- 04-long-commands/uring: 1.00x (3 metrics)
- 05-stream-memory/one1m: 1.02x (1 metrics, too few: the recording host's factor is used)
- 05-stream-memory/shapes: 1.03x (6 metrics)
- 06-stream-consumers/pel: 0.98x (8 metrics)
- 07-save-ping/tokio1: 0.99x (1 metrics, too few: the recording host's factor is used)
- 08-ft-info/ftinfo: 1.01x (6 metrics)
- 09-vector-sets/bin_768_50k: 0.91x (2 metrics, too few: the recording host's factor is used)
- 09-vector-sets/f32_768_10k: 0.84x (3 metrics)
- 09-vector-sets/q8_768_10k: 1.00x (3 metrics)
- 09-vector-sets/q8_768_50k: 1.06x (2 metrics, too few: the recording host's factor is used)
- 11-migrate-conns/migrate: 0.90x (1 metrics, too few: the recording host's factor is used)
- 12-search-fanout/cluster3: 0.91x (1 metrics, too few: the recording host's factor is used)
- 13-cluster-reshard/reshard1000: 1.03x (2 metrics, too few: the recording host's factor is used)
- 14-sinter-big-small/default: 1.04x (14 metrics)
- 14-sinter-big-small/n20m: 1.02x (7 metrics)
- 14-sinter-big-small/sunion: 1.06x (1 metrics, too few: the recording host's factor is used)
- 16-random-sample/n100: 1.00x (12 metrics)
- 16-random-sample/n10k: 1.26x (12 metrics)
- 16-random-sample/n1m: 0.94x (12 metrics)
- 17-set-memory/ints: 1.82x (4 metrics)
- 17-set-memory/strings: 2.17x (4 metrics)
- 19-hash-memory/plain: 1.05x (4 metrics)
- 19-hash-memory/ttl: 1.06x (4 metrics)
- 21-list-memory/packed: 1.00x (4 metrics)
- 21-list-memory/plain: 0.99x (4 metrics)
- 21-list-memory/short: 1.00x (4 metrics)
- 22-scan-large-count/t4: 1.12x (3 metrics)
- 23-pubsub-pattern-fanout/channel: 0.97x (2 metrics, too few: the recording host's factor is used)
- 23-pubsub-pattern-fanout/pattern: 1.00x (2 metrics, too few: the recording host's factor is used)
- 24-snapshot-size/coll-default: 1.10x (2 metrics, too few: the recording host's factor is used)
- 24-snapshot-size/coll-yes: 1.06x (2 metrics, too few: the recording host's factor is used)
- 24-snapshot-size/issue-default: 0.79x (2 metrics, too few: the recording host's factor is used)
- 24-snapshot-size/issue-yes: 0.84x (2 metrics, too few: the recording host's factor is used)
- 25-centrifuge-scripts-publish/publish: 0.92x (2 metrics, too few: the recording host's factor is used)
- 25-centrifuge-scripts-publish/scripts: 0.92x (2 metrics, too few: the recording host's factor is used)
- 27-ratelimit-restarted/t2: 1.01x (3 metrics)
- 30-script-call-cost/scripts: 0.92x (12 metrics)
- 31-zset-memory/float: 0.89x (5 metrics)
- 31-zset-memory/int: 0.87x (5 metrics)
- 32-default-threads/shared: 1.02x (8 metrics)
- 33-ft-aggregate-groupby/docs2m: 0.99x (4 metrics)
- recorded on Standard_D4s_v5 / ?: 1.00x (56 metrics)
- recorded on Standard_D4s_v5 / INTEL(R) XEON(R) PLATINUM 8573C: 0.90x (26 metrics)
- recorded on Standard_D4s_v5 / Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz: 1.02x (116 metrics)

Metrics whose own v1.2.2 factor differs from their group's by more than 1.3x (rule applied in brackets):

- 08-ft-info/ftinfo/set_rps_beside_ft_info: own 0.29x, group 1.01x (group)
- 09-vector-sets/q8_768_10k/vsim_qps: own 1.36x, group 1.00x (own-capped)
- 16-random-sample/n10k/hrandfield_10_wv_us: own 0.93x, group 1.26x (own-slower)

## 01-scan-match

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| default | dbsize | keys | 2,002,003 | 2,002,003 | 2,002,003 | 2,002,003 | bound: == 2,002,003 | pass |
| default | hscan_match_c1000_calls | calls | 201.0 | 201.0 | 201.0 | 201.0 | info | info |
| default | hscan_match_c1000_distinct | keys | 200.0 | 200.0 | 200.0 | 200.0 | bound: == 200.0 | pass |
| default | hscan_match_c1000_ms | ms | 447.2 | 378.4 | 44.09 | 428.0 | rel: <= 575.7 (35% over 426.4, recorded 428.0 scaled by host speed 1.00x (group)) | pass |
| default | keys_schedule_ms | ms | 117.3 | 56.13 | 133.6 | 60.44 | rel: <= 66.24 (10% over 60.22, recorded 60.44 scaled by host speed 1.00x (group)) | pass |
| default | keys_schedule_returned | keys | 2,000 | 2,000 | 2,000 | 2,000 | bound: == 2,000 | pass |
| default | scan_all_c1000_calls | calls | 513.0 | 512.0 | 2,002 | 512.0 | info | info |
| default | scan_all_c1000_distinct | keys | 2,002,003 | 2,002,003 | 2,002,003 | 2,002,003 | bound: == 2,002,003 | pass |
| default | scan_all_c1000_ms | ms | 3,631 | 1,786 | 2,179 | 2,180 | rel: <= 2,390 (10% over 2,172, recorded 2,180 scaled by host speed 1.00x (group))<br>ratio: gain over v1.2.2 2.03x >= 1.00x (60% of recorded 1.66x)<br>vs_redis: 0.82x Redis <= 1.3x | pass<br>pass<br>pass |
| default | scan_infix_c1000_calls | calls | 513.0 | 512.0 | 2,002 | 512.0 | info | info |
| default | scan_infix_c1000_distinct | keys | 1,112,222 | 1,112,222 | 1,112,222 | 1,112,222 | bound: == 1,112,222 | pass |
| default | scan_infix_c1000_ms | ms | 2,855 | 1,095 | 1,773 | 1,494 | rel: <= 1,638 (10% over 1,489, recorded 1,494 scaled by host speed 1.00x (group))<br>ratio: gain over v1.2.2 2.61x >= 1.08x (60% of recorded 1.80x)<br>vs_redis: 0.62x Redis <= 1.3x | pass<br>pass<br>pass |
| default | scan_match_c10000_calls | calls | 193.0 | 193.0 | 201.0 | 193.0 | info | info |
| default | scan_match_c10000_distinct | keys | 2,000 | 2,000 | 2,000 | 2,000 | bound: == 2,000 | pass |
| default | scan_match_c10000_ms | ms | 814.2 | 108.3 | 401.7 | 238.9 | rel: <= 261.8 (10% over 238.0, recorded 238.9 scaled by host speed 1.00x (group))<br>ratio: gain over v1.2.2 7.52x >= 2.07x (60% of recorded 3.45x)<br>vs_redis: 0.27x Redis <= 1.3x | pass<br>pass<br>pass |
| default | scan_match_c1000_calls | calls | 513.0 | 512.0 | 2,002 | 512.0 | info | info |
| default | scan_match_c1000_distinct | keys | 2,000 | 2,000 | 2,000 | 2,000 | bound: == 2,000 | pass |
| default | scan_match_c1000_ms | ms | 1,523 | 205.4 | 575.0 | 406.2 | rel: <= 445.2 (10% over 404.8, recorded 406.2 scaled by host speed 1.00x (group))<br>ratio: gain over v1.2.2 7.41x >= 2.47x (60% of recorded 4.12x)<br>vs_redis: 0.36x Redis <= 1.3x | pass<br>pass<br>pass |
| default | sscan_match_c1000_calls | calls | 201.0 | 201.0 | 201.0 | 201.0 | info | info |
| default | sscan_match_c1000_distinct | keys | 200.0 | 200.0 | 200.0 | 200.0 | bound: == 200.0 | pass |
| default | sscan_match_c1000_ms | ms | 910.2 | 1,443 | 37.70 | 1,093 | rel: <= 1,471 (35% over 1,089, recorded 1,093 scaled by host speed 1.00x (group)) | pass |
| default | zscan_match_c1000_calls | calls | 201.0 | 201.0 | 201.0 | 201.0 | info | info |
| default | zscan_match_c1000_distinct | keys | 200.0 | 200.0 | 200.0 | 200.0 | bound: == 200.0 | pass |
| default | zscan_match_c1000_ms | ms | 736.8 | 579.8 | 42.37 | 829.5 | rel: <= 1,116 (35% over 826.6, recorded 829.5 scaled by host speed 1.00x (group)) | pass |

## 02-growth-ping

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| ex17m | dbsize | keys | 17,500,000 | 17,500,000 | 17,500,000 | 17,500,000 | bound: == 17,500,000 | pass |
| ex17m | load_s | s | 144.2 | 141.2 | 145.9 | 143.2 | rel: <= 157.4 (10% over 143.1, recorded 143.2 scaled by host speed 1.00x (host)) | pass |
| ex17m | ping_max_ms | ms | 175.2 | 11.77 | 11.34 | 17.02 | rel: <= 25.53 (50% over 17.02, Redis 11.34 + 10) | pass |
| ex17m | ping_p99_ms | ms | 3.41 | 3.53 | 1.86 | 3.64 | rel: <= 11.86 (50% over 3.64, Redis 1.86 + 10) | pass |
| ex17m | pings | pings | 123,209 | 121,715 | 132,645 | 120,736 | info | info |
| ex17m | pings_over_10ms | pings | 67.00 | 3 | 1 | 14.00 | info | info |
| ex17m | rss_loaded_mib | MiB | 1,507 | 1,507 | 2,084 | 1,507 | rel: <= 1,583 (5% over 1,507) | pass |
| ex17m | used_memory_mib | MiB | 1,613 | 1,613 | 2,027 | 1,613 | rel: <= 1,694 (5% over 1,613) | pass |
| tokio | dbsize | keys | 8,000,000 | 8,000,000 | 8,000,000 | 8,000,000 | bound: == 8,000,000 | pass |
| tokio | load_s | s | 50.90 | 49.65 | 50.22 | 50.84 | rel: <= 55.89 (10% over 50.81, recorded 50.84 scaled by host speed 1.00x (host)) | pass |
| tokio | ping_max_ms | ms | 145.2 | 6.62 | 7.67 | 9.66 | rel: <= 17.67 (50% over 9.66, Redis 7.67 + 10)<br>ratio: gain over v1.2.2 21.95x >= 6.96x (60% of recorded 11.61x) | pass<br>pass |
| tokio | ping_p99_ms | ms | 1.87 | 2.45 | 1.19 | 3.03 | rel: <= 11.19 (50% over 3.03, Redis 1.19 + 10) | pass |
| tokio | pings | pings | 45,845 | 45,478 | 47,507 | 44,740 | info | info |
| tokio | pings_over_10ms | pings | 24.00 | 0 | 0 | 0 | info | info |
| tokio | rss_loaded_mib | MiB | 523.9 | 526.3 | 551.8 | 525.0 | rel: <= 551.3 (5% over 525.0) | pass |
| tokio | used_memory_mib | MiB | 511.4 | 511.3 | 534.8 | 511.4 | rel: <= 537.0 (5% over 511.4) | pass |
| uring1 | dbsize | keys | 8,000,000 | 8,000,000 | - | 8,000,000 | bound: == 8,000,000 | pass |
| uring1 | load_s | s | 52.12 | 50.89 | - | 55.60 | rel: <= 61.13 (10% over 55.57, recorded 55.60 scaled by host speed 1.00x (host)) | pass |
| uring1 | ping_max_ms | ms | 107.7 | 9.33 | - | 20.83 | rel: <= 31.25 (50% over 20.83)<br>ratio: gain over v1.2.2 11.54x >= 2.09x (60% of recorded 3.49x) | pass<br>pass |
| uring1 | ping_p99_ms | ms | 0.681 | 1.76 | - | 1.54 | rel: <= 2.32 (50% over 1.54) | pass |
| uring1 | pings | pings | 48,338 | 47,556 | - | 51,940 | info | info |
| uring1 | pings_over_10ms | pings | 29.00 | 0 | - | 2 | info | info |
| uring1 | rss_loaded_mib | MiB | 524.4 | 525.7 | - | 524.5 | rel: <= 550.7 (5% over 524.5) | pass |
| uring1 | used_memory_mib | MiB | 511.5 | 511.5 | - | 511.6 | rel: <= 537.2 (5% over 511.6) | pass |

## 03-give-back

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| churn | delete_ms | ms | 1,533 | 1,550 | 1,485 | 1,538 | info | info |
| churn | rss_after_15s_mib | MiB | 483.6 | 429.8 | 618.6 | 429.4 | rel: <= 536.8 (25% over 429.4, +5) | pass |
| churn | rss_after_1_5s_mib | MiB | 483.6 | 429.8 | 738.3 | 429.4 | rel: <= 536.8 (25% over 429.4, +5) | pass |
| churn | rss_after_5s_mib | MiB | 483.6 | 429.8 | 676.3 | 429.4 | rel: <= 536.8 (25% over 429.4, +5) | pass |
| churn | rss_loaded_mib | MiB | 788.5 | 790.6 | 746.3 | 789.3 | rel: <= 828.8 (5% over 789.3) | pass |
| churn | rss_start_mib | MiB | 12.42 | 14.63 | 12.60 | 12.73 | info | info |
| churn | used_after_mib | MiB | 372.1 | 372.0 | 361.8 | 372.1 | info | info |
| churn | used_loaded_mib | MiB | 742.5 | 742.5 | 719.1 | 742.6 | rel: <= 779.7 (5% over 742.6) | pass |
| hashdel | delete_ms | ms | 131.9 | 131.1 | 302.5 | 134.4 | info | info |
| hashdel | rss_after_15s_mib | MiB | 196.0 | 15.00 | 27.00 | 13.60 | rel: <= 18.60 (25% over 13.60, +5) | pass |
| hashdel | rss_after_1_5s_mib | MiB | 196.0 | 15.00 | 76.30 | 13.60 | rel: <= 18.60 (25% over 13.60, +5)<br>ratio: gain over v1.2.2 13.07x >= 8.66x (60% of recorded 14.44x) | pass<br>pass |
| hashdel | rss_after_5s_mib | MiB | 196.0 | 15.00 | 76.30 | 13.60 | rel: <= 18.60 (25% over 13.60, +5) | pass |
| hashdel | rss_loaded_mib | MiB | 369.1 | 371.1 | 124.3 | 369.9 | rel: <= 388.4 (5% over 369.9) | pass |
| hashdel | rss_start_mib | MiB | 12.43 | 14.48 | 12.50 | 13.28 | info | info |
| hashdel | used_after_mib | MiB | 1.36 | 1.33 | 0.846 | 1.39 | info | info |
| hashdel | used_loaded_mib | MiB | 295.9 | 295.8 | 109.3 | 295.9 | rel: <= 310.7 (5% over 295.9) | pass |
| hdel | delete_ms | ms | 1,277 | 1,284 | 1,221 | 1,344 | info | info |
| hdel | rss_after_15s_mib | MiB | 74.10 | 15.30 | 13.80 | 13.60 | rel: <= 18.60 (25% over 13.60, +5) | pass |
| hdel | rss_after_1_5s_mib | MiB | 74.10 | 15.30 | 35.40 | 13.60 | rel: <= 18.60 (25% over 13.60, +5)<br>ratio: gain over v1.2.2 4.84x >= 3.27x (60% of recorded 5.45x) | pass<br>pass |
| hdel | rss_after_5s_mib | MiB | 74.10 | 15.30 | 24.60 | 13.60 | rel: <= 18.60 (25% over 13.60, +5) | pass |
| hdel | rss_loaded_mib | MiB | 137.6 | 139.7 | 47.40 | 138.2 | rel: <= 145.1 (5% over 138.2) | pass |
| hdel | rss_start_mib | MiB | 12.50 | 14.67 | 12.73 | 13.18 | info | info |
| hdel | used_after_mib | MiB | 1.36 | 1.33 | 0.846 | 1.39 | info | info |
| hdel | used_loaded_mib | MiB | 132.8 | 132.8 | 29.51 | 132.8 | rel: <= 139.5 (5% over 132.8) | pass |

## 04-long-commands

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| tokio | dbsize | keys | 2,000,000 | 2,000,000 | 2,000,000 | 2,000,000 | bound: == 2,000,000 | pass |
| tokio | del_cmd_ms | ms | 131.2 | 138.3 | 324.7 | 143.3 | rel: <= 155.6 (10% over 141.4, recorded 143.3 scaled by host speed 1.01x (group)) | pass |
| tokio | del_probes_stalled | probes | 4 | 0 | 4 | 0 | info | info |
| tokio | del_reply | items | 1 | 1 | 1 | 1 | bound: == 1 | pass |
| tokio | del_worst_ping_max_ms | ms | 131.1 | 0.149 | 332.9 | 1.22 | bound: <= 20.00 | pass |
| tokio | del_worst_ping_ms | ms | 130.2 | 0.132 | 323.7 | 0.942 | bound: <= 20.00 | pass |
| tokio | keys_cmd_ms | ms | 139.2 | 90.55 | 209.7 | 161.3 | rel: <= 175.1 (10% over 159.2, recorded 161.3 scaled by host speed 1.01x (group))<br>vs_v1_2_2: 0.65x v1.2.2 <= 0.85x | pass<br>pass |
| tokio | keys_probes_stalled | probes | 4 | 0 | 4 | 0 | info | info |
| tokio | keys_reply | items | 11,111 | 11,111 | 11,111 | 11,111 | bound: == 11,111 | pass |
| tokio | keys_worst_ping_max_ms | ms | 135.6 | 1.64 | 204.4 | 8.09 | bound: <= 20.00 | pass |
| tokio | keys_worst_ping_ms | ms | 131.0 | 0.156 | 198.9 | 5.94 | bound: <= 20.00 | pass |
| tokio | scan_cmd_ms | ms | 250.7 | 62.85 | 463.0 | 112.5 | rel: <= 122.1 (10% over 111.0, recorded 112.5 scaled by host speed 1.01x (group)) | pass |
| tokio | scan_probes_stalled | probes | 4 | 0 | 4 | 0 | info | info |
| tokio | scan_reply | items | 11,111 | 11,111 | 11,111 | 11,111 | info | info |
| tokio | scan_worst_ping_max_ms | ms | 262.6 | 2.77 | 456.0 | 5.19 | bound: <= 20.00 | pass |
| tokio | scan_worst_ping_ms | ms | 240.5 | 0.405 | 450.7 | 0.449 | bound: <= 20.00 | pass |
| uring | dbsize | keys | 2,000,000 | 2,000,000 | - | 2,000,000 | bound: == 2,000,000 | pass |
| uring | del_cmd_ms | ms | 137.0 | 136.6 | - | 134.3 | rel: <= 147.3 (10% over 133.9, recorded 134.3 scaled by host speed 1.00x (group)) | pass |
| uring | del_least_worst_ping_ms | ms | 0.16 | 0.07 | - | 0.41 | rel: <= 10.41 (50% over 0.41, +10) | pass |
| uring | del_probes_stalled | probes | 1 | 1 | - | 1 | info | info |
| uring | del_reply | items | 1 | 1 | - | 1 | bound: == 1 | pass |
| uring | del_worst_ping_ms | ms | 136.9 | 135.9 | - | 132.9 | info | info |
| uring | keys_cmd_ms | ms | 145.2 | 90.84 | - | 153.6 | rel: <= 168.5 (10% over 153.2, recorded 153.6 scaled by host speed 1.00x (group))<br>vs_v1_2_2: 0.63x v1.2.2 <= 0.85x | pass<br>pass |
| uring | keys_least_worst_ping_ms | ms | 0.09 | 0.1 | - | 0.95 | rel: <= 10.95 (50% over 0.95, +10) | pass |
| uring | keys_probes_stalled | probes | 1 | 2 | - | 2 | info | info |
| uring | keys_reply | items | 11,111 | 11,111 | - | 11,111 | bound: == 11,111 | pass |
| uring | keys_worst_ping_ms | ms | 134.2 | 79.50 | - | 142.8 | info | info |
| uring | scan_cmd_ms | ms | 259.6 | 61.36 | - | 112.6 | rel: <= 123.6 (10% over 112.3, recorded 112.6 scaled by host speed 1.00x (group)) | pass |
| uring | scan_least_worst_ping_ms | ms | 0.14 | 0.07 | - | 0.13 | rel: <= 10.13 (50% over 0.13, +10) | pass |
| uring | scan_probes_stalled | probes | 1 | 2 | - | 1 | info | info |
| uring | scan_reply | items | 11,111 | 11,111 | - | 11,111 | info | info |
| uring | scan_worst_ping_ms | ms | 249.2 | 50.39 | - | 101.6 | info | info |

## 05-stream-memory

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| one1m | one_field_bytes_per_entry | B | 158.4 | 6.23 | 13.09 | 6.23 | rel: <= 7.23 (5% over 6.23, +1)<br>ratio: gain over v1.2.2 25.44x >= 15.26x (60% of recorded 25.43x) | pass<br>pass |
| one1m | one_field_load_s | s | 8.18 | 7.98 | 7.92 | 8.19 | info | info |
| one1m | one_field_rss_mib | MiB | 195.6 | 21.47 | 25.30 | 20.71 | rel: <= 25.71 (5% over 20.71, +5)<br>ratio: gain over v1.2.2 9.11x >= 5.67x (60% of recorded 9.45x) | pass<br>pass |
| one1m | one_field_used_mib | MiB | 152.4 | 7.27 | 13.28 | 7.33 | info | info |
| shapes | shared5_bytes_per_entry | B | 478.4 | 14.65 | 26.15 | 14.65 | rel: <= 15.65 (5% over 14.65, +1)<br>ratio: gain over v1.2.2 32.65x >= 19.59x (60% of recorded 32.65x) | pass<br>pass |
| shapes | shared5_load_s | s | 2.68 | 2.69 | 2.67 | 2.66 | info | info |
| shapes | unique5_bytes_per_entry | B | 478.4 | 54.01 | 75.27 | 54.01 | rel: <= 56.71 (5% over 54.01, +1)<br>ratio: gain over v1.2.2 8.86x >= 5.32x (60% of recorded 8.86x) | pass<br>pass |
| shapes | unique5_load_s | s | 2.77 | 2.8 | 2.76 | 2.76 | info | info |
| shapes | val200_bytes_per_entry | B | 350.9 | 216.5 | 216.3 | 216.5 | rel: <= 227.3 (5% over 216.5, +1)<br>ratio: gain over v1.2.2 1.62x >= 0.97x (60% of recorded 1.62x) | pass<br>pass |
| shapes | val200_load_s | s | 1.5 | 1.49 | 1.5 | 1.53 | info | info |
| shapes | xadd5_rps | req/s | 351,370 | 541,712 | 508,130 | 535,332 | rel: >= 468,261 (15% under 550,896, recorded 535,332 scaled by host speed 1.03x (group)) | pass |
| shapes | xrange5_count10_rps | req/s | 92,279 | 85,251 | 128,315 | 86,381 | rel: >= 75,558 (15% under 88,892, recorded 86,381 scaled by host speed 1.03x (group)) | pass |
| shapes | xrange_one_id_rps | req/s | 458,716 | 499,002 | 446,828 | 443,262 | rel: >= 387,727 (15% under 456,150, recorded 443,262 scaled by host speed 1.03x (group)) | pass |

## 06-stream-consumers

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| pel | pending_bytes_per_entry | B | 116.5 | 121.7 | 166.6 | 122.3 | rel: <= 128.4 (5% over 122.3, +2) | pass |
| pel | xadd_other_stream_max_ms | ms | 112.8 | 0.887 | 1.32 | 0.471 | info | info |
| pel | xadd_other_stream_rps | req/s | 99,354 | 111,297 | 138,313 | 104,384 | rel: >= 77,058 (25% under 102,744, recorded 104,384 scaled by host speed 0.98x (group)) | pass |
| pel | xadd_polled_stream_max_ms | ms | 86.33 | 0.599 | 2.46 | 0.823 | info | info |
| pel | xadd_polled_stream_rps | req/s | 42,382 | 113,186 | 142,248 | 117,371 | rel: >= 86,645 (25% under 115,527, recorded 117,371 scaled by host speed 0.98x (group))<br>ratio: gain over v1.2.2 2.67x >= 1.65x (60% of recorded 2.74x) | pass<br>pass |
| pel | xinfo_consumers_ms | ms | 70.64 | 0.371 | 0.37 | 0.395 | rel: <= 0.902 (50% over 0.402, +0.5, recorded 0.395 scaled by host speed 0.98x (group))<br>ratio: gain over v1.2.2 190.25x >= 98.93x (60% of recorded 164.88x) | pass<br>pass |
| pel | xinfo_polls | calls | 16.00 | 40.00 | 33.00 | 42.00 | bound: >= 5 | pass |
| pel | xpending_c50_ms | ms | 1.62 | 0.0908 | 0.0885 | 0.11 | rel: <= 0.612 (50% over 0.112, +0.5, recorded 0.11 scaled by host speed 0.98x (group))<br>ratio: gain over v1.2.2 17.89x >= 8.76x (60% of recorded 14.60x) | pass<br>pass |
| pel | xpending_summary_ms | ms | 6.94 | 0.175 | 0.17 | 0.193 | rel: <= 0.696 (50% over 0.196, +0.5, recorded 0.193 scaled by host speed 0.98x (group))<br>ratio: gain over v1.2.2 39.64x >= 23.61x (60% of recorded 39.34x) | pass<br>pass |
| pel | xreadgroup_c50_history_ms | ms | 1.63 | 0.108 | 0.103 | 0.133 | rel: <= 0.635 (50% over 0.135, +0.5, recorded 0.133 scaled by host speed 0.98x (group))<br>ratio: gain over v1.2.2 15.06x >= 7.21x (60% of recorded 12.02x) | pass<br>pass |

## 07-save-ping

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| tokio1 | ping_during_save_max_ms | ms | 340.7 | 2 | 533.6 | 2.19 | bound: <= 20.00 | pass |
| tokio1 | ping_during_save_ms | ms | 335.9 | 1.97 | 523.4 | 1.48 | bound: <= 20.00 | pass |
| tokio1 | save_ms | ms | 338.1 | 341.3 | 524.6 | 339.4 | info | info |

## 08-ft-info

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| ftinfo | ft_info_polls_per_s | calls/s | 18.29 | 14,714 | - | 15,600 | info | info |
| ftinfo | ft_info_text_ms | ms | 115.1 | 0.126 | - | 0.12 | rel: <= 0.619 (50% over 0.119, +0.5, recorded 0.12 scaled by host speed 1.01x (group))<br>ratio: gain over v1.2.2 915.66x >= 580.04x (60% of recorded 966.74x) | pass<br>pass |
| ftinfo | ft_info_vec_ms | ms | 115.5 | 0.126 | - | 0.125 | rel: <= 0.624 (50% over 0.124, +0.5, recorded 0.125 scaled by host speed 1.01x (group))<br>ratio: gain over v1.2.2 918.86x >= 561.09x (60% of recorded 935.15x) | pass<br>pass |
| ftinfo | hset_vec_doc_rps | req/s | 108,460 | 101,044 | - | 107,797 | rel: >= 92,584 (15% under 108,922, recorded 107,797 scaled by host speed 1.01x (group)) | pass |
| ftinfo | populate_s | s | 2.02 | 2.01 | - | 2.09 | info | info |
| ftinfo | set_rps_beside_ft_info | req/s | 32.70 | 51,045 | - | 47,430 | rel: >= 38,340 (20% under 47,925, recorded 47,430 scaled by host speed 1.01x (group)) | pass |
| ftinfo | vec_doc_bytes | B | 474.5 | 347.1 | - | 494.1 | rel: <= 518.9 (5% over 494.1, +4) | pass |

## 09-vector-sets

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| bin_768_50k | bytes_per_vector | B | 3,168 | 191.7 | 853.6 | 191.7 | rel: <= 201.3 (5% over 191.7)<br>ratio: gain over v1.2.2 16.52x >= 9.91x (60% of recorded 16.52x) | pass<br>pass |
| bin_768_50k | recall_at_10 | ratio | 1 | 0.103 | 0.106 | 0.103 | rel: >= 0.101 (2% under 0.103) | pass |
| bin_768_50k | vadd_per_s | VADD/s | 84,370 | 58,927 | 5,684 | 61,976 | info | info |
| bin_768_50k | vsim_qps | q/s | 108.0 | 1,089 | 7,598 | 750.2 | info | info |
| f32_768_10k | recall_at_10 | ratio | 1 | 1 | 1 | 1 | rel: >= 0.98 (2% under 1) | pass |
| f32_768_10k | vadd_per_s | VADD/s | 65,932 | 61,139 | 2,190 | 62,038 | rel: >= 39,089 (25% under 52,119, recorded 62,038 scaled by host speed 0.84x (group)) | pass |
| f32_768_10k | vsim_p50_ms | ms | 2.32 | 2.45 | 0.38 | 1.91 | info | info |
| f32_768_10k | vsim_qps | q/s | 944.2 | 968.8 | 6,381 | 1,116 | rel: >= 702.9 (25% under 937.1, recorded 1,116 scaled by host speed 0.84x (group)) | pass |
| q8_768_10k | recall_at_10 | ratio | 1 | 0.972 | 0.972 | 0.972 | rel: >= 0.953 (2% under 0.972) | pass |
| q8_768_10k | vadd_per_s | VADD/s | 66,711 | 64,140 | 3,889 | 52,817 | rel: >= 39,509 (25% under 52,678, recorded 52,817 scaled by host speed 1.00x (group)) | pass |
| q8_768_10k | vsim_p50_ms | ms | 2.27 | 0.735 | 0.313 | 2.07 | info | info |
| q8_768_10k | vsim_qps | q/s | 920.0 | 2,776 | 7,334 | 2,493 | rel: >= 1,870 (25% under 2,493, host speed 1.00x (own-capped))<br>ratio: gain over v1.2.2 3.02x >= 2.20x (60% of recorded 3.67x) | pass<br>pass |
| q8_768_50k | bytes_per_vector | B | 3,168 | 863.7 | 1,526 | 863.7 | rel: <= 906.9 (5% over 863.7)<br>ratio: gain over v1.2.2 3.67x >= 2.20x (60% of recorded 3.67x) | pass<br>pass |
| q8_768_50k | recall_at_10 | ratio | 1 | 0.965 | 0.963 | 0.965 | rel: >= 0.946 (2% under 0.965) | pass |
| q8_768_50k | vadd_per_s | VADD/s | 88,008 | 63,529 | 3,618 | 64,379 | info | info |
| q8_768_50k | vsim_qps | q/s | 109.0 | 319.0 | 6,337 | 118.1 | info | info |

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
| migrate | migrate_ms | ms | 0.178 | 0.106 | 0.119 | 0.105 | info | info |
| migrate | target_connections | connections | 2,000 | 1 | 1 | 1 | bound: <= 3 | pass |

## 12-search-fanout

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| cluster3 | peer_connections_max | connections | 2,000 | 1 | - | 1 | bound: <= 20.00 | pass |
| cluster3 | peer_kept_clients_max | clients | 0 | 2 | - | 2 | info | info |
| cluster3 | searches_per_s | searches/s | 4,126 | 5,919 | - | 5,772 | rel: >= 4,332 (25% under 5,776, recorded 5,772 scaled by host speed 1.00x (host)) | pass |
| cluster3 | searches_wrong_count | searches | 0 | 0 | - | 0 | bound: == 0 | pass |

## 13-cluster-reshard

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| reshard1000 | cluster_check_failed | - | 0 | 0 | 0 | 0 | bound: <= 0 | pass |
| reshard1000 | keys_after | keys | 3,000,000 | 3,000,000 | 3,000,000 | 3,000,000 | bound: == 3,000,000 | pass |
| reshard1000 | keys_moved | keys | 183,078 | 183,078 | 183,078 | 183,078 | bound: == 183,078 | pass |
| reshard1000 | load_s | s | 24.42 | 24.62 | 24.08 | 24.21 | info | info |
| reshard1000 | reshard_s | s | 354.1 | 26.97 | 7.19 | 25.30 | rel: <= 29.08 (15% over 25.29, recorded 25.30 scaled by host speed 1.00x (host))<br>ratio: gain over v1.2.2 13.13x >= 9.05x (60% of recorded 15.09x) | pass<br>pass |

## 14-sinter-big-small

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| default | 1m_big_card | members | 1,000,000 | 1,000,000 | 1,000,000 | 1,000,000 | bound: == 1,000,000 | pass |
| default | 1m_sdiff_small_big_ms | ms | 64.74 | 1.86 | 2.44 | 1.95 | rel: <= 3.87 (50% over 1.87, +2, recorded 1.95 scaled by host speed 1.04x (group))<br>ratio: gain over v1.2.2 34.84x >= 21.50x (60% of recorded 35.83x)<br>vs_redis: 0.76x Redis <= 2x | pass<br>pass<br>pass |
| default | 1m_sdiff_small_big_reply | members | 1,999 | 1,999 | 1,999 | 1,999 | bound: == 1,999 | pass |
| default | 1m_sdiffstore_big_small_ms | ms | 241.8 | 176.1 | 257.0 | 253.6 | rel: <= 304.3 (25% over 243.4, recorded 253.6 scaled by host speed 1.04x (group)) | pass |
| default | 1m_sdiffstore_big_small_reply | members | 997,903 | 997,903 | 997,903 | 997,903 | bound: == 997,903 | pass |
| default | 1m_sinter_big_small_ms | ms | 88.53 | 2.35 | 2.33 | 2.33 | rel: <= 4.24 (50% over 2.24, +2, recorded 2.33 scaled by host speed 1.04x (group))<br>ratio: gain over v1.2.2 37.73x >= 24.66x (60% of recorded 41.11x)<br>vs_redis: 1.01x Redis <= 2x | pass<br>pass<br>pass |
| default | 1m_sinter_big_small_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| default | 1m_sinter_small_big_ms | ms | 64.64 | 2.03 | 2.29 | 2.09 | rel: <= 4.01 (50% over 2.01, +2, recorded 2.09 scaled by host speed 1.04x (group))<br>ratio: gain over v1.2.2 31.86x >= 19.93x (60% of recorded 33.21x)<br>vs_redis: 0.89x Redis <= 2x | pass<br>pass<br>pass |
| default | 1m_sinter_small_big_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| default | 1m_sintercard_limit_ms | ms | 89.94 | 0.0609 | 0.0608 | 0.0547 | rel: <= 2.05 (50% over 0.0525, +2, recorded 0.0547 scaled by host speed 1.04x (group)) | pass |
| default | 1m_sintercard_limit_reply | members | 100.0 | 100.0 | 100.0 | 100.0 | bound: == 100.0 | pass |
| default | 1m_sintercard_ms | ms | 88.02 | 0.258 | 0.459 | 0.184 | rel: <= 2.18 (50% over 0.177, +2, recorded 0.184 scaled by host speed 1.04x (group)) | pass |
| default | 1m_sintercard_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| default | 1m_sinterstore_ms | ms | 85.47 | 0.317 | 0.847 | 0.3 | rel: <= 2.29 (50% over 0.288, +2, recorded 0.3 scaled by host speed 1.04x (group)) | pass |
| default | 1m_sinterstore_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| default | 5m_big_card | members | 5,000,000 | 5,000,000 | 5,000,000 | 5,000,000 | bound: == 5,000,000 | pass |
| default | 5m_sdiff_small_big_ms | ms | 445.9 | 1.94 | 3.82 | 2.03 | rel: <= 3.95 (50% over 1.95, +2, recorded 2.03 scaled by host speed 1.04x (group))<br>ratio: gain over v1.2.2 230.15x >= 132.00x (60% of recorded 220.01x)<br>vs_redis: 0.51x Redis <= 2x | pass<br>pass<br>pass |
| default | 5m_sdiff_small_big_reply | members | 1,998 | 1,998 | 1,998 | 1,998 | bound: == 1,998 | pass |
| default | 5m_sdiffstore_big_small_ms | ms | 1,570 | 1,133 | 1,804 | 1,424 | rel: <= 1,709 (25% over 1,367, recorded 1,424 scaled by host speed 1.04x (group)) | pass |
| default | 5m_sdiffstore_big_small_reply | members | 4,997,902 | 4,997,902 | 4,997,902 | 4,997,902 | bound: == 4,997,902 | pass |
| default | 5m_sinter_big_small_ms | ms | 539.0 | 2.1 | 3.45 | 2.22 | rel: <= 4.13 (50% over 2.13, +2, recorded 2.22 scaled by host speed 1.04x (group))<br>ratio: gain over v1.2.2 257.11x >= 145.00x (60% of recorded 241.66x)<br>vs_redis: 0.61x Redis <= 2x | pass<br>pass<br>pass |
| default | 5m_sinter_big_small_reply | members | 2,098 | 2,098 | 2,098 | 2,098 | bound: == 2,098 | pass |
| default | 5m_sinter_small_big_ms | ms | 448.7 | 2.04 | 3.45 | 2.06 | rel: <= 3.98 (50% over 1.98, +2, recorded 2.06 scaled by host speed 1.04x (group))<br>ratio: gain over v1.2.2 219.88x >= 130.08x (60% of recorded 216.80x)<br>vs_redis: 0.59x Redis <= 2x | pass<br>pass<br>pass |
| default | 5m_sinter_small_big_reply | members | 2,098 | 2,098 | 2,098 | 2,098 | bound: == 2,098 | pass |
| default | 5m_sintercard_limit_ms | ms | 537.7 | 0.0558 | 0.111 | 0.0539 | rel: <= 2.05 (50% over 0.0517, +2, recorded 0.0539 scaled by host speed 1.04x (group)) | pass |
| default | 5m_sintercard_limit_reply | members | 100.0 | 100.0 | 100.0 | 100.0 | bound: == 100.0 | pass |
| default | 5m_sintercard_ms | ms | 540.3 | 0.287 | 1.57 | 0.223 | rel: <= 2.21 (50% over 0.214, +2, recorded 0.223 scaled by host speed 1.04x (group)) | pass |
| default | 5m_sintercard_reply | members | 2,098 | 2,098 | 2,098 | 2,098 | bound: == 2,098 | pass |
| default | 5m_sinterstore_ms | ms | 541.3 | 0.454 | 2.23 | 0.476 | rel: <= 2.46 (50% over 0.457, +2, recorded 0.476 scaled by host speed 1.04x (group)) | pass |
| default | 5m_sinterstore_reply | members | 2,098 | 2,098 | 2,098 | 2,098 | bound: == 2,098 | pass |
| n20m | 20m_big_card | members | 20,000,000 | 20,000,000 | 20,000,000 | 20,000,000 | bound: == 20,000,000 | pass |
| n20m | 20m_sdiff_small_big_ms | ms | 2,404 | 1.99 | 4.18 | 2.01 | rel: <= 3.97 (50% over 1.97, +2, recorded 2.01 scaled by host speed 1.02x (group))<br>ratio: gain over v1.2.2 1208.05x >= 733.04x (60% of recorded 1221.73x)<br>vs_redis: 0.48x Redis <= 2x | pass<br>pass<br>pass |
| n20m | 20m_sdiff_small_big_reply | members | 1,999 | 1,999 | 1,999 | 1,999 | bound: == 1,999 | pass |
| n20m | 20m_sdiffstore_big_small_ms | ms | 7,588 | 5,159 | 7,563 | 6,899 | rel: <= 8,464 (25% over 6,771, recorded 6,899 scaled by host speed 1.02x (group)) | pass |
| n20m | 20m_sdiffstore_big_small_reply | members | 19,997,903 | 19,997,903 | 19,997,903 | 19,997,903 | bound: == 19,997,903 | pass |
| n20m | 20m_sinter_big_small_ms | ms | 2,516 | 2.28 | 3.69 | 2.29 | rel: <= 4.24 (50% over 2.24, +2, recorded 2.29 scaled by host speed 1.02x (group))<br>ratio: gain over v1.2.2 1101.47x >= 674.97x (60% of recorded 1124.96x)<br>vs_redis: 0.62x Redis <= 2x | pass<br>pass<br>pass |
| n20m | 20m_sinter_big_small_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| n20m | 20m_sinter_small_big_ms | ms | 2,386 | 2.13 | 3.61 | 2.1 | rel: <= 4.06 (50% over 2.06, +2, recorded 2.1 scaled by host speed 1.02x (group))<br>ratio: gain over v1.2.2 1118.46x >= 694.18x (60% of recorded 1156.97x)<br>vs_redis: 0.59x Redis <= 2x | pass<br>pass<br>pass |
| n20m | 20m_sinter_small_big_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| n20m | 20m_sintercard_limit_ms | ms | 2,533 | 0.0829 | 0.116 | 0.0569 | rel: <= 2.06 (50% over 0.0558, +2, recorded 0.0569 scaled by host speed 1.02x (group)) | pass |
| n20m | 20m_sintercard_limit_reply | members | 100.0 | 100.0 | 100.0 | 100.0 | bound: == 100.0 | pass |
| n20m | 20m_sintercard_ms | ms | 2,522 | 0.352 | 1.77 | 0.296 | rel: <= 2.29 (50% over 0.291, +2, recorded 0.296 scaled by host speed 1.02x (group)) | pass |
| n20m | 20m_sintercard_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| n20m | 20m_sinterstore_ms | ms | 2,531 | 0.458 | 2.72 | 0.381 | rel: <= 2.37 (50% over 0.374, +2, recorded 0.381 scaled by host speed 1.02x (group)) | pass |
| n20m | 20m_sinterstore_reply | members | 2,097 | 2,097 | 2,097 | 2,097 | bound: == 2,097 | pass |
| sunion | sunion_ms | ms | 1,259 | 1,184 | 1,231 | 1,331 | rel: <= 1,636 (25% over 1,309, recorded 1,331 scaled by host speed 1.02x (host)) | pass |
| sunion | sunion_reply | members | 1,001,999 | 1,001,999 | 1,001,999 | 1,001,999 | bound: == 1,001,999 | pass |
| sunion | sunion_worst_sadd_ms | ms | 331.5 | 3.61 | 281.9 | 0.482 | bound: <= 20.00 | pass |

## 15-brpop-many-keys

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| c1000 | blocked_after | clients | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| c1000 | jobs_per_s | jobs/s | 372.8 | 1,925 | 1,582 | 1,934 | rel: >= 1,741 (10% under 1,934)<br>ratio: gain over v1.2.2 5.16x >= 3.06x (60% of recorded 5.10x)<br>vs_redis: 0.82x Redis <= 1.5x | pass<br>pass<br>pass |
| c1000 | lost | jobs | 157.0 | 0 | 0 | 0 | bound: == 0 | pass |
| c1000 | ping_max_ms | ms | 1,130 | 24.14 | 38.54 | 37.72 | rel: <= 75.44 (100% over 37.72, Redis 38.54 + 10) | pass |
| c1000 | ping_p99_ms | ms | 370.4 | 8.13 | 26.81 | 5.21 | info | info |
| c1000x1 | blocked_after | clients | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| c1000x1 | jobs_per_s | jobs/s | 1,937 | 1,952 | 1,837 | 1,950 | rel: >= 1,755 (10% under 1,950) | pass |
| c1000x1 | lost | jobs | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| c1000x1 | ping_max_ms | ms | 6.84 | 5.66 | 11.85 | 1.91 | rel: <= 21.85 (50% over 1.91, Redis 11.85 + 10) | pass |
| c1000x1 | ping_p99_ms | ms | 1.14 | 0.796 | 6.44 | 0.407 | info | info |
| c4000 | blocked_after | clients | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| c4000 | jobs_per_s | jobs/s | 31.24 | 1,907 | 953.5 | 1,924 | rel: >= 1,732 (10% under 1,924)<br>vs_redis: 0.50x Redis <= 1.5x | pass<br>pass |
| c4000 | lost | jobs | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| c4000 | ping_max_ms | ms | 6,061 | 27.86 | 214.3 | 72.41 | rel: <= 224.3 (50% over 72.41, Redis 214.3 + 10) | pass |
| c4000 | ping_p99_ms | ms | 6,061 | 7.05 | 101.9 | 4.99 | info | info |

## 16-random-sample

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| n100 | bad_replies | replies | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| n100 | hlen | fields | 100.0 | 100.0 | 100.0 | 100.0 | bound: == 100.0 | pass |
| n100 | hrandfield_10_us | us | 60.13 | 73.83 | 60.37 | 76.54 | rel: <= 115.0 (50% over 76.69, +20, recorded 76.54 scaled by host speed 1.00x (group)) | pass |
| n100 | hrandfield_10_wv_us | us | 70.10 | 85.71 | 68.79 | 67.69 | rel: <= 101.7 (50% over 67.83, +20, recorded 67.69 scaled by host speed 1.00x (group)) | pass |
| n100 | hrandfield_1_us | us | 50.91 | 63.47 | 49.96 | 69.09 | rel: <= 103.8 (50% over 69.23, +20, recorded 69.09 scaled by host speed 1.00x (group)) | pass |
| n100 | hrandfield_m10_us | us | 62.80 | 62.59 | 76.90 | 58.77 | rel: <= 88.33 (50% over 58.88, +20, recorded 58.77 scaled by host speed 1.00x (group)) | pass |
| n100 | hrandfield_m1_us | us | 65.87 | 60.84 | 51.51 | 51.93 | rel: <= 78.06 (50% over 52.04, +20, recorded 51.93 scaled by host speed 1.00x (group)) | pass |
| n100 | hrandfield_none_us | us | 47.84 | 51.22 | 47.20 | 48.71 | rel: <= 73.22 (50% over 48.81, +20, recorded 48.71 scaled by host speed 1.00x (group)) | pass |
| n100 | zcard | members | 100.0 | 100.0 | 100.0 | 100.0 | bound: == 100.0 | pass |
| n100 | zrandmember_10_us | us | 60.78 | 69.55 | 65.92 | 59.50 | rel: <= 89.43 (50% over 59.62, +20, recorded 59.50 scaled by host speed 1.00x (group)) | pass |
| n100 | zrandmember_10_ws_us | us | 68.58 | 69.44 | 86.18 | 68.01 | rel: <= 102.2 (50% over 68.15, +20, recorded 68.01 scaled by host speed 1.00x (group)) | pass |
| n100 | zrandmember_1_us | us | 52.12 | 51.01 | 50.79 | 51.35 | rel: <= 77.19 (50% over 51.46, +20, recorded 51.35 scaled by host speed 1.00x (group)) | pass |
| n100 | zrandmember_m10_us | us | 60.08 | 61.43 | 77.38 | 58.91 | rel: <= 88.55 (50% over 59.03, +20, recorded 58.91 scaled by host speed 1.00x (group)) | pass |
| n100 | zrandmember_m1_us | us | 59.30 | 52.24 | 51.96 | 51.64 | rel: <= 77.62 (50% over 51.75, +20, recorded 51.64 scaled by host speed 1.00x (group)) | pass |
| n100 | zrandmember_none_us | us | 50.53 | 48.17 | 48.82 | 48.14 | rel: <= 72.36 (50% over 48.24, +20, recorded 48.14 scaled by host speed 1.00x (group)) | pass |
| n10k | bad_replies | replies | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| n10k | hlen | fields | 10,000 | 10,000 | 10,000 | 10,000 | bound: == 10,000 | pass |
| n10k | hrandfield_10_us | us | 65.18 | 64.14 | 61.13 | 63.16 | rel: <= 75.34 (50% over 50.23, +20, recorded 63.16 scaled by host speed 1.26x (group)) | pass |
| n10k | hrandfield_10_wv_us | us | 80.14 | 75.67 | 69.11 | 75.70 | rel: <= 121.9 (50% over 81.27, +20, recorded 75.70 scaled by host speed 0.93x (own-slower)) | pass |
| n10k | hrandfield_1_us | us | 68.02 | 52.62 | 50.25 | 69.71 | rel: <= 83.15 (50% over 55.43, +20, recorded 69.71 scaled by host speed 1.26x (group)) | pass |
| n10k | hrandfield_m10_us | us | 64.70 | 61.63 | 60.47 | 79.38 | rel: <= 94.69 (50% over 63.13, +20, recorded 79.38 scaled by host speed 1.26x (group)) | pass |
| n10k | hrandfield_m1_us | us | 55.12 | 51.43 | 50.39 | 69.82 | rel: <= 83.28 (50% over 55.52, +20, recorded 69.82 scaled by host speed 1.26x (group)) | pass |
| n10k | hrandfield_none_us | us | 50.20 | 63.51 | 47.71 | 47.95 | rel: <= 58.13 (50% over 38.13, +20, recorded 47.95 scaled by host speed 1.26x (group)) | **FAIL** |
| n10k | zcard | members | 10,000 | 10,000 | 10,000 | 10,000 | bound: == 10,000 | pass |
| n10k | zrandmember_10_us | us | 84.38 | 89.35 | 62.39 | 71.55 | rel: <= 85.35 (50% over 56.90, +20, recorded 71.55 scaled by host speed 1.26x (group)) | **FAIL** |
| n10k | zrandmember_10_ws_us | us | 94.10 | 99.52 | 70.62 | 78.67 | rel: <= 93.84 (50% over 62.56, +20, recorded 78.67 scaled by host speed 1.26x (group)) | **FAIL** |
| n10k | zrandmember_1_us | us | 76.48 | 64.56 | 50.92 | 74.70 | rel: <= 89.10 (50% over 59.40, +20, recorded 74.70 scaled by host speed 1.26x (group)) | pass |
| n10k | zrandmember_m10_us | us | 81.29 | 67.49 | 69.96 | 74.28 | rel: <= 88.60 (50% over 59.07, +20, recorded 74.28 scaled by host speed 1.26x (group)) | pass |
| n10k | zrandmember_m1_us | us | 72.01 | 74.89 | 54.12 | 64.89 | rel: <= 77.40 (50% over 51.60, +20, recorded 64.89 scaled by host speed 1.26x (group)) | pass |
| n10k | zrandmember_none_us | us | 73.57 | 59.72 | 48.07 | 60.13 | rel: <= 71.73 (50% over 47.82, +20, recorded 60.13 scaled by host speed 1.26x (group)) | pass |
| n1m | bad_replies | replies | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| n1m | hlen | fields | 1,000,000 | 1,000,000 | 1,000,000 | 1,000,000 | bound: == 1,000,000 | pass |
| n1m | hrandfield_10_us | us | 1,393 | 64.36 | 67.65 | 62.89 | rel: <= 100.7 (50% over 67.11, +20, recorded 62.89 scaled by host speed 0.94x (group))<br>ratio: gain over v1.2.2 21.64x >= 10.01x (60% of recorded 16.68x)<br>vs_redis: 0.95x Redis <= 2x | pass<br>pass<br>pass |
| n1m | hrandfield_10_wv_us | us | 1,429 | 92.31 | 73.44 | 69.82 | rel: <= 111.8 (50% over 74.51, +20, recorded 69.82 scaled by host speed 0.94x (group))<br>ratio: gain over v1.2.2 15.48x >= 9.54x (60% of recorded 15.91x)<br>vs_redis: 1.26x Redis <= 2x | pass<br>pass<br>pass |
| n1m | hrandfield_1_us | us | 1,343 | 52.82 | 54.71 | 50.94 | rel: <= 81.54 (50% over 54.36, +20, recorded 50.94 scaled by host speed 0.94x (group))<br>ratio: gain over v1.2.2 25.42x >= 12.91x (60% of recorded 21.52x)<br>vs_redis: 0.97x Redis <= 2x | pass<br>pass<br>pass |
| n1m | hrandfield_m10_us | us | 822.3 | 83.28 | 69.57 | 59.96 | rel: <= 95.97 (50% over 63.98, +20, recorded 59.96 scaled by host speed 0.94x (group))<br>ratio: gain over v1.2.2 9.87x >= 7.01x (60% of recorded 11.68x)<br>vs_redis: 1.20x Redis <= 2x | pass<br>pass<br>pass |
| n1m | hrandfield_m1_us | us | 759.5 | 80.32 | 54.02 | 51.03 | rel: <= 81.68 (50% over 54.45, +20, recorded 51.03 scaled by host speed 0.94x (group))<br>ratio: gain over v1.2.2 9.46x >= 8.06x (60% of recorded 13.43x)<br>vs_redis: 1.49x Redis <= 2x | pass<br>pass<br>pass |
| n1m | hrandfield_none_us | us | 69.04 | 49.39 | 49.58 | 48.56 | rel: <= 77.72 (50% over 51.81, +20, recorded 48.56 scaled by host speed 0.94x (group))<br>vs_redis: 1.00x Redis <= 2x | pass<br>pass |
| n1m | zcard | members | 1,000,000 | 1,000,000 | 1,000,000 | 1,000,000 | bound: == 1,000,000 | pass |
| n1m | zrandmember_10_us | us | 6,710 | 1,810 | 68.56 | 1,852 | rel: <= 2,965 (50% over 1,977, +20, recorded 1,852 scaled by host speed 0.94x (group))<br>ratio: gain over v1.2.2 3.71x >= 2.09x (60% of recorded 3.48x) | pass<br>pass |
| n1m | zrandmember_10_ws_us | us | 6,715 | 1,829 | 76.01 | 1,820 | rel: <= 2,913 (50% over 1,942, +20, recorded 1,820 scaled by host speed 0.94x (group))<br>ratio: gain over v1.2.2 3.67x >= 2.16x (60% of recorded 3.60x) | pass<br>pass |
| n1m | zrandmember_1_us | us | 6,668 | 977.1 | 88.97 | 995.2 | rel: <= 1,593 (50% over 1,062, +20, recorded 995.2 scaled by host speed 0.94x (group))<br>ratio: gain over v1.2.2 6.82x >= 3.96x (60% of recorded 6.59x) | pass<br>pass |
| n1m | zrandmember_m10_us | us | 5,859 | 1,857 | 87.52 | 1,803 | rel: <= 2,886 (50% over 1,924, +20, recorded 1,803 scaled by host speed 0.94x (group))<br>ratio: gain over v1.2.2 3.16x >= 1.82x (60% of recorded 3.03x) | pass<br>pass |
| n1m | zrandmember_m1_us | us | 5,857 | 1,032 | 52.79 | 1,040 | rel: <= 1,664 (50% over 1,109, +20, recorded 1,040 scaled by host speed 0.94x (group))<br>ratio: gain over v1.2.2 5.68x >= 3.23x (60% of recorded 5.38x) | pass<br>pass |
| n1m | zrandmember_none_us | us | 5,963 | 1,050 | 52.58 | 1,121 | rel: <= 1,794 (50% over 1,196, +20, recorded 1,121 scaled by host speed 0.94x (group))<br>ratio: gain over v1.2.2 5.68x >= 3.00x (60% of recorded 5.00x) | pass<br>pass |

## 17-set-memory

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| ints | bytes_per_member_10 | B | 67.58 | 17.18 | 8.76 | 17.18 | rel: <= 18.18 (5% over 17.18, +1)<br>ratio: gain over v1.2.2 3.93x >= 2.36x (60% of recorded 3.93x) | pass<br>pass |
| ints | bytes_per_member_100 | B | 47.43 | 3.75 | 2.81 | 3.75 | rel: <= 4.75 (5% over 3.75, +1)<br>ratio: gain over v1.2.2 12.65x >= 7.59x (60% of recorded 12.65x) | pass<br>pass |
| ints | bytes_per_member_1000 | B | 58.99 | 58.99 | 22.21 | 58.99 | rel: <= 61.93 (5% over 58.99, +1) | pass |
| ints | bytes_per_member_100000 | B | 46.28 | 46.28 | 28.83 | 46.28 | rel: <= 48.60 (5% over 46.28, +1) | pass |
| ints | encoding_10 | code | 1 | 1 | 1 | 1 | bound: == 1 | pass |
| ints | encoding_100 | code | 1 | 1 | 1 | 1 | bound: == 1 | pass |
| ints | encoding_1000 | code | 3 | 3 | 3 | 3 | bound: == 3 | pass |
| ints | encoding_100000 | code | 3 | 3 | 3 | 3 | bound: == 3 | pass |
| ints | load_100000_s | s | 1.04 | 1.07 | 1.19 | 1.66 | info | info |
| ints | load_1000_s | s | 1 | 1.01 | 1.05 | 1.96 | info | info |
| ints | load_100_s | s | 0.967 | 0.917 | 0.962 | 1.22 | info | info |
| ints | load_10_s | s | 1.48 | 1.42 | 1.47 | 2.8 | info | info |
| strings | bytes_per_member_10 | B | 75.58 | 34.18 | 24.76 | 34.18 | rel: <= 35.89 (5% over 34.18, +1)<br>ratio: gain over v1.2.2 2.21x >= 1.33x (60% of recorded 2.21x) | pass<br>pass |
| strings | bytes_per_member_100 | B | 55.43 | 20.33 | 18.49 | 20.33 | rel: <= 21.34 (5% over 20.33, +1)<br>ratio: gain over v1.2.2 2.73x >= 1.64x (60% of recorded 2.73x) | pass<br>pass |
| strings | bytes_per_member_1000 | B | 66.99 | 66.99 | 29.84 | 66.99 | rel: <= 70.33 (5% over 66.99, +1) | pass |
| strings | bytes_per_member_100000 | B | 54.28 | 54.28 | 36.82 | 54.28 | rel: <= 57.00 (5% over 54.28, +1) | pass |
| strings | encoding_10 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| strings | encoding_100 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| strings | encoding_1000 | code | 3 | 3 | 3 | 3 | bound: == 3 | pass |
| strings | encoding_100000 | code | 3 | 3 | 3 | 3 | bound: == 3 | pass |
| strings | load_100000_s | s | 1.17 | 1.18 | 1.29 | 2.54 | info | info |
| strings | load_1000_s | s | 1.16 | 1.12 | 1.15 | 2.41 | info | info |
| strings | load_100_s | s | 1.13 | 1.07 | 1.42 | 2.37 | info | info |
| strings | load_10_s | s | 1.53 | 1.5 | 1.56 | 3.25 | info | info |

## 19-hash-memory

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| plain | bytes_per_field_10 | B | 162.4 | 71.38 | 56.76 | 71.38 | rel: <= 74.95 (5% over 71.38, +1)<br>ratio: gain over v1.2.2 2.27x >= 1.36x (60% of recorded 2.27x) | pass<br>pass |
| plain | bytes_per_field_100 | B | 129.2 | 55.77 | 51.77 | 55.77 | rel: <= 58.56 (5% over 55.77, +1)<br>ratio: gain over v1.2.2 2.32x >= 1.39x (60% of recorded 2.32x) | pass<br>pass |
| plain | bytes_per_field_1000 | B | 166.6 | 166.6 | 69.91 | 166.6 | rel: <= 175.0 (5% over 166.6, +1) | pass |
| plain | bytes_per_field_100000 | B | 128.8 | 128.8 | 76.82 | 128.8 | rel: <= 135.3 (5% over 128.8, +1) | pass |
| plain | encoding_10 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| plain | encoding_100 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| plain | encoding_1000 | code | 3 | 3 | 3 | 3 | bound: == 3 | pass |
| plain | encoding_100000 | code | 3 | 3 | 3 | 3 | bound: == 3 | pass |
| plain | load_100000_s | s | 2.45 | 2.38 | 2.54 | 2.57 | info | info |
| plain | load_1000_s | s | 2.36 | 2.38 | 3.1 | 2.5 | info | info |
| plain | load_100_s | s | 2.34 | 2.33 | 2.3 | 2.39 | info | info |
| plain | load_10_s | s | 2.74 | 2.66 | 2.61 | 2.8 | info | info |
| ttl | bytes_per_field_10 | B | 192.2 | 101.2 | 72.14 | 101.2 | rel: <= 106.3 (5% over 101.2, +1)<br>ratio: gain over v1.2.2 1.90x >= 1.14x (60% of recorded 1.90x) | pass<br>pass |
| ttl | bytes_per_field_100 | B | 131.5 | 58.11 | 62.26 | 58.11 | rel: <= 61.01 (5% over 58.11, +1)<br>ratio: gain over v1.2.2 2.26x >= 1.36x (60% of recorded 2.26x) | pass<br>pass |
| ttl | bytes_per_field_1000 | B | 166.9 | 166.9 | 70.01 | 166.9 | rel: <= 175.2 (5% over 166.9, +1) | pass |
| ttl | bytes_per_field_100000 | B | 128.9 | 128.9 | 76.83 | 128.9 | rel: <= 135.3 (5% over 128.9, +1) | pass |
| ttl | encoding_10 | code | 4 | 4 | 4 | 4 | bound: == 4 | pass |
| ttl | encoding_100 | code | 4 | 4 | 4 | 4 | bound: == 4 | pass |
| ttl | encoding_1000 | code | 3 | 3 | 3 | 3 | bound: == 3 | pass |
| ttl | encoding_100000 | code | 3 | 3 | 3 | 3 | bound: == 3 | pass |
| ttl | load_100000_s | s | 2.42 | 2.46 | 2.53 | 2.56 | info | info |
| ttl | load_1000_s | s | 2.38 | 2.41 | 3.17 | 2.5 | info | info |
| ttl | load_100_s | s | 2.47 | 2.42 | 2.45 | 2.55 | info | info |
| ttl | load_10_s | s | 3.6 | 3.82 | 3.39 | 3.74 | info | info |

## 21-list-memory

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| packed | bytes_per_element_10 | B | 75.58 | 46.38 | 43.96 | 46.38 | rel: <= 48.70 (5% over 46.38, +1)<br>ratio: gain over v1.2.2 1.63x >= 0.98x (60% of recorded 1.63x) | pass<br>pass |
| packed | bytes_per_element_100 | B | 65.19 | 36.39 | 36.41 | 36.39 | rel: <= 38.21 (5% over 36.39, +1)<br>ratio: gain over v1.2.2 1.79x >= 1.07x (60% of recorded 1.79x) | pass<br>pass |
| packed | bytes_per_element_1000 | B | 64.12 | 34.69 | 34.60 | 34.69 | rel: <= 36.42 (5% over 34.69, +1)<br>ratio: gain over v1.2.2 1.85x >= 1.11x (60% of recorded 1.85x) | pass<br>pass |
| packed | bytes_per_element_100000 | B | 68.48 | 34.66 | 34.31 | 34.66 | rel: <= 36.39 (5% over 34.66, +1)<br>ratio: gain over v1.2.2 1.98x >= 1.19x (60% of recorded 1.98x) | pass<br>pass |
| packed | encoding_10 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| packed | encoding_100 | code | 2 | 5 | 2 | 5 | bound: == 5 | pass |
| packed | encoding_1000 | code | 5 | 5 | 5 | 5 | bound: == 5 | pass |
| packed | encoding_100000 | code | 5 | 5 | 5 | 5 | bound: == 5 | pass |
| packed | load_100000_s | s | 1.09 | 1.07 | 1.14 | 1.08 | info | info |
| packed | load_1000_s | s | 1.11 | 1.07 | 1.16 | 1.08 | info | info |
| packed | load_100_s | s | 1.12 | 1.08 | 1.17 | 1.09 | info | info |
| packed | load_10_s | s | 1.53 | 1.49 | 1.54 | 1.51 | info | info |
| plain | bytes_per_element_10 | B | 77.18 | 77.98 | 44.00 | 77.98 | rel: <= 81.88 (5% over 77.98, +1) | pass |
| plain | bytes_per_element_100 | B | 65.35 | 65.43 | 36.41 | 65.43 | rel: <= 68.70 (5% over 65.43, +1) | pass |
| plain | bytes_per_element_1000 | B | 64.15 | 64.16 | 34.61 | 64.16 | rel: <= 67.37 (5% over 64.16, +1) | pass |
| plain | bytes_per_element_100000 | B | 68.52 | 68.52 | 34.31 | 68.52 | rel: <= 71.95 (5% over 68.52, +1) | pass |
| plain | encoding_10 | code | 2 | 5 | 2 | 5 | bound: == 5 | pass |
| plain | encoding_100 | code | 2 | 5 | 2 | 5 | bound: == 5 | pass |
| plain | encoding_1000 | code | 5 | 5 | 5 | 5 | bound: == 5 | pass |
| plain | encoding_100000 | code | 5 | 5 | 5 | 5 | bound: == 5 | pass |
| plain | load_100000_s | s | 1.11 | 1.09 | 1.13 | 1.09 | info | info |
| plain | load_1000_s | s | 1.13 | 1.12 | 1.14 | 1.13 | info | info |
| plain | load_100_s | s | 1.23 | 1.23 | 1.24 | 1.22 | info | info |
| plain | load_10_s | s | 2.74 | 2.78 | 2.73 | 2.78 | info | info |
| queue | bullmq_200000_bytes_per_element | B | 44.98 | 8.11 | 5.03 | 8.11 | rel: <= 9.11 (5% over 8.11, +1)<br>ratio: gain over v1.2.2 5.55x >= 3.33x (60% of recorded 5.55x) | pass<br>pass |
| queue | memory_usage_bullmq_200000 | % | 11.12 | 99.93 | 96.37 | 99.95 | info | info |
| queue | memory_usage_rotated_200000 | % | 13.34 | 99.98 | 97.34 | 99.98 | info | info |
| queue | memory_usage_sawtooth_low_1000000 | % | 3.13 | 99.99 | 99.31 | 99.99 | info | info |
| queue | rotated_200000_bytes_per_element | B | 44.97 | 8.66 | 5.17 | 8.66 | rel: <= 9.66 (5% over 8.66, +1)<br>ratio: gain over v1.2.2 5.19x >= 3.12x (60% of recorded 5.19x) | pass<br>pass |
| queue | sawtooth_low_1000000_bytes_per_element | B | 191.8 | 9.31 | 5.06 | 9.32 | rel: <= 10.32 (5% over 9.32, +1)<br>ratio: gain over v1.2.2 20.59x >= 12.35x (60% of recorded 20.58x) | pass<br>pass |
| short | bytes_per_element_10 | B | 51.58 | 20.38 | 11.96 | 20.38 | rel: <= 21.40 (5% over 20.38, +1)<br>ratio: gain over v1.2.2 2.53x >= 1.52x (60% of recorded 2.53x) | pass<br>pass |
| short | bytes_per_element_100 | B | 41.19 | 10.39 | 5.69 | 10.39 | rel: <= 11.39 (5% over 10.39, +1)<br>ratio: gain over v1.2.2 3.96x >= 2.38x (60% of recorded 3.96x) | pass<br>pass |
| short | bytes_per_element_1000 | B | 40.12 | 8.69 | 5.17 | 8.69 | rel: <= 9.69 (5% over 8.69, +1)<br>ratio: gain over v1.2.2 4.62x >= 2.77x (60% of recorded 4.62x) | pass<br>pass |
| short | bytes_per_element_100000 | B | 44.48 | 8.66 | 5.04 | 8.66 | rel: <= 9.66 (5% over 8.66, +1)<br>ratio: gain over v1.2.2 5.14x >= 3.08x (60% of recorded 5.14x) | pass<br>pass |
| short | encoding_10 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| short | encoding_100 | code | 2 | 5 | 2 | 5 | bound: == 5 | pass |
| short | encoding_1000 | code | 5 | 5 | 2 | 5 | bound: == 5 | pass |
| short | encoding_100000 | code | 5 | 5 | 5 | 5 | bound: == 5 | pass |
| short | load_100000_s | s | 1.01 | 0.985 | 1.05 | 0.987 | info | info |
| short | load_1000_s | s | 1.03 | 0.983 | 1.05 | 0.98 | info | info |
| short | load_100_s | s | 1.02 | 0.966 | 1.03 | 0.966 | info | info |
| short | load_10_s | s | 1.46 | 1.44 | 1.49 | 1.43 | info | info |

## 22-scan-large-count

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| t4 | dbsize | keys | 40,000 | 40,000 | 40,000 | 40,000 | bound: == 40,000 | pass |
| t4 | ping_max_ms | ms | 3.61 | 2.48 | 10.88 | 5.54 | info | info |
| t4 | ping_p99_ms | ms | 2.18 | 0.699 | 3.08 | 0.542 | rel: <= 1.54 (50% over 0.542, +1) | pass |
| t4 | scan_call_us | us | 665.7 | 145.1 | 867.1 | 144.1 | rel: <= 160.1 (25% over 128.1, +30, recorded 144.1 scaled by host speed 1.12x (group)) | pass |
| t4 | scan_calls | calls | 4 | 4 | 4 | 4 | info | info |
| t4 | scan_returned | keys | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| t4 | scan_server_us | us | 579.3 | 75.38 | 741.6 | 75.50 | rel: <= 97.13 (25% over 67.13, +30, recorded 75.50 scaled by host speed 1.12x (group))<br>ratio: gain over v1.2.2 7.69x >= 4.99x (60% of recorded 8.32x) | pass<br>pass |

## 23-pubsub-pattern-fanout

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| channel | delivered_per_s | msg/s | 1,469,540 | 1,469,235 | 1,424,919 | 1,468,950 | rel: >= 1,344,942 (10% under 1,494,380, recorded 1,468,950 scaled by host speed 1.02x (host))<br>vs_redis: 0.97x Redis <= 1.1x | pass<br>pass |
| channel | delivery_p99_ms | ms | 0.748 | 1.02 | 0.885 | 0.685 | rel: <= 1.68 (50% over 0.685, +1) | pass |
| channel | lost | messages | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| channel | published_per_s | req/s | 91,912 | 91,884 | 89,113 | 91,870 | info | info |
| pattern | delivered_per_s | msg/s | 1,302,712 | 1,457,667 | 1,369,815 | 1,467,643 | rel: >= 1,343,745 (10% under 1,493,050, recorded 1,467,643 scaled by host speed 1.02x (host))<br>vs_redis: 0.94x Redis <= 1.1x | pass<br>pass |
| pattern | delivery_p99_ms | ms | 0.83 | 0.956 | 0.906 | 0.765 | rel: <= 1.77 (50% over 0.765, +1) | pass |
| pattern | lost | messages | 0 | 0 | 0 | 0 | bound: == 0 | pass |
| pattern | published_per_s | req/s | 80,548 | 90,226 | 84,722 | 90,840 | info | info |

## 24-snapshot-size

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| coll-default | keys | keys | 350,000 | 350,000 | 350,000 | 350,000 | info | info |
| coll-default | load_ms | ms | 1,104 | 1,004 | 506.0 | 1,004 | vs_v1_2_2: 0.91x v1.2.2 <= 1.5x | pass |
| coll-default | save_ms | ms | 801.3 | 851.5 | 945.2 | 803.0 | vs_v1_2_2: 1.06x v1.2.2 <= 2.5x | pass |
| coll-default | snapshot_mb | MB | 107.5 | 107.5 | 84.11 | 107.5 | vs_v1_2_2: 1.00x v1.2.2 <= 1.01x | pass |
| coll-yes | keys | keys | 350,000 | 350,000 | 350,000 | 350,000 | info | info |
| coll-yes | load_ms | ms | 1,104 | 1,104 | 506.9 | 1,205 | vs_v1_2_2: 1.00x v1.2.2 <= 1.5x | pass |
| coll-yes | save_ms | ms | 794.9 | 491.3 | 952.3 | 476.1 | vs_v1_2_2: 0.62x v1.2.2 <= 2.5x | pass |
| coll-yes | snapshot_mb | MB | 107.5 | 46.09 | 84.11 | 46.09 | ratio: gain over v1.2.2 2.33x >= 1.40x (60% of recorded 2.33x) | pass |
| issue-default | keys | keys | 6,000,000 | 6,000,000 | 6,000,000 | 6,000,000 | info | info |
| issue-default | load_ms | ms | 7,422 | 7,221 | 5,352 | 7,021 | vs_v1_2_2: 0.97x v1.2.2 <= 1.5x | pass |
| issue-default | save_ms | ms | 9,767 | 10,060 | 6,986 | 6,102 | vs_v1_2_2: 1.03x v1.2.2 <= 2.5x | pass |
| issue-default | snapshot_mb | MB | 1,007 | 1,007 | 213.8 | 1,007 | vs_v1_2_2: 1.00x v1.2.2 <= 1.01x | pass |
| issue-yes | keys | keys | 6,000,000 | 6,000,000 | 6,000,000 | 6,000,000 | info | info |
| issue-yes | load_ms | ms | 7,427 | 7,221 | 5,359 | 7,325 | vs_v1_2_2: 0.97x v1.2.2 <= 1.5x | pass |
| issue-yes | save_ms | ms | 8,862 | 1,354 | 6,870 | 1,375 | vs_v1_2_2: 0.15x v1.2.2 <= 2.5x | pass |
| issue-yes | snapshot_mb | MB | 1,007 | 49.47 | 213.8 | 49.47 | ratio: gain over v1.2.2 20.35x >= 12.21x (60% of recorded 20.35x)<br>vs_redis: 0.23x Redis <= 1.5x | pass<br>pass |

## 25-centrifuge-scripts-publish

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| publish | publish_8x16_per_s | calls/s | 1,357,920 | 1,282,819 | 1,250,712 | 1,554,222 | rel: >= 1,252,407 (10% under 1,391,563, recorded 1,554,222 scaled by host speed 0.90x (host))<br>vs_v1_2_2: 1.06x v1.2.2 <= 1.08x | pass<br>pass |
| publish | publish_p16_per_s | calls/s | 364,530 | 375,001 | 402,814 | 382,411 | rel: >= 308,150 (10% under 342,389, recorded 382,411 scaled by host speed 0.90x (host))<br>vs_v1_2_2: 0.97x v1.2.2 <= 1.05x | pass<br>pass |
| scripts | add_calls_per_s | calls/s | 45,738 | 62,609 | 58,393 | 50,280 | rel: >= 40,516 (10% under 45,018, recorded 50,280 scaled by host speed 0.90x (host))<br>vs_v1_2_2: 0.73x v1.2.2 <= 1.05x | pass<br>pass |
| scripts | history_calls_per_s | calls/s | 55,400 | 83,738 | 66,736 | 60,044 | rel: >= 48,384 (10% under 53,760, recorded 60,044 scaled by host speed 0.90x (host))<br>vs_v1_2_2: 0.66x v1.2.2 <= 1.05x | pass<br>pass |

## 27-ratelimit-restarted

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| t2 | checks_per_s_bg0k | checks/s | 31,407 | 31,378 | 31,240 | 31,168 | rel: >= 28,376 (10% under 31,528, recorded 31,168 scaled by host speed 1.01x (group))<br>vs_redis: 1.00x Redis <= 1.05x | pass<br>pass |
| t2 | checks_per_s_bg100k | checks/s | 28,828 | 31,372 | 31,829 | 31,399 | rel: >= 28,586 (10% under 31,762, recorded 31,399 scaled by host speed 1.01x (group))<br>vs_redis: 1.01x Redis <= 1.05x | pass<br>pass |
| t2 | checks_per_s_bg50k | checks/s | 30,886 | 31,553 | 31,830 | 31,553 | rel: >= 28,726 (10% under 31,918, recorded 31,553 scaled by host speed 1.01x (group))<br>vs_redis: 1.01x Redis <= 1.05x | pass<br>pass |
| t2 | p99_ms_bg0k | ms | 0.232 | 0.237 | 0.738 | 0.218 | rel: <= 0.718 (100% over 0.218, +0.5) | pass |
| t2 | p99_ms_bg100k | ms | 2.36 | 0.181 | 0.096 | 0.19 | rel: <= 0.69 (100% over 0.19, +0.5)<br>ratio: gain over v1.2.2 13.04x >= 19.32x (60% of recorded 32.20x) | **FAIL** |
| t2 | p99_ms_bg50k | ms | 0.853 | 0.142 | 0.089 | 0.145 | rel: <= 0.645 (100% over 0.145, +0.5)<br>ratio: gain over v1.2.2 6.01x >= 6.45x (60% of recorded 10.75x) | **FAIL** |

## 30-script-call-cost

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| scripts | lock_calls_per_s | calls/s | 96,117 | 102,919 | 113,920 | 111,013 | rel: >= 91,455 (10% under 101,616, recorded 111,013 scaled by host speed 0.92x (group)) | pass |
| scripts | lock_cpu_us_per_call | us | 10.40 | 9.73 | 8.78 | 9.01 | rel: <= 10.82 (10% over 9.84, recorded 9.01 scaled by host speed 0.92x (group))<br>vs_v1_2_2: 0.94x v1.2.2 <= 1.03x | pass<br>pass |
| scripts | lock_tag_calls_per_s | calls/s | 92,829 | 98,788 | 109,313 | 106,473 | rel: >= 87,715 (10% under 97,461, recorded 106,473 scaled by host speed 0.92x (group)) | pass |
| scripts | lock_tag_cpu_us_per_call | us | 10.77 | 10.12 | 9.15 | 9.4 | rel: <= 11.30 (10% over 10.27, recorded 9.4 scaled by host speed 0.92x (group))<br>vs_v1_2_2: 0.94x v1.2.2 <= 1.03x | pass<br>pass |
| scripts | return1_calls_per_s | calls/s | 115,248 | 114,764 | 143,610 | 125,187 | rel: >= 103,131 (10% under 114,590, recorded 125,187 scaled by host speed 0.92x (group)) | pass |
| scripts | return1_cpu_us_per_call | us | 8.69 | 8.71 | 6.95 | 8 | rel: <= 9.61 (10% over 8.74, recorded 8 scaled by host speed 0.92x (group))<br>vs_v1_2_2: 1.00x v1.2.2 <= 1.03x | pass<br>pass |
| scripts | return1_tag_calls_per_s | calls/s | 110,247 | 110,012 | 130,022 | 116,000 | rel: >= 95,563 (10% under 106,181, recorded 116,000 scaled by host speed 0.92x (group)) | pass |
| scripts | return1_tag_cpu_us_per_call | us | 9.09 | 9.08 | 7.68 | 8.65 | rel: <= 10.40 (10% over 9.45, recorded 8.65 scaled by host speed 0.92x (group))<br>vs_v1_2_2: 1.00x v1.2.2 <= 1.03x | pass<br>pass |
| scripts | tyk_calls_per_s | calls/s | 60,071 | 69,147 | 70,503 | 79,574 | rel: >= 65,554 (10% under 72,838, recorded 79,574 scaled by host speed 0.92x (group)) | pass |
| scripts | tyk_cpu_us_per_call | us | 16.64 | 14.46 | 14.16 | 12.60 | rel: <= 15.14 (10% over 13.76, recorded 12.60 scaled by host speed 0.92x (group))<br>vs_v1_2_2: 0.87x v1.2.2 <= 0.93x | pass<br>pass |
| scripts | tyk_tag_calls_per_s | calls/s | 55,496 | 66,118 | 75,447 | 74,439 | rel: >= 61,324 (10% under 68,138, recorded 74,439 scaled by host speed 0.92x (group)) | pass |
| scripts | tyk_tag_cpu_us_per_call | us | 18.02 | 15.13 | 13.25 | 13.47 | rel: <= 16.18 (10% over 14.71, recorded 13.47 scaled by host speed 0.92x (group))<br>vs_v1_2_2: 0.84x v1.2.2 <= 0.93x | pass<br>pass |

## 31-zset-memory

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| float | bytes_per_member_10 | B | 118.8 | 37.58 | 27.96 | 37.58 | rel: <= 39.46 (5% over 37.58, +1)<br>ratio: gain over v1.2.2 3.16x >= 1.90x (60% of recorded 3.16x) | pass<br>pass |
| float | bytes_per_member_100 | B | 116.4 | 27.31 | 26.16 | 27.31 | rel: <= 28.67 (5% over 27.31, +1)<br>ratio: gain over v1.2.2 4.26x >= 2.56x (60% of recorded 4.26x) | pass<br>pass |
| float | bytes_per_member_1000 | B | 132.5 | 132.5 | 68.26 | 132.5 | rel: <= 139.1 (5% over 132.5, +1) | pass |
| float | bytes_per_member_128 | B | 131.7 | 26.96 | 24.42 | 26.96 | rel: <= 28.31 (5% over 26.96, +1)<br>ratio: gain over v1.2.2 4.89x >= 2.93x (60% of recorded 4.89x) | pass<br>pass |
| float | bytes_per_member_129 | B | 130.9 | 131.2 | 78.67 | 131.2 | rel: <= 137.7 (5% over 131.2, +1) | pass |
| float | encoding_10 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| float | encoding_100 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| float | encoding_1000 | code | 5 | 5 | 5 | 5 | bound: == 5 | pass |
| float | encoding_128 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| float | encoding_129 | code | 5 | 5 | 5 | 5 | bound: == 5 | pass |
| float | load_1000_s | s | 2.26 | 2.21 | 2.33 | 2 | info | info |
| float | load_100_s | s | 2.2 | 2.14 | 3.83 | 1.87 | info | info |
| float | load_10_s | s | 2.56 | 2.47 | 2.77 | 2.24 | info | info |
| float | load_128_s | s | 2.2 | 2.14 | 4.2 | 1.86 | info | info |
| float | load_129_s | s | 2.19 | 2.2 | 2.3 | 1.97 | info | info |
| int | bytes_per_member_10 | B | 118.8 | 29.58 | 24.76 | 29.58 | rel: <= 31.06 (5% over 29.58, +1)<br>ratio: gain over v1.2.2 4.02x >= 2.41x (60% of recorded 4.02x) | pass<br>pass |
| int | bytes_per_member_100 | B | 116.4 | 19.31 | 21.04 | 19.31 | rel: <= 20.31 (5% over 19.31, +1)<br>ratio: gain over v1.2.2 6.03x >= 3.62x (60% of recorded 6.03x) | pass<br>pass |
| int | bytes_per_member_1000 | B | 132.5 | 132.5 | 68.04 | 132.5 | rel: <= 139.1 (5% over 132.5, +1) | pass |
| int | bytes_per_member_128 | B | 131.7 | 18.96 | 20.42 | 18.96 | rel: <= 19.96 (5% over 18.96, +1)<br>ratio: gain over v1.2.2 6.95x >= 4.17x (60% of recorded 6.95x) | pass<br>pass |
| int | bytes_per_member_129 | B | 130.9 | 131.2 | 77.93 | 131.2 | rel: <= 137.7 (5% over 131.2, +1) | pass |
| int | encoding_10 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| int | encoding_100 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| int | encoding_1000 | code | 5 | 5 | 5 | 5 | bound: == 5 | pass |
| int | encoding_128 | code | 2 | 2 | 2 | 2 | bound: == 2 | pass |
| int | encoding_129 | code | 5 | 5 | 5 | 5 | bound: == 5 | pass |
| int | load_1000_s | s | 2.24 | 2.21 | 2.3 | 1.97 | info | info |
| int | load_100_s | s | 2.19 | 2.11 | 3.25 | 1.84 | info | info |
| int | load_10_s | s | 2.53 | 2.44 | 2.64 | 2.16 | info | info |
| int | load_128_s | s | 2.18 | 2.09 | 3.56 | 1.82 | info | info |
| int | load_129_s | s | 2.2 | 2.21 | 2.25 | 1.96 | info | info |

## 32-default-threads

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| shared | default_threads | threads | 2 | 4 | - | 4 | info | info |
| shared | get_c50_rps_best | req/s | 120,159 | 118,176 | - | 118,057 | info | info |
| shared | get_c50_rps_best_threads | threads | 1 | 1 | - | 1 | info | info |
| shared | get_c50_rps_default | req/s | 113,919 | 85,426 | - | 85,926 | info | info |
| shared | get_c50_rps_default_vs_best | ratio | 0.948 | 0.723 | - | 0.728 | info | info |
| shared | pubsub_lost | messages | 0 | 0 | - | 0 | bound: == 0 | pass |
| shared | pubsub_per_s_best | msg/s | 1,267,172 | 1,507,938 | - | 1,442,740 | info | info |
| shared | pubsub_per_s_best_threads | threads | 2 | 2 | - | 2 | info | info |
| shared | pubsub_per_s_default | msg/s | 1,312,256 | 1,278,149 | - | 1,249,651 | info | info |
| shared | pubsub_per_s_default_vs_best | ratio | 1.04 | 0.848 | - | 0.866 | info | info |
| shared | set_p16_rps_best | req/s | 1,316,156 | 1,215,596 | - | 1,169,911 | info | info |
| shared | set_p16_rps_best_threads | threads | 2 | 2 | - | 2 | info | info |
| shared | set_p16_rps_default | req/s | 1,130,140 | 1,041,675 | - | 1,038,501 | info | info |
| shared | set_p16_rps_default_vs_best | ratio | 0.859 | 0.857 | - | 0.888 | info | info |
| shared | xadd5_rps_best | req/s | 434,180 | 551,852 | - | 560,360 | info | info |
| shared | xadd5_rps_best_threads | threads | 1 | 1 | - | 1 | info | info |
| shared | xadd5_rps_default | req/s | 316,416 | 362,922 | - | 372,449 | info | info |
| shared | xadd5_rps_default_vs_best | ratio | 0.729 | 0.658 | - | 0.665 | info | info |

## 33-ft-aggregate-groupby

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| docs2m | avg_groups | groups | 10.00 | 1,000 | - | 1,000 | bound: == 1,000 | pass |
| docs2m | avg_ms | ms | 3,645 | 1,008 | - | 1,033 | rel: <= 1,151 (10% over 1,047, recorded 1,033 scaled by host speed 0.99x (group))<br>ratio: gain over v1.2.2 3.62x >= 2.08x (60% of recorded 3.47x) | pass<br>pass |
| docs2m | bytes_per_doc | B | 1,366 | 640.8 | - | 640.9 | info | info |
| docs2m | count_groups | groups | 10.00 | 1,000 | - | 1,000 | bound: == 1,000 | pass |
| docs2m | count_ms | ms | 3,227 | 912.6 | - | 913.2 | rel: <= 1,018 (10% over 925.6, recorded 913.2 scaled by host speed 0.99x (group))<br>ratio: gain over v1.2.2 3.54x >= 2.09x (60% of recorded 3.48x) | pass<br>pass |
| docs2m | filtered_groups | groups | 10.00 | 1,000 | - | 1,000 | bound: == 1,000 | pass |
| docs2m | filtered_ms | ms | 1,194 | 343.3 | - | 358.1 | rel: <= 399.3 (10% over 363.0, recorded 358.1 scaled by host speed 0.99x (group))<br>ratio: gain over v1.2.2 3.48x >= 1.99x (60% of recorded 3.31x) | pass<br>pass |
| docs2m | load_s | s | 34.15 | 31.78 | - | 31.57 | info | info |
| docs2m | postings_ids_mib | MiB | 188.8 | 122.4 | - | 122.4 | info | info |
| docs2m | postings_numeric_mib | MiB | 94.04 | 94.04 | - | 94.04 | info | info |
| docs2m | postings_tag_mib | MiB | 282.5 | 198.6 | - | 198.6 | info | info |
| docs2m | rss_loaded_mib | MiB | 3,245 | 1,464 | - | 1,488 | rel: <= 1,563 (5% over 1,488)<br>ratio: gain over v1.2.2 2.22x >= 1.31x (60% of recorded 2.18x) | pass<br>pass |
| docs2m | rss_peak_load_mib | MiB | 3,293 | 1,518 | - | 1,509 | info | info |
| docs2m | rss_peak_mib | MiB | 4,793 | 1,518 | - | 1,543 | rel: <= 1,697 (10% over 1,543)<br>ratio: gain over v1.2.2 3.16x >= 1.86x (60% of recorded 3.10x) | pass<br>pass |
| docs2m | text_docs_mib | MiB | 54.89 | 54.89 | - | 54.89 | info | info |
| docs2m | text_postings_mib | MiB | 335.9 | 0.25 | - | 0.25 | info | info |
| docs2m | text_terms_mib | MiB | 125.8 | 157.8 | - | 157.8 | info | info |
| docs2m | used_memory_mib | MiB | 2,607 | 1,224 | - | 1,224 | rel: <= 1,285 (5% over 1,224)<br>ratio: gain over v1.2.2 2.13x >= 1.28x (60% of recorded 2.13x) | pass<br>pass |

## Not run this time

`18-mastodon-feed-fanout/default/bytes_per_member`, `18-mastodon-feed-fanout/default/feeds`, `18-mastodon-feed-fanout/default/ops_c200`, `18-mastodon-feed-fanout/default/ops_c500`, `18-mastodon-feed-fanout/default/p99_ms_c200`, `18-mastodon-feed-fanout/default/p99_ms_c500`, `18-mastodon-feed-fanout/default/read_p50_ms_c200`, `18-mastodon-feed-fanout/default/read_p50_ms_c500`, `18-mastodon-feed-fanout/default/read_p99_ms_c200`, `18-mastodon-feed-fanout/default/read_p99_ms_c500`, `18-mastodon-feed-fanout/default/used_memory_mib`, `20-envoy-ratelimit/default/keys_without_ttl`, `20-envoy-ratelimit/default/ops_c200`, `20-envoy-ratelimit/default/ops_c500`, `20-envoy-ratelimit/default/p99_ms_c200`, `20-envoy-ratelimit/default/p99_ms_c500`

