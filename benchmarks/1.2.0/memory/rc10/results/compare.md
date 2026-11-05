| section | metric | rc10 | rc8 | change | spread | verdict |
|---|---|---|---|---|---|---|
| warefeats ASCII | 1st M RSS B/key | 149.0 | 149.2 | -0.1% | 0.2% (rc8 round) | level |
| warefeats ASCII | 1st M used B/key | 146.9 | 146.9 | 0.0% | 0.0% (rc8 round) | level |
| warefeats ASCII | 2nd M RSS B/key | 154.8 | 146.7 | 5.5% | 5.6% (rc8 round) | level |
| warefeats random | 1st M RSS B/key | 149.1 | 149.2 | -0.1% | 0.2% (rc8 round) | level |
| warefeats random | 1st M used B/key | 146.9 | 146.9 | 0.0% | 0.0% (rc8 round) | level |
| warefeats random | 2nd M RSS B/key | 147.5 | 146.7 | 0.5% | 0.1% (rc8 round) | level (under 1%) |
| warefeats set-get-small | ops/s | 1,323,766 | 1,358,514 | -2.6% | 7.5% (rc8 round) | level |
| warefeats set-get-small | p99 ms | 0.327 | 0.323 | 1.2% | 3.2% (rc8 round) | level |
| warefeats set-get-small | p99.9 ms | 0.379 | 0.379 | 0.0% | 23.1% (rc8 round) | level |
| warefeats set-get-large | ops/s | 320,877 | 320,604 | 0.1% | 0.1% (rc8 round) | level |
| warefeats set-get-large | p99 ms | 1.035 | 1.111 | -6.8% | 10.2% (rc8 round) | level |
| warefeats set-get-large | p99.9 ms | 1.223 | 1.235 | -1.0% | 3.8% (rc8 round) | level |
| warefeats pipeline-30 | ops/s | 17,704,944 | 17,666,123 | 0.2% | 4.1% (rc8 round) | level |
| warefeats pipeline-30 | p99 ms | 0.639 | 0.639 | 0.0% | 3.8% (rc8 round) | level |
| warefeats pipeline-30 | p99.9 ms | 0.747 | 0.747 | 0.0% | 5.1% (rc8 round) | level |
| growth 100 B | RSS B/key at 1M | 153.5 (153.0-153.9) | 153.5 (153.3-153.6) | 0.0% | 0.6% (own) | level |
| growth 100 B | RSS B/key at 5M | 146.5 (146.3-146.7) | 146.3 (146.3-146.4) | 0.1% | 0.3% (own) | level |
| growth 100 B | RSS B/key at 10M | 144.9 (144.9-144.9) | 144.9 (144.8-144.9) | 0.0% | 0.2% (rc8 round) | level |
| growth 100 B | RSS B/key at 25M | 141.0 (141.0-141.0) | 141.0 (141.0-141.0) | -0.0% | 0.1% (rc8 round) | level |
| growth 100 B | RSS B/key at 50M | 140.7 (140.7-140.7) | 140.7 (140.6-140.7) | -0.0% | 0.0% (own) | level |
| growth 100 B | load seconds, 50M keys | 12.5 (12.1-13.0) | 12.6 (12.0-13.1) | -0.3% | 8.6% (own) | level |
| growth 1024 B | RSS B/key at 1M | 1085.2 | 1085.9 | -0.1% | 0.0% (rc8 round) | level |
| growth 1024 B | RSS B/key at 5M | 1077.9 | 1077.5 | 0.0% | 0.0% (rc8 round) | level (under 1%) |
| growth 1024 B | RSS B/key at 10M | 1076.2 | 1076.1 | 0.0% | 0.0% (rc8 round) | level (under 1%) |
| growth 1024 B | RSS B/key at 25M | 1072.0 | 1072.0 | 0.0% | 0.0% (rc8 round) | level (under 1%) |
| growth 1024 B | RSS B/key at 50M | 1071.7 | 1071.6 | 0.0% | 0.0% (rc8 round) | level (under 1%) |
| growth 1024 B | load seconds, 50M keys | 23.0 | 22.9 | 0.2% | 0.8% (rc8 round) | level |
| mixed 20 min | RSS at end GiB | 3.94 | 3.92 | 0.5% | 0.5% (rc8 round) | level |
| mixed 20 min | RSS peak GiB | 4.45 | 4.43 | 0.4% | 0.3% (rc8 round) | level (under 1%) |
| mixed 20 min | RSS/used at end | 1.15 | 1.14 | 0.5% | 0.5% (rc8 round) | level |
| mixed 20 min | GET p99 ms | 0.43 | 0.46 | -6.9% | 8.7% (rc8 round) | level |
| mixed 20 min | GET p99.9 ms | 0.57 | 0.69 | -16.3% | 21.9% (rc8 round) | level |
| mixed 20 min | SET p99 ms | 0.45 | 0.49 | -9.7% | 15.7% (rc8 round) | level |
| mixed 20 min | SET p99.9 ms | 0.69 | 0.81 | -13.9% | 24.4% (rc8 round) | level |
| snapshot BGSAVE | memory added GiB | 3.56 | 3.54 (3.53-3.54) | 0.6% | 1.9% (rc8 round) | level |
| snapshot BGSAVE | duration s | 81.4 | 81.9 (81.9-81.9) | -0.6% | 1.4% (rc8 round) | level |
| snapshot BGSAVE | worst write ms | 206 | 208 (206-210) | -0.7% | 10.8% (rc8 round) | level |
| snapshot BGSAVE | write p99.9 ms | 1.85 | 1.98 (1.97-1.99) | -6.3% | 11.0% (rc8 round) | level |
| snapshot BGSAVE | write p99 before ms | 1.40 | 1.29 (1.22-1.36) | 8.4% | 10.5% (own) | level |
| snapshot BGSAVE | memory 60 s after GiB | 33.81 | 33.82 (33.81-33.82) | -0.0% | 0.1% (rc8 round) | level |
| snapshot BGREWRITEAOF | memory added GiB | 2.34 | 2.37 (2.36-2.38) | -1.1% | 6.6% (rc8 round) | level |
| snapshot BGREWRITEAOF | duration s | 83.0 | 83.9 (83.9-84.0) | -1.2% | 0.9% (rc8 round) | better |
| snapshot BGREWRITEAOF | worst write ms | 218 | 219 (214-224) | -0.4% | 4.6% (own) | level |
| snapshot BGREWRITEAOF | write p99.9 ms | 1.79 | 1.85 (1.84-1.85) | -3.0% | 15.6% (rc8 round) | level |
| snapshot BGREWRITEAOF | write p99 before ms | 1.36 | 1.36 (1.30-1.42) | -0.3% | 16.6% (rc8 round) | level |
| snapshot BGREWRITEAOF | memory 60 s after GiB | 35.61 | 35.61 (35.61-35.61) | 0.0% | 1.9% (rc8 round) | level |
| #284 eviction | keys evicted | 0 | 0 | -% | 0.0% (rc8 round) | - |
| active defrag | RSS final MB | 387 | 389 | -0.3% | 3.9% (rc8 round) | level |
| active defrag | RSS returned MB | 335 | 334 | 0.4% | 4.7% (rc8 round) | level |
| active defrag | give-back s | 12.1 | 13.0 | -6.4% | 21.2% (rc8 round) | level |
| active defrag | compaction s | 13 | 13 | 0.0% | 28.6% (rc8 round) | level |
| active defrag | slab final MB | 321 | 321 | -0.1% | 4.7% (rc8 round) | level |
| active defrag | GET p99 during compaction ms | 0.44 | 0.33 | 34.3% | 74.1% (rc8 round) | level |
| active defrag | GET p99.9 during compaction ms | 0.80 | 0.72 | 11.1% | 57.2% (rc8 round) | level |
| active defrag | SET p99 during compaction ms | 0.20 | 0.12 | 67.2% | 91.1% (rc8 round) | level |
| active defrag | SET p99.9 during compaction ms | 0.34 | 0.14 | 154.1% | 104.7% (rc8 round) | **worse beyond spread** |
| active defrag | GET p99 during / before (same run) | 1.02 | 1.00 | 1.9% | 22.9% (rc8 round) | level |
| active defrag | GET p99.9 during / before (same run) | 1.14 | 0.97 | 16.8% | 26.1% (rc8 round) | level |
| active defrag | SET p99 during / before (same run) | 1.00 | 0.94 | 6.7% | 13.2% (rc8 round) | level |
| active defrag | SET p99.9 during / before (same run) | 0.91 | 1.00 | -8.5% | 34.1% (rc8 round) | level |
| AOF-on writes | SET p50 ms | 0.335 (0.327-0.391) | 0.399 (0.367-0.407) | -16.0% | 19.1% (own) | level |
| AOF-on writes | SET p99 ms | 0.871 (0.863-0.975) | 0.999 (0.927-1.007) | -12.8% | 12.9% (own) | level |
| AOF-on writes | SET p99.9 ms | 1.191 (1.183-1.351) | 1.415 (1.279-1.423) | -15.8% | 44.5% (rc8 round) | level |
| AOF-on writes | worst write ms | 9 (6-10) | 57 (11-182) | -83.8% | 296.9% (own) | level |
| AOF-on writes | engine CPU s | 558 (530-567) | 557 (553-558) | 0.2% | 6.6% (own) | level |

Flagged: active defrag SET p99.9 during compaction ms (+154.1%, spread 104.7%)

### rc10 against the rc8 round's published rc8 numbers (other session, other VMs of the same sizes)

| section | metric | rc10 (this run) | rc8 (rc8 round, median) | change |
|---|---|---|---|---|
| warefeats ASCII | 1st M RSS B/key | 149.0 | 149.1 | -0.1% |
| warefeats ASCII | 1st M used B/key | 146.9 | 146.9 | 0.0% |
| warefeats ASCII | 2nd M RSS B/key | 154.8 | 146.9 | 5.4% |
| warefeats random | 1st M RSS B/key | 149.1 | 149.2 | -0.1% |
| warefeats random | 1st M used B/key | 146.9 | 146.9 | 0.0% |
| warefeats random | 2nd M RSS B/key | 147.5 | 146.7 | 0.5% |
| warefeats set-get-small | ops/s | 1,323,766 | 1,310,912 | 1.0% |
| warefeats set-get-small | p99 ms | 0.327 | 0.335 | -2.4% |
| warefeats set-get-small | p99.9 ms | 0.379 | 0.391 | -3.1% |
| warefeats set-get-large | ops/s | 320,877 | 318,999 | 0.6% |
| warefeats set-get-large | p99 ms | 1.035 | 1.195 | -13.4% |
| warefeats set-get-large | p99.9 ms | 1.223 | 1.275 | -4.1% |
| warefeats pipeline-30 | ops/s | 17,704,944 | 17,298,680 | 2.3% |
| warefeats pipeline-30 | p99 ms | 0.639 | 0.651 | -1.8% |
| warefeats pipeline-30 | p99.9 ms | 0.747 | 0.767 | -2.6% |
| growth 100 B | RSS B/key at 1M | 153.5 | 153.5 | -0.0% |
| growth 100 B | RSS B/key at 5M | 146.5 | 146.7 | -0.2% |
| growth 100 B | RSS B/key at 10M | 144.9 | 145.0 | -0.1% |
| growth 100 B | RSS B/key at 25M | 141.0 | 141.0 | -0.0% |
| growth 100 B | RSS B/key at 50M | 140.7 | 140.7 | -0.0% |
| growth 100 B | load seconds, 50M keys | 12.5 | 13.0 | -4.0% |
| growth 1024 B | RSS B/key at 1M | 1085.2 | 1085.8 | -0.1% |
| growth 1024 B | RSS B/key at 5M | 1077.9 | 1077.7 | 0.0% |
| growth 1024 B | RSS B/key at 10M | 1076.2 | 1076.1 | 0.0% |
| growth 1024 B | RSS B/key at 25M | 1072.0 | 1072.0 | 0.0% |
| growth 1024 B | RSS B/key at 50M | 1071.7 | 1071.6 | 0.0% |
| growth 1024 B | load seconds, 50M keys | 23.0 | 23.0 | -0.1% |
| mixed 20 min | RSS at end GiB | 3.94 | 3.93 | 0.2% |
| mixed 20 min | RSS peak GiB | 4.45 | 4.44 | 0.1% |
| mixed 20 min | RSS/used at end | 1.15 | 1.15 | 0.2% |
| mixed 20 min | GET p99 ms | 0.43 | 0.46 | -6.1% |
| mixed 20 min | GET p99.9 ms | 0.57 | 0.63 | -8.9% |
| mixed 20 min | SET p99 ms | 0.45 | 0.48 | -6.7% |
| mixed 20 min | SET p99.9 ms | 0.69 | 0.76 | -8.9% |
| snapshot BGSAVE | memory added GiB | 3.56 | 3.57 | -0.2% |
| snapshot BGSAVE | duration s | 81.4 | 82.2 | -0.9% |
| snapshot BGSAVE | worst write ms | 206 | 214 | -3.4% |
| snapshot BGSAVE | write p99.9 ms | 1.85 | 1.92 | -3.1% |
| snapshot BGSAVE | write p99 before ms | 1.40 | 1.44 | -2.8% |
| snapshot BGSAVE | memory 60 s after GiB | 33.81 | 33.84 | -0.1% |
| snapshot BGREWRITEAOF | memory added GiB | 2.34 | 2.40 | -2.4% |
| snapshot BGREWRITEAOF | duration s | 83.0 | 83.4 | -0.5% |
| snapshot BGREWRITEAOF | worst write ms | 218 | 216 | 0.8% |
| snapshot BGREWRITEAOF | write p99.9 ms | 1.79 | 1.87 | -4.3% |
| snapshot BGREWRITEAOF | write p99 before ms | 1.36 | 1.34 | 1.2% |
| snapshot BGREWRITEAOF | memory 60 s after GiB | 35.61 | 35.93 | -0.9% |
| #284 eviction | keys evicted | 0 | 0 | -% |
| active defrag | RSS final MB | 387 | 389 | -0.4% |
| active defrag | RSS returned MB | 335 | 334 | 0.5% |
| active defrag | give-back s | 12.1 | 12.0 | 1.5% |
| active defrag | compaction s | 13 | 12 | 4.0% |
| active defrag | slab final MB | 321 | 321 | -0.1% |
| active defrag | GET p99 during compaction ms | 0.44 | 0.43 | 1.9% |
| active defrag | GET p99.9 during compaction ms | 0.80 | 0.70 | 13.7% |
| active defrag | SET p99 during compaction ms | 0.20 | 0.42 | -52.3% |
| active defrag | SET p99.9 during compaction ms | 0.34 | 0.53 | -35.9% |
| active defrag | GET p99 during / before (same run) | 1.02 | 0.99 | 2.6% |
| active defrag | GET p99.9 during / before (same run) | 1.14 | 0.99 | 14.5% |
| active defrag | SET p99 during / before (same run) | 1.00 | 1.02 | -2.1% |
| active defrag | SET p99.9 during / before (same run) | 0.91 | 1.05 | -12.9% |
| AOF-on writes | SET p50 ms | 0.335 | 0.399 | -16.0% |
| AOF-on writes | SET p99 ms | 0.871 | 0.983 | -11.4% |
| AOF-on writes | SET p99.9 ms | 1.191 | 1.679 | -29.1% |
| AOF-on writes | worst write ms | 9 | 134 | -93.1% |
| AOF-on writes | engine CPU s | 558 | 562 | -0.6% |
