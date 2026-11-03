# memory suite: generated tables

## warefeats/kv-stores harness (a324d29)

rc10 and rc8: median (min-max) over their rounds; one run for the others.

### memory-load: 1M keys x 100 B, then a second million on the same engine

| engine | runs | 1st M RSS B/key | 1st M used B/key | 2nd M RSS B/key | 2nd M used B/key |
|---|---|---|---|---|---|
| KiviDB 1.2.0-rc10 (39bfacd2) | 1 | 149.0 | 146.9 | 154.8 | 146.9 |
| KiviDB 1.2.0-rc8 | 1 | 149.2 | 146.9 | 146.7 | 146.9 |

### memory-load-random: 1M keys x 100 B, then a second million on the same engine

| engine | runs | 1st M RSS B/key | 1st M used B/key | 2nd M RSS B/key | 2nd M used B/key |
|---|---|---|---|---|---|
| KiviDB 1.2.0-rc10 (39bfacd2) | 1 | 149.1 | 146.9 | 147.5 | 146.9 |
| KiviDB 1.2.0-rc8 | 1 | 149.2 | 146.9 | 146.7 | 146.9 |

### set-get-small: SET:GET 1:10, 100 B, pipeline 1

| engine | runs | ops/s (median of passes) | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc10 (39bfacd2) | 1 | 1,323,766 | 0.151 | 0.327 | 0.379 | 52.0 |
| KiviDB 1.2.0-rc8 | 1 | 1,358,514 | 0.143 | 0.323 | 0.379 | 53.4 |

### set-get-large: SET:GET 1:10, 10 KB, pipeline 1

| engine | runs | ops/s (median of passes) | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc10 (39bfacd2) | 1 | 320,877 | 0.607 | 1.035 | 1.223 | 23.9 |
| KiviDB 1.2.0-rc8 | 1 | 320,604 | 0.607 | 1.111 | 1.235 | 23.1 |

### pipeline-30: SET:GET 1:10, 100 B, pipeline 30

| engine | runs | ops/s (median of passes) | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc10 (39bfacd2) | 1 | 17,704,944 | 0.311 | 0.639 | 0.747 | 66.2 |
| KiviDB 1.2.0-rc8 | 1 | 17,666,123 | 0.311 | 0.639 | 0.747 | 66.4 |

## Memory as the dataset grows (random values)

RSS bytes per key (used_memory bytes per key). rc10 and rc8: median over rounds.

### 100 B values (random)

| engine | runs | 1M keys | 5M keys | 10M keys | 25M keys | 50M keys | RSS at largest | load s (all stages) |
|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc10 (39bfacd2) | 2 | 153.5 (146.9) | 146.5 (143.1) | 144.9 (143.1) | 141.0 (140.1) | 140.7 (140.1) | 6.57 GiB | 12.5 (12.1-13.0) |
| KiviDB 1.2.0-rc8 | 2 | 153.5 (146.9) | 146.3 (143.1) | 144.9 (143.1) | 141.0 (140.1) | 140.7 (140.1) | 6.57 GiB | 12.6 (12.0-13.1) |

### 1024 B values (random)

| engine | runs | 1M keys | 5M keys | 10M keys | 25M keys | 50M keys | RSS at largest | load s (all stages) |
|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc10 (39bfacd2) | 1 | 1085.2 (1074.9) | 1077.9 (1071.1) | 1076.2 (1071.1) | 1072.0 (1068.1) | 1071.7 (1068.1) | 49.92 GiB | 23.0 |
| KiviDB 1.2.0-rc8 | 1 | 1085.9 (1074.9) | 1077.5 (1071.1) | 1076.1 (1071.1) | 1072.0 (1068.1) | 1071.6 (1068.1) | 49.92 GiB | 22.9 |

## Mixed workload (pair B): 10M keys, 150k ops/s, 80% GET / 20% SET (64-1024 B, TTL 60-600 s), 20 min, distinct client seeds

| engine | round | RSS at start GiB | RSS peak GiB | RSS at end GiB | used at end GiB | RSS/used at end | keys at end (M) | ops/s | GET p50/p99/p99.9 ms | SET p50/p99/p99.9 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc10 (39bfacd2) | r1 | 1.37 | 4.45 | 3.94 | 3.43 | 1.15 | 6.13 | 149,687 | 0.23/0.43/0.57 | 0.24/0.45/0.69 |
| KiviDB 1.2.0-rc8 | r1 | 1.37 | 4.43 | 3.92 | 3.43 | 1.14 | 6.13 | 149,706 | 0.29/0.46/0.69 | 0.29/0.49/0.81 |

## Snapshot under a steady 100k SET/s (uniform random keys): 30M keys x 1 KB, random values

| engine | round | event | memory before GiB | peak during GiB | added GiB | added % | duration s | worst write ms | worst 1-s p99 ms | write p99.9 over window ms | p99 before ms | memory 60 s after GiB | used after GiB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc10 (39bfacd2) | r1 | BGSAVE | 30.28 | 33.84 | 3.56 | 12% | 81.4 | 206 | 1.59 | 1.85 | 1.40 | 33.81 | 30.09 |
| KiviDB 1.2.0-rc10 (39bfacd2) | r1 | BGREWRITEAOF | 33.81 | 36.16 | 2.34 | 7% | 83.0 | 218 | 1.53 | 1.79 | 1.36 | 35.61 | 30.09 |
| KiviDB 1.2.0-rc8 | r1 | BGSAVE | 30.31 | 33.84 | 3.53 | 12% | 81.9 | 206 | 1.43 | 1.97 | 1.22 | 33.81 | 30.09 |
| KiviDB 1.2.0-rc8 | r1 | BGREWRITEAOF | 33.81 | 36.19 | 2.38 | 7% | 83.9 | 224 | 1.68 | 1.85 | 1.42 | 35.61 | 30.09 |
| KiviDB 1.2.0-rc8 | r2 | BGSAVE | 30.31 | 33.85 | 3.54 | 12% | 81.9 | 210 | 1.53 | 1.99 | 1.36 | 33.82 | 30.09 |
| KiviDB 1.2.0-rc8 | r2 | BGREWRITEAOF | 33.82 | 36.18 | 2.36 | 7% | 84.0 | 214 | 1.47 | 1.84 | 1.30 | 35.61 | 30.09 |

### Snapshot, per engine: median over rounds

| engine | runs | event | added GiB | added % | duration s | worst write ms | write p99.9 ms | p99 before ms | memory 60 s after GiB |
|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc10 (39bfacd2) | 1 | BGSAVE | 3.56 | 12 | 81.4 | 206 | 1.85 | 1.40 | 33.81 |
| KiviDB 1.2.0-rc10 (39bfacd2) | 1 | BGREWRITEAOF | 2.34 | 7 | 83.0 | 218 | 1.79 | 1.36 | 35.61 |
| KiviDB 1.2.0-rc8 | 2 | BGSAVE | 3.54 (3.53-3.54) | 12 (12-12) | 81.9 (81.9-81.9) | 208 (206-210) | 1.98 (1.97-1.99) | 1.29 (1.22-1.36) | 33.82 (33.81-33.82) |
| KiviDB 1.2.0-rc8 | 2 | BGREWRITEAOF | 2.37 (2.36-2.38) | 7 (7-7) | 83.9 (83.9-84.0) | 219 (214-224) | 1.85 (1.84-1.85) | 1.36 (1.30-1.42) | 35.61 (35.61-35.61) |

## maxmemory + BGSAVE (issue #284): 10M keys x 1 KB, maxmemory = used + 10%, allkeys-lru, 100k same-size overwrites/s (uniform)

| engine | round | keys before | keys after | evicted keys | maxmemory | lowest key count during run | SET p99 / p99.9 ms |
|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc10 (39bfacd2) | r1 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.67 / 1.01 |
| KiviDB 1.2.0-rc8 | r1 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.75 / 1.10 |

## Active defrag after deleting a random half: 5M keys x 100 B, probe load 80k GET/s + 20k SET/s (pair B)

Slab = INFO value_slab_bytes (live = value_slab_live_bytes). *Give-back time*: from the end of the delete until RSS has made 90% of its final drop. *Compaction*: seconds with active_defrag_running = 1. Latency: per-second p99 / p99.9 from memtier, median over the seconds of the window (worst second in brackets); *before* = the 50 s before the delete, *during* = while compaction runs.

| build | round | RSS after load MB | slab / live after delete MB | RSS after delete MB | RSS final MB | slab / live final MB | RSS returned MB | give-back s | compaction s | hits | GET p99 before / during ms | GET p99.9 before / during ms | SET p99 before / during ms | SET p99.9 before / during ms | max ms during |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc10 (39bfacd2) | r1 | 722 | 624 / 319 | 723 | 387 | 321 / 319 | 335 | 12.1 | 13 | 1,404,776 | 0.43 / 0.44 (0.49) | 0.70 / 0.80 (0.97) | 0.20 / 0.20 (0.23) | 0.38 / 0.34 (0.44) | 2.9 |
| KiviDB 1.2.0-rc8 | r1 | 722 | 624 / 319 | 723 | 389 | 321 / 319 | 334 | 13.0 | 13 | 1,380,595 | 0.33 / 0.33 (0.39) | 0.74 / 0.72 (0.87) | 0.13 / 0.12 (0.13) | 0.14 / 0.14 (0.16) | 2.4 |

## Write latency with the AOF on, no snapshot (pair B): 2M x 1 KB, 100k uniformly random SET/s, 180 s

The check of the snapshot runs' steady-state write p99 (`harness/aoflat.sh`). Rounds r1-r4 alternate the builds; p1 is the round with `perf record` running for 30 s.

| build | round | ops/s | SET p50 ms | p99 ms | p99.9 ms | worst write ms | median of 1-s p99 ms | engine CPU s (load + run) |
|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc10 (39bfacd2) | r1 | 100,000 | 0.327 | 0.863 | 1.191 | 6 | 0.879 | 530 |
| KiviDB 1.2.0-rc10 (39bfacd2) | r2 | 99,997 | 0.391 | 0.975 | 1.351 | 10 | 0.967 | 558 |
| KiviDB 1.2.0-rc10 (39bfacd2) | r3 | 99,999 | 0.335 | 0.871 | 1.183 | 9 | 0.879 | 567 |
| KiviDB 1.2.0-rc8 | r1 | 99,976 | 0.407 | 1.007 | 1.415 | 57 | 0.951 | 557 |
| KiviDB 1.2.0-rc8 | r2 | 99,900 | 0.399 | 0.999 | 1.423 | 182 | 0.967 | 553 |
| KiviDB 1.2.0-rc8 | r3 | 99,999 | 0.367 | 0.927 | 1.279 | 11 | 0.879 | 558 |

## rc10 against rc8

Median (min-max) over rounds. *Change* is rc10 median vs rc8 median; *spread* is the larger of the two builds' (max-min)/median. A row is flagged when rc10 is worse by more than the spread and by at least 1% (memory counts repeat to a few tenths of a percent, so a spread near zero would otherwise flag a 0.2% difference).

| section | metric | rc10 | rc8 | change | spread | verdict |
|---|---|---|---|---|---|---|
| warefeats ASCII | 1st M RSS B/key | 149.0 | 149.2 | -0.1% | 0.0% | better |
| warefeats ASCII | 1st M used B/key | 146.9 | 146.9 | 0.0% | 0.0% | level |
| warefeats ASCII | 2nd M RSS B/key | 154.8 | 146.7 | 5.5% | 0.0% | **worse beyond spread** |
| warefeats random | 1st M RSS B/key | 149.1 | 149.2 | -0.1% | 0.0% | better |
| warefeats random | 1st M used B/key | 146.9 | 146.9 | 0.0% | 0.0% | level |
| warefeats random | 2nd M RSS B/key | 147.5 | 146.7 | 0.5% | 0.0% | level (beyond spread, under 1%) |
| warefeats set-get-small | ops/s | 1,323,766 | 1,358,514 | -2.6% | 0.0% | **worse beyond spread** |
| warefeats set-get-small | p99 ms | 0.327 | 0.323 | 1.2% | 0.0% | **worse beyond spread** |
| warefeats set-get-small | p99.9 ms | 0.379 | 0.379 | 0.0% | 0.0% | level |
| warefeats set-get-large | ops/s | 320,877 | 320,604 | 0.1% | 0.0% | better |
| warefeats set-get-large | p99 ms | 1.035 | 1.111 | -6.8% | 0.0% | better |
| warefeats set-get-large | p99.9 ms | 1.223 | 1.235 | -1.0% | 0.0% | better |
| warefeats pipeline-30 | ops/s | 17,704,944 | 17,666,123 | 0.2% | 0.0% | better |
| warefeats pipeline-30 | p99 ms | 0.639 | 0.639 | 0.0% | 0.0% | level |
| warefeats pipeline-30 | p99.9 ms | 0.747 | 0.747 | 0.0% | 0.0% | level |
| growth 100 B | RSS B/key at 1M | 153.5 (153.0-153.9) | 153.5 (153.3-153.6) | 0.0% | 0.6% | level |
| growth 100 B | RSS B/key at 5M | 146.5 (146.3-146.7) | 146.3 (146.3-146.4) | 0.1% | 0.3% | level |
| growth 100 B | RSS B/key at 10M | 144.9 (144.9-144.9) | 144.9 (144.8-144.9) | 0.0% | 0.1% | level |
| growth 100 B | RSS B/key at 25M | 141.0 (141.0-141.0) | 141.0 (141.0-141.0) | -0.0% | 0.0% | level |
| growth 100 B | RSS B/key at 50M | 140.7 (140.7-140.7) | 140.7 (140.6-140.7) | -0.0% | 0.0% | level |
| growth 100 B | load seconds, 50M keys | 12.5 (12.1-13.0) | 12.6 (12.0-13.1) | -0.3% | 8.6% | level |
| growth 1024 B | RSS B/key at 1M | 1085.2 | 1085.9 | -0.1% | 0.0% | better |
| growth 1024 B | RSS B/key at 5M | 1077.9 | 1077.5 | 0.0% | 0.0% | level (beyond spread, under 1%) |
| growth 1024 B | RSS B/key at 10M | 1076.2 | 1076.1 | 0.0% | 0.0% | level (beyond spread, under 1%) |
| growth 1024 B | RSS B/key at 25M | 1072.0 | 1072.0 | 0.0% | 0.0% | level (beyond spread, under 1%) |
| growth 1024 B | RSS B/key at 50M | 1071.7 | 1071.6 | 0.0% | 0.0% | level (beyond spread, under 1%) |
| growth 1024 B | load seconds, 50M keys | 23.0 | 22.9 | 0.2% | 0.0% | level (beyond spread, under 1%) |
| mixed 20 min | RSS at end GiB | 3.94 | 3.92 | 0.5% | 0.0% | level (beyond spread, under 1%) |
| mixed 20 min | RSS peak GiB | 4.45 | 4.43 | 0.4% | 0.0% | level (beyond spread, under 1%) |
| mixed 20 min | RSS/used at end | 1.15 | 1.14 | 0.5% | 0.0% | level (beyond spread, under 1%) |
| mixed 20 min | GET p99 ms | 0.43 | 0.46 | -6.9% | 0.0% | better |
| mixed 20 min | GET p99.9 ms | 0.57 | 0.69 | -16.3% | 0.0% | better |
| mixed 20 min | SET p99 ms | 0.45 | 0.49 | -9.7% | 0.0% | better |
| mixed 20 min | SET p99.9 ms | 0.69 | 0.81 | -13.9% | 0.0% | better |
| snapshot BGSAVE | memory added GiB | 3.56 | 3.54 (3.53-3.54) | 0.6% | 0.1% | level (beyond spread, under 1%) |
| snapshot BGSAVE | duration s | 81.4 | 81.9 (81.9-81.9) | -0.6% | 0.0% | better |
| snapshot BGSAVE | worst write ms | 206 | 208 (206-210) | -0.7% | 1.8% | level |
| snapshot BGSAVE | write p99.9 ms | 1.85 | 1.98 (1.97-1.99) | -6.3% | 1.2% | better |
| snapshot BGSAVE | write p99 before ms | 1.40 | 1.29 (1.22-1.36) | 8.4% | 10.5% | level |
| snapshot BGSAVE | memory 60 s after GiB | 33.81 | 33.82 (33.81-33.82) | -0.0% | 0.0% | level |
| snapshot BGREWRITEAOF | memory added GiB | 2.34 | 2.37 (2.36-2.38) | -1.1% | 0.7% | better |
| snapshot BGREWRITEAOF | duration s | 83.0 | 83.9 (83.9-84.0) | -1.2% | 0.1% | better |
| snapshot BGREWRITEAOF | worst write ms | 218 | 219 (214-224) | -0.4% | 4.6% | level |
| snapshot BGREWRITEAOF | write p99.9 ms | 1.79 | 1.85 (1.84-1.85) | -3.0% | 0.9% | better |
| snapshot BGREWRITEAOF | write p99 before ms | 1.36 | 1.36 (1.30-1.42) | -0.3% | 8.8% | level |
| snapshot BGREWRITEAOF | memory 60 s after GiB | 35.61 | 35.61 (35.61-35.61) | 0.0% | 0.0% | level (beyond spread, under 1%) |
| #284 eviction | keys evicted | 0 | 0 | -% | 0.0% | - |
| active defrag | RSS final MB | 387 | 389 | -0.3% | 0.0% | better |
| active defrag | RSS returned MB | 335 | 334 | 0.4% | 0.0% | better |
| active defrag | give-back s | 12.1 | 13.0 | -6.4% | 0.0% | better |
| active defrag | compaction s | 13 | 13 | 0.0% | 0.0% | level |
| active defrag | slab final MB | 321 | 321 | -0.1% | 0.0% | better |
| active defrag | GET p99 during compaction ms | 0.44 | 0.33 | 34.3% | 0.0% | **worse beyond spread** |
| active defrag | GET p99.9 during compaction ms | 0.80 | 0.72 | 11.1% | 0.0% | **worse beyond spread** |
| active defrag | SET p99 during compaction ms | 0.20 | 0.12 | 67.2% | 0.0% | **worse beyond spread** |
| active defrag | SET p99.9 during compaction ms | 0.34 | 0.14 | 154.1% | 0.0% | **worse beyond spread** |
| active defrag | GET p99 during / before (same run) | 1.02 | 1.00 | 1.9% | 0.0% | **worse beyond spread** |
| active defrag | GET p99.9 during / before (same run) | 1.14 | 0.97 | 16.8% | 0.0% | **worse beyond spread** |
| active defrag | SET p99 during / before (same run) | 1.00 | 0.94 | 6.7% | 0.0% | **worse beyond spread** |
| active defrag | SET p99.9 during / before (same run) | 0.91 | 1.00 | -8.5% | 0.0% | better |
| AOF-on writes | SET p50 ms | 0.335 (0.327-0.391) | 0.399 (0.367-0.407) | -16.0% | 19.1% | level |
| AOF-on writes | SET p99 ms | 0.871 (0.863-0.975) | 0.999 (0.927-1.007) | -12.8% | 12.9% | level |
| AOF-on writes | SET p99.9 ms | 1.191 (1.183-1.351) | 1.415 (1.279-1.423) | -15.8% | 14.1% | better |
| AOF-on writes | worst write ms | 9 (6-10) | 57 (11-182) | -83.8% | 296.9% | level |
| AOF-on writes | engine CPU s | 558 (530-567) | 557 (553-558) | 0.2% | 6.6% | level |

Flagged: warefeats ASCII 2nd M RSS B/key (+5.5%, spread 0.0%); warefeats set-get-small ops/s (-2.6%, spread 0.0%); warefeats set-get-small p99 ms (+1.2%, spread 0.0%); active defrag GET p99 during compaction ms (+34.3%, spread 0.0%); active defrag GET p99.9 during compaction ms (+11.1%, spread 0.0%); active defrag SET p99 during compaction ms (+67.2%, spread 0.0%); active defrag SET p99.9 during compaction ms (+154.1%, spread 0.0%); active defrag GET p99 during / before (same run) (+1.9%, spread 0.0%); active defrag GET p99.9 during / before (same run) (+16.8%, spread 0.0%); active defrag SET p99 during / before (same run) (+6.7%, spread 0.0%)

