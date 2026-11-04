# memory suite: generated tables

## warefeats/kv-stores harness (a324d29)

rc8 and rc7: median (min-max) over their rounds; one run for the others.

### memory-load: 1M keys x 100 B, then a second million on the same engine

| engine | runs | 1st M RSS B/key | 1st M used B/key | 2nd M RSS B/key | 2nd M used B/key |
|---|---|---|---|---|---|
| KiviDB 1.2.0-rc8 (082dab3d) | 1 | 149.1 | 146.9 | 146.9 | 146.9 |
| KiviDB 1.2.0-rc7 | 1 | 149.0 | 146.9 | 147.4 | 146.9 |
| Dragonfly 2.0.2 | 1 | 136.9 | 138.7 | 147.7 | 147.1 |
| Redis 8.10.3 | 1 | 166.2 | 165.3 | 166.2 | 166.1 |
| Valkey 9.1.3 | 1 | 170.6 | 167.1 | 172.4 | 169.5 |

### memory-load-random: 1M keys x 100 B, then a second million on the same engine

| engine | runs | 1st M RSS B/key | 1st M used B/key | 2nd M RSS B/key | 2nd M used B/key |
|---|---|---|---|---|---|
| KiviDB 1.2.0-rc8 (082dab3d) | 1 | 149.2 | 146.9 | 146.7 | 146.9 |
| KiviDB 1.2.0-rc7 | 1 | 149.2 | 146.9 | 146.7 | 146.9 |

### set-get-small: SET:GET 1:10, 100 B, pipeline 1

| engine | runs | ops/s (median of passes) | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc8 (082dab3d) | 1 | 1,310,912 | 0.151 | 0.335 | 0.391 | 52.3 |
| KiviDB 1.2.0-rc7 | 1 | 1,329,474 | 0.151 | 0.339 | 0.395 | 52.7 |

### set-get-large: SET:GET 1:10, 10 KB, pipeline 1

| engine | runs | ops/s (median of passes) | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc8 (082dab3d) | 1 | 318,999 | 0.607 | 1.195 | 1.275 | 23.4 |
| KiviDB 1.2.0-rc7 | 1 | 318,937 | 0.607 | 1.195 | 1.287 | 21.1 |

### pipeline-30: SET:GET 1:10, 100 B, pipeline 30

| engine | runs | ops/s (median of passes) | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc8 (082dab3d) | 1 | 17,298,680 | 0.319 | 0.651 | 0.767 | 63.8 |
| KiviDB 1.2.0-rc7 | 1 | 17,143,693 | 0.327 | 0.655 | 0.767 | 64.4 |

## Memory as the dataset grows (random values)

RSS bytes per key (used_memory bytes per key). rc8 and rc7: median over rounds.

### 100 B values (random)

| engine | runs | 1M keys | 5M keys | 10M keys | 25M keys | 50M keys | RSS at largest | load s (all stages) |
|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc8 (082dab3d) | 1 | 153.5 (146.9) | 146.7 (143.1) | 145.0 (143.1) | 141.0 (140.1) | 140.7 (140.1) | 6.57 GiB | 13.0 |
| Redis 8.10.3 | 1 | 171.0 (165.4) | 179.3 (177.1) | 179.0 (177.3) | 174.6 (173.1) | 174.5 (173.1) | 8.15 GiB | 50.0 |
| Valkey 9.1.3 | 1 | 172.3 (167.0) | 169.2 (166.4) | 169.3 (166.7) | 166.6 (165.4) | 166.6 (165.4) | 7.77 GiB | 45.0 |
| Dragonfly 2.0.2 | 1 | 163.5 (154.8) | 150.2 (148.0) | 154.6 (148.9) | 143.1 (139.4) | 141.6 (139.6) | 6.65 GiB | 16.1 |
| KiviDB 1.2.0-rc7 | 1 | 154.1 (146.9) | 146.8 (143.1) | 145.0 (143.1) | 141.1 (140.1) | 140.7 (140.1) | 6.57 GiB | 13.0 |

### 1024 B values (random)

| engine | runs | 1M keys | 5M keys | 10M keys | 25M keys | 50M keys | RSS at largest | load s (all stages) |
|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc8 (082dab3d) | 1 | 1085.8 (1074.9) | 1077.7 (1071.1) | 1076.1 (1071.1) | 1072.0 (1068.1) | 1071.6 (1068.1) | 49.92 GiB | 23.0 |
| KiviDB 1.2.0-rc7 | 1 | 1085.8 (1074.9) | 1077.4 (1071.1) | 1076.2 (1071.1) | 1072.1 (1068.1) | 1071.7 (1068.1) | 49.92 GiB | 23.0 |

## Mixed workload (pair B): 10M keys, 150k ops/s, 80% GET / 20% SET (64-1024 B, TTL 60-600 s), 20 min, distinct client seeds

| engine | round | RSS at start GiB | RSS peak GiB | RSS at end GiB | used at end GiB | RSS/used at end | keys at end (M) | ops/s | GET p50/p99/p99.9 ms | SET p50/p99/p99.9 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc8 (082dab3d) | r1 | 1.37 | 4.45 | 3.94 | 3.43 | 1.15 | 6.13 | 149,722 | 0.26/0.46/0.60 | 0.26/0.47/0.71 |
| KiviDB 1.2.0-rc8 (082dab3d) | r2 | 1.37 | 4.44 | 3.93 | 3.43 | 1.15 | 6.13 | 149,685 | 0.27/0.46/0.66 | 0.28/0.49/0.81 |
| KiviDB 1.2.0-rc7 | r1 | 1.37 | 4.44 | 3.94 | 3.43 | 1.15 | 6.13 | 149,709 | 0.28/0.48/0.68 | 0.29/0.50/0.83 |
| KiviDB 1.2.0-rc7 | r2 | 1.37 | 4.44 | 3.93 | 3.43 | 1.15 | 6.13 | 149,661 | 0.26/0.44/0.61 | 0.27/0.46/0.75 |

## Snapshot under a steady 100k SET/s (uniform random keys): 30M keys x 1 KB, random values

| engine | round | event | memory before GiB | peak during GiB | added GiB | added % | duration s | worst write ms | worst 1-s p99 ms | write p99.9 over window ms | p99 before ms | memory 60 s after GiB | used after GiB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc8 (082dab3d) | r1 | BGSAVE | 30.29 | 33.88 | 3.59 | 12% | 82.3 | 203 | 1.66 | 1.85 | 1.46 | 33.85 | 30.09 |
| KiviDB 1.2.0-rc8 (082dab3d) | r1 | BGREWRITEAOF | 33.85 | 36.23 | 2.38 | 7% | 83.7 | 218 | 1.66 | 1.81 | 1.39 | 35.64 | 30.09 |
| KiviDB 1.2.0-rc8 (082dab3d) | r2 | BGSAVE | 30.31 | 33.86 | 3.55 | 12% | 82.0 | 224 | 1.78 | 1.98 | 1.42 | 33.83 | 30.09 |
| KiviDB 1.2.0-rc8 (082dab3d) | r2 | BGREWRITEAOF | 33.83 | 36.25 | 2.42 | 7% | 83.1 | 215 | 1.53 | 1.94 | 1.29 | 36.22 | 30.09 |
| Redis 8.10.3 | r1 | BGSAVE | 37.60 | 69.95 | 32.34 | 86% | 156.9 | 396 | 2.03 | 7.93 | 0.74 | 37.63 | 37.28 |
| Redis 8.10.3 | r1 | BGREWRITEAOF | 37.61 | 70.05 | 32.45 | 86% | 162.0 | 996 | 1.84 | 8.16 | 0.74 | 37.62 | 37.30 |
| Valkey 9.1.3 | r1 | BGSAVE | 37.89 | 67.13 | 29.24 | 77% | 158.3 | 393 | 1.50 | 2.46 | 0.81 | 38.31 | 37.40 |
| Valkey 9.1.3 | r1 | BGREWRITEAOF | 38.56 | 67.45 | 28.89 | 75% | 157.0 | 1011 | 1.96 | 2.43 | 0.66 | 39.01 | 37.42 |
| Dragonfly 2.0.2 | r1 | BGSAVE | 30.73 | 30.86 | 0.13 | 0% | 56.2 | 151 | 18.69 | 11.46 | 0.59 | 30.74 | 30.61 |
| KiviDB 1.2.0-rc7 | r1 | BGSAVE | 30.30 | 33.80 | 3.50 | 12% | 80.7 | 207 | 1.55 | 1.98 | 1.29 | 33.77 | 30.09 |
| KiviDB 1.2.0-rc7 | r1 | BGREWRITEAOF | 33.77 | 36.09 | 2.32 | 7% | 82.1 | 219 | 1.50 | 1.85 | 1.26 | 35.52 | 30.09 |
| KiviDB 1.2.0-rc7 | r2 | BGSAVE | 30.29 | 33.79 | 3.51 | 12% | 81.1 | 206 | 1.46 | 1.97 | 1.25 | 33.77 | 30.09 |
| KiviDB 1.2.0-rc7 | r2 | BGREWRITEAOF | 33.77 | 36.24 | 2.48 | 7% | 82.0 | 221 | 1.46 | 1.95 | 1.29 | 36.22 | 30.09 |

### Snapshot, per engine: median over rounds

| engine | runs | event | added GiB | added % | duration s | worst write ms | write p99.9 ms | p99 before ms | memory 60 s after GiB |
|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc8 (082dab3d) | 2 | BGSAVE | 3.57 (3.55-3.59) | 12 (12-12) | 82.2 (82.0-82.3) | 214 (203-224) | 1.92 (1.85-1.98) | 1.44 (1.42-1.46) | 33.84 (33.83-33.85) |
| KiviDB 1.2.0-rc8 (082dab3d) | 2 | BGREWRITEAOF | 2.40 (2.38-2.42) | 7 (7-7) | 83.4 (83.1-83.7) | 216 (215-218) | 1.87 (1.81-1.94) | 1.34 (1.29-1.39) | 35.93 (35.64-36.22) |
| Redis 8.10.3 | 1 | BGSAVE | 32.34 | 86 | 156.9 | 396 | 7.93 | 0.74 | 37.63 |
| Redis 8.10.3 | 1 | BGREWRITEAOF | 32.45 | 86 | 162.0 | 996 | 8.16 | 0.74 | 37.62 |
| Valkey 9.1.3 | 1 | BGSAVE | 29.24 | 77 | 158.3 | 393 | 2.46 | 0.81 | 38.31 |
| Valkey 9.1.3 | 1 | BGREWRITEAOF | 28.89 | 75 | 157.0 | 1011 | 2.43 | 0.66 | 39.01 |
| Dragonfly 2.0.2 | 1 | BGSAVE | 0.13 | 0 | 56.2 | 151 | 11.46 | 0.59 | 30.74 |
| KiviDB 1.2.0-rc7 | 2 | BGSAVE | 3.50 (3.50-3.51) | 12 (12-12) | 80.9 (80.7-81.1) | 206 (206-207) | 1.98 (1.97-1.98) | 1.27 (1.25-1.29) | 33.77 (33.77-33.77) |
| KiviDB 1.2.0-rc7 | 2 | BGREWRITEAOF | 2.40 (2.32-2.48) | 7 (7-7) | 82.1 (82.0-82.1) | 220 (219-221) | 1.90 (1.85-1.95) | 1.27 (1.26-1.29) | 35.87 (35.52-36.22) |

## maxmemory + BGSAVE (issue #284): 10M keys x 1 KB, maxmemory = used + 10%, allkeys-lru, 100k same-size overwrites/s (uniform)

| engine | round | keys before | keys after | evicted keys | maxmemory | lowest key count during run | SET p99 / p99.9 ms |
|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc8 (082dab3d) | r1 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.76 / 1.10 |
| KiviDB 1.2.0-rc7 | r1 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.74 / 1.08 |

## Active defrag after deleting a random half: 5M keys x 100 B, probe load 80k GET/s + 20k SET/s (pair B)

Slab = INFO value_slab_bytes (live = value_slab_live_bytes). *Give-back time*: from the end of the delete until RSS has made 90% of its final drop. *Compaction*: seconds with active_defrag_running = 1. Latency: per-second p99 / p99.9 from memtier, median over the seconds of the window (worst second in brackets); *before* = the 50 s before the delete, *during* = while compaction runs.

| build | round | RSS after load MB | slab / live after delete MB | RSS after delete MB | RSS final MB | slab / live final MB | RSS returned MB | give-back s | compaction s | hits | GET p99 before / during ms | GET p99.9 before / during ms | SET p99 before / during ms | SET p99.9 before / during ms | max ms during |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc8 (082dab3d) | r1 | 722 | 624 / 319 | 723 | 390 | 321 / 319 | 333 | 11.9 | 13 | 1,393,687 | 0.32 / 0.32 (0.37) | 0.66 / 0.66 (0.79) | 0.61 / 0.61 (0.77) | 0.76 / 0.81 (1.05) | 2.2 |
| KiviDB 1.2.0-rc8 (082dab3d) | r2 | 722 | 624 / 319 | 723 | 388 | 321 / 319 | 335 | 12.1 | 12 | 1,376,446 | 0.55 / 0.54 (0.59) | 0.76 / 0.74 (0.81) | 0.21 / 0.23 (0.24) | 0.25 / 0.26 (0.30) | 2.1 |
| KiviDB 1.2.0-rc7 | r1 | 722 | 624 / 319 | 722 | 402 | 336 / 319 | 320 | 9.8 | 9 | 1,137,939 | 0.38 / 0.40 (0.43) | 0.54 / 0.54 (0.66) | 0.14 / 0.14 (0.14) | 0.18 / 0.19 (0.27) | 1.9 |
| KiviDB 1.2.0-rc7 | r2 | 722 | 624 / 319 | 722 | 387 | 321 / 319 | 335 | 12.1 | 12 | 1,400,515 | 0.49 / 0.57 (0.61) | 0.77 / 0.79 (0.93) | 0.20 / 0.20 (0.21) | 0.47 / 0.46 (0.55) | 2.3 |

## Write latency with the AOF on, no snapshot (pair B): 2M x 1 KB, 100k uniformly random SET/s, 180 s

The check of the snapshot runs' steady-state write p99 (`harness/aoflat.sh`). Rounds r1-r4 alternate the builds; p1 is the round with `perf record` running for 30 s.

| build | round | ops/s | SET p50 ms | p99 ms | p99.9 ms | worst write ms | median of 1-s p99 ms | engine CPU s (load + run) |
|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc8 (082dab3d) | r1 | 99,516 | 0.399 | 0.975 | 1.367 | 160 | 0.951 | 564 |
| KiviDB 1.2.0-rc8 (082dab3d) | r2 | 99,279 | 0.407 | 1.015 | 1.991 | 100 | 0.975 | 561 |
| KiviDB 1.2.0-rc8 (082dab3d) | r3 | 99,565 | 0.375 | 0.935 | 1.343 | 162 | 0.895 | 556 |
| KiviDB 1.2.0-rc8 (082dab3d) | r4 | 99,260 | 0.399 | 0.991 | 2.023 | 109 | 0.943 | 562 |
| KiviDB 1.2.0-rc7 | r1 | 99,482 | 0.351 | 0.903 | 1.663 | 94 | 0.903 | 570 |
| KiviDB 1.2.0-rc7 | r2 | 99,530 | 0.407 | 1.007 | 1.431 | 127 | 1.047 | 564 |
| KiviDB 1.2.0-rc7 | r3 | 99,206 | 0.407 | 0.999 | 2.015 | 108 | 0.951 | 564 |
| KiviDB 1.2.0-rc7 | r4 | 99,513 | 0.383 | 0.943 | 1.327 | 133 | 0.927 | 558 |

## rc8 against rc7

Median (min-max) over rounds. *Change* is rc8 median vs rc7 median; *spread* is the larger of the two builds' (max-min)/median. A row is flagged when rc8 is worse by more than the spread and by at least 1% (memory counts repeat to a few tenths of a percent, so a spread near zero would otherwise flag a 0.2% difference).

| section | metric | rc8 | rc7 | change | spread | verdict |
|---|---|---|---|---|---|---|
| warefeats ASCII | 1st M RSS B/key | 149.1 | 149.0 | 0.1% | 0.0% | level (beyond spread, under 1%) |
| warefeats ASCII | 1st M used B/key | 146.9 | 146.9 | 0.0% | 0.0% | level |
| warefeats ASCII | 2nd M RSS B/key | 146.9 | 147.4 | -0.3% | 0.0% | better |
| warefeats random | 1st M RSS B/key | 149.2 | 149.2 | 0.0% | 0.0% | level |
| warefeats random | 1st M used B/key | 146.9 | 146.9 | 0.0% | 0.0% | level |
| warefeats random | 2nd M RSS B/key | 146.7 | 146.7 | 0.0% | 0.0% | level |
| warefeats set-get-small | ops/s | 1,310,912 | 1,329,474 | -1.4% | 0.0% | **worse beyond spread** |
| warefeats set-get-small | p99 ms | 0.335 | 0.339 | -1.2% | 0.0% | better |
| warefeats set-get-small | p99.9 ms | 0.391 | 0.395 | -1.0% | 0.0% | better |
| warefeats set-get-large | ops/s | 318,999 | 318,937 | 0.0% | 0.0% | better |
| warefeats set-get-large | p99 ms | 1.195 | 1.195 | 0.0% | 0.0% | level |
| warefeats set-get-large | p99.9 ms | 1.275 | 1.287 | -0.9% | 0.0% | better |
| warefeats pipeline-30 | ops/s | 17,298,680 | 17,143,693 | 0.9% | 0.0% | better |
| warefeats pipeline-30 | p99 ms | 0.651 | 0.655 | -0.6% | 0.0% | better |
| warefeats pipeline-30 | p99.9 ms | 0.767 | 0.767 | 0.0% | 0.0% | level |
| growth 100 B | RSS B/key at 1M | 153.5 | 154.1 | -0.4% | 0.0% | better |
| growth 100 B | RSS B/key at 5M | 146.7 | 146.8 | -0.0% | 0.0% | better |
| growth 100 B | RSS B/key at 10M | 145.0 | 145.0 | 0.0% | 0.0% | level (beyond spread, under 1%) |
| growth 100 B | RSS B/key at 25M | 141.0 | 141.1 | -0.0% | 0.0% | better |
| growth 100 B | RSS B/key at 50M | 140.7 | 140.7 | -0.0% | 0.0% | better |
| growth 100 B | load seconds, 50M keys | 13.0 | 13.0 | 0.2% | 0.0% | level (beyond spread, under 1%) |
| growth 1024 B | RSS B/key at 1M | 1085.8 | 1085.8 | 0.0% | 0.0% | level |
| growth 1024 B | RSS B/key at 5M | 1077.7 | 1077.4 | 0.0% | 0.0% | level (beyond spread, under 1%) |
| growth 1024 B | RSS B/key at 10M | 1076.1 | 1076.2 | -0.0% | 0.0% | better |
| growth 1024 B | RSS B/key at 25M | 1072.0 | 1072.1 | -0.0% | 0.0% | better |
| growth 1024 B | RSS B/key at 50M | 1071.6 | 1071.7 | -0.0% | 0.0% | better |
| growth 1024 B | load seconds, 50M keys | 23.0 | 23.0 | 0.1% | 0.0% | level (beyond spread, under 1%) |
| mixed 20 min | RSS at end GiB | 3.93 (3.93-3.94) | 3.93 (3.93-3.94) | -0.0% | 0.3% | level |
| mixed 20 min | RSS peak GiB | 4.44 (4.44-4.45) | 4.44 (4.44-4.44) | 0.0% | 0.3% | level |
| mixed 20 min | RSS/used at end | 1.15 (1.15-1.15) | 1.15 (1.15-1.15) | -0.0% | 0.3% | level |
| mixed 20 min | GET p99 ms | 0.46 (0.46-0.46) | 0.46 (0.44-0.48) | 0.0% | 8.7% | level |
| mixed 20 min | GET p99.9 ms | 0.63 (0.60-0.66) | 0.65 (0.61-0.68) | -2.5% | 10.1% | level |
| mixed 20 min | SET p99 ms | 0.48 (0.47-0.49) | 0.48 (0.46-0.50) | -0.8% | 8.3% | level |
| mixed 20 min | SET p99.9 ms | 0.76 (0.71-0.81) | 0.79 (0.75-0.83) | -3.5% | 13.6% | level |
| snapshot BGSAVE | memory added GiB | 3.57 (3.55-3.59) | 3.50 (3.50-3.51) | 1.8% | 1.2% | **worse beyond spread** |
| snapshot BGSAVE | duration s | 82.2 (82.0-82.3) | 80.9 (80.7-81.1) | 1.6% | 0.5% | **worse beyond spread** |
| snapshot BGSAVE | worst write ms | 214 (203-224) | 206 (206-207) | 3.5% | 10.1% | level |
| snapshot BGSAVE | write p99.9 ms | 1.92 (1.85-1.98) | 1.98 (1.97-1.98) | -3.0% | 7.1% | level |
| snapshot BGSAVE | write p99 before ms | 1.44 (1.42-1.46) | 1.27 (1.25-1.29) | 12.9% | 3.1% | **worse beyond spread** |
| snapshot BGSAVE | memory 60 s after GiB | 33.84 (33.83-33.85) | 33.77 (33.77-33.77) | 0.2% | 0.1% | level (beyond spread, under 1%) |
| snapshot BGREWRITEAOF | memory added GiB | 2.40 (2.38-2.42) | 2.40 (2.32-2.48) | 0.1% | 6.6% | level |
| snapshot BGREWRITEAOF | duration s | 83.4 (83.1-83.7) | 82.1 (82.0-82.1) | 1.6% | 0.8% | **worse beyond spread** |
| snapshot BGREWRITEAOF | worst write ms | 216 (215-218) | 220 (219-221) | -1.8% | 1.1% | better |
| snapshot BGREWRITEAOF | write p99.9 ms | 1.87 (1.81-1.94) | 1.90 (1.85-1.95) | -1.5% | 6.8% | level |
| snapshot BGREWRITEAOF | write p99 before ms | 1.34 (1.29-1.39) | 1.27 (1.26-1.29) | 5.3% | 7.1% | level |
| snapshot BGREWRITEAOF | memory 60 s after GiB | 35.93 (35.64-36.22) | 35.87 (35.52-36.22) | 0.2% | 1.9% | level |
| #284 eviction | keys evicted | 0 | 0 | -% | 0.0% | - |
| active defrag | RSS final MB | 389 (388-390) | 394 (387-402) | -1.4% | 3.9% | level |
| active defrag | RSS returned MB | 334 (333-335) | 328 (320-335) | 1.9% | 4.7% | level |
| active defrag | give-back s | 12.0 (11.9-12.1) | 10.9 (9.8-12.1) | 9.7% | 21.2% | level |
| active defrag | compaction s | 12 (12-13) | 10 (9-12) | 19.0% | 28.6% | level |
| active defrag | slab final MB | 321 (321-321) | 328 (321-336) | -2.3% | 4.7% | level |
| active defrag | GET p99 during compaction ms | 0.43 (0.32-0.54) | 0.49 (0.40-0.57) | -11.5% | 52.0% | level |
| active defrag | GET p99.9 during compaction ms | 0.70 (0.66-0.74) | 0.67 (0.54-0.79) | 5.7% | 36.7% | level |
| active defrag | SET p99 during compaction ms | 0.42 (0.23-0.61) | 0.17 (0.14-0.20) | 143.9% | 91.1% | **worse beyond spread** |
| active defrag | SET p99.9 during compaction ms | 0.53 (0.26-0.81) | 0.32 (0.19-0.46) | 65.6% | 104.7% | level |
| active defrag | GET p99 during / before (same run) | 0.99 (0.99-1.00) | 1.11 (1.06-1.16) | -10.8% | 8.8% | better |
| active defrag | GET p99.9 during / before (same run) | 0.99 (0.98-1.01) | 1.01 (1.00-1.03) | -2.0% | 2.7% | level |
| active defrag | SET p99 during / before (same run) | 1.02 (0.99-1.06) | 1.00 (1.00-1.00) | 2.1% | 6.7% | level |
| active defrag | SET p99.9 during / before (same run) | 1.05 (1.03-1.07) | 1.02 (0.97-1.07) | 2.9% | 9.1% | level |
| AOF-on writes | SET p50 ms | 0.399 (0.375-0.407) | 0.395 (0.351-0.407) | 1.0% | 14.2% | level |
| AOF-on writes | SET p99 ms | 0.983 (0.935-1.015) | 0.971 (0.903-1.007) | 1.2% | 10.7% | level |
| AOF-on writes | SET p99.9 ms | 1.679 (1.343-2.023) | 1.547 (1.327-2.015) | 8.5% | 44.5% | level |
| AOF-on writes | worst write ms | 134 (100-162) | 118 (94-133) | 14.3% | 46.3% | level |
| AOF-on writes | engine CPU s | 562 (556-564) | 564 (558-570) | -0.4% | 2.1% | level |

Flagged: warefeats set-get-small ops/s (-1.4%, spread 0.0%); snapshot BGSAVE memory added GiB (+1.8%, spread 1.2%); snapshot BGSAVE duration s (+1.6%, spread 0.5%); snapshot BGSAVE write p99 before ms (+12.9%, spread 3.1%); snapshot BGREWRITEAOF duration s (+1.6%, spread 0.8%); active defrag SET p99 during compaction ms (+143.9%, spread 91.1%)

