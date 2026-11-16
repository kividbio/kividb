# memory suite: generated tables

## warefeats/kv-stores harness (a324d29)

1.2.1 and 1.2.0: median (min-max) over their rounds; one run for the others.

### memory-load: 1M keys x 100 B, then a second million on the same engine

| engine | runs | 1st M RSS B/key | 1st M used B/key | 2nd M RSS B/key | 2nd M used B/key |
|---|---|---|---|---|---|
| KiviDB 1.2.1 (ccba20e2) | 1 | 149.0 | 146.9 | 147.3 | 146.9 |
| KiviDB 1.2.0 | 1 | 149.2 | 146.9 | 147.5 | 146.9 |
| Dragonfly 2.0.2 | 1 | 137.0 | 138.7 | 147.6 | 147.1 |
| Redis 8.10.3 | 1 | 165.8 | 165.3 | 166.4 | 166.1 |
| Valkey 9.1.3 | 1 | 170.2 | 167.1 | 173.4 | 169.5 |

### memory-load-random: 1M keys x 100 B, then a second million on the same engine

| engine | runs | 1st M RSS B/key | 1st M used B/key | 2nd M RSS B/key | 2nd M used B/key |
|---|---|---|---|---|---|
| KiviDB 1.2.1 (ccba20e2) | 1 | 149.0 | 146.9 | 149.6 | 146.9 |
| KiviDB 1.2.0 | 1 | 149.1 | 146.9 | 147.6 | 146.9 |
| Dragonfly 2.0.2 | 1 | 153.2 | 154.7 | 163.2 | 163.1 |
| Redis 8.10.3 | 1 | 165.7 | 165.3 | 166.6 | 166.1 |
| Valkey 9.1.3 | 1 | 170.5 | 167.1 | 173.4 | 169.5 |

### set-get-small: SET:GET 1:10, 100 B, pipeline 1

| engine | runs | ops/s (median of passes) | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| KiviDB 1.2.1 (ccba20e2) | 1 | 918,129 | 0.199 | 0.419 | 0.595 | 39.5 |
| KiviDB 1.2.0 | 1 | 918,224 | 0.199 | 0.415 | 0.623 | 40.0 |
| Dragonfly 2.0.2 | 1 | 757,768 | 0.251 | 0.539 | 0.723 | 35.2 |
| Redis 8.10.3 | 1 | 132,550 | 1.379 | 2.463 | 2.639 | 7.6 |
| Valkey 9.1.3 | 1 | 123,195 | 1.447 | 2.911 | 3.151 | 7.4 |

### set-get-large: SET:GET 1:10, 10 KB, pipeline 1

| engine | runs | ops/s (median of passes) | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| KiviDB 1.2.1 (ccba20e2) | 1 | 319,513 | 0.607 | 1.099 | 1.283 | 24.9 |
| KiviDB 1.2.0 | 1 | 319,548 | 0.611 | 1.039 | 1.231 | 24.4 |
| Dragonfly 2.0.2 | 1 | 321,424 | 0.607 | 1.055 | 1.607 | 26.2 |
| Redis 8.10.3 | 1 | 78,494 | 2.271 | 4.671 | 4.895 | 7.6 |
| Valkey 9.1.3 | 1 | 69,189 | 2.687 | 5.535 | 5.807 | 7.2 |

### pipeline-30: SET:GET 1:10, 100 B, pipeline 30

| engine | runs | ops/s (median of passes) | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| KiviDB 1.2.1 (ccba20e2) | 1 | 13,387,181 | 0.395 | 0.855 | 1.023 | 48.8 |
| KiviDB 1.2.0 | 1 | 12,670,308 | 0.435 | 0.915 | 1.203 | 45.9 |
| Dragonfly 2.0.2 | 1 | 5,885,345 | 0.939 | 2.103 | 4.079 | 25.7 |
| Redis 8.10.3 | 1 | 1,377,425 | 3.703 | 7.775 | 8.255 | 5.3 |
| Valkey 9.1.3 | 1 | 1,434,355 | 3.671 | 7.615 | 8.191 | 5.7 |

## Memory as the dataset grows (random values)

RSS bytes per key (used_memory bytes per key). 1.2.1 and 1.2.0: median over rounds.

### 100 B values (random)

| engine | runs | 1M keys | 5M keys | 10M keys | 25M keys | 50M keys | RSS at largest | load s (all stages) |
|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.1 (ccba20e2) | 1 | 153.5 (146.9) | 146.2 (143.1) | 144.7 (143.1) | 140.9 (140.1) | 140.6 (140.1) | 6.57 GiB | 15.6 |
| Redis 8.10.3 | 1 | 170.9 (165.4) | 179.4 (177.2) | 179.1 (177.4) | 174.6 (173.1) | 174.5 (173.1) | 8.15 GiB | 65.7 |
| Valkey 9.1.3 | 1 | 172.4 (167.0) | 169.2 (166.4) | 169.3 (166.7) | 166.6 (165.4) | 166.6 (165.4) | 7.77 GiB | 60.6 |
| Dragonfly 2.0.2 | 1 | 164.6 (154.8) | 150.5 (148.0) | 154.5 (148.9) | 142.9 (139.4) | 141.5 (139.6) | 6.65 GiB | 18.5 |
| KiviDB 1.2.0 | 1 | 153.2 (146.9) | 146.3 (143.1) | 144.9 (143.1) | 141.0 (140.1) | 140.7 (140.1) | 6.57 GiB | 15.3 |

### 1024 B values (random)

| engine | runs | 1M keys | 5M keys | 10M keys | 25M keys | 50M keys | RSS at largest | load s (all stages) |
|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.1 (ccba20e2) | 1 | 1085.9 (1074.9) | 1077.6 (1071.1) | 1076.2 (1071.1) | 1072.0 (1068.1) | 1071.7 (1068.1) | 49.92 GiB | 23.8 |
| Redis 8.10.3 | 1 | 1347.1 (1333.4) | 1356.9 (1345.2) | 1356.7 (1345.4) | 1352.2 (1341.1) | 1352.2 (1341.1) | 62.99 GiB | 101.5 |
| Valkey 9.1.3 | 1 | 1352.9 (1336.7) | 1347.4 (1334.8) | 1347.3 (1334.8) | 1344.4 (1333.4) | 1344.4 (1333.4) | 62.62 GiB | 65.5 |
| Dragonfly 2.0.2 | 1 | 1150.6 (1082.8) | 1091.3 (1076.0) | 1085.5 (1076.9) | 1073.0 (1067.4) | 1071.1 (1067.6) | 49.93 GiB | 27.5 |
| KiviDB 1.2.0 | 1 | 1085.6 (1074.9) | 1077.3 (1071.1) | 1075.9 (1071.1) | 1071.9 (1068.1) | 1071.6 (1068.1) | 49.92 GiB | 22.4 |

## Snapshot under a steady 100k SET/s (uniform random keys): 30M keys x 1 KB, random values

| engine | round | event | memory before GiB | peak during GiB | added GiB | added % | duration s | worst write ms | worst 1-s p99 ms | write p99.9 over window ms | p99 before ms | memory 60 s after GiB | used after GiB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.1 (ccba20e2) | r1 | BGSAVE | 30.31 | 33.46 | 3.15 | 10% | 71.3 | 229 | 2.02 | 2.38 | 1.73 | 33.43 | 30.09 |
| KiviDB 1.2.1 (ccba20e2) | r1 | BGREWRITEAOF | 33.43 | 35.53 | 2.10 | 6% | 73.4 | 252 | 2.02 | 2.54 | 1.71 | 35.00 | 30.09 |
| Redis 8.10.3 | r1 | BGSAVE | 37.62 | 71.17 | 33.54 | 89% | 180.7 | 403 | 2.35 | 8.51 | 0.73 | 37.62 | 37.29 |
| Redis 8.10.3 | r1 | BGREWRITEAOF | 37.61 | 70.94 | 33.33 | 89% | 179.2 | 1015 | 2.37 | 9.21 | 0.81 | 37.61 | 37.28 |
| Valkey 9.1.3 | r1 | BGSAVE | 37.86 | 67.08 | 29.22 | 77% | 164.0 | 392 | 1.67 | 2.64 | 0.69 | 38.19 | 37.42 |
| Valkey 9.1.3 | r1 | BGREWRITEAOF | 38.31 | 68.69 | 30.38 | 79% | 179.1 | 1012 | 2.00 | 2.67 | 0.66 | 38.47 | 37.40 |
| Dragonfly 2.0.2 | r1 | BGSAVE | 30.74 | 30.87 | 0.13 | 0% | 55.5 | 369 | 19.45 | 10.05 | 0.52 | 30.73 | 30.61 |
| KiviDB 1.2.0 | r1 | BGSAVE | 30.30 | 33.45 | 3.14 | 10% | 71.3 | 289 | 2.13 | 2.69 | 1.77 | 33.42 | 30.09 |
| KiviDB 1.2.0 | r1 | BGREWRITEAOF | 33.42 | 35.51 | 2.09 | 6% | 72.8 | 251 | 2.43 | 2.62 | 1.70 | 35.14 | 30.09 |

### Snapshot, per engine: median over rounds

| engine | runs | event | added GiB | added % | duration s | worst write ms | write p99.9 ms | p99 before ms | memory 60 s after GiB |
|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.1 (ccba20e2) | 1 | BGSAVE | 3.15 | 10 | 71.3 | 229 | 2.38 | 1.73 | 33.43 |
| KiviDB 1.2.1 (ccba20e2) | 1 | BGREWRITEAOF | 2.10 | 6 | 73.4 | 252 | 2.54 | 1.71 | 35.00 |
| Redis 8.10.3 | 1 | BGSAVE | 33.54 | 89 | 180.7 | 403 | 8.51 | 0.73 | 37.62 |
| Redis 8.10.3 | 1 | BGREWRITEAOF | 33.33 | 89 | 179.2 | 1015 | 9.21 | 0.81 | 37.61 |
| Valkey 9.1.3 | 1 | BGSAVE | 29.22 | 77 | 164.0 | 392 | 2.64 | 0.69 | 38.19 |
| Valkey 9.1.3 | 1 | BGREWRITEAOF | 30.38 | 79 | 179.1 | 1012 | 2.67 | 0.66 | 38.47 |
| Dragonfly 2.0.2 | 1 | BGSAVE | 0.13 | 0 | 55.5 | 369 | 10.05 | 0.52 | 30.73 |
| KiviDB 1.2.0 | 1 | BGSAVE | 3.14 | 10 | 71.3 | 289 | 2.69 | 1.77 | 33.42 |
| KiviDB 1.2.0 | 1 | BGREWRITEAOF | 2.09 | 6 | 72.8 | 251 | 2.62 | 1.70 | 35.14 |

## maxmemory + BGSAVE (issue #284): 10M keys x 1 KB, maxmemory = used + 10%, allkeys-lru, 100k same-size overwrites/s (uniform)

| engine | round | keys before | keys after | evicted keys | maxmemory | lowest key count during run | SET p99 / p99.9 ms |
|---|---|---|---|---|---|---|---|
| KiviDB 1.2.1 (ccba20e2) | r1 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.66 / 0.99 |
| Redis 8.10.3 | r1 | 10,000,000 | 10,000,000 | 0 | 13.78 GiB | 10.00M | 0.70 / 1.25 |
| KiviDB 1.2.0 | r1 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.65 / 1.02 |

## Active defrag after deleting a random half: 5M keys x 100 B, probe load 80k GET/s + 20k SET/s (pair B)

Slab = INFO value_slab_bytes (live = value_slab_live_bytes). *Give-back time*: from the end of the delete until RSS has made 90% of its final drop. *Compaction*: seconds with active_defrag_running = 1. Latency: per-second p99 / p99.9 from memtier, median over the seconds of the window (worst second in brackets); *before* = the 50 s before the delete, *during* = while compaction runs.

| build | round | RSS after load MB | slab / live after delete MB | RSS after delete MB | RSS final MB | slab / live final MB | RSS returned MB | give-back s | compaction s | hits | GET p99 before / during ms | GET p99.9 before / during ms | SET p99 before / during ms | SET p99.9 before / during ms | max ms during |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.1 (ccba20e2) | r1 | 724 | 624 / 319 | 724 | 388 | 319 / 319 | 337 | 12.4 | 12 | 1,383,096 | 0.38 / 0.35 (0.41) | 0.72 / 0.68 (0.87) | 0.21 / 0.21 (0.21) | 0.40 / 0.43 (0.76) | 2.0 |
| KiviDB 1.2.1 (ccba20e2) | r2 | 723 | 624 / 319 | 724 | 388 | 320 / 319 | 336 | 13.1 | 13 | 1,396,481 | 0.33 / 0.33 (0.34) | 0.93 / 0.89 (1.02) | 0.62 / 0.61 (0.67) | 0.73 / 0.73 (0.84) | 2.0 |
| KiviDB 1.2.1 (ccba20e2) | r3 | 723 | 624 / 319 | 723 | 389 | 321 / 319 | 334 | 12.0 | 13 | 1,404,450 | 0.43 / 0.38 (0.50) | 0.69 / 0.66 (0.86) | 0.15 / 0.15 (0.16) | 0.17 / 0.18 (0.21) | 1.8 |
| KiviDB 1.2.1 (ccba20e2) | r4 | 722 | 624 / 319 | 723 | 389 | 321 / 319 | 334 | 12.0 | 13 | 1,386,667 | 0.27 / 0.26 (0.35) | 0.49 / 0.54 (0.81) | 0.16 / 0.15 (0.16) | 0.27 / 0.26 (0.35) | 2.5 |
| KiviDB 1.2.1 (ccba20e2) (activedefrag no) | r1 | 724 | 624 / 319 | 724 | 691 | 624 / 319 | 33 | 1.1 | 0 | 0 | 0.34 / - | 0.44 / - | 0.56 / - | 0.71 / - | - |
| KiviDB 1.2.0 | r1 | 722 | 624 / 319 | 722 | 386 | 321 / 319 | 336 | 11.8 | 13 | 1,381,440 | 0.28 / 0.29 (0.38) | 0.42 / 0.44 (0.80) | 0.14 / 0.14 (0.14) | 0.20 / 0.19 (0.27) | 1.8 |
| KiviDB 1.2.0 | r2 | 722 | 624 / 319 | 723 | 388 | 321 / 319 | 334 | 12.9 | 13 | 1,391,133 | 0.28 / 0.26 (0.29) | 0.59 / 0.57 (0.72) | 0.14 / 0.14 (0.14) | 0.16 / 0.14 (0.18) | 1.9 |
| KiviDB 1.2.0 | r3 | 722 | 624 / 319 | 722 | 387 | 321 / 319 | 336 | 13.0 | 13 | 1,373,448 | 0.29 / 0.29 (0.32) | 0.81 / 0.81 (0.97) | 0.14 / 0.14 (0.15) | 0.18 / 0.19 (0.29) | 2.2 |
| KiviDB 1.2.0 | r4 | 723 | 624 / 319 | 723 | 389 | 321 / 319 | 334 | 13.1 | 13 | 1,375,828 | 0.14 / 0.14 (0.15) | 0.21 / 0.22 (0.34) | 0.14 / 0.14 (0.14) | 0.20 / 0.21 (0.35) | 2.1 |
| KiviDB 1.2.0 (activedefrag no) | r2 | 722 | 624 / 319 | 722 | 686 | 624 / 319 | 36 | 238.8 | 0 | 0 | 0.34 / - | 0.47 / - | 0.12 / - | 0.14 / - | - |

## 1.2.1 against 1.2.0

Median (min-max) over rounds. *Change* is the 1.2.1 median vs the 1.2.0 median; *spread* is the larger of the two builds' (max-min)/median. A row is flagged when 1.2.1 is worse by more than the spread and by at least 1% (memory counts repeat to a few tenths of a percent, so a spread near zero would otherwise flag a 0.2% difference).

| section | metric | 1.2.1 | 1.2.0 | change | spread | verdict |
|---|---|---|---|---|---|---|
| warefeats ASCII | 1st M RSS B/key | 149.0 | 149.2 | -0.1% | 0.0% | better |
| warefeats ASCII | 1st M used B/key | 146.9 | 146.9 | 0.0% | 0.0% | level |
| warefeats ASCII | 2nd M RSS B/key | 147.3 | 147.5 | -0.1% | 0.0% | better |
| warefeats random | 1st M RSS B/key | 149.0 | 149.1 | -0.1% | 0.0% | better |
| warefeats random | 1st M used B/key | 146.9 | 146.9 | 0.0% | 0.0% | level |
| warefeats random | 2nd M RSS B/key | 149.6 | 147.6 | 1.4% | 0.0% | **worse beyond spread** |
| warefeats set-get-small | ops/s | 918,129 | 918,224 | -0.0% | 0.0% | level (beyond spread, under 1%) |
| warefeats set-get-small | p99 ms | 0.419 | 0.415 | 1.0% | 0.0% | level (beyond spread, under 1%) |
| warefeats set-get-small | p99.9 ms | 0.595 | 0.623 | -4.5% | 0.0% | better |
| warefeats set-get-large | ops/s | 319,513 | 319,548 | -0.0% | 0.0% | level (beyond spread, under 1%) |
| warefeats set-get-large | p99 ms | 1.099 | 1.039 | 5.8% | 0.0% | **worse beyond spread** |
| warefeats set-get-large | p99.9 ms | 1.283 | 1.231 | 4.2% | 0.0% | **worse beyond spread** |
| warefeats pipeline-30 | ops/s | 13,387,181 | 12,670,308 | 5.7% | 0.0% | better |
| warefeats pipeline-30 | p99 ms | 0.855 | 0.915 | -6.6% | 0.0% | better |
| warefeats pipeline-30 | p99.9 ms | 1.023 | 1.203 | -15.0% | 0.0% | better |
| growth 100 B | RSS B/key at 1M | 153.5 | 153.2 | 0.2% | 0.0% | level (beyond spread, under 1%) |
| growth 100 B | RSS B/key at 5M | 146.2 | 146.3 | -0.1% | 0.0% | better |
| growth 100 B | RSS B/key at 10M | 144.7 | 144.9 | -0.2% | 0.0% | better |
| growth 100 B | RSS B/key at 25M | 140.9 | 141.0 | -0.1% | 0.0% | better |
| growth 100 B | RSS B/key at 50M | 140.6 | 140.7 | -0.0% | 0.0% | better |
| growth 100 B | load seconds, 50M keys | 15.6 | 15.3 | 2.1% | 0.0% | **worse beyond spread** |
| growth 1024 B | RSS B/key at 1M | 1085.9 | 1085.6 | 0.0% | 0.0% | level (beyond spread, under 1%) |
| growth 1024 B | RSS B/key at 5M | 1077.6 | 1077.3 | 0.0% | 0.0% | level (beyond spread, under 1%) |
| growth 1024 B | RSS B/key at 10M | 1076.2 | 1075.9 | 0.0% | 0.0% | level (beyond spread, under 1%) |
| growth 1024 B | RSS B/key at 25M | 1072.0 | 1071.9 | 0.0% | 0.0% | level (beyond spread, under 1%) |
| growth 1024 B | RSS B/key at 50M | 1071.7 | 1071.6 | 0.0% | 0.0% | level (beyond spread, under 1%) |
| growth 1024 B | load seconds, 50M keys | 23.8 | 22.4 | 6.1% | 0.0% | **worse beyond spread** |
| snapshot BGSAVE | memory added GiB | 3.15 | 3.14 | 0.2% | 0.0% | level (beyond spread, under 1%) |
| snapshot BGSAVE | duration s | 71.3 | 71.3 | 0.0% | 0.0% | level (beyond spread, under 1%) |
| snapshot BGSAVE | worst write ms | 229 | 289 | -21.0% | 0.0% | better |
| snapshot BGSAVE | write p99.9 ms | 2.38 | 2.69 | -11.3% | 0.0% | better |
| snapshot BGSAVE | write p99 before ms | 1.73 | 1.77 | -2.3% | 0.0% | better |
| snapshot BGSAVE | memory 60 s after GiB | 33.43 | 33.42 | 0.0% | 0.0% | level (beyond spread, under 1%) |
| snapshot BGREWRITEAOF | memory added GiB | 2.10 | 2.09 | 0.3% | 0.0% | level (beyond spread, under 1%) |
| snapshot BGREWRITEAOF | duration s | 73.4 | 72.8 | 0.7% | 0.0% | level (beyond spread, under 1%) |
| snapshot BGREWRITEAOF | worst write ms | 252 | 251 | 0.1% | 0.0% | level (beyond spread, under 1%) |
| snapshot BGREWRITEAOF | write p99.9 ms | 2.54 | 2.62 | -3.0% | 0.0% | better |
| snapshot BGREWRITEAOF | write p99 before ms | 1.71 | 1.70 | 0.5% | 0.0% | level (beyond spread, under 1%) |
| snapshot BGREWRITEAOF | memory 60 s after GiB | 35.00 | 35.14 | -0.4% | 0.0% | better |
| #284 eviction | keys evicted | 0 | 0 | -% | 0.0% | - |
| active defrag | RSS final MB | 389 (388-389) | 388 (386-389) | 0.3% | 0.8% | level |
| active defrag | RSS returned MB | 335 (334-337) | 335 (334-336) | 0.0% | 0.9% | level |
| active defrag | give-back s | 12.2 (12.0-13.1) | 13.0 (11.8-13.1) | -6.1% | 9.8% | level |
| active defrag | compaction s | 13 (12-13) | 13 (13-13) | 0.0% | 7.7% | level |
| active defrag | slab final MB | 320 (319-321) | 321 (321-321) | -0.1% | 0.5% | level |
| active defrag | GET p99 during compaction ms | 0.34 (0.26-0.38) | 0.27 (0.14-0.29) | 24.4% | 56.1% | level |
| active defrag | GET p99.9 during compaction ms | 0.67 (0.54-0.89) | 0.50 (0.22-0.81) | 32.6% | 116.1% | level |
| active defrag | SET p99 during compaction ms | 0.18 (0.15-0.61) | 0.14 (0.14-0.14) | 28.8% | 259.2% | level |
| active defrag | SET p99.9 during compaction ms | 0.35 (0.18-0.73) | 0.19 (0.14-0.21) | 81.7% | 156.8% | level |
| active defrag | GET p99 during / before (same run) | 0.95 (0.88-1.00) | 1.00 (0.91-1.03) | -5.4% | 12.9% | level |
| active defrag | GET p99.9 during / before (same run) | 0.95 (0.94-1.10) | 1.02 (0.95-1.05) | -6.5% | 16.8% | level |
| active defrag | SET p99 during / before (same run) | 0.99 (0.95-1.00) | 1.00 (0.94-1.00) | -0.6% | 5.6% | level |
| active defrag | SET p99.9 during / before (same run) | 1.02 (0.97-1.07) | 1.00 (0.88-1.04) | 2.3% | 16.6% | level |

Flagged: warefeats random 2nd M RSS B/key (+1.4%, spread 0.0%); warefeats set-get-large p99 ms (+5.8%, spread 0.0%); warefeats set-get-large p99.9 ms (+4.2%, spread 0.0%); growth 100 B load seconds, 50M keys (+2.1%, spread 0.0%); growth 1024 B load seconds, 50M keys (+6.1%, spread 0.0%)

