# memory suite: generated tables

## warefeats/kv-stores harness (a324d29)

1.2.1 and 1.2.0: median (min-max) over their rounds; one run for the others.

### memory-load: 1M keys x 100 B, then a second million on the same engine

| engine | runs | 1st M RSS B/key | 1st M used B/key | 2nd M RSS B/key | 2nd M used B/key |
|---|---|---|---|---|---|
| KiviDB 1.2.1 (35a413c0) | 3 | 148.9 (148.7-149.0) | 146.9 (146.9-146.9) | 149.3 (147.2-149.6) | 146.9 (146.9-146.9) |
| KiviDB 1.2.0 | 3 | 148.9 (148.9-149.0) | 146.9 (146.9-146.9) | 147.0 (147.0-156.6) | 146.9 (146.9-146.9) |

### memory-load-random: 1M keys x 100 B, then a second million on the same engine

| engine | runs | 1st M RSS B/key | 1st M used B/key | 2nd M RSS B/key | 2nd M used B/key |
|---|---|---|---|---|---|
| KiviDB 1.2.1 (35a413c0) | 3 | 149.1 (148.9-149.1) | 146.9 (146.9-146.9) | 147.4 (147.3-148.1) | 146.9 (146.9-146.9) |
| KiviDB 1.2.0 | 3 | 149.0 (148.9-149.0) | 146.9 (146.9-146.9) | 147.0 (147.0-156.7) | 146.9 (146.9-146.9) |

### set-get-small: SET:GET 1:10, 100 B, pipeline 1

| engine | runs | ops/s (median of passes) | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| KiviDB 1.2.1 (35a413c0) | 3 | 944,642 (898,679-1,097,494) | 0.191 (0.183-0.219) | 0.375 (0.347-0.383) | 0.511 (0.447-0.559) | 40.4 (35.5-48.5) |
| KiviDB 1.2.0 | 3 | 1,050,970 (915,116-1,163,226) | 0.187 (0.175-0.199) | 0.379 (0.367-0.387) | 0.583 (0.575-0.619) | 45.2 (35.0-47.3) |

### set-get-large: SET:GET 1:10, 10 KB, pipeline 1

| engine | runs | ops/s (median of passes) | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| KiviDB 1.2.1 (35a413c0) | 3 | 321,222 (320,026-329,307) | 0.607 (0.603-0.619) | 1.079 (1.051-1.151) | 1.243 (1.239-1.259) | 22.0 (21.7-26.7) |
| KiviDB 1.2.0 | 3 | 321,380 (319,895-329,481) | 0.615 (0.603-0.615) | 1.079 (1.023-1.147) | 1.251 (1.235-1.263) | 22.8 (21.0-23.6) |

### pipeline-30: SET:GET 1:10, 100 B, pipeline 30

| engine | runs | ops/s (median of passes) | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| KiviDB 1.2.1 (35a413c0) | 3 | 14,651,870 (12,517,535-16,841,145) | 0.363 (0.315-0.439) | 0.755 (0.743-0.903) | 1.171 (0.935-1.223) | 60.2 (48.6-63.5) |
| KiviDB 1.2.0 | 3 | 14,770,188 (12,287,379-16,731,168) | 0.367 (0.319-0.447) | 0.767 (0.739-0.919) | 1.095 (0.927-1.203) | 58.8 (48.4-61.0) |

## Memory as the dataset grows (random values)

RSS bytes per key (used_memory bytes per key). 1.2.1 and 1.2.0: median over rounds.

### 100 B values (random)

| engine | runs | 1M keys | 5M keys | 10M keys | 25M keys | 50M keys | RSS at largest | load s (all stages) |
|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.1 (35a413c0) | 3 | 153.5 (146.9) | 146.3 (143.1) | 144.7 (143.1) | 140.9 (140.1) | 140.6 (140.1) | 6.57 GiB | 13.0 (12.9-13.1) |
| KiviDB 1.2.0 | 3 | 153.2 (146.9) | 146.1 (143.1) | 144.8 (143.1) | 141.0 (140.1) | 140.7 (140.1) | 6.57 GiB | 13.1 (12.9-13.2) |

### 1024 B values (random)

| engine | runs | 1M keys | 5M keys | 10M keys | 25M keys | 50M keys | RSS at largest | load s (all stages) |
|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.1 (35a413c0) | 3 | 1086.0 (1074.9) | 1077.6 (1071.1) | 1075.9 (1071.1) | 1071.9 (1068.1) | 1071.6 (1068.1) | 49.92 GiB | 23.3 (23.0-24.0) |
| KiviDB 1.2.0 | 3 | 1085.4 (1074.9) | 1077.6 (1071.1) | 1076.0 (1071.1) | 1072.0 (1068.1) | 1071.6 (1068.1) | 49.92 GiB | 23.3 (23.1-24.3) |

## Snapshot under a steady 100k SET/s (uniform random keys): 30M keys x 1 KB, random values

| engine | round | event | memory before GiB | peak during GiB | added GiB | added % | duration s | worst write ms | worst 1-s p99 ms | write p99.9 over window ms | p99 before ms | memory 60 s after GiB | used after GiB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.1 (35a413c0) | r1 | BGSAVE | 30.29 | 33.87 | 3.57 | 12% | 82.2 | 188 | 1.53 | 1.82 | 1.30 | 33.84 | 30.09 |
| KiviDB 1.2.1 (35a413c0) | r1 | BGREWRITEAOF | 33.84 | 36.21 | 2.37 | 7% | 83.9 | 211 | 1.46 | 1.81 | 1.25 | 35.61 | 30.09 |
| KiviDB 1.2.1 (35a413c0) | r2 | BGSAVE | 30.32 | 33.39 | 3.07 | 10% | 71.1 | 268 | 2.03 | 2.45 | 1.61 | 33.36 | 30.09 |
| KiviDB 1.2.1 (35a413c0) | r2 | BGREWRITEAOF | 33.37 | 35.47 | 2.11 | 6% | 73.0 | 235 | 1.75 | 2.25 | 1.57 | 34.97 | 30.09 |
| KiviDB 1.2.1 (35a413c0) | r3 | BGSAVE | 30.28 | 33.44 | 3.16 | 10% | 71.5 | 202 | 1.46 | 1.71 | 1.29 | 33.41 | 30.09 |
| KiviDB 1.2.1 (35a413c0) | r3 | BGREWRITEAOF | 33.41 | 35.53 | 2.11 | 6% | 73.0 | 221 | 1.42 | 1.69 | 1.20 | 34.99 | 30.09 |
| KiviDB 1.2.0 | r1 | BGSAVE | 30.29 | 33.84 | 3.55 | 12% | 80.8 | 182 | 1.35 | 1.79 | 1.14 | 33.81 | 30.09 |
| KiviDB 1.2.0 | r1 | BGREWRITEAOF | 33.81 | 36.15 | 2.33 | 7% | 82.4 | 209 | 1.56 | 1.79 | 1.23 | 35.66 | 30.09 |
| KiviDB 1.2.0 | r2 | BGSAVE | 30.31 | 33.44 | 3.13 | 10% | 71.6 | 232 | 1.77 | 2.27 | 1.59 | 33.41 | 30.09 |
| KiviDB 1.2.0 | r2 | BGREWRITEAOF | 33.41 | 35.52 | 2.11 | 6% | 73.1 | 232 | 1.74 | 2.27 | 1.57 | 34.99 | 30.09 |
| KiviDB 1.2.0 | r3 | BGSAVE | 30.27 | 33.42 | 3.15 | 10% | 71.5 | 204 | 1.42 | 1.76 | 1.24 | 33.40 | 30.09 |
| KiviDB 1.2.0 | r3 | BGREWRITEAOF | 33.40 | 35.50 | 2.10 | 6% | 73.0 | 218 | 1.62 | 1.74 | 1.29 | 34.99 | 30.09 |

### Snapshot, per engine: median over rounds

| engine | runs | event | added GiB | added % | duration s | worst write ms | write p99.9 ms | p99 before ms | memory 60 s after GiB |
|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.1 (35a413c0) | 3 | BGSAVE | 3.16 (3.07-3.57) | 10 (10-12) | 71.5 (71.1-82.2) | 202 (188-268) | 1.82 (1.71-2.45) | 1.30 (1.29-1.61) | 33.41 (33.36-33.84) |
| KiviDB 1.2.1 (35a413c0) | 3 | BGREWRITEAOF | 2.11 (2.11-2.37) | 6 (6-7) | 73.0 (73.0-83.9) | 221 (211-235) | 1.81 (1.69-2.25) | 1.25 (1.20-1.57) | 34.99 (34.97-35.61) |
| KiviDB 1.2.0 | 3 | BGSAVE | 3.15 (3.13-3.55) | 10 (10-12) | 71.6 (71.5-80.8) | 204 (182-232) | 1.79 (1.76-2.27) | 1.24 (1.14-1.59) | 33.41 (33.40-33.81) |
| KiviDB 1.2.0 | 3 | BGREWRITEAOF | 2.11 (2.10-2.33) | 6 (6-7) | 73.1 (73.0-82.4) | 218 (209-232) | 1.79 (1.74-2.27) | 1.29 (1.23-1.57) | 34.99 (34.99-35.66) |

## maxmemory + BGSAVE (issue #284): 10M keys x 1 KB, maxmemory = used + 10%, allkeys-lru, 100k same-size overwrites/s (uniform)

| engine | round | keys before | keys after | evicted keys | maxmemory | lowest key count during run | SET p99 / p99.9 ms |
|---|---|---|---|---|---|---|---|
| KiviDB 1.2.1 (35a413c0) | r1 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.75 / 1.14 |
| KiviDB 1.2.1 (35a413c0) | r2 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.84 / 1.22 |
| KiviDB 1.2.1 (35a413c0) | r3 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.54 / 0.70 |
| KiviDB 1.2.0 | r1 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.81 / 1.20 |
| KiviDB 1.2.0 | r2 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.85 / 1.25 |
| KiviDB 1.2.0 | r3 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.85 / 1.23 |

## Active defrag after deleting a random half: 5M keys x 100 B, probe load 80k GET/s + 20k SET/s (pair B)

Slab = INFO value_slab_bytes (live = value_slab_live_bytes). *Give-back time*: from the end of the delete until RSS has made 90% of its final drop. *Compaction*: seconds with active_defrag_running = 1. Latency: per-second p99 / p99.9 from memtier, median over the seconds of the window (worst second in brackets); *before* = the 50 s before the delete, *during* = while compaction runs.

| build | round | RSS after load MB | slab / live after delete MB | RSS after delete MB | RSS final MB | slab / live final MB | RSS returned MB | give-back s | compaction s | hits | GET p99 before / during ms | GET p99.9 before / during ms | SET p99 before / during ms | SET p99.9 before / during ms | max ms during |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.1 (35a413c0) | r1 | 723 | 624 / 319 | 724 | 409 | 341 / 319 | 315 | 9.5 | 9 | 1,118,846 | 0.39 / 0.35 (0.37) | 0.66 / 0.56 (0.73) | 0.54 / 0.55 (0.58) | 0.68 / 0.73 (0.81) | 2.1 |
| KiviDB 1.2.1 (35a413c0) | r2 | 723 | 624 / 319 | 723 | 389 | 321 / 319 | 335 | 13.0 | 13 | 1,381,238 | 0.46 / 0.46 (0.58) | 0.70 / 0.68 (0.97) | 0.18 / 0.17 (0.20) | 0.45 / 0.45 (0.66) | 2.4 |
| KiviDB 1.2.1 (35a413c0) | r3 | 722 | 624 / 319 | 723 | 387 | 321 / 319 | 336 | 12.8 | 13 | 1,390,254 | 0.45 / 0.44 (0.53) | 0.70 / 0.61 (0.87) | 0.17 / 0.17 (0.17) | 0.27 / 0.29 (0.38) | 2.4 |
| KiviDB 1.2.1 (35a413c0) | r4 | 724 | 624 / 319 | 724 | 390 | 321 / 319 | 334 | 12.9 | 13 | 1,399,403 | 0.57 / 0.54 (0.62) | 0.77 / 0.74 (0.93) | 0.41 / 0.41 (0.49) | 0.61 / 0.61 (0.67) | 2.4 |
| KiviDB 1.2.1 (35a413c0) (activedefrag no) | r1 | 723 | 624 / 319 | 723 | 688 | 624 / 319 | 35 | 238.8 | 0 | 0 | 0.58 / - | 0.79 / - | 0.25 / - | 0.35 / - | - |
| KiviDB 1.2.0 | r1 | 722 | 624 / 319 | 722 | 387 | 321 / 319 | 335 | 12.7 | 13 | 1,369,599 | 0.56 / 0.55 (0.62) | 0.74 / 0.78 (0.95) | 0.24 / 0.25 (0.26) | 0.42 / 0.38 (0.60) | 2.2 |
| KiviDB 1.2.0 | r2 | 723 | 624 / 319 | 723 | 390 | 321 / 319 | 334 | 12.8 | 14 | 1,386,688 | 0.14 / 0.14 (0.19) | 0.18 / 0.18 (0.21) | 0.15 / 0.15 (0.16) | 0.17 / 0.17 (1.25) | 2.5 |
| KiviDB 1.2.0 | r3 | 723 | 624 / 319 | 724 | 387 | 321 / 319 | 336 | 12.7 | 13 | 1,374,771 | 0.39 / 0.38 (0.44) | 0.56 / 0.54 (0.71) | 0.19 / 0.20 (0.21) | 0.29 / 0.24 (0.37) | 1.8 |
| KiviDB 1.2.0 | r4 | 722 | 624 / 319 | 722 | 385 | 322 / 319 | 337 | 12.6 | 14 | 1,376,847 | 0.60 / 0.50 (0.58) | 0.77 / 0.81 (0.93) | 0.23 / 0.23 (0.26) | 0.43 / 0.38 (0.46) | 2.2 |
| KiviDB 1.2.0 (activedefrag no) | r2 | 723 | 624 / 319 | 723 | 689 | 624 / 319 | 34 | 0.6 | 0 | 0 | 0.32 / - | 0.51 / - | 0.14 / - | 0.17 / - | - |

## 1.2.1 against 1.2.0

Median (min-max) over rounds. *Change* is the 1.2.1 median vs the 1.2.0 median; *spread* is the larger of the two builds' (max-min)/median. A row is flagged when 1.2.1 is worse by more than the spread and by at least 1% (memory counts repeat to a few tenths of a percent, so a spread near zero would otherwise flag a 0.2% difference).

| section | metric | 1.2.1 | 1.2.0 | change | spread | verdict |
|---|---|---|---|---|---|---|
| warefeats ASCII | 1st M RSS B/key | 148.9 (148.7-149.0) | 148.9 (148.9-149.0) | 0.0% | 0.2% | level |
| warefeats ASCII | 1st M used B/key | 146.9 (146.9-146.9) | 146.9 (146.9-146.9) | 0.0% | 0.0% | level |
| warefeats ASCII | 2nd M RSS B/key | 149.3 (147.2-149.6) | 147.0 (147.0-156.6) | 1.6% | 6.5% | level |
| warefeats random | 1st M RSS B/key | 149.1 (148.9-149.1) | 149.0 (148.9-149.0) | 0.1% | 0.1% | level |
| warefeats random | 1st M used B/key | 146.9 (146.9-146.9) | 146.9 (146.9-146.9) | 0.0% | 0.0% | level |
| warefeats random | 2nd M RSS B/key | 147.4 (147.3-148.1) | 147.0 (147.0-156.7) | 0.3% | 6.6% | level |
| warefeats set-get-small | ops/s | 944,642 (898,679-1,097,494) | 1,050,970 (915,116-1,163,226) | -10.1% | 23.6% | level |
| warefeats set-get-small | p99 ms | 0.375 (0.347-0.383) | 0.379 (0.367-0.387) | -1.1% | 9.6% | level |
| warefeats set-get-small | p99.9 ms | 0.511 (0.447-0.559) | 0.583 (0.575-0.619) | -12.3% | 21.9% | level |
| warefeats set-get-large | ops/s | 321,222 (320,026-329,307) | 321,380 (319,895-329,481) | -0.0% | 3.0% | level |
| warefeats set-get-large | p99 ms | 1.079 (1.051-1.151) | 1.079 (1.023-1.147) | 0.0% | 11.5% | level |
| warefeats set-get-large | p99.9 ms | 1.243 (1.239-1.259) | 1.251 (1.235-1.263) | -0.6% | 2.2% | level |
| warefeats pipeline-30 | ops/s | 14,651,870 (12,517,535-16,841,145) | 14,770,188 (12,287,379-16,731,168) | -0.8% | 30.1% | level |
| warefeats pipeline-30 | p99 ms | 0.755 (0.743-0.903) | 0.767 (0.739-0.919) | -1.6% | 23.5% | level |
| warefeats pipeline-30 | p99.9 ms | 1.171 (0.935-1.223) | 1.095 (0.927-1.203) | 6.9% | 25.2% | level |
| growth 100 B | RSS B/key at 1M | 153.5 (153.3-153.5) | 153.2 (152.9-153.3) | 0.2% | 0.3% | level |
| growth 100 B | RSS B/key at 5M | 146.3 (146.0-146.5) | 146.1 (146.1-146.4) | 0.1% | 0.3% | level |
| growth 100 B | RSS B/key at 10M | 144.7 (144.6-144.9) | 144.8 (144.7-144.9) | -0.1% | 0.2% | level |
| growth 100 B | RSS B/key at 25M | 140.9 (140.9-141.0) | 141.0 (140.9-141.0) | -0.1% | 0.1% | level |
| growth 100 B | RSS B/key at 50M | 140.6 (140.6-140.7) | 140.7 (140.6-140.7) | -0.0% | 0.1% | level |
| growth 100 B | load seconds, 50M keys | 13.0 (12.9-13.1) | 13.1 (12.9-13.2) | -0.7% | 2.3% | level |
| growth 1024 B | RSS B/key at 1M | 1086.0 (1085.8-1086.1) | 1085.4 (1085.4-1085.4) | 0.1% | 0.0% | level (beyond spread, under 1%) |
| growth 1024 B | RSS B/key at 5M | 1077.6 (1077.5-1077.7) | 1077.6 (1077.4-1077.6) | 0.0% | 0.0% | level |
| growth 1024 B | RSS B/key at 10M | 1075.9 (1075.9-1076.0) | 1076.0 (1075.9-1076.1) | -0.0% | 0.0% | level |
| growth 1024 B | RSS B/key at 25M | 1071.9 (1071.9-1071.9) | 1072.0 (1072.0-1072.0) | -0.0% | 0.0% | better |
| growth 1024 B | RSS B/key at 50M | 1071.6 (1071.6-1071.6) | 1071.6 (1071.6-1071.6) | -0.0% | 0.0% | better |
| growth 1024 B | load seconds, 50M keys | 23.3 (23.0-24.0) | 23.3 (23.1-24.3) | 0.1% | 4.9% | level |
| snapshot BGSAVE | memory added GiB | 3.16 (3.07-3.57) | 3.15 (3.13-3.55) | 0.2% | 15.8% | level |
| snapshot BGSAVE | duration s | 71.5 (71.1-82.2) | 71.6 (71.5-80.8) | -0.1% | 15.6% | level |
| snapshot BGSAVE | worst write ms | 202 (188-268) | 204 (182-232) | -1.2% | 40.0% | level |
| snapshot BGSAVE | write p99.9 ms | 1.82 (1.71-2.45) | 1.79 (1.76-2.27) | 1.8% | 40.4% | level |
| snapshot BGSAVE | write p99 before ms | 1.30 (1.29-1.61) | 1.24 (1.14-1.59) | 5.2% | 36.8% | level |
| snapshot BGSAVE | memory 60 s after GiB | 33.41 (33.36-33.84) | 33.41 (33.40-33.81) | -0.0% | 1.4% | level |
| snapshot BGREWRITEAOF | memory added GiB | 2.11 (2.11-2.37) | 2.11 (2.10-2.33) | 0.1% | 12.3% | level |
| snapshot BGREWRITEAOF | duration s | 73.0 (73.0-83.9) | 73.1 (73.0-82.4) | -0.0% | 14.9% | level |
| snapshot BGREWRITEAOF | worst write ms | 221 (211-235) | 218 (209-232) | 1.4% | 11.1% | level |
| snapshot BGREWRITEAOF | write p99.9 ms | 1.81 (1.69-2.25) | 1.79 (1.74-2.27) | 0.9% | 31.4% | level |
| snapshot BGREWRITEAOF | write p99 before ms | 1.25 (1.20-1.57) | 1.29 (1.23-1.57) | -3.7% | 30.2% | level |
| snapshot BGREWRITEAOF | memory 60 s after GiB | 34.99 (34.97-35.61) | 34.99 (34.99-35.66) | 0.0% | 1.9% | level |
| #284 eviction | keys evicted | 0 (0-0) | 0 (0-0) | -% | 0.0% | - |
| active defrag | RSS final MB | 389 (387-409) | 387 (385-390) | 0.5% | 5.6% | level |
| active defrag | RSS returned MB | 335 (315-336) | 336 (334-337) | -0.3% | 6.3% | level |
| active defrag | give-back s | 12.8 (9.5-13.0) | 12.7 (12.6-12.8) | 1.2% | 27.9% | level |
| active defrag | compaction s | 13 (9-13) | 14 (13-14) | -3.7% | 30.8% | level |
| active defrag | slab final MB | 321 (321-341) | 321 (321-322) | -0.0% | 6.3% | level |
| active defrag | GET p99 during compaction ms | 0.45 (0.35-0.54) | 0.44 (0.14-0.55) | 2.7% | 92.9% | level |
| active defrag | GET p99.9 during compaction ms | 0.65 (0.56-0.74) | 0.66 (0.18-0.81) | -1.8% | 95.9% | level |
| active defrag | SET p99 during compaction ms | 0.29 (0.17-0.55) | 0.21 (0.15-0.25) | 36.6% | 132.0% | level |
| active defrag | SET p99.9 during compaction ms | 0.53 (0.29-0.73) | 0.31 (0.17-0.38) | 69.5% | 85.0% | level |
| active defrag | GET p99 during / before (same run) | 0.97 (0.91-1.00) | 0.98 (0.83-1.06) | -1.6% | 22.9% | level |
| active defrag | GET p99.9 during / before (same run) | 0.92 (0.85-0.97) | 1.02 (0.96-1.05) | -9.9% | 12.5% | level |
| active defrag | SET p99 during / before (same run) | 0.98 (0.96-1.01) | 1.02 (0.98-1.06) | -3.7% | 8.0% | level |
| active defrag | SET p99.9 during / before (same run) | 1.03 (0.99-1.09) | 0.90 (0.82-1.00) | 14.7% | 19.9% | level |


## Snapshot load: 30M x 1 KB, cold page cache, medians of 3 (loadtime.csv)

| dump written by | loaded by | load s (engine log) | start to first PONG s | peak RSS during load GiB (VmHWM) | RSS after load GiB | used_memory GiB | keys |
|---|---|---:|---:|---:|---:|---:|---:|
| 1.2.0 | 1.2.0 | 100.5 (100.2-101.2) | 106.0 | 37.97 | 35.20 | 30.07 | 30,000,000 |
| 1.2.0 | 1.2.1 | 83.4 (83.3-84.6) | 88.9 | 37.06 | 34.29 | 30.07 | 30,000,000 |
| 1.2.1 | 1.2.0 | 99.4 (99.2-101.1) | 105.2 | 37.97 | 35.20 | 30.07 | 30,000,000 |
| 1.2.1 | 1.2.1 | 84.0 (82.6-84.3) | 89.5 | 37.05 | 34.29 | 30.07 | 30,000,000 |
