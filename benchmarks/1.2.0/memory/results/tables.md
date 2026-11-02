# memory suite: generated tables

## warefeats/kv-stores harness (a324d29)

### memory-load: 1M keys x 100 B, then a second million on the same engine

| engine | 1st M RSS B/key | 1st M used B/key | 2nd M RSS B/key | 2nd M used B/key | RSS MB after 1st M | baseline RSS MB |
|---|---|---|---|---|---|---|
| kividb-rc4 | 149 | 146.9 | 147.7 | 146.9 | 158.48 | 16.41 |
| kividb-rc4-run2 | 149 | 146.9 | 146.9 | 146.9 | 158.71 | 16.57 |
| kividb-1.1.3 | 267 | 246.4 | 352.5 | 246.4 | 269.62 | 14.99 |
| dragonfly | 137.2 | 138.7 | 147.5 | 147.1 | 196.29 | 65.47 |
| redis | 165.7 | 165.3 | 166.4 | 166.1 | 182.36 | 24.35 |
| valkey | 170.3 | 167.1 | 172.7 | 169.4 | 175.28 | 12.91 |

### memory-load-random: 1M keys x 100 B, then a second million on the same engine

| engine | 1st M RSS B/key | 1st M used B/key | 2nd M RSS B/key | 2nd M used B/key | RSS MB after 1st M | baseline RSS MB |
|---|---|---|---|---|---|---|
| kividb-rc4 | 149.1 | 146.9 | 146.9 | 146.9 | 158.58 | 16.36 |
| kividb-rc4-run2 | 149.2 | 146.9 | 146.8 | 146.9 | 158.73 | 16.45 |
| kividb-1.1.3 | 266.9 | 246.4 | 352.5 | 246.4 | 269.89 | 15.34 |
| dragonfly | 153.2 | 154.7 | 163 | 163.1 | 212 | 65.87 |
| redis | 165.4 | 165.3 | 166.7 | 166.1 | 182.4 | 24.64 |
| valkey | 170 | 167.1 | 172.9 | 169.5 | 174.91 | 12.81 |

### set-get-small

| engine | ops/s (median of passes) | min-max ops/s | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| kividb-rc4 | 945,842 | 902,957-989,747 | 0.199 | 0.383 | 0.519 | 41.4 |
| kividb-rc4-run2 | 952,032 | 931,462-1,018,824 | 0.199 | 0.383 | 0.535 | 39.4 |
| kividb-1.1.3 | 942,268 | 892,319-963,534 | 0.199 | 0.383 | 0.547 | 38.4 |
| dragonfly | 871,102 | 858,339-884,827 | 0.207 | 0.407 | 0.655 | 38.5 |
| redis | 161,209 | 156,932-165,522 | 1.223 | 1.859 | 2.159 | 9.6 |
| valkey | 161,806 | 161,024-162,524 | 1.223 | 1.839 | 2.103 | 9.9 |

### set-get-large

| engine | ops/s (median of passes) | min-max ops/s | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| kividb-rc4 | 320,517 | 319,883-320,936 | 0.623 | 1.047 | 1.279 | 26.2 |
| kividb-rc4-run2 | 321,117 | 319,944-321,514 | 0.623 | 1.191 | 1.327 | 25.3 |
| kividb-1.1.3 | 320,426 | 319,217-321,154 | 0.623 | 1.019 | 1.267 | 25.8 |
| dragonfly | 322,249 | 320,894-322,793 | 0.623 | 1.027 | 1.539 | 25 |
| redis | 83,940 | 80,691-87,023 | 2.135 | 4.191 | 4.591 | 8.1 |
| valkey | 85,526 | 83,825-93,486 | 2.103 | 4.079 | 4.463 | 8.1 |

### pipeline-30

| engine | ops/s (median of passes) | min-max ops/s | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| kividb-rc4 | 14,700,533 | 14,570,644-14,936,211 | 0.359 | 0.787 | 1.031 | 59.7 |
| kividb-rc4-run2 | 14,649,783 | 14,587,344-14,760,733 | 0.359 | 0.787 | 1.039 | 58.1 |
| kividb-1.1.3 | 11,363,019 | 11,239,787-11,475,883 | 0.499 | 1.167 | 1.439 | 44.7 |
| dragonfly | 8,076,230 | 7,936,678-8,142,030 | 0.659 | 1.539 | 3.543 | 33.4 |
| redis | 1,504,429 | 1,455,942-1,513,746 | 3.391 | 9.471 | 10.623 | 6.2 |
| valkey | 1,557,586 | 1,480,617-1,593,801 | 3.847 | 8.383 | 9.119 | 6.3 |

## Memory as the dataset grows (random values)

### 100 B values (random): RSS bytes per key (used_memory bytes per key)

| engine | 1M keys | 5M keys | 10M keys | 25M keys | 50M keys | RSS at largest |
|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc4 | 153.9 (146.9) | 146.7 (143.1) | 145.0 (143.1) | 141.1 (140.1) | 140.7 (140.1) | 6.57 GiB |
| KiviDB 1.1.3 | 220.5 (194.1) | 197.5 (176.8) | 197.1 (176.8) | 183.1 (163.0) | 183.1 (163.0) | 8.54 GiB |
| Redis 8.10.3 | 170.2 (165.4) | 179.3 (177.3) | 179.0 (177.4) | 174.5 (173.1) | 174.4 (173.1) | 8.15 GiB |
| Valkey 9.1.3 | 172.4 (167.0) | 169.2 (166.4) | 169.3 (166.7) | 166.6 (165.4) | 166.6 (165.4) | 7.77 GiB |
| Dragonfly 2.0.2 | 163.8 (154.8) | 150.3 (148.0) | 155.9 (148.9) | 143.3 (139.4) | 141.5 (139.6) | 6.65 GiB |

### 1024 B values (random): RSS bytes per key (used_memory bytes per key)

| engine | 1M keys | 5M keys | 10M keys | 25M keys | 50M keys | RSS at largest |
|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc4 | 1085.7 (1074.9) | 1078.0 (1071.1) | 1076.2 (1071.1) | 1072.1 (1068.1) | 1071.7 (1068.1) | 49.92 GiB |
| KiviDB 1.1.3 | 1130.5 (1118.1) | 1109.7 (1100.8) | 1109.2 (1100.8) | 1095.2 (1087.0) | 1095.1 (1087.0) | 51.01 GiB |
| Redis 8.10.3 | 1351.8 (1333.4) | 1356.9 (1345.2) | 1356.7 (1345.4) | 1352.2 (1341.1) | 1352.2 (1341.1) | 62.99 GiB |
| Valkey 9.1.3 | 1352.9 (1336.6) | 1347.4 (1334.8) | 1347.3 (1334.8) | 1344.4 (1333.4) | 1344.4 (1333.4) | 62.62 GiB |
| Dragonfly 2.0.2 | 1097.6 (1082.8) | 1091.1 (1076.0) | 1084.9 (1076.9) | 1073.3 (1067.4) | 1071.0 (1067.6) | 49.93 GiB |

## Mixed workload: 10M keys, 150k ops/s, 80% GET / 20% SET (64-1024 B, TTL 60-600 s), 20 min

| engine | RSS at start GiB | RSS peak GiB | RSS at end GiB | used at end GiB | RSS/used at end | keys at end (M) | ops/s | GET p50/p99/p99.9 ms | SET p50/p99/p99.9 ms |
|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc4 | 1.36 | 1.44 | 1.44 | 1.35 | 1.07 | 9.74 | 150,000 | 0.28/0.57/0.78 | 0.28/0.58/0.85 |

## Snapshot under a steady 100k SET/s: 30M keys x 1 KB, random values

| engine | event | memory before GiB | peak during GiB | added GiB | added % | duration s | max write latency ms | worst 1-s p99 ms | write p99.9 over window ms | baseline 1-s p99 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc4 | BGSAVE | 30.33 | 30.58 | 0.25 | 1% | 76.3 | 314.57 | 2.01 | 2.10 | 1.53 |
| KiviDB 1.2.0-rc4 | BGREWRITEAOF | 30.38 | 30.71 | 0.33 | 1% | 78.8 | 344.20 | 1.81 | 2.11 | 1.41 |
| KiviDB 1.1.3 | BGSAVE | 63.60 | 64.53 | 0.92 | 1% | 93.1 | 408.66 | 1.06 | 1.22 | 0.85 |
| KiviDB 1.1.3 | BGREWRITEAOF | 63.63 | 64.53 | 0.89 | 1% | 88.0 | 400.53 | 0.68 | 0.85 | 0.49 |
| Redis 8.10.3 | BGSAVE | 37.61 | 39.03 | 1.42 | 4% | 182.7 | 405.75 | 1.56 | 8.57 | 0.59 |
| Redis 8.10.3 | BGREWRITEAOF | 37.61 | 39.00 | 1.39 | 4% | 180.7 | 371.58 | 2.37 | 9.21 | 0.64 |
| Valkey 9.1.3 | BGSAVE | 37.82 | 39.21 | 1.39 | 4% | 170.3 | 397.45 | 1.14 | 1.70 | 0.66 |
| Valkey 9.1.3 | BGREWRITEAOF | 38.01 | 39.18 | 1.17 | 3% | 167.4 | 492.43 | 0.92 | 1.55 | 0.61 |
| Dragonfly 2.0.2 | BGSAVE | 30.73 | 30.85 | 0.12 | 0% | 55.8 | 8.06 | 0.79 | 1.11 | 0.59 |

## maxmemory + BGSAVE (issue #284): 10M keys x 1 KB, maxmemory = used + 10%, allkeys-lru, 100k same-size overwrites/s

| engine | keys before | keys after | evicted keys | maxmemory | lowest key count during run |
|---|---|---|---|---|---|
| KiviDB 1.2.0-rc4 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M |
| KiviDB 1.1.3 | 10,000,000 | 9,992,201 | 7,984 | 11.28 GiB | 9.99M |
| Redis 8.10.3 | 10,000,000 | 10,000,000 | 0 | 13.79 GiB | 10.00M |

