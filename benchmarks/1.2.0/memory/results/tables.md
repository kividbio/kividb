# memory suite: generated tables

## warefeats/kv-stores harness (a324d29)

Candidate and rc4: median (min-max) over their rounds; one run for the others.

### memory-load: 1M keys x 100 B, then a second million on the same engine

| engine | runs | 1st M RSS B/key | 1st M used B/key | 2nd M RSS B/key | 2nd M used B/key |
|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | 3 | 149.2 (149.2-149.2) | 146.9 (146.9-146.9) | 146.8 (146.7-154.9) | 146.9 (146.9-146.9) |
| KiviDB 1.2.0-rc4 | 3 | 148.9 (148.9-149.2) | 146.9 (146.9-146.9) | 147.1 (146.8-147.6) | 146.9 (146.9-146.9) |
| KiviDB 1.1.3 | 1 | 266.9 | 246.4 | 352.5 | 246.4 |
| Dragonfly 2.0.2 | 1 | 137.1 | 138.7 | 147.4 | 147.1 |
| Redis 8.10.3 | 1 | 166.3 | 165.3 | 166.3 | 166.1 |
| Valkey 9.1.3 | 1 | 170.5 | 167.1 | 172.8 | 169.5 |

### memory-load-random: 1M keys x 100 B, then a second million on the same engine

| engine | runs | 1st M RSS B/key | 1st M used B/key | 2nd M RSS B/key | 2nd M used B/key |
|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | 3 | 149.2 (149.2-149.2) | 146.9 (146.9-146.9) | 146.7 (146.7-146.7) | 146.9 (146.9-146.9) |
| KiviDB 1.2.0-rc4 | 3 | 149.2 (149.0-149.3) | 146.9 (146.9-146.9) | 146.7 (146.7-146.9) | 146.9 (146.9-146.9) |
| KiviDB 1.1.3 | 1 | 267.0 | 246.4 | 352.5 | 246.4 |
| Dragonfly 2.0.2 | 1 | 153.1 | 154.7 | 163.3 | 163.1 |
| Redis 8.10.3 | 1 | 166.0 | 165.3 | 167.2 | 166.1 |
| Valkey 9.1.3 | 1 | 170.6 | 167.1 | 172.7 | 169.5 |

### set-get-small: SET:GET 1:10, 100 B, pipeline 1

| engine | runs | ops/s (median of passes) | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | 3 | 1,004,305 (966,437-1,015,373) | 0.191 (0.191-0.191) | 0.375 (0.367-0.375) | 0.475 (0.459-0.503) | 41.5 (40.0-41.8) |
| KiviDB 1.2.0-rc4 | 3 | 968,399 (954,011-1,026,910) | 0.191 (0.191-0.191) | 0.375 (0.371-0.383) | 0.467 (0.447-0.555) | 41.0 (39.3-42.4) |
| KiviDB 1.1.3 | 1 | 989,747 | 0.191 | 0.375 | 0.455 | 42.1 |
| Dragonfly 2.0.2 | 1 | 906,033 | 0.199 | 0.391 | 0.575 | 38.0 |
| Redis 8.10.3 | 1 | 176,722 | 1.115 | 1.623 | 1.827 | 10.4 |
| Valkey 9.1.3 | 1 | 177,037 | 1.107 | 1.631 | 1.835 | 10.0 |

### set-get-large: SET:GET 1:10, 10 KB, pipeline 1

| engine | runs | ops/s (median of passes) | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | 3 | 320,324 (320,194-320,618) | 0.623 (0.623-0.623) | 1.103 (1.027-1.139) | 1.271 (1.227-1.275) | 30.4 (23.4-32.0) |
| KiviDB 1.2.0-rc4 | 3 | 320,424 (320,234-320,462) | 0.623 (0.623-0.623) | 1.067 (1.031-1.103) | 1.259 (1.227-1.271) | 29.1 (22.5-32.3) |
| KiviDB 1.1.3 | 1 | 320,639 | 0.623 | 1.063 | 1.251 | 22.2 |
| Dragonfly 2.0.2 | 1 | 321,783 | 0.627 | 1.011 | 1.219 | 24.8 |
| Redis 8.10.3 | 1 | 118,856 | 1.679 | 2.543 | 2.767 | 14.0 |
| Valkey 9.1.3 | 1 | 117,031 | 1.703 | 2.663 | 2.983 | 9.8 |

### pipeline-30: SET:GET 1:10, 100 B, pipeline 30

| engine | runs | ops/s (median of passes) | p50 ms | p99 ms | p99.9 ms | client CPU % |
|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | 3 | 15,630,271 (15,041,498-15,684,903) | 0.343 (0.343-0.351) | 0.735 (0.731-0.759) | 0.943 (0.939-0.987) | 61.9 (60.4-62.7) |
| KiviDB 1.2.0-rc4 | 3 | 15,149,332 (15,115,581-15,588,430) | 0.351 (0.351-0.351) | 0.759 (0.735-0.759) | 0.983 (0.951-0.983) | 59.5 (58.8-61.0) |
| KiviDB 1.1.3 | 1 | 11,549,894 | 0.495 | 1.087 | 1.339 | 46.1 |
| Dragonfly 2.0.2 | 1 | 8,009,195 | 0.699 | 1.395 | 3.247 | 34.4 |
| Redis 8.10.3 | 1 | 1,604,362 | 3.695 | 7.999 | 8.959 | 6.1 |
| Valkey 9.1.3 | 1 | 1,583,534 | 3.775 | 8.175 | 8.991 | 6.0 |

## Memory as the dataset grows (random values)

RSS bytes per key (used_memory bytes per key). Candidate and rc4: median over rounds.

### 100 B values (random)

| engine | runs | 1M keys | 5M keys | 10M keys | 25M keys | 50M keys | RSS at largest | load s (all stages) |
|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | 3 | 153.8 (146.9) | 146.4 (143.1) | 145.1 (143.1) | 141.1 (140.1) | 140.7 (140.1) | 6.57 GiB | 13.0 (12.8-13.0) |
| KiviDB 1.1.3 | 1 | 219.6 (194.1) | 197.6 (176.8) | 197.1 (176.8) | 183.2 (163.0) | 183.1 (163.0) | 8.54 GiB | 12.9 |
| Redis 8.10.3 | 1 | 170.5 (165.4) | 179.3 (177.1) | 179.0 (177.3) | 174.5 (173.1) | 174.4 (173.1) | 8.15 GiB | 55.1 |
| Valkey 9.1.3 | 1 | 172.3 (167.0) | 169.2 (166.4) | 169.3 (166.7) | 166.6 (165.4) | 166.6 (165.4) | 7.77 GiB | 45.0 |
| Dragonfly 2.0.2 | 1 | 164.0 (154.8) | 150.3 (148.0) | 155.6 (148.9) | 143.4 (139.4) | 141.5 (139.6) | 6.65 GiB | 16.0 |
| KiviDB 1.2.0-rc4 | 3 | 153.6 (146.9) | 146.3 (143.1) | 144.9 (143.1) | 141.0 (140.1) | 140.7 (140.1) | 6.57 GiB | 12.9 (12.9-13.1) |

### 1024 B values (random)

| engine | runs | 1M keys | 5M keys | 10M keys | 25M keys | 50M keys | RSS at largest | load s (all stages) |
|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | 3 | 1085.7 (1074.9) | 1077.6 (1071.1) | 1076.2 (1071.1) | 1072.1 (1068.1) | 1071.7 (1068.1) | 49.92 GiB | 22.9 (22.9-23.0) |
| KiviDB 1.1.3 | 1 | 1132.5 (1118.1) | 1109.8 (1100.8) | 1109.3 (1100.8) | 1095.2 (1087.0) | 1095.1 (1087.0) | 51.01 GiB | 28.9 |
| Redis 8.10.3 | 1 | 1347.6 (1333.4) | 1356.9 (1345.1) | 1356.7 (1345.3) | 1352.2 (1341.1) | 1352.2 (1341.1) | 62.99 GiB | 63.9 |
| Valkey 9.1.3 | 1 | 1352.8 (1336.6) | 1347.4 (1334.8) | 1347.2 (1334.8) | 1344.4 (1333.4) | 1344.4 (1333.4) | 62.62 GiB | 50.9 |
| Dragonfly 2.0.2 | 1 | 1148.7 (1082.8) | 1092.3 (1076.0) | 1085.5 (1076.9) | 1073.5 (1067.5) | 1071.2 (1067.6) | 49.94 GiB | 22.8 |
| KiviDB 1.2.0-rc4 | 3 | 1086.1 (1074.9) | 1077.6 (1071.1) | 1076.2 (1071.1) | 1072.1 (1068.1) | 1071.7 (1068.1) | 49.92 GiB | 22.9 (22.9-23.0) |

## Mixed workload (pair B): 10M keys, 150k ops/s, 80% GET / 20% SET (64-1024 B, TTL 60-600 s), 20 min, distinct client seeds

| engine | round | RSS at start GiB | RSS peak GiB | RSS at end GiB | used at end GiB | RSS/used at end | keys at end (M) | ops/s | GET p50/p99/p99.9 ms | SET p50/p99/p99.9 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | r1 | 1.38 | 4.43 | 3.93 | 3.43 | 1.15 | 6.13 | 149,707 | 0.26/0.46/0.64 | 0.27/0.49/0.78 |
| KiviDB 1.2.0 (75482abb) | r2 | 1.37 | 4.44 | 3.94 | 3.43 | 1.15 | 6.13 | 149,676 | 0.30/0.47/0.79 | 0.31/0.57/1.01 |
| KiviDB 1.2.0 (75482abb) | r3 | 1.37 | 4.44 | 3.92 | 3.43 | 1.14 | 6.13 | 149,716 | 0.29/0.46/0.69 | 0.30/0.51/0.92 |
| KiviDB 1.1.3 | r1 | 1.86 | 4.70 | 4.70 | 4.02 | 1.17 | 6.14 | 149,870 | 0.29/0.46/0.66 | 0.29/0.49/0.84 |
| Redis 8.10.3 | r1 | 1.70 | 5.84 | 5.84 | 4.32 | 1.35 | 6.96 | 150,000 | 0.25/0.42/0.60 | 0.26/0.47/1.00 |
| Valkey 9.1.3 | r1 | 1.60 | 6.21 | 6.20 | 4.26 | 1.46 | 6.96 | 149,998 | 0.30/0.56/0.78 | 0.31/0.56/0.78 |
| Dragonfly 2.0.2 | r1 | 1.47 | 5.04 | 5.03 | 4.01 | 1.25 | 6.34 | 150,000 | 0.28/0.44/0.54 | 0.29/0.47/0.85 |
| KiviDB 1.2.0-rc4 | r1 | 1.37 | 4.43 | 3.94 | 3.43 | 1.15 | 6.13 | 149,704 | 0.27/0.45/0.63 | 0.28/0.48/0.78 |
| KiviDB 1.2.0-rc4 | r2 | 1.37 | 4.44 | 3.94 | 3.43 | 1.15 | 6.13 | 149,697 | 0.28/0.47/0.65 | 0.29/0.49/0.81 |
| KiviDB 1.2.0-rc4 | r3 | 1.37 | 4.44 | 3.92 | 3.43 | 1.14 | 6.13 | 149,676 | 0.29/0.48/0.72 | 0.29/0.52/0.86 |

## Snapshot under a steady 100k SET/s (uniform random keys): 30M keys x 1 KB, random values

| engine | round | event | memory before GiB | peak during GiB | added GiB | added % | duration s | worst write ms | worst 1-s p99 ms | write p99.9 over window ms | p99 before ms | memory 60 s after GiB | used after GiB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | r1 | BGSAVE | 30.29 | 33.40 | 3.11 | 10% | 70.6 | 210 | 1.70 | 1.85 | 1.42 | 33.37 | 30.09 |
| KiviDB 1.2.0 (75482abb) | r1 | BGREWRITEAOF | 33.37 | 35.45 | 2.07 | 6% | 72.5 | 220 | 1.65 | 1.90 | 1.46 | 34.98 | 30.09 |
| KiviDB 1.2.0 (75482abb) | r2 | BGSAVE | 30.30 | 33.42 | 3.12 | 10% | 70.4 | 209 | 1.44 | 1.96 | 1.39 | 33.39 | 30.09 |
| KiviDB 1.2.0 (75482abb) | r2 | BGREWRITEAOF | 33.39 | 35.47 | 2.08 | 6% | 72.0 | 221 | 1.67 | 1.85 | 1.40 | 34.96 | 30.09 |
| KiviDB 1.2.0 (75482abb) | r3 | BGSAVE | 30.31 | 33.43 | 3.12 | 10% | 70.4 | 206 | 1.56 | 2.06 | 1.38 | 33.40 | 30.09 |
| KiviDB 1.2.0 (75482abb) | r3 | BGREWRITEAOF | 33.40 | 35.49 | 2.09 | 6% | 71.9 | 226 | 1.46 | 1.87 | 1.23 | 34.96 | 30.09 |
| KiviDB 1.1.3 | r1 | BGSAVE | 63.58 | 64.51 | 0.93 | 1% | 97.9 | 297 | 0.69 | 0.84 | 0.60 | 63.61 | 31.41 |
| KiviDB 1.1.3 | r1 | BGREWRITEAOF | 63.61 | 64.51 | 0.89 | 1% | 81.0 | 289 | 1.01 | 1.13 | 0.82 | 63.61 | 31.41 |
| Redis 8.10.3 | r1 | BGSAVE | 37.60 | 70.90 | 33.29 | 89% | 175.0 | 391 | 2.59 | 7.07 | 0.73 | 37.61 | 37.28 |
| Redis 8.10.3 | r1 | BGREWRITEAOF | 37.61 | 70.80 | 33.20 | 88% | 177.6 | 1006 | 1.74 | 4.96 | 0.67 | 37.63 | 37.30 |
| Valkey 9.1.3 | r1 | BGSAVE | 37.87 | 67.35 | 29.48 | 78% | 165.2 | 408 | 1.75 | 7.07 | 0.93 | 38.24 | 37.40 |
| Valkey 9.1.3 | r1 | BGREWRITEAOF | 38.38 | 68.33 | 29.95 | 78% | 172.3 | 1024 | 4.89 | 6.75 | 0.61 | 38.64 | 37.40 |
| Dragonfly 2.0.2 | r1 | BGSAVE | 30.73 | 30.86 | 0.12 | 0% | 55.1 | 291 | 19.71 | 10.11 | 0.57 | 30.73 | 30.61 |
| KiviDB 1.2.0-rc4 | r1 | BGSAVE | 30.29 | 33.44 | 3.15 | 10% | 71.5 | 209 | 1.54 | 1.87 | 1.41 | 33.41 | 30.09 |
| KiviDB 1.2.0-rc4 | r1 | BGREWRITEAOF | 33.41 | 35.51 | 2.10 | 6% | 73.1 | 222 | 1.53 | 1.78 | 1.38 | 34.98 | 30.09 |
| KiviDB 1.2.0-rc4 | r2 | BGSAVE | 30.30 | 33.43 | 3.13 | 10% | 70.9 | 203 | 1.53 | 2.02 | 1.38 | 33.40 | 30.09 |
| KiviDB 1.2.0-rc4 | r2 | BGREWRITEAOF | 33.40 | 35.49 | 2.09 | 6% | 72.6 | 225 | 1.53 | 2.06 | 1.33 | 34.97 | 30.09 |
| KiviDB 1.2.0-rc4 | r3 | BGSAVE | 30.31 | 33.39 | 3.09 | 10% | 70.5 | 226 | 1.74 | 2.02 | 1.42 | 33.36 | 30.09 |
| KiviDB 1.2.0-rc4 | r3 | BGREWRITEAOF | 33.36 | 35.42 | 2.06 | 6% | 72.5 | 222 | 1.60 | 1.79 | 1.28 | 34.97 | 30.09 |

### Snapshot, per engine: median over rounds

| engine | runs | event | added GiB | added % | duration s | worst write ms | write p99.9 ms | p99 before ms | memory 60 s after GiB |
|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | 3 | BGSAVE | 3.12 (3.11-3.12) | 10 (10-10) | 70.4 (70.4-70.6) | 209 (206-210) | 1.96 (1.85-2.06) | 1.39 (1.38-1.42) | 33.39 (33.37-33.40) |
| KiviDB 1.2.0 (75482abb) | 3 | BGREWRITEAOF | 2.08 (2.07-2.09) | 6 (6-6) | 72.0 (71.9-72.5) | 221 (220-226) | 1.87 (1.85-1.90) | 1.40 (1.23-1.46) | 34.96 (34.96-34.98) |
| KiviDB 1.1.3 | 1 | BGSAVE | 0.93 | 1 | 97.9 | 297 | 0.84 | 0.60 | 63.61 |
| KiviDB 1.1.3 | 1 | BGREWRITEAOF | 0.89 | 1 | 81.0 | 289 | 1.13 | 0.82 | 63.61 |
| Redis 8.10.3 | 1 | BGSAVE | 33.29 | 89 | 175.0 | 391 | 7.07 | 0.73 | 37.61 |
| Redis 8.10.3 | 1 | BGREWRITEAOF | 33.20 | 88 | 177.6 | 1006 | 4.96 | 0.67 | 37.63 |
| Valkey 9.1.3 | 1 | BGSAVE | 29.48 | 78 | 165.2 | 408 | 7.07 | 0.93 | 38.24 |
| Valkey 9.1.3 | 1 | BGREWRITEAOF | 29.95 | 78 | 172.3 | 1024 | 6.75 | 0.61 | 38.64 |
| Dragonfly 2.0.2 | 1 | BGSAVE | 0.12 | 0 | 55.1 | 291 | 10.11 | 0.57 | 30.73 |
| KiviDB 1.2.0-rc4 | 3 | BGSAVE | 3.13 (3.09-3.15) | 10 (10-10) | 70.9 (70.5-71.5) | 209 (203-226) | 2.02 (1.87-2.02) | 1.41 (1.38-1.42) | 33.40 (33.36-33.41) |
| KiviDB 1.2.0-rc4 | 3 | BGREWRITEAOF | 2.09 (2.06-2.10) | 6 (6-6) | 72.6 (72.5-73.1) | 222 (222-225) | 1.79 (1.78-2.06) | 1.33 (1.28-1.38) | 34.97 (34.97-34.98) |

## maxmemory + BGSAVE (issue #284): 10M keys x 1 KB, maxmemory = used + 10%, allkeys-lru, 100k same-size overwrites/s (uniform)

| engine | round | keys before | keys after | evicted keys | maxmemory | lowest key count during run | SET p99 / p99.9 ms |
|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | r1 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.73 / 1.11 |
| KiviDB 1.2.0 (75482abb) | r2 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.70 / 1.05 |
| KiviDB 1.2.0 (75482abb) | r3 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.78 / 1.14 |
| KiviDB 1.1.3 | r1 | 10,000,000 | 8,789,704 | 11,055,716 | 11.28 GiB | 0.09M | 1.21 / 1.85 |
| Redis 8.10.3 | r1 | 10,000,000 | 10,000,000 | 0 | 13.78 GiB | 10.00M | 0.62 / 1.08 |
| KiviDB 1.2.0-rc4 | r1 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.82 / 1.18 |
| KiviDB 1.2.0-rc4 | r2 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.73 / 1.09 |
| KiviDB 1.2.0-rc4 | r3 | 10,000,000 | 10,000,000 | 0 | 10.98 GiB | 10.00M | 0.59 / 0.87 |

## Active defrag after deleting a random half: 5M keys x 100 B, probe load 80k GET/s + 20k SET/s (pair B)

Slab = INFO value_slab_bytes (live = value_slab_live_bytes). *Give-back time*: from the end of the delete until RSS has made 90% of its final drop. *Compaction*: seconds with active_defrag_running = 1. Latency: per-second p99 / p99.9 from memtier, median over the seconds of the window (worst second in brackets); *before* = the 50 s before the delete, *during* = while compaction runs.

| build | round | RSS after load MB | slab / live after delete MB | RSS after delete MB | RSS final MB | slab / live final MB | RSS returned MB | give-back s | compaction s | hits | GET p99 before / during ms | GET p99.9 before / during ms | SET p99 before / during ms | SET p99.9 before / during ms | max ms during |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | r1 | 722 | 624 / 319 | 722 | 387 | 321 / 319 | 335 | 12.8 | 13 | 1,399,554 | 0.52 / 0.51 (0.54) | 0.80 / 0.78 (1.06) | 0.26 / 0.25 (0.29) | 0.54 / 0.49 (0.77) | 2.7 |
| KiviDB 1.2.0 (75482abb) | r2 | 723 | 624 / 319 | 724 | 389 | 321 / 319 | 334 | 12.1 | 13 | 1,397,852 | 0.51 / 0.51 (0.66) | 0.80 / 0.79 (0.94) | 0.21 / 0.22 (0.23) | 0.28 / 0.27 (0.38) | 2.4 |
| KiviDB 1.2.0 (75482abb) | r3 | 724 | 624 / 319 | 724 | 390 | 321 / 319 | 334 | 13.1 | 13 | 1,398,652 | 0.51 / 0.47 (0.52) | 0.83 / 0.80 (1.19) | 0.24 / 0.24 (0.26) | 0.41 / 0.36 (0.57) | 2.3 |
| KiviDB 1.2.0 (75482abb) | r4 | 722 | 624 / 319 | 723 | 389 | 321 / 319 | 334 | 11.9 | 12 | 1,388,405 | 0.57 / 0.58 (0.62) | 0.83 / 0.89 (1.06) | 0.26 / 0.24 (0.29) | 0.37 / 0.39 (0.53) | 2.4 |
| KiviDB 1.2.0 (75482abb) | r5 | 722 | 624 / 319 | 722 | 385 | 321 / 319 | 337 | 11.7 | 13 | 1,384,211 | 0.39 / 0.39 (0.41) | 0.44 / 0.44 (0.51) | 0.20 / 0.20 (0.21) | 0.30 / 0.29 (0.35) | 2.0 |
| KiviDB 1.2.0 (75482abb) (activedefrag no) | r1 | 722 | 624 / 319 | 723 | 688 | 624 / 319 | 35 | 1.0 | 0 | 0 | 0.47 / - | 0.61 / - | 0.13 / - | 0.14 / - | - |
| KiviDB 1.2.0 (75482abb) (activedefrag no) | r2 | 721 | 624 / 319 | 722 | 687 | 624 / 319 | 35 | 1.1 | 0 | 0 | 0.34 / - | 0.48 / - | 0.16 / - | 0.18 / - | - |
| KiviDB 1.2.0 (75482abb) (activedefrag no) | r3 | 723 | 624 / 319 | 723 | 689 | 624 / 319 | 34 | 0.9 | 0 | 0 | 0.36 / - | 0.78 / - | 0.17 / - | 0.31 / - | - |
| KiviDB 1.2.0-rc4 | r1 | 723 | 624 / 319 | 724 | 389 | 321 / 319 | 335 | 12.8 | 13 | 1,379,059 | 0.28 / 0.28 (0.35) | 0.47 / 0.40 (1.06) | 0.19 / 0.17 (0.29) | 0.36 / 0.25 (0.36) | 1.8 |
| KiviDB 1.2.0-rc4 | r2 | 722 | 624 / 319 | 723 | 388 | 321 / 319 | 335 | 12.1 | 13 | 1,401,933 | 0.56 / 0.59 (0.66) | 0.81 / 0.81 (0.97) | 0.23 / 0.23 (0.25) | 0.37 / 0.29 (0.49) | 2.2 |
| KiviDB 1.2.0-rc4 | r3 | 722 | 624 / 319 | 722 | 386 | 321 / 319 | 336 | 12.1 | 13 | 1,395,036 | 0.34 / 0.38 (0.38) | 0.66 / 0.73 (1.00) | 0.27 / 0.26 (0.28) | 0.29 / 0.29 (0.34) | 2.0 |
| KiviDB 1.2.0-rc4 | r4 | 722 | 624 / 319 | 722 | 386 | 321 / 319 | 336 | 11.9 | 12 | 1,369,731 | 0.54 / 0.47 (0.56) | 0.72 / 0.73 (0.94) | 0.26 / 0.24 (0.28) | 0.39 / 0.39 (0.54) | 1.6 |
| KiviDB 1.2.0-rc4 (activedefrag no) | r2 | 723 | 624 / 319 | 723 | 689 | 624 / 319 | 34 | 0.9 | 0 | 0 | 0.35 / - | 0.63 / - | 0.44 / - | 0.66 / - | - |

## Write latency with the AOF on, no snapshot (pair B): 2M x 1 KB, 100k uniformly random SET/s, 180 s

The check of the snapshot runs' steady-state write p99 (`harness/aoflat.sh`). Rounds r1-r4 alternate the builds; p1 is the round with `perf record` running for 30 s.

| build | round | ops/s | SET p50 ms | p99 ms | p99.9 ms | worst write ms | median of 1-s p99 ms | engine CPU s (load + run) |
|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0 (75482abb) | r1 | 99,474 | 0.351 | 0.887 | 1.255 | 164 | 0.911 | 550 |
| KiviDB 1.2.0 (75482abb) | r2 | 99,532 | 0.367 | 0.919 | 1.311 | 163 | 0.927 | 547 |
| KiviDB 1.2.0 (75482abb) | r3 | 99,465 | 0.391 | 0.967 | 1.391 | 175 | 1.031 | 562 |
| KiviDB 1.2.0 (75482abb) | r4 | 99,273 | 0.359 | 0.903 | 1.255 | 168 | 0.919 | 557 |
| KiviDB 1.2.0 (75482abb) | p1 | 99,581 | 0.399 | 0.991 | 1.439 | 133 | 1.031 | 570 |
| KiviDB 1.2.0-rc4 | r1 | 99,590 | 0.359 | 0.895 | 1.255 | 125 | 0.903 | 552 |
| KiviDB 1.2.0-rc4 | r2 | 99,655 | 0.407 | 1.007 | 1.423 | 159 | 1.039 | 564 |
| KiviDB 1.2.0-rc4 | r3 | 99,387 | 0.407 | 0.999 | 1.407 | 172 | 1.031 | 562 |
| KiviDB 1.2.0-rc4 | r4 | 99,375 | 0.415 | 1.007 | 1.423 | 167 | 1.023 | 558 |
| KiviDB 1.2.0-rc4 | p1 | 99,652 | 0.399 | 0.975 | 1.399 | 155 | 1.015 | 571 |

## Against 1.2.0-rc4

Median (min-max) over rounds. *Change* is candidate median vs rc4 median; *spread* is the larger of the two builds' (max-min)/median. A row is flagged when the candidate is worse by more than the spread and by at least 1% (memory counts repeat to a few tenths of a percent, so a spread near zero would otherwise flag a 0.2% difference).

| section | metric | candidate | rc4 | change | spread | verdict |
|---|---|---|---|---|---|---|
| warefeats ASCII | 1st M RSS B/key | 149.2 (149.2-149.2) | 148.9 (148.9-149.2) | 0.2% | 0.2% | level |
| warefeats ASCII | 1st M used B/key | 146.9 (146.9-146.9) | 146.9 (146.9-146.9) | 0.0% | 0.0% | level |
| warefeats ASCII | 2nd M RSS B/key | 146.8 (146.7-154.9) | 147.1 (146.8-147.6) | -0.2% | 5.6% | level |
| warefeats random | 1st M RSS B/key | 149.2 (149.2-149.2) | 149.2 (149.0-149.3) | 0.0% | 0.2% | level |
| warefeats random | 1st M used B/key | 146.9 (146.9-146.9) | 146.9 (146.9-146.9) | 0.0% | 0.0% | level |
| warefeats random | 2nd M RSS B/key | 146.7 (146.7-146.7) | 146.7 (146.7-146.9) | 0.0% | 0.1% | level |
| warefeats set-get-small | ops/s | 1,004,305 (966,437-1,015,373) | 968,399 (954,011-1,026,910) | 3.7% | 7.5% | level |
| warefeats set-get-small | p99 ms | 0.375 (0.367-0.375) | 0.375 (0.371-0.383) | 0.0% | 3.2% | level |
| warefeats set-get-small | p99.9 ms | 0.475 (0.459-0.503) | 0.467 (0.447-0.555) | 1.7% | 23.1% | level |
| warefeats set-get-large | ops/s | 320,324 (320,194-320,618) | 320,424 (320,234-320,462) | -0.0% | 0.1% | level |
| warefeats set-get-large | p99 ms | 1.103 (1.027-1.139) | 1.067 (1.031-1.103) | 3.4% | 10.2% | level |
| warefeats set-get-large | p99.9 ms | 1.271 (1.227-1.275) | 1.259 (1.227-1.271) | 1.0% | 3.8% | level |
| warefeats pipeline-30 | ops/s | 15,630,271 (15,041,498-15,684,903) | 15,149,332 (15,115,581-15,588,430) | 3.2% | 4.1% | level |
| warefeats pipeline-30 | p99 ms | 0.735 (0.731-0.759) | 0.759 (0.735-0.759) | -3.2% | 3.8% | level |
| warefeats pipeline-30 | p99.9 ms | 0.943 (0.939-0.987) | 0.983 (0.951-0.983) | -4.1% | 5.1% | level |
| growth 100 B | RSS B/key at 1M | 153.8 (153.3-154.1) | 153.6 (153.5-154.4) | 0.1% | 0.5% | level |
| growth 100 B | RSS B/key at 5M | 146.4 (146.4-146.7) | 146.3 (146.2-146.5) | 0.1% | 0.2% | level |
| growth 100 B | RSS B/key at 10M | 145.1 (145.0-145.2) | 144.9 (144.8-145.2) | 0.2% | 0.2% | level |
| growth 100 B | RSS B/key at 25M | 141.1 (141.0-141.1) | 141.0 (141.0-141.1) | 0.1% | 0.1% | level |
| growth 100 B | RSS B/key at 50M | 140.7 (140.7-140.7) | 140.7 (140.7-140.7) | 0.0% | 0.0% | level |
| growth 100 B | load seconds, 50M keys | 13.0 (12.8-13.0) | 12.9 (12.9-13.1) | 0.7% | 1.6% | level |
| growth 1024 B | RSS B/key at 1M | 1085.7 (1085.6-1086.1) | 1086.1 (1085.9-1086.1) | -0.0% | 0.0% | level |
| growth 1024 B | RSS B/key at 5M | 1077.6 (1077.5-1078.0) | 1077.6 (1077.4-1078.0) | 0.0% | 0.0% | level |
| growth 1024 B | RSS B/key at 10M | 1076.2 (1076.0-1076.2) | 1076.2 (1076.0-1076.4) | -0.0% | 0.0% | level |
| growth 1024 B | RSS B/key at 25M | 1072.1 (1072.0-1072.1) | 1072.1 (1072.0-1072.1) | -0.0% | 0.0% | level |
| growth 1024 B | RSS B/key at 50M | 1071.7 (1071.6-1071.7) | 1071.7 (1071.6-1071.7) | -0.0% | 0.0% | level |
| growth 1024 B | load seconds, 50M keys | 22.9 (22.9-23.0) | 22.9 (22.9-23.0) | 0.1% | 0.8% | level |
| mixed 20 min | RSS at end GiB | 3.93 (3.92-3.94) | 3.94 (3.92-3.94) | -0.3% | 0.5% | level |
| mixed 20 min | RSS peak GiB | 4.44 (4.43-4.44) | 4.44 (4.43-4.44) | -0.1% | 0.3% | level |
| mixed 20 min | RSS/used at end | 1.15 (1.14-1.15) | 1.15 (1.14-1.15) | -0.3% | 0.5% | level |
| mixed 20 min | GET p99 ms | 0.46 (0.46-0.47) | 0.47 (0.45-0.48) | -1.7% | 6.8% | level |
| mixed 20 min | GET p99.9 ms | 0.69 (0.64-0.79) | 0.65 (0.63-0.72) | 7.4% | 21.9% | level |
| mixed 20 min | SET p99 ms | 0.51 (0.49-0.57) | 0.49 (0.48-0.52) | 3.2% | 15.7% | level |
| mixed 20 min | SET p99.9 ms | 0.92 (0.78-1.01) | 0.81 (0.78-0.86) | 13.9% | 24.4% | level |
| snapshot BGSAVE | memory added GiB | 3.12 (3.11-3.12) | 3.13 (3.09-3.15) | -0.3% | 1.9% | level |
| snapshot BGSAVE | duration s | 70.4 (70.4-70.6) | 70.9 (70.5-71.5) | -0.7% | 1.4% | level |
| snapshot BGSAVE | worst write ms | 209 (206-210) | 209 (203-226) | -0.4% | 10.8% | level |
| snapshot BGSAVE | write p99.9 ms | 1.96 (1.85-2.06) | 2.02 (1.87-2.02) | -2.8% | 11.0% | level |
| snapshot BGSAVE | write p99 before ms | 1.39 (1.38-1.42) | 1.41 (1.38-1.42) | -1.1% | 3.4% | level |
| snapshot BGSAVE | memory 60 s after GiB | 33.39 (33.37-33.40) | 33.40 (33.36-33.41) | -0.0% | 0.1% | level |
| snapshot BGREWRITEAOF | memory added GiB | 2.08 (2.07-2.09) | 2.09 (2.06-2.10) | -0.4% | 1.8% | level |
| snapshot BGREWRITEAOF | duration s | 72.0 (71.9-72.5) | 72.6 (72.5-73.1) | -0.8% | 0.9% | level |
| snapshot BGREWRITEAOF | worst write ms | 221 (220-226) | 222 (222-225) | -0.3% | 2.4% | level |
| snapshot BGREWRITEAOF | write p99.9 ms | 1.87 (1.85-1.90) | 1.79 (1.78-2.06) | 4.5% | 15.6% | level |
| snapshot BGREWRITEAOF | write p99 before ms | 1.40 (1.23-1.46) | 1.33 (1.28-1.38) | 5.4% | 16.6% | level |
| snapshot BGREWRITEAOF | memory 60 s after GiB | 34.96 (34.96-34.98) | 34.97 (34.97-34.98) | -0.0% | 0.1% | level |
| #284 eviction | keys evicted | 0 (0-0) | 0 (0-0) | -% | 0.0% | - |
| active defrag | RSS final MB | 389 (385-390) | 387 (386-389) | 0.5% | 1.1% | level |
| active defrag | RSS returned MB | 334 (334-337) | 335 (335-336) | -0.3% | 0.8% | level |
| active defrag | give-back s | 12.1 (11.7-13.1) | 12.1 (11.9-12.8) | -0.3% | 11.1% | level |
| active defrag | compaction s | 13 (12-13) | 13 (12-13) | 0.0% | 7.7% | level |
| active defrag | slab final MB | 321 (321-321) | 321 (321-321) | 0.0% | 0.2% | level |
| active defrag | GET p99 during compaction ms | 0.51 (0.39-0.58) | 0.42 (0.28-0.59) | 21.4% | 74.1% | level |
| active defrag | GET p99.9 during compaction ms | 0.79 (0.44-0.89) | 0.73 (0.40-0.81) | 8.8% | 57.2% | level |
| active defrag | SET p99 during compaction ms | 0.24 (0.20-0.25) | 0.24 (0.17-0.26) | 0.8% | 40.5% | level |
| active defrag | SET p99.9 during compaction ms | 0.36 (0.27-0.49) | 0.29 (0.25-0.39) | 23.4% | 62.4% | level |
| active defrag | GET p99 during / before (same run) | 1.00 (0.93-1.02) | 1.02 (0.87-1.11) | -1.7% | 22.9% | level |
| active defrag | GET p99.9 during / before (same run) | 0.99 (0.97-1.07) | 1.01 (0.85-1.11) | -1.5% | 26.1% | level |
| active defrag | SET p99 during / before (same run) | 1.00 (0.92-1.04) | 0.95 (0.87-1.00) | 4.8% | 13.2% | level |
| active defrag | SET p99.9 during / before (same run) | 0.95 (0.88-1.05) | 0.88 (0.69-0.99) | 7.1% | 34.1% | level |
| AOF-on writes | SET p50 ms | 0.363 (0.351-0.391) | 0.407 (0.359-0.415) | -10.8% | 13.8% | level |
| AOF-on writes | SET p99 ms | 0.911 (0.887-0.967) | 1.003 (0.895-1.007) | -9.2% | 11.2% | level |
| AOF-on writes | SET p99.9 ms | 1.283 (1.255-1.391) | 1.415 (1.255-1.423) | -9.3% | 11.9% | level |
| AOF-on writes | worst write ms | 166 (163-175) | 163 (125-172) | 1.9% | 29.1% | level |
| AOF-on writes | engine CPU s | 554 (547-562) | 560 (552-564) | -1.2% | 2.7% | level |

