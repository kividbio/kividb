# memory suite: generated tables

## Snapshot under a steady 100k SET/s: 30M keys x 1 KB, random values

| engine | event | memory before GiB | peak during GiB | added GiB | added % | duration s | max write latency ms | worst 1-s p99 ms | write p99.9 over window ms | baseline 1-s p99 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc4 | BGSAVE | 30.31 | 33.39 | 3.08 | 10% | 71.6 | 256.45 | 1.94 | 3.12 | 1.50 |
| KiviDB 1.2.0-rc4 | BGREWRITEAOF | 33.36 | 35.41 | 2.05 | 6% | 73.8 | 292.94 | 1.77 | 2.58 | 1.45 |
| Redis 8.10.3 | BGSAVE | 37.60 | 71.67 | 34.06 | 91% | 194.7 | 412.12 | 3.50 | 7.81 | 0.61 |
| Redis 8.10.3 | BGREWRITEAOF | 37.61 | 71.89 | 34.29 | 91% | 203.0 | 1046.68 | 2.72 | 7.49 | 0.61 |

