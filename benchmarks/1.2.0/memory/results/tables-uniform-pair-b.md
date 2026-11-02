# memory suite: generated tables

## Snapshot under a steady 100k SET/s: 30M keys x 1 KB, random values

| engine | event | memory before GiB | peak during GiB | added GiB | added % | duration s | max write latency ms | worst 1-s p99 ms | write p99.9 over window ms | baseline 1-s p99 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc4 | BGSAVE | 30.30 | 33.84 | 3.55 | 12% | 80.8 | 199.66 | 1.12 | 1.51 | 0.98 |
| KiviDB 1.2.0-rc4 | BGREWRITEAOF | 33.82 | 36.16 | 2.34 | 7% | 82.4 | 195.80 | 1.03 | 1.30 | 0.91 |

