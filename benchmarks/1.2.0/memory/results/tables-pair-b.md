# memory suite: generated tables

## Mixed workload: 10M keys, 150k ops/s, 80% GET / 20% SET (64-1024 B, TTL 60-600 s), 20 min

| engine | RSS at start GiB | RSS peak GiB | RSS at end GiB | used at end GiB | RSS/used at end | keys at end (M) | ops/s | GET p50/p99/p99.9 ms | SET p50/p99/p99.9 ms |
|---|---|---|---|---|---|---|---|---|---|
| KiviDB 1.2.0-rc4 | 1.36 | 1.44 | 1.44 | 1.36 | 1.05 | 9.75 | 150,000 | 0.25/0.43/0.52 | 0.26/0.46/0.81 |
| Valkey 9.1.3 | 1.59 | 1.68 | 1.68 | 1.57 | 1.07 | 9.76 | 150,000 | 0.29/0.47/0.69 | 0.30/0.58/0.94 |

