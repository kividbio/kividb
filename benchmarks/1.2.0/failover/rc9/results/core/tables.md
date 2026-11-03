### rc9 against rc8 (same session, alternating rounds)

Median [min - max] of the rounds. Spread is half the min-max range of each build, as % of its median.
Verdict: *slower* if rc9's best round is below rc8's worst and the medians differ by more than 1% (the run-to-run spread of the published six-round session), *faster* the other way round.

| Scenario | rc8 | rc9 | Change | Spread rc8 / rc9 | Verdict | p99 ms rc8 → rc9 | p99.9 ms rc8 → rc9 | server µs/req rc8 → rc9 |
|---|---:|---:|---:|---:|---|---:|---:|---:|
| 100 B GET p1 | 1.76M [1.75M - 1.77M] C | 1.75M [1.74M - 1.75M] C | -0.7% | ±0.7% / ±0.5% | level (clients) | 1.04 → 1.03 | 1.85 → 1.90 | 17.29 → 17.38 |
| 100 B GET p16 | 10.53M [10.49M - 10.58M] | 10.57M [10.50M - 10.65M] | +0.4% | ±0.4% / ±0.7% | level | 0.79 → 0.78 | 1.27 → 1.27 | 2.86 → 2.85 |
| 100 B SET p16 | 7.96M [7.90M - 8.01M] | 8.08M [8.05M - 8.12M] | +1.6% | ±0.7% / ±0.4% | faster | 0.97 → 0.97 | 1.26 → 1.25 | 3.55 → 3.50 |
| 100 B MSET 10 p1 | 555K [553K - 558K] | 554K [550K - 558K] | -0.3% | ±0.5% / ±0.7% | level | 2.97 → 3.00 | 5.63 → 5.63 | 53.91 → 54.05 |

### MSET and MGET of 10 keys in detail (100 B, pipeline 1, 1,200 connections)

| Build | round | commands/s | p50 / p99 / p99.9 ms | server CPU busy | server µs/cmd (user + kernel) | load generators busy |
|---|---|---:|---:|---:|---:|---:|
| rc8 MSET10 | 1 | 553K | 2.17 / 2.98 / 5.34 | 93.6% | 54.17 (37.79 + 16.38) | 61.0% |
| rc8 MSET10 | 2 | 558K | 2.16 / 2.96 / 5.92 | 93.5% | 53.64 (37.23 + 16.41) | 62.4% |
| rc9 MSET10 | 1 | 558K | 2.16 / 2.98 / 5.57 | 93.6% | 53.71 (37.81 + 15.95) | 61.8% |
| rc9 MSET10 | 2 | 550K | 2.17 / 3.02 / 5.70 | 93.5% | 54.39 (38.04 + 16.29) | 61.4% |
