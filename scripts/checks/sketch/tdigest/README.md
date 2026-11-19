# Checks of t-digests against Redis

Run by hand when `TDIGEST.*` changes; not part of CI, because they need a
Redis server with the probabilistic types (Redis 8, e.g. the `redis:8.10.2`
image).

- `compare.py <redis-port> <kividb-port> [--seed N] [--steps N] [--sequences N]`:
  randomised sequences of every `TDIGEST.*` command sent to both, RESP2 on
  database 0 and RESP3 on database 1. Errors, arity, wrong types, missing
  keys, `WATCH`, `TYPE`, `OBJECT ENCODING`, the shape of every reply, and the
  numbers of digests whose every centroid is one value (nothing to
  approximate) are compared byte for byte. The numbers of larger digests
  are judged against the true values (each key's values are tracked): a
  KiviDB answer differs if its error is over 1.5 times Redis's plus 0.02 (in
  rank, or CDF, or as a share of the values' range). `TDIGEST.INFO`'s node
  counts and compressions are left out (Redis merges on reads; KiviDB does
  not change a digest on a read).
- `compare.py <redis-port> <kividb-port> --quality`: 200,000 normal, uniform,
  lognormal and sorted (adversarial) values into digests of compression 100
  on both; the error in rank of each side's quantiles.
- `compare.py <redis-port> --capture tests/fixtures/redis-8.10.2-tdigest-replies.json`
  writes Redis's replies (every write, and the reads where nothing is
  approximated) as the fixture `tests/tdigest.rs` replays (`--sequences 4
  --steps 250 --seed 3` made the one checked in).

Recorded for the change that added these (Redis 8.10.2): `--seed 7
--sequences 20 --steps 400`: 16,000 replies compared; every exact one equal;
of 5,064 approximate ones, 36 (0.7%) with KiviDB's error over 1.5 times
Redis's plus 0.02, on digests of under 150 values with outliers of 10^10.

`--quality`, rank error of the quantile returned:

| distribution | q | Redis | KiviDB |
|---|---|---|---|
| normal | 0.001 | 0.000000 | 0.000115 |
| normal | 0.01 | 0.000020 | 0.000250 |
| normal | 0.1 | 0.000830 | 0.000280 |
| normal | 0.5 | 0.002615 | 0.000380 |
| normal | 0.9 | 0.000200 | 0.000075 |
| normal | 0.99 | 0.000100 | 0.000190 |
| normal | 0.999 | 0.000015 | 0.000240 |
| uniform | 0.001 | 0.000120 | 0.000310 |
| uniform | 0.25 | 0.002800 | 0.000035 |
| uniform | 0.5 | 0.001515 | 0.000060 |
| uniform | 0.999 | 0.000025 | 0.000145 |
| lognormal | 0.25 | 0.004495 | 0.000335 |
| lognormal | 0.5 | 0.007445 | 0.000430 |
| lognormal | 0.999 | 0.000005 | 0.000150 |
| sorted | every q | 0 | 0 |

Worst error relative to `min(q, 1 − q)`: Redis 0.12, KiviDB 0.31 (at the
0.001 and 0.999 tails, where Redis keeps finer centroids).
