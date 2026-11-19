# Checks of Top-K sketches against Redis

Run by hand when `TOPK.*` changes; not part of CI, because they need a Redis
server with the probabilistic types (Redis 8, e.g. the `redis:8.10.2`
image).

- `compare.py <redis-port> <kividb-port> [--seed N] [--steps N] [--sequences N]`:
  randomised sequences of every `TOPK.*` command (good and bad arguments,
  wrong types, missing keys, `WATCH` on failed writes, `TYPE`, `OBJECT
  ENCODING`) sent to both, RESP2 on database 0 and RESP3 on database 1.
  Sketches with a decay of 1, where nothing is left to chance, are compared
  byte for byte (items expelled, `TOPK.LIST`, `TOPK.COUNT`, `TOPK.QUERY`);
  sketches with a decay below 1 by the shape of their replies. Items with a
  zero byte are left out (Redis cuts them at it in replies). Prints each
  difference and a summary; exits 1 on any.
- `compare.py <redis-port> <kividb-port> --quality`: Zipf streams (s = 0.8,
  1.0, 1.2; 200,000 items over 20,000 distinct) into sketches of the default
  decay (0.9) on both; the share of the true top k each lists, and the mean
  relative error of its counts.
- `compare.py <redis-port> --capture tests/fixtures/redis-8.10.2-topk-replies.json`
  writes Redis's replies (decay 1 only) as the fixture `tests/topk.rs`
  replays (`--sequences 4 --steps 250 --seed 3` made the one checked in).

Recorded for the change that added these (Redis 8.10.2): `--seed 7
--sequences 30 --steps 500`, 30,000 replies compared (3,740 by shape), 0
differ. `--quality`:

| s | k | Redis recall | KiviDB recall | Redis mean error | KiviDB mean error |
|---|---|---|---|---|---|
| 0.8 | 10 | 0.900 | 1.000 | 0.0153 | 0.0169 |
| 0.8 | 50 | 0.980 | 0.980 | 0.0124 | 0.0123 |
| 0.8 | 100 | 0.980 | 0.980 | 0.0146 | 0.0148 |
| 1.0 | 10 | 1.000 | 1.000 | 0.0010 | 0.0008 |
| 1.0 | 50 | 1.000 | 1.000 | 0.0024 | 0.0029 |
| 1.0 | 100 | 0.990 | 0.990 | 0.0028 | 0.0033 |
| 1.2 | 10 | 1.000 | 1.000 | 0.0001 | 0.0001 |
| 1.2 | 50 | 1.000 | 1.000 | 0.0008 | 0.0005 |
| 1.2 | 100 | 1.000 | 1.000 | 0.0007 | 0.0011 |
