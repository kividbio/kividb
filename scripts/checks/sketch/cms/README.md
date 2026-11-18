# Checks of Count-min sketches against Redis

Run by hand when `CMS.*` changes; not part of CI, because they need a Redis
server with the probabilistic types (Redis 8, e.g. the `redis:8.10.2`
image).

- `compare.py <redis-port> <kividb-port> [--seed N] [--steps N] [--sequences N]`:
  randomised sequences of every `CMS.*` command (good and bad arguments,
  overflow at 2^32 − 1, `MERGE` with weights, wrong types, missing keys,
  `WATCH` on failed writes, `TYPE`, `OBJECT ENCODING`, `RENAME`) sent to
  both, in RESP2 on database 0 and RESP3 on database 1, every reply
  compared byte for byte. Counts are compared exactly: KiviDB places items
  as Redis does. Prints each difference and a summary; exits 1 on any.
  Sizes stay below KiviDB's limits, which Redis does not have.
- `compare.py <redis-port> --capture tests/fixtures/redis-8.10.2-cms-replies.json`
  writes Redis's replies to a sequence as the fixture `tests/cms.rs`
  replays (`--sequences 4 --steps 250 --seed 3` made the one checked in).

Recorded for the change that added these (Redis 8.10.2, `--seed 7
--sequences 30 --steps 500`): 30,000 replies compared, 0 differ.
