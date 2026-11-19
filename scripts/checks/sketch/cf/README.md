# Checks of Cuckoo filters against Redis

Run by hand when `CF.*` changes; not part of CI, because they need a Redis
server with the probabilistic types (Redis 8, e.g. the `redis:8.10.2`
image).

- `compare.py <redis-port> <kividb-port> [--seed N] [--steps N] [--sequences N]`:
  randomised sequences of every `CF.*` command sent to both, RESP2 on
  database 0 and RESP3 on database 1, every reply compared byte for byte:
  filters of bucket sizes 1 to 4, kick limits 1 to 30 and expansions 0 to 4
  that grow, fill, delete and compact; `CF.SCANDUMP` of whole filters
  `CF.LOADCHUNK`ed into another key; arbitrary chunks and headers; the `cf-*`
  settings changed mid-sequence; every error and the order arguments are
  checked in; wrong types, missing keys, `WATCH` on failed writes. Prints each
  difference and a summary; exits 1 on any. (`cf-max-expansions 1` is left
  out: see COMMANDS.md's differences.)
- `compare.py <kividb-port> --guarantees`: no false negatives over 100,000
  items in a growing filter, the false-positive rate against its bound,
  deletes of every other item, and a full non-growing filter.
- `compare.py <redis-port> --capture tests/fixtures/redis-8.10.2-cf-replies.json`
  writes Redis's replies as the fixture `tests/cf.rs` replays
  (`--sequences 4 --steps 200 --seed 3` made the one checked in).

Recorded for the change that added these (Redis 8.10.2): `--seed 7
--sequences 30 --steps 400`, 24,000 replies compared, 0 differ.
