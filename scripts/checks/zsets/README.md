# Checks for how sorted sets are stored

Run by hand when the storage of sorted sets changes; they are not part of CI
because they need an older release's binary and a Redis server.

- `xver.sh`: sorted sets in a snapshot and an AOF written by an older release
  load in the build under test, and ones written by it load in the older
  release, every member and score compared in order (scores at every edge,
  binary and empty members, sets of each form and those converted by count
  and by a long member, geo points, pops, removals and score changes after
  the adds). `OLD=<path to the older kividb> NEW=<path to the build under
  test> scripts/checks/zsets/xver.sh`; each line ends `CHECK OK` and counts
  the encodings the reader holds the sets in.
- `compare.py <redis-port> <kividb-port>`: one sequence of sorted-set commands
  sent to a Redis server and to KiviDB (growth a member at a time across the
  128 -> 129 change of form and shrinking back, a 64- and a 65-byte member,
  scores at every edge, every `ZADD` flag on each form, the count of a
  `ZADD`'s pairs, pops and blocking pops served at once, every range by
  index, score and lex, removals by each, set operations and their
  destinations' forms, `ZRANGESTORE`, `COPY`, `RENAME`, `SORT`, `DUMP`/
  `RESTORE` within each server, the settings and their older names, and the
  GEO commands on small and large sets), replies compared exactly (a
  skiplist's `ZSCAN` as the set of pairs a full scan gives, random replies by
  shape) and `OBJECT ENCODING` after each step, listed apart. Prints each
  difference and a summary.

Memory per member is recorded by regression suite `31-zset-memory`.
