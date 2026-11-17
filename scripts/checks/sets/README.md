# Checks for how sets are stored

Run by hand when the storage of sets changes (#297); they are not part of CI
because they need an older release's binary and a Redis server.

- `xver.sh`: a snapshot and an AOF written by an older release load in the
  build under test, and ones written by it load in the older release, with
  every member compared. `OLD=<path to the older kividb> NEW=<path to the
  build under test> scripts/checks/sets/xver.sh`; each line ends `CHECK OK`.
- `compare.py <redis-port> <kividb-port>`: one sequence of set commands sent
  to a Redis server and to KiviDB (growth a member at a time across every
  change of form, removals, every `*STORE`, `SMOVE`, `COPY`, `RENAME`,
  `OBJECT ENCODING` after each step), replies compared, sets as sorted lists.
  Prints each difference and a summary.

Memory per member is recorded by regression suite `17-set-memory`.
