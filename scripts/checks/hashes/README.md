# Checks for how hashes are stored

Run by hand when the storage of hashes changes; they are not part of CI
because they need an older release's binary and a Redis server.

- `xver.sh`: hashes in a snapshot and an AOF written by an older release load
  in the build under test, and ones written by it load in the older release,
  with every field, value and field deadline compared. `OLD=<path to the
  older kividb> NEW=<path to the build under test>
  scripts/checks/hashes/xver.sh`; each line ends `CHECK OK`.
- `compare.py <redis-port> <kividb-port>`: one sequence of hash commands sent
  to a Redis server and to KiviDB (growth a field at a time across every
  change of form, conversions by a long field and a long value, removals,
  field TTLs on each form, `HGETDEL`/`HGETEX`/`HSETEX`, `SORT ... BY/GET`,
  `COPY`, `RENAME`, the settings, `OBJECT ENCODING` after each step), replies
  compared: in order while Redis keeps the hash as a listpack, sorted
  otherwise. Prints each difference and a summary.

Memory per field is recorded by regression suite `19-hash-memory`.
