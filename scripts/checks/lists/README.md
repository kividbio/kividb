# Checks for how lists are stored

Run by hand when the storage of lists changes; they are not part of CI
because they need an older release's binary and a Redis server.

- `xver.sh`: lists in a snapshot and an AOF written by an older release load
  in the build under test, and ones written by it load in the older release,
  every element compared in order. `OLD=<path to the older kividb>
  NEW=<path to the build under test> scripts/checks/lists/xver.sh`; each line
  ends `CHECK OK`.
- `compare.py <redis-port> <kividb-port>`: one sequence of list commands sent
  to a Redis server and to KiviDB (every read and write on a list of each
  shape: one chunk, many chunks, made irregular by inserts in the middle,
  with an element over 1,024 bytes, plain after it went; growth and shrinking
  an element at a time across the chunk boundary from both ends; conversions
  by a long element pushed, set and inserted; moves between packed and plain
  lists; blocking commands served at once; `SORT`, `COPY`, `RENAME`, `DEL`,
  `UNLINK`; the capped list and the queue), replies compared exactly and each
  list read back whole after every write. `OBJECT ENCODING` differences are
  listed apart, grouped: KiviDB closes a chunk at 64 elements where Redis
  closes a node at 8 kB (see docs/COMPATIBILITY.md). Prints each difference
  and a summary.

Memory per element is recorded by regression suite `21-list-memory`.
