# Search: the field clock read once per query

`run.sh` counts the instructions the whole server spends on a search query,
under callgrind, for two binaries (before and after a change), over 3,000
hashes with and without a field TTL on every hash. A TTL makes each hash a
`HashEx`, the kind a query has to judge field deadlines on.

    cargo build --release                     # in each tree
    benchmarks/1.3.0/search-clock/run.sh before=/path/kividb-before after=/path/kividb-after

Needs `valgrind`, `redis-cli` and `python3`. Instruction counts do not move
between runs of one binary, so a difference of 1% is real and a regression is
anything above 1% on a row that is not a TTL row of the changed code.

Queries (every one over `FT.CREATE idx ON HASH PREFIX 1 d: SCHEMA t TAG n
NUMERIC vec VECTOR FLAT 6 ... DIM 8`):

| kind | query |
|---|---|
| `search-knn` | `FT.SEARCH idx "*=>[KNN 10 @vec $q]"` |
| `search-filter-knn` | `FT.SEARCH idx "(@t:{red})=>[KNN 10 @vec $q]"` |
| `search-tag` | `FT.SEARCH idx "@t:{red}" LIMIT 0 10` |
| `aggregate-knn` | `FT.AGGREGATE idx "*=>[KNN 10 @vec $q AS d]"` |
| `aggregate-filter-knn` | `FT.AGGREGATE idx "(@t:{red})=>[KNN 10 @vec $q AS d]"` |

Results are in `results/`.

## Result (2026-11-15, Azure D4s_v5, release build with LTO off, 2,000 hashes, 100 queries)

Instructions per query, master (c3936831) and this change. `ttl=1` has a
far deadline on one field of every hash (nothing expires).

| kind | ttl | master | change | |
|---|---|---|---|---|
| search-knn | 0 | 739,266 | 740,286 | +0.1% |
| search-knn | 1 | 767,946 | 764,133 | -0.5% |
| search-filter-knn | 0 | 512,730 | 513,180 | +0.1% |
| search-filter-knn | 1 | 5,223,048 | 5,165,153 | -1.1% |
| search-tag | 0 | 221,971 | 221,501 | -0.2% |
| search-tag | 1 | 4,960,501 | 4,901,144 | -1.2% |
| aggregate-knn | 0 | 16,164,930 | 16,141,323 | -0.1% |
| aggregate-knn | 1 | 20,604,060 | 19,874,735 | -3.5% |
| aggregate-filter-knn | 0 | 5,521,313 | 5,516,411 | -0.1% |
| aggregate-filter-knn | 1 | 10,662,963 | 10,359,379 | -2.8% |

The raw output is `results/raw.log`: `old` is master, `new` a first build of the
change that read the clock up front (it was +1.4% to +1.8% on the `ttl=0`
rows, which is why the clock is read when first needed), `new2` the change
measured above. Instruction counts leave out what a clock read costs in
cycles.
No row gains 5%, so there is no regression suite row: a change of more than
1% on one of these rows is a regression.
