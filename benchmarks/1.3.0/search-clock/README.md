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

The raw output is `results/raw.md`: `old` is master, `new` a first build of the
change that read the clock up front (it was +1.4% to +1.8% on the `ttl=0`
rows, which is why the clock is read when first needed), `new2` the change
measured above. Instruction counts leave out what a clock read costs in
cycles.
No row gains 5%, so there is no regression suite row: a change of more than
1% on one of these rows is a regression.

## Mostly hidden nearest vectors (#670 item 3)

`TTLS="0 1 h900 h990 h999"` adds the case where the vector field of that many
hashes in a thousand is past its deadline and still stored (the sweep is
paused), so an unfiltered KNN's nearest are mostly hidden from it; `ALGO=HNSW`
builds the index as HNSW. `search-knn` is the kind it changes:

    TTLS="0 1 h900 h990 h999" KINDS=search-knn ALGO=HNSW \
      benchmarks/1.3.0/search-clock/run.sh before=... after=...

Not measured under callgrind yet (no valgrind on the machine the change was
written on). Wall clock, milliseconds a query, median of 3 batches of 50,
Apple M3 Pro, `--threads 1`, master `d1a6712c` and the change, `KNN 10`:

| index | docs | ttl | master | change |
|---|---:|---|---:|---:|
| FLAT | 50,000 | 0 | 1.44 | 1.42 |
| FLAT | 50,000 | 1 | 1.38 | 1.30 |
| FLAT | 50,000 | h900 | 3.73 | 3.43 |
| FLAT | 50,000 | h990 | 6.76 | 5.89 |
| FLAT | 50,000 | h999 | 19.14 | 18.40 |
| HNSW | 50,000 | 0 | 0.84 | 0.82 |
| HNSW | 50,000 | 1 | 0.91 | 1.14 |
| HNSW | 50,000 | h900 | 1.14 | 1.26 |
| HNSW | 50,000 | h990 | 4.09 | 4.02 |
| HNSW | 50,000 | h999 | 27.78 | 23.30 |
| FLAT | 3,000 | h999 | 2.20 | 2.01 |
| HNSW | 3,000 | h999 | 4.34 | 2.14 |

The `0` and `1` rows take the same path on both (one search, nothing left
out); their differences are the laptop's noise. With `TIMEOUT 1` on HNSW,
50,000 hashes, `h999`: master 24.5 ms (it does not look at the clock),
the change 1.9 ms, returning the shown ones it had found.
