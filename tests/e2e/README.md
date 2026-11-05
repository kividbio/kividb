# KiviDB — End-to-End Test Suite

Comprehensive integration and benchmark tests for every command KiviDB supports.
Tests are written in Python using the standard `redis-py` client, so they validate
wire-level compatibility at the same time.

## Structure

```
tests/e2e/
├── conftest.py                  # Session-level fixtures (server lifecycle)
├── pytest.ini                   # pytest configuration
├── requirements.txt
├── utils/
│   └── helpers.py               # Shared fixtures, vector helpers, assertions
├── modules/
│   ├── test_strings.py          # SET/GET/INCR/APPEND/GETRANGE/SETRANGE ...
│   ├── test_lists.py            # LPUSH/RPUSH/LRANGE/LMOVE/BLPOP ...
│   ├── test_hashes.py           # HSET/HGET/HMGET/HSCAN/HINCRBY ...
│   ├── test_sets.py             # SADD/SINTER/SDIFF/SUNION/SSCAN ...
│   ├── test_sorted_sets.py      # ZADD/ZRANGE/ZRANGEBYSCORE/ZUNIONSTORE ...
│   ├── test_generics_server.py  # TTL/EXPIRE/KEYS/SCAN/PING/INFO/PubSub/HLL
│   ├── test_streams.py          # XADD/XREAD/XGROUP/XREADGROUP/XCLAIM ...
│   ├── test_json_ts_geo.py      # JSON.*/TS.*/GEO*
│   └── test_vector_search.py   # FT.CREATE/FT.SEARCH KNN/FLAT/HNSW/VADD/VSIM
└── benchmarks/
    └── bench_memtier.py         # memtier_benchmark wrapper + vector micro-bench
```

## Running locally

### Prerequisites
```bash
# Build KiviDB
cargo build --release

# Install Python dependencies
pip install -r tests/e2e/requirements.txt
```

### Run all functional tests (starts KiviDB automatically)
```bash
cd tests/e2e
pytest modules/ -v
```

### Run against an already-running KiviDB
```bash
KIVI_EXTERNAL=1 KIVI_PORT=6380 pytest modules/ -v
```

### Run a single module
```bash
pytest modules/test_vector_search.py -v
```

### Run in parallel (4 workers)
```bash
pytest modules/ -v -n 4
```

### Run memtier benchmarks
```bash
# Requires memtier_benchmark installed: https://github.com/RedisLabs/memtier_benchmark
BENCH_MEMTIER=1 pytest benchmarks/bench_memtier.py::TestMemtierBenchmarks -v -s
```

### Run vector benchmarks
```bash
BENCH_VECTOR=1 pytest benchmarks/bench_memtier.py::TestVectorBenchmarks -v -s
```

## Environment variables

| Variable          | Default       | Description                                      |
|-------------------|---------------|--------------------------------------------------|
| `KIVI_HOST`       | `127.0.0.1`   | KiviDB host                                      |
| `KIVI_PORT`       | `6380`        | KiviDB port                                      |
| `KIVI_BINARY`     | `./target/release/kividb` | Path to binary (auto-start mode)   |
| `KIVI_EXTERNAL`   | `0`           | Set to `1` if KiviDB is already running          |
| `BENCH_MEMTIER`   | `0`           | Set to `1` to enable memtier benchmarks          |
| `BENCH_VECTOR`    | `0`           | Set to `1` to enable vector benchmarks           |
| `BENCH_THREADS`   | `4`           | memtier thread count                             |
| `BENCH_CLIENTS`   | `20`          | memtier client count per thread                  |
| `BENCH_REQUESTS`  | `100000`      | Total requests per memtier test                  |
| `BENCH_PIPELINE`  | `1`           | memtier pipeline depth                           |
| `BENCH_DATA_SIZE` | `64`          | Value size in bytes                              |
| `MEMTIER_BIN`     | `memtier_benchmark` | Path to memtier binary                    |

## CI integration

The e2e workflow lives in `.github/workflows/e2e.yml`. It:

- Triggers automatically **after CI passes** on `main`/`master`
- Can be triggered **manually** from the Actions tab with optional benchmark flags
- Runs all functional test modules **in parallel** (one job per module)
- Benchmarks are **opt-in** via workflow dispatch inputs — they are never run on auto-trigger to keep CI fast
- Benchmark results are uploaded as artifacts for 30 days

## Design notes

**No mocks** — every test hits a real KiviDB process over a real TCP connection.

**Module isolation** — each test function gets a fresh `FLUSHDB` via the `r` fixture.

**Parallel-safe** — tests within a module are isolated by key naming; modules
run in separate CI jobs with their own server instance.

**Blocking command tests** — BLPOP, BRPOP, BLMOVE, XREAD BLOCK use background
threads to simulate real concurrent clients, ensuring wakeup paths are exercised.

**Concurrency stress** — `test_parallel_incr` (200 goroutines) and similar tests
catch atomicity bugs that unit tests miss.

**Vector search validation** — KNN results are validated for correct ordering
(ascending distance), recall of the nearest neighbour, and hybrid filter
correctness (all returned docs must satisfy the pre-filter).