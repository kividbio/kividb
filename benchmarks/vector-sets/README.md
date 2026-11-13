# Vector-set benchmark

Loads vectors with `VADD` and queries them with `VSIM key ... COUNT 10 [EF n]`, against
any Redis-protocol server: KiviDB, Redis 8 or anything else with vector sets. It reports
load time, memory per vector, VSIM throughput and p50/p99 latency at 1, 16 and 100
clients, and recall@10 against exact ground truth computed with numpy.

Used for #710 (VSIM search), #697 (quantized storage) and #627 (vector-set benchmarks).

## Running it

```bash
python3 -m venv venv && venv/bin/pip install redis numpy
# one server at a time, each started empty with persistence off:
KIVIDB=~/kividb/target/release/kividb REDIS=/opt/redis-8.10.2/src/redis-server   PY=venv/bin/python SIZES="10000 100000" REDIS_NOQUANT="10000 100000"   OUT=results bash run.sh
python3 summarize.py results/*.json
```

Or against a server that's already running:

```bash
venv/bin/python vset_bench.py --host 127.0.0.1 --port 6380 --n 100000 --dim 128   --clients 1,16,100 --ef default,200,800 --label kividb --out results/kividb.json
```

The data is fixed-seed random unit vectors (seed 42), plus 1000 separate query vectors.
Ground truth is exact cosine top-10.

## Caveats

- The results below are from **one 4-core Azure VM (D4s_v5) with the client on the same
  box**, so read them as relative, not absolute. Use a separate client machine for
  numbers to publish.
- **Random uniform vectors are very hard for HNSW:** Redis's recall@10 at default EF on
  100k is 0.31. Real embedding datasets (glove, dbpedia, as in #627) give much higher
  recall at the same EF, so always compare with recall next to throughput.
- **KiviDB (1.2.x/master) ignores the quantization and HNSW options:**
  - it accepts `Q8`, `NOQUANT`, `BIN`, `M` and `EF` on `VADD` and stores f32
    (`VINFO`: `quant-type f32`, `index-type FLAT`);
  - it accepts `EF`, `EPSILON` and `TRUTH` on `VSIM` and runs an exact scan.

  So its recall is always 1.0, and its query time grows linearly with the set size
  (#710). Quantized storage is #697.
- **1M vectors:** only KiviDB's load (5.4 s, 665 B/vector) and its single-client
  query (7.3 ops/s, p50 136 ms) were measured. A full 1M run on both servers is still to do.

## Results: dim 128, master 3c960de8 vs Redis 8.10.2 (Q8 default, and NOQUANT)

| Vectors | Server | Load s | Bytes/vector | EF | Clients | ops/s | p50 ms | p99 ms | recall@10 |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 10,000 | kividb | 0.1 | 632 | default | 1 | 1,403 | 0.70 | 0.85 | 1.0000 |
|  |  |  |  | default | 16 | 3,316 | 4.90 | 5.90 | 1.0000 |
|  |  |  |  | default | 100 | 3,392 | 29.17 | 33.38 | 1.0000 |
|  |  |  |  | 200 | 1 | 1,389 | 0.70 | 0.93 | 1.0000 |
|  |  |  |  | 200 | 16 | 3,270 | 4.71 | 6.59 | 1.0000 |
|  |  |  |  | 200 | 100 | 3,341 | 29.60 | 37.06 | 1.0000 |
|  |  |  |  | 800 | 1 | 1,388 | 0.70 | 0.92 | 1.0000 |
|  |  |  |  | 800 | 16 | 3,114 | 5.14 | 6.05 | 1.0000 |
|  |  |  |  | 800 | 100 | 3,369 | 29.37 | 34.18 | 1.0000 |
| 10,000 | redis | 2.2 | 882 | default | 1 | 2,222 | 0.42 | 0.62 | 0.7616 |
|  |  |  |  | default | 16 | 5,573 | 2.75 | 5.33 | 0.7616 |
|  |  |  |  | default | 100 | 5,181 | 18.65 | 33.93 | 0.7616 |
|  |  |  |  | 200 | 1 | 1,611 | 0.58 | 0.94 | 0.9103 |
|  |  |  |  | 200 | 16 | 3,985 | 3.84 | 7.82 | 0.9103 |
|  |  |  |  | 200 | 100 | 3,735 | 25.83 | 48.96 | 0.9103 |
|  |  |  |  | 800 | 1 | 769 | 1.25 | 1.94 | 0.9815 |
|  |  |  |  | 800 | 16 | 1,593 | 9.77 | 19.74 | 0.9815 |
|  |  |  |  | 800 | 100 | 1,598 | 59.20 | 134.87 | 0.9815 |
| 10,000 | redis-noquant | 2.4 | 1,260 | default | 1 | 2,147 | 0.44 | 0.62 | 0.7623 |
|  |  |  |  | default | 16 | 5,302 | 2.89 | 5.62 | 0.7623 |
|  |  |  |  | default | 100 | 4,999 | 19.37 | 35.13 | 0.7623 |
|  |  |  |  | 200 | 1 | 1,583 | 0.59 | 0.89 | 0.9180 |
|  |  |  |  | 200 | 16 | 3,874 | 3.94 | 8.24 | 0.9180 |
|  |  |  |  | 200 | 100 | 3,681 | 26.38 | 48.29 | 0.9180 |
|  |  |  |  | 800 | 1 | 717 | 1.34 | 2.29 | 0.9974 |
|  |  |  |  | 800 | 16 | 1,579 | 9.82 | 20.06 | 0.9974 |
|  |  |  |  | 800 | 100 | 1,595 | 59.91 | 143.62 | 0.9974 |
| 100,000 | kividb | 0.6 | 608 | default | 1 | 81 | 12.28 | 13.18 | 1.0000 |
|  |  |  |  | default | 16 | 169 | 93.08 | 163.34 | 1.0000 |
|  |  |  |  | default | 100 | 163 | 592.41 | 1307.25 | 1.0000 |
|  |  |  |  | 200 | 1 | 85 | 11.72 | 13.54 | 1.0000 |
|  |  |  |  | 200 | 16 | 170 | 93.62 | 101.67 | 1.0000 |
|  |  |  |  | 200 | 100 | 169 | 582.06 | 1294.94 | 1.0000 |
|  |  |  |  | 800 | 1 | 82 | 12.17 | 13.62 | 1.0000 |
|  |  |  |  | 800 | 16 | 168 | 99.86 | 116.61 | 1.0000 |
|  |  |  |  | 800 | 100 | 171 | 575.64 | 1030.09 | 1.0000 |
| 100,000 | redis | 47.9 | 884 | default | 1 | 1,600 | 0.60 | 0.84 | 0.3075 |
|  |  |  |  | default | 16 | 4,429 | 3.45 | 7.00 | 0.3075 |
|  |  |  |  | default | 100 | 4,200 | 23.04 | 42.77 | 0.3075 |
|  |  |  |  | 200 | 1 | 1,064 | 0.91 | 1.30 | 0.4772 |
|  |  |  |  | 200 | 16 | 2,919 | 5.22 | 11.08 | 0.4772 |
|  |  |  |  | 200 | 100 | 2,855 | 33.77 | 65.88 | 0.4772 |
|  |  |  |  | 800 | 1 | 366 | 2.68 | 3.58 | 0.8473 |
|  |  |  |  | 800 | 16 | 905 | 17.36 | 32.20 | 0.8473 |
|  |  |  |  | 800 | 100 | 901 | 100.91 | 253.99 | 0.8473 |
| 100,000 | redis-noquant | 55.8 | 1,269 | default | 1 | 1,520 | 0.63 | 0.93 | 0.3041 |
|  |  |  |  | default | 16 | 3,813 | 4.01 | 8.37 | 0.3041 |
|  |  |  |  | default | 100 | 3,674 | 26.42 | 46.96 | 0.3041 |
|  |  |  |  | 200 | 1 | 982 | 0.98 | 1.41 | 0.4764 |
|  |  |  |  | 200 | 16 | 2,428 | 6.23 | 14.13 | 0.4764 |
|  |  |  |  | 200 | 100 | 2,431 | 39.93 | 79.31 | 0.4764 |
|  |  |  |  | 800 | 1 | 316 | 3.11 | 4.11 | 0.8610 |
|  |  |  |  | 800 | 16 | 762 | 20.76 | 37.71 | 0.8610 |
|  |  |  |  | 800 | 100 | 778 | 118.36 | 286.70 | 0.8610 |


The raw results are in `results/*.json`.
