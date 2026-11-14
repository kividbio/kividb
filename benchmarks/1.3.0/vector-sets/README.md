# Vector sets: quantized storage (Q8, BIN) against f32 and Redis 8.10.3

A local run of `run.sh` on one machine (Apple M3 Pro, 12 cores, 18 GB, macOS), 2026-11-13. Client and server
share the machine, so treat q/s as relative between rows, not as absolute capacity.

- **Engines:** KiviDB master (`5fd3b7eb`, f32 only), this branch with `NOQUANT`, `Q8` and `BIN`, and Redis 8.10.3
  (Homebrew; 8.10.2 was not available) with the same three quantizations.
- **Data:** deterministic (`numpy` seed 697): vectors around 64 Gaussian centres, 300 queries near the same centres.
  Ground truth is exact f32 cosine in numpy.
- **Search:** KiviDB's `VSIM` is an exact scan. Redis rows marked `truth` pass `TRUTH` (Redis's own exact scan), so
  they differ from KiviDB only in how vectors are stored. `redis-q8-hnsw` is Redis's normal HNSW search, for
  reference.
- **Load:** `VADD ... FP32` pipelined 200 at a time from one client; VADD latency is 500 unpipelined replacements.
- **Search load:** `VSIM ... FP32 ... COUNT 10`, latency from one client, q/s from 8 client processes for 8 s.
- **Memory:** `used_memory` before and after the load, divided by the vector count, plus `MEMORY USAGE key SAMPLES 0`.
  Redis's figures include its HNSW graph in every row (`TRUTH` changes the search, not what is stored).
- **Passes:** one per point. Repeats of single points moved q/s by up to about 15%; memory and recall did not move.
- **Versions:** Python 3.10.18, redis-py 7.4.0, numpy 1.26.3. The KiviDB rows were rerun after the final change;
  the master and Redis rows come from the first pass of the same session.

```
BASELINE=/path/to/master/kividb CURRENT=target/release/kividb PYTHON=python3 ./run.sh
```

`DIMS` and `SIZES` override the defaults (`128 768 1536`, `10000 50000`); `REDIS`, `PORT` and `OUT` are optional.

Raw rows are in [`results/run.jsonl`](results/run.jsonl).

## Results

| dim | n | config | used_memory B/vector | MEMORY USAGE | VADD/s (pipelined) | VADD p50 / p99 ms | VSIM p50 / p95 / p99 ms | VSIM q/s (8 clients) | recall@10 |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 128 | 10K | master-f32 | 633 | 5.1 MB | 57,650 | 0.090 / 0.298 | 0.47 / 1.50 / 3.32 | 14,034 | 1.0000 |
| 128 | 10K | kividb-noquant | 633 | 6.3 MB | 50,114 | 0.085 / 0.207 | 0.48 / 0.94 / 1.17 | 15,441 | 1.0000 |
| 128 | 10K | kividb-q8 | 249 | 2.5 MB | 51,656 | 0.086 / 0.289 | 0.22 / 0.69 / 1.01 | 22,746 | 0.9753 |
| 128 | 10K | kividb-bin | 137 | 1.4 MB | 56,615 | 0.087 / 0.221 | 0.22 / 0.73 / 0.97 | 24,682 | 0.2190 |
| 128 | 10K | redis-f32-truth | 1,276 | 12.0 MB | 7,419 | 0.237 / 0.435 | 0.54 / 1.02 / 3.71 | 13,869 | 1.0000 |
| 128 | 10K | redis-q8-truth | 892 | 8.2 MB | 8,302 | 0.253 / 0.418 | 0.36 / 0.89 / 2.76 | 17,898 | 0.9753 |
| 128 | 10K | redis-bin-truth | 779 | 7.1 MB | 10,202 | 0.238 / 0.416 | 0.28 / 0.74 / 1.28 | 22,099 | 0.2193 |
| 128 | 10K | redis-q8-hnsw | 891 | 8.2 MB | 8,232 | 0.269 / 0.495 | 0.20 / 0.61 / 1.32 | 32,440 | 0.9753 |
| 128 | 50K | master-f32 | 608 | 25.6 MB | 70,682 | 0.072 / 0.169 | 11.71 / 49.57 / 87.64 | 374 | 1.0000 |
| 128 | 50K | kividb-noquant | 608 | 30.4 MB | 65,095 | 0.086 / 0.205 | 3.20 / 5.02 / 8.41 | 990 | 1.0000 |
| 128 | 50K | kividb-q8 | 224 | 11.2 MB | 65,176 | 0.121 / 0.572 | 0.94 / 1.90 / 2.42 | 5,546 | 0.9690 |
| 128 | 50K | kividb-bin | 112 | 5.6 MB | 62,655 | 0.088 / 0.280 | 0.80 / 1.46 / 2.11 | 7,404 | 0.1143 |
| 128 | 50K | redis-f32-truth | 1,272 | 60.1 MB | 7,073 | 0.392 / 0.682 | 1.76 / 2.61 / 4.95 | 2,332 | 1.0000 |
| 128 | 50K | redis-q8-truth | 886 | 40.9 MB | 8,900 | 0.273 / 0.461 | 1.05 / 2.35 / 2.95 | 4,861 | 0.9690 |
| 128 | 50K | redis-bin-truth | 774 | 35.3 MB | 10,968 | 0.239 / 0.424 | 1.35 / 6.14 / 11.00 | 3,178 | 0.1150 |
| 128 | 50K | redis-q8-hnsw | 886 | 40.9 MB | 8,620 | 0.286 / 0.512 | 0.20 / 0.84 / 1.34 | 27,671 | 0.9690 |
| 768 | 10K | master-f32 | 3,193 | 30.7 MB | 39,898 | 0.076 / 0.252 | 3.31 / 3.68 / 4.89 | 1,637 | 1.0000 |
| 768 | 10K | kividb-noquant | 3,193 | 31.9 MB | 39,578 | 0.086 / 0.102 | 3.09 / 6.00 / 6.62 | 1,723 | 1.0000 |
| 768 | 10K | kividb-q8 | 889 | 8.9 MB | 38,260 | 0.085 / 0.100 | 0.52 / 1.00 / 1.40 | 12,635 | 0.9720 |
| 768 | 10K | kividb-bin | 217 | 2.2 MB | 34,394 | 0.088 / 0.105 | 0.28 / 0.68 / 0.88 | 21,798 | 0.2300 |
| 768 | 10K | redis-f32-truth | 3,832 | 37.6 MB | 2,244 | 0.831 / 2.720 | 2.64 / 3.72 / 4.40 | 2,154 | 1.0000 |
| 768 | 10K | redis-q8-truth | 1,531 | 14.6 MB | 3,359 | 0.678 / 2.225 | 1.39 / 2.69 / 4.46 | 5,822 | 0.9720 |
| 768 | 10K | redis-bin-truth | 859 | 7.9 MB | 6,137 | 0.321 / 0.533 | 0.56 / 1.13 / 3.00 | 12,666 | 0.2267 |
| 768 | 10K | redis-q8-hnsw | 1,532 | 14.6 MB | 3,336 | 0.524 / 0.904 | 0.30 / 1.23 / 1.63 | 23,625 | 0.9720 |
| 768 | 50K | master-f32 | 3,168 | 153.6 MB | 44,930 | 0.091 / 0.292 | 16.73 / 19.49 / 26.22 | 292 | 1.0000 |
| 768 | 50K | kividb-noquant | 3,168 | 158.4 MB | 51,486 | 0.085 / 0.099 | 16.21 / 17.23 / 19.42 | 330 | 1.0000 |
| 768 | 50K | kividb-q8 | 864 | 43.2 MB | 48,787 | 0.085 / 0.100 | 6.42 / 11.03 / 13.55 | 722 | 0.9650 |
| 768 | 50K | kividb-bin | 192 | 9.6 MB | 42,273 | 0.088 / 0.109 | 0.91 / 1.59 / 2.25 | 5,675 | 0.1030 |
| 768 | 50K | redis-f32-truth | 3,831 | 188.1 MB | 2,084 | 0.758 / 2.180 | 17.45 / 18.19 / 21.73 | 383 | 1.0000 |
| 768 | 50K | redis-q8-truth | 1,526 | 72.9 MB | 3,199 | 0.664 / 1.324 | 4.56 / 6.38 / 10.64 | 1,069 | 0.9650 |
| 768 | 50K | redis-bin-truth | 854 | 39.3 MB | 6,637 | 0.370 / 0.665 | 2.24 / 4.28 / 6.21 | 1,680 | 0.1047 |
| 768 | 50K | redis-q8-hnsw | 1,527 | 72.9 MB | 3,441 | 0.568 / 1.047 | 0.40 / 1.02 / 1.91 | 18,238 | 0.9627 |
| 1536 | 10K | master-f32 | 6,265 | 61.4 MB | 19,982 | 0.128 / 0.145 | 4.75 / 9.49 / 10.49 | 956 | 1.0000 |
| 1536 | 10K | kividb-noquant | 6,265 | 62.6 MB | 20,885 | 0.118 / 0.133 | 4.35 / 5.47 / 7.25 | 1,139 | 1.0000 |
| 1536 | 10K | kividb-q8 | 1,657 | 16.6 MB | 21,379 | 0.119 / 0.135 | 0.86 / 2.56 / 3.62 | 4,400 | 0.9693 |
| 1536 | 10K | kividb-bin | 313 | 3.1 MB | 22,152 | 0.135 / 0.149 | 0.31 / 0.84 / 1.05 | 17,422 | 0.2327 |
| 1536 | 10K | redis-f32-truth | 6,907 | 68.3 MB | 1,030 | 1.406 / 2.816 | 4.58 / 5.11 / 7.82 | 1,569 | 1.0000 |
| 1536 | 10K | redis-q8-truth | 2,299 | 22.3 MB | 1,765 | 0.930 / 1.878 | 1.99 / 3.28 / 4.69 | 2,845 | 0.9693 |
| 1536 | 10K | redis-bin-truth | 956 | 8.8 MB | 4,020 | 0.479 / 0.785 | 0.83 / 1.39 / 3.55 | 8,031 | 0.2347 |
| 1536 | 10K | redis-q8-hnsw | 2,300 | 22.3 MB | 1,900 | 0.843 / 1.582 | 0.38 / 0.93 / 1.18 | 15,193 | 0.9693 |
| 1536 | 50K | master-f32 | 6,240 | 307.2 MB | 21,513 | 0.128 / 0.141 | 22.87 / 23.75 / 28.28 | 192 | 1.0000 |
| 1536 | 50K | kividb-noquant | 6,240 | 312.0 MB | 22,199 | 0.128 / 0.138 | 22.82 / 23.12 / 24.34 | 185 | 1.0000 |
| 1536 | 50K | kividb-q8 | 1,632 | 81.6 MB | 23,681 | 0.131 / 0.141 | 11.07 / 11.36 / 12.13 | 455 | 0.9557 |
| 1536 | 50K | kividb-bin | 288 | 14.4 MB | 26,105 | 0.130 / 0.147 | 1.19 / 1.87 / 2.81 | 2,899 | 0.1183 |
| 1536 | 50K | redis-f32-truth | 6,902 | 341.7 MB | 1,166 | 1.122 / 2.462 | 22.39 / 24.30 / 38.31 | 257 | 1.0000 |
| 1536 | 50K | redis-q8-truth | 2,295 | 111.3 MB | 1,914 | 0.827 / 2.056 | 11.46 / 12.90 / 14.10 | 571 | 0.9557 |
| 1536 | 50K | redis-bin-truth | 951 | 44.1 MB | 4,363 | 0.556 / 1.839 | 3.12 / 5.66 / 6.87 | 1,602 | 0.1180 |
| 1536 | 50K | redis-q8-hnsw | 2,294 | 111.3 MB | 2,031 | 0.848 / 1.749 | 0.70 / 1.26 / 2.44 | 11,286 | 0.9500 |

The first master row at 128-d/50K (374 q/s) was an outlier. Re-run: master 979 q/s, NOQUANT 1,012 q/s, at the same
recall and memory.

## Kernels

From `cargo test --release --lib vector::quant::tests::kernel_timings -- --ignored --nocapture`, ns per call:

| dim | quantize f32 / int8 / bin | distance f32 / int8 / bin | dequantize f32 / int8 / bin |
|---:|---:|---:|---:|
| 128 | 74 / 245 / 334 | 18.5 / 4.8 / 3.1 | 8.9 / 10.9 / 54 |
| 768 | 52 / 1,249 / 1,867 | 87.5 / 12.3 / 5.1 | 38 / 54 / 287 |
| 1536 | 103 / 2,375 / 3,429 | 185 / 21.0 / 7.1 | 80 / 105 / 546 |

"Quantize" includes copying the input vector. "Dequantize" is what `VEMB` pays.

## Reading the numbers

- **Memory:** NOQUANT takes the same bytes as master. Q8 takes 3.6-3.8x less than f32 at 768-1536 dimensions
  (2.5-2.7x at 128, where per-element overhead weighs more). BIN takes 15-22x less.
- **Q8 recall:** KiviDB's Q8 recall@10 equals Redis's Q8 `TRUTH` recall at every point (0.956-0.975), as both
  quantize with the same formulas. Stored values can differ from Redis's in the last bit, since KiviDB computes the
  norm in f64 and Redis in f32; over 54,000 `VSIM` scores against Redis 8.10.3, the largest difference was 5.6e-7.
- **BIN recall:** 0.10-0.23 on this data, within 0.004 of Redis's. Sign bits keep too little of these vectors for
  top-10 search. BIN distances tie often, and the two order ties differently (KiviDB by name, Redis by insertion).
- **Q8 search:** VSIM is 1.6-7.7x faster than f32 (5.7x at 128-d/50K against the re-run master). The int8 kernel
  is 4-9x faster, but at 50K elements the scan is bound by memory traffic, not by arithmetic: p50 grows with
  dimension (0.94 / 6.42 / 11.07 ms at 128 / 768 / 1536), a steady 6-7 GB/s of int8 data read from one allocation
  per element, of which the kernel takes 10-26%.
- **VADD:** within run-to-run noise of master. Quantizing costs up to 3.4 µs, against ~100 µs per call.
- **Against Redis:** the gap in VSIM q/s at 50K (Q8 455 vs HNSW 11,286 at 1536-d) is the search algorithm,
  not the storage. Against Redis's exact scan of Q8, KiviDB is faster at 10K (1.3-2.2x) and 0.68-1.14x at 50K.
