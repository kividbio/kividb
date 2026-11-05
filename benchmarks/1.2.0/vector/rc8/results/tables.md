## rc8 against rc7 (same VMs, same session)

| dataset | clients | rc7 q/s median (min-max) | rc8 q/s median (min-max) | rc8/rc7 | pass spread rc7 / rc8 | largest rep spread | verdict | recall rc7 / rc8 | p99 ms rc7 / rc8 |
|---|---:|---:|---:|---:|---:|---:|---|---:|---:|
| h-and-m | 1 | 245 (239-246) | 242 (241-243) | 0.991 | 2.9% / 0.9% | 5.0% | same | 0.9984 / 0.9984 | 16.91 / 17.23 |
| h-and-m | 16 | 3,075 (3,066-3,078) | 3,077 (3,076-3,084) | 1.001 | 0.4% / 0.3% | 4.8% | same | 0.9984 / 0.9984 | 22.25 / 22.13 |
| h-and-m | 100 | 4,472 (4,471-4,494) | 4,485 (4,482-4,499) | 1.003 | 0.5% / 0.4% | 0.9% | same | 0.9984 / 0.9984 | 84.47 / 84.55 |
| dbpedia-100K | 1 | 593 (587-598) | 591 (584-599) | 0.998 | 1.9% / 2.7% | 4.6% | same | 0.9858 / 0.9850 | 2.33 / 2.30 |
| dbpedia-100K | 16 | 7,187 (7,148-7,226) | 7,189 (7,140-7,238) | 1.000 | 1.1% / 1.4% | 1.5% | same | 0.9858 / 0.9850 | 3.07 / 3.09 |
| dbpedia-100K | 100 | 10,184 (10,168-10,200) | 10,160 (10,095-10,226) | 0.998 | 0.3% / 1.3% | 1.9% | same | 0.9858 / 0.9850 | 13.91 / 13.72 |
| glove-25 | 100 | 33,995 (33,905-34,084) | 33,934 (33,929-33,940) | 0.998 | 0.5% / 0.0% | 1.6% | same | 0.9647 / 0.9647 | 3.94 / 3.94 |
| dbpedia-1M | 1 | 333 (329-347) | 339 (332-339) | 1.017 | 5.3% / 2.0% | 4.6% | same | 0.9603 / 0.9607 | 3.90 / 3.90 |
| dbpedia-1M | 16 | 4,386 (4,376-4,466) | 4,392 (4,382-4,406) | 1.001 | 2.1% / 0.5% | 1.7% | same | 0.9603 / 0.9607 | 4.88 / 4.85 |
| dbpedia-1M | 100 | 6,316 (6,268-6,320) | 6,235 (6,148-6,277) | 0.987 | 0.8% / 2.1% | 5.8% | same | 0.9603 / 0.9607 | 22.76 / 23.17 |

### Ingest and memory (one load per pass)

| dataset | rc7 upload+index s (loads) | rc8 upload+index s (loads) | rc8/rc7 | rc7 RSS B/vector | rc8 RSS B/vector | rc8/rc7 | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| h-and-m | 15.0 (15.0, 14.9, 15.1) | 15.0 (15.0, 15.0, 15.9) | 1.006 | 21,928 | 21,930 | 1.000 | same |
| dbpedia-100K | 11.8 (11.8, 11.8) | 11.9 (11.9, 11.8) | 1.007 | 13,263 | 13,269 | 1.000 | same |
| glove-25 | 30.4 (29.7, 30.4, 31.9, 32.3, 31.9, 30.0, 29.8) | 30.1 (31.8, 30.2, 30.0, 30.0, 30.1, 29.9, 30.5) | 0.991 | 1,124 | 1,125 | 1.001 | same |
| dbpedia-1M | 224.5 (224.5, 225.9, 224.4) | 224.8 (223.6, 225.2, 224.8) | 1.002 | 13,202 | 13,202 | 1.000 | same |

## rc8 against the published 75482abb numbers (different session: indicative only)

| dataset | clients | published q/s | rc8 q/s | rc8/published | published recall | rc8 recall |
|---|---:|---:|---:|---:|---:|---:|
| h-and-m | 1 | 252 | 242 | 0.962 | 0.9984 | 0.9984 |
| h-and-m | 16 | 3,092 | 3,077 | 0.995 | 0.9984 | 0.9984 |
| h-and-m | 100 | 4,478 | 4,485 | 1.002 | 0.9984 | 0.9984 |
| dbpedia-100K | 1 | 609 | 591 | 0.971 | 0.9864 | 0.9850 |
| dbpedia-100K | 16 | 7,163 | 7,189 | 1.004 | 0.9864 | 0.9850 |
| dbpedia-100K | 100 | 10,255 | 10,160 | 0.991 | 0.9864 | 0.9850 |
| glove-25 | 100 | 33,891 | 33,934 | 1.001 | 0.9647 | 0.9647 |
| dbpedia-1M | 1 | 341 | 339 | 0.994 | 0.9604 | 0.9607 |
| dbpedia-1M | 16 | 4,509 | 4,392 | 0.974 | 0.9604 | 0.9607 |
| dbpedia-1M | 100 | 6,255 | 6,235 | 0.997 | 0.9604 | 0.9607 |

| dataset | published ingest s | rc8 ingest s | ratio | published RSS B/vector | rc8 RSS B/vector |
|---|---:|---:|---:|---:|---:|
| h-and-m | 14.9 | 15.0 | 1.010 | 21,936 | 21,930 |
| dbpedia-100K | 11.7 | 11.9 | 1.014 | 13,265 | 13,269 |
| glove-25 | 30.2 | 30.1 | 0.998 | 1,126 | 1,125 |
| dbpedia-1M | 221.0 | 224.8 | 1.017 | 13,202 | 13,202 |

## Competitors (one pass, ef 128, 100 clients) against their published numbers

| dataset | engine | published q/s | q/s now | now/published | recall now | ingest s published / now | RSS B/vector published / now | rc8 q/s now / engine |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| h-and-m | Redis 8.10.2 | 1,802 | 1,793 | 0.995 | 0.9986 | 15.1 / 14.9 | 22,785 / 22,793 | 2.50x (published 2.49x) |
| h-and-m | Dragonfly 2.0.2 | 811 | 796 | 0.982 | 0.9981 | 9.5 / 9.8 | 14,233 / 14,480 | 5.63x (published 5.52x) |
| dbpedia-100K | Redis 8.10.2 | 8,198 | 8,031 | 0.980 | 0.9805 | 10.1 / 10.3 | 14,451 / 14,778 | 1.27x (published 1.25x) |
| dbpedia-100K | Dragonfly 2.0.2 | 9,399 | 9,362 | 0.996 | 0.9803 | 9.2 / 9.3 | 8,281 / 8,378 | 1.09x (published 1.09x) |

## Memory after the drop (rc8, dbpedia-1M, upload only)

- peak RSS 12.30 GiB; RSS after load 12.30 GiB
- 5 s after the tool's delete started: RSS 10.67 GiB, used_memory 391.9 MiB, keys 61200
- 30 s after the tool's delete started: RSS 10.61 GiB, used_memory 5.6 MiB, keys 0
- 55 s after the tool's delete started: RSS 10.61 GiB, used_memory 5.6 MiB, keys 0
- last sample: RSS 0.07 GiB, used_memory 5.6 MiB
