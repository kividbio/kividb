# 52-small-lists-memory, pr-1022-52-d4s_v5

Measured on a Standard_D4s_v5 (Intel Xeon Platinum 8370C), 2026-11-24, `--threads 4`.
The candidate is the `lua,tls` build (mimalloc) of branch `fix/list-read-stats-and-memory` at the commit
recorded in `baseline.json`; v1.2.2 is a `lua,tls` build of the tag; Redis is 8.10.2 built from source
(`/opt/redis-8.10.2`). The three were started by hand, one at a time, each configuration on a fresh server,
and the suite run directly (`python suites/52-small-lists-memory.py --config ... --engine ...`), not by `run-all.sh`.

| config | metric | v1.2.2 | candidate | Redis | candidate / Redis |
|---|---|---:|---:|---:|---:|
| load | used_per_element | 59.703 | 14.1335 | 13.8531 | 1.02 |
| load | rss_per_element | 59.62 | 16.2557 | 14.3415 | 1.13 |
| load | usage_one | 3144.0 | 4239.0 | 3641.0 | 1.16 |
| load | list_len | 300.0 | 300.0 | 300.0 | 1.00 |
| load | push_s | 4.6365 | 4.7882 | 5.6839 | 0.84 |
| load-batch | used_per_element | 48.3963 | 13.4369 | 13.8519 | 0.97 |
| load-batch | rss_per_element | 51.1823 | 14.1585 | 14.3592 | 0.99 |
| load-batch | usage_one | 3144.0 | 4030.0 | 3641.0 | 1.11 |
| load-batch | list_len | 300.0 | 300.0 | 300.0 | 1.00 |
| load-batch | push_s | 1.8416 | 1.7647 | 1.642 | 1.07 |
| timeline | used_per_element | 64.3965 | 14.2371 | 13.8695 | 1.03 |
| timeline | rss_per_element | 70.3706 | 17.6988 | 14.3483 | 1.23 |
| timeline | usage_one | 3144.0 | 4270.0 | 3641.0 | 1.17 |
| timeline | list_len | 300.0 | 300.0 | 300.0 | 1.00 |
| timeline | push_s | 49.5925 | 44.5655 | 55.757 | 0.80 |
| size32 | used_per_element | 64.3963 | 35.4371 | 34.7585 | 1.02 |
| size32 | rss_per_element | 82.6696 | 42.1342 | 35.571 | 1.18 |
| size32 | usage_one | 9744.0 | 10630.0 | 10376.0 | 1.02 |
| size32 | list_len | 300.0 | 300.0 | 300.0 | 1.00 |
| size32 | push_s | 1.7618 | 1.7264 | 1.9032 | 0.91 |
| size64 | used_per_element | 96.3963 | 67.4369 | 69.025 | 0.98 |
| size64 | rss_per_element | 114.83 | 85.4712 | 70.6369 | 1.21 |
| size64 | usage_one | 19344.0 | 20230.0 | 20323.0 | 1.00 |
| size64 | list_len | 300.0 | 300.0 | 300.0 | 1.00 |
| size64 | push_s | 1.9574 | 1.9533 | 2.1189 | 0.92 |
| tiny | used_per_element | 53.0315 | 16.9013 | 14.7958 | 1.14 |
| tiny | rss_per_element | 57.9447 | 15.7423 | 15.2508 | 1.03 |
| tiny | usage_one | 444.0 | 506.0 | 401.0 | 1.26 |
| tiny | list_len | 30.0 | 30.0 | 30.0 | 1.00 |
| tiny | push_s | 0.4569 | 0.454 | 0.4579 | 0.99 |
