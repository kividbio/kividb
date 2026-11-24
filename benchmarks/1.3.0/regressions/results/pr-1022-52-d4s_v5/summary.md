# 52-small-lists-memory, pr-1022-52-d4s_v5

Measured on a Standard_D4s_v5 (Intel Xeon Platinum 8370C), 2026-11-24, `--threads 4`.
The candidate is the `lua,tls` build (mimalloc) of branch `fix/list-read-stats-and-memory` at the commit
recorded in `baseline.json`; v1.2.2 is a `lua,tls` build of the tag; Redis is 8.10.2 built from source
(`/opt/redis-8.10.2`). The three were started by hand, one at a time, each configuration on a fresh server,
and the suite run directly (`python suites/52-small-lists-memory.py --config ... --engine ...`), not by `run-all.sh`.

| config | metric | v1.2.2 | candidate | Redis | candidate / Redis |
|---|---|---:|---:|---:|---:|
| load | used_per_element | 59.703 | 14.827 | 13.8527 | 1.07 |
| load | rss_per_element | 59.62 | 16.9643 | 14.366 | 1.18 |
| load | usage_one | 3144.0 | 4447.0 | 3641.0 | 1.22 |
| load | list_len | 300.0 | 300.0 | 300.0 | 1.00 |
| load | push_s | 11.1851 | 10.467 | 6.7836 | 1.54 |
| load-batch | used_per_element | 48.3963 | 13.4369 | 13.8522 | 0.97 |
| load-batch | rss_per_element | 51.2082 | 14.138 | 14.3578 | 0.98 |
| load-batch | usage_one | 3144.0 | 4030.0 | 3641.0 | 1.11 |
| load-batch | list_len | 300.0 | 300.0 | 300.0 | 1.00 |
| load-batch | push_s | 1.6965 | 1.6437 | 1.8825 | 0.87 |
| timeline | used_per_element | 64.3967 | 14.2373 | 13.8697 | 1.03 |
| timeline | rss_per_element | 69.4381 | 17.6742 | 14.3715 | 1.23 |
| timeline | usage_one | 3144.0 | 4270.0 | 3641.0 | 1.17 |
| timeline | list_len | 300.0 | 300.0 | 300.0 | 1.00 |
| timeline | push_s | 67.6746 | 55.7646 | 88.4645 | 0.63 |
| size32 | used_per_element | 64.3965 | 35.4369 | 34.7591 | 1.02 |
| size32 | rss_per_element | 82.6778 | 41.4256 | 35.5683 | 1.16 |
| size32 | usage_one | 9744.0 | 10630.0 | 10376.0 | 1.02 |
| size32 | list_len | 300.0 | 300.0 | 300.0 | 1.00 |
| size32 | push_s | 3.3892 | 3.7098 | 3.4813 | 1.07 |
| size64 | used_per_element | 96.3963 | 67.4371 | 69.0253 | 0.98 |
| size64 | rss_per_element | 114.83 | 85.4822 | 70.615 | 1.21 |
| size64 | usage_one | 19344.0 | 20230.0 | 20323.0 | 1.00 |
| size64 | list_len | 300.0 | 300.0 | 300.0 | 1.00 |
| size64 | push_s | 3.6534 | 4.9612 | 3.9503 | 1.26 |
| tiny | used_per_element | 53.0297 | 16.9041 | 14.7959 | 1.14 |
| tiny | rss_per_element | 50.886 | 22.8284 | 15.0323 | 1.52 |
| tiny | usage_one | 444.0 | 506.0 | 401.0 | 1.26 |
| tiny | list_len | 30.0 | 30.0 | 30.0 | 1.00 |
| tiny | push_s | 0.455 | 0.645 | 0.5858 | 1.10 |
