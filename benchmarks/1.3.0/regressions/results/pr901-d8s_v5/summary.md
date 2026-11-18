# Regression suite: pr901-d8s_v5

Verdict: **ok**

NOTE: v1.2.2 has no usable value for the speed metrics of 12 gate(s): speed gates were compared absolutely against the recorded values (30-script-call-cost/scripts/lock_calls_per_s, 30-script-call-cost/scripts/lock_cpu_us_per_call, 30-script-call-cost/scripts/lock_tag_calls_per_s, 30-script-call-cost/scripts/lock_tag_cpu_us_per_call, 30-script-call-cost/scripts/return1_calls_per_s, ...).

- Machine: Standard_D8s_v5, 8 vCPU, INTEL(R) XEON(R) PLATINUM 8573C, kernel 7.0.0-1017-azure, glibc 2.39
- calibration (indicative only): 86.0 ms for a fixed sha256 loop, 8 cores
- Client: Python 3.12.3, redis-py 5.2.1
- v1_2_2: `kividb 1.2.2`, sha256 `38fc2cea40c131e041f4914527a98719e5d34dffdfe2abdc1f07ecb86aa6723e`, rustc 1.100.0
- candidate: `kividb 1.2.1 (full build: Lua, TLS)`, sha256 `90362ba3b91a0d9156876ec34065c75fac6b411c9e63a40185ded1da987330cd`, rustc 1.100.0
- redis: `Redis server v=8.10.2 sha=00000000:1 malloc=jemalloc-5.3.0 bits=64 build=75a9b36b1f0aa00`, sha256 `b0cd304980112fa6cd8dced524fdd6b136fd8efb802f5cf9e81610051a11891c`
- v1_2_2_full: `kividb 1.2.2`, sha256 `ac5e05fe1274fd1167780e512bc03aa103340f42525a2cef4a2a762e47317ad5`, rustc 1.100.0
- candidate_full: `kividb 1.2.1 (full build: Lua, TLS)`, sha256 `90362ba3b91a0d9156876ec34065c75fac6b411c9e63a40185ded1da987330cd`, rustc 1.100.0
- candidate commit: `merged2`
- date: 2026-11-18; FULL=0, SCALE=1
- run time: 30-script-call-cost 549s

## 30-script-call-cost

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| scripts | lock_calls_per_s | calls/s | 111,116 | 117,836 | 127,815 | - | rel: rel 10%: nothing recorded | new |
| scripts | lock_cpu_us_per_call | us | 9 | 8.49 | 7.82 | - | rel: rel 10%: nothing recorded<br>vs_v1_2_2: 0.94x v1.2.2 <= 1.03x | new<br>pass |
| scripts | lock_tag_calls_per_s | calls/s | 111,284 | 114,821 | 126,049 | - | rel: rel 10%: nothing recorded | new |
| scripts | lock_tag_cpu_us_per_call | us | 9 | 8.73 | 7.92 | - | rel: rel 10%: nothing recorded<br>vs_v1_2_2: 0.97x v1.2.2 <= 1.03x | new<br>pass |
| scripts | return1_calls_per_s | calls/s | 124,469 | 129,794 | 149,055 | - | rel: rel 10%: nothing recorded | new |
| scripts | return1_cpu_us_per_call | us | 8.04 | 7.71 | 6.71 | - | rel: rel 10%: nothing recorded<br>vs_v1_2_2: 0.96x v1.2.2 <= 1.03x | new<br>pass |
| scripts | return1_tag_calls_per_s | calls/s | 120,622 | 125,824 | 142,368 | - | rel: rel 10%: nothing recorded | new |
| scripts | return1_tag_cpu_us_per_call | us | 8.32 | 7.97 | 7.01 | - | rel: rel 10%: nothing recorded<br>vs_v1_2_2: 0.96x v1.2.2 <= 1.03x | new<br>pass |
| scripts | tyk_calls_per_s | calls/s | 71,562 | 81,004 | 82,340 | - | rel: rel 10%: nothing recorded | new |
| scripts | tyk_cpu_us_per_call | us | 13.99 | 12.36 | 12.14 | - | rel: rel 10%: nothing recorded<br>vs_v1_2_2: 0.88x v1.2.2 <= 0.93x | new<br>pass |
| scripts | tyk_tag_calls_per_s | calls/s | 69,898 | 79,297 | 83,256 | - | rel: rel 10%: nothing recorded | new |
| scripts | tyk_tag_cpu_us_per_call | us | 14.32 | 12.63 | 11.99 | - | rel: rel 10%: nothing recorded<br>vs_v1_2_2: 0.88x v1.2.2 <= 0.93x | new<br>pass |

