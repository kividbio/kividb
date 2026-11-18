# Regression suite: pr-884-d4s_v5

Verdict: **ok**

NOTE: v1.2.2 has no usable value for the speed metrics of 2 gate(s): speed gates were compared absolutely against the recorded values (22-scan-large-count/t4/scan_call_us, 22-scan-large-count/t4/scan_server_us).

- Machine: Standard_D4s_v5, 4 vCPU, INTEL(R) XEON(R) PLATINUM 8573C, kernel 7.0.0-1017-azure, glibc 2.39
- calibration (indicative only): 88.6 ms for a fixed sha256 loop, 4 cores
- Client: Python 3.12.3, redis-py 5.2.1
- v1_2_2: `kividb 1.2.2`, sha256 `38fc2cea40c131e041f4914527a98719e5d34dffdfe2abdc1f07ecb86aa6723e`
- candidate: `kividb 1.2.1 (default build: no Lua, no TLS)`, sha256 `1e6f7cb9c4efc4fe4feb802af18b05dafe20da90a1b3faef341ffc9967967ca0`
- redis: `Redis server v=8.10.2 sha=00000000:1 malloc=jemalloc-5.3.0 bits=64 build=75a9b36b1f0aa00`, sha256 `b0cd304980112fa6cd8dced524fdd6b136fd8efb802f5cf9e81610051a11891c`
- candidate commit: `0b5f4b1d`
- date: 2026-11-17; FULL=0, SCALE=1
- run time: 22-scan-large-count 40s

## 22-scan-large-count

| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |
|---|---|---|---:|---:|---:|---:|---|---|
| t4 | dbsize | keys | 40,000 | 40,000 | 40,000 | - | bound: eq: nothing recorded | new |
| t4 | ping_max_ms | ms | 4.65 | 2.19 | 5.34 | - | info | info |
| t4 | ping_p99_ms | ms | 2.26 | 0.434 | 2.73 | - | rel: rel 50%: nothing recorded | new |
| t4 | scan_call_us | us | 672.4 | 121.6 | 661.0 | - | rel: rel 25%: nothing recorded | new |
| t4 | scan_calls | calls | 4 | 4 | 4 | - | info | info |
| t4 | scan_returned | keys | 0 | 0 | 0 | - | bound: eq: nothing recorded | new |
| t4 | scan_server_us | us | 603.4 | 66.15 | 599.7 | - | rel: rel 25%: nothing recorded<br>ratio: ratio: nothing recorded | new<br>new |

