| section | metric | rc8 | rc7 | change | spread | verdict |
|---|---|---|---|---|---|---|
| warefeats ASCII | 1st M RSS B/key | 149.1 | 149.0 | 0.1% | 0.2% (published) | level |
| warefeats ASCII | 1st M used B/key | 146.9 | 146.9 | 0.0% | 0.0% (published) | level |
| warefeats ASCII | 2nd M RSS B/key | 146.9 | 147.4 | -0.3% | 5.6% (published) | level |
| warefeats random | 1st M RSS B/key | 149.2 | 149.2 | 0.0% | 0.2% (published) | level |
| warefeats random | 1st M used B/key | 146.9 | 146.9 | 0.0% | 0.0% (published) | level |
| warefeats random | 2nd M RSS B/key | 146.7 | 146.7 | 0.0% | 0.1% (published) | level |
| warefeats set-get-small | ops/s | 1,310,912 | 1,329,474 | -1.4% | 7.5% (published) | level |
| warefeats set-get-small | p99 ms | 0.335 | 0.339 | -1.2% | 3.2% (published) | level |
| warefeats set-get-small | p99.9 ms | 0.391 | 0.395 | -1.0% | 23.1% (published) | level |
| warefeats set-get-large | ops/s | 318,999 | 318,937 | 0.0% | 0.1% (published) | level |
| warefeats set-get-large | p99 ms | 1.195 | 1.195 | 0.0% | 10.2% (published) | level |
| warefeats set-get-large | p99.9 ms | 1.275 | 1.287 | -0.9% | 3.8% (published) | level |
| warefeats pipeline-30 | ops/s | 17,298,680 | 17,143,693 | 0.9% | 4.1% (published) | level |
| warefeats pipeline-30 | p99 ms | 0.651 | 0.655 | -0.6% | 3.8% (published) | level |
| warefeats pipeline-30 | p99.9 ms | 0.767 | 0.767 | 0.0% | 5.1% (published) | level |
| growth 100 B | RSS B/key at 1M | 153.5 | 154.1 | -0.4% | 0.5% (published) | level |
| growth 100 B | RSS B/key at 5M | 146.7 | 146.8 | -0.0% | 0.2% (published) | level |
| growth 100 B | RSS B/key at 10M | 145.0 | 145.0 | 0.0% | 0.2% (published) | level |
| growth 100 B | RSS B/key at 25M | 141.0 | 141.1 | -0.0% | 0.1% (published) | level |
| growth 100 B | RSS B/key at 50M | 140.7 | 140.7 | -0.0% | 0.0% (published) | level |
| growth 100 B | load seconds, 50M keys | 13.0 | 13.0 | 0.2% | 1.6% (published) | level |
| growth 1024 B | RSS B/key at 1M | 1085.8 | 1085.8 | 0.0% | 0.0% (published) | level |
| growth 1024 B | RSS B/key at 5M | 1077.7 | 1077.4 | 0.0% | 0.0% (published) | level (under 1%) |
| growth 1024 B | RSS B/key at 10M | 1076.1 | 1076.2 | -0.0% | 0.0% (published) | level |
| growth 1024 B | RSS B/key at 25M | 1072.0 | 1072.1 | -0.0% | 0.0% (published) | level |
| growth 1024 B | RSS B/key at 50M | 1071.6 | 1071.7 | -0.0% | 0.0% (published) | level |
| growth 1024 B | load seconds, 50M keys | 23.0 | 23.0 | 0.1% | 0.8% (published) | level |
| mixed 20 min | RSS at end GiB | 3.93 (3.93-3.94) | 3.93 (3.93-3.94) | -0.0% | 0.5% (published) | level |
| mixed 20 min | RSS peak GiB | 4.44 (4.44-4.45) | 4.44 (4.44-4.44) | 0.0% | 0.3% (own) | level |
| mixed 20 min | RSS/used at end | 1.15 (1.15-1.15) | 1.15 (1.15-1.15) | -0.0% | 0.5% (published) | level |
| mixed 20 min | GET p99 ms | 0.46 (0.46-0.46) | 0.46 (0.44-0.48) | 0.0% | 8.7% (own) | level |
| mixed 20 min | GET p99.9 ms | 0.63 (0.60-0.66) | 0.65 (0.61-0.68) | -2.5% | 21.9% (published) | level |
| mixed 20 min | SET p99 ms | 0.48 (0.47-0.49) | 0.48 (0.46-0.50) | -0.8% | 15.7% (published) | level |
| mixed 20 min | SET p99.9 ms | 0.76 (0.71-0.81) | 0.79 (0.75-0.83) | -3.5% | 24.4% (published) | level |
| snapshot BGSAVE | memory added GiB | 3.57 (3.55-3.59) | 3.50 (3.50-3.51) | 1.8% | 1.9% (published) | level |
| snapshot BGSAVE | duration s | 82.2 (82.0-82.3) | 80.9 (80.7-81.1) | 1.6% | 1.4% (published) | **worse beyond spread** |
| snapshot BGSAVE | worst write ms | 214 (203-224) | 206 (206-207) | 3.5% | 10.8% (published) | level |
| snapshot BGSAVE | write p99.9 ms | 1.92 (1.85-1.98) | 1.98 (1.97-1.98) | -3.0% | 11.0% (published) | level |
| snapshot BGSAVE | write p99 before ms | 1.44 (1.42-1.46) | 1.27 (1.25-1.29) | 12.9% | 3.4% (published) | **worse beyond spread** |
| snapshot BGSAVE | memory 60 s after GiB | 33.84 (33.83-33.85) | 33.77 (33.77-33.77) | 0.2% | 0.1% (published) | level (under 1%) |
| snapshot BGREWRITEAOF | memory added GiB | 2.40 (2.38-2.42) | 2.40 (2.32-2.48) | 0.1% | 6.6% (own) | level |
| snapshot BGREWRITEAOF | duration s | 83.4 (83.1-83.7) | 82.1 (82.0-82.1) | 1.6% | 0.9% (published) | **worse beyond spread** |
| snapshot BGREWRITEAOF | worst write ms | 216 (215-218) | 220 (219-221) | -1.8% | 2.4% (published) | level |
| snapshot BGREWRITEAOF | write p99.9 ms | 1.87 (1.81-1.94) | 1.90 (1.85-1.95) | -1.5% | 15.6% (published) | level |
| snapshot BGREWRITEAOF | write p99 before ms | 1.34 (1.29-1.39) | 1.27 (1.26-1.29) | 5.3% | 16.6% (published) | level |
| snapshot BGREWRITEAOF | memory 60 s after GiB | 35.93 (35.64-36.22) | 35.87 (35.52-36.22) | 0.2% | 1.9% (own) | level |
| #284 eviction | keys evicted | 0 | 0 | -% | 0.0% (published) | - |
| active defrag | RSS final MB | 389 (388-390) | 394 (387-402) | -1.4% | 3.9% (own) | level |
| active defrag | RSS returned MB | 334 (333-335) | 328 (320-335) | 1.9% | 4.7% (own) | level |
| active defrag | give-back s | 12.0 (11.9-12.1) | 10.9 (9.8-12.1) | 9.7% | 21.2% (own) | level |
| active defrag | compaction s | 12 (12-13) | 10 (9-12) | 19.0% | 28.6% (own) | level |
| active defrag | slab final MB | 321 (321-321) | 328 (321-336) | -2.3% | 4.7% (own) | level |
| active defrag | GET p99 during compaction ms | 0.43 (0.32-0.54) | 0.49 (0.40-0.57) | -11.5% | 74.1% (published) | level |
| active defrag | GET p99.9 during compaction ms | 0.70 (0.66-0.74) | 0.67 (0.54-0.79) | 5.7% | 57.2% (published) | level |
| active defrag | SET p99 during compaction ms | 0.42 (0.23-0.61) | 0.17 (0.14-0.20) | 143.9% | 91.1% (own) | **worse beyond spread** |
| active defrag | SET p99.9 during compaction ms | 0.53 (0.26-0.81) | 0.32 (0.19-0.46) | 65.6% | 104.7% (own) | level |
| active defrag | GET p99 during / before (same run) | 0.99 (0.99-1.00) | 1.11 (1.06-1.16) | -10.8% | 22.9% (published) | level |
| active defrag | GET p99.9 during / before (same run) | 0.99 (0.98-1.01) | 1.01 (1.00-1.03) | -2.0% | 26.1% (published) | level |
| active defrag | SET p99 during / before (same run) | 1.02 (0.99-1.06) | 1.00 (1.00-1.00) | 2.1% | 13.2% (published) | level |
| active defrag | SET p99.9 during / before (same run) | 1.05 (1.03-1.07) | 1.02 (0.97-1.07) | 2.9% | 34.1% (published) | level |
| AOF-on writes | SET p50 ms | 0.399 (0.375-0.407) | 0.395 (0.351-0.407) | 1.0% | 14.2% (own) | level |
| AOF-on writes | SET p99 ms | 0.983 (0.935-1.015) | 0.971 (0.903-1.007) | 1.2% | 11.2% (published) | level |
| AOF-on writes | SET p99.9 ms | 1.679 (1.343-2.023) | 1.547 (1.327-2.015) | 8.5% | 44.5% (own) | level |
| AOF-on writes | worst write ms | 134 (100-162) | 118 (94-133) | 14.3% | 46.3% (own) | level |
| AOF-on writes | engine CPU s | 562 (556-564) | 564 (558-570) | -0.4% | 2.7% (published) | level |

Flagged: snapshot BGSAVE duration s (+1.6%, spread 1.4%); snapshot BGSAVE write p99 before ms (+12.9%, spread 3.4%); snapshot BGREWRITEAOF duration s (+1.6%, spread 0.9%); active defrag SET p99 during compaction ms (+143.9%, spread 91.1%)

### rc8 against the published 75482abb numbers (other session; pair A is another region)

| section | metric | rc8 (this run) | 75482abb (published median) | change |
|---|---|---|---|---|
| warefeats ASCII | 1st M RSS B/key | 149.1 | 149.2 | -0.1% |
| warefeats ASCII | 1st M used B/key | 146.9 | 146.9 | 0.0% |
| warefeats ASCII | 2nd M RSS B/key | 146.9 | 146.8 | 0.1% |
| warefeats random | 1st M RSS B/key | 149.2 | 149.2 | 0.0% |
| warefeats random | 1st M used B/key | 146.9 | 146.9 | 0.0% |
| warefeats random | 2nd M RSS B/key | 146.7 | 146.7 | 0.0% |
| warefeats set-get-small | ops/s | 1,310,912 | 1,004,305 | 30.5% |
| warefeats set-get-small | p99 ms | 0.335 | 0.375 | -10.7% |
| warefeats set-get-small | p99.9 ms | 0.391 | 0.475 | -17.7% |
| warefeats set-get-large | ops/s | 318,999 | 320,324 | -0.4% |
| warefeats set-get-large | p99 ms | 1.195 | 1.103 | 8.3% |
| warefeats set-get-large | p99.9 ms | 1.275 | 1.271 | 0.3% |
| warefeats pipeline-30 | ops/s | 17,298,680 | 15,630,271 | 10.7% |
| warefeats pipeline-30 | p99 ms | 0.651 | 0.735 | -11.4% |
| warefeats pipeline-30 | p99.9 ms | 0.767 | 0.943 | -18.7% |
| growth 100 B | RSS B/key at 1M | 153.5 | 153.8 | -0.2% |
| growth 100 B | RSS B/key at 5M | 146.7 | 146.4 | 0.2% |
| growth 100 B | RSS B/key at 10M | 145.0 | 145.1 | -0.0% |
| growth 100 B | RSS B/key at 25M | 141.0 | 141.1 | -0.1% |
| growth 100 B | RSS B/key at 50M | 140.7 | 140.7 | -0.0% |
| growth 100 B | load seconds, 50M keys | 13.0 | 13.0 | 0.4% |
| growth 1024 B | RSS B/key at 1M | 1085.8 | 1085.7 | 0.0% |
| growth 1024 B | RSS B/key at 5M | 1077.7 | 1077.6 | 0.0% |
| growth 1024 B | RSS B/key at 10M | 1076.1 | 1076.2 | -0.0% |
| growth 1024 B | RSS B/key at 25M | 1072.0 | 1072.1 | -0.0% |
| growth 1024 B | RSS B/key at 50M | 1071.6 | 1071.7 | -0.0% |
| growth 1024 B | load seconds, 50M keys | 23.0 | 22.9 | 0.5% |
| mixed 20 min | RSS at end GiB | 3.93 | 3.93 | 0.1% |
| mixed 20 min | RSS peak GiB | 4.44 | 4.44 | 0.1% |
| mixed 20 min | RSS/used at end | 1.15 | 1.15 | -0.3% |
| mixed 20 min | GET p99 ms | 0.46 | 0.46 | -0.2% |
| mixed 20 min | GET p99.9 ms | 0.63 | 0.69 | -8.6% |
| mixed 20 min | SET p99 ms | 0.48 | 0.51 | -6.1% |
| mixed 20 min | SET p99.9 ms | 0.76 | 0.92 | -17.1% |
| snapshot BGSAVE | memory added GiB | 3.57 | 3.12 | 14.3% |
| snapshot BGSAVE | duration s | 82.2 | 70.4 | 16.7% |
| snapshot BGSAVE | worst write ms | 214 | 209 | 2.2% |
| snapshot BGSAVE | write p99.9 ms | 1.92 | 1.96 | -2.3% |
| snapshot BGSAVE | write p99 before ms | 1.44 | 1.39 | 3.5% |
| snapshot BGSAVE | memory 60 s after GiB | 33.84 | 33.39 | 1.3% |
| snapshot BGREWRITEAOF | memory added GiB | 2.40 | 2.08 | 15.4% |
| snapshot BGREWRITEAOF | duration s | 83.4 | 72.0 | 15.8% |
| snapshot BGREWRITEAOF | worst write ms | 216 | 221 | -2.1% |
| snapshot BGREWRITEAOF | write p99.9 ms | 1.87 | 1.87 | 0.1% |
| snapshot BGREWRITEAOF | write p99 before ms | 1.34 | 1.40 | -4.1% |
| snapshot BGREWRITEAOF | memory 60 s after GiB | 35.93 | 34.96 | 2.8% |
| #284 eviction | keys evicted | 0 | 0 | -% |
| active defrag | RSS final MB | 389 | 389 | 0.0% |
| active defrag | RSS returned MB | 334 | 334 | -0.0% |
| active defrag | give-back s | 12.0 | 12.1 | -1.1% |
| active defrag | compaction s | 12 | 13 | -3.8% |
| active defrag | slab final MB | 321 | 321 | -0.0% |
| active defrag | GET p99 during compaction ms | 0.43 | 0.51 | -15.5% |
| active defrag | GET p99.9 during compaction ms | 0.70 | 0.79 | -11.0% |
| active defrag | SET p99 during compaction ms | 0.42 | 0.24 | 73.8% |
| active defrag | SET p99.9 during compaction ms | 0.53 | 0.36 | 48.6% |
| active defrag | GET p99 during / before (same run) | 0.99 | 1.00 | -0.7% |
| active defrag | GET p99.9 during / before (same run) | 0.99 | 0.99 | 0.3% |
| active defrag | SET p99 during / before (same run) | 1.02 | 1.00 | 2.1% |
| active defrag | SET p99.9 during / before (same run) | 1.05 | 0.95 | 10.6% |
| AOF-on writes | SET p50 ms | 0.399 | 0.363 | 9.9% |
| AOF-on writes | SET p99 ms | 0.983 | 0.911 | 7.9% |
| AOF-on writes | SET p99.9 ms | 1.679 | 1.283 | 30.9% |
| AOF-on writes | worst write ms | 134 | 166 | -19.0% |
| AOF-on writes | engine CPU s | 562 | 554 | 1.4% |
