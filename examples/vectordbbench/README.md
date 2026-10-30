# VectorDBBench: KiviDB 1.1.2 on Cohere 1M

KiviDB's vector search, measured with [VectorDBBench](https://github.com/zilliztech/VectorDBBench). This is the tool behind the [VDBBench leaderboard](https://zilliz.com/vdbbench-leaderboard), so the result can be set beside the leaderboard's. The case is the leaderboard's main one: **Search Performance Test (1M Dataset, 768 Dim)** (`Performance768D1M`), over Cohere's 1,000,000 768-dimension vectors with cosine distance.

## Result

Measured on 2026-10-27, with KiviDB 1.1.2, the release binary (default build).

| | |
|---|---|
| **QPS** (the most, over the concurrencies tried) | **2,434** |
| **Recall** (k = 100) | **0.930** |
| nDCG | 0.940 |
| Latency, one client at a time | p50 5.3 ms, p95 6.7 ms, **p99 7.1 ms** |
| Load (1M inserts, the index built as they arrive) | 2,806 s |

| Clients at once | 1 | 5 | 10 | 20 | 30 | 40 | 60 | 80 | 100 | 120 |
|---|---|---|---|---|---|---|---|---|---|---|
| QPS | 187 | 972 | 1,826 | 2,359 | 2,428 | **2,434** | 2,424 | 2,402 | 2,416 | 2,403 |
| p99 (ms) | 7.2 | 7.0 | 8.0 | 14.6 | 20.7 | 21.7 | 34.2 | 41.6 | 52.5 | 60.9 |

QPS grows with the number of clients, at about 190 a client, until the server's 16 cores are busy. From there it stays at about 2,430. The full result, as VectorDBBench writes it, is in [`results/`](results/).

### Beside the leaderboard

These are the leaderboard's search performance results for the 1M dataset, as shown on 2026-10-27, with KiviDB's added:

| Database | QPS | Recall | p99 |
|---|---|---|---|
| ZillizCloud 8cu-perf | 13,316 | 0.938 | 2.0 ms |
| Milvus 16c64g, sq4u-fp16, force_merge | 9,576 | 0.919 | 2.3 ms |
| Milvus 16c64g, sq8, force_merge | 5,973 | 0.919 | 2.4 ms |
| OpenSearch 16c128g, force_merge | 3,055 | 0.907 | 7.2 ms |
| **KiviDB 1.1.2, 16c64g** | **2,434** | **0.930** | **7.1 ms** |
| ElasticCloud 8c60g, force_merge | 2,354 | 0.914 | 17.1 ms |
| QdrantCloud 16c64g | 1,242 | 0.947 | 6.4 ms |
| Pinecone p2.x8, 1 node | 1,147 | 0.926 | 13.7 ms |
| OpenSearch 16c128g | 951 | 0.914 | 13.2 ms |

Read it with these in mind:

- **This is not a leaderboard entry.** The leaderboard's systems were run by its maintainers, on their machines. This run used machines of the same size (16 cores, 64 GB) on Azure, not the same machines.
- **Recall decides what QPS means.** A system can buy QPS with lower recall. KiviDB searches the vectors as given, at full precision (float32). The two Milvus entries ahead of it compress them (sq8, sq4u/fp16), at a recall of 0.919.
- **Loading is slow.** KiviDB builds its HNSW graph as vectors arrive, and VectorDBBench inserts from 4 workers, so the 1M vectors took 47 minutes to load. Systems that index after loading (`force_merge`) load much faster. The leaderboard's search table does not count load time.

## Setup

Two machines in one private network, in Azure's Central India region:

| | Size | |
|---|---|---|
| Database | `Standard_D16s_v5`: 16 vCPU (Intel Xeon), 64 GB, Ubuntu 24.04 | KiviDB 1.1.2, `io_uring`, no persistence settings beyond the defaults |
| Client | `Standard_D16s_v5`, the same | VectorDBBench with KiviDB's client |

The client machine is as large as the database machine, so that the client, written in Python, is not what limits QPS.

KiviDB's index settings were HNSW with `M 16`, `EF_CONSTRUCTION 200`, `EF_RUNTIME 128`, and cosine distance. Each concurrency ran for 30 seconds. Recall and the single-client latency come from VectorDBBench's serial pass over the 1,000 test queries.

## Run it yourself

1. **Start the database machine** with [`server.sh`](server.sh). It downloads the KiviDB release and starts it on port 6380 with a password:

   ```bash
   KIVIDB_PASSWORD=$(openssl rand -hex 16) ./server.sh
   ```

   Keep port 6380 reachable from the client machine only.

2. **Run the benchmark from the client machine** with [`client.sh`](client.sh). It installs VectorDBBench with the KiviDB client and runs the case:

   ```bash
   KIVIDB_HOST=<database machine's private address> KIVIDB_PASSWORD=<the same> ./client.sh
   ```

   The first run downloads the dataset (about 3 GB), loads it (about 47 minutes on the machines above), and searches. To try another `EF_RUNTIME` on the same index, add `--skip-drop-old --skip-load` to the `vectordbbench` command and set `EF_RUNTIME`. A higher value gives higher recall and lower QPS.

The KiviDB client is part of VectorDBBench since [zilliztech/VectorDBBench#884](https://github.com/zilliztech/VectorDBBench/pull/884). It was merged after the 2.0.0 release, so until a release on PyPI has it, `client.sh` installs VectorDBBench from its `main` branch.
