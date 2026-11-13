# Memory suite on 1.2.1 (35a413c0): notes

## Build
- Tree comb.tgz (35a413c0), built on the round 1 engine VM the way release.yml builds it: an amazonlinux:2023 container, rustup stable = rustc 1.99.1 (58cf8f9c3 2026-10-13), `cargo build --release` (fat LTO, codegen-units 1), default features.
- Tarball SHA-256 5e98af075db8d44553fe5057a090aa1ae5ac838aae180c9976b2c2bf61cefde7. Binary b7ebacb76961b4b8b52981728b4cddae9c65f17a71cbb2d0c022736afed86b0f (`--version` prints 1.2.1-rc2, as expected).
- 1.2.0: the release tarball, SHA256SUMS check OK, binary a211d544be79bb78cfc5bc16b1e1315d20913f154cd8179f80668ab048edd72f.

## VMs and timeline (2026-11-12 UTC)
- Pair A r1, koreacentral (RG kividb-c3-mem); r2, germanywestcentral (kividb-c3-mem2, the rc2 region); r3, eastus (kividb-c3-mem3). westeurope refused new VMs: "not accepting new customers". All up about 04:00-05:30.
- Pair B, polandcentral, in kividb-c3-mem3: about 04:00-05:25.
- 4th pair-A set, germanywestcentral (kividb-c3-mem4), about 05:35-06:40: the load-time test and the 4 eviction runs left unfinished when the first three sets were deleted early. Those runs keep their round labels (r1 for both builds, r2 for 1.2.1, r3 for 1.2.0), but they ran on this set.
- No auto-shutdown was possible in polandcentral or eastus; those sets were removed by deleting their RGs. All four RGs are deleted.
- The competitors were not rerun.

## Eviction (#284)
All 6 runs (3 per build) are complete, with 0 keys evicted and dbsize 10,000,000 before and after. The 4 runs finished on the 4th set: maxmemory 11,785,446,923 / 11,785,445,295 / 11,785,919,113 / 11,785,919,944 bytes, used_memory at the end 10.72 GB.

## Analysis
`python -I harness/analyze.py raw/pair-a raw/pair-b warefeats charts` with CAND_LABEL="KiviDB 1.2.1 (35a413c0)" writes results/tables.md. Before it runs, the warefeats JSONs are renamed `<build>-r<N>.json`, and checkpoints.csv and events.csv are concatenated across the pairs. The rows of the 4 cut-short eviction runs are removed from events.csv, and their files are replaced. The load-time table comes from loadtime.csv (medians of 3).
